import logging
import os
import stat
import configparser
import threading
from datetime import datetime, timezone
import pytz
from typing import Dict

# Load configuration
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.ini'))

# Get the project root directory (parent of logger directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logs_dir = os.path.join(project_root, config.get('logging', 'log_dir'))
log_file = config.get('logging', 'log_file')
timezone_name = config.get('logging', 'timezone')

try:
    tz_info = pytz.timezone(timezone_name)
except Exception:
    tz_info = timezone.utc
    timezone_name = 'UTC'

# Create logs directory if it doesn't exist with secure permissions
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir, mode=0o700)  # Owner read/write/execute only

# Set up log file path
log_file_path = os.path.join(logs_dir, log_file)


# Define a custom formatter that includes timezone
class TZFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=tz_info)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

# Create a file handler with safe permissions
file_handler = logging.FileHandler(log_file_path, mode="a")
file_handler.setFormatter(TZFormatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
))

# Create log file if it doesn't exist and set permissions
if not os.path.exists(log_file_path):
    open(log_file_path, 'a').close()
    os.chmod(log_file_path, 0o600)  # Owner read/write only

class MultiTenantLoggerManager:
    """
    Multi-tenant logging manager that creates separate log files for each user.
    Thread-safe and handles concurrent logging from multiple user threads.
    """
    
    def __init__(self):
        self.user_loggers: Dict[str, logging.Logger] = {}
        self.user_handlers: Dict[str, logging.FileHandler] = {}
        self.lock = threading.Lock()
        
        # Create main system logger for non-user-specific events
        self.system_logger = self._create_system_logger()
    
    def _create_system_logger(self) -> logging.Logger:
        """Create system logger for non-user-specific events."""
        system_log_file_path = os.path.join(logs_dir, "system.log")
        
        # Create file handler
        system_file_handler = logging.FileHandler(system_log_file_path, mode="a")
        system_file_handler.setFormatter(TZFormatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %Z"
        ))
        
        # Create console handler
        system_console_handler = logging.StreamHandler()
        system_console_handler.setLevel(logging.INFO)
        system_console_handler.setFormatter(TZFormatter(
            "[%(asctime)s] SYSTEM - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %Z"
        ))
        
        # Configure logger
        system_logger = logging.getLogger("system_logger")
        system_logger.setLevel(logging.DEBUG)
        system_logger.addHandler(system_file_handler)
        system_logger.addHandler(system_console_handler)
        system_logger.propagate = False
        
        # Set file permissions
        if not os.path.exists(system_log_file_path):
            open(system_log_file_path, 'a').close()
        os.chmod(system_log_file_path, 0o600)
        
        return system_logger
    
    def get_user_logger(self, username: str) -> logging.Logger:
        """
        Get or create a logger for a specific user.
        
        Args:
            username: User's email address
            
        Returns:
            Logger instance for the user
        """
        with self.lock:
            if username not in self.user_loggers:
                self._create_user_logger(username)
            return self.user_loggers[username]
    
    def _create_user_logger(self, username: str):
        """Create a new logger for a user."""
        # Sanitize username for filename
        safe_username = username.replace('@', '_at_').replace('.', '_')
        log_filename = f"{safe_username}_logs.log"
        user_log_file_path = os.path.join(logs_dir, log_filename)
        
        # Create file handler with secure permissions
        user_file_handler = logging.FileHandler(user_log_file_path, mode="a")
        user_file_handler.setFormatter(TZFormatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %Z"
        ))
        
        # Create console handler with user prefix
        user_console_handler = logging.StreamHandler()
        user_console_handler.setLevel(logging.INFO)
        user_console_handler.setFormatter(TZFormatter(
            f"[%(asctime)s] {username} - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %Z"
        ))
        
        # Create logger
        logger_name = f"user_{safe_username}"
        user_logger = logging.getLogger(logger_name)
        user_logger.setLevel(logging.DEBUG)
        user_logger.addHandler(user_file_handler)
        user_logger.addHandler(user_console_handler)
        user_logger.propagate = False
        
        # Set file permissions
        if not os.path.exists(user_log_file_path):
            open(user_log_file_path, 'a').close()
        os.chmod(user_log_file_path, 0o600)
        
        # Store references
        self.user_loggers[username] = user_logger
        self.user_handlers[username] = user_file_handler
        
        self.system_logger.info(f"Created logger for user: {username}")
    
    def close_user_logger(self, username: str):
        """Close and remove a user's logger."""
        with self.lock:
            if username in self.user_loggers:
                user_logger = self.user_loggers[username]
                for handler in user_logger.handlers[:]:
                    handler.close()
                    user_logger.removeHandler(handler)
                
                del self.user_loggers[username]
                if username in self.user_handlers:
                    del self.user_handlers[username]
                
                self.system_logger.info(f"Closed logger for user: {username}")
    
    def get_system_logger(self) -> logging.Logger:
        """Get the system logger for non-user-specific events."""
        return self.system_logger
    
    def flush_all_logs(self):
        """Flush all log handlers."""
        with self.lock:
            # Flush system logger
            for handler in self.system_logger.handlers:
                handler.flush()
            
            # Flush user loggers
            for user_logger in self.user_loggers.values():
                for handler in user_logger.handlers:
                    handler.flush()

# Global multi-tenant logger manager
mt_logger_manager = MultiTenantLoggerManager()

# Legacy logger for backward compatibility
logger = logging.getLogger("email_monitor_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

# Add console handler with timezone info
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(TZFormatter(
    "[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
))

logger.addHandler(console_handler)
logger.propagate = False

# Convenience functions for multi-tenant logging
def get_user_logger(username: str) -> logging.Logger:
    """
    Get a logger for a specific user.
    
    Args:
        username: User's email address
        
    Returns:
        Logger instance for the user
    """
    return mt_logger_manager.get_user_logger(username)

def get_system_logger() -> logging.Logger:
    """
    Get the system logger for non-user-specific events.
    
    Returns:
        System logger instance
    """
    return mt_logger_manager.get_system_logger() 