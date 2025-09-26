import logging
import os
import stat
import configparser
from datetime import datetime, timezone
import pytz

# Load configuration
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.ini'))

# Get the project root directory (parent of logger directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logs_dir = os.path.join(project_root, config.get('logging', 'log_dir', fallback='logs'))
log_file = config.get('logging', 'log_file', fallback='logs.log')

# Create logs directory if it doesn't exist with secure permissions
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir, mode=0o700)  # Owner read/write/execute only

# Set up log file path
log_file_path = os.path.join(logs_dir, log_file)


# Define a custom formatter that includes timezone
class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.isoformat()

# Create a file handler with safe permissions
file_handler = logging.FileHandler(log_file_path, mode="a")
file_handler.setFormatter(UTCFormatter(
    "%(asctime)s [UTC] - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Create log file if it doesn't exist and set permissions
if not os.path.exists(log_file_path):
    open(log_file_path, 'a').close()
    os.chmod(log_file_path, 0o600)  # Owner read/write only

# Configure logging
logger = logging.getLogger("email_monitor_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

# Add console handler with timezone info
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(UTCFormatter(
    "[%(asctime)s UTC] %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Configure logger

logger.addHandler(console_handler)
logger.propagate = False 