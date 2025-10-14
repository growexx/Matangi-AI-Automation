import sys
import os
import signal
import time
from config import *
from logger.logger_setup import get_system_logger
from mail_parser.auth_handler import start_oauth_server
from mail_parser.imap_monitor import mt_monitor
from Utility.user_manager import user_manager
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        system_log = get_system_logger()
        system_log.info(f"Received signal {signum}, shutting down gracefully...")
        mt_monitor.stop_all_monitoring()
        system_log.info("Shutdown complete")
        exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def main():
    """Main entry point - Multi-Tenant Email Automation System."""
    system_log = get_system_logger()
    system_log.info("=== Multi-Tenant Email Automation System Starting ===")
    
    # Setup signal handlers for graceful shutdown
    setup_signal_handlers()
    
    try:
        # Start OAuth server for user authentication
        system_log.info("Starting OAuth authentication server...")
        if not start_oauth_server():
            system_log.error("Failed to start OAuth server")
            exit(1)
        
        system_log.info("OAuth server running at: http://localhost:5000")
        system_log.info("Users can visit the URL to authenticate and start email monitoring")
        
        # Start monitoring for existing users
        system_log.info("Starting monitoring for existing users...")
        mt_monitor.start_monitoring()
        
        # Main loop - keep the system running
        system_log.info("System is ready and running...")
        
        while True:
            time.sleep(60)  # Sleep for 60 seconds
            
            # Periodically check for new users and start monitoring
            active_users = user_manager.get_all_active_users()
            for user in active_users:
                username = user['username'] 
                if not user_manager.is_user_monitoring_active(username):
                    system_log.info(f"Starting monitoring for newly activated user: {username}")
                    mt_monitor.add_user_monitoring(username)
            
            # Log monitoring status periodically  
            status = mt_monitor.get_monitoring_status()
            if status['total_threads'] > 0:
                system_log.debug(f"Monitoring Status: {status['total_threads']} active threads for {len(status['users'])} users")
            
    except KeyboardInterrupt:
        system_log.info("Received keyboard interrupt, shutting down...")
        mt_monitor.stop_all_monitoring()
    except Exception as e:
        system_log.error(f"System error: {e}")
        system_log.exception("Detailed error:")
        mt_monitor.stop_all_monitoring()
        exit(1)

if __name__ == '__main__':
    main()
