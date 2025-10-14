import os
import sys
import time
import threading
from typing import Dict, Set
from imapclient import IMAPClient, exceptions
from config import *
from logger.logger_setup import logger as log, get_user_logger, get_system_logger
from mail_parser.auth_handler import token_manager
from mail_parser.processor import process_uid
from Utility.mongo_utils import mongo_connect
from Utility.user_manager import user_manager
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class UserMonitorThread:
    """Individual monitoring thread for a single user."""
    
    def __init__(self, username: str):
        self.username = username
        self.user_log = get_user_logger(username)
        self.system_log = get_system_logger()
        self.thread = None
        self.stop_event = threading.Event()
        self.mongo_col = None
        self.client = None
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5
        
    def start(self):
        """Start the monitoring thread for this user."""
        if self.thread and self.thread.is_alive():
            self.user_log.warning("Monitor thread already running")
            return False
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        self.user_log.info("Email monitoring started")
        return True
    
    def stop(self):
        """Stop the monitoring thread for this user."""
        if not self.thread or not self.thread.is_alive():
            return
        
        self.stop_event.set()
        
        # Close IMAP connection
        if self.client:
            try:
                self.client.logout()
            except:
                pass
            self.client = None
        
        # Wait for thread to finish
        if self.thread:
            self.thread.join(timeout=10)
            if self.thread.is_alive():
                self.user_log.warning("Monitor thread did not stop gracefully")
            else:
                self.user_log.info("Email monitoring stopped")
        
        # Update monitoring status in database
        user_manager.set_monitoring_status(self.username, False)
    
    def is_running(self) -> bool:
        """Check if the monitoring thread is running."""
        return self.thread and self.thread.is_alive() and not self.stop_event.is_set()
    
    def _connect_with_oauth(self):
        """Connect to IMAP using OAuth authentication for this user."""
        try:
            # Check if token needs proactive refresh
            if token_manager.is_token_about_to_expire(self.username, buffer_seconds=300):
                self.user_log.info("Token expiring soon, refreshing proactively...")
                token_manager.refresh_access_token(self.username)
            
            access_token = token_manager.get_valid_access_token(self.username)
            if not access_token:
                raise Exception("No valid access token available")
            
            # Create client with timeout to prevent hanging
            client = IMAPClient(IMAP_SERVER, ssl=True, timeout=30)
            client.oauth2_login(self.username, access_token)
            client.select_folder(MAILBOX, readonly=True)
            
            self.user_log.debug("IMAP connected")
            return client
            
        except Exception as e:
            self.user_log.error(f"OAuth IMAP connection failed: {e}")
            
            # Try to refresh token once
            self.user_log.info("Attempting to refresh OAuth token...")
            try:
                token_manager.refresh_access_token(self.username, force=True)
                access_token = token_manager.get_valid_access_token(self.username)
                if not access_token:
                    raise Exception("Failed to get access token after refresh")
                
                # Retry connection with fresh token
                client = IMAPClient(IMAP_SERVER, ssl=True, timeout=30)
                client.oauth2_login(self.username, access_token)
                client.select_folder(MAILBOX, readonly=True)
                
                self.user_log.debug("IMAP connection successful after token refresh")
                return client
                
            except Exception as refresh_error:
                self.user_log.error(f"Token refresh failed: {refresh_error}")
                raise
    
    def _safe_connect(self):
        """Safely connect with retries."""
        # Close existing client if it exists
        if self.client:
            try:
                self.client.logout()
            except:
                pass
            self.client = None
        
        for attempt in range(MAX_RECONNECT_ATTEMPTS):
            if self.stop_event.is_set():
                return False
            
            try:
                # Connect to MongoDB only once
                if self.mongo_col is None:
                    self.mongo_col = mongo_connect()
                    self.user_log.info("MongoDB connected")
                
                self.client = self._connect_with_oauth()
                if attempt == 0:  # Only log on first successful attempt
                    self.user_log.debug("IMAP connection ready")
                return True
                
            except Exception as e:
                self.user_log.error(f"Connection attempt {attempt+1} failed: {e}")
                self.client = None
                if attempt < MAX_RECONNECT_ATTEMPTS - 1:
                    delay = RECONNECT_DELAY * (attempt + 1)
                    self.user_log.info(f"Retrying in {delay} seconds...")
                    if self.stop_event.wait(delay):  # Use wait with timeout for clean shutdown
                        return False
                else:
                    self.user_log.error("Max reconnection attempts reached")
                    return False
        return False
    
    def _initialize_uid_tracking(self):
        """Initialize UID tracking for this user."""
        last_uid = user_manager.get_last_processed_uid(self.username)
        if last_uid is None:
            try:
                all_uids = self.client.search(['ALL'])
                if all_uids:
                    latest_uid = max(all_uids)
                    user_manager.update_last_processed_uid(self.username, latest_uid)
            except Exception as e:
                self.user_log.warning(f"UID initialization failed: {e}, will process all new emails")
    
    def _monitor_loop(self):
        """Main monitoring loop for this user."""
        self.user_log.info("Starting email monitor...")
        
        # Set monitoring status in database
        user_manager.set_monitoring_status(self.username, True)
        
        # Initial connection
        if not self._safe_connect():
            self.user_log.error("Failed to establish initial connections, exiting...")
            user_manager.set_monitoring_status(self.username, False)
            return
        
        # Initialize UID tracking
        try:
            self._initialize_uid_tracking()
        except Exception as e:
            self.user_log.warning(f"UID initialization failed: {e}")
        
        self.consecutive_errors = 0
        
        try:
            while not self.stop_event.is_set():
                try:
                    self.user_log.debug("Checking for new emails...")
                    
                    # Check if token was refreshed by another component
                    if token_manager.has_token_changed(self.username):
                        self.user_log.info("Token refreshed by another component, reconnecting...")
                        self.client = None
                        if not self._safe_connect():
                            self.user_log.error("Failed to reconnect after token change. Retrying cycle.")
                            if self.stop_event.wait(RECONNECT_DELAY):
                                break
                            continue
                    
                    # Enhanced IDLE handling with better error recovery
                    try:
                        self.client.idle()
                        responses = self.client.idle_check(timeout=IDLE_TIMEOUT)
                        self.client.idle_done()
                        # Log raw IDLE server responses for debugging
                        if responses:
                            self.user_log.debug(f"IDLE responses: {responses}")
                            
                    except (exceptions.IMAPClientError, OSError, ConnectionError) as idle_error:
                        self.user_log.warning(f"IDLE operation failed: {idle_error}")
                        # Try to gracefully exit IDLE if possible
                        try:
                            self.client.idle_done()
                        except:
                            pass  
                        
                        # Force reconnection for IDLE failures
                        self.user_log.info("Forcing reconnection due to IDLE failure...")
                        self.client = None  # Mark client as invalid
                        if not self._safe_connect():
                            self.user_log.error("Failed to reconnect after IDLE failure. Retrying cycle.")
                            if self.stop_event.wait(RECONNECT_DELAY):
                                break
                            continue

                    if responses:
                        self.user_log.info("New email detected by IDLE")
                        # Process new emails
                        try:
                            # Determine last processed UID
                            last_processed_uid = user_manager.get_last_processed_uid(self.username)
                            
                            # Helper to search for new UIDs using server-side UID range
                            def search_new_uids():
                                if last_processed_uid:
                                    return self.client.search(['UID', f"{int(last_processed_uid)+1}:*"])
                                else:
                                    return self.client.search(['ALL'])

                            # First attempt right after IDLE
                            new_uids = search_new_uids()

                            # If none found immediately after IDLE, wait briefly and retry once
                            if not new_uids:
                                time.sleep(1.0)
                                new_uids = search_new_uids()

                            if new_uids:
                                # Sort to ensure sequential processing
                                new_uids = sorted(new_uids)

                                if last_processed_uid:
                                    self.user_log.info(f"Processing {len(new_uids)} new emails: UIDs {new_uids}")
                                    for uid in new_uids:
                                        if self.stop_event.is_set():
                                            break
                                        try:
                                            self.user_log.debug(f"Processing UID: {uid}")
                                            success = process_uid(self.client, self.mongo_col, uid, folder=MAILBOX, username=self.username)
                                            if success:
                                                user_manager.update_last_processed_uid(self.username, uid)
                                                self.user_log.info(f"Processed UID {uid} (last={user_manager.get_last_processed_uid(self.username)})")
                                            else:
                                                self.user_log.warning(f"Failed to process UID {uid}, continuing with next")
                                        except Exception as e:
                                            self.user_log.exception(f"Error processing UID {uid}: {e}")
                                else:
                                    latest_uid = new_uids[-1]
                                    self.user_log.info(f"Processing latest email UID: {latest_uid}")
                                    success = process_uid(self.client, self.mongo_col, latest_uid, folder=MAILBOX, username=self.username)
                                    if success:
                                        user_manager.update_last_processed_uid(self.username, latest_uid)
                            else:
                                self.user_log.debug("IDLE fired but no new UIDs yet (possible flag change or slight delay)")
                        except Exception as e:
                            self.user_log.error(f"Failed to process new email from IDLE: {e}")
                    else:
                        # No IDLE responses - just send keep-alive, don't search for emails
                        self.user_log.debug("IDLE timeout - sending NOOP keep-alive")
                        try:
                            self.client.noop()
                        except (exceptions.IMAPClientError, OSError) as e:
                            self.user_log.warning(f"NOOP keep-alive failed: {e}. Reconnecting...")
                            self.client = None  # Mark client as invalid
                            if not self._safe_connect():
                                self.user_log.error("Failed to reconnect after NOOP failure. Retrying cycle.")
                                if self.stop_event.wait(RECONNECT_DELAY):
                                    break
                                continue
                    
                    self.consecutive_errors = 0

                except (exceptions.IMAPClientError, OSError) as e:
                    self.user_log.exception(f"IMAP error during IDLE cycle, reconnecting: {e}")
                    if not self._safe_connect():
                        self.user_log.error("Failed to reconnect, will retry after delay.")
                        if self.stop_event.wait(RECONNECT_DELAY):
                            break
                except Exception as ex:
                    self.user_log.exception(f"Unexpected exception in main loop: {ex}")
                    self.consecutive_errors += 1
                    if self.consecutive_errors > self.max_consecutive_errors:
                        self.user_log.error("Multiple consecutive errors, attempting full reconnect.")
                        if self._safe_connect():
                            self.consecutive_errors = 0
                        else:
                            self.user_log.error("Full reconnect failed, will retry after delay.")
                            if self.stop_event.wait(RECONNECT_DELAY * self.consecutive_errors):
                                break

        except Exception as e:
            self.user_log.critical(f"Critical error in email monitor: {e}")
        finally:
            try:
                if self.client:
                    self.client.logout()
            except Exception:
                pass
            user_manager.set_monitoring_status(self.username, False)
            self.user_log.info("Email monitor stopped")

# Multi-tenant monitor manager
class MultiTenantMonitor:
    """Manages multiple user monitoring threads."""
    
    def __init__(self):
        self.user_threads: Dict[str, UserMonitorThread] = {}
        self.lock = threading.Lock()
        self.system_log = get_system_logger()
        self.monitoring_active = False
        
    def start_monitoring(self):
        """Start monitoring for all active users."""
        with self.lock:
            if self.monitoring_active:
                self.system_log.info("Multi-tenant monitoring already active")
                return
            
            self.monitoring_active = True
            self.system_log.info("Starting multi-tenant email monitoring...")
            
            # Get all active users from database
            active_users = user_manager.get_all_active_users()
            self.system_log.info(f"Found {len(active_users)} active users")
            
            for user in active_users:
                username = user['username']
                if username not in self.user_threads:
                    self._start_user_monitoring(username)
    
    def _start_user_monitoring(self, username: str):
        """Start monitoring for a specific user."""
        try:
            if username in self.user_threads and self.user_threads[username].is_running():
                self.system_log.debug(f"Monitoring already active for {username}")
                return
            
            user_thread = UserMonitorThread(username)
            if user_thread.start():
                self.user_threads[username] = user_thread
                self.system_log.info(f"Started monitoring for user: {username}")
            else:
                self.system_log.error(f"Failed to start monitoring for user: {username}")
        except Exception as e:
            self.system_log.error(f"Error starting monitoring for {username}: {e}")
    
    def stop_user_monitoring(self, username: str):
        """Stop monitoring for a specific user."""
        with self.lock:
            if username in self.user_threads:
                self.user_threads[username].stop()
                del self.user_threads[username]
                self.system_log.info(f"Stopped monitoring for user: {username}")
    
    def stop_all_monitoring(self):
        """Stop monitoring for all users."""
        with self.lock:
            if not self.monitoring_active:
                return
            
            self.monitoring_active = False
            self.system_log.info("Stopping all user monitoring...")
            
            for username, user_thread in list(self.user_threads.items()):
                user_thread.stop()
            
            self.user_threads.clear()
            self.system_log.info("All user monitoring stopped")
    
    def add_user_monitoring(self, username: str):
        """Add monitoring for a new user."""
        with self.lock:
            if self.monitoring_active:
                self._start_user_monitoring(username)
    
    def get_monitoring_status(self) -> Dict:
        """Get monitoring status for all users."""
        with self.lock:
            status = {
                "active": self.monitoring_active,
                "total_threads": len(self.user_threads),
                "users": {}
            }
            
            for username, user_thread in self.user_threads.items():
                status["users"][username] = {
                    "running": user_thread.is_running(),
                    "consecutive_errors": user_thread.consecutive_errors
                }
            
            return status

# Global multi-tenant monitor instance
mt_monitor = MultiTenantMonitor()

# Multi-tenant IMAP connection function
def connect_with_oauth(username: str):
    """Connect to IMAP using OAuth authentication for a specific user."""
    user_thread = UserMonitorThread(username)
    return user_thread._connect_with_oauth()

def start_email_monitor():
    """Start multi-tenant email monitoring for all active users."""
    log.info("Starting multi-tenant email monitor...")
    mt_monitor.start_monitoring()