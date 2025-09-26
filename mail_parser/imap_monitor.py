import os
import sys
import time
from imapclient import IMAPClient, exceptions

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from logger.logger_setup import logger as log
from mail_parser.auth_handler import token_manager
from mail_parser.processor import process_uid
from Utility.mongo_utils import mongo_connect
from Utility.uid_tracker import uid_tracker, initialize_uid_tracking

def connect_with_oauth():
    """Connect to IMAP using OAuth authentication with enhanced resilience."""
    try:
        # Check if token needs proactive refresh
        if token_manager.is_token_about_to_expire(buffer_seconds=300):
            log.info("Token expiring soon, refreshing proactively...")
            token_manager.refresh_access_token()
        
        access_token = token_manager.get_valid_access_token()
        
        # Create client with timeout to prevent hanging
        client = IMAPClient(IMAP_SERVER, ssl=True, timeout=30)
        
        # Use dynamic username from OAuth tokens
        username = token_manager.get_dynamic_username()
        client.oauth2_login(username, access_token)
        client.select_folder(MAILBOX, readonly=True)
        
        # Store current token hash for change detection
        token_manager._current_token_hash = hash(access_token)
        
        log.debug("IMAP connected")
        return client
        
    except Exception as e:
        log.error("OAuth IMAP connection failed: %s", e)
        
        # Try to refresh token once
        log.info("Attempting to refresh OAuth token...")
        try:
            token_manager.refresh_access_token(force=True)
            access_token = token_manager.get_valid_access_token()
            
            # Retry connection with fresh token
            client = IMAPClient(IMAP_SERVER, ssl=True, timeout=30)
            # Use dynamic username from OAuth tokens
            username = token_manager.get_dynamic_username()
            client.oauth2_login(username, access_token)
            client.select_folder(MAILBOX, readonly=True)
            
            log.debug("IMAP connection successful after token refresh")
            return client
            
        except Exception as refresh_error:
            log.error("Token refresh failed: %s", refresh_error)
    
    raise

def start_email_monitor():
    """Main monitoring loop with OAuth authentication - processes only NEW emails detected by IDLE."""

    log.info("Starting email monitor...")


  

    mongo_col = None
    client = None
    
    def safe_connect():
        """Safely connect with retries."""
        nonlocal mongo_col, client
        
        # Close existing client if it exists
        if client:
            try:
                client.logout()
            except:
                pass  # Ignore errors when closing broken connection
            client = None
        
        for attempt in range(MAX_RECONNECT_ATTEMPTS):
            try:
                # Connect to MongoDB only once
                if mongo_col is None:
                    mongo_col = mongo_connect()
                    log.info("MongoDB connected")
                
                client = connect_with_oauth()
                if attempt == 0:  # Only log on first successful attempt
                    log.debug("IMAP connection ready")
                return True
            except Exception as e:
                log.error(f"Connection attempt {attempt+1} failed: {e}")
                client = None  # Ensure client is reset on failure
                if attempt < MAX_RECONNECT_ATTEMPTS - 1:
                    delay = RECONNECT_DELAY * (attempt + 1)
                    log.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    log.error("Max reconnection attempts reached")
                    return False
        return False


    if not safe_connect():
        log.error("Failed to establish initial connections, exiting...")
        return

    # Initialize UID tracking on first run (if no last processed UID exists)
    if uid_tracker.get_last_processed_uid() is None:
        log.info("First run detected - initializing UID tracking with latest inbox UID...")
        try:
            latest_uid = initialize_uid_tracking(client)
            if latest_uid:
                log.info("UID tracking initialized with UID: %s", latest_uid)
            else:
                log.warning("Failed to initialize UID tracking, will process all new emails")
        except Exception as e:
            log.warning("UID initialization failed: %s, will process all new emails", e)
    else:
        log.info("UID tracking active - last processed: %s", uid_tracker.get_last_processed_uid())

    consecutive_errors = 0
    
    try:
        while True:
            try:
                log.debug("Checking for new emails...")
                
                # Check if token was refreshed by another component
                if token_manager.has_token_changed():
                    log.info("Token refreshed by another component, reconnecting...")
                    client = None
                    if not safe_connect():
                        log.error("Failed to reconnect after token change. Retrying cycle.")
                        time.sleep(RECONNECT_DELAY)
                        continue
                
                # Enhanced IDLE handling with better error recovery
                try:
                    client.idle()
                    responses = client.idle_check(timeout=IDLE_TIMEOUT)
                    client.idle_done()
                except (exceptions.IMAPClientError, OSError, ConnectionError) as idle_error:
                    log.warning(f"IDLE operation failed: {idle_error}")
                    # Try to gracefully exit IDLE if possible
                    try:
                        client.idle_done()
                    except:
                        pass  
                    
                    # Force reconnection for IDLE failures
                    log.info("Forcing reconnection due to IDLE failure...")
                    client = None  # Mark client as invalid
                    if not safe_connect():
                        log.error("Failed to reconnect after IDLE failure. Retrying cycle.")
                        time.sleep(RECONNECT_DELAY)
                        continue
                    responses = []  # No responses from failed IDLE

                if responses:
                    log.info("New email detected by IDLE")
                    # IDLE only triggers for NEW emails - trust IDLE and process recent UIDs
                    try:
                        # Get all current UIDs
                        all_uids = client.search(['ALL'])
                        if all_uids:
                            # Get last processed UID for comparison
                            last_processed_uid = uid_tracker.get_last_processed_uid()
                            
                            if last_processed_uid:
                                # Process all UIDs greater than last processed
                                new_uids = [uid for uid in all_uids if int(uid) > int(last_processed_uid)]
                                if new_uids:
                                    # Sort UIDs to ensure sequential processing
                                    new_uids = sorted(new_uids)
                                    log.info("Processing %d new emails: UIDs %s", len(new_uids), new_uids)
                                    
                                    # Process each email sequentially and update UID tracker after each success
                                    for uid in new_uids:
                                        try:
                                            log.debug("Processing UID: %s", uid)
                                            success = process_uid(client, mongo_col, uid, folder=MAILBOX)
                                            if success:
                                                # Update UID tracker immediately after successful processing
                                                uid_tracker.update_last_processed(uid)
                                                log.info("Processed UID %s (thread %s)", uid, uid_tracker.get_last_processed_uid())
                                            else:
                                                log.warning("Failed to process UID %s, continuing with next", uid)
                                        except Exception as e:
                                            log.exception("Error processing UID %s: %s", uid, e)
                                            # Continue processing other emails even if one fails
                                else:
                                    log.debug("No new emails found")
                            else:
                                # No last UID - just process the latest email (IDLE triggered)
                                latest_uid = all_uids[-1]
                                log.info("Processing latest email UID: %s", latest_uid)
                                success = process_uid(client, mongo_col, latest_uid, folder=MAILBOX)
                                if success:
                                    uid_tracker.update_last_processed(latest_uid)
                        else:
                            log.warning("IDLE triggered but no emails found")
                    except Exception as e:
                        log.error("Failed to process new email from IDLE: %s", e)
                else:
                    # No IDLE responses - just send keep-alive, don't search for emails
                    log.debug("IDLE timeout - sending NOOP keep-alive")
                    try:
                        client.noop()
                    except (exceptions.IMAPClientError, OSError) as e:
                        log.warning(f"NOOP keep-alive failed: {e}. Reconnecting...")
                        client = None  # Mark client as invalid
                        if not safe_connect():
                            log.error("Failed to reconnect after NOOP failure. Retrying cycle.")
                            time.sleep(RECONNECT_DELAY)
                            continue
                
                consecutive_errors = 0

            except (exceptions.IMAPClientError, OSError) as e:
                log.exception("IMAP error during IDLE cycle, reconnecting: %s", e)
                if not safe_connect():
                    log.error("Failed to reconnect, will retry after delay.")
                    time.sleep(RECONNECT_DELAY)
            except Exception as ex:
                log.exception("Unexpected exception in main loop: %s", ex)
                consecutive_errors += 1
                if consecutive_errors > 5:
                    log.error("Multiple consecutive errors, attempting full reconnect.")
                    if safe_connect():
                        consecutive_errors = 0
                    else:
                        log.error("Full reconnect failed, will retry after delay.")
                        time.sleep(RECONNECT_DELAY * consecutive_errors)
                # No fallback needed - IDLE will catch up when reconnection succeeds

    except KeyboardInterrupt:
        log.info("Stopping email monitor...")
    except Exception as e:
        log.critical("Critical error in email monitor: %s", e)
    finally:
        try:
            if client:
                client.logout()
        except Exception:
            pass
        log.info("Email monitor stopped")