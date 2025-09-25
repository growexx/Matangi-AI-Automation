import os
import json
import time
from typing import Optional, Dict, Any
from config import BASE_DIR
from logger.logger_setup import logger as log
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



class UIDTracker:
    """Handles tracking of last processed UID for email monitoring"""
    
    def __init__(self, tracker_file_path: str = None):
        """Initialize UID tracker.
        
        Args:
            tracker_file_path: Path to UID tracking file. 
                             If None, uses uid_tracker.json in config directory.
        """
        if tracker_file_path is None:
            # Default to uid_tracker.json in config directory
            self.tracker_file_path = os.path.join(BASE_DIR, "config", "uid_tracker.json")
        else:
            self.tracker_file_path = tracker_file_path
        
        # Don't log initialization at debug level
    
    def get_last_processed_uid(self) -> Optional[int]:
        """Get the last processed UID from storage.
        
        Returns:
            The last processed UID as an integer, or None if not found.
        """
        try:
            if os.path.exists(self.tracker_file_path):
                with open(self.tracker_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    last_uid = data.get('last_processed_uid')
                    if last_uid is not None:

                        return int(last_uid)
            else:
                log.debug("UID tracker file doesn't exist, starting fresh")
            return None
        except Exception as e:
            log.error("Failed to load last processed UID: %s", e)
            return None
    
    def set_last_processed_uid(self, uid: int) -> bool:
        """Set the last processed UID in storage.
        
        Args:
            uid: The UID to store as the last processed UID.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Ensure the directory exists
            tracker_dir = os.path.dirname(self.tracker_file_path)
            if not os.path.exists(tracker_dir):
                os.makedirs(tracker_dir, mode=0o700)
            
            data = {
                'last_processed_uid': int(uid),
                'updated_at': time.time()
            }
            
            # Write to a temporary file first
            temp_file = f"{self.tracker_file_path}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            
            # Atomic rename to ensure file integrity
            os.replace(temp_file, self.tracker_file_path)
            
            # Set secure permissions
            os.chmod(self.tracker_file_path, 0o600)

            return True
            
        except Exception as e:
            log.error("Failed to save last processed UID: %s", e)
            # Clean up temp file if it exists
            if 'temp_file' in locals() and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            return False
    
    def initialize_with_latest_uid(self, client) -> Optional[int]:
        """Initialize the UID tracker with the latest UID from inbox.
        
        This should be called on first run after deployment.
        
        Args:
            client: IMAP client connection with an active session.
            
        Returns:
            The latest UID that was set, or None if failed.
        """
        try:
            # Get all UIDs from inbox (sorted by default)
            all_uids = client.search(['ALL'])
            
            if all_uids:
                # Get the latest (highest) UID
                latest_uid = max(all_uids)
                
                # Set this as the last processed UID
                if self.set_last_processed_uid(latest_uid):
                    log.info("Initialized UID tracker with latest UID: %s", latest_uid)
                    return latest_uid
                else:
                    log.error("Failed to set initial UID")
                    return None
            else:
                log.warning("No emails found in inbox for initialization")
                return None
                
        except Exception as e:
            log.error("Failed to initialize UID tracker: %s", e)
            return None
    
    def is_new_email(self, uid: int) -> bool:
        """Check if the given UID represents a new email (higher than last processed).
        
        Args:
            uid: UID to check.
            
        Returns:
            bool: True if this is a new email, False otherwise.
        """
        last_processed = self.get_last_processed_uid()
        
        if last_processed is None:
            # No tracking data exists, consider this new
            return True
        
        return int(uid) > last_processed
    
    def update_last_processed(self, uid: int) -> bool:
        """Update the last processed UID if this UID is newer.
        
        Args:
            uid: UID that was just processed.
            
        Returns:
            bool: True if the UID was updated, False otherwise.
        """
        if self.is_new_email(uid):
            return self.set_last_processed_uid(uid)
        return False


# Create a default instance for convenience
uid_tracker = UIDTracker()


def get_last_processed_uid() -> Optional[int]:
    """Get the last processed UID using the default UID tracker."""
    return uid_tracker.get_last_processed_uid()


def update_last_processed_uid(uid: int) -> bool:
    """Update the last processed UID using the default UID tracker."""
    return uid_tracker.update_last_processed(uid)


def is_new_email(uid: int) -> bool:
    """Check if the given UID is a new email using the default UID tracker."""
    return uid_tracker.is_new_email(uid)


def initialize_uid_tracking(client) -> Optional[int]:
    """Initialize the UID tracker with the latest UID from the email client.
    
    This is a convenience function that uses the default UID tracker instance.
    
    Args:
        client: IMAP client connection with an active session.
        
    Returns:
        The latest UID that was set, or None if failed.
    """
    return uid_tracker.initialize_with_latest_uid(client)


if __name__ == "__main__":
    # Example usage
    tracker = UIDTracker()
    print(f"Last processed UID: {tracker.get_last_processed_uid()}")
    
    # Example of setting a UID
    if tracker.update_last_processed(100):
        print("Successfully updated last processed UID")
    else:
        print("Failed to update last processed UID")
