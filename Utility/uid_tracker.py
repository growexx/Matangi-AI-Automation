"""
UID Tracker - Multi-tenant wrapper for MongoDB-based UID tracking.

This module provides a simple interface that delegates all UID tracking
to the UserManager, which stores per-user UIDs in MongoDB's UserDetails collection.

No file-based tracking is used. All data is stored in MongoDB.
"""
import sys
import os
from typing import Optional
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class UIDTracker:
    """
    Multi-tenant UID tracker that delegates to UserManager.
    All UID tracking is stored in MongoDB UserDetails collection.
    """
    
    def __init__(self):
        """Initialize UID tracker - no file paths needed, uses MongoDB only."""
        pass
    
    def get_last_processed_uid(self, username: str) -> Optional[int]:
        """
        Get the last processed UID for a user from MongoDB.
        
        Args:
            username: User's email address (required)
        
        Returns:
            The last processed UID as an integer, or None if not found.
        """
        if not username:
            log.error("Username is required for multi-tenant UID tracking")
            return None
            
        try:
            from Utility.user_manager import user_manager
            return user_manager.get_last_processed_uid(username)
        except Exception as e:
            log.error(f"Failed to get last processed UID for {username}: {e}")
            return None
    
    def set_last_processed_uid(self, uid: int, username: str) -> bool:
        """
        Set the last processed UID for a user in MongoDB.
        
        Args:
            uid: The UID to store as the last processed UID
            username: User's email address (required)
            
        Returns:
            bool: True if successful, False otherwise.
        """
        if not username:
            log.error("Username is required for multi-tenant UID tracking")
            return False
            
        try:
            from Utility.user_manager import user_manager
            return user_manager.update_last_processed_uid(username, uid)
        except Exception as e:
            log.error(f"Failed to set last processed UID for {username}: {e}")
            return False
    
    def is_new_email(self, uid: int, username: str) -> bool:
        """
        Check if the given UID represents a new email (higher than last processed).
        
        Args:
            uid: UID to check
            username: User's email address (required)
            
        Returns:
            bool: True if this is a new email, False otherwise.
        """
        if not username:
            log.error("Username is required for multi-tenant UID tracking")
            return True  # Default to processing if username missing
            
        last_processed = self.get_last_processed_uid(username)
        
        if last_processed is None:
            # No tracking data exists, consider this new
            return True
        
        return int(uid) > last_processed
    
    def update_last_processed(self, uid: int, username: str) -> bool:
        """
        Update the last processed UID if this UID is newer.
        
        Args:
            uid: UID that was just processed
            username: User's email address (required)
            
        Returns:
            bool: True if the UID was updated, False otherwise.
        """
        if self.is_new_email(uid, username):
            return self.set_last_processed_uid(uid, username)
        return False


# Global instance for convenience
uid_tracker = UIDTracker()


# Convenience functions for backward compatibility
def get_last_processed_uid(username: str) -> Optional[int]:
    """Get the last processed UID for a user."""
    return uid_tracker.get_last_processed_uid(username)


def update_last_processed_uid(uid: int, username: str) -> bool:
    """Update the last processed UID for a user."""
    return uid_tracker.update_last_processed(uid, username)


def is_new_email(uid: int, username: str) -> bool:
    """Check if the given UID is a new email for a user."""
    return uid_tracker.is_new_email(uid, username)


def initialize_uid_tracking(client, username: str) -> Optional[int]:
    """
    Initialize UID tracking for a user with the latest UID from inbox.
    
    Args:
        client: IMAP client connection with an active session
        username: User's email address (required)
        
    Returns:
        The latest UID that was set, or None if failed.
    """
    if not username:
        log.error("Username is required for UID tracking initialization")
        return None
        
    try:
        from Utility.user_manager import user_manager
        
        # Get all UIDs from inbox
        all_uids = client.search(['ALL'])
        
        if all_uids:
            # Get the latest (highest) UID
            latest_uid = max(all_uids)
            
            # Set this as the last processed UID in MongoDB
            if user_manager.update_last_processed_uid(username, latest_uid):
                log.debug(f"Initialized UID tracking for {username} with UID: {latest_uid}")
                return latest_uid
            else:
                log.error(f"Failed to set initial UID for {username}")
                return None
        else:
            log.warning(f"No emails found in inbox for {username}")
            return None
            
    except Exception as e:
        log.error(f"Failed to initialize UID tracker for {username}: {e}")
        return None
