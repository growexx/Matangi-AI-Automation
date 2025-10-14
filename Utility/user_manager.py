import os
import sys
import time
import threading
from typing import Dict, List, Optional
from pymongo import MongoClient
from datetime import datetime
from config import MONGO_URI, MONGO_DB, MONGO_USER_COL
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class UserManager:
    """Manages user details and authentication tokens in MongoDB for multi-tenant system."""
    
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.users_collection = self.db[MONGO_USER_COL]
        self.lock = threading.Lock()
        
        # Create indexes for efficient querying
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create necessary indexes for the UserDetails collection."""
        try:
            # Index on username for fast lookups
            self.users_collection.create_index("username", unique=True)
            # Index on sequential_id for fast lookups
            self.users_collection.create_index("sequential_id", unique=True)
            # Index on active status
            self.users_collection.create_index("is_active")
            log.debug("UserDetails collection indexes created")
        except Exception as e:
            log.warning(f"Error creating indexes: {e}")
    
    def _get_next_sequential_id(self) -> int:
        """Get the next sequential ID for a new user."""
        try:
            # Find the highest sequential_id and increment it
            highest = self.users_collection.find_one(
                {}, 
                sort=[("sequential_id", -1)]
            )
            if highest and "sequential_id" in highest:
                return highest["sequential_id"] + 1
            else:
                return 1  # First user gets ID 1
        except Exception as e:
            log.error(f"Error getting next sequential ID: {e}")
            return 1
    
    def add_user(self, username: str, oauth_tokens: Dict, full_name: str = "", client_id: str = None, client_secret: str = None) -> int:
        """
        Add a new user to the system with their OAuth tokens.
        
        Args:
            username: User's email address
            oauth_tokens: OAuth token dictionary
            full_name: User's full name
            
        Returns:
            sequential_id: Sequential integer ID for the user (1, 2, 3, ...)
        """
        with self.lock:
            try:
                # Check if user already exists
                existing_user = self.users_collection.find_one({"username": username})
                if existing_user:
                    # Update tokens and reactivate user
                    update_data = {
                        "oauth_tokens": oauth_tokens,
                        "is_active": True
                    }
                    if client_id:
                        update_data["client_id"] = client_id
                    if client_secret:
                        update_data["client_secret"] = client_secret
                        
                    self.users_collection.update_one(
                        {"username": username},
                        {"$set": update_data}
                    )
                    log.info(f"Existing user updated: {username} (ID: {existing_user['sequential_id']})")
                    return existing_user['sequential_id']
                
                # Get next sequential ID
                sequential_id = self._get_next_sequential_id()
                
                user_doc = {
                    "sequential_id": sequential_id,
                    "username": username,
                    "full_name": full_name,
                    "oauth_tokens": oauth_tokens,
                    "last_processed_uid": None,
                    "uid_updated_at": None,
                    "is_active": True,
                    "monitoring_active": False,
                    "created_at": datetime.now()
                }
                
                # Add user-specific OAuth credentials if provided
                if client_id:
                    user_doc["client_id"] = client_id
                if client_secret:
                    user_doc["client_secret"] = client_secret
                
                # Insert new user
                result = self.users_collection.insert_one(user_doc)
                if result.inserted_id:
                    log.info(f"New user added: {username} (Sequential ID: {sequential_id})")
                    return sequential_id
                else:
                    raise Exception("Failed to insert user document")
                
            except Exception as e:
                log.error(f"Failed to add user {username}: {e}")
                raise
    
    def get_user(self, username: str) -> Optional[Dict]:
        """Get user details by username."""
        try:
            return self.users_collection.find_one({"username": username})
        except Exception as e:
            log.error(f"Failed to get user {username}: {e}")
            return None
    
    def get_user_by_id(self, sequential_id: int) -> Optional[Dict]:
        """Get user details by sequential ID."""
        try:
            return self.users_collection.find_one({"sequential_id": sequential_id})
        except Exception as e:
            log.error(f"Failed to get user by ID {sequential_id}: {e}")
            return None
    
    def get_all_active_users(self) -> List[Dict]:
        """Get all active users."""
        try:
            return list(self.users_collection.find({"is_active": True}))
        except Exception as e:
            log.error(f"Failed to get active users: {e}")
            return []
    
    def update_oauth_tokens(self, username: str, oauth_tokens: Dict) -> bool:
        """Update OAuth tokens for a user."""
        with self.lock:
            try:
                result = self.users_collection.update_one(
                    {"username": username},
                    {
                        "$set": {
                            "oauth_tokens": oauth_tokens
                        }
                    }
                )
                return result.modified_count > 0
            except Exception as e:
                log.error(f"Failed to update tokens for {username}: {e}")
                return False
    
    def get_oauth_tokens(self, username: str) -> Optional[Dict]:
        """Get OAuth tokens for a user."""
        try:
            user = self.users_collection.find_one({"username": username})
            return user.get("oauth_tokens") if user else None
        except Exception as e:
            log.error(f"Failed to get tokens for {username}: {e}")
            return None
    
    def update_last_processed_uid(self, username: str, uid: int) -> bool:
        """Update the last processed UID for a user."""
        with self.lock:
            try:
                result = self.users_collection.update_one(
                    {"username": username},
                    {
                        "$set": {
                            "last_processed_uid": int(uid),
                            "uid_updated_at": datetime.now()
                        }
                    }
                )
                return result.modified_count > 0
            except Exception as e:
                log.error(f"Failed to update UID for {username}: {e}")
                return False
    
    def get_last_processed_uid(self, username: str) -> Optional[int]:
        """Get the last processed UID for a user."""
        try:
            user = self.users_collection.find_one({"username": username})
            return user.get("last_processed_uid") if user else None
        except Exception as e:
            log.error(f"Failed to get last UID for {username}: {e}")
            return None
    
    def set_monitoring_status(self, username: str, status: bool) -> bool:
        """Set monitoring status for a user."""
        with self.lock:
            try:
                result = self.users_collection.update_one(
                    {"username": username},
                    {
                        "$set": {
                            "monitoring_active": status
                        }
                    }
                )
                return result.modified_count > 0
            except Exception as e:
                log.error(f"Failed to set monitoring status for {username}: {e}")
                return False
    
    def is_user_monitoring_active(self, username: str) -> bool:
        """Check if monitoring is active for a user."""
        try:
            user = self.users_collection.find_one({"username": username})
            return user.get("monitoring_active", False) if user else False
        except Exception as e:
            log.error(f"Failed to check monitoring status for {username}: {e}")
            return False
    
    def deactivate_user(self, username: str) -> bool:
        """Deactivate a user (stop monitoring but keep data)."""
        with self.lock:
            try:
                result = self.users_collection.update_one(
                    {"username": username},
                    {
                        "$set": {
                            "is_active": False,
                            "monitoring_active": False
                        }
                    }
                )
                return result.modified_count > 0
            except Exception as e:
                log.error(f"Failed to deactivate user {username}: {e}")
                return False
    
    
    def get_user_stats(self) -> Dict:
        """Get statistics about users in the system."""
        try:
            total_users = self.users_collection.count_documents({})
            active_users = self.users_collection.count_documents({"is_active": True})
            monitoring_users = self.users_collection.count_documents({"monitoring_active": True})
            
            return {
                "total_users": total_users,
                "active_users": active_users,
                "monitoring_users": monitoring_users
            }
        except Exception as e:
            log.error(f"Failed to get user stats: {e}")
            return {"total_users": 0, "active_users": 0, "monitoring_users": 0}
    
    def get_user_id_by_username(self, username: str) -> Optional[int]:
        """Get sequential ID by username."""
        try:
            user = self.users_collection.find_one({"username": username}, {"sequential_id": 1})
            return user["sequential_id"] if user else None
        except Exception as e:
            log.error(f"Failed to get user ID for {username}: {e}")
            return None

user_manager = UserManager()