
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import sys
from logger.logger_setup import logger


class ThreadJSONStorage:
    # Handles storing thread data in JSON format
    
    def __init__(self, json_file_path: str = None):
        # Initialize JSON storage, defaults to mails.json at project root
        if json_file_path is None:
            # Default to mails.json at project root
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.json_file_path = os.path.join(project_root, "mails.json")
        else:
            self.json_file_path = json_file_path
        
        logger.debug("JSON storage: %s", self.json_file_path)
    
    def load_existing_data(self) -> List[Dict[str, Any]]:
        # Load existing data from JSON file
        try:
            if os.path.exists(self.json_file_path):
                with open(self.json_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        logger.debug("Loaded existing data with %d threads", len(data))
                        return data
                    elif isinstance(data, dict) and 'threads' in data:
                        # Handle old format
                        logger.debug("Converting old format to new format")
                        return data.get('threads', [])
                    else:
                        return []
            else:
                logger.info("JSON file doesn't exist, will create new one")
                return []
        except Exception as e:
            logger.error("Failed to load existing JSON data: %s", e)
            return []
    
    def _create_empty_structure(self) -> Dict[str, Any]:
        """Create empty JSON structure"""
        return {
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "total_threads": 0,
                "description": "Fetched email threads from Matangi Email Automation"
            },
            "threads": []
        }
    
    def save_thread_data(self, thread_data: Dict[str, Any]) -> bool:
        # Save or update thread data in JSON file
        try:
            # Create simplified thread data (remove extra fields)
            simplified_data = {
                "thread_id": thread_data.get("thread_id"),
                "subject": thread_data.get("subject"),
                "Mails": thread_data.get("Mails", [])
            }
            
            if append and os.path.exists(self.json_file_path):
                # Load existing data
                try:
                    with open(self.json_file_path, 'r', encoding='utf-8') as f:
                        existing_threads = json.load(f)
                    if not isinstance(existing_threads, list):
                        existing_threads = []
                except:
                    existing_threads = []
            else:
                existing_threads = []
            
            # Check if thread already exists (update if it does)
            thread_id = thread_data.get("thread_id")
            existing_thread_index = None
            
            for i, existing_thread in enumerate(existing_threads):
                if existing_thread.get("thread_id") == thread_id:
                    existing_thread_index = i
                    break
            
            if existing_thread_index is not None:
                # Update existing thread
                existing_threads[existing_thread_index] = simplified_data
                logger.debug("Updated thread %s", thread_id)
            else:
                # Add new thread
                existing_threads.append(simplified_data)
                logger.debug("Added thread %s", thread_id)
            
            # Save to file as simple array
            with open(self.json_file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_threads, f, indent=2, ensure_ascii=False)
            
            logger.debug("Saved to mails.json")
            return True
            
        except Exception as e:
            logger.error("Failed to save thread data to JSON: %s", e)
            return False
    
    def get_thread_by_id(self, thread_id: str) -> Optional[Dict[str, Any]]:
        """Get specific thread data by ID"""
        try:
            threads = self.load_existing_data()
            for thread in threads:
                if thread.get("thread_id") == thread_id:
                    return thread
            return None
        except Exception as e:
            logger.error("Failed to get thread %s: %s", thread_id, e)
            return None
    
    def get_all_threads(self) -> List[Dict[str, Any]]:
        """Get all stored threads"""
        try:
            return self.load_existing_data()
        except Exception as e:
            logger.error("Failed to get all threads: %s", e)
            return []
    
    def get_recent_threads(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most recently fetched threads"""
        try:
            threads = self.get_all_threads()
            # Return last N threads (most recently added)
            return threads[-limit:] if len(threads) > limit else threads
        except Exception as e:
            logger.error("Failed to get recent threads: %s", e)
            return []
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get statistics about stored data"""
        try:
            threads = self.load_existing_data()
            
            total_emails = sum(len(thread.get("Mails", [])) for thread in threads)
            
            stats = {
                "total_threads": len(threads),
                "total_emails": total_emails,
                "file_size_bytes": os.path.getsize(self.json_file_path) if os.path.exists(self.json_file_path) else 0
            }
            
            return stats
        except Exception as e:
            logger.error("Failed to get storage stats: %s", e)
            return {}

