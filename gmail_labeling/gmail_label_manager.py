#!/usr/bin/env python3
"""
Gmail Label Manager - Enhanced with Fast Search and Smart Label Management
"""

import os
import sys
from typing import Dict, List, Any, Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pymongo
from pymongo import IndexModel, TEXT
from mail_parser.auth_handler import get_gmail_service
from gmail_labeling.label_config import get_label_color, get_intent_label, get_sentiment_label
from Utility.mongo_utils import mongo_connect
from logger.logger_setup import logger as log, get_user_logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Gmail system labels that should NEVER be removed by our automation
GMAIL_SYSTEM_LABELS = {
    'INBOX', 'SENT', 'DRAFT', 'SPAM', 'TRASH', 'IMPORTANT', 'STARRED',
    'UNREAD', 'CHAT', 'CATEGORY_PERSONAL', 'CATEGORY_SOCIAL', 
    'CATEGORY_PROMOTIONS', 'CATEGORY_UPDATES', 'CATEGORY_FORUMS',
    '[Gmail]/All Mail', '[Gmail]/Drafts', '[Gmail]/Important', 
    '[Gmail]/Sent Mail', '[Gmail]/Spam', '[Gmail]/Starred', '[Gmail]/Trash'
}

# Our custom label categories (these can be removed/updated)
CUSTOM_LABEL_CATEGORIES = {
    # Intent labels
    'Inquiry', 'Status', 'Complaint', 'Pricing-Negotiation', 'Proposal',
    'Logistics', 'Acknowledgement', 'Status-of-Inquiry', 'Unclassified',
    # Sentiment labels  
    'Higher-Positive', 'Positive', 'Neutral', 'Negative', 'Higher-Negative'
}

class GmailLabelManager:
    """Enhanced Gmail Label Manager with fast search and smart label management."""
    
    def __init__(self, username: str):
        """Initialize the Gmail Label Manager."""
        self.username = username
        self.service = None
        self.mongo_col = None
        self.user_log = get_user_logger(username)  # Use user-specific logger
        self._initialize_services()
        self._ensure_mongodb_indexes()
    
    def _initialize_services(self):
        """Initialize Gmail and MongoDB services."""
        try:
            self.service = get_gmail_service(self.username)
            self.mongo_col = mongo_connect()

        except Exception as e:
            self.user_log.error(f"Failed to initialize Gmail Label Manager: {e}")
            raise
    
    def _ensure_mongodb_indexes(self):
        """Ensure MongoDB has proper indexes for fast message searching."""
        try:
            if self.mongo_col is None:
                return
                
            # Create indexes for fast searching
            indexes_to_create = [
                IndexModel([("messages.message_id", pymongo.ASCENDING)], 
                          name="message_id_index", background=True),
                IndexModel([("thread_id", pymongo.ASCENDING)], 
                          name="thread_id_index", background=True),
                IndexModel([("messages.uid", pymongo.ASCENDING)], 
                          name="uid_index", background=True),
                IndexModel([("messages.message_id", TEXT)], 
                          name="message_id_text_index", background=True)
            ]
            
            # Check existing indexes
            existing_indexes = self.mongo_col.list_indexes()
            existing_index_names = [idx['name'] for idx in existing_indexes]
            
            # Create missing indexes
            for index in indexes_to_create:
                if index.document['name'] not in existing_index_names:
                    self.mongo_col.create_indexes([index])
                    log.info(f"Created MongoDB index: {index.document['name']}")
            

            
        except Exception as e:
            log.warning(f"Failed to create MongoDB indexes: {e}")
    
    def search_message_by_id(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Fast search for message by message_id using MongoDB index.
        
        Args:
            message_id: The Message-ID header value to search for
            
        Returns:
            Dictionary containing thread info and message details, or None if not found
        """
        try:
            if self.mongo_col is None:
                return None
                
            # Find message by message_id in the messages array
            query = {"messages.message_id": message_id}
            projection = {"thread_id": 1, "subject": 1, "messages.$": 1}
            
            doc = self.mongo_col.find_one(query, projection)
            
            if doc and "messages" in doc and doc["messages"]:
                message = doc["messages"][0]  # Get the matching message
                log.info(f"Found message {message_id} in thread {doc.get('thread_id')}")
                
                # Extract sender information from the message
                sender = message.get('from', '')
                if not sender:
                    sender = message.get('sender', '')
                
                return {
                    "thread_id": doc.get("thread_id"),
                    "subject": doc.get("subject"),
                    "sender": sender,
                    "message": message
                }
            else:
                log.debug(f"Message {message_id} not found in MongoDB")
                return None
                
        except Exception as e:
            log.error(f"Error searching for message {message_id}: {e}")
            return None
    
    def search_gmail_message_by_id(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Search for a message in Gmail using the Message-ID header.

        Args:
            message_id: The Message-ID header value to search for
            
        Returns:
            Dictionary containing Gmail message and thread info, or None if not found
        """
        try:
            if not self.service or not message_id:
                return None
            
            # Clean up message_id - remove < > if present
            clean_message_id = message_id.strip('<>')
            
            # Search Gmail using the message-id
            search_query = f'rfc822msgid:{clean_message_id}'
            log.debug(f"Searching Gmail with query: {search_query}")
            
            results = self.service.users().messages().list(
                userId='me',
                q=search_query,
                maxResults=1
            ).execute()
            
            messages = results.get('messages', [])
            
            if not messages:
                log.debug(f"Message {message_id} not found in Gmail")
                return None
            
            gmail_message_id = messages[0]['id']
            gmail_thread_id = messages[0]['threadId']
            
            # Get the full message details
            message_detail = self.service.users().messages().get(
                userId='me',
                id=gmail_message_id,
                format='full'
            ).execute()
            
            # Get thread details
            thread_detail = self.service.users().threads().get(
                userId='me',
                id=gmail_thread_id
            ).execute()
            
            log.info(f"Found Gmail message {gmail_message_id} in thread {gmail_thread_id}")
            
            return {
                "gmail_message_id": gmail_message_id,
                "gmail_thread_id": gmail_thread_id,
                "message_detail": message_detail,
                "thread_detail": thread_detail,
                "subject": self._extract_header(message_detail, 'Subject'),
                "from": self._extract_header(message_detail, 'From'),
                "to": self._extract_header(message_detail, 'To'),
                "date": self._extract_header(message_detail, 'Date')
            }
            
        except HttpError as e:
            log.error(f"Gmail API error searching for message {message_id}: {e}")
            return None
        except Exception as e:
            log.error(f"Error searching Gmail for message {message_id}: {e}")
            return None
    
    
    def _extract_header(self, message_detail: Dict, header_name: str) -> str:
        """Extract header value from Gmail message."""
        try:
            headers = message_detail.get('payload', {}).get('headers', [])
            for header in headers:
                if header.get('name', '').lower() == header_name.lower():
                    return header.get('value', '')
            return ''
        except Exception:
            return ''
    
    def get_existing_thread_labels(self, thread_id: str) -> Dict[str, List[str]]:
        """
        Get existing Gmail labels for all messages in a thread.
        
        Args:
            thread_id: The Gmail thread ID
            
        Returns:
            Dictionary with 'system_labels' and 'custom_labels' lists
        """
        try:
            if not self.service:
                return {"system_labels": [], "custom_labels": []}
            
            # Get thread details from Gmail
            thread = self.service.users().threads().get(
                userId='me', 
                id=thread_id
            ).execute()
            
            all_labels = set()
            
            # Collect labels from all messages in the thread
            for message in thread.get('messages', []):
                message_labels = message.get('labelIds', [])
                all_labels.update(message_labels)
            
            # Get label details to convert IDs to names
            labels_result = self.service.users().labels().list(userId='me').execute()
            label_lookup = {label['id']: label['name'] for label in labels_result.get('labels', [])}
            
            # Separate system and custom labels
            system_labels = []
            custom_labels = []
            
            for label_id in all_labels:
                label_name = label_lookup.get(label_id, label_id)
                
                if (label_id in GMAIL_SYSTEM_LABELS or 
                    label_name in GMAIL_SYSTEM_LABELS or
                    label_id.startswith('CATEGORY_')):
                    system_labels.append(label_name)
                elif label_name in CUSTOM_LABEL_CATEGORIES:
                    custom_labels.append(label_name)
            
            log.debug(f"Thread {thread_id} has {len(system_labels)} system labels, {len(custom_labels)} custom labels")
            
            return {
                "system_labels": system_labels,
                "custom_labels": custom_labels
            }
            
        except HttpError as e:
            log.error(f"Gmail API error getting thread labels for {thread_id}: {e}")
            return {"system_labels": [], "custom_labels": []}
        except Exception as e:
            log.error(f"Error getting thread labels for {thread_id}: {e}")
            return {"system_labels": [], "custom_labels": []}
    
    def remove_custom_labels_from_thread(self, thread_id: str, labels_to_remove: List[str]) -> bool:
        """
        Remove specific custom labels from all messages in a thread.
        NEVER removes system labels like Inbox, Sent, etc.
        
        Args:
            thread_id: The Gmail thread ID
            labels_to_remove: List of custom label names to remove
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.service or not labels_to_remove:
                return False
            
            # Get all labels to find IDs
            labels_result = self.service.users().labels().list(userId='me').execute()
            label_name_to_id = {label['name']: label['id'] for label in labels_result.get('labels', [])}
            
            # Filter out system labels (safety check)
            safe_labels_to_remove = []
            for label_name in labels_to_remove:
                if (label_name not in GMAIL_SYSTEM_LABELS and 
                    label_name in CUSTOM_LABEL_CATEGORIES):
                    if label_name in label_name_to_id:
                        safe_labels_to_remove.append(label_name_to_id[label_name])
                else:
                    log.warning(f"Skipping removal of system label: {label_name}")
            
            if not safe_labels_to_remove:
                log.info(f"No safe custom labels to remove from thread {thread_id}")
                return True
            
            # Remove labels from the entire thread
            self.service.users().threads().modify(
                userId='me',
                id=thread_id,
                body={
                    'removeLabelIds': safe_labels_to_remove
                }
            ).execute()
            
            # Custom labels removed from thread
            return True
            
        except HttpError as e:
            log.error(f"Gmail API error removing labels from thread {thread_id}: {e}")
            return False
        except Exception as e:
            log.error(f"Error removing labels from thread {thread_id}: {e}")
            return False
    
    def create_label_if_not_exists(self, label_name: str) -> Optional[str]:
        """
        Create a Gmail label if it doesn't exist.
        
        Args:
            label_name: Name of the label to create
            
        Returns:
            Label ID if successful, None otherwise
        """
        try:
            if not self.service:
                return None
            
            # Check if label already exists
            labels_result = self.service.users().labels().list(userId='me').execute()
            existing_labels = {label['name']: label['id'] for label in labels_result.get('labels', [])}
            
            if label_name in existing_labels:
                return existing_labels[label_name]
            
            # Create new label with color
            color_config = get_label_color(label_name)
            
            label_body = {
                'name': label_name,
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show',
                'color': {
                    'backgroundColor': color_config.get('color', '#999999'),
                    'textColor': color_config.get('textColor', '#ffffff')
                }
            }
            
            try:
                created_label = self.service.users().labels().create(
                    userId='me',
                    body=label_body
                ).execute()
                log.info(f"Created Gmail label: {label_name}")
                return created_label['id']
            except HttpError as e:
                # If color is rejected by Gmail palette, retry without color
                if hasattr(e, 'status_code') and e.status_code == 400 or 'invalidArgument' in str(e):
                    log.warning(f"Color rejected for label {label_name}, retrying without color: {e}")
                    label_body_fallback = {
                        'name': label_name,
                        'labelListVisibility': 'labelShow',
                        'messageListVisibility': 'show'
                    }
                    created_label = self.service.users().labels().create(
                        userId='me',
                        body=label_body_fallback
                    ).execute()
                    log.info(f"Created Gmail label without color: {label_name}")
                    return created_label['id']
                raise
            
        except HttpError as e:
            log.error(f"Gmail API error creating label {label_name}: {e}")
            return None
        except Exception as e:
            log.error(f"Error creating label {label_name}: {e}")
            return None
    
    def apply_labels_to_thread(self, thread_id: str, label_names: List[str]) -> bool:
        """
        Apply labels to all messages in a thread.
        
        Args:
            thread_id: The Gmail thread ID
            label_names: List of label names to apply
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.service or not label_names:
                return False
            
            # Create labels if they don't exist and get their IDs
            label_ids = []
            for label_name in label_names:
                label_id = self.create_label_if_not_exists(label_name)
                if label_id:
                    label_ids.append(label_id)
            
            if not label_ids:
                log.warning(f"No valid labels to apply to thread {thread_id}")
                return False
            
            # Apply labels to the entire thread
            self.service.users().threads().modify(
                userId='me',
                id=thread_id,
                body={
                    'addLabelIds': label_ids
                }
            ).execute()
            
            log.info(f"Applied {len(label_ids)} labels to thread {thread_id}")
            return True
            
        except HttpError as e:
            log.error(f"Gmail API error applying labels to thread {thread_id}: {e}")
            return False
        except Exception as e:
            log.error(f"Error applying labels to thread {thread_id}: {e}")
            return False


def apply_ml_labels(thread_id: str, ml_results: Dict[str, Any], subject: str = "", username: str = None) -> bool:
    """
    Enhanced function to apply ML-generated labels to a Gmail thread.
    
    This function implements smart label management:
    1. Removes old custom labels (preserving system labels like Inbox)
    2. Applies new ML-generated labels
    3. Uses fast MongoDB indexing for message lookup
    
    Args:
        thread_id: The Gmail thread ID
        ml_results: Results from ML pipeline containing intent, sentiment, and labels
        subject: Email subject (for logging)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        if not username:
            log.error(f"Username required for Gmail labeling of thread {thread_id}")
            return False
            
        # Initialize label manager
        label_manager = GmailLabelManager(username)
        
        # Get new labels from ML results
        new_labels = ml_results.get('gmail_labels', [])
        if not new_labels:
            log.warning(f"No labels to apply for thread {thread_id}")
            return False
        
        log.info(f"Processing thread {thread_id} ({subject}): applying labels {new_labels}")
        
        # Step 1: Get existing labels in the thread
        existing_labels = label_manager.get_existing_thread_labels(thread_id)
        old_custom_labels = existing_labels.get('custom_labels', [])
        
        # Step 2: Remove old custom labels (but preserve system labels like Inbox)
        if old_custom_labels:
            log.info(f"Removing {len(old_custom_labels)} old custom labels from thread {thread_id}: {old_custom_labels}")
            remove_success = label_manager.remove_custom_labels_from_thread(thread_id, old_custom_labels)
            if not remove_success:
                log.warning(f"Failed to remove old labels from thread {thread_id}")
        else:
            log.debug(f"No old custom labels to remove from thread {thread_id}")
        
        # Step 3: Apply new labels
        apply_success = label_manager.apply_labels_to_thread(thread_id, new_labels)
        
        if apply_success:
            log.info(f"Successfully updated thread {thread_id}: removed {len(old_custom_labels)} old labels, applied {len(new_labels)} new labels")
            return True
        else:
            log.error(f"Failed to apply new labels to thread {thread_id}")
            return False
            
    except Exception as e:
        log.error(f"Error in apply_ml_labels for thread {thread_id}: {e}")
        return False


def get_gmail_thread_id_from_message_id(message_id: str, username: str) -> Optional[str]:
    """
    Convert Message-ID to Gmail Thread ID using Gmail API.
    
    Args:
        message_id: Email Message-ID header
        username: Username for Gmail API access
        
    Returns:
        Gmail thread ID if found, None otherwise
    """
    try:
        label_manager = GmailLabelManager(username)
        gmail_info = label_manager.search_gmail_message_by_id(message_id)
        
        if gmail_info:
            gmail_thread_id = gmail_info.get('gmail_thread_id')

            return gmail_thread_id
        else:
            log.debug(f"Message-ID {message_id} not found in Gmail (likely external email)")
            return None
            
    except Exception as e:
        log.warning(f"Failed to get Gmail thread ID for {message_id}: {e}")
        return None


def xgm_to_hex(xgm_thrid_raw: str) -> str:
    """
    Convert X-GM-THRID value (may include parentheses or other chars)
    to the hex string expected by Gmail REST API.
    """
    import re
    m = re.search(r'(\d+)', str(xgm_thrid_raw))
    if not m:
        raise ValueError(f"no decimal thread id found in: {xgm_thrid_raw!r}")
    dec = int(m.group(1))
    return format(dec, 'x')   # lowercase hex without '0x'


def get_gmail_thread_id_from_xgm_thrid(xgm_thrid: str, username: str) -> Optional[str]:
    """
    For external emails: Convert X-GM-THRID to Gmail Thread ID.
    X-GM-THRID from IMAP is decimal, but Gmail API expects hexadecimal.
    
    Args:
        xgm_thrid: X-GM-THRID value from IMAP (decimal string)
        username: Username for Gmail API access
        
    Returns:
        Gmail thread ID (hex format), None if not found
    """
    try:
        if not xgm_thrid or not username:
            return None
            
        # Convert decimal X-GM-THRID to hex format for Gmail API
        try:
            hex_thread_id = xgm_to_hex(xgm_thrid)
            log.info(f"Converted X-GM-THRID {xgm_thrid} to hex: {hex_thread_id}")
        except ValueError as e:
            log.warning(f"Failed to convert X-GM-THRID to hex: {e}")
            return None
            
        # Verify thread exists using hex format
        label_manager = GmailLabelManager(username)
        if not label_manager.service:
            return None
            
        try:
            thread = label_manager.service.users().threads().get(
                userId='me',
                id=hex_thread_id
            ).execute()
            
            if thread:
                log.info(f"Verified hex thread ID {hex_thread_id} exists in Gmail - can apply labels")
                return hex_thread_id  # Return hex format for Gmail API
            else:
                log.debug(f"Hex thread ID {hex_thread_id} not found in Gmail")
                return None
                
        except Exception as e:
            log.debug(f"Thread verification failed for {hex_thread_id}: {e}")
            return None
            
    except Exception as e:
        log.warning(f"Failed to process X-GM-THRID {xgm_thrid}: {e}")
        return None


def search_and_tag_message_enhanced(message_id: str, intent: str, sentiment: str, username: str = None, xgm_thrid: str = None) -> bool:
    """
    Enhanced function: Use MongoDB thread info to directly label Gmail thread.
    
    Simple approach:
    1. Get message from MongoDB 
    2. Extract Gmail thread ID from the message
    3. Apply labels directly to Gmail thread
    
    Args:
        message_id: The Message-ID header to search for
        intent: The detected intent
        sentiment: The detected sentiment
        username: Username for Gmail service
        xgm_thrid: Optional X-GM-THRID for external emails
        
    Returns:
        True if successful, False otherwise
    """
    try:
        if not username:
            log.error(f"Username required for Gmail labeling of message {message_id}")
            return False
            
        # Initialize label manager
        label_manager = GmailLabelManager(username)
        
        # Step 1: Get message from MongoDB
        mongo_info = label_manager.search_message_by_id(message_id)
        if not mongo_info:
            log.warning(f"Message {message_id} not found in MongoDB")
            return False
        
        # Step 2: Convert Message-ID to Gmail Thread ID
        gmail_thread_id = get_gmail_thread_id_from_message_id(message_id, username)
        
        # If Message-ID search fails, try alternative approaches for external emails
        if not gmail_thread_id:
            log.debug(f"Message-ID {message_id} not found in Gmail, trying alternative approaches for external email")
            
            # APPROACH 1: Look for ANY Gmail emails in the same conversation thread
            thread_data = mongo_info.get('thread_data', {})
            thread_id = thread_data.get('thread_id') or mongo_info.get('thread_id')
            
            if thread_id:
                log.info(f"Searching for Gmail emails in conversation thread: {thread_id}")
                
                # Get ALL emails from this thread from MongoDB
                thread_messages = label_manager.mongo_col.find_one({"thread_id": thread_id})
                if thread_messages and "Mails" in thread_messages:
                    for email_doc in thread_messages.get("Mails", []):
                        email_msg_id = email_doc.get("message_id", "")
                        
                        # Skip if no message_id or if it's the same external email
                        if not email_msg_id or email_msg_id == message_id:
                            continue
                            
                        # Check if this email is from Gmail (typically gmail.com domain)
                        if "@mail.gmail.com" in email_msg_id or "@googlemail.com" in email_msg_id:
                            log.info(f"Found Gmail email in conversation: {email_msg_id}, trying to get thread ID")
                            gmail_thread_id = get_gmail_thread_id_from_message_id(email_msg_id, username)
                            if gmail_thread_id:
                                log.info(f"Successfully found Gmail thread ID from conversation: {gmail_thread_id}")
                                break
            
            # APPROACH 2: If no Gmail emails found, try X-GM-THRID (though usually fails)
            if not gmail_thread_id:
                candidate_xgm_thrid = xgm_thrid
                if not candidate_xgm_thrid and thread_id and not '@' in str(thread_id):
                    candidate_xgm_thrid = str(thread_id)
                
                if candidate_xgm_thrid:
                    log.info(f"Trying X-GM-THRID approach as fallback: {candidate_xgm_thrid}")
                    gmail_thread_id = get_gmail_thread_id_from_xgm_thrid(candidate_xgm_thrid, username)
        
        if not gmail_thread_id:
            log.info(f"Message {message_id} not found in Gmail using both Message-ID and X-GM-THRID approaches (skipping labeling)")
            return True  # Return True to avoid treating as error
        
        subject = mongo_info.get('subject', 'No Subject')
        log.info(f"Using Gmail thread ID: {gmail_thread_id}, Subject: {subject}")
        
        # Step 3: Create ML results structure
        intent_label = get_intent_label(intent)
        sentiment_label = get_sentiment_label(sentiment)
        
        ml_results = {
            'intent': intent,
            'sentiment': sentiment,
            'gmail_labels': [intent_label, sentiment_label]
        }
        
        # Step 4: Apply labels directly to Gmail thread using stored thread ID
        success = apply_ml_labels(gmail_thread_id, ml_results, subject, username)
        
        if success:
            pass 
        else:
            log.error(f"Failed to apply labels to Gmail thread {gmail_thread_id}")
        
        return success
        
    except Exception as e:
        log.error(f"Error in search_and_tag_message_enhanced for {message_id}: {e}")
        return False


def search_and_tag_message(message_id: str, intent: str, sentiment: str, username: str = None, xgm_thrid: str = None) -> bool:
    """
    Fast search for a message by Message-ID and apply tags.
    
    Args:
        message_id: The Message-ID header to search for
        intent: The detected intent
        sentiment: The detected sentiment
        username: Username for Gmail API access
        xgm_thrid: Optional X-GM-THRID for external emails
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Use the enhanced version by default
        return search_and_tag_message_enhanced(message_id, intent, sentiment, username, xgm_thrid)
        
    except Exception as e:
        log.error(f"Error in search_and_tag_message for {message_id}: {e}")
        return False


