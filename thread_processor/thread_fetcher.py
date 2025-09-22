import os
import json
import re
from typing import Dict, List, Optional, Any
from imapclient import IMAPClient
import email
from email import policy
from email.utils import parseaddr, parsedate_to_datetime
from pymongo import MongoClient
from datetime import datetime
import sys
import os
import config
from logger.logger_setup import logger
from mail_parser.auth_handler import token_manager


# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class EmailBodyProcessor:
    """Handles email body extraction and cleaning"""
    
    @staticmethod
    def extract_body(msg: email.message.EmailMessage) -> Optional[str]:
        """Extract text/plain if available, else text/html. Returns None if nothing found."""
        try:
            if msg.is_multipart():
                # First try to find text/plain
                for part in msg.walk():
                    ctype = part.get_content_type()
                    disp = str(part.get("Content-Disposition", "") or "")
                    if ctype == "text/plain" and "attachment" not in disp:
                        return part.get_payload(decode=True).decode(errors="replace")
                
                # Fallback to text/html
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        return part.get_payload(decode=True).decode(errors="replace")
                return None
            else:
                return msg.get_payload(decode=True).decode(errors="replace")
        except Exception:
            logger.exception("extract_body failed")
            return None

    @staticmethod
    def clean_body(text: str, remove_signature: bool = True, sender_name: str = None) -> str:
        # Clean email body - simple and fast approach
        if not text:
            return ""
        
        # Normalize newlines
        txt = text.replace('\r\n', '\n').replace('\r', '\n')
        lines = txt.split('\n')

        # Simple patterns to cut at
        cut_words = ['on ', 'from:', 'sent:', 'to:', 'subject:', '-----', 'original message']
        disclaimer_words = ['disclaimer', 'confidential']
        sig_words = ['regards', 'best', 'thanks', 'cheers', 'sincerely']

        cleaned_lines = []
        
        for i, line in enumerate(lines):
            line_lower = line.strip().lower()
            
            # Skip empty lines
            if not line.strip():
                cleaned_lines.append(line)
                continue
                
            # Check for cut patterns (drop everything from here)
            if (line.strip().startswith('>') or 
                any(word in line_lower for word in cut_words)):
                break  # Cut everything from here
                
            # Check for disclaimer (drop this line only)
            if any(word in line_lower for word in disclaimer_words):
                continue  # Skip this line only
                
            # Check for signature with name (drop everything from here)
            if remove_signature and sender_name:
                if any(sig in line_lower for sig in sig_words):
                    # Check if name appears in next few lines
                    name_found = False
                    for j in range(i + 1, min(i + 4, len(lines))):
                        if j < len(lines) and sender_name.lower() in lines[j].lower():
                            name_found = True
                            break
                    if name_found:
                        break  # Cut from signature
                        
            cleaned_lines.append(line)
        
        # Return cleaned text
        return '\n'.join(cleaned_lines).strip()

    @staticmethod
    def _extract_name_parts(sender_name: str) -> list:
        """
        Extract name parts from sender name for flexible matching.
        
        Args:
            sender_name: Full name like "Yash Pandya" or "John Doe"
            
        Returns:
            List of name parts in various formats for matching
        """
        if not sender_name:
            return []
        
        # Clean the name - remove extra whitespace 
        name = re.sub(r'\s+', ' ', sender_name.strip())
 
        
        parts = name.split()
        if len(parts) < 2:
            return [name.lower(), name.upper(), name.title()]
        
        # Generate various name combinations
        name_variants = []
        first_name = parts[0]
        last_name = parts[-1]
        
        # Add individual parts
        name_variants.extend([first_name, last_name])
        
        # Add full name variations
        full_name = ' '.join(parts)
        name_variants.extend([
            full_name,                   
            f"{last_name} {first_name}", 
            f"{first_name}, {last_name}",
            f"{last_name}, {first_name}", 
        ])
        
        # Add case variations for each variant
        final_variants = []
        for variant in name_variants:
            final_variants.extend([
                variant.lower(),
                variant.upper(), 
                variant.title(),
                variant  # original case
            ])
        
        return list(set(final_variants)) 
    
    @staticmethod
    def _find_signature_with_name(lines: list, name_parts: list) -> int:
        if not name_parts:
            return None
        
        # Simple signature starters
        sig_starters = ['regards', 'best', 'thanks', 'cheers', 'sincerely']
        
        # Look for signature starter followed by name in next few lines
        for i, line in enumerate(lines):
            line_lower = line.strip().lower()
            
            # Check if line starts with signature words
            if any(starter in line_lower for starter in sig_starters):
                # Check next 3 lines for sender name
                for j in range(i + 1, min(i + 4, len(lines))):
                    check_line = lines[j].strip()
                    if check_line and any(name.lower() in check_line.lower() for name in name_parts):
                        return i  # Drop everything from signature starter
        
        return None
    
    
    @staticmethod
    def _find_traditional_signature(lines: list) -> int:
        # Simple signature detection - drop everything from signature starters
        sig_words = ['regards', 'best', 'thanks', 'cheers', 'sincerely']
        
        for i, line in enumerate(lines):
            line_clean = line.strip().lower()
            if any(word in line_clean for word in sig_words):
                return i
        
        return None


class IMAPFolderResolver:
    """Handles IMAP folder mapping and resolution"""
    
    @staticmethod
    def map_local_folder_to_imap(stored: str) -> str:
        """Map local folder names to IMAP folder names"""
        if not stored:
            return stored
        
        s = stored.strip().lower()
        if s in ("inbox", "inbox/"):
            return "INBOX"
        if s in ("sent", "sent mail", "sentitems", "sent items"):
            return "[Gmail]/Sent Mail"
        
        return stored

    @staticmethod
    def resolve_folder(imap_client: IMAPClient, desired: str) -> Optional[str]:
        """Resolve folder name on IMAP server"""
        try:
            folders = [f for flags, delim, f in imap_client.list_folders()]
        except Exception:
            logger.exception("list_folders failed")
            return None
        
        if desired in folders:
            return desired
        
        if not desired:
            return None
        
        dl = desired.lower()

        for f in folders:
            if f.lower() == dl:
                return f
        
        # Partial match
        for f in folders:
            if dl in f.lower():
                return f
        
        return None


class ThreadFetcher:
    """Main class for fetching and processing email threads"""
    
    def __init__(self, save_to_json=False, limit=5, shared_imap_client=None):
        # Initialize ThreadFetcher - save_to_json deprecated (using SimpleNamespace now)
        self.limit = limit
        self.shared_imap_client = shared_imap_client  # Reuse existing connection
        
        # IMAP configuration
        self.imap_server = config.IMAP_SERVER
        self.username = None  # Will be set dynamically from OAuth tokens
        self.use_oauth = True  # Always use OAuth for Gmail
        
        # MongoDB configuration
        self.mongo_uri = config.MONGO_URI
        self.mongo_db = config.MONGO_DB
        self.mongo_col = config.MONGO_COL
        
        # Connection settings
        self.connection_timeout = 60
        self.max_retries = 5
        self.retry_delay = 5
        
        # Initialize components
        self.body_processor = EmailBodyProcessor()
        self.folder_resolver = IMAPFolderResolver()

    def _create_imap_connection_with_retry(self) -> 'IMAPClient':
        # Create IMAP connection with retry logic and error handling
        import time
        from imapclient import IMAPClient
        
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                logger.info("Creating IMAP connection (attempt %d/%d)", attempt + 1, self.max_retries)
                
                # Create connection with timeout
                client = IMAPClient(self.imap_server, ssl=True, timeout=self.connection_timeout)
                
                # Authenticate
                if self.use_oauth:
                    self._authenticate_oauth(client)
                else:
                    self._authenticate_password(client)
                
                logger.info("IMAP connection established successfully")
                return client
                
            except Exception as e:
                last_error = e
                logger.warning("IMAP connection attempt %d failed: %s", attempt + 1, e)
                
                if attempt < self.max_retries - 1:
                    logger.info("Retrying in %d seconds...", self.retry_delay)
                    time.sleep(self.retry_delay)
                    # Exponential backoff
                    self.retry_delay *= 2
        
        # All attempts failed
        logger.error("Failed to establish IMAP connection after %d attempts", self.max_retries)
        raise Exception(f"IMAP connection failed: {last_error}")
    
    def _authenticate_oauth(self, client):
        """Handle OAuth authentication with token refresh"""
        tokens = token_manager.load_tokens()
        if not tokens or not tokens.get('access_token'):
            raise Exception("No valid OAuth tokens available")
        
        # Get dynamic username from tokens
        username = token_manager.get_dynamic_username()
        
        try:
            client.oauth2_login(username, tokens['access_token'])
            logger.debug("IMAP OAuth login successful")
        except Exception as e:
            logger.warning("OAuth login failed, attempting token refresh: %s", e)
            
            # CRITICAL: Only refresh if we're NOT using shared IMAP client
            # This prevents token conflicts with IMAP monitor
            if self.shared_imap_client:
                logger.error("OAuth failed with shared client - not refreshing token to avoid conflicts")
                raise Exception("OAuth authentication failed with shared client")
            
            # Try to refresh token (only for independent connections)
            try:
                logger.info("ThreadFetcher refreshing token (independent connection)")
                token_manager.refresh_access_token()
                refreshed_tokens = token_manager.load_tokens()
                if refreshed_tokens and refreshed_tokens.get('access_token'):
                    # Use dynamic username for retry
                    username = token_manager.get_dynamic_username()
                    client.oauth2_login(username, refreshed_tokens['access_token'])
                    logger.info("IMAP OAuth login successful after token refresh")
                else:
                    raise Exception("Failed to refresh OAuth tokens")
            except Exception as refresh_error:
                logger.error("Token refresh failed: %s", refresh_error)
                raise Exception("OAuth authentication failed and token refresh failed")
    
    def _authenticate_password(self, client):
        """Handle password authentication"""
        password = (os.environ.get("MAIL_PASS") or 
                   os.environ.get("PASSWORD") or 
                   getattr(config, 'PASSWORD', None) or "")
        if not password:
            raise Exception("No password available for IMAP authentication")
        client.login(self.username, password)
    
    def _execute_with_connection_retry(self, operation_func, *args, **kwargs):
        """Execute an operation with connection retry logic"""
        import time
        
        # Use shared IMAP client if available (prevents token conflicts)
        if self.shared_imap_client:
            try:
                return operation_func(self.shared_imap_client, *args, **kwargs)
            except Exception as e:
                logger.warning("Shared IMAP client failed, falling back to new connection: %s", e)
                # Fall through to create new connection
        
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                # Create fresh connection for each attempt
                with self._create_imap_connection_with_retry() as client:
                    return operation_func(client, *args, **kwargs)
                    
            except Exception as e:
                last_error = e
                logger.warning("Operation attempt %d failed: %s", attempt + 1, e)
                
                # Retry on any error
                if attempt < self.max_retries - 1:
                    logger.info("Retrying in %d seconds...", self.retry_delay)
                    time.sleep(self.retry_delay)
                    continue
        
        # All attempts failed
        logger.error("Operation failed after %d attempts", self.max_retries)
        raise Exception(f"Operation failed: {last_error}")
    

    def _build_canonical_messages_from_doc(self, doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract message information from MongoDB document"""
        msgs = []
        if doc.get("messages"):
            for m in doc["messages"]:
                msgs.append({
                    "message_id": m.get("message_id") or m.get("msg_id"),
                    "uid": m.get("uid"),
                    "stored_folder": m.get("folder"),
                    "date": m.get("date")  # Include date for sorting
                })
        else:
            # Fallback to old format
            mids = doc.get("msg_ids", [])
            uids = doc.get("uids", [])
            for i, mid in enumerate(mids):
                msgs.append({
                    "message_id": mid,
                    "uid": uids[i] if i < len(uids) else None,
                    "stored_folder": None,
                    "date": None
                })
        return msgs

    def fetch_messages_for_doc(self, imap_client: IMAPClient, doc: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:

        results = []
        messages = self._build_canonical_messages_from_doc(doc)
        
        # Sort messages by date if available, otherwise keep original order
        messages_with_dates = []
        messages_without_dates = []
        
        for m in messages:
            if m.get("date"):
                messages_with_dates.append(m)
            else:
                messages_without_dates.append(m)
        
        # Sort messages with dates chronologically
        if messages_with_dates:
            try:
                messages_with_dates.sort(key=lambda x: parsedate_to_datetime(x["date"]) if x.get("date") else datetime.min)
            except Exception:
                logger.warning("Failed to sort messages by date, keeping original order")
        
        # Combine sorted messages with dates and messages without dates
        sorted_messages = messages_with_dates + messages_without_dates
        
        # Take the last 'limit' messages (most recent)
        recent_messages = sorted_messages[-limit:] if len(sorted_messages) > limit else sorted_messages
        
        for m in recent_messages:
            mid = m.get("message_id")
            uid = m.get("uid")
            stored_folder = m.get("stored_folder")
            
            # Resolve folder
            mapped = self.folder_resolver.map_local_folder_to_imap(stored_folder)
            folder_on_server = (self.folder_resolver.resolve_folder(imap_client, mapped) or 
                              self.folder_resolver.resolve_folder(imap_client, stored_folder))
            
            if not folder_on_server:
                logger.warning("Could not resolve folder '%s' for %s; skipping", stored_folder, mid or uid)
                continue

            try:
                imap_client.select_folder(folder_on_server, readonly=True)
            except Exception:
                logger.exception("select_folder failed for %s", folder_on_server)
                continue

            fetched = None

            # message-id search in folder
            if mid:
                try:
                    uids = imap_client.search(["HEADER", "Message-ID", mid])
                    if uids:
                        resp = imap_client.fetch([uids[0]], ["RFC822"])
                        entry = resp.get(uids[0]) or next(iter(resp.values()), None)
                        if entry and b"RFC822" in entry:
                            raw = entry[b"RFC822"]
                            msg = email.message_from_bytes(raw, policy=policy.default)
                            fetched = self._process_message(msg, folder_on_server, uids[0], mid)
                except Exception:
                    logger.exception("Message-ID search failed for %s in %s", mid, folder_on_server)

            # Fallback to UID fetch
            if not fetched and uid:
                try:
                    resp = imap_client.fetch([uid], ["RFC822"])
                    entry = resp.get(uid) or next(iter(resp.values()), None)
                    if entry and b"RFC822" in entry:
                        raw = entry[b"RFC822"]
                        msg = email.message_from_bytes(raw, policy=policy.default)
                        fetched = self._process_message(msg, folder_on_server, uid, mid)
                except Exception:
                    logger.exception("UID fetch failed for %s in %s", uid, folder_on_server)

            if fetched:
                results.append(fetched)
            else:
                logger.info("Could not fetch message (message_id=%s uid=%s) in %s", mid, uid, folder_on_server)

        return results

    def _process_message(self, msg: email.message.EmailMessage, folder: str, uid: int, message_id: str) -> Dict[str, Any]:
        """Process a single email message and return structured data"""
        date_raw = msg.get("Date")
        from_raw = msg.get("From") or ""
        name, addr = parseaddr(from_raw)
        body = self.body_processor.extract_body(msg)
        # Pass sender name for intelligent signature removal
        clean_body = self.body_processor.clean_body(body, remove_signature=True, sender_name=name)
        
        return {
            "message_id": msg.get("Message-ID", message_id),
            "folder": folder,
            "uid": uid,
            "date_raw": date_raw,
            "from_raw": from_raw,
            "from_name": name or addr,
            "from_email": addr,
            "body": clean_body
        }

    def build_thread_json(self, thread_id: str, limit: int = 5, imap_client=None) -> Optional[Dict[str, Any]]:

        try:
            # Connect to MongoDB
            mongo_client = MongoClient(self.mongo_uri)
            col = mongo_client[self.mongo_db][self.mongo_col]
            
            # Find the thread document
            doc = col.find_one({"thread_id": thread_id})
            if not doc:
                logger.error("Thread %s not found in MongoDB", thread_id)
                return None

            subject = doc.get("subject", "")
            
            # Use existing IMAP client or create new one with resilience
            if imap_client is not None:
                # Use the existing authenticated client from the pipeline

                try:
                    fetched = self.fetch_messages_for_doc(imap_client, doc, limit)
                except Exception as e:
                    logger.warning("Existing IMAP client failed, creating new connection: %s", e)
                    # Fallback to new connection with retry
                    fetched = self._execute_with_connection_retry(
                        self.fetch_messages_for_doc, doc, limit
                    )
            else:
                # Create new connection with retry logic

                fetched = self._execute_with_connection_retry(
                    self.fetch_messages_for_doc, doc, limit
                )

            # Sort by parsed date if possible, else keep document order
            def _parse_date(dstr):
                try:
                    return parsedate_to_datetime(dstr) if dstr else datetime.min
                except Exception:
                    return datetime.min

            fetched_sorted = sorted(
                fetched,
                key=lambda x: _parse_date(x.get("date_raw"))
            )

            # Build email list with proper indexing
            mails = []
            total_emails = len(fetched_sorted)
            
            for idx, m in enumerate(fetched_sorted, start=1):
                mails.append({
                    "Email": idx,
                    "date": m.get("date_raw") or "",
                    "from": m.get("from_name") or m.get("from_raw") or "",
                    "folder": m.get("folder") or "",
                    "body": m.get("body") or ""
                })

            # Build final output
            result = {
                "thread_id": thread_id,
                "subject": subject,
                "total_emails_in_thread": doc.get("msg_count", len(doc.get("msg_ids", []))),
                "emails_fetched": total_emails,
                "Mails": mails
            }
            
            
            # Return data for object creation in integration.py
            
            return result
            
        except Exception as e:
            logger.error("Failed to build thread JSON for %s: %s", thread_id, e)
            return None

