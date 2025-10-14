import datetime
import re
from email.header import decode_header, make_header
from pymongo import MongoClient
from logger.logger_setup import logger as log
import config
from typing import Optional

def clean_thread_subject(subject):
    """
    Clean email subject to get original thread topic by removing Re:, Fwd: prefixes.
    
    Args:
        subject: The email subject to clean
        
    Returns:
        Cleaned subject without reply/forward prefixes
    """
    if not subject:
        return "No Subject"
    
    # Remove Re:, Fwd:, RE:, FW:, AW:, SV: and similar prefixes (case insensitive)
    cleaned = re.sub(r'^(RE?|FWD?|FW|AW|SV|ANTW):\s*', '', subject.strip(), flags=re.IGNORECASE)
    
    # Keep cleaning until no more prefixes found (handles nested cases)
    while True:
        new_cleaned = re.sub(r'^(RE?|FWD?|FW|AW|SV|ANTW):\s*', '', cleaned, flags=re.IGNORECASE)
        if new_cleaned == cleaned:
            break
        cleaned = new_cleaned
    
    return cleaned.strip() if cleaned.strip() else "No Subject"

def extract_new_content(body_text, log_cleaning=False):
    """
    Extract only the new content from an email body, removing quoted text.
    Less aggressive to avoid cutting legitimate content.
    
    Args:
        body_text: The email body text to clean
        log_cleaning: Whether to log the cleaning operation (default: False)
    """
    if not body_text:
        return ""
    
    import re
    import unicodedata as _ud
    from logger.logger_setup import logger as log
    
    original_length = len(body_text)
    
    # Normalize line endings (handle both real CR/LF and literal sequences)
    body_text = body_text.replace('\r\n', '\n').replace('\r', '\n')
    body_text = body_text.replace('\\r\\n', '\n').replace('\\r', '\n').replace('\\n', '\n')

    # Normalize Unicode: convert NBSP/narrow NBSP/etc. to normal space, drop zero-width controls
    def _normalize_unicode_spaces(s: str) -> str:
        # translate various hard/nbsp spaces to regular space
        trans = {
            0x00A0: 0x20,  # NO-BREAK SPACE
            0x202F: 0x20,  # NARROW NO-BREAK SPACE
            0x2007: 0x20,  # FIGURE SPACE
            0x2009: 0x20,  # THIN SPACE
            0x200A: 0x20,  # HAIR SPACE
        }
        s = s.translate(trans)
        # remove zero-width and direction marks but keep newlines/tabs
        return ''.join(ch for ch in s if not (_ud.category(ch).startswith('C') and ch not in ('\n', '\t')))

    body_text = _normalize_unicode_spaces(body_text)
    
    # Find the earliest quote marker
    earliest_pos = len(body_text)
    cut_due_to_quote = False
    
    # Look for "On ... wrote:" patterns (common Gmail reply format)
    # This handles multiple variations including:
    # - Different date formats
    # - Different quote indicators (>, |, etc.)
    # - Different whitespace patterns
    # - Different variations of "wrote" (wrote, wrote at, etc.)
    gmail_quote_patterns = [
        r'(?:\n|^)\s*(?:>|\|)?\s*On\s+[^\n]*?\s+wrote\s*:\s*\n',  # Standard Gmail format
        r'(?:\n|^)\s*(?:>|\|)?\s*On\s+[^\n]*?\s+wrote\s+at\s*:\s*\n',  # Some clients use "wrote at:"
        r'(?:\n|^)\s*(?:>|\|)?\s*On\s+[^\n]*?\s+wrote\s*\n',  # No colon
        r'(?:\n|^)\s*(?:>|\|)?\s*On\s+[^\n]*?\s+wrote\s*:\s*$',  # At end of line
        r'(?:\n|^)\s*(?:>|\|)?\s*On\s+[^\n]*?\s+wrote\s*$'  # At end of line, no colon
    ]
    
    for pattern in gmail_quote_patterns:
        match = re.search(pattern, body_text, re.IGNORECASE | re.DOTALL)
        if match and match.start() < earliest_pos:
            earliest_pos = match.start()
            cut_due_to_quote = True
    
    # Look for Outlook-style quoted header blocks
    outlook_quote_pattern = r'(\n\s*From:.*?\n\s*Subject:|^\s*From:.*?\n\s*Subject:)'
    outlook_quote_match = re.search(outlook_quote_pattern, body_text, re.IGNORECASE | re.DOTALL)
    if outlook_quote_match and outlook_quote_match.start() < earliest_pos:
        earliest_pos = outlook_quote_match.start()
        cut_due_to_quote = True
    
    # If we found quoted content, keep only the part before it
    if cut_due_to_quote:
        body_text = body_text[:earliest_pos]
    
    # Remove any remaining quoted lines starting with '>'
    lines = body_text.split('\n')
    cleaned_lines = []
    for line in lines:
        if line.lstrip().startswith('>'):
            break
        cleaned_lines.append(line)
    
    # Join and clean up the result
    result = '\n'.join(cleaned_lines).strip()
    
    # Remove excessive whitespace
    result = re.sub(r'\n\s*\n\s*\n', '\n\n', result)  # Max 2 consecutive newlines
    result = re.sub(r'\r\n', '\n', result)  # Normalize line endings
    result = re.sub(r'[ \t]+\n', '\n', result)  # Trim trailing spaces before newline
    
    if log_cleaning and (cut_due_to_quote or len(result) != original_length):
        log.debug("Body cleaned: removed quotes/signatures")
    
    return result

def mongo_connect():
    """Connect to MongoDB."""
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    return db[config.MONGO_COL]


def fetch_thread_emails_from_imap(client, thread_id: str, limit: int = 10):
    """
    Fetch thread emails directly from IMAP using message_id with UID fallback.
    This ensures no email content is stored in MongoDB for security.
    
    Args:
        client: IMAP client connection
        thread_id: Thread ID to fetch emails for
        limit: Number of recent emails to fetch (default: 10)
        
    Returns:
        Thread data with email bodies fetched directly from IMAP
    """
    import email
    from email.policy import default
    
    try:
        # Get message metadata from MongoDB (uid, message_id only)
        mongo_col = mongo_connect()
        thread_doc = mongo_col.find_one({"thread_id": thread_id})
        
        if not thread_doc:
            log.warning(f"Thread {thread_id} not found in MongoDB")
            return None
            
        messages = thread_doc.get("messages", [])
        if not messages:
            log.warning(f"Thread {thread_id} has no messages in MongoDB")
            return None
            
        # Sort by date and get last N messages
        sorted_messages = sorted(messages, key=lambda x: x.get('date', datetime.datetime.min))
        recent_messages = sorted_messages[-limit:] if len(sorted_messages) > limit else sorted_messages
        
        thread_data = {
            "thread_id": thread_id,
            "total_emails_in_thread": len(messages),
            "created_at": thread_doc.get("created_at", "").isoformat() if hasattr(thread_doc.get("created_at", ""), 'isoformat') else str(thread_doc.get("created_at", "")),
            "last_updated": thread_doc.get("last_updated", "").isoformat() if hasattr(thread_doc.get("last_updated", ""), 'isoformat') else str(thread_doc.get("last_updated", "")),
            "Mails": []
        }
        
        # Fetch each email directly from IMAP
        original_folder = client.folder_name if hasattr(client, 'folder_name') else 'INBOX'
        folders_to_check = ['INBOX', '[Gmail]/Sent Mail']
        
        emails_fetched = 0
        emails_cleaned = 0
        extracted_subject = "No Subject"  # Will be set from first email
        
        for i, msg_meta in enumerate(recent_messages):
            try:
                uid = msg_meta.get('uid')
                message_id = msg_meta.get('message_id', '').strip('<>')
                folder = msg_meta.get('folder', 'INBOX')
                
                email_msg = None
                
                # Primary: Try to fetch using message_id
                if message_id:
                    for search_folder in folders_to_check:
                        try:
                            client.select_folder(search_folder, readonly=True)
                            # Search by message-id
                            search_results = client.search(['HEADER', 'Message-ID', message_id])
                            if search_results:
                                search_uid = search_results[0]
                                resp = client.fetch([search_uid], ['BODY.PEEK[]'])
                                raw_data = resp.get(search_uid, {}).get(b'RFC822') or resp.get(search_uid, {}).get(b'BODY[]')
                                if raw_data:
                                    email_msg = email.message_from_bytes(raw_data, policy=default)
                                    break
                        except Exception as e:
                            log.debug(f"Message-ID search failed in {search_folder}: {e}")
                            continue
                
                # Fallback: Try to fetch using UID
                if not email_msg and uid:
                    try:
                        target_folder = folder if folder else 'INBOX'
                        client.select_folder(target_folder, readonly=True)
                        resp = client.fetch([uid], ['BODY.PEEK[]'])
                        raw_data = resp.get(uid, {}).get(b'RFC822') or resp.get(uid, {}).get(b'BODY[]')
                        if raw_data:
                            email_msg = email.message_from_bytes(raw_data, policy=default)
                    except Exception as e:
                        log.warning(f"UID fallback failed for UID {uid}: {e}")
                
                if email_msg:
                    emails_fetched += 1
                    # Extract fresh email data
                    from_header = email_msg.get('From', '')
                    try:
                        from_name = str(make_header(decode_header(from_header or '')))
                        if "<" in from_name and ">" in from_name:
                            from_name = from_name.split("<")[0].strip().strip('"')
                    except Exception:
                        from_name = from_header
                    
                    # Extract subject from first email fetched (for ML pipeline)
                    if emails_fetched == 1:  
                        try:
                            subject_header = email_msg.get('Subject', '')
                            if subject_header:
                                decoded_subject = str(make_header(decode_header(subject_header)))
                                # Clean subject to get original thread topic (remove Re:, Fwd: prefixes)
                                extracted_subject = clean_thread_subject(decoded_subject)
                        except Exception:
                            extracted_subject = clean_thread_subject(subject_header) if subject_header else "No Subject"
                    
                    # Extract body content
                    body_content = ""
                    if email_msg.is_multipart():
                        plain_parts = []
                        for part in email_msg.walk():
                            if part.get_content_type() == 'text/plain':
                                try:
                                    payload = part.get_payload(decode=True)
                                    if payload:
                                        plain_parts.append(payload.decode(part.get_content_charset() or 'utf-8', errors='ignore'))
                                except Exception:
                                    continue
                        if plain_parts:
                            body_content = '\n\n'.join([p.strip() for p in plain_parts if p])
                    else:
                        try:
                            payload = email_msg.get_payload(decode=True)
                            if payload:
                                body_content = payload.decode(email_msg.get_content_charset() or 'utf-8', errors='ignore')
                        except Exception:
                            body_content = str(email_msg.get_payload() or '')
                    
                    # Clean the body content
                    if body_content:
                        original_length = len(body_content)
                        body_content = extract_new_content(body_content, log_cleaning=False)
                        if len(body_content) != original_length:
                            emails_cleaned += 1
                    
                    mail_data = {
                        "Email": i + 1,
                        "date": msg_meta.get("date", "").isoformat() if hasattr(msg_meta.get("date", ""), 'isoformat') else str(msg_meta.get("date", "")),
                        "from": from_name,
                        "folder": folder,
                        "body": body_content.strip(),
                        "message_id": msg_meta.get("message_id", "")  # Preserve for threading
                    }
                    thread_data["Mails"].append(mail_data)
                    
                else:
                    log.warning(f"Failed to fetch email {i+1} for thread {thread_id}")
                    
            except Exception as e:
                log.error(f"Error fetching email {i+1} in thread {thread_id}: {e}")
                continue
        
        # Restore original folder
        try:
            client.select_folder(original_folder, readonly=True)
        except Exception:
            pass
        
        # Add subject to thread_data (extracted from first email)
        thread_data["subject"] = extracted_subject
        
        # Log summary
        log.info(f"Fetching {emails_fetched} emails from IMAP for thread {thread_id}")
        log.info(f"IMAP fetch complete: {emails_fetched} emails retrieved, {emails_cleaned} cleaned") 
        if emails_cleaned > 0:
            log.info(f"Body cleaning removed quotes/signatures from {emails_cleaned} emails")
            
        return thread_data
    except Exception as e:
        log.error(f"Failed to fetch thread {thread_id} from IMAP: {e}")
        return None

def _message_to_doc(uid, msg, folder):
    """Convert email.message to a JSON-serializable dict for Mongo."""
    import email.utils
    import re as _re2
    
    date_hdr = msg.get('Date')
    if date_hdr:
        try:
            # Parse the date header to a datetime object
            dt_tuple = email.utils.parsedate_tz(date_hdr)
            if dt_tuple:
                dt = datetime.datetime.fromtimestamp(email.utils.mktime_tz(dt_tuple))
            else:
                dt = datetime.datetime.now()
        except Exception:
            dt = datetime.datetime.now()
    else:
        dt = datetime.datetime.now()

    # Extract body content and clean it
    body_content = ""
    if msg.is_multipart():
        # Aggregate all text/plain parts in order
        plain_parts = []
        html_fallback = None
        for part in msg.walk():
            ctype = part.get_content_type()
            try:
                if ctype == 'text/plain':
                    payload = part.get_payload(decode=True)
                    if payload:
                        plain_parts.append(payload.decode(part.get_content_charset() or 'utf-8', errors='ignore'))
                elif ctype == 'text/html' and html_fallback is None:
                    payload = part.get_payload(decode=True)
                    if payload:
                        html_fallback = payload.decode(part.get_content_charset() or 'utf-8', errors='ignore')
            except Exception as e:
                log.warning(f"Error decoding MIME part ({ctype}): {e}")
                continue

        if plain_parts:
            body_content = '\n\n'.join([p.strip() for p in plain_parts if p])
        elif html_fallback:
            # Simple HTML to text conversion
            try:
                from html import unescape
                import re as _re
                html_text = html_fallback
                html_text = html_text.replace('\r\n', '\n').replace('\r', '\n')
                html_text = _re.sub(r'(?i)<br\s*/?>', '\n', html_text)
                html_text = _re.sub(r'(?i)</p\s*>', '\n\n', html_text)
                # Remove all other tags
                html_text = _re.sub(r'<[^>]+>', '', html_text)
                html_text = unescape(html_text)
                # Collapse excessive whitespace
                html_text = _re.sub(r'\n\s*\n\s*\n+', '\n\n', html_text)
                body_content = html_text
            except Exception as e:
                log.warning(f"Error converting HTML to text: {e}")
                body_content = html_fallback
    else:
        # For non-multipart messages
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                body_content = payload.decode(msg.get_content_charset() or 'utf-8', errors='ignore')
            else:
                body_content = str(msg.get_payload() or '')
        except Exception as e:
            log.warning(f"Error decoding message: {e}")
            body_content = str(msg.get_payload() or '')
    
    # Clean up the body content
    if body_content:
        # Remove any null bytes that might cause issues
        body_content = body_content.replace('\x00', ' ')
        # Normalize line endings (handle both real and literal CR/LF)
        body_content = body_content.replace('\r\n', '\n').replace('\r', '\n')
        body_content = body_content.replace('\\r\\n', '\n').replace('\\r', '\n').replace('\\n', '\n')
        # Extract only the new content (remove quoted text)
        body_content = extract_new_content(body_content, log_cleaning=False)
        # Final tidy: collapse >2 blank lines
        import re as _re2
        body_content = _re2.sub(r'\n\s*\n\s*\n+', '\n\n', body_content)
    else:
        log.warning("Empty body content after extraction")

    # Decode headers safely
    try:
        from_header = str(make_header(decode_header(msg.get('From', '') or '')))
    except Exception:
        from_header = msg.get('From', '')
        
    try:
        to_header = str(make_header(decode_header(msg.get('To', '') or '')))
    except Exception:
        to_header = msg.get('To', '')

    try:
        subject_header = str(make_header(decode_header(msg.get('Subject', '') or '')))
    except Exception:
        subject_header = msg.get('Subject', '')

    # Store only essential fields - subject is stored at thread level, 
    # from/to/body/cc/bcc excluded per requirements
    doc = {
        "uid": int(uid),
        "date": dt,
        "folder": folder,
        "message_id": msg.get('Message-ID', ''),

    }
    return doc

def atomic_upsert_thread(col, thread_id, uid, msg, folder):
    """Minimal atomic upsert for a fresh DB - no subject storage per requirements."""
    now = datetime.datetime.now()
    uid_int = int(uid)
    msg_id = msg.get('Message-ID', '')
    msg_doc = _message_to_doc(uid, msg, folder)

    filter_q = {"thread_id": thread_id, "uids": {"$ne": uid_int}}
    
    update_q = {
        "$setOnInsert": {
            "thread_id": thread_id,
            "created_at": now,
        },
        "$set": {
            "last_updated": now,
        },
        "$push": {
            "messages": {
                "$each": [msg_doc],
                "$sort": {"date": 1}
            }
        },
        "$addToSet": {
            "uids": uid_int,
            "message_ids": msg_id
        }
    }
    res = col.update_one(filter_q, update_q, upsert=True)
    return res

def atomic_upsert_thread_with_user_id(col, thread_id, uid, msg, folder, user_id):
    """Multi-tenant atomic upsert with user_id for data integrity."""
    now = datetime.datetime.now()
    uid_int = int(uid)
    msg_id = msg.get('Message-ID', '')
    msg_doc = _message_to_doc(uid, msg, folder)
    
    # Add user_id to message document for complete isolation
    msg_doc['user_id'] = user_id

    # Filter by thread_id AND user_id for complete data integrity
    filter_q = {
        "thread_id": thread_id, 
        "user_id": user_id,
        "uids": {"$ne": uid_int}
    }
    
    update_q = {
        "$setOnInsert": {
            "thread_id": thread_id,
            "user_id": user_id,  # Ensure user_id is set at thread level
            "created_at": now,
        },
        "$set": {
            "last_updated": now,
        },
        "$push": {
            "messages": {
                "$each": [msg_doc],
                "$sort": {"date": 1}
            }
        },
        "$addToSet": {
            "uids": uid_int,
            "message_ids": msg_id
        }
    }
    res = col.update_one(filter_q, update_q, upsert=True)
    return res


def get_thread_from_mongo(thread_id: str, limit: int = 10):
    """

    
    Args:
        thread_id: Thread ID to fetch
        limit: Number of recent emails to return (default: 10)
        
    Returns:
        Thread data with last N emails or None if not found
    """
    try:
        mongo_col = mongo_connect()
        
        # Find the thread document with messages sorted by date
        pipeline = [
            {"$match": {"thread_id": thread_id}},
            {"$unwind": "$messages"},
            {"$sort": {"messages.date": 1}},
            {
                "$group": {
                    "_id": "$_id",
                    "thread_id": {"$first": "$thread_id"},
                    "subject": {"$first": "$subject"},
                    "created_at": {"$first": "$created_at"},
                    "last_updated": {"$first": "$last_updated"},
                    "messages": {"$push": "$messages"}
                }
            }
        ]
        
        result = list(mongo_col.aggregate(pipeline))
        
        if not result:
            # Fallback to simple query
            simple_doc = mongo_col.find_one({"thread_id": thread_id})
            if simple_doc:
                all_messages = simple_doc.get("messages", [])
                if all_messages:
                    def get_fallback_date(msg):
                        date_val = msg.get('date', datetime.datetime.min)
                        if isinstance(date_val, str):
                            try:
                                from dateutil import parser
                                parsed_date = parser.parse(date_val)
                                if parsed_date.tzinfo is not None:
                                    parsed_date = parsed_date.replace(tzinfo=None)
                                return parsed_date
                            except (ValueError, AttributeError):
                                return datetime.datetime.min
                        elif isinstance(date_val, datetime.datetime):
                            if date_val.tzinfo is not None:
                                return date_val.replace(tzinfo=None)
                            return date_val
                        else:
                            return datetime.datetime.min
                    
                    all_messages_sorted = sorted(all_messages, key=lambda x: get_fallback_date(x))
                    thread_doc = {
                        "thread_id": thread_id,
                        "subject": simple_doc.get("subject", "No Subject"),
                        "created_at": simple_doc.get("created_at", ""),
                        "last_updated": simple_doc.get("last_updated", ""),
                        "messages": all_messages_sorted
                    }
                else:
                    log.warning("Thread %s has no messages", thread_id)
                    return None
            else:
                log.warning("Thread %s not found in MongoDB", thread_id)
                return None
        else:
            thread_doc = result[0]
        
        all_messages = thread_doc.get("messages", [])
        
        if not all_messages:
            log.warning("Thread %s has no messages", thread_id)
            return None
        
        # Sort by date for chronological order
        def get_message_date(msg):
            date_val = msg.get('date')
            if isinstance(date_val, str):
                try:
                    from dateutil import parser
                    parsed_date = parser.parse(date_val)
                    # Convert to naive datetime if it has timezone info
                    if parsed_date.tzinfo is not None:
                        parsed_date = parsed_date.replace(tzinfo=None)
                    return parsed_date
                except (ValueError, AttributeError):
                    return datetime.datetime.min
            elif isinstance(date_val, datetime.datetime):
                # Convert to naive datetime if it has timezone info
                if date_val.tzinfo is not None:
                    return date_val.replace(tzinfo=None)
                return date_val
            else:
                return datetime.datetime.min
            
        all_messages = sorted(all_messages, key=lambda x: get_message_date(x))
        
        
        thread_data = {
            "thread_id": thread_id,
            "total_emails_in_thread": len(all_messages),
            "created_at": thread_doc.get("created_at", "").isoformat() if hasattr(thread_doc.get("created_at", ""), 'isoformat') else str(thread_doc.get("created_at", "")),
            "last_updated": thread_doc.get("last_updated", "").isoformat() if hasattr(thread_doc.get("last_updated", ""), 'isoformat') else str(thread_doc.get("last_updated", "")),
            "Mails": []
        }
        
        # Process messages and clean quoted text
        for i, msg in enumerate(all_messages):
            try:
                # Extract sender name
                from_field = msg.get("from") or msg.get("From") or msg.get("sender") or ""
                if isinstance(from_field, list) and from_field:
                    from_field = from_field[0] if from_field else ""
                
                from_name = ""
                if from_field:
                    if "<" in str(from_field) and ">" in str(from_field):
                        from_name = str(from_field).split("<")[0].strip().strip('"')
                    else:
                        from_name = str(from_field)
                
                # Extract body content (already cleaned when stored)
                body_content = (msg.get("body") or 
                              msg.get("Body") or 
                              msg.get("text_content") or 
                              msg.get("plain_text") or 
                              msg.get("content") or "").strip()
                
                folder = msg.get("folder", "") or msg.get("Folder", "")
                
                mail_data = {
                    "Email": i + 1,
                    "date": msg.get("date", "").isoformat() if hasattr(msg.get("date", ""), 'isoformat') else str(msg.get("date", "")),
                    "from": from_name,
                    "folder": folder,
                    "body": body_content
                }
                thread_data["Mails"].append(mail_data)
            except Exception as e:
                log.error("Error processing message %d in thread %s: %s", i+1, thread_id, e)
        
        # Return last N messages only
        total_messages = len(thread_data["Mails"])
        start_idx = max(0, total_messages - limit)
        output_messages = thread_data["Mails"][start_idx:]
        
        # Reindex Email numbers for output
        for i, mail in enumerate(output_messages):
            mail["Email"] = i + 1
        
        # Create final output with last N messages
        output_data = {
            **thread_data,
            "Mails": output_messages,
            "fetched_emails_count": len(output_messages),
            "backfill_count": start_idx,
            "all_messages_count": total_messages
        }
        
        return output_data
        
    except Exception as e:
        log.error("Failed to fetch thread %s from MongoDB: %s", thread_id, e)
        return None
