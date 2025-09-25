import datetime
from email.header import decode_header, make_header
from pymongo import MongoClient
import config
from logger.logger_setup import logger as log

def extract_new_content(body_text):
    """
    Extract only the new content from an email body, removing quoted text.
    """
    if not body_text:
        return ""
    
    import re
    
    # First, let's find where quoted content starts and preserve content before it
    quoted_patterns = [
        r'(\n\s*On .+? at .+? .+? <.+?>\s*wrote:)',  # Standard Gmail pattern
        r'(\n\s*On .+?, .+? at .+? .+? <.+?> wrote:)', # With comma after day
        r'(\n\s*.*?<.+?>\s*wrote:)',                   # Generic <email> wrote: pattern
        r'(\n\s*From:)',                               # Outlook style
        r'(\n\s*----.*Original Message.*----)',        # Forward patterns
        r'(\n\s*_{5,})',                              # Divider lines
    ]
    
    # Find the earliest quote marker
    earliest_match = None
    earliest_pos = len(body_text)
    
    for pattern in quoted_patterns:
        match = re.search(pattern, body_text, re.IGNORECASE | re.DOTALL)
        if match and match.start() < earliest_pos:
            earliest_pos = match.start()
            earliest_match = match
    
    # If we found quoted content, keep only the part before it
    if earliest_match:
        body_text = body_text[:earliest_pos]
    
    # Split into lines for line-by-line processing
    lines = body_text.split('\n')
    new_content_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        
        # Skip lines that look like quoted content indicators
        if (line_stripped.startswith('On ') and 'wrote:' in line_stripped) or \
           (line_stripped.startswith('From:')) or \
           (line_stripped.startswith('Sent:')) or \
           (line_stripped.startswith('To:')) or \
           (line_stripped.startswith('Subject:')) or \
           (line_stripped.startswith('>')):
            break
            
        # Skip empty lines at the start
        if not new_content_lines and not line_stripped:
            continue
            
        new_content_lines.append(line)
    
    # Join and clean up the result
    result = '\n'.join(new_content_lines).strip()
    
    # Remove excessive whitespace
    result = re.sub(r'\n\s*\n\s*\n', '\n\n', result)  # Max 2 consecutive newlines
    result = re.sub(r'\r\n', '\n', result)  # Normalize line endings
    
    return result

def mongo_connect():
    """Connect to MongoDB."""
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    return db[config.MONGO_COL]

def _message_to_doc(uid, msg, folder):
    """Convert email.message to a JSON-serializable dict for Mongo."""
    import email.utils
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
        # First try to find text/plain part
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body_content = payload.decode('utf-8', errors='ignore')
                        break
                except Exception as e:
                    log.warning(f"Error decoding part: {e}")
                    continue
        
        # If no text/plain part found, try to get any text content
        if not body_content:
            for part in msg.walk():
                if part.get_content_maintype() == 'text':
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            body_content = payload.decode('utf-8', errors='ignore')
                            break
                    except Exception as e:
                        log.warning(f"Error decoding text part: {e}")
                        continue
    else:
        # For non-multipart messages
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                body_content = payload.decode('utf-8', errors='ignore')
            else:
                body_content = str(msg.get_payload() or '')
        except Exception as e:
            log.warning(f"Error decoding message: {e}")
            body_content = str(msg.get_payload() or '')
    
    # Clean up the body content
    if body_content:
        # Remove any null bytes that might cause issues
        body_content = body_content.replace('\x00', ' ')
        # Extract only the new content (remove quoted text)
        body_content = extract_new_content(body_content)
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

    doc = {
        "uid": int(uid),
        "date": dt,
        "folder": folder,
        "message_id": msg.get('Message-ID', ''),
        "from": from_header,
        "to": to_header,
        "subject": subject_header,
        "body": body_content.strip(),
        "cc": msg.get('Cc', ''),
        "bcc": msg.get('Bcc', '')
    }
    return doc

def atomic_upsert_thread(col, thread_id, uid, msg, folder='INBOX'):
    """Minimal atomic upsert for a fresh DB."""
    try:
        subj = str(make_header(decode_header(msg.get('Subject', '') or '')))
    except Exception:
        subj = msg.get('Subject', '')
    
    now = datetime.datetime.now()
    uid_int = int(uid)
    msg_id = msg.get('Message-ID', '')
    msg_doc = _message_to_doc(uid, msg, folder)

    filter_q = {"thread_id": thread_id, "uids": {"$ne": uid_int}}
    
    update_q = {
        "$setOnInsert": {
            "thread_id": thread_id,
            "created_at": now,
            "subject": subj,
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
        },
    }

    res = col.update_one(filter_q, update_q, upsert=True)
    return res


def get_thread_from_mongo(thread_id: str, limit: int = 5):
    """
    Fetch thread data from MongoDB with last N emails
    
    Args:
        thread_id: Thread ID to fetch
        limit: Number of recent emails to return (default: 5)
        
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
            "subject": thread_doc.get("subject", "No Subject"),
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
