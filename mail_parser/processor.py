import os
import sys
import email
from email.policy import default

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from logger.logger_setup import logger as log
from Utility.mongo_utils import atomic_upsert_thread, mongo_connect
from thread_processor.integration import integrate_with_processor_after_storage
import email
from email import policy


def backfill_thread_emails(client, mongo_col, thread_id):
    # Backfill missing thread emails from INBOX + SENT folders
    try:
        # Get existing emails from MongoDB for this thread
        existing_doc = mongo_col.find_one({"thread_id": thread_id})
        existing_emails = set()
        
        if existing_doc and existing_doc.get("messages"):
            # New format: messages array
            for msg in existing_doc.get("messages", []):
                uid = msg.get("uid")
                folder = msg.get("folder", "INBOX")
                if uid:
                    existing_emails.add((uid, folder))
        elif existing_doc and existing_doc.get("uids"):
            # Old format: uids array (assume INBOX)
            for uid in existing_doc.get("uids", []):
                existing_emails.add((uid, "INBOX"))
        
        # Search INBOX and SENT for all emails with this thread_id
        folders_to_search = ["INBOX", "[Gmail]/Sent Mail"]
        new_emails_found = 0
        
        for search_folder in folders_to_search:
            try:
                client.select_folder(search_folder, readonly=True)
                
                # Search for emails with this thread_id (X-GM-THRID)
                search_criteria = ['X-GM-THRID', str(thread_id)]
                uids = client.search(search_criteria)
                
                
                for uid in uids:
                    # Check if this email is already in MongoDB
                    if (uid, search_folder) not in existing_emails:
                        try:
                            # Fetch and process this missing email
                            resp = client.fetch([uid], ["RFC822", "X-GM-THRID"])
                            if uid in resp:
                                raw_data = resp[uid]
                                if b"RFC822" in raw_data:
                                    raw_msg = raw_data[b"RFC822"]
                                    msg = email.message_from_bytes(raw_msg, policy=policy.default)
                                    
                                    # Store this email in MongoDB
                                    atomic_upsert_thread(mongo_col, thread_id, uid, msg, folder=search_folder)
                                    new_emails_found += 1
                        
                        except Exception as e:
                            log.warning(f"Failed to backfill UID {uid} from {search_folder}: {e}")
                            continue
            
            except Exception as e:
                log.warning(f"Failed to search folder {search_folder} for thread {thread_id}: {e}")
                continue
        
        
    except Exception as e:
        log.error(f"Thread backfill failed for {thread_id}: {e}")


def process_uid(client, mongo_col, uid, folder=MAILBOX):
    # Fetch RFC822 + X-GM-THRID, upsert into Mongo - pure IDLE processing
    original_folder = client.folder_name if hasattr(client, 'folder_name') else MAILBOX
    
    # Check if this UID is already processed for this folder
    existing = mongo_col.find_one({"messages.uid": uid, "messages.folder": folder})
    if existing:
        log.debug(f"UID {uid} already processed for folder {folder}, skipping")
        return True

    try:
        try:
            client.select_folder(folder, readonly=True)
        except Exception:
            pass

        resp = client.fetch([uid], ['BODY.PEEK[]', 'X-GM-THRID'])
    except Exception as e:
        log.exception("Failed to fetch UID %s: %s", uid, e)
        return False

    data = resp.get(uid, {})
    raw = data.get(b'RFC822') or data.get(b'BODY[]')

    if not raw:
        log.warning("UID %s has no RFC822; skipping", uid)
        return False

    try:
        msg = email.message_from_bytes(raw, policy=default)
    except Exception as e:
        log.exception("Failed to parse message for UID %s: %s", uid, e)
        return False

    raw_tid = data.get(b'X-GM-THRID')
    if raw_tid:
        try:
            thread_id = raw_tid.decode('utf-8') if isinstance(raw_tid, bytes) else str(raw_tid)
            if not thread_id.strip() or not thread_id.replace('-', '').isdigit():
                log.warning("Invalid X-GM-THRID format: %s, falling back to Message-ID", thread_id)
                thread_id = msg.get('Message-ID') or f"no-thread-{uid}"
        except (UnicodeDecodeError, AttributeError):
            log.warning("Failed to decode X-GM-THRID, falling back to Message-ID")
            thread_id = msg.get('Message-ID') or f"no-thread-{uid}"
    else:
        thread_id = msg.get('Message-ID') or f"no-thread-{uid}"

    is_sent_folder = folder.lower() in ['sent', '[gmail]/sent mail', '[google mail]/sent mail']
    if is_sent_folder:
        references = msg.get('In-Reply-To') or msg.get('References')
        if not references:
            log.info("Sent mail UID %s has no reply headers; skipping", uid)
            return False
        if not mongo_col.find_one({"thread_id": thread_id}):
            log.info("Sent mail UID %s references unknown thread; skipping", uid)
            return False

    try:
        # Backfill missing emails from thread before processing current email
        backfill_thread_emails(client, mongo_col, thread_id)
        
        atomic_upsert_thread(mongo_col, thread_id, uid, msg, folder=folder)
        
        # Fetch thread data and save to mails.json using existing IMAP client
        try:
            thread_data = integrate_with_processor_after_storage(thread_id, client)
            if thread_data:
                pass  # Thread info already logged by integration
            else:
                log.warning("Failed to fetch thread data for %s", thread_id)
        except Exception as e:
            log.warning("Thread fetcher failed for %s: %s", thread_id, e)
            
    except Exception as e:
        log.exception("Mongo upsert failed for UID %s: %s", uid, e)
        return False

    if not is_sent_folder:
        try:
            client.select_folder('[Gmail]/Sent Mail', readonly=True)
            sent_uids = client.search(['X-GM-THRID', thread_id])
        except Exception:
            sent_uids = []
        finally:
            try:
                client.select_folder(original_folder, readonly=False)
            except Exception as e:
                log.warning("Failed to restore original folder %s: %s", original_folder, e)

        if sent_uids:
            try:
                existing_doc = mongo_col.find_one(
                    {"thread_id": thread_id}, 
                    {"uids": 1}
                )
                existing_uids = set(existing_doc.get("uids", [])) if existing_doc else set()
            except Exception as e:
                log.warning("Failed to get existing UIDs: %s", e)
                existing_uids = set()

            for s_uid in sent_uids:
                s_uid_int = int(s_uid) 
                if s_uid_int in existing_uids:
                    continue

                try:
                    try:
                        client.select_folder('[Gmail]/Sent Mail', readonly=True)
                    except Exception:
                        pass
                    resp = client.fetch([s_uid], ['BODY.PEEK[]'])
                    try:
                        client.select_folder(original_folder, readonly=False)
                    except Exception:
                        pass

                    raw_sent = resp.get(s_uid, {}).get(b'RFC822') or resp.get(s_uid, {}).get(b'BODY[]')

                    if not raw_sent:
                        continue
                    msg_sent = email.message_from_bytes(raw_sent, policy=default)

                    try:
                        atomic_upsert_thread(mongo_col, thread_id, s_uid_int, msg_sent, folder='Sent')
                        
                        # Fetch thread data and save to mails.json using existing IMAP client
                        try:
                            thread_data = integrate_with_processor_after_storage(thread_id, client)
                            if thread_data:
                                pass  # Thread info already logged
                            else:
                                log.warning("Failed to fetch thread data for %s (sent)", thread_id)
                        except Exception as e:
                            log.warning("Thread fetcher failed for %s (sent): %s", thread_id, e)
                            
                    except Exception as e:
                        log.warning("Failed to upsert sent UID %s into thread %s: %s", s_uid_int, thread_id, e)

                    existing_uids.add(s_uid_int)
                except Exception as e:
                    log.warning("Failed to process sent UID %s: %s", s_uid, e)
                    try:
                        client.select_folder(original_folder, readonly=False)
                    except Exception:
                        pass

    log.info("Processed UID %s (thread %s)", uid, thread_id)
    return True
