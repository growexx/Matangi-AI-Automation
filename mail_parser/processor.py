import os
import sys
import email
import datetime
from email.policy import default

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from logger.logger_setup import logger as log
from Utility.mongo_utils import atomic_upsert_thread, mongo_connect, get_thread_from_mongo, fetch_thread_emails_from_imap
from Utility.uid_tracker import uid_tracker
from Utility.json_converter import convert_thread_to_json
from ml_pipeline import process_thread
from gmail_labeling.gmail_label_manager import apply_ml_labels

def backfill_thread_emails(client, mongo_col, thread_id, current_uid=None):
    """
    Backfill missing emails in a thread by searching IMAP for all emails 
    with the same X-GM-THRID and adding missing ones to MongoDB.
    
    Args:
        client: IMAP client connection
        mongo_col: MongoDB collection
        thread_id: Thread ID to backfill
        current_uid: UID of the current email being processed (to avoid double-processing)
    
    Returns:
        Number of emails backfilled
    """
    backfilled_count = 0
    original_folder = client.folder_name if hasattr(client, 'folder_name') else MAILBOX
    
    try:
        # Get existing UIDs from MongoDB for this thread
        existing_thread = mongo_col.find_one({"thread_id": thread_id})
        existing_uids = set(existing_thread.get("uids", [])) if existing_thread else set()
        log.debug("Thread %s has %d existing emails in MongoDB", thread_id, len(existing_uids))
        
        # Search in INBOX and Sent folders separately to get correct folder mapping
        uid_folder_map = {}  # Map UID to its actual folder
        folders_to_check = [MAILBOX, '[Gmail]/Sent Mail']
        
        for folder in folders_to_check:
            try:
                client.select_folder(folder, readonly=True)
                thread_uids = client.search(['X-GM-THRID', thread_id])
                for uid in thread_uids:
                    uid_folder_map[uid] = folder
            except Exception as e:
                log.debug("Could not search folder %s: %s", folder, e)
                continue
        
        # Find missing UIDs (those not in MongoDB)
        all_thread_uids = set(uid_folder_map.keys())
        missing_uids = all_thread_uids - existing_uids
        if current_uid:
            missing_uids.discard(int(current_uid))  # Remove current email as it's already processed
        
        if missing_uids:
            log.info("Backfilling %d missing emails for thread %s", len(missing_uids), thread_id)
        
        # Backfill missing emails using correct folder mapping
        for missing_uid in missing_uids:
            try:
                # Use the folder where we found this UID
                email_folder = uid_folder_map.get(missing_uid, 'INBOX')
                client.select_folder(email_folder, readonly=True)
                resp = client.fetch([missing_uid], ['BODY.PEEK[]'])
                data = resp.get(missing_uid, {})
                raw = data.get(b'RFC822') or data.get(b'BODY[]')
                
                if raw:
                    import email
                    from email.policy import default
                    msg = email.message_from_bytes(raw, policy=default)
                    
                    atomic_upsert_thread(mongo_col, thread_id, missing_uid, msg, folder=email_folder)
                    backfilled_count += 1
                else:
                    log.warning("No content found for UID %s", missing_uid)
                    
            except Exception as e:
                log.error("Failed to backfill UID %s: %s", missing_uid, e)
                continue
        
    except Exception as e:
        log.error("Backfill process failed for thread %s: %s", thread_id, e)
    finally:
        # Restore original folder
        try:
            client.select_folder(original_folder, readonly=False)
        except Exception:
            pass
    
    return backfilled_count

def process_uid(client, mongo_col, uid, folder=MAILBOX):
    """Fetch RFC822 + X-GM-THRID, upsert into Mongo - pure IDLE processing with UID tracking."""
    original_folder = client.folder_name if hasattr(client, 'folder_name') else MAILBOX
    
    # Check if this is a new email (INBOX only)
    if folder == MAILBOX and not uid_tracker.is_new_email(uid):
        log.debug("UID %s already processed, skipping", uid)
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
        # First, add the current email to MongoDB
        atomic_upsert_thread(mongo_col, thread_id, uid, msg, folder=folder)
        
        # Backfill: find ALL emails in this thread and add missing ones
        try:
            backfilled_count = backfill_thread_emails(client, mongo_col, thread_id, current_uid=uid)
        except Exception as e:
            log.warning("Backfill failed for thread %s: %s", thread_id, e)
        
        # Fetch thread emails directly from IMAP for ML processing (secure approach)
        try:
            log.info("Fetching latest emails from IMAP for ML processing (thread %s)", thread_id)
            thread_data = fetch_thread_emails_from_imap(client, thread_id, limit=10)
            if thread_data:
                # Convert to JSON and log the structure
                final_json = convert_thread_to_json(thread_data, thread_id)
                log.info("Thread JSON prepared for ML pipeline: %s emails fetched", len(final_json.get('Mails', [])))
                
                # Process through ML pipeline for classification
                try:
                    log.info("Running ML pipeline for thread %s", thread_id)
                    ml_results = process_thread(final_json)
                    log.info("ML Pipeline Results for thread %s: Intent=%s, Sentiment=%s, Labels=%s", 
                            thread_id, ml_results.get('intent'), ml_results.get('sentiment'), 
                            ml_results.get('gmail_labels'))
                    
                    # Store ML results in the final JSON for future use
                    final_json['ml_results'] = ml_results
                    
                    # Apply Gmail labels to the latest email in the thread
                    try:
                        from gmail_labeling.gmail_label_manager import GmailLabelManager
                        subject = final_json.get('subject', 'No Subject')
                        current_message_id = msg.get('Message-ID')
                        if current_message_id:
                            label_manager = GmailLabelManager()
                            gmail_info = label_manager.search_gmail_message_by_id(current_message_id)
                            if gmail_info:
                                gmail_thread_id = gmail_info.get('gmail_thread_id')
                                log.debug(f"Found Gmail thread {gmail_thread_id} for message {current_message_id}")
                                
                                # Get existing labels and remove old custom labels
                                existing_labels = label_manager.get_existing_thread_labels(gmail_thread_id)
                                old_custom_labels = existing_labels.get('custom_labels', [])
                                if old_custom_labels:
                                    log.debug(f"Removing {len(old_custom_labels)} old custom labels from thread {gmail_thread_id}")
                                    label_manager.remove_custom_labels_from_thread(gmail_thread_id, old_custom_labels)
                                
                                # Apply new labels
                                new_labels = ml_results.get('gmail_labels', [])
                                apply_success = label_manager.apply_labels_to_thread(gmail_thread_id, new_labels)
                                if apply_success:
                                    log.info(f"Successfully applied Gmail labels {new_labels} to thread {gmail_thread_id}")
                                else:
                                    log.error(f"Failed to apply new labels to thread {gmail_thread_id}")
                            else:
                                log.warning(f"Gmail message not found for Message-ID {current_message_id}")
                        else:
                            log.warning("No Message-ID found for Gmail labeling")
                    except Exception as label_error:
                        log.error("Gmail labeling failed for thread %s: %s", thread_id, label_error)
                    
                except Exception as ml_error:
                    log.error("ML pipeline failed for thread %s: %s", thread_id, ml_error)
                    # Continue processing even if ML fails
                    final_json['ml_results'] = {
                        "error": str(ml_error),
                        "intent": "Unknown",
                        "sentiment": "Neutral"
                    }
                
                # Thread processed successfully
                
            else:
                log.warning("Failed to fetch thread data from MongoDB for %s", thread_id)
        except Exception as e:
            log.warning("MongoDB thread fetch failed for %s: %s", thread_id, e)
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
                        
                        # Fetch latest emails directly from IMAP for ML processing (sent emails)
                        try:
                            log.info("Fetching latest emails from IMAP for ML processing (sent email thread %s)", thread_id)
                            thread_data = fetch_thread_emails_from_imap(client, thread_id, limit=10)
                            if thread_data:
                                # Convert to JSON and process through ML pipeline for sent emails too
                                try:
                                    final_json = convert_thread_to_json(thread_data, thread_id)
                                    log.info("Running ML pipeline for thread %s (sent email)", thread_id)
                                    ml_results = process_thread(final_json)
                                    log.info("ML Pipeline Results for thread %s (sent): Intent=%s, Sentiment=%s, Labels=%s", 
                                            thread_id, ml_results.get('intent'), ml_results.get('sentiment'), 
                                            ml_results.get('gmail_labels'))
                                    
                                    # Apply Gmail labels for sent emails too
                                    try:
                                        log.info("Applying Gmail labels for thread %s (sent email)", thread_id)
                                        subject = final_json.get('subject', 'No Subject')
                                        label_success = apply_ml_labels(thread_id, ml_results, subject)
                                        if label_success:
                                            log.info("Successfully applied Gmail labels %s to thread %s (sent)", 
                                                    ml_results.get('gmail_labels'), thread_id)
                                    except Exception as label_error:
                                        log.error("Gmail labeling failed for sent email thread %s: %s", thread_id, label_error)
                                        
                                except Exception as ml_error:
                                    log.error("ML pipeline failed for sent email thread %s: %s", thread_id, ml_error)
                            else:
                                log.warning("Failed to fetch thread data from MongoDB for %s (sent)", thread_id)
                        except Exception as e:
                            log.warning("MongoDB thread fetch failed for %s (sent): %s", thread_id, e)
                            
                    except Exception as e:
                        log.warning("Failed to upsert sent UID %s into thread %s: %s", s_uid_int, thread_id, e)

                    existing_uids.add(s_uid_int)
                except Exception as e:
                    log.warning("Failed to process sent UID %s: %s", s_uid, e)
                    try:
                        client.select_folder(original_folder, readonly=False)
                    except Exception:
                        pass

    # Update UID tracker (INBOX only)
    if folder == MAILBOX:
        try:
            uid_tracker.set_last_processed_uid(uid)
            log.debug("Updated last processed UID to: %s", uid)
        except Exception as e:
            log.warning("Failed to update UID tracker: %s", e)

    return True
