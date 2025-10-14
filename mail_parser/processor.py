import os
import sys
import email
import datetime
from email.policy import default
from config import *
from logger.logger_setup import logger as log, get_user_logger
from Utility.mongo_utils import atomic_upsert_thread, mongo_connect, get_thread_from_mongo, fetch_thread_emails_from_imap
from Utility.json_converter import convert_thread_to_json
from ml_pipeline import process_thread
from gmail_labeling.gmail_label_manager import apply_ml_labels
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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
        
        # Search in INBOX and Sent folders separately to get correct folder mapping
        uid_folder_map = {}  # Map UID to its actual folder
        folders_to_check = [MAILBOX, '[Gmail]/Sent Mail']
        
        # For Message-ID based threads, get X-GM-THRID from the email first
        gmail_thrid = None
        if '@' in thread_id:  # Message-ID
            try:
                # Find this email in INBOX to get its X-GM-THRID
                client.select_folder(MAILBOX, readonly=True)
                clean_message_id = thread_id.strip('<>')
                specific_uids = client.search(['HEADER', 'Message-ID', clean_message_id])
                
                if specific_uids:
                    uid = specific_uids[0]
                    data = client.fetch([uid], ['X-GM-THRID'])
                    raw_thrid = data.get(uid, {}).get(b'X-GM-THRID')
                    
                    if raw_thrid:
                        gmail_thrid = raw_thrid.decode('utf-8') if isinstance(raw_thrid, bytes) else str(raw_thrid)

            except Exception as e:
                log.debug(f"Could not get X-GM-THRID from Message-ID: {e}")
        
        # Now search ALL folders using the X-GM-THRID (finds all emails in conversation)
        for folder in folders_to_check:
            try:
                client.select_folder(folder, readonly=True)
                
                # If we have X-GM-THRID, use it (finds ALL related emails including sent)
                if gmail_thrid:
                    thread_uids = client.search(['X-GM-THRID', gmail_thrid])
                    # Found emails in folder with X-GM-THRID
                elif '@' in thread_id:
                    # Fallback: search by Message-ID only (won't find sent mail)
                    clean_thread_id = thread_id.strip('<>')
                    thread_uids = client.search(['HEADER', 'Message-ID', clean_thread_id])
                    log.debug(f"Searching folder {folder} by Message-ID (fallback): {clean_thread_id}")
                else:
                    # Direct X-GM-THRID search for legacy threads
                    thread_uids = client.search(['X-GM-THRID', thread_id])
                    log.debug(f"Searching folder {folder} by X-GM-THRID: {thread_id}")
                    
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
            log.info(f"Backfilling {len(missing_uids)} missing emails for thread {gmail_thrid or thread_id}")
        
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

def process_uid(client, mongo_col, uid, folder=MAILBOX, username=None):
    """Fetch RFC822 + X-GM-THRID, upsert into Mongo - multi-tenant processing with UID tracking."""
    original_folder = client.folder_name if hasattr(client, 'folder_name') else MAILBOX
    
    # Get user ID for multi-tenant support
    user_id = None
    if username:
        from Utility.user_manager import user_manager
        user_id = user_manager.get_user_id_by_username(username)
        if not user_id:
            log.error(f"User ID not found for {username}")
            return False
    
    # Check if this is a new email (INBOX only) - pass username for multi-tenant support
    if folder == MAILBOX:
        from Utility.user_manager import user_manager
        last_processed = user_manager.get_last_processed_uid(username)
        if last_processed and uid <= last_processed:
            log.debug("UID %s already processed for user %s, skipping", uid, username or 'legacy')
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

    # NEW APPROACH: Always use Message-ID as primary identifier
    # Let Gmail API determine the actual thread ID later
    message_id = msg.get('Message-ID')
    
    # Also capture X-GM-THRID for labeling external emails
    xgm_thrid = None
    raw_tid = data.get(b'X-GM-THRID')
    if raw_tid:
        try:
            xgm_thrid = raw_tid.decode('utf-8') if isinstance(raw_tid, bytes) else str(raw_tid)
            log.debug(f"Processing UID: {uid}")
        except (UnicodeDecodeError, AttributeError):
            xgm_thrid = None
    
    if message_id:
        thread_id = message_id  # Use Message-ID as thread identifier
        # Use Message-ID as thread identifier
    else:
        # Fallback if no Message-ID exists
        if xgm_thrid:
            thread_id = xgm_thrid
            log.debug(f"Fallback to X-GM-THRID: {thread_id}")
        else:
            thread_id = f"no-thread-{uid}"
            log.warning(f"No thread identifiers found, using: {thread_id}")

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
        # First, add the current email to MongoDB with user_id for multi-tenant support
        if user_id:
            # For multi-tenant mode, we need to modify the atomic_upsert_thread to include user_id
            # This ensures data integrity between users
            from Utility.mongo_utils import atomic_upsert_thread_with_user_id
            atomic_upsert_thread_with_user_id(mongo_col, thread_id, uid, msg, folder=folder, user_id=user_id)
        else:
            # Legacy fallback
            atomic_upsert_thread(mongo_col, thread_id, uid, msg, folder=folder)
        
        # Backfill: find ALL emails in this thread and add missing ones
        try:
            backfilled_count = backfill_thread_emails(client, mongo_col, thread_id, current_uid=uid)
        except Exception as e:
            log.warning("Backfill failed for thread %s: %s", thread_id, e)
        
        # Fetch thread emails directly from IMAP for ML processing 
        try:

            thread_data = fetch_thread_emails_from_imap(client, thread_id, limit=10)
            if thread_data:
                # Convert to JSON and log the structure
                final_json = convert_thread_to_json(thread_data, thread_id)
                # Prepare thread data for ML processing
                
                # Process through ML pipeline for classification
                try:
                    user_log = get_user_logger(username) if username else log
                    ml_results = process_thread(final_json)
                    user_log.info("ML Results: Intent=%s, Sentiment=%s", 
                            ml_results.get('intent'), ml_results.get('sentiment'))
                    
                    # Store ML results in the final JSON for future use
                    final_json['ml_results'] = ml_results
                    
                    # Apply Gmail labels to the latest email in the thread
                    try:
                        if username:  # Only apply labels if we have a username
                            from gmail_labeling.gmail_label_manager import search_and_tag_message
                            subject = final_json.get('subject', 'No Subject')
                            current_message_id = msg.get('Message-ID')
                            if current_message_id:
                                intent = ml_results.get('intent', 'Unclassified')
                                sentiment = ml_results.get('sentiment', 'Neutral')
                                success = search_and_tag_message(
                                    message_id=current_message_id,
                                    intent=intent,
                                    sentiment=sentiment,
                                    username=username,
                                    xgm_thrid=xgm_thrid  # Pass X-GM-THRID for external emails
                                )
                                # Gmail labels applied
                            else:
                                user_log.warning("No Message-ID found for Gmail labeling")
                        else:
                            log.debug("No username provided - skipping Gmail labeling")
                    except Exception as label_error:
                        user_log.error("Gmail labeling failed for thread %s: %s", thread_id, label_error)
                    
                    # Generate reply after successful ML processing and labeling
                    from reply_generation import process_thread_reply
                    process_thread_reply(thread_id, final_json, ml_results, username)
                    
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


    # UID tracking is now handled by user_manager in imap_monitor.py
    # No need to update here as it's already done after successful processing
    return True
