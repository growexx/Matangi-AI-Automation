import datetime
from email.header import decode_header, make_header
from pymongo import MongoClient
import config
from logger.logger_setup import logger as log

def mongo_connect():
    """Connect to MongoDB."""
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    return db[config.MONGO_COL]

def _message_to_doc(uid, msg, folder):
    """Convert email.message to a JSON-serializable dict for Mongo."""
    date_hdr = msg.get('Date')
    dt = date_hdr if date_hdr else datetime.datetime.now()

    doc = {
        "uid": int(uid),
        "date": dt,
        "folder": folder,
        "message_id": msg.get('Message-ID', ''),
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
