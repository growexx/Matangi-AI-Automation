import json
import sys
import os
from typing import Dict, Any
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def convert_thread_to_json(thread_data: Dict[str, Any], thread_id: str) -> Dict[str, Any]:
    """
    Convert MongoDB thread data to final JSON structure
    
    Args:
        thread_data: Thread data from MongoDB
        thread_id: Thread ID
        
    Returns:
        Final JSON structure ready for logging/processing
    """
    all_emails = thread_data.get("Mails", [])
    
    # Show only the latest 10 emails (or fewer if thread has less than 10)
    latest_emails = all_emails[-10:] if len(all_emails) > 10 else all_emails
    
    # Renumber emails sequentially starting from 1
    renumbered_emails = []
    for i, email in enumerate(latest_emails, 1):
        email_copy = email.copy()
        email_copy["Email"] = i
        renumbered_emails.append(email_copy)
    
    final_json = {
        "thread_id": thread_id,
        "subject": thread_data.get("subject", "No Subject"),
        "total_emails_in_thread": len(all_emails),
        "emails_fetched": len(latest_emails),
        "Mails": renumbered_emails
    }

    log.info("Final JSON: %s", json.dumps(final_json, indent=2))
    
    return final_json
