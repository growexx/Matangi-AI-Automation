import json
import sys
import os
from types import SimpleNamespace
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger.logger_setup import logger as log
from Utility.email_thread_object import create_email_thread_object


def convert_thread_to_json(thread_data: Dict[str, Any], thread_id: str) -> Dict[str, Any]:
    """
    Convert MongoDB thread data to final JSON structure
    
    Args:
        thread_data: Thread data from MongoDB
        thread_id: Thread ID
        
    Returns:
        Final JSON structure ready for logging/processing
    """
    final_json = {
        "thread_id": thread_id,
        "subject": thread_data.get("subject", "No Subject"),
        "Mails": thread_data["Mails"]
    }

    log.info("Final JSON: %s", json.dumps(final_json, indent=2))
    
    return final_json


def convert_json_to_object(final_json: Dict[str, Any]) -> SimpleNamespace:
    """
    Convert JSON structure to object
    
    Args:
        final_json: JSON structure to convert
        
    Returns:
        SimpleNamespace object representing the thread
    """
    thread_obj = create_email_thread_object(final_json)
    log.info("Created thread object for thread ID: %s", final_json.get('thread_id'))
    return thread_obj
