#!/usr/bin/env python3
"""
JSON Conversion Module
Handles converting MongoDB thread data to JSON format and objects
"""

import json
import sys
import os
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger.logger_setup import logger as log
from Utility.email_thread_object import create_email_thread_object, show_thread_object


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


def convert_json_to_object(final_json: Dict[str, Any]) -> None:
    """
    Convert JSON structure to object and log
    
    Args:
        final_json: JSON structure to convert
    """

    thread_obj = create_email_thread_object(final_json)
