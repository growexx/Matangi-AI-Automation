# ML pipeline integration for email threads

import json
import sys
import os
from typing import Optional, Dict, Any
from thread_processor.thread_fetcher import ThreadFetcher
from thread_processor.email_thread_object import create_email_thread_object, show_thread_object
from logger.logger_setup import logger


def integrate_with_processor_after_storage(thread_id: str, imap_client=None):

    try:
        # Get JSON from thread fetcher 
        thread_fetcher = ThreadFetcher(save_to_json=False, shared_imap_client=imap_client)
        thread_data = thread_fetcher.build_thread_json(thread_id, limit=5)
        
        if not thread_data:
            logger.error(f"Failed to fetch thread data for {thread_id}")
            return None
        
        # Direct JSON→Object conversion
        thread_obj = create_email_thread_object(thread_data)
        
        logger.debug(f"Thread {thread_id} converted to object")
        show_thread_object(thread_obj)
        

        return thread_obj
        
    except Exception as e:
        logger.exception(f"Error processing thread {thread_id} after storage: {e}")
        return None


