
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from types import SimpleNamespace
import sys
import os
from logger.logger_setup import logger


def create_email_thread_object(thread_data: Dict[str, Any]) -> SimpleNamespace:
    
    # nested Mails list to SimpleNamespace objects
    if 'Mails' in thread_data:
        thread_data = thread_data.copy()  
        thread_data['Mails'] = [SimpleNamespace(**mail) for mail in thread_data['Mails']]
    
    return SimpleNamespace(**thread_data)


def show_thread_object(thread_obj: SimpleNamespace):
    logger.info(f"Thread {thread_obj.thread_id} - {thread_obj.subject} ({len(thread_obj.Mails)} emails):")
    for mail in thread_obj.Mails:
        sender = getattr(mail, 'from')
        logger.info(f"  [{mail.Email}] {sender}: {mail.body}")

