import os
import sys
from typing import Dict, List, Any, Optional
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def format_thread_for_llm(thread_data: Dict[str, Any], max_emails: int = 10) -> str:
    """
    Format thread data for LLM processing, using only the most recent emails.
    
    Args:
        thread_data: The thread data JSON/object with the structure:
            {
                "thread_id": "unique_thread_id",
                "subject": "Email Subject",
                "total_emails_in_thread": N,
                "emails_fetched": M,
                "Mails": [
                    {
                        "Email": 1,
                        "date": "Thu, 23 Sep 2025 12:34:56 +0000",
                        "from": "Sender Name",
                        "folder": "INBOX",
                        "body": "Email body text..."
                    },
                    ...
                ]
            }
        max_emails: Maximum number of emails to include (most recent ones)
        
    Returns:
        Formatted text ready for LLM input
    """
    try:
        # Extract subject
        subject = thread_data.get("subject", "No Subject")
        formatted_text = f"Subject: {subject}\n\n"
    
        # Get emails
        emails = thread_data.get("Mails", [])
        total_emails = len(emails)
        
        if not emails:
            log.warning("No emails found in thread data")
            return f"Subject: {subject}\n\nNo email content available."
        
        # Sort emails by date
        # The emails should already be in chronological order based on the thread_fetcher.py code
        
        # Take only the most recent max_emails
        if total_emails > max_emails:
            emails = emails[-max_emails:]
            formatted_text += f"Note: This is a thread with {total_emails} emails. Showing only the {max_emails} most recent.\n\n"
        
        # Format each email
        for i, email in enumerate(emails):
            email_num = email.get("Email", i+1)
            sender = email.get("from", "Unknown")
            date = email.get("date", "")
            body = email.get("body", "").strip()
            
            formatted_text += f"Email {email_num} from {sender} on {date}:\n{body}\n\n"
        
        return formatted_text
        
    except Exception as e:
        log.error(f"Error formatting thread for LLM: {e}")
        return "Error formatting email thread data."