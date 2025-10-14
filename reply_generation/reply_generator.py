"""
Reply Generation Module

This module handles generating email replies based on thread context,
intent classification, and sentiment analysis, and saves them as Gmail Drafts.
"""

import os
import sys
import base64
from datetime import datetime
from typing import Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger.logger_setup import logger as log
from Utility.llm_utils import LLMExecutor
from Utility.ml_text_utils import format_thread_for_llm
from mail_parser.auth_handler import get_gmail_service


class ReplyGenerator:
    def __init__(self, provider: str = "azure_openai"):
        """Initialize the reply generator with an LLM executor.
        
        Args:
            provider: The LLM provider to use (default: "azure_openai")
        """
        self.llm = LLMExecutor(provider=provider)
        
    def generate_reply(
        self,
        thread_data: Dict[str, Any],
        intent: str,
        sentiment_label: str,
        username: str = None,
        thread_id: str = None
    ) -> str:
        """Generate a reply for the given email thread and save as Gmail Draft.
        
        Args:
            thread_data: The thread data in the standard format
            intent: The detected intent of the email
            sentiment_label: The sentiment label (e.g., "positive", "negative")
            username: Username for Gmail API access
            thread_id: Thread ID for X-GM-THRID conversion in drafts
            
        Returns:
            The generated reply text
        """
        try:
            # Format the email thread
            formatted_thread = format_thread_for_llm(thread_data)
            
            # Prepare the prompt
            prompt = f"""Act as Professional email assistant for business communication & Draft reply to the latest email in the provided thread.

            # THREAD ORDERING
            - EMAIL_THREAD is ordered oldest to latest
            - Each message is labelled Mail1, Mail2, ... where Mail1 is the latest.
            - Always draft the reply to last mail. Use others only for context.


            # INSTRUCTIONS
            1.Read the entire thread
            2.Identify and summarize key points, direct questions and action items.
            3.Address the main point, question, or request in the latest email
            4.Refer earlier emails when relevant
            5.Tone Matching (based on SENTIMENT_LABEL = {sentiment_label}):
                Higher Negative / Negative: formal, apologetic, solution-focused
                Neutral: Professional, concise, and clear
                Positive / Higher Positive: friendly
            6.consider mail intent to tailor response appropriately: {intent}


            #Output Format:
            Produce exactly three parts, separated by a single blank line:
                1.Start with the greeting
                2.Then body (3-5 sentences)
                3.End with a single specific closing action/request line
                4.Do not add anything after closing line or sign off such as "Thanks","Best Regards"


            # INPUTS
            - EMAIL_THREAD: {formatted_thread}"""

            # Generate the reply using LLM
            result = self.llm.execute_with_retry(
                prompt=prompt,
                max_tokens=500,
                temperature=0.7
            )
            
            reply = result.get("content", "Failed to generate reply")
            
            # Create Gmail draft with proper threading
            if username:
                self._create_gmail_draft(reply, thread_data, username, thread_id)
            else:
                log.warning("No username provided, cannot create Gmail draft")
            
            return reply
            
        except Exception as e:
            log.error(f"Error generating reply: {e}")
            raise
    
    def _create_gmail_draft(self, reply: str, thread_data: Dict[str, Any], username: str, thread_id: str = None) -> None:
        """Create a Gmail draft with the generated reply and proper threading.
        
        Args:
            reply: The generated reply content
            thread_data: The complete thread data with messages
            username: Username for Gmail API access
        """
        try:
            # Get Gmail service
            service = self._get_gmail_service(username)
            if not service:
                log.error(f"Failed to get Gmail service for {username}")
                return
                
            # Get the latest message for threading
            messages = thread_data.get("messages", []) or thread_data.get("Mails", [])
            if not messages:
                log.warning("No messages found in thread data")
                return
                
            latest_message = messages[-1]  # Last message in thread
            
            # Extract threading information
            original_message_id = latest_message.get("message_id", "")
            subject = thread_data.get("subject", "Re: Email Response")
            original_from = latest_message.get("from", "")
            original_to = latest_message.get("to", "")
            thread_id = thread_data.get("thread_id", "")
            
            # Build simple thread-based reply
            draft_message = self._build_thread_reply(reply, subject)
            
            # Get Gmail thread ID for proper conversation context
            gmail_thread_id = None
            if latest_message.get("message_id"):
                try:
                    from gmail_labeling.gmail_label_manager import get_gmail_thread_id_from_message_id, get_gmail_thread_id_from_xgm_thrid
                    
                    # Try Message-ID approach first
                    gmail_thread_id = get_gmail_thread_id_from_message_id(latest_message.get("message_id"), username)
                    
                    # If Message-ID fails, try X-GM-THRID for external emails
                    if not gmail_thread_id:
                        log.info(f"Message-ID not found, trying X-GM-THRID approach for external email draft")
                        
                        # Look for X-GM-THRID in thread data or use thread_id if numeric
                        if thread_id and not '@' in str(thread_id):
                            xgm_thrid = str(thread_id)
                            log.info(f"Trying X-GM-THRID for draft threading: {xgm_thrid}")
                            gmail_thread_id = get_gmail_thread_id_from_xgm_thrid(xgm_thrid, username)
                    
                    if gmail_thread_id:
                        log.info(f"Found Gmail thread ID for draft: {gmail_thread_id}")
                    else:
                        log.info(f"Message not found in Gmail using both approaches: creating new thread")
                        
                except Exception as e:
                    log.warning(f"Could not find Gmail thread ID for draft: {e}")
                    gmail_thread_id = None
            
            # Create the draft
            draft_request = {
                'message': draft_message
            }
            
            # Add thread ID if available
            if gmail_thread_id:
                draft_request['message']['threadId'] = gmail_thread_id
                
            draft = service.users().drafts().create(
                userId='me',
                body=draft_request
            ).execute()
            
            draft_id = draft.get('id')
            log.info(f"Gmail draft created successfully for {username}: Draft ID {draft_id}")
            
        except HttpError as e:
            log.error(f"Gmail API error creating draft for {username}: {e}")
        except Exception as e:
            log.error(f"Error creating Gmail draft for {username}: {e}")
    
    def _get_gmail_service(self, username: str):
        """Get Gmail API service for the user."""
        try:
            service = get_gmail_service(username)
            return service
        except Exception as e:
            log.error(f"Error building Gmail service for {username}: {e}")
            return None
    
    def _build_thread_reply(self, reply: str, subject: str) -> Dict:
        """
        Build a simple Gmail draft message for thread-based reply.
        Gmail will automatically handle recipients and threading when using threadId.
        
        Args:
            reply: Generated reply content
            subject: Email subject (will add Re: if needed)
            
        Returns:
            Gmail message object
        """
        # Clean subject - add Re: if not present
        if not subject.lower().startswith('re:'):
            clean_subject = f"Re: {subject}"
        else:
            clean_subject = subject
        
        # Create simple message - Gmail handles recipients via threadId
        message = MIMEText(reply)
        message['subject'] = clean_subject
        
        # Convert to Gmail API format
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
        
        return {
            'raw': raw_message
        }
    
    def _extract_email_address(self, email_string: str) -> str:
        """Extract email address from 'Name <email@domain.com>' format."""
        if not email_string:
            return ""
        
        if '<' in email_string and '>' in email_string:
            return email_string.split('<')[1].split('>')[0].strip()
        elif '@' in email_string:
            return email_string.strip()
        else:
            # If no email format found, return empty string to avoid invalid header
            log.warning(f"No valid email found in: {email_string}")
            return ""


def generate_email_reply(
    thread_data: Dict[str, Any],
    intent: str,
    sentiment_label: str,
    provider: str = "azure_openai",
    username: str = None,
    thread_id: str = None
) -> str:
    """Convenience function to generate an email reply in one call.
    
    Args:
        thread_data: The thread data in the standard format
        intent: The detected intent of the email
        sentiment_label: The sentiment label (e.g., "positive", "negative")
        provider: The LLM provider to use (default: "azure_openai")
        username: Username for Gmail API access
        thread_id: Thread ID for X-GM-THRID conversion in drafts
        
    Returns:
        The generated reply text
    """
    generator = ReplyGenerator(provider=provider)
    return generator.generate_reply(
        thread_data=thread_data,
        intent=intent,
        sentiment_label=sentiment_label,
        username=username,
        thread_id=thread_id
    )


def process_thread_reply(thread_id: str, final_json: Dict[str, Any], ml_results: Dict[str, Any], username: str = None):
    """Process thread and generate reply - used by the main pipeline.
    
    Args:
        thread_id: The thread ID for logging
        final_json: The complete thread JSON data
        ml_results: ML analysis results containing intent and sentiment
        username: Username for Gmail API access
    """
    try:
        log.info("Generating email reply for thread %s", thread_id)
        
        intent = ml_results.get('intent', 'Unknown')
        sentiment = ml_results.get('sentiment', 'Neutral')
        
        reply = generate_email_reply(
            thread_data=final_json,
            intent=intent,
            sentiment_label=sentiment,
            username=username,
            thread_id=thread_id  # Pass thread_id for X-GM-THRID conversion
        )
        
        log.info("Email reply generated for thread %s and saved to Gmail Drafts", thread_id)
        final_json['generated_reply'] = reply
        
    except Exception as reply_error:
        log.error("Reply generation failed for thread %s: %s", thread_id, reply_error)
