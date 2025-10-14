"""
Reply Generation Module

This module handles generating email replies based on thread context,
intent classification, and sentiment analysis.
"""

import os
import sys
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
from Utility.llm_utils import LLMExecutor
from Utility.ml_text_utils import format_thread_for_llm
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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
        output_dir: str = "generated_replies"
    ) -> str:
        """Generate a reply for the given email thread.
        
        Args:
            thread_data: The thread data in the standard format
            intent: The detected intent of the email
            sentiment_label: The sentiment label (e.g., "positive", "negative")
            output_dir: Directory to save the generated reply (default: "generated_replies")
            
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
            
            # Save the reply to a file
            self._save_reply(reply, thread_data.get("thread_id", "unknown"), output_dir)
            
            return reply
            
        except Exception as e:
            log.error(f"Error generating reply: {e}")
            raise
    
    def _save_reply(self, reply: str, thread_id: str, output_dir: str) -> str:
        """Save the generated reply to a file.
        
        Args:
            reply: The generated reply text
            thread_id: The thread ID for the email
            output_dir: Directory to save the reply
            
        Returns:
            Path to the saved file
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timestamp and thread ID
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"draft_{timestamp}_{thread_id[:8]}.txt"
            filepath = os.path.join(output_dir, filename)
            
            # Write the reply to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(reply)
                
            log.info(f"Reply saved to {filepath}")
            return filepath
            
        except Exception as e:
            log.error(f"Error saving reply to file: {e}")
            raise

def generate_email_reply(
    thread_data: Dict[str, Any],
    intent: str,
    sentiment_label: str,
    output_dir: str = "generated_replies",
    provider: str = "azure_openai"
) -> str:
    """Convenience function to generate an email reply in one call.
    
    Args:
        thread_data: The thread data in the standard format
        intent: The detected intent of the email
        sentiment_label: The sentiment label (e.g., "positive", "negative")
        output_dir: Directory to save the generated reply (default: "generated_replies")
        provider: The LLM provider to use (default: "azure_openai")
        
    Returns:
        The generated reply text
    """
    generator = ReplyGenerator(provider=provider)
    return generator.generate_reply(thread_data, intent, sentiment_label, output_dir)

def process_reply_generation(thread_id: str, final_json: Dict[str, Any], ml_results: Dict[str, Any]) -> None:
    """Process reply generation for a thread with proper error handling and logging.
    
    Args:
        thread_id: The thread ID for logging
        final_json: The complete thread JSON data
        ml_results: ML analysis results containing intent and sentiment
    """
    try:
        log.info("Generating email reply for thread %s", thread_id)
        
        intent = ml_results.get('intent', 'Unknown')
        sentiment = ml_results.get('sentiment', 'Neutral')
        
        reply = generate_email_reply(
            thread_data=final_json,
            intent=intent,
            sentiment_label=sentiment,
            output_dir="generated_replies"
        )
        
        log.info("Email reply generated for thread %s and saved to generated_replies/", thread_id)
        final_json['generated_reply'] = reply
        
    except Exception as reply_error:
        log.error("Reply generation failed for thread %s: %s", thread_id, reply_error)
