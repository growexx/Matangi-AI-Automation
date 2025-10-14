import os
import sys
import json
from typing import Dict, Any, Optional
from Utility.llm_utils import LLMExecutor
from Utility.ml_text_utils import format_thread_for_llm
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def analyze_sentiment(thread_data: Dict[str, Any], provider: str = "azure_openai") -> Dict[str, Any]:
    """
    Analyze the sentiment of the most recent email in a thread.
    
    Args:
        thread_data: The email thread data in JSON format
        provider: The LLM provider to use ("azure_openai" or "aws_bedrock")
        
    Returns:
        Dictionary with the detected sentiment and confidence
    """
    try:
        # Format the thread data for LLM processing (use last 10 emails)
        formatted_thread = format_thread_for_llm(thread_data, max_emails=10)
        
        # Get the most recent email content
        emails = thread_data.get("Mails", [])
        if not emails:
            log.warning("No emails found in thread data")
            return {"sentiment": "Neutral", "confidence": 0.0, "error": "No emails found"}
        
        current_email = emails[-1].get("body", "").strip()
        if not current_email:
            log.warning("Most recent email has no body content")
            return {"sentiment": "Neutral", "confidence": 0.0, "error": "Empty email body"}
        
        # Get subject for better context
        subject = thread_data.get("subject", "No Subject")
        
        # Create the prompt
        prompt = f"""
        Act as an email sentiment analyzer. Analyze the sentiment of the most recent email in this thread.
        
        Thread Subject: "{subject}"
        
        Classify the sentiment into EXACTLY ONE of these categories:
        - Higher Positive: The email expresses strong satisfaction, enthusiasm, or very positive emotions
        - Positive: The email expresses moderate satisfaction, gratitude, or positive emotions
        - Neutral: The email is factual, informational, or doesn't express strong emotions
        - Negative: The email expresses moderate dissatisfaction, frustration, or negative emotions
        - Higher Negative: The email expresses strong dissatisfaction, anger, or very negative emotions
        
        IMPORTANT INSTRUCTIONS:
        - You MUST classify the email into EXACTLY ONE of the categories listed above
        - You MUST return ONLY the category name, nothing else
        - If uncertain, choose the most appropriate category based on the email content
        - "Email 1" is the oldest email
        - The email with the highest number is the latest email
        - Focus on analyzing the sentiment of ONLY the most recent email
        - Consider the context from previous emails AND the subject line if relevant
        - Look for emotional tone, urgency indicators, and satisfaction/dissatisfaction signals


        CRITICAL: 
        -Focus ONLY on the most recent email with folder="INBOX" (received email)
        - Ignore emails from "[Gmail]/Sent Mail" folder for sentiment analysis 
        - Use Sent Mail emails ONLY as context to understand the conversation flow
        - Complaint override rule: If the most recent INBOX email contains a complaint: even if written in an polite/neutral/factual tone - classify it as Negative or Higher Negative 

        
        {formatted_thread}
        
        Your classification (return ONLY ONE category name from the list above):
        """
        
        # Execute LLM call
        executor = LLMExecutor(provider=provider)
        result = executor.execute_with_retry(
            prompt=prompt,
            temperature=0.3,  # Lower temperature for more deterministic classification
            max_tokens=50     # We only need a short response
        )
        
        # Extract the sentiment from the response
        content = result.get("content", "").strip()
        
        # Clean up the response - we want just the category name
        # Remove any quotes, periods, or extra text
        sentiment = content.strip('"').strip("'").strip('.').strip()
        
        # Validate that the sentiment is one of our expected categories
        valid_sentiments = ["Higher Positive", "Positive", "Neutral", "Negative", "Higher Negative"]
        
        if sentiment not in valid_sentiments:
            log.warning(f"LLM returned unexpected sentiment: {sentiment}")
            # Try to find the closest match in our valid sentiments
            for valid_sentiment in valid_sentiments:
                if valid_sentiment.lower() in sentiment.lower():
                    sentiment = valid_sentiment
                    break
            else:
                # If no match found, log the error but don't default to any category
                log.error(f"LLM returned invalid sentiment that couldn't be matched: {content}")
                # Return the raw response for manual review
                return {"sentiment": "Invalid Response", "error": f"LLM returned invalid category: {content}", "raw_response": content}
        
        return {
            "sentiment": sentiment,
            "confidence": 0.9,  # Hard-coded confidence since LLM doesn't provide it
            "raw_response": content
        }
        
    except Exception as e:
        log.error(f"Error analyzing sentiment: {e}")
        return {"sentiment": "Neutral", "confidence": 0.0, "error": str(e)}


def get_sentiment_label(sentiment: str) -> str:
    """
    Convert sentiment category to a Gmail label name.
    
    Args:
        sentiment: The detected sentiment category
        
    Returns:
        Gmail label name
    """
    # Import from label config for consistency
    try:
        from gmail_labeling.label_config import get_sentiment_label as get_clean_sentiment_label
        return get_clean_sentiment_label(sentiment)
    except ImportError:
        # Fallback if import fails
        sentiment_label_map = {
            "Higher Positive": "Higher-Positive",
            "Positive": "Positive",
            "Neutral": "Neutral", 
            "Negative": "Negative",
            "Higher Negative": "Higher-Negative"
        }
        return sentiment_label_map.get(sentiment, "Neutral")