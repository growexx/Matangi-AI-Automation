import os
import sys
import time
import json
import concurrent.futures
from typing import Dict, Any, Optional, Tuple

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ml.intent_detection import detect_intent, get_intent_label
from ml.sentiment_analysis import analyze_sentiment, get_sentiment_label
from logger.logger_setup import logger as log

def process_thread(thread_data: Dict[str, Any], provider: str = "azure_openai") -> Dict[str, Any]:
    """
    Process an email thread through the ML pipeline.
    
    This function runs intent detection and sentiment analysis in parallel
    and returns the combined results.
    
    Args:
        thread_data: The email thread data in JSON format
        provider: The LLM provider to use ("azure_openai" or "aws_bedrock")
        
    Returns:
        Dictionary with the analysis results
    """
    start_time = time.time()
    log.info(f"Processing thread {thread_data.get('thread_id', 'unknown')}")
    
    # Run intent detection and sentiment analysis in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both tasks
        intent_future = executor.submit(detect_intent, thread_data, provider)
        sentiment_future = executor.submit(analyze_sentiment, thread_data, provider)
        
        # Wait for both to complete
        intent_result = intent_future.result()
        sentiment_result = sentiment_future.result()
    
    # Extract the results
    intent = intent_result.get("intent", "Unknown")
    sentiment = sentiment_result.get("sentiment", "Neutral")
    
    # Check for errors
    has_error = False
    error_messages = []
    
    if "error" in intent_result:
        has_error = True
        error_messages.append(f"Intent error: {intent_result['error']}")
    
    if "error" in sentiment_result:
        has_error = True
        error_messages.append(f"Sentiment error: {sentiment_result['error']}")
    
    # Get Gmail labels
    intent_label = get_intent_label(intent)
    sentiment_label = get_sentiment_label(sentiment)
    
    # Combine results
    result = {
        "thread_id": thread_data.get("thread_id", ""),
        "subject": thread_data.get("subject", ""),
        "intent": intent,
        "sentiment": sentiment,
        "gmail_labels": [intent_label, sentiment_label],
        "has_error": has_error,
        "error_messages": error_messages if has_error else [],
        "processing_time": time.time() - start_time
    }
    
    log.info(f"Thread {result['thread_id']} analyzed: Intent={intent}, Sentiment={sentiment}, Time={result['processing_time']:.2f}s")
    return result