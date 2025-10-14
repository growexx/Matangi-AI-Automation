import os
import sys
import json
from typing import Dict, Any, Optional
from Utility.llm_utils import LLMExecutor
from Utility.ml_text_utils import format_thread_for_llm
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def detect_intent(thread_data: Dict[str, Any], provider: str = "azure_openai") -> Dict[str, Any]:
    """
    Detect the intent of the most recent email in a thread.
    
    Args:
        thread_data: The email thread data in JSON format
        provider: The LLM provider to use ("azure_openai" or "aws_bedrock")
        
    Returns:
        Dictionary with the detected intent and confidence
    """
    try:
        # Format the thread data for LLM processing (use last 10 emails)
        formatted_thread = format_thread_for_llm(thread_data, max_emails=10)
        
        # Extract customer category if available (default to "Customer")
        # This would need to be determined elsewhere in your system
        customer_category = "Customer"  # Default value
        
        # Get the most recent email content
        emails = thread_data.get("Mails", [])
        if not emails:
            log.warning("No emails found in thread data")
            return {"intent": "Unknown", "confidence": 0.0, "error": "No emails found"}
        
        current_email = emails[-1].get("body", "").strip()
        if not current_email:
            log.warning("Most recent email has no body content")
            return {"intent": "Unknown", "confidence": 0.0, "error": "Empty email body"}
        
        # Get subject 
        subject = thread_data.get("subject", "No Subject")
        
        # Create the prompt
        prompt = f"""
        Act as an email Topic classifier. Classify to below categories:
        Status, Complaint, Inquiry, Pricing Negotiation, Proposal, Logistics, Acknowledgement, Status of Inquiry
        
        Thread Subject: "{subject}"
        
        Description of each category:
        - Status: Order Inquiry, Delivery Status, any Document Request like MSDS/CoA/Spec/ for shipping, Test Reports, Compliance Certificates
        - Complaint: Quality Complaint, Delivery Delay, Response Delay
        - Inquiry: Product Info Request, Sample Request, General Inquiry, Quotation
        - Pricing Negotiation: Price Discussion, Counter-Offer, Discount/Price Discussion, Payment Terms, Volume Pricing
        - Proposal: Proposal Submission, Draft Contract Review, Commercial Terms Clarification, Final Contract Review, Signature Request
        - Logistics: Dispatch Coordination, Shipping Request, Route/Transport Issues
        - Acknowledgement: Receipt confirmations, polite thank-yous, document update confirmations, simple one-line acknowledgements
        - Status of Inquiry: Follow-ups asking for progress on a prior request or inquiry
        
        IMPORTANT INSTRUCTIONS:
        - You MUST classify the email into EXACTLY ONE of the categories listed above
        - You MUST return ONLY the category name, nothing else
        - If uncertain, choose the most appropriate category based on the email content
        - "Email 1" is the oldest email
        - The email with the highest number is the latest email
        - Classify emails in chronological order: start with Email 1,2 and so on
        - While classifying also consider previous email context AND the subject line
        - Use the subject line as additional context for better classification accuracy
        
        CRITICAL RULE:
        - Emails from prospects or suspects must NOT be classified as Status.
        
        Examples:
            Request to complete PRQ and provide product flowchart → Inquiry
            Request for updated statement → Status
            Request to review and confirm Purchase Order → Inquiry
            Request for Port pricing → Inquiry
            Request for INR quotes and bid details for approval process → Pricing Negotiation
            Customer confirms payment timing → Pricing Negotiation
            Clarifies who is responsible for payment → Pricing Negotiation
            Simple thank you email → Acknowledgement
            Following up on previous request → Status of Inquiry
     
        {formatted_thread}
        Email: "{current_email}"
        Customer Category: "{customer_category}"  # can be Prospect, Customer, Suspect
        
        Your classification (return ONLY ONE category name from the list above):
        
        """
        
        # Execute LLM call
        executor = LLMExecutor(provider=provider)
        result = executor.execute_with_retry(
            prompt=prompt,
            temperature=0.3,  # Lower temperature for more deterministic classification
            max_tokens=50     # We only need a short response
        )
        
        # Extract the intent from the response
        content = result.get("content", "").strip()
        
        # Clean up the response - we want just the category name
        # Remove any quotes, periods, or extra text
        intent = content.strip('"').strip("'").strip('.').strip()
        
        # Validate that the intent is one of our expected categories
        valid_intents = [
            "Status", "Complaint", "Inquiry", "Pricing Negotiation", 
            "Proposal", "Logistics", "Acknowledgement", "Status of Inquiry"
        ]
        
        if intent not in valid_intents:
            log.warning(f"LLM returned unexpected intent: {intent}")
            # Try to find the closest match in our valid intents
            for valid_intent in valid_intents:
                if valid_intent.lower() in intent.lower():
                    intent = valid_intent
                    break
            else:
                # If no match found, log the error but don't default to any category
                log.error(f"LLM returned invalid intent that couldn't be matched: {content}")
                # Return the raw response for manual review
                return {"intent": "Invalid Response", "error": f"LLM returned invalid category: {content}", "raw_response": content}
        
        return {
            "intent": intent,
            "confidence": 0.9,  # Hard-coded confidence since LLM doesn't provide it
            "raw_response": content
        }
        
    except Exception as e:
        log.error(f"Error detecting intent: {e}")
        return {"intent": "Unknown", "confidence": 0.0, "error": str(e)}


def get_intent_label(intent: str) -> str:
    """
    Convert intent category to a Gmail label name.
    
    Args:
        intent: The detected intent category
        
    Returns:
        Gmail label name
    """
    # Import from label config for consistency
    try:
        from gmail_labeling.label_config import get_intent_label as get_clean_intent_label
        return get_clean_intent_label(intent)
    except ImportError:
        # Fallback if import fails
        intent_label_map = {
            "Status": "Status",
            "Complaint": "Complaint",
            "Inquiry": "Inquiry", 
            "Pricing Negotiation": "Pricing-Negotiation",
            "Proposal": "Proposal",
            "Logistics": "Logistics",
            "Acknowledgement": "Acknowledgement", 
            "Status of Inquiry": "Status-of-Inquiry",
            "Unknown": "Unclassified"
        }
        return intent_label_map.get(intent, "Unclassified")
