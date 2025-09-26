#!/usr/bin/env python3
"""
Gmail Label Configuration - Colors and Mappings
"""

# Gmail-supported color palette (subset). Keep values within this list to avoid API rejections.
GMAIL_COLOR_PALETTE = {
    '1': {"backgroundColor": "#cc3a21", "textColor": "#ffffff"},   # Red
    '2': {"backgroundColor": "#ff6d01", "textColor": "#ffffff"},   # Orange
    '3': {"backgroundColor": "#fad165", "textColor": "#000000"},   # Yellow
    '4': {"backgroundColor": "#16a765", "textColor": "#ffffff"},   # Green
    '5': {"backgroundColor": "#007a87", "textColor": "#ffffff"},   # Teal
    '6': {"backgroundColor": "#4a86e8", "textColor": "#ffffff"},   # Blue
    '7': {"backgroundColor": "#7a4199", "textColor": "#ffffff"},   # Purple
    '8': {"backgroundColor": "#666666", "textColor": "#ffffff"},   # Gray
    '11': {"backgroundColor": "#43d692", "textColor": "#ffffff"},  # Light Green
    '12': {"backgroundColor": "#00bfa5", "textColor": "#ffffff"},  # Cyan
    '13': {"backgroundColor": "#4fc3f7", "textColor": "#ffffff"},  # Light Blue
    '15': {"backgroundColor": "#999999", "textColor": "#ffffff"},  # Light Gray
    '22': {"backgroundColor": "#e07798", "textColor": "#ffffff"},  # Light Red
}

# Map labels to allowed palette entries
LABEL_COLORS = {
    # Intent Labels
    "Inquiry": {"color": GMAIL_COLOR_PALETTE['6']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['6']["textColor"]},           # Blue
    "Status": {"color": GMAIL_COLOR_PALETTE['7']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['7']["textColor"]},            # Purple
    "Complaint": {"color": GMAIL_COLOR_PALETTE['1']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['1']["textColor"]},         # Red
    "Pricing-Negotiation": {"color": GMAIL_COLOR_PALETTE['2']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['2']["textColor"]}, # Orange
    "Proposal": {"color": GMAIL_COLOR_PALETTE['12']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['12']["textColor"]},        # Cyan
    "Logistics": {"color": GMAIL_COLOR_PALETTE['4']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['4']["textColor"]},         # Green
    "Acknowledgement": {"color": GMAIL_COLOR_PALETTE['8']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['8']["textColor"]},   # Gray
    "Status-of-Inquiry": {"color": GMAIL_COLOR_PALETTE['13']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['13']["textColor"]}, # Light Blue
    "Unclassified": {"color": GMAIL_COLOR_PALETTE['15']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['15']["textColor"]},    # Light Gray
    
    # Sentiment Labels
    "Higher-Positive": {"color": GMAIL_COLOR_PALETTE['11']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['11']["textColor"]}, # Light Green
    "Positive": {"color": GMAIL_COLOR_PALETTE['4']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['4']["textColor"]},          # Green
    "Neutral": {"color": GMAIL_COLOR_PALETTE['3']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['3']["textColor"]},           # Yellow
    "Negative": {"color": GMAIL_COLOR_PALETTE['22']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['22']["textColor"]},        # Light Red
    "Higher-Negative": {"color": GMAIL_COLOR_PALETTE['1']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['1']["textColor"]},   # Red
}

def get_intent_label(intent: str) -> str:
    """
    Convert intent category to a clean Gmail label name (no prefix).
    
    Args:
        intent: The detected intent category
        
    Returns:
        Clean Gmail label name
    """
    # Map intent categories to clean Gmail labels (no prefix)
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


def get_sentiment_label(sentiment: str) -> str:
    """
    Convert sentiment category to a clean Gmail label name (no prefix).
    
    Args:
        sentiment: The detected sentiment category
        
    Returns:
        Clean Gmail label name
    """
    # Map sentiment categories to clean Gmail labels (no prefix)
    sentiment_label_map = {
        "Higher Positive": "Higher-Positive",
        "Positive": "Positive", 
        "Neutral": "Neutral",
        "Negative": "Negative",
        "Higher Negative": "Higher-Negative"
    }
    
    return sentiment_label_map.get(sentiment, "Neutral")


def get_label_color(label_name: str) -> dict:
    """
    Get color configuration for a label.
    
    Args:
        label_name: The label name
        
    Returns:
        Dictionary with color and textColor
    """
    return LABEL_COLORS.get(label_name, {"color": GMAIL_COLOR_PALETTE['15']["backgroundColor"], "textColor": GMAIL_COLOR_PALETTE['15']["textColor"]})
