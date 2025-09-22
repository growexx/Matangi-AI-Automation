"""
Thread Processing Module

This module contains all components for fetching, processing, and storing email thread data:
- ThreadFetcher: Fetches email threads from IMAP and MongoDB
- ThreadJSONStorage: Handles JSON storage of thread data
- ThreadProcessorIntegration: Integration with the main email processing pipeline
"""

__version__ = "1.0.0"

# Import main classes for easy access
from .thread_fetcher import ThreadFetcher
from .integration import integrate_with_processor_after_storage

__all__ = [
    'ThreadFetcher',
    'integrate_with_processor_after_storage'
]
