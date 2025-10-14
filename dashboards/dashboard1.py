import configparser
import os
import sys
import json
import base64
from datetime import datetime, timedelta
from email.utils import parsedate_tz, mktime_tz
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pytz
from pymongo import MongoClient
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.retry_framework import retry_service

# Module-level logger
logger = logging.getLogger("gmail_unreplied")

class GmailUnrepliedChecker:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.gmail_service = None
        self.credentials = None
        self.label_cache = {}  # Cache for label name mapping
        
        # Configuration settings
        self.BATCH_SIZE = int(self.config.get('settings', 'batch_size'))
        self.START_DATE = self.config.get('gmail', 'start_date')
        self.SCOPES = self.config.get('settings', 'scopes').split(',')
        self.TIMEZONE = self.config.get('settings', 'timezone')
        self.TARGET_LABELS = [label.strip() for label in self.config.get('labels', 'target_labels').split(',')]
        
        # OAuth credentials from config
        self.CLIENT_ID = self.config.get('gmail', 'client_id')
        self.CLIENT_SECRET = self.config.get('gmail', 'client_secret')
        
        # MongoDB settings
        self.MONGO_CONNECTION = self.config.get('mongodb', 'connection_string')
        self.USER_DATABASE = self.config.get('mongodb', 'user_database')
        self.USER_COLLECTION = self.config.get('mongodb', 'user_collection')
        self.ANALYTICS_DATABASE = self.config.get('mongodb', 'analytics_database')
        self.DASHBOARD1_COLLECTION = self.config.get('mongodb', 'dashboard1_collection')
        
        # Dashboard1 specific settings
        self.INTENT_FIELD_NAME = self.config.get('dashboard1', 'intent_field_name')
        self.UNREPLIED_EMAILS_FIELD_NAME = self.config.get('dashboard1', 'unreplied_emails_field_name')
        
        # Initialize IST timezone
        self.ist_timezone = pytz.timezone(self.TIMEZONE)
        
        # MongoDB client
        self.mongo_client = MongoClient(self.MONGO_CONNECTION)
        self.user_collection = self.mongo_client[self.USER_DATABASE][self.USER_COLLECTION]
        self.analytics_db = self.mongo_client[self.ANALYTICS_DATABASE]
        self.dashboard1_collection = self.analytics_db[self.DASHBOARD1_COLLECTION]
        
        # File paths - programmatically set to data/ and logs/ folders  
        log_filename = self.config.get('settings', 'log_filename')
        self.LOG_FILENAME = os.path.join('logs', log_filename)
        
        summary_file = self.config.get('dashboard1', 'intent_summary_file')
        detail_file = self.config.get('dashboard1', 'detailed_json_file')
        self.SUMMARY_JSON_FILE = os.path.join('data', 'dashboard1', summary_file)
        self.DETAILED_JSON_FILE = os.path.join('data', 'dashboard1', detail_file)
        
        # Ensure required directories exist
        os.makedirs("data/dashboard1", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # Setup logging
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        if not logger.handlers:
            handler = logging.FileHandler(self.LOG_FILENAME)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    def get_active_users(self):
        """Fetch all active users from MongoDB"""
        try:
            users = list(self.user_collection.find({
                "oauth_tokens": {"$exists": True}
            }))
            logger.info(f"Found {len(users)} active users in database")
            return users
        except Exception as e:
            logger.error(f"Error fetching users from MongoDB: {e}")
            return []

    def make_api_call_with_retry(self, api_call_func):
        """Execute Gmail API call with 65s fixed delay and previous data fallback"""
        result = retry_service.gmail_retry(api_call_func, operation_name="Gmail API call")
        if result is None:
            logger.warning("Gmail API failed - showing previous run data (no new JSON generated)")
        return result

    def get_label_names(self, label_ids):
        """Convert label IDs to readable names using cache"""
        try:
            # Load label cache if not already loaded
            if not self.label_cache:
                labels_response = self.make_api_call_with_retry(
                    lambda: self.gmail_service.users().labels().list(userId='me').execute()
                )
                if labels_response is None:
                    return []  # Return empty list if API failed
                for label in labels_response.get('labels', []):
                    if 'id' in label:
                        self.label_cache[label['id']] = label.get('name', label['id'])

            readable_labels = []
            for label_id in label_ids:
                label_name = self.label_cache.get(label_id)
                if not label_name:
                    label = self.gmail_service.users().labels().get(userId='me', id=label_id).execute()
                    label_name = label.get('name', label_id)
                    self.label_cache[label_id] = label_name
                readable_labels.append(label_name)
            
            return readable_labels
        except Exception as e:
            logger.error(f"Error getting label names: {e}")
            return label_ids  # Return original IDs if conversion fails

    def authenticate_user(self, user_data):
        """Authenticate user using stored tokens"""
        try:
            username = user_data.get('username', 'Unknown')
            oauth_tokens = user_data.get('oauth_tokens', {})
            
            # Debug: Log what tokens we have
            logger.info(f"Tokens available for {username}: {list(oauth_tokens.keys())}")
            
            access_token = oauth_tokens.get('access_token')
            refresh_token = oauth_tokens.get('refresh_token')
            
            if not access_token:
                logger.error(f"No access token found for user: {username}")
                return False
            
            # Create credentials - use refresh_token from MongoDB + client credentials from config
            if refresh_token and self.CLIENT_ID and self.CLIENT_SECRET:
                logger.info(f"Creating credentials with full OAuth info for {username}")
                self.credentials = Credentials(
                    token=access_token,
                    refresh_token=refresh_token,
                    token_uri='https://oauth2.googleapis.com/token',
                    client_id=self.CLIENT_ID,
                    client_secret=self.CLIENT_SECRET
                )
            else:
                logger.info(f"Creating credentials with access_token only for {username}")
                self.credentials = Credentials(token=access_token)
            
            # Build Gmail service
            self.gmail_service = build('gmail', 'v1', credentials=self.credentials)
            
            # Test connection
            profile = self.gmail_service.users().getProfile(userId='me').execute()
            logger.info(f"Connected to Gmail: {profile['emailAddress']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to authenticate user {user_data.get('username', 'Unknown')}: {e}")
            return False


    def get_threads_from_inbox(self):
        """Get all thread IDs from INBOX since start_date"""
        logger.info(f"Fetching threads from INBOX since {self.START_DATE}...")

        # Convert start_date to query format
        start_date = datetime.strptime(self.START_DATE, '%Y-%m-%d')
        query = f'in:inbox after:{start_date.strftime("%Y/%m/%d")}'

        threads = []
        next_page_token = None

        try:
            while True:
                # Single call handles both first page and subsequent pages
                result = self.make_api_call_with_retry(
                    lambda: self.gmail_service.users().threads().list(
                        userId='me', q=query, pageToken=next_page_token
                    ).execute()
                )

                threads.extend(result.get('threads', []))
                next_page_token = result.get('nextPageToken')

                if not next_page_token:
                    break  # No more pages

            logger.info(f"Found {len(threads)} threads in INBOX")
            return [thread['id'] for thread in threads]

        except Exception as e:
            logger.error(f"Error fetching threads: {e}")
            return []


    def get_thread_details(self, thread_id):
        """Get thread details including latest message in INBOX"""
        try:
            thread = self.make_api_call_with_retry(
                lambda: self.gmail_service.users().threads().get(
                    userId='me', id=thread_id, 
                    fields='messages(id,labelIds,payload(headers))'
                ).execute()
            )
            
            # Find latest message in INBOX
            inbox_messages = []
            for message in thread['messages']:
                headers = message['payload'].get('headers', [])
                labels = message.get('labelIds', [])
                
                if 'INBOX' in labels:
                    # Get date, subject, and from headers
                    date_header = next((h['value'] for h in headers if h['name'].lower() == 'date'), None)
                    subject_header = next((h['value'] for h in headers if h['name'].lower() == 'subject'), None)
                    from_header = next((h['value'] for h in headers if h['name'].lower() == 'from'), None)
                    
                    if date_header:
                        readable_labels = self.get_label_names(labels)
                        inbox_messages.append({
                            'id': message['id'],
                            'date': date_header,
                            'subject': subject_header or 'No Subject',
                            'from': from_header or 'Unknown Sender',
                            'labels': readable_labels,
                            'timestamp': self.parse_date(date_header)
                        })
            
            if not inbox_messages:
                return None
            
            # Sort by timestamp and get latest
            inbox_messages.sort(key=lambda x: x['timestamp'], reverse=True)
            latest_inbox = inbox_messages[0]
            
            return {
                'thread_id': thread_id,
                'latest_inbox_date': latest_inbox['date'],
                'latest_inbox_timestamp': latest_inbox['timestamp'],
                'subject': latest_inbox['subject'],
                'from': latest_inbox['from'],
                'labels': latest_inbox['labels']
            }
            
        except Exception as e:
            logger.error(f"Error getting thread details for {thread_id}: {e}")
            return None

    def check_threads_in_sent_batch(self, thread_ids):
        """Check multiple threads in SENT folder using batch processing"""
        sent_threads = {}
        
        # Process in batches
        for i in range(0, len(thread_ids), self.BATCH_SIZE):
            batch = thread_ids[i:i + self.BATCH_SIZE]
            logger.info(f"Checking batch {i//self.BATCH_SIZE + 1} ({len(batch)} threads) in SENT...")
            
            for thread_id in batch:
                try:
                    # Check if thread exists in SENT
                    thread = self.make_api_call_with_retry(
                        lambda: self.gmail_service.users().threads().get(
                            userId='me', id=thread_id,
                            fields='messages(labelIds,payload(headers))'
                        ).execute()
                    )
                    
                    # Find latest message in SENT
                    sent_messages = []
                    for message in thread['messages']:
                        labels = message.get('labelIds', [])
                        if 'SENT' in labels:
                            headers = message['payload'].get('headers', [])
                            date_header = next((h['value'] for h in headers if h['name'].lower() == 'date'), None)
                            
                            if date_header:
                                sent_messages.append({
                                    'date': date_header,
                                    'timestamp': self.parse_date(date_header)
                                })
                    
                    if sent_messages:
                        # Sort by timestamp and get latest
                        sent_messages.sort(key=lambda x: x['timestamp'], reverse=True)
                        sent_threads[thread_id] = {
                            'latest_sent_date': sent_messages[0]['date'],
                            'latest_sent_timestamp': sent_messages[0]['timestamp']
                        }
                
                except Exception as e:
                    # Thread not found in SENT or other error
                    continue
        
        return sent_threads

    def parse_date(self, date_string):
        """Parse email date string to IST timestamp"""
        try:
            parsed_date = parsedate_tz(date_string)
            if parsed_date:
                # Convert to UTC timestamp first
                utc_timestamp = mktime_tz(parsed_date)
                # Convert to IST datetime
                utc_dt = datetime.fromtimestamp(utc_timestamp, tz=pytz.UTC)
                ist_dt = utc_dt.astimezone(self.ist_timezone)
                return ist_dt.timestamp()
            return 0
        except:
            return 0
    
    def get_current_ist_timestamp(self):
        """Get current timestamp in IST"""
        return datetime.now(self.ist_timezone).timestamp()
    
    def categorize_labels(self, labels):
        """Categorize labels based on target categories"""
        found_categories = []
        for label in labels:
            if label in self.TARGET_LABELS:
                found_categories.append(label)
        return found_categories if found_categories else ['Unclassified']
    
    def get_next_user_sequence_id(self):
        """Get next sequential ID for user in dashboard1 collection"""
        try:
            # Find the highest user_id in collection
            result = self.dashboard1_collection.find_one(
                {}, 
                sort=[("user_id", -1)]
            )
            return (result.get("user_id", 0) + 1) if result else 1
        except Exception as e:
            logger.error(f"Error getting next sequence ID: {e}")
            return 1

    def store_user_intent_analytics_mongodb(self, user_email, full_name, categorized_emails):
        """Store user intent analytics data as single document per user with retry"""
        def _store_operation():
            # Initialize categories structure with all target labels
            categories = {}
            for label in self.TARGET_LABELS:
                categories[label] = []
            
            # Populate categories with actual emails
            for email_data in categorized_emails:
                email_categories = email_data.get('categories', ['Unclassified'])
                for category in email_categories:
                    if category in categories:
                        email_info = {
                            "subject": email_data.get('subject', 'No Subject'),
                            "from_email": email_data.get('from', 'Unknown Sender'),
                            "hours_unreplied": email_data.get('hours_unreplied', 0),
                            "inbox_date": email_data.get('inbox_date', '')
                        }
                        categories[category].append(email_info)
            
            # Check if user already exists to preserve user_id
            existing_user = self.dashboard1_collection.find_one({"user_email": user_email})
            user_id = existing_user.get("user_id") if existing_user else self.get_next_user_sequence_id()
            
            document = {
                "user_id": user_id,
                "user_email": user_email,
                "full_name": full_name,
                "categories": categories
            }
            
            # Use upsert to update existing or insert new
            self.dashboard1_collection.update_one(
                {"user_email": user_email},
                {"$set": document},
                upsert=True
            )
            logger.info(f"Stored/updated intent analytics for {user_email} with user_id {user_id}")
            return True
        
        return retry_service.mongodb_retry(_store_operation, operation_name=f"MongoDB store for {user_email}")
    
    def clear_user_data_from_mongodb(self, user_email):
        """No longer needed - using upsert instead"""
        # Kept for backward compatibility but does nothing
        pass

    def calculate_hours_difference(self, inbox_timestamp, sent_timestamp):
        """Calculate hours difference between sent and inbox"""
        return (sent_timestamp - inbox_timestamp) / 3600

    def find_unreplied_emails_for_user(self, user_data):
        """Find unreplied emails for a specific user"""
        username = user_data.get('username', 'Unknown')
        logger.info("="*60)
        logger.info(f"Processing user: {username}")
        logger.info("="*60)
        
        # Get all thread IDs from INBOX
        thread_ids = self.get_threads_from_inbox()
        if not thread_ids:
            logger.info(f"No threads found in INBOX for {username}")
            return {
                'user': username,
                'categories': {},
                'total_unreplied_24h': 0,
                'timestamp': datetime.now(self.ist_timezone).isoformat(),
                'status': 'no_threads'
            }
        
        # Get thread details
        logger.info(f"Getting thread details for {len(thread_ids)} threads...")
        thread_details = {}
        for thread_id in thread_ids:
            details = self.get_thread_details(thread_id)
            if details:
                thread_details[thread_id] = details
        
        logger.info(f"Processed {len(thread_details)} threads")
        
        # Check which threads have replies in SENT
        logger.info("Checking for replies in SENT folder...")
        sent_threads = self.check_threads_in_sent_batch(list(thread_details.keys()))
        
        # Analyze unreplied emails with category counting
        unreplied_emails = []
        category_counts = {label: 0 for label in self.TARGET_LABELS}
        current_time = self.get_current_ist_timestamp()
        
        for thread_id, details in thread_details.items():
            inbox_timestamp = details['latest_inbox_timestamp']
            
            if thread_id in sent_threads:
                # Thread has reply in SENT
                sent_timestamp = sent_threads[thread_id]['latest_sent_timestamp']
                hours_diff = self.calculate_hours_difference(inbox_timestamp, sent_timestamp)
                
                if hours_diff < 0:  # Negative means not replied (inbox is newer than sent)
                    hours_since_inbox = (current_time - inbox_timestamp) / 3600
                    if hours_since_inbox > 24:
                        categories = self.categorize_labels(details['labels'])
                        for category in categories:
                            category_counts[category] += 1
                        
                        email_data = {
                            'thread_id': thread_id,
                            'subject': details['subject'],
                            'from': details['from'],
                            'labels': details['labels'],
                            'categories': categories,
                            'hours_unreplied': hours_since_inbox,
                            'inbox_date': details['latest_inbox_date']
                        }
                        unreplied_emails.append(email_data)
            else:
                # No reply found in SENT folder
                hours_since_inbox = (current_time - inbox_timestamp) / 3600
                if hours_since_inbox > 24:
                    categories = self.categorize_labels(details['labels'])
                    for category in categories:
                        category_counts[category] += 1
                    
                    email_data = {
                        'thread_id': thread_id,
                        'subject': details['subject'],
                        'from': details['from'],
                        'labels': details['labels'],
                        'categories': categories,
                        'hours_unreplied': hours_since_inbox,
                        'inbox_date': details['latest_inbox_date']
                    }
                    unreplied_emails.append(email_data)
        
        # Clear label cache for next user
        self.label_cache = {}
        
        # Store user data in MongoDB with new structure
        full_name = user_data.get('full_name', 'Unknown')
        self.store_user_intent_analytics_mongodb(username, full_name, unreplied_emails)
        
        # Log detailed email information
        self.log_detailed_emails(username, unreplied_emails)
        
        # Extract user ID from email (part before @)
        user_id = username.split('@')[0] if '@' in username else username
        
        # Convert categories to new structure (include zero counts)
        intent_array = []
        for category in self.TARGET_LABELS:
            intent_array.append({
                "category": category,
                "count": category_counts.get(category, 0)
            })
        
        return {
            'id': user_id,
            'email': username,
            'fullName': user_data.get('full_name', 'Unknown'),
            self.INTENT_FIELD_NAME: intent_array,
            'totalUnreplied24h': len(unreplied_emails),
            self.UNREPLIED_EMAILS_FIELD_NAME: unreplied_emails  # Include detailed email data for Dashboard2
        }

    def process_all_users(self):
        """Process all active users and generate consolidated report"""
        logger.info("Gmail Unreplied Email Checker - Multi-User Production Mode")
        logger.info(f"Start Date: {self.START_DATE}")
        logger.info(f"Timezone: {self.TIMEZONE}")
        logger.info(f"Target Categories: {', '.join(self.TARGET_LABELS)}")
        # Get all active users
        users = self.get_active_users()
        if not users:
            logger.info("No active users found in database")
            return
        
        # Process each user
        all_results = {
            "users": []
        }
        
        logger.info(f"Starting analysis for {len(users)} users at {datetime.now(self.ist_timezone).isoformat()}")
        
        for user_data in users:
            try:
                # Authenticate user
                if self.authenticate_user(user_data):
                    # Clear existing MongoDB data for this user
                    username = user_data.get('username', 'Unknown')
                    self.clear_user_data_from_mongodb(username)
                    
                    # Process user's emails
                    user_result = self.find_unreplied_emails_for_user(user_data)
                    all_results["users"].append(user_result)
                    
                    logger.info(f"Completed processing for {user_result['email']} - {user_result['totalUnreplied24h']} unreplied emails")
                else:
                    # Add failed authentication result with empty categories
                    username = user_data.get('username', 'Unknown')
                    user_id = username.split('@')[0] if '@' in username else username
                    all_results["users"].append({
                        "id": user_id,
                        "email": username,
                        "fullName": user_data.get('full_name', 'Unknown'),
                        self.INTENT_FIELD_NAME: [],
                        "totalUnreplied24h": 0
                    })
                    logger.error(f"Authentication failed for {username}")
                    
            except Exception as e:
                username = user_data.get('username', 'Unknown')
                logger.error(f"Error processing user {username}: {e}")
                user_id = username.split('@')[0] if '@' in username else username
                all_results["users"].append({
                    "id": user_id,
                    "email": username,
                    "fullName": user_data.get('full_name', 'Unknown'),
                    self.INTENT_FIELD_NAME: [],
                    "totalUnreplied24h": 0
                })
        
        # Display and save results
        self.display_consolidated_results(all_results)
        self.save_detailed_emails_json(all_results)
        
        return all_results
    
    def log_detailed_emails(self, username, unreplied_emails):
        """Log detailed email information for a user"""
        if not unreplied_emails:
            logger.info(f"No unreplied emails found for {username}")
            return
        logger.info(f"DETAILED UNREPLIED EMAILS FOR: {username}")

        for i, email in enumerate(unreplied_emails, 1):
            logger.info(f"Mail{i}: {email['subject']}, Unreplied Hours: {email['hours_unreplied']:.1f}")
    
    def save_detailed_emails_json(self, results):
        """Save detailed emails data to separate JSON file with retry"""
        def _save_operation():
            detailed_data = {
                "users": []
            }
            
            for user_result in results['users']:
                detailed_user = {
                    'id': user_result['id'],
                    'email': user_result['email'],
                    'fullName': user_result['fullName'],
                    'totalUnreplied24h': user_result['totalUnreplied24h'],
                    self.UNREPLIED_EMAILS_FIELD_NAME: user_result.get(self.UNREPLIED_EMAILS_FIELD_NAME, [])
                }
                detailed_data["users"].append(detailed_user)
            
            with open(self.DETAILED_JSON_FILE, 'w') as f:
                json.dump(detailed_data, f, indent=2)
            
            logger.info(f"Detailed emails data saved to: {self.DETAILED_JSON_FILE}")
            return True
        
        return retry_service.file_retry(_save_operation, operation_name="Save detailed emails JSON")
    
    def display_consolidated_results(self, results):
        """Display consolidated results for all users"""
        logger.info("CONSOLIDATED UNREPLIED EMAILS REPORT")
        
        # Per-user summary
        total_unreplied = 0
        for user_result in results['users']:
            total_unreplied += user_result['totalUnreplied24h']
            logger.info(f"Completed: {user_result['email']} ({user_result['fullName']}): {user_result['totalUnreplied24h']} unreplied")
        
        
        # Create summary data without detailed emails
        summary_data = {
            "users": []
        }
        
        for user_result in results['users']:
            summary_user = {
                'id': user_result['id'],
                'email': user_result['email'],
                'fullName': user_result['fullName'],
                self.INTENT_FIELD_NAME: user_result.get(self.INTENT_FIELD_NAME, []),
                'totalUnreplied24h': user_result['totalUnreplied24h']
            }
            summary_data["users"].append(summary_user)
        
        # Save summary JSON to file with retry
        def _save_summary_operation():
            report_filename_format = self.config.get('settings', 'report_filename_format', fallback='unreplied_analysis_%Y%m%d_%H%M%S.json')
            if '%' in report_filename_format:
                filename = datetime.now(self.ist_timezone).strftime(report_filename_format)
            else:
                filename = self.SUMMARY_JSON_FILE  # Use the programmatic path
            
            with open(filename, 'w') as f:
                json.dump(summary_data, f, indent=2)
            
            logger.info(f"Summary report saved to: {filename}")
            return True
        
        retry_service.file_retry(_save_summary_operation, operation_name="Save summary report JSON")

def main():
    """Main function with comprehensive logging"""
    try:
        checker = GmailUnrepliedChecker()
        start_time = datetime.now(checker.ist_timezone)
        logger.info(f"Starting Gmail Unreplied Email Analysis at {start_time.isoformat()}")
        
        results = checker.process_all_users()
        
        # Log final summary
        if results:
            total_users = len(results['users'])
            total_unreplied = sum(user['totalUnreplied24h'] for user in results['users'])
            logger.info("Analysis completed successfully:")
            logger.info(f"- Users processed: {total_users}")
            logger.info(f"- Total unreplied emails: {total_unreplied}")
        
        end_time = datetime.now(checker.ist_timezone)
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Analysis completed in {duration:.2f} seconds")
            
    except Exception as e:
        logger.error(f"Critical error during analysis: {e}")
        sys.exit(1)
    finally:
        # Close MongoDB connection
        try:
            if 'checker' in locals():
                checker.mongo_client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.warning(f"Error closing MongoDB connection: {e}")


if __name__ == "__main__":
    main()
