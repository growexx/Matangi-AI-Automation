import os
import sys
import json
import logging
import configparser
from datetime import datetime
from email.utils import parsedate_tz, mktime_tz
import pytz
from pymongo import MongoClient
from collections import OrderedDict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.retry_framework import retry_service

# Module-level logger
logger = logging.getLogger("dashboard2_aging")

class AgingReportProcessor:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        
        # Dashboard2 specific settings
        self.TIMEZONE = self.config.get('settings', 'timezone')
        
        # File paths for logging
        log_filename = self.config.get('dashboard2', 'log_filename')
        self.LOG_FILENAME = os.path.join('logs', log_filename)
        
        # Day-based bucket settings - all configurable
        self.BUCKET_1_TO_2_DAYS = self.config.get('dashboard2', 'bucket_1_to_2_days')
        self.BUCKET_2_TO_3_DAYS = self.config.get('dashboard2', 'bucket_2_to_3_days')
        self.BUCKET_3_TO_7_DAYS = self.config.get('dashboard2', 'bucket_3_to_7_days')
        self.BUCKET_ABOVE_7_DAYS = self.config.get('dashboard2', 'bucket_above_7_days')
        
        # Configurable day thresholds (in hours for calculation)
        self.THRESHOLD_1_DAY = self.config.getint('dashboard2', 'threshold_1_day')
        self.THRESHOLD_2_DAYS = self.config.getint('dashboard2', 'threshold_2_days')
        self.THRESHOLD_3_DAYS = self.config.getint('dashboard2', 'threshold_3_days')
        self.THRESHOLD_7_DAYS = self.config.getint('dashboard2', 'threshold_7_days')
        self.MIN_UNREPLIED_HOURS = self.config.getint('dashboard2', 'min_unreplied_hours')
        
        # MongoDB settings
        self.MONGO_CONNECTION = self.config.get('mongodb', 'connection_string')
        self.USER_DATABASE = self.config.get('mongodb', 'user_database')
        self.USER_COLLECTION = self.config.get('mongodb', 'user_collection')
        self.ANALYTICS_DATABASE = self.config.get('mongodb', 'analytics_database')
        self.DASHBOARD2_COLLECTION = self.config.get('mongodb', 'dashboard2_collection')
        
        # Dashboard1 settings to access field names
        self.UNREPLIED_EMAILS_FIELD_NAME = self.config.get('dashboard1', 'unreplied_emails_field_name')
        
        # Initialize timezone
        self.ist_timezone = pytz.timezone(self.TIMEZONE)
        
        # MongoDB client
        self.mongo_client = MongoClient(self.MONGO_CONNECTION)
        self.user_collection = self.mongo_client[self.USER_DATABASE][self.USER_COLLECTION]
        self.analytics_db = self.mongo_client[self.ANALYTICS_DATABASE]
        self.dashboard2_collection = self.analytics_db[self.DASHBOARD2_COLLECTION]
        
        # MongoDB operations use retry_service
        
        # Ensure logs directory exists
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
        
    def categorize_by_time_bucket(self, hours_unreplied):
        """Categorize unreplied emails by configurable day-based time buckets"""
        if self.THRESHOLD_1_DAY < hours_unreplied <= self.THRESHOLD_2_DAYS:
            return self.BUCKET_1_TO_2_DAYS
        elif self.THRESHOLD_2_DAYS < hours_unreplied <= self.THRESHOLD_3_DAYS:
            return self.BUCKET_2_TO_3_DAYS 
        elif self.THRESHOLD_3_DAYS < hours_unreplied <= self.THRESHOLD_7_DAYS:
            return self.BUCKET_3_TO_7_DAYS
        elif hours_unreplied > self.THRESHOLD_7_DAYS:
            return self.BUCKET_ABOVE_7_DAYS
        else:
            return None
    
    def get_next_user_sequence_id(self):
        """Get next sequential ID for user in dashboard2 collection"""
        try:
            # Find the highest user_id in collection
            result = self.dashboard2_collection.find_one(
                {}, 
                sort=[("user_id", -1)]
            )
            return (result.get("user_id", 0) + 1) if result else 1
        except Exception as e:
            logger.error(f"Error getting next sequence ID: {e}")
            return 1

    def store_user_aging_analytics_mongodb(self, user_email, full_name, count_by_bucket):
        """Store user aging analytics data with consolidated structure in MongoDB"""
        def _store_operation():
            # Check if user already exists to preserve user_id
            existing_user = self.dashboard2_collection.find_one({"user_email": user_email})
            user_id = existing_user.get("user_id") if existing_user else self.get_next_user_sequence_id()
            
            # Calculate total unreplied count
            total_unreplied = sum(bucket['count'] for bucket in count_by_bucket)
            
            # Ensure order: user_id, full_name, user_email, ...
            document = OrderedDict([
                ("user_id", user_id),
                ("full_name", full_name),
                ("user_email", user_email),
                ("count_by_bucket", count_by_bucket),
                ("total_unreplied", total_unreplied)
            ])
            
            # Remove $oid if present in document
            if "_id" in document:
                del document["_id"]
            self.dashboard2_collection.update_one(
                {"user_email": user_email},
                {"$set": document},
                upsert=True
            )
            logger.info(f"Stored/updated aging analytics for {user_email} with user_id {user_id}")
            return True
        
        return retry_service.mongodb_retry(_store_operation, operation_name=f"MongoDB aging store for {user_email}")
    
    def clear_user_data_from_mongodb(self, user_email):
        """No longer needed - using upsert instead"""
        # Kept for backward compatibility but does nothing
        pass

    def parse_email_date_to_ist(self, date_string):
        """Parse email date string to IST timestamp using robust parsing like Dashboard 1"""
        try:
            # Use same parsing logic as Dashboard 1
            parsed_date = parsedate_tz(date_string)
            if parsed_date:
                # Convert to UTC timestamp first
                utc_timestamp = mktime_tz(parsed_date)
                # Convert to IST datetime
                utc_dt = datetime.fromtimestamp(utc_timestamp, tz=pytz.UTC)
                ist_dt = utc_dt.astimezone(self.ist_timezone)
                return ist_dt.timestamp()
            return 0
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_string}': {e}")
            return 0

    def load_dashboard1_data(self):
        """Load data from Dashboard1 MongoDB collection"""
        def _load_operation():
            # Get all users from dashboard1 collection
            users = list(self.analytics_db[self.config.get('mongodb', 'dashboard1_collection')].find({}))
            
            # Convert MongoDB data to expected format
            data = {
                "users": []
            }
            
            current_ist_time = datetime.now(self.ist_timezone).timestamp()
            
            for user in users:
                unreplied_emails = []
                for intent in user.get('Intent', []):
                    for email in intent.get('emails', []):
                        # Use robust date parsing like Dashboard 1
                        inbox_timestamp = self.parse_email_date_to_ist(email['inbox_date'])
                        
                        if inbox_timestamp > 0:
                            # Calculate hours unreplied using IST timestamps
                            hours_unreplied = (current_ist_time - inbox_timestamp) / 3600
                        else:
                            # If parsing fails, log and skip this email
                            logger.warning(f"Skipping email with unparseable date: {email['inbox_date']}")
                            continue

                        unreplied_emails.append({
                            'thread_id': email['subject'],
                            'subject': email['subject'],
                            'from': email['from'],
                            'hours_unreplied': hours_unreplied,
                            'inbox_date': email['inbox_date']
                        })
                
                user_data = {
                    'id': user.get('user_id'),
                    'email': user.get('user_email'),
                    'fullName': user.get('full_name'),
                    self.UNREPLIED_EMAILS_FIELD_NAME: unreplied_emails
                }
                data["users"].append(user_data)
            
            logger.info(f"Loaded data for {len(data['users'])} users from MongoDB")
            return data
            
        return retry_service.mongodb_retry(_load_operation, operation_name="Load Dashboard1 MongoDB data")

    def process_user_aging_buckets(self, user_data):
        """Process a single user's data into aging time buckets with consolidated structure"""
        user_id = user_data.get('id', 'unknown')
        email = user_data.get('email', 'unknown@example.com')
        full_name = user_data.get('fullName', 'Unknown')
        
        # Initialize bucketed emails storage with day-based names
        bucketed_emails = {
            '1_to_2_days': [],
            '2_to_3_days': [],
            '3_to_7_days': [],
            'above_7_days': []
        }
        
        # Map config bucket names to display format
        bucket_name_map = {
            self.BUCKET_1_TO_2_DAYS: '1_to_2_days',
            self.BUCKET_2_TO_3_DAYS: '2_to_3_days',
            self.BUCKET_3_TO_7_DAYS: '3_to_7_days',
            self.BUCKET_ABOVE_7_DAYS: 'above_7_days'
        }
        
        # Get unreplied emails data from dashboard1
        unreplied_emails = user_data.get(self.UNREPLIED_EMAILS_FIELD_NAME, [])
        
        # Process each email using actual hours_unreplied data
        for email_data in unreplied_emails:
            hours_unreplied = email_data.get('hours_unreplied', 0)
            time_bucket = self.categorize_by_time_bucket(hours_unreplied)
            if time_bucket:
                # Map to new bucket name format
                new_bucket_name = bucket_name_map.get(time_bucket)
                if new_bucket_name:
                    # Add email to appropriate bucket (remove 'to' field)
                    email_info = {
                        "from": email_data.get('from', 'Unknown Sender'),
                        "date": email_data.get('inbox_date', ''),
                        "subject": email_data.get('subject', 'No Subject')
                    }
                    bucketed_emails[new_bucket_name].append(email_info)
        
        # Build count_by_bucket array with emails - configurable order
        count_by_bucket = []
        for bucket_key in ['1_to_2_days', '2_to_3_days', '3_to_7_days', 'above_7_days']:
            count_by_bucket.append({
                "category": bucket_key,
                "count": len(bucketed_emails[bucket_key]),
                "emails": bucketed_emails[bucket_key]
            })
        
        # Store consolidated data in MongoDB
        self.store_user_aging_analytics_mongodb(email, full_name, count_by_bucket)
        
        total_unreplied = len(unreplied_emails)
        
        return {
            'user_id': user_id,
            'fullName': full_name,
            'user': email,
            'count_by_bucket': count_by_bucket,
            'total_unreplied': total_unreplied
        }

    def process_all_users(self):
        """Process all users and generate aging report"""
        logger.info("=" * 70)
        logger.info("Dashboard 2 - Aging Report Analysis Started")
        logger.info("=" * 70)

        # Load dashboard1 data
        dashboard1_data = self.load_dashboard1_data()
        if not dashboard1_data:
            return None
            
        users = dashboard1_data.get('users', [])
        if not users:
            logger.warning("No users found in dashboard1 data")
            return None
        
        # Process each user into aging buckets
        aging_report = []
        for user_data in users:
            try:
                user_aging = self.process_user_aging_buckets(user_data)
                aging_report.append(user_aging)
                
                # Log aging summary for this user  
                logger.info(f"User: {user_aging['user']}")
                bucket_counts = {b['category']: b['count'] for b in user_aging['count_by_bucket']}
                logger.info(f"  1-2 days: {bucket_counts.get('1_to_2_days', 0)}, 2-3 days: {bucket_counts.get('2_to_3_days', 0)}, 3-7 days: {bucket_counts.get('3_to_7_days', 0)}, >7 days: {bucket_counts.get('above_7_days', 0)}")
                
            except Exception as e:
                logger.error(f"Error processing user {user_data.get('email', 'unknown')}: {e}")
                continue
        
        # Create final aging report with simplified structure
        result = {
            "users": aging_report
        }
        
        # Log the results
        self.log_results(result)
        
        logger.info("=" * 70)
        logger.info(f"Aging Report completed. Processed {len(aging_report)} users.")

    def log_results(self, results):
        """Log aging report results"""
        if results and "users" in results:
            for user in results["users"]:
                logger.info(f"User: {user['user']} ({user['fullName']})")
                for bucket in user.get('count_by_bucket', []):
                    logger.info(f"  {bucket['category']}: {bucket['count']} emails")
                logger.info(f"  Total unreplied: {user.get('total_unreplied', 0)}")

def main():
    """Main function"""
    try:
        processor = AgingReportProcessor()
        processor.process_all_users()
        
    except Exception as e:
        logger.error(f"Critical error in dashboard2 aging report: {e}")
        return 1
    finally:
        # Close MongoDB connection
        try:
            if 'processor' in locals():
                processor.mongo_client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.warning(f"Error closing MongoDB connection: {e}")
    
    return 0

if __name__ == "__main__":
    main()
