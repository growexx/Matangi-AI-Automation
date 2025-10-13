import os
import sys
import json
import logging
import configparser
from datetime import datetime
import pytz
from pymongo import MongoClient

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
        
        # File paths - programmatically set to data/ and logs/ folders
        input_file = self.config.get('dashboard2', 'aging_input_file')
        output_file = self.config.get('dashboard2', 'aging_output_file')
        log_filename = self.config.get('dashboard2', 'log_filename')
        
        self.INPUT_JSON_FILE = os.path.join('data', 'dashboard1', input_file)
        self.OUTPUT_JSON_FILE = os.path.join('data', 'dashboard2', output_file)
        self.LOG_FILENAME = os.path.join('logs', log_filename)
        self.BUCKET_24H_TO_48H = self.config.get('dashboard2', 'bucket_24h_to_48h')
        self.BUCKET_48H_TO_72H = self.config.get('dashboard2', 'bucket_48h_to_72h')
        self.BUCKET_72H_TO_168H = self.config.get('dashboard2', 'bucket_72h_to_168h')
        self.BUCKET_ABOVE_168H = self.config.get('dashboard2', 'bucket_above_168h')
        
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
        
        # Ensure required directories exist
        os.makedirs("data/dashboard2", exist_ok=True)
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
        """Categorize unreplied emails by time buckets"""
        if 24 < hours_unreplied <= 48:
            return self.BUCKET_24H_TO_48H
        elif 48 < hours_unreplied <= 72:
            return self.BUCKET_48H_TO_72H 
        elif 72 < hours_unreplied <= 168:
            return self.BUCKET_72H_TO_168H
        elif hours_unreplied > 168:
            return self.BUCKET_ABOVE_168H
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

    def store_user_aging_analytics_mongodb(self, user_email, full_name, bucketed_emails):
        """Store user aging analytics data as single document per user with retry"""
        def _store_operation():
            user_id = self.get_next_user_sequence_id()
            
            # Initialize time buckets structure
            time_buckets = {
                self.BUCKET_24H_TO_48H: [],
                self.BUCKET_48H_TO_72H: [],
                self.BUCKET_72H_TO_168H: [],
                self.BUCKET_ABOVE_168H: []
            }
            
            # Populate time buckets with emails
            for bucket_name, emails in bucketed_emails.items():
                if bucket_name in time_buckets:
                    time_buckets[bucket_name] = emails
            
            document = {
                "user_id": user_id,
                "user_email": user_email,
                "full_name": full_name,
                "time_buckets": time_buckets
            }
            
            self.dashboard2_collection.insert_one(document)
            logger.info(f"Stored aging analytics for {user_email} with user_id {user_id}")
            return True
        
        return retry_service.mongodb_retry(_store_operation, operation_name=f"MongoDB aging store for {user_email}")
    
    def clear_user_data_from_mongodb(self, user_email):
        """Clear existing data for a user before storing new data"""
        try:
            result = self.dashboard2_collection.delete_many({"user_email": user_email})
            logger.info(f"Cleared {result.deleted_count} existing time bucket records for {user_email}")
        except Exception as e:
            logger.error(f"Error clearing user data from MongoDB: {e}")

    def load_dashboard1_data(self):
        """Load data from dashboard1.py output with retry"""
        def _load_operation():
            input_file = self.INPUT_JSON_FILE
            
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input file {input_file} not found. Please run dashboard1.py first.")
                
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Loaded data for {len(data.get('users', []))} users from {input_file}")
            return data
        
        return retry_service.file_retry(_load_operation, operation_name="Load Dashboard1 JSON data")

    def process_user_aging_buckets(self, user_data):
        """Process a single user's data into aging time buckets"""
        user_id = user_data.get('id', 'unknown')
        email = user_data.get('email', 'unknown@example.com')
        full_name = user_data.get('fullName', 'Unknown')
        
        # Initialize time bucket counts and email storage
        aging_buckets = {
            'count_24h_to_48h': 0,
            'count_48h_to_72h': 0,
            'count_72h_to_168h': 0,
            'count_above_168h': 0
        }
        
        # Initialize bucketed emails storage for MongoDB
        bucketed_emails = {
            self.BUCKET_24H_TO_48H: [],
            self.BUCKET_48H_TO_72H: [],
            self.BUCKET_72H_TO_168H: [],
            self.BUCKET_ABOVE_168H: []
        }
        
        # Clear existing MongoDB data for this user
        self.clear_user_data_from_mongodb(email)
        
        # Get unreplied emails data from dashboard1
        unreplied_emails = user_data.get(self.UNREPLIED_EMAILS_FIELD_NAME, [])
        
        # Process each email using actual hours_unreplied data
        for email_data in unreplied_emails:
            hours_unreplied = email_data.get('hours_unreplied', 0)
            time_bucket = self.categorize_by_time_bucket(hours_unreplied)
            if time_bucket:
                aging_buckets[time_bucket] += 1
                # Add email to appropriate bucket for MongoDB
                email_info = {
                    "subject": email_data.get('subject', 'No Subject'),
                    "from_email": email_data.get('from', 'Unknown Sender'),
                    "hours_unreplied": hours_unreplied,
                    "categories": email_data.get('categories', [])
                }
                bucketed_emails[time_bucket].append(email_info)
        
        # Store user aging analytics in MongoDB
        self.store_user_aging_analytics_mongodb(email, full_name, bucketed_emails)
        
        total_unreplied = len(unreplied_emails)
        
        return {
            'user': email,  
            'fullName': full_name,  
            'count_24h_to_48h': aging_buckets['count_24h_to_48h'],    # >24h and <=48h
            'count_48h_to_72h': aging_buckets['count_48h_to_72h'],    # >48h and <=72h  
            'count_72h_to_168h': aging_buckets['count_72h_to_168h'],  # >72h and <=168h
            'count_above_168h': aging_buckets['count_above_168h'],    # >168h
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
                logger.info(f"  24-48h: {user_aging['count_24h_to_48h']}, 48-72h: {user_aging['count_48h_to_72h']}, 72-168h: {user_aging['count_72h_to_168h']}, >168h: {user_aging['count_above_168h']}")
                
            except Exception as e:
                logger.error(f"Error processing user {user_data.get('email', 'unknown')}: {e}")
                continue
        
        # Create final aging report with simplified structure
        result = {
            "users": aging_report
        }
        
        # Save result
        self.save_results(result)
        
        logger.info("=" * 70)
        logger.info(f"Aging Report completed. Processed {len(aging_report)} users.")

    def save_results(self, results):
        """Save aging report to JSON file with retry"""
        def _save_operation():
            with open(self.OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"Aging report saved to {self.OUTPUT_JSON_FILE}")
            return True
        
        return retry_service.file_retry(_save_operation, operation_name="Save aging report JSON")

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