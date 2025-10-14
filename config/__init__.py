import os
import configparser
import logging

# Base paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configure paths
CONFIG_PATH = os.getenv("CONFIG_FILE", os.path.join(BASE_DIR, "config", "config.ini"))
DEFAULT_TOKEN_FILE = os.path.join(BASE_DIR, "config", "oauth_tokens.json")

# Load configuration
config = configparser.ConfigParser()
loaded = config.read(CONFIG_PATH)

# Paths configuration
TOKEN_FILE = os.getenv("TOKEN_FILE", config.get('paths', 'token_file', fallback=DEFAULT_TOKEN_FILE))
if not os.path.isabs(TOKEN_FILE):
    TOKEN_FILE = os.path.join(BASE_DIR, TOKEN_FILE)

# OAuth CONFIG
CLIENT_ID = config.get('oauth', 'client_id')
CLIENT_SECRET = config.get('oauth', 'client_secret')
REDIRECT_URI = config.get('oauth', 'redirect_uri')
AUTH_URL = config.get('oauth', 'auth_url')
TOKEN_URL = config.get('oauth', 'token_url')
SCOPES = config.get('oauth', 'scopes')

# IMAP CONFIG
IMAP_SERVER = config.get('imap', 'server')
MAILBOX = config.get('imap', 'mailbox')


# IDLE tuning
IDLE_TIMEOUT = int(config.get('monitor', 'idle_timeout', fallback='900'))
MAX_RECONNECT_ATTEMPTS = int(config.get('monitor', 'max_reconnect_attempts', fallback='5'))
RECONNECT_DELAY = int(config.get('monitor', 'reconnect_delay', fallback='10'))

# Mongo config
MONGO_URI = config.get('mongo', 'uri')
MONGO_DB = config.get('mongo', 'db')
MONGO_COL = config.get('mongo', 'collection')
MONGO_USER_COL = config.get('mongo', 'user_collection')
