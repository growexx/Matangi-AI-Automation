import os
import sys
import time
import json
import threading
import imaplib
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from flask import Flask, redirect, request
from urllib.parse import urlencode
from typing import Dict, Optional, Any
from config import *
import config as config_module
from logger.logger_setup import logger as log
from Utility.user_manager import user_manager


oauth_app = None
oauth_server_thread = None
oauth_server_active = False

class TokenManager:
    """Multi-tenant token manager for OAuth token management."""
    
    def __init__(self):
        self._refresh_locks = {}  # Per-user locks
        self._locks_lock = threading.Lock()
        self._last_refresh_times = {}  # Per-user refresh times
        self._refresh_cooldown = 30  # seconds - prevent rapid successive refreshes
        self._current_token_hashes = {}  # Track token changes per user
    
    def _get_user_lock(self, username):
        """Get or create a lock for a specific user."""
        with self._locks_lock:
            if username not in self._refresh_locks:
                self._refresh_locks[username] = threading.Lock()
            return self._refresh_locks[username]
    
    def load_tokens(self, username: str):
        """Load tokens from MongoDB for a specific user."""
        try:
            tokens = user_manager.get_oauth_tokens(username)
            if tokens:
                pass  
            return tokens
        except Exception as e:
            log.warning(f"Failed to load tokens for {username}: {e}")
            return None
    
    def save_tokens(self, tokens, username: str):
        """Save tokens to MongoDB for a specific user."""
        try:
            if user_manager.update_oauth_tokens(username, tokens):
                log.debug(f"OAuth tokens saved for user {username}")
            else:
                log.error(f"Failed to save tokens for user {username}")
        except Exception as e:
            log.error(f"Failed to save tokens for {username}: {e}")
    
    def get_valid_access_token(self, username: str):
        """Get a valid access token, refreshing if necessary."""
        tokens = self.load_tokens(username)
        if not tokens:
            raise Exception(f"No tokens available for {username}. Please authenticate.")
        
        # Check if token is expired or about to expire (1 minute buffer)
        if 'expires_at' in tokens and time.time() >= (tokens['expires_at'] - 60):
            return self.refresh_access_token(username)
        
        return tokens.get('access_token')
    
    def is_token_about_to_expire(self, username: str, buffer_seconds=300):
        """Check if token will expire within buffer time (5 minutes default)"""
        tokens = self.load_tokens(username)
        if not tokens or 'expires_at' not in tokens:
            return True
        
        expires_at = tokens.get('expires_at', 0)
        current_time = time.time()
        
        will_expire = (expires_at - current_time) <= buffer_seconds
        if will_expire:
            log.debug("Token will expire in %.1f seconds", expires_at - current_time)
        
        return will_expire
    
    def has_token_changed(self, username: str):
        """Check if token has been refreshed by another component"""
        try:
            tokens = self.load_tokens(username)
            if not tokens:
                return False
                
            current_hash = hash(tokens.get('access_token', ''))
            
            if username not in self._current_token_hashes:
                self._current_token_hashes[username] = current_hash
                return False
            
            if current_hash != self._current_token_hashes[username]:
                log.info(f"Token change detected for {username} (refreshed by another component)")
                self._current_token_hashes[username] = current_hash
                return True
                
            return False
        except Exception as e:
            log.warning(f"Error checking token change for {username}: {e}")
            return False
    
    def refresh_access_token(self, username: str, force=False):
        """Thread-safe refresh of access token with cooldown protection"""
        refresh_lock = self._get_user_lock(username)
        
        with refresh_lock:
            current_time = time.time()
            last_refresh = self._last_refresh_times.get(username, 0)
            
            # Prevent rapid successive refreshes unless forced
            if not force and (current_time - last_refresh) < self._refresh_cooldown:
                log.debug(f"Token refresh skipped for {username} - within cooldown period ({current_time - last_refresh:.1f}s ago)")
                tokens = self.load_tokens(username)
                return tokens.get('access_token') if tokens else None
            
            # Check if another component already refreshed the token
            if not force and not self.is_token_about_to_expire(username, buffer_seconds=60):
                log.debug(f"Token refresh skipped for {username} - token still valid")
                tokens = self.load_tokens(username)
                return tokens.get('access_token') if tokens else None
            
            return self._do_token_refresh(username)
    
    def _do_token_refresh(self, username: str):
        """Internal method to perform the actual token refresh"""
        tokens = self.load_tokens(username)
        if not tokens or not tokens.get('refresh_token'):
            raise Exception(f"No refresh token available for {username}. Please re-authorize.")
        
        log.debug(f"Refreshing access token for {username}...")
        
        try:
            data = {
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET,
                'refresh_token': tokens['refresh_token'],
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(TOKEN_URL, data=data)
            
            if response.status_code == 200:
                new_tokens = response.json()
                
                # Keep the existing refresh token if not provided
                if 'refresh_token' not in new_tokens:
                    new_tokens['refresh_token'] = tokens['refresh_token']
                
                # Preserve additional fields from original tokens
                for field in ['username', 'full_name']:
                    if field in tokens:
                        new_tokens[field] = tokens[field]
                
                expires_in = new_tokens.get('expires_in', 3600)
                new_tokens['expires_at'] = time.time() + expires_in
                new_tokens['created_at'] = time.time()
                
                self.save_tokens(new_tokens, username)
                self._last_refresh_times[username] = time.time()
                
                log.info(f"Token refreshed for {username} (expires: {expires_in//3600}h {(expires_in%3600)//60}m)")
                return new_tokens['access_token']
            else:
                log.error(f"Token refresh failed for {username}: {response.text}")
                raise Exception(f"Token refresh failed: {response.status_code}")
                
        except Exception as e:
            log.error(f"Token refresh error for {username}: {e}")
            raise Exception(f"Token refresh failed: {e}")
    
    def get_dynamic_username(self, username: str):
        """Get the dynamic username (just return the provided username in multi-tenant)"""
        return username
    
    def has_valid_tokens(self, username: str):
        """Check if user has valid tokens"""
        user = user_manager.get_user(username)
        if user and user.get('oauth_tokens'):
            log.info(f"Found existing tokens for {username}")
            return True
        else:
            log.debug(f"No tokens found for {username}")
            return False
    
    def test_user_connection(self, username: str) -> bool:
        """Test IMAP connection for a user."""
        try:
            access_token = self.get_valid_access_token(username)
            if not access_token:
                return False
            
            import imaplib
            imap = imaplib.IMAP4_SSL(IMAP_SERVER)
            auth_string = f"user={username}\x01auth=Bearer {access_token}\x01\x01"
            imap.authenticate("XOAUTH2", lambda x: auth_string.encode("utf-8"))
            
            status, _ = imap.select("INBOX", readonly=True)
            imap.logout()
            
            if status == "OK":
                log.debug(f"IMAP connection test successful for {username}")
                return True
            else:
                log.error(f"IMAP connection test failed for {username} with status: {status}")
                return False
                
        except Exception as e:
            log.error(f"IMAP connection test failed for {username}: {e}")
            return False
    
    def refresh_tokens(self, username: str):
        """Refresh OAuth tokens and return the updated tokens."""
        try:
            tokens = self.load_tokens(username)
            if not tokens or not tokens.get('refresh_token'):
                log.error(f"No refresh token available for {username}")
                return None
            
            # Use the refresh_access_token method which handles the actual refresh
            access_token = self.refresh_access_token(username, force=True)
            if access_token:
                # Return the updated tokens
                return self.load_tokens(username)
            else:
                log.error(f"Failed to refresh tokens for {username}")
                return None
                
        except Exception as e:
            log.error(f"Error refreshing tokens for {username}: {e}")
            return None

# Global token manager instance
token_manager = TokenManager()

# Multi-tenant functions

def start_oauth_server():
    """Start the OAuth server in a background thread."""
    global oauth_server_thread, oauth_server_active, oauth_app
    
    if oauth_server_active:
        log.info("OAuth server already running")
        return True
    
    try:
        oauth_app = create_oauth_server()
        
        def run_server():
            global oauth_server_active
            oauth_server_active = True
            # Bind to localhost only to avoid exposing the OAuth callback externally
            oauth_app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
        
        oauth_server_thread = threading.Thread(target=run_server, daemon=True)
        oauth_server_thread.start()
        
        # Give server time to start
        time.sleep(2)
        
        log.info("Multi-tenant OAuth server started successfully!")
        log.info("Users can authenticate at: http://localhost:5000")
        
        return True
        
    except Exception as e:
        log.error(f"Failed to start OAuth server: {e}")
        return False

def stop_oauth_server():
    """Stop the OAuth server."""
    global oauth_server_active
    oauth_server_active = False
    log.info("OAuth server stopped")

def create_oauth_server():
    """Create Flask server for OAuth callback."""
    app = Flask(__name__)
    app.logger.disabled = True
    
    @app.route('/')
    def home():
        """Redirect directly to Google OAuth page"""
        # Ensure scopes are properly formatted as space-separated string
        scopes = ' '.join(scope.strip() for scope in SCOPES.split(','))
        
        params = {
            'client_id': CLIENT_ID,
            'response_type': 'code',
            'redirect_uri': REDIRECT_URI,
            'scope': scopes,
            'access_type': 'offline',
            'prompt': 'consent',
            'include_granted_scopes': 'true'
        }
        oauth_url = f"{AUTH_URL}?{urlencode(params)}"
        return redirect(oauth_url)
    
    @app.route('/callback')
    def callback():
        global oauth_complete
        code = request.args.get("code")
        error = request.args.get("error")
        
        log.info("OAuth callback received")
        
        if error:
            log.error(f"OAuth error received: {error}")
            return f"<h1>OAuth Error</h1><p>{error}</p>", 400
        
        if not code:
            return "<h1>Error</h1><p>No authorization code received</p>", 400

        token_data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code": code,
            "grant_type": "authorization_code"
        }

        try:
            response = requests.post(TOKEN_URL, data=token_data)
            response.raise_for_status()
            token_json = response.json()

            if "access_token" not in token_json:
                return f"<h1>Token Error</h1><p>{token_json.get('error_description', 'Unknown error')}</p>", 400

            expires_in = token_json.get('expires_in', 3600)
            expires_at = time.time() + expires_in

            # Extract email from OpenID token (with openid scope, email is in the token response)
            username = None
            try:
                # With OpenID scopes, we can decode the ID token to get email
                import base64
                import json
                
                # First try to get email from userinfo endpoint (more reliable)
                headers = {'Authorization': f'Bearer {token_json["access_token"]}'}
                
                # Try multiple userinfo endpoints with retry logic
                userinfo_endpoints = [
                    'https://www.googleapis.com/oauth2/v2/userinfo',
                    'https://openidconnect.googleapis.com/v1/userinfo'
                ]
                
                username = None
                for endpoint in userinfo_endpoints:
                    for attempt in range(3):  # 3 retry attempts
                        try:
                            log.debug(f"Trying userinfo endpoint: {endpoint} (attempt {attempt + 1})")
                            userinfo_response = requests.get(
                                endpoint, 
                                headers=headers, 
                                timeout=10,
                                verify=True
                            )
                            
                            if userinfo_response.status_code == 200:
                                user_info = userinfo_response.json()
                                if user_info.get('email'):
                                    username = user_info['email']

                                    full_name = user_info.get('name', '')
                                    log.info("User info extracted - Email: %s, Name: %s", username, full_name)
                                    break
                                else:
                                    log.warning("No email found in userinfo response")
                            else:
                                log.warning(f"Userinfo endpoint returned status: {userinfo_response.status_code}")
                                
                        except requests.exceptions.RequestException as req_err:
                            log.warning(f"Request error on attempt {attempt + 1}: {req_err}")
                            if attempt < 2:  # Don't sleep on last attempt
                                time.sleep(2)  # Wait 2 seconds before retry
                        except Exception as endpoint_err:
                            log.warning(f"Unexpected error on attempt {attempt + 1}: {endpoint_err}")
                            
                    if username:  
                        break
                
                if not username:
                    log.error("Failed to extract email from all userinfo endpoints")
                    return "<h1>Error</h1><p>Could not extract email from Google account</p>", 400
                    
            except Exception as e:
                log.error("Failed to extract email from OAuth: %s", e)
                return f"<h1>Error</h1><p>Failed to extract email: {str(e)}</p>", 500
            
            if not username:
                log.error("No username extracted from OAuth")
                return "<h1>Error</h1><p>Could not determine email address</p>", 400
    
            tokens = {
                'access_token': token_json['access_token'],
                'refresh_token': token_json.get('refresh_token'),
                'expires_at': expires_at,
                'created_at': time.time(),
                'username': username,  # Store dynamic username
                'full_name': full_name if 'full_name' in locals() else ''  # Store user's full name if available
            }

            # Add user to multi-tenant system
            try:
                user_id = user_manager.add_user(username, tokens, full_name)
                log.info(f"User {username} added to multi-tenant system (Sequential ID: {user_id})")
            except Exception as e:
                log.error(f"Failed to add user to system: {e}")
                return f"<h1>Error</h1><p>Failed to add user to system: {str(e)}</p>", 500
            
            test_result = token_manager.test_user_connection(username)
            
            return f"""
            <html>
            <head><title>OAuth Complete</title></head>
            <body>
                <h1>IMAP Test: {'Successful' if test_result else 'Failed'}</h1>
                <h2>Status: Email monitoring started</h2>
                <p>You can close this window.</p>
            </body>
            </html>
            """

        except Exception as e:
            return f"<h1>Error</h1><p>Failed to exchange code: {str(e)}</p>", 500

    return app

def get_gmail_service(username: str):
    """Create Gmail API service object for a specific user with automatic token refresh."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        import time
        
        # Get user data including OAuth credentials
        user_data = user_manager.get_user(username)
        if not user_data:
            raise Exception(f"User not found: {username}")
        
        tokens = token_manager.load_tokens(username)
        if not tokens:
            raise Exception(f"No tokens available for {username}")
        
        # Check if token is expired and refresh if needed
        expires_at = tokens.get('expires_at', 0)
        current_time = time.time()
        
        if current_time >= expires_at - 300:  # Refresh 5 minutes before expiry
            log.info(f"Token expired/expiring for {username}, attempting refresh...")
            try:
                # Refresh tokens
                refreshed_tokens = token_manager.refresh_tokens(username)
                if refreshed_tokens:
                    tokens = refreshed_tokens
                    log.info(f"Successfully refreshed tokens for {username}")
                else:
                    log.warning(f"Failed to refresh tokens for {username}, using existing tokens")
            except Exception as refresh_error:
                log.error(f"Token refresh failed for {username}: {refresh_error}")
                # Continue with existing tokens as fallback
        
        # Get user-specific credentials, fallback to config defaults
        client_id = user_data.get('client_id') or CLIENT_ID
        client_secret = user_data.get('client_secret') or CLIENT_SECRET
        
        # Use scopes from config only
        scopes = SCOPES.split(',') if isinstance(SCOPES, str) else SCOPES
        
        creds = Credentials(
            token=tokens.get('access_token'),
            refresh_token=tokens.get('refresh_token'),
            token_uri=TOKEN_URL,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes
        )
        
        # Test if credentials are valid by refreshing if needed
        if creds.expired and creds.refresh_token:
            try:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
                # Save refreshed tokens back to database
                updated_tokens = {
                    'access_token': creds.token,
                    'refresh_token': creds.refresh_token,
                    'expires_at': creds.expiry.timestamp() if creds.expiry else time.time() + 3600,
                    'created_at': time.time(),
                    'username': username,
                    'full_name': tokens.get('full_name', '')
                }
                token_manager.save_tokens(username, updated_tokens)
                log.info(f"Refreshed and saved new tokens for {username}")
            except Exception as e:
                log.error(f"Failed to refresh credentials for {username}: {e}")
                raise

        service = build('gmail', 'v1', credentials=creds)
        return service
    except Exception as e:
        log.error(f"Failed to create Gmail service for {username}: {e}")
        return None
