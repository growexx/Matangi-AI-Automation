import os
import sys
import time
import json
import threading
import imaplib
import requests
from flask import Flask, redirect, request
from urllib.parse import urlencode

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from logger.logger_setup import logger as log

# Global variable to signal OAuth completion
oauth_complete = False

class TokenManager:
    def __init__(self):
        self.tokens = None  # Initialize as None to force new OAuth flow
        self._refresh_lock = threading.Lock()
        self._last_refresh_time = 0
        self._refresh_cooldown = 30  # seconds - prevent rapid successive refreshes
        self._current_token_hash = None  # Track token is_token_about_to_expirechanges
    
    def load_tokens(self):
        """Load tokens from file if they exist and are valid."""
        try:
            if os.path.exists(config.TOKEN_FILE):
                with open(config.TOKEN_FILE, 'r') as f:
                    tokens = json.load(f)
                # Token loaded (logged once)
                return tokens
        except Exception as e:
            log.warning(f"Failed to load tokens: {e}")
        return None
    
    def save_tokens(self, tokens):
        """Save tokens to file."""
        try:
            # Ensure the directory exists
            token_dir = os.path.dirname(config.TOKEN_FILE)
            if not os.path.exists(token_dir):
                os.makedirs(token_dir, mode=0o700)
            
            # Save tokens with secure permissions
            with open(config.TOKEN_FILE, 'w') as f:
                json.dump(tokens, f, indent=2)
            os.chmod(config.TOKEN_FILE, 0o600)
            
            self.tokens = tokens
            log.debug("OAuth tokens saved")
        except Exception as e:
            log.error(f"Failed to save tokens: {e}")
    
    def get_valid_access_token(self):
        """Get a valid access token, refreshing if necessary."""
        if not self.tokens:
            raise Exception("No tokens available. Please run OAuth setup.")
        
        if 'expires_at' in self.tokens and time.time() >= (self.tokens['expires_at'] - 60):
            return self.refresh_access_token()
        
        return self.tokens.get('access_token')
    
    def is_token_about_to_expire(self, buffer_seconds=300):
        """Check if token will expire within buffer time (5 minutes default)"""
        tokens = self.load_tokens()
        if not tokens or 'expires_at' not in tokens:
            return True
        
        expires_at = tokens.get('expires_at', 0)
        current_time = time.time()
        
        will_expire = (expires_at - current_time) <= buffer_seconds
        if will_expire:
            log.debug("Token will expire in %.1f seconds", expires_at - current_time)
        
        return will_expire
    
    def has_token_changed(self):
        """Check if token has been refreshed by another component"""
        try:
            tokens = self.load_tokens()
            if not tokens:
                return False
                
            current_hash = hash(tokens.get('access_token', ''))
            
            if self._current_token_hash is None:
                self._current_token_hash = current_hash
                return False
            
            if current_hash != self._current_token_hash:
                log.info("Token change detected (refreshed by another component)")
                self._current_token_hash = current_hash
                return True
                
            return False
        except Exception as e:
            log.warning("Error checking token change: %s", e)
            return False
    
    def refresh_access_token(self, force=False):
        """Thread-safe refresh of access token with cooldown protection"""
        with self._refresh_lock:
            current_time = time.time()
            
            # Prevent rapid successive refreshes unless forced
            if not force and (current_time - self._last_refresh_time) < self._refresh_cooldown:
                log.debug("Token refresh skipped - within cooldown period (%.1fs ago)", 
                         current_time - self._last_refresh_time)
                return self.load_tokens()
            
            # Check if another component already refreshed the token
            if not force and not self.is_token_about_to_expire(buffer_seconds=60):
                log.debug("Token refresh skipped - token still valid")
                return self.load_tokens()
            
            return self._do_token_refresh()
    
    def _do_token_refresh(self):
        """Internal method to perform the actual token refresh"""
        tokens = self.load_tokens()
        if not tokens or not tokens.get('refresh_token'):
            raise Exception("No refresh token available. Please re-authorize.")
        
        log.debug("Refreshing access token...")
        
        try:
            data = {
                'client_id': config.CLIENT_ID,
                'client_secret': config.CLIENT_SECRET,
                'refresh_token': tokens['refresh_token'],
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(config.TOKEN_URL, data=data)
            
            if response.status_code == 200:
                new_tokens = response.json()
                
                # Keep the existing refresh token if not provided
                if 'refresh_token' not in new_tokens:
                    new_tokens['refresh_token'] = tokens['refresh_token']
                
                # Preserve the username from original tokens
                if 'username' in tokens:
                    new_tokens['username'] = tokens['username']
                
                expires_in = new_tokens.get('expires_in', 3600)
                new_tokens['expires_at'] = time.time() + expires_in
                new_tokens['created_at'] = time.time()
                
                self.save_tokens(new_tokens)
                self._last_refresh_time = time.time()
                
                log.info("Token refreshed (expires: %dh %dm)", expires_in//3600, (expires_in%3600)//60)
                return new_tokens['access_token']
            else:
                log.error("Token refresh failed: %s", response.text)
                raise Exception(f"Token refresh failed: {response.status_code}")
                
        except Exception as e:
            log.error("Token refresh error: %s", e)
            raise Exception(f"Token refresh failed: {e}")
    
    def get_dynamic_username(self):
        """Get the dynamic username from stored tokens"""
        tokens = self.load_tokens()
        if tokens and tokens.get('username'):
            log.debug("Using dynamic username: %s", tokens['username'])
            return tokens['username']
        
        # Fallback to config if no dynamic username
        log.warning("No dynamic username found, using config default: %s", config.USERNAME)
        return config.USERNAME
    
    def force_refresh_tokens(self):
        """Always use fresh OAuth flow on pipeline restart for completely fresh tokens"""
        log.debug("Checking for existing tokens...")
        self.tokens = None  # Clear cached tokens
        existing_tokens = self.load_tokens()  # Load from file
        
        if existing_tokens:
            log.debug("forcing fresh OAuth for latest tokens")
            return False  # Always return False to trigger fresh OAuth flow
        else:
            log.debug("No existing tokens found")
            return False  # No tokens, trigger OAuth flow
    
    def update_config_username(self, username):
        """Update the username in config.ini file dynamically"""
        import configparser
        import os
        
        try:
            # Path to config file
            config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.ini')
            
            config_parser = configparser.ConfigParser()
            config_parser.read(config_path)
            
            config_parser.set('imap', 'username', username)
            
            # Write back to file
            with open(config_path, 'w') as config_file:
                config_parser.write(config_file)
            
            log.info("Config updated with dynamic username: %s", username)
            
            # Also update the runtime config module
            config.USERNAME = username
            
        except Exception as e:
            log.error("Failed to update config with username: %s", e)

# Global token manager instance
token_manager = TokenManager()

def create_oauth_server():
    """Create Flask server for OAuth callback."""
    app = Flask(__name__)
    app.logger.disabled = True
    
    @app.route('/')
    def home():
        """Redirect directly to Google OAuth page"""
        # Ensure scopes are properly formatted as space-separated string
        scopes = ' '.join(scope.strip() for scope in config.SCOPES.split(','))
        
        params = {
            'client_id': config.CLIENT_ID,
            'response_type': 'code',
            'redirect_uri': config.REDIRECT_URI,
            'scope': scopes,
            'access_type': 'offline',
            'prompt': 'consent',
            'include_granted_scopes': 'true'
        }
        oauth_url = f"{config.AUTH_URL}?{urlencode(params)}"
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
            "client_id": config.CLIENT_ID,
            "client_secret": config.CLIENT_SECRET,
            "redirect_uri": config.REDIRECT_URI,
            "code": code,
            "grant_type": "authorization_code"
        }

        try:
            response = requests.post(config.TOKEN_URL, data=token_data)
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
                                    log.info("Dynamic username extracted from userinfo: %s", username)
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
                            
                    if username:  # If we got a username, break out of endpoint loop
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
                'username': username  # Store dynamic username
            }

            token_manager.save_tokens(tokens)
            
            # Update config.ini with the dynamic username
            if username != config.USERNAME:
                token_manager.update_config_username(username)
            
            test_result = test_imap_connection(tokens['access_token'], username)
            
            oauth_complete = True

            return f"""
            <html>
            <head><title>OAuth Complete</title></head>
            <body>
                <h1>OAuth Setup Completed!</h1>
                <p><strong>IMAP Test:</strong> {test_result}</p>
            </body>
            </html>
            """

        except Exception as e:
            return f"<h1>Error</h1><p>Failed to exchange code: {str(e)}</p>", 500

    return app

def test_imap_connection(access_token, username=None):
    """Test IMAP connection with access token."""
    try:
        imap = imaplib.IMAP4_SSL(config.IMAP_SERVER)
        # Use provided username or get from token manager
        test_username = username or token_manager.get_dynamic_username()
        auth_string = f"user={test_username}\x01auth=Bearer {access_token}\x01\x01"
        imap.authenticate("XOAUTH2", lambda x: auth_string.encode("utf-8"))
        
        status, _ = imap.select("INBOX", readonly=True)
        imap.logout()
        
        if status == "OK":
            return "Successful"
        else:
            return f"Failed (Status: {status})"
    except Exception as e:
        return f"Failed ({str(e)})"

def run_oauth_flow():
    """Run the OAuth flow to get initial tokens."""
    global oauth_complete
    oauth_complete = False
    
    log.info("Starting OAuth setup...")
    
    auth_params = {
        'client_id': config.CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': config.REDIRECT_URI,
        'scope': config.SCOPES,
        'access_type': 'offline',
        'prompt': 'consent'
    }
    auth_url = f"{config.AUTH_URL}?{urlencode(auth_params)}"
    
    app = create_oauth_server()
    server_thread = threading.Thread(
        target=lambda: app.run(port=5000, debug=False, use_reloader=False),
        daemon=True
    )
    server_thread.start()
    
    log.info("OAuth server started successfully!")    
    log.info("Waiting for OAuth completion...")
    
    timeout = 900  
    start_time = time.time()
    
    while not oauth_complete and (time.time() - start_time) < timeout:
        time.sleep(1)
    
    if oauth_complete:
        log.info("OAuth authentication successful!")
        
        # CRITICAL: Force token manager to reload fresh tokens immediately
        token_manager.tokens = None  # Clear any cached tokens
        fresh_tokens = token_manager.load_tokens()  # Load fresh tokens from file
        if fresh_tokens:
            token_manager.tokens = fresh_tokens
            # Test the fresh tokens immediately
            try:
                access_token = token_manager.get_valid_access_token()
            except Exception as e:
                log.error(f"Token validation failed: {e}")
                return False
        else:
            log.error("Failed to load fresh tokens!")
            return False
            
        return True
    else:
        log.error("OAuth timed out. Please try again.")
        return False
