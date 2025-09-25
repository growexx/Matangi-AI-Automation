import sys
import os
import argparse
from config import *
from logger.logger_setup import logger as log
from mail_parser.auth_handler import token_manager, run_oauth_flow, test_imap_connection
from mail_parser.imap_monitor import start_email_monitor

def main():
    """Main function - handles OAuth setup and starts monitoring."""
    parser = argparse.ArgumentParser(description='Matangi Email Automation Pipeline')
    parser.add_argument('--web', action='store_true', help='Start web server on port 5050')
    parser.add_argument('--port', type=int, default=5050, help='Web server port (default: 5050)')
    parser.add_argument('--fresh-auth', action='store_true',
                       help='Force fresh OAuth authentication (deletes existing tokens)')
    args = parser.parse_args()
    
    if args.web:
        log.info("Starting Matangi Email Automation Web Server")
        log.info("="*50)
        log.info(f"Web Dashboard will be available at: http://localhost:{args.port}")
        log.info("="*50)
        
        # Import and start web server
        try:
            from pipeline.web_server import app
            app.run(host='0.0.0.0', port=args.port, debug=False)
        except ImportError:
            log.error("Flask not installed. Install with: pip install flask")
            exit(1)
        except Exception as e:
            log.error(f"Web server error: {e}")
            exit(1)
        return
    
    log.info("Matangi Email Automation Starting...")
    
    # Check config variables
    if not CLIENT_ID or CLIENT_ID == "your-google-client-id":
        log.error("Client ID not configured in config.ini")
        exit(1)
    
    if not CLIENT_SECRET or CLIENT_SECRET == "your-google-client-secret":
        log.error("Client secret not configured in config.ini")
        exit(1)
    
    log.debug("OAuth credentials configured")
    
    # Check if we should force fresh OAuth or use existing tokens
    if args.fresh_auth:
        log.info("Forcing fresh OAuth authentication...")
        # Delete existing tokens to force new authentication
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)
            log.info("Deleted existing OAuth tokens")
    
    # Check if valid tokens exist
    if token_manager.force_refresh_tokens():
        start_email_monitor()
    else:
        # Need to run OAuth flow
        log.info("OAuth setup required...")
        try:
            if run_oauth_flow():
                start_email_monitor()
            else:
                log.error("OAuth setup failed")
                exit(1)
        except Exception as e:
            log.error(f"Error during OAuth setup: {e}")
            log.exception("Detailed error:")
            exit(1)

if __name__ == '__main__':
    main()
