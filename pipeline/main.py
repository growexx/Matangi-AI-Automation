import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from logger.logger_setup import logger as log
from mail_parser.auth_handler import token_manager, run_oauth_flow
from mail_parser.imap_monitor import start_email_monitor

def main():
    log.info("Matangi Email Automation Starting...")
    log.debug("OAuth credentials configured")

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
