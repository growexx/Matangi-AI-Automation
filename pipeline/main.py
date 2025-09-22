import os
from config import *
from logger.logger_setup import logger as log
from mail_parser.auth_handler import token_manager, run_oauth_flow
from mail_parser.imap_monitor import start_email_monitor

def main():
 
    log.info("Email Automation Starting...")
    log.debug("OAuth credentials configured")
    
    # Check if valid tokens exist
    if token_manager.force_refresh_tokens():
        start_email_monitor()
    else:

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
