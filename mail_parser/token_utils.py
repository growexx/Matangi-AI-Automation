import os
import config

def ensure_token_directory():
    """Ensure the directory for token file exists"""
    token_dir = os.path.dirname(config.TOKEN_FILE)
    if not os.path.exists(token_dir):
        os.makedirs(token_dir)  
    return token_dir