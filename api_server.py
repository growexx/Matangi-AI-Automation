import os
import json
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import configparser

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_server")

# Initialize FastAPI app
app = FastAPI(
    title="Gmail Unreplied Emails API",
    description="Production API for unreplied email analytics data",
    version="1.0.0"
)

# Add CORS middleware for MERN team
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Helper Functions
class APIHelpers:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        
    def get_json_file_path(self, dashboard="dashboard1"):
        """Get path to the JSON data file for specified dashboard"""
        if dashboard == "dashboard1":
            filename = self.config.get('api', 'dashboard1_json_file')
            return os.path.join('data', 'dashboard1', filename)
        elif dashboard == "dashboard2":
            filename = self.config.get('api', 'dashboard2_json_file')
            return os.path.join('data', 'dashboard2', filename)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown dashboard: {dashboard}")
    
    def load_json_data(self, dashboard="dashboard1"):
        """Load JSON data from file with error handling"""
        json_file = self.get_json_file_path(dashboard)
        
        if not os.path.exists(json_file):
            raise HTTPException(
                status_code=404, 
                detail=f"Data file {json_file} not found. Please run {dashboard}.py first."
            )
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return data
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail=f"Invalid JSON data file: {json_file}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

# Initialize API helpers
api_helpers = APIHelpers()

# API Endpoints
@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "service": "Gmail Unreplied Emails API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "unreplied_emails": "/unreplied-emails",
            "aging_report": "/aging-report",
        }
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Gmail Unreplied Emails API"
    }

@app.get("/unreplied-emails", tags=["Data"])
async def get_unreplied_emails():
    """
    Get Intent Analysis (Dashboard 1)
    Returns category-wise unreplied email counts per user
    """
    try:
        data = api_helpers.load_json_data("dashboard1")
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/aging-report", tags=["Data"])
async def get_aging_report():
    """
    Get Aging Report (Dashboard 2)
    Returns time bucket counts of unreplied emails per user
    """
    try:
        data = api_helpers.load_json_data("dashboard2")
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def run_api_server():
    """Run FastAPI server"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    host = config.get('api', 'host')
    port = int(config.get('api', 'port'))
    
    logger.info("Starting Gmail Unreplied Emails API Server...")

    try:
        uvicorn.run(app, host=host, port=8080, log_level="info")
    except Exception as e:
        logger.error(f"Error running FastAPI server: {e}")

if __name__ == "__main__":
    run_api_server()
