import subprocess
import sys
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("main")

def run_dashboard1():
    """Run Dashboard 1 - Intent Analysis"""
    logger.info("Starting Dashboard 1 - Intent Analysis")
    result = subprocess.run([sys.executable, "dashboards/dashboard1.py"])
    if result.returncode == 0:
        logger.info("Dashboard 1 completed successfully")
    else:
        logger.error(f"Dashboard 1 failed with return code {result.returncode}")
    return result.returncode

def run_dashboard2():
    """Run Dashboard 2 - Aging Report"""
    logger.info("Starting Dashboard 2 - Aging Report")
    result = subprocess.run([sys.executable, "dashboards/dashboard2.py"])
    if result.returncode == 0:
        logger.info("Dashboard 2 completed successfully")
    else:
        logger.error(f"Dashboard 2 failed with return code {result.returncode}")
    return result.returncode

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("Gmail Unreplied Email Analytics - Dashboard Pipeline")
    logger.info("=" * 70)
    
    # Run Dashboard 1 (Intent Analysis)
    dashboard1_status = run_dashboard1()
    
    # Run Dashboard 2 (Aging Report) only if Dashboard 1 succeeded
    if dashboard1_status == 0:
        dashboard2_status = run_dashboard2()
        
        if dashboard2_status == 0:
            logger.info("=" * 70)
            logger.info("All dashboards completed successfully!")
            logger.info("Data stored in MongoDB")
            logger.info("=" * 70)
        else:
            logger.error("Dashboard 2 failed. Check logs for details.")
            sys.exit(1)
    else:
        logger.error("Dashboard 1 failed. Skipping Dashboard 2.")
        sys.exit(1)






