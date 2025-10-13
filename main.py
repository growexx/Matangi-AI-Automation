import subprocess
import sys

def run_dashboard1():
    subprocess.run([sys.executable, "dashboards/dashboard1.py"])

def run_dashboard2():
    subprocess.run([sys.executable, "dashboards/dashboard2.py"])

def run_api_server():
    subprocess.run([sys.executable, "api_server.py"])

if __name__ == "__main__":
    run_dashboard1()
    run_dashboard2()
    run_api_server()
