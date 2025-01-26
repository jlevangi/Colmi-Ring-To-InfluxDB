#!/usr/bin/env python3
import os
import sys
import argparse
import shutil
from modules.config import load_config
from modules.database import fetch_database, open_database
from modules.extractor import extract_data
from modules.writer import write_results
from modules.monitor import monitor_file

load_config()  # Load environment variables from .env file

if __name__ == "__main__":
    print("Starting script...")
    
    parser = argparse.ArgumentParser(description="Process smart ring stats and write to InfluxDB.")
    parser.add_argument("--now", action="store_true", help="Start the process immediately instead of waiting for a file update")
    parser.add_argument("--debug", action="store_true", help="Print detailed debug information")
    args = parser.parse_args()

    if not os.getenv("INFLUXDB_URL"):
        print("Error: INFLUXDB_URL not set in environment")
        sys.exit(1)

    LOCAL_PATH = os.getenv("LOCAL_PATH")
    print("Checking LOCAL_PATH directory...")
    if not os.path.exists(os.path.dirname(LOCAL_PATH)):
        print(f"Error: Directory {os.path.dirname(LOCAL_PATH)} does not exist")
        sys.exit(1)

    def run_sync_job(debug=False):
        tempdir = fetch_database()
        print(f"Fetched database to temporary directory: {tempdir}")
        conn, cur = open_database(tempdir)
        print("Opened database connection")

        results = extract_data(cur, debug)
        if not results:
            print("Data extraction failed")
            return

        print(f"Extracted {len(results)} data points")
        write_results(results, debug)
        
        conn.close()
        print("Closed database connection")
        if tempdir not in ["/", ""] and os.getenv("REMOVE_TEMP_DB") == "Y":
            shutil.rmtree(tempdir)
            print(f"Removed temporary directory: {tempdir}")
        elif os.getenv("REMOVE_TEMP_DB") == "N":
            print(f"Temporary directory retained: {tempdir}")

    if args.now:
        print("Starting sync job immediately...")
        run_sync_job(args.debug)
    else:
        print("Starting file monitoring...")
        monitor_file(LOCAL_PATH, lambda: run_sync_job(args.debug))
    
    print("Script terminated.")