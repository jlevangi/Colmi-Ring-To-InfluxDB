#!/usr/bin/env python3
#
# Fetch a Gadgetbridge database export from a local drive
# and extract smart ring stats to write into InfluxDB.
#
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import argparse
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

load_dotenv()  # Load environment variables from .env file

### Config section
# Path to the local export file
LOCAL_PATH = os.getenv("LOCAL_PATH")

# How far back in time should we query when extracting stats?
QUERY_DURATION = int(os.getenv("QUERY_DURATION", 86400))

# InfluxDB settings
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_MEASUREMENT = os.getenv("INFLUXDB_MEASUREMENT")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# For testing/debugging only
REMOVE_TEMP_DB = os.getenv("REMOVE_TEMP_DB", "Y")

def fetch_database():
    '''Fetch the database from the local path'''
    if not os.path.exists(LOCAL_PATH):
        print("Error: Export file does not exist")
        sys.exit(1)
        
    tempdir = tempfile.mkdtemp()
    shutil.copy(LOCAL_PATH, f'{tempdir}/gadgetbridge.sqlite')
    return tempdir

def open_database(tempdir):
    '''Open a handle on the database'''
    conn = sqlite3.connect(f"{tempdir}/gadgetbridge.sqlite")
    cur = conn.cursor()
    return conn, cur

def extract_data(cur, debug=False):
    '''Query the database for smart ring data'''
    results = []
    devices = {}
    devices_observed = {}
    query_start_bound = int(time.time()) - QUERY_DURATION
    # Some tables use ms timestamps
    query_start_bound_ms = query_start_bound * 1000

    # Pull out device names (focusing on Colmi ring)
    device_query = "select _id, NAME from DEVICE where NAME LIKE '%Colmi%'"
    try:
        res = cur.execute(device_query)
    except sqlite3.OperationalError as e:
        print("Unable to fetch stats - received an empty database")
        return False
    
    for r in res.fetchall():
        devices[f"dev-{r[0]}"] = r[1]

    if not devices:
        print("No Colmi ring found in database")
        return False

    print("Devices found:", devices)

    # Get stress level data
    print("Querying stress level data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, STRESS FROM COLMI_STRESS_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")
    
    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "stress_level" : r[2]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts

    print("Stress level data points:", len(results))

    # Get battery level data
    print("Querying battery level data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, LEVEL FROM BATTERY_LEVEL "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "battery_level" : r[2]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts

    print("Battery level data points:", len(results))

    # Get sleep session data
    print("Querying sleep session data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, WAKEUP_TIME FROM COLMI_SLEEP_SESSION_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        start_ts = r[0] * 1000000  # Convert to nanoseconds
        wakeup_ts = r[2] * 1000000  # Convert to nanoseconds
        total_sleep_time = wakeup_ts - start_ts  # total sleep time in nanoseconds
        
        # Generate time slept values in 15-minute increments
        increment = 15 * 60 * 1000000000  # 15 minutes in nanoseconds
        current_ts = start_ts
        while current_ts + increment <= wakeup_ts:
            row = {
                    "timestamp": current_ts,
                    "fields" : {
                        "time_slept" : int(increment / 1000000)  # Convert to milliseconds and ensure integer type
                    },
                    "tags" : {
                        "device" : devices[f"dev-{r[1]}"]
                    }
            }
            results.append(row)
            current_ts += increment

        # Add the remaining sleep time
        if current_ts < wakeup_ts:
            remaining_time = wakeup_ts - current_ts
            row = {
                    "timestamp": current_ts,
                    "fields" : {
                        "time_slept" : int(remaining_time / 1000000)  # Convert to milliseconds and ensure integer type
                    },
                    "tags" : {
                        "device" : devices[f"dev-{r[1]}"]
                    }
            }
            results.append(row)

        # Add the final wakeup time point
        row = {
                "timestamp": wakeup_ts,
                "fields" : {
                    "wakeup_time" : wakeup_ts
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < wakeup_ts:
            devices_observed[f"dev-{r[1]}"] = wakeup_ts

    print("Sleep session data points:", len(results))

    # Get sleep stage data
    print("Querying sleep stage data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, STAGE FROM COLMI_SLEEP_STAGE_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "sleep_stage" : r[2]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts

    print("Sleep stage data points:", len(results))

    # Get HRV data
    print("Querying HRV data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, VALUE FROM COLMI_HRV_VALUE_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "hrv_value" : r[2]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts

    print("HRV data points:", len(results))

    # Get activity data
    print("Querying activity data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, STEPS, DISTANCE, CALORIES FROM COLMI_ACTIVITY_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "activity_steps" : r[2],
                    "activity_distance" : r[3],
                    "activity_calories" : r[4]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts

    print("Activity data points:", len(results))
    if debug:
        for row in results:
            if "activity_steps" in row["fields"]:
                print(f"Activity data row: {row}")

    # Get SPO2 data
    print("Querying SPO2 data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, SPO2 FROM COLMI_SPO2_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "spo2" : r[2]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"]
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts

    print("SPO2 data points:", len(results))

    # Get heart rate data
    print("Querying heart rate data...")
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, HEART_RATE FROM COLMI_HEART_RATE_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "AND HEART_RATE > 0 AND HEART_RATE < 254 "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "heart_rate" : r[2]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"],
                    "sample_type" : "periodic_samples"
                }
        }
        results.append(row)
        if f"dev-{r[1]}" not in devices_observed or devices_observed[f"dev-{r[1]}"] < row_ts:
            devices_observed[f"dev-{r[1]}"] = row_ts

    print("Heart rate data points:", len(results))

    # Create a field to record when we last synced
    now = time.time_ns()
    for device in devices_observed:
        row_ts = devices_observed[device]
        row_age = now - row_ts
        row = {
                "timestamp": now,
                "fields" : {
                    "last_seen" : row_ts,
                    "last_seen_age" : row_age
                },
                "tags" : {
                    "device" : devices[device],
                    "sample_type" : "sync_check"
                }
        }
        results.append(row)

    return results

def write_results(results, debug=False):
    print("Connecting to InfluxDB...")
    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as _client:
        with _client.write_api(write_options=SYNCHRONOUS) as _write_client:
            for row in results:
                p = Point(INFLUXDB_MEASUREMENT)
                for tag in row['tags']:
                    p = p.tag(tag, row['tags'][tag])
                    
                for field in row['fields']:
                    if field in ['activity_steps', 'activity_distance', 'activity_calories'] and row['fields'][field] == -1:
                        continue
                    else:
                        p = p.field(field, row['fields'][field])
                    
                p = p.time(row['timestamp'])
                try:
                    _write_client.write(INFLUXDB_BUCKET, INFLUXDB_ORG, p)
                    if debug:
                        print(f"Successfully wrote point: {p}")
                except Exception as e:
                    print(f"Failed to write point: {p}, error: {e}")

                # Write wakeup time as a separate point
                if 'wakeup_time' in row['fields']:
                    p_wakeup = Point(INFLUXDB_MEASUREMENT)
                    p_wakeup = p_wakeup.tag("device", row['tags']['device'])
                    p_wakeup = p_wakeup.field("wakeup_time", row['fields']['wakeup_time'])
                    p_wakeup = p_wakeup.time(row['fields']['wakeup_time'])
                    try:
                        _write_client.write(INFLUXDB_BUCKET, INFLUXDB_ORG, p_wakeup)
                        if debug:
                            print(f"Successfully wrote wakeup point: {p_wakeup}")
                    except Exception as e:
                        print(f"Failed to write wakeup point: {p_wakeup}, error: {e}")

def monitor_file(file_path, sync_function, poll_interval=1):
    last_modified = os.path.getmtime(file_path)
    
    while True:
        try:
            current_modified = os.path.getmtime(file_path)
            
            if current_modified != last_modified:
                print(f"File {file_path} modified. Running sync job...")
                sync_function()
                last_modified = current_modified
            
            time.sleep(poll_interval)
        
        except Exception as e:
            print(f"Error monitoring file: {e}")
            break

def run_sync_job(debug=False):
    tempdir = fetch_database()
    print(f"Fetched database to temporary directory: {tempdir}")
    conn, cur = open_database(tempdir)
    print("Opened database connection")

    # Extract data from the DB
    results = extract_data(cur, debug)
    if not results:
        print("Data extraction failed")
        return

    print(f"Extracted {len(results)} data points")

    # Write out to InfluxDB
    write_results(results, debug)
    
    # Tidy up
    conn.close()
    print("Closed database connection")
    if tempdir not in ["/", ""] and REMOVE_TEMP_DB == "Y":
        shutil.rmtree(tempdir)
        print(f"Removed temporary directory: {tempdir}")
    elif REMOVE_TEMP_DB == "N":
        print(f"Temporary directory retained: {tempdir}")

if __name__ == "__main__":
    print("Starting script...")
    
    parser = argparse.ArgumentParser(description="Process smart ring stats and write to InfluxDB.")
    parser.add_argument("--now", action="store_true", help="Start the process immediately instead of waiting for a file update")
    parser.add_argument("--debug", action="store_true", help="Print detailed debug information")
    args = parser.parse_args()

    if not INFLUXDB_URL:
        print("Error: INFLUXDB_URL not set in environment")
        sys.exit(1)

    print("Checking LOCAL_PATH directory...")
    # Ensure the LOCAL_PATH directory exists
    if not os.path.exists(os.path.dirname(LOCAL_PATH)):
        print(f"Error: Directory {os.path.dirname(LOCAL_PATH)} does not exist")
        sys.exit(1)

    if args.now:
        print("Starting sync job immediately...")
        run_sync_job(args.debug)
    else:
        print("Starting file monitoring...")
        monitor_file(LOCAL_PATH, lambda: run_sync_job(args.debug))
    
    print("Script terminated.")