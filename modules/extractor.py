import time
import os
import sqlite3

def extract_data(cur, debug=False):
    '''Query the database for smart ring data'''
    results = []
    devices = {}
    devices_observed = {}
    query_start_bound = int(time.time()) - int(os.getenv("QUERY_DURATION", 86400))
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
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, LEVEL, BATTERY_INDEX FROM BATTERY_LEVEL "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000000  # Convert to nanoseconds
        row = {
                "timestamp": row_ts,
                "fields" : {
                    "battery_level" : r[2]
                },
                "tags" : {
                    "device" : devices[f"dev-{r[1]}"],
                    "battery" : r[3]
                }
        }
        results.append(row)
        if debug:
            print(f"Extracted battery level data: {row}")
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
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, STAGE, DURATION FROM COLMI_SLEEP_STAGE_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000  # Convert to nanoseconds
        device_id = f"dev-{r[1]}"
        stage = r[2]
        duration = r[3] * 60 * 1000000000  # Convert minutes to nanoseconds
        
        row = {
            "timestamp": row_ts,
            "fields": {
                "sleep_stage": stage,
                "sleep_stage_duration": duration
            },
            "tags": {
                "device": devices[device_id]
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
    data_query = ("SELECT TIMESTAMP, DEVICE_ID, STEPS, CALORIES, DISTANCE, RAW_KIND FROM COLMI_ACTIVITY_SAMPLE "
        f"WHERE TIMESTAMP >= {query_start_bound} "
        "AND DEVICE_ID IN (" + ",".join([k.split('-')[1] for k in devices.keys()]) + ") "
        "ORDER BY TIMESTAMP ASC")

    res = cur.execute(data_query)
    for r in res.fetchall():
        row_ts = r[0] * 1000000000  # Convert to nanoseconds
        row = {
            "timestamp": row_ts,
            "fields": {
                "activity_steps": r[2],
                "activity_calories": r[3],
                "activity_distance": r[4]
            },
            "tags": {
                "device": devices[f"dev-{r[1]}"],
                "activity_kind": r[5],
                "sample_type": "activity"
            }
        }
        results.append(row)
        if debug:
            print(f"Extracted activity data: {row}")
            print(f"Activity data timestamp: {row_ts}")
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
