from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os

def write_results(results, debug=True):
    INFLUXDB_URL = os.getenv("INFLUXDB_URL")
    INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
    INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
    INFLUXDB_MEASUREMENT = os.getenv("INFLUXDB_MEASUREMENT")
    INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

    print(f"Total results to write: {len(results)}")
    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as _client:
        with _client.write_api(write_options=SYNCHRONOUS) as _write_client:
            for idx, row in enumerate(results, 1):
                p = Point(INFLUXDB_MEASUREMENT)
                
                # Add all tags
                for tag in row['tags']:
                    p = p.tag(tag, row['tags'][tag])
                
                # Add all fields with type checking
                for field, value in row['fields'].items():
                    if isinstance(value, (int, float, str)):
                        p = p.field(field, value)
                    else:
                        print(f"Skipping field {field} with unexpected type: {type(value)}")
                
                # Set timestamp
                p = p.time(row['timestamp'])
                
                if debug:
                    print(f"Preparing point {idx}: {p}")
                    print(f"Point timestamp: {row['timestamp']}")
                    if "activity_steps" in row['fields']:
                        print(f"Activity point {idx} timestamp: {row['timestamp']}")
                        print(f"Activity point details: {row}")
                
                try:
                    response = _write_client.write(INFLUXDB_BUCKET, INFLUXDB_ORG, p)
                    if debug:
                        print(f"Successfully wrote point {idx}")
                        if "activity_steps" in row['fields']:
                            print(f"Successfully wrote activity point {idx}")
                            print(f"InfluxDB response: {response}")
                except Exception as e:
                    print(f"Failed to write point {idx}: {e}")
                    print(f"Point details: {p}")
                    if "activity_steps" in row['fields']:
                        print(f"Failed to write activity point {idx}: {e}")
                        print(f"Activity point details: {p}")
