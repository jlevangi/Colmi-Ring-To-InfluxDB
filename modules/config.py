import os
from dotenv import load_dotenv

def load_config():
    load_dotenv()  # Load environment variables from .env file

    # Ensure required environment variables are set
    required_vars = ["LOCAL_PATH", "INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_MEASUREMENT", "INFLUXDB_BUCKET"]
    for var in required_vars:
        if not os.getenv(var):
            raise EnvironmentError(f"Error: {var} not set in environment")
