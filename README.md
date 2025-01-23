# Colmi Ring to InfluxDB

This project fetches a Gadgetbridge database export from a local drive and extracts smart ring stats to write into InfluxDB. The script monitors the database file for changes and runs the sync job whenever the file is modified.

## Prerequisites

- Docker
- Python 3.9+
- InfluxDB instance
- Gadgetbridge database export file

## Setup

1. Clone the repository:
    ```sh
    git clone https://github.com/jlevangi/Colmi-Ring-To-InfluxDB.git
    cd Colmi-Ring-To-InfluxDB
    ```

2. Create a `.env` file in the project root directory and set the following environment variables:
    ```env
    LOCAL_PATH=/path/to/Gadgetbridge.db
    QUERY_DURATION=86400
    INFLUXDB_URL=http://localhost:8086
    INFLUXDB_TOKEN=your-influxdb-token
    INFLUXDB_ORG=your-org
    INFLUXDB_MEASUREMENT=your-measurement
    INFLUXDB_BUCKET=your-bucket
    REMOVE_TEMP_DB=Y
    ```

3. Build the Docker image:
    ```sh
    docker build -t colmi-ring-to-influxdb .
    ```

4. Run the Docker container:
    ```sh
    docker run -d --name colmi-ring-to-influxdb --env-file .env colmi-ring-to-influxdb
    ```

## Usage

The script will monitor the specified `LOCAL_PATH` for changes. Whenever the file is modified, the sync job will run, fetching the database, extracting data, and writing it to InfluxDB.

## Development

To run the script locally without Docker:

1. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

2. Run the script:
    ```sh
    python app.py
    ```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.