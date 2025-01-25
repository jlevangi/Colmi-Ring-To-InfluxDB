import os
import time

def monitor_file(file_path, sync_function, poll_interval=1):
    last_modified = 0
    print(f"Starting file monitoring for: {file_path}")

    while True:
        try:
            if not os.path.exists(file_path):
                print(f"File {file_path} does not exist. Waiting...")
                time.sleep(poll_interval)
                continue

            current_modified = os.path.getmtime(file_path)
            
            if current_modified != last_modified:
                print(f"File {file_path} modified. Running sync job...")
                sync_function()
                last_modified = current_modified
            
            time.sleep(poll_interval)
        
        except Exception as e:
            print(f"Error monitoring file: {e}")
            time.sleep(poll_interval)
