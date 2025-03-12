import os
import zipfile
import socket
import json
import sys
import time

# Configurations
ZIP_FILE = "insert.zip"  # Name of the zip file
BOOTSTRAP_IP = "127.0.0.1"  # Localhost for testing
BOOTSTRAP_PORT = 5000  # Default bootstrap node port

# Function to check if the bootstrap node is available
def check_bootstrap():
    """Checks if the bootstrap node is available."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((BOOTSTRAP_IP, BOOTSTRAP_PORT))
            return True
    except (socket.error, ConnectionRefusedError):
        return False

# Step 1: Unzip the file
def unzip_files(zip_file):
    """Extracts the ZIP file in the current directory."""
    if not os.path.exists(zip_file):
        print(f"[ERROR] {zip_file} not found!")
        return []
    
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall("unzipped_files")  # Extract into a folder
    return sorted(os.listdir("unzipped_files"))  # Return sorted file names

# Step 2: Send insertion request to Bootstrap Node
def insert_into_dht(key, value):
    """Sends an insert request to the Bootstrap Node, which determines the target DHT node."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((BOOTSTRAP_IP, BOOTSTRAP_PORT))
            request = {"type": "insert", "key": key, "value": value}
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(4096).decode())
            print(f"[INSERT] {key}: {value} -> {response.get('status', 'No response')}")
    except Exception as e:
        print(f"[ERROR] Failed to insert {key}: {e}")

# Step 3: Process files and insert contents
def process_files():
    files = unzip_files(ZIP_FILE)
    if not files:
        return
    
    start_time_total = time.time()
    for file_to_process in files:
        print(f"[PROCESS] Sending data from {file_to_process} to Bootstrap Node")
        start_time_file = time.time()
        with open(os.path.join("unzipped_files", file_to_process), "r") as f:
            for line in f:
                song = line.strip()
                if song:
                    insert_into_dht(song, song)
        print(f"[TIME] {file_to_process} processed in {time.time() - start_time_file:.4f} seconds")

    print(f"\n[TOTAL TIME] All requests processed in {time.time() - start_time_total:.4f} seconds")

if __name__ == "__main__":
    if not check_bootstrap():
        print("[ERROR] Bootstrap Node is not active! Exiting...")
        sys.exit(1)
    
    print("[INFO] Bootstrap Node is active. Proceeding with file processing...")
    process_files()

