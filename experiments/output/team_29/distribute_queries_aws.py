import os
import zipfile
import socket
import json
import sys
import time

# Configurations: Set the node's IP & port
ZIP_FILE = "queries.zip"  # Name of the zip file containing queries

# Function to check node availability
def check_node_availability(node_ip, node_port):
    """Checks if a node is available by attempting to connect to its port."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)  # Timeout for connection
            s.connect((node_ip, node_port))
            return True
    except (socket.error, ConnectionRefusedError):
        return False

# Function to get the list of active nodes
def get_active_nodes(all_nodes):
    """Returns a list of active nodes by checking their availability."""
    active_nodes = []
    for node_ip, node_port in all_nodes:
        if check_node_availability(node_ip, node_port):
            print(f"[INFO] Node {node_ip}:{node_port} is active.")
            active_nodes.append((node_ip, node_port))
        else:
            print(f"[INFO] Node {node_ip}:{node_port} is not active.")
    return active_nodes

# Step 1: Unzip the file
def unzip_files(zip_file):
    """Extracts the ZIP file in the current directory."""
    if not os.path.exists(zip_file):
        print(f"[ERROR] {zip_file} not found!")
        return []

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall("unzipped_files")  # Extract into a folder
    return sorted(os.listdir("unzipped_files"))  # Return file names sorted

# Step 2: Query data in DHT
def query_dht(node_ip, node_port, key):
    """Sends a query request to the given DHT node."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            request = {"type": "query", "key": key}
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(4096).decode())
            print(f"[QUERY] {key}: {response['value']}")
    except Exception as e:
        print(f"[ERROR] Failed to query {key}: {e}")

# Step 3: Process query files and query the DHT
def process_queries(active_nodes):
    files = unzip_files(ZIP_FILE)
    if not files:
        return


    # Round-robin file distribution among active nodes
    num_nodes = len(active_nodes)
    start_time_total = time.time()
    for i, file_to_process in enumerate(files):
        node_index = i % num_nodes  # Round-robin assignment of files to nodes
        node_ip, node_port = active_nodes[node_index]

        print(f"[PROCESS] Node {node_ip}:{node_port} handling {file_to_process}")
        start_time_node = time.time() 
        with open(os.path.join("unzipped_files", file_to_process), "r") as f:
            for line in f:
                song = line.strip()
                if song:
                    query_dht(node_ip, node_port, song)
        print(f"[TIME] {file_to_process} processed in {time.time() - start_time_node:.4f} seconds")

    print(f"\n[TOTAL TIME] All requests processed in {time.time() - start_time_total:.4f} seconds")

if __name__ == "__main__":
    # Define a list of all possible nodes (including inactive ones)
    all_nodes = [
        ("10.0.36.124", 6000),
        ("10.0.36.15", 6000),
        #("10.0.0.3", 5003),
        #("10.0.0.4", 5004),
        #("10.0.0.5", 5005),
        #("10.0.0.6", 5006),
        #("10.0.0.7", 5007),
        #("10.0.0.8", 5008),
        #("10.0.0.9", 5009),
        #("10.0.0.10", 5010),
    ]

    # Get the active nodes from the system
    active_nodes = get_active_nodes(all_nodes)

    if not active_nodes:
        print("[ERROR] No active nodes found! Exiting...")
        sys.exit(1)

    # Process query files and query them on the assigned active nodes
    process_queries(active_nodes)

