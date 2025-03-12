import os
import zipfile
import socket
import json
import sys
import time

# Get the list of active nodes dynamically
def discover_active_nodes():
    """Simulates discovering active nodes in the system."""
    return [
        ("10.0.36.124", 6000),
        ("10.0.36.15", 6000),
        #("10.0.0.3", 5003),
        #("10.0.0.4", 5004),
    ]

ZIP_FILE = "requests.zip"

# Unzip the file
def unzip_files(zip_file):
    """Extracts the ZIP file into 'unzipped_requests' directory."""
    if not os.path.exists(zip_file):
        print(f"[ERROR] {zip_file} not found!")
        return []

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall("unzipped_requests")  
    return sorted(os.listdir("unzipped_requests"))  

# Send insert requests
def insert_into_dht(node_ip, node_port, key, value):
    """Sends an insert request to the given DHT node."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            request = {"type": "insert", "key": key, "value": value}
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(4096).decode())
            print(f"[INSERT] {key}: {value} -> {response['status']}")
    except Exception as e:
        print(f"[ERROR] Failed to insert {key}: {e}")

# Send query requests
def query_dht(node_ip, node_port, key):
    """Sends a query request to the given DHT node."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            request = {"type": "query", "key": key}
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(4096).decode())
            print(f"[QUERY] {key}: {response.get('value', 'NOT FOUND')}")
    except Exception as e:
        print(f"[ERROR] Failed to query {key}: {e}")

# Process request files (Round Robin)
def process_requests():
    files = unzip_files(ZIP_FILE)
    if not files:
        return

    active_nodes = discover_active_nodes()
    num_nodes = len(active_nodes)
    start_time_total = time.time()
    for i, file_to_process in enumerate(files):
        node_index = i % num_nodes  # Round-robin distribution
        node_ip, node_port = active_nodes[node_index]

        print(f"[PROCESS] Node {node_ip}:{node_port} handling {file_to_process}")
        start_time_node = time.time()
        with open(os.path.join("unzipped_requests", file_to_process), "r") as f:
            for line in f:
                parts = line.strip().split(", ")

                if len(parts) == 2 and parts[0] == "query":
                    query_dht(node_ip, node_port, parts[1])

                elif len(parts) == 3 and parts[0] == "insert":
                    insert_into_dht(node_ip, node_port, parts[1], parts[2])

                else:
                    print(f"[ERROR] Invalid request format in {file_to_process}: {line.strip()}")

        print(f"[TIME] {file_to_process} processed in {time.time() - start_time_node:.4f} seconds")

    print(f"\n[TOTAL TIME] All requests processed in {time.time() - start_time_total:.4f} seconds")


'''
def process_requests():
    start_time_total = time.time()  # Start total execution timer

    files = unzip_files(ZIP_FILE)
    if not files:
        return

    active_nodes = discover_active_nodes()
    num_nodes = len(active_nodes)

    for i, file_to_process in enumerate(files):
        node_ip, node_port = active_nodes[i % num_nodes]  # Round-robin distribution
        print(f"[PROCESS] Node {node_ip}:{node_port} handling {file_to_process}")

        start_time_node = time.time()  # Start node processing timer

        with open(os.path.join("unzipped_requests", file_to_process), "r") as f:
            for line in f:
                parts = line.strip().split(", ")
                if len(parts) == 2 and parts[0] == "query":
                    send_request(node_ip, node_port, {"type": "query", "key": parts[1]})
                elif len(parts) == 3 and parts[0] == "insert":
                    send_request(node_ip, node_port, {"type": "insert", "key": parts[1], "value": parts[2]})
                else:
                    print(f"[ERROR] Invalid request format in {file_to_process}: {line.strip()}")

        print(f"[TIME] {file_to_process} processed in {time.time() - start_time_node:.4f} seconds")

    print(f"\n[TOTAL TIME] All requests processed in {time.time() - start_time_total:.4f} seconds")
'''

if __name__ == "__main__":
    process_requests()

