import zipfile
import os
import socket
import json

# Configurations
NODE_IPS = [
    ("10.0.36.124", 6000),  # Replace with actual active node IPs and ports
    ("10.0.36.15", 6000),
    # Add more active nodes if necessary
]

INSERT_ZIP = "insert.zip"
REQUEST_ZIP = "requests.zip"

def extract_songs_from_insert_zip(zip_filename):
    """Extracts song names from insert_XX_part.txt files inside the zip."""
    songs = set()  # Using a set to avoid duplicates
    with zipfile.ZipFile(zip_filename, 'r') as zipf:
        for file_name in zipf.namelist():
            print(f"[DEBUG] Processing insert file: {file_name}")  # Debug log for file names inside zip
            with zipf.open(file_name) as f:
                for line in f:
                    line = line.decode('utf-8').strip()
                    if line:
                        songs.add(line)  # Add song name directly (no "insert," prefix)
                        print(f"[DEBUG] Found song in insert file: {line}")  # Debug log for found song names
    return songs

def extract_songs_from_request_zip(zip_filename):
    """Extracts song names from request_XX_part.txt files inside the zip."""
    songs = set()  # Using a set to avoid duplicates
    with zipfile.ZipFile(zip_filename, 'r') as zipf:
        for file_name in zipf.namelist():
            print(f"[DEBUG] Processing request file: {file_name}")  # Debug log for file names inside zip
            with zipf.open(file_name) as f:
                for line in f:
                    line = line.decode('utf-8').strip()
                    if line and line.startswith("insert,"):
                        song_name = line.split(",")[1].strip()  # Extract song name from "insert, song_name"
                        songs.add(song_name)
                        print(f"[DEBUG] Found song in request file: {song_name}")  # Debug log for found song names
    return songs

def query_all_keys_from_dht(node_ip, node_port):
    """Queries all keys from the DHT node and logs issues with responses."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            request = {"type": "query", "key": "*"}
            s.send(json.dumps(request).encode())
            response = b""  # Initialize an empty byte string to hold the response
            
            # Increase the buffer size to handle large responses (e.g., 8192 bytes)
            while True:
                chunk = s.recv(8192)  # Receive in chunks
                if not chunk:
                    break  # Exit the loop if no more data is received
                response += chunk  # Append the received chunk to the response
                
            # Try to parse the entire response into JSON
            try:
                response_data = json.loads(response.decode())
                if response_data['status'] == "success":
                    print(f"[QUERY] Retrieved keys from {node_ip}:{node_port}")
                    return set(response_data['data'].keys())
                else:
                    print(f"[QUERY ERROR] Failed to retrieve keys from {node_ip}:{node_port}")
                    return set()
            except json.JSONDecodeError as e:
                print(f"[ERROR] Failed to decode JSON response from {node_ip}:{node_port}: {e}")
                print(f"[ERROR] Response: {response.decode()[:500]}...")  # Log the first 500 characters of the response
                return set()
    except Exception as e:
        print(f"[ERROR] Query failed for {node_ip}:{node_port}: {e}")
        return set()



def delete_from_dht(node_ip, node_port, key):
    """Sends a delete request to the given DHT node."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            request = {"type": "delete", "key": key}
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(4096).decode())
            print(f"[DELETE] {key} -> {response['status']}")
    except Exception as e:
        print(f"[ERROR] Failed to delete {key}: {e}")

def send_delete_requests(songs):
    """Sends delete requests to active nodes in a round-robin fashion."""
    active_node_count = len(NODE_IPS)

    # Query all keys from each node
    all_keys = set()
    for node_ip, node_port in NODE_IPS:
        keys_from_node = query_all_keys_from_dht(node_ip, node_port)
        all_keys.update(keys_from_node)

    print(f"[INFO] All keys in the system before deletion: {all_keys}")

    # Now, send delete requests for each key
    for song in songs:
        print(f"[INFO] Sending delete request for: {song}")
        for node_ip, node_port in NODE_IPS:
            delete_from_dht(node_ip, node_port, song)

    print("[INFO] Delete requests sent. Verifying deletion...")

    # Verify if the song is deleted from all nodes
    for song in songs:
        for node_ip, node_port in NODE_IPS:
            keys = query_all_keys_from_dht(node_ip, node_port)
            if song in keys:
                print(f"[ERROR] {song} still found on {node_ip}:{node_port}")
            else:
                print(f"[SUCCESS] {song} successfully deleted from {node_ip}:{node_port}")

def create_delete_songs_file(songs, filename="delete_songs.txt"):
    """Creates a text file containing all the songs to delete."""
    with open(filename, 'w') as f:
        for song in sorted(songs):
            f.write(f"{song}\n")

def main():
    # Step 1: Extract song names from inserts.zip
    print("[INFO] Extracting songs from inserts.zip...")
    insert_songs = extract_songs_from_insert_zip(INSERT_ZIP)
    print(f"[INFO] Found {len(insert_songs)} songs in {INSERT_ZIP}")

    # Step 2: Extract song names from requests.zip
    print("[INFO] Extracting songs from requests.zip...")
    request_songs = extract_songs_from_request_zip(REQUEST_ZIP)
    print(f"[INFO] Found {len(request_songs)} songs in {REQUEST_ZIP}")

    # Step 3: Combine all the songs (no duplicates)
    all_songs = insert_songs.union(request_songs)  # Union to merge the two sets

    # Step 4: Save the combined list of songs to delete_songs.txt
    create_delete_songs_file(all_songs)
    print(f"Created delete_songs.txt with {len(all_songs)} songs to delete.")

    # Step 5: Send delete requests to active nodes in round-robin fashion
    send_delete_requests(all_songs)

if __name__ == "__main__":
    main()

