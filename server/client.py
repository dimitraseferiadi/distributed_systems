import socket
import json
import sys

def send_request(node_ip, node_port, request: dict) -> dict:
    """Send a JSON request to the specified node and return the JSON response."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            s.send(json.dumps(request).encode())
            response = s.recv(4096).decode()
            if response:
                return json.loads(response)
            else:
                return {"status": "error", "message": "No response from node"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def print_help():
    help_text = """
Available commands:
    insert <key> <value>   - Insert or update a key-value pair.
    query <key>            - Query for a key (use "*" to get all keys).
    delete <key>           - Delete a key-value pair.
    depart                 - Gracefully remove the current node from the system.
    overlay                - Request the current Chord ring topology.
    help                   - Display this help message.
    exit                   - Exit the client.
    """
    print(help_text)

def main():
    if len(sys.argv) < 3:
        print("Usage: python client.py <node_ip> <node_port>")
        sys.exit(1)

    node_ip = sys.argv[1]
    node_port = int(sys.argv[2])
    
    print(f"[CLIENT] Connected to node at {node_ip}:{node_port}")
    print_help()

    while True:
        try:
            command = input("Chordify> ").strip()
            if not command:
                continue

            parts = command.split()
            cmd = parts[0].lower()

            if cmd == "insert":
                if len(parts) < 3:
                    print("Usage: insert <key> <value>")
                    continue
                key = parts[1]
                value = " ".join(parts[2:])  # Allow values with spaces.
                request = {"type": "insert", "key": key, "value": value}
                response = send_request(node_ip, node_port, request)
                print(response)
            
            elif cmd == "query":
                if len(parts) != 2:
                    print("Usage: query <key>")
                    continue
                key = parts[1]
                request = {"type": "query", "key": key}
                response = send_request(node_ip, node_port, request)
                print(response)

            elif cmd == "delete":
                if len(parts) != 2:
                    print("Usage: delete <key>")
                    continue
                key = parts[1]
                request = {"type": "delete", "key": key}
                response = send_request(node_ip, node_port, request)
                print(response)

            elif cmd == "depart":
                # The client sends a depart request to the node.
                request = {"type": "depart", "node_info": {"ip": node_ip, "port": node_port}}
                response = send_request(node_ip, node_port, request)
                print(response)
                print("[CLIENT] Depart command issued. Exiting client.")
                break

            elif cmd == "overlay":
                # Overlay command to get the current ring topology.
                # In this basic implementation, we assume the node supports a request of type 'overlay'.
                request = {"type": "overlay"}
                response = send_request(node_ip, node_port, request)
                print(response)

            elif cmd == "help":
                print_help()

            elif cmd == "exit":
                print("Exiting client.")
                break

            else:
                print("Invalid command. Type 'help' to see available commands.")

        except KeyboardInterrupt:
            print("\nExiting client.")
            break

if __name__ == "__main__":
    main()
