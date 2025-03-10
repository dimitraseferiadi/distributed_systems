import argparse
import json
import socket
from pprint import pprint

# Buffer size for sending and receiving data over the network
BUFF_SIZE = 1024

def send_request(host, port, request: dict) -> dict:
    """Send a JSON request to the specified node and return the JSON response."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.send(json.dumps(request).encode())  # Send the request as a JSON string
            response = s.recv(4096).decode()  # Receive response
            if response:
                return json.loads(response)  # Parse the JSON response
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

def interactive_mode(host, port):
    """
    Provides an interactive CLI where users can enter commands dynamically.
    """
    print("Chord DHT Client. Type 'help' for available commands.")
    while True:
        try:
            command = input("[CLIENT] Enter command: ").strip()
            if not command:
                continue

            parts = command.split()
            cmd = parts[0].lower()

            if cmd == "insert":
                if len(parts) < 3:
                    print("Usage: insert <key> <value>")
                    continue
                key = parts[1]
                value = " ".join(parts[2:])  # Allow values with spaces
                request = {"type": "insert", "key": key, "value": value}
                response = send_request(host, port, request)
                pprint(response)

            elif cmd == "query":
                if len(parts) != 2:
                    print("Usage: query <key>")
                    continue
                key = parts[1]
                request = {"type": "query", "key": key}
                response = send_request(host, port, request)
                pprint(response)

            elif cmd == "delete":
                if len(parts) != 2:
                    print("Usage: delete <key>")
                    continue
                key = parts[1]
                request = {"type": "delete", "key": key}
                response = send_request(host, port, request)
                pprint(response)

            elif cmd == "depart":
                request = {"type": "depart", "node_info": {"ip": host, "port": port}}
                response = send_request(host, port, request)
                pprint(response)
                print("[CLIENT] Depart command issued. Exiting client.")
                break

            elif cmd == "overlay":
                request = {"type": "overlay"}
                response = send_request(host, port, request)
                pprint(response)

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

def main():
    """
    Parses command-line arguments and executes the corresponding command.
    If no command is provided, enters interactive mode.
    """
    parser = argparse.ArgumentParser(description="CLI to interact with a Chord DHT node.")
    parser.add_argument("command", type=str, nargs="?", help="Command to run (insert, delete, query, depart, overlay, info, help)")
    parser.add_argument("key_or_value", type=str, nargs="?", help="Key for query, insert, or delete")
    parser.add_argument("value", type=str, nargs="?", help="Value for insert")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Node host")
    parser.add_argument("--port", type=int, default=5000, help="Node port")
    args = parser.parse_args()

    if args.command is None:
        interactive_mode(args.host, args.port)  # Enter interactive mode if no command is provided
        return

    cmd = args.command.lower()
    if cmd == "insert" and args.key_or_value and args.value:
        request = {"type": "insert", "key": args.key_or_value, "value": args.value}
    elif cmd == "delete" and args.key_or_value:
        request = {"type": "delete", "key": args.key_or_value}
    elif cmd == "query" and args.key_or_value:
        request = {"type": "query", "key": args.key_or_value}
    elif cmd == "depart":
        request = {"type": "depart", "node_info": {"ip": args.host, "port": args.port}}
    elif cmd == "overlay":
        request = {"type": "overlay"}
    elif cmd == "help":
        print_help()
        return
    else:
        print("Invalid command. Type 'help' for a list of commands.")
        return

    response = send_request(args.host, args.port, request)  # Send request to the server
    pprint(response)

if __name__ == "__main__":
    main()  # Run the script if executed directly

