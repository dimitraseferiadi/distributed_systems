import socket
import json

def send_request(command_type, data=None):
    """Send a request to the bootstrap node."""
    bootstrap_ip = "127.0.0.1"
    bootstrap_port = 5000
    
    request = {"type": command_type}
    if data:
        request.update(data)
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((bootstrap_ip, bootstrap_port))
            s.send(json.dumps(request).encode())
            response = json.loads(s.recv(4096).decode())
            return response
    except Exception as e:
        return {"status": "error", "message": str(e)}

def main():
    print("Chord DHT CLI - Type 'help' for commands.")
    while True:
        command = input("chord> ").strip().split()
        if not command:
            continue
        
        cmd = command[0].lower()
        
        if cmd == "insert" and len(command) == 3:
            key, value = command[1], command[2]
            response = send_request("insert", {"key": key, "value": value})
        elif cmd == "delete" and len(command) == 2:
            key = command[1]
            response = send_request("delete", {"key": key})
        elif cmd == "query" and len(command) == 2:
            key = command[1]
            response = send_request("query", {"key": key})
        elif cmd == "depart" and len(command) == 2:
            node_id = command[1]
            response = send_request("depart", {"node_id": node_id})
        elif cmd == "overlay":
            response = send_request("overlay")
        elif cmd == "help":
            print("""
Commands:
  insert <key> <value>  - Insert a key-value pair.
  delete <key>          - Delete a key-value pair.
  query <key>           - Retrieve value for a key ('*' for all).
  depart <node_id>      - Remove a node from the system.
  overlay               - Show the network topology.
  help                  - Show this help message.
  exit                  - Quit the CLI.
            """)
            continue
        elif cmd == "exit":
            print("Exiting Chord CLI.")
            break
        else:
            print("Invalid command. Type 'help' for a list of commands.")
            continue
        
        print("Response:", response)

if __name__ == "__main__":
    main()
