import socket
import json
import shlex  # ✅ Properly handle quoted input

def send_request_2(command_type, data=None):
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

            # ✅ Read response in chunks to avoid truncation
            data = b""
            while True:
                chunk = s.recv(4096)  
                if not chunk:
                    break
                data += chunk
            
            response = json.loads(data.decode())  # ✅ Ensure full JSON is received
            return response
    except json.JSONDecodeError:
        return {"status": "error", "message": "Invalid JSON response from server"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def send_request(address, command_type, data=None):
    """Send a request to a specified node."""
    request = {"type": command_type}
    if data:
        request.update(data)
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(address)
            s.send(json.dumps(request).encode())

            # ✅ Read response in chunks to avoid truncation
            data = b""
            while True:
                chunk = s.recv(4096)  
                if not chunk:
                    break
                data += chunk
            
            response = json.loads(data.decode())  # ✅ Ensure full JSON is received
            return response
    except json.JSONDecodeError:
        return {"status": "error", "message": "Invalid JSON response from server"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def main():
    print("Chord DHT CLI - Type 'help' for commands.")
    while True:
        try:
            command = shlex.split(input("chord> ").strip())  # ✅ Handles quotes correctly
        except ValueError as e:
            print(f"[ERROR] Invalid command format: {e}")
            continue
        
        if not command:
            continue
        
        cmd = command[0].lower()
        
        if cmd == "insert" and len(command) >= 4:
            key = command[1]
            value = " ".join(command[4:])  # ✅ Capture full value (multi-word support)
            node_ip = command[2]
            node_port = int(command[3])
            response = send_request((node_ip, node_port), "insert", {"key": key, "value": value})
        elif cmd == "query" and len(command) >= 3:
            key = command[1]
            node_ip = command[2]
            node_port = int(command[3])
            response = send_request((node_ip, node_port), "query", {"key": key})
        elif cmd == "delete" and len(command) >= 3:
            key = command[1]
            response = send_request((node_ip, node_port), "delete", {"key": key})
        elif cmd == "depart":
            node_id = command[1]
            response = send_request_2("depart", {"node_id": node_id})
        elif cmd == "overlay":
            response = send_request(("127.0.0.1", 5000), "overlay")

        elif cmd == "help":
            print("""
Commands:
  insert <key> <node_ip> <node_port> <value>  - Insert a key-value pair at a specific node.
  query <key> <node_ip> <node_port>           - Retrieve value for a key from a specific node.
  delete <key> <node_ip> <node_port>          - Delete a key-value pair.
  depart <node_id>                            - Remove a node from the system.
  overlay                                     - Show the network topology.
  help                                        - Show this help message.
  exit                                        - Quit the CLI.
            """)
            continue
        elif cmd == "exit":
            print("Exiting Chord CLI.")
            break
        else:
            print("Invalid command. Type 'help' for a list of commands.")
            continue
        
        # ✅ Prettier response formatting
        print(f"Response: {json.dumps(response, indent=2)}")

if __name__ == "__main__":
    main()

