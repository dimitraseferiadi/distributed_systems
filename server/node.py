import socket
import threading
import hashlib
import json

# Utility function for hashing keys using SHA1
def sha1_hash(key: str) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest(), 16)

class ChordNode:
    def __init__(self, ip: str, port: int, bootstrap_ip: str = None, bootstrap_port: int = None):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        # Unique node ID generated from ip:port
        self.node_id = sha1_hash(f"{ip}:{port}")
        self.predecessor = None
        self.successor = None  # Initially, self is the only node in the ring.
        self.data_store = {}  # <key: hashed_key, value: value> dictionary for key-value pairs

        # Bootstrap node information for joining the ring.
        self.bootstrap_ip = bootstrap_ip
        self.bootstrap_port = bootstrap_port

        self.running = True

    def start_server(self):
        """Start the server to listen for incoming connections."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.address)
        server_socket.listen(5)
        print(f"[SERVER] Node {self.node_id} listening on {self.ip}:{self.port}")

        while self.running:
            try:
                client_socket, _ = server_socket.accept()
                client_handler = threading.Thread(
                    target=self.handle_request,
                    args=(client_socket,)
                )
                client_handler.start()
            except Exception as e:
                print(f"[ERROR] Server error: {e}")

    

    def handle_request(self, client_socket):
        """Handle incoming requests from other nodes or clients."""
        try:
            request = json.loads(client_socket.recv(4096).decode())
            response = self.process_request(request)
            client_socket.send(json.dumps(response).encode())
        except Exception as e:
            print(f"[ERROR] Handling request failed: {e}")
        finally:
            client_socket.close()

    def process_request(self, request: dict) -> dict:
        """
        Process different types of incoming requests.
        The request should have a "type" field (e.g., insert, query, delete, join, depart)
        and any additional data needed for the operation.
        """
        req_type = request.get("type")
        if req_type == "insert":
            return self.insert(request["key"], request["value"])
        elif req_type == "query":
            return self.query(request["key"])
        elif req_type == "delete":
            return self.delete(request["key"])
        elif req_type == "join":
            return self.handle_join(request["node_info"])
        elif req_type == "depart":
            return self.handle_depart(request["node_info"])
        return {"status": "error", "message": "Unknown request type"}
    

    def join_ring(self):
        """Join the Chord ring by contacting the bootstrap node. """
        print(f"[JOIN] Node {self.node_id} attempting to join via bootstrap {self.bootstrap_ip}:{self.bootstrap_port}")
        if not self.bootstrap_ip or not self.bootstrap_port:
            print("[JOIN] No bootstrap node provided. This node will start a new ring.")
            return
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.bootstrap_ip, self.bootstrap_port))
                request = {
                    "type": "join",
                    "node_info": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
                }
                s.send(json.dumps(request).encode())
                response = json.loads(s.recv(4096).decode())
                # Process the response to set successor and predecessor
                self.predecessor = response.get("predecessor")
                self.successor = response.get("successor")
                print(f"[JOIN] Node {self.node_id} joined. Predecessor: {self.predecessor}, Successor: {self.successor}")
        except Exception as e:
            print(f"[ERROR] Failed to join the ring: {e}")
    
    def lookup(self, key: str):
        hashed_key = sha1_hash(key)
        if self.predecessor and self.predecessor[2] < hashed_key <= self.node_id:
            return {"status": "success", "node": self.node_id, "message": "Responsible node found."}
        return self.forward_request(self.successor, {"type": "lookup", "key": key})
    
    def forward_request(self, node, request):
        return self.send_message((node[0], node[1]), request)
    
    def depart(self):
        """
        Gracefully depart from the ring.
        Notify other nodes (e.g., successor and predecessor) that this node is leaving.
        """
        print(f"[DEPART] Node {self.node_id} is departing from the ring.")
        
        # Notify predecessor and successor to update their states
        if self.predecessor and self.successor:
            self.send_message(self.predecessor, {"type": "update_successor", "node": self.successor})
            self.send_message(self.successor, {"type": "update_predecessor", "node": self.predecessor})
        # Notify the bootstrap node about the departure
        if self.bootstrap_ip and self.bootstrap_port:
            self.send_message((self.bootstrap_ip, self.bootstrap_port), {"type": "node_departed", "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}})

        # Now that the departure is communicated, clean up local state (predecessor, successor, etc.)
        self.cleanup_local_state()

        sys.exit(0)

    def cleanup_local_state(self):
        """Clean up local state after departure."""
        # Update the predecessor and successor references, this node is now leaving
        print(f"[CLEANUP] Node {self.node_id} cleaning up state.")
        self.predecessor = None
        self.successor = None
        
    def send_message(self, address, message):
        """Send a message to a given address but handle connection failures gracefully."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if isinstance(address, dict):
                    address = (address["ip"], address["port"])
                #s.settimeout(3)  # Avoid hanging indefinitely
                s.connect(address)
                s.send(json.dumps(message).encode())
            
        except (socket.error, ConnectionRefusedError):
            print(f"[WARNING] Could not contact {address}. The node may have already departed.")

        
    def run(self):
        threading.Thread(target=self.start_server, daemon=True).start()
        self.join_ring()
        while True:
            try:
                cmd = input(f"[NODE {self.node_id}] Enter command: ").strip()
                if cmd == "depart":
                    self.depart()
                elif cmd == "lookup":
                    key = input("Enter key to lookup: ")
                    print(self.lookup(key))
                elif cmd == "help":
                    print("Commands: depart, lookup, help")
            except KeyboardInterrupt:
                print("\n[EXIT] Shutting down node.")
                break


if __name__ == "__main__":
    import sys
    # Example usage:
    # Run as: python node.py <ip> <port> [<bootstrap_ip> <bootstrap_port>]
    if len(sys.argv) < 3:
        print("Usage: python node.py <ip> <port> [<bootstrap_ip> <bootstrap_port>]")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    bootstrap_ip = sys.argv[3] if len(sys.argv) > 3 else None
    bootstrap_port = int(sys.argv[4]) if len(sys.argv) > 4 else None

    node = ChordNode(ip, port, bootstrap_ip, bootstrap_port)
    node.run()
