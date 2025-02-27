import socket
import threading
import hashlib
import json

# Utility function for hashing keys using SHA1
def sha1_hash(key: str) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest(), 16)

def in_range(val, start, end, include_end=True):
    """
    Check if 'val' is in the circular interval (start, end].
    When include_end is False, the interval is (start, end).
    This handles wrap-around in the identifier space.
    """
    if start < end:
        return (start < val <= end) if include_end else (start < val < end)
    else:
        # Wrap-around case.
        return (val > start or val <= end) if include_end else (val > start or val < end)

def normalize_node(node):
    """Ensure that a node is in tuple form: (ip, port, node_id)."""
    if node is None:
        return None
    if isinstance(node, dict):
        return (node["ip"], node["port"], node["node_id"])
    return node

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
        
        self.replication_factor = 1

        # Bootstrap node information for joining the ring.
        self.bootstrap_ip = bootstrap_ip
        self.bootstrap_port = bootstrap_port
        if bootstrap_ip and bootstrap_port:
            self.bootstrap_id = sha1_hash(f"{bootstrap_ip}:{bootstrap_port}")
        else:
            self.bootstrap_id = None
        
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
            raw_message = client_socket.recv(4096)
            if not raw_message:
                return
            message = raw_message.decode()

            try:
                request = json.loads(message)
            except Exception as e:
                print(f"[ERROR] JSON decode error: {e}")
                return

            if not isinstance(request, dict):
                print(f"[ERROR] Request is not a dict: {request}")
                return

            try:
                response = self.process_request(request)
            except Exception as e:
                print(f"[ERROR] process_request exception: {e}")
                response = {"status": "error", "message": str(e)}

            response_json = json.dumps(response).encode()

            try:
                client_socket.sendall(response_json)
            except Exception as e:
                print(f"[ERROR] client_socket.sendall failed: {e}")

        except Exception as e:
            print(f"[ERROR] Handling request failed: {e}")
        finally:
            try:
                client_socket.close()
            except Exception as e:
                print(f"[ERROR] Closing socket failed: {e}")



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
        elif req_type == "find_successor":
            successor = self.find_successor(request["node_id"])
            return {"status": "success", "successor": successor}
        elif req_type == "update_predecessor":
            self.update_predecessor(request["node"])
            return {"status": "success"}
        elif req_type == "update_successor":
            self.update_successor(request["node"])
            return {"status": "success"}
        return {"status": "error", "message": "Unknown request type"}
    
    def insert(self, key: str, value: str) -> dict:
        """Insert a key-value pair into the correct node."""
        key_id = sha1_hash(key)
        print(f"[INSERT] Key: {key} → Hash ID: {key_id}")

        # Check if this node is responsible for storing the key
        if self.is_responsible_for_key(key_id):
            self.data_store[key_id] = value  # Store using the hashed key
            print(f"[INSERT] Stored at Node {self.node_id}: {key} → {value}")
            return {"status": "success", "node": self.node_id, "message": "Key stored"}

        # Otherwise, forward the request to the correct node
        successor_ip, successor_port, _ = self.find_successor(self.node_id)
        if (successor_ip, successor_port) == (self.ip, self.port):  # Safety check
            print(f"[WARNING] Unexpected self-reference in successor lookup! Storing locally.")
            self.data_store[key_id] = value
            return {"status": "success", "node": self.node_id, "message": "Key stored locally as a fallback"}

        response = self.send_message((successor_ip, successor_port), {
            "type": "insert",
            "key": key,
            "value": value
        })

        return response if response else {"status": "error", "message": "Failed to store key"}

    def is_responsible_for_key(self, key_id):
        """Check if this node is responsible for storing the given key."""
        if self.predecessor is None:
            return True  # If there's no predecessor, this node is alone in the ring.

        pred_id = self.predecessor[2]  # Predecessor node ID

        return in_range(key_id, pred_id, self.node_id, include_end=True)
    
    def is_responsible_for_key(self, key_id):
        """Check if this node is responsible for storing the given key."""
        # If no predecessor, this node is alone.
        if self.predecessor is None:
            return True
        
        pred = normalize_node(self.predecessor)
        pred_id = pred[2]
        
        # Normal (no wrap-around) and wrap-around cases.
        if pred_id < self.node_id:
            return pred_id < key_id <= self.node_id
        else:
            return key_id > pred_id or key_id <= self.node_id

    def find_successor(self, node_id):
        """Find the successor of a given node ID in the ring."""

        if isinstance(self.successor, dict):
            self.successor = (self.successor["ip"], self.successor["port"], self.successor["node_id"])
        elif isinstance(self.successor, list):
            self.successor = tuple(self.successor)

        # If there's only one node, it must be its own successor
        if self.successor is None or self.successor == (self.ip, self.port, self.node_id):
            return (self.ip, self.port, self.node_id)

        # Check if node_id lies in the interval (self.node_id, successor]
        if in_range(node_id, self.node_id, self.successor[2], include_end=True):
            return self.successor
        
        # Forward the request to the closest preceding node
        next_node = self.closest_preceding_node(node_id)
        if not next_node:
            return self.successor  # Fallback if no better node

        response = self.send_message((next_node[0], next_node[1]), {
            "type": "find_successor",
            "node_id": node_id
        })

        new_successor = response.get("successor", self.successor)
        
        if isinstance(new_successor, list):
            new_successor = tuple(new_successor)

        return new_successor
    
    def closest_preceding_node(self, node_id):
        """Returns the closest known node to the given ID"""
        return self.successor  # Temporary logic, later use a finger table

    def update_predecessor(self, node_info):
        new_pred = (node_info["ip"], node_info["port"], node_info["node_id"])
        
        # Normalize existing predecessor if necessary.
        if self.predecessor is not None:
            self.predecessor = normalize_node(self.predecessor)
        
        # For simplicity, if there's no predecessor, accept the new one.
        if self.predecessor is None:
            self.predecessor = new_pred
            print(f"[UPDATE] Predecessor set to: {self.predecessor}")
            return
        
        # Use your logic (e.g., in_range) to decide if the update is valid.
        if in_range(new_pred[2], self.predecessor[2], self.node_id, include_end=False):
            self.predecessor = new_pred
            print(f"[UPDATE] New predecessor updated to: {self.predecessor}")
        else:
            print(f"[WARNING] Ignored invalid predecessor update: {node_info}")

    def update_successor(self, node_info):
        new_succ = (node_info["ip"], node_info["port"], node_info["node_id"])
        # Update if there's no successor or new_succ is a better candidate.
        if self.successor is None or in_range(new_succ[2], self.node_id, self.successor[2] if self.successor else self.node_id, include_end=False):
            self.successor = new_succ
            print(f"[UPDATE] New successor set to: {self.successor}")
        else:
            print(f"[WARNING] Ignored invalid successor update: {node_info}")

    def query(self, key: str) -> dict:
        """Query for a key (or all keys if key == '*')."""
        if key == "*":
            # Return all key-value pairs.
            # For clarity, return keys in their original hashed format.
            return {"status": "success", "data": self.data_store}
        else:
            hashed_key = sha1_hash(key)
            value = self.data_store.get(hashed_key, None)
            if value is not None:
                print(f"[QUERY] Found key: {key} (hash: {hashed_key}) with value: {value}")
                return {"status": "success", "value": value}
            else:
                print(f"[QUERY] Key: {key} (hash: {hashed_key}) not found.")
                return {"status": "error", "message": "Key not found"}

    def delete(self, key: str) -> dict:
        """Delete a key-value pair."""
        hashed_key = sha1_hash(key)
        if hashed_key in self.data_store:
            del self.data_store[hashed_key]
            print(f"[DELETE] Key: {key} (hash: {hashed_key}) deleted.")
            return {"status": "success", "node_id": self.node_id}
        else:
            print(f"[DELETE] Key: {key} (hash: {hashed_key}) not found for deletion.")
            return {"status": "error", "message": "Key not found"}

    def join_ring(self):
        """Join the Chord ring by contacting the bootstrap node."""
        print(f"[JOIN] Node {self.node_id} attempting to join via bootstrap {self.bootstrap_ip}:{self.bootstrap_port}")
            
        if not self.bootstrap_ip or not self.bootstrap_port:
            print("[JOIN] No bootstrap node provided. This node will start a new ring.")
            self.successor = (self.ip, self.port, self.node_id)
            self.predecessor = (self.ip, self.port, self.node_id)
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

                if response.get("status") == "success":
                    self.predecessor = response.get("predecessor")
                    self.successor = response.get("successor")

                    # If no successor is given, the bootstrap might be alone
                    if not self.successor or self.successor == (None, None, None):
                        self.successor = (self.bootstrap_ip, self.bootstrap_port, self.bootstrap_id)
                        print(f"[JOIN] Bootstrap is alone, setting it as my successor: {self.successor}")

                    print(f"[JOIN] Initial info → Predecessor: {self.predecessor}, Successor: {self.successor}")

                    # Validate successor using find_successor
                    correct_successor = self.find_successor(self.node_id)
                    if correct_successor and correct_successor != self.successor:
                        self.successor = correct_successor
                        print(f"[JOIN] Updated successor after validation: {self.successor}")

                    # Notify the successor to update its predecessor
                    if self.successor and isinstance(self.successor, tuple) and len(self.successor) == 3:
                        successor_ip, successor_port, successor_id = self.successor
                        if (successor_ip, successor_port) != (self.ip, self.port):
                            response = self.send_message((successor_ip, successor_port), {
                                "type": "update_predecessor",
                                "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
                            })

                    print(f"[JOIN] Node {self.node_id} successfully joined. Final Successor: {self.successor}. Final Predecessor: {self.predecessor}")

        except Exception as e:
            print(f"[ERROR] Failed to join the ring: {e}")

    
    def replicate_data(self):
        """Placeholder for replication logic. Currently does nothing."""
        if self.replication_factor > 1:
            print(f"[REPLICATION] Future replication logic goes here. Factor: {self.replication_factor}")

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
                
                s.settimeout(3)
                s.connect(address)
                s.send(json.dumps(message).encode())

                response = s.recv(4096).decode()
                return json.loads(response) if response else {}
        except socket.timeout:
            print(f"[ERROR] Timeout while connecting to {address}")
        except ConnectionRefusedError:
            print(f"[ERROR] Connection refused by {address}")
        except socket.error as e:
            print(f"[ERROR] Socket error while contacting {address}: {e}")

        return {"status": "error", "message": "Node unreachable"}

        
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
