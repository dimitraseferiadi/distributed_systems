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
        elif (req_type == "query" and request.get("key") == "*"):
            new_request = {
                "type": "query_all",
                "origin": (self.ip, self.port, self.node_id),  # this node is the origin
                "data": self.data_store.copy(),
                "initial": True 
            }
            return self.handle_global_query(new_request)
        elif req_type == "query_all":
            return self.handle_global_query(request)
        elif req_type == "query":
            return self.normal_query(request["key"])
        elif req_type == "delete":
            return self.delete(request["key"])
        elif req_type == "join":
            return self.handle_join(request["node_info"])
        elif req_type == "depart":
            return self.depart(request["node_id"])
        elif req_type == "find_successor":
            hops = request.get("hops", 0)
            successor = self.find_successor(request["node_id"], hops)
            return {"status": "success", "successor": successor}
        elif req_type == "update_predecessor":
            node_data = request.get("node") or request.get("node_info")
            if not node_data:
                return {"status": "error", "message": "Missing node data for update_predecessor"}
            self.update_predecessor(node_data)
            return {"status": "success"}
        elif req_type == "update_successor":
            node_data = request.get("node") or request.get("node_info")
            if not node_data:
                return {"status": "error", "message": "Missing node data for update_successor"}
            self.update_successor(node_data)
            return {"status": "success"}
        elif req_type == "get_predecessor":
            return {"status": "success", "predecessor": self.predecessor}
        elif req_type == "shutdown":
            print(f"[DEPART] Node {self.node_id} is shutting down.")
            self.running = False
            return {"status": "success", "message": "Node shutting down."}
        elif req_type == "get_neighbors":
            return {
                "status": "success",
                "predecessor": tuple(self.predecessor) if self.predecessor else None,
                "successor": tuple(self.successor) if self.successor else None,
                "keys": self.data_store
            }
        elif req_type == "find_node":
            return self.find_node(request["node_id"])
        elif req_type == "confirm_node":
            # This request is sent when a node is found via find_successor to verify if it's the correct one.
            if self.node_id == request["node_id"]:
                return {"status": "success", "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}}
            else:
                return {"status": "error", "message": f"Node {request['node_id']} not found here."}
        elif req_type == "get_keys":
            print(f"[TRANSFER] Node {self.node_id} providing {len(self.data_store)} keys for transfer.")
            return {"status": "success", "keys": self.data_store}
        elif req_type == "transfer_keys":
            keys_to_transfer = request.get("keys", {})
            if isinstance(keys_to_transfer, dict):  # Ensure it's a dictionary before merging
                self.data_store.update(keys_to_transfer)
                print(f"[TRANSFER] Received {len(keys_to_transfer)} keys.")
                return {"status": "success"}
            else:
                print("[ERROR] transfer_keys received invalid data format")
                return {"status": "error", "message": "Invalid data format"}
        
        return {"status": "error", "message": "Unknown request type"}
    
    def insert(self, key: str, value: str) -> dict:
        """Insert a key-value pair into the correct node."""
        key_id = sha1_hash(key)
        print(f"[INSERT] Key: {key} → Hash ID: {key_id}")

        # Check if this node is responsible for storing the key.
        if self.is_responsible_for_key(key_id):
            self.data_store[key_id] = value
            print(f"[INSERT] Stored at Node {self.node_id}: {key} → {value}")
            return {"status": "success", "node": self.node_id, "message": "Key stored"}
        
        # Look up the correct successor using the key's hash (not self.node_id!)
        successor = self.find_successor(key_id)
        if successor == (self.ip, self.port, self.node_id):
            self.data_store[key_id] = value
            print(f"[INSERT] (After lookup) Key belongs here. Stored locally at Node {self.node_id}")
            return {"status": "success", "node": self.node_id, "message": "Key stored locally"}
        
        response = self.send_message((successor[0], successor[1]), {
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
    

    def find_successor(self, identifier, hops=0):
        MAX_HOPS = 10  # Prevent infinite recursion
        self_id = (self.ip, self.port, self.node_id)

        # If we've hit the hop limit, assume self.
        if hops >= MAX_HOPS:
            return self_id

        # If the ring is a one-node ring, return self.
        if self.successor == self_id:
            return self_id

        # If this node is responsible for the key, return self.
        if self.is_responsible_for_key(identifier):
            return self_id

        # Normalize the successor pointer.
        succ = self.successor
        if isinstance(succ, dict):
            succ = normalize_node(succ)
        elif isinstance(succ, list):
            succ = tuple(succ)
        
        # If our successor is ourself (or invalid), return self.
        if succ == self_id:
            return self_id

        # If identifier falls between this node and our successor, then our successor is responsible.
        if in_range(identifier, self.node_id, succ[2], include_end=True):
            return succ

        # Otherwise, delegate the lookup to our successor (incrementing the hop count).
        response = self.send_message((succ[0], succ[1]), {
            "type": "find_successor",
            "node_id": identifier,
            "hops": hops + 1
        })
        candidate = response.get("successor")
        if candidate:
            candidate = normalize_node(candidate)
            return candidate

        # Fallback: if no candidate was returned, assume our successor is responsible.
        return succ
    
    def closest_preceding_node(self, identifier):
        """
        Return the closest node we know that precedes the given identifier.
        (In a complete implementation you would scan your finger table.
        Here, with only a successor pointer, we simply check if our successor is closer.)
        """
        if self.successor and self.successor != (self.ip, self.port, self.node_id) \
        and in_range(self.successor[2], self.node_id, identifier, include_end=False):
            return self.successor
        return None

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
        #else:
            #print(f"[WARNING] Ignored invalid predecessor update: {node_info}")

    def update_successor(self, node_info):
        new_succ = (node_info["ip"], node_info["port"], node_info["node_id"])

        # Normalize existing successor if necessary.
        if self.successor is not None:
            self.successor = normalize_node(self.successor)
        
        # For simplicity, if there's no successor, accept the new one.
        if self.successor is None:
            self.successor = new_succ
            print(f"[UPDATE] Successor set to: {self.successor}")
            return
        
        # Use your logic (e.g., in_range) to decide if the update is valid.
        if in_range(new_succ[2], self.successor[2], self.node_id, include_end=False):
            self.successor = new_succ
            print(f"[UPDATE] New successor updated to: {self.successor}")
        #else:
            #print(f"[WARNING] Ignored invalid successor update: {node_info}")

    def handle_global_query(self, request: dict) -> dict:
        """
        Handles a global query request ("query_all") by aggregating key–value pairs from all nodes.
        The message should include:
        - 'origin': the initiating node's identity (tuple)
        - 'data': a dict containing already aggregated key–value pairs
        - 'initial': a flag indicating that this is the very first hop.
        """
        origin = tuple(request.get("origin", (self.ip, self.port, self.node_id)))
        collected_data = request.get("data", {})
        initial = request.get("initial", False)

        # Merge this node's local data with collected data
        collected_data.update(self.data_store)

        # If this query has looped back to the origin, return the collected data
        if not initial and origin == (self.ip, self.port, self.node_id):
            return {"status": "success", "data": collected_data}
        
        # If only one node in the ring, return data directly
        if self.successor == (self.ip, self.port, self.node_id):
            return {"status": "success", "data": collected_data}

        # Prepare to forward the request
        forwarded_request = {
            "type": "query_all",
            "origin": origin,
            "data": collected_data,
            "initial": False
        }
        # Send request to successor
        response = self.send_message((self.successor[0], self.successor[1]), forwarded_request)
        
        if not response or "status" not in response:
            return {"status": "success", "data": collected_data}

        # Merge successor response into collected data
        successor_data = response.get("data", {})
        collected_data.update(successor_data)

        return {"status": "success", "data": collected_data}

    def normal_query(self, key: str) -> dict:
        """Handles a normal single-key query."""
        key_id = sha1_hash(key)
        if self.is_responsible_for_key(key_id):
            if key_id in self.data_store:
                value = self.data_store[key_id]
                return {"status": "success", "value": value}
            else:
                return {"status": "error", "message": "Key not found"}
        else:
            successor = self.find_successor(key_id)
            if successor == (self.ip, self.port, self.node_id):
                return {"status": "error", "message": "Key not found in ring"}
            response = self.send_message((successor[0], successor[1]), {
                "type": "query",
                "key": key
            })
            return response


    def query(self, key: str) -> dict:
        """Entry point for query requests initiated by this node."""
        if key == "*":
            if self.successor == (self.ip, self.port, self.node_id):
                return {"status": "success", "data": self.data_store}
            collected = self.data_store.copy()
            response = self.send_message((self.successor[0], self.successor[1]), {
                "type": "query_all",
                "origin": (self.ip, self.port, self.node_id),
                "data": collected,
                "initial": True
            })
            return response
        else:
            return self.normal_query(key)
        
    def stabilize(self):
        """
        Ask our successor for its predecessor and update our pointers if needed.
        This routine helps keep the ring consistent.
        """
        # Request our successor's predecessor.
        response = self.send_message((self.successor[0], self.successor[1]), {
            "type": "get_predecessor"
        })
        x = response.get("predecessor")
        if x:
            x = normalize_node(x)
            # If x is between self and our current successor, then x might be a better successor.
            if in_range(x[2], self.node_id, self.successor[2], include_end=False):
                self.successor = x
        # Notify our successor to update its predecessor pointer.
        self.send_message((self.successor[0], self.successor[1]), {
            "type": "update_predecessor",
            "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
        })

    def delete(self, key: str) -> dict:
        """
        Deletes a key-value pair from the distributed hash table.
        """
        key_id = sha1_hash(key)  # Hash the key to find its responsible node

        # Check if this node is responsible for the key
        if self.is_responsible_for_key(key_id):
            if key_id in self.data_store:
                del self.data_store[key_id]
                print(f"[DELETE] Key '{key}' deleted from node {self.node_id}")
                return {"status": "success", "message": f"Key '{key}' deleted"}
            else:
                print(f"[DELETE] Key '{key}' not found at node {self.node_id}")
                return {"status": "error", "message": "Key not found"}

        # Otherwise, forward the request to the responsible node
        successor = self.find_successor(key_id)
        if successor == (self.ip, self.port, self.node_id):
            print(f"[ERROR] delete: Responsible node thinks it's itself but key not found")
            return {"status": "error", "message": "Key not found in ring"}

        response = self.send_message((successor[0], successor[1]), {
            "type": "delete",
            "key": key
        })
        return response

    
    def join_ring(self):
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
                    # Directly use the bootstrap response for neighbors.
                    self.predecessor = normalize_node(response.get("predecessor"))
                    self.successor = normalize_node(response.get("successor"))
                    
                    # If no valid successor is provided, fallback to bootstrap.
                    if not self.successor or self.successor == (None, None, None):
                        self.successor = (self.bootstrap_ip, self.bootstrap_port, self.bootstrap_id)
                        print(f"[JOIN] Bootstrap is alone, setting it as my successor: {self.successor}")
                        
                    print(f"[JOIN] Joined the ring successfully.\n  Predecessor: {self.predecessor}\n  Successor: {self.successor}")

                    # Notify your successor to update its predecessor pointer,
                    # but only if your successor isn't yourself.
                    if self.successor and (self.successor[0], self.successor[1]) != (self.ip, self.port):
                        self.send_message((self.successor[0], self.successor[1]), {
                            "type": "update_predecessor",
                            "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
                        })
                else:
                    print(f"[JOIN] Join failed: {response.get('message')}")
        except Exception as e:
            print(f"[ERROR] Failed to join the ring: {e}")

    
    def replicate_data(self):
        """Placeholder for replication logic. Currently does nothing."""
        if self.replication_factor > 1:
            print(f"[REPLICATION] Future replication logic goes here. Factor: {self.replication_factor}")

    def depart(self, node_id: int) -> dict:
        """
        Removes a specific node from the Chord ring.
        - Transfers its keys to its successor.
        - Updates predecessor and successor pointers.
        - Notifies the departing node to shut down.
        """
        print(f"[DEPART] Request to remove node {node_id} from the ring.")

        self.send_message((self.bootstrap_ip, self.bootstrap_port), {
            "type": "depart",
            "node_id": self.node_id
        })

        # If this node is the one leaving, handle self-depart
        if node_id == self.node_id:
            return self.self_depart()
        
        # Find the departing node's successor and predecessor
        response = self.send_message((self.successor[0], self.successor[1]), {
            "type": "find_node",
            "node_id": node_id
        })
        departing_node = response.get("node")
        
        if not departing_node:
            print(f"[ERROR] Node {node_id} not found in the ring.")
            return {"status": "error", "message": "Node not found in ring"}

        departing_ip, departing_port, departing_id = departing_node["ip"], departing_node["port"], departing_node["node_id"]

        # Ask the departing node for its successor and predecessor
        response = self.send_message((departing_ip, departing_port), {"type": "get_neighbors"})
        if response.get("status") != "success":
            return {"status": "error", "message": "Failed to get neighbors of departing node"}

        predecessor = response.get("predecessor")
        successor = response.get("successor")

        print(f"[DEPART] Node {departing_id} -> Predecessor: {predecessor}, Successor: {successor}")

        # Ensure correct tuple indexing
        if isinstance(predecessor, list):
            predecessor = tuple(predecessor)
        if isinstance(successor, list):
            successor = tuple(successor)

        # Transfer keys to successor
        if successor and successor != (departing_ip, departing_port, departing_id):
            print(f"[DEPART] Transferring keys from {departing_id} to {successor[2]}")
            transfer_response = self.send_message((successor[0], successor[1]), {
                "type": "transfer_keys",
                "keys": response.get("keys", {})
            })
            if transfer_response.get("status") == "success":
                print(f"[DEPART] Key transfer to {successor[2]} successful.")
            else:
                print(f"[ERROR] Key transfer failed: {transfer_response.get('message', 'Unknown error')}")

        # Notify predecessor to update its successor
        if predecessor:
            print(f"[DEPART] Updating predecessor {predecessor[2]} to point to {successor[2]}")
            self.send_message((predecessor[0], predecessor[1]), {
                "type": "update_successor",
                "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
            })

        # Notify successor to update its predecessor
        if successor and successor != (departing_ip, departing_port, departing_id):
            print(f"[DEPART] Updating successor {successor[2]} to point to {predecessor[2]}")
            self.send_message((successor[0], successor[1]), {
                "type": "update_predecessor",
                "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
            })

        # Finally, tell the node to shut itself down
        print(f"[DEPART] Notifying {departing_id} to shut down.")
        self.send_message((departing_ip, departing_port), {
            "type": "shutdown"
        })

        return {"status": "success", "message": f"Node {node_id} has left the ring."}

    def self_depart(self) -> dict:
        """
        Handles the departure of the current node itself.
        """
        print(f"[DEPART] Node {self.node_id} is leaving the ring.")

        # If this is the only node in the ring, just shut down.
        if self.successor == (self.ip, self.port, self.node_id) and self.predecessor is None:
            print(f"[DEPART] Node {self.node_id} was the only node in the ring. Shutting down.")
            self.running = False
            return {"status": "success", "message": f"Node {self.node_id} has left."}

        # Transfer keys to successor
        if self.successor and self.successor != (self.ip, self.port, self.node_id):
            print(f"[DEPART] Transferring keys to successor {self.successor[2]}")
            self.send_message((self.successor[0], self.successor[1]), {
                "type": "transfer_keys",
                "keys": self.data_store
            })

        # Notify predecessor
        if self.predecessor:
            print(f"[DEPART] Notifying predecessor {self.predecessor[2]} to update successor.")
            self.send_message((self.predecessor[0], self.predecessor[1]), {
                "type": "update_successor",
                "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
            })

        # Notify successor
        if self.successor and self.successor != (self.ip, self.port, self.node_id):
            print(f"[DEPART] Notifying successor {self.successor[2]} to update predecessor.")
            self.send_message((self.successor[0], self.successor[1]), {
                "type": "update_predecessor",
                "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
            })

        # Stop the node
        self.running = False
        return {"status": "success", "message": f"Node {self.node_id} has departed."}
    
    def find_node(self, node_id: int) -> dict:
        """
        Finds a node in the ring given its node_id.
        Returns the node's (ip, port, node_id) if found.
        """
        node_id = int(node_id)
        print(f"[FIND_NODE] Searching for node {node_id}")

        # If this node matches, return itself
        if self.node_id == node_id:
            return {"status": "success", "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}}

        # Find the responsible node
        successor = self.find_successor(node_id)
        
        # If the responsible node is this node, return an error (node not found)
        if successor == (self.ip, self.port, self.node_id):
            return {"status": "error", "message": f"Node {node_id} not found in ring"}

        # Query the responsible node for confirmation
        response = self.send_message((successor[0], successor[1]), {
            "type": "confirm_node",
            "node_id": node_id
        })

        if response.get("status") == "success":
            return response
        else:
            return {"status": "error", "message": f"Node {node_id} not found"}

    def send_message(self, address, message):
        """Send a message to a given address but handle connection failures gracefully."""
        if address == (self.ip, self.port):
            return self.process_request(message)
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
