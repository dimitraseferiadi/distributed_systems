import socket
import threading
import hashlib
import json
import threading
import time

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
    elif isinstance(node, list):
        return tuple(node)
    return node

class ChordNode:
    def __init__(self, ip: str, port: int, replication_factor: int, replication_consistency: str = "eventual", bootstrap_ip: str = None, bootstrap_port: int = None):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        # Unique node ID generated from ip:port
        self.node_id = sha1_hash(f"{ip}:{port}")
        self.predecessor = None
        self.successor = None  # Initially, self is the only node in the ring.
        self.data_store = {}  # <key: hashed_key, value: value> dictionary for key-value pairs
        
        self.replication_factor = replication_factor
        self.replication_consistency = replication_consistency

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


    def async_replicate(self, message, replica_list):
        """Asynchronously replicate a write message to a list of replicas."""
        def replicate():
            # Optionally add a small delay to simulate lazy propagation.
            time.sleep(1)
            for replica in replica_list:
                # Send the replication message to each replica.
                self.send_message((replica[0], replica[1]), message)
        threading.Thread(target=replicate, daemon=True).start()

    def process_request(self, request: dict) -> dict:
        """
        Process different types of incoming requests.
        The request should have a "type" field (e.g., insert, query, delete, join, depart)
        and any additional data needed for the operation.
        """
   
        req_type = request.get("type")
        try:
            if req_type == "insert":
                if self.replication_consistency == "linearizability":
                    return self.chain_insert_primary(request["key"], request["value"])
                else:
                    return self.insert(request["key"], request["value"])
            elif (req_type == "query" and request.get("key") == "*"):
                new_request = {
                    "type": "query_all",
                    "origin": (self.ip, self.port, self.node_id),
                    "data": self.data_store.copy(),
                    "initial": True 
                }
                return self.handle_global_query(new_request)
            elif req_type == "query_all":
                return self.handle_global_query(request)
            elif req_type == "query":
                key = request.get("key")
                key_id = sha1_hash(key)
                if self.replication_consistency == "linearizability":
                    return self.chain_query(key_id)
                else:
                    return self.normal_query(key)
            elif req_type == "delete":
                if self.replication_consistency == "linearizability":
                    return self.chain_delete_primary(request["key"])
                else:
                    return self.delete(request["key"])
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
            elif req_type == "reset_successor":
                self.reset_successor()
                return {"status": "success"}
            elif req_type == "reset_predecessor":
                self.reset_predecessor()
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
            elif req_type == "get_keys":
                print(f"[TRANSFER] Node {self.node_id} providing {len(self.data_store)} keys for transfer.")
                return {"status": "success", "keys": self.data_store}
            elif req_type == "transfer_keys":
                keys_to_transfer = request.get("keys", {})
                if isinstance(keys_to_transfer, dict):  
                    cleaned_keys = {int(key.strip()): value for key, value in keys_to_transfer.items()}
                    self.data_store.update(cleaned_keys)
                    print(f"[TRANSFER] Received {len(keys_to_transfer)} keys.")
                    return {"status": "success"}
                else:
                    print("[ERROR] transfer_keys received invalid data format")
                    return {"status": "error", "message": "Invalid data format"}
            elif req_type == "replicate_insert":
                key_id = request["key_id"]
                value = request["value"]
                remaining = request.get("remaining", 0)
                self.data_store[key_id] = value
                print(f"[REPLICATE INSERT] Node {self.node_id} stored replica for key {key_id}")
                if remaining > 1:
                    self.replicate_insert(key_id, value, remaining=remaining - 1)
                return {"status": "success", "node": self.node_id}
            elif req_type == "replicate_delete":
                key = request["key"]
                remaining = request.get("remaining", 0)
                key_id = sha1_hash(key)
                if key_id in self.data_store:
                    del self.data_store[key_id]
                    print(f"[REPLICATE DELETE] Node {self.node_id} deleted replica for key {key}")
                if remaining > 1:
                    self.replicate_delete(key, remaining=remaining - 1)
                return {"status": "success", "node": self.node_id}
            elif req_type == "repair_replication":
                if self.replication_consistency == "linearizability":
                    self.repair_replication()
                else:
                    for key_id, value in self.data_store.items():
                        if self.is_responsible_for_key(key_id):
                            print(f"[REPAIR] Re-replicating key {key_id} from node {self.node_id}")
                            self.replicate_insert(key_id=str(key_id), value=value, remaining=self.replication_factor - 1)
                    return {"status": "success"}
            elif req_type == "chain_insert_primary":
                return self.chain_insert_primary(request["key"], request["value"])
            elif req_type == "chain_insert_replica":
                return self.chain_insert_replica(request["key_id"], request["value"], request.get("remaining", self.replication_factor - 1))
            elif req_type == "chain_query":
                key_id = request.get("key_id")
                forwarded = request.get("forwarded", False)
                return self.chain_query(str(key_id), forwarded)
            elif req_type == "chain_delete_primary":
                return self.chain_delete_primary(request["key"])
            elif req_type == "chain_delete_replica":
                return self.chain_delete_replica(request["key"], request.get("remaining", self.replication_factor - 1))

            return {"status": "error", "message": "Unknown request type"}
        
        except Exception as e:
            print(f"[ERROR] process_request exception: {e}")
            return {"status": "error", "message": str(e)}


    def insert(self, key: str, value: str) -> dict:
        """Insert a key-value pair into the correct node."""
        key_id = sha1_hash(key)
        print(f"[INSERT] Key: {key} → Hash ID: {key_id}")

        # Check if this node is responsible for storing the key.
        if self.is_responsible_for_key(key_id):
            self.data_store[key_id] = value
            print(f"[INSERT] Stored at Node {self.node_id}: {key} → {value}")
        

        # Instead of waiting for replication to finish, spawn async replication.
            replica_message = {
                "type": "replicate_insert",
                "key_id": key_id,
                "value": value,
                "remaining": self.replication_factor - 1
            }
        # Get the list of replica nodes. You can implement get_replica_nodes() 
        # based on your ring structure. Here we assume the successor chain.
            replicas = []
            next_node = normalize_node(self.successor)
            for _ in range(self.replication_factor - 1):
                if next_node != (self.ip, self.port, self.node_id):
                    replicas.append(next_node)
                    # For a basic version, get the next successor in the ring.
                    next_node = self.send_message((next_node[0], next_node[1]), {"type": "get_neighbors"}).get("successor", next_node)
                else:
                    break
        
        # Spawn asynchronous replication.
            self.async_replicate(replica_message, replicas)
        
            return {"status": "success", "node": self.node_id, "message": "Key stored (primary) – replication in progress"}
    
        # Otherwise, forward the request to the correct node.
        # Look up the correct successor using the key's hash
        successor = self.find_successor(key_id)
        if successor == [self.ip, self.port, self.node_id]:
            self.data_store[key_id] = value
            print(f"[INSERT] (After lookup) Key belongs here. Stored locally at Node {self.node_id}")
            return {"status": "success", "node": self.node_id, "message": "Key stored locally"}
    
        response = self.send_message((successor[0], successor[1]), {
            "type": "insert",
            "key": key,
            "value": value
        })

        return response if response else {"status": "error", "message": "Failed to store key"}


    def replicate_insert(self, key_id: str, value: str, remaining: int):
        if remaining <= 0:
            return
        # If the ring has only one node, nothing to replicate.
        if self.successor == (self.ip, self.port, self.node_id):
            return
        # Forward the replication request to your successor.
        message = {
            "type": "replicate_insert",
            "key_id": key_id,
            "value": value,
            "remaining": remaining
        }
        self.send_message((self.successor[0], self.successor[1]), message)

    def chain_insert_primary(self, key: str, value: str) -> dict:
        key_id = sha1_hash(key)
        # If not responsible, forward to correct primary.
        if not self.is_responsible_for_key(key_id):
            successor = self.find_successor(key_id)
            message = {
                "type": "insert",
                "key": key,
                "value": value
            }
            return self.send_message((successor[0], successor[1]), message)
        
        # Store locally.
        self.data_store[key_id] = value
        print(f"[CHAIN INSERT PRIMARY] Node {self.node_id} stored key '{key}'")
        
        # If only one replica or no valid successor, we are also the tail.
        if self.replication_factor == 1 or self.successor == (self.ip, self.port, self.node_id):
            return {"status": "success", "message": "Write committed at tail", "node": self.node_id}
            
        # Forward the update to our immediate successor (the replica handler).
        message = {
            "type": "chain_insert_replica",
            "key_id": key_id,
            "value": value,
            "remaining": self.replication_factor - 1
        }
        return self.send_message((self.successor[0], self.successor[1]), message)

    def chain_insert_replica(self, key_id: str, value: str, remaining: int) -> dict:
        self.data_store[key_id] = value
        print(f"[CHAIN INSERT REPLICA] Node {self.node_id} stored key")
        
        if remaining > 1:
            message = {
                "type": "chain_insert_replica",
                "key_id": key_id,
                "value": value,
                "remaining": remaining - 1
            }
            return self.send_message((self.successor[0], self.successor[1]), message)
        else:
            # Tail node.
            return {"status": "success", "message": "Write committed at tail", "node": self.node_id}

    def is_responsible_for_key(self, key_id):
        """Check if this node is responsible for storing the given key."""
        if self.predecessor is None or self.successor == (self.ip, self.port, self.node_id):
            return True  # If there's no predecessor, this node is alone in the ring.

        pred_id = self.predecessor[2]  # Predecessor node ID

        return in_range(key_id, pred_id, self.node_id, include_end=True)
    

    def find_successor(self, identifier, hops=0):
        MAX_HOPS = 10  # Prevent infinite recursion
        self_id = (self.ip, self.port, self.node_id)

        # If we've hit the hop limit, assume self.
        #if hops >= MAX_HOPS:
         #   return self_id

        # If the ring is a one-node ring, return self.
        if self.successor == self_id:
            return self_id

        # If this node is responsible for the key, return self.
        if self.is_responsible_for_key(identifier):
            return self_id

        # Normalize the successor pointer.
        succ = self.successor
        succ = normalize_node(succ)
        
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
    
    def update_predecessor(self, node_info):
        #new_pred = (node_info["ip"], node_info["port"], node_info["node_id"])
        new_pred = normalize_node(node_info)
        
        # Normalize existing predecessor if necessary.
        if self.predecessor is not None:
            self.predecessor = normalize_node(self.predecessor)
        
        # For simplicity, if there's no predecessor, accept the new one.
        if self.predecessor is None:
            self.predecessor = new_pred
            print(f"[UPDATE] Predecessor set to: {self.predecessor}")
            return
        
        if in_range(new_pred[2], self.predecessor[2], self.node_id, include_end=False):
            self.predecessor = new_pred
            print(f"[UPDATE] New predecessor updated to: {self.predecessor}")
        #else:
            #print(f"[WARNING] Ignored invalid predecessor update: {node_info}")

    def reset_predecessor(self):
        """Reset the predecessor to allow a proper update."""
        print(f"[RESET] Predecessor reset before updating.")
        self.predecessor = None
    
    def reset_successor(self):
        """Reset the successor to allow a proper update."""
        print(f"[RESET] Successor reset before updating.")
        self.successor = None
    
    def update_successor(self, node_info):
        #new_succ = (node_info["ip"], node_info["port"], node_info["node_id"])
        new_succ = normalize_node(node_info)

        # Normalize existing successor if necessary.
        if self.successor is not None:
            self.successor = normalize_node(self.successor)
        
        # For simplicity, if there's no successor, accept the new one.
        if self.successor is None:
            self.successor = new_succ
            print(f"[UPDATE] Successor set to: {self.successor}")
            return
        
        # Use your logic (e.g., in_range) to decide if the update is valid.
        if in_range(new_succ[2], self.node_id, self.successor[2], include_end=False):
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

        self.successor = normalize_node(self.successor)

        # If this query has looped back to the origin, return the collected data
        if not initial and origin == (self.ip, self.port, self.node_id):
            return {"status": "success", "data": collected_data}
        
        # If only one node in the ring, return data directly
        if self.successor == tuple([self.ip, self.port, self.node_id]):
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
        
        successor = self.find_successor(key_id)

        if successor == [self.ip, self.port, self.node_id]:
            print(f"[WARNING] Detected self-loop while querying {key}. Returning error.")
            return {"status": "error", "message": "Key not found in ring"}
        response = self.send_message((successor[0], successor[1]), {
            "type": "query",
            "key": key
        })
        return response if response else {"status": "error", "message": "Query failed"}

    def chain_query(self, key_id: str, forwarded: bool = False) -> dict:
        key_id = int(key_id)
        # If this is not a forwarded (tail) request, check if we are the responsible primary.
        if not forwarded and not self.is_responsible_for_key(key_id):
            successor = self.find_successor(key_id)
            message = {
                "type": "chain_query",
                "key_id": str(key_id),
                "forwarded": False  # initial query
            }
            return self.send_message((successor[0], successor[1]), message)
        
        # Determine the tail node for this key.
        tail = self.get_tail_for_key(key_id)

        # If we are not the tail and this is not a forwarded request, forward as a tail request.
        if (self.ip, self.port, self.node_id) != tail and not forwarded:
            message = {
                "type": "chain_query",
                "key_id": str(key_id),
                "forwarded": True 
            }
            return self.send_message((tail[0], tail[1]), message)
        else:
            # We're either the tail or this is already a forwarded tail request; serve the read.
            if key_id in self.data_store:
                return {"status": "success", "value": self.data_store[key_id]}
            else:
                return {"status": "error", "message": "Key not found"}

    def get_tail_for_key(self, key_id) -> tuple:
        primary = self.find_successor(key_id)
        current = primary
        for _ in range(self.replication_factor - 1):
            response = self.send_message((current[0], current[1]), {"type": "get_neighbors"})
            if response.get("successor"):
                current = tuple(response["successor"])
            else:
                break
        return current
        
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
        key_id = sha1_hash(key)
        if self.is_responsible_for_key(key_id):
            if key_id in self.data_store:
                del self.data_store[key_id]
                print(f"[DELETE] Key '{key}' deleted from node {self.node_id}")
            # Spawn async replication deletion.
                replica_message = {
                    "type": "replicate_delete",
                    "key": key,
                    "remaining": self.replication_factor - 1
                }
            # Here, determine replica nodes similarly.
                replicas = []
                next_node = normalize_node(self.successor)
                for _ in range(self.replication_factor - 1):
                    if next_node != (self.ip, self.port, self.node_id):
                        replicas.append(next_node)
                        next_node = self.send_message((next_node[0], next_node[1]), {"type": "get_neighbors"}).get("successor", next_node)
                    else:
                        break
                self.async_replicate(replica_message, replicas)
                return {"status": "success", "message": f"Key '{key}' deleted"}
            else:
                return {"status": "error", "message": "Key not found"}
    
        successor = self.find_successor(key_id)
        if successor == (self.ip, self.port, self.node_id):
            return {"status": "error", "message": "Key not found in ring"}
        response = self.send_message((successor[0], successor[1]), {"type": "delete", "key": key})
        return response

    def replicate_delete(self, key: str, remaining: int):
        if remaining <= 0:
            return
        if self.successor == (self.ip, self.port, self.node_id):
            return
        message = {
            "type": "replicate_delete",
            "key": key,
            "remaining": remaining
        }
        self.send_message((self.successor[0], self.successor[1]), message)

    def chain_delete_primary(self, key: str) -> dict:
        key_id = sha1_hash(key)
        if not self.is_responsible_for_key(key_id):
            successor = self.find_successor(key_id)
            message = {
                "type": "delete",
                "key": key
            }
            return self.send_message((successor[0], successor[1]), message)
        
        if key_id in self.data_store:
            del self.data_store[key_id]
            print(f"[CHAIN DELETE PRIMARY] Node {self.node_id} deleted key '{key}'")
        else:
            print(f"[CHAIN DELETE PRIMARY] Key '{key}' not found at Node {self.node_id}")
        
        if self.replication_factor == 1 or self.successor == (self.ip, self.port, self.node_id):
            return {"status": "success", "message": "Delete committed at tail", "node": self.node_id}
        
        message = {
            "type": "chain_delete_replica",
            "key": key,
            "remaining": self.replication_factor - 1
        }
        return self.send_message((self.successor[0], self.successor[1]), message)

    def chain_delete_replica(self, key: str, remaining: int) -> dict:
        key_id = sha1_hash(key)
        if key_id in self.data_store:
            del self.data_store[key_id]
            print(f"[CHAIN DELETE REPLICA] Node {self.node_id} deleted key '{key}'")
        else:
            print(f"[CHAIN DELETE REPLICA] Key '{key}' not found at Node {self.node_id}")
        
        if remaining > 1:
            message = {
                "type": "chain_delete_replica",
                "key": key,
                "remaining": remaining - 1
            }
            return self.send_message((self.successor[0], self.successor[1]), message)
        else:
            return {"status": "success", "message": "Delete committed at tail", "node": self.node_id}

    def repair_replication(self):
        # Iterate over a copy of the data_store to avoid iteration-modification errors.
        for key_id, value in list(self.data_store.items()):
            # Determine the current primary responsible for key_id.
            current_primary = self.find_successor(key_id)
            if current_primary != (self.ip, self.port, self.node_id):
                # The responsible node has changed (e.g. due to departure).
                print(f"[REPAIR] Key {key_id} no longer belongs to node {self.node_id}. Forwarding to new primary {current_primary}")
                # Remove key from local store.
                del self.data_store[key_id]
                # Forward the key to the new primary to initiate chain replication.
                message = {
                    "type": "chain_insert_replica",
                    "key_id": key_id,
                    "value": value,
                    "remaining": self.replication_factor - 1
                }
                self.send_message((current_primary[0], current_primary[1]), message)
            else:
                # This node is still responsible; re-establish replication along the chain.
                print(f"[REPAIR] Re-replicating key {key_id} from node {self.node_id}")
                message = {
                    "type": "chain_insert_replica",
                    "key_id": key_id,
                    "value": value,
                    "remaining": self.replication_factor - 1,
                }
                self.send_message((self.successor[0], self.successor[1]), message)
        return {"status": "success"}


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

                    # Notify your successor to update its predecessor pointer, but only if your successor isn't yourself.
                    if self.successor and (self.successor[0], self.successor[1]) != (self.ip, self.port):
                        self.send_message((self.successor[0], self.successor[1]), {
                            "type": "update_predecessor",
                            "node": {"ip": self.ip, "port": self.port, "node_id": self.node_id}
                        })
                else:
                    print(f"[JOIN] Join failed: {response.get('message')}")
        except Exception as e:
            print(f"[ERROR] Failed to join the ring: {e}")

    def send_message(self, address, message):
        """Send a message to a given address but handle connection failures gracefully."""
        #if address == (self.ip, self.port) and message.get("type") == "query":
         #   return {"status": "error", "message": "trying to connect to itself"}
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if isinstance(address, dict):
                    address = (address["ip"], address["port"])
                
                s.settimeout(30)
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
        try:
            while True:
                continue
        except KeyboardInterrupt:
            print("\n[EXIT] Shutting down node.")

if __name__ == "__main__":
    import sys
    # Example usage:
    # Run as: python node.py <ip> <port> <replication_factor> <replication_consistency> [<bootstrap_ip> <bootstrap_port>]
    # python node.py 127.0.0.1 6000 3 127.0.0.1 5000
    if len(sys.argv) < 5:
        print("Usage: python node.py <ip> <port> <replication_factor> <replication_consistency> [<bootstrap_ip> <bootstrap_port>]")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    replication_factor = int(sys.argv[3])
    replication_consistency = sys.argv[4]
    bootstrap_ip = sys.argv[5] if len(sys.argv) > 5 else None
    bootstrap_port = int(sys.argv[6]) if len(sys.argv) > 6 else None

    node = ChordNode(ip, port, replication_factor, replication_consistency, bootstrap_ip, bootstrap_port)
    node.run()
