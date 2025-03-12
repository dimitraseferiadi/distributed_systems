from node import ChordNode
import socket
import threading
import json
import hashlib


# Utility function for hashing keys using SHA1.
def sha1_hash(key: str) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest(), 16)

class BootstrapNode(ChordNode):
    def __init__(self, ip: str, port: int, replication_factor: int):
        super().__init__(ip, port, replication_factor)
        print(f"[BOOTSTRAP] Starting bootstrap node with ID: {self.node_id} at {self.ip}:{self.port}")
        
        # Store nodes as dictionaries with keys: ip, port, node_id
        self.nodes = [{"ip": self.ip, "port": self.port, "node_id": self.node_id}]

        # Ensure thread safety
        self.lock = threading.Lock()  

        # Start server thread to listen for join requests.
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()
        
    def handle_join(self, new_node_info: dict) -> dict:
        """
        Handles a node joining and assigns its predecessor and successor.
        Also updates the bootstrap node's own successor pointer so that it
        is not left pointing to itself when other nodes are present.
        """
        print(f"[BOOTSTRAP] Received join request from node: {new_node_info}")

        bootstrap_info = {"ip": self.ip, "port": self.port, "node_id": self.node_id}
        
        with self.lock:
            # Ensure the bootstrap node is in the nodes list.
            if not any(node["node_id"] == self.node_id for node in self.nodes):
                self.nodes.append(bootstrap_info)
            
            # Check for duplicates.
            if any(node["node_id"] == new_node_info["node_id"] for node in self.nodes):
                return {"status": "error", "message": "Node already exists"}
            
            # Add the new node.
            self.nodes.append(new_node_info)
            self.nodes.sort(key=lambda node: node["node_id"])
            
            # Determine the new node's neighbors.
            predecessor, successor = self.find_neighbors(new_node_info["node_id"])
            
            # **Update the bootstrap node's own successor pointer.**
            # Find the bootstrap node in the sorted list.
            bootstrap_index = next(i for i, node in enumerate(self.nodes) if node["node_id"] == self.node_id)
            # The new successor for bootstrap is the next node in the sorted order.
            new_bootstrap_successor = self.nodes[(bootstrap_index + 1) % len(self.nodes)]
            if new_bootstrap_successor["node_id"] != self.node_id:
                self.successor = new_bootstrap_successor
                print(f"[BOOTSTRAP] Updated bootstrap successor to: {self.successor}")
            else:
                # Only bootstrap exists.
                self.successor = bootstrap_info
            
            # Notify the new node's neighbors so that they update their pointers.
            self.send_message(predecessor, {"type": "update_successor", "node_info": new_node_info})
            self.send_message(successor, {"type": "update_predecessor", "node_info": new_node_info})
        
        print(f"[BOOTSTRAP] Node {new_node_info['node_id']} joined. Current nodes: {self.nodes}")
        return {"status": "success", "predecessor": predecessor, "successor": successor}
    
    def handle_depart(self, departing_node_id: int) -> dict:
        """
        Handles a node departure by:
        - Removing the departing node from the node list.
        - Requesting the departing node to transfer its keys to its successor.
        - Updating the departed node's neighbors to point to each other.
        - Sending a shutdown request to the departing node.
        """
        print(f"[BOOTSTRAP] Handling departure of node {departing_node_id}")

        with self.lock:
            # Ensure the node exists before attempting removal
            departing_node_id = int(departing_node_id)
            departing_node = next((node for node in self.nodes if node["node_id"] == departing_node_id), None)
            if not departing_node:
                print(f"[WARNING] Node {departing_node_id} not found in Bootstrap's record. Skipping.")
                return {"status": "error", "message": "Node not found in Bootstrap list"}

            # Identify predecessor and successor
            predecessor, successor = self.find_neighbors(departing_node_id)
            print(f"predecessor: {predecessor}, successor: {successor}")

            # Ensure valid sucessor
            if successor and successor["node_id"] == departing_node_id:
                successor = None

            # Ensure valid predecessor
            if predecessor and predecessor["node_id"] == departing_node_id:
                predecessor = None
                
            # Request keys from departing node
            key_response = self.send_message((departing_node["ip"], departing_node["port"]), {
                "type": "get_keys"
            })

            keys_to_transfer = key_response.get("keys", {})
            print(f"[TRANSFER] Collected {len(keys_to_transfer)} keys from departing node {departing_node_id}.")

            # Request departing node to transfer its keys to successor
            if keys_to_transfer and successor:
                print(f"[TRANSFER] Sending {len(keys_to_transfer)} keys to successor {successor['node_id']}")
                self.send_message((successor["ip"], successor["port"]), {
                    "type": "transfer_keys",
                    "keys": keys_to_transfer
                })

            # Notify predecessor and successor to update their pointers
            if predecessor:
                self.send_message(predecessor, {"type": "reset_successor"}) 
                self.send_message(predecessor, {"type": "update_successor", "node_info": successor})
            if successor:
                self.send_message(successor, {"type": "reset_predecessor"}) 
                self.send_message(successor, {"type": "update_predecessor", "node_info": predecessor})

            # Send shutdown request to the departing node
            self.send_message((departing_node["ip"], departing_node["port"]), {
                "type": "shutdown"
            })

            # Remove the departing node from Bootstrap's list
            self.nodes = [node for node in self.nodes if node["node_id"] != departing_node_id]
            print(f"[BOOTSTRAP] Node {departing_node_id} removed. Updated nodes: {self.nodes}")

            # Update Bootstrap’s successor if the departed node was its direct successor
            if self.successor[2] == departing_node_id:
                new_bootstrap_successor = self.nodes[0] if self.nodes else {"ip": self.ip, "port": self.port, "node_id": self.node_id}
                self.successor = (new_bootstrap_successor["ip"], 
                    new_bootstrap_successor["port"], 
                    new_bootstrap_successor["node_id"])
                print(f"[BOOTSTRAP] Updated successor to: {self.successor}")

            # Trigger replication repair on the successor so that keys are re-replicated properly.
            if successor:
                self.send_message(successor, {"type": "repair_replication"})
        return {"status": "success", "message": f"Node {departing_node_id} successfully removed from the network"}

    def find_neighbors(self, node_id: int):
        """
        Returns the predecessor and the successor of the node.
        """
        if len(self.nodes) == 1:
            return None, None
        index = next(i for i, node in enumerate(self.nodes) if node["node_id"] == node_id)
        predecessor = self.nodes[index - 1] if index > 0 else self.nodes[-1]
        successor = self.nodes[index + 1] if index < len(self.nodes) - 1 else self.nodes[0]
        return predecessor, successor
    
    def get_overlay(self) -> dict:
        """
        Returns the current ring topology, only including active nodes.
        """
        active_nodes = []
        for node in self.nodes:
            if self.is_node_alive(node):
                active_nodes.append(node)

        with self.lock:
            self.nodes = active_nodes  # Remove unreachable nodes

        return {"status": "success", "overlay": active_nodes}

    def is_node_alive(self, node: dict) -> bool:
        """
        Check if a node is still active by attempting a connection.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)  # Quick timeout to avoid long delays
                s.connect((node["ip"], node["port"]))
            return True
        except (socket.error, ConnectionRefusedError):
            return False

    def process_request(self, request: dict) -> dict:
        """Override request processing to include bootstrap-specific requests."""
        req_type = request.get("type")

        if req_type == "join":
            node_info = request.get("node_info")
            return self.handle_join(node_info)
        elif req_type == "depart":
            node_id = request.get("node_id")
            return self.handle_depart(node_id)
        elif req_type == "overlay":
            return self.get_overlay()
        else:
            return super().process_request(request)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python bootstrap.py <ip> <port> <replication_factor")
        # python bootstrap.py 127.0.0.1 5000
        exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    replication_factor = int(sys.argv[3])
    bootstrap = BootstrapNode(ip, port, replication_factor)

    # Keep the bootstrap node running indefinitely.
    try:
        while True:
            continue
    except KeyboardInterrupt:
        print("\n[BOOTSTRAP] Shutting down.")
