from node import ChordNode
import socket
import threading
import json
import hashlib


# Utility function for hashing keys using SHA1.
def sha1_hash(key: str) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest(), 16)

class BootstrapNode(ChordNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)
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


    def handle_depart(self, departing_node_info: dict) -> dict:
        departing_node_id = departing_node_info.get("node_id")
        if departing_node_id is None:
            return {"status": "error", "message": "Invalid departure request. No node_id provided."}

        print(f"[BOOTSTRAP] Node {departing_node_id} is departing.")

        with self.lock:
            # Look up the node to remove.
            node_to_remove = self.get_node_by_id(departing_node_id)
            if node_to_remove is None:
                return {"status": "error", "message": f"Node {departing_node_id} not found in the ring."}
            
            # Remove the node from the list.
            self.nodes.remove(node_to_remove)
            print(f"[DEBUG] Removed node {departing_node_id}. New node list: {self.nodes}")

            # If there are still nodes left, update neighbors based on the new ring.
            if self.nodes:
                # In the new sorted list, determine the neighbors of the departing node's previous position.
                # To do this, we can determine where the departing node would have been.
                sorted_ids = [node["node_id"] for node in self.nodes]
                # Find the index where the departing node would fit.
                insert_index = 0
                for i, nid in enumerate(sorted_ids):
                    if nid > departing_node_id:
                        insert_index = i
                        break
                else:
                    insert_index = len(sorted_ids)

                # The predecessor is the node just before this index (wrap-around if needed).
                pred_index = (insert_index - 1) % len(self.nodes)
                # The successor is the node at the insertion index (wrap-around if needed).
                succ_index = insert_index % len(self.nodes)
                predecessor = self.nodes[pred_index]
                successor = self.nodes[succ_index]

                print(f"[DEBUG] New neighbors for updates: predecessor: {predecessor}, successor: {successor}")

                # Notify the neighbors to update their pointers.
                self.send_message((predecessor["ip"], predecessor["port"]), {
                    "type": "update_successor",
                    "node_info": successor
                })
                self.send_message((successor["ip"], successor["port"]), {
                    "type": "update_predecessor",
                    "node_info": predecessor
                })
            else:
                print("[DEBUG] No nodes left in the ring after departure.")

        print(f"[BOOTSTRAP] Updated node list after departure: {self.nodes}")
        return {"status": "success", "message": f"Node {departing_node_id} has left the ring."}


    
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
        node_info = request.get("node_info")

        if req_type == "join":
            return self.handle_join(node_info)
            
        elif req_type in ("depart", "node_departed"):
            node_info = request.get("node_info") or request.get("node")

            # Allow specifying only a node_id for departure
            if not node_info:
                node_id = request.get("node_id")
                if node_id is None:
                    return {"status": "error", "message": "No node information provided for departure."}
                node_info = self.get_node_by_id(node_id)
                if not node_info:
                    return {"status": "error", "message": f"Node {node_id} not found in the ring."}

            return self.handle_depart(node_info)
        elif req_type == "overlay":
            return self.get_overlay()
        else:
            return super().process_request(request)

    def get_node_by_id(self, node_id: int):
        """Find a node in the ring by its ID."""
        print(f"[DEBUG] Searching for node_id: {node_id}")
        print(f"[DEBUG] Current nodes: {self.nodes}")

        for node in self.nodes:
            print(f"[DEBUG] Checking node: {node['node_id']}")
            if node["node_id"] == node_id:
                return node
        return None


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python bootstrap.py <ip> <port>")
        exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    bootstrap = BootstrapNode(ip, port)

    # Keep the bootstrap node running indefinitely.
    try:
        while True:
            command = input("Bootstrap> ").strip().lower()
            if command == "overlay":
                print(bootstrap.get_overlay())
            elif command == "help":
                print("Commands:\n overlay - Print current ring topology\n help - Show this message\n exit - Stop the bootstrap node")
            elif command == "exit":
                print("Exiting bootstrap node.")
                break
    except KeyboardInterrupt:
        print("\n[BOOTSTRAP] Shutting down.")
