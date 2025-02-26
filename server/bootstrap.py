import socket
import threading
import json
import hashlib

# Utility function for hashing keys using SHA1.
def sha1_hash(key: str) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest(), 16)

class BootstrapNode:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.node_id = sha1_hash(f"{ip}:{port}")
        print(f"[BOOTSTRAP] Starting bootstrap node with ID: {self.node_id} at {self.ip}:{self.port}")
        
        # Store nodes as dictionaries with keys: ip, port, node_id
        self.nodes = []
        # Ensure thread safety
        self.lock = threading.Lock()  

        # Start server thread to listen for join requests.
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()

    def start_server(self):
        """Listen for incoming join (and possibly other) requests."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.address)
        server_socket.listen(5)
        print(f"[BOOTSTRAP] Listening on {self.ip}:{self.port} for join requests...")

        while True:
            client_socket, _ = server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client_socket,)).start()

    def handle_request(self, client_socket: socket.socket):
        try:
            message = client_socket.recv(4096).decode()
            if not message:
                client_socket.close()
                return
            request = json.loads(message)
            response = self.process_request(request)
            client_socket.send(json.dumps(response).encode())
        except Exception as e:
            print(f"[BOOTSTRAP ERROR] {e}")
        finally:
            client_socket.close()

    def process_request(self, request: dict) -> dict:
        req_type = request.get("type")
        node_info = request.get("node_info")

        if req_type == "join":
            return self.handle_join(node_info)
        elif req_type == "depart":
            return self.handle_depart(node_info)
        elif req_type == "overlay":
            return self.get_overlay()
        else:
            return {"status": "error", "message": "Unsupported request type"}

    def handle_join(self, new_node_info: dict) -> dict:
        """
        Handles node joining, assigns predecessor and successor.
        """
        print(f"[BOOTSTRAP] Received join request from node: {new_node_info}")
        
        with self.lock:
            if any(node["node_id"] == new_node_info["node_id"] for node in self.nodes):
                return {"status": "error", "message": "Node already exists"}
            
            self.nodes.append(new_node_info)
            self.nodes.sort(key=lambda node: node["node_id"])  # Ensure order

            predecessor, successor = self.find_neighbors(new_node_info["node_id"])

        print(f"[BOOTSTRAP] Node {new_node_info['node_id']} joined. Current nodes: {self.nodes}")
        return {"status": "success", "predecessor": predecessor, "successor": successor}


    def handle_depart(self, departing_node_info: dict) -> dict:
        """
        Handles node departure and updates the ring.
        """
        departing_node_id = departing_node_info["node_id"]
        print(f"[BOOTSTRAP] Node {departing_node_id} is departing.")

        with self.lock:
            # Find the departing node and remove it
            self.nodes = [node for node in self.nodes if node["node_id"] != departing_node_id]

            # Update predecessor and successor of affected nodes
            if self.nodes:
                predecessor, successor = self.find_neighbors(departing_node_id)

                if predecessor:
                    self.send_message(predecessor, {"type": "update_successor", "node": successor})
                if successor:
                    self.send_message(successor, {"type": "update_predecessor", "node": predecessor})

        print(f"[BOOTSTRAP] Updated node list after departure: {self.nodes}")
        return {"status": "success"}

    
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
