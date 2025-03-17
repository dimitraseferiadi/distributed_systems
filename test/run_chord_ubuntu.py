import subprocess
import time

BOOTSTRAP_SCRIPT = "./bootstrap.py"
NODE_SCRIPT = "./node.py"
CLIENT_SCRIPT = "./client.py"

BOOTSTRAP_IP = "127.0.0.1"
BOOTSTRAP_PORT = 5000 

NODE_IP = "127.0.0.1"
NUM_NODES = 10
BASE_PORT = 5001  
REPLICATION_FACTOR = 2
REPLICATION_CONSISTENCY = "eventual"


# Start Bootstrap Node
print("[START] Launching Bootstrap Node...")
subprocess.Popen(f'gnome-terminal -- bash -c "python3 {BOOTSTRAP_SCRIPT} {BOOTSTRAP_IP} {BOOTSTRAP_PORT} {REPLICATION_FACTOR} {REPLICATION_CONSISTENCY}; exec bash"', shell=True)
time.sleep(2)

# Start Nodes
for i in range(NUM_NODES):
    port = BASE_PORT + i
    print(f"[START] Launching Node on port {port}...")
    subprocess.Popen(f'gnome-terminal -- bash -c "python3 {NODE_SCRIPT} {NODE_IP} {port} {REPLICATION_FACTOR} {REPLICATION_CONSISTENCY} {BOOTSTRAP_IP} {BOOTSTRAP_PORT}; exec bash"', shell=True)
    time.sleep(2)

# Start Client
print("[START] Launching Client...")
subprocess.Popen(f'gnome-terminal -- bash -c "python3 {CLIENT_SCRIPT}; exec bash"', shell=True)

