import subprocess
import time
import sys
import os

# Adjust paths if needed
BOOTSTRAP_SCRIPT = "bootstrap.py"
NODE_SCRIPT = "node.py"

# Bootstrap node settings
BOOTSTRAP_IP = "127.0.0.1"
BOOTSTRAP_PORT = 5000  

# Node settings
NODE_IP = "127.0.0.1"
NUM_NODES = 3
BASE_PORT = 6000  # Ports for nodes will be 6000, 6001, 6002...

def run_command(command):
    """Runs a command in a new terminal window."""
    if sys.platform.startswith("win"):
        subprocess.Popen(["start", "cmd", "/k"] + command, shell=True)
    elif sys.platform.startswith("linux"):
        subprocess.Popen(["x-terminal-emulator", "-e"] + command)
    else:
        print("Unsupported OS. Run manually:", " ".join(command))

# Start Bootstrap Node
print("[START] Launching Bootstrap Node...")
run_command(["python3", BOOTSTRAP_SCRIPT, BOOTSTRAP_IP, str(BOOTSTRAP_PORT)])

# Wait for Bootstrap Node to be ready
time.sleep(2)

# Start Chord Nodes
for i in range(NUM_NODES):
    node_port = BASE_PORT + i
    print(f"[START] Launching Node {i+1} on port {node_port}...")
    run_command(["python3", NODE_SCRIPT, NODE_IP, str(node_port), BOOTSTRAP_IP, str(BOOTSTRAP_PORT)])

print("[INFO] All nodes started. Check terminal windows for logs.")
