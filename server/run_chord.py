import subprocess
import time

BOOTSTRAP_SCRIPT = "C:/Users/dhmht/Downloads/distributed_systems/server/bootstrap.py"
NODE_SCRIPT = "C:/Users/dhmht/Downloads/distributed_systems/server/node.py"
CLIENT_SCRIPT = "C:/Users/dhmht/Downloads/distributed_systems/server/client.py"


BOOTSTRAP_IP = "127.0.0.1"
BOOTSTRAP_PORT = 5000 

NODE_IP = "127.0.0.1"
NUM_NODES = 3
BASE_PORT = 6000  

# Start Windows Terminal command
wt_cmd = 'wt new-tab'

# Start Bootstrap Node in first pane
print("[START] Launching Bootstrap Node...")
wt_cmd += f' cmd /k "python {BOOTSTRAP_SCRIPT} {BOOTSTRAP_IP} {BOOTSTRAP_PORT}"'

time.sleep(2)  # Wait for Bootstrap Node to start

# Create an equal-sized grid for Nodes and Clients
wt_cmd += f' ; split-pane -p 50 -H cmd /k "python {NODE_SCRIPT} {NODE_IP} {BASE_PORT} {BOOTSTRAP_IP} {BOOTSTRAP_PORT}"'
time.sleep(2)
wt_cmd += f' ; split-pane -p 50 -V cmd /k "python {NODE_SCRIPT} {NODE_IP} {BASE_PORT+1} {BOOTSTRAP_IP} {BOOTSTRAP_PORT}"'
time.sleep(2)
wt_cmd += f' ; split-pane -p 50 -V cmd /k "python {NODE_SCRIPT} {NODE_IP} {BASE_PORT+2} {BOOTSTRAP_IP} {BOOTSTRAP_PORT}"'
time.sleep(2)

# Open Clients in a **new tab** with the first client immediately
print(f"[START] Launching Client")
client_cmd = f'cmd /k "python {CLIENT_SCRIPT}"'
wt_cmd += f' ; new-tab {client_cmd}'


# Run all commands in a single Windows Terminal window
subprocess.run(wt_cmd, shell=True)
