import socket  # Import the socket module for network communication
import threading  # Import threading for concurrent execution
import time  # Import time to measure throughput

def connect_to_server(host='localhost', port=5000):
    """Creates a socket connection to the server."""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a TCP/IP socket
    try:
        client_socket.connect((host, port))  # Attempt to connect to the server
        print(f"Connected to Chordify server at {host}:{port}")  # Print success message
        return client_socket  # Return the socket object
    except ConnectionRefusedError:
        print("Failed to connect to the server. Make sure the server is running.")  # Handle connection failure
        return None  # Return None if connection fails

def send_command(client_socket, command):
    """Sends a command to the server and prints the response."""
    try:
        client_socket.sendall(command.encode('utf-8'))  # Send command to the server as bytes
        response = client_socket.recv(4096).decode('utf-8')  # Receive response from the server
        print(response)  # Print the server's response
    except (BrokenPipeError, ConnectionResetError):
        print("Lost connection to the server.")  # Handle lost connection

def process_file(filename, host, port):
    """Reads commands from a file and sends them to the server."""
    client_socket = connect_to_server(host, port)  # Connect to the server
    if not client_socket:
        return
    
    with open(filename, 'r') as file:
        commands = file.readlines()  # Read all commands from the file
    
    start_time = time.time()  # Record start time
    for command in commands:
        send_command(client_socket, command.strip())  # Send each command to the server
    end_time = time.time()  # Record end time
    
    total_time = end_time - start_time
    throughput = len(commands) / total_time if total_time > 0 else 0  # Compute throughput
    print(f"Processed {filename}: {throughput:.2f} commands per second.")
    
    client_socket.close()  # Close the connection

def run_experiments():
    """Runs batch experiments with multiple files and threads."""
    host = input("Enter server host (default: localhost): ") or 'localhost'  # Get server host
    port = input("Enter server port (default: 5000): ") or '5000'  # Get server port
    port = int(port)  # Convert port to integer
    
    files = [f"insert_{i}.txt" for i in range(10)]  # List of insert files
    threads = []
    
    for file in files:
        thread = threading.Thread(target=process_file, args=(file, host, port))  # Create a thread
        threads.append(thread)
        thread.start()  # Start the thread
    
    for thread in threads:
        thread.join()  # Wait for all threads to complete
    
    print("All experiments completed.")

def cli_client():
    """Interactive CLI client for the Chordify application."""
    host = input("Enter server host (default: localhost): ") or 'localhost'  # Get server host
    port = input("Enter server port (default: 5000): ") or '5000'  # Get server port
    port = int(port)  # Convert port to integer
    
    client_socket = connect_to_server(host, port)  # Connect to the server
    if not client_socket:
        return  # Exit if connection fails

    # Display available commands to the user
    print("\nAvailable commands:")
    print("insert <key> <value> - Insert a song")
    print("delete <key> - Delete a song")
    print("query <key> - Search for a song")
    print("depart - Leave the network")
    print("overlay - Show network topology")
    print("help - Show this help message")
    print("exit - Close the client")
    print("run_experiments - Run batch insert/query experiments")
    
    while True:
        try:
            command = input("Chordify> ").strip()  # Get user command input and remove extra spaces
            if command.lower() == "exit":  # Check if user wants to exit
                print("Closing connection...")
                client_socket.close()  # Close the connection
                break  # Exit the loop
            elif command.lower() == "run_experiments":
                run_experiments()  # Run batch experiments
            else:
                send_command(client_socket, command)  # Send command to the server
        except KeyboardInterrupt:
            print("\nExiting...")  # Handle Ctrl+C gracefully
            client_socket.close()  # Close the connection
            break  # Exit the loop

if __name__ == "__main__":
    cli_client()  # Run the CLI client

