import socket
import sys

# Define default server address and port
DEFAULT_SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345

# Check for command line arguments for IP address
server_host = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SERVER_HOST

# Connect to server
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    client_socket.connect((server_host, SERVER_PORT))
    print(f"[*] Connected to server at {server_host}:{SERVER_PORT}")
except socket.error as e:
    print(f"Failed to connect to server: {e}")
    sys.exit(1)

while True:
    try:
        command = input("Enter command: ").strip()
        if not command:
            continue  # Skip empty input

        # Send command to server
        if command == "QUIT":
            client_socket.send(command.encode())
            print(f"[*] Sent: {command}")
            response = client_socket.recv(1024).decode().strip()
            print(f"[*] Server response: {response}")
            client_socket.close()
            break
        else:
            client_socket.send(command.encode())
            print(f"[*] Sent: {command}")

            # Set a timeout for the response
            client_socket.settimeout(10.0)  # 10 seconds timeout

            # Receive and display response
            response = client_socket.recv(1024).decode().strip()
            print(f"[*] Server response: {response}")

            # Reset the timeout to None (blocking mode)
            client_socket.settimeout(None)

    except socket.timeout as e:
        print("Request timed out.")
    except socket.error as e:
        print(f"Socket error occurred: {e}")
    except KeyboardInterrupt:
        print("\n[*] Interrupted by user, exiting.")
        break
    except Exception as e:
        print(f"An error occurred: {e}")
        break
        
# Close client socket
try:
    client_socket.close()
except socket.error as e:
    print(f"Failed to close the socket: {e}")
