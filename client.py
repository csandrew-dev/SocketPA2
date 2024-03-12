import socket
import sys
import logging

# Define default server address and port
DEFAULT_SERVER_HOST = '127.0.0.1'
DEFAULT_SERVER_PORT = 12345
BUFFER_SIZE = 1024  # Size of the buffer used to receive data from the server
TIMEOUT_SECONDS = 10.0  # Timeout duration for server response

# Configure the logging
logging.basicConfig(level=logging.INFO)

# Define the main function for the client program
def main(server_host, server_port):
    # Using `with` statement for automatic socket management
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        try:
            # Attempt to connect to the server
            client_socket.connect((server_host, server_port))
            logging.info(f"Connected to server at {server_host}:{server_port}")
        except socket.error as e:
            # If the connection fails, log the error and exit the program
            logging.error(f"Failed to connect to server: {e}")
            sys.exit(1)

        while True:
            try:
                # Prompt the user to enter a command
                command = input("Enter command: ").strip()
                if not command:
                    continue  # Skip empty input and prompt again

                # Send the command to the server
                client_socket.send(command.encode())
                logging.info(f"Sent: {command}")

                # Special handling for the QUIT command
                if command.upper() == "QUIT":
                    handle_server_response(client_socket)
                    break  # Exit the loop and close the client after handling response

                # Handle general server response for other commands
                handle_server_response(client_socket)

            except socket.timeout:
                # Handle the case where the server doesn't respond within the timeout
                logging.warning("Request timed out.")
            except socket.error as e:
                # Handle any other socket related errors and exit the loop
                logging.error(f"Socket error occurred: {e}")
                break
            except KeyboardInterrupt:
                # Allow the user to interrupt the client with Ctrl+C
                logging.info("Interrupted by user, exiting.")
                break
            except Exception as e:
                # Log any other exceptions that occur
                logging.error(f"An error occurred: {e}")
                break

# Define a function to handle server responses
def handle_server_response(client_socket):
    # Apply a timeout to the socket to wait for a response
    client_socket.settimeout(TIMEOUT_SECONDS)
    
    try:
        # Receive the response from the server
        response = client_socket.recv(BUFFER_SIZE).decode().strip()
        if response == '':
            # If the response is empty, it means the server closed the connection
            raise socket.error("Server closed the connection.")
        # Log the server's response
        logging.info(f"Server response: {response}")
    finally:
        # Reset the socket timeout to None (blocking mode) after handling the response
        client_socket.settimeout(None)


# Execute the program
if __name__ == "__main__":
    # Retrieve server host and port from command line arguments
    server_host = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SERVER_HOST
    server_port = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_SERVER_PORT
    
    # Run the main function with the provided host and port
    main(server_host, server_port)
