# Import necessary libraries
import socket
import sqlite3
import sys
import threading

# Define server address and port
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345

#root user id
root_user_id = 1

# A global variable to keep track of whether the server is running
is_server_running = True

# Define a list to keep track of all the client threads
client_threads = []

# Define a lock for database access
db_lock = threading.Lock()

# Connect to SQLite database
conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Create tables if they do not exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Users (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT,
        last_name TEXT,
        user_name TEXT NOT NULL,
        password TEXT,
        usd_balance DOUBLE NOT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Stocks (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_symbol VARCHAR(4) NOT NULL,
        stock_name VARCHAR(20) NOT NULL,
        stock_balance DOUBLE,
        user_id INTEGER,
        FOREIGN KEY (user_id) REFERENCES Users (ID)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS ActiveUsers (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        user_name TEXT NOT NULL,
        ip_address TEXT NOT NULL,
        port TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES Users (ID)
    )
''')

# Insert default user into Users table
cursor.execute("SELECT * FROM Users WHERE user_name = ?", ('John',))
existing_user = cursor.fetchone()

if not existing_user:
    cursor.execute("INSERT INTO Users (first_name, last_name, user_name, password, usd_balance) VALUES (?, ?, ?, ?, ?)",
                   ('John', 'Doe', 'John', 'John01', 100.0))
    conn.commit()

# Insert root user for testing shutdown
cursor.execute("SELECT * FROM Users WHERE user_name = ?", ('Root',))
root_user = cursor.fetchone()

if not root_user:
    cursor.execute("INSERT INTO Users (first_name, last_name, user_name, password, usd_balance) VALUES (?, ?, ?, ?, ?)",
                   ('Root', 'User', 'Root', 'Root01', 1000000.0))
    conn.commit()

# Close database connection after initial setup
# conn.close()


# Define functions to process different commands

def process_buy_command(conn, cursor, command_parts):
    """
    Process the 'BUY' command to buy stocks.
    """
    # Extract relevant information from command_parts and cast to appropriate types
    try:
        ticker = command_parts[1]
        stock_amount = float(command_parts[2])
        stock_price = float(command_parts[3])
        user_id = int(command_parts[4])
    except IndexError:
        return "400 invalid command, missing arguments"
    except ValueError:
        return "400 invalid command, invalid arguments"
    except Exception as e:
        return f"400 invalid command, an error occurred: {e}"

    # Check if the user exists
    cursor.execute("SELECT COUNT(*) FROM Users WHERE ID = ?", (user_id,))
    user_exists = cursor.fetchone()[0]

    if not user_exists:
        return f"400 user {user_id} not found."

    # Perform necessary operations
    # Fetch user's balance
    cursor.execute("SELECT usd_balance FROM Users WHERE ID = ?", (user_id,))
    user_balance = cursor.fetchone()[0]

    # Calculate the total cost of buying the stocks
    total_cost = stock_amount * stock_price

    # Check if the user has enough funds
    if user_balance < total_cost:
        return "400 insufficient funds"
    else:
        # Deduct the total cost from the user's balance
        new_balance = user_balance - total_cost
        cursor.execute("UPDATE Users SET usd_balance = ? WHERE ID = ?", (new_balance, user_id))

        # Check if the user already owns stocks of the given ticker
        cursor.execute("SELECT stock_balance FROM Stocks WHERE user_id = ? AND stock_symbol = ?", (user_id, ticker))
        existing_stock_balance = cursor.fetchone()

        if existing_stock_balance is None:
            # Insert a new record for the user's stocks
            cursor.execute("INSERT INTO Stocks (stock_symbol, stock_name, stock_balance, user_id) VALUES (?, ?, ?, ?)",
                           (ticker, "", stock_amount, user_id))
            updated_stock_balance = stock_amount  # Set the updated balance
        else:
            # Update the existing stock balance
            updated_stock_balance = existing_stock_balance[0] + stock_amount
            cursor.execute("UPDATE Stocks SET stock_balance = ? WHERE user_id = ? AND stock_symbol = ?",
                           (updated_stock_balance, user_id, ticker))

        # Commit the transaction
        conn.commit()

        # Generate appropriate response
        response = f"200 OK\nBOUGHT: New balance: {updated_stock_balance} {ticker}. USD balance ${new_balance}"
        return response

def process_sell_command(conn, cursor, command_parts):
    """
    Process the 'SELL' command to sell stocks.
    """
    # Extract relevant information from command_parts and cast to appropriate types
    try:
        ticker = command_parts[1]
        stock_amount = float(command_parts[2])
        stock_price = float(command_parts[3])
        user_id = int(command_parts[4])
    except IndexError:
        return "400 invalid command, missing arguments"
    except ValueError:
        return "400 invalid command, invalid arguments"
    except Exception as e:
        return f"400 invalid command, an error occurred: {e}"

    # Check if the user exists
    cursor.execute("SELECT COUNT(*) FROM Users WHERE ID = ?", (user_id,))
    user_exists = cursor.fetchone()[0]

    if not user_exists:
        return f"400 User {user_id} not found."

    # Check if the user has the specified amount of stock to sell
    cursor.execute("SELECT stock_balance FROM Stocks WHERE user_id = ? AND stock_symbol = ?", (user_id, ticker))
    existing_stock_balance = cursor.fetchone()

    if existing_stock_balance is None or existing_stock_balance[0] < stock_amount:
        return f"400 insufficient stock balance for {ticker}"

    # Fetch user's balance
    cursor.execute("SELECT usd_balance FROM Users WHERE ID = ?", (user_id,))
    user_balance = cursor.fetchone()[0]

    # Calculate the total amount to be received from selling the stocks
    total_amount = stock_amount * stock_price

    # Update user's balance and stock balance
    new_balance = user_balance + total_amount
    updated_stock_balance = existing_stock_balance[0] - stock_amount

    # Update user's balance and stock balance in the database
    cursor.execute("UPDATE Users SET usd_balance = ? WHERE ID = ?", (new_balance, user_id))
    cursor.execute("UPDATE Stocks SET stock_balance = ? WHERE user_id = ? AND stock_symbol = ?",
                   (updated_stock_balance, user_id, ticker))

    # Commit the transaction
    conn.commit()

    # Generate appropriate response
    response = f"200 OK\nSOLD: New balance: {updated_stock_balance} {ticker}. USD balance ${new_balance}"
    return response

def list_records(user_id, is_root, cursor):
    """
    List records from the database

.
    """
    if is_root:
        return process_list_command(user_id=5, cursor=cursor)  # Assuming root user ID is 5
    else:
        return process_list_command(user_id=user_id, cursor=cursor)

def process_list_command(user_id, cursor):
    """
    Process the 'LIST' command to list all stocks.
    """
    # Fetch all records from the Stocks table
    if user_id == root_user_id:
        # If the user is root, fetch stock data with user names for all users
        cursor.execute('''
            SELECT Stocks.ID, Stocks.stock_symbol, Stocks.stock_name, Stocks.stock_balance,
                   Users.first_name, Users.last_name, Users.user_name
            FROM Stocks
            JOIN Users ON Users.ID = Stocks.user_id
        ''')
    else:
        # If the user is not root, fetch only the stock data pertaining to the user
        cursor.execute('''
            SELECT Stocks.ID, Stocks.stock_symbol, Stocks.stock_name, Stocks.stock_balance,
                   Users.first_name, Users.last_name
            FROM Stocks
            JOIN Users ON Users.ID = Stocks.user_id
            WHERE Stocks.user_id = ?
        ''', (user_id,))
    stocks_data = cursor.fetchall()

    if not stocks_data:
        return "No records found in the Stocks database."

    # Generate the response message with the list of records
    response = "200 OK\n"
    for stock in stocks_data:
        if user_id == root_user_id:
            user_full_name = f"{stock[4]} {stock[5]}" if stock[4] and stock[5] else "Unknown User"
            response += f"{stock[0]} {stock[1]} {stock[3]} {user_full_name} {stock[6]}\n"
        else:
            response += f"{stock[0]} {stock[1]} {stock[3]}\n"

    return response

def process_balance_command(user_id, cursor):
    """
    Process the 'BALANCE' command to display user balances.
    """
    # Fetch all records from the Users table
    if user_id == root_user_id:
        cursor.execute("SELECT * FROM Users")
    else:
        cursor.execute("SELECT * FROM Users WHERE ID = ?", (user_id,))
    users_data = cursor.fetchall()

    if not users_data:
        return "No records found in the Users database."

    # Generate the response message with the balance for each user
    response = "200 OK\n"
    for user in users_data:
        balance = user[5]  # The USD balance is stored in the sixth column (index 5)
        if(not user[1] or  not user[2]):
            full_name = user[3]
        else:
            full_name = f"{user[1]} {user[2]}"

        response += f"Balance for user {full_name}: ${balance}\n"

    return response

def process_login_command(user_name, password, client_address):
    """
    Process the 'LOGIN' command to log in users.
    """
    try:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.cursor()
            # Select the user with a matching username and password
            cursor.execute("SELECT * FROM Users WHERE user_name = ? AND password = ?", (user_name, password))
            user = cursor.fetchone()
            if user:
                # Correct login
                user_id = user[0]
                cursor.execute("INSERT INTO ActiveUsers (user_id, user_name, ip_address, port) VALUES (?, ?, ?, ?)",
                                (user_id, user_name, client_address[0], client_address[1]))
                return "200 OK", user_id  # Return the user ID as well for future commands
            else:
                # Incorrect login
                return "403 Wrong UserID or Password", None
    except Exception as e:
        return f"500 Internal Server Error: {e}", None
    
def process_help_command(user_id=None, invalid_command=None):
    """
    Process the 'HELP' command to display help messages and assist with incorrect commands.

    Args:
        user_id (int, optional): The ID of the user, if available. Defaults to None.
        invalid_command (str, optional): The invalid command entered by the user. Defaults to None.

    Returns:
        str: The help message or assistance for incorrect commands.
    """
    if user_id is None:
        help_message = """
        To use the system, please log in using the following command:
        LOGIN <user_name> <password>

        Once logged in, you can access additional commands and get further help.
        """
    else:
        help_message = """
        Available Commands:
    - LOGIN <user_name> <password>: Log in with your username and password.
    - BUY <stock_symbol> <amount> <price> <user_id>: Buy stocks with the specified amount, price, and user ID.
    - SELL <stock_symbol> <amount> <price> <user_id>: Sell stocks with the specified amount, price, and user ID.
    - LIST [<user_id>]: List all stocks. If no user ID is provided, list all stocks for the logged-in user.
    - BALANCE [<user_id>]: Display the balance. If no user ID is provided, display the balance for the logged-in user.
    - LOOKUP <stock_name>: Search for stocks by name.
    - DEPOSIT <amount>: Deposit funds into your account.
    - LOGOUT: Log out from the system.
    - WHO: Display active users (root user only).
    - HELP: Display this help message.
    - QUIT: Terminate the connection.
    - SHUTDOWN: Shutdown the server (root user only).
        """

    if invalid_command:
        # Check if the invalid command is partially correct to provide suggestions
        suggestions = []
        available_commands = [
            "LOGIN", "BUY", "SELL", "LIST", "BALANCE", "LOOKUP", "DEPOSIT", "LOGOUT", "WHO", "HELP", "QUIT", "SHUTDOWN"
        ]
        for command in available_commands:
            if command.startswith(invalid_command.upper()):
                suggestions.append(command)
        
        if suggestions:
            return f"Did you mean one of these commands?: {' '.join(suggestions)}"

    return help_message

def handle_shutdown_command(user_id, cursor):
    """
    Process the 'SHUTDOWN' command to shutdown the server.
    """
    # Check if the user is root
    cursor.execute("SELECT ID FROM Users WHERE user_name = 'root'")
    root_user_id = cursor.fetchone()[0]
    if user_id == root_user_id:
        global is_server_running
        # Shutdown server and disconnect all clients
        print("Server shutdown initiated. All connected clients will be terminated.")
        is_server_running = False  # Set the variable to False to stop the server loop
        return "Server is shutting down."
    else:
        return "Error: Only the root user has the authority to execute a server shutdown."

def process_logout_command(cursor, user_id):
    """
    Process the 'LOGOUT' command to log out users.
    """
    # Delete the user from the ActiveUsers table
    cursor.execute("DELETE FROM ActiveUsers WHERE user_id = ?", (user_id,))
    return "200 OK"

def process_who_command(cursor, user_id):
    """
    Process the 'WHO' command to display active users.
    """
    # Check if the user is root
    if user_id != root_user_id:  # Assuming root user ID is 5
        return "403 Access denied: WHO command is only allowed for the root user."

    # Fetch active users from the database
    cursor.execute("SELECT user_name, ip_address FROM ActiveUsers")
    active_users = cursor.fetchall()

    if not active_users:
        return "No active users found."

    # Generate the response message with the list of active users
    response = "200 OK\nThe list of active users:\n"
    # Clear previous list
    for user in active_users:
        response += f"{user[0]} {user[1]}\n"

    return response

def process_lookup_command(user_id, cursor, command_parts):
    """
    Process the 'LOOKUP' command to search for stocks.
    """
    # Extract relevant information from command_parts
    try:
        stock_name = command_parts[1]
    except IndexError:
        return "400 invalid command, missing arguments"

    if user_id == root_user_id:  # Assuming root user ID is 1
        cursor.execute('''
            SELECT stock_symbol, stock_balance, user_name
            FROM Stocks
            JOIN Users ON Users.ID = Stocks.user_id
            WHERE stock_name LIKE ? OR stock_symbol LIKE ?
        ''', ('%' + stock_name + '%', '%' + stock_name + '%'))

        matched_stocks = cursor.fetchall()

        # Generate the response message with the list of matched records
        response = f"200 OK\nFound {len(matched_stocks)} match{'es' if len(matched_stocks) > 1 else ''} for '{stock_name}':\n"
        for stock in matched_stocks:
            response += f"{stock[0]} {stock[1]} {stock[2]}\n"
    else:
        # Search for the stock records matching the given name
        cursor.execute('''
            SELECT stock_symbol, stock_balance
            FROM Stocks
            JOIN Users ON Users.ID = Stocks.user_id
            WHERE (stock_name LIKE ? OR stock_symbol LIKE ?) AND user_id = ?
        ''', ('%' + stock_name + '%', '%' + stock_name + '%', user_id))

        matched_stocks = cursor.fetchall()

        # Generate the response message with the list of matched records
        response = f"200 OK\nFound {len(matched_stocks)} match{'es' if len(matched_stocks) > 1 else ''} for '{stock_name}':\n"
        for stock in matched_stocks:
            response += f"{stock[0]} {stock[1]}\n"

    if not matched_stocks:
        return f"404 Your search for '{stock_name}' did not match any records."

    return response

def process_deposit_command(conn, cursor, command_parts, client_address):
    """
    Process the 'DEPOSIT' command to deposit funds into a user's account.
    """
    try:
        amount = float(command_parts[1])
    except (IndexError, ValueError):
        return "400 invalid command, missing or invalid amount"

    # Fetch user_id from ActiveUsers table based on the client's IP address
    cursor.execute("SELECT user_id FROM ActiveUsers WHERE ip_address = ? AND port = ?", (client_address[0], client_address[1]))
    user_data = cursor.fetchone()

    if not user_data:
        return "403 not logged in, please login first"

    user_id = user_data[0]

    # Fetch user's balance
    cursor.execute("SELECT usd_balance FROM Users WHERE ID = ?", (user_id,))
    user_balance = cursor.fetchone()[0]

    # Update the user's balance
    new_balance = user_balance + amount
    cursor.execute("UPDATE Users SET usd_balance = ? WHERE ID = ?", (new_balance, user_id))

    # Commit the transaction
    conn.commit()

    # Generate appropriate response
    response = f"200 OK\nDEPOSIT: New balance: ${new_balance}"
    return response

def handle_client(client_socket, client_address):
    """
    Handle client connections and requests.
    """
    global is_server_running
    # Initialize the user_id as None to indicate that the client is not logged in
    user_id = None
    conn = sqlite3.connect('database.db')  # Create a new connection
    cursor = conn.cursor()  # Create a new cursor

    try:
        print(f"[*] Accepted connection from {client_address[0]}:{client_address[1]}")

        # Handle client requests
        while True:
            print("[*] Waiting for a command...")
            data = client_socket.recv(1024).decode().strip()
            if not data:
                break
            print(f"[*] Received: {data}")

            # Split the received data into command and arguments
            command_parts = data.split()
            print(f"[*] Command: {command_parts[0]}")

            # Ensure the command is valid and has the correct format
            if len(command_parts) < 1:
                response = "403 message format error"
            elif command_parts[0] == "HELP":
                response = process_help_command(user_id)
            elif user_id is None and command_parts[0] != "QUIT":
                if command_parts[0] == "LOGIN":
                    if len(command_parts) != 3:
                        response = "400 invalid command, missing arguments"
                    else:
                        response, user_id = process_login_command(command_parts[1], command_parts[2], client_address)
                else:
                    response = process_help_command()
            else:
                command = command_parts[0]

                if command == "BUY":
                    if user_id is None:
                        response = "403 not logged in, please login first"
                    else:
                        response = process_buy_command(conn, cursor, command_parts)  # Pass conn and cursor here
                elif command == "SELL":
                    if user_id is None:
                        response = "403 not logged in, please login first"
                    else:
                        response = process_sell_command(conn, cursor, command_parts)  # Pass conn and cursor here
                elif command == "LIST":
                    if user_id is None:
                        response = "403 not logged in, please login first"
                    else:
                        response = process_list_command(user_id, cursor)  # Pass cursor here
                elif command == "BALANCE":
                    if user_id is None:
                        response = "403 not logged in, please login first"
                    else:
                        response = process_balance_command(user_id, cursor)  # Pass cursor here
                elif command == "LOOKUP":
                    if user_id is None:
                        response = "403 not logged in, please login first"
                    else:
                        response = process_lookup_command(user_id, cursor, command_parts)
                elif command == "DEPOSIT":
                        response = process_deposit_command(conn, cursor, command_parts, client_address)  # Pass client_address here
                elif command == "LOGOUT":
                    response = process_logout_command(cursor, user_id)  # Pass cursor here
                    client_socket.send(response.encode())
                    # Remove the user from ActiveUsers table
                    cursor.execute("DELETE FROM ActiveUsers WHERE user_id = ?", (user_id,))
                    conn.commit()
                    client_socket.close()
                    break
                elif command == "WHO":
                        if user_id is None:
                            response = "403 not logged in, please login first"
                        else:
                            response = process_who_command(cursor, user_id)  # Pass cursor here

                elif command == "HELP":
                    response = process_help_command(user_id)
                elif command == "QUIT":
                    response = "200 OK"
                    client_socket.send(response.encode())
                    client_socket.close()
                    break
                elif command == "SHUTDOWN":
                    if user_id is None:
                        response = "403 not logged in, please login first"
                    else:
                        response = handle_shutdown_command(user_id, cursor)  # Pass cursor here
                        client_socket.send(response.encode())
                        client_socket.close()
                        break
                else:
                    response = "400 invalid command"
                   

            # Send the response back to the client
            client_socket.send(response.encode())

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # If the user is logged in and the client socket is still open, perform cleanup
        if user_id is not None and not client_socket._closed:
            # Remove the user from ActiveUsers table
            cursor.execute("DELETE FROM ActiveUsers WHERE user_id = ?", (user_id,))
            conn.commit()
        # Close cursor and connection
        cursor.close()
        conn.close()
        # Close client socket
        client_socket.close()

        

# Create a server socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_HOST, SERVER_PORT))
server_socket.listen(10)
print(f"[*] Listening on {SERVER_HOST}:{SERVER_PORT}")

# Accept multiple clients using threads
while is_server_running:
    try:
        client_socket, client_address = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()
        client_threads.append(client_thread)  # Add the thread to the client_threads list
    except KeyboardInterrupt:
        print("\n[*] Interrupted by user, initiating server shutdown.")
        break
    except socket.error:
        if not is_server_running:
            break  # Break if socket error occurs due to shutdown

# Close server socket
server_socket.close()
