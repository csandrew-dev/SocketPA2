import socket
import sqlite3
import sys
import threading

# Define server address and port
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345

# A global variable to keep track of whether the server is running
is_server_running = True

# Define a list to keep track of all the client threads
client_threads = []

# Connect to SQLite database
with sqlite3.connect('database.db') as conn:
    cursor = conn.cursor()
    # Your database operations...
    # No need to explicitly close the connection; it will be closed automatically.

# Create tables if not exist
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

# Insert default user into Users table
cursor.execute("SELECT * FROM Users WHERE user_name = ?", ('jd',))
existing_user = cursor.fetchone()

if not existing_user:
    cursor.execute("INSERT INTO Users (first_name, last_name, user_name, password, usd_balance) VALUES (?, ?, ?, ?, ?)",
                           ('John', 'Doe', 'jd', 'password', 100.0))
    conn.commit()

# Insert root user for testing shutdown
cursor.execute("SELECT * FROM Users WHERE user_name = ?", ('root',))
root_user = cursor.fetchone()

if not root_user:
    cursor.execute("INSERT INTO Users (first_name, last_name, user_name, password, usd_balance) VALUES (?, ?, ?, ?, ?)",
                           ('Root', 'User', 'root', 'rootpassword', 1000000.0))
    conn.commit()


# Close database connection after initial setup
conn.close()



# Define functions to process different commands

def process_buy_command(conn, cursor, command_parts):
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

def process_list_command(user_id, cursor):
    # Fetch all records from the Stocks table
    if user_id == 2:
        # If the user is root, fetch stock data with user names for all users
        cursor.execute('''
            SELECT Stocks.ID, Stocks.stock_symbol, Stocks.stock_name, Stocks.stock_balance,
                   Users.first_name, Users.last_name
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
    # for row in stocks_data:
    #     response += f"{row[0]}. {row[1]} {row[3]} {row[4]}\n"
    for stock in stocks_data:
        user_full_name = f"{stock[4]} {stock[5]}" if stock[4] and stock[5] else "Unknown User"
        if user_id == 2:
            response += f"{stock[0]} {stock[1]} {stock[3]} {user_full_name}\n"
        else:
            response += f"{stock[0]} {stock[1]} {stock[3]}\n"

    return response


def process_balance_command(user_id, cursor):
    # Fetch all records from the Users table
    if user_id == 2:
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

def process_login_command(cursor, user_name, password):
    # Select the user with a matching username and password
    cursor.execute("SELECT * FROM Users WHERE user_name = ? AND password = ?", (user_name, password))
    user = cursor.fetchone()
    if user:
        # Correct login
        return "200 OK", user[0]  # Return the user ID as well for future commands
    else:
        # Incorrect login
        return "403 Wrong UserID or Password", None
    
def process_help_command(user_id=None):
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
    - HELP: Display this help message.
    - QUIT: Terminate the connection.
    - SHUTDOWN: Shutdown the server (only accessible to the root user).
        """

    return help_message

def handle_shutdown_command(user_id, cursor):
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




def handle_client(client_socket, client_address):
    global is_server_running
    # Initialize the user_id as None to indicate that the client is not logged in
    user_id = None

    try:
        print(f"[*] Accepted connection from {client_address[0]}:{client_address[1]}")

        # Individual connections to the database
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()

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
                        response, user_id = process_login_command(cursor, command_parts[1], command_parts[2])
                else:
                    response = process_help_command()
            else:
                command = command_parts[0]

                if command == "BUY":
                    response = process_buy_command(conn, cursor, command_parts)
                elif command == "SELL":
                    response = process_sell_command(conn, cursor, command_parts)
                elif command == "LIST":
                    response = process_list_command(user_id, cursor)
                elif command == "BALANCE":
                    response = process_balance_command(user_id, cursor)
                elif command == "HELP":
                    response = process_help_command(user_id)
                elif command == "QUIT":
                    response = "200 OK"
                    client_socket.send(response.encode())
                    client_socket.close()
                    break
                elif command == "SHUTDOWN":
                    response = handle_shutdown_command(user_id, cursor)
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
        # Close client socket
        client_socket.close()

        # Close database connection
        conn.close()

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
