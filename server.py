import socket
import sqlite3
import sys

# Define server address and port
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345

# Connect to SQLite database
conn = sqlite3.connect('database.db')
cursor = conn.cursor()

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


# Define functions to process different commands

def process_buy_command(command_parts):
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

def process_sell_command(command_parts):
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

def process_list_command():
    # Fetch all records from the Stocks table
    cursor.execute("SELECT * FROM Stocks")
    stocks_data = cursor.fetchall()

    if not stocks_data:
        return "No records found in the Stocks database."

    # Generate the response message with the list of records
    response = "200 OK\n"
    for row in stocks_data:
        response += f"{row[0]}. {row[1]} {row[3]} {row[4]}\n"

    return response

def process_balance_command():
    # Fetch all records from the Users table
    cursor.execute("SELECT * FROM Users")
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

# Accept only one client
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_HOST, SERVER_PORT))
server_socket.listen(1)
print(f"[*] Listening on {SERVER_HOST}:{SERVER_PORT}")

while True:
    try: 
        client_socket, client_address = server_socket.accept()
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
            else:
                command = command_parts[0]
                if command == "BUY":
                    response = process_buy_command(command_parts)
                elif command == "SELL":
                    response = process_sell_command(command_parts)
                elif command == "LIST":
                    response = process_list_command()
                elif command == "BALANCE":
                    response = process_balance_command()
                elif command == "SHUTDOWN":
                    response = "200 OK"
                    client_socket.send(response.encode())
                    client_socket.close()
                    server_socket.close()
                    conn.close()
                    sys.exit()
                else:
                    response = "400 invalid command"
                
            # Send the response back to the client
            client_socket.send(response.encode())

    except KeyboardInterrupt:
        print("\n[*] Interrupted by user, exiting.")
        break
    except Exception as e:
        print(f"An error occurred: {e}")
        break
        

    # Close socket
    client_socket.close()

# Close database connection
conn.close()