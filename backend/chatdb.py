from flask import Flask, request, jsonify
import mysql.connector
import pandas as pd
import csv
import os

app = Flask(__name__)

class ChatDB:
    def __init__(self, host, user, password, database):
        # Connect to MySQL server without specifying a database
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
        )
        cursor = conn.cursor()

        # Check if the database exists, create if not
        cursor.execute(f"SHOW DATABASES LIKE '{database}';")
        result = cursor.fetchone()

        if result:
            print(f"Database '{database}' already exists.")
        else:
            print(f"Database '{database}' does not exist. Creating it now.")
            cursor.execute(f"CREATE DATABASE {database};")
            print(f"Database '{database}' created successfully.")

        cursor.close()
        conn.close()

        # Now connect to the newly created or existing database
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.conn.cursor()

    def create_table(self, create_table_sql):
        try:
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            return {"message": "Table created successfully."}
        except mysql.connector.Error as err:
            self.conn.rollback()
            return {"error": str(err)}

    def create_table_and_insert_data(self, table_name, headers, data):
        """Method to create table dynamically and insert data (used for CSV uploads)."""
        print(f"Table name to be created: {table_name}") 
        try:
            # Create table with columns based on headers
            columns = ", ".join([f"`{header.replace(' ', '_')}` VARCHAR(255)" for header in headers])  # Use backticks here
            create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns});"  # Wrap table name in backticks
            print(f"Executing SQL for table creation: {create_table_query}")  # Print the SQL query for table creation
            response = self.create_table(create_table_query)

            # Insert data into the table
            for row in data:
                placeholders = ", ".join(["%s"] * len(row))
                insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{header}`' for header in headers])}) VALUES ({placeholders})"  # Wrap header names in backticks
                print(f"Executing SQL for data insertion: {insert_query} with values: {row}")  # Print the SQL query for data insertion
                self.cursor.execute(insert_query, tuple(row))

            self.conn.commit()
            return {"message": f"Data imported successfully into {table_name}."}
        except mysql.connector.Error as err:
            self.conn.rollback()
            return {"error": str(err)}


    def execute_custom_query(self, query, params=None):
        """Executes a custom SQL query and returns the result."""
        try:
            print(f"Executing custom SQL query: {query} with params: {params}")  # Print the SQL query being executed
            self.cursor.execute(query, params)
            results = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            data = [dict(zip(columns, row)) for row in results]
            return {"data": data}
        except mysql.connector.Error as err:
            return {"error": str(err)}


    def close(self):
        self.cursor.close()
        self.conn.close()

# Flask Routes to Expose the API

@app.route("/api/upload/csv", methods=["POST"])
def upload_csv():
    """Uploads a CSV file and imports the data into a database table."""
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    file = request.files['file']

    # Save file temporarily to process
    file_path = os.path.join("/tmp", file.filename)
    file.save(file_path)

    # Parse CSV and insert into database
    headers, data = parse_csv(file_path)
    if headers and data:
        table_name = file.filename.split(".")[0]  # Use the filename as table name
        db = ChatDB(host="localhost", user="root", password="password", database="chatdb")
        response = db.create_table_and_insert_data(table_name, headers, data)
        db.close()
        return jsonify(response)
    else:
        return jsonify({"error": "Failed to parse CSV file."}), 400

@app.route("/api/query", methods=["POST"])
def execute_query():
    """Executes a custom SQL query and returns the result."""
    req_data = request.get_json()
    query = req_data.get("query")
    params = req_data.get("params", None)
    
    if not query:
        return jsonify({"error": "Query is required"}), 400

    db = ChatDB(host="localhost", user="root", password="password", database="chatdb")
    result = db.execute_custom_query(query, params)
    db.close()
    return jsonify(result)

# Utility functions for CSV and Excel parsing
def parse_excel(file_path):
    try:
        excel_data = pd.ExcelFile(file_path)
        sheets = {sheet: pd.read_excel(excel_data, sheet_name=sheet) for sheet in excel_data.sheet_names}
        print(f"Excel file parsed successfully. Sheets: {excel_data.sheet_names}")
        return sheets
    except Exception as e:
        print(f"Error parsing Excel file: {e}")
        return None
    
def parse_csv(file_path):
    """Parses the uploaded CSV file into headers and data rows."""
    try:
        with open(file_path, mode='r') as file:
            reader = csv.reader(file)
            headers = next(reader)  # First row is assumed to be the header
            rows = [row for row in reader]  # Remaining rows are the data
            print(f"CSV file parsed successfully. Columns: {headers}")
            return headers, rows
    except Exception as e:
        print(f"Error parsing CSV file: {e}")
        return None, None

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
