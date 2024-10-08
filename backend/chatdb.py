import mysql.connector
import pandas as pd
import csv

class ChatDB:
    def __init__(self, host, user, password, database):
        # Connect to MySQL server without specifying a database
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password
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
            print("Table created successfully.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.conn.rollback()

    def import_data(self, table_name, dataframe):
        try:
            cols = ",".join([str(i) for i in dataframe.columns.tolist()])

            for i, row in dataframe.iterrows():
                sql = f"INSERT INTO {table_name} ({cols}) VALUES ({'%s, ' * (len(row) - 1)} %s)"
                self.cursor.execute(sql, tuple(row))

            self.conn.commit()
            print(f"Data imported successfully into {table_name}.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.conn.rollback()

    def create_table_and_insert_data(self, table_name, headers, data):
        """Method to create table dynamically and insert data (used for CSV uploads)."""
        try:
            # Create table with columns based on headers
            columns = ", ".join([f"{header.replace(' ', '_')} VARCHAR(255)" for header in headers])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
            self.create_table(create_table_query)

            # Insert data into the table
            for row in data:
                placeholders = ", ".join(["%s"] * len(row))
                insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"
                self.cursor.execute(insert_query, tuple(row))
            
            self.conn.commit()
            print(f"Data imported successfully into {table_name}.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.conn.rollback()

    def upload_csv(self, file_path):
        """Method to upload CSV file data into a single table."""
        headers, data = parse_csv(file_path)  # Parse the CSV file
        if headers and data:
            table_name = file_path.split("/")[-1].split(".")[0]  # Use the file name as table name
            self.create_table_and_insert_data(table_name, headers, data)
        else:
            print("Failed to parse CSV file.")

    def upload_excel(self, file_path):
        """Method to upload Excel file data into tables for each sheet."""
        sheets = parse_excel(file_path)
        if sheets:
            for sheet_name, dataframe in sheets.items():
                self.create_table(sheet_name, dataframe)  # Create table for each sheet
                self.import_data(sheet_name, dataframe)  # Import data into each table
        else:
            print("Failed to parse Excel file.")

    def close(self):
        self.cursor.close()
        self.conn.close()

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
