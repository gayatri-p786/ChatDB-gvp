# cli.py

import os
import sys
from chatdb import ChatDB, parse_excel, parse_csv, load_model, generate_query  # Import necessary functions


def create_table_and_import_data(db, sheets):
    for sheet_name, dataframe in sheets.items():
        # Using the new method for creating tables and importing data
        headers = dataframe.columns.tolist()
        data = dataframe.values.tolist()
        print(f"Creating table '{sheet_name}' and loading data...")
        response = db.create_table_and_insert_data(sheet_name, headers, data)
        # print(response)
    print("Data upload completed.")

def upload_data(db):
    file_path = input("Enter the path to the Excel or CSV file to upload: ")
    if not os.path.isfile(file_path):
        print("File not found. Please provide a valid file path.")
        return

    if file_path.endswith(".xlsx"):
        sheets = parse_excel(file_path)
        if sheets:
            create_table_and_import_data(db, sheets)
        else:
            print("Failed to parse Excel file.")
    elif file_path.endswith(".csv"):
        headers, data = parse_csv(file_path)
        if headers and data:
            table_name = os.path.splitext(os.path.basename(file_path))[0]  # Extract filename without extension
            response = db.create_table_and_insert_data(table_name, headers, data)
            print(response)
        else:
            print("Failed to parse CSV file.")
    else:
        print("Unsupported file type. Only .xlsx and .csv files are supported.")

def generate_sample_queries(db, db_name):
    try:
        # Retrieve the schema information for the connected database
        schema_info = db.get_schema_info(db_name)

        # Load the model and tokenizer for generating SQL queries
        tokenizer, model = load_model('./sql_query_generator')

        # Generate the SQL queries
        generated_queries = generate_query(schema_info, tokenizer, model)

        # Handle the generated queries
        for i, query in enumerate(generated_queries):
            print(f"Generated Query {i + 1}: {query}")
    except Exception as e:
        print(f"An error occurred while generating sample queries: {e}")

def main():
    # Initialize connection details for MySQL
    host = "localhost"  # Use the common host value here
    user = "root"  # Use the common username here
    password = "root"  # Use the common password here

    while True:
        print("\nMenu:")
        print("1. Upload Data (Create DB and Tables)")
        print("2. Generate Sample Queries")
        print("3. Exit")
        choice = input("Choose an option (1-3): ")

        if choice == '1':
            # Ask for the database name for creating tables and inserting data
            database = input("Enter the MySQL database name to upload data: ")
            db = ChatDB(host, user, password, database)  # New instance for the specified database
            upload_data(db)  # Upload data to this specific database
            db.close()  # Close the connection after upload

        elif choice == '2':
            # Ask for the database name to generate sample queries
            database = input("Enter the MySQL database name to generate sample queries: ")
            db = ChatDB(host, user, password, database)  # New instance for the specified database
            generate_sample_queries(db, database)  # Generate queries for this specific database
            db.close()  # Close the connection after query generation

        elif choice == '3':
            print("Exiting the program.")
            break

        else:
            print("Invalid choice. Please enter a number between 1 and 3.")

if __name__ == "__main__":
    main()
