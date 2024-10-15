# cli.py

import argparse
from chatdb import ChatDB, parse_excel, parse_csv

def create_table_and_import_data(db, sheets):
    for sheet_name, dataframe in sheets.items():
        # Using the new method for creating tables and importing data
        headers = dataframe.columns.tolist()
        data = dataframe.values.tolist()
        response = db.create_table_and_insert_data(sheet_name, headers, data)
        print(response)

def main():
    parser = argparse.ArgumentParser(description="Upload Excel or CSV files into MySQL.")
    parser.add_argument("file", help="Path to the Excel or CSV file to upload")
    parser.add_argument("--host", required=True, help="MySQL host")
    parser.add_argument("--user", required=True, help="MySQL user")
    parser.add_argument("--password", required=True, help="MySQL password")
    parser.add_argument("--database", required=True, help="MySQL database")
    args = parser.parse_args()

    db = ChatDB(args.host, args.user, args.password, args.database)

    if args.file.endswith(".xlsx"):
        sheets = parse_excel(args.file)
        if sheets:
            create_table_and_import_data(db, sheets)
        else:
            print("Failed to parse Excel file.")
    elif args.file.endswith(".csv"):
        # Use the parse_csv utility to read the CSV file
        headers, data = parse_csv(args.file)
        if headers and data:
            # Assuming 'args' is an ArgumentParser object and 'file' is an argument.
            table_name = args.file.split("/")[-1].split(".")[0]  # Extract only the filename without the extension
  # Use the filename as table name
            response = db.create_table_and_insert_data(table_name, headers, data)
            print(response)
        else:
            print("Failed to parse CSV file.")
    else:
        print("Unsupported file type. Only .xlsx and .csv files are supported.")
    
    db.close()  # Make sure to close the database connection

if __name__ == "__main__":
    main()
