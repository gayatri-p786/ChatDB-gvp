# cli.py

import argparse
from chatdb import ChatDB, parse_excel

def create_tables_from_sheets(db, sheets):
    for sheet_name, dataframe in sheets.items():
        columns = ", ".join([f"{col.replace(' ', '_')} VARCHAR(255)" for col in dataframe.columns])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {sheet_name} ({columns});"
        db.create_table(create_table_sql)

def import_data_into_tables(db, sheets):
    for sheet_name, dataframe in sheets.items():
        db.import_data(sheet_name, dataframe)

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
        db.upload_excel(args.file)
    elif args.file.endswith(".csv"):
        db.upload_csv(args.file)
    else:
        print("Unsupported file type. Only .xlsx and .csv files are supported.")
    
    db.close()  # Make sure to close the database connection
    
if __name__ == "__main__":
    main()