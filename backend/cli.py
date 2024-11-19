# cli.py

import os
import sys
import re
from chatdb import ChatDB, parse_excel, parse_csv, generate_description  # Import necessary functions


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

# def generate_query_with_construct(db, db_name, construct):
#     try:
#         schema_info = db.get_schema_info(db_name)
#         tokenizer, model = load_model('sql_generation_model_2')
#         generated_results = generate_query(schema_info, tokenizer, model, construct)

#         if generated_results:
#             result = generated_results[0]
#             query = result['query']
#             constructs = result['constructs']
            
#             # Generate description
#             description = generate_description(query)
            
#             print(f"Generated Query with {construct}:")
#             print(f"Description: {description}")
#             print(f"Query: {query}")
#             print(f"Constructs: {', '.join(constructs)}")
#         else:
#             print(f"No query generated for construct: {construct}")
#     except Exception as e:
#         print(f"An error occurred while generating query with construct: {e}")

# def generate_sample_queries(db, db_name):
#     try:
#         # Retrieve the schema information for the connected database
#         schema_info = db.get_schema_info(db_name)

#         # Load the model and tokenizer for generating SQL queries
#         tokenizer, model = load_model('./sql_generation_model_2')

#         # Generate the SQL queries
#         generated_results = generate_query(schema_info, tokenizer, model)

#         # Handle the generated queries, constructs, and descriptions
#         for i, result in enumerate(generated_results):
#             print(f"Generated Query {i + 1}:")
            
#             # Generate description
#             description = generate_description(result['query'])
            
#             print(f"Description: {description}")
#             print(f"Query: {result['query']}")
#             print(f"Constructs: {', '.join(result['constructs'])}")
#             print("---")
#     except Exception as e:
#         print(f"An error occurred while generating sample queries: {e}")

def display_table_info(db, table_name):
    table_info = db.get_table_info_and_sample_data(table_name)
    if table_info:
        print(f"\nTable: {table_info['table_name']}")
        print("Structure:")
        for column in table_info['structure']:
            print(f"  {column['Field']} ({column['Type']})")
        print("\nSample Data:")
        if table_info['sample_data']:
            headers = table_info['sample_data'][0].keys()
            row_format = "  ".join(["{:<15}" for _ in headers])
            print(row_format.format(*headers))
            print("  " + "-" * (15 * len(headers)))
            for row in table_info['sample_data']:
                print(row_format.format(*[str(val)[:15] for val in row.values()]))
        else:
            print("  (No data available)")
        print("\n")
    else:
        print(f"Unable to retrieve information for table {table_name}")


def main():
    host = "localhost"
    user = "root"
    password = "root"
    db = None

    print("Welcome to ChatDB! I'm your AI assistant for database operations.")
    print("You can ask me to create a new database, use an existing one, upload data, generate queries, and more.")

    current_database = None

    while True:
        user_input = input("\nYou: ").strip().lower()

        if "exit" in user_input or "quit" in user_input or "bye" in user_input:
            print("Thank you for using ChatDB. Goodbye!")
            break

        if "create" in user_input or "new database" in user_input or "upload data" in user_input:
            database_name = input("What would you like to name your new database? ")
            db = ChatDB(host, user, password, database_name)
            current_database = database_name
            print(f"Great! I've created a new database called '{database_name}'. Now, let's upload some data.")
            upload_data(db)

        elif "use" in user_input or "switch" in user_input or "change database" in user_input:
            # databases = db.list_databases()
            print("Here are the existing databases:")
            databases = ChatDB.list_databases(host, user, password)
            for i, db_name in enumerate(databases, 1):
                print(f"{i}. {db_name}")
            database_choice = input("Which database would you like to use? (Enter the name or number) ")
            if database_choice.isdigit() and 1 <= int(database_choice) <= len(databases):
                current_database = databases[int(database_choice) - 1]
            elif database_choice in databases:
                current_database = database_choice
            else:
                print("I'm sorry, that's not a valid choice. Please try again.")
                continue
            print(f"Now using database: {current_database}")
            db = ChatDB(host, user, password, current_database)

        elif re.search(r'\b(show|display|view)\s+(tables?|schema)\b', user_input):
            if not current_database:
                print("Please select a database first.")
            else:
                schema = db.get_schema_info(current_database)
                print(f"\nDatabase: {schema['db_id']}")
                print("Tables:")
                for idx, table in enumerate(schema['table_names']):
                    print(f"  {idx + 1}. {table}")
                
                table_choice = input("\nEnter the number of the table you want to view (or 'all' for all tables): ")
                if table_choice.lower() == 'all':
                    for table in schema['table_names']:
                        display_table_info(db, table)
                elif table_choice.isdigit() and 1 <= int(table_choice) <= len(schema['table_names']):
                    selected_table = schema['table_names'][int(table_choice) - 1]
                    display_table_info(db, selected_table)
                else:
                    print("Invalid choice. Please try again.")

        elif "generate" in user_input and "sample" in user_input and "queries" in user_input:
            if not current_database:
                print("Please select a database first.")
            else:
                sample_queries = db.generate_sample_queries()
                print("Generated Sample Queries:")
                for i, query in enumerate(sample_queries, 1):
                    description = generate_description(query)
                    print(f"Query {i}:")
                    print(f"Description: {description}")
                    print(f"SQL: {query}")
                    print()
                
                while True:
                    execute_option = input("\nEnter the number of the query you'd like to execute (1-5), 'all' to execute all, or 'exit' to return to the main menu: ").strip().lower()
                    
                    if execute_option == 'exit':
                        break
                    elif execute_option == 'all':
                        for i, query in enumerate(sample_queries, 1):
                            print(f"\nExecuting Query {i}:")
                            print(f"SQL: {query}")
                            result = db.execute_custom_query(query)
                            if "error" in result:
                                print(f"Error executing query: {result['error']}")
                            else:
                                print("Query results:")
                                for row in result['data']:
                                    print(row)
                    elif execute_option.isdigit() and 1 <= int(execute_option) <= 5:
                        query_index = int(execute_option) - 1
                        query_to_execute = sample_queries[query_index]
                        print(f"\nExecuting Query {execute_option}:")
                        print(f"SQL: {query_to_execute}")
                        result = db.execute_custom_query(query_to_execute)
                        if "error" in result:
                            print(f"Error executing query: {result['error']}")
                        else:
                            print("Query results:")
                            for row in result['data']:
                                print(row)
                    else:
                        print("Invalid input. Please enter a number between 1 and 5, 'all', or 'exit'.")

        elif "generate" in user_input and "query" in user_input and "using" in user_input:
            if not current_database:
                print("Please select a database first.")
            else:
                construct = user_input.split("using")[-1].strip()
                sample_queries = db.generate_sample_queries(construct=construct)
                print(f"Generated Query using {construct}:")
                for i, query in enumerate(sample_queries, 1):
                    description = generate_description(query)
                    print(f"Query {i}:")
                    print(f"Description: {description}")
                    print(f"SQL: {query}")
                    print()
                
                execute_option = input("Would you like to execute this query? (yes/no): ").strip().lower()
                if execute_option == 'yes':
                    query_to_execute = sample_queries[0]  # There's only one query when using a specific construct
                    print(f"Executing query: {query_to_execute}")
                    result = db.execute_custom_query(query_to_execute)
                    if "error" in result:
                        print(f"Error executing query: {result['error']}")
                    else:
                        print("Query results:")
                        for row in result['data']:
                            print(row)

        else:
            print("I'm not sure how to help with that. You can ask me to:")
            print("- Create a new database and upload data")
            print("- Use a different database")
            print("- Generate sample queries")
            print("- Generate a query using a specific SQL construct")
            print("- Exit the program")

    # db.close()

if __name__ == "__main__":
    main()