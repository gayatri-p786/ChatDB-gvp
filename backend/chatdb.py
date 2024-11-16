import mysql.connector
import pandas as pd
import csv
import re
import json
from transformers import T5Tokenizer, T5ForConditionalGeneration

class ChatDB:
    @classmethod
    def list_databases(cls, host, user, password):
        try:
            # Connect to MySQL server without specifying a database
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password
            )
            cursor = conn.cursor()

            # Execute query to show all databases
            cursor.execute("SHOW DATABASES")

            # Fetch all results
            databases = [db[0] for db in cursor.fetchall()]

            # Close cursor and connection
            cursor.close()
            conn.close()

            return databases

        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return []
        

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

        # cursor.close()
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

    def get_table_info_and_sample_data(self, table_name, sample_size=5):
        try:
            cursor = self.cursor
            
            # Get table structure
            cursor.execute(f"DESCRIBE {table_name}")
            table_structure = cursor.fetchall()
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {sample_size}")
            sample_data = cursor.fetchall()
            
            # cursor.close()
            
            return {
                "table_name": table_name,
                "structure": table_structure,
                "sample_data": sample_data
            }
        except Exception as e:
            print(f"Error getting table info and sample data: {e}")
            return None

    def get_schema_info(self, database):
        
        cursor = self.cursor
        # print("here")

        # Get table names
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        schema = {
            "db_id": database,
            "table_names": [table[0] for table in tables],
            "columns": {},
            "primary_keys": [],
            "foreign_keys": {}
        }

        # Get columns for each table
        for idx, table in enumerate(schema["table_names"]):
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            schema["columns"][str(idx)] = [
                {"column_name": col[0], "column_type": col[1].decode('utf-8') if isinstance(col[1], bytes) else col[1]} for col in columns
            ]

        # Get primary keys
        for idx, table in enumerate(schema["table_names"]):
            cursor.execute(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'")
            primary_keys = cursor.fetchall()
            for pk in primary_keys:
                schema["primary_keys"].append(idx)

        # Get foreign keys
        for idx, table in enumerate(schema["table_names"]):
            cursor.execute(f"""
                SELECT column_name, referenced_table_name, referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = '{database}' AND table_name = '{table}' AND referenced_table_name IS NOT NULL
            """)
            foreign_keys = cursor.fetchall()
            schema["foreign_keys"][str(idx)] = [
                [idx, schema["table_names"].index(fk[1])] for fk in foreign_keys
            ]

        # cursor.close()
        return schema

    def close(self):
        self.cursor.close()
        self.conn.close()


def load_model(model_path):
    try:
        # print("model is here", model_path)
        # Initialize tokenizer from pretrained T5 model first
        tokenizer = T5Tokenizer.from_pretrained(model_path)
        
        # Load the fine-tuned model
        model = T5ForConditionalGeneration.from_pretrained(
            model_path,
            return_dict=True
        )
        
        return tokenizer, model
    except Exception as e:
        print(f"Error loading model: {str(e)}")
        raise

def generate_query(schema_info, tokenizer, model, construct=None):
    try:
        full_input = json.dumps(schema_info)
        
        if construct:
            full_input += f"\nTask: Generate a query using {construct}"
        else:
            full_input += "\nTask: Generate a random SQL query"
        
        inputs = tokenizer(
            full_input,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=512
        )
        
        output = model.generate(
            inputs['input_ids'],
            max_length=200,
            do_sample=True,
            top_k=50,
            top_p=0.95,
            num_return_sequences=5 if not construct else 1,
            pad_token_id=tokenizer.pad_token_id
        )

        # print("Raw model output:", output)

        decoded_outputs = [tokenizer.decode(output[i], skip_special_tokens=True) for i in range(len(output))]
        
        results = []
        for decoded_output in decoded_outputs:
            print("Decoded Output:", decoded_output)

            # Initialize variables for query and constructs
            query = ""
            constructs = []

            # Check if the output contains "Query:" and "Constructs:"
            try:
                # Extract the Query part
                if "Query:" in decoded_output:
                    query_part = decoded_output.split("Query:")[1].strip()
                    if "Constructs:" in query_part:
                        query = query_part.split("Constructs:")[0].strip()  # Exclude constructs
                    else:
                        query = query_part  # Take the remaining part if no Constructs section

                # Extract the Constructs part if it exists
                if "Constructs:" in decoded_output:
                    constructs_part = decoded_output.split("Constructs:")[1].strip()
                    constructs = [c.strip() for c in constructs_part.split(',')]  # Split constructs by comma

            except Exception as e:
                print(f"Error processing decoded output: {str(e)}")

            results.append({'query': query, 'constructs': constructs})
        return results
    
    except Exception as e:
        print(f"Error generating query: {str(e)}")
        raise


def generate_description(query):
    description = "This query"

    # Identify the main action
    if query.strip().upper().startswith("SELECT"):
        description += " retrieves"
    elif query.strip().upper().startswith("INSERT"):
        description += " inserts"
    elif query.strip().upper().startswith("UPDATE"):
        description += " updates"
    elif query.strip().upper().startswith("DELETE"):
        description += " deletes"

    # Identify what's being selected
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE)
    if select_match:
        select_clause = select_match.group(1)
        if '*' in select_clause:
            description += " all columns"
        elif re.search(r"\bCOUNT\s*\(", select_clause, re.IGNORECASE):
            description += " the count of"
        elif re.search(r"\bSUM\s*\(", select_clause, re.IGNORECASE):
            description += " the sum of"
        elif re.search(r"\bAVG\s*\(", select_clause, re.IGNORECASE):
            description += " the average of"
        elif re.search(r"\bMAX\s*\(", select_clause, re.IGNORECASE):
            description += " the maximum of"
        elif re.search(r"\bMIN\s*\(", select_clause, re.IGNORECASE):
            description += " the minimum of"
        else:
            description += f" {select_clause}"

    # Identify the table(s)
    from_match = re.search(r"FROM\s+(.*?)(?:\s+WHERE|\s+GROUP BY|\s+ORDER BY|\s*$)", query, re.IGNORECASE)
    if from_match:
        tables = from_match.group(1).strip()
        description += f" from the {tables} table(s)"

    # Describe JOIN if present
    join_match = re.search(r"JOIN\s+\w+", query, re.IGNORECASE)
    if join_match:
        description += " and joins it with another table"

    # Describe WHERE clause if present
    where_match = re.search(r"WHERE\s+", query, re.IGNORECASE)
    if where_match:
        description += " with specific conditions"

    # Describe GROUP BY if present
    group_by_match = re.search(r"GROUP BY\s+(.*?)(?:\s+HAVING|\s+ORDER BY|\s*$)", query, re.IGNORECASE)
    if group_by_match:
        group_by_columns = group_by_match.group(1).strip()
        description += f" grouped by {group_by_columns}"

    # Describe HAVING clause if present
    having_match = re.search(r"HAVING\s+", query, re.IGNORECASE)
    if having_match:
        description += " and filters the groups"

    # Describe ORDER BY if present
    order_by_match = re.search(r"ORDER BY\s+(.*?)(?:\s+LIMIT|\s*$)", query, re.IGNORECASE)
    if order_by_match:
        order_by_columns = order_by_match.group(1).strip()
        description += f" and sorts the results by {order_by_columns}"

    # Describe LIMIT if present
    limit_match = re.search(r"LIMIT\s+\d+", query, re.IGNORECASE)
    if limit_match:
        limit_value = limit_match.group(0).split()[1]
        description += f" with a limit of {limit_value} result(s)"

    return description.strip() + "."

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

