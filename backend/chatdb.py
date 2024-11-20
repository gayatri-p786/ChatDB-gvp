import mysql.connector
import pandas as pd
import csv
import re
import difflib
import random

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
        
    def get_all_tables(self):
        try:
            self.cursor.execute("SHOW TABLES")
            return [table[0] for table in self.cursor.fetchall()]
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return []

    def get_table_columns(self, table_name):
        try:
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = [column[0] for column in self.cursor.fetchall()]
            return columns
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return []

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


    def get_table_info(self):
        try:
            self.cursor.execute("SHOW TABLES")
            tables = [table[0] for table in self.cursor.fetchall()]
            
            table_info = {}
            for table in tables:
                self.cursor.execute(f"DESCRIBE {table}")
                columns = self.cursor.fetchall()
                
                numeric_cols = []
                categorical_cols = []
                
                for col in columns:
                    col_name = col[0]
                    col_type = col[1]
                    
                    if 'int' in col_type.lower() or 'float' in col_type.lower() or 'double' in col_type.lower() or 'decimal' in col_type.lower():
                        numeric_cols.append(col_name)
                    else:
                        categorical_cols.append(col_name)
                
                table_info[table] = {
                    'columns': [{'Field': col[0], 'Type': col[1]} for col in columns],
                    'numeric_columns': numeric_cols,
                    'categorical_columns': categorical_cols
                }
            
            return table_info
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return {}
        
    def generate_query_templates(self):
        table_info = self.get_table_info()
        templates = []

        for table, info in table_info.items():
            numeric_cols = info['numeric_columns']
            categorical_cols = info['categorical_columns']
            all_cols = numeric_cols + categorical_cols

            # Infer date columns based on column names
            date_cols = [col for col in all_cols if 'date' in col.lower() or 'year' in col.lower()]

            # Basic SELECT
            templates.append(f"SELECT * FROM {table} LIMIT 10")

            if numeric_cols and categorical_cols:
                # Aggregation with GROUP BY
                templates.append(f"SELECT {categorical_cols[0]}, SUM({numeric_cols[0]}) FROM {table} GROUP BY {categorical_cols[0]} LIMIT 5")
                templates.append(f"SELECT {categorical_cols[0]}, AVG({numeric_cols[0]}) FROM {table} GROUP BY {categorical_cols[0]} ORDER BY AVG({numeric_cols[0]}) DESC LIMIT 5")
                
                # WHERE clause with both numeric and categorical
                templates.append(f"SELECT * FROM {table} WHERE {categorical_cols[0]} = {{categorical}} AND {numeric_cols[0]} > {{numeric}} LIMIT 5")

            if len(numeric_cols) >= 2:
                # Multiple numeric columns
                templates.append(f"SELECT * FROM {table} WHERE {numeric_cols[0]} BETWEEN {{numeric_low}} AND {{numeric_high}} LIMIT 5")

            if len(categorical_cols) >= 2:
                # Multiple categorical columns
                templates.append(f"SELECT {categorical_cols[0]}, {categorical_cols[1]}, COUNT(*) FROM {table} GROUP BY {categorical_cols[0]}, {categorical_cols[1]} ORDER BY COUNT(*) DESC LIMIT 5")

            if numeric_cols:
                # Numeric operations
                templates.append(f"SELECT MAX({numeric_cols[0]}) FROM {table}")
                templates.append(f"SELECT MIN({numeric_cols[0]}) FROM {table}")
                templates.append(f"SELECT AVG({numeric_cols[0]}) FROM {table}")

            if categorical_cols:
                # DISTINCT on categorical
                templates.append(f"SELECT DISTINCT {categorical_cols[0]} FROM {table} LIMIT 5")
                templates.append(f"SELECT {categorical_cols[0]}, COUNT(*) FROM {table} GROUP BY {categorical_cols[0]} HAVING COUNT(*) > {{numeric}} LIMIT 5")

            # LIKE query
            if categorical_cols:
                templates.append(f"SELECT * FROM {table} WHERE {categorical_cols[0]} LIKE '{{like_pattern}}' LIMIT 5")

            # ORDER BY
            if all_cols:
                templates.append(f"SELECT * FROM {table} ORDER BY {all_cols[0]} {{order}} LIMIT 10")

            # Date specific queries
            if date_cols:
                templates.append(f"SELECT * FROM {table} WHERE {date_cols[0]} = '{{date}}' LIMIT 5")
                if len(date_cols) >= 2:
                    templates.append(f"SELECT * FROM {table} WHERE {date_cols[0]} BETWEEN '{{date_start}}' AND '{{date_end}}' LIMIT 5")

        return templates

    def generate_sample_queries(self, num_queries=5, construct=None):
        templates = self.generate_query_templates()
        table_info = self.get_table_info()

        if construct:
            templates = [t for t in templates if construct.lower() in t.lower()]
            num_queries = 1

        sample_queries = set()
        attempts = 0
        max_attempts = 50

        while len(sample_queries) < num_queries and attempts < max_attempts:
            if not templates:
                break

            template = random.choice(templates)
            
            for table, info in table_info.items():
                if table in template:
                    query = template
                    placeholders = re.findall(r'\{(\w+)\}', query)
                    
                    for placeholder in placeholders:
                        if placeholder.startswith('numeric'):
                            if info['numeric_columns']:
                                col = random.choice(info['numeric_columns'])
                                self.cursor.execute(f"SELECT {col} FROM {table} ORDER BY RAND() LIMIT 1")
                                result = self.cursor.fetchone()
                                value = result[0] if result else 0
                                query = query.replace(f"{{{placeholder}}}", str(value))
                            else:
                                # If no numeric columns, replace with a default value
                                query = query.replace(f"{{{placeholder}}}", "0")
                        elif placeholder.startswith('categorical'):
                            if info['categorical_columns']:
                                col = random.choice(info['categorical_columns'])
                                self.cursor.execute(f"SELECT DISTINCT {col} FROM {table} ORDER BY RAND() LIMIT 1")
                                result = self.cursor.fetchone()
                                value = result[0] if result else ''
                                query = query.replace(f"{{{placeholder}}}", f"'{value}'")
                            else:
                                # If no categorical columns, replace with a default value
                                query = query.replace(f"{{{placeholder}}}", "''")
                        elif placeholder in ['date', 'date_start', 'date_end']:
                            date_cols = [col for col in info['categorical_columns'] if 'date' in col.lower() or 'year' in col.lower()]
                            if date_cols:
                                col = random.choice(date_cols)
                                self.cursor.execute(f"SELECT {col} FROM {table} ORDER BY RAND() LIMIT 1")
                                result = self.cursor.fetchone()
                                value = result[0] if result else '2000-01-01'
                                query = query.replace(f"{{{placeholder}}}", f"'{value}'")
                            else:
                                # If no date columns, replace with a default value
                                query = query.replace(f"{{{placeholder}}}", "'2000-01-01'")
                        elif placeholder == 'like_pattern':
                            if info['categorical_columns']:
                                col = random.choice(info['categorical_columns'])
                                self.cursor.execute(f"SELECT {col} FROM {table} ORDER BY RAND() LIMIT 1")
                                result = self.cursor.fetchone()
                                value = result[0] if result else ''
                                pattern = f"%{value[:3]}%" if value else '%'
                                query = query.replace(f"{{{placeholder}}}", f"'{pattern}'")
                            else:
                                # If no categorical columns, replace with a default pattern
                                query = query.replace(f"{{{placeholder}}}", "'%'")
                        elif placeholder == 'order':
                            order = random.choice(['ASC', 'DESC'])
                            query = query.replace(f"{{{placeholder}}}", order)

                    break

            if query not in sample_queries:
                sample_queries.add(query)

            attempts += 1

        return list(sample_queries)

    def close(self):
        self.cursor.close()
        self.conn.close()


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
            description += " the count of rows"
        elif re.search(r"\bSUM\s*\((\w+)\)", select_clause, re.IGNORECASE):
            sum_col = re.search(r"\bSUM\s*\((\w+)\)", select_clause, re.IGNORECASE).group(1)
            description += f" the sum of {sum_col}"
        elif re.search(r"\bAVG\s*\((\w+)\)", select_clause, re.IGNORECASE):
            avg_col = re.search(r"\bAVG\s*\((\w+)\)", select_clause, re.IGNORECASE).group(1)
            description += f" the average of {avg_col}"
        elif re.search(r"\bMAX\s*\(", select_clause, re.IGNORECASE):
            description += " the maximum value"
        elif re.search(r"\bMIN\s*\(", select_clause, re.IGNORECASE):
            description += " the minimum value"
        else:
            description += f" {select_clause}"

    # Identify the table(s)
    from_match = re.search(r"FROM\s+(.*?)(?:\s+WHERE|\s+GROUP BY|\s+ORDER BY|\s*$)", query, re.IGNORECASE)
    if from_match:
        tables = from_match.group(1).strip()
        description += f" from the {tables} table"

    # Describe WHERE clause if present
    where_match = re.search(r"WHERE\s+(.*?)(?:\s+GROUP BY|\s+ORDER BY|\s*$)", query, re.IGNORECASE)
    if where_match:
        where_clause = where_match.group(1).strip()
        description += f" where {where_clause}"

    # Describe GROUP BY if present
    group_by_match = re.search(r"GROUP BY\s+(.*?)(?:\s+HAVING|\s+ORDER BY|\s*$)", query, re.IGNORECASE)
    if group_by_match:
        group_by_columns = group_by_match.group(1).strip()
        description += f" grouped by {group_by_columns}"

    # Describe HAVING clause if present
    having_match = re.search(r"HAVING\s+(.*?)(?:\s+ORDER BY|\s*$)", query, re.IGNORECASE)
    if having_match:
        having_clause = having_match.group(1).strip()
        description += f" having {having_clause}"

    # Describe ORDER BY if present
    order_by_match = re.search(r"ORDER BY\s+(.*?)(?:\s+LIMIT|\s*$)", query, re.IGNORECASE)
    if order_by_match:
        order_by_columns = order_by_match.group(1).strip()
        description += f" ordered by {order_by_columns}"

    # Describe LIMIT if present
    limit_match = re.search(r"LIMIT\s+(\d+)", query, re.IGNORECASE)
    if limit_match:
        limit_value = limit_match.group(1)
        description += f" limited to {limit_value} result(s)"

    return description.strip() + "."

def natural_language_to_sql(db, question):
    question = question.lower()
    
    all_tables = db.get_all_tables()
    table_columns = {table: db.get_table_columns(table) for table in all_tables}
    
    patterns = {
        "total {A} by {B}": "SELECT {B}, SUM({A}) FROM {table} GROUP BY {B}",
        "average {A} by {B}": "SELECT {B}, AVG({A}) FROM {table} GROUP BY {B}",
        "count of {A} by {B}": "SELECT {B}, COUNT({A}) FROM {table} GROUP BY {B}",
        "list all {A}": "SELECT DISTINCT {A} FROM {table}",
        "top {N} {A} by {B}": "SELECT {A}, {B} FROM {table} ORDER BY {B} DESC LIMIT {N}",
        "find {A} where {B} is {C}": "SELECT {A} FROM {table} WHERE {B} = '{C}'",
        "maximum {A}": "SELECT MAX({A}) FROM {table}",
        "minimum {A}": "SELECT MIN({A}) FROM {table}",
        "{A} greater than {B}": "SELECT * FROM {table} WHERE {A} > {B}",
        "{A} less than {B}": "SELECT * FROM {table} WHERE {A} < {B}",
        "{A} between {B} and {C}": "SELECT * FROM {table} WHERE {A} BETWEEN {B} AND {C}",
        "{A} like {B}": "SELECT * FROM {table} WHERE {A} LIKE '%{B}%'",
        "count distinct {A}": "SELECT COUNT(DISTINCT {A}) FROM {table}",
        "group {A} by {B}": "SELECT {B}, COUNT(*) FROM {table} GROUP BY {B}",
        "sum of {A}": "SELECT SUM({A}) FROM {table}"
    }
    
    def find_best_match(word, columns):
        return max(columns, key=lambda x: difflib.SequenceMatcher(None, word, x).ratio())
    
    def select_table(columns):
        table_scores = {table: sum(1 for col in columns if col in table_cols) 
                        for table, table_cols in table_columns.items()}
        return max(table_scores, key=table_scores.get)
    
    for pattern, sql_template in patterns.items():
        if all(keyword in question for keyword in pattern.split() if keyword not in ['{A}', '{B}', '{C}', '{N}']):
            words = question.split()
            A = next((find_best_match(word, sum(table_columns.values(), [])) for word in words 
                      if any(difflib.SequenceMatcher(None, word, col).ratio() > 0.6 for col in sum(table_columns.values(), []))), None)
            B = next((find_best_match(word, sum(table_columns.values(), [])) for word in reversed(words) 
                      if word != A and any(difflib.SequenceMatcher(None, word, col).ratio() > 0.6 for col in sum(table_columns.values(), []))), None)
            C = next((word for word in words if word not in pattern.split() and word != A and word != B), None)
            N = next((word for word in words if word.isdigit()), None)
            
            if A:
                table = select_table([A, B] if B else [A])
                if table:
                    sql = sql_template.format(A=A, B=B, C=C, N=N, table=table)
                    return sql
    
    return "Sorry, I couldn't generate a SQL query for that question."

# def natural_language_to_sql(db, question):
#     # print(f"Processing question: {question}")
    
#     question = question.lower()
    
#     # Get all tables and their columns
#     all_tables = db.get_all_tables()
#     # print(f"All tables: {all_tables}")
#     table_columns = {table: db.get_table_columns(table) for table in all_tables}
#     # print(f"Table columns: {table_columns}")
    
#     # Define some basic patterns
#     patterns = {
#         "total {A} by {B}": "SELECT {B}, SUM({A}) FROM {table} GROUP BY {B}",
#         "average {A} by {B}": "SELECT {B}, AVG({A}) FROM {table} GROUP BY {B}",
#         "count of {A} by {B}": "SELECT {B}, COUNT({A}) FROM {table} GROUP BY {B}",
#         "list all {A}": "SELECT DISTINCT {A} FROM {table}",
#         "top {N} {A} by {B}": "SELECT {A}, {B} FROM {table} ORDER BY {B} DESC LIMIT {N}"
#     }
    
#     for pattern, sql_template in patterns.items():
#         print(f"Checking pattern: {pattern}")
#         if all(keyword in question for keyword in pattern.split() if keyword not in ['{A}', '{B}', '{N}']):
#             print(f"Pattern matched: {pattern}")
#             words = question.split()
#             A = next((word for word in words if any(word in columns for columns in table_columns.values())), None)
#             B = next((word for word in reversed(words) if word != A and any(word in columns for columns in table_columns.values())), None)
#             N = next((word for word in words if word.isdigit()), None)
            
#             # print(f"Extracted A: {A}, B: {B}, N: {N}")
            
#             if A and B:
#                 # Find the table that contains both A and B
#                 table = next((table for table, columns in table_columns.items() if A in columns and B in columns), None)
#                 # print(f"Found table: {table}")
#                 if table:
#                     sql = sql_template.format(A=A, B=B, N=N, table=table)
#                     # print(f"Generated SQL: {sql}")
#                     return sql
#                 else:
#                     print("No table found containing both A and B")
#             else:
#                 print("Could not extract both A and B from the question")
    
#     print("No matching pattern or valid extraction found")
#     return "Sorry, I couldn't generate a SQL query for that question."

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

