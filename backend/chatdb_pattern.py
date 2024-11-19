import mysql.connector
from mysql.connector import Error
import random

class ChatDB:
    @classmethod
    def list_databases(cls, host, user, password):
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password
            )
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall()]
            cursor.close()
            conn.close()
            return databases
        except Error as err:
            print(f"Error: {err}")
            return []

    def __init__(self, host, user, password, database):
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
            )
            cursor = conn.cursor()

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

            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.conn.cursor(dictionary=True)
        except Error as err:
            print(f"Error: {err}")

    def get_table_info(self):
        try:
            self.cursor.execute("SHOW TABLES")
            tables = [table['Tables_in_' + self.conn.database] for table in self.cursor.fetchall()]
            
            table_info = {}
            for table in tables:
                self.cursor.execute(f"DESCRIBE {table}")
                columns = self.cursor.fetchall()
                
                numeric_cols = []
                categorical_cols = []
                
                for col in columns:
                    col_name = col['Field']
                    col_type = col['Type']
                    
                    if 'int' in col_type or 'float' in col_type or 'double' in col_type or 'decimal' in col_type:
                        numeric_cols.append(col_name)
                    else:
                        categorical_cols.append(col_name)
                
                table_info[table] = {
                    'columns': columns,
                    'numeric_columns': numeric_cols,
                    'categorical_columns': categorical_cols
                }
            
            return table_info
        except Error as err:
            print(f"Error: {err}")
            return {}

    def generate_query_templates(self):
        table_info = self.get_table_info()
        templates = []

        for table, info in table_info.items():
            numeric_cols = info['numeric_columns']
            categorical_cols = info['categorical_columns']

            if numeric_cols and categorical_cols:
                templates.append(f"SELECT SUM({numeric_cols[0]}) FROM {table} GROUP BY {categorical_cols[0]}")
                templates.append(f"SELECT {categorical_cols[0]}, AVG({numeric_cols[0]}) FROM {table} GROUP BY {categorical_cols[0]} ORDER BY AVG({numeric_cols[0]}) DESC LIMIT 5")
            
            if categorical_cols:
                templates.append(f"SELECT * FROM {table} WHERE {categorical_cols[0]} = '{{category_value}}'")
            
            if numeric_cols:
                templates.append(f"SELECT * FROM {table} WHERE {numeric_cols[0]} > {{threshold}}")
            
            if categorical_cols:
                templates.append(f"SELECT {categorical_cols[0]}, COUNT(*) FROM {table} GROUP BY {categorical_cols[0]} HAVING COUNT(*) > {{min_count}}")

        return templates

    def generate_sample_queries(self, num_queries=5, construct=None):
    
        templates = self.generate_query_templates()
        table_info = self.get_table_info()

        # Filter templates based on the given construct
        if construct:
            templates = [t for t in templates if construct.lower() in t.lower()]

        sample_queries = []
        for _ in range(num_queries):
            if not templates:
                break  # Exit if no templates are available

            template = random.choice(templates)
            
            for table, info in table_info.items():
                if table in template:
                    if '{{category_value}}' in template:
                        category_col = random.choice(info['categorical_columns']) if info['categorical_columns'] else None
                        if category_col:
                            self.cursor.execute(f"SELECT DISTINCT {category_col} FROM {table} LIMIT 1")
                            category_value = self.cursor.fetchone()[category_col]
                            template = template.replace('{{category_value}}', str(category_value))
                    
                    if '{{threshold}}' in template:
                        numeric_col = random.choice(info['numeric_columns']) if info['numeric_columns'] else None
                        if numeric_col:
                            self.cursor.execute(f"SELECT AVG({numeric_col}) as avg_value FROM {table}")
                            avg_value = self.cursor.fetchone()['avg_value']
                            template = template.replace('{{threshold}}', str(avg_value))
                    
                    if '{{min_count}}' in template:
                        template = template.replace('{{min_count}}', str(random.randint(1, 10)))
                    
                    break
            
            sample_queries.append(template)

        return sample_queries

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()