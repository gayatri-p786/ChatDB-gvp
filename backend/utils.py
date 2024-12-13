import csv
import re
import difflib
import pandas as pd



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

