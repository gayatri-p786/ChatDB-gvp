import json
import re

def extract_info(dataset):
    structured_data = []
    for record in dataset:
        db_id = record['db_id']
        table_names = record['table_names']
        columns = record['columns']
        primary_keys = record['primary_keys']
        foreign_keys = record['foreign_keys']
        queries = record['queries']
        query_constructs = []

        for query_record in queries:
            sql_query = query_record['query']
            constructs = []
            
            # Clauses
            if re.search(r'\bWHERE\b', sql_query, re.IGNORECASE):
                constructs.append('WHERE')
            if re.search(r'\bJOIN\b', sql_query, re.IGNORECASE):
                constructs.append('JOIN')
            if re.search(r'\bGROUP BY\b', sql_query, re.IGNORECASE):
                constructs.append('GROUP BY')
            if re.search(r'\bHAVING\b', sql_query, re.IGNORECASE):
                constructs.append('HAVING')
            if re.search(r'\bORDER BY\b', sql_query, re.IGNORECASE):
                constructs.append('ORDER BY')
            if re.search(r'\bLIMIT\b', sql_query, re.IGNORECASE):
                constructs.append('LIMIT')
            
            # Set operations
            if re.search(r'\bINTERSECT\b', sql_query, re.IGNORECASE):
                constructs.append('INTERSECT')
            if re.search(r'\bUNION\b', sql_query, re.IGNORECASE):
                constructs.append('UNION')
            if re.search(r'\bEXCEPT\b', sql_query, re.IGNORECASE):
                constructs.append('EXCEPT')
            
            # Aggregate functions
            if re.search(r'\bAVG\s*\(', sql_query, re.IGNORECASE):
                constructs.append('AVG')
            if re.search(r'\bMIN\s*\(', sql_query, re.IGNORECASE):
                constructs.append('MIN')
            if re.search(r'\bMAX\s*\(', sql_query, re.IGNORECASE):
                constructs.append('MAX')
            if re.search(r'\bSUM\s*\(', sql_query, re.IGNORECASE):
                constructs.append('SUM')
            if re.search(r'\bCOUNT\s*\(', sql_query, re.IGNORECASE):
                constructs.append('COUNT')
            
            # Other common functions and keywords
            if re.search(r'\bDISTINCT\b', sql_query, re.IGNORECASE):
                constructs.append('DISTINCT')
            if re.search(r'\bLIKE\b', sql_query, re.IGNORECASE):
                constructs.append('LIKE')
            if re.search(r'\bIN\b', sql_query, re.IGNORECASE):
                constructs.append('IN')
            if re.search(r'\bBETWEEN\b', sql_query, re.IGNORECASE):
                constructs.append('BETWEEN')
            if re.search(r'\bEXISTS\b', sql_query, re.IGNORECASE):
                constructs.append('EXISTS')
            if re.search(r'\bCASE\b', sql_query, re.IGNORECASE):
                constructs.append('CASE')
            if re.search(r'\bSUBSTRING\s*\(', sql_query, re.IGNORECASE):
                constructs.append('SUBSTRING')
            if re.search(r'\bCONCAT\s*\(', sql_query, re.IGNORECASE):
                constructs.append('CONCAT')
            
            query_constructs.append({
                'query': sql_query,
                'query_type': query_record['query_type'],
                'constructs': constructs
            })

        structured_data.append({
            'db_id': db_id,
            'table_names': table_names,
            'columns': columns,
            'primary_keys': primary_keys,
            'foreign_keys': foreign_keys,
            'queries': query_constructs
        })
    return structured_data

# Load the dataset
with open('../datasets/final_dataset.json', 'r') as file:
    dataset = json.load(file)

# Process the dataset
structured_dataset = extract_info(dataset)

# Save the structured data
with open('../datasets/structured_final_dataset.json', 'w') as json_file:
    json.dump(structured_dataset, json_file, indent=2)

print('structured_final_dataset.json created with extended query constructs included.')