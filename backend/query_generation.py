# generate_queries.py

import json
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration

def load_model(model_path):
    tokenizer = T5Tokenizer.from_pretrained(model_path)
    model = T5ForConditionalGeneration.from_pretrained(model_path)
    return tokenizer, model

def generate_query(schema_info, tokenizer, model):
    # Tokenize the schema info
    inputs = tokenizer(schema_info, return_tensors="pt", padding=True)
    # Generate SQL query
    output = model.generate(inputs['input_ids'], max_length=50)
    # Decode the generated query
    query = tokenizer.decode(output[0], skip_special_tokens=True)
    return query

def main():
    # Load the trained model and tokenizer
    tokenizer, model = load_model('./sql_query_generator')

    # Define a new database schema
    new_schema = {
        "db_id": "new_database",
        "table_names": ["employees", "departments"],
        "columns": {
            "0": [
                {"column_name": "employee_id", "column_type": "number"},
                {"column_name": "name", "column_type": "text"}
            ],
            "1": [
                {"column_name": "department_id", "column_type": "number"},
                {"column_name": "department_name", "column_type": "text"}
            ]
        },
        "primary_keys": [0, 1],
        "foreign_keys": {
            "0": [[0, 1]]
        }
    }

    schema_input = json.dumps(new_schema)
    generated_query = generate_query(schema_input, tokenizer, model)
    print("Generated Query:", generated_query)

if __name__ == "__main__":
    main()
