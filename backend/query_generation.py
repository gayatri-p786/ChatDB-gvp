import json
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration

def load_model(model_path):
    try:
        # Initialize tokenizer from base T5 model first
        base_model_name = "t5-base"  # or "t5-small" if you used that as base
        tokenizer = T5Tokenizer.from_pretrained(base_model_name)
        
        # Load the fine-tuned model
        model = T5ForConditionalGeneration.from_pretrained(
            model_path,
            return_dict=True
        )
        
        return tokenizer, model
    except Exception as e:
        print(f"Error loading model: {str(e)}")
        raise

def generate_query(schema_info, tokenizer, model):
    try:
        # Add prefix for T5 task
        prefix = "generate sql: "
        full_input = prefix + schema_info
        
        # Tokenize with proper padding and truncation
        inputs = tokenizer(
            full_input,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=512
        )
        
        # Generate SQL query with better parameters
        output = model.generate(
            inputs['input_ids'],
            max_length=150,
            do_sample=True,          # Enable sampling for more variability
            top_k=50,                # Limit sampling to top K tokens
            top_p=0.95,              # Use nucleus sampling
            num_return_sequences=1,  # Number of sequences to return
            pad_token_id=tokenizer.pad_token_id
        )

        
        # Decode the generated query
        query = tokenizer.decode(output[0], skip_special_tokens=True)
        return query
    except Exception as e:
        print(f"Error generating query: {str(e)}")
        raise

def main():
    try:
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
        
    except Exception as e:
        print(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()