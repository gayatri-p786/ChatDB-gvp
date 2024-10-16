import json
import os

# Paths to your dataset
TABLES_PATH = "../datasets/tables.json"
TRAIN_PATH = "../datasets/train_spider.json"
OUTPUT_PATH = "../datasets/final_dataset.json"


def load_json(file_path):
    """Load json file."""
    with open(file_path, "r") as f:
        return json.load(f)


def extract_table_info(tables):
    """
    Extract table schema information from tables.json.
    We extract table names, columns grouped by table, primary keys, and foreign keys.
    """
    schema_data = []
    for table in tables:
        db_id = table["db_id"]
        table_names = table["table_names"]
        column_names = table["column_names"]
        column_types = table["column_types"]
        primary_keys = table["primary_keys"]
        foreign_keys = table["foreign_keys"]

        # Group columns by table index
        columns_grouped = {}
        for col_idx, (table_idx, col_name) in enumerate(column_names):
            if table_idx != -1:  # Skip if it's a special column
                if table_idx not in columns_grouped:
                    columns_grouped[table_idx] = []
                columns_grouped[table_idx].append({
                    "column_name": col_name,
                    "column_type": column_types[col_idx]
                })

        # Group foreign keys by the table they reference
        foreign_keys_grouped = {}
        for (col_idx, ref_col_idx) in foreign_keys:
            table_idx = column_names[col_idx][0]
            if table_idx not in foreign_keys_grouped:
                foreign_keys_grouped[table_idx] = []
            foreign_keys_grouped[table_idx].append((col_idx, ref_col_idx))

        schema_info = {
            "db_id": db_id,
            "table_names": table_names,
            "columns": columns_grouped,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys_grouped
        }

        schema_data.append(schema_info)

    return schema_data


def extract_query_info(train_data):
    """
    Extract db_id and query from train.json.
    We also add query tags like SELECT, INSERT, etc.
    """
    queries_data = []
    for item in train_data:
        db_id = item["db_id"]
        query = item["query"]
        query_tokens = item["query_toks"]

        # Extract query type (first keyword in the SQL statement, e.g., SELECT, INSERT)
        query_type = query_tokens[0].upper()

        queries_data.append({
            "db_id": db_id,
            "query": query,
            "query_type": query_type
        })

    return queries_data


def combine_data_grouped(schema_data, queries_data):
    """
    Combine schema information and query data into a grouped structure.
    Each entry will have the schema with queries grouped together.
    """
    combined_data = []
    for schema in schema_data:
        db_id = schema["db_id"]
        # Find all queries associated with this db_id
        associated_queries = [query for query in queries_data if query["db_id"] == db_id]

        # Group the queries for this db
        queries_grouped = [{"query": query["query"], "query_type": query["query_type"]} for query in associated_queries]

        entry = {
            "db_id": db_id,
            "table_names": schema["table_names"],
            "columns": schema["columns"],
            "primary_keys": schema["primary_keys"],
            "foreign_keys": schema["foreign_keys"],
            "queries": queries_grouped
        }
        combined_data.append(entry)

    return combined_data


def save_json(data, output_file):
    """Save the processed data into a JSON file."""
    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    # Load tables.json and train.json
    tables_data = load_json(TABLES_PATH)
    train_data = load_json(TRAIN_PATH)

    # Extract schema info and query info
    schema_info = extract_table_info(tables_data)
    query_info = extract_query_info(train_data)

    # Combine them into a grouped structure
    final_data_grouped = combine_data_grouped(schema_info, query_info)

    # Save the final dataset
    save_json(final_data_grouped, OUTPUT_PATH)

    print(f"Preprocessing complete. Final dataset saved to {OUTPUT_PATH}")
