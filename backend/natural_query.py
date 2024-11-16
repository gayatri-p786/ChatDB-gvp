import re
import spacy

# Load SpaCy model for tokenization and lemmatization
nlp = spacy.load("en_core_web_sm")

# Preprocessing function
def preprocess_query(query):
    # Convert to lowercase and remove unnecessary special characters
    query = query.lower()
    query = re.sub(r'[^a-zA-Z0-9\s]', '', query)
    
    # Skip tokenization and lemmatization
    return query.strip()



# Define SQL Query Patterns (Natural language patterns)
PATTERNS = {
    "total <A> by <B>": r"(?:total|sum|aggregate)\s+([a-zA-Z0-9_]+)\s+(?:by|for|per)\s+([a-zA-Z0-9_]+)",
    "average <A> by <B>": r"(?:average|mean)\s+([a-zA-Z0-9_]+)\s+(?:by|for|per)\s+([a-zA-Z0-9_]+)",
    "count <A> by <B>": r"(?:count|number|quantity)\s+([a-zA-Z0-9_]+)\s+(?:by|for|per)\s+([a-zA-Z0-9_]+)",
    "max <A> by <B>": r"(?:max|maximum|highest)\s+([a-zA-Z0-9_]+)\s+(?:by|for|per)\s+([a-zA-Z0-9_]+)",
    "min <A> by <B>": r"(?:min|minimum|lowest)\s+([a-zA-Z0-9_]+)\s+(?:by|for|per)\s+([a-zA-Z0-9_]+)",
}

# Function to match the query with predefined patterns
def match_pattern(query):
    query = preprocess_query(query)
    
    # Try matching each pattern
    for pattern, regex in PATTERNS.items():
        match = re.match(regex, query)
        if match:
            return pattern, match.groups()
    
    return None, None

# Function to generate SQL from matched pattern
def map_query_to_sql(query):
    pattern, entities = match_pattern(query)
    if not pattern:
        return "No matching pattern found"
    
    if pattern == "select <A> from <B>":
        columns, table = entities
        return f"SELECT {columns} FROM {table}"

    elif pattern == "total <A> by <B>":
        field, category = entities
        return f"SELECT {category}, SUM({field}) FROM {category} GROUP BY {category}"

    elif pattern == "find <A> where <B> is <C>":
        field, condition, value = entities
        return f"SELECT {field} FROM sales WHERE {condition} = '{value}'"

    elif pattern == "find <A> where <B> > <C>":
        field, condition, value = entities
        return f"SELECT {field} FROM sales WHERE {condition} > {value}"

    elif pattern == "find <A> where <B> < <C>":
        field, condition, value = entities
        return f"SELECT {field} FROM sales WHERE {condition} < {value}"

    elif pattern == "find <A> sorted by <B>":
        field, category = entities
        return f"SELECT {field} FROM sales ORDER BY {category}"

    elif pattern == "find <A> sorted by <B> <order>":
        field, category, order = entities
        return f"SELECT {field} FROM sales ORDER BY {category} {order.upper()}"

    elif pattern == "update <B> set <A> = <C> where <D> = <E>":
        table, column, value, condition, cond_value = entities
        return f"UPDATE {table} SET {column} = '{value}' WHERE {condition} = '{cond_value}'"

    elif pattern == "delete from <B> where <C> = <D>":
        table, condition, value = entities
        return f"DELETE FROM {table} WHERE {condition} = '{value}'"

    elif pattern == "join <A> and <B> on <C> = <D>":
        table1, table2, column1, column2 = entities
        return f"SELECT * FROM {table1} JOIN {table2} ON {table1}.{column1} = {table2}.{column2}"

    elif pattern == "select <A> from <B> join <C> on <D> = <E>":
        columns, table1, table2, column1, column2 = entities
        return f"SELECT {columns} FROM {table1} JOIN {table2} ON {table1}.{column1} = {table2}.{column2}"

# Example usage:
if __name__ == "__main__":
    queries = [
        "Find total sales amount broken down by product category",
        "Which movies were released in 2000?",
        "Calculate average revenue by region",
        "Count orders by customer type",
        "Which movies have a 'PG' rating?",
        "Which movies include 'David Mamet' in the writers array field?",
        "Which movies have a runtime greater than 90 minutes?",
        "Show me the total server performance across different regions",
        "What is the average sales amount per customer?",
        "Find the top 5 products by sales volume",
        "Show me the monthly revenue trend for the past year",
        "What is the distribution of customer ages?"
    ]
    
    for query in queries:
        sql_query = map_query_to_sql(query)
        print(f"Query: {query}\nSQL: {sql_query}\n")
