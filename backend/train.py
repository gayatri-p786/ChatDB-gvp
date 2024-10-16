# train_model.py

import json
from sklearn.model_selection import train_test_split
from transformers import T5Tokenizer, T5ForConditionalGeneration, Trainer, TrainingArguments
import torch

def load_dataset(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def prepare_data(final_dataset):
    inputs = []
    outputs = []

    for entry in final_dataset:
        schema_info = {
            "db_id": entry["db_id"],
            "table_names": entry["table_names"],
            "columns": entry["columns"],
            "primary_keys": entry["primary_keys"],
            "foreign_keys": entry["foreign_keys"]
        }
        queries = entry["queries"]

        for query in queries:
            inputs.append(json.dumps(schema_info))  # Convert schema info to string
            outputs.append(query['query'])  # Extract the SQL query

    return inputs, outputs

def tokenize_data(tokenizer, inputs, outputs):
    train_encodings = tokenizer(inputs, truncation=True, padding=True, max_length=512)
    train_labels = tokenizer(outputs, truncation=True, padding=True, max_length=512)
    return train_encodings, train_labels

class SQLDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: val[idx] for key, val in self.encodings.items()}
        item['labels'] = self.labels['input_ids'][idx]
        return item

    def __len__(self):
        return len(self.labels['input_ids'])

def main():
    # Load the dataset
    final_dataset = load_dataset('../datasets/final_dataset.json')

    # Prepare the inputs and outputs
    inputs, outputs = prepare_data(final_dataset)

    # Split the dataset into training and validation sets
    X_train, X_val, y_train, y_val = train_test_split(inputs, outputs, test_size=0.2, random_state=42)

    # Load the tokenizer and model
    tokenizer = T5Tokenizer.from_pretrained('t5-small')
    model = T5ForConditionalGeneration.from_pretrained('t5-small')

    # Tokenize the data
    train_encodings, train_labels = tokenize_data(tokenizer, X_train, y_train)
    val_encodings, val_labels = tokenize_data(tokenizer, X_val, y_val)

    # Create dataset objects
    train_dataset = SQLDataset(train_encodings, train_labels)
    val_dataset = SQLDataset(val_encodings, val_labels)

    # Define training arguments
    training_args = TrainingArguments(
        output_dir='./results',          
        num_train_epochs=3,              # Reduced epochs
        per_device_train_batch_size=42,  # Increased batch size
        per_device_eval_batch_size=42,   
        warmup_steps=500,                 
        weight_decay=0.01,               
        logging_dir='./logs',            
        logging_steps=10,
        learning_rate=2e-3,              # Adjust learning rate
        evaluation_strategy="epoch",
    )

    # Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
    )

    # Train the model
    trainer.train()

    # Save the model
    model.save_pretrained('./sql_query_generator')
    tokenizer.save_pretrained('./sql_query_generator')

if __name__ == "__main__":
    main()
