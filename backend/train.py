import json
from sklearn.model_selection import train_test_split
from transformers import T5Tokenizer, T5ForConditionalGeneration, Trainer, TrainingArguments
import torch
from torch.utils.data import Dataset
from google.colab import drive
import os

# Mount Google Drive
drive.mount('/content/drive')

class SQLDataset(Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        # Properly construct tensors using clone() and detach()
        item = {
            key: val[idx].clone().detach() 
            for key, val in self.encodings.items()
        }
        item['labels'] = self.labels['input_ids'][idx].clone().detach()
        return item

    def __len__(self):
        return len(self.labels['input_ids'])

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
        
        schema_str = f"""
        Database: {schema_info['db_id']}
        Tables:
        """
        
        for table_idx, table_name in enumerate(schema_info['table_names']):
            schema_str += f"\n{table_name} ("
            columns = schema_info['columns'][str(table_idx)]
            column_strs = [f"{col['column_name']} {col['column_type']}" for col in columns]
            schema_str += ", ".join(column_strs) + ")"
        
        schema_str += "\nPrimary Keys: " + ", ".join(str(pk) for pk in schema_info['primary_keys'])
        
        if schema_info['foreign_keys']:
            schema_str += "\nForeign Keys: "
            for fk in schema_info['foreign_keys'].values():
                schema_str += f"{fk[0][0]} references {fk[0][1]}, "

        queries = entry["queries"]
        for query in queries:
            input_text = f"Generate SQL query for schema: {schema_str}\nTask: {query['query_type']}"
            inputs.append(input_text)
            outputs.append(query['query'])

    return inputs, outputs

def tokenize_data(inputs, outputs, tokenizer):
    # Tokenize inputs
    input_encodings = tokenizer(
        inputs,
        padding='max_length',
        truncation=True,
        max_length=512,
        return_tensors='pt'
    )
    
    # Tokenize outputs
    output_encodings = tokenizer(
        outputs,
        padding='max_length',
        truncation=True,
        max_length=128,
        return_tensors='pt'
    )
    
    return {
        'input_ids': input_encodings.input_ids,
        'attention_mask': input_encodings.attention_mask,
        'labels': output_encodings.input_ids
    }

def train_model():
    DRIVE_PATH = '/content/drive/MyDrive/ChatDB'
    os.makedirs(f'{DRIVE_PATH}/model', exist_ok=True)
    os.makedirs(f'{DRIVE_PATH}/logs', exist_ok=True)
    os.makedirs(f'{DRIVE_PATH}/checkpoints', exist_ok=True)

    print("Initializing model and tokenizer...")
    model_name = "t5-base"
    tokenizer = T5Tokenizer.from_pretrained(model_name)
    model = T5ForConditionalGeneration.from_pretrained(model_name)

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    model = model.to(device)

    print("Loading dataset...")
    dataset_path = f'{DRIVE_PATH}/final_dataset.json'
    with open(dataset_path, 'r') as f:
        final_dataset = json.load(f)

    print("Preparing data...")
    inputs, outputs = prepare_data(final_dataset)
    
    train_inputs, val_inputs, train_outputs, val_outputs = train_test_split(
        inputs, outputs, test_size=0.2, random_state=42
    )

    print(f"Training samples: {len(train_inputs)}")
    print(f"Validation samples: {len(val_inputs)}")

    print("Creating datasets...")
    # Create train dataset
    train_encodings = tokenize_data(train_inputs, train_outputs, tokenizer)
    train_dataset = SQLDataset(
        {k: v for k, v in train_encodings.items() if k != 'labels'},
        {'input_ids': train_encodings['labels']}
    )

    # Create validation dataset
    val_encodings = tokenize_data(val_inputs, val_outputs, tokenizer)
    val_dataset = SQLDataset(
        {k: v for k, v in val_encodings.items() if k != 'labels'},
        {'input_ids': val_encodings['labels']}
    )

    training_args = TrainingArguments(
        output_dir=f'{DRIVE_PATH}/checkpoints',
        num_train_epochs=5,
        per_device_train_batch_size=8,
        per_device_eval_batch_size=8,
        warmup_steps=500,
        weight_decay=0.01,
        logging_dir=f'{DRIVE_PATH}/logs',
        logging_steps=100,
        evaluation_strategy="steps",
        eval_steps=500,
        save_steps=500,
        load_best_model_at_end=True,
        metric_for_best_model="eval_loss",
        greater_is_better=False,
        fp16=True,
        gradient_accumulation_steps=2,
        learning_rate=5e-5,
        save_total_limit=2,
    )

    print("Initializing trainer...")
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
    )

    print("Starting training...")
    trainer.train()

    print("Saving model and tokenizer...")
    model_save_path = f'{DRIVE_PATH}/model/sql_query_generator'
    model.save_pretrained(model_save_path)
    tokenizer.save_pretrained(model_save_path)
    
    print(f"Training complete! Model and tokenizer saved to {model_save_path}")

    print("Generating example predictions...")
    model.eval()
    with torch.no_grad():
        for i in range(min(5, len(val_inputs))):
            input_ids = tokenizer(
                val_inputs[i],
                return_tensors="pt",
                padding=True,
                truncation=True
            ).input_ids.to(device)
            
            outputs = model.generate(input_ids, max_length=128)
            predicted_query = tokenizer.decode(outputs[0], skip_special_tokens=True)
            
            with open(f'{DRIVE_PATH}/example_predictions.txt', 'a') as f:
                f.write(f"Input: {val_inputs[i]}\n")
                f.write(f"Expected: {val_outputs[i]}\n")
                f.write(f"Predicted: {predicted_query}\n")
                f.write("-" * 80 + "\n")

if __name__ == "__main__":
    try:
        train_model()
    except Exception as e:
        print(f"Error occurred: {str(e)}")