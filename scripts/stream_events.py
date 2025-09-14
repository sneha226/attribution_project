from google.cloud import bigquery
from datetime import datetime
import random

# Set your GCP project and dataset
project_id = "cohesive-beach-445504-m1"
dataset_id = "attribution_dataset"
table_id = "stg_events"

client = bigquery.Client(project=project_id)

# Example values for event simulation
users = ["user_1", "user_2", "user_3", "user_4", "user_5"]
events = ["page_view", "purchase", "add_to_cart", "login", "signup"]
platforms = ["web", "ios", "android"]
sources = ["google", "facebook", "direct", "email"]

def generate_event():
    return {
        "user_id": random.choice(users),
        "event_name": random.choice(events),
        "event_timestamp": datetime.utcnow().isoformat(),
        "platform": random.choice(platforms),
        "traffic_source": random.choice(sources),
    }

def insert_batch_events(batch_size=10):
    events_batch = [generate_event() for _ in range(batch_size)]
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    errors = client.insert_rows_json(table_ref, events_batch)
    if errors:
        print("Error inserting batch:", errors)
    else:
        print(f"Inserted {batch_size} events:")
        for e in events_batch:
            print(e)

if __name__ == "__main__":
    # Insert a batch of 10 events
    insert_batch_events(batch_size=10)
