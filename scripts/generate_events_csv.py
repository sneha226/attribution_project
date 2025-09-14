import csv, random, time, uuid, subprocess
from datetime import datetime
from google.cloud import bigquery

# -----------------------------
# BigQuery config
# -----------------------------

client = bigquery.Client(project="cohesive-beach-445504-m1")
table_id = "cohesive-beach-445504-m1.attribution_dataset.streaming_events"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("platform", "STRING"),
        bigquery.SchemaField("traffic_source", "STRING"),
        bigquery.SchemaField("event_id", "STRING"),
    ],
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
)

# -----------------------------
# Event generation config
# -----------------------------
events = ["page_view", "purchase", "login"]
platforms = ["web", "ios", "android"]
sources = ["google", "facebook", "direct"]

NUM_BATCHES = 3
EVENTS_PER_BATCH = 5

# -----------------------------
# 1️⃣ Generate & stream events
# -----------------------------
print("Starting event generation and streaming...")
for i in range(NUM_BATCHES):
    filename = f"batch_{i}.csv"
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id","event_name","event_timestamp","platform","traffic_source","event_id"])
        for _ in range(EVENTS_PER_BATCH):
            writer.writerow([
                f"user_{uuid.uuid4()}",
                random.choice(events),
                datetime.utcnow().isoformat(),
                random.choice(platforms),
                random.choice(sources),
                str(uuid.uuid4())
            ])
    with open(filename, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)
        load_job.result()
    print(f"✅ Loaded batch {i} with {EVENTS_PER_BATCH} events")
    time.sleep(1)

# -----------------------------
# 2️⃣ Run dbt models
# -----------------------------
print("\nRunning dbt models...")
subprocess.run(["dbt", "clean"], cwd="C:/Users/sneha/attribution_project")
subprocess.run(["dbt", "deps"], cwd="C:/Users/sneha/attribution_project")
subprocess.run(["dbt", "run"], cwd="C:/Users/sneha/attribution_project")

# -----------------------------
# 3️⃣ Run dbt tests
# -----------------------------
print("\nRunning dbt tests...")
subprocess.run(["dbt", "test"], cwd="C:/Users/sneha/attribution_project")

print("\n🎉 End-to-end pipeline completed!")

# from google.cloud import bigquery
# from datetime import datetime
# import random
# import google.auth

# # ✅ Use Application Default Credentials (from gcloud auth application-default login)
# credentials, project_id = google.auth.default(
#     scopes=["https://www.googleapis.com/auth/cloud-platform"]
# )

# client = bigquery.Client(credentials=credentials, project=project_id)

# # Dataset & Table info
# dataset_id = "attribution_dataset"
# table_id = "stg_events"

# # Example values for event simulation
# users = ["user_1", "user_2", "user_3", "user_4", "user_5"]
# events = ["page_view", "purchase", "add_to_cart", "login", "signup"]
# platforms = ["web", "ios", "android"]
# sources = ["google", "facebook", "direct", "email"]

# def generate_event():
#     """Generate one random event"""
#     return {
#         "user_id": random.choice(users),
#         "event_name": random.choice(events),
#         "event_timestamp": datetime.utcnow().isoformat(),
#         "platform": random.choice(platforms),
#         "traffic_source": random.choice(sources),
#     }

# def insert_batch_events(batch_size=10):
#     """Insert a batch of events into BigQuery"""
#     events_batch = [generate_event() for _ in range(batch_size)]
#     table_ref = f"{project_id}.{dataset_id}.{table_id}"

#     errors = client.insert_rows_json(table_ref, events_batch)
#     if errors:
#         print("❌ Error inserting batch:", errors)
#     else:
#         print(f"✅ Inserted {batch_size} events:")
#         for e in events_batch:
#             print(e)

# if __name__ == "__main__":
#     insert_batch_events(batch_size=10)
