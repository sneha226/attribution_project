from google.cloud import bigquery
import csv, random, time, uuid
from datetime import datetime

client = bigquery.Client()
table_id = "luminous-wharf-471617-h4.attribution_dataset.streaming_events"

# Schema config
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

users = ["user_1","user_2","user_3"]
events = ["page_view","purchase","login"]
platforms = ["web","ios","android"]
sources = ["google","facebook","direct"]

for i in range(3):  # simulate 3 small batches
    filename = f"batch_{i}.csv"
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id","event_name","event_timestamp","platform","traffic_source","event_id"])
        for _ in range(5):  # 5 events per batch
            writer.writerow([
                random.choice(users),
                random.choice(events),
                datetime.utcnow().isoformat(),
                random.choice(platforms),
                random.choice(sources),
                str(uuid.uuid4())
            ])
    with open(filename, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)
        load_job.result()
    print(f"Loaded batch {i}")
    time.sleep(5)  # wait 5 sec before next batch
