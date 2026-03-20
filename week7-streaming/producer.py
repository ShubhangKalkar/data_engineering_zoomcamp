import json
from time import time
import pandas as pd
from kafka import KafkaProducer

TOPIC = "green-trips"
SERVER = "localhost:9092"

FILE_PATH = r"../data/green/green_tripdata_2025-10.parquet"

COLS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# 1. Load data
df = pd.read_parquet(FILE_PATH, columns=COLS)

# 2. FIX: Replace NaN values with None (converts to 'null' in JSON)
# We use where(pd.notnull(df), None) to ensure compatibility across types
df = df.where(pd.notnull(df), None)

# 3. Convert datetimes to strings
for c in ["lpep_pickup_datetime", "lpep_dropoff_datetime"]:
    df[c] = df[c].astype(str)

# 2. Force all NaN/NaT to None using a more aggressive approach
df = df.astype(object).where(pd.notnull(df), None)

# 3. Check for NaN one last time before sending
print("Checking for NaNs...")
print(df.isna().sum())

print("row count:", len(df))
# Quick check: should see None instead of NaN in the output
print(df.head(2).to_dict(orient="records"))

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=json_serializer
)

t0 = time()

# 4. Send records
for row in df.to_dict(orient="records"):
    producer.send(TOPIC, value=row)

producer.flush()

t1 = time()
print(f"took {(t1 - t0):.2f} seconds")