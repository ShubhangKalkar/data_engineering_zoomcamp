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

df = pd.read_parquet(FILE_PATH, columns=COLS)

# convert datetimes to strings for JSON serialization
for c in ["lpep_pickup_datetime", "lpep_dropoff_datetime"]:
    df[c] = df[c].astype(str)

print("row count:", len(df))
print(df.head(2).to_dict(orient="records"))

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=json_serializer
)

t0 = time()

for row in df.to_dict(orient="records"):
    producer.send(TOPIC, value=row)

producer.flush()

t1 = time()
print(f"took {(t1 - t0):.2f} seconds")
