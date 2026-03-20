import json
from kafka import KafkaConsumer

TOPIC = "green-trips"
SERVER = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[SERVER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=10000,  # stops after no new data
)

total = 0
gt5 = 0

for msg in consumer:
    total += 1
    value = msg.value

    try:
        if float(value["trip_distance"]) > 5.0:
            gt5 += 1
    except:
        pass

print("Total messages:", total)
print("Trips with distance > 5:", gt5)