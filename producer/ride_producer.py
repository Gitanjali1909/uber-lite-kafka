from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

cab_ids = [f"cab_{i}" for i in range(1, 11)]

while True:
    msg = {
        "cab_id": random.choice(cab_ids),
        "lat": round(random.uniform(28.5, 28.8), 6),   # Delhi-ish
        "lon": round(random.uniform(77.1, 77.3), 6),
        "timestamp": time.time()
    }
    producer.send("rides", msg)
    print("Sent:", msg)
    time.sleep(1)
