# producer/cab_producer.py
import argparse, json, time, random
from kafka import KafkaProducer
from datetime import datetime

def make_random_cabs(n, bbox):
    lat_min, lat_max, lon_min, lon_max = bbox
    return [
        {
            "cab_id": f"cab_{i}",
            "lat": random.uniform(lat_min, lat_max),
            "lon": random.uniform(lon_min, lon_max),
            "speed": random.uniform(5, 15),   # m/s
            "ts": datetime.utcnow().isoformat()
        }
        for i in range(n)
    ]

def replay_from_csv(path):
    import csv
    for r in csv.DictReader(open(path)):
        # Expect columns: cab_id,lat,lon,speed,ts
        yield {
            "cab_id": r["cab_id"],
            "lat": float(r["lat"]),
            "lon": float(r["lon"]),
            "speed": float(r.get("speed", 10.0)),
            "ts": r.get("ts")
        }

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["random","replay"], default="random")
    p.add_argument("--num-cabs", type=int, default=40)
    p.add_argument("--rate", type=float, default=1.0, help="pings per cab per second (approx)")
    p.add_argument("--bbox", nargs=4, type=float, default=[37.70,37.81,-122.52,-122.36], help="lat_min lat_max lon_min lon_max")
    p.add_argument("--file", type=str, default="")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    if args.mode == "random":
        cabs = make_random_cabs(args.num_cabs, args.bbox)
        # continuously move them
        while True:
            for cab in cabs:
                # tiny random move
                cab["lat"] += random.uniform(-0.0005, 0.0005)
                cab["lon"] += random.uniform(-0.0005, 0.0005)
                cab["speed"] = max(1.0, cab["speed"] + random.uniform(-1, 1))
                cab["ts"] = datetime.utcnow().isoformat()
                producer.send("rides", value=cab)
            time.sleep(1.0 / max(0.1, args.rate))

    else:
        for row in replay_from_csv(args.file):
            producer.send("rides", value=row)
            time.sleep(0.2)
