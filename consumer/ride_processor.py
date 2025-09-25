import json, time
from kafka import KafkaConsumer, KafkaProducer
import redis
from utils.geo import haversine_m, grid_key
from datetime import datetime, timezone

# -------- CONFIG ----------
KAFKA_BOOTSTRAP = "localhost:9092"
REDIS_HOST = "localhost"
SURGE_THRESHOLD = 12   # pings per minute in a grid -> alert

# -------- REDIS ----------
r = redis.StrictRedis(host=REDIS_HOST, port=6379, decode_responses=True)

# -------- KAFKA ----------
consumer = KafkaConsumer(
    "rides",
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="ride-processor"
)

proc_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -------- HELPERS ----------
def compute_eta_meters(speed_m_s, meters_ahead=3000):
    """ETA in seconds to cover 'meters_ahead' at current speed"""
    if speed_m_s and speed_m_s > 0.1:
        return meters_ahead / speed_m_s
    return meters_ahead / 5.0  # fallback speed

# -------- PROCESS LOOP ----------
for msg in consumer:
    val = msg.value
    cab = val["cab_id"]
    lat = float(val["lat"])
    lon = float(val["lon"])
    speed = float(val.get("speed", 10.0))
    ts = val.get("ts", datetime.now(timezone.utc).isoformat())

    # ETA
    eta_seconds = compute_eta_meters(speed)
    eta_min = round(eta_seconds / 60, 2)

    # Update Redis
    key = f"cab:{cab}"
    r.hset(key, mapping={"lat": lat, "lon": lon, "speed": speed, "eta_min": eta_min, "ts": ts})
    r.sadd("active_cabs", cab)

    # Zone counts (1-min sliding window)
    zone = grid_key(lat, lon, precision=2)
    zkey = f"zone:{zone}"
    r.incr(zkey)
    r.expire(zkey, 65)

    # Push processed message to Kafka
    processed = {"cab_id": cab, "lat": lat, "lon": lon, "eta_min": eta_min, "ts": ts, "zone": zone}
    proc_producer.send("processed_rides", value=processed)

    # Metrics: avg ETA
    try:
        active = list(r.smembers("active_cabs"))
        total_eta, n = 0.0, 0
        for c in active:
            d = r.hgetall(f"cab:{c}")
            if d and d.get("eta_min"):
                total_eta += float(d["eta_min"])
                n += 1
        avg_eta = (total_eta / n) if n else 0.0
        r.rpush("metrics:avg_eta", json.dumps({"ts": datetime.utcnow().isoformat(), "avg_eta": avg_eta, "active": len(active)}))
        r.ltrim("metrics:avg_eta", -500, -1)
    except Exception:
        pass

    # SURGE detection
    count = int(r.get(zkey) or 0)
    if count >= SURGE_THRESHOLD:
        alert = {"type": "surge", "zone": zone, "count": count, "ts": datetime.utcnow().isoformat()}
        r.rpush("alerts", json.dumps(alert))
        r.expire(zkey, 30)  # backoff

    # STUCK detection
    prev = r.hgetall(f"cab_state:{cab}") or {}
    stuck_count = int(prev.get("stuck_count", 0))
    if speed < 1.0:
        stuck_count += 1
    else:
        stuck_count = 0
    r.hset(f"cab_state:{cab}", mapping={"stuck_count": stuck_count, "last_speed": speed})
    if stuck_count >= 3:
        alert = {"type": "stuck", "cab": cab, "zone": zone, "ts": datetime.utcnow().isoformat()}
        r.rpush("alerts", json.dumps(alert))
        r.hset(f"cab_state:{cab}", "stuck_count", 0)

    time.sleep(0.001)  # prevent tight loop
