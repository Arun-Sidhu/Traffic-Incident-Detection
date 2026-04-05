# gps_ping_producer.py
import os, json, time, uuid, random
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "gps_pings_raw")

DUP_PROB = 0.02
OOO_PROB = 0.02
PRINT_EVERY_SEC = 10

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=20,
    request_timeout_ms=30000,
)

NUM_VEHICLES = 50
vehicles = {
    f"veh-{i}": {
        "lat": 47.61 + random.uniform(-0.02, 0.02),
        "lon": -122.33 + random.uniform(-0.03, 0.03),
        "speed": random.uniform(5, 20),
    }
    for i in range(NUM_VEHICLES)
}

ACCIDENT_START_SEC = int(os.getenv("ACCIDENT_START_SEC", "30"))
ACCIDENT_BOX = {"lat_min": 47.605, "lat_max": 47.615, "lon_min": -122.340, "lon_max": -122.325}

def in_box(lat, lon, box):
    return box["lat_min"] <= lat <= box["lat_max"] and box["lon_min"] <= lon <= box["lon_max"]

print(f"Producing to {TOPIC} via {BOOTSTRAP}. Ctrl+C to stop.")

# Counters
total_events = total_records = total_dup_events = total_ooo_events = 0
int_events = int_records = int_dup_events = int_ooo_events = 0

start_mon = time.monotonic()
next_print_mon = start_mon + PRINT_EVERY_SEC
accident_on = False

try:
    while True:
        now_ms = int(time.time() * 1000)

        # Toggle accident after warmup (once per tick)
        new_accident_on = (time.monotonic() - start_mon) >= ACCIDENT_START_SEC
        if new_accident_on and not accident_on:
            elapsed = time.monotonic() - start_mon
            print(f"Accident mode ON at t={elapsed:.1f}s")
        accident_on = new_accident_on

        for vid, s in vehicles.items():
            s["lat"] += random.uniform(-0.0005, 0.0005)
            s["lon"] += random.uniform(-0.0007, 0.0007)

            base_speed = max(0.0, s["speed"] + random.uniform(-2, 2))

            if accident_on and in_box(s["lat"], s["lon"], ACCIDENT_BOX):
                speed = base_speed * random.uniform(0.05, 0.25)
            else:
                speed = base_speed

            event = {
                "vehicleId": vid,
                "eventId": str(uuid.uuid4()),
                "timestampMs": now_ms,
                "lat": s["lat"],
                "lon": s["lon"],
                "speedMps": speed,
            }

            make_ooo = (random.random() < OOO_PROB)
            make_dup = (random.random() < DUP_PROB)

            if make_ooo:
                event["timestampMs"] = now_ms - random.randint(500, 5000)

            total_events += 1
            int_events += 1
            if make_ooo:
                total_ooo_events += 1
                int_ooo_events += 1
            if make_dup:
                total_dup_events += 1
                int_dup_events += 1

            if make_dup:
                producer.send(TOPIC, key=vid, value=event)
                producer.send(TOPIC, key=vid, value=event)
                total_records += 2
                int_records += 2
            else:
                producer.send(TOPIC, key=vid, value=event)
                total_records += 1
                int_records += 1

        producer.flush()

        now_mon = time.monotonic()
        if now_mon >= next_print_mon:
            elapsed = now_mon - start_mon
            rate = int_records / PRINT_EVERY_SEC
            print(
                f"[{elapsed:6.1f}s] accident_on={accident_on} "
                f"sent_records(total)={total_records} events(total)={total_events} "
                f"dup_events(total)={total_dup_events} ooo_events(total)={total_ooo_events} | "
                f"last_{PRINT_EVERY_SEC}s: records={int_records} events={int_events} "
                f"dup={int_dup_events} ooo={int_ooo_events} rate={rate:.1f} msg/s"
            )
            int_events = int_records = int_dup_events = int_ooo_events = 0
            next_print_mon = now_mon + PRINT_EVERY_SEC

        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    producer.flush()
    producer.close()
