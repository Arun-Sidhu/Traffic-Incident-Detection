# postgres_sink.py
import os, json
from kafka import KafkaConsumer
import psycopg2

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:29092")
TOPIC = os.getenv("TOPIC", "traffic_incidents")

PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "trafficdb")
PG_USER = os.getenv("PG_USER", "traffic")
PG_PASS = os.getenv("PG_PASS", "traffic")

INSERT_EVENT_SQL = """
  INSERT INTO incident_events
    (incident_id, segment_id, status, start_ts, end_ts,
     severity, baseline_speed_mps, observed_speed_mps, observed_count)
  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
  ON CONFLICT (incident_id, status, end_ts) DO NOTHING;
"""

UPSERT_CURRENT_SQL = """
  INSERT INTO current_incidents
    (incident_id, segment_id, start_ts,
     severity, baseline_speed_mps, observed_speed_mps, observed_count, updated_at)
  VALUES (%s,%s,%s,%s,%s,%s,%s, now())
  ON CONFLICT (incident_id) DO UPDATE SET
    segment_id = EXCLUDED.segment_id,
    start_ts = EXCLUDED.start_ts,
    severity = EXCLUDED.severity,
    baseline_speed_mps = EXCLUDED.baseline_speed_mps,
    observed_speed_mps = EXCLUDED.observed_speed_mps,
    observed_count = EXCLUDED.observed_count,
    updated_at = now();
"""

DELETE_CURRENT_SQL = "DELETE FROM current_incidents WHERE incident_id = %s;"

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="postgres-sink",
    )

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False

    print(f"Listening on '{TOPIC}' -> writing to Postgres... Ctrl+C to stop.")

    try:
        with conn.cursor() as cur:
            for msg in consumer:
                try:
                    e = msg.value

                    incident_id = e.get("incidentId")
                    segment_id  = e.get("segmentId")
                    status      = e.get("status")
                    if status == "close":
                        status = "closed"
                    start_ts    = int(e.get("startTs") or 0)
                    end_ts      = int(e.get("endTs") or 0)

                    severity = e.get("severity")
                    baseline = e.get("baselineSpeedMps")
                    observed = e.get("observedSpeedMps")
                    count    = e.get("observedCount")

                    if not incident_id or not segment_id or status not in ("open", "closed"):
                        continue

                    # 1) History table
                    cur.execute(
                        INSERT_EVENT_SQL,
                        (incident_id, segment_id, status, start_ts, end_ts,
                         severity, baseline, observed, count),
                    )

                    # 2) Current table
                    if status == "open":
                        cur.execute(
                            UPSERT_CURRENT_SQL,
                            (incident_id, segment_id, start_ts,
                             severity, baseline, observed, count),
                        )
                    else:
                        cur.execute(DELETE_CURRENT_SQL, (incident_id,))

                    # Commit DB, then commit Kafka offset
                    conn.commit()
                    consumer.commit()

                except Exception as ex:
                    conn.rollback()
                    print("error:", ex)

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()