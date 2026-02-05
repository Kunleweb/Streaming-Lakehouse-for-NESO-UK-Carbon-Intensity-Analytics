import json
import time
import hashlib
from datetime import datetime, timezone
import requests
from confluent_kafka import Producer


API_URL = "https://api.carbonintensity.org.uk/intensity"
TOPIC = "carbon.intensity.uk"
KAFKA_BOOTSTRAP = "localhost:9092"

POLL_SECONDS = 60 
def make_event_id(from_ts: str, to_ts: str) -> str:
    raw = f"{from_ts}|{to_ts}|NATIONAL"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA] Delivery failed: {err}")
    else:
        pass

def fetch_intensity():
    r = requests.get(API_URL, timeout=10)
    r.raise_for_status()
    return r.json()

def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print("[PRODUCER] Starting poller...")

    while True:
        fetched_at = datetime.now(timezone.utc).isoformat()

        try:
            payload = fetch_intensity()
            rows = payload.get("data", [])
            if not rows:
                print("[PRODUCER] No data returned.")
                time.sleep(POLL_SECONDS)
                continue

            for row in rows:
                from_ts = row["from"]
                to_ts = row["to"]
                intensity = row.get("intensity", {})
                event = {
                    "event_id": make_event_id(from_ts, to_ts),
                    "from_ts": from_ts,
                    "to_ts": to_ts,
                    "forecast_gco2_kwh": intensity.get("forecast"),
                    "actual_gco2_kwh": intensity.get("actual"),
                    "index": intensity.get("index"),
                    "fetched_at": fetched_at,
                    "source": "carbonintensity.uk/intensity",
                }

                producer.produce(
                    TOPIC,
                    key=event["event_id"],
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report
                )

            producer.flush(5)
            print(f"[PRODUCER] Published {len(rows)} event(s) at {fetched_at}")

        except Exception as e:
            print(f"[PRODUCER] Error: {e}")

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()

