import json
import random
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer

from config import BOOTSTRAP_SERVERS, TOPIC, CSV_PATH, PRODUCER_INTERVAL_SECONDS


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Produced to topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def build_event(row):
    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "symbol": row["symbol"],
        "trade_date": row["date"],
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": int(row["volume"]),
        "source": "csv_simulator",
    }


def main():
    df = pd.read_csv(CSV_PATH)
    records = df.to_dict(orient="records")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "stock-csv-producer",
        "acks": "all",
    })

    print(f"Producing stock events to Kafka topic: {TOPIC}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            row = random.choice(records)
            event = build_event(row)

            producer.produce(
                TOPIC,
                key=event["symbol"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)
            time.sleep(PRODUCER_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()
    