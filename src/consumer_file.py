import json
from pathlib import Path

from confluent_kafka import Consumer

from config import BOOTSTRAP_SERVERS, TOPIC


OUTPUT_PATH = Path("output/stock_ticks.jsonl")


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "stock-file-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])

    print(f"Consuming from Kafka topic: {TOPIC}")
    print(f"Writing events to: {OUTPUT_PATH}")
    print("Waiting for messages... Press Ctrl+C to stop.")

    try:
        with OUTPUT_PATH.open("a", encoding="utf-8") as file:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                event = json.loads(msg.value().decode("utf-8"))

                file.write(json.dumps(event) + "\n")
                file.flush()

                print(
                    f"Saved event: symbol={event['symbol']}, "
                    f"close={event['close']}, "
                    f"time={event['event_time']}"
                )

    except KeyboardInterrupt:
        print("Stopping file consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
    