import json
from confluent_kafka import Consumer
from config import BOOTSTRAP_SERVERS, TOPIC


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "stock-local-consumer",
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([TOPIC])

    print(f"Consuming from Kafka topic: {TOPIC}")
    print("Waiting for messages... Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            print(event)

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
    