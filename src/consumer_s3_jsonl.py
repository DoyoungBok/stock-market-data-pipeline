import json
import time
import uuid
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer

from config import (
    BOOTSTRAP_SERVERS,
    TOPIC,
    AWS_REGION,
    S3_BUCKET,
    S3_PREFIX,
    BATCH_SIZE,
    BATCH_SECONDS,
    CONSUMER_GROUP_ID,
)


def make_s3_key():
    now = datetime.now(timezone.utc)

    return (
        f"{S3_PREFIX}/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
        f"stock_ticks_{now:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}.jsonl"
    )


def upload_batch_to_s3(s3_client, events):
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is empty. Check your .env file.")

    body = "\n".join(json.dumps(event) for event in events) + "\n"
    key = make_s3_key()

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    return key


def main():
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC])

    batch = []
    last_upload_time = time.time()

    print(f"Consuming from Kafka topic: {TOPIC}")
    print(f"Uploading batches to: s3://{S3_BUCKET}/{S3_PREFIX}/")
    print("Waiting for messages... Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is not None:
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                event = json.loads(msg.value().decode("utf-8"))
                batch.append(event)

                print(f"Buffered event: {event['symbol']} close={event['close']}")

            should_upload = (
                batch
                and (
                    len(batch) >= BATCH_SIZE
                    or now - last_upload_time >= BATCH_SECONDS
                )
            )

            if should_upload:
                key = upload_batch_to_s3(s3_client, batch)

                consumer.commit(asynchronous=False)

                print(f"Uploaded {len(batch)} events to s3://{S3_BUCKET}/{key}")

                batch.clear()
                last_upload_time = now

    except KeyboardInterrupt:
        print("Stopping S3 consumer...")

        if batch:
            key = upload_batch_to_s3(s3_client, batch)
            consumer.commit(asynchronous=False)
            print(f"Uploaded final {len(batch)} events to s3://{S3_BUCKET}/{key}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
    