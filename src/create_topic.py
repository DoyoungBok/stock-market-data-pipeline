from confluent_kafka.admin import AdminClient, NewTopic
from config import BOOTSTRAP_SERVERS, TOPIC


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topic = NewTopic(
        TOPIC,
        num_partitions=3,
        replication_factor=1,
    )

    futures = admin.create_topics([topic])

    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Topic already exists: {topic_name}")
            else:
                print(f"Failed to create topic: {e}")


if __name__ == "__main__":
    main()
    