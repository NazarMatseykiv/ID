import csv
import json
import os
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker1:29092,broker2:29093"
)

TOPICS = ["Topic1", "Topic2"]


def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='producer-admin'
    )

    topics = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
        for topic in TOPICS
    ]

    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        print("Topics created successfully")
    except TopicAlreadyExistsError:
        print("Topics already exist")
    except Exception as e:
        print(f"Topic creation error: {e}")


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def send_messages():
    with open('data.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            message = {
                "id": int(row['id']),
                "name": row['name'],
                "age": int(row['age']),
                "city": row['city']
            }

            for topic in TOPICS:
                producer.send(topic, value=message)
                print(f"Sent to {topic}: {message}")

            time.sleep(1)

    producer.flush()


if __name__ == '__main__':
    print("Waiting for Kafka to start...")
    time.sleep(15)

    create_topics()
    send_messages()

    print("All messages sent successfully")