import time
from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer  # type: ignore
from hdfs import InsecureClient  # type: ignore

from insert_into_db import insert_into_mysql
import env

KAFKA_TOPIC = env.KAFKA_TOPIC
KAFKA_SERVER = env.KAFKA_SERVER

HDFS_URL = env.HDFS_URL
HDFS_PATH = env.HDFS_DATALAKE


def create_kafka_consumer():
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda v: v.decode("utf-8"),
                group_id="batch-consumer-group",
                auto_offset_reset="earliest",
            )
        except Exception as e:
            print(f"Kafka not available yet, retrying... Error: {e}")
            time.sleep(5)
    print("Created consumer!")
    return consumer


def get_latest_data(consumer):
    data = []
    for message in consumer:
        print("\nGot message:")
        value = json.loads(message.value)
        if len(list(value["timestamp"].keys())) > 0:
            print(value["timestamp"][list(value["timestamp"].keys())[0]])
            print(value["timestamp"][list(value["timestamp"].keys())[-1]])
        data.append(value)
        return value

    return data[-1]


def save_to_hdfs(data):
    # try:
    # Prepare the header (column names) and rows (data)
    header = list(data.keys())
    rows = [
        [data[col][str(i)] for col in header] for i in list(data["timestamp"].keys())
    ]

    if len(rows) == 0:
        print("Data is empty")
        return
    print("Got data")

    timestamp = data["timestamp"][list(data["timestamp"].keys())[-1]]

    with open("/app/date.txt", "w") as f:
        f.write(timestamp)
    f.close()

    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    year = dt.year
    month = dt.month
    day = dt.day

    print(f"{day}-{month}-{year}")

    file_path = f"{HDFS_PATH}/{year}/{month}/data_{day}.csv"
    print(file_path)

    client = InsecureClient(HDFS_URL, user="root")

    if client.status(file_path, strict=False):
        print(f"File {file_path} already exists. Skipping...")
    else:
        with client.write(file_path) as writer:
            writer.write(f'{",".join(map(str, header))}\n')
            for row in rows:
                writer.write(f"{','.join(map(str, row))}\n")
        print("Uploaded data to DATALAKE!")


if __name__ == "__main__":
    consumer = create_kafka_consumer()
    # consumer.poll(timeout_ms=0)
    # consumer.seek_to_end()
    print("Waiting for latest message...")
    latest_message = get_latest_data(consumer)
    consumer.close()
    print(latest_message['timestamp'].keys())
    if latest_message:
        save_to_hdfs(latest_message)
        insert_into_mysql(latest_message)
    else:
        print("No messages to process.")
