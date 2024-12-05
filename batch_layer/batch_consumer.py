import time
from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer
from hdfs import InsecureClient
from kafka import TopicPartition


KAFKA_TOPIC = "bitcoin-batch"
KAFKA_SERVER = "kafka:9092"

HDFS_URL = "http://hadoop-namenode:9870"
HDFS_PATH = "/crypto/bitcoin/datalake"

def create_kafka_consumer():
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda v: v.decode("utf-8"),
                group_id="batch-consumer-group",
                auto_offset_reset="latest",  # Đọc từ thông điệp mới nhất
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
    
    if len(row) == 0:
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
    consumer.poll(timeout_ms=0)  # Xóa bộ đệm cũ
    consumer.seek_to_end()  # Di chuyển đến offset cuối cùng
    print("Waiting for latest message...")
    latest_message = get_latest_data(consumer)
    if latest_message:
        save_to_hdfs(latest_message)
    else:
        print("No messages to process.")
