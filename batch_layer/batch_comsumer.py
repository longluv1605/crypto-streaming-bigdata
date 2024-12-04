import time
from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer
from hdfs import InsecureClient

KAFKA_TOPIC = "bitcoin-batch"
KAFKA_SERVER = "kafka:9092"

HDFS_URL = "http://hadoop-namenode:9870"
HDFS_PATH = "/crypto/bitcoin/datalake"
client = InsecureClient(HDFS_URL, user="root")


# Get data from Kafka
def create_kafka_consumer():
    consumer = None
    while consumer is None:
        try:
            # Create a Kafka consumer
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda v: v.decode("utf-8"),
                group_id="batch-consumer-group",
                auto_offset_reset="earliest",
            )
        except Exception as e:
            print(f"Kafka not available yet, retrying... Error: {e}")
            time.sleep(5)  # Retry every 5 seconds
    print("Created consumer!")
    return consumer


consumer = create_kafka_consumer()


def get_api_data():
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<")
    for message in consumer:
        print("--------------<>--------------")
        try:
            data = json.loads(message.value)
            print("Loaded data from message")
            print(data)
            return data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            continue


def save_to_hdfs(data):
    # try:
    # Prepare the header (column names) and rows (data)
    header = list(data.keys())
    rows = [[data[col][str(i)] for col in header] for i in range(len(data["timestamp"]))]
    print("Got data")
    
    time = data['timestamp']['0']
    
    with open('/app/date.txt', 'w') as f:
        f.write(time)
    f.close()

    dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    year = dt.year
    month = dt.month
    day = dt.day

    print(f"{day}-{month}-{year}")

    file_path = f"{HDFS_PATH}/{year}/{month}/data_{day}.csv"
    with client.write(file_path) as writer:
        print("Created writer!")
        writer.write(f'{",".join(map(str, header))}\n')
        print("Wrote header")

        i = 0
        for row in rows:
            writer.write(f"{','.join(map(str, row))}\n")
            print("Wrote line ", i)
            i += 1
    # except Exception as e:
    #     print(f"Error put data to datalake: {e}")
    print("Uploaded data to DATALAKE!")


if __name__ == "__main__":
    print("------------START !!!!!!!!!!!! --------------------")
    raw = get_api_data()
    save_to_hdfs(data=raw)
