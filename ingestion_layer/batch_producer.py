import time
import json
import pandas as pd
from kafka import KafkaProducer  # type: ignore
import env

KAFKA_TOPIC = env.BATCH_TOPIC
KAFKA_SERVER = env.KAFKA_SERVER


def create_kafka_producer():
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,  # Use the internal Docker port 9092
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as e:
            print(f"Kafka not available yet, retrying... Error: {e}")
            time.sleep(5)  # Retry every 5 seconds
    print("Created producer")
    return producer


def get_idx(filepath="/app/idx.txt"):
    with open(filepath, "r") as f:
        idx = f.readline()
    f.close()
    with open(filepath, "w") as f:
        f.write(str(int(idx) + 1440))
    f.close()
    return int(idx)


def get_data_from_api(idx, datapath="/app/data/batch_data.csv"):
    df = pd.read_csv(datapath)
    data = df.iloc[idx : idx + 1440]
    print(data.shape)
    return data.to_dict()


producer = create_kafka_producer()


def batch_bitcoin_data():
    try:
        # time.sleep(10)
        idx = get_idx()
        print(idx)
        data = get_data_from_api(idx)
        print(f"Sending: {data.keys()}")
        producer.send(KAFKA_TOPIC, data)
        print(f"Sent: {data.keys()} to {KAFKA_TOPIC}")
        producer.close()
    except Exception as e:
        print("Error to sent batch_bitcoin_data: ", e)


if __name__ == "__main__":
    batch_bitcoin_data()
