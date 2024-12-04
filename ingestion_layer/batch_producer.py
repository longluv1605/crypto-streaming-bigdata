import time
import json
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC = 'bitcoin-batch'
KAFKA_SERVER = 'kafka:9092'


def create_kafka_producer():
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,  # Use the internal Docker port 9092
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"Kafka not available yet, retrying... Error: {e}")
            time.sleep(5)  # Retry every 5 seconds
    print("Created producer")
    return producer


def get_idx(filepath='/app/idx.txt'):
    with open(filepath, 'r') as f:
        idx = f.readline()
    f.close()
    with open(filepath, 'w') as f:
        f.write(str(int(idx) + 1440))
    f.close()
    return int(idx)

def get_data_from_api(idx, datapath='/app/data/batch_data.csv'):
    df = pd.read_csv(datapath)
    data = df.iloc[idx:idx+1440]
    return data

def batch_bitcoin_data():
    producer = create_kafka_producer()
    try:
        idx = get_idx()
        data = get_data_from_api(idx)
        while idx < len(data):
            show = data.iloc[:2].to_dict()
            data = data.to_dict()
            print(f"Sending: {show}")
            producer.send(KAFKA_TOPIC, data)
            print(f"Sent: {show}")
            time.sleep(5)
    except Exception as e:
        print("Error to sent batch_bitcoin_data: ", e)

if __name__ == "__main__":
    batch_bitcoin_data()
