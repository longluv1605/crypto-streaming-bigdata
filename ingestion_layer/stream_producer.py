import time
import json
import pandas as pd
from kafka import KafkaProducer #type: ignore
import env

KAFKA_TOPIC = env.STREAM_TOPIC
KAFKA_SERVER = env.KAFKA_SERVER


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

producer = create_kafka_producer()
def stream_bitcoin_data():
    idx = 0
    data = pd.read_csv('/app/data/stream_data.csv')
    data = data[['timestamp', 'close']]
    while idx < len(data):
        row = data.iloc[idx].to_dict()
        print(f"Sending: {row}")
        producer.send(KAFKA_TOPIC, row)
        print(f"Sent: {row}")
        time.sleep(5)
        idx += 1

if __name__ == "__main__":
    stream_bitcoin_data()
