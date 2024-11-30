import time
import json
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC = 'bitcoin-topic'
KAFKA_SERVER = 'kafka:9092'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_bitcoin_data():
    idx = 0
    data = pd.read_csv('/app/data/stream_data.csv')
    data = data[['timestamp', 'close']]
    while idx < len(data):
        row = data.iloc[idx].to_dict()
        producer.send(KAFKA_TOPIC, row)
        print(f"Sent: {row}")
        time.sleep(5)
        idx += 1

if __name__ == "__main__":
    stream_bitcoin_data()
