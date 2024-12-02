import time
import json
from kafka import KafkaConsumer

KAFKA_TOPIC = 'bitcoin-topic'
KAFKA_SERVER = 'kafka:9092'

def create_kafka_consumer():
    consumer = None
    while consumer is None:
        try:
            # Create a Kafka consumer
            consumer = KafkaConsumer(
                            KAFKA_TOPIC,
                            group_id='crypto',
                            auto_offset_reset='latest',
                            bootstrap_servers=KAFKA_SERVER,
                            value_deserializer=lambda v: v.decode('utf-8')
                        )
        except Exception as e:
            print(f"Kafka not available yet, retrying... Error: {e}")
            time.sleep(5)  # Retry every 5 seconds
    return consumer

consumer = create_kafka_consumer()
def get_api_data():
    for message in consumer:
        try: 
            data = json.loads(message.value)
            return data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            continue