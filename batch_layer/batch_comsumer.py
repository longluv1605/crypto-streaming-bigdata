import time
from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer
from hdfs import InsecureClient

KAFKA_TOPIC = 'bitcoin-batch'
KAFKA_SERVER = 'kafka:9092'


HDFS_URL = "http://hadoop-namenode:9870"
HDFS_PATH = "/crypto/bitcoin/datalake"
client = InsecureClient(HDFS_URL, user='root')

# Get data from Kafka
def create_kafka_consumer():
    consumer = None
    while consumer is None:
        try:
            # Create a Kafka consumer
            consumer = KafkaConsumer(
                            KAFKA_TOPIC,
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
            print(data)
            return data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            continue
        

def save_to_hdfs(data):
    # try:
    # Prepare the header (column names) and rows (data)
    header = list(data.keys())
    rows = [ [data[col][i] for col in header] for i in range(len(data['timestamp'])) ]

    timestamp = time.time()
    dt = datetime.fromtimestamp(timestamp) - timedelta(hours=1)
    year = dt.year
    month = dt.month
    day = dt.day
    
    file_path = f"{HDFS_PATH}/{year}/{month}/data_{day}.csv"
    with client.write(file_path) as writer:
        writer.write(f'{",".join(map(str, header))}')
        
        for row in rows:
            writer.write(f"{','.join(map(str, row))}\n")
    # except Exception as e:
    #     print(f"Error put data to datalake: {e}")
    

if __name__=='__main__':
    raw = get_api_data()
    save_to_hdfs(data=raw)
    