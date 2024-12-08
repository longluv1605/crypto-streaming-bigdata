from collections import deque
from stream_consumer import get_api_data
from insert_to_hbase import insert_into_hbase
import pickle
from datetime import datetime, timedelta
import numpy as np

WINDOW=32
def load_model():
    with open("/app/trained_model/xgboost.pkl", "rb") as model_file, open('/app/trained_model/scaler.pkl', 'rb') as scaler_file:
        model = pickle.load(model_file)
        scaler = pickle.load(scaler_file)   
    return model, scaler

def streaming_process():
    dq = deque(maxlen=WINDOW)
    print("Created deque!")
    model, scaler = load_model()
    print("Loaded Model!")

    while True:
        real_data = get_api_data()
        if real_data:
            print(real_data)
            print("Received data!")
            
            try:
                timestamp = real_data['timestamp']
                close_price = float(real_data['close']) 
                dq.append([timestamp, close_price])
                print("Inserted into queue!")
            except ValueError as e:
                print(f"Error converting 'close' to float: {e}")
                continue
            
            if len(dq) == 32:
                store = np.array(list(dq))
                x = store[:, 1].astype(float)
                print(x)
                print("Extracted input")
                
                x = scaler.transform([x])
                print(x)
                print("Scaled input")
                
                pred = model.predict(x)[0]
                print(pred)
                print("Predicted!")
                
                dt = datetime.strptime(real_data['timestamp'], '%Y-%m-%d %H:%M:%S')
                new_dt = dt + timedelta(minutes=1)
                new_timestamp = new_dt.strftime('%Y-%m-%d %H:%M:%S')

                
                pred_data = {
                    'timestamp': new_timestamp,
                    'close': float(pred)
                }
                print(pred_data)
                
                insert_into_hbase('pred_stream', pred_data)
            insert_into_hbase('real_stream', real_data)
            
        
if __name__ == "__main__":
    streaming_process()