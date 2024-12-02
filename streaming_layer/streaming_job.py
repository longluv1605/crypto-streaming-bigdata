from collections import deque
from api_consumer import get_api_data
from insert_to_hbase import insert_into_hbase
import pickle
from datetime import datetime, timedelta

WINDOW=32

def load_model():
    with open("/app/trained_model/xgboost.pkl", "rb") as f:
        model = pickle.load(f)   
    return model

def streaming_process():
    dq = deque(maxlen=WINDOW)
    print("Created deque!")
    model = load_model()
    print("Loaded Model!")

    while True:
        real_data = get_api_data()
        if real_data:
            print(real_data)
            print("Received data!")
            
            dq.append([real_data['timestamp'], real_data['close']])
            print("Inserted into queue!")
            
            insert_into_hbase('real_stream', real_data)
            
            if len(dq) == 32:
                store = list(dq)
                x = store[:, 1]
                print(x)
                print("Extracted input")
                
                pred = model.predict([x])[0]
                print(pred)
                print("Predicted!")
                
                dt = datetime.strptime(real_data['timestamp'], '%Y-%m-%d %H:%M:%S')
                new_dt = dt + timedelta(minutes=1)
                new_timestamp = new_dt.strftime('%Y-%m-%d %H:%M:%S')

                
                pred_data = {
                    'timestamp': new_timestamp,
                    'close': pred
                }
                print(pred_data)
                
                insert_into_hbase('pred_stream', pred_data)
                
        
if __name__ == "__main__":
    streaming_process()