import happybase
import env
from datetime import datetime
import time

# Kết nối tới HBase
HOST = env.HBASE_HOST
PORT = env.HBASE_PORT

def fetch_latest_data(table_name):
    connection = None
    try:
        # Tạo kết nối
        connection = happybase.Connection(HOST, port=PORT)
        table = connection.table(table_name)

        # Quét bảng và lấy dữ liệu mới nhất
        # `scan` trả về tất cả các dòng, có thể được sắp xếp theo timestamp
        rows = table.scan(columns=[b'price:close', b'time:timestamp'])

        latest_data = None
        latest_timestamp = None

        for row_key, data in rows:
            # Chuyển đổi timestamp từ byte sang string và sau đó thành datetime
            timestamp_str = data[b'time:timestamp'].decode('utf-8')
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            # Lấy giá trị price:close
            close_price = float(data[b'price:close'].decode('utf-8'))

            # Cập nhật dữ liệu mới nhất dựa trên timestamp
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_data = {
                    "close": close_price,
                    "timestamp": timestamp
                }
                latest_timestamp = timestamp

        if latest_data:
            # print(f"Latest Data: {latest_data}")
            return latest_data
        else:
            print("No data found.")
            return None

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None
    finally:
        # Đóng kết nối nếu tồn tại
        if connection:
            connection.close()


# Ví dụ lấy dữ liệu mới nhất mỗi giây
if __name__ == "__main__":
    table_name = "pred_stream"
    while True:
        data = fetch_latest_data(table_name)
        if data:
            print(f"Close Price: {data['close']}, Timestamp: {data['timestamp']}")
        
        # Chờ một giây trước khi lấy dữ liệu mới
        time.sleep(1)
