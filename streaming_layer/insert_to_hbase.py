import happybase

HOST='hbase:9090'

def insert_into_hbase(table_name, data):
    """
    Chèn dữ liệu vào HBase.

    Args:
        host (str): Địa chỉ HBase Thrift server.
        table_name (str): Tên bảng HBase.
        data (dict): Dữ liệu cần chèn (phải chứa khóa 'timestamp' và 'close').

    """
    try:
        connection = happybase.Connection(HOST)
        table = connection.table(table_name)

        row_key = data['timestamp']

        table.put(row_key, {
            b'price:close': str(data['close']).encode('utf-8'),
            b'time:timestamp': data['timestamp'].encode('utf-8')
        })

        print(f"Inserted data into {table_name}: {data}")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        connection.close()
