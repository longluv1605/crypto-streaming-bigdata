import happybase

HOST = "hbase"
PORT = 9090


def insert_into_hbase(table_name, data):
    """
    Insert data into HBase.

    Args:
        table_name (str): Name of the HBase table.
        data (dict): Data to insert (must contain keys 'timestamp' and 'close').

    """
    connection = None
    try:
        # Establish the connection
        connection = happybase.Connection(HOST, port=PORT)
        table = connection.table(table_name)

        # Create row key and insert data
        row_key = data["timestamp"]
        table.put(
            row_key,
            {
                b"price:close": str(data["close"]).encode("utf-8"),
                b"time:timestamp": data["timestamp"].encode("utf-8"),
            },
        )

        print(f"Inserted data into {table_name}: {data}")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        # Close connection if it was successfully created
        if connection:
            connection.close()
