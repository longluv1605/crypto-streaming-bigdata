import happybase # type: ignore
import env

HOST = env.HBASE_HOST
PORT = env.HBASE_PORT


def insert_into_hbase(table_name, data):
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
