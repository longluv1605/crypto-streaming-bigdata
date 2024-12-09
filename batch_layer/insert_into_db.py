import mysql.connector # type: ignore
import env

def prepare_data(data):
    # Normalize into a list of dictionaries
    normalized_data = [
        {key: value[str(idx)] for key, value in data.items()}
        for idx in list(data['timestamp'].keys())
    ]
    return normalized_data


def insert_into_mysql(data):
    normalized_data = prepare_data(data)

    # Connect to MySQL
    connection = mysql.connector.connect(
        host=env.MYSQL_HOST,
        user=env.MYSQL_USER,
        password=env.MYSQL_PASS,
        database=env.MYSQL_DB
    )

    cursor = connection.cursor()

    # Define the insert query
    insert_query = """
    INSERT INTO bitcoin_raw (timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    # Insert each record
    for record in normalized_data:
        cursor.execute(insert_query, (
            record['timestamp'],
            record['open'],
            record['high'],
            record['low'],
            record['close'],
            record['volume']
        ))

    # Commit changes and close connection
    connection.commit()
    cursor.close()
    connection.close()

    print("Data inserted successfully!")
