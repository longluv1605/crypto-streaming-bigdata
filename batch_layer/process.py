from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, unix_timestamp, lead
from pyspark.sql.window import Window
import time
from datetime import datetime, timedelta

from hdfs import InsecureClient

# HDFS Configurations
HDFS_URL = "http://hadoop-namenode:9870"
HDFS_DATALAKE = "/crypto/bitcoin/datalake"
HDFS_WAREHOUSE = "/crypto/bitcoin/warehouse"

# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("Bitcoin Batch Processing") \
    .getOrCreate()
    
def process_to_warehouse():
    timestamp = time.time()
    dt = datetime.fromtimestamp(timestamp) - timedelta(hours=1)
    year = dt.year
    month = dt.month
    day = dt.day

    # 2. Đọc dữ liệu từ HDFS
    data_path = f"{HDFS_URL}/{HDFS_DATALAKE}/{year}/{month}/data_{day}.csv"
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # 3. Làm sạch dữ liệu
    df_cleaned = df.dropna()  # Loại bỏ các hàng có giá trị null
    df_cleaned = df_cleaned.withColumn("timestamp", unix_timestamp(col("timestamp")))

    # 4. Tính các đặc trưng (Feature Engineering)
    window_spec = Window.orderBy("timestamp")

    df_features = df_cleaned \
        .withColumn("price_avg", (col("high") + col("low")) / 2) \
        .withColumn("price_change", col("close") - col("open")) \
        .withColumn("moving_avg", avg("close").over(window_spec.rowsBetween(-5, 0))) \
        .withColumn("volatility", stddev("close").over(window_spec.rowsBetween(-5, 0)))

    # 5. Tạo cột future_close (dịch chuyển giá close để làm nhãn)
    df_features = df_features.withColumn("future_close", lead("close").over(window_spec))

    # 6. Loại bỏ các bản ghi không có giá trị future_close
    df_features = df_features.filter(col("future_close").isNotNull())

    # 7. Ghi dữ liệu đã xử lý lại vào HDFS
    save_path = f"{HDFS_URL}/{HDFS_WAREHOUSE}/{year}/{month}/data_{day}.csv"
    df_features.write.csv(save_path, header=True)

    # (Optional) Ghi vào cơ sở dữ liệu (ví dụ: MySQL)
    # write_to_database(df_features)

# def write_to_database(df_features):
#     # Example: Ghi dữ liệu vào MySQL
#     df_features.write.format("jdbc") \
#         .option("url", "jdbc:mysql://localhost:3306/crypto") \
#         .option("driver", "com.mysql.cj.jdbc.Driver") \
#         .option("dbtable", "bitcoin_features") \
#         .option("user", "your_username") \
#         .option("password", "your_password") \
#         .mode("append") \
#         .save()

# 8. Gọi hàm xử lý
process_to_warehouse()
