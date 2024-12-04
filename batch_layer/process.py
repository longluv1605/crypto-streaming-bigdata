from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, unix_timestamp
from pyspark.sql.window import Window
import time
import datetime

from hdfs import InsecureClient

HDFS_URL = "http://hadoop-namenode:9870"
HDFS_PATH = "/crypto/bitcoin/datalake"


# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("Bitcoin Batch Processing") \
    .getOrCreate()

timestamp = time.time()
dt = datetime.datetime.fromtimestamp(timestamp)
year = dt.year
month = dt.month
day = dt.day

# 2. Đọc dữ liệu từ HDFS
data_path = f"hdfs:///crypto/bitcoin/datalake/{year}/{month}/data_{day}.csv"
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


# 5. Ghi dữ liệu đã xử lý lại vào HDFS
save_path = f"hdfs:///crypto/bitcoin/warehouse/{year}/{month}/data_{day}.csv"
df_features.write.csv(save_path, header=True)


-----------------
Lam them phan ghi vao database (option)