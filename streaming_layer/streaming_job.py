import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, unix_timestamp, lit, size, window, collect_list, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BitcoinPrediction") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load trained model
with open("/app/trained_model/xgboost.pkl", "rb") as f:
    model = pickle.load(f)

# Define schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("close", DoubleType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bitcoin-topic") \
    .load()

print("======================================================  1  ====================================================================")
   
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()


# Deserialize and preprocess the data
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.timestamp", "data.close") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Add water mark
df = df.withWatermark("timestamp", "1 minute")

# Process real_data immediately (real-time data processing)
def process_real_data(batch_df, _):
    # Print the real-time data
    print("======================================================  2  ====================================================================")
    
    batch_df.show(truncate=False)
    
    # Write real_data to HBase
    batch_df.write \
        .format("org.apache.phoenix.spark") \
        .option("phoenix.url", "jdbc:phoenix:hbase:2181") \
        .option("table", "real_stream") \
        .mode("append") \
        .save()


# Write real data to HBase
df.writeStream \
    .foreachBatch(process_real_data) \
    .outputMode("append") \
    .start()

# Apply time-based window (32 minutes) for price_sequence
df_windowed = df.withColumn(
    "window", window("timestamp", "32 minutes")
) \
    .groupBy("window") \
    .agg(collect_list("close").alias("price_sequence")) \
    .filter(size(col("price_sequence")) == 32)

# Process pred_data (Prediction)
def process_pred_data(batch_df, _):
    # Chuyển đổi sang DataFrame pandas
    pandas_df = batch_df.toPandas()
    
    # Tách cột window thành start và end
    pandas_df['window_start'] = pandas_df['window'].apply(lambda x: x['start'])
    pandas_df['window_end'] = pandas_df['window'].apply(lambda x: x['end'])
    
    X = pandas_df["price_sequence"].tolist()
    
    # Dự đoán sử dụng mô hình đã huấn luyện
    predictions = [model.predict([seq])[0] for seq in X]  # Assuming model.predict() returns a list of predictions
    
    pandas_df["predicted_close"] = predictions

    # Thêm 1 phút vào thời gian kết thúc của cửa sổ
    pandas_df["timestamp"] = pandas_df["window_end"] + pd.Timedelta(seconds=60)  # Thêm 1 phút vào thời gian kết thúc

    pred_data = pandas_df[["timestamp", "predicted_close"]]

    # Kiểm tra xem dữ liệu có trống không trước khi tạo PySpark DataFrame
    if not pred_data.empty:
        # Chuyển pandas DataFrame thành PySpark DataFrame
        print("======================================================  3  ====================================================================")
        spark_pred_data = spark.createDataFrame(pred_data)
        
        # Ghi pred_data vào HBase bằng PySpark DataFrame
        spark_pred_data.write \
            .format("org.apache.phoenix.spark") \
            .option("phoenix.url", "jdbc:phoenix:hbase:2181") \
            .option("table", "pred_stream") \
            .mode("append") \
            .save()
    else:
        print("Dữ liệu trống, không thể ghi vào HBase")


# Write predicted data to HBase
df_windowed.writeStream \
    .foreachBatch(process_pred_data) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
