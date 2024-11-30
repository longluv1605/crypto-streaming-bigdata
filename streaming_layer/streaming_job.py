import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, collect_list, size, unix_timestamp, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

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
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "bitcoin-topic") \
    .load()

# Deserialize and preprocess the data
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.timestamp", "data.close") \
    .withColumn("timestamp", unix_timestamp(col("timestamp")).cast("long"))

# Process real_data immediately
def process_real_data(batch_df, _):
    # Write real_data to HBase
    batch_df.write \
        .format("org.apache.phoenix.spark") \
        .option("phoenix.url", "jdbc:phoenix:hbase:2181") \
        .option("table", "real_stream") \
        .mode("append") \
        .save()

df.writeStream \
    .foreachBatch(process_real_data) \
    .outputMode("append") \
    .start()

# Prepare pred_data (with 32 records in window)
window_spec = Window.orderBy("timestamp").rowsBetween(-31, 0)
df_windowed = df.withColumn("price_sequence", collect_list("close").over(window_spec)) \
    .filter(size(col("price_sequence")) == 32)

# Process pred_data
def process_pred_data(batch_df, _):
    pandas_df = batch_df.toPandas()
    X = pandas_df["price_sequence"].tolist()
    predictions = [model.predict([seq]) for seq in X]
    pandas_df["predicted_close"] = predictions

    # Add 1 minute to the latest timestamp for pred_data
    pandas_df["timestamp"] = pandas_df["timestamp"] + 60

    pred_data = pandas_df[["timestamp", "predicted_close"]]

    # Write pred_data to HBase
    pred_data.write \
        .format("org.apache.phoenix.spark") \
        .option("phoenix.url", "jdbc:phoenix:hbase:2181") \
        .option("table", "pred_stream") \
        .mode("append") \
        .save()

df_windowed.writeStream \
    .foreachBatch(process_pred_data) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
