from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, unix_timestamp, lead
from pyspark.sql.window import Window
from datetime import datetime


HDFS_URL = "hdfs://hadoop-namenode:8020"
HDFS_DATALAKE = "/crypto/bitcoin/datalake"
HDFS_WAREHOUSE = "/crypto/bitcoin/warehouse"

mysql_jar_path = "/spark/jars/mysql-connector-java-8.0.28.jar"

spark = SparkSession.builder \
    .appName("Bitcoin Batch Processing") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \
    .config("spark.jars", mysql_jar_path) \
    .config("spark.driver.extraClassPath", mysql_jar_path) \
    .config("spark.executor.extraClassPath", mysql_jar_path) \
    .getOrCreate()
    
    
def process_to_warehouse():
    
    with open('/app/date.txt', 'r') as f:
        timestamp = f.readline()
    f.close()
    
    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    year = dt.year
    month = dt.month
    day = dt.day

    print("------------------------- Created time ------------------")
    
    data_path = f"{HDFS_URL}/{HDFS_DATALAKE}/{year}/{month}/data_{day}.csv"
    print(data_path)
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    print("------------------------- Read data ------------------")

    
    df_cleaned = df.na.drop() 
    df_cleaned = df_cleaned.dropDuplicates(['timestamp'])
    # df_cleaned = df_cleaned.withColumn("timestamp", unix_timestamp(col("timestamp")))
    
    print("------------------------- Cleaned data ------------------")

    window_spec = Window.orderBy("timestamp")

    df_features = df_cleaned \
        .withColumn("price_avg", (col("high") + col("low")) / 2) \
        .withColumn("price_change", col("close") - col("open")) \
        .withColumn("moving_avg", avg("close").over(window_spec.rowsBetween(-5, 0))) \
        .withColumn("volatility", stddev("close").over(window_spec.rowsBetween(-5, 0)))
        
    print("------------------------- Created features ------------------")
    

    df_features = df_features.withColumn("future_close", lead("close").over(window_spec))

    df_features = df_features.na.drop()
    
    print("------------------------- Created future close col ------------------")
    

    save_path = f"{HDFS_URL}/{HDFS_WAREHOUSE}/{year}/{month}/data_{day}.csv"
    df_features.write.mode("overwrite").csv(save_path, header=True)
    
    print("------------------------- Saved to warehouse ------------------")
    

    write_to_database(df_features)

def write_to_database(df_features):
    df_features.write.format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/crypto") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "bitcoin_processed") \
        .option("user", "admin") \
        .option("password", "admin") \
        .mode("append") \
        .save()
    print("Inserted data in to MySQL")

process_to_warehouse()
