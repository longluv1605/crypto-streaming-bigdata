from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
# from xgboost.spark import SparkXGBRegressor
from pyspark.ml.regression import GBTRegressor
from datetime import datetime
import sys


# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("XGBoost in PySpark") \
    .getOrCreate()


# 2. Đọc dữ liệu từ HDFS
HDFS_URL = "hdfs://hadoop-namenode:8020"
HDFS_WAREHOUSE = "/crypto/bitcoin/warehouse"

with open('/app/date.txt', 'r') as f:
    timestamp = f.readline()
f.close()

if sys.argv[1] is not None and sys.argv[2] is not None:
    year = sys.argv[1]
    month = sys.argv[2]
else:    
    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    year = dt.year
    month = dt.month


file_path = f"{HDFS_URL}/{HDFS_WAREHOUSE}/{year}/{month}/*.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)


# 3. Chuẩn bị dữ liệu
# Chuyển đổi các cột đặc trưng thành vector
feature_columns = ['price_avg', 'price_change', 'moving_avg', 'volatility']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df = assembler.transform(df)

# Đặt cột "future_close" làm mục tiêu
df = df.select("features", "future_close")


# 4. Chia tập dữ liệu thành train và test
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)


# 5. Huấn luyện mô hình XGBoost
# xgb = SparkXGBRegressor(objective="reg:squarederror", 
#                         maxDepth=5, 
#                         eta=0.1, 
#                         numRound=100, 
#                         featuresCol="features", 
#                         labelCol="future_close")

gbt = GBTRegressor(featuresCol="features", labelCol="future_close", maxIter=100)
model = gbt.fit(train_data)

# 6. Dự đoán trên tập kiểm tra
predictions = model.transform(test_data)

# 7. Đánh giá mô hình
evaluator = RegressionEvaluator(labelCol="future_close", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE): {rmse}")

# 8. Lưu mô hình
model.write().overwrite().save(f"{HDFS_URL}/crypto/bitcoin/models/gbt_bitcoin_model")
