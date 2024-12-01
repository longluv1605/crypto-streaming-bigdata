#!/bin/bash

# Attempt to run the first command
echo "Attempting to run primary spark-submit command..."
spark-submit --master spark://spark-master:7077 /app/streaming_job.py

# Check if the first command failed
if [ $? -ne 0 ]; then
  echo "Primary spark-submit failed, falling back to secondary command..."
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --master spark://spark-master:7077 /app/streaming_job.py
else
  echo "Primary spark-submit succeeded."
fi
