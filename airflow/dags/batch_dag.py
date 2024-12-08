from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
# from airflow.utils.trigger_rule import TriggerRule # type: ignore
from datetime import datetime

start_date = datetime(2024, 12, 1)

with DAG("batch_dag", start_date=start_date, schedule_interval="0 0 * * *", catchup=True) as dag:
    # Task requesting
    requesting = BashOperator(
        task_id="requesting",
        bash_command="docker exec batch-job python3 /app/batch_consumer.py",
    )

    # Task ingestion, chạy ngay cả khi requesting chưa hoàn thành
    ingestion = BashOperator(
        task_id="ingestion",
        bash_command="sleep 5 && docker exec ingestion python /app/batch_producer.py"
    )

    # Task processing
    processing = BashOperator(
        task_id="processing",
        bash_command="docker exec batch-job python3 /app/process.py",
    )

    # Định nghĩa workflow
    ingestion >> requesting >> processing
