from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

start_date=datetime(2024, 11, 1)

year = None
month = None

with DAG("refresh_model", start_date=start_date, schedule_interval="0 0 1 * *", catchup=True) as dag:
    train = BashOperator(
        task_id='train',
        bash_command=f"docker exec batch_job python3 /app/train_model.py {year} {month}"
    )
    
    train
