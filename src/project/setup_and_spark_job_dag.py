from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello from Airflow!")

with DAG(
        dag_id='setup_and_spark_job_dag',
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    start = PythonOperator(
        task_id='say_hello',
        python_callable=hello_world
    )
