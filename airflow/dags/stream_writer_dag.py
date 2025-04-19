from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_load_sample_data():
    subprocess.run(["make", "run-load-sample-data"], check=True)

def run_producer():
    subprocess.run(["make", "run-producer"], check=True)

def run_consumer():
    subprocess.run(["make", "run-consumer"], check=True)

with DAG(
        dag_id="stream_writer_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    load_sample_data_task = PythonOperator(
        task_id="run_load_sample_data",
        python_callable=run_load_sample_data,
    )

    producer_task = PythonOperator(
        task_id="run_producer",
        python_callable=run_producer,
    )

    consumer_task = PythonOperator(
        task_id="run_consumer",
        python_callable=run_consumer,
    )

    # Set task dependencies
    load_sample_data_task >> producer_task >> consumer_task