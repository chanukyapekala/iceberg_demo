from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_structured_streaming():
    subprocess.run(["make", "run-structured-streaming"], check=True)

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
    structured_streaming_task = PythonOperator(
        task_id="run_structured_streaming",
        python_callable=run_structured_streaming,
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
    producer_task >> structured_streaming_task >> consumer_task