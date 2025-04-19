from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_basic():
    import subprocess
    subprocess.run(["make", "run-basic"], check=True)

def run_producer():
    import subprocess
    subprocess.run(["make", "run-producer"], check=True)

def run_consumer():
    import subprocess
    subprocess.run(["make", "run-consumer"], check=True)

with DAG(
        dag_id="stream_writer_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    basic_task = PythonOperator(
        task_id="run_basic",
        python_callable=run_basic,
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
    basic_task >> producer_task >> consumer_task