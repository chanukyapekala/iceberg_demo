from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Define Python callables for each task
def run_load_sample_data():
    subprocess.run(["make", "run-load-sample-data"], check=True)

def run_linear_regression():
    subprocess.run(["make", "run-linear-regression"], check=True)

# Define the DAG
with DAG(
        dag_id="ml_genai_writer_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    # Define tasks
    load_sample_data_task = PythonOperator(
        task_id="run_load_sample_data",
        python_callable=run_load_sample_data,
    )

    ml_task = PythonOperator(
        task_id="ml_issue",
        python_callable=run_linear_regression,
    )

    # Set task dependencies
    load_sample_data_task >> ml_task