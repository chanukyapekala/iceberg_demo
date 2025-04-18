from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Define Python callables for each task
def run_load_sample_data():
    subprocess.run(["make", "run-load-sample-data"], check=True)

def run_ml():
    subprocess.run(["make", "run-ml-issue"], check=True)

def run_genai():
    subprocess.run(["make", "run-gen_ai_issue"], check=True)

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
        python_callable=run_ml,
    )

    genai_task = PythonOperator(
        task_id="run_genai",
        python_callable=run_genai,
    )

    # Set task dependencies
    load_sample_data_task >> ml_task >> genai_task