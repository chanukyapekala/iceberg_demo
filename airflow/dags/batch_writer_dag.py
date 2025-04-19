from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Define Python callables for each task
def run_basic():
    subprocess.run(["make", "run-basic"], check=True)

def run_schema_evolution():
    subprocess.run(["make", "run-schema-evolution"], check=True)

def run_partitioning():
    subprocess.run(["make", "run-partitioning"], check=True)

def run_scd_type_2():
    subprocess.run(["make", "run-scd-type-2"], check=True)

# Define the DAG
with DAG(
        dag_id="batch_writer_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    # Define tasks
    basic_task = PythonOperator(
        task_id="run_basic",
        python_callable=run_basic,
    )

    schema_evolution_task = PythonOperator(
        task_id="run_schema_evolution",
        python_callable=run_schema_evolution,
    )

    partitioning_task = PythonOperator(
        task_id="run_partitioning",
        python_callable=run_partitioning,
    )

    scd_type_2_task = PythonOperator(
        task_id="run_scd_type_2",
        python_callable=run_scd_type_2,
    )

    # Set task dependencies
    basic_task >> schema_evolution_task >> partitioning_task >> scd_type_2_task