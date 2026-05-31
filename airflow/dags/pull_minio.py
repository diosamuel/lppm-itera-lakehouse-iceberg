from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_print():
    print("Hello Airflow 2.9.1 👋 DAG is working!")

with DAG(
    dag_id="test_print_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # Airflow 2.9+ prefers schedule instead of schedule_interval
    catchup=False,
    tags=["test"]
) as dag:

    print_task = PythonOperator(
        task_id="print_hello",
        python_callable=test_print
    )