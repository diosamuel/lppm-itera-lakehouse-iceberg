from datetime import datetime

from airflow.operators.python import PythonOperator

from airflow import DAG


def test_print():
    print("hello hello hello")


with DAG(
    dag_id="testtest",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Airflow 2.9+ prefers schedule instead of schedule_interval
    catchup=False,
    tags=["test"],
) as dag:
    print_task = PythonOperator(task_id="print_hello", python_callable=test_print)
