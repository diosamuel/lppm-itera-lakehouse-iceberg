from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
def process():
    hook = S3Hook(aws_conn_id="minio_s3")
    client = hook.get_conn()
    response = client.head_object(Bucket="sipaper", Key="sipaper.xlsx")
    metadata = {
        "size": response.get("ContentLength"),
        "content_type": response.get("ContentType"),
        "last_modified": str(response.get("LastModified")),
        "etag": response.get("ETag"),
    }
    print(f"File metadata: {metadata}")

with DAG(
    dag_id="process_sipaper",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    max_active_runs=1
):
    wait_file = S3KeySensor(
        task_id="wait_file",
        bucket_name="sipaper",
        bucket_key="sipaper.xlsx",
        aws_conn_id="minio_s3",
        poke_interval=60,
        timeout=180,
        mode="poke"
    )

    process_task = PythonOperator(
        task_id="process",
        python_callable=process
    )

    wait_file >> process_task
