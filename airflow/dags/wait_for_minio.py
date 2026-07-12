from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from datetime import datetime, timedelta


@task.sensor(poke_interval=60, timeout=60 * 60 * 24, mode="reschedule")
def wait_file():
    hook = S3Hook(aws_conn_id="minio_s3")
    client = hook.get_conn()
    try:
        client.head_object(Bucket="sipaper", Key="sipaper.xlsx")
        return "sipaper.xlsx"
    except Exception:
        return False


@task
def process(s3_key: str):
    hook = S3Hook(aws_conn_id="minio_s3")
    client = hook.get_conn()
    response = client.head_object(Bucket="sipaper", Key=s3_key)
    metadata = {
        "size": response.get("ContentLength"),
        "content_type": response.get("ContentType"),
        "last_modified": str(response.get("LastModified")),
        "etag": response.get("ETag"),
    }
    print(f"File metadata: {metadata}")
    return metadata


@dag(
    dag_id="process_sipaper",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=6)},
)
def process_sipaper_dag():
    s3_key = wait_file()
    process(s3_key)


process_sipaper_dag()
