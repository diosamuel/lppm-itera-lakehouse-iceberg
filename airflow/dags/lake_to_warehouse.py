from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json

RAW_FILES = [
    {
        "file":"raw_data_penelitian.xlsx",
        "category":"penelitian",
    },{
        "file":"raw_data_pengabdian.xlsx",
        "category":"pengabdian"
    },
    {
        "file":"raw_data_buku_keilmuan.xlsx",
        "category":"buku_keilmuan",
    },
    {
        "file":"raw_data_sitasi.xlsx",
        "category":"sitasi"
    }
]

BUCKET = "sipaper"
MANIFEST_KEY = "_manifest/etag.json"

@task
def loadCurrentManifest():
    hook = S3Hook(aws_conn_id="minio_s3")
    try:
        body = hook.read_key(bucket_name=BUCKET,key=MANIFEST_KEY)
        return json.loads(body)
    except Exception:
        return {}

@task
def fetchCurrentETag():
    hook = S3Hook(aws_conn_id="minio_s3")
    client = hook.get_conn()
    out = {}
    for key in RAW_FILES:
        if not hook.check_for_key(key["file"],bucket_name=BUCKET):
            continue
        head = client.head_object(Bucket=BUCKET,Key=key["file"])
        out[key["file"]] = {
            "etag":head["ETag"].strip('"'),
            "last_checked":datetime.utcnow().isoformat() + "Z",
            "category":key["category"]
        }
    return out

@task
def checkDiffManifest(prev,curr):
    # compare previous etag vs current etag
    result = {}
    for key,info in curr.items():
        old_etag = prev.get(key, {}).get("etag")
        info["changed"] = info["etag"] != old_etag
        result[key] = info
    return result

@task.branch
def isRunPipeline(diff):
    for val in diff.values():
        if val["changed"]:
            return "transformData"
    return "noFileChanged"

@task
def writeNewManifest(diff):
    S3Hook(aws_conn_id="minio_s3").load_string(
        json.dumps(diff),
        key=MANIFEST_KEY,
        bucket_name=BUCKET,
        replace=True
    )

@dag(
    dag_id="lake_to_warehouse",
    start_date=datetime(2026,1,1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries":1,
        "retry_delay":timedelta(minutes=5)
    }
)
def lakeToWarehouse():
    prev = loadCurrentManifest()
    current = fetchCurrentETag()
    diff = checkDiffManifest(prev, current)
    branch = isRunPipeline(diff)

    transformData = BashOperator(
        task_id="transformData",
        bash_command="docker exec lppm-spark-iceberg spark-submit --deploy-mode client /home/iceberg/pipeline/index.py",
    )
    noFileChanged = EmptyOperator(task_id="noFileChanged")
    writeManifest = writeNewManifest(diff)

    branch >> [transformData, noFileChanged]
    transformData >> writeManifest

lakeToWarehouse()
