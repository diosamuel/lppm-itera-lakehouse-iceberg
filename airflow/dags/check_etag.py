from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
def loadPrevManifest():
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
def diffManifest(prev,curr):
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
            return "runLoadData"
    return "skip"

@task
def writeManifest(diff):
    S3Hook(aws_conn_id="minio_s3").load_string(
        json.dumps(diff),
        key=MANIFEST_KEY,
        bucket_name=BUCKET,
        replace=True
    )
    return diff

@task
def runLoadData(diff):
    changed = []
    for key, value in diff.items():
        if value["changed"]:
            changed.append(key)

    print(diff)
    print("--file changed--")
    print(changed)
    # process by file changed

@dag(
    dag_id="check_raw_etag",
    start_date = datetime(2026,1,1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries":1,
        "retry_delay":timedelta(minutes=5)
    }
)
def checkRawETagDag():
    prev = loadPrevManifest()
    current = fetchCurrentETag()
    diff = diffManifest(prev,current)

    branch = isRunPipeline(diff)
    written = writeManifest(diff)

    run = runLoadData(written)
    skip = EmptyOperator(task_id="skip")

    branch >> [run,skip]

checkRawETagDag()
