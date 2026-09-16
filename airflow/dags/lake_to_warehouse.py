import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

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
    log.info("Loading manifest s3://%s/%s", BUCKET, MANIFEST_KEY)
    try:
        manifest = json.loads(hook.read_key(bucket_name=BUCKET, key=MANIFEST_KEY))
        log.info("Manifest loaded: %d file(s)", len(manifest))
        return manifest
    except Exception:
        log.warning("Manifest not found, starting empty", exc_info=True)
        return {}

@task
def fetchCurrentETag():
    hook = S3Hook(aws_conn_id="minio_s3")
    client = hook.get_conn()
    log.info("Fetching ETags for %d raw file(s)", len(RAW_FILES))
    out = {}
    for key in RAW_FILES:
        if not hook.check_for_key(key["file"], bucket_name=BUCKET):
            log.warning("Missing raw file: %s", key["file"])
            continue
        head = client.head_object(Bucket=BUCKET, Key=key["file"])
        out[key["file"]] = {
            "etag": head["ETag"].strip('"'),
            "last_checked": datetime.now(timezone.utc).isoformat(),
            "category": key["category"]
        }
        log.info("%s: etag=%s", key["file"], out[key["file"]]["etag"])
    log.info("ETags fetched: %d/%d", len(out), len(RAW_FILES))
    return out

@task
def checkDiffManifest(prev, curr):
    log.info("Diffing manifests: prev=%d current=%d", len(prev), len(curr))
    result = {}
    for key, info in curr.items():
        info["changed"] = info["etag"] != prev.get(key, {}).get("etag")
        result[key] = info
    changed_files = sorted(k for k, i in result.items() if i["changed"])
    log.info("Diff: %d changed file(s): %s", len(changed_files), changed_files)
    return result

@task
def getChangedCategories(diff):
    changed = sorted({info["category"] for info in diff.values() if info.get("changed")})
    log.info("Changed categories: %s", changed)
    return changed

@task.branch
def isRunPipeline(changed_categories):
    if changed_categories:
        log.info("Branch: runPipeline")
        return "runPipeline"
    log.info("Branch: noFileChanged")
    return "noFileChanged"

@task
def writeNewManifest(diff):
    log.info("Writing manifest: %d file(s)", len(diff))
    S3Hook(aws_conn_id="minio_s3").load_string(
        json.dumps(diff),
        key=MANIFEST_KEY,
        bucket_name=BUCKET,
        replace=True
    )
    log.info("Manifest written")

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
    changed = getChangedCategories(diff)
    branch = isRunPipeline(changed)

    runPipeline = BashOperator(
        task_id="runPipeline",
        bash_command=(
            "docker exec lppm-spark-iceberg spark-submit --deploy-mode client "
            "/home/iceberg/pipeline/run.py "
            "{{ ti.xcom_pull(task_ids='getChangedCategories') | join(' ') }}"
        ),
    )
    noFileChanged = EmptyOperator(task_id="noFileChanged")
    writeManifest = writeNewManifest(diff)

    branch >> [runPipeline, noFileChanged]
    runPipeline >> writeManifest

lakeToWarehouse()
