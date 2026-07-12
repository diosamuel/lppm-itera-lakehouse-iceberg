from pprint import pprint

import pandas as pd

from setup_minio import SetupMinioS3

BUCKET = "sipaper"
FOLDERS = ("buku_keilmuan", "penelitian", "pengabdian", "sitasi")

minio = SetupMinioS3(endpoint_url="http://minio:9000", bucket=BUCKET).initialize()


def preview_csv(key):
    body = minio.client.get_object(Bucket=BUCKET, Key=key)["Body"]
    df = pd.read_csv(body, nrows=5)
    # print(f"\n--- Top 5 rows of {key} ---")
    # print(df.to_string(index=False))


for folder in FOLDERS:
    for obj in minio.client.list_objects_v2(Bucket=BUCKET, Prefix=folder).get("Contents", []):
        key = obj["Key"]
        if not key.endswith((".csv", ".pdf")) or any(p.startswith("Template") for p in key.split("/")):
            continue
        head = minio.client.head_object(Bucket=BUCKET, Key=key)
        pprint({
            "key": key,
            "content_type": head.get("ContentType"),
            "size": head.get("ContentLength"),
            "last_modified": head.get("LastModified"),
            "etag": head.get("ETag"),
            "metadata": head.get("Metadata", {}),
        })
        if key.endswith(".csv"):
            preview_csv(key)
