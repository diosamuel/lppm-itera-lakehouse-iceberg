import sys
from pathlib import Path

from bronze import CATEGORIES, ingest_category
from index import run_silver_gold
from setup.setup_catalog import SetupIcebergCatalog
from setup.setup_minio import SetupMinioS3
from setup.setup_spark import SetupSpark

BASE_DIR = Path(__file__).resolve().parent

if __name__ == "__main__":
    StorageS3 = SetupMinioS3(
        endpoint_url="http://minio:9000",
        bucket="sipaper",
    ).initialize()
    IcebergCatalog = SetupIcebergCatalog(
        catalog_name="default",
        namespace="silver",
    ).initialize()
    IcebergCatalog.create_namespace("bronze")
    IcebergCatalog.create_namespace("gold")
    SparkSession = SetupSpark(
        app_name="sipaper-pipeline",
        catalog_name="default",
    ).initialize()

    changed = sys.argv[1:] if len(sys.argv) > 1 else list(CATEGORIES.keys())
    unknown = [c for c in changed if c not in CATEGORIES]
    if unknown:
        print(f"[pipeline] unknown categories: {unknown}; valid: {list(CATEGORIES)}")
        sys.exit(1)

    bronze_cache = {}
    for category in changed:
        cached = ingest_category(SparkSession, category, CATEGORIES[category], StorageS3.client)
        if cached is not None:
            bronze_cache[category] = cached

    run_silver_gold(SparkSession, bronze_cache=bronze_cache, categories=changed)

    for df in bronze_cache.values():
        df.unpersist()
