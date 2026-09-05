import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from setup.setup_catalog import SetupIcebergCatalog
from setup.setup_minio import SetupMinioS3
from setup.setup_spark import SetupSpark
from transform.xlsx_clean import clean_xlsx_sheet, list_xlsx_year_sheets

BASE_DIR = Path(__file__).resolve().parent

CATEGORIES = {
    "penelitian": os.getenv("RAW_XLSX_PATH", "s3a://sipaper/raw_data_penelitian.xlsx"),
    "pengabdian": os.getenv("RAW_PENGABDIAN_XLSX_PATH", "s3a://sipaper/raw_data_pengabdian.xlsx"),
    "buku_keilmuan": os.getenv("RAW_BUKU_KEILMUAN_XLSX_PATH", "s3a://sipaper/raw_data_buku_keilmuan.xlsx"),
    "sitasi": os.getenv("RAW_SITASI_XLSX_PATH", "s3a://sipaper/raw_data_sitasi.xlsx"),
}


def ingest_category(spark, category, xlsx_path, s3_client):
    sheets = list_xlsx_year_sheets(xlsx_path, s3_client)
    print(f"[bronze] {category}: year sheets = {sheets}")
    if not sheets:
        print(f"[bronze] {category}: no year sheets found, skipping")
        return

    batches = []
    for sheet in sheets:
        df = clean_xlsx_sheet(spark, xlsx_path, sheet).withColumn(
            "tahun", F.lit(int(sheet)).cast("int")
        )
        batches.append(df)

    united = batches[0]
    for batch in batches[1:]:
        united = united.unionByName(batch, allowMissingColumns=True)

    united = united.cache()
    row_count = united.count()
    united.writeTo(f"bronze.{category}").createOrReplace()
    print(f"[bronze] written bronze.{category} (rows={row_count})")
    return united


if __name__ == "__main__":
    StorageS3 = SetupMinioS3(
        endpoint_url="http://minio:9000",
        bucket="sipaper",
    ).initialize()
    SetupIcebergCatalog(
        catalog_name="default",
        namespace="bronze",
    ).initialize()
    SparkSession = SetupSpark(
        app_name="sipaper-bronze",
        catalog_name="default",
    ).initialize()

    targets = sys.argv[1:] if len(sys.argv) > 1 else list(CATEGORIES.keys())
    unknown = [c for c in targets if c not in CATEGORIES]
    if unknown:
        print(f"[bronze] unknown categories: {unknown}; valid: {list(CATEGORIES)}")
        sys.exit(1)
    for category in targets:
        ingest_category(SparkSession, category, CATEGORIES[category], StorageS3.client)
