import io
import os
import pyarrow as pa
import pyarrow.csv as pa_csv
from extract_transform import Transform
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.types import IntegerType
from pyspark.sql import functions as F
from schema.listSchema import default_schema, default_schema_enrichment, sitasi_schema
from setup_catalog import SetupIcebergCatalog
from setup_minio import SetupMinioS3
from setup_spark import SetupSpark
from write_audit_publish import WAPWorkflow

# Initialize storage and catalog
StorageS3 = SetupMinioS3(
    endpoint_url="http://minio:9000",
    bucket="sipaper",
).initialize()

# Upload PDF documents from data/pdf/penelitian and data/pdf/pengabdian
PDF_BASE_DIR = "/home/iceberg/notebooks/data/pdf"
YEARS = ["2021", "2022", "2023", "2024", "2025"]

for research_type in ["penelitian", "pengabdian"]:
    type_dir = os.path.join(PDF_BASE_DIR, research_type)
    if not os.path.isdir(type_dir):
        continue
    for subfolder in os.listdir(type_dir):
        subfolder_path = os.path.join(type_dir, subfolder)
        if not os.path.isdir(subfolder_path):
            continue
        for filename in os.listdir(subfolder_path):
            filepath = os.path.join(subfolder_path, filename)
            if not os.path.isfile(filepath):
                continue
            for year in YEARS:
                s3_key = f"pdf/{research_type}/{year}/{subfolder}/{filename}"
                result = StorageS3.upload(filename=s3_key, filepath=filepath)
                if result.get("status") == "success":
                    print(f"Uploaded: s3://sipaper/{s3_key}")
                else:
                    print(f"Skipped: s3://sipaper/{s3_key} ({result.get('message')})")

IcebergCatalog = SetupIcebergCatalog(
    catalog_name="default",
    namespace="default",
).initialize()

SparkSession = SetupSpark(
    app_name="sipaper",
    catalog_name="default",
).initialize()

list_directory = os.listdir("/home/iceberg/notebooks/data")

for file in list_directory:
    if file.endswith(".csv"):
        research_type = file.rsplit("_", 1)[0]
        year = file.rsplit("_", 1)[1].split(".")[0]
        file_endpoint = None
        if research_type == "penelitian":
            file_endpoint = f"/csv/penelitian/{year}/{file}"
        elif research_type == "pengabdian":
            file_endpoint = f"/csv/pengabdian/{year}/{file}"
        elif research_type == "buku_keilmuan":
            file_endpoint = f"/csv/buku_keilmuan/{year}/{file}"
        elif research_type == "sitasi":
            file_endpoint = f"/csv/sitasi/{year}/{file}"

        if file_endpoint is None:
            print(f"Skipping unrecognised file: {file}")
            continue

        StorageS3.upload(
            filename=file_endpoint,
            filepath=f"/home/iceberg/notebooks/data/{file}",
        )

buku_keilmuan = IcebergCatalog.create_table("buku_keilmuan", default_schema)
penelitian = IcebergCatalog.create_table("penelitian", default_schema)
pengabdian = IcebergCatalog.create_table("pengabdian", default_schema)
sitasi = IcebergCatalog.create_table("sitasi", sitasi_schema)

# Penelitian
[
    csv_penelitian_2021,
    csv_penelitian_2022,
    csv_penelitian_2023,
    csv_penelitian_2024,
    csv_penelitian_2025,
] = [
    StorageS3.load("csv/penelitian/2021/penelitian_2021.csv"),
    StorageS3.load("csv/penelitian/2022/penelitian_2022.csv"),
    StorageS3.load("csv/penelitian/2023/penelitian_2023.csv"),
    StorageS3.load("csv/penelitian/2024/penelitian_2024.csv"),
    StorageS3.load("csv/penelitian/2025/penelitian_2025.csv"),
]
# print(csv_penelitian_2021["content"])

res = (
    Transform(spark=SparkSession, document_type="penelitian")
    .processData(csv_penelitian_2021["content"], 2021)
    .processData(csv_penelitian_2022["content"], 2022)
    .processData(csv_penelitian_2023["content"], 2023)
    .processData(csv_penelitian_2024["content"], 2024)
    .processData(csv_penelitian_2025["content"], 2025)
    .join()
)
res.writeTo("default.default.penelitian").createOrReplace()

# Pengabdian
[
    csv_pengabdian_2021,
    csv_pengabdian_2022,
    csv_pengabdian_2023,
    csv_pengabdian_2024,
    csv_pengabdian_2025,
] = [
    StorageS3.load("csv/pengabdian/2021/pengabdian_2021.csv"),
    StorageS3.load("csv/pengabdian/2022/pengabdian_2022.csv"),
    StorageS3.load("csv/pengabdian/2023/pengabdian_2023.csv"),
    StorageS3.load("csv/pengabdian/2024/pengabdian_2024.csv"),
    StorageS3.load("csv/pengabdian/2025/pengabdian_2025.csv"),
]

res = (
    (
        Transform(spark=SparkSession, document_type="pengabdian")
        .processData(csv_pengabdian_2021["content"], 2021)
        .processData(csv_pengabdian_2022["content"], 2022)
        .processData(csv_pengabdian_2023["content"], 2023)
        .processData(csv_pengabdian_2024["content"], 2024)
        .processData(csv_pengabdian_2025["content"], 2025)
        .join()
    )
    .writeTo("default.default.pengabdian")
    .createOrReplace()
)


# Buku Keilmuan
[csv_buku_keilmuan_2023, csv_buku_keilmuan_2024] = [
    StorageS3.load("csv/buku_keilmuan/2023/buku_keilmuan_2023.csv"),
    StorageS3.load("csv/buku_keilmuan/2024/buku_keilmuan_2024.csv"),
]

res = (
    (
        Transform(spark=SparkSession, document_type="buku_keilmuan")
        .processData(csv_buku_keilmuan_2023["content"], 2023)
        .processData(csv_buku_keilmuan_2024["content"], 2024)
        .join()
    )
    .writeTo("default.default.buku_keilmuan")
    .createOrReplace()
)

# Sitasi
[csv_sitasi_2026] = [
    StorageS3.load("csv/sitasi/2026/sitasi_2026.csv"),
]

res = (
    (
        Transform(spark=SparkSession, document_type="sitasi")
        .processSitasiData(csv_sitasi_2026["content"], 2026)
        .join()
    )
    .writeTo("default.default.sitasi")
    .createOrReplace()
)
