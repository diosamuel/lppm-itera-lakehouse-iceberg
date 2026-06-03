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

# from tools.utils_spark import (
#     get_faculty_udf,
#     get_prodi_udf,
#     map_faculty_degree_udf,
#     match_name_udf,
#     match_unique_id_udf,
# )
from write_audit_publish import WAPWorkflow

# Initialize storage and catalog
StorageS3 = SetupMinioS3(
    endpoint_url="http://minio:9000",
    bucket="sipaper",
).initialize()

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
        print(research_type)
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

# Peneilitian
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
    Transform(spark=SparkSession, document_type="pengabdian")
    .processData(csv_pengabdian_2021["content"], 2021)
    .processData(csv_pengabdian_2022["content"], 2022)
    .processData(csv_pengabdian_2023["content"], 2023)
    .processData(csv_pengabdian_2024["content"], 2024)
    .processData(csv_pengabdian_2025["content"], 2025)
    .join()
)
res.writeTo("default.default.pengabdian").createOrReplace()
