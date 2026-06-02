import os

from schema.listSchema import default_schema, default_schema_enrichment, sitasi_schema
from setup_catalog import SetupIcebergCatalog
from setup_minio import SetupMinioS3
from setup_spark import SetupSpark
from write_audit_publish import WAPWorkflow

# Initialize storage and catalog
storage = SetupMinioS3(
    endpoint_url="http://minio:9000",
    bucket="sipaper",
).initialize()

IcebergCatalog = SetupIcebergCatalog(
    catalog_name="default",
    namespace="default",
).initialize()

# spark = SetupSpark(
#     appname="sipaper",
#     catalog=catalog,
# ).initialize()

list_directory = os.listdir("/home/iceberg/notebooks/data")

# for file in list_directory:
#     if file.endswith(".csv"):
#         research_type = file.rsplit("_", 1)[0]
#         year = file.rsplit("_", 1)[1].split(".")[0]
#         print(research_type)
#         file_endpoint = None
#         if research_type == "penelitian":
#             file_endpoint = f"/csv/penelitian/{year}/{file}"
#         elif research_type == "pengabdian":
#             file_endpoint = f"/csv/pengabdian/{year}/{file}"
#         elif research_type == "buku_keilmuan":
#             file_endpoint = f"/csv/buku_keilmuan/{year}/{file}"
#         elif research_type == "sitasi":
#             file_endpoint = f"/csv/sitasi/{year}/{file}"

#         if file_endpoint is None:
#             print(f"Skipping unrecognised file: {file}")
#             continue

#         storage.upload(
#             filename=file_endpoint,
#             filepath=f"/home/iceberg/notebooks/data/{file}",
#         )

# print(storage.list_file())
# IcebergCatalog.create_namespace()
print(IcebergCatalog.catalog.list_namespaces())
# catalog.get_table("sipaper")
# ── Pipeline placeholders ─────────────────────────────────────────────────── #

# listdir the csv, upload as is to Minio on /sipaper/csv path
# listdir the pdf too
# storage.upload(
#     filename="/csv/buku_keilmuan/2023/buku_keilmuan_2023.csv",
#     filepath="./data/buku_keilmuan_2023.csv",
# )
# do WAP on the minio
# ====WAP HERE========
# save to iceberg
# ====ICEBERG HERE=======
# do some query
# ====QUERY HERE========


# print(SetupSpark)
