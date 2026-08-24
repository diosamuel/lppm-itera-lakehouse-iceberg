import os
from pathlib import Path

from pyspark.sql import functions as F

from tools.extract_transform import Transform
from tools.dosen_name_mapper import map_dosen_name_udf
from setup_catalog import SetupIcebergCatalog
from setup_minio import SetupMinioS3
from setup_spark import SetupSpark


BASE_DIR = Path(__file__).resolve().parent


def run_sql_file(spark, sql_file):
    sql_text = Path(sql_file).read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    for statement in statements:
        spark.sql(statement)


# Initialize storage
StorageS3 = SetupMinioS3(
    endpoint_url="http://minio:9000",
    bucket="sipaper",
).initialize()

# Upload PDF documents
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
                s3_key = f"{research_type}/{year}/pdf/{subfolder}/{filename}"
                result = StorageS3.upload(filename=s3_key, filepath=filepath)
                if result.get("status") == "success":
                    print(f"Uploaded: s3://sipaper/{s3_key}")
                else:
                    print(f"Skipped: s3://sipaper/{s3_key} ({result.get('message')})")

# Initialize PDF folder structure
DOC_TYPES = ["laporan_akhir", "laporan_kemajuan", "proposal"]

for research_type in ["penelitian", "pengabdian"]:
    for year in YEARS:
        for doc_type in DOC_TYPES:
            folder_key = f"{research_type}/{year}/pdf/{doc_type}/"
            StorageS3.client.put_object(Bucket="sipaper", Key=folder_key, Body=b"")
            print(f"Created folder: s3://sipaper/{folder_key}")

# Initialize catalog and spark
IcebergCatalog = SetupIcebergCatalog(
    catalog_name="default",
    namespace="silver",
).initialize()

SparkSession = SetupSpark(
    app_name="sipaper",
    catalog_name="default",
).initialize()

# Upload CSV files
list_directory = os.listdir("/home/iceberg/notebooks/data")
for file in list_directory:
    if file.endswith(".csv"):
        print(file)
        research_type = file.rsplit("_", 1)[0]
        year = file.rsplit("_", 1)[1].split(".")[0]
        file_endpoint = None
        if research_type == "penelitian":
            file_endpoint = f"/penelitian/{year}/csv/{file}"
        elif research_type == "pengabdian":
            file_endpoint = f"/pengabdian/{year}/csv/{file}"
        elif research_type == "buku_keilmuan":
            file_endpoint = f"/buku_keilmuan/{year}/csv/{file}"
        elif research_type == "sitasi":
            file_endpoint = f"/sitasi/{year}/csv/{file}"

        if file_endpoint is None:
            print(f"Skipping unrecognised file: {file}")
            continue

        StorageS3.upload(
            filename=file_endpoint,
            filepath=f"/home/iceberg/notebooks/data/{file}",
        )

# Penelitian
csv_penelitian = [
    StorageS3.load("penelitian/2021/csv/penelitian_2021.csv"),
    StorageS3.load("penelitian/2022/csv/penelitian_2022.csv"),
    StorageS3.load("penelitian/2023/csv/penelitian_2023.csv"),
    StorageS3.load("penelitian/2024/csv/penelitian_2024.csv"),
    StorageS3.load("penelitian/2025/csv/penelitian_2025.csv"),
]

res = (
    Transform(spark=SparkSession, document_type="penelitian")
    .processData(csv_penelitian[0]["path"], 2021)
    .processData(csv_penelitian[1]["path"], 2022)
    .processData(csv_penelitian[2]["path"], 2023)
    .processData(csv_penelitian[3]["path"], 2024)
    .processData(csv_penelitian[4]["path"], 2025)
    .join()
)
res = res.withColumn(
    "id",
    F.concat(
        F.lit("PENELITIAN-"),
        F.xxhash64(
            F.coalesce(F.col("judul_proposal"), F.lit("")),
            F.coalesce(F.col("ketua_peneliti"), F.lit("")),
            F.col("tahun"),
        ).cast("string"),
    ),
)
res.writeTo("silver.penelitian").createOrReplace()
print("Written silver.penelitian")

# Pengabdian
csv_pengabdian = [
    StorageS3.load("pengabdian/2021/csv/pengabdian_2021.csv"),
    StorageS3.load("pengabdian/2022/csv/pengabdian_2022.csv"),
    StorageS3.load("pengabdian/2023/csv/pengabdian_2023.csv"),
    StorageS3.load("pengabdian/2024/csv/pengabdian_2024.csv"),
    StorageS3.load("pengabdian/2025/csv/pengabdian_2025.csv"),
]

res = (
    Transform(spark=SparkSession, document_type="pengabdian")
    .processData(csv_pengabdian[0]["path"], 2021)
    .processData(csv_pengabdian[1]["path"], 2022)
    .processData(csv_pengabdian[2]["path"], 2023)
    .processData(csv_pengabdian[3]["path"], 2024)
    .processData(csv_pengabdian[4]["path"], 2025)
    .join()
)
res = res.withColumn(
    "id",
    F.concat(
        F.lit("PENGABDIAN-"),
        F.xxhash64(
            F.coalesce(F.col("judul_proposal"), F.lit("")),
            F.coalesce(F.col("ketua_peneliti"), F.lit("")),
            F.col("tahun"),
        ).cast("string"),
    ),
)
res.writeTo("silver.pengabdian").createOrReplace()
print("Written silver.pengabdian")

# Buku Keilmuan
csv_buku_keilmuan = [
    StorageS3.load("buku_keilmuan/2023/csv/buku_keilmuan_2023.csv"),
    StorageS3.load("buku_keilmuan/2024/csv/buku_keilmuan_2024.csv"),
]

res = (
    Transform(spark=SparkSession, document_type="buku_keilmuan")
    .processData(csv_buku_keilmuan[0]["path"], 2023)
    .processData(csv_buku_keilmuan[1]["path"], 2024)
    .join()
)
res = res.withColumn(
    "id",
    F.concat(
        F.lit("BUKU_KEILMUAN-"),
        F.xxhash64(
            F.coalesce(F.col("judul_proposal"), F.lit("")),
            F.coalesce(F.col("ketua_peneliti"), F.lit("")),
            F.col("tahun"),
        ).cast("string"),
    ),
)
res.writeTo("silver.buku_keilmuan").createOrReplace()
print("Written silver.buku_keilmuan")

# Sitasi
csv_sitasi = [
    StorageS3.load("sitasi/2026/csv/sitasi_2026.csv"),
]

res = Transform(spark=SparkSession, document_type="sitasi").processSitasiData(csv_sitasi[0]["path"], 2026).join()
res = res.withColumn(
    "ketua_peneliti",
    map_dosen_name_udf(F.col("ketua_peneliti")),
)
res = res.withColumn(
    "id",
    F.concat(
        F.lit("SITASI-"),
        F.xxhash64(
            F.coalesce(F.col("judul_proposal"), F.lit("")),
            F.coalesce(F.col("ketua_peneliti"), F.lit("")),
            F.coalesce(F.col("doi"), F.lit("")),
        ).cast("string"),
    ),
)
res.writeTo("silver.sitasi").createOrReplace()
print("Written silver.sitasi")

# Skema Mapping
SparkSession.sql("DROP TABLE IF EXISTS silver.dim_skema")
run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_skema.sql")
print("Written silver.dim_skema")

# SDGs Mapping
SparkSession.sql("DROP TABLE IF EXISTS silver.dim_sdgs")
run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_sdgs.sql")
print("Written silver.dim_sdgs")

# Dimensi Dosen (Gold)
run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_dosen.sql")
print("Written gold.dim_dosen")

# Dimensi Jurnal (Gold)
run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_jurnal.sql")
print("Written gold.dim_jurnal")

# Dimensi Hibah Proposal (Gold)
run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_hibah_proposal.sql")
print("Written gold.dim_hibah_proposal")

# Fakta Dosen Hibah (Gold)
run_sql_file(SparkSession, BASE_DIR / "schema" / "fact_dosen_hibah.sql")
print("Written gold.fact_dosen_hibah")

# Fakta Hibah (Gold)
run_sql_file(SparkSession, BASE_DIR / "schema" / "fact_hibah.sql")
print("Written gold.fact_hibah")

# Fakta Sitasi (Gold)
run_sql_file(SparkSession, BASE_DIR / "schema" / "fact_sitasi.sql")
print("Written gold.fact_sitasi")
