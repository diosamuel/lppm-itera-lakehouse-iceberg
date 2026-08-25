import os
from pathlib import Path
from pyspark.sql import functions as F
from transform.extract_transform import Transform
from tools.dosen_name_mapper import map_dosen_name_udf
from transform.xlsx_clean import clean_xlsx_sheet, list_xlsx_year_sheets
from setup.setup_catalog import SetupIcebergCatalog
from setup.setup_minio import SetupMinioS3
from setup.setup_spark import SetupSpark

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

RAW_XLSX = os.getenv("RAW_XLSX_PATH", "s3a://sipaper/raw_data_penelitian.xlsx")
PENELITIAN_SHEETS = list_xlsx_year_sheets(RAW_XLSX, StorageS3.client)
print(f"Penelitian year sheets: {PENELITIAN_SHEETS}")
csv_penelitian = []
for sheet in PENELITIAN_SHEETS:
    out = f"s3a://sipaper/penelitian/{sheet}/csv/penelitian_{sheet}.csv"
    (
        clean_xlsx_sheet(SparkSession, RAW_XLSX, sheet)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(out)
    )
    csv_penelitian.append((out, int(sheet)))

penelitian_builder = Transform(spark=SparkSession, document_type="penelitian")
for path, year in csv_penelitian:
    penelitian_builder.processData(path, year)
res = penelitian_builder.join()
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
RAW_PENGABDIAN_XLSX = os.getenv("RAW_PENGABDIAN_XLSX_PATH", "s3a://sipaper/raw_data_pengabdian.xlsx")
PENGABDIAN_SHEETS = list_xlsx_year_sheets(RAW_PENGABDIAN_XLSX, StorageS3.client)
print(f"Pengabdian year sheets: {PENGABDIAN_SHEETS}")
csv_pengabdian = []
for sheet in PENGABDIAN_SHEETS:
    out = f"s3a://sipaper/pengabdian/{sheet}/csv/pengabdian_{sheet}.csv"
    (
        clean_xlsx_sheet(SparkSession, RAW_PENGABDIAN_XLSX, sheet)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(out)
    )
    csv_pengabdian.append((out, int(sheet)))

pengabdian_builder = Transform(spark=SparkSession, document_type="pengabdian")
for path, year in csv_pengabdian:
    pengabdian_builder.processData(path, year)
res = pengabdian_builder.join()
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
RAW_BUKU_KEILMUAN_XLSX = os.getenv("RAW_BUKU_KEILMUAN_XLSX_PATH", "s3a://sipaper/raw_data_buku_keilmuan.xlsx")
BUKU_KEILMUAN_SHEETS = list_xlsx_year_sheets(RAW_BUKU_KEILMUAN_XLSX, StorageS3.client)
print(f"Buku Keilmuan year sheets: {BUKU_KEILMUAN_SHEETS}")
csv_buku_keilmuan = []
for sheet in BUKU_KEILMUAN_SHEETS:
    out = f"s3a://sipaper/buku_keilmuan/{sheet}/csv/buku_keilmuan_{sheet}.csv"
    (
        clean_xlsx_sheet(SparkSession, RAW_BUKU_KEILMUAN_XLSX, sheet)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(out)
    )
    csv_buku_keilmuan.append((out, int(sheet)))

buku_builder = Transform(spark=SparkSession, document_type="buku_keilmuan")
for path, year in csv_buku_keilmuan:
    buku_builder.processData(path, year)
res = buku_builder.join()
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
RAW_SITASI_XLSX = os.getenv("RAW_SITASI_XLSX_PATH", "s3a://sipaper/raw_data_sitasi.xlsx")
SITASI_SHEETS = list_xlsx_year_sheets(RAW_SITASI_XLSX, StorageS3.client)
print(f"Sitasi year sheets: {SITASI_SHEETS}")
sitasi_builder = Transform(spark=SparkSession, document_type="sitasi")
for sheet in SITASI_SHEETS:
    out = f"s3a://sipaper/sitasi/{sheet}/csv/sitasi_{sheet}.csv"
    (
        clean_xlsx_sheet(SparkSession, RAW_SITASI_XLSX, sheet)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(out)
    )
    sitasi_builder.processSitasiData(out, int(sheet))
res = sitasi_builder.join()
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
