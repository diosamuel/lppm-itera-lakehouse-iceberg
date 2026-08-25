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

# Initialize catalog and spark
IcebergCatalog = SetupIcebergCatalog(
    catalog_name="default",
    namespace="silver",
).initialize()

SparkSession = SetupSpark(
    app_name="sipaper",
    catalog_name="default",
).initialize()

RAW_XLSX = os.getenv("RAW_XLSX_PATH", "s3a://sipaper/raw_data_penelitian.xlsx")
PENELITIAN_SHEETS = list_xlsx_year_sheets(RAW_XLSX, StorageS3.client)
print(f"Penelitian year sheets: {PENELITIAN_SHEETS}")

penelitian_builder = Transform(spark=SparkSession, document_type="penelitian")
for sheet in PENELITIAN_SHEETS:
    df = clean_xlsx_sheet(SparkSession, RAW_XLSX, sheet)
    penelitian_builder.processData(df, int(sheet))
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

pengabdian_builder = Transform(spark=SparkSession, document_type="pengabdian")
for sheet in PENGABDIAN_SHEETS:
    df = clean_xlsx_sheet(SparkSession, RAW_PENGABDIAN_XLSX, sheet)
    pengabdian_builder.processData(df, int(sheet))
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

buku_builder = Transform(spark=SparkSession, document_type="buku_keilmuan")
for sheet in BUKU_KEILMUAN_SHEETS:
    df = clean_xlsx_sheet(SparkSession, RAW_BUKU_KEILMUAN_XLSX, sheet)
    buku_builder.processData(df, int(sheet))
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
    df = clean_xlsx_sheet(SparkSession, RAW_SITASI_XLSX, sheet)
    sitasi_builder.processSitasiData(df, int(sheet))
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
