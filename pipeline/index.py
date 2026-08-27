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


def compute_fact_hibah(spark):
    """Compute fact_hibah DataFrame with dimension join columns for WAP split."""
    return spark.sql("""
        WITH hibah_lengkap AS (
            SELECT
                id, jenis, tahun, status, ketua_peneliti, nip_ketua_peneliti,
                skema, sdgs, usulan_biaya, nim_anggota_mahasiswa, nip_anggota_dosen,
                nama_anggota_mahasiswa, nama_anggota_dosen
            FROM silver.penelitian
            UNION ALL
            SELECT
                id, jenis, tahun, status, ketua_peneliti, nip_ketua_peneliti,
                skema, sdgs, usulan_biaya, nim_anggota_mahasiswa, nip_anggota_dosen,
                nama_anggota_mahasiswa, nama_anggota_dosen
            FROM silver.pengabdian
            UNION ALL
            SELECT
                id, jenis, tahun, status, ketua_peneliti, nip_ketua_peneliti,
                skema, sdgs, usulan_biaya, nim_anggota_mahasiswa, nip_anggota_dosen,
                nama_anggota_mahasiswa, nama_anggota_dosen
            FROM silver.buku_keilmuan
        )
        SELECT
            CAST(xxhash64(h.id) AS INT) AS hibah_fact_id,
            d.dosen_id AS ketua_id,
            h.id AS hibah_proposal_id,
            sk.skema_id,
            sd.sdgs_id,
            h.jenis AS jenis_hibah,
            h.tahun,
            h.status AS status_hibah,
            CASE WHEN h.nama_anggota_mahasiswa IS NOT NULL THEN SIZE(h.nama_anggota_mahasiswa) ELSE 0 END AS total_anggota_mahasiswa,
            CASE WHEN h.nama_anggota_dosen IS NOT NULL THEN SIZE(h.nama_anggota_dosen) ELSE 0 END AS total_anggota_dosen,
            h.usulan_biaya
        FROM hibah_lengkap h
        LEFT JOIN gold.dim_dosen d
            ON h.ketua_peneliti = d.nama
            AND h.nip_ketua_peneliti[0] = d.nip
        LEFT JOIN gold.dim_skema sk
            ON LOWER(TRIM(h.skema)) = LOWER(TRIM(sk.nama_skema))
        LEFT JOIN gold.dim_sdgs sd
            ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(sd.kode_sdgs))
    """)


def compute_fact_dosen_hibah(spark):
    """Compute fact_dosen_hibah DataFrame with dimension join columns for WAP split."""
    return spark.sql("""
        WITH hibah_lengkap AS (
            SELECT id, jenis, tahun, status, nama_anggota_dosen, nip_anggota_dosen,
                   ketua_peneliti, nip_ketua_peneliti, prodi, fakultas
            FROM silver.penelitian
            UNION ALL
            SELECT id, jenis, tahun, status, nama_anggota_dosen, nip_anggota_dosen,
                   ketua_peneliti, nip_ketua_peneliti, prodi, fakultas
            FROM silver.pengabdian
            UNION ALL
            SELECT id, jenis, tahun, status, nama_anggota_dosen, nip_anggota_dosen,
                   ketua_peneliti, nip_ketua_peneliti, prodi, fakultas
            FROM silver.buku_keilmuan
        ),
        hibah_dosen AS (
            SELECT
                id AS hibah_proposal_id, jenis, tahun, status,
                ketua_peneliti AS nama, nip_ketua_peneliti[0] AS nip,
                'ketua' AS role, prodi, fakultas
            FROM hibah_lengkap
            UNION ALL
            SELECT
                id AS hibah_proposal_id, jenis, tahun, status,
                t.nama_anggota_dosen AS nama, t.nip_anggota_dosen AS nip,
                'anggota' AS role, prodi, fakultas
            FROM hibah_lengkap
            LATERAL VIEW EXPLODE(arrays_zip(nama_anggota_dosen, nip_anggota_dosen)) AS t
        )
        SELECT
            CAST(xxhash64(COALESCE(d.dosen_id, 0), h.hibah_proposal_id, h.role) AS INT) AS dosen_hibah_id,
            d.dosen_id,
            h.hibah_proposal_id,
            h.tahun,
            h.role,
            h.jenis AS jenis_hibah,
            h.status AS status_hibah,
            h.prodi,
            h.fakultas
        FROM hibah_dosen h
        LEFT JOIN gold.dim_dosen d
            ON (h.nip IS NOT NULL AND h.nip <> '0' AND h.nip = d.nip)
            OR ((h.nip IS NULL OR h.nip = '0')
                AND (d.nip IS NULL OR d.nip = '0')
                AND h.nama = d.nama)
    """)


def compute_fact_sitasi(spark):
    """Compute fact_sitasi DataFrame with dimension join columns for WAP split."""
    return spark.sql("""
        SELECT
            CAST(xxhash64(COALESCE(d.dosen_id, 0), COALESCE(j.jurnal_id, 0)) AS INT) AS sitasi_id,
            d.dosen_id,
            j.jurnal_id,
            COUNT(*) AS total_publikasi,
            SUM(CASE WHEN j.kategori_jurnal = 'INTERNASIONAL' THEN 1 ELSE 0 END) AS total_internasional,
            SUM(CASE WHEN j.kategori_jurnal = 'NASIONAL' THEN 1 ELSE 0 END) AS total_nasional
        FROM silver.sitasi s
        LEFT JOIN gold.dim_dosen d
            ON s.ketua_peneliti = d.nama
        LEFT JOIN gold.dim_jurnal j
            ON s.jurnal = j.nama_jurnal
            AND s.jurnal_kategori = j.kategori_jurnal
        GROUP BY d.dosen_id, j.jurnal_id
    """)


def write_wap(spark, df, table, mismatch_filter, partition_cols=None):
    """Write DataFrame to both main and audit-swap branch.

    Args:
        mismatch_filter: Column expression that is True for mismatched rows.
        partition_cols: Optional partition columns for Iceberg table.
    Returns:
        (main_count, mismatch_count)
    """
    matched = df.filter(~mismatch_filter)
    mismatched = df.filter(mismatch_filter)

    # Write matched rows to main branch
    matched_writer = matched.writeTo(table)
    if partition_cols:
        matched_writer = matched_writer.partitioned_by(*partition_cols)
    matched_writer.createOrReplace()
    print(f"  Main branch: {matched.count()} rows")

    # Write ALL rows to audit-swap branch
    audit_writer = df.writeTo(table).branch("audit-swap")
    if partition_cols:
        audit_writer = audit_writer.partitioned_by(*partition_cols)
    audit_writer.createOrReplace()
    print(f"  Audit branch: {df.count()} rows")

    return matched.count(), mismatched.count()


# ---------------------------------------------------------------------------
# Initialize storage
# ---------------------------------------------------------------------------
StorageS3 = SetupMinioS3(
    endpoint_url="http://minio:9000",
    bucket="sipaper",
).initialize()

IcebergCatalog = SetupIcebergCatalog(
    catalog_name="default",
    namespace="silver",
).initialize()

SparkSession = SetupSpark(
    app_name="sipaper",
    catalog_name="default",
).initialize()

# ---------------------------------------------------------------------------
# Silver layer
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Dimension tables (static inserts)
# ---------------------------------------------------------------------------
SparkSession.sql("DROP TABLE IF EXISTS silver.dim_skema")
run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_skema.sql")
print("Written silver.dim_skema")

SparkSession.sql("DROP TABLE IF EXISTS silver.dim_sdgs")
run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_sdgs.sql")
print("Written silver.dim_sdgs")

run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_dosen.sql")
print("Written gold.dim_dosen")

run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_jurnal.sql")
print("Written gold.dim_jurnal")

run_sql_file(SparkSession, BASE_DIR / "schema" / "dim_hibah_proposal.sql")
print("Written gold.dim_hibah_proposal")

# ---------------------------------------------------------------------------
# Gold fact tables — Write Audit Publish (WAP)
# ---------------------------------------------------------------------------
print("\n=== WAP: fact_hibah ===")
fact_hibah_df = compute_fact_hibah(SparkSession)
main_count, mismatch_count = write_wap(
    SparkSession, fact_hibah_df, "gold.fact_hibah",
    mismatch_filter=F.col("skema_id").isNull() | F.col("sdgs_id").isNull(),
    partition_cols=["tahun"],
)
print(f"  Mismatched (skema/sdgs): {mismatch_count}")

print("\n=== WAP: fact_dosen_hibah ===")
fact_dosen_hibah_df = compute_fact_dosen_hibah(SparkSession)
main_count, mismatch_count = write_wap(
    SparkSession, fact_dosen_hibah_df, "gold.fact_dosen_hibah",
    mismatch_filter=F.col("dosen_id").isNull() | F.col("prodi").isNull(),
    partition_cols=["tahun"],
)
print(f"  Mismatched (dosen/prodi): {mismatch_count}")

print("\n=== WAP: fact_sitasi ===")
fact_sitasi_df = compute_fact_sitasi(SparkSession)
main_count, mismatch_count = write_wap(
    SparkSession, fact_sitasi_df, "gold.fact_sitasi",
    mismatch_filter=F.col("dosen_id").isNull() | F.col("jurnal_id").isNull(),
)
print(f"  Mismatched (dosen/jurnal): {mismatch_count}")

# ---------------------------------------------------------------------------
# Audit tables — store mismatched rows for manual review
# ---------------------------------------------------------------------------
print("\n=== Writing audit tables ===")
run_sql_file(SparkSession, BASE_DIR / "schema" / "audit" / "audit_faktor_hibah.sql")
print("Written audit.audit_faktor_hibah")

run_sql_file(SparkSession, BASE_DIR / "schema" / "audit" / "audit_dosen_hibah.sql")
print("Written audit.audit_dosen_hibah")

run_sql_file(SparkSession, BASE_DIR / "schema" / "audit" / "audit_sitasi.sql")
print("Written audit.audit_sitasi")

print("\n=== Pipeline complete ===")
print("Audit branch 'audit-swap' is ready for manual review.")
print("After review, run: spark.sql(\"CALL gold.system.merge_branches('fact_hibah', 'audit-swap')\")")
