"""Data-quality runner untuk skema silver.

Menjalankan 3 kelompok check dan menulis hasilnya ke tabel Iceberg `dq.dq_results`:
  1. Consistency — anomaly tertukar skema/sdgs (reuse AUDIT dari
     pipeline/quality_check/schema_sdgs.sql via pipeline.audit.swapped_skema_sdgs)
  2. Completeness — NULL/empty pada kolom wajib per tabel
  3. Referential integrity — nilai tidak match dengan dim_*
     (skema vs dim_skema, sdgs vs dim_sdgs, prodi vs dim_prodi)

Idempoten: namespace `dq` + tabel `dq.dq_results` dibuat jika belum ada;
setiap run meng-append baris baru (dengan `run_id` & `checked_at` UTC).

Jalankan di dalam container spark:
    docker compose exec spark-iceberg spark-submit --deploy-mode client \
        /home/iceberg/pipeline/quality_check/dq_runner.py
"""
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# pipeline/ root ke sys.path agar modul pipeline.* bisa diimpor
BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from setup.setup_spark import SetupSpark
from setup.setup_catalog import SetupIcebergCatalog
# reuse template AUDIT + fungsi audit() — single source of truth (schema_sdgs.sql)
from audit.swapped_skema_sdgs import AUDIT, audit

from pyspark.sql import functions as F, types as T

# konfigurasi check per tabel silver
CHECKS = {
    "silver.penelitian": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "skema", "sdgs", "prodi", "usulan_biaya", "tahun"],
        "ref": {"skema": ("gold.dim_skema", "nama_skema"),
                "sdgs": ("gold.dim_sdgs", "kode_sdgs"),
                "prodi": ("gold.dim_prodi", "nama_prodi")},
        "swap": True,
    },
    "silver.pengabdian": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "skema", "sdgs", "prodi", "usulan_biaya", "tahun"],
        "ref": {"skema": ("gold.dim_skema", "nama_skema"),
                "sdgs": ("gold.dim_sdgs", "kode_sdgs"),
                "prodi": ("gold.dim_prodi", "nama_prodi")},
        "swap": True,
    },
    "silver.buku_keilmuan": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "skema", "sdgs", "prodi", "usulan_biaya", "tahun"],
        "ref": {"skema": ("gold.dim_skema", "nama_skema"),
                "sdgs": ("gold.dim_sdgs", "kode_sdgs"),
                "prodi": ("gold.dim_prodi", "nama_prodi")},
        "swap": True,
    },
    "silver.sitasi": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "doi", "jurnal", "sitasi",
                       "tanggal_terbit_timestamp", "prodi"],
        "ref": {"prodi": ("gold.dim_prodi", "nama_prodi")},
        "swap": False,
    },
}

THRESHOLD = 0.0  # 0 violation diperbolehkan; ubah per-rule jika perlu


def run_sql_file(spark, sql_file):
    text = (BASE / "schema" / sql_file).read_text(encoding="utf-8")
    for stmt in [s.strip() for s in text.split(";") if s.strip()]:
        spark.sql(stmt)


def ensure_dq(spark, retries=5, delay=3):
    """Buat namespace `dq` + tabel `dq.dq_results` jika belum ada (idempoten).

    REST catalog fixture memakai SQLite (tanpa busy-timeout) sehingga kadang
    menolak tulis bersamaan dengan SQLITE_BUSY — lakukan retry dengan backoff.
    Idempoten, jadi mengulang aman.
    """
    for attempt in range(retries):
        try:
            SetupIcebergCatalog(catalog_name="default", namespace="dq").initialize()
            run_sql_file(spark, "dq_results.sql")
            return
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"[DQ] transient error membuat dq.dq_results "
                  f"(percobaan {attempt + 1}/{retries}): {str(e)[:120]}")
            time.sleep(delay)


def _row(run_id, checked_at, rule, table, column, ctype, violations, total):
    """Bangun satu baris hasil DQ sebagai tuple (skema dq.dq_results)."""
    v = int(violations or 0)
    n = int(total or 0)
    pass_rate = 1.0 if n == 0 else round((n - v) / n, 4)
    passed = v <= THRESHOLD
    return (run_id, rule, table, column, ctype, v, n, pass_rate, passed, THRESHOLD, checked_at)


def check_nulls(spark, t, cols, run_id, checked_at):
    """Completeness: hitung NULL/empty per kolom wajib."""
    total = spark.sql(f"SELECT count(*) AS c FROM {t}").first()["c"]
    rows = []
    for col in cols:
        v = spark.sql(
            f"SELECT count(*) AS c FROM {t} "
            f"WHERE {col} IS NULL OR TRIM(CAST({col} AS string)) = ''"
        ).first()["c"]
        rows.append(_row(run_id, checked_at, f"not_null_{col}", t, col, "completeness", v, total))
    return rows


def check_swaps(spark, t, run_id, checked_at):
    """Consistency: anomaly tertukar skema/sdgs (dari AUDIT schema_sdgs.sql)."""
    a = audit(spark, t)
    total = a["total"]
    rows = []
    for rule in ["both_swapped", "skema_only", "sdgs_only", "sdgs_eq_skema"]:
        rows.append(_row(run_id, checked_at, rule, t, "skema,sdgs", "consistency", a[rule], total))
    return rows


def check_referential(spark, t, col, ref_table, ref_col, run_id, checked_at):
    """Referential integrity: nilai kolom tidak ditemukan di tabel referensi dim_*."""
    q = f"""
    SELECT count(*) AS v
    FROM {t} h
    LEFT JOIN {ref_table} r
      ON LOWER(TRIM(CAST(h.{col} AS string))) = LOWER(TRIM(CAST(r.{ref_col} AS string)))
    WHERE h.{col} IS NOT NULL AND TRIM(CAST(h.{col} AS string)) <> '' AND r.{ref_col} IS NULL
    """
    v = spark.sql(q).first()["v"]
    total = spark.sql(f"SELECT count(*) AS c FROM {t} WHERE {col} IS NOT NULL").first()["c"]
    return [_row(run_id, checked_at, f"ref_{col}", t, col, "referential", v, total)]


def main():
    spark = SetupSpark(app_name="dq-check-silver", catalog_name="default").initialize()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE default")
    ensure_dq(spark)

    run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc)
    print(f"[DQ] run_id={run_id} checked_at={checked_at.isoformat()}")

    rows = []
    for t, cfg in CHECKS.items():
        print(f"[DQ] {t}")
        rows += check_nulls(spark, t, cfg["mandatory"], run_id, checked_at)
        if cfg["swap"]:
            rows += check_swaps(spark, t, run_id, checked_at)
        for col, (rt, rc) in cfg["ref"].items():
            rows += check_referential(spark, t, col, rt, rc, run_id, checked_at)

    schema = T.StructType([
        T.StructField("run_id", T.StringType()),
        T.StructField("rule_name", T.StringType()),
        T.StructField("table_name", T.StringType()),
        T.StructField("column", T.StringType()),
        T.StructField("check_type", T.StringType()),
        T.StructField("violations_count", T.LongType()),
        T.StructField("total_rows", T.LongType()),
        T.StructField("pass_rate", T.DoubleType()),
        T.StructField("passed", T.BooleanType()),
        T.StructField("threshold", T.DoubleType()),
        T.StructField("checked_at", T.TimestampType()),
    ])
    out = spark.createDataFrame(rows, schema)
    out.show(50, truncate=False)

    out.write.mode("append").saveAsTable("dq.dq_results")
    failed = sum(1 for r in rows if not r[8])
    print(f"[DQ] {len(rows)} rule dievaluasi, {failed} FAIL — ditulis ke dq.dq_results")
    spark.stop()


if __name__ == "__main__":
    main()
