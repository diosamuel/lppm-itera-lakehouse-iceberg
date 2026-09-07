import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from setup.setup_spark import SetupSpark
from write.swapped_skema_sdgs import audit
from audit.null_check import checkNulls
from audit.validity_check import checkValidity
from audit.uniqueness_check import checkUniqueness
from pyspark.sql import types as T

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

# konfigurasi check per tabel silver
CHECKS = {
    "silver.penelitian": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "skema", "sdgs", "prodi", "usulan_biaya", "tahun"],
        "ref": {"skema": ("gold.dim_skema", "nama_skema"),
                "sdgs": ("gold.dim_sdgs", "kode_sdgs"),
                "prodi": ("gold.dim_prodi", "nama_prodi")},
        "unique": ["id"],
        "swap": True,
    },
    "silver.pengabdian": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "skema", "sdgs", "prodi", "usulan_biaya", "tahun"],
        "ref": {"skema": ("gold.dim_skema", "nama_skema"),
                "sdgs": ("gold.dim_sdgs", "kode_sdgs"),
                "prodi": ("gold.dim_prodi", "nama_prodi")},
        "unique": ["id"],
        "swap": True,
    },
    "silver.buku_keilmuan": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "skema", "sdgs", "prodi", "usulan_biaya", "tahun"],
        "ref": {"skema": ("gold.dim_skema", "nama_skema"),
                "sdgs": ("gold.dim_sdgs", "kode_sdgs"),
                "prodi": ("gold.dim_prodi", "nama_prodi")},
        "unique": ["id"],
        "swap": True,
    },
    "silver.sitasi": {
        "mandatory": ["judul_proposal", "ketua_peneliti", "doi", "jurnal", "sitasi",
                       "tanggal_terbit_timestamp", "prodi"],
        "ref": {"prodi": ("gold.dim_prodi", "nama_prodi"),
                "ketua_peneliti": ("gold.dim_dosen", "nama")},
        "unique": ["id"],
        "swap": False,
    },
}

THRESHOLD = 0.0  # 0 violation diperbolehkan; ubah per-rule jika perlu


def runSqlFile(spark, sql_file):
    text = (BASE / "schema" / sql_file).read_text(encoding="utf-8")
    for stmt in [s.strip() for s in text.split(";") if s.strip()]:
        spark.sql(stmt)


def buildRow(run_id, checked_at, rule, table, column, ctype, violations, total):
    """Bangun satu baris hasil DQ sebagai tuple (skema dq.dq_report)."""
    v = int(violations or 0)
    n = int(total or 0)
    score = 100.0 if n == 0 else round(((n - v) / n) * 100, 2)
    status = "PASS" if v <= THRESHOLD else "FAIL"
    return (run_id, checked_at, table, column, ctype, rule, n, v, score, THRESHOLD * 100, status)


def checkSwaps(spark, t, run_id, checked_at):
    """Consistency: anomaly tertukar skema/sdgs (dari AUDIT schema_sdgs.sql)."""
    a = audit(spark, t)
    total = a["total"]
    rows = []
    for rule in ["both_swapped", "skema_only", "sdgs_only", "sdgs_eq_skema"]:
        rows.append(buildRow(run_id, checked_at, rule, t, "skema,sdgs", "consistency", a[rule], total))
    return rows


def main():
    spark = SetupSpark(app_name="dq-check-silver", catalog_name="default").initialize()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE default")
    runSqlFile(spark, "dq_results.sql")

    run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc)
    print(f"[DQ] run_id={run_id} checked_at={checked_at.isoformat()}")

    rows = []
    for t, cfg in CHECKS.items():
        print(f"[DQ] {t}")
        rows += checkNulls(spark, t, cfg["mandatory"], run_id, checked_at, buildRow, THRESHOLD)
        if cfg["swap"]:
            rows += checkSwaps(spark, t, run_id, checked_at)
        for col, (rt, rc) in cfg["ref"].items():
            rows += checkValidity(spark, t, col, rt, rc, run_id, checked_at, buildRow, THRESHOLD)
        for col in cfg.get("unique", []):
            rows += checkUniqueness(spark, t, col, run_id, checked_at, buildRow, THRESHOLD)

    schema = T.StructType([
        T.StructField("run_id", T.StringType()),
        T.StructField("run_timestamp", T.TimestampType()),
        T.StructField("table_name", T.StringType()),
        T.StructField("column_name", T.StringType()),
        T.StructField("dimension", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("total_records", T.LongType()),
        T.StructField("failed_records", T.LongType()),
        T.StructField("score", T.DoubleType()),
        T.StructField("threshold", T.DoubleType()),
        T.StructField("status", T.StringType()),
    ])
    out = spark.createDataFrame(rows, schema)
    out.show(50, truncate=False)

    out.write.mode("append").saveAsTable("dq.dq_report")
    failed = sum(1 for r in rows if r[10] == "FAIL")
    print(f"[DQ] {len(rows)} rule dievaluasi, {failed} FAIL — ditulis ke dq.dq_report")
    spark.stop()


if __name__ == "__main__":
    main()
