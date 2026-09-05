"""WAP: koreksi skema/sdgs tertukar di silver — WRITE -> AUDIT -> PUBLISH.

Flow Write-Audit-Publish saja (tanpa sel pemeriksaan/deskripsi), diselaraskan
dengan spark/notebooks/WAP_fix_swapped_skema_sdgs.ipynb.
SparkSession dibangun lewat SetupSpark (pipeline/setup/setup_spark.py).
SQL audit dibaca dari pipeline/quality_check/schema_sdgs.sql (single source
of truth, pola audit_*.py + *.sql).

Jalankan di dalam container spark:
    docker compose exec spark-iceberg spark-submit --deploy-mode client \
        /home/iceberg/pipeline/audit/swapped_skema_sdgs.py [silver.<table> ...]
(tanpa argumen: semua tabel di TABLES)
"""
import sys
from pathlib import Path

# pipeline/ root ke sys.path agar `setup.setup_spark` bisa diimpor
BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from setup.setup_spark import SetupSpark
from pyspark.sql import functions as F

TABLES = ["silver.penelitian", "silver.pengabdian"]
BRANCH = "audit-swap"

# Relasi union seluruh silver hibah (untuk audit global di akhir run).
# Tanpa alias 'h' — template .sql sudah menulis `FROM {t} h`.
ALL_HIBAH = (
    "(SELECT * FROM silver.penelitian "
    "UNION ALL SELECT * FROM silver.pengabdian "
    "UNION ALL SELECT * FROM silver.buku_keilmuan)"
)


def load_audit_sql():
    """Baca template audit dari pipeline/quality_check/schema_sdgs.sql.
    Buang baris komentar `--` dan trailing `;` agar siap di-format {t}."""
    text = (BASE / "quality_check" / "schema_sdgs.sql").read_text()
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return "\n".join(lines).strip().rstrip(";").strip()


# template parameter {t} — di-format saat dipanggil di audit()
AUDIT = load_audit_sql()


def fix_skema_sdgs(spark, src_sql):
    """Kembalikan baris dengan skema/sdgs terkoreksi (3 skenario auto-fix).
    src_sql: nama tabel, atau relasi SQL seperti
    "silver.penelitian VERSION AS OF <snapshot_id>" (untuk recovery/time travel).

    !!! PENTING: withColumns (atomic), BUKAN withColumn berantai.
    withColumn berantai membuat F.col("skema") di new_sdgs merujuk ke kolom skema
    yang SUDAH ditimpa new_skema -> nilai asli hilang dan kedua kolom diisi nilai
    yang sama (bug insiden: 542 baris sdgs == skema di silver.penelitian)."""
    t = spark.sql(f"SELECT * FROM {src_sql}")

    # daftar nilai valid dari tabel referensi (lowercased + trimmed)
    sk_vals = [r[0] for r in spark.table("gold.dim_skema")
               .select(F.lower(F.trim(F.col("nama_skema")))).collect()]
    sd_vals = [r[0] for r in spark.table("gold.dim_sdgs")
               .select(F.lower(F.trim(F.col("kode_sdgs")))).collect()]

    t = t.withColumn("_skema", F.lower(F.trim(F.col("skema"))))
    t = t.withColumn("_sdgs",  F.lower(F.trim(F.col("sdgs"))))

    is_skema_valid   = F.coalesce(F.col("_skema").isin(sk_vals), F.lit(False))
    is_sdgs_valid    = F.coalesce(F.col("_sdgs").isin(sd_vals), F.lit(False))
    is_skema_in_sdgs = F.coalesce(F.col("_skema").isin(sd_vals), F.lit(False))
    is_sdgs_in_skema = F.coalesce(F.col("_sdgs").isin(sk_vals), F.lit(False))

    both_swapped = is_skema_in_sdgs & is_sdgs_in_skema & ~is_skema_valid & ~is_sdgs_valid
    skema_only   = F.col("_skema").isNull() & is_sdgs_in_skema
    sdgs_only    = F.col("_sdgs").isNull() & is_skema_in_sdgs

    new_skema = (
        F.when(both_swapped, F.col("sdgs"))
        .when(skema_only, F.col("sdgs"))
        .when(sdgs_only, F.lit(None))
        .otherwise(F.col("skema"))
    )
    new_sdgs = (
        F.when(both_swapped, F.col("skema"))
        .when(skema_only, F.lit(None))
        .when(sdgs_only, F.col("skema"))
        .otherwise(F.col("sdgs"))
    )
    # withColumns mengevaluasi SEMUA ekspresi terhadap DataFrame input (nilai asli)
    return (t.withColumns({"skema": new_skema, "sdgs": new_sdgs})
             .drop("_skema", "_sdgs"))


def audit(spark, t):
    """Kembalikan hasil audit relasi {t} sebagai dict.
    t: nama tabel, atau "... VERSION AS OF '<branch>'", atau ALL_HIBAH."""
    row = spark.sql(AUDIT.format(t=t)).first()
    return {k: int(row[k]) for k in row.asDict()}


def broken(a):
    """Jumlah baris yang bisa diperbaiki otomatis (swap/skema_only/sdgs_only)."""
    return a["both_swapped"] + a["skema_only"] + a["sdgs_only"]


def wap_write(spark, t):
    """WRITE: terapkan fix ke branch (main tidak tersentuh)."""
    spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"ALTER TABLE {t} DROP BRANCH IF EXISTS `{BRANCH}`")
    spark.sql(f"ALTER TABLE {t} CREATE BRANCH `{BRANCH}`")
    fix_skema_sdgs(spark, t).createOrReplaceTempView("_wap_fix")
    spark.conf.set("spark.wap.branch", BRANCH)
    spark.sql(f"INSERT OVERWRITE TABLE {t} SELECT * FROM _wap_fix")
    spark.conf.unset("spark.wap.branch")


def wap_publish(spark, t):
    """PUBLISH (gated): fast_forward ke main hanya jika audit branch lolos.
    Return True jika dipublish, False jika diblok (branch dibiarkan untuk inspeksi)."""
    a_branch = audit(spark, f"{t} VERSION AS OF '{BRANCH}'")
    a_main   = audit(spark, t)

    gates = {
        "broken == 0":         broken(a_branch) == 0,
        "sdgs_eq_skema == 0":  a_branch["sdgs_eq_skema"] == 0,
        "invalid tidak bertambah":
            a_branch["invalid_skema"] + a_branch["invalid_sdgs"]
            <= a_main["invalid_skema"] + a_main["invalid_sdgs"],
        "total rows sama":     a_branch["total"] == a_main["total"],
    }
    print(f"  audit branch {t} -> {a_branch}")
    for name, ok in gates.items():
        print(f"    gate [{name}]: {'PASS' if ok else 'FAIL'}")

    if all(gates.values()):
        spark.sql(f"CALL default.system.fast_forward('{t}', 'main', '{BRANCH}')")
        spark.sql(f"ALTER TABLE {t} DROP BRANCH IF EXISTS `{BRANCH}`")
        return True
    print(f"    BLOK PUBLISH — branch `{BRANCH}` dipertahankan untuk inspeksi:")
    print(f"      SELECT * FROM {t} VERSION AS OF '{BRANCH}'")
    return False


def main():
    spark = SetupSpark(
        app_name="wap-fix-swapped-skema-sdgs", catalog_name="default"
    ).initialize()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE default")
    tables = sys.argv[1:] or TABLES

    published = {}
    for t in tables:
        print(f"[WAP] {t}")
        print(f"  audit main -> {audit(spark, t)}")
        wap_write(spark, t)
        published[t] = wap_publish(spark, t)

    # audit global: seluruh silver hibah sekaligus (pengecek schema_sdgs.sql)
    print("\n[audit global] silver (penelitian + pengabdian + buku_keilmuan)")
    g = audit(spark, ALL_HIBAH)
    print(f"  -> {g}")
    if broken(g) == 0 and g["sdgs_eq_skema"] == 0:
        print("  status: BERSIH (semua swap terkoreksi, tidak ada sdgs == skema)")
    else:
        print("  status: masih ada pelanggaran — periksa invalid_* (manual only)")

    print("\n=== ringkasan ===")
    for t, ok in published.items():
        print(f"  {t}: {'PUBLISHED' if ok else 'BLOCKED'}")
    spark.stop()


if __name__ == "__main__":
    main()
