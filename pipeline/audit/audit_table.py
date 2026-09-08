"""Audit & WAP Table Repair: Pembersihan Baris Null & Rekonsiliasi Skema/SDGs Tertukar.

Modul ini mengimplementasikan alur Write-Audit-Publish (WAP) untuk tabel hibah silver:
  - silver.penelitian
  - silver.pengabdian
  - silver.buku_keilmuan

Alur WAP (Write-Audit-Publish) Berbasis Apache Iceberg:
1. AUDIT (Main Branch Baseline):
   - Menghitung anomali pada tabel sumber di branch `main`:
     * `null_judul`: proposal tanpa judul (baris kosong sisa file spreadsheet)
     * `both_swapped`: skema dan sdgs tertukar posisi
     * `skema_only`: nilai skema ada di kolom sdgs
     * `sdgs_only`: nilai sdgs ada di kolom skema
     * `invalid_skema` / `invalid_sdgs`: nilai tidak terdaftar di referensi gold.dim_*
     * `sdgs_eq_skema`: anomali regresi nilai sdgs sama dengan skema

2. WRITE (Isolasi di Staging Branch):
   - Mengaktifkan table properties: `write.wap.enabled = true`.
   - Membuat/mengosongkan branch staging `audit-swap`.
   - Menjalankan pembersihan/perbaikan otomatis:
     * Menghapus baris jika `judul_proposal` NULL atau string kosong.
     * Mengembalikan nilai skema dan sdgs yang tertukar ke posisi yang benar.
   - Menulis hasil perbaikan HANYA ke branch staging `audit-swap`. Branch `main` tidak tersentuh.

3. AUDIT (Branch Verification):
   - Menjalankan audit SQL kembali pada data di branch: `<table> VERSION AS OF 'audit-swap'`.

4. PUBLISH (Gated Fast-Forward):
   - Gerbang penentu (Gates):
     * `broken == 0`: tidak boleh ada anomali yang tersisa di branch.
     * `null_judul == 0`: tidak boleh ada baris tanpa judul proposal di branch.
     * `sdgs_eq_skema == 0`: nilai skema dan sdgs tidak boleh identik.
     * `invalid tidak bertambah`: nilai invalid di branch <= main.
     * `total rows sesuai`: baris di branch persis sama dengan (total main - null_judul main).
   - Jika SEMUA gate lolos (PASS):
     * Fast-forward branch `main` ke snapshot branch `audit-swap`.
     * Menghapus branch staging `audit-swap`.
   - Jika gate gagal (FAIL):
     * Publikasi diblokir, branch `audit-swap` dipertahankan untuk inspeksi manual.

Dapat dijalankan secara mandiri:
    spark-submit --deploy-mode client /home/iceberg/pipeline/audit/audit_table.py
"""

import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from setup.setup_spark import SetupSpark
from pyspark.sql import functions as F

# Tabel-tabel silver hibah yang diaudit dan diperbaiki
TABLES = ["silver.penelitian", "silver.pengabdian", "silver.buku_keilmuan"]
BRANCH = "audit-swap"

ALL_HIBAH = (
    "(SELECT * FROM silver.penelitian "
    "UNION ALL SELECT * FROM silver.pengabdian "
    "UNION ALL SELECT * FROM silver.buku_keilmuan)"
)


def load_audit_sql():
    """Membaca template query audit dari pipeline/quality_check/schema_sdgs.sql.

    Menghapus baris komentar SQL (`--`) dan trailing semicolon (`;`) agar
    siap diformat dengan nama relasi `{t}`.
    """
    text = (BASE / "quality_check" / "schema_sdgs.sql").read_text()
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return "\n".join(lines).strip().rstrip(";").strip()


# Template parameter {t} — diformat saat dipanggil di fungsi audit()
AUDIT = load_audit_sql()


def fix_skema_sdgs(spark, src_sql):
    """Logika pembersihan & perbaikan data:

    1. Hapus baris di mana kolom judul_proposal bernilai NULL atau string kosong.
       (Seringkali berasal dari baris spreadsheet kosong yang ikut ter-import).
    2. Cek validitas skema & SDGs terhadap gold.dim_skema dan gold.dim_sdgs.
    3. Perbaiki skema dan SDGs yang posisinya saling tertukar:
       - both_swapped: skema berisi nilai SDGs, sdgs berisi nilai skema -> tukar balik.
       - skema_only: skema kosong namun kolom sdgs berisi nilai skema -> pindahkan.
       - sdgs_only: sdgs kosong namun kolom skema berisi nilai sdgs -> pindahkan.
    """
    t = spark.sql(f"SELECT * FROM {src_sql}")

    # 1. Perbaikan audit: hapus baris yang kolom judul_proposalnya null atau kosong
    t = t.filter(
        F.col("judul_proposal").isNotNull()
        & (F.trim(F.col("judul_proposal")) != "")
    )

    # 2. Ambil nilai referensi master dari Gold dimensions
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

    # 3. Rekonsiliasi nilai swap
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
    return (t.withColumns({"skema": new_skema, "sdgs": new_sdgs})
             .drop("_skema", "_sdgs"))


def audit(spark, t):
    """Menjalankan query audit kualitas data terhadap target relasi/tabel `t`.

    Target `t` dapat berupa nama tabel (e.g. 'silver.penelitian') ataupun
    branch snapshot (e.g. \"silver.penelitian VERSION AS OF 'audit-swap'\").
    """
    row = spark.sql(AUDIT.format(t=t)).first()
    return {k: int(row[k]) for k in row.asDict()}


def broken(a):
    """Menghitung total baris anomali yang dapat diperbaiki otomatis:

    (null_judul + both_swapped + skema_only + sdgs_only).
    """
    return a["null_judul"] + a["both_swapped"] + a["skema_only"] + a["sdgs_only"]


def wap_write(spark, t):
    """Fase WRITE (WAP):

    1. Mengaktifkan konfigurasi WAP pada tabel target.
    2. Membuat branch isolasi `audit-swap`.
    3. Menerapkan pembersihan (fix_skema_sdgs) dan menuliskan hasilnya HANYA
       ke branch staging `audit-swap` via INSERT OVERWRITE. Main branch tetap aman.
    """
    spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"ALTER TABLE {t} DROP BRANCH IF EXISTS `{BRANCH}`")
    spark.sql(f"ALTER TABLE {t} CREATE BRANCH `{BRANCH}`")
    fix_skema_sdgs(spark, t).createOrReplaceTempView("_wap_fix")
    spark.conf.set("spark.wap.branch", BRANCH)
    spark.sql(f"INSERT OVERWRITE TABLE {t} SELECT * FROM _wap_fix")
    spark.conf.unset("spark.wap.branch")


def wap_publish(spark, t):
    """Fase PUBLISH (WAP - Gated):

    1. Membandingkan metrik audit antara branch staging vs branch main.
    2. Mengevaluasi seluruh gerbang kualitas data (Quality Gates):
       - broken == 0: semua anomali teratasi
       - null_judul == 0: tidak ada proposal tanpa judul di branch
       - sdgs_eq_skema == 0: nilai skema dan sdgs tidak bertabrakan
       - invalid tidak bertambah: baris tidak valid tidak bertambah
       - total rows sesuai: jumlah baris tepat berkurang sebanyak baris null yang dihapus
    3. Jika lolos: fast-forward branch main ke branch audit-swap, lalu hapus branch staging.
    4. Jika gagal: publikasi dibatalkan, branch staging tetap disimpan untuk investigasi.
    """
    a_branch = audit(spark, f"{t} VERSION AS OF '{BRANCH}'")
    a_main   = audit(spark, t)

    gates = {
        "broken == 0":         broken(a_branch) == 0,
        "null_judul == 0":     a_branch["null_judul"] == 0,
        "sdgs_eq_skema == 0":  a_branch["sdgs_eq_skema"] == 0,
        "invalid tidak bertambah":
            a_branch["invalid_skema"] + a_branch["invalid_sdgs"]
            <= a_main["invalid_skema"] + a_main["invalid_sdgs"],
        "total rows sesuai (main - null_judul)":
            a_branch["total"] == a_main["total"] - a_main["null_judul"],
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
    """Entry point eksekusi audit dan perbaikan tabel-tabel hibah silver."""
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

    # Audit global: seluruh silver hibah sekaligus (pengecek schema_sdgs.sql)
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

    # Exit code 1 bila ada tabel yang di-BLOCK agar Airflow menangkap kegagalan
    if not all(published.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
