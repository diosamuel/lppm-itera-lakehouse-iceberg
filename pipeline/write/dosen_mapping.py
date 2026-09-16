"""WAP dosen: map nama dosen silver.sitasi yang belum match ke gold.dim_dosen.

Alur (urutan di dalam satu WAP, branch `audit-dosen`):

1. AUDIT (main)   : silver.sitasi LEFT JOIN gold.dim_dosen -> hitung yang tidak match
2. WRITE (sitasi) : map nama unmatched/unvalid pakai lookup data-driven dari
                    gold.dim_dosen (root nama via standardize_nama_dosen) ->
                    tulis ke branch `audit-dosen` di silver.sitasi
3. WRITE (dim)    : nama yang TETAP tidak match setelah mapping + valid sebagai person
                    -> insert sbg baris dosen baru ke branch `audit-dosen` di gold.dim_dosen
4. AUDIT (branch) : join sitasi-branch vs dim-branch -> gate publish
5. PUBLISH        : fast_forward silver.sitasi & gold.dim_dosen main -> audit-dosen

Deteksi author-list / non-person (data-driven, TANPA CSV):
  - standardize_nama_dosen(name) == None  -> "Yayasan"/"Penerbit"/sampah -> invalid
  - hasil standardize masih mengandung koma -> daftar penulis (author-list) -> invalid
  - selain itu -> nama orang valid (kandidat dosen baru bila belum match)

Tidak membaca CSV apa pun: lookup nama kanonik dibangun dari data silver/dim itu sendiri
(CSV root hanyalah contoh analisis).

Jalankan (di dalam container spark):
    spark-submit --deploy-mode client /home/iceberg/pipeline/write/dosen_mapping.py
"""

import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from setup.setup_spark import SetupSpark
from tools.nama_dosen_audit import standardize_nama_dosen

SITASI = "silver.sitasi"
DIM = "gold.dim_dosen"
BRANCH = "audit-dosen"


def load_audit_sql():
    """Baca template audit dari pipeline/quality_check/schema_dosen.sql.

    Buang baris komentar `--` dan trailing `;` agar siap di-format {sitasi}/{dim}.
    """
    text = (BASE / "quality_check" / "schema_dosen.sql").read_text()
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return "\n".join(lines).strip().rstrip(";").strip()


# template parameter {sitasi}/{dim} — di-format saat dipanggil di audit()
AUDIT = load_audit_sql()


def is_invalid_person(name):
    """True bila nama bukan orang valid: author-list / non-person.

    standardize_nama_dosen menghapus gelar & prefix. Bila hasilnya None -> non-person
    ("Yayasan", "Penerbit", sampah). Bila hasilnya masih mengandung koma -> daftar
    beberapa penulis (koma pemisah orang, bukan koma pemisah gelar yang sudah dibuang).
    """
    if name is None:
        return True
    name = str(name).strip()
    if not name:
        return True
    std = standardize_nama_dosen(name)
    if std is None:
        return True
    return "," in std


def register_invalid_udf(spark):
    """Register UDF deteksi invalid (author-list/non-person) utk audit SQL."""
    spark.udf.register("is_invalid_person_udf", is_invalid_person, BooleanType())
    return F.udf(is_invalid_person, BooleanType())


def build_name_lookup(spark):
    """Bangun lookup root-nama -> nama kanonik dari gold.dim_dosen (data silver).

    root = standardize_nama_dosen(nama) — strip gelar/punctuation, uppercase.
    Bila satu root punya banyak nama kanonik (variasi gelar), pilih yang terpanjang
    (paling lengkap). Return dict {root: nama_kanonik}.
    """
    rows = spark.table(DIM).select("nama").distinct().collect()
    lookup = {}
    for r in rows:
        nama = r["nama"]
        if not nama:
            continue
        root = standardize_nama_dosen(nama)
        if not root or "," in root:
            # nama dim yang aneh / bukan person — lewati
            continue
        cur = lookup.get(root)
        if cur is None or len(nama) > len(cur):
            lookup[root] = nama
    return lookup


def map_unmatched_dosen(sitasi_df, lookup):
    """Map ketua_peneliti yang belum match ke nama kanonik dim_dosen.

    - kosong / invalid (author-list, yayasan, penerbit) -> dibiarkan
    - persis sudah ada di dim -> dibiarkan (sudah match)
    - root-nya cocok lookup -> ganti ke nama kanonik
    - selain itu -> dibiarkan (kandidat dosen baru)

    Dijalankan sebagai UDF biasa (lookup di-broadcast via closure).
    """
    def _map(name):
        if name is None:
            return None
        name = str(name).strip()
        if not name:
            return name
        root = standardize_nama_dosen(name)
        if root is None or "," in root:
            # invalid / author-list — biarkan apa adanya (hanya diaudit)
            return name
        return lookup.get(root, name)

    map_udf = F.udf(_map)
    return sitasi_df.withColumn("ketua_peneliti", map_udf(F.col("ketua_peneliti")))


def audit(spark, sitasi_ref, dim_ref):
    """Audit kategori match sitasi vs dim_dosen.

    sitasi_ref / dim_ref: nama tabel ("silver.sitasi") atau snapshot branch
    ("silver.sitasi VERSION AS OF 'audit-dosen'").
    """
    row = spark.sql(AUDIT.format(sitasi=sitasi_ref, dim=dim_ref)).first()
    return {k: int(row[k]) for k in row.asDict()}


def wap_write_sitasi(spark, lookup):
    """WRITE sitasi: map nama ke branch `audit-dosen` (main tidak tersentuh)."""
    spark.sql(f"ALTER TABLE {SITASI} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"ALTER TABLE {SITASI} DROP BRANCH IF EXISTS `{BRANCH}`")
    spark.sql(f"ALTER TABLE {SITASI} CREATE BRANCH `{BRANCH}`")

    mapped = map_unmatched_dosen(spark.table(SITASI), lookup)
    mapped.createOrReplaceTempView("_wap_sitasi_map")
    spark.conf.set("spark.wap.branch", BRANCH)
    spark.sql(f"INSERT OVERWRITE TABLE {SITASI} SELECT * FROM _wap_sitasi_map")
    spark.conf.unset("spark.wap.branch")
    print(f"  [WAP] {SITASI} ditulis ke branch `{BRANCH}`")


def wap_write_dim_dosen(spark):
    """WRITE dim: insert dosen baru (nama sitasi valid yang belum match) ke branch.

    Kandidat = nama distinct dari silver.sitasi yang:
      - tidak kosong
      - valid sebagai person (bukan author-list / non-person)
      - belum match ke dim_dosen.nama (anti-join)
    dosen_id = xxhash64(nama, '') cast int — konsisten dengan dim_dosen.sql.
    """
    spark.sql(f"ALTER TABLE {DIM} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"ALTER TABLE {DIM} DROP BRANCH IF EXISTS `{BRANCH}`")
    spark.sql(f"ALTER TABLE {DIM} CREATE BRANCH `{BRANCH}`")

    invalid_udf = register_invalid_udf(spark)

    cand = (
        spark.sql(
            "SELECT DISTINCT ketua_peneliti AS nama FROM silver.sitasi "
            "WHERE ketua_peneliti IS NOT NULL AND TRIM(ketua_peneliti) <> ''"
        )
        .withColumn("_invalid", invalid_udf(F.col("nama")))
        .filter(~F.col("_invalid"))
        .drop("_invalid")
        .join(spark.table(DIM).select("nama"), on="nama", how="anti")
        .withColumn("dosen_id", F.xxhash64(F.col("nama"), F.lit("")).cast("int"))
        .withColumn("nip", F.lit(None).cast("string"))
        .select("dosen_id", "nama", "nip")
    )

    n_new = cand.count()
    print(f"  [WAP] {n_new} dosen baru akan di-insert ke {DIM}")
    if n_new == 0:
        # tidak ada yang perlu di-insert — branch sama dengan main
        spark.table(DIM).createOrReplaceTempView("_wap_dim_full")
    else:
        full = spark.table(DIM).unionByName(cand, allowMissingColumns=True)
        full.createOrReplaceTempView("_wap_dim_full")

    spark.conf.set("spark.wap.branch", BRANCH)
    spark.sql(f"INSERT OVERWRITE TABLE {DIM} SELECT * FROM _wap_dim_full")
    spark.conf.unset("spark.wap.branch")
    print(f"  [WAP] {DIM} ditulis ke branch `{BRANCH}`")


def wap_publish(spark):
    """PUBLISH (gated): fast_forward kedua tabel ke main hanya jika audit branch lolos.

    Gate:
      - unmatched_valid == 0     (semua nama valid sudah match / jadi dosen baru)
      - invalid_rows tidak berubah (author-list/non-person dibiarkan, hanya diaudit)
      - empty_rows tidak berubah   (completeness di luar scope)
      - total rows sitasi sama     (mapping 1:1, tidak ada baris hilang)
    """
    a_main = audit(spark, SITASI, DIM)
    a_branch = audit(
        spark,
        f"{SITASI} VERSION AS OF '{BRANCH}'",
        f"{DIM} VERSION AS OF '{BRANCH}'",
    )

    gates = {
        "unmatched_valid == 0": a_branch["unmatched_valid"] == 0,
        "unmatched_valid turun": a_branch["unmatched_valid"] < a_main["unmatched_valid"],
        "invalid_rows tidak berubah": a_branch["invalid_rows"] == a_main["invalid_rows"],
        "empty_rows tidak berubah": a_branch["empty_rows"] == a_main["empty_rows"],
        "total rows sitasi sama": a_branch["total"] == a_main["total"],
    }
    print(f"  audit main   -> {a_main}")
    print(f"  audit branch -> {a_branch}")
    for name, ok in gates.items():
        print(f"    gate [{name}]: {'PASS' if ok else 'FAIL'}")

    if all(gates.values()):
        spark.sql(f"CALL default.system.fast_forward('{SITASI}', 'main', '{BRANCH}')")
        spark.sql(f"CALL default.system.fast_forward('{DIM}', 'main', '{BRANCH}')")
        spark.sql(f"ALTER TABLE {SITASI} DROP BRANCH IF EXISTS `{BRANCH}`")
        spark.sql(f"ALTER TABLE {DIM} DROP BRANCH IF EXISTS `{BRANCH}`")
        print("  PUBLISHED — main di-fast-forward ke branch `audit-dosen`")
        return True

    print(f"  BLOK PUBLISH — branch `{BRANCH}` dipertahankan untuk inspeksi:")
    print(f"    SELECT * FROM {SITASI} VERSION AS OF '{BRANCH}'")
    print(f"    SELECT * FROM {DIM} VERSION AS OF '{BRANCH}'")
    return False


def main():
    spark = SetupSpark(
        app_name="wap-dosen-mapping", catalog_name="default"
    ).initialize()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE default")

    register_invalid_udf(spark)

    print("[WAP dosen] 1. audit main")
    a_main = audit(spark, SITASI, DIM)
    print(f"  -> {a_main}")

    print("[WAP dosen] 2. bangun lookup nama kanonik dari dim_dosen")
    lookup = build_name_lookup(spark)
    print(f"  lookup: {len(lookup)} root nama")

    print("[WAP dosen] 3. write sitasi (map nama) ke branch")
    wap_write_sitasi(spark, lookup)

    print("[WAP dosen] 4. write dim_dosen (insert dosen baru) ke branch")
    wap_write_dim_dosen(spark)

    print("[WAP dosen] 5. publish (gated)")
    ok = wap_publish(spark)
    print(f"\n=== ringkasan: {'PUBLISHED' if ok else 'BLOCKED'} ===")
    spark.stop()

    # Exit code 1 bila di-BLOCK — biar Airflow tidak lapor sukses palsu
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
