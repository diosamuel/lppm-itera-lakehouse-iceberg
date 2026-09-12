"""Rebuild gold tables downstream of the data_quality_check repairs.

data_quality_check repairs silver.penelitian/pengabdian/buku_keilmuan (purge
null judul, swap skema/sdgs) and silver.sitasi (dosen name mapping). Gold is
built from silver, so it goes stale after the repairs. This rebuilds ONLY the
affected gold tables — dim_hibah_proposal, fact_hibah, fact_dosen_hibah,
dim_dosen, dim_jurnal, fact_sitasi — and leaves the static dims
(dim_prodi, dim_skema, dim_sdgs) untouched, unlike run.py which rebuilds all gold.

Jalankan (di dalam container spark):
    spark-submit --deploy-mode client /home/iceberg/pipeline/rebuild_gold.py
"""

import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

from index import rebuildGoldDependents
from setup.setup_spark import SetupSpark


def main():
    spark = SetupSpark(
        app_name="rebuild-gold-dependents", catalog_name="default"
    ).initialize()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE default")

    print("[rebuild gold] dependents of data_quality_check repairs")
    try:
        rebuildGoldDependents(spark)
    except Exception as exc:
        print(f"[rebuild gold] GAGAL: {exc}")
        spark.stop()
        sys.exit(1)

    print("\n=== ringkasan: gold dependents rebuilt ===")
    spark.stop()


if __name__ == "__main__":
    main()
