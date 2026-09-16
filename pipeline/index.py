import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from pyspark.sql import functions as F
# from audit.audit_table import TABLES as WAP_TABLES, wap_publish, wap_write
from setup.setup_catalog import SetupIcebergCatalog
from setup.setup_spark import SetupSpark
from tools.dosen_name_mapper import map_dosen_name_udf
from transform.extract_transform import Transform

def run_sql_file(spark, sql_file):
    sql_text = (BASE_DIR / "schema" / sql_file).read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    for statement in statements:
        spark.sql(statement)


def year_sheets_from_bronze(spark, table):
    rows = (
        spark.read.table(table)
        .select("tahun")
        .distinct()
        .orderBy("tahun")
        .collect()
    )
    return [str(r.tahun) for r in rows]


def _bronze_df(spark, category, bronze_cache=None):
    if bronze_cache and category in bronze_cache:
        return bronze_cache[category]
    return spark.read.table(f"bronze.{category}")


def buildSilverHibah(spark, category, bronze_cache, id_prefix):
    builder = Transform(spark=spark, document_type=category)
    for sheet in year_sheets_from_bronze(spark, f"bronze.{category}"):
        df = (
            _bronze_df(spark, category, bronze_cache)
            .filter(F.col("tahun") == int(sheet))
            .drop("tahun")
        )
        builder.processData(df, int(sheet))
    res = builder.join()
    res = res.withColumn(
        "id",
        F.concat(
            F.lit(id_prefix),
            F.xxhash64(
                F.coalesce(F.col("judul_proposal"), F.lit("")),
                F.coalesce(F.col("ketua_peneliti"), F.lit("")),
                F.col("tahun"),
            ).cast("string"),
        ),
    )
    res.writeTo(f"silver.{category}").createOrReplace()
    print(f"Written silver.{category}")


def buildSilverSitasi(spark, bronze_cache):
    builder = Transform(spark=spark, document_type="sitasi")
    for sheet in year_sheets_from_bronze(spark, "bronze.sitasi"):
        df = (
            _bronze_df(spark, "sitasi", bronze_cache)
            .filter(F.col("tahun") == int(sheet))
            .drop("tahun")
        )
        builder.processSitasiData(df, int(sheet))
    res = builder.join()
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


SILVER_BUILDERS = {
    "penelitian": lambda spark, cache: buildSilverHibah(spark, "penelitian", cache, "PENELITIAN-"),
    "pengabdian": lambda spark, cache: buildSilverHibah(spark, "pengabdian", cache, "PENGABDIAN-"),
    "buku_keilmuan": lambda spark, cache: buildSilverHibah(spark, "buku_keilmuan", cache, "BUKU_KEILMUAN-"),
    "sitasi": buildSilverSitasi,
}


GOLD_DDL_FILES = [
    "dim_prodi.sql",
    "dim_skema.sql",
    "dim_sdgs.sql",
    "dim_dosen.sql",
    "dim_jurnal.sql",
    "dim_hibah_proposal.sql",
    "fact_hibah.sql",
    "fact_dosen_hibah.sql",
    "fact_sitasi.sql",
]


def buildGoldTable(spark):
    for sql_file in GOLD_DDL_FILES:
        run_sql_file(spark, sql_file)
        print(f"Written gold.{sql_file.removesuffix('.sql')}")


# Gold tables downstream of the silver tables repaired by the data_quality_check
# DAG (silver hibah + silver.sitasi). The static dims (dim_prodi, dim_skema,
# dim_sdgs) are deliberately excluded, so a rebuild does not touch all of gold.
# dim_dosen is handled separately in rebuildGoldDependents (see below).
GOLD_DEPENDENT_FILES = [
    "dim_hibah_proposal.sql",
    "fact_hibah.sql",
    "fact_dosen_hibah.sql",
    "dim_jurnal.sql",
    "fact_sitasi.sql",
]


def rebuildGoldDependents(spark):
    """Rebuild only the gold tables downstream of the data_quality_check repairs.

    The DAG repairs silver.penelitian/pengabdian/buku_keilmuan (purge null judul,
    swap skema/sdgs) and silver.sitasi (dosen mapping). Gold was built from the
    pre-repair silver, so those tables are stale. Order matters:

    1. dim_dosen is rebuilt from its hibah-only DDL first. This intentionally
       drops the sitasi-derived dosen rows the dosen WAP had added.
    2. The dosen WAP is re-applied, re-inserting those sitasi dosen. It must run
       before fact_sitasi, which joins dim_dosen to resolve dosen_id.
    3. The remaining dependent dims/facts are rebuilt.
    """
    from write.dosen_mapping import (
        build_name_lookup,
        register_invalid_udf,
        wap_publish,
        wap_write_dim_dosen,
        wap_write_sitasi,
    )

    run_sql_file(spark, "dim_dosen.sql")
    print("Written gold.dim_dosen")

    register_invalid_udf(spark)
    lookup = build_name_lookup(spark)
    wap_write_sitasi(spark, lookup)
    wap_write_dim_dosen(spark)
    if not wap_publish(spark):
        raise RuntimeError("dosen WAP publish diblokir — rebuild gold dibatalkan")

    for sql_file in GOLD_DEPENDENT_FILES:
        run_sql_file(spark, sql_file)
        print(f"Written gold.{sql_file.removesuffix('.sql')}")


def runSilverGold(spark, bronze_cache=None, categories=None):
    """Build silver and gold tables.

    Args:
        spark: active SparkSession.
        bronze_cache: optional {category: DataFrame} cache from the bronze
            ingest step (avoids re-reading changed categories).
        categories: iterable of silver categories to rebuild. None (default) means
            all categories. Silver tables outside `categories` are left untouched;
            gold is always fully rebuilt from the resulting silver tables.
    """
    if categories is None:
        categories = list(SILVER_BUILDERS.keys())
    categories = set(categories)

    for category, build in SILVER_BUILDERS.items():
        if category not in categories:
            print(f"Skipped silver.{category} (not in changed categories)")
            continue
        build(spark, bronze_cache)

    # print("Running WAP audit & repair on silver tables...")
    # for t in WAP_TABLES:
    #     wap_write(spark, t)
    #     wap_publish(spark, t)

    buildGoldTable(spark)


if __name__ == "__main__":
    IcebergCatalog = SetupIcebergCatalog(
        catalog_name="default",
        namespace="silver",
    ).initialize()
    # IcebergCatalog.create_namespace("bronze")
    # IcebergCatalog.create_namespace("gold")
    # audit/DQ namespace — tabel hasil data-quality check (dq.dq_results, dst.)
    # IcebergCatalog.create_namespace("dq")
    SparkSession = SetupSpark(
        app_name="sipaper",
        catalog_name="default",
    ).initialize()
    runSilverGold(SparkSession)
