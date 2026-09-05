from pathlib import Path

from pyspark.sql import functions as F
from setup.setup_catalog import SetupIcebergCatalog
from setup.setup_spark import SetupSpark
from tools.dosen_name_mapper import map_dosen_name_udf
from transform.extract_transform import Transform

BASE_DIR = Path(__file__).resolve().parent

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


def _build_silver_hibah(spark, category, bronze_cache, id_prefix):
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


def _build_silver_sitasi(spark, bronze_cache):
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
    "penelitian": lambda spark, cache: _build_silver_hibah(spark, "penelitian", cache, "PENELITIAN-"),
    "pengabdian": lambda spark, cache: _build_silver_hibah(spark, "pengabdian", cache, "PENGABDIAN-"),
    "buku_keilmuan": lambda spark, cache: _build_silver_hibah(spark, "buku_keilmuan", cache, "BUKU_KEILMUAN-"),
    "sitasi": _build_silver_sitasi,
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


def _build_gold(spark):
    for sql_file in GOLD_DDL_FILES:
        run_sql_file(spark, sql_file)
        print(f"Written gold.{sql_file.removesuffix('.sql')}")


def run_silver_gold(spark, bronze_cache=None, categories=None):
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

    _build_gold(spark)


if __name__ == "__main__":
    IcebergCatalog = SetupIcebergCatalog(
        catalog_name="default",
        namespace="silver",
    ).initialize()
    IcebergCatalog.create_namespace("bronze")
    IcebergCatalog.create_namespace("gold")
    # audit/DQ namespace — tabel hasil data-quality check (dq.dq_results, dst.)
    IcebergCatalog.create_namespace("dq")
    SparkSession = SetupSpark(
        app_name="sipaper",
        catalog_name="default",
    ).initialize()
    run_silver_gold(SparkSession)
