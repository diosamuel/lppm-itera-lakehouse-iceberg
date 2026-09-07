from pyspark.sql import functions as F

def checkNulls(spark, table, columns, run_id, checked_at, buildRow, threshold=0.0):
    total = spark.sql(f"SELECT count(*) AS c FROM {table}").first()["c"]
    rows = []
    for col in columns:
        v = spark.sql(
            f"SELECT count(*) AS c FROM {table} "
            f"WHERE {col} IS NULL OR TRIM(CAST({col} AS string)) = ''"
        ).first()["c"]
        rows.append(buildRow(run_id, checked_at, f"not_null_{col}", table, col, "completeness", v, total))
    return rows
