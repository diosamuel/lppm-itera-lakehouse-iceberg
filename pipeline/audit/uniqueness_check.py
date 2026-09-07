"""Audit: Uniqueness — nilai kolom harus unik (tidak ada duplikat).

Mengembalikan list of rows untuk ditulis ke dq.dq_report.

Jalankan di dalam container spark:
    from audit.uniqueness_check import checkUniqueness
"""


def checkUniqueness(spark, table, column, run_id, checked_at, buildRow, threshold=0.0):
    """Uniqueness: hitung duplikat per kolom.

    Args:
        spark: SparkSession
        table: nama tabel sumber (e.g. "silver.penelitian")
        column: nama kolom yang dicek (e.g. "id")
        run_id: UUID run identifier
        checked_at: timestamp UTC
        buildRow: function to build DQ result row
        threshold: allowed violation threshold (default 0.0)

    Returns:
        list of tuples matching dq.dq_report schema
    """
    total = spark.sql(f"SELECT count(*) AS c FROM {table}").first()["c"]
    v = spark.sql(
        f"SELECT count(*) AS c FROM {table} "
        f"WHERE {column} IN (SELECT {column} FROM {table} GROUP BY {column} HAVING count(*) > 1)"
    ).first()["c"]
    return [buildRow(run_id, checked_at, f"unique_{column}", table, column, "uniqueness", v, total)]
