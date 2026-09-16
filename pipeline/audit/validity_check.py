"""Audit: Validity — nilai kolom tidak match dengan dim_*.

Mengembalikan list of rows untuk ditulis ke dq.dq_report.

Jalankan di dalam container spark:
    from audit.validity_check import checkValidity
"""


def checkValidity(spark, table, column, ref_table, ref_column, run_id, checked_at, buildRow, threshold=0.0):
    """Validity: nilai kolom tidak ditemukan di tabel referensi dim_*.

    Args:
        spark: SparkSession
        table: nama tabel sumber (e.g. "silver.penelitian")
        column: nama kolom yang dicek (e.g. "skema")
        ref_table: tabel referensi (e.g. "gold.dim_skema")
        ref_column: kolom referensi (e.g. "nama_skema")
        run_id: UUID run identifier
        checked_at: timestamp UTC
        buildRow: function to build DQ result row
        threshold: allowed violation threshold (default 0.0)

    Returns:
        list of tuples matching dq.dq_report schema
    """
    q = f"""
    SELECT count(*) AS v
    FROM {table} h
    LEFT JOIN {ref_table} r
      ON LOWER(TRIM(CAST(h.{column} AS string))) = LOWER(TRIM(CAST(r.{ref_column} AS string)))
    WHERE h.{column} IS NOT NULL AND TRIM(CAST(h.{column} AS string)) <> \'\' AND r.{ref_column} IS NULL
    """
    v = spark.sql(q).first()["v"]
    total = spark.sql(f"SELECT count(*) AS c FROM {table} WHERE {column} IS NOT NULL").first()["c"]
    return [buildRow(run_id, checked_at, f"valid_{column}", table, column, "validity", v, total)]
