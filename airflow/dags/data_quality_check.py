"""DAG: Data Quality Check (skema silver).

Menjalankan pipeline/quality_check/dq_runner.py di dalam container spark.
Semua logika DQ ada di dq_runner.py — DAG hanya orkestrasi (no redundant code).

Check yang dijalankan (lihat dq_runner.py untuk detail):
  1. Consistency — anomaly tertukar skema/sdgs
  2. Completeness — NULL pada kolom wajib
  3. Referential integrity — skema/sdgs/prodi vs dim_*

Hasil ditulis ke Iceberg `dq.dq_results` (namespace `dq` dibuat oleh
pipeline/index.py). Jalankan setelah DAG lake_to_warehouse agar silver
sudah diperbarui.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(
    dag_id="data_quality_check",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 * * *",  # 00:30 WIB, setelah lake_to_warehouse (@daily)
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Data quality check pada skema silver -> dq.dq_results",
)
def dataQualityCheck():
    BashOperator(
        task_id="run_dq_check",
        bash_command=(
            "docker exec lppm-spark-iceberg spark-submit --deploy-mode client "
            "/home/iceberg/pipeline/quality_check/dq_runner.py"
        ),
    )


dataQualityCheck()
