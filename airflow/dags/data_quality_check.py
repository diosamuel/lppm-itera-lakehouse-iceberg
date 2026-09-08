"""DAG: Data Quality Check + WAP repair (skema silver + dosen sitasi).

Alur check → repair → check:
  1. dq_pre   : pipeline/quality_check/dq_runner.py  (baseline, ditulis ke dq.dq_report)
  2. wap_swap : pipeline/audit/audit_table.py       (fix null judul + skema/sdgs tertukar, branch audit-swap)
  3. wap_dosen: pipeline/write/dosen_mapping.py      (map + insert dosen sitasi, branch audit-dosen)
  4. dq_post  : pipeline/quality_check/dq_runner.py  (verifikasi setelah repair)

Kedua WAP melempar exit code 1 saat publish di-BLOCK (gate gagal). Karena
@task.bash mempropagasi exit code, DAG otomatis berhenti (task gagal) dan
dq_post TIDAK dijalankan — tidak ada laporan sukses palsu.

Semua logika audit/WAP ada di pipeline/write + pipeline/quality_check —
DAG hanya orkestrasi (no redundant code).

Jalankan SETELAH DAG lake_to_warehouse agar silver+gold sudah diperbarui
(rebuild gold.dim_dosen akan menghapus insert WAP dosen, jadi urutan ini penting).
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="data_quality_check",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 * * *",  # 00:30 WIB, setelah lake_to_warehouse (@daily)
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    description="DQ check -> WAP repair (skema/sdgs + dosen) -> DQ check ulang",
)
def dataQualityCheck():
    @task.bash
    def dq_pre():
        return (
            "docker exec lppm-spark-iceberg spark-submit --deploy-mode client "
            "/home/iceberg/pipeline/quality_check/dq_runner.py"
        )

    @task.bash
    def wap_swap_skema_sdgs():
        return (
            "docker exec lppm-spark-iceberg spark-submit --deploy-mode client "
            "/home/iceberg/pipeline/audit/audit_table.py"
        )

    @task.bash
    def wap_dosen_mapping():
        return (
            "docker exec lppm-spark-iceberg spark-submit --deploy-mode client "
            "/home/iceberg/pipeline/write/dosen_mapping.py"
        )

    @task.bash
    def dq_post():
        return (
            "docker exec lppm-spark-iceberg spark-submit --deploy-mode client "
            "/home/iceberg/pipeline/quality_check/dq_runner.py"
        )

    pre = dq_pre()
    swap = wap_swap_skema_sdgs()
    dosen = wap_dosen_mapping()
    post = dq_post()

    pre >> swap >> dosen >> post


dataQualityCheck()
