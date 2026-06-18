# Airflow Debugging Cheat Sheet

## 1. Cek Executor Airflow

```bash
airflow config get-value core executor
```

### Kegunaan

Mengetahui task dijalankan di mana.

```text
SequentialExecutor
    ↓
Single Process

LocalExecutor
    ↓
Scheduler Container

CeleryExecutor
    ↓
Worker Container
```

Sebelum mencari log, selalu cek executor dulu.

---

## 2. Cek Daftar Connection

```bash
airflow connections list
```

### Kegunaan

Memastikan connection sudah ada.

Contoh:

```text
minio_s3
postgres_default
spark_default
```

---

## 3. Lihat Detail Connection

```bash
airflow connections get minio_s3
```

### Kegunaan

Verifikasi:

- Connection ID
- Connection Type
- Extra

Apakah endpoint MinIO sudah benar.

---

## 4. Membuat Connection via CLI

```bash
airflow connections add minio_s3 \
    --conn-type aws \
    --conn-extra '...'
```

### Kegunaan

Otomatisasi setup Airflow.

Biasanya dipakai di:

- docker-compose
- airflow-init
- CI/CD

---

## 5. Lihat DAG yang Terdaftar

```bash
airflow dags list
```

### Kegunaan

Memastikan DAG berhasil diparse.

Jika DAG tidak muncul:

- Syntax Error
- Import Error
- Dependency Error

---

## 6. Cek Run DAG

```bash
airflow dags list-runs -d wait_for_xlsx
```

### Kegunaan

Melihat history run.

Contoh:

```text
manual__2026...
success
failed
running
```

---

## 7. Cek Status Semua Task pada DAG Run

```bash
airflow tasks states-for-dag-run \
    wait_for_xlsx \
    <run_id>
```

### Kegunaan

Melihat task mana yang:

```text
success
failed
queued
running
```

---

## 8. Menjalankan Task Secara Manual (Paling Penting)

```bash
airflow tasks test wait_for_xlsx process 2026-06-14
```

### Kegunaan

Menjalankan task tanpa scheduler.

Sangat berguna untuk:

- Debugging
- Unit Testing
- Validasi Connection
- Validasi API
- Validasi S3

Output langsung muncul di terminal.

---

## 9. Cek User Airflow

```bash
airflow users list
```

### Kegunaan

Melihat:

- username
- email
- role

---

## 10. Masuk ke Container Airflow

```bash
docker exec -it lppm-airflow-scheduler bash
```

atau

```bash
docker exec -it lppm-airflow-webserver bash
```

### Kegunaan

Menjalankan seluruh command Airflow CLI.

---

# LOGGING & DEBUGGING

## 11. Cek Log Scheduler

```bash
docker logs lppm-airflow-scheduler
```

Realtime:

```bash
docker logs -f lppm-airflow-scheduler
```

100 baris terakhir:

```bash
docker logs --tail 100 lppm-airflow-scheduler
```

### Kegunaan

Debug:

- DAG tidak muncul
- Scheduler mati
- Task tidak dijadwalkan
- ImportError
- Parsing DAG gagal

---

## 12. Cek Log Webserver

```bash
docker logs lppm-airflow-webserver
```

Realtime:

```bash
docker logs -f lppm-airflow-webserver
```

### Kegunaan

Debug:

- UI tidak bisa diakses
- Login error
- Gunicorn error
- Permission issue

---

## 13. Cek Log Airflow Init

```bash
docker logs airflow-init
```

### Kegunaan

Debug:

- Database migration gagal
- User admin gagal dibuat
- Connection gagal dibuat
- Dependency installation gagal

---

## 14. Cari File Log Airflow

```bash
find /opt/airflow/logs -type f
```

### Kegunaan

Menemukan semua log Airflow.

---

## 15. Cari Log DAG Tertentu

```bash
find /opt/airflow/logs -type f | grep wait_for_xlsx
```

### Kegunaan

Menemukan semua log terkait DAG tertentu.

---

## 16. Cari Log Task Tertentu

```bash
find /opt/airflow/logs -type f | grep process
```

### Kegunaan

Menemukan log task tertentu.

---

## 17. Lihat Isi Log

```bash
cat <logfile>
```

Contoh:

```bash
cat /opt/airflow/logs/dag_id=wait_for_xlsx/run_id=.../task_id=process/attempt=1.log
```

---

## 18. Monitor Log Realtime

```bash
tail -f <logfile>
```

Contoh:

```bash
tail -f /opt/airflow/logs/dag_id=wait_for_xlsx/run_id=.../task_id=process/attempt=1.log
```

### Kegunaan

Monitoring task yang sedang berjalan.

---

## 19. Cek Log Task dari CLI

Lihat run DAG terlebih dahulu:

```bash
airflow dags list-runs -d wait_for_xlsx
```

Kemudian:

```bash
airflow tasks logs \
    wait_for_xlsx \
    process \
    <run_id>
```

### Kegunaan

Melihat log task tertentu tanpa membuka UI.

Contoh:

```text
INFO - Using connection ID 'minio_s3'
INFO - File metadata: {...}
INFO - Marking task as SUCCESS
```

---

## 20. Cek Log Task dari UI

```text
DAG
 ↓
Task
 ↓
Logs
```

### Kegunaan

Melihat:

- print()
- logger.info()
- Exception
- Stack Trace

---

## 21. Cari Error Cepat

Cari ERROR:

```bash
docker logs lppm-airflow-scheduler | grep ERROR
```

Cari Exception:

```bash
docker logs lppm-airflow-scheduler | grep Exception
```

Cari Traceback:

```bash
docker logs lppm-airflow-scheduler | grep Traceback
```

### Kegunaan

Mempercepat root cause analysis.

---

# MINIO / S3 DEBUGGING

## 22. List Object dalam Bucket

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

hook = S3Hook(aws_conn_id="minio_s3")

print(
    hook.list_keys(bucket_name="sipaper")
)
```

### Kegunaan

Melihat object yang benar-benar ada.

Output:

```python
['sipaper.xlsx']
```

---

## 23. Cek Metadata Object

```python
response = client.head_object(
    Bucket="sipaper",
    Key="sipaper.xlsx"
)
```

### Kegunaan

Mendapat:

- Size
- ETag
- LastModified
- ContentType

Tanpa download file.

---

## 24. Download Object

```python
response = client.get_object(
    Bucket="sipaper",
    Key="sipaper.xlsx"
)
```

### Kegunaan

Mengambil isi file.

Biasanya:

```python
pd.read_excel(response["Body"])
```

---

# Logging Best Practice

Jangan:

```python
print(metadata)
```

Gunakan:

```python
import logging

logger = logging.getLogger(__name__)

logger.info("Metadata: %s", metadata)
```

### Kelebihan

- Timestamp otomatis
- Searchable
- Production ready
- Terintegrasi dengan Airflow UI

---

# Workflow Debugging yang Direkomendasikan

```text
1. airflow dags list
        ↓
2. airflow dags list-runs
        ↓
3. airflow tasks states-for-dag-run
        ↓
4. airflow tasks test
        ↓
5. airflow connections get
        ↓
6. airflow tasks logs
        ↓
7. docker logs airflow-scheduler
        ↓
8. docker logs airflow-webserver
        ↓
9. Debug dependency (S3, DB, API)
```

---

# 5 Senjata Utama Debugging Airflow

```bash
airflow tasks test
airflow tasks logs
airflow connections get
docker logs airflow-scheduler
docker logs airflow-webserver
```

Untuk stack:

```text
Airflow
   ↓
MinIO
   ↓
Spark
   ↓
Iceberg
   ↓
Trino
```

90% masalah sehari-hari biasanya bisa ditemukan menggunakan lima command di atas.