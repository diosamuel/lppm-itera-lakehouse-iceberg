# LPPM ITERA Lakehouse — Project Instructions

Research-grant analytics lakehouse for LPPM ITERA (Institut Teknologi Sumatera).
Ingests SIPAPER Excel data (penelitian, pengabdian, buku keilmuan, sitasi) into a
medallion lakehouse and serves BI dashboards.

## Stack & Services (docker-compose.yaml)

- **Spark 3.5 (image `diosamuel/lppm-spark-iceberg`)** — master `spark://spark-iceberg:7077`, container `lppm-spark-iceberg`
- **Iceberg REST catalog** — `apache/iceberg-rest-fixture:1.10.1`, container `lppm-iceberg-rest`, URI `http://rest:8181`
- **MinIO** (S3) — container `lppm-minio`, endpoint `http://minio:9000`, warehouse `s3://warehouse/`, raw bucket `sipaper`
- **Trino** — container `lppm-trino`, port 8085, catalog `default` (query engine for Superset)
- **Airflow** — image `diosamuel/lppm-airflow`, orchestrates the pipeline
- **Superset** (+ `superset-redis`) — BI dashboards, connects via Trino
- **Postgres** — container `lppm-postgres`, shared metadata
- Python 3.12, PySpark; Excel ingestion uses `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`

## Commands

```bash
./start.sh                     # docker compose up -d + spark-submit pipeline/index.py (full run)
docker compose exec spark-iceberg spark-submit --deploy-mode client /home/iceberg/pipeline/index.py
docker compose exec spark-iceberg spark-submit --deploy-mode client /home/iceberg/pipeline/run.py [category ...]
./reset_iceberg_tables.sh      # drop & recreate Iceberg tables (destructive)
```

- `pipeline/run.py` — full entry point: setup MinIO + catalog + Spark → bronze ingest → silver+gold. Accepts category args (default: all of `penelitian, pengabdian, buku_keilmuan, sitasi`).
- `pipeline/index.py` — `run_silver_gold(spark, bronze_cache=None, categories=None)`, builds silver tables (scoped by `categories` when given) and always rebuilds all gold fact/dim tables.
- `pipeline/bronze.py` — `CATEGORIES` map (raw XLSX paths in MinIO, overridable via env: `RAW_XLSX_PATH` etc.).
- Airflow DAG `airflow/dags/lake_to_warehouse.py` — daily, manifest-based: detects changed raw files in MinIO, runs pipeline only for changed categories.

## Data Model (namespaces in catalog `default`)

- **bronze** — raw per-category tables, one row-set per year sheet, with `tahun`
- **silver** — cleaned: `silver.penelitian`, `silver.pengabdian`, `silver.buku_keilmuan`, ... IDs are `xxhash64` of (judul_proposal, ketua_peneliti, tahun) prefixed per category (e.g. `PENELITIAN-...`)
- **gold** — dims: `dim_hibah_proposal`, `dim_dosen`, `dim_jurnal`, `dim_prodi`, `dim_skema`, `dim_sdgs`; facts: `fact_hibah`, `fact_dosen_hibah`, `fact_sitasi` (DDL in `pipeline/schema/*.sql`, applied via `run_sql_file`)
- **audit** — (in progress) audit/DQ tracking tables, never exposed to BI

## Key Modules

- `pipeline/setup/` — `SetupSpark` (builds SparkSession w/ REST catalog + S3FileIO + PYTHONPATH so pipeline modules are importable in UDFs), `SetupIcebergCatalog` (`.initialize()`, `.create_namespace()`), `SetupMinioS3`
- `pipeline/transform/` — `Transform` class (silver builder, per-year `processData` + `join`), `xlsx_clean.py`, `jurnal_clean.py`, `extract_pdf.py`
- `pipeline/tools/nama_dosen_audit.py` — dosen-name helpers/UDFs: `clean_dosen_name_udf`, `standardize_nama_dosen_udf`, `is_valid_dosen_name_udf`. **Reuse these; do not duplicate.**
- `pipeline/tools/dosen_name_mapper.py` — maps raw names to `dim_dosen` (see `dosen_name_mapping.csv` at repo root)
- `pipeline/audit/audit_sdgs_skema.py` + `pipeline/tools/audit_sdgs_skema.sql` — detect/repair swapped skema↔SDGs values
- `pipeline/quality_check/` — DQ SQL checks (growing)

## Conventions

- Everything must be **idempotent** (safe to re-run: `createOrReplace`, `IF EXISTS/IF NOT EXISTS`, MERGE).
- `run_id` (UUID / Airflow run_id) is the end-to-end trace key for a pipeline run.
- SQL DDL lives in `pipeline/schema/*.sql`, executed by splitting on `;` (no trailing comments inside statements).
- Spark jobs run inside the `lppm-spark-iceberg` container; use `docker exec lppm-spark-iceberg spark-submit --deploy-mode client /home/iceberg/<path>`.
- Config via `.env` (see `.env.example`): `MINIO_*`, `REST_CATALOG_URL`, `TRINO_*`. Don't commit secrets.
- Trino queries gold via catalog `default`; Superset should only read published `main` data, never staging/audit branches.

## Roadmap (active work)

`notes/TODO.md` — implementing **Write-Audit-Publish (WAP)**: gold writes go to Iceberg branch `audit-swap` (`write.wap.enabled`), audits run on the branch, then `FAST-FORWARD main TO audit-swap` on approval; plus DQ monitoring (SQL + Python runners, `audit.dq_results`, separate Airflow DAG `data_quality_check`) and Superset dashboards. Follow the 12-step build order in TODO.md §3.

## Notes / Docs (authoritative references)

- `notes/TODO.md` — WAP & DQ roadmap
- `notes/schema-pdf.md` — SIPAPER document formats (proposal sections)
- `notes/KPI.md`, `notes/KPI_SUPERSET.md` — KPI definitions & dashboard specs
- `notes/WAP for Dosen.md` — dosen name clustering/dedup rules
- `notes/Grant.md`, `notes/PendanaanITERA.md` — funding schemes background

## Working Style

- The project mixes Indonesian and English in docs/code comments — keep consistent with surrounding files.
- Prefer editing existing modules over creating parallel ones.
- Test spark-submit commands against the running containers before claiming success.
