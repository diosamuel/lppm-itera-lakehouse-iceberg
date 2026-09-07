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
docker compose exec spark-iceberg spark-submit --deploy-mode client /home/iceberg/pipeline/quality_check/dq_runner.py
./reset_iceberg_tables.sh      # drop & recreate Iceberg tables (destructive)
```

- `pipeline/run.py` — full entry point: setup MinIO + catalog + Spark → bronze ingest → silver+gold. Accepts category args (default: all of `penelitian, pengabdian, buku_keilmuan, sitasi`).
- `pipeline/index.py` — `run_silver_gold(spark, bronze_cache=None, categories=None)`, builds silver tables (scoped by `categories` when given) and always rebuilds all gold fact/dim tables.
- `pipeline/bronze.py` — `CATEGORIES` map (raw XLSX paths in MinIO, overridable via env: `RAW_XLSX_PATH` etc.).
- `pipeline/quality_check/dq_runner.py` — DQ runner (manual spark-submit): checks silver tables, appends results to `dq.dq_report`. Check-only — detects, never repairs/publishes. NOT wired into `index.py` or the Airflow DAG.
- Airflow DAG `airflow/dags/lake_to_warehouse.py` — daily, manifest-based: detects changed raw files in MinIO, runs pipeline only for changed categories.
- Query via Trino: `docker exec lppm-trino trino --server http://localhost:8085 --catalog default --execute "SELECT ..."` (note: default port 8080 does not work — Trino listens on 8085).

## Data Model (namespaces in catalog `default`)

- **bronze** — raw per-category tables, one row-set per year sheet, with `tahun`
- **silver** — cleaned: `silver.penelitian`, `silver.pengabdian`, `silver.buku_keilmuan`, ... IDs are `xxhash64` of (judul_proposal, ketua_peneliti, tahun) prefixed per category (e.g. `PENELITIAN-...`)
- **gold** — dims: `dim_hibah_proposal`, `dim_dosen`, `dim_jurnal`, `dim_prodi`, `dim_skema`, `dim_sdgs`; facts: `fact_hibah`, `fact_dosen_hibah`, `fact_sitasi` (DDL in `pipeline/schema/*.sql`, applied via `run_sql_file`)
- **dq** — `dq.dq_report`: DQ results per `run_id` (written by `quality_check/dq_runner.py`), never exposed to BI
- **audit** — (planned, see TODO.md) audit/WAP tracking tables; namespace not created yet

## Key Modules

- `pipeline/setup/` — `SetupSpark` (builds SparkSession w/ REST catalog + S3FileIO + PYTHONPATH so pipeline modules are importable in UDFs), `SetupIcebergCatalog` (`.initialize()`, `.create_namespace()`), `SetupMinioS3`
- `pipeline/transform/` — `Transform` class (silver builder, per-year `processData` + `join`), `xlsx_clean.py`, `jurnal_clean.py`, `extract_pdf.py`
- `pipeline/tools/nama_dosen_audit.py` — dosen-name helpers: `clean_dosen_name`, `preclean_dosen_name`, `standardize_nama_dosen`. **Reuse these; do not duplicate.**
- `pipeline/tools/dosen_name_mapper.py` — maps raw names to `dim_dosen` (see `dosen_name_mapping.csv` at repo root); single consumer of `standardize_nama_dosen`. Applied to silver.sitasi `ketua_peneliti` at silver build.
- `pipeline/audit/` — DQ check helpers consumed by `dq_runner.py`: `null_check.py` (completeness), `validity_check.py` (ref-table match — incl. `valid_ketua_peneliti`: silver.sitasi dosen vs `gold.dim_dosen.nama`, currently 19/190 unmatched), `uniqueness_check.py`
- `pipeline/audit/swapped_skema_sdgs.py` + `pipeline/quality_check/schema_sdgs.sql` — detect/repair swapped skema↔SDGs values. Contains a working standalone WAP flow (`wap_write` → branch `audit-swap` → gates → `wap_publish` fast-forward), but it is NOT wired into the pipeline/Airflow — run manually.
- Unmatched sitasi dosen (fix pending, discussed later): root CSVs define the taxonomy — `dosen_name_mapping.csv` (variant→dim), `dosen_missing_in_dim.csv` (valid dosen without hibah records → need new dim rows), `dosen_reject.csv` (author lists / non-person names → reject)

## Conventions

- Everything must be **idempotent** (safe to re-run: `createOrReplace`, `IF EXISTS/IF NOT EXISTS`, MERGE).
- `run_id` (UUID / Airflow run_id) is the end-to-end trace key for a pipeline run.
- SQL DDL lives in `pipeline/schema/*.sql`, executed by splitting on `;` (no trailing comments inside statements).
- Spark jobs run inside the `lppm-spark-iceberg` container; use `docker exec lppm-spark-iceberg spark-submit --deploy-mode client /home/iceberg/<path>`.
- Config via `.env` (see `.env.example`): `MINIO_*`, `REST_CATALOG_URL`, `TRINO_*`. Don't commit secrets.
- Trino queries gold via catalog `default`; Superset should only read published `main` data, never staging/audit branches.

## Roadmap (active work)

`notes/TODO.md` — implementing **Write-Audit-Publish (WAP)**: gold writes go to Iceberg branch `audit-swap` (`write.wap.enabled`), audits run on the branch, then `FAST-FORWARD main TO audit-swap` on approval; plus DQ monitoring (SQL + Python runners, `audit.dq_results`, separate Airflow DAG `data_quality_check`) and Superset dashboards. Follow the 12-step build order in TODO.md §3.

Verified WAP mechanics on this stack (Spark 3.5 + iceberg-rest 1.10.1):
- write to branch: `ALTER TABLE t SET TBLPROPERTIES ('write.wap.enabled'='true')` + `spark.conf.set("spark.wap.branch", "audit-swap")` → `INSERT OVERWRITE` lands on the branch only, `main` untouched. `spark.wap.id` does NOT redirect in this deployment — use `spark.wap.branch`.
- publish: `CALL default.system.fast_forward('<table>', 'main', 'audit-swap')` then `ALTER TABLE ... DROP BRANCH`.
- `CREATE OR REPLACE TABLE` preserves branches and table properties.
- Current state: WAP + DQ are standalone scripts, NOT wired into `index.py`/`run.py`/Airflow yet (TODO.md §1.5, §2.5).

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
