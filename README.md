<p align="center">
  <img src="assets/logo-lppm-baru-scaled.png" alt="LPPM ITERA" width="300"/>
</p>

<h1 align="center">LPPM ITERA Data Lakehouse</h1>

<p align="center">
  Research paper data lakehouse built on <b>Apache Iceberg</b>, <b>MinIO</b>, <b>Spark</b>, <b>Superset</b>, and <b>Airflow</b>.
</p>

---

## Stacks

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter (Spark) | http://localhost:8888 | - |
| Spark UI | http://localhost:8080 | - |
| Spark Thrift Server | http://localhost:10000 | - |
| Trino | http://localhost:8085 | `trino` |
| Iceberg REST | http://localhost:8181 | - |
| MinIO Console | http://localhost:9001 | `admin` / `password` |
| Superset | http://localhost:8088 | `admin` / `admin` |
| Airflow | http://localhost:8082 | `airflow` / `airflow` |

## Query Engine Connections (Superset SQLAlchemy URI)

| Engine | SQLAlchemy URI | Use case |
|--------|----------------|----------|
| Spark SQL (Thrift) | `hive://lppm-spark-iceberg:10000/silver` | WAP testing, Iceberg branches |
| Trino | `trino://trino@lppm-trino:8085/default` | Dashboard queries (read-only) |

## Prerequisites

- Docker Engine
- Minimum 8 GB RAM

## How to Run

```bash
./start.sh
```

## Data Layers

| Layer | Purpose |
|-------|---------|
| **Bronze** | Raw data from source |
| **Audit** | Awaiting validation |
| **Silver** | Cleaned & validated |
| **Gold** | Dimension & fact tables for analytics |

## Schema

<p align="center">
  <img src="assets/multifact-schema.png" alt="Multi-Fact Schema" width="900"/>
</p>

## Common Commands

```bash
# Open PySpark shell
docker compose exec spark-iceberg pyspark

# List Airflow DAGs
docker compose --profile debug run --rm airflow-cli airflow dags list

# Tear down (preserve data)
docker compose down

# Tear down (wipe data)
docker compose down -v
```

## Project Layout

```
.
├── airflow/        # DAGs, plugins, config
├── minio/          # Bucket bootstrap script
├── notebooks/      # Jupyter notebooks
├── pipeline/       # Reusable Python pipeline
├── spark/          # Spark + Iceberg image
├── superset/       # Superset image & config
├── warehouse/      # Local Iceberg warehouse
└── docker-compose.yaml
```


## dbt Docs

The `dbt/lakehouse_docs` project is a documentation-only layer (no `dbt run`).
Edit table/column descriptions in `dbt/lakehouse_docs/models/sources.yml`.

Generate & serve docs:

```bash
# Generate documentation (queries Trino for catalog metadata)
uv run dbt docs generate --project-dir dbt/lakehouse_docs --profiles-dir dbt/lakehouse_docs

# Serve docs at http://localhost:8090
uv run dbt docs serve --project-dir dbt/lakehouse_docs --profiles-dir dbt/lakehouse_docs --port 8090

# Generate without querying Trino (manual docs only)
uv run dbt docs generate --project-dir dbt/lakehouse_docs --profiles-dir dbt/lakehouse_docs --empty-catalog
```

## Reset Iceberg Tables

`reset_iceberg_tables.sh` wipes Iceberg table data (silver + gold) and the REST
catalog SQLite DB, **without** touching raw source files in `sipaper/` or
Superset/Airflow metadata in Postgres.

```bash
# Interactive (prompts for confirmation)
./reset_iceberg_tables.sh

# Skip confirmation
./reset_iceberg_tables.sh -y
```

What it does:

| Step | Action |
|------|--------|
| 1 | Stop trino, airflow, spark-iceberg, rest |
| 2 | Delete `minio_data/warehouse/{silver,gold}` |
| 3 | Wipe Docker volume `iceberg-rest-catalog` (SQLite catalog DB) |
| 4 | Restart `rest` + `minio` |

After reset, re-run the pipeline:

```bash
uv run python pipeline/index.py
```

To wipe **everything** including raw source files and Postgres (Superset,
Airflow):

```bash
docker compose down -v
docker compose up -d
```
