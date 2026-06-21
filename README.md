<p align="center">
  <img src="logo-lppm-baru-scaled.png" alt="LPPM ITERA" width="300"/>
</p>

<h1 align="center">LPPM ITERA Data Lakehouse</h1>

<p align="center">
  Research paper data lakehouse built on <b>Apache Iceberg</b>, <b>MinIO</b>, <b>Spark</b>, <b>Superset</b>, and <b>Airflow</b>.
</p>

---

## Stack

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter (Spark) | http://localhost:8888 | - |
| Spark UI | http://localhost:8080 | - |
| Iceberg REST | http://localhost:8181 | - |
| MinIO Console | http://localhost:9001 | `admin` / `password` |
| Superset | http://localhost:8088 | `admin` / `admin` |
| Airflow | http://localhost:8082 | `airflow` / `airflow` |

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
