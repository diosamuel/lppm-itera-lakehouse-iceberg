# LPPM ITERA Data Lakehouse

A local data lakehouse for organizing research paper data, built on **Apache Iceberg** with **MinIO** as object storage, **Spark** for compute, **Apache Superset** for BI, and **Apache Airflow** for orchestration.

## Stack

| Service           | URL                          | Default credentials |
| ----------------- | ---------------------------- | ------------------- |
| Jupyter (Spark)   | http://localhost:8888        | —                   |
| Spark UI          | http://localhost:8080        | —                   |
| Iceberg REST      | http://localhost:8181        | —                   |
| MinIO Console     | http://localhost:9001        | `admin` / `password`|
| Superset          | http://localhost:8088        | `admin` / `admin`   |
| Airflow Webserver | http://localhost:8082        | `airflow` / `airflow` |

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Minimum 8 GB RAM free for the stack
- On Windows: WSL2 backend recommended      

## Quick start

```bash
docker compose up -d
```

First run takes a few minutes (image builds + DB migrations). Watch progress with:

```bash
docker compose ps
docker compose logs -f airflow-init superset-init
```

Tear it down (preserving volumes):

```bash
docker compose down
```

Tear it down and wipe all data:

```bash
docker compose down -v
```

## Project layout

```
.
├── airflow/            # Airflow DAGs, plugins, config, init script
├── dags/               # (legacy) Airflow DAG samples
├── minio/              # MinIO bucket bootstrap script (mc-init.sh)
├── notebooks/          # Jupyter notebooks (mounted into spark-iceberg)
├── pipeline/           # Reusable Python pipeline code
├── spark/              # Spark + Iceberg image build context
├── superset/           # Superset image, config, init script
├── warehouse/          # Local Iceberg warehouse (bind-mounted)
└── docker-compose.yaml
```

## Common tasks

Open a Spark shell inside the container:

```bash
docker compose exec spark-iceberg pyspark
```

Run an Airflow CLI command:

```bash
docker compose --profile debug run --rm airflow-cli airflow dags list
```

Reset Superset metadata only:

```bash
docker compose down
docker volume rm lppm-itera-lakehouse-iceberg_superset-db-data
docker compose up -d
```