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
| Superset Connection | http://localhost:8088      | hive://spark-iceberg:10000 |
| Airflow Webserver | http://localhost:8082        | `airflow` / `airflow` |

## Prerequisites

- Docker Engine
- Minimum 8 GB RAM free for the stack

## Quick start

```bash
docker compose up -d
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
