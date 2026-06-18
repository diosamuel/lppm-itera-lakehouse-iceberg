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


## Data Lakehouse Architecture

Five namespaces organized by data quality and processing stage:

| Namespace | Description | Tables |
|-----------|-------------|--------|
| **bronze** | Raw data, as-is from source | `sitasi`, `penelitian`, `pengabdian`, `buku_keilmuan` |
| **audit** | Raw data awaiting validation before merge to silver | `sitasi`, `penelitian`, `pengabdian`, `buku_keilmuan` |
| **silver** | Cleaned and validated data | `sitasi`, `penelitian`, `pengabdian`, `buku_keilmuan` |
| **gold** | Dimension and fact tables for analytics | `dim_dosen`, `dim_skema`, `dim_sdgs`, `dim_jurnal`, `dim_hibah`, `dim_hibah_progress`, `dim_hibah_final`, `fact_hibah`, `fact_dosen_hibah`, `fact_sitasi` |

### Bronze Layer
> Raw data, as-is from source

```
bronze.sitasi
bronze.penelitian
bronze.pengabdian
bronze.buku_keilmuan
```

### Audit Layer
> Raw data awaiting validation before merge to silver

```
audit.sitasi
audit.penelitian
audit.pengabdian
audit.buku_keilmuan
```

### Silver Layer
> Cleaned and validated data

```
silver.sitasi
silver.penelitian
silver.pengabdian
silver.buku_keilmuan
```

### Gold Layer
> Dimension and fact tables for analytics modeling

**Dimension Tables:**
```
gold.dim_dosen
gold.dim_skema
gold.dim_sdgs
gold.dim_jurnal
gold.dim_hibah
gold.dim_hibah_progress
gold.dim_hibah_final
```

**Fact Tables:**
```
gold.fact_hibah
gold.fact_dosen_hibah
gold.fact_sitasi
```

