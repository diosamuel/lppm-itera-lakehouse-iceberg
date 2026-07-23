{% docs lineage_overview %}

# LPPM ITERA Lakehouse Lineage Overview

Three-layer data warehouse architecture untuk analitik LPPM ITERA.

## Data Warehouse Layers

### Bronze Layer (Raw)
Raw ingestion data dari MinIO S3. File CSV dari sistem SIPAPER ITERA.

- `bronze_penelitian` - Raw research grant data
- `bronze_pengabdian` - Raw community service grant data
- `bronze_buku_keilmuan` - Raw academic book data
- `bronze_sitasi` - Raw citation data

### Silver Layer (Cleaned)
Transformed and cleaned data with business logic applied via PySpark.

- `silver_penelitian` - Cleaned penelitian (932 rows)
- `silver_pengabdian` - Cleaned pengabdian (600 rows)
- `silver_buku_keilmuan` - Cleaned buku keilmuan (97 rows)
- `silver_sitasi` - Cleaned sitasi (190 rows)
- `silver.dim_skema` - Skema lookup table
- `silver.dim_sdgs` - SDGs lookup table

### Gold Layer (Analytics)
Conformed dimension/fact tables untuk Superset dashboarding.

#### Dimensions
- `gold_dim_dosen` - Lecturer dimension (1,053 rows, with prodi/fakultas)
- `gold_dim_skema` - Grant scheme dimension (25 rows)
- `gold_dim_sdgs` - SDGs dimension (21 rows)
- `gold_dim_jurnal` - Journal dimension (14 rows)
- `gold_dim_hibah_proposal` - Hibah proposal dimension (1,629 rows)

#### Facts
- `gold_fact_hibah` - Grant facts, grain: per hibah (2,058 rows)
- `gold_fact_dosen_hibah` - Lecturer participation facts, grain: per dosen per hibah (7,642 rows)
- `gold_fact_sitasi` - Citation facts, grain: per dosen per jurnal (69 rows)

## Lineage Graph

```
  bronze.penelitian ─┐
  bronze.pengabdian ─┼─→ silver.* ─┬─→ gold.dim_dosen ──────┐
  bronze.buku_keilmuan┘            │                        │
                                   ├─→ gold.dim_jurnal      │
  bronze.sitasi ────→ silver.sitasi┘                        │
                                                            ▼
                          gold.dim_hibah_proposal ←── silver.*
                                    │
                                    ▼
              gold.fact_hibah ──────┼────── gold.fact_dosen_hibah
                                    │
              gold.dim_skema ───────┤
              gold.dim_sdgs ────────┘
                                    
              gold.fact_sitasi ←── silver.sitasi + dim_dosen + dim_jurnal
```

{% enddocs %}