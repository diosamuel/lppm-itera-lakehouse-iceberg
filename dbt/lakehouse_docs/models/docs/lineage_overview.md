{% docs lineage_overview %}

# LPPM ITERA Lakehouse Lineage Overview

This dbt project demonstrates a simple three-layer data warehouse architecture:

## Data Warehouse Layers

### Bronze Layer (Raw)
Raw ingestion data from MinIO S3. Ephemeral models that directly reference sources.

- `bronze_penelitian` - Raw research grant data
- `bronze_pengabdian` - Raw community service grant data

### Silver Layer (Cleaned)
Transformed and cleaned data with business logic applied.

- `silver_penelitian_cleaned` - Cleaned and standardized penelitian data
- `silver_deduped` - Deduplicated research data

### Gold Layer (Analytics)
Aggregated and standardized dimension/fact tables for reporting.

#### Dimensions
- `gold_dim_dosen` - Lecturer dimension
- `gold_dim_hibah` - Grant dimension
- `gold_dim_skema` - Grant scheme dimension
- `gold_dim_sdgs` - SDGs dimension

#### Facts
- `gold_fact_hibah` - Grant facts
- `gold_fact_dosen_hibah` - Lecturer participation facts
- `gold_fact_sitasi` - Citation facts

## Lineage Graph

```
                       bronze.penelitian (S3)
                                   │
                                   ▼
                            bronze_penelitian
                                   │
                                   ▼
                      silver_penelitian_cleaned
                                   │
                                   ▼
                             silver_deduped
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
       gold_dim_dosen      gold_dim_hibah      gold_dim_skema
              │                    │
              │                    │
              ▼                    ▼
        gold_fact_hibah    gold_fact_dosen_hibah
                                   │
                                   ▼
                            gold_fact_sitasi
```

## Materialization Types

- **ephemeral**: In-memory models, not stored in database (bronze)
- **view**: SQL views, logic executed on query (silver)
- **table**: Physical tables, materialized for performance (gold)

{% enddocs %}
