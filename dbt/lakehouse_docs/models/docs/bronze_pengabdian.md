{% docs bronze_pengabdian %}

## Bronze Layer: Raw Pengabdian Data

This model represents the raw ingestion of community service grant data from MinIO S3.

### Data Flow
```
bronze.pengabdian (S3) → bronze_pengabdian (ephemeral)
```

### Source
- **Source**: `bronze.pengabdian` table from MinIO S3
- **Layer**: Bronze (raw ingestion)
- **Materialization**: Ephemeral (in-memory)

### Columns
Same schema as bronze_penelitian (sharing hibah_columns in sources.yml)

{% enddocs %}
