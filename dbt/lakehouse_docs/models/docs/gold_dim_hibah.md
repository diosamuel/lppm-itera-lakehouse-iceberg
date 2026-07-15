{% docs gold_dim_hibah %}

## Gold Layer: Grant Dimension Table

This model creates a standardized dimension for research grants.

### Data Flow
```
silver_deduped → gold_dim_hibah (table)
```

### Dependencies
- ``silver_deduped``

### Transformations
1. Generate surrogate key using MD5 hash of grant ID
2. Extract grant metadata (title, type, status, year)
3. Count total student participants per grant

### Surrogate Key
- `hibah_id`: `MD5(ID)` - unique identifier for each grant

### Business Purpose
This dimension enables analysis of grant characteristics, types,
and participation metrics.

{% enddocs %}
