{% docs gold_fact_hibah %}

## Gold Layer: Grant Fact Table

This model creates facts for grant analysis and reporting.

### Data Flow
```
silver_deduped → gold_fact_hibah (table)
```

### Dependencies
- ``silver_deduped``

### Transformations
1. Generate surrogate key combining grant ID and year
2. Create references to dimension tables (hibah, ketua)
3. Include budget amounts and grant status

### Surrogate Keys
- `hibah_fact_id`: `MD5(ID || TAHUN)` - unique fact record identifier
- `hibah_id`: Reference to `gold_dim_hibah`
- `ketua_id`: Reference to `gold_dim_dosen`

### Fact Measures
- `usulan_biaya`: Budget proposal amount

{% enddocs %}
