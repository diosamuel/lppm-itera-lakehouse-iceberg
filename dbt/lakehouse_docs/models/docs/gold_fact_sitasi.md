{% docs gold_fact_sitasi %}

## Gold Layer: Citation Fact Table

This model creates facts for citation analysis and reporting.

### Data Flow
```
silver_deduped → gold_fact_sitasi (table)
```

### Dependencies
- ``silver_deduped``

### Transformations
1. Generate surrogate key combining grant ID and publication date
2. Create references to lecturer and journal dimension tables
3. Extract citation metrics and publication details

### Surrogate Keys
- `sitasi_fact_id`: `MD5(ID || TAHUN)` - unique citation fact identifier
- `dosen_id`: Reference to `gold_dim_dosen`
- `jurnal_id`: Reference to `gold_dim_jurnal`

### Fact Measures
- `sitasi`: Citation count
- `jumlah_publikasi`: Number of publications (always 1 per record)
- `triwulan`: Quarter of publication

{% enddocs %}
