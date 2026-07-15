{% docs gold_fact_dosen_hibah %}

## Gold Layer: Lecturer Participation Fact Table

This model tracks lecturer participation in research grants.

### Data Flow
```
silver_deduped → gold_fact_dosen_hibah (table)
```

### Dependencies
- ``silver_deduped``

### Transformations
1. Generate surrogate key combining lecturer NIP and grant ID
2. Create references to lecturer and grant dimension tables
3. Track participation role and count

### Surrogate Keys
- `id`: `MD5(NIP || ID)` - unique participation identifier
- `dosen_id`: Reference to `gold_dim_dosen`
- `hibah_id`: Reference to `gold_dim_hibah`

### Fact Measures
- `jumlah`: Count of lecturer participation (always 1 per record)
- `role`: Lecturer role in the grant

{% enddocs %}
