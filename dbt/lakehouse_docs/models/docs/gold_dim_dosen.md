{% docs gold_dim_dosen %}

## Gold Layer: Lecturer Dimension Table

This model creates a standardized dimension for lecturers used in reporting.

### Data Flow
```
silver_deduped → gold_dim_dosen (table)
```

### Dependencies
- ``silver_deduped``

### Transformations
1. Extract unique lecturers from research grants
2. Generate surrogate key using MD5 hash of NIP
3. Standardize names and departments to uppercase
4. Add timestamp for updates

### Surrogate Key
- `dosen_id`: `MD5(NIP)` - unique identifier for each lecturer

### Business Purpose
This dimension enables analysis of lecturer performance, participation,
and contributions across research grants.

{% enddocs %}
