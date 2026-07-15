{% docs gold_dim_skema %}

## Gold Layer: Grant Scheme Dimension Table

This model creates a standardized dimension for grant schemes.

### Data Flow
```
silver_deduped → gold_dim_skema (table)
```

### Dependencies
- ``silver_deduped``

### Transformations
1. Extract unique grant schemes
2. Generate surrogate key using MD5 hash of scheme name
3. Determine maximum funding based on scheme type

### Funding Rules
- **Unggulan**: max 50,000,000
- **Utama**: max 100,000,000
- **Default**: 25,000,000

### Surrogate Key
- `skema_id`: `MD5(SCHEME)` - unique identifier for each grant scheme

{% enddocs %}
