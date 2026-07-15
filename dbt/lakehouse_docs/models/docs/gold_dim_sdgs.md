{% docs gold_dim_sdgs %}

## Gold Layer: SDGs Dimension Table

This model creates a standardized dimension for Sustainable Development Goals.

### Data Flow
```
silver_deduped → gold_dim_sdgs (table)
```

### Dependencies
- ``silver_deduped``

### Transformations
1. Extract unique SDGs from research grants
2. Parse SDGs code and description from combined field
3. Generate surrogate key using MD5 hash
4. Determine if SDGs is classified as "Utama" or "Unggulan"

### Surrogate Key
- `sdgs_id`: `MD5(SDGS)` - unique identifier for each SDG

### Columns
- `kode_sdgs`: SDGs code (e.g., "SDG 1")
- `deskripsi`: SDGs description
- `is_utama`: Flag for main SDGs classification
- `is_unggulan`: Flag for excellent SDGs classification

{% enddocs %}
