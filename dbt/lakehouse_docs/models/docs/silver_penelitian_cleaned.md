{% docs silver_penelitian_cleaned %}

## Silver Layer: Cleaned Penelitian Data

This model transforms and cleans the raw penelitian data from the bronze layer.

### Data Flow
```
bronze_penelitian → silver_penelitian_cleaned (view) → silver_deduped
```

### Dependencies
- ``bronze_penelitian``

### Transformations
1. Trim whitespace from text fields
2. Standardize case (UPPER) for categorical fields
3. Clean budget amounts (remove non-numeric characters)
4. Cast numeric fields to appropriate types
5. Filter out records without valid year

### Columns
- All columns from bronze_penelitian with cleaned/transformed values

{% enddocs %}
