{% docs silver_deduped %}

## Silver Layer: Deduplicated Research Data

This model removes duplicate records from the cleaned penelitian data.

### Data Flow
```
silver_penelitian_cleaned → silver_deduped (view)
```

### Dependencies
- ``silver_penelitian_cleaned``

### Transformations
- Uses window function `ROW_NUMBER()` to identify duplicates
- Keeps the most recent record based on year and update timestamp
- Filters to keep only unique records (rn = 1)

### Use Case
This deduplication ensures each research grant appears only once in the data model,
using the most recent version when duplicates exist.

{% enddocs %}
