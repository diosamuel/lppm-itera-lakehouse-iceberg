{% docs gold_dim_skema %}

## Gold Layer: Skema Dimension Table

Dimensi skema hibah yang diisi secara statik via SQL INSERT. Berisi 25 skema penelitian dan pengabdian.

### Data Flow
```
dim_skema.sql (static INSERT) → gold.dim_skema
```

### Grain
- 1 row = 1 skema hibah

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `skema_id` | INT | Surrogate key (PK, 1-25) |
| `nama_skema` | VARCHAR | Nama skema hibah |
| `pendanaan_maks` | INT | Pendanaan maksimum (Rupiah), NULL jika tidak terdefinisi |

### Business Purpose
Memungkinkan analisis hibah berdasarkan skema dan pendanaan maksimum di Superset.

{% enddocs %}