{% docs gold_dim_jurnal %}

## Gold Layer: Jurnal Dimension Table

Dimensi jurnal yang distandarisasi dari silver.sitasi. Mengelompokkan jurnal berdasarkan nama dan kategori.

### Data Flow
```
silver.sitasi → gold.dim_jurnal
```

### Transformations
1. Extract unique jurnal + jurnal_kategori dari silver.sitasi
2. Generate surrogate key menggunakan `ROW_NUMBER() OVER (ORDER BY nama_jurnal)`
3. Map `jurnal` → `nama_jurnal`, `jurnal` → `rank_jurnal` (untuk future ranking), `jurnal_kategori` → `kategori_jurnal`

### Grain
- 1 row = 1 jurnal unik (berdasarkan nama + kategori)

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `jurnal_id` | INT | Surrogate key (PK) |
| `nama_jurnal` | VARCHAR | Nama jurnal (sudah distandarisasi) |
| `rank_jurnal` | VARCHAR | Peringkat jurnal (saat ini sama dengan nama_jurnal) |
| `kategori_jurnal` | VARCHAR | Kategori jurnal (INTERNASIONAL, NASIONAL, LAINNYA) |

### Business Purpose
Memungkinkan analisis publikasi berdasarkan jurnal dan kategorinya (internasional/nasional) di Superset.

{% enddocs %}