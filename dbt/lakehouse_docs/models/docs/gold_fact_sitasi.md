{% docs gold_fact_sitasi %}

## Gold Layer: Sitasi Fact Table

Tabel fakta untuk analisis publikasi dan sitasi dosen. Grain: 1 row = 1 dosen per 1 jurnal.

### Data Flow
```
silver.sitasi
  → JOIN dim_dosen (ketua_peneliti = nama) → dosen_id
  → JOIN dim_jurnal (jurnal = nama_jurnal) → jurnal_id
  → GROUP BY dosen_id, jurnal_id
  → gold.fact_sitasi
```

### Grain
- 1 row = 1 dosen per 1 jurnal (aggregated)

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `sitasi_id` | INT | Surrogate key (PK) |
| `dosen_id` | INT | FK ke dim_dosen.dosen_id |
| `jurnal_id` | INT | FK ke dim_jurnal.jurnal_id |
| `total_publikasi` | INT | Total publikasi dosen di jurnal tersebut |
| `total_internasional` | INT | Total publikasi internasional dosen di jurnal tersebut |
| `total_nasional` | INT | Total publikasi nasional dosen di jurnal tersebut |

### Fact Measures
- `total_publikasi`: COUNT(*) publications per (dosen, jurnal)
- `total_internasional`: SUM(CASE WHEN kategori_jurnal = 'INTERNASIONAL')
- `total_nasional`: SUM(CASE WHEN kategori_jurnal = 'NASIONAL')

### Business Purpose
Analisis produktivitas publikasi dosen per jurnal, kategori (internasional/nasional), dan per fakultas.

{% enddocs %}