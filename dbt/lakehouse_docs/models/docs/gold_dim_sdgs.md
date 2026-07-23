{% docs gold_dim_sdgs %}

## Gold Layer: SDGs Dimension Table

Dimensi Sustainable Development Goals (SDGs) dan kategori fundamental ITERA. Diisi secara statik via SQL INSERT.

### Data Flow
```
dim_sdgs.sql (static INSERT) → gold.dim_sdgs
```

### Grain
- 1 row = 1 kategori SDGs

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `sdgs_id` | INT | Surrogate key (PK, 1-21) |
| `kode_sdgs` | VARCHAR | Kode/nama SDGs (e.g. 'SDG 1 No Poverty', 'Dasar Fundamental') |

### Values
SDG 1-17 (UN Sustainable Development Goals) + 4 kategori ITERA:
- Dasar Fundamental
- Hilirisasi Produk
- ITERA for Sumatera
- Kepeloporan
- Revolusi Industri 4.0

{% enddocs %}