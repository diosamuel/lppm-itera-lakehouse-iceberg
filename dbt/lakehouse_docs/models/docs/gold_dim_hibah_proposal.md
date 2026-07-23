{% docs gold_dim_hibah_proposal %}

## Gold Layer: Hibah Proposal Dimension Table

Dimensi proposal hibah yang distandarisasi dari union silver.penelitian, silver.pengabdian, dan silver.buku_keilmuan.

### Data Flow
```
silver.penelitian ∪ silver.pengabdian ∪ silver.buku_keilmuan → gold.dim_hibah_proposal
```

### Transformations
1. Union 3 silver tables berdasarkan id, judul_proposal, status_proposal, jenis
2. Deduplikasi dengan GROUP BY

### Grain
- 1 row = 1 proposal hibah unik

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `hibah_proposal_id` | VARCHAR | Surrogate identifier dari silver (e.g. PENELITIAN-1) |
| `judul_proposal` | VARCHAR | Judul proposal hibah |
| `status_proposal` | VARCHAR | Status upload proposal (Sudah Upload, Belum Upload) |
| `jenis_hibah` | VARCHAR | Jenis hibah (penelitian, pengabdian, buku_keilmuan) |

{% enddocs %}