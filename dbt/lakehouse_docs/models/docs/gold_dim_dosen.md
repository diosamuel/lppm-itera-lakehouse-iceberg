{% docs gold_dim_dosen %}

## Gold Layer: Dosen Dimension Table

Dimensi dosen yang distandarisasi dari silver.penelitian, silver.pengabdian, dan silver.buku_keilmuan.

### Data Flow
```
silver.penelitian ∪ silver.pengabdian ∪ silver.buku_keilmuan → gold.dim_dosen
```

### Transformations
1. Union semua dosen (ketua + anggota) dari 3 silver tables
2. Generate surrogate key menggunakan `ROW_NUMBER() OVER (ORDER BY nama)`
3. Ambil prodi dan fakultas pertama yang ditemukan untuk setiap dosen (`FIRST()`)
4. Deduplikasi berdasarkan nama + NIP

### Grain
- 1 row = 1 dosen unik

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `dosen_id` | INT | Surrogate key (PK) |
| `nip` | VARCHAR | NIP dosen |
| `nama` | VARCHAR | Nama dosen (sudah distandarisasi) |
| `prodi` | VARCHAR | Program studi dosen |
| `fakultas` | VARCHAR | Fakultas dosen |

### Business Purpose
Memungkinkan analisis kontribusi dosen lintas hibah dan sitasi, serta filter/group by prodi atau fakultas di Superset.

{% enddocs %}