{% docs gold_fact_dosen_hibah %}

## Gold Layer: Dosen-Hibah Fact Table

Tabel fakta untuk menganalisis keterlibatan setiap dosen dalam kegiatan hibah berdasarkan peran (ketua/anggota). Grain: 1 row = 1 dosen per 1 hibah.

### Data Flow
```
silver.penelitian ∪ silver.pengabdian ∪ silver.buku_keilmuan
  → explode anggota dosen + ketua peneliti
  → JOIN dim_dosen → dosen_id
  → gold.fact_dosen_hibah
```

### Grain
- 1 row = 1 dosen yang terlibat dalam 1 hibah tertentu (sebagai ketua atau anggota)

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `dosen_hibah_id` | INT | Surrogate key (PK) |
| `dosen_id` | INT | FK ke dim_dosen.dosen_id |
| `hibah_proposal_id` | VARCHAR | FK ke dim_hibah_proposal.hibah_proposal_id |
| `tahun` | INT | Tahun pelaksanaan hibah |
| `role` | VARCHAR | Peran dosen (ketua, anggota) |
| `jenis_hibah` | VARCHAR | Jenis hibah (penelitian, pengabdian, buku_keilmuan) |
| `status_hibah` | VARCHAR | Status hibah (diterima, ditolak, diusulkan) |

### Example Data
| dosen_hibah_id | dosen_id | hibah_proposal_id | tahun | role | jenis_hibah | status_hibah |
|---|---|---|---|---|---|---|
| 1 | 96 | BUKU_KEILMUAN-1 | 2023 | ketua | buku_keilmuan | ditolak |
| 2 | 113 | BUKU_KEILMUAN-1 | 2023 | anggota | buku_keilmuan | ditolak |
| 3 | 790 | BUKU_KEILMUAN-10 | 2023 | ketua | buku_keilmuan | diterima |

### Business Purpose
Analisis kontribusi dosen lintas hibah, tracking peran (ketua vs anggota), dan acceptance rate per dosen.

{% enddocs %}