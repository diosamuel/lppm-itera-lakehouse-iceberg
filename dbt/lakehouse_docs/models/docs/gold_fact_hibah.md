{% docs gold_fact_hibah %}

## Gold Layer: Hibah Fact Table

Tabel fakta untuk analisis hibah (penelitian, pengabdian, buku keilmuan). Grain: 1 row = 1 hibah proposal.

### Data Flow
```
silver.penelitian ∪ silver.pengabdian ∪ silver.buku_keilmuan
  → JOIN dim_dosen (ketua) → ketua_id
  → JOIN dim_skema → skema_id
  → JOIN dim_sdgs → sdgs_id
  → gold.fact_hibah
```

### Grain
- 1 row = 1 hibah proposal (dari sudut pandang ketua peneliti)

### Columns
| Column | Type | Description |
|--------|------|-------------|
| `hibah_fact_id` | INT | Surrogate key (PK) |
| `ketua_id` | INT | FK ke dim_dosen.dosen_id |
| `hibah_proposal_id` | VARCHAR | FK ke dim_hibah_proposal.hibah_proposal_id |
| `skema_id` | INT | FK ke dim_skema.skema_id |
| `sdgs_id` | INT | FK ke dim_sdgs.sdgs_id |
| `jenis_hibah` | VARCHAR | Jenis hibah (penelitian, pengabdian, buku_keilmuan) |
| `tahun` | INT | Tahun pelaksanaan |
| `status_hibah` | VARCHAR | Status hibah (diterima, ditolak, diusulkan) |
| `total_anggota_mahasiswa` | INT | Jumlah anggota mahasiswa dalam tim |
| `total_anggota_dosen` | INT | Jumlah anggota dosen dalam tim |
| `usulan_biaya` | BIGINT | Total biaya yang diusulkan (Rupiah) |

### Fact Measures
- `total_anggota_mahasiswa`: COUNT array size dari nim_anggota_mahasiswa
- `total_anggota_dosen`: COUNT array size dari nip_anggota_dosen
- `usulan_biaya`: SUM biaya yang diusulkan

### Business Purpose
Analisis distribusi hibah per tahun, per skema, per fakultas, dan tracking budget.

{% enddocs %}