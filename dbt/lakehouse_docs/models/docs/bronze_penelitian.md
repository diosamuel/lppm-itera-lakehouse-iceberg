{% docs bronze_penelitian %}

## Bronze Layer: Raw Penelitian Data

This model represents the raw ingestion of research grant data from MinIO S3.

### Data Flow
```
bronze.penelitian (S3) → bronze_penelitian (ephemeral) → silver_penelitian_cleaned → silver_deduped
```

### Source
- **Source**: `bronze.penelitian` table from MinIO S3
- **Layer**: Bronze (raw ingestion)
- **Materialization**: Ephemeral (in-memory)

### Columns
- `id`: Unique identifier for the research grant
- `judul_proposal`: Proposal title
- `ketua_peneliti`: Principal investigator
- `jenis`: Research type
- `status`: Current status
- `skema`: Grant scheme
- `scope`: Research scope
- `sdgs`: Sustainable Development Goals alignment
- `usulan_biaya`: Budget proposal amount
- `status_proposal`: Proposal status
- `tahun`: Year
- `prodi`: Study program
- `fakultas`: Faculty
- `nip_ketua_peneliti`: Principal investigator NIP
- `nim_anggota_mahasiswa`: Student member NIM
- `nama_anggota_mahasiswa`: Student member name
- `nip_anggota_dosen`: Lecturer member NIP
- `nama_anggota_dosen`: Lecturer member name
- `advisor`: Advisor name

{% enddocs %}
