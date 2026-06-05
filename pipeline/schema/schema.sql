
CREATE TABLE IF NOT EXISTS default_schema (
    no               BIGINT,
    judul_proposal   STRING,
    ketua_peneliti   STRING,
    jenis            STRING,
    status           STRING,
    skema            STRING,
    scope            STRING,
    sdgs             STRING,
    program_studi    STRING,
    anggota_dosen    STRING,
    anggota_mahasiswa STRING,
    advisor          STRING,
    usulan_biaya     BIGINT,
    status_proposal  STRING
);
CREATE TABLE IF NOT EXISTS default_schema_enrichment (
    no               BIGINT,
    judul_proposal   STRING,
    ketua_peneliti   STRING,
    jenis            STRING,
    status           STRING,
    skema            STRING,
    scope            STRING,
    sdgs             STRING,
    program_studi    STRING,
    anggota_dosen    STRING,
    anggota_mahasiswa STRING,
    advisor          STRING,
    usulan_biaya     BIGINT,
    status_proposal  STRING,
    -- enrichment fields
    tahun                  INT,
    prodi                  STRING,
    fakultas               STRING,
    nim_mahasiswa          ARRAY<INT>,
    nip_anggota_dosen      ARRAY<INT>,
    nama_anggota_dosen     ARRAY<STRING>,
    nama_anggota_mahasiswa ARRAY<STRING>
);

CREATE TABLE IF NOT EXISTS sitasi_schema (
    no              STRING,
    nama_dosen      STRING,
    nama_prodi      STRING,
    fakultas        STRING,
    tanggal_terbit  STRING,
    kategori        STRING,
    judul           STRING,
    sitasi          STRING,
    triwulan        STRING,
    publikasi       STRING,
    doi             STRING
);

CREATE gold.
