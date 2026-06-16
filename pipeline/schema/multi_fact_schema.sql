
-- LPPM ITERA Data Warehouse Schema
-- DIMENSIONS

CREATE TABLE IF NOT EXISTS iceberg.lppm.dim_dosen (
    dosen_id INT,
    nip VARCHAR,
    nama VARCHAR,
    prodi VARCHAR,
    fakultas VARCHAR
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['fakultas']
);

CREATE TABLE IF NOT EXISTS iceberg.lppm.dim_skema (
    skema_id INT,
    nama_skema VARCHAR
) WITH (
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS iceberg.lppm.dim_sdgs (
    sdgs_id INT,
    kode_sdgs VARCHAR,
    deskripsi VARCHAR
) WITH (
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS iceberg.lppm.dim_jurnal (
    jurnal_id INT,
    nama_jurnal VARCHAR,
    kategori VARCHAR
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['kategori']
);

CREATE TABLE IF NOT EXISTS iceberg.lppm.dim_grant (
    grant_id VARCHAR,
    judul_proposal VARCHAR,
    status_proposal VARCHAR
) WITH (
    format = 'PARQUET'
);
-- FACT TABLES

CREATE TABLE IF NOT EXISTS iceberg.lppm.fact_grant (
    grant_fact_id INT,
    ketua_id INT,
    grant_id VARCHAR,
    skema_id INT,
    sdgs_id INT,
    usulan_biaya BIGINT,
    jumlah_grant INT
) WITH (
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS iceberg.lppm.fact_dosen_grant (
    id INT,
    dosen_id INT,
    grant_id VARCHAR,
    tahun INT,
    role VARCHAR,
    jumlah INT,
    jenis VARCHAR
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['jenis']
);

CREATE TABLE IF NOT EXISTS iceberg.lppm.fact_sitasi (
    sitasi_fact_id INT,
    dosen_id INT,
    jurnal_id INT,
    tahun INT,
    sitasi BIGINT,
    jumlah_publikasi INT
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tahun']
);
