CREATE OR REPLACE TABLE gold.fact_hibah USING iceberg PARTITIONED BY (identity(tahun)) AS
WITH hibah_lengkap AS (
    SELECT
        id, jenis, tahun, status, ketua_peneliti, nip_ketua_peneliti,
        skema, sdgs, usulan_biaya, nim_anggota_mahasiswa, nip_anggota_dosen,
        nama_anggota_mahasiswa, nama_anggota_dosen
    FROM silver.penelitian
    UNION ALL
    SELECT
        id, jenis, tahun, status, ketua_peneliti, nip_ketua_peneliti,
        skema, sdgs, usulan_biaya, nim_anggota_mahasiswa, nip_anggota_dosen,
        nama_anggota_mahasiswa, nama_anggota_dosen
    FROM silver.pengabdian
    UNION ALL
    SELECT
        id, jenis, tahun, status, ketua_peneliti, nip_ketua_peneliti,
        skema, sdgs, usulan_biaya, nim_anggota_mahasiswa, nip_anggota_dosen,
        nama_anggota_mahasiswa, nama_anggota_dosen
    FROM silver.buku_keilmuan
)
SELECT
    CAST(xxhash64(h.id) AS INT) AS hibah_fact_id,
    d.dosen_id AS ketua_id,
    h.id AS hibah_proposal_id,
    sk.skema_id,
    sd.sdgs_id,
    h.jenis AS jenis_hibah,
    h.tahun,
    h.status AS status_hibah,
    CASE WHEN h.nama_anggota_mahasiswa IS NOT NULL THEN SIZE(h.nama_anggota_mahasiswa) ELSE 0 END AS total_anggota_mahasiswa,
    CASE WHEN h.nama_anggota_dosen IS NOT NULL THEN SIZE(h.nama_anggota_dosen) ELSE 0 END AS total_anggota_dosen,
    h.usulan_biaya
FROM hibah_lengkap h
LEFT JOIN gold.dim_dosen d
    ON h.ketua_peneliti = d.nama
    AND h.nip_ketua_peneliti[0] = d.nip
LEFT JOIN gold.dim_skema sk
    ON LOWER(TRIM(h.skema)) = LOWER(TRIM(sk.nama_skema))
LEFT JOIN gold.dim_sdgs sd
    ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(sd.kode_sdgs))
