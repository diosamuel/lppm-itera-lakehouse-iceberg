CREATE TABLE gold.fact_dosen_hibah AS
WITH hibah_lengkap AS (
    select id, jenis, tahun, status, nama_anggota_dosen, nip_anggota_dosen, ketua_peneliti, nip_ketua_peneliti, prodi, fakultas from silver.penelitian
    union all
    select id, jenis, tahun, status, nama_anggota_dosen, nip_anggota_dosen, ketua_peneliti, nip_ketua_peneliti, prodi, fakultas from silver.pengabdian
    union all
    select id, jenis, tahun, status, nama_anggota_dosen, nip_anggota_dosen, ketua_peneliti, nip_ketua_peneliti, prodi, fakultas from silver.buku_keilmuan
),
hibah_dosen AS (
    SELECT
        id AS hibah_proposal_id,
        jenis,
        tahun,
        status,
        ketua_peneliti AS nama,
        nip_ketua_peneliti[0] AS nip,
        'ketua' AS role,
        prodi,
        fakultas
    FROM hibah_lengkap

    UNION ALL

    SELECT
        id AS hibah_proposal_id,
        jenis,
        tahun,
        status,
        t.nama_anggota_dosen AS nama,
        t.nip_anggota_dosen AS nip,
        'anggota' AS role,
        prodi,
        fakultas
    FROM hibah_lengkap
    LATERAL VIEW EXPLODE(arrays_zip(nama_anggota_dosen, nip_anggota_dosen)) AS t
)

SELECT
    ROW_NUMBER() OVER (ORDER BY d.dosen_id, h.hibah_proposal_id) AS dosen_hibah_id,
    d.dosen_id,
    h.hibah_proposal_id,
    h.tahun,
    h.role,
    h.jenis AS jenis_hibah,
    h.status AS status_hibah,
    h.prodi,
    h.fakultas
FROM hibah_dosen h
LEFT JOIN gold.dim_dosen d
    ON (h.nip IS NOT NULL AND h.nip <> '0' AND h.nip = d.nip)
    OR ((h.nip IS NULL OR h.nip = '0')
        AND (d.nip IS NULL OR d.nip = '0')
        AND h.nama = d.nama)
