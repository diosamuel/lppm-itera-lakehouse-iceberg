WITH hibah AS (
    SELECT
        s.judul_proposal,
        s.ketua_peneliti,
        s.jenis,
        s.status,
        s.skema AS nama_skema,
        s.scope,
        s.sdgs,
        s.advisor,
        s.usulan_biaya,
        s.status_proposal,
        s.tahun,
        s.prodi,
        s.fakultas,
        s.nip_ketua_peneliti[1] AS nip_ketua_peneliti,
        br."anggota mahasiswa" AS nama_anggota_mahasiswa,
        br."anggota dosen" AS nama_anggota_dosen,
        s.id
    FROM silver.penelitian s
    INNER JOIN bronze.penelitian br
        ON UPPER(TRIM(br."judul proposal")) = UPPER(TRIM(s.judul_proposal))
    WHERE s.status = 'diterima'

    UNION ALL

    SELECT
        s.judul_proposal,
        s.ketua_peneliti,
        s.jenis,
        s.status,
        s.skema AS nama_skema,
        s.scope,
        s.sdgs,
        s.advisor,
        s.usulan_biaya,
        s.status_proposal,
        s.tahun,
        s.prodi,
        s.fakultas,
        s.nip_ketua_peneliti[1] AS nip_ketua_peneliti,
        br."anggota mahasiswa" AS nama_anggota_mahasiswa,
        br."anggota dosen" AS nama_anggota_dosen,
        s.id
    FROM silver.pengabdian s
    INNER JOIN bronze.pengabdian br
        ON UPPER(TRIM(br."judul proposal")) = UPPER(TRIM(s.judul_proposal))
    WHERE s.status = 'diterima'

    UNION ALL

    SELECT
        s.judul_proposal,
        s.ketua_peneliti,
        s.jenis,
        s.status,
        s.skema AS nama_skema,
        s.scope,
        s.sdgs,
        s.advisor,
        s.usulan_biaya,
        s.status_proposal,
        s.tahun,
        s.prodi,
        s.fakultas,
        s.nip_ketua_peneliti[1] AS nip_ketua_peneliti,
        br."anggota mahasiswa" AS nama_anggota_mahasiswa,
        br."anggota dosen" AS nama_anggota_dosen,
        s.id
    FROM silver.buku_keilmuan s
    INNER JOIN bronze.buku_keilmuan br
        ON UPPER(TRIM(br."judul proposal")) = UPPER(TRIM(s.judul_proposal))
    WHERE s.status = 'diterima'
)

SELECT *
FROM hibah;
