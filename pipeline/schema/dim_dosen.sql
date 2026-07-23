USE default.gold;
CREATE TABLE dim_dosen AS
WITH hibah_lengkap AS (
    select nama_anggota_dosen, nip_anggota_dosen, ketua_peneliti, nip_ketua_peneliti from silver.penelitian
    union all
    select nama_anggota_dosen, nip_anggota_dosen, ketua_peneliti, nip_ketua_peneliti from silver.pengabdian
    union all
    select nama_anggota_dosen, nip_anggota_dosen, ketua_peneliti, nip_ketua_peneliti from silver.buku_keilmuan
),
dosen_rows AS (
    SELECT
        ketua_peneliti AS nama,
        nip_ketua_peneliti[0] AS nip
    FROM hibah_lengkap
    UNION ALL
    SELECT
        t.nama_anggota_dosen AS nama,
        t.nip_anggota_dosen AS nip
    FROM hibah_lengkap
    LATERAL VIEW EXPLODE(arrays_zip(nama_anggota_dosen, nip_anggota_dosen)) AS t
)
SELECT
    ROW_NUMBER() OVER (ORDER BY nama) AS dosen_id,
    nama,
    nip
FROM (
    SELECT nama, nip
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY nip ORDER BY LENGTH(nama) DESC) AS rn
        FROM dosen_rows
        WHERE nip IS NOT NULL AND nip != '0'
    ) t
    WHERE rn = 1

    UNION

    SELECT nama, nip
    FROM dosen_rows
    WHERE nip IS NULL OR nip = '0'
    GROUP BY nama, nip
) t
