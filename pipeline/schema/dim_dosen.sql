USE default.gold;
CREATE OR REPLACE TABLE dim_dosen USING iceberg AS
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
    WHERE ketua_peneliti IS NOT NULL AND TRIM(ketua_peneliti) != ''
    UNION ALL
    SELECT
        t.nama_anggota_dosen AS nama,
        t.nip_anggota_dosen AS nip
    FROM hibah_lengkap
    LATERAL VIEW EXPLODE(arrays_zip(nama_anggota_dosen, nip_anggota_dosen)) AS t
    WHERE t.nama_anggota_dosen IS NOT NULL AND TRIM(t.nama_anggota_dosen) != ''
)
SELECT
    CAST(xxhash64(COALESCE(nama, ''), COALESCE(nip, '')) AS INT) AS dosen_id,
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
    WHERE (nip IS NULL OR nip = '0')
      AND nama IS NOT NULL AND TRIM(nama) != ''
    GROUP BY nama, nip
) t
WHERE nama IS NOT NULL AND TRIM(nama) != ''
