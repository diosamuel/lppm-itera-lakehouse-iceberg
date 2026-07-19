USE default.gold;
CREATE TABLE dim_dosen AS
WITH anggota AS (
    SELECT
        dosen,
        nip
    FROM hibah_lengkap
    CROSS JOIN UNNEST(nama_anggota_dosen, nip_anggota_dosen) AS t(dosen, nip)
    GROUP BY dosen, nip
),
ketua AS (
    SELECT
        ketua_peneliti AS dosen,
        nip_ketua_peneliti[1] AS nip
    FROM hibah_lengkap
    GROUP BY ketua_peneliti, nip_ketua_peneliti[1]
),
dosen_unique AS (
    SELECT dosen, nip
    FROM anggota
    UNION
    SELECT dosen, nip
    FROM ketua
)
SELECT
    ROW_NUMBER() OVER (ORDER BY dosen) AS dosen_id,
    dosen,
    nip
FROM dosen_unique;
