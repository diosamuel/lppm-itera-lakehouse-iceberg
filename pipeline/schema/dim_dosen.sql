WITH dosen_ketua AS (
    SELECT
        ketua_peneliti AS nama_dosen,
        nip_ketua_peneliti[1] AS nidn
    FROM silver.pengabdian

    UNION

    SELECT
        ketua_peneliti AS nama_dosen,
        nip_ketua_peneliti[1] AS nidn
    FROM silver.penelitian

    UNION

    SELECT
        ketua_peneliti AS nama_dosen,
        nip_ketua_peneliti[1] AS nidn
    FROM silver.buku_keilmuan

    UNION

    SELECT
        ketua_peneliti AS nama_dosen,
        NULL AS nidn
    FROM silver.sitasi
),

dosen_anggota AS (
    SELECT
        nama_dosen,
        nip_anggota_dosen[item] AS nidn
    FROM (
        SELECT
            nama_anggota_dosen,
            nip_anggota_dosen
        FROM silver.pengabdian

        UNION

        SELECT
            nama_anggota_dosen,
            nip_anggota_dosen
        FROM silver.penelitian

        UNION

        SELECT
            nama_anggota_dosen,
            nip_anggota_dosen
        FROM silver.buku_keilmuan
    ) anggota
    CROSS JOIN UNNEST(nama_anggota_dosen) WITH ORDINALITY AS t(nama_dosen, item)
),

all_dosen AS (
    SELECT * FROM dosen_ketua
    UNION ALL
    SELECT * FROM dosen_anggota
),

clean_dosen AS (
    SELECT
        nama_dosen,
        CASE
            WHEN TRY_CAST(nidn AS BIGINT) = 0 THEN NULL
            ELSE nidn
        END AS nidn
    FROM all_dosen
)

SELECT
    nama_dosen,
    MAX(nidn) AS nidn
FROM clean_dosen
GROUP BY nama_dosen
ORDER BY nama_dosen;