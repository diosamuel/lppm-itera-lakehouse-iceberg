WITH all_dosen AS (
    SELECT ketua_peneliti AS nama_dosen, nip_ketua_peneliti[1] AS nidn
    FROM silver.pengabdian
    UNION ALL
    SELECT ketua_peneliti AS nama_dosen, nip_ketua_peneliti[1] AS nidn
    FROM silver.penelitian
    UNION ALL
    SELECT ketua_peneliti AS nama_dosen, nip_ketua_peneliti[1] AS nidn
    FROM silver.buku_keilmuan
    UNION ALL
    SELECT ketua_peneliti AS nama_dosen, NULL AS nidn
    FROM silver.sitasi
    UNION ALL
    SELECT t.nama_dosen, anggota.nip_anggota_dosen[t.item] AS nidn
    FROM (
        SELECT nama_anggota_dosen, nip_anggota_dosen
        FROM silver.pengabdian
        UNION ALL
        SELECT nama_anggota_dosen, nip_anggota_dosen
        FROM silver.penelitian
        UNION ALL
        SELECT nama_anggota_dosen, nip_anggota_dosen
        FROM silver.buku_keilmuan
    ) anggota
    CROSS JOIN UNNEST(anggota.nama_anggota_dosen) WITH ORDINALITY AS t(nama_dosen, item)
)
SELECT
    nama_dosen,
    MAX(CASE WHEN TRY_CAST(nidn AS BIGINT) = 0 THEN NULL ELSE nidn END) AS nidn
FROM all_dosen
WHERE nama_dosen IS NOT NULL
  AND nama_dosen != ''
GROUP BY nama_dosen
ORDER BY nama_dosen;