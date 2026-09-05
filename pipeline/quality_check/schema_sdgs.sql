-- Audit skema/sdgs tertukar untuk relasi sumber {t}.
--
-- {t} dapat berupa:
--   - nama tabel           : "silver.penelitian"
--   - relasi snapshot/branch: "silver.penelitian VERSION AS OF 'audit-swap'"
--   - union seluruh silver : "(SELECT * FROM silver.penelitian
--                              UNION ALL SELECT * FROM silver.pengabdian
--                              UNION ALL SELECT * FROM silver.buku_keilmuan)"
--
-- Kategori:
--   both_swapped  : skema & sdgs tidak valid di referensi masing-masing, tapi
--                   valid jika dipertukarkan -> tukar balik (auto-fix)
--   skema_only    : skema NULL, sdgs berisi skema -> pindah sdgs -> skema (auto-fix)
--   sdgs_only     : sdgs NULL, skema berisi SDGs -> pindah skema -> sdgs (auto-fix)
--   invalid_skema : tidak valid di dim_skema MAUPUN cross-check -> manual only
--   invalid_sdgs  : tidak valid di dim_sdgs  MAUPUN cross-check -> manual only
--   sdgs_eq_skema : regression guard: skema & sdgs tidak boleh sama
--
-- Jika semua nilai 0 -> data bersih.
WITH checked AS (
    SELECT
        h.id,
        h.skema,
        h.sdgs,
        sm.nama_skema  AS skema_match,
        dm.kode_sdgs   AS sdgs_match,
        sm2.nama_skema AS sdgs_is_skema,
        dm2.kode_sdgs  AS skema_is_sdgs
    FROM {t} h
    LEFT JOIN gold.dim_skema sm  ON LOWER(TRIM(h.skema)) = LOWER(TRIM(sm.nama_skema))
    LEFT JOIN gold.dim_sdgs  dm  ON LOWER(TRIM(h.sdgs))  = LOWER(TRIM(dm.kode_sdgs))
    LEFT JOIN gold.dim_skema sm2 ON LOWER(TRIM(h.sdgs))  = LOWER(TRIM(sm2.nama_skema))
    LEFT JOIN gold.dim_sdgs  dm2 ON LOWER(TRIM(h.skema)) = LOWER(TRIM(dm2.kode_sdgs))
)
SELECT
    SUM(CASE WHEN skema_match IS NULL AND sdgs_match IS NULL
              AND skema_is_sdgs IS NOT NULL AND sdgs_is_skema IS NOT NULL
         THEN 1 ELSE 0 END) AS both_swapped,
    SUM(CASE WHEN skema IS NULL AND sdgs_is_skema IS NOT NULL AND skema_is_sdgs IS NULL
         THEN 1 ELSE 0 END) AS skema_only,
    SUM(CASE WHEN sdgs IS NULL AND skema_is_sdgs IS NOT NULL AND sdgs_is_skema IS NULL
         THEN 1 ELSE 0 END) AS sdgs_only,
    SUM(CASE WHEN skema IS NOT NULL AND skema_match IS NULL AND skema_is_sdgs IS NULL
         THEN 1 ELSE 0 END) AS invalid_skema,
    SUM(CASE WHEN sdgs IS NOT NULL AND sdgs_match IS NULL AND sdgs_is_skema IS NULL
         THEN 1 ELSE 0 END) AS invalid_sdgs,
    SUM(CASE WHEN sdgs IS NOT NULL AND skema IS NOT NULL
              AND LOWER(TRIM(sdgs)) = LOWER(TRIM(skema))
         THEN 1 ELSE 0 END) AS sdgs_eq_skema,
    COUNT(*) AS total
FROM checked;
