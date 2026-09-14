-- Audit baris tanpa judul untuk relasi sumber {t} (WAP purge silver.sitasi).
--
-- Silver.sitasi tidak punya kolom skema/sdgs, jadi audit hanya mengukur
-- kelengkapan judul_proposal. {t} dapat berupa:
--   - nama tabel            : "silver.sitasi"
--   - relasi snapshot/branch: "silver.sitasi VERSION AS OF 'audit-swap'"
--
-- null_judul : baris dengan judul_proposal NULL atau string kosong -> hapus.
-- total      : jumlah baris relasi (untuk gerbang kesesuaian baris).
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN judul_proposal IS NULL OR TRIM(CAST(judul_proposal AS string)) = ''
             THEN 1 ELSE 0 END) AS null_judul
FROM {t};
