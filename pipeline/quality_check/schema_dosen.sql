-- Audit nama dosen silver.sitasi vs gold.dim_dosen (WAP dosen mapping).
--
-- Parameter:
--   {sitasi} : relasi sumber sitasi
--              - "silver.sitasi" (main)
--              - "silver.sitasi VERSION AS OF 'audit-dosen'" (branch)
--   {dim}    : relasi dim_dosen
--              - "gold.dim_dosen"
--              - "gold.dim_dosen VERSION AS OF 'audit-dosen'"
--
-- Kategori (mutually exclusive, partisi penuh):
--   empty_rows     : ketua_peneliti NULL / empty string
--                    -> completeness issue sumber data, di luar scope mapping
--   invalid_rows   : tidak kosong TAPI author-list / non-person
--                    (is_invalid_person_udf -> standardize == NULL atau mengandung koma)
--                    -> dibiarkan as-is, HANYA diaudit, tidak di-map / di-insert
--   unmatched_valid: tidak kosong, bukan invalid, tapi tidak match ke dim_dosen
--                    -> kandidat mapping; setelah WAP harusnya 0
--   matched        : ketua_peneliti match persis ke dim_dosen.nama
--
-- NOTE: UDF is_invalid_person_udf harus di-register dulu oleh dosen_mapping.py
-- (spark.udf.register) sebelum query ini dijalankan.
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN s.ketua_peneliti IS NULL OR TRIM(s.ketua_peneliti) = ''
             THEN 1 ELSE 0 END) AS empty_rows,
    SUM(CASE WHEN s.ketua_peneliti IS NOT NULL AND TRIM(s.ketua_peneliti) <> ''
              AND is_invalid_person_udf(s.ketua_peneliti)
             THEN 1 ELSE 0 END) AS invalid_rows,
    SUM(CASE WHEN d.nama IS NULL
              AND s.ketua_peneliti IS NOT NULL AND TRIM(s.ketua_peneliti) <> ''
              AND NOT is_invalid_person_udf(s.ketua_peneliti)
             THEN 1 ELSE 0 END) AS unmatched_valid,
    SUM(CASE WHEN d.nama IS NOT NULL THEN 1 ELSE 0 END) AS matched
FROM {sitasi} s
LEFT JOIN {dim} d
    ON s.ketua_peneliti = d.nama
