-- ============================================================
-- DQ manual test — mereproduksi 50 rule dari dq_runner.py
-- Portable: jalan di Trino DAN Spark SQL (tanpa CAST ke string).
-- Jalankan:  trino --server http://localhost:8085 --catalog default --file dq_manual_test.sql
--            (atau spark.sql(setelah ganti ',' | join))
-- Output = baris dq.dq_results (tanpa run_id/checked_at):
--   rule_name, table_name, column, check_type, violations_count, total_rows, pass_rate, passed
-- ============================================================

-- ---------- 1. COMPLETENESS (28 rule) — NULL/empty per kolom wajib ----------
WITH completeness AS (
    -- silver.buku_keilmuan (7)
    SELECT 'not_null_judul_proposal' rule, 'silver.buku_keilmuan' tbl, 'judul_proposal' col,
           SUM(CASE WHEN judul_proposal IS NULL OR TRIM(judul_proposal)='' THEN 1 ELSE 0 END) v, COUNT(*) t FROM silver.buku_keilmuan
    UNION ALL SELECT 'not_null_ketua_peneliti','silver.buku_keilmuan','ketua_peneliti',
           SUM(CASE WHEN ketua_peneliti IS NULL OR TRIM(ketua_peneliti)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.buku_keilmuan
    UNION ALL SELECT 'not_null_skema','silver.buku_keilmuan','skema',
           SUM(CASE WHEN skema IS NULL OR TRIM(skema)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.buku_keilmuan
    UNION ALL SELECT 'not_null_sdgs','silver.buku_keilmuan','sdgs',
           SUM(CASE WHEN sdgs IS NULL OR TRIM(sdgs)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.buku_keilmuan
    UNION ALL SELECT 'not_null_prodi','silver.buku_keilmuan','prodi',
           SUM(CASE WHEN prodi IS NULL OR TRIM(prodi)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.buku_keilmuan
    UNION ALL SELECT 'not_null_usulan_biaya','silver.buku_keilmuan','usulan_biaya',
           SUM(CASE WHEN usulan_biaya IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.buku_keilmuan
    UNION ALL SELECT 'not_null_tahun','silver.buku_keilmuan','tahun',
           SUM(CASE WHEN tahun IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.buku_keilmuan
    -- silver.penelitian (7)
    UNION ALL SELECT 'not_null_judul_proposal','silver.penelitian','judul_proposal',
           SUM(CASE WHEN judul_proposal IS NULL OR TRIM(judul_proposal)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.penelitian
    UNION ALL SELECT 'not_null_ketua_peneliti','silver.penelitian','ketua_peneliti',
           SUM(CASE WHEN ketua_peneliti IS NULL OR TRIM(ketua_peneliti)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.penelitian
    UNION ALL SELECT 'not_null_skema','silver.penelitian','skema',
           SUM(CASE WHEN skema IS NULL OR TRIM(skema)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.penelitian
    UNION ALL SELECT 'not_null_sdgs','silver.penelitian','sdgs',
           SUM(CASE WHEN sdgs IS NULL OR TRIM(sdgs)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.penelitian
    UNION ALL SELECT 'not_null_prodi','silver.penelitian','prodi',
           SUM(CASE WHEN prodi IS NULL OR TRIM(prodi)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.penelitian
    UNION ALL SELECT 'not_null_usulan_biaya','silver.penelitian','usulan_biaya',
           SUM(CASE WHEN usulan_biaya IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.penelitian
    UNION ALL SELECT 'not_null_tahun','silver.penelitian','tahun',
           SUM(CASE WHEN tahun IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.penelitian
    -- silver.pengabdian (7)
    UNION ALL SELECT 'not_null_judul_proposal','silver.pengabdian','judul_proposal',
           SUM(CASE WHEN judul_proposal IS NULL OR TRIM(judul_proposal)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.pengabdian
    UNION ALL SELECT 'not_null_ketua_peneliti','silver.pengabdian','ketua_peneliti',
           SUM(CASE WHEN ketua_peneliti IS NULL OR TRIM(ketua_peneliti)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.pengabdian
    UNION ALL SELECT 'not_null_skema','silver.pengabdian','skema',
           SUM(CASE WHEN skema IS NULL OR TRIM(skema)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.pengabdian
    UNION ALL SELECT 'not_null_sdgs','silver.pengabdian','sdgs',
           SUM(CASE WHEN sdgs IS NULL OR TRIM(sdgs)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.pengabdian
    UNION ALL SELECT 'not_null_prodi','silver.pengabdian','prodi',
           SUM(CASE WHEN prodi IS NULL OR TRIM(prodi)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.pengabdian
    UNION ALL SELECT 'not_null_usulan_biaya','silver.pengabdian','usulan_biaya',
           SUM(CASE WHEN usulan_biaya IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.pengabdian
    UNION ALL SELECT 'not_null_tahun','silver.pengabdian','tahun',
           SUM(CASE WHEN tahun IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.pengabdian
    -- silver.sitasi (7)
    UNION ALL SELECT 'not_null_judul_proposal','silver.sitasi','judul_proposal',
           SUM(CASE WHEN judul_proposal IS NULL OR TRIM(judul_proposal)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.sitasi
    UNION ALL SELECT 'not_null_ketua_peneliti','silver.sitasi','ketua_peneliti',
           SUM(CASE WHEN ketua_peneliti IS NULL OR TRIM(ketua_peneliti)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.sitasi
    UNION ALL SELECT 'not_null_doi','silver.sitasi','doi',
           SUM(CASE WHEN doi IS NULL OR TRIM(doi)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.sitasi
    UNION ALL SELECT 'not_null_jurnal','silver.sitasi','jurnal',
           SUM(CASE WHEN jurnal IS NULL OR TRIM(jurnal)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.sitasi
    UNION ALL SELECT 'not_null_sitasi','silver.sitasi','sitasi',
           SUM(CASE WHEN sitasi IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.sitasi
    UNION ALL SELECT 'not_null_tanggal_terbit_timestamp','silver.sitasi','tanggal_terbit_timestamp',
           SUM(CASE WHEN tanggal_terbit_timestamp IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver.sitasi
    UNION ALL SELECT 'not_null_prodi','silver.sitasi','prodi',
           SUM(CASE WHEN prodi IS NULL OR TRIM(prodi)='' THEN 1 ELSE 0 END), COUNT(*) FROM silver.sitasi
),
-- ---------- 2. CONSISTENCY (12 rule) — anomaly tertukar skema/sdgs ----------
hibah AS (
    SELECT 'silver.buku_keilmuan' tbl, id, skema, sdgs FROM silver.buku_keilmuan
    UNION ALL SELECT 'silver.penelitian', id, skema, sdgs FROM silver.penelitian
    UNION ALL SELECT 'silver.pengabdian', id, skema, sdgs FROM silver.pengabdian
),
checked AS (
    SELECT h.tbl, h.id, h.skema, h.sdgs,
           sm.nama_skema AS skema_match,
           dm.kode_sdgs  AS sdgs_match,
           sm2.nama_skema AS sdgs_is_skema,
           dm2.kode_sdgs  AS skema_is_sdgs
    FROM hibah h
    LEFT JOIN gold.dim_skema sm  ON LOWER(TRIM(h.skema)) = LOWER(TRIM(sm.nama_skema))
    LEFT JOIN gold.dim_sdgs  dm  ON LOWER(TRIM(h.sdgs))  = LOWER(TRIM(dm.kode_sdgs))
    LEFT JOIN gold.dim_skema sm2 ON LOWER(TRIM(h.sdgs))  = LOWER(TRIM(sm2.nama_skema))
    LEFT JOIN gold.dim_sdgs  dm2 ON LOWER(TRIM(h.skema)) = LOWER(TRIM(dm2.kode_sdgs))
),
consistency AS (
    SELECT 'both_swapped' rule, tbl, 'skema,sdgs' col,
           SUM(CASE WHEN skema_match IS NULL AND sdgs_match IS NULL
                      AND skema_is_sdgs IS NOT NULL AND sdgs_is_skema IS NOT NULL THEN 1 ELSE 0 END) v, COUNT(*) t
    FROM checked GROUP BY tbl
    UNION ALL
    SELECT 'skema_only', tbl, 'skema,sdgs',
           SUM(CASE WHEN skema IS NULL AND sdgs_is_skema IS NOT NULL AND skema_is_sdgs IS NULL THEN 1 ELSE 0 END), COUNT(*)
    FROM checked GROUP BY tbl
    UNION ALL
    SELECT 'sdgs_only', tbl, 'skema,sdgs',
           SUM(CASE WHEN sdgs IS NULL AND skema_is_sdgs IS NOT NULL AND sdgs_is_skema IS NULL THEN 1 ELSE 0 END), COUNT(*)
    FROM checked GROUP BY tbl
    UNION ALL
    SELECT 'sdgs_eq_skema', tbl, 'skema,sdgs',
           SUM(CASE WHEN sdgs IS NOT NULL AND skema IS NOT NULL
                      AND LOWER(TRIM(sdgs)) = LOWER(TRIM(skema)) THEN 1 ELSE 0 END), COUNT(*)
    FROM checked GROUP BY tbl
),
-- ---------- 3. REFERENTIAL (10 rule) — nilai tidak match dim_* ----------
referential AS (
    -- silver.buku_keilmuan
    SELECT 'ref_skema' rule, 'silver.buku_keilmuan' tbl, 'skema' col,
           (SELECT COUNT(*) FROM silver.buku_keilmuan h LEFT JOIN gold.dim_skema r ON LOWER(TRIM(h.skema))=LOWER(TRIM(r.nama_skema))
             WHERE h.skema IS NOT NULL AND TRIM(h.skema)<>'' AND r.nama_skema IS NULL) v,
           (SELECT COUNT(*) FROM silver.buku_keilmuan WHERE skema IS NOT NULL) t
    UNION ALL SELECT 'ref_sdgs','silver.buku_keilmuan','sdgs',
           (SELECT COUNT(*) FROM silver.buku_keilmuan h LEFT JOIN gold.dim_sdgs r ON LOWER(TRIM(h.sdgs))=LOWER(TRIM(r.kode_sdgs))
             WHERE h.sdgs IS NOT NULL AND TRIM(h.sdgs)<>'' AND r.kode_sdgs IS NULL),
           (SELECT COUNT(*) FROM silver.buku_keilmuan WHERE sdgs IS NOT NULL)
    UNION ALL SELECT 'ref_prodi','silver.buku_keilmuan','prodi',
           (SELECT COUNT(*) FROM silver.buku_keilmuan h LEFT JOIN gold.dim_prodi r ON LOWER(TRIM(h.prodi))=LOWER(TRIM(r.nama_prodi))
             WHERE h.prodi IS NOT NULL AND TRIM(h.prodi)<>'' AND r.nama_prodi IS NULL),
           (SELECT COUNT(*) FROM silver.buku_keilmuan WHERE prodi IS NOT NULL)
    -- silver.penelitian
    UNION ALL SELECT 'ref_skema','silver.penelitian','skema',
           (SELECT COUNT(*) FROM silver.penelitian h LEFT JOIN gold.dim_skema r ON LOWER(TRIM(h.skema))=LOWER(TRIM(r.nama_skema))
             WHERE h.skema IS NOT NULL AND TRIM(h.skema)<>'' AND r.nama_skema IS NULL),
           (SELECT COUNT(*) FROM silver.penelitian WHERE skema IS NOT NULL)
    UNION ALL SELECT 'ref_sdgs','silver.penelitian','sdgs',
           (SELECT COUNT(*) FROM silver.penelitian h LEFT JOIN gold.dim_sdgs r ON LOWER(TRIM(h.sdgs))=LOWER(TRIM(r.kode_sdgs))
             WHERE h.sdgs IS NOT NULL AND TRIM(h.sdgs)<>'' AND r.kode_sdgs IS NULL),
           (SELECT COUNT(*) FROM silver.penelitian WHERE sdgs IS NOT NULL)
    UNION ALL SELECT 'ref_prodi','silver.penelitian','prodi',
           (SELECT COUNT(*) FROM silver.penelitian h LEFT JOIN gold.dim_prodi r ON LOWER(TRIM(h.prodi))=LOWER(TRIM(r.nama_prodi))
             WHERE h.prodi IS NOT NULL AND TRIM(h.prodi)<>'' AND r.nama_prodi IS NULL),
           (SELECT COUNT(*) FROM silver.penelitian WHERE prodi IS NOT NULL)
    -- silver.pengabdian
    UNION ALL SELECT 'ref_skema','silver.pengabdian','skema',
           (SELECT COUNT(*) FROM silver.pengabdian h LEFT JOIN gold.dim_skema r ON LOWER(TRIM(h.skema))=LOWER(TRIM(r.nama_skema))
             WHERE h.skema IS NOT NULL AND TRIM(h.skema)<>'' AND r.nama_skema IS NULL),
           (SELECT COUNT(*) FROM silver.pengabdian WHERE skema IS NOT NULL)
    UNION ALL SELECT 'ref_sdgs','silver.pengabdian','sdgs',
           (SELECT COUNT(*) FROM silver.pengabdian h LEFT JOIN gold.dim_sdgs r ON LOWER(TRIM(h.sdgs))=LOWER(TRIM(r.kode_sdgs))
             WHERE h.sdgs IS NOT NULL AND TRIM(h.sdgs)<>'' AND r.kode_sdgs IS NULL),
           (SELECT COUNT(*) FROM silver.pengabdian WHERE sdgs IS NOT NULL)
    UNION ALL SELECT 'ref_prodi','silver.pengabdian','prodi',
           (SELECT COUNT(*) FROM silver.pengabdian h LEFT JOIN gold.dim_prodi r ON LOWER(TRIM(h.prodi))=LOWER(TRIM(r.nama_prodi))
             WHERE h.prodi IS NOT NULL AND TRIM(h.prodi)<>'' AND r.nama_prodi IS NULL),
           (SELECT COUNT(*) FROM silver.pengabdian WHERE prodi IS NOT NULL)
    -- silver.sitasi (hanya prodi)
    UNION ALL SELECT 'ref_prodi','silver.sitasi','prodi',
           (SELECT COUNT(*) FROM silver.sitasi h LEFT JOIN gold.dim_prodi r ON LOWER(TRIM(h.prodi))=LOWER(TRIM(r.nama_prodi))
             WHERE h.prodi IS NOT NULL AND TRIM(h.prodi)<>'' AND r.nama_prodi IS NULL),
           (SELECT COUNT(*) FROM silver.sitasi WHERE prodi IS NOT NULL)
),
all_rules AS (
    SELECT rule, tbl, col, 'completeness' ctype, v, t FROM completeness
    UNION ALL
    SELECT rule, tbl, col, 'consistency', v, t FROM consistency
    UNION ALL
    SELECT rule, tbl, col, 'referential', v, t FROM referential
)
SELECT
    rule                                       AS rule_name,
    tbl                                        AS table_name,
    col                                        AS column,
    ctype                                      AS check_type,
    v                                          AS violations_count,
    t                                          AS total_rows,
    CAST(COALESCE(ROUND(CAST(t - v AS DOUBLE) / NULLIF(t, 0), 4), 1.0) AS DECIMAL(6,4)) AS pass_rate,
    (v = 0)                                    AS passed
FROM all_rules
ORDER BY table_name, check_type, rule_name;
