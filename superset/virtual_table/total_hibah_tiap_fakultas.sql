-- SELECT
--     COALESCE(fakultas, 'Tidak tercatat') AS fakultas,
--     COUNT(DISTINCT hibah_proposal_id) AS total_hibah
-- FROM gold.fact_dosen_hibah
-- WHERE status_hibah = 'diterima'
-- GROUP BY fakultas
-- ORDER BY total_hibah DESC;
    SELECT
        COALESCE(fdh.fakultas, 'Tidak tercatat') AS fakultas,
        fdh.prodi,
        d.nama,
        fdh.jenis_hibah,
        fdh.tahun,
        sk.nama_skema,
        fdh.hibah_proposal_id
    FROM default.gold.fact_dosen_hibah fdh
    LEFT JOIN default.gold.dim_dosen d
        ON fdh.dosen_id = d.dosen_id
    LEFT JOIN default.gold.fact_hibah fh
        ON fdh.hibah_proposal_id = fh.hibah_proposal_id
    LEFT JOIN default.gold.dim_skema sk
        ON fh.skema_id = sk.skema_id
    WHERE fdh.status_hibah = 'diterima'
      AND fdh.role = 'ketua'
