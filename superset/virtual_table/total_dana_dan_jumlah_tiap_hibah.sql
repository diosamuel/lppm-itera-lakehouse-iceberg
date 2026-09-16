-- SELECT
--     jenis_hibah,
--     COUNT(*) AS total_hibah,
--     SUM(usulan_biaya) AS total_dana
-- FROM gold.fact_hibah
-- WHERE status_hibah = 'diterima'
-- GROUP BY jenis_hibah
-- ORDER BY total_dana DESC

    SELECT
        fh.hibah_proposal_id,
        fh.jenis_hibah,
        fdh.fakultas,
        fdh.prodi,
        d.nama,
        fh.tahun,
        sk.nama_skema,
        fh.usulan_biaya
    FROM default.gold.fact_hibah fh
    LEFT JOIN default.gold.dim_skema sk
        ON fh.skema_id = sk.skema_id
    LEFT JOIN (
        SELECT
            hibah_proposal_id,
            MAX(prodi) AS prodi,
            MAX(fakultas) AS fakultas,
            MAX(dosen_id) AS dosen_id
        FROM default.gold.fact_dosen_hibah
        WHERE role = 'ketua'
        GROUP BY hibah_proposal_id
    ) fdh ON fh.hibah_proposal_id = fdh.hibah_proposal_id
    LEFT JOIN default.gold.dim_dosen d
        ON COALESCE(fh.ketua_id, fdh.dosen_id) = d.dosen_id
    WHERE fh.status_hibah = 'diterima';
