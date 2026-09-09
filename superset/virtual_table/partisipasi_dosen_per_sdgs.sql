    SELECT
        d.nama,
        sd.kode_sdgs,
        COUNT(DISTINCT fdh.hibah_proposal_id) AS total_proposal
    FROM default.gold.dim_dosen d
    JOIN default.gold.fact_dosen_hibah fdh ON d.dosen_id = fdh.dosen_id
    JOIN default.gold.fact_hibah fh ON fdh.hibah_proposal_id = fh.hibah_proposal_id
    JOIN default.gold.dim_sdgs sd ON fh.sdgs_id = sd.sdgs_id
    GROUP BY d.nama, sd.kode_sdgs
    ORDER BY total_proposal DESC
