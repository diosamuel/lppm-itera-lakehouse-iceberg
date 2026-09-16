   SELECT
        d.nama,
        s.nama_skema,
        COUNT(DISTINCT fdh.hibah_proposal_id) AS total_proposal
    FROM default.gold.dim_dosen d
    JOIN default.gold.fact_dosen_hibah fdh ON d.dosen_id = fdh.dosen_id
    JOIN default.gold.fact_hibah fh ON fdh.hibah_proposal_id = fh.
  hibah_proposal_id
    JOIN default.gold.dim_skema s ON fh.skema_id = s.skema_id
    GROUP BY d.nama, s.skema_id, s.nama_skema, s.pendanaan_maks
    ORDER BY total_proposal desc
