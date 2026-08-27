CREATE OR REPLACE TABLE audit.audit_dosen_hibah USING iceberg AS
SELECT
    f.dosen_hibah_id,
    f.dosen_id,
    f.hibah_proposal_id,
    f.tahun,
    f.role,
    f.jenis_hibah,
    f.status_hibah,
    f.prodi,
    f.fakultas,
    'prodi_no_match' AS mismatch_reason,
    current_timestamp() AS audited_at
FROM gold.fact_dosen_hibah f
LEFT JOIN gold.dim_prodi p
    ON LOWER(TRIM(f.prodi)) = LOWER(TRIM(p.nama_prodi))
WHERE p.prodi_id IS NULL
