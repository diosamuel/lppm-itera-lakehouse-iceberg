CREATE OR REPLACE TABLE audit.audit_sitasi USING iceberg AS
SELECT
    sitasi_id,
    dosen_id,
    jurnal_id,
    total_publikasi,
    total_internasional,
    total_nasional,
    CASE
        WHEN dosen_id IS NULL AND jurnal_id IS NULL THEN 'dosen_jurnal_no_match'
        WHEN dosen_id IS NULL THEN 'dosen_no_match'
        WHEN jurnal_id IS NULL THEN 'jurnal_no_match'
    END AS mismatch_reason,
    current_timestamp() AS audited_at
FROM gold.fact_sitasi
WHERE dosen_id IS NULL OR jurnal_id IS NULL
