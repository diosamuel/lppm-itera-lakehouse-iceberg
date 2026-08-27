CREATE OR REPLACE TABLE audit.audit_faktor_hibah USING iceberg AS
SELECT
    hibah_fact_id,
    ketua_id,
    hibah_proposal_id,
    skema_id,
    sdgs_id,
    jenis_hibah,
    tahun,
    status_hibah,
    usulan_biaya,
    CASE
        WHEN skema_id IS NULL AND sdgs_id IS NULL THEN 'skema_sdgs_no_match'
        WHEN skema_id IS NULL THEN 'skema_no_match'
        WHEN sdgs_id IS NULL THEN 'sdgs_no_match'
    END AS mismatch_reason,
    current_timestamp() AS audited_at
FROM gold.fact_hibah
WHERE skema_id IS NULL OR sdgs_id IS NULL
