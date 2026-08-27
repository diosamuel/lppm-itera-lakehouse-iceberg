CREATE OR REPLACE TABLE gold.dim_hibah_proposal USING iceberg AS
WITH hibah_lengkap AS (
    SELECT id, judul_proposal, status_proposal, jenis FROM silver.penelitian
    UNION ALL
    SELECT id, judul_proposal, status_proposal, jenis FROM silver.pengabdian
    UNION ALL
    SELECT id, judul_proposal, status_proposal, jenis FROM silver.buku_keilmuan
)
SELECT
    id AS hibah_proposal_id,
    judul_proposal,
    status_proposal,
    jenis AS jenis_hibah
FROM hibah_lengkap
GROUP BY id, judul_proposal, status_proposal, jenis
