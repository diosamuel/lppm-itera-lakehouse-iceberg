CREATE OR REPLACE TABLE gold.fact_sitasi USING iceberg AS
SELECT
    CAST(xxhash64(COALESCE(d.dosen_id, 0), COALESCE(j.jurnal_id, 0)) AS INT) AS sitasi_id,
    d.dosen_id,
    j.jurnal_id,
    COUNT(*) AS total_publikasi,
    SUM(CASE WHEN j.kategori_jurnal = 'INTERNASIONAL' THEN 1 ELSE 0 END) AS total_internasional,
    SUM(CASE WHEN j.kategori_jurnal = 'NASIONAL' THEN 1 ELSE 0 END) AS total_nasional
FROM silver.sitasi s
LEFT JOIN dim_dosen d
    ON s.ketua_peneliti = d.nama
LEFT JOIN dim_jurnal j
    ON s.jurnal = j.nama_jurnal
    AND s.jurnal_kategori = j.kategori_jurnal
GROUP BY d.dosen_id, j.jurnal_id
