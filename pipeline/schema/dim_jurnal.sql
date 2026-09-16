CREATE OR REPLACE TABLE gold.dim_jurnal USING iceberg AS
SELECT
    CAST(xxhash64(nama_jurnal, kategori_jurnal) AS INT) AS jurnal_id,
    nama_jurnal,
    rank_jurnal,
    kategori_jurnal
FROM (
    SELECT
        jurnal AS nama_jurnal,
        jurnal AS rank_jurnal,
        jurnal_kategori AS kategori_jurnal
    FROM silver.sitasi
    GROUP BY jurnal, jurnal_kategori
) src
