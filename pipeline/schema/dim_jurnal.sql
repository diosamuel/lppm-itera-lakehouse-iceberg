CREATE TABLE gold.dim_jurnal AS
SELECT
    ROW_NUMBER() OVER (ORDER BY nama_jurnal) AS jurnal_id,
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
