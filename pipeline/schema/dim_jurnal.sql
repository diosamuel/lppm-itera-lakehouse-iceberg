 WITH dim_jurnal AS
  (SELECT publikasi AS nama_jurnal,
          jurnal AS rank_jurnal,
          jurnal_kategori AS kategori_jurnal
   FROM silver.sitasi)
SELECT *, row_number() over (order by nama_jurnal) as id
FROM dim_jurnal
GROUP BY nama_jurnal,
         rank_jurnal,
         kategori_jurnal;