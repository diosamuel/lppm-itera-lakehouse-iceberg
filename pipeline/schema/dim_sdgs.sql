WITH dim_sdgs AS
  (SELECT sdgs,
          row_number() OVER (
                             ORDER BY sdgs ASC) AS id,
    CASE when sdgs like '%SDG 11%' then true
    END as is_important
   FROM
     (SELECT sdgs
      FROM silver.pengabdian
      UNION SELECT sdgs
      FROM silver.penelitian
      UNION SELECT sdgs
      FROM silver.buku_keilmuan)
   WHERE sdgs like '%SDG%' )
SELECT *
FROM dim_sdgs
ORDER BY sdgs ASC