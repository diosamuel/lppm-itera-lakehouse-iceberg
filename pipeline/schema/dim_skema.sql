SELECT *
FROM
  (SELECT skema
   FROM silver.pengabdian
   UNION SELECT skema
   FROM silver.penelitian
   UNION SELECT skema
   FROM silver.buku_keilmuan
   UNION SELECT sdgs AS skema
   FROM silver.pengabdian
   UNION SELECT sdgs AS skema
   FROM silver.penelitian
   UNION SELECT sdgs AS skema
   FROM silver.buku_keilmuan)
WHERE skema not like '%SDG%'
  AND lower(skema) like '%program%'
  OR lower(skema) like '%skema%'
  OR lower(skema) like '%gbu%'