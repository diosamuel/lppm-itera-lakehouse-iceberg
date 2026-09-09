SELECT d.nama,
  sum(f.total_internasional) AS total_internasional,
  sum(f.total_nasional)      AS total_nasional,
  sum(f.total_internasional) + sum(f.total_nasional) as total
FROM gold.fact_sitasi f
JOIN gold.dim_dosen d ON d.dosen_id = f.dosen_id
GROUP BY d.nama
