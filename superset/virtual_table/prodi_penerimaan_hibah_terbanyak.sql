SELECT
    prodi,
    COUNT(*) AS total
FROM fact_dosen_hibah
WHERE role = 'ketua'
  AND status_hibah = 'diterima'
GROUP BY prodi
ORDER BY total DESC
LIMIT 5;
