SELECT
    d.nama,
    COUNT(DISTINCT CASE WHEN f.role = 'ketua' THEN f.hibah_proposal_id END) AS total_ketua,
    COUNT(DISTINCT CASE WHEN f.role = 'anggota' THEN f.hibah_proposal_id END) AS total_anggota,
    COUNT(DISTINCT CASE WHEN f.jenis_hibah = 'penelitian' THEN f.hibah_proposal_id END) AS total_penelitian,
    COUNT(DISTINCT CASE WHEN f.jenis_hibah = 'pengabdian' THEN f.hibah_proposal_id END) AS total_pengabdian,
    COUNT(DISTINCT CASE WHEN f.jenis_hibah = 'buku_keilmuan' THEN f.hibah_proposal_id END) AS total_buku_keilmuan,
    COUNT(DISTINCT f.hibah_proposal_id) AS total_hibah
FROM gold.fact_dosen_hibah f
JOIN gold.dim_dosen d ON f.dosen_id = d.dosen_id
WHERE f.status_hibah = 'diterima'
GROUP BY d.nama
ORDER BY total_hibah DESC;
