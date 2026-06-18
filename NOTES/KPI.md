# KPI Multi-Fact Schema

KPI yang dapat diambil dari schema `gold.*` dan `silver.*` untuk dashboard LPPM ITERA.

---

## A. Penelitian

### 1. Total Proposal Penelitian per Tahun
```sql
SELECT tahun, COUNT(*) AS total_proposal
FROM silver.penelitian
GROUP BY tahun
ORDER BY tahun;
```

### 2. Total Pendanaan Diterima per Tahun
```sql
SELECT tahun, SUM(usulan_biaya) AS total_dana
FROM silver.penelitian
WHERE status = 'diterima'
GROUP BY tahun
ORDER BY tahun;
```

### 3. Acceptance Rate Penelitian per Tahun
```sql
SELECT
    tahun,
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'diterima' THEN 1 ELSE 0 END) AS diterima,
    ROUND(100.0 * SUM(CASE WHEN status = 'diterima' THEN 1 ELSE 0 END) / COUNT(*), 2) AS acceptance_rate
FROM silver.penelitian
GROUP BY tahun
ORDER BY tahun;
```

### 4. Rata-rata Dana per Proposal Penelitian
```sql
SELECT tahun, AVG(usulan_biaya) AS avg_dana, MIN(usulan_biaya) AS min_dana, MAX(usulan_biaya) AS max_dana
FROM silver.penelitian
WHERE status = 'diterima'
GROUP BY tahun
ORDER BY tahun;
```

### 5. Top 10 Dosen Penerima Hibah Penelitian Terbanyak
```sql
SELECT d.nama, COUNT(*) AS jumlah_hibah, SUM(f.usulan_biaya) AS total_dana
FROM gold.fact_hibah f
JOIN gold.dim_dosen d ON f.ketua_id = d.dosen_id
GROUP BY d.nama
ORDER BY jumlah_hibah DESC
LIMIT 10;
```

### 6. Distribusi Skema Penelitian
```sql
SELECT s.nama_skema, COUNT(*) AS jumlah
FROM gold.fact_hibah f
JOIN gold.dim_skema s ON f.skema_id = s.skema_id
GROUP BY s.nama_skema
ORDER BY jumlah DESC;
```

### 7. Penelitian per Fakultas
```sql
SELECT fakultas, COUNT(*) AS total_proposal, SUM(usulan_biaya) AS total_dana
FROM silver.penelitian
WHERE status = 'diterima'
GROUP BY fakultas
ORDER BY total_proposal DESC;
```

### 8. Penelitian per SDGs
```sql
SELECT sd.kode_sdgs, COUNT(*) AS jumlah_penelitian
FROM gold.fact_hibah f
JOIN gold.dim_sdgs sd ON f.sdgs_id = sd.sdgs_id
GROUP BY sd.kode_sdgs
ORDER BY jumlah_penelitian DESC;
```

### 9. Total Anggota Dosen Dilibatkan per Tahun
```sql
SELECT tahun, SUM(CARDINALITY(nip_anggota_dosen)) AS total_anggota_dosen
FROM silver.penelitian
GROUP BY tahun
ORDER BY tahun;
```

### 10. Total Mahasiswa Dilibatkan per Tahun
```sql
SELECT tahun, SUM(CARDINALITY(nim_anggota_mahasiswa)) AS total_mahasiswa
FROM silver.penelitian
GROUP BY tahun
ORDER BY tahun;
```

---

## B. Pengabdian

### 1. Total Pengabdian per Tahun
```sql
SELECT tahun, COUNT(*) AS total_pengabdian
FROM silver.pengabdian
GROUP BY tahun
ORDER BY tahun;
```

### 2. Total Pendanaan Pengabdian per Tahun
```sql
SELECT tahun, SUM(usulan_biaya) AS total_dana
FROM silver.pengabdian
WHERE status = 'diterima'
GROUP BY tahun
ORDER BY tahun;
```

### 3. Top 10 Dosen Pengabdian Terbanyak
```sql
SELECT ketua_peneliti AS nama, COUNT(*) AS jumlah, SUM(usulan_biaya) AS total_dana
FROM silver.pengabdian
WHERE status = 'diterima'
GROUP BY ketua_peneliti
ORDER BY jumlah DESC
LIMIT 10;
```

### 4. Distribusi Skema PKM
```sql
SELECT skema, COUNT(*) AS jumlah
FROM silver.pengabdian
WHERE skema IS NOT NULL
GROUP BY skema
ORDER BY jumlah DESC;
```

### 5. Pengabdian per Fakultas
```sql
SELECT fakultas, COUNT(*) AS total, SUM(usulan_biaya) AS total_dana
FROM silver.pengabdian
WHERE status = 'diterima'
GROUP BY fakultas
ORDER BY total DESC;
```

### 6. Rata-rata Dana per Pengabdian
```sql
SELECT tahun, AVG(usulan_biaya) AS avg_dana
FROM silver.pengabdian
WHERE status = 'diterima'
GROUP BY tahun
ORDER BY tahun;
```

### 7. Acceptance Rate Pengabdian
```sql
SELECT
    tahun,
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'diterima' THEN 1 ELSE 0 END) AS diterima,
    ROUND(100.0 * SUM(CASE WHEN status = 'diterima' THEN 1 ELSE 0 END) / COUNT(*), 2) AS acceptance_rate
FROM silver.pengabdian
GROUP BY tahun
ORDER BY tahun;
```

### 8. Jumlah Anggota Dosen per Pengabdian
```sql
SELECT tahun, AVG(CARDINALITY(nip_anggota_dosen)) AS avg_anggota
FROM silver.pengabdian
GROUP BY tahun
ORDER BY tahun;
```

### 9. Mahasiswa Dilibatkan dalam Pengabdian
```sql
SELECT tahun, SUM(CARDINALITY(nim_anggota_mahasiswa)) AS total_mahasiswa
FROM silver.pengabdian
GROUP BY tahun
ORDER BY tahun;
```

### 10. Trend Pengabdian Year-over-Year
```sql
SELECT
    tahun,
    COUNT(*) AS total,
    LAG(COUNT(*)) OVER (ORDER BY tahun) AS prev_year,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY tahun) AS growth
FROM silver.pengabdian
GROUP BY tahun
ORDER BY tahun;
```

---

## C. Buku Keilmuan

### 1. Total Buku Keilmuan per Tahun
```sql
SELECT tahun, COUNT(*) AS total_buku
FROM silver.buku_keilmuan
GROUP BY tahun
ORDER BY tahun;
```

### 2. Total Dana Buku Keilmuan per Tahun
```sql
SELECT tahun, SUM(usulan_biaya) AS total_dana
FROM silver.buku_keilmuan
WHERE status = 'diterima'
GROUP BY tahun
ORDER BY tahun;
```

### 3. Top 10 Dosen Penulis Buku Keilmuan
```sql
SELECT ketua_peneliti AS nama, COUNT(*) AS jumlah_buku
FROM silver.buku_keilmuan
WHERE status = 'diterima'
GROUP BY ketua_peneliti
ORDER BY jumlah_buku DESC
LIMIT 10;
```

### 4. Distribusi Skema Buku Keilmuan
```sql
SELECT skema, COUNT(*) AS jumlah
FROM silver.buku_keilmuan
WHERE skema IS NOT NULL
GROUP BY skema
ORDER BY jumlah DESC;
```

### 5. Buku Keilmuan per Fakultas
```sql
SELECT fakultas, COUNT(*) AS total
FROM silver.buku_keilmuan
WHERE status = 'diterima'
GROUP BY fakultas
ORDER BY total DESC;
```

### 6. Acceptance Rate Buku Keilmuan
```sql
SELECT
    tahun,
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'diterima' THEN 1 ELSE 0 END) AS diterima,
    ROUND(100.0 * SUM(CASE WHEN status = 'diterima' THEN 1 ELSE 0 END) / COUNT(*), 2) AS acceptance_rate
FROM silver.buku_keilmuan
GROUP BY tahun
ORDER BY tahun;
```

### 7. Rata-rata Dana per Buku Keilmuan
```sql
SELECT tahun, AVG(usulan_biaya) AS avg_dana
FROM silver.buku_keilmuan
WHERE status = 'diterima'
GROUP BY tahun
ORDER BY tahun;
```

### 8. Jumlah Anggota Dosen per Buku
```sql
SELECT tahun, AVG(CARDINALITY(nip_anggota_dosen)) AS avg_anggota
FROM silver.buku_keilmuan
GROUP BY tahun
ORDER BY tahun;
```

### 9. Mahasiswa Dilibatkan dalam Buku Keilmuan
```sql
SELECT tahun, SUM(CARDINALITY(nim_anggota_mahasiswa)) AS total_mahasiswa
FROM silver.buku_keilmuan
GROUP BY tahun
ORDER BY tahun;
```

### 10. Trend Buku Keilmuan Year-over-Year
```sql
SELECT
    tahun,
    COUNT(*) AS total,
    LAG(COUNT(*)) OVER (ORDER BY tahun) AS prev_year,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY tahun) AS growth
FROM silver.buku_keilmuan
GROUP BY tahun
ORDER BY tahun;
```

---

## D. Sitasi

### 1. Total Sitasi per Tahun
```sql
SELECT tahun, SUM(sitasi) AS total_sitasi
FROM gold.fact_sitasi
GROUP BY tahun
ORDER BY tahun;
```

### 2. Total Publikasi per Tahun
```sql
SELECT tahun, COUNT(*) AS total_publikasi
FROM gold.fact_sitasi
GROUP BY tahun
ORDER BY tahun;
```

### 3. Top 10 Dosen Berdasarkan Sitasi
```sql
SELECT d.nama, SUM(fs.sitasi) AS total_sitasi, COUNT(*) AS total_publikasi
FROM gold.fact_sitasi fs
JOIN gold.dim_dosen d ON fs.dosen_id = d.dosen_id
GROUP BY d.nama
ORDER BY total_sitasi DESC
LIMIT 10;
```

### 4. Sitasi per Kategori Jurnal
```sql
SELECT j.kategori_jurnal, COUNT(*) AS publikasi, SUM(fs.sitasi) AS total_sitasi
FROM gold.fact_sitasi fs
JOIN gold.dim_jurnal j ON fs.jurnal_id = j.jurnal_id
GROUP BY j.kategori_jurnal
ORDER BY total_sitasi DESC;
```

### 5. Sitasi per Rank Jurnal (Q1, Q2, dst)
```sql
SELECT j.rank_jurnal, COUNT(*) AS publikasi, SUM(fs.sitasi) AS total_sitasi
FROM gold.fact_sitasi fs
JOIN gold.dim_jurnal j ON fs.jurnal_id = j.jurnal_id
GROUP BY j.rank_jurnal
ORDER BY total_sitasi DESC;
```

### 6. Publikasi per Fakultas
```sql
SELECT d.fakultas, COUNT(*) AS total_publikasi, SUM(fs.sitasi) AS total_sitasi
FROM gold.fact_sitasi fs
JOIN gold.dim_dosen d ON fs.dosen_id = d.dosen_id
GROUP BY d.fakultas
ORDER BY total_publikasi DESC;
```

### 7. Total DOI Terbit
```sql
SELECT
    COUNT(*) AS total_publikasi,
    SUM(CASE WHEN doi IS NOT NULL AND doi != '' THEN 1 ELSE 0 END) AS total_doi,
    ROUND(100.0 * SUM(CASE WHEN doi IS NOT NULL AND doi != '' THEN 1 ELSE 0 END) / COUNT(*), 2) AS persentase_doi
FROM gold.fact_sitasi;
```

### 8. Sitasi per Triwulan
```sql
SELECT tahun, triwulan, SUM(sitasi) AS total_sitasi
FROM gold.fact_sitasi
GROUP BY tahun, triwulan
ORDER BY tahun, triwulan;
```

### 9. Rata-rata Sitasi per Publikasi
```sql
SELECT
    tahun,
    COUNT(*) AS publikasi,
    SUM(sitasi) AS total_sitasi,
    ROUND(AVG(sitasi), 2) AS avg_sitasi
FROM gold.fact_sitasi
GROUP BY tahun
ORDER BY tahun;
```

### 10. Top 10 Jurnal Berdasarkan Sitasi
```sql
SELECT j.nama_jurnal, j.rank_jurnal, COUNT(*) AS publikasi, SUM(fs.sitasi) AS total_sitasi
FROM gold.fact_sitasi fs
JOIN gold.dim_jurnal j ON fs.jurnal_id = j.jurnal_id
GROUP BY j.nama_jurnal, j.rank_jurnal
ORDER BY total_sitasi DESC
LIMIT 10;
```

---

## E. Cross-Category (Bonus)

### 1. Total Output LPPM per Kategori
```sql
SELECT 'penelitian' AS kategori, tahun, COUNT(*) AS total
FROM silver.penelitian
UNION ALL
SELECT 'pengabdian', tahun, COUNT(*) FROM silver.pengabdian
UNION ALL
SELECT 'buku_keilmuan', tahun, COUNT(*) FROM silver.buku_keilmuan
ORDER BY tahun, kategori;
```

### 2. Dosen Paling Produktif (Semua Kategori)
```sql
SELECT nama, SUM(jumlah) AS total_output
FROM (
    SELECT ketua_peneliti AS nama, COUNT(*) AS jumlah FROM silver.penelitian
    UNION ALL
    SELECT ketua_peneliti, COUNT(*) FROM silver.pengabdian
    UNION ALL
    SELECT ketua_peneliti, COUNT(*) FROM silver.buku_keilmuan
) all_output
GROUP BY nama
ORDER BY total_output DESC
LIMIT 10;
```
