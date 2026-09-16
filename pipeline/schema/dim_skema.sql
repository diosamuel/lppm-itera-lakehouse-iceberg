USE default.gold;

CREATE OR REPLACE TABLE dim_skema (
    skema_id INT,
    nama_skema STRING,
    pendanaan_maks INT
) USING iceberg;

INSERT INTO dim_skema (skema_id, nama_skema, pendanaan_maks) VALUES
(1, 'GBU 45', NULL),
(2, 'Kolaborasi', NULL),
(3, 'Madya', NULL),
(4, 'PDP Pemula', 20000000),
(5, 'Penugasan Kerjasama PKM', NULL),
(6, 'PKM Reguler', NULL),
(7, 'Program Desa Binaan', 25000000),
(8, 'Program Desa Binaan Kuliah Kerja Nyata', 25000000),
(9, 'Program Kemitraan Masyarakat (PKM)', 10000000),
(10, 'Program Layanan Kepakaran dan Pembelajaran Masyarakat (LKPM)', 8000000),
(11, 'Program Pemberdayaan dan Pembelajaran Masyarakat (PPM)', 7500000),
(12, 'Program Penelitian Dasar', NULL),
(13, 'Program Penelitian Dosen Pemula', 20000000),
(14, 'Program Penelitian Penugasan', NULL),
(15, 'Program Pengabdian Penugasan (PPP)', NULL),
(16, 'Program Pengembangan Produk Unggulan Daerah (PPUD)', 15000000),
(17, 'Program Penguatan Kelompok Keilmuan (PKK)', 3000000),
(18, 'Program Penugasan Pengabdian Kerjasama', NULL),
(19, 'Program Teknologi Tepat Guna (TTG)', 20000000),
(20, 'Skema Keilmuan', 8000000),
(21, 'Skema Pendanaan Bersama', NULL),
(22, 'Skema Penelitian Berbasis Kepakaran', 20000000),
(23, 'Skema Penelitian Penugasan', NULL),
(24, 'Skema Penelitian Prioritas', 60000000),
(25, 'Skema Penugasan', NULL);
