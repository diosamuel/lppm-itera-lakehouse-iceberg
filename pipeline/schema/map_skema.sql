-- CREATE TABLE lookup_skema AS
-- WITH skema_tb AS (
--     SELECT skema
--     FROM silver.penelitian
--     UNION
--     SELECT skema
--     FROM silver.pengabdian
--     UNION
--     SELECT skema
--     FROM silver.buku_keilmuan
--     UNION
--     SELECT sdgs AS skema
--     FROM silver.penelitian
--     UNION
--     SELECT sdgs AS skema
--     FROM silver.pengabdian
--     UNION
--     SELECT sdgs AS skema
--     FROM silver.buku_keilmuan
-- )
-- SELECT *
-- FROM skema_tb
-- WHERE lower(skema) LIKE '%skema%'
--    OR lower(skema) LIKE '%program%'
--    OR lower(skema) LIKE '%penugasan%'
--    OR lower(skema) LIKE '%kolaborasi%'
--    OR lower(skema) LIKE '%madya%'
--    OR lower(skema) LIKE '%pdp%'
--    OR lower(skema) LIKE '%pkm%'
-- ORDER BY skema DESC;

CREATE TABLE lookup_skema (
    skema VARCHAR
)
INSERT INTO lookup_skema (skema)
VALUES ('Skema Penugasan'),
       ('GBU 45'),
       ('Kolaborasi'),
       ('Skema Penelitian Prioritas'),
       ('Skema Penelitian Penugasan'),
       ('Skema Penelitian Berbasis Kepakaran'),
       ('Skema Pendanaan Bersama'),
       ('Skema Keilmuan'),
       ('Program teknologi tepat guna (TTG) maks. Rp 20.000.000'),
       ('Program pengembangan produk unggulan daerah (PPUD) maks Rp 15.000.000'),
       ('Program pemberdayaan dan pembelajaran masyarakat (PPM) maks Rp 7.500.000'),
       ('Program Penugasan Pengabdian Kerjasama'),
       ('Program Penguatan Kelompok Keilmuan (PKK)'),
       ('Program Pengabdian Penugasan (PPP)'),
       ('Program Penelitian Penugasan'),
       ('Program Penelitian Dosen Pemula'),
       ('Program Penelitian Dasar'),
       ('Program Layanan Kepakaran dan Pembelajaran Masyarakat (LKPM)'),
       ('Program Layanan Kepakaran dan Pembelajaran Masyarakat'),
       ('Program Kemitraan Masyarakat (PKM) maks. Rp 10.000.000'),
       ('Program Desa Binaan-Kuliah Kerja Nyata'),
       ('Program Desa Binaan'),
       ('Penugasan Kerjasama PKM'),
       ('PKM Reguler'),
       ('PDP (Pemula)'),
       ('Madya'),
       ('Kolaborasi');
