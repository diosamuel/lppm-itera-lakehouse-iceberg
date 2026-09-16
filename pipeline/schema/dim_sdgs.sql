USE default.gold;

CREATE OR REPLACE TABLE dim_sdgs (
    sdgs_id INT,
    kode_sdgs STRING
) USING iceberg;

INSERT INTO dim_sdgs (sdgs_id, kode_sdgs) VALUES
(1, 'SDG 1 No Poverty'),
(2, 'SDG 2 Zero Hunger'),
(3, 'SDG 3 Good health and well being'),
(4, 'SDG 4 Quality education'),
(5, 'SDG 6 Clean water and sanitation'),
(6, 'SDG 7 Affordable and clean energy'),
(7, 'SDG 8 Decent work and economic growth'),
(8, 'SDG 9 Industry innovation and infrastructure'),
(9, 'SDG 10 Reduced inequalities'),
(10, 'SDG 11 Sustainable cities and communities'),
(11, 'SDG 12 Responsible consumption and production'),
(12, 'SDG 13 Climate action'),
(13, 'SDG 14 Life below water'),
(14, 'SDG 15 Life on land'),
(15, 'SDG 16 Peace justice and strong institutions'),
(16, 'SDG 17 Partnerships for the goals'),
(17, 'Dasar Fundamental'),
(18, 'Hilirisasi Produk'),
(19, 'ITERA for Sumatera'),
(20, 'Kepeloporan'),
(21, 'Revolusi Industri 4 0');
