-- CREATE TABLE sdgs_mapping AS
-- WITH sdgs AS (
--     SELECT sdgs FROM silver.penelitian
--     UNION
--     SELECT sdgs FROM silver.pengabdian
--     UNION
--     SELECT sdgs FROM silver.buku_keilmuan
-- )
-- SELECT *
-- FROM sdgs
-- WHERE sdgs LIKE '%SDG%';

CREATE TABLE sdgs_mapping (sdgs VARCHAR) 
INSERT INTO sdgs_mapping (sdgs)
VALUES ('SDG 4: Quality education'),
       ('SDG 11: Sustainable cities and communities'),
       ('SDG 3: Good health and well-being'),
       ('SDG 13: Climate action'),
       ('SDG 16: Peace, justice, and strong institutions'),
       ('SDG 9: Industry, innovation and infrastructure'),
       ('SDG 6: Clean water and sanitation'),
       ('SDG 12: Responsible consumption and production'),
       ('SDG 17: Partnerships for the goals'),
       ('SDG 7: Affordable and clean energy'),
       ('SDG 2: Zero Hunger'),
       ('SDG 8: Decent work and economic growth'),
       ('SDG 15: Life on land'),
       ('SDG 14: Life below water'),
       ('SDG 10: Reduced inequalities'),
       ('SDG 1: No Poverty'),
       ('ITERA for Sumatera'),
       ('Hilirisasi Produk'),
       ('Revolusi Industri 4.0'),
       ('Kepeloporan'),
       ('Dasar/Fundamental');