import os
import sys

from trino.dbapi import connect

sys.path.insert(0, os.path.dirname(__file__))

# dim_jurnal: jurnal_id(int), nama_jurnal(varchar), rank_jurnal(varchar), kategori_jurnal(varchar)
dim_jurnal_sql = """
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY publikasi, jurnal, jurnal_kategori) AS INTEGER) AS jurnal_id,
    publikasi AS nama_jurnal,
    jurnal AS rank_jurnal,
    jurnal_kategori AS kategori_jurnal
FROM silver.sitasi
GROUP BY publikasi, jurnal, jurnal_kategori
ORDER BY publikasi
"""

# dim_dosen: dosen_id(int), nip(varchar), nama(varchar), prodi(varchar), fakultas(varchar)
dim_dosen_sql = """
WITH all_dosen AS (
    SELECT element_at(nip_ketua_peneliti, 1) AS nip, ketua_peneliti AS nama, prodi, fakultas
    FROM silver.penelitian WHERE ketua_peneliti IS NOT NULL
    UNION
    SELECT element_at(nip_ketua_peneliti, 1) AS nip, ketua_peneliti AS nama, prodi, fakultas
    FROM silver.pengabdian WHERE ketua_peneliti IS NOT NULL
    UNION
    SELECT element_at(nip_ketua_peneliti, 1) AS nip, ketua_peneliti AS nama, prodi, fakultas
    FROM silver.buku_keilmuan WHERE ketua_peneliti IS NOT NULL
    UNION
    SELECT NULL AS nip, ketua_peneliti AS nama, prodi, fakultas
    FROM silver.sitasi WHERE ketua_peneliti IS NOT NULL
    UNION ALL
    SELECT t.nip, t.nama, p.prodi, p.fakultas
    FROM silver.penelitian p
    CROSS JOIN UNNEST(p.nip_anggota_dosen, p.nama_anggota_dosen) WITH ORDINALITY AS t(nip, nama, ord)
    WHERE t.nama IS NOT NULL
    UNION ALL
    SELECT t.nip, t.nama, p.prodi, p.fakultas
    FROM silver.pengabdian p
    CROSS JOIN UNNEST(p.nip_anggota_dosen, p.nama_anggota_dosen) WITH ORDINALITY AS t(nip, nama, ord)
    WHERE t.nama IS NOT NULL
    UNION ALL
    SELECT t.nip, t.nama, p.prodi, p.fakultas
    FROM silver.buku_keilmuan p
    CROSS JOIN UNNEST(p.nip_anggota_dosen, p.nama_anggota_dosen) WITH ORDINALITY AS t(nip, nama, ord)
    WHERE t.nama IS NOT NULL
),
all_advisor AS (
    SELECT advisor AS nama FROM silver.penelitian WHERE advisor IS NOT NULL
    UNION
    SELECT advisor FROM silver.pengabdian WHERE advisor IS NOT NULL
    UNION
    SELECT advisor FROM silver.buku_keilmuan WHERE advisor IS NOT NULL
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY nama) AS INTEGER) AS dosen_id,
    nip,
    nama,
    prodi,
    fakultas
FROM (
    SELECT DISTINCT nip, nama, prodi, fakultas FROM all_dosen WHERE nama IS NOT NULL
    UNION
    SELECT NULL AS nip, nama, NULL AS prodi, NULL AS fakultas FROM all_advisor WHERE nama IS NOT NULL
) d
ORDER BY nama
"""

# dim_skema: skema_id(int), nama_skema(varchar), pendanaan_maks(int)
dim_skema_sql = """
WITH cleaned AS (
    SELECT DISTINCT UPPER(TRIM(skema)) AS nama_skema
    FROM (
        SELECT skema FROM silver.penelitian WHERE skema IS NOT NULL
        UNION ALL
        SELECT skema FROM silver.pengabdian WHERE skema IS NOT NULL
        UNION ALL
        SELECT skema FROM silver.buku_keilmuan WHERE skema IS NOT NULL
    ) all_skema
    WHERE UPPER(TRIM(skema)) NOT LIKE '%SDG%'
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY nama_skema) AS INTEGER) AS skema_id,
    nama_skema,
    CASE
        WHEN nama_skema = 'SKEMA PENELITIAN PRIORITAS' THEN 60000000
        WHEN nama_skema = 'SKEMA PENELITIAN BERBASIS KEPAKARAN' THEN 20000000
        WHEN nama_skema = 'SKEMA KEILMUAN' THEN 8000000
        WHEN nama_skema = 'PROGRAM DESA BINAAN-KULIAH KERJA NYATA' THEN 25000000
        WHEN nama_skema = 'PROGRAM LAYANAN KEPAKARAN DAN PEMBELAJARAN MASYARAKAT (LKPM)' THEN 8000000
        WHEN nama_skema = 'PROGRAM PENGUATAN KELOMPOK KEILMUAN  (PKK)' THEN 3000000
        ELSE NULL
    END AS pendanaan_maks
FROM cleaned
ORDER BY nama_skema
"""

# dim_sdgs: sdgs_id(int), kode_sdgs(varchar), deskripsi(varchar), is_utama(varchar), is_unggulan(varchar)
dim_sdgs_sql = """
WITH cleaned AS (
    SELECT DISTINCT TRIM(sdgs) AS sdgs FROM silver.penelitian WHERE sdgs IS NOT NULL
    UNION
    SELECT DISTINCT TRIM(sdgs) AS sdgs FROM silver.pengabdian WHERE sdgs IS NOT NULL
    UNION
    SELECT DISTINCT TRIM(sdgs) AS sdgs FROM silver.buku_keilmuan WHERE sdgs IS NOT NULL
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY sdgs) AS INTEGER) AS sdgs_id,
    sdgs AS kode_sdgs,
    sdgs AS deskripsi,
    CASE WHEN sdgs IN ('SDG 1: No Poverty', 'SDG 4: Quality education', 'SDG 7: Affordable and clean energy') THEN 'true' ELSE 'false' END AS is_utama,
    CASE WHEN sdgs IN ('SDG 7: Affordable and clean energy', 'SDG 9: Industry, innovation and infrastructure') THEN 'true' ELSE 'false' END AS is_unggulan
FROM cleaned
WHERE sdgs LIKE '%SDG%'
ORDER BY sdgs
"""

# dim_hibah: hibah_id(varchar), judul_proposal(varchar), ..., total_mahasiswa(int), status(varchar)
dim_hibah_sql = """
SELECT
    CONCAT('PENELITIAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id,
    judul_proposal,
    status_proposal,
    judul_proposal AS judul_penelitian,
    ketua_peneliti AS identitas_pengusul,
    CAST(usulan_biaya AS BIGINT) AS jumlah_dana_usulan,
    CAST(NULL AS VARCHAR) AS rekam_jejak_ketua_pengusul,
    CAST(NULL AS VARCHAR) AS bidang_kepakaran,
    CAST(NULL AS VARCHAR) AS bidang_penugasan,
    CAST(NULL AS VARCHAR) AS bidang_prioritas,
    sdgs,
    CAST(NULL AS VARCHAR) AS ringkasan,
    CAST(NULL AS VARCHAR) AS kata_kunci,
    CAST(NULL AS VARCHAR) AS pendahuluan,
    CAST(NULL AS VARCHAR) AS metode,
    CAST(NULL AS VARCHAR) AS hasil_yang_diharapkan,
    CAST(NULL AS VARCHAR) AS jadwal_penelitian,
    CAST(NULL AS VARCHAR) AS biaya_penelitian,
    CAST(NULL AS VARCHAR) AS daftar_pustaka,
    scope AS scope_penelitian,
    CAST(NULL AS VARCHAR) AS file_link,
    COALESCE(CARDINALITY(nama_anggota_mahasiswa), 0) AS total_mahasiswa,
    status
FROM silver.penelitian
UNION ALL
SELECT
    CONCAT('PENGABDIAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id,
    judul_proposal,
    status_proposal,
    judul_proposal AS judul_penelitian,
    ketua_peneliti AS identitas_pengusul,
    CAST(usulan_biaya AS BIGINT) AS jumlah_dana_usulan,
    CAST(NULL AS VARCHAR) AS rekam_jejak_ketua_pengusul,
    CAST(NULL AS VARCHAR) AS bidang_kepakaran,
    CAST(NULL AS VARCHAR) AS bidang_penugasan,
    CAST(NULL AS VARCHAR) AS bidang_prioritas,
    sdgs,
    CAST(NULL AS VARCHAR) AS ringkasan,
    CAST(NULL AS VARCHAR) AS kata_kunci,
    CAST(NULL AS VARCHAR) AS pendahuluan,
    CAST(NULL AS VARCHAR) AS metode,
    CAST(NULL AS VARCHAR) AS hasil_yang_diharapkan,
    CAST(NULL AS VARCHAR) AS jadwal_penelitian,
    CAST(NULL AS VARCHAR) AS biaya_penelitian,
    CAST(NULL AS VARCHAR) AS daftar_pustaka,
    CAST(NULL AS VARCHAR) AS scope_penelitian,
    CAST(NULL AS VARCHAR) AS file_link,
    COALESCE(CARDINALITY(nama_anggota_mahasiswa), 0) AS total_mahasiswa,
    status
FROM silver.pengabdian
UNION ALL
SELECT
    CONCAT('BUKU_KEILMUAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id,
    judul_proposal,
    status_proposal,
    judul_proposal AS judul_penelitian,
    ketua_peneliti AS identitas_pengusul,
    CAST(usulan_biaya AS BIGINT) AS jumlah_dana_usulan,
    CAST(NULL AS VARCHAR) AS rekam_jejak_ketua_pengusul,
    CAST(NULL AS VARCHAR) AS bidang_kepakaran,
    CAST(NULL AS VARCHAR) AS bidang_penugasan,
    CAST(NULL AS VARCHAR) AS bidang_prioritas,
    sdgs,
    CAST(NULL AS VARCHAR) AS ringkasan,
    CAST(NULL AS VARCHAR) AS kata_kunci,
    CAST(NULL AS VARCHAR) AS pendahuluan,
    CAST(NULL AS VARCHAR) AS metode,
    CAST(NULL AS VARCHAR) AS hasil_yang_diharapkan,
    CAST(NULL AS VARCHAR) AS jadwal_penelitian,
    CAST(NULL AS VARCHAR) AS biaya_penelitian,
    CAST(NULL AS VARCHAR) AS daftar_pustaka,
    CAST(NULL AS VARCHAR) AS scope_penelitian,
    CAST(NULL AS VARCHAR) AS file_link,
    COALESCE(CARDINALITY(nama_anggota_mahasiswa), 0) AS total_mahasiswa,
    status
FROM silver.buku_keilmuan
"""

# fact hibah descripton
# 1 grain = 1 row = 1 hibah (penelitian,pengabdian.buku_keilmuan), it capture on how hibah was stored in one row transactional
# fact_hibah: hibah_fact_id(int), hibah_id(varchar), hibah_final_id(int), hibah_progress_id(int), ketua_id(int), skema_id(int), sdgs_id(int), usulan_biaya(bigint)
# Now covers all 3 hibah types matching dim_hibah

fact_hibah_sql = """
WITH dim_dosen AS (
    SELECT MIN(dosen_id) AS dosen_id, nama FROM gold.dim_dosen GROUP BY nama
),
dim_skema AS (
    SELECT skema_id, nama_skema FROM gold.dim_skema
),
dim_sdgs AS (
    SELECT sdgs_id, kode_sdgs FROM gold.dim_sdgs
),
all_hibah AS (
    SELECT
        judul_proposal,
        ketua_peneliti,
        skema,
        sdgs,
        usulan_biaya,
        CONCAT('PENELITIAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id
    FROM silver.penelitian
    UNION ALL
    SELECT
        judul_proposal,
        ketua_peneliti,
        skema,
        sdgs,
        usulan_biaya,
        CONCAT('PENGABDIAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id
    FROM silver.pengabdian
    UNION ALL
    SELECT
        judul_proposal,
        ketua_peneliti,
        skema,
        sdgs,
        usulan_biaya,
        CONCAT('BUKU_KEILMUAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id
    FROM silver.buku_keilmuan
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY h.hibah_id) AS INTEGER) AS hibah_fact_id,
    h.hibah_id,
    CAST(0 AS INTEGER) AS hibah_final_id,
    CAST(0 AS INTEGER) AS hibah_progress_id,
    dd.dosen_id AS ketua_id,
    ds.skema_id,
    dsd.sdgs_id,
    CAST(h.usulan_biaya AS BIGINT) AS usulan_biaya
FROM all_hibah h
LEFT JOIN dim_dosen dd ON h.ketua_peneliti = dd.nama
LEFT JOIN dim_skema ds ON upper(h.skema) = ds.nama_skema
LEFT JOIN dim_sdgs dsd ON h.sdgs = dsd.kode_sdgs
"""

# fact dosen hibah: 1 grain = 1 research (dosen) = combined role not only head of research (ketua), it focused on researcher do the research
# fact_dosen_hibah: id(int), dosen_id(int), hibah_id(varchar), tahun(int), role(varchar), jumlah(int), jenis(varchar), status_hibah(varchar)
# Covers all 3 hibah types (penelitian, pengabdian, buku_keilmuan) with both ketua and anggota roles.
# hibah_id uses prefix: PENELITIAN-{n}, PENGABDIAN-{n}, BUKU_KEILMUAN-{n}
fact_dosen_hibah_sql = """
WITH dim_dosen AS (
    SELECT MIN(dosen_id) AS dosen_id, nama FROM gold.dim_dosen GROUP BY nama
),
all_hibah AS (
    SELECT
        judul_proposal,
        ketua_peneliti,
        nama_anggota_dosen,
        tahun,
        jenis,
        status,
        CONCAT('PENELITIAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id
    FROM silver.penelitian
    UNION ALL
    SELECT
        judul_proposal,
        ketua_peneliti,
        nama_anggota_dosen,
        tahun,
        jenis,
        status,
        CONCAT('PENGABDIAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id
    FROM silver.pengabdian
    UNION ALL
    SELECT
        judul_proposal,
        ketua_peneliti,
        nama_anggota_dosen,
        tahun,
        jenis,
        status,
        CONCAT('BUKU_KEILMUAN-', CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR)) AS hibah_id
    FROM silver.buku_keilmuan
),
ketua_rows AS (
    SELECT
        h.judul_proposal,
        h.ketua_peneliti AS nama_dosen,
        h.hibah_id,
        h.tahun,
        'ketua' AS role,
        h.jenis,
        h.status
    FROM all_hibah h
    WHERE h.ketua_peneliti IS NOT NULL
),
anggota_rows AS (
    SELECT
        h.judul_proposal,
        t.nama AS nama_dosen,
        h.hibah_id,
        h.tahun,
        'anggota' AS role,
        h.jenis,
        h.status
    FROM all_hibah h
    CROSS JOIN UNNEST(h.nama_anggota_dosen) WITH ORDINALITY AS t(nama, ord)
    WHERE t.nama IS NOT NULL
),
combined AS (
    SELECT * FROM ketua_rows
    UNION ALL
    SELECT * FROM anggota_rows
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY c.jenis, c.hibah_id, c.role, dd.dosen_id) AS INTEGER) AS id,
    dd.dosen_id,
    c.hibah_id,
    CAST(c.tahun AS INTEGER) AS tahun,
    c.role,
    CAST(1 AS INTEGER) AS jumlah,
    c.jenis,
    c.status AS status_hibah
FROM combined c
JOIN dim_dosen dd ON c.nama_dosen = dd.nama
"""

# fact_sitasi: sitasi_fact_id(int), dosen_id(int), jurnal_id(int), tahun(int), bulan(int), hari(int), sitasi(bigint), jumlah_publikasi(int), doi(varchar), triwulan(int)
fact_sitasi_sql = """
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY s.id) AS INTEGER) AS sitasi_fact_id,
    d.dosen_id,
    j.jurnal_id,
    CAST(s.tanggal_terbit_tahun AS INTEGER) AS tahun,
    CAST(s.tanggal_terbit_bulan AS INTEGER) AS bulan,
    CAST(s.tanggal_terbit_hari AS INTEGER) AS hari,
    CAST(s.sitasi AS BIGINT) AS sitasi,
    CAST(1 AS INTEGER) AS jumlah_publikasi,
    s.doi,
    CAST(s.triwulan AS INTEGER) AS triwulan
FROM silver.sitasi s
LEFT JOIN (
    SELECT nama, CAST(ROW_NUMBER() OVER (ORDER BY nama) AS INTEGER) AS dosen_id
    FROM (SELECT DISTINCT ketua_peneliti AS nama FROM silver.sitasi WHERE ketua_peneliti IS NOT NULL) d0
) d ON s.ketua_peneliti = d.nama
LEFT JOIN (
    SELECT nama_jurnal, rank_jurnal, kategori_jurnal, jurnal_id
    FROM (
        SELECT publikasi AS nama_jurnal, jurnal AS rank_jurnal, jurnal_kategori AS kategori_jurnal,
               CAST(ROW_NUMBER() OVER (ORDER BY publikasi, jurnal, jurnal_kategori) AS INTEGER) AS jurnal_id
        FROM (SELECT DISTINCT publikasi, jurnal, jurnal_kategori FROM silver.sitasi) j0
    ) j1
) j ON s.publikasi = j.nama_jurnal AND s.jurnal = j.rank_jurnal AND s.jurnal_kategori = j.kategori_jurnal
"""

# All SQL queries for gold layer
GOLD_SQL = [
    ("dim_jurnal", dim_jurnal_sql),
    ("dim_dosen", dim_dosen_sql),
    ("dim_skema", dim_skema_sql),
    ("dim_sdgs", dim_sdgs_sql),
    ("dim_hibah", dim_hibah_sql),
    ("fact_hibah", fact_hibah_sql),
    ("fact_dosen_hibah", fact_dosen_hibah_sql),
    ("fact_sitasi", fact_sitasi_sql),
]


def main():
    host = "lppm-trino"
    port = 8085
    catalog = "default"
    schema = "gold"
    user = "trino"

    con = connect(host=host, port=port, catalog=catalog, schema=schema, user=user)
    cur = con.cursor()

    for table_name, sql in GOLD_SQL:
        full_table = f"{catalog}.{schema}.{table_name}"

        # Clear existing data
        cur.execute(f"DELETE FROM {full_table}")
        cur.fetchall()

        # Insert new data from silver
        cur.execute(f"INSERT INTO {full_table} {sql}")
        cur.fetchall()

        # Get actual row count
        cur.execute(f"SELECT COUNT(*) FROM {full_table}")
        count = cur.fetchone()[0]
        print(f"Inserted into {full_table}: {count} rows")

    con.close()
    print("Gold layer tables populated via Trino.")


if __name__ == "__main__":
    main()
