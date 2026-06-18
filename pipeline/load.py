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
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY nama) AS INTEGER) AS dosen_id,
    nip,
    nama,
    prodi,
    fakultas
FROM (
    SELECT DISTINCT nip, nama, prodi, fakultas FROM all_dosen WHERE nama IS NOT NULL
) d
ORDER BY nama
"""

# dim_skema: skema_id(int), nama_skema(varchar), pendanaan_maks(int)
dim_skema_sql = """
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY nama_skema) AS INTEGER) AS skema_id,
    nama_skema,
    CAST(NULL AS INTEGER) AS pendanaan_maks
FROM (
    SELECT DISTINCT upper(skema) AS nama_skema
    FROM (
        SELECT upper(sdgs) AS skema FROM silver.penelitian
        UNION ALL
        SELECT upper(sdgs) AS skema FROM silver.pengabdian
        UNION ALL
        SELECT upper(sdgs) AS skema FROM silver.buku_keilmuan
        UNION ALL
        SELECT upper(skema) FROM silver.penelitian
        UNION ALL
        SELECT upper(skema) FROM silver.pengabdian
        UNION ALL
        SELECT upper(skema) FROM silver.buku_keilmuan
    ) all_skema
    WHERE skema IS NOT NULL AND skema NOT LIKE '%SDG%'
) distinct_skema
ORDER BY nama_skema
"""

# dim_sdgs: sdgs_id(int), kode_sdgs(varchar), deskripsi(varchar), is_utama(varchar), is_unggulan(varchar)
dim_sdgs_sql = """
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY sdgs) AS INTEGER) AS sdgs_id,
    sdgs AS kode_sdgs,
    sdgs AS deskripsi,
    CAST(NULL AS VARCHAR) AS is_utama,
    CAST(NULL AS VARCHAR) AS is_unggulan
FROM (
    SELECT DISTINCT sdgs FROM silver.penelitian WHERE sdgs IS NOT NULL
    UNION
    SELECT DISTINCT sdgs FROM silver.pengabdian WHERE sdgs IS NOT NULL
    UNION
    SELECT DISTINCT sdgs FROM silver.buku_keilmuan WHERE sdgs IS NOT NULL
) sdgs
ORDER BY sdgs
"""

# dim_hibah: hibah_id(varchar), judul_proposal(varchar), ..., total_mahasiswa(int)
dim_hibah_sql = """
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) AS VARCHAR) AS hibah_id,
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
    CAST(NULL AS INTEGER) AS total_mahasiswa
FROM silver.penelitian
UNION ALL
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) + 1000000 AS VARCHAR) AS hibah_id,
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
    CAST(NULL AS INTEGER) AS total_mahasiswa
FROM silver.pengabdian
UNION ALL
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY judul_proposal) + 2000000 AS VARCHAR) AS hibah_id,
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
    CAST(NULL AS INTEGER) AS total_mahasiswa
FROM silver.buku_keilmuan
"""

# fact_hibah: hibah_fact_id(int), hibah_id(varchar), hibah_final_id(int), hibah_progress_id(int), ketua_id(int), skema_id(int), sdgs_id(int), usulan_biaya(bigint)
fact_hibah_sql = """
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY p.judul_proposal) AS INTEGER) AS hibah_fact_id,
    CAST(ROW_NUMBER() OVER (ORDER BY p.judul_proposal) AS VARCHAR) AS hibah_id,
    CAST(0 AS INTEGER) AS hibah_final_id,
    CAST(0 AS INTEGER) AS hibah_progress_id,
    d.dosen_id AS ketua_id,
    s.skema_id AS skema_id,
    sd.sdgs_id AS sdgs_id,
    CAST(p.usulan_biaya AS BIGINT) AS usulan_biaya
FROM silver.penelitian p
LEFT JOIN (
    SELECT nama, CAST(ROW_NUMBER() OVER (ORDER BY nama) AS INTEGER) AS dosen_id
    FROM (SELECT DISTINCT ketua_peneliti AS nama FROM silver.penelitian WHERE ketua_peneliti IS NOT NULL) d0
) d ON p.ketua_peneliti = d.nama
LEFT JOIN (
    SELECT nama_skema, CAST(ROW_NUMBER() OVER (ORDER BY nama_skema) AS INTEGER) AS skema_id
    FROM (SELECT DISTINCT upper(skema) AS nama_skema FROM silver.penelitian WHERE skema IS NOT NULL) s0
) s ON upper(p.skema) = s.nama_skema
LEFT JOIN (
    SELECT sdgs, CAST(ROW_NUMBER() OVER (ORDER BY sdgs) AS INTEGER) AS sdgs_id
    FROM (SELECT DISTINCT sdgs FROM silver.penelitian WHERE sdgs IS NOT NULL) sd0
) sd ON p.sdgs = sd.sdgs
"""

# fact_dosen_hibah: id(int), dosen_id(int), hibah_id(varchar), tahun(int), role(varchar), jumlah(int), jenis(varchar), status_hibah(varchar)
fact_dosen_hibah_sql = """
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY p.ketua_peneliti, p.judul_proposal) AS INTEGER) AS id,
    d.dosen_id,
    CAST(ROW_NUMBER() OVER (ORDER BY p.judul_proposal) AS VARCHAR) AS hibah_id,
    CAST(p.tahun AS INTEGER) AS tahun,
    'ketua' AS role,
    CAST(1 AS INTEGER) AS jumlah,
    p.jenis AS jenis,
    p.status AS status_hibah
FROM silver.penelitian p
JOIN (
    SELECT nama, CAST(ROW_NUMBER() OVER (ORDER BY nama) AS INTEGER) AS dosen_id
    FROM (SELECT DISTINCT ketua_peneliti AS nama FROM silver.penelitian WHERE ketua_peneliti IS NOT NULL) d0
) d ON p.ketua_peneliti = d.nama
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
