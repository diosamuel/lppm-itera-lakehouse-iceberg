-- OLTP
CREATE NAMESPACE IF NOT EXISTS local.oltp;

CREATE TABLE local.oltp.default (
 no BIGINT,
 judul_proposal STRING,
 ketua_peneliti STRING,
 jenis STRING,
 status STRING,
 skema STRING,
 scope STRING,
 sdgs STRING,
 program_studi STRING,
 anggota_dosen STRING,
 anggota_mahasiswa STRING,
 advisor STRING,
 usulan_biaya BIGINT,
 status_proposal STRING,
 tahun INT,
 prodi STRING,
 nim_anggota_mahasiswa ARRAY<INT>,
 nip_anggota_dosen ARRAY<INT>,
 anggota_dosen_list ARRAY<INT>,
 anggota_mahasiswa_list ARRAY<INT>
)
USING ICEBERG;


CREATE TABLE local.oltp.sitasi (
    no STRING,
    nama_dosen STRING,
    nama_prodi STRING,
    fakultas STRING,
    tanggal_terbit STRING,
    kategori STRING,
    judul STRING,
    sitasi STRING,
    triwulan STRING,
    publikasi STRING,
    doi STRING,
)
using ICEBERG