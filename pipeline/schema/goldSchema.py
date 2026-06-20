import os
import sys
from dotenv import load_dotenv
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestampType

sys.path.insert(0, os.path.dirname(__file__))

from setup_catalog import SetupIcebergCatalog
from setup_spark import SetupSpark

load_dotenv()

dim_dosen_schema = Schema(
    NestedField(field_id=1, name="dosen_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="nip", field_type=StringType(), required=False),
    NestedField(field_id=3, name="nama", field_type=StringType(), required=False),
    NestedField(field_id=4, name="prodi", field_type=StringType(), required=False),
    NestedField(field_id=5, name="fakultas", field_type=StringType(), required=False),
)

dim_skema_schema = Schema(
    NestedField(field_id=1, name="skema_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="nama_skema", field_type=StringType(), required=False),
    NestedField(field_id=3, name="pendanaan_maks", field_type=IntegerType(), required=False),
)

dim_sdgs_schema = Schema(
    NestedField(field_id=1, name="sdgs_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="kode_sdgs", field_type=StringType(), required=False),
    NestedField(field_id=3, name="deskripsi", field_type=StringType(), required=False),
    NestedField(field_id=4, name="is_utama", field_type=StringType(), required=False),
    NestedField(field_id=5, name="is_unggulan", field_type=StringType(), required=False),
)

dim_jurnal_schema = Schema(
    NestedField(field_id=1, name="jurnal_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="nama_jurnal", field_type=StringType(), required=False),
    NestedField(field_id=3, name="rank_jurnal", field_type=StringType(), required=False),
    NestedField(field_id=4, name="kategori_jurnal", field_type=StringType(), required=False),
)
dim_hibah_proposal_schema = Schema(
    NestedField(field_id=1, name="hibah_proposal_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="judul_proposal", field_type=StringType(), required=False),
    NestedField(field_id=3, name="status_proposal", field_type=StringType(), required=False),
    NestedField(field_id=4, name="judul_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=5, name="identitas_pengusul", field_type=StringType(), required=False),
    NestedField(field_id=6, name="rekam_jejak_ketua_pengusul", field_type=StringType(), required=False),
    NestedField(field_id=7, name="bidang_kepakaran", field_type=StringType(), required=False),
    NestedField(field_id=8, name="bidang_penugasan", field_type=StringType(), required=False),
    NestedField(field_id=9, name="bidang_prioritas", field_type=StringType(), required=False),
    NestedField(field_id=10, name="sdgs", field_type=StringType(), required=False),
    NestedField(field_id=11, name="ringkasan", field_type=StringType(), required=False),
    NestedField(field_id=12, name="kata_kunci", field_type=StringType(), required=False),
    NestedField(field_id=13, name="pendahuluan", field_type=StringType(), required=False),
    NestedField(field_id=14, name="metode", field_type=StringType(), required=False),
    NestedField(field_id=15, name="hasil_yang_diharapkan", field_type=StringType(), required=False),
    NestedField(field_id=16, name="jadwal_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=17, name="biaya_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=18, name="daftar_pustaka", field_type=StringType(), required=False),
    NestedField(field_id=19, name="scope_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=20, name="total_mahasiswa", field_type=IntegerType(), required=False),
)


dim_hibah_progress_schema = Schema(
    NestedField(field_id=1, name="hibah_proposal_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="judul_proposal", field_type=StringType(), required=False),
    NestedField(field_id=3, name="status_proposal", field_type=StringType(), required=False),
    NestedField(field_id=4, name="judul_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=5, name="identitas_pengusul", field_type=StringType(), required=False),
    NestedField(field_id=6, name="bidang_penugasan", field_type=StringType(), required=False),
    NestedField(field_id=7, name="sdgs", field_type=StringType(), required=False),
    NestedField(field_id=8, name="ringkasan", field_type=StringType(), required=False),
    NestedField(field_id=9, name="kata_kunci", field_type=StringType(), required=False),
    NestedField(field_id=10, name="hasil_pelaksanaan_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=11, name="status_luaran", field_type=StringType(), required=False),
    NestedField(field_id=12, name="kendala_pelaksanaan_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=13, name="rencana_tahapan_selanjutnya", field_type=StringType(), required=False),
    NestedField(field_id=14, name="daftar_pustaka", field_type=StringType(), required=False),
    NestedField(field_id=15, name="scope_penelitian", field_type=StringType(), required=False),
)

dim_hibah_final_schema = Schema(
    NestedField(field_id=1, name="hibah_proposal_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="judul_proposal", field_type=StringType(), required=False),
    NestedField(field_id=3, name="status_proposal", field_type=StringType(), required=False),
    NestedField(field_id=4, name="judul_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=5, name="identitas_pengusul", field_type=StringType(), required=False),
    NestedField(field_id=6, name="bidang_penugasan", field_type=StringType(), required=False),
    NestedField(field_id=7, name="sdgs", field_type=StringType(), required=False),
    NestedField(field_id=8, name="ringkasan", field_type=StringType(), required=False),
    NestedField(field_id=9, name="kata_kunci", field_type=StringType(), required=False),
    NestedField(field_id=10, name="hasil_pelaksanaan_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=11, name="status_luaran", field_type=StringType(), required=False),
    NestedField(field_id=12, name="kendala_pelaksanaan_penelitian", field_type=StringType(), required=False),
    NestedField(field_id=13, name="rencana_tahapan_selanjutnya", field_type=StringType(), required=False),
    NestedField(field_id=14, name="daftar_pustaka", field_type=StringType(), required=False),
    NestedField(field_id=15, name="scope_penelitian", field_type=StringType(), required=False),
)


dim_file_schema = Schema(
    NestedField(field_id=1, name="file_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="upload_time", field_type=TimestampType(), required=False),
    NestedField(field_id=3, name="size", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="filename", field_type=StringType(), required=False),
    NestedField(field_id=5, name="file_link", field_type=StringType(), required=False),
)


fact_hibah_schema = Schema(
    NestedField(field_id=1, name="hibah_fact_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="ketua_id", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="hibah_proposal_id", field_type=StringType(), required=False),
    NestedField(field_id=4, name="hibah_progress_id", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="skema_id", field_type=IntegerType(), required=False),
    NestedField(field_id=6, name="sdgs_id", field_type=IntegerType(), required=False),
    NestedField(field_id=7, name="file_id", field_type=StringType(), required=False),
    NestedField(field_id=8, name="hibah_final_id", field_type=IntegerType(), required=False),
)


fact_dosen_hibah_schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="dosen_id", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="hibah_proposal_id", field_type=StringType(), required=False),
    NestedField(field_id=4, name="tahun", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="role", field_type=StringType(), required=False),
    NestedField(field_id=6, name="jumlah", field_type=IntegerType(), required=False),
    NestedField(field_id=7, name="jenis", field_type=StringType(), required=False),
    NestedField(field_id=8, name="status_hibah", field_type=StringType(), required=False),
)

fact_sitasi_schema = Schema(
    NestedField(field_id=1, name="sitasi_fact_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="dosen_id", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="jurnal_id", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="tahun", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="bulan", field_type=IntegerType(), required=False),
    NestedField(field_id=6, name="hari", field_type=IntegerType(), required=False),
    NestedField(field_id=7, name="sitasi", field_type=LongType(), required=False),
    NestedField(field_id=8, name="jumlah_publikasi", field_type=IntegerType(), required=False),
    NestedField(field_id=9, name="doi", field_type=StringType(), required=False),
    NestedField(field_id=10, name="triwulan", field_type=IntegerType(), required=False),
)


GOLD_TABLES = [
    ("dim_dosen",dim_dosen_schema),
    ("dim_skema",dim_skema_schema),
    ("dim_sdgs",dim_sdgs_schema),
    ("dim_jurnal",dim_jurnal_schema),
    ("dim_hibah_proposal",dim_hibah_proposal_schema),
    ("dim_hibah_final",dim_hibah_final_schema),
    ("dim_hibah_progress",dim_hibah_progress_schema),
    ("fact_hibah",fact_hibah_schema),
    ("fact_dosen_hibah",fact_dosen_hibah_schema),
    ("fact_sitasi",fact_sitasi_schema),
]


SparkSession = SetupSpark(
    app_name="init_gold",
    catalog_name="default"
)

Catalog = SetupIcebergCatalog(
    catalog_name="default",
    namespace="gold"
).initialize()


for table_name, schema in GOLD_TABLES:
    table = Catalog.create_replace_table(table_name,schema)
    print('created table gold: ', table)

print("Gold layer table init")
