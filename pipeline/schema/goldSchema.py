import os
import sys

from dotenv import load_dotenv
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestampType

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from setup.setup_catalog import SetupIcebergCatalog
from setup.setup_spark import SetupSpark

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
    NestedField(field_id=4, name="jenis_hibah", field_type=StringType(), required=False),
)

fact_hibah_schema = Schema(
    NestedField(field_id=1, name="hibah_fact_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="ketua_id", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="hibah_proposal_id", field_type=StringType(), required=False),
    NestedField(field_id=4, name="skema_id", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="sdgs_id", field_type=IntegerType(), required=False),
    NestedField(field_id=6, name="jenis_hibah", field_type=StringType(), required=False),
    NestedField(field_id=7, name="tahun", field_type=IntegerType(), required=False),
    NestedField(field_id=8, name="status_hibah", field_type=StringType(), required=False),
    NestedField(field_id=9, name="total_anggota_mahasiswa", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="total_anggota_dosen", field_type=IntegerType(), required=False),
    NestedField(field_id=11, name="usulan_biaya", field_type=LongType(), required=False),
)

fact_dosen_hibah_schema = Schema(
    NestedField(field_id=1, name="dosen_hibah_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="dosen_id", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="hibah_proposal_id", field_type=StringType(), required=False),
    NestedField(field_id=4, name="tahun", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="role", field_type=StringType(), required=False),
    NestedField(field_id=6, name="jenis_hibah", field_type=StringType(), required=False),
    NestedField(field_id=7, name="status_hibah", field_type=StringType(), required=False),
)

fact_sitasi_schema = Schema(
    NestedField(field_id=1, name="sitasi_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="dosen_id", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="jurnal_id", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="total_publikasi", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="total_internasional", field_type=IntegerType(), required=False),
    NestedField(field_id=6, name="total_nasional", field_type=IntegerType(), required=False),
)


GOLD_TABLES = [
    ("dim_dosen", dim_dosen_schema),
    ("dim_skema", dim_skema_schema),
    ("dim_sdgs", dim_sdgs_schema),
    ("dim_jurnal", dim_jurnal_schema),
    ("dim_hibah_proposal", dim_hibah_proposal_schema),
    ("fact_hibah", fact_hibah_schema),
    ("fact_dosen_hibah", fact_dosen_hibah_schema),
    ("fact_sitasi", fact_sitasi_schema),
]


SparkSession = SetupSpark(app_name="init_gold", catalog_name="default")
Catalog = SetupIcebergCatalog(catalog_name="default", namespace="gold").initialize()

for table_name, schema in GOLD_TABLES:
    table = Catalog.create_replace_table(table_name, schema)
    print("created table gold: ", table)

print("Gold layer table init")
