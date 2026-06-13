from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

# Typical default schema is for Penelitian , Pengabdian & Buku Keilmuan
default_schema = Schema(
    NestedField(field_id=1, name="no", field_type=LongType(), required=False),
    NestedField(
        field_id=2, name="judul_proposal", field_type=StringType(), required=False
    ),
    NestedField(
        field_id=3, name="ketua_peneliti", field_type=StringType(), required=False
    ),
    NestedField(field_id=4, name="jenis", field_type=StringType(), required=False),
    NestedField(field_id=5, name="status", field_type=StringType(), required=False),
    NestedField(field_id=6, name="skema", field_type=StringType(), required=False),
    NestedField(field_id=7, name="scope", field_type=StringType(), required=False),
    NestedField(field_id=8, name="sdgs", field_type=StringType(), required=False),
    NestedField(
        field_id=9, name="program_studi", field_type=StringType(), required=False
    ),
    NestedField(
        field_id=10, name="anggota_dosen", field_type=StringType(), required=False
    ),
    NestedField(
        field_id=11, name="anggota_mahasiswa", field_type=StringType(), required=False
    ),
    NestedField(field_id=12, name="advisor", field_type=StringType(), required=False),
    NestedField(
        field_id=13, name="usulan_biaya", field_type=LongType(), required=False
    ),
    NestedField(
        field_id=14, name="status_proposal", field_type=StringType(), required=False
    ),
)

default_schema_enrichment = Schema(
    *default_schema.fields,
    NestedField(field_id=15, name="tahun", field_type=IntegerType(), required=False),
    NestedField(field_id=16, name="prodi", field_type=StringType(), required=False),
    NestedField(field_id=17, name="fakultas", field_type=StringType(), required=False),
    NestedField(
        field_id=18,
        name="nim_mahasiswa",
        field_type=ListType(
            element_id=101, element_type=IntegerType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=19,
        name="nip_anggota_dosen",
        field_type=ListType(
            element_id=102, element_type=IntegerType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=20,
        name="nama_anggota_dosen",
        field_type=ListType(
            element_id=103, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=21,
        name="nama_anggota_mahasiswa",
        field_type=ListType(
            element_id=104, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
)

sitasi_schema = Schema(
    NestedField(field_id=1, name="no", field_type=StringType(), required=False),
    NestedField(field_id=2, name="nama_dosen", field_type=StringType(), required=False),
    NestedField(field_id=3, name="nama_prodi", field_type=StringType(), required=False),
    NestedField(field_id=4, name="fakultas", field_type=StringType(), required=False),
    NestedField(
        field_id=5, name="tanggal_terbit", field_type=StringType(), required=False
    ),
    NestedField(field_id=6, name="kategori", field_type=StringType(), required=False),
    NestedField(field_id=7, name="judul", field_type=StringType(), required=False),
    NestedField(field_id=8, name="sitasi", field_type=StringType(), required=False),
    NestedField(field_id=9, name="triwulan", field_type=StringType(), required=False),
    NestedField(field_id=10, name="publikasi", field_type=StringType(), required=False),
    NestedField(field_id=11, name="doi", field_type=StringType(), required=False),
    NestedField(field_id=12, name="hari", field_type=IntegerType(), required=False),
    NestedField(field_id=13, name="bulan", field_type=IntegerType(), required=False),
    NestedField(field_id=14, name="tahun", field_type=IntegerType(), required=False),
    NestedField(
        field_id=15,
        name="tanggal_terbit_timestamp",
        field_type=TimestampType(),
        required=False,
    ),
)
