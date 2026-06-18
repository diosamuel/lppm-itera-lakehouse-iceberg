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

# Final output schema for Penelitian, Pengabdian & Buku Keilmuan
# Matches columns produced by Transform.processData()
default_schema = Schema(
    NestedField(field_id=1, name="judul_proposal", field_type=StringType(), required=False),
    NestedField(field_id=2, name="ketua_peneliti", field_type=StringType(), required=False),
    NestedField(field_id=3, name="jenis", field_type=StringType(), required=False),
    NestedField(field_id=4, name="status", field_type=StringType(), required=False),
    NestedField(field_id=5, name="skema", field_type=StringType(), required=False),
    NestedField(field_id=6, name="scope", field_type=StringType(), required=False),
    NestedField(field_id=7, name="sdgs", field_type=StringType(), required=False),
    NestedField(field_id=8, name="usulan_biaya", field_type=LongType(), required=False),
    NestedField(field_id=9, name="status_proposal", field_type=StringType(), required=False),
    NestedField(field_id=10, name="tahun", field_type=IntegerType(), required=False),
    NestedField(field_id=11, name="prodi", field_type=StringType(), required=False),
    NestedField(field_id=12, name="fakultas", field_type=StringType(), required=False),
    NestedField(
        field_id=13,
        name="nip_ketua_peneliti",
        field_type=ListType(
            element_id=301, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=14,
        name="nim_anggota_mahasiswa",
        field_type=ListType(
            element_id=302, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=15,
        name="nama_anggota_mahasiswa",
        field_type=ListType(
            element_id=303, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=16,
        name="nip_anggota_dosen",
        field_type=ListType(
            element_id=304, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=17,
        name="nama_anggota_dosen",
        field_type=ListType(
            element_id=305, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
    NestedField(field_id=18, name="advisor", field_type=StringType(), required=False),
)

# Final output schema for Sitasi
# Matches columns produced by Transform.processSitasiData()
sitasi_schema = Schema(
    NestedField(field_id=1, name="prodi", field_type=StringType(), required=False),
    NestedField(field_id=2, name="fakultas", field_type=StringType(), required=False),
    NestedField(field_id=3, name="ketua_peneliti", field_type=StringType(), required=False),
    NestedField(field_id=4, name="judul_proposal", field_type=StringType(), required=False),
    NestedField(field_id=5, name="sitasi", field_type=LongType(), required=False),
    NestedField(field_id=6, name="triwulan", field_type=LongType(), required=False),
    NestedField(field_id=7, name="publikasi", field_type=StringType(), required=False),
    NestedField(field_id=8, name="doi", field_type=StringType(), required=False),
    NestedField(field_id=9, name="tanggal_terbit_hari", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="tanggal_terbit_bulan", field_type=IntegerType(), required=False),
    NestedField(field_id=11, name="tanggal_terbit_tahun", field_type=IntegerType(), required=False),
    NestedField(
        field_id=12,
        name="tanggal_terbit_timestamp",
        field_type=TimestampType(),
        required=False,
    ),
    NestedField(field_id=13, name="jurnal", field_type=StringType(), required=False),
    NestedField(field_id=14, name="jurnal_kategori", field_type=StringType(), required=False),
)
