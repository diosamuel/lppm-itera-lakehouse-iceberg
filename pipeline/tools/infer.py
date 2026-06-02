from pyspark.sql import functions as F
from tools.utils_spark import (
    get_faculty_udf,
    get_prodi_udf,
    map_faculty_degree_udf,
    mapping_date,
    match_name_udf,
    match_unique_id_udf,
    normalize_date_udf,
)

"""
df: Spark Dataframe
processData: penelitian, pengabdian, buku keilmuan
"""


def processData(df):
    # Infer schema & rename columns
    rename_map = {
        "No": ("no", "long"),
        "Judul Proposal": ("judul_proposal", "string"),
        "Ketua Peneliti": ("ketua_peneliti", "string"),
        "Jenis": ("jenis", "string"),
        "Status": ("status", "string"),
        "Skema": ("skema", "string"),
        "Scope": ("scope", "string"),
        "SDGs": ("sdgs", "string"),
        "Program Studi": ("program_studi", "string"),
        "Anggota Dosen": ("anggota_dosen", "string"),
        "Anggota Mahasiswa": ("anggota_mahasiswa", "string"),
        "Advisor": ("advisor", "string"),
        "Usulan Biaya": ("usulan_biaya", "long"),
        "Status Proposal": ("status_proposal", "string"),
    }

    existing_cols = set(df.columns)
    for old, (new, dtype) in rename_map.items():
        if old in existing_cols:
            df = df.withColumn(new, F.col(old).cast(dtype)).drop(old)

    # Transform
    transformed = (
        df.withColumn("tahun", F.lit(str(tahun)))
        .withColumn("prodi", get_prodi_udf(F.col("program_studi")))
        .withColumn("fakultas", get_faculty_udf(F.col("program_studi")))
        .withColumn(
            "nim_anggota_mahasiswa", match_unique_id_udf(F.col("anggota_mahasiswa"))
        )
        .withColumn(
            "name_anggota_mahasiswa", match_name_udf(F.col("anggota_mahasiswa"))
        )
        .withColumn("nip_anggota_dosen", match_unique_id_udf(F.col("anggota_dosen")))
        .withColumn("name_anggota_dosen", match_name_udf(F.col("anggota_dosen")))
    )
    return transformed


"""
df: Spark Dataframe
"""


def processSitasiData(df):
    rename_map = {
        "No": ("no", "long"),
        "Nama Dosen": ("nama_dosen", "string"),
        "Nama Prodi": ("nama_prodi", "string"),
        "Fakultas": ("fakultas", "string"),
        "Tanggal Terbit": ("tanggal_terbit", "string"),
        "Kategori": ("kategori", "string"),
        "Judul": ("judul", "string"),
        "Sitasi": ("sitasi", "long"),
        "Triwulan": ("triwulan", "long"),
        "Publikasi": ("publikasi", "string"),
        "DOI": ("doi", "string"),
    }

    existing_cols = set(df.columns)
    for old, (new, dtype) in rename_map.items():
        if old in existing_cols:
            df = df.withColumn(new, F.col(f"`{old}`").cast(dtype)).drop(old)

    return df
