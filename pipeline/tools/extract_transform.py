from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType
from tools.jurnal_clean import clean_publikasi_udf
from tools.utils import (
    capture_doi_udf,
    clean_tanggal_udf,
    get_faculty_udf,
    get_prodi_udf,
    map_faculty_degree_udf,
    match_name_udf,
    match_unique_id_udf,
    normalize_whitespace_udf,
    removeNaN_udf,
    standarizing_journal_udf,
    remove_non_alphanumeric_udf,
    map_skema_udf
)

load_dotenv()
class Transform:
    RENAME_MAPS = {
        "penelitian": {
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
        },
        "sitasi": {
            "Nama Dosen": ("nama_dosen", "string"),
            "Nama Prodi": ("prodi", "string"),
            "Fakultas": ("fakultas", "string"),
            "Tanggal Terbit": ("tanggal_terbit", "string"),
            "Kategori": ("kategori", "string"),
            "Judul": ("judul_proposal", "string"),
            "Sitasi": ("sitasi", "long"),
            "Triwulan": ("triwulan", "long"),
            "Publikasi": ("publikasi", "string"),
            "DOI": ("doi", "string"),
        },
    }

    def __init__(self, spark, document_type):
        self.spark = spark
        self.document_type = document_type
        self._batches: list = []

    def renameAndCast(self, spark_df, rename_map):
        if "_c0" in spark_df.columns:
            spark_df = spark_df.drop("_c0")
        if "No" in spark_df.columns:
            spark_df = spark_df.drop("No")
        for old, (new, dtype) in rename_map.items():
            if old not in spark_df.columns:
                continue
            spark_df = spark_df.withColumnRenamed(old, new)
            if dtype == "string":
                spark_df = spark_df.withColumn(new, removeNaN_udf(F.col(new)))
            elif dtype == "long":
                spark_df = spark_df.withColumn(new, F.col(new).cast(LongType()))
        return spark_df

    def processData(self, df, tahun):
        """
        df = raw CSV bytes or StorageS3.load() response dict
        """
        spark_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(df)
        rename_map = self.RENAME_MAPS.get(self.document_type, self.RENAME_MAPS["penelitian"])
        spark_df = self.renameAndCast(spark_df, rename_map)

        # Transform
        transformed = (
            spark_df.withColumn("tahun", F.lit(tahun).cast(IntegerType()))
            .withColumn("prodi", get_prodi_udf(F.col("program_studi")))
            .withColumn("fakultas", get_faculty_udf(F.col("program_studi")))
            .withColumn("nip_ketua_peneliti", match_unique_id_udf(F.col("ketua_peneliti")))
            .withColumn("ketua_peneliti", match_name_udf(F.col("ketua_peneliti"))[0])
            .withColumn("nim_anggota_mahasiswa", match_unique_id_udf(F.col("anggota_mahasiswa")))
            .withColumn("nama_anggota_mahasiswa", match_name_udf(F.col("anggota_mahasiswa")))
            .withColumn("nip_anggota_dosen", match_unique_id_udf(F.col("anggota_dosen")))
            .withColumn("nama_anggota_dosen", match_name_udf(F.col("anggota_dosen")))
            .withColumn("advisor", match_name_udf(F.col("advisor"))[0])
            .withColumn("skema", remove_non_alphanumeric_udf(F.col("skema")))
            .withColumn("skema", map_skema_udf(F.col("skema")))
            .withColumn("sdgs", remove_non_alphanumeric_udf(F.col("sdgs")))
            .withColumn("sdgs", map_skema_udf(F.col("sdgs")))
            .drop("program_studi", "anggota_dosen", "anggota_mahasiswa")
            .replace(float("nan"), None)
            .replace("", None)
        )
        self._batches.append(transformed)
        return self

    def processSitasiData(self, df, tahun):
        spark_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(df)
        spark_df = self.renameAndCast(spark_df, self.RENAME_MAPS["sitasi"])

        # Extract components from tanggal_terbit
        tanggal = clean_tanggal_udf(F.col("tanggal_terbit"))
        # Extract components from kategori (journal standardization)
        journal = standarizing_journal_udf(F.col("kategori"))

        spark_df = (
            spark_df
            # Rename & transform core fields
            .withColumn("ketua_peneliti", match_name_udf(F.col("nama_dosen"))[0])
            .withColumn("fakultas", map_faculty_degree_udf(F.lower(F.col("prodi"))))
            .withColumn("prodi", get_prodi_udf(F.col("prodi")))
            .withColumn("doi", capture_doi_udf(F.col("doi")))
            # Expand tanggal_terbit struct
            .withColumn("tanggal_terbit_hari", tanggal["tanggal"])
            .withColumn("tanggal_terbit_bulan", tanggal["bulan"])
            .withColumn("tanggal_terbit_tahun", tanggal["tahun"])
            .withColumn("tanggal_terbit_timestamp", F.when(tanggal["timestamp"].isNotNull(), tanggal["timestamp"]))
            # Expand kategori/journal struct
            .withColumn("jurnal", journal["jurnal"])
            .withColumn("jurnal_kategori", journal["groups"])
            # Expand publikasi struct
            .withColumn("publikasi", clean_publikasi_udf(F.col("publikasi")))
            .withColumn("jurnal_nama", F.col("publikasi.jurnal_nama"))
            .withColumn("jurnal_volume", F.col("publikasi.jurnal_volume"))
            .withColumn("jurnal_issue", F.col("publikasi.jurnal_issue"))
            .withColumn("jurnal_halaman", F.col("publikasi.jurnal_halaman"))
            .withColumn("jurnal_tahun", F.col("publikasi.jurnal_tahun"))
            # Drop intermediate & source columns
            .drop("tanggal_terbit", "nama_dosen", "triwulan", "kategori", "publikasi")
        )

        self._batches.append(spark_df)
        return self

    def join(self):
        """Union all accumulated batches into a single DataFrame and reset state."""
        if not self._batches:
            return None
        result = self._batches[0]
        for batch in self._batches[1:]:
            result = result.unionByName(batch, allowMissingColumns=True)
        self._batches = []
        return result
