import io
import os
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tools.utils import (
    clean_tanggal_udf,
    get_faculty_udf,
    get_prodi_udf,
    map_faculty_degree_udf,
    match_name_udf,
    match_unique_id_udf,
    removeNaN_udf,
    capture_doi_udf,
    standarizing_journal_udf,
)
load_dotenv()
class Transform:
    def __init__(self, spark, document_type):
        self.spark = spark
        self.document_type = document_type
        self._batches: list = []

    def toSparkDF(self, load_response):
        """Convert raw CSV bytes or a StorageS3.load() response dict into a Spark DataFrame."""
        if isinstance(load_response, bytes):
            raw_bytes = load_response
        elif isinstance(load_response, dict):
            if not load_response.get("status"):
                raise ValueError(f"Failed to load file: {load_response.get('message')}")
            raw_bytes = load_response["content"]
        else:
            raise TypeError(
                f"Expected bytes or dict, got {type(load_response).__name__}"
            )
        pdf = pd.read_csv(io.BytesIO(raw_bytes))
        pdf = pdf.loc[
            :,
            pdf.columns.str.strip().astype(bool)
            & ~pdf.columns.str.match(r"^Unnamed: \d+$"),
        ]
        return self.spark.createDataFrame(pdf)

    def processData(self, df, tahun):
        """
        df = raw CSV bytes or StorageS3.load() response dict
        """
        spark_df = self.toSparkDF(df)
        # Infer schema & rename columns
        rename_map = {
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

        for old, (new, dtype) in rename_map.items():
            if old in spark_df.columns:
                spark_df = spark_df.withColumnRenamed(old, new)
                if dtype == "string":
                    spark_df = spark_df.withColumn(new, removeNaN_udf(F.col(new)))
        spark_df = spark_df.drop("No")

        # Transform
        transformed = (
            spark_df.withColumn("tahun", F.lit(str(tahun)))
            .withColumn("prodi", get_prodi_udf(F.col("program_studi")))
            .withColumn("fakultas", get_faculty_udf(F.col("program_studi")))
            .withColumn("ketua_peneliti",match_name_udf(F.col("ketua_peneliti"))[0])
            .withColumn("nip_ketua_peneliti",match_unique_id_udf(F.col("ketua_peneliti")))
            .withColumn("nim_anggota_mahasiswa", match_unique_id_udf(F.col("anggota_mahasiswa")))
            .withColumn("nama_anggota_mahasiswa", match_name_udf(F.col("anggota_mahasiswa")))
            .withColumn("nip_anggota_dosen", match_unique_id_udf(F.col("anggota_dosen")))
            .withColumn("nama_anggota_dosen", match_name_udf(F.col("anggota_dosen")))
            .withColumn("advisor",match_name_udf(F.col("advisor"))[0])
            .replace(float("nan"), None)
            .replace("", None)
            .drop('program_studi')
            .drop('anggota_dosen')
            .drop('anggota_mahasiswa')
        )
        self._batches.append(transformed)
        return self

    def processSitasiData(self, df, tahun):
        spark_df = self.toSparkDF(df)
        rename_map = {
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
        }

        for old, (new, dtype) in rename_map.items():
            if old in spark_df.columns:
                spark_df = spark_df.withColumnRenamed(old, new)
                if dtype == "string":
                    spark_df = spark_df.withColumn(new, removeNaN_udf(F.col(new)))

        spark_df = (
            spark_df.drop("No")
            .withColumn("ketua_peneliti",match_name_udf(F.col("nama_dosen"))[0])
            .withColumn("fakultas", map_faculty_degree_udf(F.lower(F.col("prodi"))))
            .withColumn("_tanggal_terbit", clean_tanggal_udf(F.col("tanggal_terbit")))
            .withColumn("tanggal_terbit_hari", F.col("_tanggal_terbit.tanggal"))
            .withColumn("tanggal_terbit_bulan", F.col("_tanggal_terbit.bulan"))
            .withColumn("tanggal_terbit_tahun", F.col("_tanggal_terbit.tahun"))
            .withColumn("tanggal_terbit_timestamp", F.col("_tanggal_terbit.timestamp"))
            .withColumn("doi",capture_doi_udf(F.col("doi")))
            .withColumn("_journal", standarizing_journal_udf(F.col("kategori")))
            .withColumn("jurnal", F.col("_journal.jurnal"))
            .withColumn("jurnal_kategori", F.col("_journal.groups"))
            .drop("_journal")
            .drop("kategori")
            .drop("_tanggal_terbit")
            .drop("tanggal_terbit")
            .drop("nama_dosen")
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
