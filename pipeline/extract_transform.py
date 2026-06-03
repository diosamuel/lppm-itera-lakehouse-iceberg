import io
import os

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
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

load_dotenv()

"""
Transform returned Spark DataFrame
"""


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
        # Drop columns with empty or whitespace-only names
        pdf = pdf.loc[:, pdf.columns.str.strip().astype(bool)]
        return self.spark.createDataFrame(pdf)

    def processData(self, df, tahun):
        df = self.toSparkDF(df)
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
                df = df.withColumn(new, F.col(f"`{old}`").cast(dtype)).drop(old)

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
            .withColumn(
                "nip_anggota_dosen", match_unique_id_udf(F.col("anggota_dosen"))
            )
            .withColumn("name_anggota_dosen", match_name_udf(F.col("anggota_dosen")))
        )
        self._batches.append(transformed)
        return self

    def processSitasiData(self, df, tahun):
        df = self._load_to_spark_df(df)
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

        self._batches.append(df)
        return self

    def cleanData(self, df, tahun):
        if (
            self.document_type == "penelitian"
            or self.document_type == "pengabdian"
            or self.document_type == "buku_keilmuan"
        ):
            df = self.processData(df, tahun)
        elif self.document_type == "sitasi":
            df = self.processSitasiData(df, tahun)
        return df

    def join(self):
        """Union all accumulated batches into a single DataFrame and reset state."""
        if not self._batches:
            return None
        result = self._batches[0]
        for batch in self._batches[1:]:
            result = result.unionByName(batch, allowMissingColumns=True)
        self._batches = []
        return result
