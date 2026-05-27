import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os

from setup_minio import MinioS3
from tools.utils_spark import (
    match_unique_id_udf,
    match_name_udf,
    get_prodi_udf,
    get_faculty_udf,
    map_faculty_degree_udf,
    normalize_date_udf,
    mapping_date,
)

load_dotenv()


class Extract:
    def __init__(self):
        self.data = "sipaper.xlsx"
        self.df = None
        self.minio = MinioS3(
            endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            bucket="lake",
        ).initialize()

    def buildSpark(self):
        spark = SparkSession.builder.appName("LPPM").getOrCreate()
        return spark

    def readData(self):
        """Read sipaper.xlsx from MinIO lake bucket."""
        content = self.minio.load(self.data)
        if isinstance(content, dict) and content.get("status") == "error":
            raise FileNotFoundError(content["message"])
        self.df = pd.read_excel(io.BytesIO(content), sheet_name=None, engine="openpyxl")
        return self.df

    def getSheetsByPrefix(self, prefix):
        """Get sheet names that start with a given prefix."""
        return [name for name in self.df.keys() if name.startswith(prefix)]

    def getPenelitian(self):
        return self.getSheetsByPrefix("Penelitian")

    def getPengabdian(self):
        return self.getSheetsByPrefix("Pengabdian")

    def getBuku(self):
        return self.getSheetsByPrefix("Buku Keilmuan")

    def getSitasi(self):
        return self.getSheetsByPrefix("Data Sitasi")


class Transform(Extract):
    def __init__(self, document_type):
        super().__init__()
        self.document_type = document_type
        self.spark = self.buildSpark()

    def transform(self):
        """Route transformation based on document_type."""
        self.readData()

        if self.document_type == "penelitian":
            return self.transformPenelitian()
        elif self.document_type == "pengabdian":
            return self._transform_pengabdian()
        elif self.document_type == "buku":
            return self.transformBuku()
        elif self.document_type == "sitasi":
            return self.transformSitasi()
        else:
            raise ValueError(f"Unknown document_type: {self.document_type}")

    def transformDefault(self, sheet_names):
        """Transform sheets that share the default schema (penelitian, pengabdian, buku)."""
        spark_dfs = []
        for sheet_name in sheet_names:
            tahun = sheet_name.split("-")[-1].strip() # if the name was pengabdian - 2021 for example
            print(f"Processing: {sheet_name}")

            temp_df = (
                self.spark.createDataFrame(self.df[sheet_name])
                .withColumn("tahun", F.lit(tahun))
                .withColumn("prodi", get_prodi_udf(F.col("Program Studi")))
                .withColumn("fakultas", get_faculty_udf(F.col("Program Studi")))
                .withColumn("nim_mahasiswa", match_unique_id_udf(F.col("Anggota Mahasiswa")))
                .withColumn("nip_anggota_dosen", match_unique_id_udf(F.col("Anggota Dosen")))
                .withColumn("anggota_dosen", match_name_udf(F.col("Anggota Dosen")))
                .withColumn("mahasiswa", match_name_udf(F.col("Anggota Mahasiswa")))
            )
            spark_dfs.append(temp_df)

        final_df = spark_dfs[0]
        for sdf in spark_dfs[1:]:
            final_df = final_df.unionByName(sdf, allowMissingColumns=True)

        return final_df

    def transformPenelitian(self):
        """Transform penelitian sheets."""
        return self.transformDefault(self.getPenelitian())

    def transformPengabdian(self):
        """Transform pengabdian sheets."""
        return self.transformDefault(self.getPengabdian())

    def transformBuku(self):
        """Transform buku keilmuan sheets."""
        return self.transformDefault(self.getBuku())

    def transformSitasi(self):
        """Transform sitasi sheets into a single Spark DataFrame."""
        spark_dfs = []
        for sheet_name in self.getSitasi():
            tahun = sheet_name.split("-")[-1].strip()
            print(f"Processing: {sheet_name}")

            temp_df = self.spark.createDataFrame(self.df[sheet_name])
            temp_df = temp_df.withColumn("tahun", F.lit(tahun))
            temp_df = mapping_date(temp_df, "Tanggal Terbit")
            spark_dfs.append(temp_df)

        final_df = spark_dfs[0]
        for sdf in spark_dfs[1:]:
            final_df = final_df.unionByName(sdf, allowMissingColumns=True)

        return final_df
