import pandas as pd
from pyspark.sql import SparkSession
from pyiceberg.catalog import load_catalog
import dotenv
from tools import utils

#Store in S3

class Extract:
    def __init__(self):
        self.data = "sipaper.xlsx"
        self.df = None

    def buildSpark(self):
        spark = SparkSession.builder.appName("LPPM").getOrCreate()
        return spark

    def readData(self):
        self.df = pd.read_excel(self.data, engine="openpyxl")
        return self.df


class Transform(Extract):
    def __init__(self, pipeline_type):
        super().__init__()
        self.type = pipeline_type

    def cleanup(self):
        df = self.readData()

        if self.type == "penelitian":
            df["NIM"] = df["Anggota Mahasiswa"].apply(utils.matchUniqueID)
            df["NIP Ketua"] = df["Ketua Peneliti"].apply(utils.matchUniqueID)
            df["NIP Anggota Dosen"] = df["Anggota Dosen"].apply(utils.matchUniqueID)

        self.df = df
        return df