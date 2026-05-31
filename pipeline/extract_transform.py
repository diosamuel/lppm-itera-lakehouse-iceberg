import io
import pandas as pd
from pyspark.sql import SparkSession, functions as F
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
    def __init__(self,sparkName):
        self.sparkName = sparkName
        self.df = None
        self.minio = MinioS3(
            endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            bucket="lake",
        ).initialize()
        self.spark = None

    def buildSpark(self):
        sparkInit = SparkSession.builder.appName("LPPM").getOrCreate()
        self.spark = sparkInit
        return self.spark

    def readData(self):
        content = self.minio.load(self.data)
        return content

    def getSheetsByPrefix(self, prefix):
        """Get sheet names that start with a given prefix."""
        return [name for name in self.df.keys() if name.startswith(prefix)]

class Transform():
    def __init__(self, document_type):
        super().__init__()
        self.document_type = document_type
        self.spark = self.buildSpark()