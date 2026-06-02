import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


class SetupSpark:
    def __init__(self, appname, catalog):
        self.app_name = appname
        self.catalog_name = catalog

        rest_uri = os.getenv("REST_CATALOG_URL", "http://rest:8181")
        s3_endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "password")
        catalog = self.catalog_name

        self.spark = (
            SparkSession.builder.appName(self.app_name)
            # Iceberg Spark
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # REST catalog registration
            .config(
                f"spark.sql.catalog.{catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{catalog}.type", "rest")
            .config(f"spark.sql.catalog.{catalog}.uri", rest_uri)
            .config(f"spark.sql.catalog.{catalog}.warehouse", "s3://warehouse/")
            .config(
                f"spark.sql.catalog.{catalog}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(f"spark.sql.catalog.{catalog}.s3.endpoint", s3_endpoint)
            # MinIO
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # Default catalog
            .config("spark.sql.defaultCatalog", catalog)
            .getOrCreate()
        )
        print(f"SparkSession '{self.app_name}' started: catalog: '{catalog}'")

    def initialize(self):
        return self.spark
