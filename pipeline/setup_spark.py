import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


class SetupSpark:
    def __init__(self, app_name, catalog_name):
        self.app_name = app_name
        self.catalog_name = catalog_name
        self.rest_uri = os.getenv("REST_CATALOG_URL", "http://rest:8181")
        self.s3_endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "password")

        self.spark = None

    def initialize(self):
        # input tools to spark worker
        pipeline_dir = os.path.dirname(os.path.abspath(__file__))
        existing = os.environ.get("PYTHONPATH", "")
        os.environ["PYTHONPATH"] = f"{pipeline_dir}:{existing}".rstrip(":")

        self.spark = (
            SparkSession.builder.appName(self.app_name)
            # Iceberg Spark
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # REST catalog registration
            .config(
                f"spark.sql.catalog.{self.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "rest")
            .config(f"spark.sql.catalog.{self.catalog_name}.uri", self.rest_uri)
            .config(
                f"spark.sql.catalog.{self.catalog_name}.warehouse", "s3://warehouse/"
            )
            .config(
                f"spark.sql.catalog.{self.catalog_name}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                f"spark.sql.catalog.{self.catalog_name}.s3.endpoint", self.s3_endpoint
            )
            # MinIO
            .config("spark.hadoop.fs.s3a.access.key", self.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # Default catalog
            .config("spark.sql.defaultCatalog", self.catalog_name)
            # Make pipeline modules importable inside UDFs on every worker
            .config("spark.executorEnv.PYTHONPATH", pipeline_dir)
            .getOrCreate()
        )
        print(f"SparkSession '{self.app_name}' started: catalog: '{self.catalog_name}'")
        return self.spark
