import os

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

load_dotenv()


class SetupIcebergCatalog:
    def __init__(self, catalog_name, namespace):
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.catalog = None

    def initialize(self):
        self.catalog = load_catalog(
            self.catalog_name,
            **{
                "type": "rest",
                "uri": os.getenv("REST_CATALOG_URL", "http://rest:8181"),
                "s3.endpoint": os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
                "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "admin"),
                "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "password"),
            },
        )
        self.create_namespace()
        return self

    def create_namespace(self, namespace=None):
        ns = namespace or self.namespace
        try:
            self.catalog.create_namespace_if_not_exists(ns)
        except Exception:
            pass  # Namespace already exists, ignore
        print(f"Namespace '{ns}' ready.")
        return self.catalog

    def create_table(self, table_name, schema):
        full_name = f"{self.namespace}.{table_name}"
        existing_tables = [t[1] for t in self.catalog.list_tables(self.namespace)]
        if table_name in existing_tables:
            try:
                return self.catalog.load_table(full_name)
            except NoSuchTableError:
                print(f"drop '{full_name}'")
                self.catalog.drop_table(full_name)

        table = self.catalog.create_table(full_name, schema=schema)
        return table

    def create_replace_table(self, table_name, schema):
        full_name = f"{self.namespace}.{table_name}"
        existing_tables = [t[1] for t in self.catalog.list_tables(self.namespace)]
        if table_name in existing_tables:
            self.catalog.drop_table(full_name)
        table = self.catalog.create_table(full_name, schema=schema)
        return table

    def get_table(self, table_name):
        return self.catalog.load_table(f"{self.namespace}.{table_name}")
