from pyiceberg.catalog import load_catalog
import os
from dotenv import load_dotenv
load_dotenv()

class SetupCatalog:
    def __init__(self, catalog_name="lppm", namespace="default"):
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.catalog = None

    def init(self):
        """Initialize connection to the Iceberg REST Catalog."""
        self.catalog = load_catalog(
            self.catalog_name,
            **{
                "type": "rest",
                "uri": os.getenv("REST_CATALOG_URL", "http://localhost:8181"),
                "s3.endpoint": os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
                "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "admin"),
                "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "password"),
            },
        )
        return self

    def create_namespace(self):
        """Create the namespace if it doesn't exist."""
        existing = [ns[0] for ns in self.catalog.list_namespaces()]
        if self.namespace not in existing:
            self.catalog.create_namespace_if_not_exists(self.namespace)
            print(f"Namespace '{self.namespace}' created.")
        else:
            print(f"Namespace '{self.namespace}' already exists.")
        return self

    def create_table(self, table_name, schema=None):
        full_name = f"{self.namespace}.{table_name}"

        existing_tables = [t[1] for t in self.catalog.list_tables(self.namespace)]
        if table_name in existing_tables:
            return self.catalog.load_table(full_name)

        table = self.catalog.create_table(full_name, schema=schema)
        return table