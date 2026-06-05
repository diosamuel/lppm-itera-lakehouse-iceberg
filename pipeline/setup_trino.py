import os

import trino
from dotenv import load_dotenv

load_dotenv()


class SetupTrino:
    """
    Thin wrapper around the Trino DBAPI connection, consistent with
    the SetupSpark / SetupIcebergCatalog pattern in this project.

    Usage:
        trino_client = SetupTrino(catalog="iceberg", schema="default").initialize()
        rows = trino_client.execute("SELECT * FROM penelitian LIMIT 10")
    """

    def __init__(
        self,
        catalog: str = "default",
        schema: str = "default",
    ):
        self.host = os.getenv("TRINO_HOST", "localhost")
        self.port = int(os.getenv("TRINO_PORT", "8085"))
        self.user = os.getenv("TRINO_USER", "trino")
        self.catalog = catalog
        self.schema = schema
        self.conn = None

    def initialize(self):
        self.conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )
        print(
            f"Trino connected → {self.host}:{self.port} "
            f"| catalog: {self.catalog} | schema: {self.schema}"
        )
        return self

    # ------------------------------------------------------------------
    # Core helpers
    # ------------------------------------------------------------------

    def cursor(self):
        """Return a raw Trino cursor for manual control."""
        return self.conn.cursor()

    def execute(self, query: str, params=None) -> list:
        """
        Run a query and return all rows as a list of tuples.

        Example:
            rows = trino_client.execute("SELECT * FROM penelitian LIMIT 5")
        """
        cur = self.cursor()
        cur.execute(query, params)
        return cur.fetchall()

    def execute_df(self, query: str, params=None):
        """
        Run a query and return a pandas DataFrame.
        Requires pandas to be installed.

        Example:
            df = trino_client.execute_df("SELECT tahun, COUNT(*) FROM penelitian GROUP BY tahun")
        """
        import pandas as pd  # lazy import — pandas is optional

        cur = self.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=columns)

    # ------------------------------------------------------------------
    # Catalog introspection helpers
    # ------------------------------------------------------------------

    def show_schemas(self) -> list:
        return [row[0] for row in self.execute(f"SHOW SCHEMAS FROM {self.catalog}")]

    def show_tables(self, schema: str = None) -> list:
        target = schema or self.schema
        return [
            row[0]
            for row in self.execute(f"SHOW TABLES FROM {self.catalog}.{target}")
        ]

    def describe_table(self, table: str, schema: str = None) -> list:
        target_schema = schema or self.schema
        return self.execute(
            f"DESCRIBE {self.catalog}.{target_schema}.{table}"
        )

    # ------------------------------------------------------------------
    # Iceberg metadata helpers
    # ------------------------------------------------------------------

    def table_snapshots(self, table: str, schema: str = None):
        """Return snapshot history for an Iceberg table."""
        target_schema = schema or self.schema
        return self.execute_df(
            f'SELECT * FROM {self.catalog}.{target_schema}."{table}$snapshots"'
        )

    def table_files(self, table: str, schema: str = None):
        """Return data file info for an Iceberg table."""
        target_schema = schema or self.schema
        return self.execute_df(
            f'SELECT * FROM {self.catalog}.{target_schema}."{table}$files"'
        )

    def close(self):
        if self.conn:
            self.conn.close()
            print("Trino connection closed.")
