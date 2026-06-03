# Write-Audit-Publish (WAP) workflow for Apache Iceberg + PySpark
#
# Workflow
# ────────
# WRITE
#   1. Generate a unique WAP session id (wap_id)
#   2. ALTER TABLE … SET TBLPROPERTIES 'write.wap.enabled' = 'true'
#   3. spark.conf.set("spark.wap.id", wap_id)  → all writes go to a staged branch
#   4. Append / overwrite data — changes are isolated from the main branch
#
# AUDIT
#   1. Locate the WAP snapshot via the snapshots metadata table
#   2. Time-travel read the staged snapshot for inspection
#   3. Check for NULL / NaN values in key columns (usulan_biaya, ketua_peneliti, …)
#   4. If validation fails → discard (don't publish); optionally rollback main branch
#
# PUBLISH
#   5. cherrypick_snapshot → promotes the staged snapshot into the main branch
#      OR rollback_to_snapshot → reverts main to a known-good snapshot on failure

import os
import sys
import uuid

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))
from setup_catalog import SetupIcebergCatalog
from setup_spark import SetupSpark

load_dotenv()


class WAPWorkflow:
    """
    Write-Audit-Publish workflow for an Apache Iceberg table.
    """

    def __init__(
        self,
        spark,
        catalog,
        namespace: str,
        table_name: str,
        catalog_name: str,
    ):
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.table_name = table_name
        self.catalog_name = catalog_name

        # Spark SQL reference: <namespace>.<table>  (default catalog is already set)
        self.full_table = f"{namespace}.{table_name}"

        # Unique identifier for this WAP session
        self.wap_id = None
        self.wap_snapshot_id = None

    # Write phase
    def enable_wap(self):
        """Enable WAP on the table and register the session branch id."""
        wapSyntaxEnabled = f"""
            ALTER TABLE {self.full_table}
            SET TBLPROPERTIES ('write.wap.enabled' = 'true')
        """
        self.spark.sql(wapSyntaxEnabled)
        self.wap_id = uuid.uuid4().hex
        self.spark.conf.set("spark.wap.id", self.wap_id)
        return self

    def write(self, df):
        """Append the DataFrame to the WAP branch."""
        df.writeTo(self.full_table).append()
        return self

    # Audit phase
    def get_wap_snapshot(self):
        """Retrieve the snapshot id created by this WAP session."""
        snapshotSQL = f"""
            SELECT snapshot_id FROM {self.full_table}.snapshots
            ORDER BY committed_at DESC
        """
        snapshots_df = self.spark.sql(snapshotSQL)
        wap_row = snapshots_df.head().snapshot_id
        if wap_row is None:
            raise RuntimeError(f"WAP snapshot not found for wap_id={self.wap_id}. ")

        self.wap_snapshot_id = wap_row
        return self.wap_snapshot_id

    def audit_transform(self, column_name: str, default_value=None):
        if self.wap_snapshot_id is None:
            self.get_wap_snapshot()

        staged_df = self.spark.read.option("snapshot-id", self.wap_snapshot_id).table(
            self.full_table
        )

        if column_name in staged_df.columns:
            print(f"Audit PASSED — column '{column_name}' already exists.")
            return True

        # Column missing — add it and overwrite the staged snapshot
        print(
            f"Column '{column_name}' not found — adding with default: {default_value!r}"
        )
        transformed_df = staged_df.withColumn(column_name, F.lit(default_value))
        transformed_df.writeTo(self.full_table).overwrite(F.lit(True))
        self.get_wap_snapshot()  # refresh to point at the new overwrite snapshot
        print(f"Transformation done — column '{column_name}' added and re-staged.")
        return True

    def publish(self):
        """Cherry-pick the staged WAP snapshot into the main branch."""
        if self.wap_snapshot_id is None:
            self.get_wap_snapshot()

        cherrypickPublishSQL = f"""
            CALL {self.catalog_name}.system.cherrypick_snapshot
            ('{self.full_table}', {self.wap_snapshot_id})
        """
        self.spark.sql(cherrypickPublishSQL)
        return self
