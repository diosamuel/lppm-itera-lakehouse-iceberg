import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from setup.setup_catalog import SetupIcebergCatalog
from setup.setup_spark import SetupSpark

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent


def run_sql_file(spark, sql_file):
    sql_text = Path(sql_file).read_text(encoding="utf-8")
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        spark.sql(stmt)


def init_audit(spark):
    """Create audit tables for mismatched dimension data."""
    run_sql_file(spark, BASE_DIR / "audit" / "audit_faktor_hibah.sql")
    print("Created audit.audit_faktor_hibah")

    run_sql_file(spark, BASE_DIR / "audit" / "audit_dosen_hibah.sql")
    print("Created audit.audit_dosen_hibah")

    run_sql_file(spark, BASE_DIR / "audit" / "audit_sitasi.sql")
    print("Created audit.audit_sitasi")


def enable_wap(spark):
    """Enable Write Audit Publish and create audit-swap branch on gold fact tables."""
    wap_tables = ["gold.fact_hibah", "gold.fact_dosen_hibah", "gold.fact_sitasi"]
    for table in wap_tables:
        spark.sql(f"""
            ALTER TABLE {table} SET TBLPROPERTIES (
                'write.wap.enabled'='true'
            )
        """)
        spark.sql(f"""
            ALTER TABLE {table} DROP BRANCH IF EXISTS `audit-swap`
        """)
        spark.sql(f"""
            ALTER TABLE {table} CREATE BRANCH `audit-swap`
        """)
        print(f"WAP enabled + audit-swap branch created on {table}")


if __name__ == "__main__":
    SparkSession = SetupSpark(
        app_name="init_audit", catalog_name="default"
    ).initialize()

    init_audit(SparkSession)
    enable_wap(SparkSession)

    print("Audit schema + WAP init complete")
