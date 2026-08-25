import os
import re
from io import BytesIO
import boto3
import openpyxl
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def _default_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
    )


def list_xlsx_sheets(s3_path, s3_client=None):
    matchS3 = re.compile(r"^s3a?://([^/]+)/(.+)$")
    m = matchS3.match(s3_path)
    if not m:
        raise ValueError(f"Expected an s3a://bucket/key path, got {s3_path!r}")
    bucket, key = m.group(1), m.group(2)
    client = s3_client or _default_s3_client()
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    wb = openpyxl.load_workbook(BytesIO(body), read_only=True)
    try:
        return list(wb.sheetnames)
    finally:
        wb.close()


def list_xlsx_year_sheets(s3_path, s3_client=None):
    matchYear = re.compile(r"\d{4}")
    return [s for s in list_xlsx_sheets(s3_path, s3_client) if matchYear.fullmatch(s)]


def read_xlsx_raw(spark, s3_path, sheet_name):
    return (
        spark.read.format("excel")
        .option("header", "false")
        .option("keepEmptyRows", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .option("dataAddress", f"'{sheet_name}'!A1")
        .load(s3_path)
    )


def find_first_nonempty_column(raw, value_cols):
    counts = raw.select(*[F.count(F.col(c)).alias(c) for c in value_cols]).first().asDict()
    for idx, col_name in enumerate(value_cols):
        if counts.get(col_name, 0) > 0:
            return idx
    raise ValueError("Every column is empty; cannot find a header column.")


def find_last_nonempty_column(raw, value_cols):
    counts = raw.select(*[F.count(F.col(c)).alias(c) for c in value_cols]).first().asDict()
    last_idx = -1
    for idx, col_name in enumerate(value_cols):
        if counts.get(col_name, 0) > 0:
            last_idx = idx
    if last_idx < 0:
        raise ValueError("Every column is empty; cannot find a header column.")
    return last_idx


def find_first_nonempty_row(raw, value_cols) -> int:
    cells = F.array(*[F.col(c) for c in value_cols])
    has_content = F.size(F.filter(cells, lambda x: x.isNotNull())) > 0
    row = (
        raw.withColumn("_has_content", has_content)
        .filter("_has_content")
        .select(F.min(F.col("_row_id")).alias("first_row"))
        .first()
    )
    if row is None or row["first_row"] is None:
        raise ValueError("Every row is empty; cannot find a header row.")
    return int(row["first_row"])


def sanitizeHeaders(values, fallback):
    seen = {}
    result = []
    for raw_name, fb in zip(values, fallback):
        name = str(raw_name).strip() if raw_name is not None and str(raw_name).strip() else fb
        unique = name
        while unique in seen:
            seen[name] += 1
            unique = f"{name}_{seen[name]}"
        seen[unique] = 1
        result.append(unique)
    return result


def clean_xlsx_sheet(spark, s3_path, sheet_name):
    raw = read_xlsx_raw(spark, s3_path, sheet_name).withColumn(
        "_row_id", F.monotonically_increasing_id()
    )
    value_cols = [c for c in raw.columns if c != "_row_id"]

    first_col_idx = find_first_nonempty_column(raw, value_cols)
    last_col_idx = find_last_nonempty_column(raw, value_cols)
    kept_cols = value_cols[first_col_idx : last_col_idx + 1]

    first_row_id = find_first_nonempty_row(raw, value_cols)
    content = raw.filter(F.col("_row_id") >= first_row_id)

    header_row = content.filter(F.col("_row_id") == first_row_id).select(*kept_cols).first()
    if header_row is None:
        raise ValueError(f"Header row vanished for sheet '{sheet_name}'.")
    header_dict = header_row.asDict()
    header_values = [header_dict[c] for c in kept_cols]
    new_names = sanitizeHeaders(header_values, kept_cols)

    body = content.filter(F.col("_row_id") > first_row_id).select(*kept_cols)
    for old, new in zip(kept_cols, new_names):
        body = body.withColumnRenamed(old, new)
    return body
