from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

PREFIXES = ["dr. ", "dr ", "dr. eng. ", "eng. ", "ir. ", "ir ",
            "prof. ", "prof ", "prof. dr. ", "apt. ", "apt "]

def base_name(name):
    if not name:
        return None
    name = name.strip().rstrip(".")
    # strip multiple prefixes berurutan
    while True:
        matched = False
        name_lower = name.lower()
        for p in PREFIXES:
            if name_lower.startswith(p):
                name = name[len(p):].strip()
                matched = True
                break
        if not matched:
            break
    if "," in name:
        name = name[: name.index(",")].strip()
    return name.lower()

base_name_udf = F.udf(base_name, StringType())

spark = SparkSession.builder.appName("map_dosen").getOrCreate()

# Ambil nama dari sitasi & dim_dosen
sitasi = spark.sql("SELECT DISTINCT TRIM(TRAILING '.' FROM ketua_peneliti) AS nama FROM silver.sitasi")
dosen = spark.sql("SELECT nama FROM gold.dim_dosen")

# Base name matching
sitasi_base = sitasi.withColumn("base", base_name_udf("nama"))
dosen_base = dosen.withColumn("base", base_name_udf("nama"))

# Join
mapped = sitasi_base.alias("s").join(
    dosen_base.alias("d"),
    F.col("s.base") == F.col("d.base"),
    "left"
).select(
    F.col("s.nama").alias("sitasi_nama"),
    F.col("d.nama").alias("dosen_nama")
).distinct().orderBy("sitasi_nama")

mapped.show(200, truncate=False)

# Truly unmatched
unmatched = mapped.filter(F.col("dosen_nama").isNull())
print(f"\nTotal truly unmatched: {unmatched.count()}")
