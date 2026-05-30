import re
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructType,
    StructField,
    IntegerType,
)
MONTH_MAP = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
    # English fallback
    "january": 1, "february": 2, "march": 3, "may": 5,
    "june": 6, "july": 7, "august": 8, "october": 10, "december": 12,
}

BULAN_MAP = {
    "januari": "january", "februari": "february", "maret": "march",
    "april": "april", "mei": "may", "juni": "june",
    "juli": "july", "agustus": "august", "september": "september",
    "oktober": "october", "november": "november", "desember": "december",
}


def matchUniqueID(text):
    """Extract NIP/NIM from parenthesized text. Returns list of numeric strings or None."""
    if isinstance(text, str):
        result = re.findall(r'\((\d+)\)', text)
        return result or None
    return None


# def matchName(text):
#     """Extract the name portion (everything outside parentheses), stripped."""
#     if isinstance(text, str):
#         name = re.sub(r'\(.*?\)', '', text).strip()
#         # Remove trailing/leading commas or semicolons left over
#         name = name.strip(",;").strip()
#         return name if name else None
#     return None

def matchNames(text):
    """
    Extract multiple names from string like:
    "Dr. Andy Darmawan, S.Si., M.Si.(0031058501),Ayu Oshin Yap Sinaga, S.P., M.Si.(0008089102)"
    → ["Dr. Andy Darmawan, S.Si., M.Si.", "Ayu Oshin Yap Sinaga, S.P., M.Si."]

    Splits by ')' since each group ends with a closing parenthesis.
    """
    if not isinstance(text, str):
        return None
    parts = text.split(")")
    names = []
    for p in parts:
        name = re.sub(r"\(.*", "", p).strip()
        name = name.strip(",; ").strip()
        if name:
            names.append(name)
    return names if names else None

def getProdi(text):
    """Extract study program name from a raw label."""
    if text is None:
        return None
    text = text.lower().strip()
    text = text.replace("program studi", "").strip()
    parts = text.split("-")
    if len(parts) > 1:
        return parts[1].strip()
    return text


def getFaculty(text):
    """Map faculty abbreviation to full name."""
    if text is None:
        return None
    mapper = {
        "FS": "Fakultas Sains",
        "FTI": "Fakultas Teknologi Industri",
        "FTIK": "Fakultas Teknologi Industri dan Kewilayahan",
    }
    key = text.split("-")[0].strip()
    return mapper.get(key)


def mapFacultyDegree(prodi):
    """Map study program name to its parent faculty abbreviation."""
    if prodi is None:
        return None

    mapper = {
        "FS": [
            "biologi", "fisika", "sains lingkungan kelautan",
            "sains atmosfer dan keplanetan", "sap", "sains data",
            "farmasi", "kimia", "aktuaria", "sains aktuaria", "matematika",
        ],
        "FTI": [
            "teknik pertambangan", "teknik elektro", "teknik informatika",
            "teknik kimia", "teknologi pangan", "teknik geologi",
            "rekayasa kosmetik", "teknik material", "teknik biosistem",
            "teknik biomedis", "teknik fisika", "teknik geofisika",
            "teknologi industri pertanian", "teknik industri",
            "rekayasa keolahragaan", "teknik mesin", "teknik sistem energi",
            "rekayasa kehutanan", "rekayasa instrumentasi dan automasi",
            "rekayasa minyak dan gas",
        ],
        "FTIK": [
            "perencanaan wilayah dan kota", "teknik sipil", "arsitektur",
            "arsitektur lanskap", "teknik lingkungan", "teknik geomatika",
            "teknik perkeretaapian", "desain komunikasi visual",
            "rekayasa tata kelola air terpadu", "pariwisata","teknik kelautan"
        ],
    }

    for key, programs in mapper.items():
        if prodi in programs:
            return key
    return None


def normalizeDate(text):
    """Parse date string into (day, month, year) struct."""
    if text is None:
        return (None, None, None)

    text = str(text).strip().lower()

    # 1. YYYY-MM-DD
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return (int(match.group(3)), int(match.group(2)), int(match.group(1)))

    # 2. DD Month YYYY
    match = re.match(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", text)
    if match:
        day = int(match.group(1))
        month = MONTH_MAP.get(match.group(2))
        year = int(match.group(3))
        return (day, month, year)

    # 3. Month YYYY
    match = re.match(r"([a-z]+)\s+(\d{4})", text)
    if match:
        month = MONTH_MAP.get(match.group(1))
        year = int(match.group(2))
        return (None, month, year)

    # 4. YYYY only
    match = re.match(r"^\d{4}$", text)
    if match:
        return (None, None, int(text))

    return (None, None, None)

def mapping_date(df, column):
    """Apply date normalization to a Spark DataFrame column.

    Replaces Indonesian month names, parses the date string into
    separate day, month, and year columns.

    Args:
        df: A PySpark DataFrame.
        column: The name of the column containing date strings.

    Returns:
        The DataFrame with three new columns: "day", "month", "year".
    """
    for indo, eng in BULAN_MAP.items():
        df = df.withColumn(
            column,
            F.regexp_replace(F.col(column), indo, eng),
        )

    df = df.withColumn("parsed", normalize_date_udf(F.col(column)))

    df = (
        df.withColumn("day", F.col("parsed.day"))
        .withColumn("month", F.col("parsed.month"))
        .withColumn("year", F.col("parsed.year"))
        .drop("parsed")
    )

    return df

# user define function spark
match_unique_id_udf = F.udf(matchUniqueID, ArrayType(StringType()))
match_name_udf = F.udf(matchNames, ArrayType(StringType()))
get_prodi_udf = F.udf(getProdi, StringType())
get_faculty_udf = F.udf(getFaculty, StringType())
map_faculty_degree_udf = F.udf(mapFacultyDegree, StringType())

normalizeDateSchema = StructType([
    StructField("day", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True),
])

normalize_date_udf = F.udf(normalizeDate, normalizeDateSchema)