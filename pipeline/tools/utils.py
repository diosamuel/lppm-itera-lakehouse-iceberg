import re
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

standarizingJournalSchema = StructType(
    [
        StructField("jurnal", StringType(), True),
        StructField("groups", StringType(), True),
    ]
)

cleanTanggalSchema = StructType(
    [
        StructField("tanggal", IntegerType(), True),
        StructField("bulan", IntegerType(), True),
        StructField("tahun", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

normalizeDateSchema = StructType(
    [
        StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
    ]
)
def removeNaN(value):
    """Return None for NaN floats, empty strings, and null-like sentinel strings."""
    empty_char = {"", "nan", "none", "null", "n/a", "na", "-",'nat','tidak ada'}
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # float NaN != NaN is always True
        return None
    if isinstance(value, str) and value.strip().lower() in empty_char:
        return None
    return value


MONTH_MAP = {
    "januari": 1,
    "februari": 2,
    "maret": 3,
    "april": 4,
    "mei": 5,
    "juni": 6,
    "juli": 7,
    "agustus": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "desember": 12,
    "january": 1,
    "february": 2,
    "march": 3,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "october": 10,
    "december": 12,
}

BULAN_MAP = {
    "januari": "january",
    "februari": "february",
    "maret": "march",
    "april": "april",
    "mei": "may",
    "juni": "june",
    "juli": "july",
    "agustus": "august",
    "september": "september",
    "oktober": "october",
    "november": "november",
    "desember": "december",
}


def matchUniqueID(text):
    text = removeNaN(text)
    if isinstance(text, str):
        result = re.findall(r"\((\d+)\)", text)
        return result or None
    return None


def matchNames(text):
    text = removeNaN(text)
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
    text = removeNaN(text)
    if text is None:
        return None

    text = text.lower().strip()
    text = text.replace("program studi", "").strip()
    parts = text.split("-")
    if len(parts) > 1:
        return parts[1].strip().upper()
    return text


def getFaculty(text):
    text = removeNaN(text)
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
    prodi = removeNaN(prodi)
    if prodi is None:
        return None

    mapper = {
        "FS": [
            "biologi",
            "fisika",
            "sains lingkungan kelautan",
            "sains atmosfer dan keplanetan",
            "sap",
            "sains data",
            "farmasi",
            "kimia",
            "aktuaria",
            "sains aktuaria",
            "matematika",
        ],
        "FTI": [
            "teknik pertambangan",
            "teknik elektro",
            "teknik informatika",
            "teknik kimia",
            "teknologi pangan",
            "teknik geologi",
            "rekayasa kosmetik",
            "teknik material",
            "teknik biosistem",
            "teknik biomedis",
            "teknik fisika",
            "teknik geofisika",
            "teknologi industri pertanian",
            "teknik industri",
            "rekayasa keolahragaan",
            "teknik mesin",
            "teknik sistem energi",
            "rekayasa kehutanan",
            "rekayasa instrumentasi dan automasi",
            "rekayasa minyak dan gas",
        ],
        "FTIK": [
            "perencanaan wilayah dan kota",
            "teknik sipil",
            "arsitektur",
            "arsitektur lanskap",
            "teknik lingkungan",
            "teknik geomatika",
            "teknik perkeretaapian",
            "desain komunikasi visual",
            "rekayasa tata kelola air terpadu",
            "pariwisata",
            "teknik kelautan",
        ],
    }

    for key, programs in mapper.items():
        if prodi in programs:
            return key.upper()
    return None


def normalizeDate(text):
    text = removeNaN(text)
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

def cleanScope(text):
    handle = {
        "LAIN - LAIN": "LAIN-LAIN",
    }

    if text is None:
        return None

    text_clean = text.upper().strip()
    text_clean = handle.get(text_clean, text_clean)
    return text_clean


def cleanTanggal(text):
    text = removeNaN(text)
    result = {
        "tanggal": 0,
        "bulan": 0,
        "tahun": 0,
        "timestamp": None,
    }

    def build_result(day, month, year):
        parsed = result | {"tanggal": day, "bulan": month, "tahun": year}
        if day == 0 or month == 0 or year == 0:
            return parsed

        try:
            date_value = datetime(year, month, day)
        except (TypeError, ValueError):
            return result

        parsed["timestamp"] = date_value
        return parsed

    if text is None:
        return result

    text = str(text).strip().lower()

    match = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", text)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        return build_result(day, month, year)

    match = re.match(r"^(\d{1,2})\s+([a-z]+)\s+(\d{4})$", text)
    if match:
        day = int(match.group(1))
        month = MONTH_MAP.get(match.group(2), None)
        year = int(match.group(3))
        return build_result(day, month, year)

    match = re.match(r"^([a-z]+)\s+(\d{4})$", text)
    if match:
        month = MONTH_MAP.get(match.group(1), None)
        year = int(match.group(2))
        return build_result(0, month, year)

    match = re.match(r"^\d{4}$", text)
    if match:
        return build_result(0, 0, int(text))

    return result

def captureDOI(text):
    regex = r'10\.\d{4,9}/[^\s"<>]+'
    if text is not None:
        match = re.search(regex, text, re.IGNORECASE)
        if match:
            return f"https://doi.org/{match.group(0)}"
    return text

def standarizingJournal(text):
    text = removeNaN(text)
    if text is None:
        return {"jurnal": None, "groups": None}

    journal = str(text).strip().upper()

    if "INTERNASIONAL" in journal:
        groups = "INTERNASIONAL"
    elif "NASIONAL" in journal or "SINTA" in journal:
        groups = "NASIONAL"
    else:
        groups = "LAINNYA"

    return {"jurnal": journal, "groups": groups}

def standarizingNamaDosen(text):
    text = removeNaN(text)
# user define function spark
removeNaN_udf = F.udf(removeNaN, StringType())
match_unique_id_udf = F.udf(matchUniqueID, ArrayType(StringType()))
match_name_udf = F.udf(matchNames, ArrayType(StringType()))
get_prodi_udf = F.udf(getProdi, StringType())
get_faculty_udf = F.udf(getFaculty, StringType())
map_faculty_degree_udf = F.udf(mapFacultyDegree, StringType())
clean_scope_udf = F.udf(cleanScope,StringType())
capture_doi_udf = F.udf(captureDOI,StringType())
normalize_date_udf = F.udf(normalizeDate, normalizeDateSchema)
clean_tanggal_udf = F.udf(cleanTanggal, cleanTanggalSchema)
standarizing_journal_udf = F.udf(standarizingJournal, standarizingJournalSchema)
