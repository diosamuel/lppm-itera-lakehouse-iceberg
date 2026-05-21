import re
import numpy as np


def matchUniqueID(text):
    """Extract Nomor Induk Pegawai (NIP) or Nomor Induk Mahasiswa (NIM) from parenthesized text.

    Finds all substrings enclosed in parentheses and returns them as a list.
    Empty matches are replaced with None.

    Args:
        text: A string potentially containing NIP values in parentheses,
              e.g. "Dr. Budi (198501012010011001)".

    Returns:
        A list of extracted NIP strings (or None for empty matches),
        or None if the input is not a string.
    """
    if isinstance(text, str):
        result = re.findall(r'\((.*?)\)', text)
        res = [x if str(x).strip() else None for x in result]
        return res
    else:
        return None


def getProdi(text):
    """Extract the program studi (study program) name from a raw label.

    Strips the "Program Studi" prefix and, if the text contains a hyphen,
    returns the part after the first hyphen (typically the prodi name).

    Args:
        text: A string like "Program Studi FTI - Teknik Informatika",
              or None.

    Returns:
        The cleaned prodi name in lowercase, or None if input is None.
    """
    if text is None:
        return None

    text = text.lower().strip()
    text = text.replace("program studi", "").strip()
    parts = text.split("-")
    if len(parts) > 1:
        return parts[1].strip()

    return text


def getFaculty(text):
    """Map a faculty abbreviation to its full name.

    Expects the abbreviation to appear before the first hyphen in the input.

    Args:
        text: A string like "FTI - Teknik Informatika".

    Returns:
        The full faculty name, e.g. "Fakultas Teknologi Industri".

    Raises:
        KeyError: If the abbreviation is not one of FS, FTI, or FTIK.
    """
    mapper: dict = {
        "FS": "Fakultas Sains",
        "FTI": "Fakultas Teknologi Industri",
        "FTIK": "Fakultas Teknologi Industri dan Kewilayahan"
    }
    return mapper[text.split("-")[0].strip()]


def mapFacultyDegree(prodi):
    """Map a study program name to its parent faculty abbreviation.

    Looks up the given prodi (lowercase) in a predefined mapping of
    faculty → list of study programs.

    Args:
        prodi: The study program name in lowercase,
               e.g. "teknik informatika".

    Returns:
        The faculty abbreviation ("FS", "FTI", or "FTIK"),
        or None if the prodi is not found in any faculty.
    """
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
            "matematika"
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
            "teknik kelautan",
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
        ]
    }

    for key in mapper.keys():
        if prodi in mapper[key]:
            return key

    return None


def normalize_date(text):
    """Parse various date string formats into a (day, month, year) tuple.

    Designed specifically for the 'Tanggal Terbit' (publication date) column
    in the sitasi (citation) dataset. Handles the following formats:

        1. ISO-like:    "2023-05-14"  → (14, 5, 2023)
        2. DD Month YYYY: "14 mei 2023" → (14, 5, 2023)
        3. Month YYYY:    "mei 2023"    → (None, 5, 2023)
        4. Year only:     "2023"        → (None, None, 2023)

    Args:
        text: A date string in one of the supported formats, or None.

    Returns:
        A tuple of (day, month, year) where any component may be None
        if it cannot be determined from the input.
    """
    if text is None:
        return (None, None, None)

    text = str(text).strip().lower()

    # 1. Full datetime (YYYY-MM-DD)
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


normalizeDate_udf = F.udf(normalize_date, schema)


def mappingDate(df, column):
    """Apply date normalization to a Spark DataFrame column.

    Replaces Indonesian month names with English equivalents, then parses
    the date string into separate day, month, and year columns using
    normalize_date.

    Args:
        df: A PySpark DataFrame.
        column: The name of the column containing date strings
                (e.g. "Tanggal Terbit").

    Returns:
        The DataFrame with three new columns added: "day", "month", "year".
    """
    for indo, eng in bulan_map.items():
        df = df.withColumn(
            column,
            F.regexp_replace(
                F.col(column),
                indo,
                eng
            )
        )

    df = df.withColumn(
        "parsed",
        normalize_udf(F.col(column))
    )

    df = (
        df.withColumn("day", F.col("parsed.day"))
        .withColumn("month", F.col("parsed.month"))
        .withColumn("year", F.col("parsed.year"))
        .drop("parsed")
    )

    return df
