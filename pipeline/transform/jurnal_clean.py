import html
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

cleanPublikasiSchema = StructType(
    [
        StructField("jurnal_nama", StringType(), True),
        StructField("jurnal_volume", StringType(), True),
        StructField("jurnal_issue", StringType(), True),
        StructField("jurnal_halaman", StringType(), True),
        StructField("jurnal_tahun", StringType(), True),
    ]
)


def cleanPublikasi(pub):
    if pub is None:
        return {
            "jurnal_nama": None,
            "jurnal_volume": None,
            "jurnal_issue": None,
            "jurnal_halaman": None,
            "jurnal_tahun": None,
        }

    pub = html.unescape(str(pub)).strip()
    pub = re.sub(r",\s*NULL-NULL|NULL-NULL\s*,", "", pub)

    res = {
        "jurnal_nama": pub,
        "jurnal_volume": None,
        "jurnal_issue": None,
        "jurnal_halaman": None,
        "jurnal_tahun": None,
    }

    # Pattern A: Name Volume (Issue), Year
    #   - Must come BEFORE the pages variant so ", 2025" is not eaten as "jurnal_halaman"
    m = re.match(r"^(.*?)\s+(\d+)\s*\(\s*([^)]+?)\s*\)\s*,\s*(\d{4})\s*$", pub)
    if m:
        res["jurnal_nama"] = m.group(1).strip().rstrip(",").strip().upper()
        res["jurnal_volume"] = m.group(2)
        res["jurnal_issue"] = m.group(3)
        res["jurnal_tahun"] = m.group(4)
        return res

    # Pattern B: Name Volume (Issue), Pages, Year  (pages mandatory)
    m = re.match(
        r"^(.*?)\s+(\d+)\s*\(\s*([^)]+?)\s*\)\s*,\s*([\d\-–\+]+)(?:,\s*(\d{4}))?\s*$",
        pub.rstrip("."),
    )
    if m:
        res["jurnal_nama"] = m.group(1).strip().rstrip(",").strip().upper()
        res["jurnal_volume"] = m.group(2)
        res["jurnal_issue"] = m.group(3)
        res["jurnal_halaman"] = html.unescape(m.group(4)).replace("\u2013", "-")
        if m.group(5):
            res["jurnal_tahun"] = m.group(5)
        return res

    year_m = re.search(r",\s*(\d{4})\s*$", pub)
    if year_m:
        res["jurnal_tahun"] = year_m.group(1)

    # Pattern C: Name Volume, Pages, Year
    m = re.match(r"^(.*?)\s+(\d+)\s*,\s*([\d\-\–\+]+)\s*,\s*\d{4}\s*$", pub)
    if m:
        res["jurnal_nama"] = m.group(1).strip().rstrip(",").strip().upper()
        res["jurnal_volume"] = m.group(2)
        res["jurnal_halaman"] = m.group(3)
        return res

    # Pattern 4: Name, Pages, Year
    m = re.match(r"^(.*?),[\s]*([\d\-\–\+]+),\s*\d{4}\s*$", pub)
    if m:
        res["jurnal_nama"] = m.group(1).strip().rstrip(",").strip().upper()
        res["jurnal_halaman"] = m.group(2)
        return res

    # Pattern 5: Name, Year
    m = re.match(r"^(.*?),[\s]*(\d{4})\s*$", pub)
    if m:
        res["jurnal_nama"] = m.group(1).strip().rstrip(",").strip().upper()
        return res

    res["jurnal_nama"] = pub.strip().rstrip(",").strip().upper()
    return res


clean_publikasi_udf = F.udf(cleanPublikasi, cleanPublikasiSchema)
