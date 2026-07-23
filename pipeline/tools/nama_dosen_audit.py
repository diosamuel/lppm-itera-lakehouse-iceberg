import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

DEGREES = [
    "S.T",
    "M.T",
    "M.Si",
    "S.Si",
    "M.Sc",
    "S.Pd",
    "M.Eng",
    "Ph.D",
    "S.T.P",
    "S.Farm",
    "S.Kom",
    "S.TP",
    "S.Hut",
    "M.Pd",
    "S.P",
    "M.Farm",
    "S.Kel",
    "M.Kom",
    "M.S.Farm",
    "M.Ars",
    "M.Cs",
    "S.P.W.K",
    "M.S",
    "M.P.W.K",
    "S.Pi",
    "M.Ds",
    "S.Ds",
    "M.Sn",
    "M.T.P",
    "S.Sn",
    "M.Or",
    "M.Mat",
    "M.Ars.L",
    "PhD",
    "S.Ars",
    "S.Mat",
    "DEA",
    "IPU",
    "IPM",
    "Apt",
    "CISA",
    "MT",
    "ST",
    "SST",
    "SP",
    "MM",
    "MS",
    "M.Ling",
    "M.Par",
    "M.R.K",
    "S.ST",
    "M.A",
    "S.S",
    "S.Pt",
    "M.EM",
    "M.P",
    "S.Far",
    "M.URP",
    "M.Arch",
    "M.Benv",
    "M.T.I",
    "S.Ci",
    "M.App.Sc",
    "S.Par",
    "S.Komp",
    "B.Eng",
    "C.Eng",
    "M.M.Par",
    "M.Agr",
    "S.Sos",
    "M.Aktr",
    "M.I.P",
    "M.Komp",
    "D.Eng",
    "B.Sc",
    "MSME",
    "MURP",
    "M.Hum",
    "M.Han",
    "S.Gz",
    "S.S.T",
    "M.TP",
    "M.Log",
    "S.Stat",
    "D.E.A",
    "ST.Par",
    "M.U.D",
    "M.PWK",
    "M.Eng.Sc",
    "M.Li",
    "M.I.A",
    "M.Stat",
    "MScTech",
    "M.Pharm.Sci",
    "M.Pharm.S",
    "S.Arsl",
    "M.Biotech",
    "M.Biomed",
    "M.Clin.Pharm",
    "M.Pharm",
    "S.S.I",
    "MS.Arch",
    "B.Sc\\(Hons\\)",
    "PHD",
    "STP",
    "M.BEnv",
    "S.Si.Kom",
]

PREFIXES = [
    "Prof\\.",
    "Dr\\.Eng\\.",
    "Dr\\.-Ing\\.",
    "Dr\\.",
    "Ir\\.",
    "apt\\.",
    "Drs\\.",
    "Dr,-Ing\\.",
    "Ir,",
]


def _escape_degree(d):
    esc = d if "\\(" in d else re.escape(d)
    return esc.replace(r"\.", r"\.\s*")


# Pre-compiled patterns
sortGelar = sorted(DEGREES, key=len, reverse=True)
polaGelar = "|".join(_escape_degree(d) for d in sortGelar)
gelarSingle = (
    r"(?:[,.]\s*|\s+)(?:" + polaGelar + r")"
    r"\.?(?:\([^()]*\))?\.?"
)
_SUFFIX_RE = re.compile(r"(?:" + gelarSingle + r")+$", re.IGNORECASE)
_PREFIX_RE = re.compile(
    r"^(?:(?:" + "|".join(PREFIXES) + r")[,\s]*)+",
    re.IGNORECASE,
)
_UNCLOSED_BRACKET = re.compile(r"\([^()]*$")


def _strip_suffix(name):
    prev = None
    while prev != name:
        prev = name
        name = _SUFFIX_RE.sub("", name).strip(" .,")
    return name


def clean_dosen_name(name):
    """Strip academic degrees and honorific prefixes from a lecturer name."""
    if name is None:
        return None
    name = str(name)
    name = _strip_suffix(name)
    name = _UNCLOSED_BRACKET.sub("", name)
    name = name.strip(" .,")
    name = _strip_suffix(name)
    name = _PREFIX_RE.sub("", name)
    name = name.strip(" .,")
    return name if name else None


clean_dosen_name_udf = F.udf(clean_dosen_name, StringType())


def clean_nama_dosen_column(df: DataFrame, column: str = "dosen", output: str = "dosen_clean") -> DataFrame:
    """Add a cleaned name column to a Spark DataFrame."""
    return df.withColumn(output, clean_dosen_name_udf(F.col(column)))


# ── Name standardisation for LEFT JOIN matching ────────────────

def preclean_dosen_name(name):
    """Remove garbage characters and detect invalid entries."""
    if name is None:
        return None, False
    name = str(name)
    name = name.replace("\xa0", " ").replace("\n", "").replace("\r", "").strip()

    # Reject author lists (contains ",...")
    if ",..." in name or ", ..." in name:
        return None, False

    # Reject non-person entries
    if "Yayasan" in name or "Penerbit" in name:
        return None, False

    return name, True


def standardize_nama_dosen(name):
    """Clean and uppercase name root for join key."""
    name, valid = preclean_dosen_name(name)
    if not valid:
        return None
    name = clean_dosen_name(name)
    if name is None:
        return None
    name = name.upper().strip()
    # Collapse multi-space
    name = re.sub(r"\s+", " ", name)
    return name if name else None


def is_valid_dosen_name(name):
    """Flag whether a dosen name is usable (not author-list, not publisher)."""
    _, valid = preclean_dosen_name(name)
    return valid


standardize_nama_dosen_udf = F.udf(standardize_nama_dosen, StringType())
is_valid_dosen_name_udf = F.udf(is_valid_dosen_name, StringType())
