import csv
import difflib
import os
import re
import shutil
import tempfile

from dotenv import load_dotenv
from setup_minio import SetupMinioS3

load_dotenv()

BASE_DIR = "/home/iceberg/notebooks/data"
PDF_TEMPLATE_DIR = os.path.join(BASE_DIR, "pdf")
CSV_PATH = os.path.join(BASE_DIR, "diterimadanpdflengkap.csv")

PENELITIAN_FALLBACK_SKEMA = "Kepakaran"
PENGABDIAN_FALLBACK_SKEMA = "PWB"

FUZZY_THRESHOLD = 0.35


# Template index builder


def extractSkemaName(fname: str, jenis: str, subdir: str) -> str | None:
    stem = os.path.splitext(fname)[0]

    if jenis == "penelitian":
        if subdir == "Proposal":
            stem = re.sub(r"^\d+\.Itera_\d{4}_Format Proposal Penelitian\s*", "", stem)
            stem = re.sub(r"^berbasis\s+", "", stem)
        else:
            stem = re.sub(r"^\d+\.\s*", "", stem)
            stem = stem.split(" - ")[0] if " - " in stem else stem
        return stem.strip() or None

    if jenis == "pengabdian":
        prefixes = []
        if subdir == "Format Proposal":
            prefixes = [
                "Template Proposal PkM ITERA 2026_Skema ",
                "Template Proposal PkM ITERA ",
                "Template Proposal PkM ",
            ]
        else:
            prefixes = [
                "Template Laporan Akhir PkM ITERA 2026_Skema ",
                "Template Laporan Akhir PkM ITERA 2026 ",
                "Template Laporan Akhir PkM ",
            ]
        for p in prefixes:
            if stem.startswith(p):
                stem = stem.removeprefix(p)
                break
        return stem.strip() or None

    return None


def build_template_index(base_dir: str) -> dict:
    """
    Returns:
      {
        "penelitian": {
          "<skema_normal>": {"proposal": "<path>", "laporankemajuan": "<path>", "laporanakhir": "<path>"},
          ...
        },
        "pengabdian": { ... }
      }
    """
    index = {}

    doc_map = {
        ("penelitian", "Proposal"): "proposal",
        ("penelitian", "Laporan Kemajuan"): "laporankemajuan",
        ("penelitian", "Laporan Akhir"): "laporanakhir",
        ("pengabdian", "Format Proposal"): "proposal",
        ("pengabdian", "Format Laporan Akhir"): "laporanakhir",
    }

    for jenis in ("penelitian", "pengabdian"):
        jenis_dir = os.path.join(base_dir, jenis)
        if not os.path.isdir(jenis_dir):
            continue

        index[jenis] = {}

        for subdir in os.listdir(jenis_dir):
            subdir_path = os.path.join(jenis_dir, subdir)
            if not os.path.isdir(subdir_path):
                continue

            doc_type = doc_map.get((jenis, subdir))
            if not doc_type:
                continue

            for fname in os.listdir(subdir_path):
                if not fname.lower().endswith(".pdf"):
                    continue

                skema = extractSkemaName(fname, jenis, subdir)
                if not skema:
                    continue

                index[jenis].setdefault(skema, {})[doc_type] = os.path.join(subdir_path, fname)

    return index


# Fuzzy matching


def normalize_for_match(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def fuzzy_match_skema(csv_skema: str, available: list[str]) -> str | None:
    """
    Return the best-matching template skema name, or None if below threshold.
    Also checks substring containment as a strong signal.
    """
    if not csv_skema or not csv_skema.strip():
        return None

    target = normalize_for_match(csv_skema)
    if not target:
        return None

    best = None
    best_score = 0.0

    for candidate in available:
        norm_candidate = normalize_for_match(candidate)

        # Substring match is very strong
        if target in norm_candidate or norm_candidate in target:
            ratio = difflib.SequenceMatcher(None, target, norm_candidate).ratio()
            # Boost substring matches
            score = max(ratio, 0.7)
        else:
            score = difflib.SequenceMatcher(None, target, norm_candidate).ratio()

        if score > best_score:
            best_score = score
            best = candidate

    if best_score >= FUZZY_THRESHOLD:
        return best
    return None


# Generate aliases for template skema names


SKEMA_ALIASES = [
    ("Penguatan Kelompok Keilmuan", "Penguatan KK"),
    ("Penguatan Kelompok Keilmuan", "Penguatan KK"),
    ("Pemberdayaan Wilayah Berdampak", "PWB"),
    ("Pemberdayaan Masyarakan Lingkup Kepakaran", "Pemberdayaan Masyarakat Lingkup Kepakaran"),
]


def resolve_skema(csv_skema: str, index: dict, jenis: str, fallback: str) -> str:
    """
    Resolve a CSV skema value to a template skema name.
    Uses: direct lookup → alias lookup → fuzzy match → fallback
    """
    available = list(index.get(jenis, {}).keys())
    if not available:
        return fallback

    norm_csv = normalize_for_match(csv_skema) if csv_skema else ""

    # 1. Direct match after normalization
    for a in available:
        if normalize_for_match(a) == norm_csv:
            return a

    # 2. Alias resolution
    for canonical, alias in SKEMA_ALIASES:
        if normalize_for_match(alias) == norm_csv:
            if canonical in available:
                return canonical
            # Try to find canonical in available via normalize
            for a in available:
                if normalize_for_match(a) == normalize_for_match(canonical):
                    return a

    # 3. Fuzzy match
    fuzzy = fuzzy_match_skema(csv_skema, available)
    if fuzzy:
        return fuzzy

    # 4. Fallback
    return fallback


# Filename helpers


def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def to_camel_case(text: str) -> str:
    words = text.split()
    if not words:
        return ""
    return words[0] + "".join(w.capitalize() for w in words[1:])


def judul_prefix(title: str) -> str:
    parts = normalize(title).split()
    return to_camel_case(" ".join(parts[:2])) if parts else "null"


def ketua_prefix(name: str) -> str:
    raw = name.split(",")[0]
    parts = normalize(raw).split()
    return to_camel_case(" ".join(parts[:2])) if parts else "null"


def skema_suffix(skema: str) -> str:
    if not skema or not skema.strip():
        return "null"
    return to_camel_case(normalize(skema))


# Document type helpers


def doc_types(jenis: str) -> list[str]:
    if jenis == "penelitian":
        return ["proposal", "laporankemajuan", "laporanakhir"]
    if jenis == "pengabdian":
        return ["proposal", "laporanakhir"]
    raise ValueError(f"Unknown jenis: {jenis}")


# Main


def main():
    if not os.path.isfile(CSV_PATH):
        print(f"[ERROR] CSV not found at {CSV_PATH}")
        return
    if not os.path.isdir(PDF_TEMPLATE_DIR):
        print(f"[ERROR] PDF template directory not found at {PDF_TEMPLATE_DIR}")
        return

    print("Building template index ...")
    template_index = build_template_index(PDF_TEMPLATE_DIR)
    for jenis, skemas in template_index.items():
        print(f"  {jenis}: {len(skemas)} skema templates")
        for skema, docs in sorted(skemas.items()):
            have = ", ".join(sorted(docs))
            print(f"    • {skema:40s}  [{have}]")

    print()

    storage = SetupMinioS3(
        endpoint_url="http://minio:9000",
        bucket="sipaper",
    ).initialize()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Processing {len(rows)} rows from CSV ...\n")

    ok = 0
    err = 0

    for i, row in enumerate(rows, start=1):
        judul = (row.get("judul_proposal") or "").strip()
        ketua = (row.get("ketua_peneliti") or "").strip()
        tahun = (row.get("tahun") or "").strip()
        jenis = (row.get("jenis") or "").strip().lower()
        skema_asli = (row.get("skema") or "").strip()

        if not judul or not jenis:
            print(f"[{i:04d}] SKIP — missing judul or jenis")
            continue
        if jenis not in template_index:
            print(f"[{i:04d}] SKIP — unknown jenis '{jenis}'")
            continue

        fallback = PENELITIAN_FALLBACK_SKEMA if jenis == "penelitian" else PENGABDIAN_FALLBACK_SKEMA
        matched_skema = resolve_skema(skema_asli, template_index, jenis, fallback)

        template_docs = template_index[jenis].get(matched_skema, {})
        template_label = matched_skema
        if matched_skema != skema_asli or not skema_asli:
            pass  # fallback or fuzzy matched

        name_prefix = f"{judul_prefix(judul)}_{ketua_prefix(ketua)}"
        skema_part = skema_suffix(skema_asli)

        for doc in doc_types(jenis):
            src_path = template_docs.get(doc)
            if not src_path:
                print(f"[{i:04d}] WARN — no {doc} template for skema '{matched_skema}', skipping")
                continue

            filename = f"{name_prefix}_{doc}_{skema_part}_{tahun}.pdf"
            minio_key = f"{jenis}/{tahun}/pdf/{doc}/{filename}"

            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                shutil.copy2(src_path, tmp.name)
                tmp_path = tmp.name

            result = storage.upload(filename=minio_key, filepath=tmp_path)
            os.unlink(tmp_path)

            status = "success" if result.get("status") == "success" else "error"
            if status == "success":
                ok += 1
            else:
                err += 1

            print(
                f"[{i:04d}] {status.upper():7s} | "
                f"judul={judul[:45]:45s} | "
                f"ketua={ketua[:22]:22s} | "
                f"skema={skema_asli[:28]:28s} | "
                f"template={template_label:40s} | "
                f"minio=s3://sipaper/{minio_key}"
            )

    print(f"\nDone. {ok} success, {err} error(s).")


if __name__ == "__main__":
    main()
