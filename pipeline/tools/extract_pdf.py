import re
from pathlib import Path
from difflib import SequenceMatcher
from collections import Counter
import pdfplumber

SECTION_TITLES = [
    "Judul Penelitian",
    "Identitas Pengusul",
    "Jumlah Dana Usulan",
    "Rekam Jejak Ketua Pengusul",
    "Bidang Kepakaran",
    "Sustainable Development Goals (SDGs)",
    "Ringkasan",
    "Kata kunci",
    "PENDAHULUAN",
    "METODE",
    "HASIL YANG DIHARAPKAN",
    "JADWAL PENELITIAN",
    "BIAYA PENELITIAN",
    "DAFTAR PUSTAKA",
    "LAMPIRAN",
]

MATCH_THRESHOLD = 0.75
FONT_SIZE_DELTA = 0.5
BOLD_KEYWORDS = ("bold", "Bold", "BOLD")


class PDFExtractor:
    def __init__(self, pdf_path):
        self.pdf_path = Path(pdf_path)
        self.lines = []
        self.boundaries = []
        self.sections = {}

    def extract(self):
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        with pdfplumber.open(self.pdf_path) as pdf:
            self.lines = self._extract_lines(pdf)

        body_size = self._detect_body_font_size()
        self.boundaries = self._find_boundaries(body_size)
        self.sections = self._split_sections()
        return self.sections

    def _extract_lines(self, pdf):
        lines = []
        for page_num, page in enumerate(pdf.pages, start=1):
            chars = page.chars
            if not chars:
                continue
            line_map = {}
            for ch in chars:
                key = round(ch["top"] / 2) * 2
                line_map.setdefault(key, []).append(ch)
            for y_key in sorted(line_map):
                group = sorted(line_map[y_key], key=lambda c: c["x0"])
                text = "".join(c["text"] for c in group).strip()
                if not text:
                    continue
                max_size = max(c["size"] for c in group)
                bold = any(self._is_bold(c.get("fontname", "")) for c in group)
                lines.append({
                    "page": page_num,
                    "text": text,
                    "max_size": max_size,
                    "is_bold": bold,
                    "y_top": y_key,
                })
        return lines

    def _detect_body_font_size(self):
        sizes = [round(ln["max_size"], 1) for ln in self.lines]
        return Counter(sizes).most_common(1)[0][0]

    def _find_boundaries(self, body_size):
        boundaries = []
        already_matched = set()

        for idx, ln in enumerate(self.lines):
            text = ln["text"]
            max_size = ln["max_size"]
            is_bold = ln["is_bold"]
            size_ok = (max_size >= body_size + FONT_SIZE_DELTA)
            threshold = MATCH_THRESHOLD if (size_ok or is_bold) else MATCH_THRESHOLD + 0.15

            matched_title, score = self._best_match(text)
            if matched_title is None or score < threshold:
                continue
            if matched_title in already_matched:
                continue

            already_matched.add(matched_title)
            boundaries.append({
                "title": matched_title,
                "found_text": text,
                "score": round(score, 3),
                "page": ln["page"],
                "line_index": idx,
            })

        boundaries.sort(key=lambda b: b["line_index"])
        return boundaries

    def _split_sections(self):
        sections = {}
        sentinel_idx = len(self.lines)
        spans = []
        for i, bnd in enumerate(self.boundaries):
            start = bnd["line_index"]
            end = self.boundaries[i + 1]["line_index"] if i + 1 < len(self.boundaries) else sentinel_idx
            spans.append((bnd["title"], start, end))

        for title, start, end in spans:
            text_parts = []
            prev_page = None
            for ln in self.lines[start:end]:
                if prev_page is not None and ln["page"] != prev_page:
                    text_parts.append("")
                text_parts.append(ln["text"])
                prev_page = ln["page"]
            sections[title] = "\n".join(text_parts).strip()
        return sections

    def _best_match(self, line):
        best_title, best_score = None, 0.0
        for title in SECTION_TITLES:
            score = SequenceMatcher(None, line.lower().strip(), title.lower().strip()).ratio()
            if score > best_score:
                best_title, best_score = title, score
        if best_score >= MATCH_THRESHOLD:
            return best_title, best_score
        return None, best_score

    @staticmethod
    def _is_bold(fontname):
        return any(kw in (fontname or "") for kw in BOLD_KEYWORDS)
