from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
import re

import pdfplumber

# ── Section Templates ──────────────────────────────────────────────────────────
_BASE_HEADER = ["Judul Penelitian", "Identitas Pengusul"]
_REKAM_JEJAK = ["Rekam Jejak Ketua Pengusul"]
_SDG_RINGKASAN = ["Sustainable Development Goals (SDGs)", "Ringkasan", "Kata kunci"]

_PROPOSAL_END = ["PENDAHULUAN", "METODE", "HASIL YANG DIHARAPKAN", "JADWAL PENELITIAN", "DAFTAR PUSTAKA"]
_LAPORAN_END = ["HASIL PELAKSANAAN PENELITIAN", "STATUS LUARAN", "KENDALA PELAKSANAAN PENELITIAN", "RENCANA TAHAPAN SELANJUTNYA", "DAFTAR PUSTAKA"]

# ── Document Type Definitions ──────────────────────────────────────────────────
DOCUMENT_TYPES = {
    "proposal_mandiri": _BASE_HEADER + _REKAM_JEJAK + ["Bidang Kepakaran"] + _SDG_RINGKASAN + _PROPOSAL_END,
    "proposal_umum": _BASE_HEADER + ["Jumlah Dana Usulan"] + _REKAM_JEJAK + ["Bidang Kepakaran"] + _SDG_RINGKASAN + ["BIAYA PENELITIAN"] + _PROPOSAL_END,
    "proposal_penugasan": _BASE_HEADER + ["Jumlah Dana Usulan"] + _REKAM_JEJAK + ["Bidang Penugasan"] + _SDG_RINGKASAN + ["BIAYA PENELITIAN"] + _PROPOSAL_END,
    "proposal_prioritas": _BASE_HEADER + ["Jumlah Dana Usulan"] + _REKAM_JEJAK + ["Bidang Prioritas"] + _SDG_RINGKASAN + ["BIAYA PENELITIAN"] + _PROPOSAL_END,
    "laporan_kemajuan": _BASE_HEADER + ["Bidang Penugasan"] + _SDG_RINGKASAN + _LAPORAN_END,
    "laporan_akhir": _BASE_HEADER + ["Bidang Penugasan"] + _SDG_RINGKASAN + _LAPORAN_END,
}

# ── Subtitle Templates (for stripping instructional text) ──────────────────────
SUBTITLE_MAP = {
    "Bidang Kepakaran": "Tuliskan poin Bidang Kepakaran yang terkait dengan penelitian ini. Berikut ini adalah pilihan Bidang Kepakaran",
    "Sustainable Development Goals (SDGs)": "Tuliskan poin Sustainable Development Goals yang terkait dengan penelitian ini. Berikut ini adalah pilihan Sustainable Development Goals",
    "Ringkasan": "Ringkasan penelitian (maksimum 300 kata) berisi Urgensi, tujuan, metode, dan luaran yang ditargetkan Ringkasan dan kata kunci tidak lebih dari halaman pertama.",
    "Kata kunci": "Isian 5 kata kunci yang dipisahkan dengan tanda titik koma (;)",
    "PENDAHULUAN": "Pendahuluan penelitian tidak lebih dari 1000 kata yang memuat, latar belakang, rumusan permasalahan yang akan diteliti, pendekatan pemecahan masalah, state-of-the-art dan kebaruan, peta jalan (road map) penelitian setidaknya 5 tahun. Sitasi disusun dan ditulis berdasarkan sistem nomor sesuai dengan urutan pengutipan.",
    "METODE": "Metode penelitian ditulis dengan mengikuti gaya penulisan ilmiah (maksimum 1000 kata). Untuk dapat menggambarkan proses penelitian, bagian ini dapat dilengkapi dengan diagram alir penelitian yang mencakup apa yang sudah dilakukan dan apa yang akan dilakukan untuk mencapai tujuan penelitian. Diagram alir dibuat dalam bentuk JPG/PNG. Metode penelitian harus memuat sekurang-kurangnya prosedur penelitian, hasil yang diharapkan, indikator capaian yang ditargetkan, serta anggota tim/mitra yang bertanggung jawab pada setiap tahapan penelitian. Metode penelitian harus sejalan dengan Rencana Anggaran Biaya (RAB).",
    "HASIL YANG DIHARAPKAN": "Hasil yang diharapkan/luaran yang dijanjikan",
    "JADWAL PENELITIAN": "Jadwal penelitian disusun berdasarkan pelaksanaan penelitian, harap disesuaikan dengan lama waktu pelaksanaan penelitian yang diusulkan.",
    "BIAYA PENELITIAN": "Biaya penelitian disusun berdasarkan kebutuhan yang rasional dan meyesuaikan dengan luaran penelitian yang dijanjikan dan skema yang dipilih",
    "DAFTAR PUSTAKA": "Gunakan format referensi IEEE. Sitasi disusun dan ditulis berdasarkan sistem nomor sesuai dengan urutan pengutipan. Hanya pustaka yang disitasi pada usulan penelitian yang dicantumkan dalam Daftar Pustaka.",
}

# ── Detection Thresholds ───────────────────────────────────────────────────────
MATCH_THRESHOLD = 0.75
FONT_SIZE_DELTA = 0.5
BOLD_KEYWORDS = ("bold", "Bold", "BOLD")


class PDFExtractor:
    """
    Extract text from a PDF by detecting section headings.

    Usage:
        # Auto-detect document type
        extractor = PDFExtractor("proposal.pdf")
        sections = extractor.extract()

        # Or specify manually
        extractor = PDFExtractor("proposal.pdf", doc_type="proposal_mandiri")
        sections = extractor.extract()
    """

    def __init__(self, pdf_path, doc_type=None):
        if doc_type is not None and doc_type not in DOCUMENT_TYPES:
            raise ValueError(f"Unknown doc_type: '{doc_type}'. Must be one of {list(DOCUMENT_TYPES)}")
        self.pdf_path = Path(pdf_path)
        self.doc_type = doc_type
        self.lines = []
        self.boundaries = []
        self.sections = {}

    def extract(self):
        """Main entry point. Returns {section_title: section_text}."""
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        with pdfplumber.open(self.pdf_path) as pdf:
            self.lines = self._extract_lines(pdf)

        # Auto-detect doc_type if not specified
        if self.doc_type is None:
            self.doc_type = self._detect_doc_type()

        body_font = self._detect_body_font()
        self.boundaries = self._find_boundaries(body_font)
        self.sections = self._split_sections()
        return self.sections

    # ── Private helpers ────────────────────────────────────────────────────────

    def _extract_lines(self, pdf):
        """Extract every text line from the PDF, grouped by vertical position."""
        lines = []
        for page_num, page in enumerate(pdf.pages, 1):
            if not page.chars:
                continue

            y_groups = {}
            for ch in page.chars:
                y_key = round(ch["top"] / 2) * 2
                y_groups.setdefault(y_key, []).append(ch)

            for y_pos in sorted(y_groups):
                chars = sorted(y_groups[y_pos], key=lambda c: c["x0"])
                text = "".join(c["text"] for c in chars).strip()
                if not text:
                    continue

                lines.append({
                    "page": page_num,
                    "text": text,
                    "font_size": max(c["size"] for c in chars),
                    "is_bold": any(self._is_bold(c.get("fontname", "")) for c in chars),
                    "y_position": y_pos,
                })
        return lines

    def _detect_doc_type(self):
        """Auto-detect document type by counting matching section headings."""
        all_text = " ".join(line["text"].lower() for line in self.lines)
        scores = {}

        for doc_type, sections in DOCUMENT_TYPES.items():
            # Count how many sections appear in the PDF
            matches = sum(1 for s in sections if s.lower() in all_text)
            # Normalize by total sections to get ratio
            scores[doc_type] = matches / len(sections)

        best_type = max(scores, key=scores.get)
        best_score = scores[best_type]

        # Require at least 40% of sections to match for a confident detection
        if best_score < 0.4:
            raise ValueError(
                f"Cannot auto-detect document type. "
                f"Best match: '{best_type}' with {best_score:.0%} confidence. "
                f"Please specify doc_type manually."
            )

        return best_type

    def _detect_body_font(self):
        """Find the most common font size (assumed to be body text)."""
        sizes = [round(l["font_size"], 1) for l in self.lines]
        return Counter(sizes).most_common(1)[0][0]

    def _find_boundaries(self, body_font):
        """Scan lines to find section heading boundaries."""
        boundaries = []
        seen = set()
        section_list = DOCUMENT_TYPES[self.doc_type]

        for idx, line in enumerate(self.lines):
            is_heading_style = (
                line["font_size"] >= body_font + FONT_SIZE_DELTA or line["is_bold"]
            )
            thresh = MATCH_THRESHOLD if is_heading_style else MATCH_THRESHOLD + 0.15

            title, score = self._best_match(line["text"], section_list)
            if title is None or score < thresh or title in seen:
                continue

            seen.add(title)
            boundaries.append({
                "title": title,
                "found_text": line["text"],
                "score": round(score, 3),
                "page": line["page"],
                "line_index": idx,
            })

        boundaries.sort(key=lambda b: b["line_index"])
        return boundaries

    def _split_sections(self):
        """Split full text into sections based on detected headings."""
        sections = {}
        total = len(self.lines)

        for i, b in enumerate(self.boundaries):
            start = b["line_index"]
            end = self.boundaries[i + 1]["line_index"] if i + 1 < len(self.boundaries) else total

            parts, prev_page = [], None
            for line in self.lines[start + 1 : end]:
                if prev_page is not None and line["page"] != prev_page:
                    parts.append("")
                parts.append(line["text"])
                prev_page = line["page"]

            text = "\n".join(parts).strip().replace("\n", " ")
            sections[b["title"]] = self._remove_subtitle(b["title"], text)

        return sections

    def _best_match(self, line_text, section_list):
        """Return (title, score) of best matching section heading, or (None, score)."""
        line_lower = line_text.lower().strip()
        best_title, best_score = None, 0.0

        for title in section_list:
            score = SequenceMatcher(None, line_lower, title.lower().strip()).ratio()
            if score > best_score:
                best_title, best_score = title, score

        return (best_title, best_score) if best_score >= MATCH_THRESHOLD else (None, best_score)

    @staticmethod
    def _is_bold(font_name):
        return bool(font_name) and any(k in font_name for k in BOLD_KEYWORDS)

    def _remove_subtitle(self, title, text):
        """Strip known subtitle/instructional text from section content."""
        if title == "Kata kunci":
            text = re.sub(
                r"Substansi\s+Usulan\s+Petunjuk:\s+Pengusul\s+hanya\s+diperkenankan\s+"
                r"mengisi\s+di\s+tempat\s+yang\s+telah\s+disediakan\s+\(diberi\s+warna\s+[\.\s…\-]+\)\s+"
                r"sesuai\s+dengan\s+petunjuk\s+pengisian\s+dan\s+tidak\s+diperkenankan\s+untuk\s+"
                r"memodifikasi\s+template\s+menghapus\s+setiap\s+bagian",
                "", text, flags=re.IGNORECASE,
            ).strip()

        subtitle = SUBTITLE_MAP.get(title)
        if not subtitle:
            return text

        pattern = re.escape(subtitle)
        for old, new in [(r"\ ", r"\s+"), (r"\-", r"\s*-\s*"), (r"\(", r"\(\s*"),
                          (r"\)", r"\s*\)"), (r"\/", r"\s*/\s*")]:
            pattern = pattern.replace(old, new)

        match = re.match(pattern, text, flags=re.IGNORECASE)
        if match:
            return text[match.end():].strip()

        clean_sub = " ".join(subtitle.split())
        clean_txt = " ".join(text.split())
        if clean_txt.lower().startswith(clean_sub.lower()):
            return text[len(subtitle):].strip()

        return text
