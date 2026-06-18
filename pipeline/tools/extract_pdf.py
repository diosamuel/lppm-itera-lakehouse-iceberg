import re
from pathlib import Path
from difflib import SequenceMatcher
from collections import Counter
import pdfplumber

SECTION_PROPOSAL_MANDIRI = [
    "Judul Penelitian",
    "Identitas Pengusul",
    "Rekam Jejak Ketua Pengusul",
    "Bidang Kepakaran",
    "Sustainable Development Goals (SDGs)",
    "Ringkasan",
    "Kata kunci",
    "PENDAHULUAN",
    "METODE",
    "HASIL YANG DIHARAPKAN",
    "JADWAL PENELITIAN",
    "DAFTAR PUSTAKA",
]

SECTION_PROPOSAL_UMUM = [
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
]

SECTION_PROPOSAL_PENUGASAN = [
    "Judul Penelitian",
    "Identitas Pengusul",
    "Jumlah Dana Usulan",
    "Rekam Jejak Ketua Pengusul",
    "Bidang Penugasan",
    "Sustainable Development Goals (SDGs)",
    "Ringkasan",
    "Kata kunci",
    "PENDAHULUAN",
    "METODE",
    "HASIL YANG DIHARAPKAN",
    "JADWAL PENELITIAN",
    "BIAYA PENELITIAN",
    "DAFTAR PUSTAKA",
]

SECTION_PROPOSAL_PRIORITAS = [
    "Judul Penelitian",
    "Identitas Pengusul",
    "Jumlah Dana Usulan",
    "Rekam Jejak Ketua Pengusul",
    "Bidang Prioritas",
    "Sustainable Development Goals (SDGs)",
    "Ringkasan",
    "Kata kunci",
    "PENDAHULUAN",
    "METODE",
    "HASIL YANG DIHARAPKAN",
    "JADWAL PENELITIAN",
    "BIAYA PENELITIAN",
    "DAFTAR PUSTAKA",
]

SECTION_LAPORAN_KEMAJUAN = [
    "Judul Penelitian",
    "Identitas Pengusul",
    "Bidang Penugasan",
    "Sustainable Development Goals (SDGs)",
    "Ringkasan",
    "Kata kunci",
    "HASIL PELAKSANAAN PENELITIAN",
    "STATUS LUARAN",
    "KENDALA PELAKSANAAN PENELITIAN",
    "RENCANA TAHAPAN SELANJUTNYA",
    "DAFTAR PUSTAKA",
]

SECTION_LAPORAN_AKHIR = [
    "Judul Penelitian",
    "Identitas Pengusul",
    "Bidang Penugasan",
    "Sustainable Development Goals (SDGs)",
    "Ringkasan",
    "Kata kunci",
    "HASIL PELAKSANAAN PENELITIAN",
    "STATUS LUARAN",
    "KENDALA PELAKSANAAN PENELITIAN",
    "RENCANA TAHAPAN SELANJUTNYA",
    "DAFTAR PUSTAKA",
]

# Map doc_type string to its section list
DOCUMENT_TYPES = {
    "proposal_mandiri": SECTION_PROPOSAL_MANDIRI,
    "proposal_umum": SECTION_PROPOSAL_UMUM,
    "proposal_penugasan": SECTION_PROPOSAL_PENUGASAN,
    "proposal_prioritas": SECTION_PROPOSAL_PRIORITAS,
    "laporan_kemajuan": SECTION_LAPORAN_KEMAJUAN,
    "laporan_akhir": SECTION_LAPORAN_AKHIR,
}

SUBTITLE_MAP = {
    "Bidang Kepakaran": "Tuliskan poin Bidang Kepakaran yang terkait dengan penelitian ini. Berikut ini adalah pilihan Bidang Kepakaran",
    "Ringkasan": "Ringkasan penelitian (maksimum 300 kata) berisi Urgensi, tujuan, metode, dan luaran yang ditargetkan Ringkasan dan kata kunci tidak lebih dari halaman pertama.",
    "Kata kunci": "Isian 5 kata kunci yang dipisahkan dengan tanda titik koma (;)",
    "PENDAHULUAN": "Pendahuluan penelitian tidak lebih dari 1000 kata yang memuat, latar belakang, rumusan permasalahan yang akan diteliti, pendekatan pemecahan masalah, state-of-the-art dan kebaruan, peta jalan (road map) penelitian setidaknya 5 tahun. Sitasi disusun dan ditulis berdasarkan sistem nomor sesuai dengan urutan pengutipan.",
    "METODE": "Metode penelitian ditulis dengan mengikuti gaya penulisan ilmiah (maksimum 1000 kata). Untuk dapat menggambarkan proses penelitian, bagian ini dapat dilengkapi dengan diagram alir penelitian yang mencakup apa yang sudah dilakukan dan apa yang akan dilakukan untuk mencapai tujuan penelitian. Diagram alir dibuat dalam bentuk JPG/PNG. Metode penelitian harus memuat sekurang-kurangnya prosedur penelitian, hasil yang diharapkan, indikator capaian yang ditargetkan, serta anggota tim/mitra yang bertanggung jawab pada setiap tahapan penelitian. Metode penelitian harus sejalan dengan Rencana Anggaran Biaya (RAB).",
    "HASIL YANG DIHARAPKAN": "Hasil yang diharapkan/luaran yang dijanjikan",
    "JADWAL PENELITIAN": "Jadwal penelitian disusun berdasarkan pelaksanaan penelitian, harap disesuaikan dengan lama waktu pelaksanaan penelitian yang diusulkan.",
    "BIAYA PENELITIAN": "Biaya penelitian disusun berdasarkan kebutuhan yang rasional dan meyesuaikan dengan luaran penelitian yang dijanjikan dan skema yang dipilih",
    "DAFTAR PUSTAKA": "Gunakan format referensi IEEE. Sitasi disusun dan ditulis berdasarkan sistem nomor sesuai dengan urutan pengutipan. Hanya pustaka yang disitasi pada usulan penelitian yang dicantumkan dalam Daftar Pustaka.",
}

# Minimum similarity ratio (0-1) for a line to match a section title
MATCH_THRESHOLD = 0.75

# How much bigger than body font a heading must be (in points)
FONT_SIZE_DELTA = 0.5

# Font name substrings that indicate bold text
BOLD_KEYWORDS = ("bold", "Bold", "BOLD")


class PDFExtractor:
    """
    Extract text from a PDF by detecting section headings.

    Usage:
        extractor = PDFExtractor("proposal.pdf", doc_type="proposal_mandiri")
        sections = extractor.extract()
        # sections = {"Judul Penelitian": "...", "PENDAHULUAN": "...", ...}
    """

    def __init__(self, pdf_path, doc_type):
        """
        Args:
            pdf_path: Path to the PDF file.
            doc_type: Document type key. Must be one of:
                      'proposal_mandiri', 'proposal_umum',
                      'laporan_kemajuan', 'laporan_akhir'.
        """
        if doc_type not in DOCUMENT_TYPES:
            valid_types = list(DOCUMENT_TYPES.keys())
            raise ValueError(
                f"Unknown doc_type: '{doc_type}'. Must be one of {valid_types}"
            )

        self.pdf_path = Path(pdf_path)
        self.doc_type = doc_type
        self.lines = []
        self.boundaries = []
        self.sections = {}

    def extract(self):
        """
        Main entry point. Reads the PDF and returns a dict of
        {section_title: section_text}.

        Returns:
            dict: Mapping of section heading to its full text content.
        """
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        # Step 1: Read all text lines from PDF
        with pdfplumber.open(self.pdf_path) as pdf:
            self.lines = self.extractLines(pdf)

        # Step 2: Find where each section heading appears
        section_list = DOCUMENT_TYPES[self.doc_type]
        body_font_size = self.detectBodyFontSize()
        self.boundaries = self.findSectionBoundaries(body_font_size, section_list)

        # Step 3: Split text into sections based on headings
        self.sections = self.splitIntoSection()
        return self.sections

    def extractLines(self, pdf):
        """
        Extract every text line from the PDF, grouping characters
        that sit on the same vertical position into one line.

        Returns:
            list[dict]: Each dict has keys: page, text, font_size, is_bold, y_position.
        """
        lines = []

        for page_number, page in enumerate(pdf.pages, start=1):
            characters = page.chars
            if not characters:
                continue

            # Group characters by their vertical position (y coordinate)
            # Characters on the same line share roughly the same y value
            y_position_to_characters = {}
            for char in characters:
                # Round y to nearest 2 pixels to group nearby characters
                y_key = round(char["top"] / 2) * 2
                if y_key not in y_position_to_characters:
                    y_position_to_characters[y_key] = []
                y_position_to_characters[y_key].append(char)

            # Process each group of characters (each visual line)
            for y_position in sorted(y_position_to_characters):
                characters_in_line = y_position_to_characters[y_position]

                # Sort characters left to right by their x position
                characters_in_line.sort(key=lambda char: char["x0"])

                # Join all characters into a single text string
                line_text = "".join(char["text"] for char in characters_in_line).strip()
                if not line_text:
                    continue

                # Get the largest font size in this line
                largest_font_size = max(char["size"] for char in characters_in_line)

                # Check if any character in the line is bold
                line_is_bold = any(
                    self.isBoldChar(char.get("fontname", ""))
                    for char in characters_in_line
                )

                lines.append({
                    "page": page_number,
                    "text": line_text,
                    "font_size": largest_font_size,
                    "is_bold": line_is_bold,
                    "y_position": y_position,
                })

        return lines

    def detectBodyFontSize(self):
        """
        Find the most common font size in the document,
        which is assumed to be the body text size.

        Returns:
            float: The most frequently used font size.
        """
        all_font_sizes = [round(line["font_size"], 1) for line in self.lines]
        most_common_size = Counter(all_font_sizes).most_common(1)[0][0]
        return most_common_size

    def findSectionBoundaries(self, body_font_size, section_list):
        """
        Scan all lines and find which ones match a section heading.

        A line is considered a heading if:
        - It closely matches a known section title, AND
        - It is either bold OR has a larger font size than body text.

        Returns:
            list[dict]: Each dict has keys: title, found_text, score, page, line_index.
        """
        boundaries = []
        already_matched_titles = set()

        for line_index, line in enumerate(self.lines):
            line_text = line["text"]
            line_font_size = line["font_size"]
            line_is_bold = line["is_bold"]

            # Check if font size is larger than body text
            is_larger_font = (line_font_size >= body_font_size + FONT_SIZE_DELTA)

            # Use stricter threshold for lines that are NOT bold or larger
            if is_larger_font or line_is_bold:
                similarity_threshold = MATCH_THRESHOLD
            else:
                similarity_threshold = MATCH_THRESHOLD + 0.15

            # Find the best matching section title for this line
            matched_title, similarity_score = self.findBestMatch(line_text, section_list)

            # Skip if no good match found
            if matched_title is None or similarity_score < similarity_threshold:
                continue

            # Skip if this title was already matched earlier
            if matched_title in already_matched_titles:
                continue

            already_matched_titles.add(matched_title)
            boundaries.append({
                "title": matched_title,
                "found_text": line_text,
                "score": round(similarity_score, 3),
                "page": line["page"],
                "line_index": line_index,
            })

        # Sort boundaries by their position in the document
        boundaries.sort(key=lambda boundary: boundary["line_index"])
        return boundaries

    def splitIntoSection(self):
        """
        Split the full text into sections based on detected headings.

        Returns:
            dict: Mapping of section title to its text content.
        """
        sections = {}
        total_lines = len(self.lines)

        for boundary_index, boundary in enumerate(self.boundaries):
            # Section starts at this heading
            start_index = boundary["line_index"]

            # Section ends at the next heading (or end of document)
            if boundary_index + 1 < len(self.boundaries):
                end_index = self.boundaries[boundary_index + 1]["line_index"]
            else:
                end_index = total_lines

            # Collect all text lines between start and end
            # Skip start_index (that's the heading itself)
            text_parts = []
            previous_page = None

            for line in self.lines[start_index + 1:end_index]:
                # Add blank line between pages
                if previous_page is not None and line["page"] != previous_page:
                    text_parts.append("")
                text_parts.append(line["text"])
                previous_page = line["page"]

            section_text = "\n".join(text_parts).strip()
            section_text = section_text.replace("\n", " ")
            section_text = self.removeSubtitle(boundary["title"], section_text)
            sections[boundary["title"]] = section_text

        return sections

    def findBestMatch(self, line_text, section_list):
        """
        Compare a line of text against all known section titles
        and return the best match.

        Returns:
            tuple: (matched_title, similarity_score) or (None, best_score)
        """
        best_title = None
        best_score = 0.0

        for title in section_list:
            # Compare lowercase, stripped versions
            similarity = SequenceMatcher(
                None,
                line_text.lower().strip(),
                title.lower().strip()
            ).ratio()

            if similarity > best_score:
                best_title = title
                best_score = similarity

        if best_score >= MATCH_THRESHOLD:
            return best_title, best_score
        return None, best_score

    @staticmethod
    def isBoldChar(font_name):
        """Check if a font name indicates bold text."""
        if not font_name:
            return False
        return any(keyword in font_name for keyword in BOLD_KEYWORDS)

    @staticmethod
    def removeSubtitle(title, text):
        """
        If a section has a known subtitle template, strip it from the
        beginning of the extracted text.
        """
        subtitle = SUBTITLE_MAP.get(title)
        if not subtitle:
            return text

        # Normalize spaces for comparison
        clean_subtitle = " ".join(subtitle.split())
        clean_text = " ".join(text.split())

        if clean_text.startswith(clean_subtitle):
            text = text[len(subtitle):].strip()

        return text
