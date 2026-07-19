# PDF Extractor (`extract_pdf.py`)

This module extracts structured section text from LPPM proposal and report PDFs.
It uses `pdfplumber` to read character-level PDF data, detects section headings,
then returns a dictionary where each key is a section title and each value is the
text inside that section.

## Purpose

LPPM research documents use repeated templates such as proposal, progress report,
and final report formats. Each format has known section headings, for example:

- `Judul Penelitian`
- `Identitas Pengusul`
- `Ringkasan`
- `Kata kunci`
- `PENDAHULUAN`
- `METODE`
- `DAFTAR PUSTAKA`

`PDFExtractor` uses those headings as boundaries so downstream processing can
read specific document parts without manually parsing the whole PDF.

## Main Class

```python
from pipeline.tools.extract_pdf import PDFExtractor

extractor = PDFExtractor("proposal.pdf")
sections = extractor.extract()
```

`sections` is returned as:

```python
{
    "Judul Penelitian": "...",
    "Identitas Pengusul": "...",
    "Ringkasan": "...",
    "Kata kunci": "...",
}
```

You can also force a document type:

```python
extractor = PDFExtractor("proposal.pdf", doc_type="proposal_mandiri")
sections = extractor.extract()
```

## Supported Document Types

The supported types are defined in `DOCUMENT_TYPES`.

| Type | Description |
| --- | --- |
| `proposal_mandiri` | Proposal without funding amount and budget section. |
| `proposal_umum` | General proposal with funding amount and research budget. |
| `proposal_penugasan` | Assignment proposal with funding amount, assignment field, and budget. |
| `proposal_prioritas` | Priority proposal with funding amount, priority field, and budget. |
| `laporan_kemajuan` | Progress report using report result/status sections. |
| `laporan_akhir` | Final report using report result/status sections. |

If `doc_type` is not provided, the extractor tries to detect it automatically.

## How Extraction Works

### 1. Validate Input

`extract()` first checks whether the PDF path exists. If the file is missing, it
raises `FileNotFoundError`.

### 2. Read PDF Lines

The extractor opens the PDF with `pdfplumber` and reads `page.chars`, which are
individual text characters with position, font, and size metadata.

Characters are grouped into text lines by their vertical position:

```python
y_key = round(ch["top"] / 2) * 2
```

This rounds nearby character positions into the same line group. Each extracted
line stores:

- `page`: page number
- `text`: full line text
- `font_size`: largest font size in that line
- `is_bold`: whether any character font name contains `bold`
- `y_position`: rounded vertical position

### 3. Detect Document Type

When `doc_type` is omitted, `_detect_doc_type()` joins all extracted line text
and scores each known document type.

The score is:

```text
number of expected section headings found / total expected headings
```

The document type with the highest score is selected. If the best score is below
`40%`, extraction stops with `ValueError` and the caller should pass `doc_type`
manually.

### 4. Detect Body Font

`_detect_body_font()` finds the most common font size in the document. This is
assumed to be the normal body-text font size.

That value is used to distinguish normal paragraphs from headings.

### 5. Find Section Boundaries

`_find_boundaries()` scans every extracted line and compares it against the
expected headings for the selected document type.

A line is treated as heading-like when:

- its font size is at least `0.5` larger than the body font, or
- it contains a bold font name

Heading text is matched with `difflib.SequenceMatcher`, so minor PDF extraction
differences can still match the expected section title.

Default matching rules:

| Constant | Meaning |
| --- | --- |
| `MATCH_THRESHOLD = 0.75` | Minimum fuzzy-match score for heading candidates. |
| `FONT_SIZE_DELTA = 0.5` | Font-size increase needed to treat a line as heading-like. |
| `BOLD_KEYWORDS` | Font-name strings used to identify bold text. |

If a line looks like a heading, it uses the normal `0.75` threshold. If it does
not look like a heading, the threshold is raised to `0.90` to avoid false
matches in body text.

Each heading is only accepted once. Accepted boundaries store:

- canonical section title
- actual text found in the PDF
- match score
- page number
- line index

### 6. Split Text Into Sections

`_split_sections()` uses the detected boundaries to slice `self.lines`.

For each heading:

- content starts after the heading line
- content ends before the next detected heading
- a blank line is inserted when content crosses to a new PDF page
- internal newlines are collapsed into spaces before returning the result

The final output is a dictionary:

```python
{
    section_title: cleaned_section_text
}
```

### 7. Remove Template Instructions

Some LPPM sections include instructional text from the template, such as
directions for `Ringkasan`, `METODE`, or `DAFTAR PUSTAKA`.

`_remove_subtitle()` removes known leading instructions defined in
`SUBTITLE_MAP`. It also has a special cleanup rule for `Kata kunci` to remove
the proposal-substance instruction block when it appears in the extracted text.

## Important Data Structures

### `DOCUMENT_TYPES`

Maps each supported document type to the ordered section headings expected in
that type.

The ordering matters because detected boundaries are sorted by their line index
and then used to split the document.

### `SUBTITLE_MAP`

Maps section titles to instructional text that should be removed from the
beginning of extracted section content.

This keeps returned values closer to the user's actual submitted content instead
of the form template.

## Error Cases

| Case | Error |
| --- | --- |
| PDF file does not exist | `FileNotFoundError` |
| `doc_type` is unknown | `ValueError` |
| Auto-detection confidence is below 40% | `ValueError` |

## Practical Notes

- This extractor works best on text-based PDFs. Scanned image PDFs need OCR
  before this module can read meaningful text.
- Heading detection depends on the document template. New templates should be
  added to `DOCUMENT_TYPES`.
- If a template instruction appears in output, add the exact leading instruction
  text to `SUBTITLE_MAP`.
- The module currently provides extraction logic only. It does not write to
  MinIO, Spark, Iceberg, or dbt by itself.

## Minimal Example

```python
from pipeline.tools.extract_pdf import PDFExtractor

pdf_path = "ardikasatria-proposal-penelitian-kepakaran-2025.pdf"

extractor = PDFExtractor(pdf_path, doc_type="proposal_mandiri")
sections = extractor.extract()

print(sections["Judul Penelitian"])
print(sections.get("Ringkasan"))
```

