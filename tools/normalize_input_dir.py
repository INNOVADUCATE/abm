#!/usr/bin/env python3
"""Normalize a flat PDF pool into ABM socio folders by detected ID."""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import shutil
import tempfile
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from pypdf import PdfReader
except Exception:  # Optional dependency
    PdfReader = None

try:
    import fitz  # PyMuPDF
except Exception:  # Optional dependency
    fitz = None


CANDIDATE_11_RE = re.compile(r"(?<!\d)(?:\d[\s-]*){11}(?!\d)")
CANDIDATE_DNI_RE = re.compile(r"(?<!\d)\d{7,8}(?!\d)")
DATE_DDMMYYYY_RE = re.compile(r"^(0[1-9]|[12]\d|3[01])(0[1-9]|1[0-2])(19\d{2}|20\d{2})$")
DATE_YYYYMMDD_RE = re.compile(r"^(19\d{2}|20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$")
KEYWORDS = ("dni", "cuil", "cuit", "documento", "identidad", "afip", "renaper")
ROTATIONS = (0, 90, 180, 270)
DOC_TYPE_MARKERS = (
    "APORTE",
    "OSDE",
    "OSEP",
    "AFIP",
    "ARCA",
    "ARBA",
    "SSS",
)
GENERIC_NAME_TOKENS = {
    "ACTA",
    "ADENDA",
    "AFIP",
    "ARCA",
    "ARBA",
    "APORTE",
    "OSDE",
    "OSEP",
    "SSS",
    "FORM",
    "FORMULARIO",
}


@dataclass
class Detection:
    id_type: str
    id_value: str
    method: str
    confidence: float = 0.0
    rotation_used: int = 0


class PaddleOcrBackend:
    def __init__(self) -> None:
        try:
            from paddleocr import PaddleOCR

            try:
                self._ocr = PaddleOCR(use_angle_cls=False, lang="es", show_log=False)
            except TypeError:
                self._ocr = PaddleOCR(use_angle_cls=False, lang="es")
        except Exception as exc:
            raise RuntimeError(f"No se pudo inicializar PaddleOCR: {exc}") from exc

    def extract_text(self, image_path: Path) -> str:
        result = self._ocr.ocr(str(image_path), cls=False)
        lines: list[str] = []
        for block in result or []:
            for item in block or []:
                if isinstance(item, list) and len(item) >= 2 and isinstance(item[1], (list, tuple)):
                    lines.append(str(item[1][0]))
        return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize flat PDF directory by CUIL/DNI folders")
    parser.add_argument("--in", dest="input_dir", required=True, help="Input directory with loose PDFs")
    parser.add_argument("--out", dest="output_dir", required=True, help="Output normalized directory")
    parser.add_argument("--csv", dest="csv_path", default=None, help="CSV path (default: <out>/index.csv)")
    parser.add_argument("--scan-pdf-text", action="store_true", help="Try embedded PDF text before OCR")
    parser.add_argument("--ocr", action="store_true", help="Run lightweight OCR only on SIN_ID cases")
    parser.add_argument("--max-ocr-pages", type=int, default=2, help="Max pages per PDF for OCR (default: 2)")
    parser.add_argument(
        "--ocr-topk",
        type=int,
        default=0,
        help="Extra top-k pages by embedded-text score to add to [0,last] candidates",
    )
    parser.add_argument(
        "--group-by-filename-name",
        action="store_true",
        help="Fallback: agrupa SIN_ID por nombre de socio derivado del filename",
    )
    return parser.parse_args()


def _normalize_filename_tokens(stem: str) -> list[str]:
    ascii_text = unicodedata.normalize("NFKD", stem).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Z0-9]+", " ", ascii_text.upper())
    collapsed = re.sub(r"\s+", " ", cleaned).strip()
    return collapsed.split() if collapsed else []


def derive_name_group_from_filename(stem: str) -> tuple[str | None, str]:
    tokens = _normalize_filename_tokens(stem)
    if not tokens:
        return None, "filename_name: vacio tras limpieza"

    marker_idx = next((idx for idx, token in enumerate(tokens) if token in DOC_TYPE_MARKERS), None)
    marker = tokens[marker_idx] if marker_idx is not None else ""

    if marker_idx is None:
        candidate_tokens = [t for t in tokens if not t.isdigit()]
    else:
        before = [t for t in tokens[:marker_idx] if not t.isdigit()]
        after = [t for t in tokens[marker_idx + 1 :] if not t.isdigit() and t not in GENERIC_NAME_TOKENS]
        candidate_tokens = before if before else after

    if not candidate_tokens:
        return None, f"filename_name: sin tokens de nombre util ({'marker=' + marker if marker else 'sin marker'})"

    if all(token in GENERIC_NAME_TOKENS for token in candidate_tokens):
        return None, "filename_name: tokens genericos"

    socio_key = " ".join(candidate_tokens)
    note = f"filename_name: cleaned='{ ' '.join(tokens) }'"
    if marker:
        note += f", tipo_doc='{marker}'"
    return socio_key, note


def normalize_digits(value: str) -> str:
    return re.sub(r"\D", "", value)


def looks_like_date(candidate: str) -> bool:
    return bool(DATE_DDMMYYYY_RE.match(candidate) or DATE_YYYYMMDD_RE.match(candidate))


def find_cuil_candidates(text: str) -> Iterable[str]:
    for raw in CANDIDATE_11_RE.findall(text):
        digits = normalize_digits(raw)
        if len(digits) == 11:
            yield digits


def find_dni_candidates(text: str) -> Iterable[str]:
    for raw in CANDIDATE_DNI_RE.findall(text):
        if looks_like_date(raw):
            continue
        yield raw


def detect_id_from_text(text: str, method: str, confidence: float = 0.0, rotation: int = 0) -> Detection | None:
    for cuil in find_cuil_candidates(text):
        return Detection(id_type="CUIL", id_value=cuil, method=method, confidence=confidence, rotation_used=rotation)
    for dni in find_dni_candidates(text):
        return Detection(id_type="DNI", id_value=dni, method=method, confidence=confidence, rotation_used=rotation)
    return None


def extract_pdf_text(pdf_path: Path) -> str:
    if PdfReader is None:
        raise RuntimeError("pypdf no está instalado. Instalá con: pip install pypdf")
    reader = PdfReader(str(pdf_path))
    return "\n".join((page.extract_text() or "") for page in reader.pages)


def extract_pdf_page_texts(pdf_path: Path) -> list[str]:
    if PdfReader is None:
        return []
    try:
        reader = PdfReader(str(pdf_path))
        return [(page.extract_text() or "") for page in reader.pages]
    except Exception:
        return []


def get_page_count(pdf_path: Path) -> int:
    if fitz is not None:
        try:
            with fitz.open(pdf_path) as doc:
                return len(doc)
        except Exception:
            pass
    if PdfReader is not None:
        try:
            return len(PdfReader(str(pdf_path)).pages)
        except Exception:
            pass
    return 1


def page_score(text: str) -> int:
    if not text:
        return 0
    digits = len(re.findall(r"\d", text))
    kws = sum(1 for kw in KEYWORDS if kw in text.lower())
    return digits + 20 * kws


def candidate_pages_for_ocr(pdf_path: Path, max_pages: int, extra_topk: int) -> list[int]:
    count = max(1, get_page_count(pdf_path))
    candidates = {0, count - 1}

    if extra_topk > 0:
        page_texts = extract_pdf_page_texts(pdf_path)
        if page_texts:
            ranked = sorted(range(len(page_texts)), key=lambda i: page_score(page_texts[i]), reverse=True)
            for idx in ranked[:extra_topk]:
                candidates.add(idx)

    ordered = sorted(i for i in candidates if 0 <= i < count)
    return ordered[: max(1, max_pages)]


def ocr_score(text: str, detection: Detection | None) -> float:
    digit_count = len(re.findall(r"\d", text))
    base = float(digit_count)
    if detection:
        base += 80.0 if detection.id_type == "CUIL" else 60.0
    return base


def detect_with_ocr(pdf_path: Path, pages: list[int]) -> tuple[Detection | None, str]:
    if fitz is None:
        return None, "OCR no disponible: falta PyMuPDF (fitz)."
    try:
        backend = PaddleOcrBackend()
    except Exception as exc:
        return None, f"OCR no disponible: {exc}"

    best_detection: Detection | None = None
    best_score = -1.0

    try:
        with fitz.open(pdf_path) as doc, tempfile.TemporaryDirectory(prefix="abm_ocr_") as tmp:
            temp_dir = Path(tmp)
            for page_idx in pages:
                if page_idx < 0 or page_idx >= len(doc):
                    continue
                page = doc[page_idx]
                for rot in ROTATIONS:
                    pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0).prerotate(rot), alpha=False)
                    img_path = temp_dir / f"p{page_idx}_r{rot}.png"
                    pix.save(str(img_path))
                    text = backend.extract_text(img_path)
                    detection = detect_id_from_text(text, method="ocr", rotation=rot)
                    score = ocr_score(text, detection)
                    if score > best_score:
                        best_score = score
                        best_detection = detection
    except Exception as exc:
        return None, f"Error OCR: {exc}"

    if best_detection is None:
        return None, "OCR ejecutado sin encontrar ID"

    best_detection.confidence = best_score
    return best_detection, ""


def unique_name_prefix(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()[:10]


def process_pdf(
    pdf_path: Path,
    out_dir: Path,
    scan_pdf_text: bool,
    ocr: bool,
    max_ocr_pages: int,
    ocr_topk: int,
    group_by_filename_name: bool,
) -> dict[str, str]:
    filename = pdf_path.name
    notes: list[str] = []
    error_msg = ""
    candidate_pages = ""
    rotation_used = ""
    ocr_used = "false"

    detection = detect_id_from_text(pdf_path.stem, method="filename", confidence=100.0)

    if detection is None and scan_pdf_text:
        try:
            pdf_text = extract_pdf_text(pdf_path)
            detection = detect_id_from_text(pdf_text, method="text", confidence=90.0)
        except Exception as exc:
            error_msg = str(exc)

    if detection is None and ocr:
        pages = candidate_pages_for_ocr(pdf_path, max_pages=max_ocr_pages, extra_topk=ocr_topk)
        candidate_pages = ",".join(str(p) for p in pages)
        ocr_detection, ocr_note = detect_with_ocr(pdf_path, pages)
        ocr_used = "true"
        if ocr_note:
            notes.append(ocr_note)
        if ocr_detection is not None:
            detection = ocr_detection
            rotation_used = str(ocr_detection.rotation_used)

    filename_name_key: str | None = None
    if detection is None and group_by_filename_name:
        filename_name_key, filename_note = derive_name_group_from_filename(pdf_path.stem)
        notes.append(filename_note)

    if detection:
        folder = out_dir / f"{detection.id_type}_{detection.id_value}"
        dest_name = filename
        detected_id_type = detection.id_type
        detected_id = detection.id_value
        method = detection.method
        confidence = f"{detection.confidence:.2f}" if detection.confidence else ""
    elif filename_name_key:
        folder = out_dir / f"NOMBRE_{filename_name_key}"
        dest_name = filename
        detected_id_type = "NOMBRE"
        detected_id = filename_name_key
        method = "filename_name"
        confidence = ""
    else:
        folder = out_dir / "SIN_ID"
        dest_name = f"{unique_name_prefix(pdf_path)}__{filename}"
        detected_id_type = "SIN_ID"
        detected_id = ""
        method = "none"
        confidence = ""

    folder.mkdir(parents=True, exist_ok=True)
    dest_path = folder / dest_name
    shutil.copy2(pdf_path, dest_path)

    return {
        "filename": filename,
        "detected_id_type": detected_id_type,
        "detected_id": detected_id,
        "dest_path": str(dest_path),
        "method": method,
        "errors": error_msg,
        "candidate_pages": candidate_pages,
        "rotation_used": rotation_used,
        "ocr_used": ocr_used,
        "confidence": confidence,
        "notes": " | ".join(notes),
    }


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input inválido: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = Path(args.csv_path).expanduser().resolve() if args.csv_path else output_dir / "index.csv"

    pdfs = sorted(p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == ".pdf")
    fieldnames = [
        "filename",
        "detected_id_type",
        "detected_id",
        "dest_path",
        "method",
        "errors",
        "candidate_pages",
        "rotation_used",
        "ocr_used",
        "confidence",
        "notes",
    ]

    counters = {"filename": 0, "filename_name": 0, "text": 0, "ocr": 0, "sin_id_final": 0}

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for pdf_path in pdfs:
            row = process_pdf(
                pdf_path,
                output_dir,
                scan_pdf_text=args.scan_pdf_text,
                ocr=args.ocr,
                max_ocr_pages=max(1, args.max_ocr_pages),
                ocr_topk=max(0, args.ocr_topk),
                group_by_filename_name=args.group_by_filename_name,
            )
            writer.writerow(row)
            method = row["method"]
            if method == "filename":
                counters["filename"] += 1
            elif method == "text":
                counters["text"] += 1
            elif method == "ocr":
                counters["ocr"] += 1
            elif method == "filename_name":
                counters["filename_name"] += 1
            else:
                counters["sin_id_final"] += 1

    print(f"OK. PDFs procesados: {len(pdfs)}")
    print(f"CSV resumen: {csv_path}")
    print(
        "Conteos => total: {total}, filename: {filename}, filename_name: {filename_name}, pdf_text: {text}, ocr: {ocr}, sin_id_final: {sin}".format(
            total=len(pdfs),
            filename=counters["filename"],
            filename_name=counters["filename_name"],
            text=counters["text"],
            ocr=counters["ocr"],
            sin=counters["sin_id_final"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
