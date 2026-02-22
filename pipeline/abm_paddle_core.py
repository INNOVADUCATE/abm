"""
ABM – OCR LLM READY (sin Tkinter)
---------------------------------
Pipeline de OCR para documentos PDF usando PaddleOCR.
- Convierte PDF -> imágenes (PyMuPDF)
- Corre OCR página por página (PaddleOCR)
- Genera:
    * TXT "LLM-ready" con todo el texto concatenado
    * JSON crudo con el detalle por página (para trazabilidad)
- Deja todo logueado en JSONL + .log de texto

Requisitos:
    pip install "paddleocr" "paddlepaddle" fitz opencv-python numpy
"""

import os
import sys
import json
import time
import hashlib
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import fitz  # PyMuPDF
import cv2
import numpy as np
from paddleocr import PaddleOCR


# ============================================================
# CONFIG BÁSICA DE RUTAS
# ============================================================

BASE_DIR = Path(os.getcwd())
OUTPUT_DIR = BASE_DIR / "salida_final"
TEMP_DIR = BASE_DIR / "temp_proceso"
RAW_OCR_DIR = BASE_DIR / "raw_ocr"
LOG_DIR = BASE_DIR / "logs"

for d in (OUTPUT_DIR, TEMP_DIR, RAW_OCR_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGER ESTRUCTURADO
# ============================================================

@dataclass
class LogRecord:
    timestamp: str
    nivel: str
    evento: str
    modulo: str
    mensaje: str
    archivo: Optional[str] = None
    estado: Optional[str] = None
    metadata: Dict[str, Any] = None


class StructuredLogger:
    def __init__(self, session_name: str = "ocr_llm_ready", ui_callback=None) -> None:
        self.ui_callback = ui_callback
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.jsonl_path = LOG_DIR / f"{session_name}_{ts}.jsonl"
        self.txt_path = LOG_DIR / f"{session_name}_{ts}.log"

    @staticmethod
    def _now_iso_utc() -> str:
        # Versión sin deprecations: datetime con timezone UTC
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def log(
        self,
        nivel: str,
        evento: str,
        modulo: str,
        mensaje: str,
        archivo: Optional[str] = None,
        estado: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        rec = LogRecord(
            timestamp=self._now_iso_utc(),
            nivel=nivel,
            evento=evento,
            modulo=modulo,
            mensaje=mensaje,
            archivo=archivo,
            estado=estado,
            metadata=metadata or {},
        )

        # JSONL estructurado
        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")

        # Log humano
        line = f"[{datetime.now().strftime('%H:%M:%S')}] [{nivel}] [{modulo}] {mensaje}"
        with open(self.txt_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

        if self.ui_callback:
            self.ui_callback(line, nivel)


# ============================================================
# UTILIDADES
# ============================================================

def compute_document_id(path: Path) -> str:
    """ID determinista a partir de la ruta (se puede reemplazar por UUID real)."""
    return hashlib.sha1(str(path).encode("utf-8")).hexdigest()[:16]


def pdf_to_images(pdf_path: Path, temp_dir: Path = TEMP_DIR, zoom: float = 2.0) -> List[Path]:
    """Renderiza un PDF a imágenes PNG (una por página)."""
    temp_dir.mkdir(parents=True, exist_ok=True)
    doc = fitz.open(str(pdf_path))
    image_paths: List[Path] = []

    for page_index in range(len(doc)):
        page = doc[page_index]
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        img_path = temp_dir / f"{pdf_path.stem}_p{page_index+1}.png"
        pix.save(str(img_path))
        image_paths.append(img_path)

    doc.close()
    return image_paths


# ============================================================
# MOTOR OCR – VERSIÓN GENERAL PARA LLM
# ============================================================

class PaddleOCREngineLLM:
    """
    Envoltorio mínimo sobre PaddleOCR:
    - Usa PaddleOCR(lang="es") como en tu test manual (máxima compatibilidad).
    - No fuerza modelos ni flags raros → evitamos errores tipo 'show_log' / 'device'.
    - Devuelve:
        * texto plano por página
        * lista de items {bbox, text, score} para trazabilidad.
    """

    def __init__(self, lang: str = "es", logger: Optional[StructuredLogger] = None) -> None:
        self.lang = lang
        self.logger = logger
        self._ocr: Optional[PaddleOCR] = None

    def _log(self, nivel: str, msg: str) -> None:
        if self.logger:
            self.logger.log(nivel, "OCR_ENGINE", "paddle_engine", msg)
        else:
            print(f"[{nivel}] {msg}")

    def _init_engine(self) -> None:
        if self._ocr is not None:
            return

        try:
            self._log("INFO", f"Inicializando PaddleOCR(lang='{self.lang}')…")
            # Llamada mínima: tu instalación ya funciona con esto.
            self._ocr = PaddleOCR(lang=self.lang)
            self._log("INFO", "PaddleOCR inicializado correctamente.")
        except Exception as e:
            self._log("ERROR", f"Error inicializando PaddleOCR: {e}")
            raise

    def ocr_page(self, image_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Ejecuta OCR sobre una imagen y devuelve:
            - texto_plano (str)
            - raw_items: lista de dicts {bbox, text, score}
        """
        self._init_engine()
        assert self._ocr is not None

        # Usamos la API 'ocr' que sabés que funciona en tu entorno.
        result = self._ocr.ocr(str(image_path))

        if not result:
            return "", []

        plain_lines: List[str] = []
        raw_items: List[Dict[str, Any]] = []

        first = result[0]

        # Caso nuevo: list[dict] con rec_texts / rec_scores / rec_polys
        if isinstance(first, dict):
            rec_texts = first.get("rec_texts") or []
            rec_scores = first.get("rec_scores") or []
            rec_polys = first.get("rec_polys") or first.get("rec_boxes") or []

            n = min(len(rec_texts), len(rec_scores), len(rec_polys))
            for i in range(n):
                txt = str(rec_texts[i]).strip()
                if not txt:
                    continue
                score = float(rec_scores[i])
                poly = rec_polys[i]

                try:
                    bbox = [[int(x), int(y)] for x, y in poly]
                except Exception:
                    bbox = [list(map(int, pt)) for pt in poly]

                plain_lines.append(txt)
                raw_items.append({"bbox": bbox, "text": txt, "score": score})

            return "\n".join(plain_lines), raw_items

        # Caso antiguo: lista de líneas [[box, (text, score)], ...]
        for line in result:
            for box, (txt, score) in line:
                txt = str(txt).strip()
                if not txt:
                    continue
                plain_lines.append(txt)
                raw_items.append(
                    {
                        "bbox": box,
                        "text": txt,
                        "score": float(score),
                    }
                )

        return "\n".join(plain_lines), raw_items


# ============================================================
# PIPELINE COMPLETO PDF → TXT + JSON (LLM-READY)
# ============================================================

def process_pdf_with_paddle_llm(
    pdf_path: Path,
    logger: Optional[StructuredLogger] = None,
    output_dir: Path = OUTPUT_DIR,
    temp_dir: Path = TEMP_DIR,
    raw_ocr_dir: Path = RAW_OCR_DIR,
    lang: str = "es",
) -> Dict[str, Any]:
    """
    1) PDF -> imágenes
    2) OCR página por página
    3) TXT "LLM ready"
    4) JSON crudo por página
    """
    pdf_path = pdf_path.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_ocr_dir.mkdir(parents=True, exist_ok=True)

    if logger is None:
        logger = StructuredLogger(session_name="ocr_llm_ready")

    document_id = compute_document_id(pdf_path)

    logger.log(
        "INFO",
        "PDF_INGRESADO",
        "pipeline",
        f"Procesando {pdf_path.name}",
        archivo=str(pdf_path),
        estado="PENDIENTE",
        metadata={"document_id": document_id},
    )

    t0 = time.time()

    # 1) PDF -> imágenes
    image_paths = pdf_to_images(pdf_path, temp_dir=temp_dir, zoom=2.0)
    logger.log(
        "INFO",
        "PDF_RENDER_OK",
        "pipeline",
        f"{len(image_paths)} páginas renderizadas",
        archivo=str(pdf_path),
        estado="OK",
        metadata={"paginas": len(image_paths)},
    )

    # 2) OCR
    engine = PaddleOCREngineLLM(lang=lang, logger=logger)

    all_text_per_page: List[str] = []
    all_raw_per_page: List[Dict[str, Any]] = []

    for idx, img_path in enumerate(image_paths, start=1):
        logger.log(
            "INFO",
            "OCR_PAGINA_INICIO",
            "pipeline",
            f"OCR página {idx}/{len(image_paths)}",
            archivo=str(pdf_path),
            estado="PENDIENTE",
            metadata={"pagina": idx},
        )

        page_t0 = time.time()
        try:
            text_page, raw_items = engine.ocr_page(img_path)
            all_text_per_page.append(text_page)

            all_raw_per_page.append(
                {
                    "pagina": idx,
                    "image_path": str(img_path),
                    "items": raw_items,
                }
            )

            logger.log(
                "INFO",
                "OCR_PAGINA_OK",
                "pipeline",
                f"OCR página {idx} OK (chars={len(text_page)})",
                archivo=str(pdf_path),
                estado="OK",
                metadata={
                    "pagina": idx,
                    "chars": len(text_page),
                    "items": len(raw_items),
                    "tiempo_seg": round(time.time() - page_t0, 3),
                },
            )

        except Exception as e:
            logger.log(
                "ERROR",
                "OCR_PAGINA_ERROR",
                "pipeline",
                f"Error en página {idx}: {e}",
                archivo=str(pdf_path),
                estado="ERROR",
                metadata={"pagina": idx, "traceback": traceback.format_exc()},
            )
            all_text_per_page.append(f"[ERROR OCR PÁGINA {idx}]")
            all_raw_per_page.append(
                {
                    "pagina": idx,
                    "image_path": str(img_path),
                    "items": [],
                    "error": str(e),
                }
            )

    # 3) Guardar TXT LLM-ready
    txt_output = output_dir / f"{pdf_path.stem}_OCR_LLM_READY.txt"
    with open(txt_output, "w", encoding="utf-8") as tf:
        tf.write(f"DOCUMENTO: {pdf_path.name}\n")
        tf.write(f"DOCUMENT_ID: {document_id}\n")
        tf.write(f"FECHA_PROCESO: {StructuredLogger._now_iso_utc()}\n")
        tf.write(f"FUENTE: PaddleOCR(lang='{lang}')\n")
        tf.write("=" * 60 + "\n\n")
        for i, page_text in enumerate(all_text_per_page, start=1):
            tf.write(f"--- PÁGINA {i} ---\n")
            tf.write(page_text or "[SIN TEXTO DETECTADO]")
            tf.write("\n\n")

    logger.log(
        "INFO",
        "TXT_GUARDADO",
        "pipeline",
        f"TXT LLM-ready guardado en {txt_output}",
        archivo=str(txt_output),
        estado="OK",
        metadata={"paginas": len(all_text_per_page)},
    )

    # 4) Guardar JSON crudo
    json_output = raw_ocr_dir / f"{pdf_path.stem}_OCR_LLM_READY_RAW.json"
    with open(json_output, "w", encoding="utf-8") as jf:
        json.dump(
            {
                "documento": pdf_path.name,
                "document_id": document_id,
                "paginas": all_raw_per_page,
            },
            jf,
            ensure_ascii=False,
            indent=2,
        )

    logger.log(
        "INFO",
        "RAW_JSON_GUARDADO",
        "pipeline",
        f"JSON crudo guardado en {json_output}",
        archivo=str(json_output),
        estado="OK",
        metadata={"paginas": len(all_raw_per_page)},
    )

    elapsed = time.time() - t0
    logger.log(
        "INFO",
        "PDF_PROCESADO_OK",
        "pipeline",
        f"Procesamiento completado en {elapsed:.2f}s",
        archivo=str(pdf_path),
        estado="OK",
        metadata={
            "paginas": len(image_paths),
            "tiempo_seg": round(elapsed, 3),
            "txt_output": str(txt_output),
            "json_output": str(json_output),
        },
    )

    return {
        "document_id": document_id,
        "pdf": str(pdf_path),
        "txt_output": str(txt_output),
        "json_output": str(json_output),
        "paginas": len(image_paths),
        "tiempo_seg": elapsed,
    }


# ============================================================
# CLI SIMPLE (sin Tkinter)
# ============================================================

if __name__ == "__main__":
    print("ABM – OCR LLM READY (sin Tkinter)")
    print("Ingresá la ruta completa del PDF a procesar:")
    pdf_path_str = input("> ").strip().strip('"')

    if not pdf_path_str:
        print("No se ingresó ningún archivo. Saliendo.")
        sys.exit(0)

    pdf_file = Path(pdf_path_str)

    if not pdf_file.exists():
        print(f"ERROR: no se encontró el archivo: {pdf_file}")
        sys.exit(1)

    logger = StructuredLogger(session_name="ocr_llm_ready_cli")

    try:
        result = process_pdf_with_paddle_llm(pdf_file, logger=logger)

        print("\n=== OCR TERMINADO ===")
        print("PDF:        ", result["pdf"])
        print("TXT OCR:    ", result["txt_output"])
        print("RAW JSON:   ", result["json_output"])
        print("Páginas:    ", result["paginas"])
        print("Tiempo [s]: ", f"{result['tiempo_seg']:.2f}")

    except Exception as e:
        print("ERROR CRÍTICO:", e)
        sys.exit(1)
