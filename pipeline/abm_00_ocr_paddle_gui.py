"""
ABM – Paso 0 (OCR con Paddle) – Ejecutable a clicks
---------------------------------------------------
- Seleccionás 1 o varios PDFs
- Genera TXT OCR en: ./raw_ocr/
- Genera JSON LLM-ready en: ./json_llm/00_ocr/
- Deja logs en: ./logs/

Usa tu core existente:
- abm_paddle_core.py (process_pdf_with_paddle_llm + StructuredLogger)

Requisitos:
  pip install paddleocr paddlepaddle pymupdf
"""

import os
import sys
import json
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
from datetime import datetime

# Core existente (OJO: la función correcta se llama process_pdf_with_paddle_llm)
from abm_paddle_core import process_pdf_with_paddle_llm, StructuredLogger


ROOT = Path(__file__).resolve().parent

RAW_PDFS_DIR = ROOT / "raw_pdfs"
RAW_OCR_DIR = ROOT / "raw_ocr"
OUT_OCR_DIR = ROOT / "json_llm" / "00_ocr"
TEMP_DIR = ROOT / "temp_proceso"
LOG_DIR = ROOT / "logs"

for d in (RAW_PDFS_DIR, RAW_OCR_DIR, OUT_OCR_DIR, TEMP_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)


def now_stamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_ocr(selected_files: list[str], cliente_id: str | None, lang: str):
    logger = StructuredLogger(session_name=f"ocr_{now_stamp()}")

    results = []
    for f in selected_files:
        pdf_path = Path(f)

        # Ejecuta OCR + genera outputs
        res = process_pdf_with_paddle_llm(
            pdf_path=pdf_path,
            logger=logger,
            output_dir=OUT_OCR_DIR,    # JSON LLM-ready
            temp_dir=TEMP_DIR,
            raw_ocr_dir=RAW_OCR_DIR,   # TXT OCR final
            lang=lang,
        )
        results.append(res)

    # Guardar un resumen general (útil para auditoría)
    resumen = {
        "schema": "abm_ocr_batch_v1",
        "fecha_proceso": datetime.now().isoformat(timespec="seconds"),
        "cliente_id": cliente_id,
        "lang": lang,
        "total_pdfs": len(results),
        "items": results,
        "paths": {
            "raw_ocr_dir": str(RAW_OCR_DIR),
            "out_ocr_dir": str(OUT_OCR_DIR),
            "logs_dir": str(LOG_DIR),
        },
    }
    resumen_path = OUT_OCR_DIR / f"batch_ocr_{now_stamp()}.json"
    resumen_path.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")

    return results, resumen_path


def open_folder(path: Path):
    try:
        os.startfile(str(path))  # Windows
    except Exception:
        pass


def gui():
    root = tk.Tk()
    root.title("ABM – Paso 0 | OCR con Paddle (PDF → TXT + JSON LLM-ready)")
    root.geometry("760x360")
    root.resizable(False, False)

    cliente_id_var = tk.StringVar(value="")
    lang_var = tk.StringVar(value="es")

    header = tk.Label(
        root,
        text="Seleccioná PDFs y generá OCR (TXT) + JSON LLM-ready",
        font=("Segoe UI", 12, "bold"),
    )
    header.pack(pady=(14, 8))

    box = tk.Frame(root)
    box.pack(pady=8)

    tk.Label(box, text="Cliente ID (opcional):").grid(row=0, column=0, sticky="w", padx=10, pady=6)
    tk.Entry(box, textvariable=cliente_id_var, width=55).grid(row=0, column=1, sticky="w", padx=10, pady=6)

    tk.Label(box, text="Idioma PaddleOCR:").grid(row=1, column=0, sticky="w", padx=10, pady=6)
    tk.Entry(box, textvariable=lang_var, width=10).grid(row=1, column=1, sticky="w", padx=10, pady=6)

    info = tk.Label(
        root,
        text=(
            "Entrada:\n"
            f"  - PDFs (cualquier carpeta, sugerido: {RAW_PDFS_DIR})\n\n"
            "Salida:\n"
            f"  - TXT OCR: {RAW_OCR_DIR}\n"
            f"  - JSON LLM-ready: {OUT_OCR_DIR}\n"
            f"  - Temp: {TEMP_DIR}\n"
            f"  - Logs: {LOG_DIR}\n"
        ),
        fg="gray",
        justify="left",
    )
    info.pack(pady=10)

    def on_select_and_run():
        try:
            files = filedialog.askopenfilenames(
                title="Seleccionar PDFs a OCR",
                initialdir=str(RAW_PDFS_DIR),
                filetypes=[("PDF", "*.pdf"), ("Todos", "*.*")],
            )
            if not files:
                return

            cliente_id = cliente_id_var.get().strip() or None
            lang = (lang_var.get().strip() or "es").lower()

            results, resumen_path = run_ocr(list(files), cliente_id, lang)

            ok_txt = sum(1 for r in results if Path(r["txt_output"]).exists())
            ok_json = sum(1 for r in results if Path(r["json_output"]).exists())

            messagebox.showinfo(
                "OCR listo",
                f"PDFs procesados: {len(results)}\n"
                f"TXT generados:   {ok_txt}\n"
                f"JSON generados:  {ok_json}\n\n"
                f"Resumen batch:\n{resumen_path}",
            )

            open_folder(RAW_OCR_DIR)

        except Exception as e:
            messagebox.showerror("Error", f"{type(e).__name__}: {e}")

    tk.Button(
        root,
        text="Seleccionar PDFs y ejecutar OCR",
        height=2,
        command=on_select_and_run
    ).pack(pady=10)

    root.mainloop()


def cli():
    # CLI opcional: procesa todos los PDFs dentro de raw_pdfs/
    cliente_id = None
    lang = "es"

    pdfs = list(RAW_PDFS_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No hay PDFs en {RAW_PDFS_DIR}")
        sys.exit(0)

    results, resumen_path = run_ocr([str(p) for p in pdfs], cliente_id, lang)
    print("OK. PDFs:", len(results))
    print("TXT:", RAW_OCR_DIR)
    print("JSON:", OUT_OCR_DIR)
    print("Batch:", resumen_path)


if __name__ == "__main__":
    # Si lo corrés con "python abm_00_ocr_paddle_gui.py --cli", usa CLI.
    if "--cli" in sys.argv:
        cli()
    else:
        gui()
