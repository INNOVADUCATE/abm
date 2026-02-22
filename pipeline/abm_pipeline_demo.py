from pathlib import Path
import json
from typing import Optional

import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox

from abm_paddle_core import process_pdf_with_paddle, StructuredLogger
from abm_extractor_campos import analizar_txt_abm
from abm_llm_postprocess import run_llm_postprocess


# ============================================================
# DECISIÓN: ¿CUÁNDO ESCALAR A LLM?
# ============================================================

def necesita_llm(analisis):
    """
    Decide si el legajo necesita análisis LLM profundo.
    Regla ABM: mejor escalar de más que perder información.
    """

    # Sin nombre claro → LLM
    if analisis.campos.nombre_completo is None:
        return True

    # Sin identificación mínima → LLM
    if analisis.campos.dni is None and analisis.campos.cuil_cuit is None:
        return True

    # No se pudo inferir tipo de persona → LLM
    if analisis.campos.tipo_persona is None:
        return True

    # Muchos faltantes → LLM
    if len(analisis.faltantes) > 3:
        return True

    return False


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def procesar_legajo_pdf(pdf_path: Path, cliente_id: Optional[str] = None):
    """
    Pipeline híbrido ABM:
      1) OCR con PaddleOCR
      2) Análisis rápido (regex)
      3) Decisión automática
      4) Escalado a LLM (Gemma) si hace falta
    """

    pdf_path = pdf_path.resolve()
    logger = StructuredLogger(session_name="abm_pipeline_hibrido")

    # --------------------------------------------------------
    # 1) OCR
    # --------------------------------------------------------
    logger.log(
        nivel="INFO",
        evento="PIPELINE_INICIO",
        modulo="pipeline",
        mensaje=f"Iniciando pipeline para {pdf_path.name}",
        archivo=str(pdf_path),
    )

    ocr_result = process_pdf_with_paddle(
        pdf_path,
        cliente_id=cliente_id,
        logger=logger,
    )

    txt_path = Path(ocr_result["txt_output"])

    # --------------------------------------------------------
    # 2) ANÁLISIS RÁPIDO (REGEX)
    # --------------------------------------------------------
    analisis_regex = analizar_txt_abm(
        txt_path=txt_path,
        document_id=ocr_result["document_id"],
        cliente_id=cliente_id,
        logger=logger,
    )

    # --------------------------------------------------------
    # 3) DECISIÓN
    # --------------------------------------------------------
    if necesita_llm(analisis_regex):
        logger.log(
            nivel="INFO",
            evento="ESCALAR_LLM",
            modulo="pipeline",
            mensaje="Análisis regex insuficiente → escalando a LLM",
            metadata={
                "faltantes": len(analisis_regex.faltantes),
                "nombre": analisis_regex.campos.nombre_completo,
                "tipo_persona": analisis_regex.campos.tipo_persona,
            },
        )

        # ----------------------------------------------------
        # 4) LLM POSTPROCESO (Gemma3)
        # ----------------------------------------------------
        json_llm_path = run_llm_postprocess(
            txt_path=txt_path,
            pdf_path=pdf_path,
            cliente_id=cliente_id,
            logger=logger,
        )

        logger.log(
            nivel="INFO",
            evento="PIPELINE_FIN_LLM",
            modulo="pipeline",
            mensaje="Pipeline finalizado con análisis LLM",
            archivo=str(json_llm_path),
        )

        resultado_final = {
            "modo": "LLM",
            "json_resultado": str(json_llm_path),
            "ocr": ocr_result,
        }

    else:
        logger.log(
            nivel="INFO",
            evento="PIPELINE_FIN_REGEX",
            modulo="pipeline",
            mensaje="Análisis regex suficiente → no se usa LLM",
        )

        resultado_final = {
            "modo": "REGEX",
            "analisis_regex": analisis_regex,
            "ocr": ocr_result,
        }

    # --------------------------------------------------------
    # 5) RESUMEN CONSOLA
    # --------------------------------------------------------
    print("\n================ RESUMEN PIPELINE ================\n")
    print("PDF:        ", ocr_result["pdf"])
    print("TXT OCR:    ", ocr_result["txt_output"])
    print("Document ID:", ocr_result["document_id"])
    print("Páginas:    ", ocr_result["paginas"])
    print("Tiempo OCR: ", round(ocr_result["tiempo_seg"], 2), "s")
    print("Modo final: ", resultado_final["modo"])

    if resultado_final["modo"] == "REGEX":
        print("\nCampos detectados (regex):")
        print(json.dumps(analisis_regex.campos.__dict__, indent=2, ensure_ascii=False))
        print("\nFaltantes:")
        print(analisis_regex.faltantes)

    else:
        print("\nJSON final LLM:")
        print(resultado_final["json_resultado"])

    return resultado_final


# ============================================================
# UI SIMPLE (FILE DIALOG)
# ============================================================

def seleccionar_pdf_y_correr():
    root = tk.Tk()
    root.withdraw()

    pdf_file = filedialog.askopenfilename(
        title="Seleccionar PDF de legajo ABM",
        filetypes=[("Archivos PDF", "*.pdf"), ("Todos", "*.*")],
    )

    if not pdf_file:
        print("No se seleccionó ningún archivo.")
        root.destroy()
        return

    cliente_id = simpledialog.askstring(
        "Cliente / Socio",
        "Ingresá CUIT o código de socio (opcional):",
        parent=root,
    )
    cliente_id = (cliente_id or "").strip() or None

    root.destroy()

    try:
        procesar_legajo_pdf(Path(pdf_file), cliente_id)
        messagebox.showinfo("Proceso finalizado", "El legajo fue procesado correctamente.")
    except Exception as e:
        print(f"[ERROR] Falló el procesamiento: {e}")
        try:
            tk.Tk().withdraw()
            messagebox.showerror("Error", f"Falló el procesamiento:\n{e}")
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    seleccionar_pdf_y_correr()
