import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
import hashlib
import os

# IMPORTANTE: no usamos logger
from abm_extractor_campos import analizar_txt_abm


# ---------- util ----------
def file_md5(p: Path) -> str:
    h = hashlib.md5()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


# ---------- acción principal ----------
def run_evidencia():
    try:
        files = filedialog.askopenfilenames(
            title="Seleccionar TXT OCR (raw_ocr/*.txt)",
            filetypes=[("TXT", "*.txt")]
        )

        if not files:
            return

        out_dir = ROOT / "json_llm" / "01_evidencia"
        out_dir.mkdir(parents=True, exist_ok=True)

        cliente_id = cliente_id_var.get().strip() or None

        generados = []

        for f in files:
            txt_path = Path(f)
            document_id = file_md5(txt_path)

            # LLAMADA LIMPIA – SIN LOGGER
            analizar_txt_abm(
                txt_path=txt_path,
                document_id=document_id,
                cliente_id=cliente_id,
                output_dir=out_dir
            )

            generados.append(f"{txt_path.stem}_ANALISIS_ABM.json")

        messagebox.showinfo(
            "Listo",
            "Archivos generados:\n\n" + "\n".join(generados)
        )

        # Abrir carpeta de salida
        os.startfile(out_dir)

    except Exception as e:
        messagebox.showerror(
            "Error",
            f"{type(e).__name__}:\n{e}"
        )


# ---------- UI ----------
ROOT = Path(__file__).resolve().parent

root = tk.Tk()
root.title("ABM – Paso B | Evidencia (TXT → _ANALISIS_ABM.json)")
root.geometry("560x220")
root.resizable(False, False)

tk.Label(root, text="Cliente ID (opcional):").pack(pady=(15, 5))
cliente_id_var = tk.StringVar()
tk.Entry(root, textvariable=cliente_id_var, width=60).pack()

tk.Button(
    root,
    text="Seleccionar TXT y generar evidencia",
    height=2,
    command=run_evidencia
).pack(pady=25)

tk.Label(
    root,
    text="Salida fija: ./json_llm/01_evidencia/\nGenera: <nombre>_ANALISIS_ABM.json",
    fg="gray"
).pack()

root.mainloop()
