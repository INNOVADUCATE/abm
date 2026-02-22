# abm_reasoner_gui.py
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
import subprocess
import sys

def run_reasoner():
    files = filedialog.askopenfilenames(
        title="Seleccionar archivos *_ANALISIS_ABM.json",
        filetypes=[("ABM análisis", "*_ANALISIS_ABM.json")]
    )

    if not files:
        return

    model = model_var.get().strip()
    if not model:
        messagebox.showerror("Error", "Debés indicar un modelo Ollama.")
        return

    python = sys.executable

    ok = 0
    for f in files:
        try:
            subprocess.run(
                [
                    python,
                    "abm_reasoner.py",
                    "--input", f,
                    "--model", model
                ],
                check=True
            )
            ok += 1
        except subprocess.CalledProcessError as e:
            messagebox.showerror("Error", f"Error procesando:\n{f}\n\n{e}")
            return

    messagebox.showinfo("Listo", f"Procesados correctamente: {ok}")

# --- UI ---
root = tk.Tk()
root.title("ABM Reasoner (por clicks)")
root.geometry("420x200")
root.resizable(False, False)

tk.Label(root, text="Modelo Ollama (local):").pack(pady=(15, 5))

model_var = tk.StringVar(value="deepseek-r1:7b")
tk.Entry(root, textvariable=model_var, width=40).pack()

tk.Button(
    root,
    text="Seleccionar archivos y procesar",
    command=run_reasoner,
    height=2
).pack(pady=25)

tk.Label(
    root,
    text="Seleccioná uno o varios *_ANALISIS_ABM.json\nTodo es local, sin cloud",
    fg="gray"
).pack()

root.mainloop()
