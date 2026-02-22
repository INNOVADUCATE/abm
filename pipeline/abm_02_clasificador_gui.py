import json
import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime

from urllib.request import Request, urlopen
import json

def ollama_tags_models(base_url: str) -> list[str]:
    tags_url = base_url.replace("/api/generate", "/api/tags").replace("/api/chat", "/api/tags")
    req = Request(tags_url, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="ignore"))
    return [m.get("model") for m in data.get("models", []) if m.get("model")]

def resolve_model(ollama_url: str, wanted: str) -> str:
    models = ollama_tags_models(ollama_url)
    if wanted in models:
        return wanted
    # intenta match por substring (deepseek-r1-0528-qwen3) y 8b
    key = "deepseek-r1-0528-qwen3"
    for m in models:
        if key in m and "8b" in m:
            return m
    return wanted  # fallback

# ---------------------------
# Config
# ---------------------------
ROOT = Path(__file__).resolve().parent
OUT_DOCS = ROOT / "json_llm" / "02_clasificado" / "documentos"
OUT_SOCIOS = ROOT / "json_llm" / "02_clasificado" / "socios"
DEFAULT_OLLAMA_URL = "http://localhost:11434/api/generate"

# Taxonomía (Paso C): solo clasificar, no validar faltantes
DOC_TYPES = [
    "dni",
    "afip_arca",
    "ingresos_brutos_atm",
    "titulo_habilitante",
    "matricula_profesional",
    "sss_prestador",
    "seguro_mala_praxis",
    "habilitacion_laboratorio",
    "designacion_director_tecnico",
    "adhesion_abm",
    "adhesion_osep",
    "adhesion_pami",
    "aceptacion_pago_convenios",
    "cuenta_bancaria",
    "otros",
]

# ---------------------------
# Utils
# ---------------------------
def now_utc():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_uid(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\W+", "_", s)
    return s[:80] if s else "SIN_ID"

def read_text(txt_path: Path) -> str:
    return txt_path.read_text(encoding="utf-8", errors="ignore")

def norm(s: str) -> str:
    s = s.replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s

def lower(s: str) -> str:
    return norm(s).lower()

def pick_snippets(text: str, max_lines: int = 60) -> str:
    keywords = [
        "dni", "documento", "idarg", "registro nacional",
        "afip", "arca", "monotrib", "responsable inscrip",
        "ingresos brutos", "atm", "arba", "rentas",
        "póliza", "poliza", "aseguradora", "mala praxis", "responsabilidad civil",
        "superintendencia", "servicios de salud", "prestador", "registro nacional de prestadores",
        "habilitacion", "habilitación", "expediente", "ley",
        "director técnico", "director tecnico", "designación", "designacion",
        "osep", "pami", "abm", "adhes", "convenio", "aceptación", "aceptacion",
        "cbu", "alias", "banco", "cuenta",
        "universidad", "facultad", "diploma", "título", "titulo", "bioqu",
        "matrícula", "matricula", "mat.",
        "vigencia", "vence", "vencimiento",
    ]
    lines = norm(text).split("\n")
    out = []
    seen = set()

    for i, line in enumerate(lines):
        ll = line.lower()
        if any(k in ll for k in keywords):
            for j in range(max(0, i - 1), min(len(lines), i + 2)):
                l = lines[j].strip()
                if l and l not in seen:
                    seen.add(l)
                    out.append(l)
        if len(out) >= max_lines:
            break

    return "\n".join(out[:max_lines])

# ---------------------------
# Heurística (rápida y estable)
# ---------------------------
def score_by_keywords(text: str) -> dict:
    t = lower(text)

    def has_any(*ws):
        return any(w in t for w in ws)

    def has_all(*ws):
        return all(w in t for w in ws)

    scores = {k: 0 for k in DOC_TYPES}

    # DNI
    if has_any("registro nacional de las personas", "idarg", "mercosur", "date of expiry", "fecha de vencimiento"):
        scores["dni"] += 3
    if has_any("dni", "documento") and re.search(r"\b\d{1,2}\.\d{3}\.\d{3}\b|\b\d{7,8}\b", t):
        scores["dni"] += 2

    # AFIP/ARCA
    if has_any("afip", "arca", "monotrib", "responsable inscrip", "constancia de inscripción", "constancia de opcion"):
        scores["afip_arca"] += 3
    if has_all("vigencia", "constancia"):
        scores["afip_arca"] += 1

    # Ingresos brutos (ATM/ARBA/Rentas)
    if has_any("ingresos brutos", "atm", "arba", "rentas", "dirección general de rentas", "direccion general de rentas"):
        scores["ingresos_brutos_atm"] += 3
    if has_any("constancia vence", "la presente constancia vence"):
        scores["ingresos_brutos_atm"] += 1

    # Seguro mala praxis
    if has_any("mala praxis", "póliza", "poliza", "aseguradora", "responsabilidad civil"):
        scores["seguro_mala_praxis"] += 3
    if has_any("vigencia", "cobertura") and has_any("asegur", "poliza", "póliza"):
        scores["seguro_mala_praxis"] += 1

    # SSS Prestador
    if has_any("superintendencia de servicios de salud", "registro nacional de prestadores", "sssalud", "prestador"):
        scores["sss_prestador"] += 3

    # Matrícula
    if has_any("matrícula", "matricula", "mat."):
        scores["matricula_profesional"] += 2

    # Título habilitante
    if has_any("universidad", "facultad", "diploma", "título", "titulo", "bioqu"):
        scores["titulo_habilitante"] += 2

    # Habilitación laboratorio
    if has_any("habilitación", "habilitacion", "expediente", "s/ habilitacion", "ley"):
        scores["habilitacion_laboratorio"] += 2

    # Director técnico
    if has_any("director técnico", "director tecnico", "designación", "designacion"):
        scores["designacion_director_tecnico"] += 2

    # Adhesiones
    if has_any("adhes", "adhesión", "adhesion") and has_any("abm"):
        scores["adhesion_abm"] += 2
    if has_any("osep"):
        scores["adhesion_osep"] += 2
    if has_any("pami"):
        scores["adhesion_pami"] += 2

    # Aceptación pago / convenios
    if has_any("aceptación de pago", "aceptacion de pago", "convenio", "convenios"):
        scores["aceptacion_pago_convenios"] += 2

    # Cuenta bancaria
    if has_any("cbu", "alias", "cuenta bancaria", "banco"):
        scores["cuenta_bancaria"] += 2

    return scores

def pick_top_types(scores: dict) -> list:
    # devuelve 1-3 tipos con confianza
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_type, best_score = ranked[0]
    if best_score <= 0:
        return [{"tipo": "otros", "confianza": "baja", "score": 0}]

    out = []
    for tipo, sc in ranked[:3]:
        if sc <= 0:
            continue
        conf = "baja"
        if sc >= 3:
            conf = "alta"
        elif sc == 2:
            conf = "media"
        out.append({"tipo": tipo, "confianza": conf, "score": sc})

    return out or [{"tipo": best_type, "confianza": "baja", "score": best_score}]

# ---------------------------
# Ollama (opcional)
# ---------------------------
def ollama_classify(ollama_url: str, model: str, snippets: str) -> dict:
    """
    Devuelve JSON con tipos elegidos. Si falla, lanza excepción.
    """
    prompt = f"""
Sos un clasificador de documentos para ABM. Tenés que decidir a qué tipo(s) pertenece este documento.
NO inventes. Elegí 1 a 3 tipos de esta lista exacta:
{DOC_TYPES}

Devolvé ÚNICAMENTE JSON con este formato:
{{
  "tipos": [{{"tipo":"...", "confianza":"alta|media|baja", "motivo":"..."}}]
}}

Snippets (evidencia):
{snippets}
""".strip()

    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.0, "num_ctx": 8192},
    }

    req = Request(
        ollama_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )

    with urlopen(req, timeout=180) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")

    data = json.loads(raw)
    txt = (data.get("response") or "").strip()

    # parse robusto
    txt = txt.replace("```json", "").replace("```", "").strip()
    i, j = txt.find("{"), txt.rfind("}")
    if i == -1 or j == -1 or j <= i:
        raise ValueError("Ollama no devolvió JSON parseable.")
    return json.loads(txt[i : j + 1])

# ---------------------------
# Core: clasificar 1 análisis
# ---------------------------
def classify_one(analysis_path: Path, use_llm: bool, model: str, ollama_url: str):
    data = json.loads(analysis_path.read_text(encoding="utf-8", errors="ignore"))

    txt_path = Path(data.get("txt_path") or "")
    if not txt_path.is_absolute():
        txt_path = (analysis_path.parent / txt_path).resolve()
    if not txt_path.exists():
        raise FileNotFoundError(f"No existe txt_path: {txt_path}")

    text = read_text(txt_path)
    snippets = pick_snippets(text, max_lines=70)

    campos = data.get("campos", {}) or {}
    evidencia = data.get("evidencia", {}) or {}

    # socio_uid: preferimos CUIT, luego DNI, luego document_id
    cuit = (campos.get("cuil_cuit") or "").strip()
    dni = (campos.get("dni") or "").strip()
    document_id = str(data.get("document_id") or analysis_path.stem)

    socio_uid = cuit or dni or document_id
    socio_uid_safe = safe_uid(socio_uid)

    # Heurística base
    scores = score_by_keywords(text)
    tipos_h = pick_top_types(scores)

    tipos_final = tipos_h
    llm_info = {"usado": False, "model": None, "error": None}

    if use_llm:
        try:
            out = ollama_classify(ollama_url=ollama_url, model=model, snippets=snippets)
            tipos = out.get("tipos", [])
            # validar tipos
            tipos_ok = []
            for it in tipos:
                t = (it.get("tipo") or "").strip()
                if t in DOC_TYPES:
                    tipos_ok.append({
                        "tipo": t,
                        "confianza": (it.get("confianza") or "media"),
                        "motivo": (it.get("motivo") or ""),
                        "source": "ollama"
                    })
            if tipos_ok:
                tipos_final = tipos_ok
                llm_info = {"usado": True, "model": model, "error": None}
        except Exception as e:
            llm_info = {"usado": False, "model": model, "error": str(e)}
            # fallback a heurística

    out_doc = {
        "schema": "abm_doc_clasificado_v1",
        "fecha_proceso_utc": now_utc(),
        "socio_uid": socio_uid,
        "document_id": document_id,
        "input": {
            "analysis_json": str(analysis_path),
            "txt_path": str(txt_path),
        },
        "campos_aportados": {
            "dni": campos.get("dni"),
            "cuil_cuit": campos.get("cuil_cuit"),
            "matricula": campos.get("matricula"),
            "nombre_completo": campos.get("nombre_completo"),
            "direccion": campos.get("direccion"),
            "localidad": campos.get("localidad"),
        },
        "evidencia_resumen": {
            "dni_candidatos": evidencia.get("dni_candidatos"),
            "dni_mrz": evidencia.get("dni_mrz"),
            "cuit_candidatos": evidencia.get("cuit_candidatos"),
            "matricula_candidatos": evidencia.get("matricula_candidatos"),
            "keywords": evidencia.get("keywords"),
        },
        "clasificacion": {
            "tipos": tipos_final,
            "heuristica_scores": scores,
            "llm": llm_info,
        },
        "snippets": snippets,
    }

    # guardar documento clasificado
    OUT_DOCS.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DOCS / f"{analysis_path.stem}_CLASIFICADO.json"
    out_path.write_text(json.dumps(out_doc, ensure_ascii=False, indent=2), encoding="utf-8")

    return socio_uid_safe, out_doc, out_path

def merge_socios(consol: dict, socio_uid_safe: str, doc_obj: dict):
    s = consol.setdefault(socio_uid_safe, {
        "schema": "abm_socio_clasificado_v1",
        "fecha_proceso_utc": now_utc(),
        "socio_uid_safe": socio_uid_safe,
        "campos_consolidados": {},
        "documentos": [],
        "tipos_detectados": {},
    })

    # campos consolidados: mantener el primero no-nulo
    campos = doc_obj.get("campos_aportados", {}) or {}
    cc = s["campos_consolidados"]
    for k, v in campos.items():
        if v and not cc.get(k):
            cc[k] = v

    # acumular tipos
    for t in (doc_obj.get("clasificacion", {}).get("tipos") or []):
        tipo = t.get("tipo")
        if not tipo:
            continue
        s["tipos_detectados"][tipo] = s["tipos_detectados"].get(tipo, 0) + 1

    s["documentos"].append({
        "document_id": doc_obj.get("document_id"),
        "analysis_json": doc_obj.get("input", {}).get("analysis_json"),
        "txt_path": doc_obj.get("input", {}).get("txt_path"),
        "tipos": doc_obj.get("clasificacion", {}).get("tipos"),
    })

# ---------------------------
# GUI
# ---------------------------
def run_gui():
    try:
        files = filedialog.askopenfilenames(
            title="Seleccionar *_ANALISIS_ABM.json",
            filetypes=[("ABM análisis", "*_ANALISIS_ABM.json"), ("JSON", "*.json")]
        )
        if not files:
            return

        use_llm = use_llm_var.get()

        # 1) SIEMPRE definir ollama_url primero
        ollama_url = (ollama_url_var.get().strip() or DEFAULT_OLLAMA_URL).strip()

        # (opcional) arreglar si el usuario puso solo http://localhost:11434
        if ollama_url.endswith("11434"):
            ollama_url = ollama_url + "/api/generate"
        if ollama_url.endswith("/api/tags"):
            ollama_url = ollama_url.replace("/api/tags", "/api/generate")

        # 2) después definir model
        model = model_var.get().strip() or "sam860/deepseek-r1-0528-qwen3:8b"

        # 3) recién acá, si tenés resolve_model, lo llamás
        try:
            if use_llm:
                model = resolve_model(ollama_url, model)
        except NameError:
            # si no pegaste resolve_model, no pasa nada
            pass

        OUT_DOCS.mkdir(parents=True, exist_ok=True)
        OUT_SOCIOS.mkdir(parents=True, exist_ok=True)

        consol = {}
        ok = 0
        for f in files:
            analysis_path = Path(f)
            socio_uid_safe, doc_obj, out_path = classify_one(
                analysis_path=analysis_path,
                use_llm=use_llm,
                model=model,
                ollama_url=ollama_url,
            )
            merge_socios(consol, socio_uid_safe, doc_obj)
            ok += 1

        socios_generados = []
        for socio_uid_safe, obj in consol.items():
            p = OUT_SOCIOS / f"SOCIO_{socio_uid_safe}.json"
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            socios_generados.append(str(p))

        messagebox.showinfo(
            "Listo",
            f"Clasificados: {ok}\n\nSalida:\n{OUT_DOCS}\n{OUT_SOCIOS}\n\nModelo: {model}"
        )
        os.startfile(str(OUT_SOCIOS))

    except Exception as e:
        messagebox.showerror("Error", f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    root = tk.Tk()
    root.title("ABM – Paso C | Clasificación (ANALISIS → CLASIFICADO)")
    root.geometry("720x320")
    root.resizable(False, False)

    frame = tk.Frame(root)
    frame.pack(pady=12)

    use_llm_var = tk.BooleanVar(value=True)
    tk.Checkbutton(frame, text="Usar Ollama (recomendado)", variable=use_llm_var).grid(row=0, column=0, sticky="w", padx=10, pady=6)

    tk.Label(frame, text="Modelo Ollama:").grid(row=1, column=0, sticky="w", padx=10)
    model_var = tk.StringVar(value="deepseek-r1-0528-qwen3-8b")
    tk.Entry(frame, textvariable=model_var, width=60).grid(row=1, column=1, padx=10, pady=4, sticky="w")

    tk.Label(frame, text="Ollama URL:").grid(row=2, column=0, sticky="w", padx=10)
    ollama_url_var = tk.StringVar(value=DEFAULT_OLLAMA_URL)
    tk.Entry(frame, textvariable=ollama_url_var, width=60).grid(row=2, column=1, padx=10, pady=4, sticky="w")

    tk.Button(
        root,
        text="Seleccionar *_ANALISIS_ABM.json y clasificar",
        height=2,
        command=run_gui
    ).pack(pady=18)

    tk.Label(
        root,
        text="Salida:\n./json_llm/02_clasificado/documentos/*_CLASIFICADO.json\n./json_llm/02_clasificado/socios/SOCIO_<uid>.json\n\n(No calcula faltantes todavía)",
        fg="gray"
    ).pack()

    root.mainloop()
