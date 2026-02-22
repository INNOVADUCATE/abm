import json
import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
IN_DEFAULT = ROOT / "json_llm" / "02_clasificado" / "documentos"
OUT_DOCS = ROOT / "json_llm" / "03_aportes" / "documentos"
OUT_SOCIOS = ROOT / "json_llm" / "03_aportes" / "socios"

# --------- Mapa: tipo_clasificado -> aporta_a (clave) + vence + prioridad ----------
APORTES_MAP = {
    "dni":                         {"key": "identidad.dni",                    "vence": False, "prioridad": "media"},
    "afip_arca":                    {"key": "fiscal.afip_arca",                 "vence": True,  "prioridad": "alta"},
    "ingresos_brutos_atm":          {"key": "fiscal.ingresos_brutos",           "vence": True,  "prioridad": "alta"},
    "titulo_habilitante":           {"key": "profesion.titulo_habilitante",     "vence": False, "prioridad": "alta"},
    "matricula_profesional":        {"key": "profesion.matricula",              "vence": True,  "prioridad": "alta"},
    "sss_prestador":                {"key": "profesion.sss",                    "vence": True,  "prioridad": "alta"},
    "seguro_mala_praxis":           {"key": "ejercicio.seguro_mala_praxis",     "vence": True,  "prioridad": "alta"},
    "habilitacion_laboratorio":     {"key": "ejercicio.habilitacion_laboratorio","vence": True, "prioridad": "alta"},
    "designacion_director_tecnico": {"key": "ejercicio.director_tecnico",       "vence": True,  "prioridad": "media"},
    "adhesion_abm":                 {"key": "adhesiones.abm",                   "vence": True,  "prioridad": "media"},
    "adhesion_osep":                {"key": "adhesiones.osep",                  "vence": True,  "prioridad": "media"},
    "adhesion_pami":                {"key": "adhesiones.pami",                  "vence": True,  "prioridad": "media"},
    "aceptacion_pago_convenios":    {"key": "adhesiones.convenios_pago",        "vence": True,  "prioridad": "media"},
    "cuenta_bancaria":              {"key": "admin.cuenta_bancaria",            "vence": False, "prioridad": "baja"},
    "otros":                        {"key": "otros",                            "vence": None,  "prioridad": "baja"},
}

# --------- Keywords por tipo (para filtrar fechas por contexto) ----------
TYPE_DATE_KEYWORDS = {
    "seguro_mala_praxis": [
        "póliza", "poliza", "asegur", "aseguradora", "cobertura",
        "responsabilidad civil", "mala praxis", "rc"
    ],
    "matricula_profesional": [
        "matrícula", "matricula", "colegio", "consejo", "habilitación", "habilitacion"
    ],
    "sss_prestador": [
        "superintendencia", "servicios de salud", "prestador", "certificado",
        "vigencia del certificado", "registro nacional de prestadores"
    ],
    "afip_arca": [
        "afip", "arca", "monotrib", "responsable inscrip", "constancia", "inscripción", "inscripcion"
    ],
    "ingresos_brutos_atm": [
        "ingresos brutos", "atm", "rentas", "arba", "jurisdicción", "jurisdiccion"
    ],
    "habilitacion_laboratorio": [
        "habilitación", "habilitacion", "laboratorio", "expediente", "sanitaria", "municipal"
    ],
    "designacion_director_tecnico": [
        "director técnico", "director tecnico", "designación", "designacion"
    ],
    "adhesion_osep": ["osep", "adhes"],
    "adhesion_pami": ["pami", "adhes"],
    "adhesion_abm": ["abm", "adhes"],
    "aceptacion_pago_convenios": ["convenio", "aceptación", "aceptacion", "pago"],
}

GENERIC_EXPIRY_WORDS = ["vigencia", "vence", "venc", "vto", "vencimiento", "hasta", "desde"]

def kw_hits(ctx: str, kws: list[str]) -> int:
    return sum(1 for k in kws if k in ctx)

def has_any(ctx: str, kws: list[str]) -> bool:
    return any(k in ctx for k in kws)


DATE_PATTERNS = [
    (re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b"), "%d/%m/%Y"),
    (re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\b"), "%d-%m-%Y"),
    (re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"), "%Y-%m-%d"),
]

def parse_date(s: str):
    for _, fmt in DATE_PATTERNS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def extract_vigencia_labels(doc_type: str, text: str):
    """
    Extrae vigencia usando etiquetas fuertes:
    - Seguro: 'vigencia desde' / 'vigencia hasta'
    - Matrícula: 'desde fecha' / 'hasta fecha'
    Devuelve (desde, hasta, conf, evidencia_list) o (None, None, None, [])
    """
    t = text.lower()
    evid = []

    def grab(label, m):
        start = max(0, m.start() - 60)
        end = min(len(t), m.end() + 60)
        evid.append({"label": label, "raw": m.group(1), "ctx": t[start:end][:220]})

    desde = None
    hasta = None

    if doc_type == "seguro_mala_praxis":
        m1 = re.search(r"vigencia\s+desde\s*[:\-]?\s*(\d{2}[/-]\d{2}[/-]\d{4})", t)
        m2 = re.search(r"vigencia\s+hasta\s*[:\-]?\s*(\d{2}[/-]\d{2}[/-]\d{4})", t)
        if m1:
            desde = parse_date(m1.group(1)); grab("vigencia_desde", m1)
        if m2:
            hasta = parse_date(m2.group(1)); grab("vigencia_hasta", m2)

    if doc_type == "matricula_profesional":
        m1 = re.search(r"desde\s+fecha\s*(\d{2}[/-]\d{2}[/-]\d{4})", t)
        m2 = re.search(r"hasta\s+fecha\s*(\d{2}[/-]\d{2}[/-]\d{4})", t)
        if m1:
            desde = parse_date(m1.group(1)); grab("desde_fecha", m1)
        if m2:
            hasta = parse_date(m2.group(1)); grab("hasta_fecha", m2)

    if not (desde or hasta):
        return None, None, None, []

    conf = "media"
    if desde and hasta:
        conf = "alta"

    return desde, hasta, conf, evid


def pick_vigencia_for_type(doc_type: str, dates_ctx: list):
    """
    Filtra fechas por keywords del tipo de documento para evitar contaminación.
    Devuelve: (vig_desde, vig_hasta, confianza, evidencia_list)
    """
    kws = TYPE_DATE_KEYWORDS.get(doc_type, [])
    if not kws:
        return None, None, None, []

    # candidatos que realmente "pertenecen" al tipo (por keywords cercanas)
    candidates = []
    for x in dates_ctx:
        ctx = x["ctx"]
        hits = kw_hits(ctx, kws)
        if hits <= 0:
            continue
        exp_hits = kw_hits(ctx, GENERIC_EXPIRY_WORDS)
        candidates.append({**x, "kw_hits": hits, "exp_hits": exp_hits})
        # SANITY CHECK: asegurar estructura
        clean = []
        for c in candidates:
            if isinstance(c, dict) and "date" in c and "ctx" in c:
                clean.append(c)

        return clean

    if not candidates:
        return None, None, None, []

    # Elegimos desde/hasta con preferencia por role_guess y luego por kw_hits
    desde_cands = [c for c in candidates if c["role_guess"] == "desde"]
    hasta_cands = [c for c in candidates if c["role_guess"] in ("hasta", "vence")]

    # si no hay roles claros, usamos las fechas extremas pero SOLO dentro de candidates
    if desde_cands:
        best_desde = sorted(desde_cands, key=lambda c: (c["date"], -c["kw_hits"], -c["exp_hits"]))[0]
        vig_desde = best_desde["date"]
    else:
        # “desde” estimado: más temprana con mayor evidencia
        best_desde = sorted(candidates, key=lambda c: (c["date"], -c["kw_hits"], -c["exp_hits"]))[0]
        vig_desde = best_desde["date"]

    if hasta_cands:
        best_hasta = sorted(hasta_cands, key=lambda c: (c["date"], c["kw_hits"], c["exp_hits"]))[-1]
        vig_hasta = best_hasta["date"]
    else:
        # “hasta” estimado: más tardía con mayor evidencia
        best_hasta = sorted(candidates, key=lambda c: (c["date"], c["kw_hits"], c["exp_hits"]))[-1]
        vig_hasta = best_hasta["date"]

    # Confianza: alta si hay 2+ keywords del tipo cerca y hay palabras de vencimiento
    conf = "baja"
    best_hits = max(c["kw_hits"] for c in candidates)
    best_exp = max(c["exp_hits"] for c in candidates)
    if best_hits >= 2 and best_exp >= 1:
        conf = "alta"
    elif best_hits >= 1 and best_exp >= 1:
        conf = "media"

    # evidencia: guardamos las 3 mejores ventanas
    evidence = sorted(candidates, key=lambda c: (-c["kw_hits"], -c["exp_hits"]))[:3]
    evidencia_list = [
        {
            "raw": e["raw"],
            "role_guess": e["role_guess"],
            "kw_hits": e["kw_hits"],
            "exp_hits": e["exp_hits"],
            "ctx": e["ctx"][:220],
        }
        for e in evidence
    ]

    return candidates


def resolve_txt_path(doc_obj: dict, doc_json_path: Path):
    p = (doc_obj.get("input") or {}).get("txt_path")
    if not p:
        return None
    p = Path(p)
    if p.exists():
        return p
    # fallback relativo
    maybe = (doc_json_path.parent / p).resolve()
    return maybe if maybe.exists() else None

def enrich_one(doc_json_path: Path):
    doc_obj = json.loads(doc_json_path.read_text(encoding="utf-8", errors="ignore"))

    socio_uid = doc_obj.get("socio_uid") or "SIN_ID"
    socio_uid_safe = re.sub(r"\W+", "_", str(socio_uid)).strip("_")[:80] or "SIN_ID"

    tipos = ((doc_obj.get("clasificacion") or {}).get("tipos") or [])
    tipos_list = []
    for t in tipos:
        tt = t.get("tipo") if isinstance(t, dict) else None
        if tt:
            tipos_list.append(tt)
    if not tipos_list:
        tipos_list = ["otros"]

    txt_path = resolve_txt_path(doc_obj, doc_json_path)
    full_text = ""
    if txt_path:
        full_text = txt_path.read_text(encoding="utf-8", errors="ignore")

    dates_ctx = []

    aportes = []
    vencimientos = {}

    for dt in tipos_list:
        meta = APORTES_MAP.get(dt, APORTES_MAP["otros"])
        key = meta["key"]
        vence = meta["vence"]
        prioridad = meta["prioridad"]

        if vence and full_text:
            vig_desde, vig_hasta, vig_conf, vig_evid = extract_vigencia_labels(dt, full_text)
        else:
            vig_desde, vig_hasta, vig_conf, vig_evid = (None, None, None, [])


        vig_desde, vig_hasta, vig_conf, vig_evid = (
            pick_vigencia_for_type(dt, dates_ctx) if vence else (None, None, None, [])
        )


        aportes.append({
            "tipo_documento": dt,
            "aporte_key": key,
            "vence": vence,
            "prioridad": prioridad,
            "vigencia": {
                "desde": vig_desde.isoformat() if vig_desde else None,
                "hasta": vig_hasta.isoformat() if vig_hasta else None,
                "fuente": "regex_ctx_tipo" if (vig_desde or vig_hasta) else None,
                "confianza": vig_conf,
                "evidencia": vig_evid,
            }

        })

        if key not in vencimientos:
            vencimientos[key] = {
                "vence": vence,
                "prioridad": prioridad,
                "desde": vig_desde.isoformat() if vig_desde else None,
                "hasta": vig_hasta.isoformat() if vig_hasta else None,
                "fuente": "regex_ctx" if (vig_desde or vig_hasta) else None,
            }

    out_doc = {
        "schema": "abm_doc_aportes_v1",
        "fecha_proceso": datetime.now().isoformat(timespec="seconds"),
        "socio_uid": socio_uid,
        "document_id": doc_obj.get("document_id"),
        "input_doc_clasificado": str(doc_json_path),
        "input_txt_path": str(txt_path) if txt_path else None,
        "clasificacion": doc_obj.get("clasificacion"),
        "campos_aportados": doc_obj.get("campos_aportados"),
        "aportes": aportes,
        "vencimientos_detectados": vencimientos,
    }

    OUT_DOCS.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DOCS / (doc_json_path.stem.replace("_CLASIFICADO", "") + "_APORTES.json")
    out_path.write_text(json.dumps(out_doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return socio_uid_safe, out_doc, out_path

def merge_socio(consol: dict, socio_uid_safe: str, out_doc: dict):
    s = consol.setdefault(socio_uid_safe, {
        "schema": "abm_socio_aportes_v1",
        "fecha_proceso": datetime.now().isoformat(timespec="seconds"),
        "socio_uid_safe": socio_uid_safe,
        "campos_consolidados": {},
        "aportes_resumen": {},
        "documentos": []
    })

    # campos consolidados (primer no nulo)
    campos = out_doc.get("campos_aportados") or {}
    cc = s["campos_consolidados"]
    for k, v in campos.items():
        if v and not cc.get(k):
            cc[k] = v

    # resumen aportes (último hasta encontrado gana si es más futuro)
    for ap in (out_doc.get("aportes") or []):
        key = ap.get("aporte_key")
        if not key:
            continue
        cur = s["aportes_resumen"].get(key)
        new_hasta = (ap.get("vigencia") or {}).get("hasta")
        if not cur:
            s["aportes_resumen"][key] = ap
        else:
            cur_hasta = (cur.get("vigencia") or {}).get("hasta")
            if new_hasta and (not cur_hasta or new_hasta > cur_hasta):
                s["aportes_resumen"][key] = ap

    s["documentos"].append({
        "document_id": out_doc.get("document_id"),
        "input_doc_clasificado": out_doc.get("input_doc_clasificado"),
        "aportes": out_doc.get("aportes"),
    })

def run_gui():
    try:
        files = filedialog.askopenfilenames(
            title="Seleccionar *_CLASIFICADO.json (Paso C)",
            filetypes=[("CLASIFICADO", "*_CLASIFICADO.json"), ("JSON", "*.json")]
        )
        if not files:
            return

        OUT_SOCIOS.mkdir(parents=True, exist_ok=True)
        consol = {}
        ok = 0

        for f in files:
            socio_uid_safe, out_doc, out_path = enrich_one(Path(f))
            merge_socio(consol, socio_uid_safe, out_doc)
            ok += 1

        socios_paths = []
        for socio_uid_safe, obj in consol.items():
            p = OUT_SOCIOS / f"SOCIO_{socio_uid_safe}.json"
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            socios_paths.append(str(p))

        messagebox.showinfo("Listo", f"Procesados: {ok}\nSalida:\n{OUT_DOCS}\n{OUT_SOCIOS}")
        os.startfile(str(OUT_SOCIOS))

    except Exception as e:
        messagebox.showerror("Error", f"{type(e).__name__}: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    root.title("ABM – Paso C.1 | Aportes + vencimientos (CLASIFICADO → APORTES)")
    root.geometry("720x260")
    root.resizable(False, False)

    tk.Button(root, text="Seleccionar *_CLASIFICADO.json y generar aportes", height=2, command=run_gui).pack(pady=30)
    tk.Label(
        root,
        text="Salida:\n./json_llm/03_aportes/documentos/*_APORTES.json\n./json_llm/03_aportes/socios/SOCIO_*.json\n\n(No calcula faltantes todavía)",
        fg="gray",
    ).pack()

    root.mainloop()
