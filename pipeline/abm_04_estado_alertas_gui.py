import json
import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
from datetime import date, datetime

ROOT = Path(__file__).resolve().parent
IN_DEFAULT = ROOT / "json_llm" / "03_aportes" / "socios"
OUT_SOCIOS = ROOT / "json_llm" / "04_estado" / "socios"
OUT_SOCIOS_HUMANO = ROOT / "json_llm" / "04_estado" / "socios_humano"
OUT_RESUMEN = ROOT / "json_llm" / "04_estado" / "resumen.json"

# ==========================
# Requisitos (default)
# - key debe coincidir con aporte_key (ej: ejercicio.seguro_mala_praxis)
# - bloquea=True: si falta o está vencido -> BLOQUEA
# ==========================
DEFAULT_REQUISITOS = [
    # 1) OBLIGATORIOS BLOQUEANTES
    {"key": "identidad.dni", "label": "DNI", "tipo": "obligatorio", "vence": False, "prioridad": "media", "bloquea": True},
    {"key": "fiscal.afip_arca", "label": "Constancia AFIP/ARCA", "tipo": "obligatorio", "vence": True, "prioridad": "alta", "bloquea": True},
    {"key": "fiscal.ingresos_brutos", "label": "Ingresos Brutos (ATM/ARBA)", "tipo": "obligatorio", "vence": True, "prioridad": "alta", "bloquea": True},
    {"key": "profesion.titulo_habilitante", "label": "Título habilitante", "tipo": "obligatorio", "vence": False, "prioridad": "alta", "bloquea": True},
    {"key": "profesion.matricula", "label": "Matrícula profesional", "tipo": "obligatorio", "vence": True, "prioridad": "alta", "bloquea": True},
    {"key": "profesion.sss", "label": "Inscripción SSS (prestador)", "tipo": "obligatorio", "vence": True, "prioridad": "alta", "bloquea": True},
    {"key": "ejercicio.seguro_mala_praxis", "label": "Seguro de mala praxis", "tipo": "obligatorio", "vence": True, "prioridad": "alta", "bloquea": True},
    {"key": "ejercicio.habilitacion_laboratorio", "label": "Habilitación de laboratorio (si corresponde)", "tipo": "obligatorio", "vence": True, "prioridad": "alta", "bloquea": False},
    {"key": "ejercicio.director_tecnico", "label": "Designación Director Técnico (si corresponde)", "tipo": "obligatorio", "vence": True, "prioridad": "media", "bloquea": False},

    # 2) CONDICIONALES (no bloquean por defecto)
    {"key": "adhesiones.abm", "label": "Adhesión ABM", "tipo": "condicional", "vence": True, "prioridad": "media", "bloquea": False},
    {"key": "adhesiones.osep", "label": "Adhesión OSEP", "tipo": "condicional", "vence": True, "prioridad": "media", "bloquea": False},
    {"key": "adhesiones.pami", "label": "Adhesión PAMI", "tipo": "condicional", "vence": True, "prioridad": "media", "bloquea": False},
    {"key": "adhesiones.convenios_pago", "label": "Aceptación de pago / convenios", "tipo": "condicional", "vence": True, "prioridad": "media", "bloquea": False},

    # 3) DATOS ÚTILES (no bloquean)
    {"key": "admin.cuenta_bancaria", "label": "Cuenta bancaria (CBU/Alias)", "tipo": "dato", "vence": False, "prioridad": "baja", "bloquea": False},
]

# Si existe un archivo abm_requisitos.json en la raíz, lo usa (para que lo adaptes sin tocar código)
REQ_FILE = ROOT / "abm_requisitos.json"


# ----------------- utils -----------------
def safe_uid(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\W+", "_", s).strip("_")
    return s[:80] if s else "SIN_ID"

def parse_iso_date(s: str):
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None

def today_local() -> date:
    return date.today()

def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")

def normalize_text(value: str) -> str:
    if not value:
        return ""
    return " ".join(str(value).split()).strip()

def short_uid(value: str) -> str:
    if not value:
        return ""
    return str(value).strip()[:8]

def build_display_name(nombre: str, apellido: str, dni: str, cuil: str, socio_uid_safe: str) -> str:
    nombre_completo = normalize_text(" ".join([nombre or "", apellido or ""]).strip())
    if nombre_completo:
        return nombre_completo

    if dni:
        return f"Socio · DNI {dni}"
    if cuil:
        return f"Socio · CUIL {cuil}"
    suffix = short_uid(socio_uid_safe)
    return f"Socio · {suffix}" if suffix else "Socio"

def guess_identity_source(socio: dict) -> str:
    for doc in socio.get("documentos") or []:
        for key in ("file_path", "input_doc_clasificado", "analysis_json", "txt_path"):
            path_value = doc.get(key)
            if path_value:
                return os.path.basename(str(path_value))
    return None

def build_identity(socio: dict, socio_uid_safe: str) -> dict:
    cliente = socio.get("cliente") or {}
    campos = socio.get("campos_consolidados") or {}
    nombre = normalize_text(cliente.get("nombre") or cliente.get("razon_social") or campos.get("nombre") or "")
    apellido = normalize_text(campos.get("apellido") or "")
    dni = normalize_text(campos.get("dni") or cliente.get("dni") or "") or None
    cuil = normalize_text(campos.get("cuil_cuit") or cliente.get("cliente_id") or "") or None
    display_name = build_display_name(nombre, apellido, dni, cuil, socio_uid_safe)
    return {
        "display_name": display_name,
        "dni": dni,
        "cuil_cuit": cuil,
        "source": guess_identity_source(socio),
        "confidence": None,
    }

def build_identity_keys(socio: dict, socio_uid_safe: str, identity: dict) -> list:
    cliente = socio.get("cliente") or {}
    campos = socio.get("campos_consolidados") or {}
    keys = [
        socio.get("socio_uid"),
        socio.get("socio_uid_safe"),
        socio_uid_safe,
        socio.get("socio_id"),
        socio.get("cliente_id"),
        cliente.get("cliente_id"),
        cliente.get("dni"),
        campos.get("dni"),
        campos.get("cuil_cuit"),
        identity.get("dni"),
        identity.get("cuil_cuit"),
    ]
    documentos = socio.get("documentos") or []
    for doc in documentos:
        keys.append(doc.get("document_id"))
        keys.append(doc.get("file_path"))
        keys.append(doc.get("input_doc_clasificado"))
    seen = set()
    output = []
    for key in keys:
        if not key:
            continue
        key_str = str(key)
        if key_str in seen:
            continue
        seen.add(key_str)
        output.append(key_str)
    return output

def build_humano_summary(out: dict, socio: dict, dias_prox: int) -> str:
    identity = out.get("identity") or {}
    display_name = identity.get("display_name") or "Socio"
    dni = identity.get("dni") or "-"
    cuil = identity.get("cuil_cuit") or "-"
    estado = out.get("estado_general") or out.get("estado_global") or "SIN_ESTADO"
    hoy = parse_iso_date(out.get("hoy")) or today_local()

    pendientes = [it for it in (out.get("items") or []) if it.get("estado") in ("FALTANTE", "ERROR")]
    vencidos = []
    proximos = []
    for it in out.get("items") or []:
        vig = it.get("vigencia") or {}
        hasta_raw = vig.get("hasta")
        hasta = parse_iso_date(hasta_raw)
        if not hasta:
            continue
        days_left = (hasta - hoy).days
        entry = f"- {it.get('label') or it.get('key')}: {hasta_raw}"
        if days_left < 0:
            vencidos.append(entry)
        elif days_left <= dias_prox:
            proximos.append(entry)

    documentos = []
    for doc in socio.get("documentos") or []:
        file_path = doc.get("file_path") or doc.get("input_doc_clasificado") or doc.get("analysis_json") or doc.get("txt_path")
        basename = os.path.basename(file_path) if file_path else None
        document_id = doc.get("document_id")
        if basename and document_id:
            documentos.append(f"- {basename} (document_id: {document_id})")
        elif basename:
            documentos.append(f"- {basename}")
        elif document_id:
            documentos.append(f"- document_id: {document_id}")

    lines = [
        f"SOCIO: {display_name}",
        "",
        "IDs:",
        f"- socio_uid: {out.get('socio_uid_safe') or '-'}",
        f"- DNI: {dni}",
        f"- CUIL/CUIT: {cuil}",
        "",
        f"Estado general: {estado}",
        "",
        "Pendientes:",
    ]
    if pendientes:
        lines.extend([f"- {it.get('label') or it.get('key')} ({it.get('estado')})" for it in pendientes])
    else:
        lines.append("- Sin pendientes.")

    lines.append("")
    lines.append("Vencimientos vencidos:")
    lines.extend(vencidos or ["- Sin vencimientos vencidos."])
    lines.append("")
    lines.append("Vencimientos próximos:")
    lines.extend(proximos or ["- Sin vencimientos próximos."])
    lines.append("")
    lines.append("Documentos vistos:")
    lines.extend(documentos or ["- Sin documentos registrados."])
    lines.append("")
    return "\n".join(lines)

def first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                return cleaned
        else:
            return value
    return None


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def normalize_id_value(value: str) -> str:
    if value is None:
        return None
    return str(value).strip()


def is_cuil(value: str) -> bool:
    if not value:
        return False
    digits = re.sub(r"\D", "", str(value))
    return len(digits) == 11


def shorten_uid(value: str, length: int = 8) -> str:
    if not value:
        return "SIN_ID"
    return str(value)[:length]


def build_identity(socio: dict, socio_uid_safe: str) -> dict:
    cliente = socio.get("cliente") or {}
    campos = socio.get("campos_consolidados") or {}

    nombre = first_non_empty(cliente.get("nombre"), campos.get("nombre"))
    apellido = first_non_empty(cliente.get("apellido"), campos.get("apellido"))

    base_name = first_non_empty(nombre, apellido)
    if base_name:
        name_parts = []
        if nombre:
            name_parts.append(nombre)
        if apellido:
            name_parts.append(apellido)
        base_name = normalize_whitespace(" ".join(name_parts))

    dni = normalize_id_value(first_non_empty(campos.get("dni"), cliente.get("dni")))
    cuil_cuit = normalize_id_value(first_non_empty(campos.get("cuil_cuit")))
    if not cuil_cuit:
        cliente_id = normalize_id_value(cliente.get("cliente_id"))
        if cliente_id and is_cuil(cliente_id):
            cuil_cuit = cliente_id

    display_name = None
    name_display = base_name.upper() if base_name else None
    id_label = None
    id_value = None
    if dni:
        id_label = "DNI"
        id_value = dni
    elif cuil_cuit:
        id_label = "CUIL"
        id_value = cuil_cuit

    if name_display and id_value:
        display_name = f"{name_display} · {id_label} {id_value}"
    elif name_display:
        display_name = name_display
    elif id_value:
        display_name = f"Socio · {id_label} {id_value}"
    else:
        display_name = f"Socio · {shorten_uid(socio_uid_safe)}"

    return {
        "display_name": display_name,
        "dni": dni,
        "cuil_cuit": cuil_cuit,
        "source": None,
        "confidence": None,
    }


def build_identity_keys(socio: dict, socio_uid_safe: str) -> list:
    cliente = socio.get("cliente") or {}
    campos = socio.get("campos_consolidados") or {}

    keys = []

    def add_key(key: str, value):
        if value is None:
            return
        if isinstance(value, str) and not value.strip():
            return
        keys.append({"key": key, "value": value})

    add_key("socio_uid", socio.get("socio_uid"))
    add_key("socio_uid_safe", socio_uid_safe)
    add_key("dni", campos.get("dni"))
    add_key("dni_cliente", cliente.get("dni"))
    add_key("cuil_cuit", campos.get("cuil_cuit"))
    add_key("cliente_id", cliente.get("cliente_id"))
    add_key("cuil_cuit_cliente", cliente.get("cuil_cuit"))

    return keys


def build_human_summary(out: dict, socio: dict) -> str:
    identity = out.get("identity") or {}
    display_name = identity.get("display_name") or "Socio"
    socio_uid = socio.get("socio_uid") or out.get("socio_uid_safe")
    dni = identity.get("dni")
    cuil_cuit = identity.get("cuil_cuit")

    lines = [
        f"SOCIO: {display_name}",
        "",
        "IDs:",
        f"- socio_uid: {socio_uid or 'N/A'}",
        f"- dni: {dni or 'N/A'}",
        f"- cuil: {cuil_cuit or 'N/A'}",
        "",
        f"Estado general: {out.get('estado_global')}",
        "",
        "Pendientes:",
    ]

    pendientes = [it for it in out.get("items", []) if it.get("estado") in ("FALTANTE", "ERROR")]
    if pendientes:
        for it in pendientes:
            label = it.get("label") or it.get("key")
            lines.append(f"- {label} ({it.get('estado')})")
    else:
        lines.append("- Sin pendientes")

    lines.extend(["", "Vencimientos:"])
    vencimientos = []
    for it in out.get("items", []):
        vig = it.get("vigencia") or {}
        hasta = vig.get("hasta")
        if hasta:
            label = it.get("label") or it.get("key")
            vencimientos.append(f"- {label}: {hasta}")
    if vencimientos:
        lines.extend(vencimientos)
    else:
        lines.append("- Sin vencimientos informados")

    lines.extend(["", "Documentos vistos:"])
    documentos = socio.get("documentos") or []
    if documentos:
        for doc in documentos:
            file_path = doc.get("file_path") or doc.get("input_doc_clasificado")
            file_name = Path(file_path).name if file_path else None
            doc_id = doc.get("document_id")
            if file_name and doc_id:
                lines.append(f"- {file_name} ({doc_id})")
            elif file_name:
                lines.append(f"- {file_name}")
            elif doc_id:
                lines.append(f"- {doc_id}")
    else:
        lines.append("- Sin documentos registrados")

    return "\n".join(lines) + "\n"


def load_requisitos():
    if REQ_FILE.exists():
        try:
            data = json.loads(REQ_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass
    return DEFAULT_REQUISITOS


def evaluar_item(req, aporte_obj, hoy: date, dias_prox: int):
    """
    Devuelve dict con estado del requisito.
    Estados: OK | FALTANTE | VENCIDO | PROXIMO | REVISAR
    """
    key = req["key"]
    vence = bool(req.get("vence"))
    bloquea = bool(req.get("bloquea"))
    prioridad = req.get("prioridad", "media")
    label = req.get("label", key)
    tipo = req.get("tipo", "obligatorio")

    if aporte_obj is None:
        return {
            "key": key, "label": label, "tipo": tipo,
            "estado": "FALTANTE",
            "vence": vence, "bloquea": bloquea, "prioridad": prioridad,
            "vigencia": None,
            "detalle": "No hay aporte registrado para este requisito."
        }

    # Si no vence, con que esté presente ya está OK
    vig = (aporte_obj.get("vigencia") or {})
    desde = parse_iso_date(vig.get("desde"))
    hasta = parse_iso_date(vig.get("hasta"))
    conf = vig.get("confianza")
    fuente = vig.get("fuente")

    if not vence:
        return {
            "key": key, "label": label, "tipo": tipo,
            "estado": "OK",
            "vence": False, "bloquea": bloquea, "prioridad": prioridad,
            "vigencia": {"desde": vig.get("desde"), "hasta": vig.get("hasta"), "confianza": conf, "fuente": fuente},
            "detalle": "Presente (no vence)."
        }

    # Vence pero no hay fecha -> REVISAR
    if hasta is None:
        return {
            "key": key, "label": label, "tipo": tipo,
            "estado": "REVISAR",
            "vence": True, "bloquea": bloquea, "prioridad": prioridad,
            "vigencia": {"desde": vig.get("desde"), "hasta": vig.get("hasta"), "confianza": conf, "fuente": fuente},
            "detalle": "Presente, pero no se detectó fecha de vencimiento (revisar)."
        }

    # Evaluar vencido / próximo
    days_left = (hasta - hoy).days

    if days_left < 0:
        return {
            "key": key, "label": label, "tipo": tipo,
            "estado": "VENCIDO",
            "vence": True, "bloquea": bloquea, "prioridad": prioridad,
            "vigencia": {"desde": vig.get("desde"), "hasta": vig.get("hasta"), "confianza": conf, "fuente": fuente},
            "detalle": f"Vencido hace {-days_left} día(s)."
        }

    if days_left <= dias_prox:
        return {
            "key": key, "label": label, "tipo": tipo,
            "estado": "PROXIMO",
            "vence": True, "bloquea": bloquea, "prioridad": prioridad,
            "vigencia": {"desde": vig.get("desde"), "hasta": vig.get("hasta"), "confianza": conf, "fuente": fuente},
            "detalle": f"Vence en {days_left} día(s)."
        }

    return {
        "key": key, "label": label, "tipo": tipo,
        "estado": "OK",
        "vence": True, "bloquea": bloquea, "prioridad": prioridad,
        "vigencia": {"desde": vig.get("desde"), "hasta": vig.get("hasta"), "confianza": conf, "fuente": fuente},
        "detalle": f"Vigente. Vence en {days_left} día(s)."
    }


def determinar_estado_global(items_eval):
    """
    Reglas:
    - Si hay VENCIDO bloqueante o FALTANTE bloqueante -> BLOQUEA
    - Si hay PROXIMO (alta) o REVISAR (alta) -> REVISAR
    - Si hay cualquier FALTANTE/REVISAR/PROXIMO no bloqueante -> REVISAR
    - Si todo OK -> OK
    """
    def is_block(e):
        return e.get("bloquea") and e.get("estado") in ("FALTANTE", "VENCIDO")

    if any(is_block(e) for e in items_eval):
        return "BLOQUEA"

    if any(e.get("estado") in ("PROXIMO", "REVISAR", "FALTANTE") for e in items_eval):
        return "REVISAR"

    return "OK"


def build_notificaciones(items_eval):
    notas = []
    for e in items_eval:
        st = e["estado"]
        if st == "OK":
            continue
        sev = "baja"
        if e.get("prioridad") == "alta":
            sev = "alta"
        elif e.get("prioridad") == "media":
            sev = "media"

        notas.append({
            "severidad": sev,
            "estado": st,
            "key": e["key"],
            "label": e["label"],
            "detalle": e.get("detalle")
        })
    # ordenar: alta primero, luego estado, etc.
    order = {"alta": 0, "media": 1, "baja": 2}
    notas.sort(key=lambda x: (order.get(x["severidad"], 9), x["estado"], x["label"]))
    return notas


def process_socio_file(p: Path, requisitos, dias_prox: int):
    socio = json.loads(p.read_text(encoding="utf-8", errors="ignore"))

    socio_uid_safe = socio.get("socio_uid_safe") or safe_uid(p.stem.replace("SOCIO_", ""))
    hoy = today_local()

    aportes_resumen = socio.get("aportes_resumen") or {}
    # aportes_resumen puede ser dict {key: aporte_obj}
    # o a veces venir como lista; lo normalizamos
    if isinstance(aportes_resumen, list):
        tmp = {}
        for it in aportes_resumen:
            k = it.get("aporte_key")
            if k:
                tmp[k] = it
        aportes_resumen = tmp

    items_eval = []
    for req in requisitos:
        key = req["key"]
        aporte_obj = aportes_resumen.get(key)
        items_eval.append(evaluar_item(req, aporte_obj, hoy, dias_prox))

    estado_global = determinar_estado_global(items_eval)
    notificaciones = build_notificaciones(items_eval)
    def identity_safe(s, uid):
        # Helper to avoid crash if build_identity implementation is buggy
        try: return build_identity(s, uid)
        except: return {}
        
    identity = identity_safe(socio, socio_uid_safe)
    identity_keys = build_identity_keys(socio, socio_uid_safe)

    out = {
        "schema": "abm_socio_estado_v1",
        "fecha_proceso": now_ts(),
        "hoy": hoy.isoformat(),
        "socio_uid_safe": socio_uid_safe,
        "campos_consolidados": socio.get("campos_consolidados") or {},
        "cliente": socio.get("cliente") or {},
        "estado_global": estado_global,
        "identity": identity,
        "identity_keys": identity_keys,
        "items": items_eval,
        "notificaciones": notificaciones,
        "input": {
            "socio_aportes_json": str(p)
        }
    }
    return socio_uid_safe, out, socio


def run_gui():
    try:
        dias_prox = int(dias_var.get().strip() or "30")

        files = filedialog.askopenfilenames(
            title="Seleccionar SOCIO_*.json (desde json_llm/03_aportes/socios)",
            initialdir=str(IN_DEFAULT) if IN_DEFAULT.exists() else str(ROOT),
            filetypes=[("SOCIO aportes", "SOCIO_*.json"), ("JSON", "*.json")]
        )
        if not files:
            return

        requisitos = load_requisitos()

        OUT_SOCIOS.mkdir(parents=True, exist_ok=True)
        OUT_SOCIOS_HUMANO.mkdir(parents=True, exist_ok=True)
        resumen = {
            "schema": "abm_resumen_estado_v1",
            "fecha_proceso": now_ts(),
            "hoy": today_local().isoformat(),
            "dias_proximos_vencimientos": dias_prox,
            "totales": {
                "socios_procesados": 0,
                "estado_OK": 0,
                "estado_REVISAR": 0,
                "estado_BLOQUEA": 0,
                "items_faltantes": 0,
                "items_vencidos": 0,
                "items_proximos": 0,
                "items_revisar": 0,
            }
        }

        for f in files:
            socio_uid_safe, out, socio = process_socio_file(Path(f), requisitos, dias_prox)

            # contadores
            resumen["totales"]["socios_procesados"] += 1
            eg = out["estado_global"]
            if eg == "OK":
                resumen["totales"]["estado_OK"] += 1
            elif eg == "REVISAR":
                resumen["totales"]["estado_REVISAR"] += 1
            else:
                resumen["totales"]["estado_BLOQUEA"] += 1

            for it in out["items"]:
                st = it["estado"]
                if st == "FALTANTE":
                    resumen["totales"]["items_faltantes"] += 1
                elif st == "VENCIDO":
                    resumen["totales"]["items_vencidos"] += 1
                elif st == "PROXIMO":
                    resumen["totales"]["items_proximos"] += 1
                elif st == "REVISAR":
                    resumen["totales"]["items_revisar"] += 1

            out_path = OUT_SOCIOS / f"SOCIO_{socio_uid_safe}_ESTADO.json"
            out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

            summary_path = OUT_SOCIOS_HUMANO / f"{socio_uid_safe}.md"
            summary_path.write_text(build_human_summary(out, socio), encoding="utf-8")

        OUT_RESUMEN.parent.mkdir(parents=True, exist_ok=True)
        OUT_RESUMEN.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")

        messagebox.showinfo("Listo", f"Procesados: {resumen['totales']['socios_procesados']}\nSalida:\n{OUT_SOCIOS}\nResumen:\n{OUT_RESUMEN}")
        os.startfile(str(OUT_SOCIOS))

    except Exception as e:
        messagebox.showerror("Error", f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    root = tk.Tk()
    root.title("ABM – Paso D | Estado + alertas (SOCIO aportes → ESTADO)")
    root.geometry("720x320")
    root.resizable(False, False)

    frame = tk.Frame(root)
    frame.pack(pady=16)

    tk.Label(frame, text="Ventana de “próximos vencimientos” (días):").grid(row=0, column=0, sticky="w", padx=10, pady=6)
    dias_var = tk.StringVar(value="30")
    tk.Entry(frame, textvariable=dias_var, width=10).grid(row=0, column=1, sticky="w", padx=10, pady=6)

    tk.Button(root, text="Seleccionar SOCIO_*.json (Paso C.1) y generar estado", height=2, command=run_gui).pack(pady=18)

    tk.Label(
        root,
        text="Entrada esperada:\n./json_llm/03_aportes/socios/SOCIO_*.json\n\nSalida:\n./json_llm/04_estado/socios/SOCIO_<uid>_ESTADO.json\n./json_llm/04_estado/resumen.json",
        fg="gray"
    ).pack()

    root.mainloop()
