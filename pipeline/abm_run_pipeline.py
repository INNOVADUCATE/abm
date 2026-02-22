import argparse
import hashlib
import json
import logging
import sqlite3
import time
import re
import sys
import os
from pathlib import Path

# Fix: Ensure script directory is in sys.path so sibling imports work
sys.path.append(str(Path(__file__).resolve().parent))

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from abm_02_clasificador_gui import OUT_DOCS as CLASIF_DOCS_DIR
from abm_02_clasificador_gui import OUT_SOCIOS as CLASIF_SOCIOS_DIR
from abm_02_clasificador_gui import classify_one, merge_socios
from abm_03_aportes_vencimientos_gui import OUT_DOCS as APORTES_DOCS_DIR
from abm_03_aportes_vencimientos_gui import OUT_SOCIOS as APORTES_SOCIOS_DIR
from abm_03_aportes_vencimientos_gui import enrich_one, merge_socio
from abm_04_estado_alertas_gui import DEFAULT_REQUISITOS, process_socio_file
from abm_extractor_campos import analizar_txt_abm
from abm_paddle_core import StructuredLogger, compute_document_id, process_pdf_with_paddle_llm


ROOT = Path(__file__).resolve().parent

DEFAULT_UI_SOCIOS_DIR = (ROOT / ".." / ".." / "CODIGO_FRANCO" / "ui_ux" / "data" / "socios").resolve()
OUT_OCR_DIR = ROOT / "json_llm" / "00_ocr"
OUT_EVIDENCIA_DIR = ROOT / "json_llm" / "01_evidencia"
OUT_ESTADO_SOCIOS_DIR = ROOT / "json_llm" / "04_estado" / "socios"
OUT_ESTADO_RESUMEN = ROOT / "json_llm" / "04_estado" / "resumen.json"
OUT_PERFILES_DIR = ROOT / "json_llm" / "05_perfiles_socios"
OUT_PERFILES_PATCHES_DIR = ROOT / "json_llm" / "05_perfiles_socios_patches"
PROCESS_LOG_DIR = ROOT / "json_llm" / "logs"
PROCESS_LOG_PATH = PROCESS_LOG_DIR / "process_log.json"
OCR_PROCESS_LOG_PATH = PROCESS_LOG_DIR / "ocr_process_log.json"
STATE_DB_PATH = PROCESS_LOG_DIR / "processed_files.sqlite"

logger = logging.getLogger(__name__)


def atomic_json_write(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(f"{path}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        logger.exception("Error en escritura atómica JSON: %s", path)
        raise


@dataclass
class LogMessage:
    nivel: str
    codigo: str
    mensaje_humano: str
    detalle_tecnico: Optional[str] = None
    documento_tipo: Optional[str] = None


@dataclass
class ProcessLogItem:
    fecha_proceso: str
    document_id: str
    socio_uid: str
    etapa: str
    estado: str
    file_path: Optional[str] = None
    mensajes: List[LogMessage] = field(default_factory=list)


class ProcessLogger:
    def __init__(self, output_path: Path = PROCESS_LOG_PATH) -> None:
        self.items: List[ProcessLogItem] = []
        self.output_path = output_path

    def add(
        self,
        document_id: str,
        socio_uid: str,
        etapa: str,
        estado: str,
        mensaje_humano: str,
        codigo: str = "INFO",
        detalle_tecnico: Optional[str] = None,
        documento_tipo: Optional[str] = None,
        file_path: Optional[str] = None,
    ) -> None:
        item = ProcessLogItem(
            fecha_proceso=datetime.now().isoformat(timespec="seconds"),
            document_id=document_id,
            socio_uid=socio_uid,
            etapa=etapa,
            estado=estado,
            file_path=file_path,
            mensajes=[
                LogMessage(
                    nivel=estado,
                    codigo=codigo,
                    mensaje_humano=mensaje_humano,
                    detalle_tecnico=detalle_tecnico,
                    documento_tipo=documento_tipo,
                )
            ],
        )
        self.items.append(item)

    def write(self, resumen: Dict[str, Any]) -> None:
        PROCESS_LOG_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema": "abm_process_log_v1",
            "fecha_proceso": datetime.now().isoformat(timespec="seconds"),
            "resumen": resumen,
            "items": [self._item_to_dict(item) for item in self.items],
        }
        atomic_json_write(self.output_path, payload)

    @staticmethod
    def _item_to_dict(item: ProcessLogItem) -> Dict[str, Any]:
        return {
            "fecha_proceso": item.fecha_proceso,
            "document_id": item.document_id,
            "socio_uid": item.socio_uid,
            "etapa": item.etapa,
            "estado": item.estado,
            "file_path": item.file_path,
            "mensajes": [
                {
                    "nivel": msg.nivel,
                    "codigo": msg.codigo,
                    "mensaje_humano": msg.mensaje_humano,
                    "detalle_tecnico": msg.detalle_tecnico,
                    "documento_tipo": msg.documento_tipo,
                }
                for msg in item.mensajes
            ],
        }


def discover_pdfs(ui_socios_dir: Path) -> List[Path]:
    if not ui_socios_dir.exists():
        return []
    return sorted(ui_socios_dir.rglob("*.pdf"))


def infer_socio_uid(ui_socios_dir: Path, pdf_path: Path) -> str:
    try:
        rel = pdf_path.relative_to(ui_socios_dir)
        if rel.parts:
            return rel.parts[0]
    except ValueError:
        pass
    return pdf_path.parent.name


def ensure_dirs(ocr_output_dir: Path) -> None:
    for d in [
        ocr_output_dir,
        OUT_EVIDENCIA_DIR,
        CLASIF_DOCS_DIR,
        CLASIF_SOCIOS_DIR,
        APORTES_DOCS_DIR,
        APORTES_SOCIOS_DIR,
        OUT_ESTADO_SOCIOS_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _only_digits(value: Any) -> str:
    return re.sub(r"\D", "", _clean_str(value))


def _looks_like_hash(value: Any) -> bool:
    candidate = _clean_str(value)
    if not candidate or " " in candidate:
        return False
    return bool(re.fullmatch(r"[A-Fa-f0-9]{16,64}", candidate))




def _safe_file_stem(value: Any) -> str:
    cleaned = _clean_str(value)
    if not cleaned:
        return "socio"
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned)
    return safe.strip("._") or "socio"

def _parse_iso_date(date_value: Any) -> Optional[date]:
    if date_value is None:
        return None
    raw = _clean_str(date_value)
    if not raw:
        return None

    for candidate in (raw, raw.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate).date()
        except ValueError:
            continue
    return None


def _is_invalid_display_name(value: Any) -> bool:
    display_name = _clean_str(value)
    if not display_name:
        return True
    upper_name = display_name.upper()
    if upper_name == "SIN NOMBRE":
        return True
    if display_name.startswith("#"):
        return True
    if len(display_name) > 15 and " " not in display_name:
        return True
    return False


def _load_ocr_text_for_profile(ocr_output_dir: Path, profile: Dict[str, Any]) -> Optional[str]:
    if not ocr_output_dir.exists():
        return None

    ids = profile.get("ids") or {}
    candidates = [
        _clean_str(profile.get("socio_uid")),
        _clean_str(ids.get("dni")),
        _clean_str(ids.get("cuil_cuit")),
        _clean_str(ids.get("cliente_id")),
    ]
    candidates = [c for c in candidates if c]
    if not candidates:
        return None

    txt_files = sorted(ocr_output_dir.glob("*.txt"))
    for candidate in candidates:
        normalized = re.sub(r"[^a-zA-Z0-9]", "", candidate).lower()
        if not normalized:
            continue
        for txt_path in txt_files:
            stem = re.sub(r"[^a-zA-Z0-9]", "", txt_path.stem).lower()
            if normalized and normalized in stem:
                try:
                    return txt_path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue

    return None


def _build_profile_patch(profile: Dict[str, Any], identity: Dict[str, Any]) -> Dict[str, Any]:
    socio_uid = _clean_str(profile.get("socio_uid"))
    return {
        "schema": "abm_socio_profile_patch_v1",
        "socio_uid": socio_uid,
        "display_name": _clean_str(identity.get("display_name")),
        "ids": {
            "dni": _clean_str(identity.get("dni")),
            "cuil_cuit": _clean_str(identity.get("cuil_cuit")),
        },
        "confidence": float(identity.get("confidence") or 0.0),
        "evidence": identity.get("evidence") or {"spans": []},
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


def _write_profile_patch(patch: Dict[str, Any], patches_dir: Path) -> Path:
    patches_dir.mkdir(parents=True, exist_ok=True)
    socio_uid = _clean_str(patch.get("socio_uid"))
    file_key = _safe_file_stem(socio_uid)
    out_path = patches_dir / f"{file_key}.json"
    atomic_json_write(out_path, patch)
    return out_path


def build_socio_profile(socio_json: dict, now: date) -> dict:
    socio = socio_json.get("socio") or {}
    identity = socio_json.get("identity") or socio.get("identity") or {}
    cliente = socio_json.get("cliente") or socio.get("cliente") or {}
    campos = socio_json.get("campos_consolidados") or socio.get("campos_consolidados") or {}

    nombre_completo = _clean_str(
        campos.get("nombre_completo")
        or cliente.get("nombre_completo")
        or cliente.get("nombre")
    )
    nombre = _clean_str(campos.get("nombre"))
    apellido = _clean_str(campos.get("apellido"))
    apellido_nombre = " ".join(part for part in [apellido, nombre] if part).strip()

    dni = _only_digits(cliente.get("dni") or campos.get("dni") or identity.get("dni"))
    cuil_cuit = _only_digits(campos.get("cuil_cuit") or identity.get("cuil_cuit"))
    cliente_id = _clean_str(cliente.get("cliente_id"))
    document_id = _clean_str(
        socio_json.get("document_id")
        or socio.get("document_id")
        or socio_json.get("uid")
        or socio.get("uid")
        or socio_json.get("socio_uid")
        or socio_json.get("socio_uid_safe")
        or socio.get("socio_uid")
    )

    ids = {
        "dni": dni,
        "cuil_cuit": cuil_cuit,
        "cliente_id": cliente_id,
    }

    if len(cuil_cuit) == 11:
        socio_uid = cuil_cuit
    elif len(dni) in (7, 8):
        socio_uid = dni
    elif cliente_id:
        socio_uid = cliente_id
    else:
        socio_uid = document_id

    if nombre_completo:
        display_name = nombre_completo
    elif apellido_nombre:
        display_name = apellido_nombre
    elif cliente_id:
        display_name = cliente_id
    elif dni:
        display_name = f"DNI {dni}"
    else:
        display_name = document_id

    items = socio_json.get("items") or []
    documentos: List[Dict[str, Any]] = []
    faltantes: List[Dict[str, Any]] = []
    vencidos: List[Dict[str, Any]] = []
    proximos_30: List[Dict[str, Any]] = []

    for item in items:
        key = _clean_str(item.get("key"))
        label = _clean_str(item.get("label"))
        estado = _clean_str(item.get("estado"))
        vigencia_hasta_raw = (item.get("vigencia") or {}).get("hasta")
        vigencia_hasta_date = _parse_iso_date(vigencia_hasta_raw)
        vigencia_hasta = vigencia_hasta_date.isoformat() if vigencia_hasta_date else None

        doc = {
            "key": key,
            "label": label,
            "estado": estado,
            "vigencia_hasta": vigencia_hasta,
        }
        documentos.append(doc)

        mini_doc = {"key": key, "label": label}
        if estado == "FALTANTE":
            faltantes.append(mini_doc)

        if vigencia_hasta_date is not None:
            if vigencia_hasta_date < now:
                vencidos.append(mini_doc)
            elif now <= vigencia_hasta_date <= now + timedelta(days=30):
                proximos_30.append(mini_doc)

    estado_general = _clean_str(socio_json.get("estado_general") or socio_json.get("estado"))
    notes: List[str] = []
    if not (nombre_completo or apellido_nombre):
        notes.append("No se encontró nombre consolidado para el socio.")
    if not (len(cuil_cuit) == 11 or len(dni) in (7, 8) or cliente_id):
        notes.append("No se encontró identificador principal (CUIL/CUIT, cliente_id o DNI).")
    if _looks_like_hash(display_name):
        notes.append("Advertencia: display_name quedó con apariencia de hash.")

    return {
        "schema": "abm_socio_profile_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "socio_uid": socio_uid,
        "display_name": display_name,
        "ids": ids,
        "estado_general": estado_general,
        "documentos": documentos,
        "faltantes": faltantes,
        "vencidos": vencidos,
        "proximos_30": proximos_30,
        "notes": notes,
    }


def write_socio_profile(profile: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    socio_uid = _clean_str(profile.get("socio_uid"))
    file_key = _safe_file_stem(socio_uid)
    out_path = out_dir / f"{file_key}.json"
    atomic_json_write(out_path, profile)


def write_txt(profile: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    socio_uid = _clean_str(profile.get("socio_uid"))
    file_key = _safe_file_stem(socio_uid)

    faltantes_count = len(profile.get("faltantes") or [])
    vencidos_count = len(profile.get("vencidos") or [])
    proximos_count = len(profile.get("proximos_30") or [])
    estado_general = _clean_str(profile.get("estado_general"))
    estado_line = estado_general if estado_general else "SIN_ESTADO"

    lines = [
        f"{_clean_str(profile.get('display_name'))}",
        f"UID: {file_key}",
        f"Estado general: {estado_line}",
        f"Faltantes: {faltantes_count} · Vencidos: {vencidos_count} · Próximos 30 días: {proximos_count}",
    ]
    out_txt_path = out_dir / f"{file_key}.txt"
    out_txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def rebuild_profiles_from_estado(
    process_logger: Optional[ProcessLogger] = None,
    source_dir: Path = OUT_ESTADO_SOCIOS_DIR,
    out_dir: Path = OUT_PERFILES_DIR,
) -> Dict[str, int]:
    stats = {"ok": 0, "warning": 0, "error": 0}

    for estado_path in sorted(source_dir.glob("*.json")):
        try:
            socio_json = json.loads(estado_path.read_text(encoding="utf-8"))
            profile = build_socio_profile(socio_json, datetime.now().date())
            if not _clean_str(profile.get("socio_uid")):
                profile["socio_uid"] = estado_path.stem

            notes = profile.get("notes") or []
            has_name_warning = any("nombre consolidado" in str(note).lower() for note in notes)
            has_hash_warning = _looks_like_hash(profile.get("display_name"))
            if not _clean_str(profile.get("display_name")):
                profile["display_name"] = _clean_str(profile.get("socio_uid")) or "SIN_NOMBRE"
                has_name_warning = True

            write_socio_profile(profile, out_dir)
            write_txt(profile, out_dir)

            socio_uid = _clean_str(profile.get("socio_uid")) or estado_path.stem
            if has_name_warning or has_hash_warning:
                stats["warning"] += 1
                if process_logger:
                    warning_msg = "Perfil regenerado con fallback por falta de nombre consolidado."
                    warning_code = "PROFILE_REBUILD_WARNING"
                    if has_hash_warning:
                        warning_msg = "Perfil regenerado con display_name con apariencia de hash."
                        warning_code = "PROFILE_REBUILD_HASH_DISPLAY_NAME"
                    process_logger.add(
                        document_id=estado_path.stem,
                        socio_uid=socio_uid,
                        etapa="profile_rebuild",
                        estado="warning",
                        mensaje_humano=warning_msg,
                        codigo=warning_code,
                        file_path=str(estado_path),
                    )
            else:
                stats["ok"] += 1
                if process_logger:
                    process_logger.add(
                        document_id=estado_path.stem,
                        socio_uid=socio_uid,
                        etapa="profile_rebuild",
                        estado="ok",
                        mensaje_humano="Perfil regenerado desde 04_estado correctamente.",
                        codigo="PROFILE_REBUILD_OK",
                        file_path=str(estado_path),
                    )
        except Exception as exc:
            stats["error"] += 1
            if process_logger:
                process_logger.add(
                    document_id=estado_path.stem,
                    socio_uid=estado_path.stem,
                    etapa="profile_rebuild",
                    estado="error",
                    mensaje_humano="No se pudo regenerar el perfil del socio desde 04_estado.",
                    codigo="PROFILE_REBUILD_ERROR",
                    detalle_tecnico=str(exc),
                    file_path=str(estado_path),
                )

    return stats

def init_state_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path TEXT PRIMARY KEY,
            size INTEGER,
            mtime REAL,
            sha1 TEXT,
            status TEXT,
            last_stage TEXT,
            last_processed_at TEXT,
            error_msg TEXT,
            duplicate_of TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_processed_files_sha1 ON processed_files(sha1)")
    return conn


def compute_sha1(file_path: Path) -> str:
    sha1 = hashlib.sha1()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha1.update(chunk)
    return sha1.hexdigest()


def evaluate_file_state(
    conn: sqlite3.Connection, file_path: Path
) -> Dict[str, Optional[str]]:
    stat = file_path.stat()
    size = stat.st_size
    mtime = stat.st_mtime

    row = conn.execute(
        "SELECT size, mtime, sha1 FROM processed_files WHERE file_path = ?",
        (str(file_path),),
    ).fetchone()

    sha1 = compute_sha1(file_path)
    if row and row[0] == size and row[1] == mtime and row[2] == sha1:
        return {
            "skip_reason": "unchanged",
            "sha1": sha1,
            "duplicate_of": None,
            "size": size,
            "mtime": mtime,
        }

    duplicate_row = conn.execute(
        "SELECT file_path FROM processed_files WHERE sha1 = ? AND file_path != ? LIMIT 1",
        (sha1, str(file_path)),
    ).fetchone()

    if duplicate_row:
        return {
            "skip_reason": "duplicate",
            "sha1": sha1,
            "duplicate_of": duplicate_row[0],
            "size": size,
            "mtime": mtime,
        }

    return {
        "skip_reason": None,
        "sha1": sha1,
        "duplicate_of": None,
        "size": size,
        "mtime": mtime,
    }


def upsert_state(
    conn: sqlite3.Connection,
    file_path: Path,
    size: int,
    mtime: float,
    sha1: str,
    status: str,
    last_stage: str,
    error_msg: Optional[str] = None,
    duplicate_of: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO processed_files (
            file_path, size, mtime, sha1, status, last_stage, last_processed_at, error_msg, duplicate_of
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET
            size = excluded.size,
            mtime = excluded.mtime,
            sha1 = excluded.sha1,
            status = excluded.status,
            last_stage = excluded.last_stage,
            last_processed_at = excluded.last_processed_at,
            error_msg = excluded.error_msg,
            duplicate_of = excluded.duplicate_of
        """,
        (
            str(file_path),
            size,
            mtime,
            sha1,
            status,
            last_stage,
            datetime.now().isoformat(timespec="seconds"),
            error_msg,
            duplicate_of,
        ),
    )
    conn.commit()


def update_progress_metrics(
    resumen: Dict[str, Any],
    start_time: float,
    processed_index: int,
    total_detected: int,
    worked_docs: int,
) -> None:
    elapsed_sec = max(0.0, time.time() - start_time)
    avg_sec_per_doc = elapsed_sec / worked_docs if worked_docs else None
    remaining = max(total_detected - processed_index, 0)
    eta_sec = avg_sec_per_doc * remaining if avg_sec_per_doc is not None else None
    resumen["processed_index"] = processed_index
    resumen["total_detected"] = total_detected
    resumen["worked_docs"] = worked_docs
    resumen["elapsed_sec"] = round(elapsed_sec, 2)
    resumen["avg_sec_per_doc"] = round(avg_sec_per_doc, 2) if avg_sec_per_doc is not None else None
    resumen["eta_sec"] = round(eta_sec, 2) if eta_sec is not None else None


def run_pipeline(
    ui_socios_dir: Path,
    dias_prox: int,
    ocr_output_dir: Path,
    processed_db_path: Path,
    force_reprocess: bool = False,
    langextract_fallback: bool = False,
    profiles_patches_dir: Path = OUT_PERFILES_PATCHES_DIR,
) -> None:
    ensure_dirs(ocr_output_dir)
    process_logger = ProcessLogger()
    logger = StructuredLogger(session_name="abm_run_pipeline")
    state_conn = init_state_db(processed_db_path)

    pdfs = discover_pdfs(ui_socios_dir)
    resumen = {
        "pdfs_detectados": len(pdfs),
        "documentos_ok": 0,
        "documentos_warning": 0,
        "documentos_error": 0,
        "documentos_skipped": 0,
        "documentos_duplicate": 0,
        "documentos_procesados": 0,
        "socios_clasificados": 0,
        "socios_aportes": 0,
        "socios_estado": 0,
    }

    consol_clasif: Dict[str, Dict[str, Any]] = {}
    consol_aportes: Dict[str, Dict[str, Any]] = {}

    start_time = time.time()
    total_detected = len(pdfs)

    # Escribimos métricas cada 3 documentos para dar feedback sin spamear el log.
    write_every = 3
    processed_index = 0
    worked_docs = 0

    try:
        for pdf_path in pdfs:
            processed_index += 1
            socio_uid = infer_socio_uid(ui_socios_dir, pdf_path)
            document_id = compute_document_id(pdf_path)
            file_label = pdf_path.name
            file_path = str(pdf_path)
            state_info = evaluate_file_state(state_conn, pdf_path)
            if force_reprocess:
                state_info["skip_reason"] = None
            if state_info["skip_reason"] == "unchanged":
                resumen["documentos_skipped"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="skipped",
                    mensaje_humano=f"Documento sin cambios, se omite OCR. ({file_label})",
                    codigo="SKIPPED_UNCHANGED",
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "skipped",
                    "pipeline",
                )
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="profile_rebuild",
                    estado="ok",
                    mensaje_humano=f"Documento saltado por DELTA; se regenerará perfil desde 04_estado. ({file_label})",
                    codigo="PROFILE_REBUILD_DEFERRED",
                    file_path=file_path,
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue
            if state_info["skip_reason"] == "duplicate":
                resumen["documentos_duplicate"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="skipped",
                    mensaje_humano=f"Documento duplicado detectado, se omite OCR. ({file_label})",
                    codigo="SKIPPED_DUPLICATE",
                    detalle_tecnico=state_info["duplicate_of"],
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "skipped",
                    "pipeline",
                    duplicate_of=state_info["duplicate_of"],
                )
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="profile_rebuild",
                    estado="ok",
                    mensaje_humano=f"Documento duplicado (DELTA); se regenerará perfil desde 04_estado. ({file_label})",
                    codigo="PROFILE_REBUILD_DEFERRED",
                    file_path=file_path,
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue

            try:
                ocr_res = process_pdf_with_paddle_llm(
                    pdf_path=pdf_path,
                    logger=logger,
                    output_dir=ocr_output_dir,
                    temp_dir=ROOT / "temp_proceso",
                    raw_ocr_dir=ROOT / "raw_ocr",
                    lang="es",
                )
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="ok",
                    mensaje_humano=f"OCR completado correctamente. ({file_label})",
                    codigo="OCR_OK",
                    file_path=file_path,
                )
                worked_docs += 1
                resumen["documentos_procesados"] += 1
            except Exception as exc:
                resumen["documentos_error"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="error",
                    mensaje_humano=f"No se pudo completar el OCR del documento. ({file_label})",
                    codigo="OCR_ERROR",
                    detalle_tecnico=str(exc),
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "error",
                    "ocr",
                    error_msg=str(exc),
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue

            try:
                txt_path = Path(ocr_res["txt_output"])
                analizar_txt_abm(
                    txt_path=txt_path,
                    document_id=document_id,
                    cliente_id=socio_uid,
                    output_dir=OUT_EVIDENCIA_DIR,
                )
                analysis_path = OUT_EVIDENCIA_DIR / f"{txt_path.stem}_ANALISIS_ABM.json"
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="evidencia",
                    estado="ok",
                    mensaje_humano=f"Evidencia generada. ({file_label})",
                    codigo="EVIDENCIA_OK",
                    file_path=file_path,
                )
            except Exception as exc:
                resumen["documentos_error"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="evidencia",
                    estado="error",
                    mensaje_humano=f"No se pudo analizar el texto OCR. ({file_label})",
                    codigo="EVIDENCIA_ERROR",
                    detalle_tecnico=str(exc),
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "error",
                    "evidencia",
                    error_msg=str(exc),
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue

            try:
                socio_uid_safe, doc_obj, _ = classify_one(
                    analysis_path=analysis_path,
                    use_llm=False,
                    model="",
                    ollama_url="",
                )
                merge_socios(consol_clasif, socio_uid_safe, doc_obj)
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="clasificacion",
                    estado="ok",
                    mensaje_humano=f"Documento clasificado. ({file_label})",
                    codigo="CLASIFICACION_OK",
                    documento_tipo=(doc_obj.get("clasificacion", {}).get("tipos") or [{}])[0].get("tipo"),
                    file_path=file_path,
                )
                resumen["documentos_ok"] += 1
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "ok",
                    "pipeline",
                )
            except Exception as exc:
                resumen["documentos_warning"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="clasificacion",
                    estado="warning",
                    mensaje_humano=f"No se pudo clasificar el documento con heurísticas. ({file_label})",
                    codigo="CLASIFICACION_WARNING",
                    detalle_tecnico=str(exc),
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "warning",
                    "clasificacion",
                    error_msg=str(exc),
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue

            update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
            if processed_index % write_every == 0 or processed_index == total_detected:
                process_logger.write(resumen)

        for socio_uid_safe, obj in consol_clasif.items():
            socio_path = CLASIF_SOCIOS_DIR / f"SOCIO_{socio_uid_safe}.json"
            socio_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            resumen["socios_clasificados"] += 1

        for doc_path in CLASIF_DOCS_DIR.glob("*_CLASIFICADO.json"):
            try:
                socio_uid_safe, out_doc, _ = enrich_one(doc_path)
                merge_socio(consol_aportes, socio_uid_safe, out_doc)
            except Exception:
                continue

        for socio_uid_safe, obj in consol_aportes.items():
            socio_path = APORTES_SOCIOS_DIR / f"SOCIO_{socio_uid_safe}.json"
            socio_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            resumen["socios_aportes"] += 1

        resumen_estado = {
            "schema": "abm_resumen_estado_v1",
            "fecha_proceso": datetime.now().isoformat(timespec="seconds"),
            "hoy": datetime.now().date().isoformat(),
            "dias_proximos_vencimientos": dias_prox,
            "socios": [],
        }

        for socio_path in APORTES_SOCIOS_DIR.glob("SOCIO_*.json"):
            try:
                socio_uid_safe, out_estado = process_socio_file(socio_path, DEFAULT_REQUISITOS, dias_prox)
                out_path = OUT_ESTADO_SOCIOS_DIR / f"SOCIO_{socio_uid_safe}_ESTADO.json"
                atomic_json_write(out_path, out_estado)
                resumen_estado["socios"].append(out_path.name)
                resumen["socios_estado"] += 1
            except Exception:
                continue

        atomic_json_write(OUT_ESTADO_RESUMEN, resumen_estado)

        rebuild_profiles_from_estado(process_logger=process_logger, source_dir=OUT_ESTADO_SOCIOS_DIR, out_dir=OUT_PERFILES_DIR)
        langextract_identity = None
        if langextract_fallback:
            try:
                from tools.langextract_identity import extract_identity as langextract_identity  # type: ignore
            except Exception as exc:
                langextract_identity = None
                process_logger.add(
                    document_id="pipeline",
                    socio_uid="-",
                    etapa="profile_enrich",
                    estado="warning",
                    mensaje_humano="Fallback de LangExtract no disponible; se continúa sin parches.",
                    codigo="PROFILE_ENRICH_WARNING",
                    detalle_tecnico=str(exc),
                )

        for estado_path in OUT_ESTADO_SOCIOS_DIR.glob("*.json"):
            try:
                socio_json = json.loads(estado_path.read_text(encoding="utf-8"))
                profile = build_socio_profile(socio_json, datetime.now().date())
                if not _clean_str(profile.get("socio_uid")):
                    profile["socio_uid"] = estado_path.stem
                if not _clean_str(profile.get("display_name")):
                    profile["display_name"] = _clean_str(profile.get("socio_uid"))

                if _looks_like_hash(profile.get("display_name")):
                    process_logger.add(
                        document_id=_clean_str(profile.get("socio_uid")) or estado_path.stem,
                        socio_uid=_clean_str(profile.get("socio_uid")) or estado_path.stem,
                        etapa="profile_enrich",
                        estado="warning",
                        mensaje_humano="El display_name del perfil mantiene apariencia de hash.",
                        codigo="PROFILE_HASH_DISPLAY_NAME",
                        file_path=str(estado_path),
                    )

                if langextract_fallback and _is_invalid_display_name(profile.get("display_name")):
                    socio_uid = _clean_str(profile.get("socio_uid")) or estado_path.stem
                    if langextract_identity is None:
                        process_logger.add(
                            document_id="profile_patch",
                            socio_uid=socio_uid,
                            etapa="profile_enrich",
                            estado="warning",
                            mensaje_humano="Se omitió fallback de identidad: herramienta no disponible.",
                            codigo="PROFILE_ENRICH_WARNING",
                            file_path=str(estado_path),
                        )
                    else:
                        ocr_text = _load_ocr_text_for_profile(ocr_output_dir, profile)
                        if not ocr_text:
                            process_logger.add(
                                document_id="profile_patch",
                                socio_uid=socio_uid,
                                etapa="profile_enrich",
                                estado="warning",
                                mensaje_humano="Se omitió fallback de identidad por falta de OCR TXT para el socio.",
                                codigo="PROFILE_ENRICH_NO_OCR",
                                file_path=str(estado_path),
                            )
                        else:
                            try:
                                identity = langextract_identity(ocr_text)
                                patched_display_name = _clean_str(identity.get("display_name"))
                                if patched_display_name:
                                    patch = _build_profile_patch(profile, identity)
                                    patch_path = _write_profile_patch(patch, profiles_patches_dir)
                                    process_logger.add(
                                        document_id="profile_patch",
                                        socio_uid=socio_uid,
                                        etapa="profile_enrich",
                                        estado="ok",
                                        mensaje_humano="Se aplicó patch de identidad con fallback LangExtract.",
                                        codigo="PROFILE_ENRICH_OK",
                                        file_path=str(patch_path),
                                    )
                                else:
                                    process_logger.add(
                                        document_id="profile_patch",
                                        socio_uid=socio_uid,
                                        etapa="profile_enrich",
                                        estado="warning",
                                        mensaje_humano="Fallback de identidad ejecutado sin nombre utilizable; no se creó patch.",
                                        codigo="PROFILE_ENRICH_EMPTY",
                                        file_path=str(estado_path),
                                    )
                            except Exception as exc:
                                process_logger.add(
                                    document_id="profile_patch",
                                    socio_uid=socio_uid,
                                    etapa="profile_enrich",
                                    estado="error",
                                    mensaje_humano="Falló fallback de identidad; se continúa sin patch.",
                                    codigo="PROFILE_ENRICH_ERROR",
                                    detalle_tecnico=str(exc),
                                    file_path=str(estado_path),
                                )

                write_socio_profile(profile, OUT_PERFILES_DIR)
                write_txt(profile, OUT_PERFILES_DIR)
            except Exception:
                continue
    finally:
        process_logger.write(resumen)
        state_conn.close()


def run_rebuild_profiles_only(dias_prox: int = 30) -> None:
    process_logger = ProcessLogger()
    resumen = {
        "mode": "rebuild_profiles_only",
        "dias_proximos_vencimientos": dias_prox,
        "socios_perfiles_ok": 0,
        "socios_perfiles_warning": 0,
        "socios_perfiles_error": 0,
    }

    try:
        stats = rebuild_profiles_from_estado(process_logger=process_logger, source_dir=OUT_ESTADO_SOCIOS_DIR, out_dir=OUT_PERFILES_DIR)
        resumen["socios_perfiles_ok"] = stats["ok"]
        resumen["socios_perfiles_warning"] = stats["warning"]
        resumen["socios_perfiles_error"] = stats["error"]
    finally:
        process_logger.write(resumen)


def run_ocr_only(
    ui_socios_dir: Path,
    ocr_output_dir: Path,
    processed_db_path: Path,
    force_reprocess: bool = False,
) -> None:
    ensure_dirs(ocr_output_dir)
    process_logger = ProcessLogger(output_path=OCR_PROCESS_LOG_PATH)
    logger = StructuredLogger(session_name="abm_ocr_only")
    state_conn = init_state_db(processed_db_path)

    pdfs = discover_pdfs(ui_socios_dir)
    start_time = time.time()
    total_detected = len(pdfs)
    processed_index = 0
    worked_docs = 0
    write_every = 3
    resumen = {
        "pdfs_detectados": len(pdfs),
        "documentos_ok": 0,
        "documentos_error": 0,
        "documentos_skipped": 0,
        "documentos_duplicate": 0,
        "documentos_procesados": 0,
    }

    try:
        for pdf_path in pdfs:
            processed_index += 1
            socio_uid = infer_socio_uid(ui_socios_dir, pdf_path)
            document_id = compute_document_id(pdf_path)
            file_label = pdf_path.name
            file_path = str(pdf_path)
            state_info = evaluate_file_state(state_conn, pdf_path)
            if force_reprocess:
                state_info["skip_reason"] = None

            if state_info["skip_reason"] == "unchanged":
                resumen["documentos_skipped"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="skipped",
                    mensaje_humano=f"Documento sin cambios, se omite OCR. ({file_label})",
                    codigo="SKIPPED_UNCHANGED",
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "skipped",
                    "ocr",
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue

            if state_info["skip_reason"] == "duplicate":
                resumen["documentos_duplicate"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="skipped",
                    mensaje_humano=f"Documento duplicado detectado, se omite OCR. ({file_label})",
                    codigo="SKIPPED_DUPLICATE",
                    detalle_tecnico=state_info["duplicate_of"],
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "skipped",
                    "ocr",
                    duplicate_of=state_info["duplicate_of"],
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue

            try:
                process_pdf_with_paddle_llm(
                    pdf_path=pdf_path,
                    logger=logger,
                    output_dir=ocr_output_dir,
                    temp_dir=ROOT / "temp_proceso",
                    raw_ocr_dir=ROOT / "raw_ocr",
                    lang="es",
                )
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="ok",
                    mensaje_humano=f"OCR completado correctamente. ({file_label})",
                    codigo="OCR_OK",
                    file_path=file_path,
                )
                worked_docs += 1
                resumen["documentos_procesados"] += 1
                resumen["documentos_ok"] += 1
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "ok",
                    "ocr",
                )
            except Exception as exc:
                resumen["documentos_error"] += 1
                process_logger.add(
                    document_id=document_id,
                    socio_uid=socio_uid,
                    etapa="ocr",
                    estado="error",
                    mensaje_humano=f"No se pudo completar el OCR del documento. ({file_label})",
                    codigo="OCR_ERROR",
                    detalle_tecnico=str(exc),
                    file_path=file_path,
                )
                upsert_state(
                    state_conn,
                    pdf_path,
                    state_info["size"],
                    state_info["mtime"],
                    state_info["sha1"],
                    "error",
                    "ocr",
                    error_msg=str(exc),
                )
                update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
                if processed_index % write_every == 0 or processed_index == total_detected:
                    process_logger.write(resumen)
                continue

            update_progress_metrics(resumen, start_time, processed_index, total_detected, worked_docs)
            if processed_index % write_every == 0 or processed_index == total_detected:
                process_logger.write(resumen)
    finally:
        process_logger.write(resumen)
        state_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ABM pipeline orquestado para UI Franco.")
    parser.add_argument(
        "--ui-socios-dir",
        default=str(DEFAULT_UI_SOCIOS_DIR),
        help="Ruta a ui_ux/data/socios",
    )
    parser.add_argument(
        "--dias-prox",
        type=int,
        default=30,
        help="Días para considerar vencimientos próximos.",
    )
    parser.add_argument(
        "--ocr-only",
        action="store_true",
        help="Ejecuta solo OCR (sin pipeline completo).",
    )
    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Reprocesa PDFs aunque estén marcados como ya procesados.",
    )
    parser.add_argument(
        "--ocr-output-dir",
        default=str(OUT_OCR_DIR),
        help="Directorio de salida para OCR (txt/json).",
    )
    parser.add_argument(
        "--processed-db",
        default=str(STATE_DB_PATH),
        help="Base SQLite para estado de archivos procesados.",
    )
    parser.add_argument(
        "--rebuild-profiles-only",
        action="store_true",
        help="Regenera únicamente 05_perfiles_socios desde 04_estado/socios.",
    )
    parser.add_argument(
        "--langextract-fallback",
        action="store_true",
        help="Activa fallback opcional de identidad para display_name inválido.",
    )
    parser.add_argument(
        "--profiles-patches-dir",
        default=str(OUT_PERFILES_PATCHES_DIR),
        help="Directorio para patches de perfiles enriquecidos.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ui_socios_dir = Path(args.ui_socios_dir).expanduser()
    ocr_output_dir = Path(args.ocr_output_dir).expanduser()
    processed_db_path = Path(args.processed_db).expanduser()
    if args.rebuild_profiles_only:
        run_rebuild_profiles_only(args.dias_prox)

    profiles_patches_dir = Path(args.profiles_patches_dir).expanduser()
    if args.ocr_only:
        run_ocr_only(ui_socios_dir, ocr_output_dir, processed_db_path, args.force_reprocess)
    else:
        run_pipeline(
            ui_socios_dir,
            args.dias_prox,
            ocr_output_dir,
            processed_db_path,
            args.force_reprocess,
            args.langextract_fallback,
            profiles_patches_dir,
        )


if __name__ == "__main__":
    main()

# Ejemplos (no ejecutar automáticamente):
# - Pipeline completo:
#   python abm_run_pipeline.py --ui-socios-dir /ruta/a/pdfs
# - OCR-only:
#   python abm_run_pipeline.py --ui-socios-dir /ruta/a/pdfs --ocr-only
# - Cambiar salida OCR y DB de estado:
#   python abm_run_pipeline.py --ui-socios-dir /ruta/a/pdfs --ocr-output-dir /ruta/ocr --processed-db /ruta/processed_files.sqlite
