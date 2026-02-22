"""
ABM - Extractor de campos y evidencia sobre TXT OCR
--------------------------------------------------
Lee el TXT generado por OCR y produce un JSON
*_ANALISIS_ABM.json con evidencia y detección preliminar.
"""

from __future__ import annotations

import re
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any, List, Optional


# ---------------------------
#  EVIDENCIA CRUDA
# ---------------------------

def build_evidencia_abm(texto: str) -> Dict[str, Any]:
    """
    Junta evidencia cruda para razonamiento posterior.
    NO decide presencia/ausencia.
    """
    evidencia = {}

    evidencia["dni_candidatos"] = re.findall(
        r"(?:dni|documento)\D{0,20}([\d\.J]{7,12})",
        texto,
        flags=re.IGNORECASE,
    )

    evidencia["dni_mrz"] = re.findall(r"IDARG(\d{7,8})", texto)

    evidencia["cuit_candidatos"] = re.findall(
        r"\b\d{2}-?\d{8}-?\d\b",
        texto,
    )

    evidencia["matricula_candidatos"] = re.findall(
        r"(?:matr[ií]cula|mat\.?)\D{0,15}(\d{2,10})",
        texto,
        flags=re.IGNORECASE,
    )

    evidencia["keywords"] = {
        "dni": "dni" in texto.lower() or "documento" in texto.lower(),
        "osep": "osep" in texto.lower(),
        "pami": "pami" in texto.lower(),
        "seguro": "seguro" in texto.lower(),
        "mala_praxis": "mala praxis" in texto.lower(),
    }

    return evidencia


# ---------------------------
#  MODELOS DE DATOS
# ---------------------------

@dataclass
class CamposABM:
    dni: Optional[str] = None
    cuil_cuit: Optional[str] = None
    nombre_completo: Optional[str] = None
    direccion: Optional[str] = None
    localidad: Optional[str] = None
    matricula: Optional[str] = None
    laboratorio: Optional[str] = None
    tipo_persona: Optional[str] = None  # unipersonal / sociedad / None


@dataclass
class AnalisisABM:
    document_id: str
    cliente_id: Optional[str]
    campos: CamposABM
    documentos_detectados: Dict[str, bool]
    faltantes: List[str]
    txt_path: str


# ---------------------------
#  NORMALIZACIÓN
# ---------------------------

def normalize_text(txt: str) -> str:
    txt = txt.replace("\r", "\n")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n+", "\n", txt)
    return txt


# ---------------------------
#  EXTRACCIÓN DE CAMPOS
# ---------------------------

def extract_campos_basicos(text: str) -> CamposABM:
    t = normalize_text(text).lower()

    dni = None
    m_dni = re.search(r"(dni|documento)\D{0,15}(\d{7,8})", t)
    if m_dni:
        dni = m_dni.group(2)

    cuil = None
    m_cuit = re.search(r"\b(\d{2}-\d{7,8}-\d)\b", t)
    if not m_cuit:
        m_cuit = re.search(r"\b(\d{11})\b", t)
    if m_cuit:
        cuil = m_cuit.group(1)

    matricula = None
    m_mat = re.search(r"matr[ií]cula[^\d]{0,15}(\d{3,10})", t)
    if m_mat:
        matricula = m_mat.group(1)

    direccion = None
    m_dom = re.search(r"domicilio\s*[:,]\s*(.+)", t)
    if m_dom:
        direccion = m_dom.group(1).split("\n")[0].strip()

    localidad = None
    m_loc = re.search(
        r"(maip[uú]|mendoza|godoy cruz|las heras|luj[aá]n de cuyo|guaymall[eé]n|san mart[ií]n)",
        t,
    )
    if m_loc:
        localidad = m_loc.group(1)

    nombre_completo = None
    if cuil:
        for line in t.split("\n"):
            if cuil.replace("-", "") in line.replace("-", ""):
                posible = re.sub(r"\d", "", line)
                posible = re.sub(r"cuit|cuil|nro|numero|nº", "", posible)
                posible = posible.strip(" -,:")
                if len(posible) > 3:
                    nombre_completo = posible.upper()
                    break

    tipo_persona = None
    if any(x in t for x in ["sociedad", "s.a.", "s.a.s", "s.r.l"]):
        tipo_persona = "sociedad"
    elif "monotributo" in t:
        tipo_persona = "unipersonal"

    return CamposABM(
        dni=dni,
        cuil_cuit=cuil,
        nombre_completo=nombre_completo,
        direccion=direccion,
        localidad=localidad,
        matricula=matricula,
        laboratorio=None,
        tipo_persona=tipo_persona,
    )


# ---------------------------
#  DOCUMENTOS PRELIMINARES
# ---------------------------

def detect_documentos_abm(text: str, campos: CamposABM) -> Dict[str, bool]:
    t = normalize_text(text).lower()

    def has(*words: str) -> bool:
        return all(w.lower() in t for w in words)

    return {
        "dni": bool(campos.dni) or has("dni"),
        "titulo": has("titulo") or has("título"),
        "matricula_actualizada": has("matricula"),
        "constancia_cuit": has("cuit") or has("cuil"),
        "seguro_mala_praxis": "mala praxis" in t,
        "adhesion_pami": "pami" in t,
        "adhesion_osep": "osep" in t,
    }


# ---------------------------
#  FUNCIÓN PRINCIPAL
# ---------------------------

def analizar_txt_abm(
    txt_path: Path,
    document_id: str,
    cliente_id: Optional[str],
    output_dir: Optional[Path] = None,
) -> AnalisisABM:
    """
    Analiza un TXT OCR y genera *_ANALISIS_ABM.json
    """
    txt_path = txt_path.resolve()
    if output_dir is None:
        output_dir = txt_path.parent

    raw_text = txt_path.read_text(encoding="utf-8", errors="ignore")

    campos = extract_campos_basicos(raw_text)
    docs_detectados = detect_documentos_abm(raw_text, campos)
    evidencia = build_evidencia_abm(raw_text)

    analisis = AnalisisABM(
        document_id=document_id,
        cliente_id=cliente_id,
        campos=campos,
        documentos_detectados=docs_detectados,
        faltantes=[],
        txt_path=str(txt_path),
    )

    json_path = output_dir / f"{txt_path.stem}_ANALISIS_ABM.json"

    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(
            {
                "document_id": analisis.document_id,
                "cliente_id": analisis.cliente_id,
                "campos": asdict(analisis.campos),
                "documentos_detectados_preliminar": analisis.documentos_detectados,
                "evidencia": evidencia,
                "faltantes": [],
                "txt_path": analisis.txt_path,
            },
            jf,
            ensure_ascii=False,
            indent=2,
        )

    return analisis
