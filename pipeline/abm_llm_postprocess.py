"""
ABM – Postproceso LLM con Gemma3 (Ollama)
-----------------------------------------

Pipeline de análisis inteligente sobre TXT OCR:
- Extrae entidades (nombres, DNI, CUIT, mails, teléfonos, etc.).
- Llena campos de cliente usando esas entidades.
- Marca documentos presentes con estados útiles (DETECTADO, VENCE..., REVISAR, NO DETECTADO).
- Extrae fechas de vencimiento cuando las encuentra.
- Genera bloque de resumen para dashboard.

Modos de uso:

1) MODO MENÚ (sin argumentos):
   python abm_llm_postprocess.py

   - Lista los .txt de BASE_DIR/salida_final
   - Elegís uno por número
   - Genera el JSON en BASE_DIR/json_llm

2) MODO DIRECTO:
   python abm_llm_postprocess.py "RUTA_AL_TXT" --model gemma3:4b

Requisitos:
   - Ollama instalado y en PATH
   - Modelo descargado, por ejemplo:
       ollama pull gemma3:4b
"""

import os
import sys
import json
import time
import hashlib
import traceback
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

# ============================================================
# CONFIGURACIÓN BÁSICA
# ============================================================

BASE_DIR = Path(os.getcwd())

# Carpeta donde se guardarán los JSON finales de análisis LLM
ANALISIS_LLM_DIR = BASE_DIR / "json_llm"
ANALISIS_LLM_DIR.mkdir(parents=True, exist_ok=True)

# Carpeta donde esperan los TXT LLM-ready
SALIDA_FINAL_DIR = BASE_DIR / "salida_final"

# Modelo Ollama por defecto (podés cambiar a gemma3:1b si querés)
OLLAMA_MODEL = "gemma3:4b"

# Límite de caracteres de texto OCR que mandamos al modelo
MAX_OCR_CHARS = 20000


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
    metadata: Optional[Dict[str, Any]] = None


class StructuredLogger:
    def __init__(self, session_name: str = "llm_postprocess"):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = BASE_DIR / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.log_dir / f"{session_name}_{ts}.jsonl"
        self.txt_path = self.log_dir / f"{session_name}_{ts}.log"

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
            timestamp=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            nivel=nivel,
            evento=evento,
            modulo=modulo,
            mensaje=mensaje,
            archivo=archivo,
            estado=estado,
            metadata=metadata or {},
        )
        # JSONL
        with open(self.jsonl_path, "a", encoding="utf-8") as jf:
            jf.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")

        # Texto legible
        line = f"[{datetime.now().strftime('%H:%M:%S')}] [{nivel}] [{modulo}] {mensaje}"
        with open(self.txt_path, "a", encoding="utf-8") as tf:
            tf.write(line + "\n")

        print(line)


# ============================================================
# UTILIDADES GENERALES
# ============================================================

def compute_document_id(any_path: Path) -> str:
    """
    ID determinista basado en la ruta (puede ser TXT o PDF).
    """
    return hashlib.sha1(str(any_path).encode("utf-8")).hexdigest()[:16]


def normalize_name_for_filename(name: str) -> str:
    """
    Normaliza un nombre para usarlo como nombre de archivo:
    - Mayúsculas
    - Reemplaza espacios por "_"
    - Quita caracteres raros
    """
    import re
    if not name:
        return "UNKNOWN"

    # Quitar saltos de línea y espacios múltiples
    name = " ".join(str(name).strip().split())

    # Pasar a mayúsculas
    name = name.upper()

    # Reemplazar acentos básicos (rápido y sucio, suficiente para filenames)
    replacements = {
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
        "Ñ": "N",
    }
    for k, v in replacements.items():
        name = name.replace(k, v)

    # Reemplazar cualquier cosa que no sea letra, número o espacio por espacio
    name = re.sub(r"[^A-Z0-9 ]+", " ", name)

    # Compactar espacios y cambiar por _
    name = "_".join(name.split())

    # Evitar cadena vacía
    return name or "UNKNOWN"
# ============================================================
# CONSTRUCCIÓN DEL PROMPT PARA GEMMA
# ============================================================

def build_llm_prompt(
    ocr_text: str,
    pdf_name: Optional[str],
    document_id: str,
    cliente_id: Optional[str] = None,
) -> str:
    """
    Construye el prompt para Gemma3 (1B o 4B) pensado para legajos ABM.

    - Menos rígido, pero más inteligente.
    - Extrae entidades (dni, cuit, mails, teléfonos, etc.).
    - Detecta documentos presentes / faltantes.
    - Intenta identificar vencimientos con fecha + estado.
    - Usa "REVISAR: ..." cuando hay dudas, en vez de dejar todo en null.
    """

    # Limitamos texto para que Gemma no se ahogue
    if len(ocr_text) > MAX_OCR_CHARS:
        ocr_text = ocr_text[:MAX_OCR_CHARS] + "\n\n[TRUNCADO_POR_LONGITUD]\n"

    schema_desc = r"""
Vas a analizar TEXTO OCR (posiblemente con errores) de documentación de la
Asociación Bioquímica de Mendoza (ABM). Puede contener:

- DNI
- constancia de CUIT / AFIP / Monotributo
- matrícula profesional
- habilitación de laboratorio
- seguro de mala praxis
- constancias ATM / ARCA / ingresos brutos
- adhesiones a OSEP / PAMI / otras obras sociales
- notas, formularios, bonos, etc.

Debes devolver EXCLUSIVAMENTE un único objeto JSON con esta estructura EXACTA
(en este ORDEN de claves a nivel raíz):

{
  "metadata": {
    "document_id": string,
    "pdf_nombre": string,
    "pdf_ruta": string,
    "cliente_id": string | null,
    "fecha_proceso": string (ISO 8601),
    "modelo": string,
    "modo_inferencia": string,
    "nombre_archivo_recomendado": string
  },
  "entities": {
    "names": string[],
    "dni": string[],
    "cuil_cuit": string[],
    "emails": string[],
    "phone_numbers": string[],
    "institution_names": string[],
    "street_addresses": string[],
    "city_towns": string[],
    "departments": string[],
    "postal_codes": string[],
    "professional_matrics": string[],
    "insurance_providers": string[],
    "dates": string[]
  },
  "documents": [
    {
      "type": string,
      "fields": string[]
    }
  ],
  "signatures": string[],
  "cliente": {
    "nombre_completo": string | null,
    "dni": string | null,
    "cuil_cuit": string | null,
    "direccion": string | null,
    "localidad": string | null,
    "matricula": string | null,
    "laboratorio": string | null,
    "tipo_persona": "FISICA" | "JURIDICA" | null
  },
  "documentos_presentes": {
    "dni": bool,
    "titulo": bool,
    "matricula_actualizada": bool,
    "constancia_cuit": bool,
    "atm_ingresos_brutos": bool,
    "seguro_mala_praxis": bool,
    "inscripcion_super_intendencia_salud": bool,
    "habilitacion_laboratorio": bool,
    "adhesion_abm": bool,
    "adhesion_pami": bool,
    "adhesion_osep": bool,
    "aceptacion_pago": bool,
    "estatuto_social": bool,
    "contrato_social": bool,
    "constancia_arca": bool,
    "cuenta_bancaria": bool,
    "nota_designacion_director_tecnico": bool
  },
  "vencimientos": {
    "dni": {
      "fecha": string | null,
      "estado": "VIGENTE" | "VENCIDO" | "FALTANTE" | "REVISAR" | null
    },
    "matricula": {
      "fecha": string | null,
      "estado": "VIGENTE" | "VENCIDO" | "FALTANTE" | "REVISAR" | null
    },
    "mala_praxis": {
      "fecha": string | null,
      "estado": "VIGENTE" | "VENCIDO" | "FALTANTE" | "REVISAR" | null
    },
    "ingresos_brutos": {
      "fecha": string | null,
      "estado": "VIGENTE" | "VENCIDO" | "FALTANTE" | "REVISAR" | null
    },
    "monotributo": {
      "fecha": string | null,
      "estado": "VIGENTE" | "VENCIDO" | "FALTANTE" | "REVISAR" | null
    },
    "arca": {
      "fecha": string | null,
      "estado": "VIGENTE" | "VENCIDO" | "FALTANTE" | "REVISAR" | null
    },
    "prestador_sss": {
      "fecha": string | null,
      "estado": "VIGENTE" | "VENCIDO" | "FALTANTE" | "REVISAR" | null
    }
  },
  "faltantes": string[],
  "observaciones": string[],
  "otros_datos": {
    "telefonos": string[],
    "emails": string[],
    "horarios_atencion": string[],
    "contactos": string[],
    "otros": string[]
  },
  "resumen": string
}

REGLAS GENERALES IMPORTANTES:

1) Si el OCR casi no tiene texto útil:
   - Devolver todas las claves igual, pero con la mayoría de valores en null,
     false, [] o "REVISAR: sin información suficiente".
   - "resumen" debe explicar que no se encontró información relevante.
   Si detectás variaciones del nombre, normalizá al formato más coherente
   corrigiendo errores de OCR (ej: "Aoa" → "Ana", "Mara" → "María").
   Si hay múltiples nombres, elegir el más completo o más frecuente.

2) Limpieza e interpretación del OCR:
   - Unir palabras partidas (ej: "Bio­quí­mica" -> "Bioquímica").
   - Quitar caracteres raros y duplicados.
   - Normalizar espacios.
   - Respetar tildes y texto en español en UTF-8 limpio.

3) Extracción de ENTIDADES (bloque "entities"):
   - "names": nombres de personas o profesionales detectados.
   - "dni": sólo valores que, al quitar puntos/espacios, sean 7–9 dígitos y NO contengan letras.
   - "cuil_cuit": cadenas tipo XX-XXXXXXXX-X o variantes razonables.
   - "emails": cualquier texto tipo correo@dominio.
   - "phone_numbers": teléfonos con o sin guiones.
   - "institution_names": nombres de laboratorios, instituciones, clínicas, etc.
   - "professional_matrics": matrículas numéricas coherentes (no inventar).
   - No inventes datos: sólo usa lo que aparezca en el OCR, corrigiendo errores simples.

4) Cliente (bloque "cliente"):
   - "nombre_completo": persona principal del legajo.
     - Si hay múltiples personas, elegir la que tenga más señales claras (DNI, matrícula, firma).
     - Si sólo aparece el nombre del laboratorio y no de persona, usar el laboratorio
       y tipo_persona = "JURIDICA".
   - "tipo_persona":
       - "FISICA" si es un/a profesional (Bioq., Dra., Dr., etc.).
       - "JURIDICA" si es S.A., SRL, laboratorio como sociedad, etc.
       - null si no se puede saber.
   - Si un dato no puede leerse pero hay indicios, usar:
       "REVISAR: dato borroso" en vez de null.

5) Documentos presentes (bloque "documentos_presentes"):
   Marcar true / false usando estas pistas (soft, no rígido):

   - dni:
       true si aparece DNI del profesional o copia de DNI.
   - titulo:
       true si se menciona "título", "diploma", "grado universitario" o similar.
   - matricula_actualizada:
       true si hay matrícula profesional con fecha o constancia reciente.
   - constancia_cuit:
       true si aparece AFIP, CUIT, constancia de inscripción, monotributo, etc.
   - atm_ingresos_brutos:
       true si aparece "ATM", "ingresos brutos", "ARBA" u organismo tributario similar.
   - seguro_mala_praxis:
       true si aparece póliza, RC profesional, seguro de responsabilidad profesional, etc.
   - inscripcion_super_intendencia_salud:
       true si aparece "Superintendencia de Servicios de Salud".
   - habilitacion_laboratorio:
       true si hay habilitación de laboratorio, resolución sanitaria, ministerio de salud, etc.
   - adhesion_abm:
       true si hay formulario o constancia de la Asociación Bioquímica de Mendoza.
   - adhesion_pami:
       true si aparece PAMI como convenio o prestador.
   - adhesion_osep:
       true si aparecen formularios OSEP, registros o constancias OSEP.
   - aceptacion_pago:
       true si hay nota o formulario de aceptación de condiciones de pago.
   - estatuto_social / contrato_social:
       true si aparecen términos de sociedad, estatuto, acta constitutiva, contrato social.
   - constancia_arca:
       true si aparece "ARCA", organismo recaudador provincial relacionado.
   - cuenta_bancaria:
       true si aparecen CBU, alias, número de cuenta, banco, etc.
   - nota_designacion_director_tecnico:
       true si hay nota donde se designa director técnico o responsable técnico.

6) Vencimientos (bloque "vencimientos"):
   - Formato de fechas: interpretar SIEMPRE como formato argentino DD/MM/AAAA.
   - Si falta el año, usar: "REVISAR: fecha sin año".
   - "estado" debe ser:
       - "VIGENTE" si la fecha es posterior a la fecha de emisión del documento o claramente actual.
       - "VENCIDO" si la fecha es claramente pasada.
       - "FALTANTE" si el documento está marcado como ausente en documentos_presentes.
       - "REVISAR" si hay fecha dudosa, ilegible o ambigua.
       - null si no se puede inferir nada.
   - Siempre que haya fecha legible, llená "fecha" con el texto original normalizado.

7) Faltantes:
   - Lista de claves de documentos_presentes que deberían estar y están en false.
   - No repitas valores, sin comentarios extra.

8) Observaciones:
   - Lista de textos cortos con advertencias o comentarios útiles para auditoría.
   - Ejemplos:
       - "REVISAR: hay dos CUIT distintos"
       - "REVISAR: firma del profesional ilegible"
       - "No se detectó seguro de mala praxis"

9) Otros datos:
   - Telefonos, emails, horarios de atención, contactos, etc. que sean útiles y no
     encajen en los campos anteriores.

10) Resumen:
   - Texto breve en español, explicando el estado general del legajo:
       - si parece completo o incompleto
       - si faltan documentos clave
       - si hay algo urgente a revisar (vencimientos, inconsistencia de datos, etc.)

11) Sobre datos contradictorios:
   - Si hay dos valores diferentes para el mismo dato:
       - elegir el más coherente o el más repetido
       - y agregar una observación "REVISAR: datos contradictorios en <campo>".

12) NOMBRE DE ARCHIVO RECOMENDADO:
   - En metadata.nombre_archivo_recomendado devolver un nombre sencillo, por ejemplo:
       "<APELLIDO>_<NOMBRE>.json"
       usando mayúsculas y sin espacios.
   - Si no hay nombre claro, usar:
       "DOCUMENTO_SIN_IDENTIFICAR.json".

13) Formato de salida:
   - Debes devolver EXCLUSIVAMENTE el objeto JSON final.
   - No incluyas explicaciones, comentarios, Markdown, ni texto antes o después del JSON.
"""

    prompt = f"""
Sos un asistente experto en documentación de la Asociación Bioquímica de Mendoza (ABM).

Tu tarea es:
1) Entender el texto OCR de un legajo (aunque tenga errores).
2) Extraer entidades y datos importantes del profesional / laboratorio.
3) Detectar qué documentos ABM parecen estar presentes o faltar.
4) Identificar fechas y estados de vencimiento cuando sea posible.
5) Devolver un JSON estructurado para ser consumido por un sistema de gestión.

Información del contexto:
- document_id sugerido: {document_id}
- pdf_nombre lógico: {pdf_name}
- cliente_id (si se conoce): {cliente_id}

Especificación detallada del JSON que debés devolver:
{schema_desc}

A continuación vas a recibir el TEXTO OCR COMPLETO entre <<<OCR>>> y <<<FIN_OCR>>>.

<<<OCR>>>
{ocr_text}
<<<FIN_OCR>>>

Recordatorio final:
- Responde SOLO el JSON, sin texto adicional.
- Respeta los nombres de las claves exactamente como se indicaron.
"""

    return prompt.strip()




# ============================================================
# LLAMADA A OLLAMA
# ============================================================

def call_ollama_gemma(prompt: str, logger: StructuredLogger) -> str:
    """
    Llama a Ollama usando el modelo OLLAMA_MODEL.
    Le pasa el prompt por stdin para evitar problemas de longitud.
    Devuelve el texto bruto generado por el modelo.
    """
    cmd = ["ollama", "run", OLLAMA_MODEL]

    logger.log(
        nivel="INFO",
        evento="OLLAMA_CALL",
        modulo="llm_postprocess",
        mensaje=f"Llamando a Ollama: {' '.join(cmd)}",
        metadata={"model": OLLAMA_MODEL},
    )

    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            check=False,
            encoding="utf-8",
            errors="replace",
        )
    except FileNotFoundError:
        raise RuntimeError(
            "No se encontró el ejecutable 'ollama'. "
            "Verificá que esté instalado y en el PATH."
        )

    elapsed = time.time() - t0

    if proc.returncode != 0:
        logger.log(
            nivel="ERROR",
            evento="OLLAMA_ERROR",
            modulo="llm_postprocess",
            mensaje=f"Ollama devolvió código {proc.returncode}",
            metadata={"stderr": proc.stderr, "tiempo_seg": round(elapsed, 3)},
        )
        raise RuntimeError(f"Ollama error ({proc.returncode}): {proc.stderr}")

    logger.log(
        nivel="INFO",
        evento="OLLAMA_OK",
        modulo="llm_postprocess",
        mensaje="Respuesta recibida de Ollama",
        metadata={"tiempo_seg": round(elapsed, 3)},
    )

    return proc.stdout.strip()


# ============================================================
# PARSEO Y POSTPROCESADO DEL JSON DEL LLM
# ============================================================

def parse_llm_json(raw_output: str, logger: StructuredLogger) -> Dict[str, Any]:
    """
    Intenta extraer un JSON válido de la salida del modelo.
    - Busca el primer '{' y el último '}' para cortar basura.
    - Si falla, devuelve un JSON de error.
    """
    try:
        start = raw_output.find("{")
        end = raw_output.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("No se encontraron llaves JSON en la salida del modelo.")

        json_str = raw_output[start : end + 1]
        data = json.loads(json_str)
        return data

    except Exception as e:
        logger.log(
            nivel="ERROR",
            evento="JSON_PARSE_ERROR",
            modulo="llm_postprocess",
            mensaje=f"No se pudo parsear JSON del LLM: {e}",
            metadata={"raw_output_preview": raw_output[:500]},
        )
        # Fallback: guardamos todo en un contenedor
        return {
            "error": "LLM_JSON_INVALIDO",
            "detalle": str(e),
            "raw_output": raw_output,
        }


def safe_get_list(d: Dict[str, Any], key: str) -> List[Any]:
    v = d.get(key, [])
    if isinstance(v, list):
        return v
    if v is None:
        return []
    return [v]


def normalize_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    normalized = "".join(normalized.split()).replace(".", "")
    return normalized or None


def is_valid_dni(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.isdigit() and 7 <= len(value) <= 9


def is_valid_cuit(value: Optional[str]) -> bool:
    if not value:
        return False
    candidate = value.replace("-", "")
    return candidate.isdigit() and len(candidate) == 11


def postprocess_llm_data(
    data: Dict[str, Any],
    txt_path: Path,
    pdf_name: str,
    pdf_ruta: str,
    document_id: str,
    model_name: str,
) -> Dict[str, Any]:
    """
    Ajusta y completa el JSON que vino del LLM:
    - Asegura estructura mínima.
    - Normaliza tipos.
    - Llena metadata.
    - Calcula faltantes.
    - Asegura bloques: entities, cliente, documentos_presentes, vencimientos,
      observaciones[], otros_datos, resumen.
    """
    if not isinstance(data, dict):
        data = {"error": "FORMATO_NO_DICT", "raw": data}

    entities = data.setdefault("entities", {})
    if not isinstance(entities, dict):
        entities = {}
        data["entities"] = entities

    metadata = data.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        data["metadata"] = metadata

    cliente = data.setdefault("cliente", {})
    if not isinstance(cliente, dict):
        cliente = {}
        data["cliente"] = cliente

    documentos_presentes = data.setdefault("documentos_presentes", {})
    if not isinstance(documentos_presentes, dict):
        documentos_presentes = {}
        data["documentos_presentes"] = documentos_presentes

    vencimientos = data.setdefault("vencimientos", {})
    if not isinstance(vencimientos, dict):
        vencimientos = {}
        data["vencimientos"] = vencimientos

    # Observaciones como lista
    obs = data.get("observaciones", [])
    if isinstance(obs, str):
        obs = [obs] if obs.strip() else []
    elif not isinstance(obs, list):
        obs = []
    data["observaciones"] = obs

    # otros_datos como dict con listas
    otros_datos = data.setdefault("otros_datos", {})
    if not isinstance(otros_datos, dict):
        otros_datos = {}
        data["otros_datos"] = otros_datos

    for k in ["telefonos", "emails", "horarios_atencion", "contactos", "otros"]:
        v = otros_datos.get(k, [])
        if isinstance(v, str):
            v = [v] if v.strip() else []
        elif not isinstance(v, list):
            v = []
        otros_datos[k] = v

    # resumen
    resumen = data.setdefault("resumen", {})
    if not isinstance(resumen, dict):
        resumen = {}
        data["resumen"] = resumen
    resumen.setdefault("principal_documento_detectado", None)
    if not isinstance(resumen.get("riesgos_detectados"), list):
        resumen["riesgos_detectados"] = safe_get_list(resumen, "riesgos_detectados")
    nivel_conf = resumen.get("nivel_confianza")
    if nivel_conf not in ("BAJO", "MEDIO", "ALTO"):
        resumen["nivel_confianza"] = "MEDIO"

    # Rellenar metadata mínima
    metadata.setdefault("document_id", document_id)
    metadata.setdefault("pdf_nombre", pdf_name)
    metadata.setdefault("pdf_ruta", pdf_ruta)
    metadata.setdefault("cliente_id", None)
    metadata.setdefault(
        "fecha_proceso",
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    metadata.setdefault("modelo", model_name)
    metadata.setdefault("modo_inferencia", "OCR_TXT_ANALISIS_COMPLETO")

    # Asegurar que documentos_presentes tenga todas las claves
    doc_keys = [
        "dni",
        "titulo",
        "matricula_actualizada",
        "constancia_cuit",
        "atm_ingresos_brutos",
        "seguro_mala_praxis",
        "inscripcion_super_intendencia_salud",
        "habilitacion_laboratorio",
        "adhesion_abm",
        "adhesion_pami",
        "adhesion_osep",
        "aceptacion_pago",
        "estatuto_social",
        "contrato_social",
        "constancia_arca",
        "cuenta_bancaria",
        "nota_designacion_director_tecnico",
    ]
    for k in doc_keys:
        v = documentos_presentes.get(k)
        if v is None:
            documentos_presentes[k] = "NO DETECTADO"
        elif isinstance(v, bool):
            documentos_presentes[k] = "DETECTADO" if v else "NO DETECTADO"
        else:
            documentos_presentes[k] = str(v)

    # Calcular faltantes según estados
    faltantes = []
    for k, v in documentos_presentes.items():
        if isinstance(v, str) and v.strip().upper().startswith("NO DETECTADO"):
            faltantes.append(k)
    data["faltantes"] = faltantes

    # Asegurar campos en vencimientos
    venc_keys = [
        "dni",
        "matricula",
        "mala_praxis",
        "ingresos_brutos",
        "monotributo",
        "arca",
        "prestador_sss",
    ]
    for k in venc_keys:
        if k not in vencimientos:
            vencimientos[k] = None

    # Intentar completar cliente usando entities si faltan datos
    names = safe_get_list(entities, "names")
    dni_list = safe_get_list(entities, "dni")
    cuil_list = safe_get_list(entities, "cuil_cuit")
    labs = safe_get_list(entities, "institution_names")
    streets = safe_get_list(entities, "street_addresses")
    towns = safe_get_list(entities, "city_towns")

    if not cliente.get("nombre_completo") and names:
        cliente["nombre_completo"] = names[0]

    if not cliente.get("dni") and dni_list:
        # Los IDs deben guardarse como strings para evitar serialización a float.
        normalized_dni = normalize_id(dni_list[0])
        cliente["dni"] = normalized_dni if is_valid_dni(normalized_dni) else None

    if not cliente.get("cuil_cuit") and cuil_list:
        # Los IDs deben guardarse como strings para evitar serialización a float.
        normalized_cuit = normalize_id(cuil_list[0])
        cliente["cuil_cuit"] = normalized_cuit if is_valid_cuit(normalized_cuit) else None

    if not cliente.get("laboratorio") and labs:
        cliente["laboratorio"] = labs[0]

    if not cliente.get("direccion") and streets:
        cliente["direccion"] = streets[0]

    if not cliente.get("localidad") and towns:
        cliente["localidad"] = towns[0]

    # Inferir tipo_persona si falta
    tipo = cliente.get("tipo_persona")
    if tipo not in ("FISICA", "JURIDICA"):
        nombre = (cliente.get("nombre_completo") or "").upper()
        lab = (cliente.get("laboratorio") or "").upper()
        if "S.A" in lab or "SA " in lab or "LABORATORIO" in lab:
            cliente["tipo_persona"] = "JURIDICA"
        elif nombre:
            cliente["tipo_persona"] = "FISICA"
        else:
            cliente["tipo_persona"] = None

    # Generar nombre_archivo_recomendado basado en nombre de persona preferentemente
    nombre_archivo = None
    nombre_persona = (cliente.get("nombre_completo") or "").upper()
    if nombre_persona and "LABORATORIO" not in nombre_persona and "S.A" not in nombre_persona and " SA " not in nombre_persona:
        nombre_archivo = normalize_name_for_filename(nombre_persona)
    else:
        # Buscar en entities.names alguna que parezca persona
        for n in names:
            nu = n.upper()
            if "LABORATORIO" not in nu and "S.A" not in nu and " SA " not in nu:
                nombre_archivo = normalize_name_for_filename(nu)
                break

    # Si no encontramos persona, usar laboratorio o nombre base del TXT
    if not nombre_archivo:
        if labs:
            nombre_archivo = normalize_name_for_filename(labs[0])
        else:
            nombre_archivo = normalize_name_for_filename(txt_path.stem)

    metadata["nombre_archivo_recomendado"] = nombre_archivo

    return data
# ============================================================
# FUNCIÓN PRINCIPAL DE POSTPROCESO
# ============================================================

def run_llm_postprocess(
    txt_path: Path,
    pdf_path: Optional[Path] = None,
    cliente_id: Optional[str] = None,
    logger: Optional[StructuredLogger] = None,
) -> Path:
    """
    Lee el TXT LLM-ready, llama a Gemma3 vía Ollama y guarda
    un JSON final <NOMBRE_NORMALIZADO>_ANALISIS_COMPLETO.json en ANALISIS_LLM_DIR.

    - No depende de que exista físicamente el PDF.
    - Usa el nombre detectado en el contenido para nombrar el archivo.
    """
    if logger is None:
        logger = StructuredLogger(session_name="llm_postprocess")

    txt_path = txt_path.resolve()

    # Nombre lógico de "PDF" sólo para metadata
    if pdf_path is not None:
        pdf_name = pdf_path.name
        pdf_ruta = str(pdf_path)
    else:
        pdf_name = txt_path.with_suffix(".pdf").name
        pdf_ruta = str(txt_path.with_suffix(".pdf"))

    logger.log(
        nivel="INFO",
        evento="LLM_POSTPROCESS_START",
        modulo="llm_postprocess",
        mensaje=f"Iniciando postproceso LLM para {pdf_name}",
        archivo=str(txt_path),
        estado="PENDIENTE",
        metadata={
            "txt_path": str(txt_path),
            "pdf_ruta_logica": pdf_ruta,
            "cliente_id": cliente_id,
        },
    )

    # Leer TXT OCR
    try:
        with open(txt_path, "r", encoding="utf-8") as f:
            ocr_text = f.read()
    except Exception as e:
        logger.log(
            nivel="ERROR",
            evento="TXT_READ_ERROR",
            modulo="llm_postprocess",
            mensaje=f"No se pudo leer el TXT: {e}",
            archivo=str(txt_path),
            estado="ERROR",
            metadata={"traceback": traceback.format_exc()},
        )
        raise

    # Usamos el TXT para generar el ID (consistente por archivo)
    document_id = compute_document_id(txt_path)

    # Construir prompt
    prompt = build_llm_prompt(
        ocr_text=ocr_text,
        pdf_name=pdf_name,
        document_id=document_id,
        cliente_id=cliente_id,
    )

    # Llamar a Ollama / Gemma
    raw_output = call_ollama_gemma(prompt, logger=logger)

    # Parsear JSON
    data = parse_llm_json(raw_output, logger=logger)



    # Postprocesar y completar estructura
    data = postprocess_llm_data(
        data=data,
        txt_path=txt_path,
        pdf_name=pdf_name,
        pdf_ruta=pdf_ruta,
        document_id=document_id,
        model_name=OLLAMA_MODEL,
    )

    # Nombre final basado en metadata.nombre_archivo_recomendado
    metadata = data.get("metadata", {})
    
    nombre_archivo_recomendado = metadata.get("nombre_archivo_recomendado") or txt_path.stem
    final_filename = f"{nombre_archivo_recomendado}_ANALISIS_COMPLETO.json"

    out_path = ANALISIS_LLM_DIR / final_filename
    with open(out_path, "w", encoding="utf-8") as jf:
        json.dump(data, jf, ensure_ascii=False, indent=2)

    logger.log(
        nivel="INFO",
        evento="LLM_POSTPROCESS_OK",
        modulo="llm_postprocess",
        mensaje=f"Análisis LLM guardado en {out_path}",
        archivo=str(out_path),
        estado="OK",
        metadata={"tam_bytes": out_path.stat().st_size},
    )

    return out_path


# ============================================================
# CLI con MENÚ INTERACTIVO
# ============================================================

if __name__ == "__main__":
    import argparse

    # Si NO se pasan argumentos → activar menú
    if len(sys.argv) == 1:
        print("\n=== MENÚ INTERACTIVO ABM LLM POSTPROCESS ===")

        base_folder = SALIDA_FINAL_DIR

        if not base_folder.exists():
            print(f"La carpeta no existe: {base_folder}")
            sys.exit(1)

        txt_files = list(base_folder.glob("*.txt"))

        if not txt_files:
            print("No se encontraron archivos .txt en la carpeta.")
            sys.exit(1)

        print("\nArchivos encontrados:\n")
        for i, f in enumerate(txt_files, 1):
            print(f"[{i}] {f.name}")

        choice = input("\nElegí el número del archivo a procesar: ").strip()

        try:
            idx = int(choice)
            txt_path = txt_files[idx - 1]
        except Exception:
            print("Opción inválida.")
            sys.exit(1)

        # Podríamos preguntar por modelo, pero dejamos OLLAMA_MODEL por defecto
        cliente_id = None
        logger = StructuredLogger(session_name="llm_postprocess_menu")

        try:
            out_json = run_llm_postprocess(
                txt_path=txt_path,
                pdf_path=None,
                cliente_id=cliente_id,
                logger=logger,
            )
            print("\n=== LLM POSTPROCESO TERMINADO ===")
            print("TXT:      ", str(txt_path))
            print("JSON LLM: ", str(out_json))

        except Exception as e:
            print("\nERROR crítico:", e)
            sys.exit(1)

        sys.exit(0)

    # ------------------------------
    # MODO ARGUMENTOS NORMAL
    # ------------------------------
    parser = argparse.ArgumentParser(
        description="Postproceso LLM para OCR ABM usando Gemma3 en Ollama."
    )
    parser.add_argument("txt", help="Ruta al TXT generado por el pipeline de OCR")
    parser.add_argument("--pdf", help="Ruta al PDF original (opcional, sólo metadata)", default=None)
    parser.add_argument("--cliente-id", help="CUIT / ID cliente (opcional)", default=None)
    parser.add_argument("--model", help=f"Modelo Ollama (default: {OLLAMA_MODEL})",
                        default=OLLAMA_MODEL)

    args = parser.parse_args()

    OLLAMA_MODEL = args.model  # override desde CLI

    txt_path = Path(args.txt)
    pdf_path = Path(args.pdf) if args.pdf else None

    logger = StructuredLogger(session_name="llm_postprocess_cli")

    try:
        out_json = run_llm_postprocess(
            txt_path=txt_path,
            pdf_path=pdf_path,
            cliente_id=args.cliente_id or None,
            logger=logger,
        )
        print("\n=== LLM POSTPROCESO TERMINADO ===")
        print("TXT:      ", str(txt_path))
        print("JSON LLM: ", str(out_json))
    except Exception as e:
        print("\nERROR crítico en postproceso LLM:", e)
        sys.exit(1)
