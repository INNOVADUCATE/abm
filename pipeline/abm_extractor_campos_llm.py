# abm_extractor_campos.py
# ------------------------------------------------------------
# ABM DocFlow - Extractor de campos (fase "cruda" con LLM)
# - Lee texto (TXT o JSON con {"text": ...})
# - Llama a un LLM (Ollama / OpenAI-compatible) para extraer campos
# - Devuelve JSON estructurado "crudo" + evidencias + warnings
#
# Requisitos:
#   pip install requests
#
# Ejemplos:
#   python abm_extractor_campos.py --input socio.txt --out socio_extracted.json --provider ollama --model llama3.1:8b
#   python abm_extractor_campos.py --input socio.json --out socio_extracted.json --provider ollama --model llama3.1:8b --base-url http://localhost:11434
#   python abm_extractor_campos.py --input socio.txt --provider openai_compat --model gpt-4o-mini --base-url http://localhost:8080/v1 --api-key sk-xxx
#
# Notas:
# - Si tu Ollama no soporta /api/chat, el script cae a /api/generate automáticamente.
# - La salida está pensada para que el normalizador y reasoner trabajen después.
# ------------------------------------------------------------

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# -----------------------------
# Utilidades
# -----------------------------

def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _read_text_file(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="ignore")


def _load_input_text(input_path: Path) -> Tuple[str, Dict[str, Any]]:
    """
    Devuelve: (text, meta)
    Soporta:
      - .txt/.md => todo el contenido
      - .json => si existe key "text" usa eso; sino intenta "content" o concatena strings
    """
    meta: Dict[str, Any] = {"input_path": str(input_path)}
    if not input_path.exists():
        raise FileNotFoundError(f"No existe: {input_path}")

    suffix = input_path.suffix.lower()
    if suffix in [".txt", ".md", ".log"]:
        text = _read_text_file(input_path)
        meta["input_type"] = "text"
        return text, meta

    if suffix == ".json":
        raw = json.loads(_read_text_file(input_path))
        meta["input_type"] = "json"

        if isinstance(raw, dict):
            for k in ("text", "content", "ocr_text", "full_text"):
                if k in raw and isinstance(raw[k], str) and raw[k].strip():
                    meta["json_key_used"] = k
                    return raw[k], meta

            # fallback: concatena strings de valores
            chunks: List[str] = []
            for k, v in raw.items():
                if isinstance(v, str) and v.strip():
                    chunks.append(f"[{k}]\n{v}")
            if chunks:
                meta["json_key_used"] = "concat(dict strings)"
                return "\n\n".join(chunks), meta

        if isinstance(raw, list):
            chunks = []
            for i, it in enumerate(raw):
                if isinstance(it, str) and it.strip():
                    chunks.append(it)
                elif isinstance(it, dict):
                    # si trae "text" dentro
                    t = it.get("text") or it.get("content")
                    if isinstance(t, str) and t.strip():
                        chunks.append(t)
            if chunks:
                meta["json_key_used"] = "concat(list items)"
                return "\n\n".join(chunks), meta

        raise ValueError("JSON sin texto utilizable (no encontré keys text/content ni strings para concatenar).")

    # Otros formatos: intenta leer como texto plano
    meta["input_type"] = "unknown-as-text"
    return _read_text_file(input_path), meta


def _safe_json_loads(s: str) -> Optional[Any]:
    try:
        return json.loads(s)
    except Exception:
        return None


def _extract_json_object(s: str) -> Optional[Dict[str, Any]]:
    """
    Intenta recuperar el primer objeto JSON válido dentro de una respuesta de LLM
    aunque venga con texto extra, fences, etc.
    """
    s = s.strip()

    # Si ya es JSON directo
    obj = _safe_json_loads(s)
    if isinstance(obj, dict):
        return obj

    # Quitar fences ```json ... ```
    s2 = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE).strip()
    s2 = re.sub(r"\s*```$", "", s2).strip()

    obj = _safe_json_loads(s2)
    if isinstance(obj, dict):
        return obj

    # Buscar el primer "{" ... "}" balanceado
    start = s2.find("{")
    if start == -1:
        return None

    # escaneo balanceado simple
    depth = 0
    for i in range(start, len(s2)):
        ch = s2[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = s2[start:i + 1]
                obj = _safe_json_loads(candidate)
                if isinstance(obj, dict):
                    return obj
                return None
    return None


def _clip(s: str, n: int = 220) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


# -----------------------------
# Clientes LLM
# -----------------------------

@dataclass
class LLMConfig:
    provider: str  # "ollama" | "openai_compat"
    model: str
    base_url: str
    api_key: Optional[str] = None
    timeout_s: int = 60


class LLMClient:
    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg
        self.session = requests.Session()

    def chat_json(self, system: str, user: str, temperature: float = 0.1) -> Dict[str, Any]:
        """
        Retorna dict con:
          - ok: bool
          - raw_text: str (respuesta del modelo)
          - json: dict|None (parseada)
          - provider_meta: dict
          - warnings: list[str]
        """
        if self.cfg.provider == "ollama":
            return self._ollama_chat(system, user, temperature=temperature)
        if self.cfg.provider == "openai_compat":
            return self._openai_compat_chat(system, user, temperature=temperature)
        raise ValueError(f"Provider no soportado: {self.cfg.provider}")

    def _ollama_chat(self, system: str, user: str, temperature: float = 0.1) -> Dict[str, Any]:
        """
        Soporta:
          - Ollama /api/chat (si está disponible)
          - fallback a /api/generate
        """
        warnings: List[str] = []
        base = self.cfg.base_url.rstrip("/")
        model = self.cfg.model

        # 1) Intentar /api/chat
        chat_url = f"{base}/api/chat"
        payload_chat = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "options": {"temperature": temperature},
            "stream": False,
        }
        try:
            r = self.session.post(chat_url, json=payload_chat, timeout=self.cfg.timeout_s)
            if r.status_code == 200:
                data = r.json()
                # Ollama suele devolver {"message":{"content":...}}
                content = ""
                if isinstance(data, dict):
                    msg = data.get("message")
                    if isinstance(msg, dict):
                        content = msg.get("content") or ""
                    else:
                        content = data.get("response") or ""
                parsed = _extract_json_object(content)
                return {
                    "ok": parsed is not None,
                    "raw_text": content,
                    "json": parsed,
                    "provider_meta": {"endpoint": "/api/chat", "status": r.status_code},
                    "warnings": warnings + ([] if parsed else ["No pude parsear JSON de la respuesta (api/chat)."]),
                }
            else:
                warnings.append(f"Ollama /api/chat devolvió status {r.status_code}, intento fallback a /api/generate.")
        except Exception as e:
            warnings.append(f"Ollama /api/chat falló ({type(e).__name__}: {e}), intento fallback a /api/generate.")

        # 2) Fallback /api/generate
        gen_url = f"{base}/api/generate"
        # En generate, se pasa todo como prompt (instrucciones + texto)
        prompt = f"SYSTEM:\n{system}\n\nUSER:\n{user}\n\nDEVOLVÉ SOLO JSON."
        payload_gen = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature},
        }
        try:
            r = self.session.post(gen_url, json=payload_gen, timeout=self.cfg.timeout_s)
            data = r.json() if r.status_code == 200 else {}
            content = ""
            if isinstance(data, dict):
                content = data.get("response") or ""
            parsed = _extract_json_object(content)
            return {
                "ok": parsed is not None,
                "raw_text": content,
                "json": parsed,
                "provider_meta": {"endpoint": "/api/generate", "status": r.status_code},
                "warnings": warnings + ([] if parsed else ["No pude parsear JSON de la respuesta (api/generate)."]),
            }
        except Exception as e:
            return {
                "ok": False,
                "raw_text": "",
                "json": None,
                "provider_meta": {"endpoint": "/api/generate", "error": f"{type(e).__name__}: {e}"},
                "warnings": warnings + [f"Fallback /api/generate falló ({type(e).__name__}: {e})."],
            }

    def _openai_compat_chat(self, system: str, user: str, temperature: float = 0.1) -> Dict[str, Any]:
        """
        OpenAI compatible:
          POST {base_url}/chat/completions
          headers Authorization: Bearer {api_key}
        """
        warnings: List[str] = []
        base = self.cfg.base_url.rstrip("/")
        url = f"{base}/chat/completions"

        headers = {"Content-Type": "application/json"}
        if self.cfg.api_key:
            headers["Authorization"] = f"Bearer {self.cfg.api_key}"

        payload = {
            "model": self.cfg.model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            # Pedimos JSON estricto si el backend lo soporta; si no, no rompe
            "response_format": {"type": "json_object"},
        }

        try:
            r = self.session.post(url, json=payload, headers=headers, timeout=self.cfg.timeout_s)
            if r.status_code != 200:
                return {
                    "ok": False,
                    "raw_text": r.text,
                    "json": None,
                    "provider_meta": {"endpoint": "/chat/completions", "status": r.status_code},
                    "warnings": [f"openai_compat devolvió status {r.status_code}"],
                }
            data = r.json()
            content = ""
            # OpenAI style: choices[0].message.content
            try:
                content = data["choices"][0]["message"]["content"]
            except Exception:
                content = json.dumps(data)[:2000]

            parsed = _extract_json_object(content)
            return {
                "ok": parsed is not None,
                "raw_text": content,
                "json": parsed,
                "provider_meta": {"endpoint": "/chat/completions", "status": r.status_code},
                "warnings": warnings + ([] if parsed else ["No pude parsear JSON de la respuesta (openai_compat)."]),
            }
        except Exception as e:
            return {
                "ok": False,
                "raw_text": "",
                "json": None,
                "provider_meta": {"endpoint": "/chat/completions", "error": f"{type(e).__name__}: {e}"},
                "warnings": [f"openai_compat falló ({type(e).__name__}: {e})."],
            }


# -----------------------------
# Prompt de extracción
# -----------------------------

SYSTEM_PROMPT = """
Sos un extractor de información institucional. Tu tarea es leer un texto (posible OCR, con ruido)
y devolver UNICAMENTE un objeto JSON válido, sin comentarios, sin markdown, sin texto extra.

Reglas:
- Si un dato no existe o no se puede inferir con alta confianza, usar null.
- No inventes números de matrícula ni fechas.
- Para fechas: preferir formato ISO "YYYY-MM-DD" si se reconoce con claridad. Si no, dejar null y guardar el texto crudo en "observaciones".
- Incluí EVIDENCIA: para cada dato clave, cuando sea posible, agregar un snippet corto exacto que lo respalde.
- Los nombres propios conservar mayúsculas/minúsculas, pero sin adornos.
- Teléfonos: conservar como string, sin formatear agresivo.
- Emails: en minúscula si se detectan.
- Devuelve además un array "warnings" con problemas detectados (OCR raro, fechas ambiguas, etc.)
"""

USER_TEMPLATE = """
Texto a analizar:
----------------
{TEXT}
----------------

Devolvé un JSON con esta estructura EXACTA (mismos keys), llenando lo que puedas:

{{
  "socio": {{
    "nombre_completo": null,
    "matricula": null,
    "dni": null,
    "cuit": null,
    "email": null,
    "telefono": null,
    "domicilio": null,
    "localidad": null,
    "provincia": null
  }},
  "documentos": [
    {{
      "tipo": null,
      "detalle": null,
      "numero": null,
      "fecha_emision": null,
      "fecha_vencimiento": null,
      "estado_texto": null,
      "entidad_emisora": null,
      "observaciones": null,
      "evidencia": {{
        "snippet": null
      }}
    }}
  ],
  "aportes_cuotas": {{
    "periodo_texto": null,
    "monto_texto": null,
    "estado_texto": null,
    "fecha_pago": null,
    "observaciones": null,
    "evidencia": {{
      "snippet": null
    }}
  }},
  "vencimientos_detectados": [
    {{
      "concepto": null,
      "fecha": null,
      "fecha_texto": null,
      "observaciones": null,
      "evidencia": {{
        "snippet": null
      }}
    }}
  ],
  "metadatos_extraccion": {{
    "idioma_detectado": "es",
    "calidad_ocr": null,
    "confianza_global": null
  }},
  "warnings": []
}}

IMPORTANTE:
- Si no encontrás ningún documento, devolvé "documentos": [] (lista vacía).
- Si no encontrás vencimientos, devolvé "vencimientos_detectados": [].
- No agregues keys nuevas.
- El JSON debe ser válido.
"""


def build_user_prompt(text: str) -> str:
    # Si el texto es enorme, recortamos para evitar explotar tokens.
    # (Más adelante podemos hacer chunking; por ahora, corte seguro.)
    MAX_CHARS = 18000
    t = text.strip()
    if len(t) > MAX_CHARS:
        t = t[:MAX_CHARS]
        t += "\n\n[TRUNCADO: el texto original era más largo]"
    return USER_TEMPLATE.format(TEXT=t)


# -----------------------------
# Extracción principal
# -----------------------------

def extract_campos(text: str, client: LLMClient) -> Dict[str, Any]:
    """
    Ejecuta el LLM y devuelve un payload estable para el pipeline.
    """
    sys_prompt = SYSTEM_PROMPT.strip()
    user_prompt = build_user_prompt(text)

    llm_res = client.chat_json(sys_prompt, user_prompt, temperature=0.1)

    out: Dict[str, Any] = {
        "ok": bool(llm_res.get("ok")),
        "timestamp_utc": _now_iso(),
        "input_preview": _clip(text, 260),
        "llm": {
            "provider": client.cfg.provider,
            "model": client.cfg.model,
            "base_url": client.cfg.base_url,
            "provider_meta": llm_res.get("provider_meta", {}),
        },
        "warnings": llm_res.get("warnings", []) or [],
        "data": None,
        "raw_text": llm_res.get("raw_text", ""),
    }

    data = llm_res.get("json")
    if isinstance(data, dict):
        # sane defaults: listas vacías si vinieron null
        if data.get("documentos") is None:
            data["documentos"] = []
        if data.get("vencimientos_detectados") is None:
            data["vencimientos_detectados"] = []

        out["data"] = data
    else:
        out["ok"] = False

    return out


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ABM - Extractor de campos (crudo) con LLM")
    p.add_argument("--input", required=True, help="Ruta a .txt o .json con texto")
    p.add_argument("--out", default="", help="Ruta de salida .json (si se omite, imprime por stdout)")
    p.add_argument("--provider", default="ollama", choices=["ollama", "openai_compat"], help="Proveedor LLM")
    p.add_argument("--model", default="llama3.1:8b", help="Modelo (ej: llama3.1:8b)")
    p.add_argument("--base-url", default="http://localhost:11434", help="Base URL del proveedor")
    p.add_argument("--api-key", default="", help="API key si aplica (openai_compat)")
    p.add_argument("--timeout", type=int, default=60, help="Timeout (segundos)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    input_path = Path(args.input).expanduser().resolve()
    text, meta = _load_input_text(input_path)

    cfg = LLMConfig(
        provider=args.provider,
        model=args.model,
        base_url=args.base_url,
        api_key=(args.api_key.strip() or None),
        timeout_s=int(args.timeout),
    )
    client = LLMClient(cfg)

    result = extract_campos(text, client)

    # anexamos meta de input
    result["input_meta"] = meta

    pretty = json.dumps(result, ensure_ascii=False, indent=2)

    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(pretty, encoding="utf-8")
        print(f"[OK] Guardado: {out_path}")
    else:
        print(pretty)

    # exit code según ok
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
