#!/usr/bin/env python3
"""
Extractor de identidad (fallback conservador)

Objetivo:
- Intentar usar LangExtract (si está instalado) para inferir display_name.
- Si no está disponible o falla, hacer fallback regex (DNI/CUIL) sin romper.

Salida mínima:
{
  "display_name": "...",
  "dni": "...",
  "cuil_cuit": "...",
  "apellido": "...",
  "nombre": "...",
  "confidence": 0.0-1.0,
  "evidence": {"spans": [...]}
}
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _load_text(input_path: Optional[Path], inline_text: Optional[str]) -> str:
    if inline_text is not None:
        return inline_text
    if input_path is None:
        raise ValueError("Debe indicar --input o --text.")
    return input_path.read_text(encoding="utf-8", errors="ignore")


def _normalize_dni(token: str) -> str:
    return re.sub(r"\D", "", token)


def _normalize_cuil(token: str) -> str:
    digits = re.sub(r"\D", "", token)
    if len(digits) == 11:
        return f"{digits[0:2]}-{digits[2:10]}-{digits[10]}"
    return token.strip()


def _regex_extract_ids(text: str) -> Tuple[str, str, List[Dict[str, Any]]]:
    spans: List[Dict[str, Any]] = []

    cuil_match = re.search(r"\b\d{2}[-\s]?\d{8}[-\s]?\d\b", text)
    cuil = ""
    if cuil_match:
        cuil_raw = cuil_match.group(0)
        cuil = _normalize_cuil(cuil_raw)
        spans.append(
            {
                "field": "cuil_cuit",
                "text": cuil_raw,
                "start": cuil_match.start(),
                "end": cuil_match.end(),
                "source": "regex",
            }
        )

    dni_match = re.search(r"\b\d{1,2}\.\d{3}\.\d{3}\b|\b\d{7,8}\b", text)
    dni = ""
    if dni_match:
        dni_raw = dni_match.group(0)
        dni = _normalize_dni(dni_raw)
        spans.append(
            {
                "field": "dni",
                "text": dni_raw,
                "start": dni_match.start(),
                "end": dni_match.end(),
                "source": "regex",
            }
        )

    return dni, cuil, spans


def _try_langextract_display_name(text: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Mejor esfuerzo para distintos layouts de librería LangExtract.
    Si no existe o falla, retorna ("", []).
    """
    try:
        import langextract  # type: ignore
    except Exception:
        return "", []

    def _candidate_from_obj(obj: Any) -> Tuple[str, List[Dict[str, Any]]]:
        spans: List[Dict[str, Any]] = []
        if obj is None:
            return "", spans

        # Caso dict: buscar llaves típicas
        if isinstance(obj, dict):
            for key in ("display_name", "full_name", "name", "person_name"):
                value = obj.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip(), spans

            entities = obj.get("entities")
            if isinstance(entities, list):
                for ent in entities:
                    if not isinstance(ent, dict):
                        continue
                    label = str(ent.get("label") or ent.get("type") or "").lower()
                    txt = ent.get("text")
                    if isinstance(txt, str) and txt.strip() and any(
                        k in label for k in ("person", "name", "full_name")
                    ):
                        spans.append(
                            {
                                "field": "display_name",
                                "text": txt,
                                "start": ent.get("start", -1),
                                "end": ent.get("end", -1),
                                "source": "langextract",
                            }
                        )
                        return txt.strip(), spans
            return "", spans

        # Caso objeto con .entities
        entities = getattr(obj, "entities", None)
        if isinstance(entities, list):
            for ent in entities:
                label = str(getattr(ent, "label", "") or getattr(ent, "type", "")).lower()
                txt = getattr(ent, "text", None)
                if isinstance(txt, str) and txt.strip() and any(
                    k in label for k in ("person", "name", "full_name")
                ):
                    spans.append(
                        {
                            "field": "display_name",
                            "text": txt,
                            "start": getattr(ent, "start", -1),
                            "end": getattr(ent, "end", -1),
                            "source": "langextract",
                        }
                    )
                    return txt.strip(), spans
        return "", spans

    # 1) API posible: langextract.extract(text)
    try:
        if hasattr(langextract, "extract"):
            out = langextract.extract(text)
            name, spans = _candidate_from_obj(out)
            if name:
                return name, spans
    except Exception:
        pass

    # 2) API posible: langextract.from_text(text)
    try:
        if hasattr(langextract, "from_text"):
            out = langextract.from_text(text)
            name, spans = _candidate_from_obj(out)
            if name:
                return name, spans
    except Exception:
        pass

    return "", []


def _split_name(display_name: str) -> Tuple[str, str]:
    if not display_name.strip():
        return "", ""
    if "," in display_name:
        left, right = display_name.split(",", 1)
        return left.strip(), right.strip()

    parts = [p for p in display_name.split() if p.strip()]
    if len(parts) == 1:
        return "", parts[0]
    # heurística mínima: último token como nombre, resto apellido
    return " ".join(parts[:-1]), parts[-1]


def extract_identity(text: str) -> Dict[str, Any]:
    dni, cuil, evidence_spans = _regex_extract_ids(text)

    display_name, name_spans = _try_langextract_display_name(text)
    evidence_spans.extend(name_spans)

    apellido, nombre = _split_name(display_name)

    confidence = 0.0
    if display_name:
        confidence = 0.85
    elif dni or cuil:
        confidence = 0.35

    return {
        "display_name": display_name,
        "dni": dni,
        "cuil_cuit": cuil,
        "apellido": apellido,
        "nombre": nombre,
        "confidence": confidence,
        "evidence": {"spans": evidence_spans},
    }


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Extractor conservador de identidad con fallback.")
    p.add_argument("--input", type=Path, help="Ruta a TXT OCR de entrada.")
    p.add_argument("--text", type=str, help="Texto crudo de entrada (alternativa a --input).")
    p.add_argument("--out", type=Path, help="Ruta de salida JSON (opcional).")
    p.add_argument("--pretty", action="store_true", help="Imprime JSON con indentación.")
    return p


def main() -> int:
    args = _build_parser().parse_args()
    text = _load_text(args.input, args.text)
    result = extract_identity(text)

    if args.pretty:
        payload = json.dumps(result, ensure_ascii=False, indent=2)
    else:
        payload = json.dumps(result, ensure_ascii=False)

    if args.out:
        args.out.write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
