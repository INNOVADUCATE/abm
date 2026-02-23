"""
openclaw/error_classifier.py
OpenClaw – Error Classification Engine

Classifies errors from OCR (PaddleOCR) and LLM stages.
Determines retry strategy and escalation policy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional


class ErrorSource(Enum):
    OCR = "ocr"
    LLM = "llm"
    DB = "db"
    IO = "io"
    SYSTEM = "system"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    TRANSIENT = auto()   # Safe to retry immediately
    DEGRADED  = auto()   # Retry with backoff
    FATAL     = auto()   # Requires human intervention
    UNKNOWN   = auto()


@dataclass
class ClassifiedError:
    raw_message:  str
    source:       ErrorSource
    severity:     ErrorSeverity
    retry_allowed: bool
    suggested_wait_s: int            # seconds before retry
    notes:        str = ""
    tags:         list[str] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"[{self.source.value.upper()}] [{self.severity.name}] "
            f"retry={self.retry_allowed} wait={self.suggested_wait_s}s | "
            f"{self.raw_message[:120]}"
        )


# ---------------------------------------------------------------------------
# Pattern tables
# ---------------------------------------------------------------------------

_OCR_TRANSIENT = [
    r"CUDA out of memory",
    r"RuntimeError: CUDA",
    r"Timeout",
    r"ConnectionReset",
]

_OCR_FATAL = [
    r"No module named 'paddle",
    r"ImportError",
    r"model file not found",
    r"cannot open shared object",
]

_LLM_TRANSIENT = [
    r"RateLimitError",
    r"APIConnectionError",
    r"APITimeoutError",
    r"502 Bad Gateway",
    r"503 Service Unavailable",
    r"overloaded",
]

_LLM_FATAL = [
    r"AuthenticationError",
    r"InvalidRequestError",
    r"model not found",
]

_DB_TRANSIENT = [
    r"database is locked",
    r"OperationalError: disk I/O",
]

_DB_FATAL = [
    r"no such table",
    r"no such column",
    r"disk is full",
]


def _match_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify(raw_message: str) -> ClassifiedError:
    """
    Classify an error string and return a ClassifiedError with
    retry strategy and severity.
    """
    msg = raw_message.strip()

    # --- OCR ---
    if _match_any(msg, _OCR_FATAL):
        return ClassifiedError(
            raw_message=msg,
            source=ErrorSource.OCR,
            severity=ErrorSeverity.FATAL,
            retry_allowed=False,
            suggested_wait_s=0,
            notes="Fatal OCR import/model error – check PaddleOCR installation.",
            tags=["paddle", "fatal"],
        )
    if _match_any(msg, _OCR_TRANSIENT):
        return ClassifiedError(
            raw_message=msg,
            source=ErrorSource.OCR,
            severity=ErrorSeverity.TRANSIENT,
            retry_allowed=True,
            suggested_wait_s=10,
            notes="CUDA / timeout – safe to retry after cooldown.",
            tags=["paddle", "transient"],
        )

    # --- LLM ---
    if _match_any(msg, _LLM_FATAL):
        return ClassifiedError(
            raw_message=msg,
            source=ErrorSource.LLM,
            severity=ErrorSeverity.FATAL,
            retry_allowed=False,
            suggested_wait_s=0,
            notes="Fatal LLM auth/model error – check API key and model name.",
            tags=["llm", "fatal"],
        )
    if _match_any(msg, _LLM_TRANSIENT):
        return ClassifiedError(
            raw_message=msg,
            source=ErrorSource.LLM,
            severity=ErrorSeverity.DEGRADED,
            retry_allowed=True,
            suggested_wait_s=30,
            notes="LLM rate limit or transient API error – retry with backoff.",
            tags=["llm", "transient"],
        )

    # --- DB ---
    if _match_any(msg, _DB_FATAL):
        return ClassifiedError(
            raw_message=msg,
            source=ErrorSource.DB,
            severity=ErrorSeverity.FATAL,
            retry_allowed=False,
            suggested_wait_s=0,
            notes="Fatal DB schema/disk error – manual intervention required.",
            tags=["db", "fatal"],
        )
    if _match_any(msg, _DB_TRANSIENT):
        return ClassifiedError(
            raw_message=msg,
            source=ErrorSource.DB,
            severity=ErrorSeverity.TRANSIENT,
            retry_allowed=True,
            suggested_wait_s=5,
            notes="DB locked – safe to retry shortly.",
            tags=["db", "transient"],
        )

    # --- Fallback ---
    return ClassifiedError(
        raw_message=msg,
        source=ErrorSource.UNKNOWN,
        severity=ErrorSeverity.UNKNOWN,
        retry_allowed=False,
        suggested_wait_s=0,
        notes="Unclassified error – manual review recommended.",
        tags=["unknown"],
    )


def classify_log_file(log_path: Path, tail_lines: int = 200) -> list[ClassifiedError]:
    """
    Scan the last N lines of a log file and return all classified errors.
    """
    if not log_path.exists():
        return []

    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    results: list[ClassifiedError] = []
    for line in lines[-tail_lines:]:
        lower = line.lower()
        if any(kw in lower for kw in ("error", "exception", "traceback", "failed", "critical")):
            results.append(classify(line.strip()))

    return results
