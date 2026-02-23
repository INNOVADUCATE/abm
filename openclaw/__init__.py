"""
OpenClaw – Intelligent Operations Layer for ABM Pipeline

Architecture:
    OCR (Paddle) → LLM → JSON → DB
         ↑________________________↑
                  OpenClaw
         (meta-orchestrator / supervisor)

Modules:
    orchestrator     – smart retry & pipeline coordination
    monitor          – CPU / memory / process health
    supervisor       – hung-process detection & restart policy
    error_classifier – OCR / LLM / DB error classification
    rag_store        – historical run storage (future RAG)
"""

from openclaw.error_classifier import classify, classify_log_file, ClassifiedError
from openclaw.monitor import snapshot_process, snapshot_system, is_pid_alive
from openclaw.supervisor import Supervisor, SupervisorDecision, SupervisorReport
from openclaw.orchestrator import PipelineOrchestrator, RetryPolicy, OrchestrationResult
from openclaw.rag_store import RAGStore

__all__ = [
    "classify",
    "classify_log_file",
    "ClassifiedError",
    "snapshot_process",
    "snapshot_system",
    "is_pid_alive",
    "Supervisor",
    "SupervisorDecision",
    "SupervisorReport",
    "PipelineOrchestrator",
    "RetryPolicy",
    "OrchestrationResult",
    "RAGStore",
]
