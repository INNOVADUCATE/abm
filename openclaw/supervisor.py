"""
openclaw/supervisor.py
OpenClaw – Process Supervisor

Decides whether to restart, wait, or escalate based on
process health, log activity, and error severity.
Never restarts without explicit policy approval.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional

from openclaw.monitor import (
    ProcessSnapshot,
    SystemSnapshot,
    is_pid_alive,
    log_is_active,
    snapshot_process,
    snapshot_system,
)
from openclaw.error_classifier import (
    ClassifiedError,
    ErrorSeverity,
    classify_log_file,
)

logger = logging.getLogger("openclaw.supervisor")


class SupervisorDecision(Enum):
    HEALTHY          = auto()
    WAIT             = auto()   # Degraded but recoverable – do nothing yet
    RESTART          = auto()   # Safe to restart automatically
    ESCALATE         = auto()   # Needs human – do NOT restart
    PROCESS_MISSING  = auto()   # PID not found


@dataclass
class SupervisorReport:
    decision:        SupervisorDecision
    reason:          str
    process_snap:    Optional[ProcessSnapshot]
    system_snap:     Optional[SystemSnapshot]
    top_errors:      list[ClassifiedError] = field(default_factory=list)
    restart_count:   int = 0
    timestamp:       float = field(default_factory=time.time)

    def summary(self) -> str:
        lines = [
            f"── SupervisorReport ──────────────────────────────",
            f"  Decision : {self.decision.name}",
            f"  Reason   : {self.reason}",
        ]
        if self.process_snap:
            lines.append(f"  Process  : {self.process_snap}")
        if self.system_snap:
            lines.append(f"  System   : {self.system_snap}")
        if self.top_errors:
            lines.append(f"  Errors   : {len(self.top_errors)} classified errors in log")
            for e in self.top_errors[:3]:
                lines.append(f"    → {e}")
        lines.append(f"──────────────────────────────────────────────────")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Policy constants
# ---------------------------------------------------------------------------

MAX_AUTO_RESTARTS       = 3     # after this many restarts → ESCALATE
LOG_STALE_SECS          = 90    # log not updated for N secs → suspect hang
FATAL_ERRORS_THRESHOLD  = 1     # any fatal error → ESCALATE


# ---------------------------------------------------------------------------
# Supervisor core
# ---------------------------------------------------------------------------

class Supervisor:
    """
    Stateful supervisor that tracks restart history and makes
    policy decisions about process health.
    """

    def __init__(self, log_path: Optional[Path] = None, project_root: Optional[Path] = None):
        self.log_path     = log_path
        self.project_root = project_root
        self._restart_count = 0

    def evaluate(self, pid: Optional[int]) -> SupervisorReport:
        """
        Full health evaluation for a running pipeline process.
        Returns a SupervisorReport with a concrete Decision.
        """
        system_snap = None
        try:
            system_snap = snapshot_system(self.project_root)
        except Exception as exc:
            logger.warning("Could not collect system snapshot: %s", exc)

        # 1. PID check
        if pid is None or not is_pid_alive(pid):
            return SupervisorReport(
                decision=SupervisorDecision.PROCESS_MISSING,
                reason="PID not found – process may have exited or never started.",
                process_snap=None,
                system_snap=system_snap,
                restart_count=self._restart_count,
            )

        # 2. Process snapshot
        proc_snap = snapshot_process(pid)
        if proc_snap is None:
            return SupervisorReport(
                decision=SupervisorDecision.PROCESS_MISSING,
                reason=f"PID {pid} disappeared during evaluation.",
                process_snap=None,
                system_snap=system_snap,
                restart_count=self._restart_count,
            )

        # 3. Error classification from log
        errors: list[ClassifiedError] = []
        if self.log_path:
            try:
                errors = classify_log_file(self.log_path)
            except Exception as exc:
                logger.warning("Error classifier failed: %s", exc)

        fatal_errors   = [e for e in errors if e.severity == ErrorSeverity.FATAL]
        has_fatal      = len(fatal_errors) >= FATAL_ERRORS_THRESHOLD

        # 4. Log staleness
        log_active, log_age = True, 0.0
        if self.log_path:
            log_active, log_age = log_is_active(self.log_path, LOG_STALE_SECS)

        # 5. Decision tree
        if has_fatal:
            return SupervisorReport(
                decision=SupervisorDecision.ESCALATE,
                reason=f"Fatal error detected in log: {fatal_errors[0].notes}",
                process_snap=proc_snap,
                system_snap=system_snap,
                top_errors=errors[:5],
                restart_count=self._restart_count,
            )

        if proc_snap.is_hung:
            if self._restart_count >= MAX_AUTO_RESTARTS:
                return SupervisorReport(
                    decision=SupervisorDecision.ESCALATE,
                    reason=(
                        f"Process hung AND restart limit reached "
                        f"({self._restart_count}/{MAX_AUTO_RESTARTS})."
                    ),
                    process_snap=proc_snap,
                    system_snap=system_snap,
                    top_errors=errors[:5],
                    restart_count=self._restart_count,
                )
            return SupervisorReport(
                decision=SupervisorDecision.RESTART,
                reason=f"Process hung: {proc_snap.hung_reason}",
                process_snap=proc_snap,
                system_snap=system_snap,
                top_errors=errors[:5],
                restart_count=self._restart_count,
            )

        if not log_active:
            return SupervisorReport(
                decision=SupervisorDecision.WAIT,
                reason=f"Log stale for {log_age:.0f}s – monitoring, not restarting yet.",
                process_snap=proc_snap,
                system_snap=system_snap,
                top_errors=errors[:5],
                restart_count=self._restart_count,
            )

        if system_snap and system_snap.bottleneck_detected:
            return SupervisorReport(
                decision=SupervisorDecision.WAIT,
                reason=f"System bottleneck: {system_snap.bottleneck_reason}",
                process_snap=proc_snap,
                system_snap=system_snap,
                top_errors=errors[:5],
                restart_count=self._restart_count,
            )

        return SupervisorReport(
            decision=SupervisorDecision.HEALTHY,
            reason="Process running normally.",
            process_snap=proc_snap,
            system_snap=system_snap,
            top_errors=errors[:5],
            restart_count=self._restart_count,
        )

    def record_restart(self) -> None:
        """Call this each time a restart is actually performed."""
        self._restart_count += 1
        logger.info("Supervisor restart recorded (total: %d)", self._restart_count)

    def reset_restart_count(self) -> None:
        """Reset counter after successful stabilization."""
        self._restart_count = 0
