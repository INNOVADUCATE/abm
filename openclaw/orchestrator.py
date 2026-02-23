"""
openclaw/orchestrator.py
OpenClaw – Intelligent Pipeline Orchestrator

Handles smart retry, backoff, and pipeline coordination.
Acts as meta-orchestrator ABOVE the pipeline – never inside it.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from openclaw.error_classifier import ClassifiedError, ErrorSeverity, classify_log_file
from openclaw.supervisor import Supervisor, SupervisorDecision

logger = logging.getLogger("openclaw.orchestrator")


@dataclass
class RetryPolicy:
    max_attempts:    int   = 3
    base_wait_s:     int   = 15    # initial backoff
    backoff_factor:  float = 2.0   # exponential multiplier
    max_wait_s:      int   = 300   # cap at 5 minutes

    def wait_for_attempt(self, attempt: int) -> int:
        wait = int(self.base_wait_s * (self.backoff_factor ** (attempt - 1)))
        return min(wait, self.max_wait_s)


@dataclass
class OrchestrationResult:
    success:         bool
    attempts:        int
    final_reason:    str
    elapsed_total_s: float
    errors_seen:     list[ClassifiedError] = field(default_factory=list)

    def __str__(self) -> str:
        status = "✓ SUCCESS" if self.success else "✗ FAILED"
        return (
            f"{status} after {self.attempts} attempt(s) "
            f"in {self.elapsed_total_s:.1f}s | {self.final_reason}"
        )


class PipelineOrchestrator:
    """
    Meta-orchestrator for the ABM pipeline.

    Responsibilities:
    - Decide when and how to retry failed runs
    - Apply exponential backoff for transient errors
    - Escalate fatal errors immediately
    - Track orchestration history
    - Integrate with Supervisor for health decisions
    """

    def __init__(
        self,
        pipeline_script: Path,
        input_dir:       Path,
        log_path:        Path,
        pid_file:        Path,
        project_root:    Path,
        policy:          Optional[RetryPolicy] = None,
    ):
        self.pipeline_script = pipeline_script
        self.input_dir       = input_dir
        self.log_path        = log_path
        self.pid_file        = pid_file
        self.project_root    = project_root
        self.policy          = policy or RetryPolicy()
        self.supervisor      = Supervisor(log_path=log_path, project_root=project_root)
        self._history:       list[OrchestrationResult] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def launch_and_supervise(self) -> OrchestrationResult:
        """
        Launch the pipeline and supervise it to completion or failure.
        Returns an OrchestrationResult.
        """
        start_total = time.time()
        attempt     = 0
        errors_seen: list[ClassifiedError] = []

        while attempt < self.policy.max_attempts:
            attempt += 1
            logger.info("Orchestrator: starting pipeline attempt %d/%d",
                        attempt, self.policy.max_attempts)

            pid = self._launch_pipeline()
            if pid is None:
                result = OrchestrationResult(
                    success=False,
                    attempts=attempt,
                    final_reason="Failed to launch pipeline process.",
                    elapsed_total_s=time.time() - start_total,
                )
                self._history.append(result)
                return result

            # Supervise until done or actionable
            decision, reason, new_errors = self._supervise_until_done(pid)
            errors_seen.extend(new_errors)

            if decision == SupervisorDecision.HEALTHY:
                result = OrchestrationResult(
                    success=True,
                    attempts=attempt,
                    final_reason=reason,
                    elapsed_total_s=time.time() - start_total,
                    errors_seen=errors_seen,
                )
                self._history.append(result)
                return result

            if decision == SupervisorDecision.ESCALATE:
                result = OrchestrationResult(
                    success=False,
                    attempts=attempt,
                    final_reason=f"ESCALATED: {reason}",
                    elapsed_total_s=time.time() - start_total,
                    errors_seen=errors_seen,
                )
                self._history.append(result)
                return result

            # RESTART or WAIT: apply backoff and retry
            wait = self._compute_wait(new_errors, attempt)
            logger.warning(
                "Orchestrator: attempt %d ended (%s) – waiting %ds before retry",
                attempt, decision.name, wait,
            )
            self.supervisor.record_restart()
            time.sleep(wait)

        # Exhausted attempts
        result = OrchestrationResult(
            success=False,
            attempts=attempt,
            final_reason=f"Exhausted {self.policy.max_attempts} attempts.",
            elapsed_total_s=time.time() - start_total,
            errors_seen=errors_seen,
        )
        self._history.append(result)
        return result

    def analyze_log_errors(self) -> list[ClassifiedError]:
        """
        Scan current log for errors and return classified list.
        """
        if not self.log_path.exists():
            return []
        return classify_log_file(self.log_path)

    def history(self) -> list[OrchestrationResult]:
        return list(self._history)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _launch_pipeline(self) -> Optional[int]:
        """
        Launch the pipeline as a background process.
        Write PID to pid_file. Return the PID or None on failure.
        """
        try:
            log_handle = self.log_path.open("ab")  # append binary
            proc = subprocess.Popen(
                ["python", str(self.pipeline_script), str(self.input_dir)],
                stdout=log_handle,
                stderr=log_handle,
                cwd=str(self.project_root),
                # Windows: don't inherit console (no CTRL-C propagation)
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
            self.pid_file.write_text(str(proc.pid), encoding="utf-8")
            logger.info("Pipeline launched: PID %d", proc.pid)
            return proc.pid
        except Exception as exc:
            logger.error("Failed to launch pipeline: %s", exc)
            return None

    def _supervise_until_done(
        self, pid: int
    ) -> tuple[SupervisorDecision, str, list[ClassifiedError]]:
        """
        Poll the process until it exits or the supervisor flags a problem.
        Returns (final_decision, reason, errors_found).
        """
        import psutil

        poll_interval = 10  # seconds
        errors: list[ClassifiedError] = []

        while True:
            time.sleep(poll_interval)

            report = self.supervisor.evaluate(pid)
            errors = report.top_errors

            if report.decision == SupervisorDecision.PROCESS_MISSING:
                # Process ended – check for errors in log
                log_errors = self.analyze_log_errors()
                fatal = [e for e in log_errors if e.severity == ErrorSeverity.FATAL]
                if fatal:
                    return SupervisorDecision.ESCALATE, fatal[0].notes, log_errors
                return SupervisorDecision.HEALTHY, "Pipeline process exited cleanly.", log_errors

            if report.decision in (SupervisorDecision.ESCALATE, SupervisorDecision.RESTART):
                # Kill the hung process before returning
                try:
                    proc = psutil.Process(pid)
                    proc.terminate()
                    proc.wait(timeout=10)
                except Exception:
                    pass
                return report.decision, report.reason, errors

            # HEALTHY or WAIT → continue polling
            logger.debug("Supervisor: %s – %s", report.decision.name, report.reason)

    def _compute_wait(
        self, errors: list[ClassifiedError], attempt: int
    ) -> int:
        """
        Compute wait time based on errors and policy backoff.
        Respects per-error suggested_wait_s for transient errors.
        """
        policy_wait  = self.policy.wait_for_attempt(attempt)
        error_wait   = max((e.suggested_wait_s for e in errors), default=0)
        return max(policy_wait, error_wait)
