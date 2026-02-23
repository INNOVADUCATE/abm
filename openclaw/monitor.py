"""
openclaw/monitor.py
OpenClaw – Resource & Process Monitor

Monitors CPU, memory, and process health.
Detects hung processes and reports bottlenecks.
Does NOT touch the OCR/LLM pipeline internals.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


@dataclass
class ProcessSnapshot:
    pid:          int
    name:         str
    status:       str
    cpu_percent:  float
    mem_rss_mb:   float
    num_threads:  int
    elapsed_s:    float
    is_hung:      bool
    hung_reason:  str = ""

    def __str__(self) -> str:
        hung_tag = f" ⚠ HUNG: {self.hung_reason}" if self.is_hung else ""
        return (
            f"PID {self.pid} [{self.status}]  "
            f"CPU {self.cpu_percent:.1f}%  "
            f"MEM {self.mem_rss_mb:.1f} MB  "
            f"Threads {self.num_threads}  "
            f"Up {self.elapsed_s:.0f}s"
            f"{hung_tag}"
        )


@dataclass
class SystemSnapshot:
    cpu_percent_total:   float
    mem_used_percent:    float
    mem_available_gb:    float
    disk_free_gb:        float
    bottleneck_detected: bool
    bottleneck_reason:   str = ""

    def __str__(self) -> str:
        bn = f"  ⚠ BOTTLENECK: {self.bottleneck_reason}" if self.bottleneck_detected else ""
        return (
            f"CPU {self.cpu_percent_total:.1f}%  "
            f"MEM {self.mem_used_percent:.1f}% used  "
            f"({self.mem_available_gb:.1f} GB free)  "
            f"DISK {self.disk_free_gb:.1f} GB free"
            f"{bn}"
        )


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

CPU_HUNG_THRESHOLD_PCT  = 0.5    # process using < 0.5% CPU for too long = suspect
CPU_HUNG_IDLE_SECS      = 120    # seconds at near-zero CPU before flagging
CPU_SYSTEM_HIGH_PCT     = 90.0
MEM_SYSTEM_HIGH_PCT     = 90.0
DISK_LOW_GB             = 1.0


def _check_psutil() -> None:
    if not PSUTIL_AVAILABLE:
        raise RuntimeError(
            "psutil is not installed. Run: pip install psutil"
        )


# ---------------------------------------------------------------------------
# Process monitoring
# ---------------------------------------------------------------------------

def snapshot_process(pid: int) -> Optional[ProcessSnapshot]:
    """
    Return a ProcessSnapshot for the given PID.
    Returns None if process no longer exists.
    """
    _check_psutil()

    try:
        proc = psutil.Process(pid)
        with proc.oneshot():
            name        = proc.name()
            status      = proc.status()
            mem_info    = proc.memory_info()
            num_threads = proc.num_threads()
            create_time = proc.create_time()

        # CPU percent needs two samples; use interval=1.0
        cpu = proc.cpu_percent(interval=1.0)
        elapsed = time.time() - create_time
        mem_mb  = mem_info.rss / (1024 * 1024)

        is_hung     = False
        hung_reason = ""

        if status in ("zombie", "dead"):
            is_hung     = True
            hung_reason = f"process status is '{status}'"
        elif status == "stopped":
            is_hung     = True
            hung_reason = "process is stopped (SIGSTOP)"
        elif cpu < CPU_HUNG_THRESHOLD_PCT and elapsed > CPU_HUNG_IDLE_SECS:
            is_hung     = True
            hung_reason = (
                f"CPU {cpu:.2f}% < {CPU_HUNG_THRESHOLD_PCT}% "
                f"for >{CPU_HUNG_IDLE_SECS}s – possible hang"
            )

        return ProcessSnapshot(
            pid=pid,
            name=name,
            status=status,
            cpu_percent=cpu,
            mem_rss_mb=mem_mb,
            num_threads=num_threads,
            elapsed_s=elapsed,
            is_hung=is_hung,
            hung_reason=hung_reason,
        )

    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def is_pid_alive(pid: int) -> bool:
    """Check whether a PID is currently running."""
    _check_psutil()
    return psutil.pid_exists(pid)


# ---------------------------------------------------------------------------
# System monitoring
# ---------------------------------------------------------------------------

def snapshot_system(check_path: Optional[Path] = None) -> SystemSnapshot:
    """
    Return a SystemSnapshot with CPU, memory, and disk stats.
    check_path: path used for disk check (defaults to CWD).
    """
    _check_psutil()

    cpu_pct  = psutil.cpu_percent(interval=0.5)
    mem      = psutil.virtual_memory()
    mem_used = mem.percent
    mem_free = mem.available / (1024 ** 3)

    disk_path = str(check_path) if check_path else "."
    try:
        disk      = psutil.disk_usage(disk_path)
        disk_free = disk.free / (1024 ** 3)
    except Exception:
        disk_free = -1.0

    bottleneck = False
    reason     = ""

    if cpu_pct >= CPU_SYSTEM_HIGH_PCT:
        bottleneck = True
        reason     = f"System CPU at {cpu_pct:.1f}%"
    elif mem_used >= MEM_SYSTEM_HIGH_PCT:
        bottleneck = True
        reason     = f"System RAM at {mem_used:.1f}% used"
    elif 0 < disk_free < DISK_LOW_GB:
        bottleneck = True
        reason     = f"Only {disk_free:.2f} GB disk space remaining"

    return SystemSnapshot(
        cpu_percent_total=cpu_pct,
        mem_used_percent=mem_used,
        mem_available_gb=mem_free,
        disk_free_gb=disk_free,
        bottleneck_detected=bottleneck,
        bottleneck_reason=reason,
    )


# ---------------------------------------------------------------------------
# Log activity monitor
# ---------------------------------------------------------------------------

def log_is_active(log_path: Path, stale_after_secs: int = 60) -> tuple[bool, float]:
    """
    Returns (is_active, seconds_since_last_write).
    A log is considered stale if it hasn't been written to recently.
    """
    if not log_path.exists():
        return False, -1.0

    mtime   = log_path.stat().st_mtime
    age_s   = time.time() - mtime
    active  = age_s < stale_after_secs
    return active, age_s
