"""
abm_agent.py
ABM Pipeline Agent – Production Grade
─────────────────────────────────────────────────────────────────────────────
Architecture:
    abm_launch.cmd
        └── python abm_agent.py          ← this file
                ├── subprocess.Popen     ← launches pipeline (no CMD/PS hell)
                ├── PID file             ← tracks background process
                ├── pathlib everywhere   ← no string-path bugs
                ├── sqlite3 (safe)       ← parameterized queries only
                └── openclaw/            ← intelligent supervision layer
                        ├── orchestrator
                        ├── monitor
                        ├── supervisor
                        ├── error_classifier
                        └── rag_store

Usage:
    python abm_agent.py                  ← interactive menu
    python abm_agent.py --run            ← non-interactive: run + supervise
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# ── Optional psutil (soft dependency) ────────────────────────────────────────
try:
    import psutil
    PSUTIL_OK = True
except ImportError:
    PSUTIL_OK = False

# ── OpenClaw ──────────────────────────────────────────────────────────────────
try:
    from openclaw.monitor import snapshot_process, snapshot_system, is_pid_alive
    from openclaw.supervisor import Supervisor, SupervisorDecision
    from openclaw.orchestrator import PipelineOrchestrator, RetryPolicy
    from openclaw.error_classifier import classify_log_file, ErrorSeverity
    from openclaw.rag_store import RAGStore
    OPENCLAW_OK = True
except ImportError as _oc_err:
    OPENCLAW_OK = False
    _oc_import_error = str(_oc_err)


# ═══════════════════════════════════════════════════════════════════════════════
# PATHS  (edit here – nowhere else)
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_ROOT   = Path(r"C:\Users\aguss\OneDrive\Documentos\GitHub\abm")
PIPELINE_SCRIPT = PROJECT_ROOT / "pipeline" / "abm_run_pipeline.py"
INPUT_DIR      = PROJECT_ROOT / "runs" / "mini_in"
LOG_FILE       = PROJECT_ROOT / "runs" / "mini_full_run.log"
DB_FILE        = PROJECT_ROOT / "runs" / "mini_isolated_processed.sqlite"
PID_FILE       = PROJECT_ROOT / "runs" / ".pipeline.pid"
RAG_DB_FILE    = PROJECT_ROOT / "runs" / ".openclaw_rag.sqlite"
OUTPUT_DIR     = PROJECT_ROOT / "runs" / "outputs"

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("abm_agent")


# ═══════════════════════════════════════════════════════════════════════════════
# PID MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def read_pid() -> Optional[int]:
    """Read PID from file. Returns None if missing, invalid, or process dead."""
    if not PID_FILE.exists():
        return None
    try:
        pid = int(PID_FILE.read_text(encoding="utf-8").strip())
        if PSUTIL_OK and not psutil.pid_exists(pid):
            PID_FILE.unlink(missing_ok=True)
            return None
        return pid
    except (ValueError, OSError):
        return None


def write_pid(pid: int) -> None:
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid), encoding="utf-8")


def clear_pid() -> None:
    PID_FILE.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE LAUNCH  (the right way – pure Python, no CMD/PowerShell)
# ═══════════════════════════════════════════════════════════════════════════════

def launch_pipeline() -> Optional[int]:
    """
    Launch abm_run_pipeline.py in the background.

    Key decisions:
    - subprocess.Popen → no CMD/PS shell needed
    - CREATE_NO_WINDOW → no console popup on Windows
    - stdout/stderr → appended to LOG_FILE
    - PID saved to file immediately
    """
    if not PIPELINE_SCRIPT.exists():
        print(f"[ERROR] Pipeline script not found: {PIPELINE_SCRIPT}")
        return None

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    existing_pid = read_pid()
    if existing_pid is not None:
        print(f"[WARN] Pipeline already running (PID {existing_pid}). Use Kill first.")
        return existing_pid

    print(f"[INFO] Launching pipeline...")
    print(f"       Script : {PIPELINE_SCRIPT}")
    print(f"       Input  : {INPUT_DIR}")
    print(f"       Log    : {LOG_FILE}")

    try:
        log_handle = LOG_FILE.open("ab")  # append binary – safe on Windows

        creation_flags = 0
        if sys.platform == "win32":
            creation_flags = subprocess.CREATE_NO_WINDOW

        proc = subprocess.Popen(
            [sys.executable, str(PIPELINE_SCRIPT), str(INPUT_DIR)],
            stdout=log_handle,
            stderr=log_handle,
            cwd=str(PROJECT_ROOT),
            creationflags=creation_flags,
        )

        write_pid(proc.pid)
        print(f"[OK] Pipeline started. PID: {proc.pid}")
        logger.info("Pipeline launched: PID %d", proc.pid)
        return proc.pid

    except Exception as exc:
        logger.error("Failed to launch pipeline: %s", exc)
        print(f"[ERROR] Could not launch pipeline: {exc}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# LOG TAIL  (pure Python – no tail.exe, no PowerShell)
# ═══════════════════════════════════════════════════════════════════════════════

def tail_log(n_lines: int = 40, follow: bool = False) -> None:
    """
    Display last N lines of the log file.
    If follow=True, stream new lines until Ctrl+C.
    """
    if not LOG_FILE.exists():
        print(f"[WARN] Log file not found: {LOG_FILE}")
        return

    with LOG_FILE.open("r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    for line in lines[-n_lines:]:
        print(line, end="")

    if not follow:
        return

    print("\n[Ctrl+C to stop tailing]\n")
    try:
        with LOG_FILE.open("r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)  # seek to end
            while True:
                chunk = f.read(4096)
                if chunk:
                    print(chunk, end="", flush=True)
                else:
                    time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[Tail stopped]")


# ═══════════════════════════════════════════════════════════════════════════════
# PROCESS STATUS
# ═══════════════════════════════════════════════════════════════════════════════

def show_status() -> None:
    pid = read_pid()
    if pid is None:
        print("[INFO] No pipeline process running.")
        return

    print(f"[INFO] Pipeline PID: {pid}")

    if not PSUTIL_OK:
        print("[WARN] psutil not installed – install with: pip install psutil")
        return

    if OPENCLAW_OK:
        snap = snapshot_process(pid)
        if snap:
            print(f"       {snap}")
        else:
            print(f"[WARN] PID {pid} not found – may have exited.")
            clear_pid()

        sys_snap = snapshot_system(PROJECT_ROOT)
        print(f"[SYS]  {sys_snap}")

        # OpenClaw supervisor quick-check
        sup = Supervisor(log_path=LOG_FILE, project_root=PROJECT_ROOT)
        report = sup.evaluate(pid)
        print(f"\n── OpenClaw Supervisor ──────────────────────────")
        print(f"  Decision : {report.decision.name}")
        print(f"  Reason   : {report.reason}")
        if report.top_errors:
            print(f"  Errors   : {len(report.top_errors)} in log")
        print(f"─────────────────────────────────────────────────")
    else:
        # Fallback without OpenClaw
        try:
            proc = psutil.Process(pid)
            print(f"  Status : {proc.status()}")
            print(f"  CPU    : {proc.cpu_percent(interval=1)}%")
            mem_mb = proc.memory_info().rss / (1024 * 1024)
            print(f"  MEM    : {mem_mb:.1f} MB")
        except psutil.NoSuchProcess:
            print(f"[WARN] PID {pid} not found.")
            clear_pid()


# ═══════════════════════════════════════════════════════════════════════════════
# KILL PROCESS
# ═══════════════════════════════════════════════════════════════════════════════

def kill_process(force: bool = False) -> None:
    pid = read_pid()
    if pid is None:
        print("[INFO] No pipeline process to kill.")
        return

    if not PSUTIL_OK:
        print("[WARN] psutil not available – cannot safely kill. Install psutil.")
        return

    try:
        proc = psutil.Process(pid)
        name = proc.name()
        print(f"[INFO] Terminating PID {pid} ({name})...")

        if force:
            proc.kill()
            print(f"[OK] PID {pid} killed (SIGKILL).")
        else:
            proc.terminate()
            try:
                proc.wait(timeout=10)
                print(f"[OK] PID {pid} terminated gracefully.")
            except psutil.TimeoutExpired:
                proc.kill()
                print(f"[OK] PID {pid} force-killed after timeout.")

        clear_pid()

    except psutil.NoSuchProcess:
        print(f"[WARN] PID {pid} already gone.")
        clear_pid()
    except psutil.AccessDenied:
        print(f"[ERROR] Access denied for PID {pid}.")


# ═══════════════════════════════════════════════════════════════════════════════
# DB REPORT  (safe parameterized SQLite – no inline SQL strings)
# ═══════════════════════════════════════════════════════════════════════════════

def _db_connect() -> Optional[sqlite3.Connection]:
    if not DB_FILE.exists():
        print(f"[WARN] DB not found: {DB_FILE}")
        return None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as exc:
        print(f"[ERROR] Cannot open DB: {exc}")
        return None


def show_db_report() -> None:
    conn = _db_connect()
    if conn is None:
        return

    print(f"\n── DB Report: {DB_FILE.name} ──────────────────────")

    try:
        # List all tables
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()

        if not tables:
            print("  (empty database – no tables)")
            return

        for row in tables:
            table = row["name"]
            try:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM [{table}]"   # brackets handle reserved words
                ).fetchone()[0]
                print(f"  Table [{table}] → {count:,} rows")

                # Show column names
                cols = conn.execute(
                    f"PRAGMA table_info([{table}])"
                ).fetchall()
                col_names = ", ".join(c["name"] for c in cols)
                print(f"    Columns: {col_names}")

                # Show sample row
                sample = conn.execute(
                    f"SELECT * FROM [{table}] LIMIT 1"
                ).fetchone()
                if sample:
                    sample_dict = dict(sample)
                    for k, v in sample_dict.items():
                        val_str = str(v)[:80] if v is not None else "NULL"
                        print(f"    {k}: {val_str}")

            except sqlite3.Error as exc:
                print(f"  [ERROR] Could not query [{table}]: {exc}")

    finally:
        conn.close()

    print(f"──────────────────────────────────────────────────\n")


# ═══════════════════════════════════════════════════════════════════════════════
# GIT SANITY CHECK
# ═══════════════════════════════════════════════════════════════════════════════

def git_sanity() -> None:
    print("[INFO] Running Git sanity check...")

    git = shutil.which("git")
    if not git:
        print("[WARN] git not found in PATH.")
        return

    commands = [
        (["git", "status", "--short"],  "Working tree status"),
        (["git", "log", "--oneline", "-5"], "Last 5 commits"),
    ]

    for cmd, label in commands:
        print(f"\n  ── {label}")
        try:
            result = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=15,
            )
            out = result.stdout.strip() or "(nothing)"
            for line in out.splitlines():
                print(f"    {line}")
        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            print(f"    [ERROR] {exc}")

    print()


# ═══════════════════════════════════════════════════════════════════════════════
# CLEAN OUTPUTS
# ═══════════════════════════════════════════════════════════════════════════════

def clean_outputs() -> None:
    targets = [
        OUTPUT_DIR,
        PROJECT_ROOT / "runs" / "outputs",
    ]

    confirm = input("[?] This will delete output files. Continue? (y/N) ").strip().lower()
    if confirm != "y":
        print("[Cancelled]")
        return

    for target in targets:
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
            print(f"[OK] Removed: {target}")
        else:
            print(f"[SKIP] Not found: {target}")

    print("[OK] Clean complete.")


# ═══════════════════════════════════════════════════════════════════════════════
# OPENCLAW PANEL
# ═══════════════════════════════════════════════════════════════════════════════

def openclaw_panel() -> None:
    if not OPENCLAW_OK:
        print(f"[ERROR] OpenClaw not available: {_oc_import_error}")
        print("        Check that the openclaw/ directory is next to abm_agent.py")
        return

    print("\n── OpenClaw Intelligence Panel ──────────────────────────────")

    # Error analysis
    print("\n[1] Log error scan")
    errors = classify_log_file(LOG_FILE)
    if not errors:
        print("    No errors found in log.")
    else:
        fatal   = [e for e in errors if e.severity == ErrorSeverity.FATAL]
        others  = [e for e in errors if e.severity != ErrorSeverity.FATAL]
        print(f"    Total classified: {len(errors)} | Fatal: {len(fatal)}")
        for e in (fatal + others)[:10]:
            print(f"    {e}")

    # System snapshot
    print("\n[2] System snapshot")
    try:
        snap = snapshot_system(PROJECT_ROOT)
        print(f"    {snap}")
    except RuntimeError as exc:
        print(f"    [WARN] {exc}")

    # Process snapshot
    print("\n[3] Process health")
    pid = read_pid()
    if pid:
        try:
            psnap = snapshot_process(pid)
            print(f"    {psnap}" if psnap else f"    PID {pid} not found.")
        except RuntimeError as exc:
            print(f"    [WARN] {exc}")
    else:
        print("    No pipeline running.")

    # Supervisor evaluation
    print("\n[4] Supervisor decision")
    sup = Supervisor(log_path=LOG_FILE, project_root=PROJECT_ROOT)
    report = sup.evaluate(pid)
    print(report.summary())

    # RAG summary
    print("[5] Run history (RAG store)")
    try:
        rag = RAGStore(RAG_DB_FILE)
        rag.print_summary()
    except Exception as exc:
        print(f"    [WARN] RAG store unavailable: {exc}")

    print("──────────────────────────────────────────────────────────────\n")


def openclaw_auto_supervise() -> None:
    """
    Run pipeline with full OpenClaw orchestration and retry logic.
    """
    if not OPENCLAW_OK:
        print(f"[ERROR] OpenClaw not available: {_oc_import_error}")
        return

    print("[OpenClaw] Starting supervised pipeline run...")

    orch = PipelineOrchestrator(
        pipeline_script=PIPELINE_SCRIPT,
        input_dir=INPUT_DIR,
        log_path=LOG_FILE,
        pid_file=PID_FILE,
        project_root=PROJECT_ROOT,
        policy=RetryPolicy(max_attempts=3, base_wait_s=15, backoff_factor=2.0),
    )

    result = orch.launch_and_supervise()
    print(f"\n[OpenClaw] Orchestration result: {result}")

    # Persist to RAG store
    try:
        rag = RAGStore(RAG_DB_FILE)
        rag.record_run(
            success=result.success,
            attempts=result.attempts,
            elapsed_s=result.elapsed_total_s,
            reason=result.final_reason,
            errors=[
                {"source": e.source.value, "severity": e.severity.name, "msg": e.raw_message[:200]}
                for e in result.errors_seen
            ],
        )
    except Exception as exc:
        logger.warning("Could not persist to RAG store: %s", exc)


# ═══════════════════════════════════════════════════════════════════════════════
# INTERACTIVE MENU
# ═══════════════════════════════════════════════════════════════════════════════

MENU = """
╔══════════════════════════════════════════════════╗
║          ABM Pipeline Agent  v2.0               ║
║          + OpenClaw Intelligence Layer          ║
╠══════════════════════════════════════════════════╣
║  1) Git sanity check                            ║
║  2) Clean outputs                               ║
║  3) Run MINI pipeline (background)             ║
║  4) Tail log (last 40 lines)                   ║
║  5) Follow log (streaming)                     ║
║  6) Status – process + system                  ║
║  7) Kill pipeline                              ║
║  8) DB report                                  ║
║  9) OpenClaw panel (errors / health / RAG)     ║
║ 10) OpenClaw: supervised run (auto-retry)      ║
║  0) Exit                                       ║
╚══════════════════════════════════════════════════╝
"""

_OC_STATUS = "✓ loaded" if OPENCLAW_OK else "✗ unavailable"


def run_menu() -> None:
    print(MENU)
    print(f"  OpenClaw: {_OC_STATUS}")
    print(f"  Project : {PROJECT_ROOT}\n")

    dispatch = {
        "1":  git_sanity,
        "2":  clean_outputs,
        "3":  launch_pipeline,
        "4":  lambda: tail_log(40, follow=False),
        "5":  lambda: tail_log(20, follow=True),
        "6":  show_status,
        "7":  kill_process,
        "8":  show_db_report,
        "9":  openclaw_panel,
        "10": openclaw_auto_supervise,
    }

    while True:
        try:
            choice = input("abm> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[Exit]")
            break

        if choice == "0":
            print("[Exit]")
            break

        handler = dispatch.get(choice)
        if handler:
            try:
                handler()
            except KeyboardInterrupt:
                print("\n[Interrupted]")
            except Exception as exc:
                logger.exception("Unhandled error in menu action: %s", exc)
                print(f"[ERROR] {exc}")
        else:
            print(f"[?] Unknown option: {choice!r}")


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL HANDLING
# ═══════════════════════════════════════════════════════════════════════════════

def _setup_signals() -> None:
    def _sigint(sig: int, frame: object) -> None:
        print("\n\n[SIGINT] Caught Ctrl+C – exiting agent cleanly.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _sigint)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _sigint)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    _setup_signals()

    parser = argparse.ArgumentParser(description="ABM Pipeline Agent")
    parser.add_argument("--run", action="store_true",
                        help="Non-interactive: launch pipeline with OpenClaw supervision")
    parser.add_argument("--status", action="store_true",
                        help="Non-interactive: print status and exit")
    parser.add_argument("--kill", action="store_true",
                        help="Non-interactive: kill running pipeline")
    parser.add_argument("--report", action="store_true",
                        help="Non-interactive: print DB report and exit")
    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.kill:
        kill_process()
    elif args.report:
        show_db_report()
    elif args.run:
        if OPENCLAW_OK:
            openclaw_auto_supervise()
        else:
            launch_pipeline()
    else:
        run_menu()


if __name__ == "__main__":
    main()
