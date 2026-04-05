#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_PATH = Path(__file__).resolve()
ROOT = Path(os.environ.get("CODEXLAB_ROOT", SCRIPT_PATH.parent))
CONTROL_DIR = ROOT / "control"
DB_PATH = CONTROL_DIR / "control.db"
EVENTS_DIR = CONTROL_DIR / "events"
EVENTS_FILE = EVENTS_DIR / "events.jsonl"
RUNS_DIR = CONTROL_DIR / "runs"
DAEMON_DIR = CONTROL_DIR / "daemon"
DAEMON_PID_FILE = DAEMON_DIR / "daemon.pid"
DAEMON_STATE_FILE = DAEMON_DIR / "daemon-state.json"
DAEMON_LOG_FILE = DAEMON_DIR / "daemon.log"
TASKS_DIR = ROOT / "tasks"
AGENTS_DIR = ROOT / "agents"
LAB_HOME = ROOT / ".codex-home"
TEMPLATES_DIR = ROOT / "templates"
REAL_CODEX = os.environ.get("CODEXLAB_CODEX_BIN", "/usr/bin/codex")
DEFAULT_EXECUTOR = os.environ.get("CODEXLAB_EXECUTOR", "mock")
DEFAULT_EXEC_TIMEOUT = float(os.environ.get("CODEXLAB_EXEC_TIMEOUT", "120"))
WORKER_LANES = ("worker-a", "worker-b")
ALL_LANES = (
    ("worker-a", "worker"),
    ("worker-b", "worker"),
    ("evaluator", "evaluator"),
)
MAX_RETRY_FAILURES = 3
MAX_TOTAL_EVALUATIONS = 12
MAX_ROLE_SWAPS = 6
RUBRIC_WEIGHTS = {
    "correctness": 35.0,
    "completeness": 25.0,
    "risk": 15.0,
    "maintainability": 15.0,
    "verification": 10.0,
}
RUBRIC_CRITERIA = tuple(RUBRIC_WEIGHTS.keys())

DEFAULT_WORKER_TEMPLATE = textwrap.dedent(
    """\
    You are {lane_id}, a CodexLab worker agent.

    Task ID: {task_id}
    Title: {task_title}

    Primary task brief:
    {task_prompt}

    Current published champion summary:
    {champion_summary}

    Current published champion body:
    {champion_body}

    Improvement brief from the evaluator:
    {loser_brief}

    Instructions:
    - Produce the strongest possible answer for the task.
    - If this is a retry, explicitly improve on the published champion.
    - Keep the response concrete and high signal.
    - Return only JSON matching the required schema.
    """
)

DEFAULT_EVALUATOR_TEMPLATE = textwrap.dedent(
    """\
    You are the CodexLab evaluator.

    Task ID: {task_id}
    Title: {task_title}

    Original task brief:
    {task_prompt}

    Compare the two submissions below.
    Score each submission on a 0-5 rubric for correctness, completeness, risk, maintainability, and verification.
    Derive the overall winner from those rubric scores.
    Never produce a tie.
    The loser brief must tell the lower-scoring worker exactly how to beat the winner on the next retry.

    LEFT SUBMISSION
    ID: {left_submission_id}
    Lane: {left_lane_id}
    Summary:
    {left_summary}

    Body:
    {left_body}

    RIGHT SUBMISSION
    ID: {right_submission_id}
    Lane: {right_lane_id}
    Summary:
    {right_summary}

    Body:
    {right_body}

    Return only JSON matching the required schema.
    """
)


class LaneExecutionError(RuntimeError):
    def __init__(self, message: str, exit_code: int = 1, run_status: str = "failed"):
        super().__init__(message)
        self.exit_code = exit_code
        self.run_status = run_status


@dataclass
class RunHandle:
    run_id: str
    lane_id: str
    run_dir: Path


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_layout() -> None:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    DAEMON_DIR.mkdir(parents=True, exist_ok=True)
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
    for lane_id, _lane_type in ALL_LANES:
        (AGENTS_DIR / lane_id / "home").mkdir(parents=True, exist_ok=True)
        (AGENTS_DIR / lane_id / "workspace").mkdir(parents=True, exist_ok=True)


def connect() -> sqlite3.Connection:
    ensure_layout()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS lanes (
            lane_id TEXT PRIMARY KEY,
            lane_type TEXT NOT NULL,
            status TEXT NOT NULL,
            active_task_id TEXT,
            active_submission_id TEXT,
            active_run_id TEXT,
            notes TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            published_submission_id TEXT,
            champion_submission_id TEXT,
            challenger_submission_id TEXT,
            champion_lane_id TEXT,
            challenger_lane_id TEXT,
            champion_score REAL,
            challenger_score REAL,
            challenger_failed_attempts INTEGER NOT NULL DEFAULT 0,
            total_evaluations INTEGER NOT NULL DEFAULT 0,
            role_swaps INTEGER NOT NULL DEFAULT 0,
            masterpiece_locked INTEGER NOT NULL DEFAULT 0,
            max_retry_failures INTEGER NOT NULL DEFAULT 3,
            max_total_evaluations INTEGER NOT NULL DEFAULT 12,
            max_role_swaps INTEGER NOT NULL DEFAULT 6
        );

        CREATE TABLE IF NOT EXISTS submissions (
            submission_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            lane_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            round_number INTEGER NOT NULL,
            retry_number INTEGER NOT NULL,
            status TEXT NOT NULL,
            summary TEXT NOT NULL,
            artifact_path TEXT NOT NULL,
            created_at TEXT NOT NULL,
            published INTEGER NOT NULL DEFAULT 0,
            superseded_by TEXT,
            meta_json TEXT
        );

        CREATE TABLE IF NOT EXISTS evaluations (
            evaluation_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            left_submission_id TEXT NOT NULL,
            right_submission_id TEXT NOT NULL,
            winner_submission_id TEXT NOT NULL,
            loser_submission_id TEXT NOT NULL,
            winner_lane_id TEXT NOT NULL,
            loser_lane_id TEXT NOT NULL,
            winner_score REAL NOT NULL,
            loser_score REAL NOT NULL,
            score_delta REAL NOT NULL,
            rationale TEXT NOT NULL,
            loser_brief TEXT NOT NULL,
            swap_occurred INTEGER NOT NULL DEFAULT 0,
            masterpiece_locked INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            scorecard_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reservations (
            reservation_id TEXT PRIMARY KEY,
            lane_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            reservation_type TEXT NOT NULL,
            priority INTEGER NOT NULL,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            lane_id TEXT NOT NULL,
            task_id TEXT,
            submission_id TEXT,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            command TEXT NOT NULL,
            cwd TEXT NOT NULL,
            codex_home TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            exit_code INTEGER
        );
        """
    )
    for key, value in (
        ("schema_version", "2"),
        ("next_task_id", "1"),
        ("next_submission_id", "1"),
        ("next_evaluation_id", "1"),
        ("next_reservation_id", "1"),
        ("next_run_id", "1"),
    ):
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)", (key, value))
    for lane_id, lane_type in ALL_LANES:
        conn.execute(
            """
            INSERT OR IGNORE INTO lanes(lane_id, lane_type, status, active_task_id, active_submission_id, active_run_id, notes, updated_at)
            VALUES(?, ?, 'idle', NULL, NULL, NULL, '', ?)
            """,
            (lane_id, lane_type, now_utc()),
        )
    conn.commit()


def allocate_id(conn: sqlite3.Connection, meta_key: str, prefix: str) -> str:
    current = int(conn.execute("SELECT value FROM meta WHERE key = ?", (meta_key,)).fetchone()[0])
    conn.execute("UPDATE meta SET value = ? WHERE key = ?", (str(current + 1), meta_key))
    return f"{prefix}-{current:04d}"


def append_event(event_type: str, **payload: Any) -> None:
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    record = {"ts": now_utc(), "type": event_type, **payload}
    with EVENTS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def daemon_pid_payload() -> dict[str, Any] | None:
    payload = read_json(DAEMON_PID_FILE)
    if not payload:
        return None
    pid = int(payload.get("pid", 0) or 0)
    if pid and not process_alive(pid):
        try:
            DAEMON_PID_FILE.unlink()
        except FileNotFoundError:
            pass
        return None
    return payload


def daemon_runtime_snapshot() -> dict[str, Any]:
    pid_payload = daemon_pid_payload()
    state_payload = read_json(DAEMON_STATE_FILE) or {}
    pid = int(pid_payload.get("pid", 0) or 0) if pid_payload else 0
    return {
        "running": bool(pid_payload and process_alive(pid)),
        "pid": pid,
        "pid_file": str(DAEMON_PID_FILE),
        "state_file": str(DAEMON_STATE_FILE),
        "log_file": str(DAEMON_LOG_FILE),
        "pid_payload": pid_payload,
        "state": state_payload,
    }


def fetch_lane(conn: sqlite3.Connection, lane_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM lanes WHERE lane_id = ?", (lane_id,)).fetchone()
    if row is None:
        raise SystemExit(f"Unknown lane: {lane_id}")
    return row


def fetch_task(conn: sqlite3.Connection, task_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
    if row is None:
        raise SystemExit(f"Unknown task: {task_id}")
    return row


def fetch_submission(conn: sqlite3.Connection, submission_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM submissions WHERE submission_id = ?", (submission_id,)).fetchone()
    if row is None:
        raise SystemExit(f"Unknown submission: {submission_id}")
    return row


def lane_queue(conn: sqlite3.Connection, lane_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM reservations
        WHERE lane_id = ? AND status = 'queued'
        ORDER BY priority ASC, created_at ASC, reservation_id ASC
        """,
        (lane_id,),
    ).fetchall()


def lane_paths(lane_id: str) -> dict[str, Path]:
    root = AGENTS_DIR / lane_id
    return {
        "root": root,
        "home": root / "home",
        "workspace": root / "workspace",
    }


def task_workspace_path(lane_id: str, task_id: str) -> Path:
    return lane_paths(lane_id)["workspace"] / "tasks" / task_id


def worktree_workspace_path(lane_id: str, task_id: str) -> Path:
    return lane_paths(lane_id)["workspace"] / "worktrees" / task_id


def target_repo_path() -> Path:
    raw = os.environ.get("CODEXLAB_TARGET_REPO")
    if raw:
        return Path(raw).expanduser().resolve()
    return ROOT


def git_probe(repo_path: Path) -> dict[str, Any]:
    probe: dict[str, Any] = {
        "path": str(repo_path),
        "exists": repo_path.exists(),
        "git_available": shutil.which("git") is not None,
        "is_repo": False,
        "has_head": False,
        "top_level": None,
        "head_sha": None,
        "error": None,
    }
    if not probe["exists"]:
        probe["error"] = "target repo path does not exist"
        return probe
    if not probe["git_available"]:
        probe["error"] = "git binary is not available"
        return probe

    top = subprocess.run(
        ["git", "-C", str(repo_path), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    )
    if top.returncode != 0:
        probe["error"] = top.stderr.strip() or "not a git repository"
        return probe
    probe["is_repo"] = True
    probe["top_level"] = top.stdout.strip()

    head = subprocess.run(
        ["git", "-C", str(repo_path), "rev-parse", "--verify", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if head.returncode == 0:
        probe["has_head"] = True
        probe["head_sha"] = head.stdout.strip()
    else:
        probe["error"] = head.stderr.strip() or "repository has no committed HEAD yet"
    return probe


def existing_task_workspace(lane_id: str, task_id: str) -> Path | None:
    for candidate in (
        worktree_workspace_path(lane_id, task_id),
        task_workspace_path(lane_id, task_id),
    ):
        manifest = candidate / "codexlab-task.json"
        if manifest.exists():
            return candidate
    return None


def hydrate_workspace_metadata(manifest_path: Path) -> dict[str, Any]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    workspace_path = manifest_path.parent
    changed = False
    inferred_kind = "git-worktree" if "/worktrees/" in str(manifest_path) else "directory"
    if "workspace_kind" not in payload:
        payload["workspace_kind"] = inferred_kind
        changed = True
    if "workspace_path" not in payload:
        payload["workspace_path"] = str(workspace_path)
        changed = True
    if "repo_root" not in payload:
        payload["repo_root"] = None
        changed = True
    if "head_sha" not in payload:
        payload["head_sha"] = None
        changed = True
    if "fallback_reason" not in payload:
        payload["fallback_reason"] = "legacy manifest backfilled" if inferred_kind == "directory" else None
        changed = True
    if changed:
        write_json(manifest_path, payload)
    return payload


def ensure_git_worktree(repo_top: str, lane_id: str, task_id: str) -> Path:
    workspace = worktree_workspace_path(lane_id, task_id)
    if workspace.exists():
        return workspace
    workspace.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["git", "-C", repo_top, "worktree", "add", "--detach", str(workspace), "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "git worktree add failed"
        raise LaneExecutionError(message)
    return workspace


def prepare_task_workspace(conn: sqlite3.Connection, lane_id: str, task_id: str) -> Path:
    task = fetch_task(conn, task_id)
    existing = existing_task_workspace(lane_id, task_id)
    if existing is not None:
        hydrate_workspace_metadata(existing / "codexlab-task.json")
        return existing

    repo_probe = git_probe(target_repo_path())
    workspace_kind = "directory"
    fallback_reason = None
    repo_root = None
    head_sha = None
    if repo_probe["is_repo"] and repo_probe["has_head"] and repo_probe["top_level"]:
        try:
            workspace = ensure_git_worktree(str(repo_probe["top_level"]), lane_id, task_id)
            workspace_kind = "git-worktree"
            repo_root = str(repo_probe["top_level"])
            head_sha = repo_probe["head_sha"]
        except LaneExecutionError as exc:
            workspace = task_workspace_path(lane_id, task_id)
            workspace.mkdir(parents=True, exist_ok=True)
            fallback_reason = str(exc)
    else:
        workspace = task_workspace_path(lane_id, task_id)
        workspace.mkdir(parents=True, exist_ok=True)
        fallback_reason = repo_probe["error"] or "git worktree unavailable"

    metadata = {
        "task_id": task_id,
        "lane_id": lane_id,
        "title": task["title"],
        "prompt": task["prompt"],
        "prepared_at": now_utc(),
        "workspace_path": str(workspace),
        "workspace_kind": workspace_kind,
        "repo_root": repo_root,
        "head_sha": head_sha,
        "fallback_reason": fallback_reason,
    }
    write_json(workspace / "codexlab-task.json", metadata)
    append_event(
        "workspace_prepared",
        lane_id=lane_id,
        task_id=task_id,
        workspace_kind=workspace_kind,
        workspace_path=str(workspace),
        repo_root=repo_root,
        fallback_reason=fallback_reason,
    )
    return workspace


def task_dir(task_id: str) -> Path:
    return TASKS_DIR / task_id


def ensure_task_dirs(task_id: str) -> Path:
    base = task_dir(task_id)
    (base / "submissions").mkdir(parents=True, exist_ok=True)
    (base / "evaluations").mkdir(parents=True, exist_ok=True)
    (base / "current_best").mkdir(parents=True, exist_ok=True)
    return base


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id


def start_run(
    conn: sqlite3.Connection,
    lane_id: str,
    task_id: str,
    mode: str,
    command: list[str],
    cwd: Path,
    codex_home: Path,
) -> RunHandle:
    run_id = allocate_id(conn, "next_run_id", "RUN")
    target = run_dir(run_id)
    target.mkdir(parents=True, exist_ok=True)
    (target / "command.json").write_text(json.dumps(command, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    conn.execute(
        """
        INSERT INTO runs(run_id, lane_id, task_id, submission_id, mode, status, command, cwd, codex_home, started_at, finished_at, exit_code)
        VALUES(?, ?, ?, NULL, ?, 'running', ?, ?, ?, ?, NULL, NULL)
        """,
        (run_id, lane_id, task_id, mode, json.dumps(command, ensure_ascii=True), str(cwd), str(codex_home), now_utc()),
    )
    conn.execute(
        """
        UPDATE lanes
        SET active_run_id = ?, notes = '', updated_at = ?
        WHERE lane_id = ?
        """,
        (run_id, now_utc(), lane_id),
    )
    conn.commit()
    append_event("run_started", run_id=run_id, lane_id=lane_id, task_id=task_id, mode=mode)
    return RunHandle(run_id=run_id, lane_id=lane_id, run_dir=target)


def finish_run(
    conn: sqlite3.Connection,
    handle: RunHandle,
    status: str,
    exit_code: int,
    submission_id: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE runs
        SET status = ?, finished_at = ?, exit_code = ?, submission_id = COALESCE(?, submission_id)
        WHERE run_id = ?
        """,
        (status, now_utc(), exit_code, submission_id, handle.run_id),
    )
    conn.execute(
        """
        UPDATE lanes
        SET active_run_id = NULL, updated_at = ?
        WHERE lane_id = ? AND active_run_id = ?
        """,
        (now_utc(), handle.lane_id, handle.run_id),
    )
    conn.commit()
    append_event("run_finished", run_id=handle.run_id, lane_id=handle.lane_id, status=status, exit_code=exit_code)


def set_lane_error(conn: sqlite3.Connection, lane_id: str, message: str) -> None:
    conn.execute(
        """
        UPDATE lanes
        SET status = 'error', notes = ?, updated_at = ?
        WHERE lane_id = ?
        """,
        (message[:400], now_utc(), lane_id),
    )
    conn.commit()
    append_event("lane_error", lane_id=lane_id, message=message[:400])


def queue_lane_reservation(
    conn: sqlite3.Connection,
    lane_id: str,
    task_id: str,
    reservation_type: str,
    reason: str,
) -> str:
    reservation_id = allocate_id(conn, "next_reservation_id", "R")
    priority_map = {
        "duel_retry": 0,
        "recovered_task": 5,
        "new_task": 10,
    }
    priority = priority_map.get(reservation_type, 10)
    conn.execute(
        """
        INSERT INTO reservations(reservation_id, lane_id, task_id, reservation_type, priority, status, reason, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, 'queued', ?, ?, ?)
        """,
        (reservation_id, lane_id, task_id, reservation_type, priority, reason, now_utc(), now_utc()),
    )
    append_event(
        "lane_reserved",
        reservation_id=reservation_id,
        lane_id=lane_id,
        task_id=task_id,
        reservation_type=reservation_type,
        reason=reason,
    )
    return reservation_id


def promote_lane_from_queue(conn: sqlite3.Connection, lane_id: str) -> dict[str, Any] | None:
    queued = lane_queue(conn, lane_id)
    if not queued:
        return None
    reservation = queued[0]
    status = "retrying" if reservation["reservation_type"] == "duel_retry" else "assigned"
    conn.execute(
        """
        UPDATE lanes
        SET status = ?, active_task_id = ?, active_submission_id = NULL, notes = '', updated_at = ?
        WHERE lane_id = ?
        """,
        (status, reservation["task_id"], now_utc(), lane_id),
    )
    conn.execute(
        "UPDATE reservations SET status = 'promoted', updated_at = ? WHERE reservation_id = ?",
        (now_utc(), reservation["reservation_id"]),
    )
    append_event(
        "lane_promoted",
        lane_id=lane_id,
        task_id=reservation["task_id"],
        reservation_id=reservation["reservation_id"],
        reservation_type=reservation["reservation_type"],
    )
    return {
        "lane_id": lane_id,
        "task_id": reservation["task_id"],
        "reservation_id": reservation["reservation_id"],
        "reservation_type": reservation["reservation_type"],
    }


def promote_idle_lanes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    promotions: list[dict[str, Any]] = []
    for lane in conn.execute(
        """
        SELECT lane_id FROM lanes
        WHERE status = 'idle'
          AND active_task_id IS NULL
          AND active_submission_id IS NULL
          AND active_run_id IS NULL
        ORDER BY lane_id ASC
        """
    ).fetchall():
        promoted = promote_lane_from_queue(conn, lane["lane_id"])
        if promoted is not None:
            promotions.append(promoted)
    return promotions


def release_lane_and_promote(conn: sqlite3.Connection, lane_id: str, finished_task_id: str | None = None) -> None:
    lane = fetch_lane(conn, lane_id)
    if finished_task_id is not None and lane["active_task_id"] not in (None, finished_task_id):
        return
    conn.execute(
        """
        UPDATE lanes
        SET status = 'idle', active_task_id = NULL, active_submission_id = NULL, notes = '', updated_at = ?
        WHERE lane_id = ?
        """,
        (now_utc(), lane_id),
    )
    promote_lane_from_queue(conn, lane_id)


def assign_or_reserve_task(conn: sqlite3.Connection, lane_id: str, task_id: str, reservation_type: str, reason: str) -> None:
    lane = fetch_lane(conn, lane_id)
    desired_status = "retrying" if reservation_type == "duel_retry" else "assigned"
    if lane["active_task_id"] is None:
        conn.execute(
            """
            UPDATE lanes
            SET status = ?, active_task_id = ?, active_submission_id = NULL, notes = '', updated_at = ?
            WHERE lane_id = ?
            """,
            (desired_status, task_id, now_utc(), lane_id),
        )
        append_event(
            "lane_assigned",
            lane_id=lane_id,
            task_id=task_id,
            reservation_type=reservation_type,
            reason=reason,
        )
        return

    if lane["active_task_id"] == task_id and reservation_type == "duel_retry":
        conn.execute(
            """
            UPDATE lanes
            SET status = 'retrying', active_submission_id = NULL, notes = '', updated_at = ?
            WHERE lane_id = ?
            """,
            (now_utc(), lane_id),
        )
        append_event("lane_retrying", lane_id=lane_id, task_id=task_id, reason=reason)
        return

    queue_lane_reservation(conn, lane_id, task_id, reservation_type, reason)


def sync_task_state(conn: sqlite3.Connection, task_id: str) -> None:
    task = fetch_task(conn, task_id)
    base = ensure_task_dirs(task_id)
    submissions = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM submissions WHERE task_id = ? ORDER BY created_at ASC, submission_id ASC",
            (task_id,),
        ).fetchall()
    ]
    evaluations = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM evaluations WHERE task_id = ? ORDER BY created_at ASC, evaluation_id ASC",
            (task_id,),
        ).fetchall()
    ]
    snapshot = {
        "task": dict(task),
        "submissions": submissions,
        "evaluations": evaluations,
    }
    (base / "state.json").write_text(json.dumps(snapshot, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    published_id = task["published_submission_id"]
    current_best = base / "current_best" / "current.json"
    if published_id:
        published = fetch_submission(conn, published_id)
        current_best.write_text(
            json.dumps({"task_id": task_id, "submission": dict(published)}, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
    elif current_best.exists():
        current_best.unlink()


def submission_brief(conn: sqlite3.Connection, submission_id: str | None) -> dict[str, Any] | None:
    if not submission_id:
        return None
    row = query_submission(conn, submission_id)
    body = read_submission_body(row["artifact_path"])
    return {
        "submission_id": row["submission_id"],
        "lane_id": row["lane_id"],
        "phase": row["phase"],
        "round_number": row["round_number"],
        "retry_number": row["retry_number"],
        "status": row["status"],
        "summary": row["summary"],
        "body_preview": textwrap.shorten(body.replace("\n", " "), width=180, placeholder="...") if body else "",
        "published": bool(row["published"]),
        "created_at": row["created_at"],
    }


def latest_evaluation_brief(conn: sqlite3.Connection, task_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT * FROM evaluations
        WHERE task_id = ?
        ORDER BY created_at DESC, evaluation_id DESC
        LIMIT 1
        """,
        (task_id,),
    ).fetchone()
    if row is None:
        return None
    try:
        scorecard = json.loads(row["scorecard_json"])
    except json.JSONDecodeError:
        scorecard = {}
    return {
        "evaluation_id": row["evaluation_id"],
        "created_at": row["created_at"],
        "winner_submission_id": row["winner_submission_id"],
        "loser_submission_id": row["loser_submission_id"],
        "winner_lane_id": row["winner_lane_id"],
        "loser_lane_id": row["loser_lane_id"],
        "winner_score": row["winner_score"],
        "loser_score": row["loser_score"],
        "score_delta": row["score_delta"],
        "rationale": row["rationale"],
        "loser_brief": row["loser_brief"],
        "swap_occurred": bool(row["swap_occurred"]),
        "masterpiece_locked": bool(row["masterpiece_locked"]),
        "left_submission_id": scorecard.get("left_submission_id"),
        "right_submission_id": scorecard.get("right_submission_id"),
        "left_total": scorecard.get("left_total"),
        "right_total": scorecard.get("right_total"),
        "winner_rubric": scorecard.get("winner_rubric"),
        "loser_rubric": scorecard.get("loser_rubric"),
    }


def status_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    lanes = []
    for row in conn.execute("SELECT * FROM lanes ORDER BY lane_id ASC").fetchall():
        queued = lane_queue(conn, row["lane_id"])
        item = dict(row)
        item["queued_reservations"] = [
            {
                "reservation_id": entry["reservation_id"],
                "task_id": entry["task_id"],
                "reservation_type": entry["reservation_type"],
                "priority": entry["priority"],
            }
            for entry in queued
        ]
        lanes.append(item)

    tasks = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM tasks ORDER BY created_at DESC, task_id DESC"
        ).fetchall()
    ]
    return {"root": str(ROOT), "db_path": str(DB_PATH), "executor": DEFAULT_EXECUTOR, "lanes": lanes, "tasks": tasks}


def dashboard_snapshot(conn: sqlite3.Connection, task_id: str | None = None) -> dict[str, Any]:
    base = status_snapshot(conn)
    daemon = daemon_runtime_snapshot()
    recovery = recovery_snapshot(conn)
    task_queue_map: dict[str, list[dict[str, Any]]] = {}
    for lane in base["lanes"]:
        for reservation in lane["queued_reservations"]:
            task_queue_map.setdefault(reservation["task_id"], []).append(
                {
                    "lane_id": lane["lane_id"],
                    "reservation_id": reservation["reservation_id"],
                    "reservation_type": reservation["reservation_type"],
                    "priority": reservation["priority"],
                }
            )

    tasks: list[dict[str, Any]] = []
    for raw_task in base["tasks"]:
        if task_id and raw_task["task_id"] != task_id:
            continue
        item = dict(raw_task)
        item["prompt_preview"] = textwrap.shorten(item["prompt"].replace("\n", " "), width=120, placeholder="...")
        item["published_submission"] = submission_brief(conn, item["published_submission_id"])
        item["champion_submission"] = submission_brief(conn, item["champion_submission_id"])
        item["challenger_submission"] = submission_brief(conn, item["challenger_submission_id"])
        item["latest_evaluation"] = latest_evaluation_brief(conn, item["task_id"])
        item["queued_on_lanes"] = sorted(task_queue_map.get(item["task_id"], []), key=lambda entry: (entry["priority"], entry["lane_id"]))
        tasks.append(item)

    summary = {
        "task_total": len(tasks),
        "task_locked": sum(1 for task in tasks if task["masterpiece_locked"]),
        "task_retrying": sum(1 for task in tasks if task["status"] == "challenger_retrying"),
        "task_in_progress": sum(1 for task in tasks if task["status"] not in {"masterpiece_locked", "cancelled"}),
        "lane_idle": sum(1 for lane in base["lanes"] if lane["status"] == "idle"),
        "lane_busy": sum(1 for lane in base["lanes"] if lane["status"] != "idle"),
        "queued_reservations": sum(len(lane["queued_reservations"]) for lane in base["lanes"]),
        "stale_runs": len(recovery["stale_runs"]),
        "repairable_lanes": len(recovery["lane_repairs"]),
    }
    return {
        "root": base["root"],
        "db_path": base["db_path"],
        "executor": base["executor"],
        "daemon": daemon,
        "summary": summary,
        "lanes": base["lanes"],
        "tasks": tasks,
    }


def runs_snapshot(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT * FROM runs
            ORDER BY started_at DESC, run_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]


def workspace_status_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    lanes = {row["lane_id"]: dict(row) for row in conn.execute("SELECT * FROM lanes ORDER BY lane_id ASC").fetchall()}
    tasks = {row["task_id"]: dict(row) for row in conn.execute("SELECT * FROM tasks").fetchall()}
    queued_task_ids = {
        row["task_id"]
        for row in conn.execute("SELECT DISTINCT task_id FROM reservations WHERE status = 'queued'").fetchall()
    }
    items: list[dict[str, Any]] = []
    for lane_id, _lane_type in ALL_LANES:
        for base in (
            lane_paths(lane_id)["workspace"] / "tasks",
            lane_paths(lane_id)["workspace"] / "worktrees",
        ):
            if not base.exists():
                continue
            for manifest_path in sorted(base.glob("*/codexlab-task.json")):
                payload = hydrate_workspace_metadata(manifest_path)
                lane = lanes.get(lane_id, {})
                task = tasks.get(payload.get("task_id", ""), {})
                payload["manifest_path"] = str(manifest_path)
                payload["lane_status"] = lane.get("status")
                payload["lane_active_task_id"] = lane.get("active_task_id")
                payload["task_status"] = task.get("status")
                payload["task_is_queued"] = payload.get("task_id") in queued_task_ids
                items.append(payload)
    return {
        "root": str(ROOT),
        "target_repo": str(target_repo_path()),
        "items": items,
    }


def query_task(conn: sqlite3.Connection, task_id: str | None) -> sqlite3.Row | None:
    if not task_id:
        return None
    return conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()


def query_run(conn: sqlite3.Connection, run_id: str | None) -> sqlite3.Row | None:
    if not run_id:
        return None
    return conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()


def query_submission(conn: sqlite3.Connection, submission_id: str | None) -> sqlite3.Row | None:
    if not submission_id:
        return None
    return conn.execute("SELECT * FROM submissions WHERE submission_id = ?", (submission_id,)).fetchone()


def infer_lane_resume_status(task: sqlite3.Row | None, lane: sqlite3.Row, submission: sqlite3.Row | None) -> tuple[str, str | None, str | None]:
    next_task_id = lane["active_task_id"] if task is not None else None
    next_submission_id = lane["active_submission_id"] if submission is not None else None
    if task is not None and bool(task["masterpiece_locked"]):
        return "idle", None, None
    if lane["lane_type"] == "evaluator":
        return ("assigned" if next_task_id else "idle"), next_task_id, None
    if next_submission_id:
        return "waiting_eval", next_task_id, next_submission_id
    if next_task_id:
        if task is not None and task["champion_submission_id"] and task["challenger_lane_id"] == lane["lane_id"]:
            return "retrying", next_task_id, None
        return "assigned", next_task_id, None
    return "idle", None, None


def lane_recovery_action(conn: sqlite3.Connection, lane: sqlite3.Row) -> dict[str, Any] | None:
    if not lane["active_run_id"] and lane["status"] != "error":
        return None
    run = query_run(conn, lane["active_run_id"])
    task = query_task(conn, lane["active_task_id"])
    submission = query_submission(conn, lane["active_submission_id"])
    next_status, next_task_id, next_submission_id = infer_lane_resume_status(task, lane, submission)

    if task is not None and bool(task["masterpiece_locked"]):
        reason = "task already locked; clearing stranded lane state"
    elif lane["status"] == "error":
        if lane["lane_type"] == "evaluator":
            reason = "cleared evaluator error state and reopened evaluation lane"
        elif next_submission_id:
            reason = "cleared worker error state and restored waiting submission"
        elif next_task_id:
            reason = "cleared worker error state and reopened task attempt"
        else:
            reason = "cleared worker error state with missing task context"
    elif lane["lane_type"] == "evaluator":
        reason = "cleared stale evaluator run"
    elif next_submission_id:
        reason = "worker submission already existed; cleared stale run handle"
    elif next_task_id:
        reason = "worker run abandoned; lane reopened for retry"
    else:
        reason = "cleared lane with missing task or submission references"

    return {
        "lane_id": lane["lane_id"],
        "lane_type": lane["lane_type"],
        "run_id": lane["active_run_id"],
        "run_exists": bool(run),
        "run_status": run["status"] if run else None,
        "repair_source": "error_state" if lane["status"] == "error" else "stale_run",
        "current_status": lane["status"],
        "current_task_id": lane["active_task_id"],
        "current_submission_id": lane["active_submission_id"],
        "next_status": next_status,
        "next_task_id": next_task_id,
        "next_submission_id": next_submission_id,
        "reason": reason,
    }


def recovery_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    runtime = daemon_runtime_snapshot()
    active_run_ids = {
        row["active_run_id"]
        for row in conn.execute("SELECT active_run_id FROM lanes WHERE active_run_id IS NOT NULL").fetchall()
    }
    stale_runs = []
    for row in conn.execute(
        """
        SELECT * FROM runs
        WHERE status = 'running'
        ORDER BY started_at ASC, run_id ASC
        """
    ).fetchall():
        item = dict(row)
        item["referenced_by_lane"] = row["run_id"] in active_run_ids
        stale_runs.append(item)

    lane_repairs = []
    for lane in conn.execute(
        """
        SELECT * FROM lanes
        WHERE active_run_id IS NOT NULL
           OR status = 'error'
        ORDER BY lane_id ASC
        """
    ).fetchall():
        action = lane_recovery_action(conn, lane)
        if action is not None:
            lane_repairs.append(action)

    return {
        "daemon": runtime,
        "stale_runs": stale_runs,
        "lane_repairs": lane_repairs,
    }


def apply_recovery_plan(conn: sqlite3.Connection, snapshot: dict[str, Any], *, requeue: bool = False) -> dict[str, Any]:
    if snapshot["daemon"]["running"]:
        raise SystemExit("Cannot apply recovery while the daemon is still running")

    abandoned_runs: list[str] = []
    repaired_lanes: list[str] = []
    requeued_reservations: list[dict[str, Any]] = []
    affected_tasks: set[str] = set()

    for run in snapshot["stale_runs"]:
        conn.execute(
            """
            UPDATE runs
            SET status = 'abandoned',
                finished_at = COALESCE(finished_at, ?),
                exit_code = COALESCE(exit_code, 125)
            WHERE run_id = ? AND status = 'running'
            """,
            (now_utc(), run["run_id"]),
        )
        abandoned_runs.append(run["run_id"])
        append_event("run_recovered", run_id=run["run_id"], lane_id=run["lane_id"], task_id=run["task_id"])
        if run["task_id"]:
            affected_tasks.add(run["task_id"])

    for action in snapshot["lane_repairs"]:
        should_requeue = (
            requeue
            and action["lane_type"] == "worker"
            and action["next_task_id"] is not None
            and action["next_submission_id"] is None
            and action["next_status"] in {"assigned", "retrying"}
        )
        if should_requeue:
            conn.execute(
                """
                UPDATE lanes
                SET status = 'idle',
                    active_task_id = NULL,
                    active_submission_id = NULL,
                    active_run_id = NULL,
                    notes = ?,
                    updated_at = ?
                WHERE lane_id = ?
                """,
                (f"requeued: {action['reason']}"[:400], now_utc(), action["lane_id"]),
            )
            reservation_type = "duel_retry" if action["next_status"] == "retrying" else "recovered_task"
            reservation_id = queue_lane_reservation(
                conn,
                action["lane_id"],
                action["next_task_id"],
                reservation_type,
                f"recovery requeue: {action['reason']}",
            )
            requeued_reservations.append(
                {
                    "reservation_id": reservation_id,
                    "lane_id": action["lane_id"],
                    "task_id": action["next_task_id"],
                    "reservation_type": reservation_type,
                }
            )
        else:
            conn.execute(
                """
                UPDATE lanes
                SET status = ?,
                    active_task_id = ?,
                    active_submission_id = ?,
                    active_run_id = NULL,
                    notes = ?,
                    updated_at = ?
                WHERE lane_id = ?
                """,
                (
                    action["next_status"],
                    action["next_task_id"],
                    action["next_submission_id"],
                    action["reason"][:400],
                    now_utc(),
                    action["lane_id"],
                ),
            )
        repaired_lanes.append(action["lane_id"])
        append_event(
            "lane_recovered",
            lane_id=action["lane_id"],
            run_id=action["run_id"],
            next_status=action["next_status"],
            next_task_id=action["next_task_id"],
            next_submission_id=action["next_submission_id"],
            requeued=bool(should_requeue),
        )
        if action["current_task_id"]:
            affected_tasks.add(action["current_task_id"])
        if action["next_task_id"]:
            affected_tasks.add(action["next_task_id"])

    conn.commit()
    for task_id in sorted(affected_tasks):
        if query_task(conn, task_id) is not None:
            sync_task_state(conn, task_id)

    return {
        "abandoned_runs": abandoned_runs,
        "repaired_lanes": repaired_lanes,
        "requeued_reservations": requeued_reservations,
    }


def latest_round_number(conn: sqlite3.Connection, task_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(round_number), 0) FROM submissions WHERE task_id = ?",
        (task_id,),
    ).fetchone()
    return int(row[0] or 0)


def next_retry_number(conn: sqlite3.Connection, task_id: str, lane_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(retry_number), -1) FROM submissions WHERE task_id = ? AND lane_id = ?",
        (task_id, lane_id),
    ).fetchone()
    return int(row[0] or -1) + 1


def last_loser_brief(conn: sqlite3.Connection, task_id: str, lane_id: str) -> str:
    row = conn.execute(
        """
        SELECT loser_brief
        FROM evaluations
        WHERE task_id = ? AND loser_lane_id = ?
        ORDER BY created_at DESC, evaluation_id DESC
        LIMIT 1
        """,
        (task_id, lane_id),
    ).fetchone()
    if row is None:
        return "No retry brief yet. Produce the strongest draft you can."
    return str(row["loser_brief"])


def read_submission_body(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def load_template(filename: str, default_text: str) -> str:
    path = TEMPLATES_DIR / filename
    if path.exists():
        return path.read_text(encoding="utf-8")
    return default_text


def parse_json_object_arg(label: str, raw: str | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{label} must be valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{label} must decode to a JSON object")
    return payload


def rubric_item_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            criterion: {"type": "number", "minimum": 0, "maximum": 5}
            for criterion in RUBRIC_CRITERIA
        },
        "required": list(RUBRIC_CRITERIA),
    }


def worker_output_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "summary": {"type": "string"},
            "body": {"type": "string"},
        },
        "required": ["summary", "body"],
    }


def evaluator_output_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "left_rubric": rubric_item_schema(),
            "right_rubric": rubric_item_schema(),
            "rationale": {"type": "string"},
            "loser_brief": {"type": "string"},
        },
        "required": ["left_rubric", "right_rubric", "rationale", "loser_brief"],
    }


def build_worker_prompt(conn: sqlite3.Connection, task: sqlite3.Row, lane_id: str) -> str:
    champion_summary = "No published champion yet."
    champion_body = "No published champion yet."
    if task["published_submission_id"]:
        champion = fetch_submission(conn, task["published_submission_id"])
        champion_summary = champion["summary"]
        champion_body = read_submission_body(champion["artifact_path"]) or champion["summary"]
    template = load_template("worker_prompt.md", DEFAULT_WORKER_TEMPLATE)
    return template.format(
        lane_id=lane_id,
        task_id=task["task_id"],
        task_title=task["title"],
        task_prompt=task["prompt"],
        champion_summary=champion_summary,
        champion_body=champion_body,
        loser_brief=last_loser_brief(conn, task["task_id"], lane_id),
    )


def build_evaluator_prompt(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    left: sqlite3.Row,
    right: sqlite3.Row,
) -> str:
    template = load_template("evaluator_prompt.md", DEFAULT_EVALUATOR_TEMPLATE)
    return template.format(
        task_id=task["task_id"],
        task_title=task["title"],
        task_prompt=task["prompt"],
        left_submission_id=left["submission_id"],
        left_lane_id=left["lane_id"],
        left_summary=left["summary"],
        left_body=read_submission_body(left["artifact_path"]) or left["summary"],
        right_submission_id=right["submission_id"],
        right_lane_id=right["lane_id"],
        right_summary=right["summary"],
        right_body=read_submission_body(right["artifact_path"]) or right["summary"],
    )


def clamp_rubric_score(value: float) -> float:
    return max(0.0, min(5.0, round(float(value), 2)))


def rubric_from_total(total_score: float) -> dict[str, float]:
    rating = clamp_rubric_score(float(total_score) / 20.0)
    return {criterion: rating for criterion in RUBRIC_CRITERIA}


def normalize_rubric(raw: dict[str, Any], fallback_total: float | None = None) -> dict[str, float]:
    if not isinstance(raw, dict):
        if fallback_total is None:
            raise LaneExecutionError("Rubric payload must be an object")
        return rubric_from_total(fallback_total)
    missing = [criterion for criterion in RUBRIC_CRITERIA if criterion not in raw]
    if missing:
        if fallback_total is None:
            raise LaneExecutionError(f"Rubric payload is missing fields: {', '.join(missing)}")
        filled = dict(raw)
        base = rubric_from_total(fallback_total)
        for criterion in missing:
            filled[criterion] = base[criterion]
        raw = filled
    return {criterion: clamp_rubric_score(float(raw[criterion])) for criterion in RUBRIC_CRITERIA}


def rubric_total(rubric: dict[str, float]) -> float:
    total = 0.0
    for criterion, weight in RUBRIC_WEIGHTS.items():
        total += (float(rubric[criterion]) / 5.0) * weight
    return round(total, 2)


def extract_lane_rubric(entry: dict[str, Any], lane_id: str) -> dict[str, float] | None:
    lane_key = f"{lane_id}_rubric"
    if lane_key in entry:
        return normalize_rubric(entry[lane_key])
    lane_value = entry.get(lane_id)
    if isinstance(lane_value, dict):
        return normalize_rubric(lane_value)
    return None


def parse_evaluator_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise LaneExecutionError("Evaluator output must be a JSON object")
    left_rubric = normalize_rubric(payload.get("left_rubric", {}), payload.get("left_score"))
    right_rubric = normalize_rubric(payload.get("right_rubric", {}), payload.get("right_score"))
    left_total = rubric_total(left_rubric)
    right_total = rubric_total(right_rubric)
    if left_total == right_total:
        raise LaneExecutionError("Evaluator produced a tied rubric total")
    rationale = str(payload.get("rationale", "")).strip()
    loser_brief = str(payload.get("loser_brief", "")).strip()
    if not rationale or not loser_brief:
        raise LaneExecutionError("Evaluator output is missing rationale or loser_brief")
    return {
        "left_rubric": left_rubric,
        "right_rubric": right_rubric,
        "left_total": left_total,
        "right_total": right_total,
        "rationale": rationale,
        "loser_brief": loser_brief,
    }


def load_mock_plan() -> dict[str, Any]:
    raw = os.environ.get("CODEXLAB_MOCK_PLAN", "").strip()
    if not raw:
        return {}
    candidate = Path(raw)
    if candidate.exists():
        return json.loads(candidate.read_text(encoding="utf-8"))
    return json.loads(raw)


def mock_worker_output(conn: sqlite3.Connection, task: sqlite3.Row, lane_id: str) -> dict[str, Any]:
    plan = load_mock_plan()
    attempt_index = next_retry_number(conn, task["task_id"], lane_id)
    planned = (
        plan.get("worker_outputs", {})
        .get(task["task_id"], {})
        .get(lane_id, [])
    )
    if attempt_index < len(planned):
        payload = planned[attempt_index]
        if isinstance(payload, dict):
            summary = str(payload.get("summary", f"{lane_id} attempt {attempt_index + 1}"))
            body = str(payload.get("body", summary))
        else:
            summary = str(payload)
            body = summary
    else:
        phase = "retry" if task["champion_submission_id"] else "initial"
        summary = f"{lane_id} {phase} attempt {attempt_index + 1} for {task['task_id']}"
        body = (
            f"Mock submission from {lane_id} for {task['task_id']}.\n"
            f"Attempt: {attempt_index + 1}\n"
            f"Task: {task['prompt'].strip()}\n"
        )
    return {
        "summary": summary,
        "body": body,
        "meta": {"executor": "mock", "attempt_index": attempt_index},
    }


def mock_evaluator_output(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    left: sqlite3.Row,
    right: sqlite3.Row,
) -> dict[str, Any]:
    plan = load_mock_plan()
    eval_index = int(task["total_evaluations"] or 0)
    planned = plan.get("evaluation_scores", {}).get(task["task_id"], [])
    if eval_index < len(planned):
        entry = planned[eval_index]
        left_rubric = extract_lane_rubric(entry, left["lane_id"])
        right_rubric = extract_lane_rubric(entry, right["lane_id"])
        if left_rubric is None:
            left_rubric = rubric_from_total(float(entry[left["lane_id"]]))
        if right_rubric is None:
            right_rubric = rubric_from_total(float(entry[right["lane_id"]]))
        rationale = str(entry.get("rationale", "mock plan evaluation"))
        loser_brief = str(entry.get("loser_brief", "Revise to beat the published champion on the next retry."))
    else:
        if task["champion_submission_id"] is None:
            lane_scores = {"worker-a": 91.0, "worker-b": 82.0}
        else:
            lane_scores = {
                task["champion_lane_id"]: 92.0,
                task["challenger_lane_id"]: 87.0 + float(min(int(task["challenger_failed_attempts"] or 0), 2)),
            }
        left_rubric = rubric_from_total(lane_scores.get(left["lane_id"], 80.0))
        right_rubric = rubric_from_total(lane_scores.get(right["lane_id"], 80.0))
        if rubric_total(left_rubric) == rubric_total(right_rubric):
            right_rubric["verification"] = clamp_rubric_score(right_rubric["verification"] - 0.1)
        rationale = "Mock evaluator compared the two submissions and kept the stronger one ahead."
        loser_brief = "Focus on correctness gaps and increase detail to beat the current champion."
    left_total = rubric_total(left_rubric)
    right_total = rubric_total(right_rubric)
    if left_total == right_total:
        raise LaneExecutionError("Mock evaluator produced a tied scorecard")
    return {
        "left_rubric": left_rubric,
        "right_rubric": right_rubric,
        "left_total": left_total,
        "right_total": right_total,
        "rationale": rationale,
        "loser_brief": loser_brief,
        "meta": {"executor": "mock", "eval_index": eval_index},
    }


def run_codex_command(
    command: list[str],
    prompt: str,
    env: dict[str, str],
    work_dir: Path,
    run_output_dir: Path,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    (run_output_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
    try:
        completed = subprocess.run(
            command,
            cwd=work_dir,
            input=prompt,
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        stdout_text = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", errors="replace")
        stderr_text = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", errors="replace")
        (run_output_dir / "stdout.log").write_text(stdout_text, encoding="utf-8")
        (run_output_dir / "stderr.log").write_text(stderr_text, encoding="utf-8")
        raise LaneExecutionError(
            f"codex exec timed out after {timeout_seconds:g}s",
            exit_code=124,
            run_status="timeout",
        ) from exc
    (run_output_dir / "stdout.log").write_text(completed.stdout, encoding="utf-8")
    (run_output_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
    return completed


def codex_worker_output(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    lane_id: str,
    handle: RunHandle,
    timeout_seconds: float,
) -> dict[str, Any]:
    paths = lane_paths(lane_id)
    workspace = task_workspace_path(lane_id, task["task_id"])
    prompt = build_worker_prompt(conn, task, lane_id)
    schema_path = handle.run_dir / "worker-output.schema.json"
    output_path = handle.run_dir / "worker-output.json"
    schema_path.write_text(json.dumps(worker_output_schema(), indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    command = [
        REAL_CODEX,
        "--ask-for-approval",
        "never",
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        "workspace-write",
        "--cd",
        str(workspace),
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--color",
        "never",
        "-",
    ]
    env = os.environ.copy()
    env["CODEX_HOME"] = str(paths["home"])
    completed = run_codex_command(command, prompt, env, workspace, handle.run_dir, timeout_seconds)
    if completed.returncode != 0:
        raise LaneExecutionError(f"worker codex exec failed with exit code {completed.returncode}", completed.returncode)
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LaneExecutionError("worker codex exec did not write an output file") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("summary"), str) or not isinstance(payload.get("body"), str):
        raise LaneExecutionError("worker codex exec wrote invalid JSON output")
    payload["meta"] = {"executor": "codex", "run_id": handle.run_id}
    return payload


def codex_evaluator_output(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    left: sqlite3.Row,
    right: sqlite3.Row,
    handle: RunHandle,
    timeout_seconds: float,
) -> dict[str, Any]:
    paths = lane_paths("evaluator")
    workspace = task_workspace_path("evaluator", task["task_id"])
    prompt = build_evaluator_prompt(conn, task, left, right)
    schema_path = handle.run_dir / "evaluator-output.schema.json"
    output_path = handle.run_dir / "evaluator-output.json"
    schema_path.write_text(json.dumps(evaluator_output_schema(), indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    command = [
        REAL_CODEX,
        "--ask-for-approval",
        "never",
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        "workspace-write",
        "--cd",
        str(workspace),
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--color",
        "never",
        "-",
    ]
    env = os.environ.copy()
    env["CODEX_HOME"] = str(paths["home"])
    completed = run_codex_command(command, prompt, env, workspace, handle.run_dir, timeout_seconds)
    if completed.returncode != 0:
        raise LaneExecutionError(f"evaluator codex exec failed with exit code {completed.returncode}", completed.returncode)
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LaneExecutionError("evaluator codex exec did not write an output file") from exc
    parsed = parse_evaluator_payload(payload)
    parsed["meta"] = {"executor": "codex", "run_id": handle.run_id}
    return parsed


def record_submission(
    conn: sqlite3.Connection,
    task_id: str,
    lane_id: str,
    summary: str,
    body: str,
    meta_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = fetch_task(conn, task_id)
    lane = fetch_lane(conn, lane_id)
    if lane["active_task_id"] != task_id:
        raise SystemExit(f"Lane {lane_id} is not actively assigned to {task_id}")

    submission_id = allocate_id(conn, "next_submission_id", "S")
    phase = "initial" if task["champion_submission_id"] is None else "retry"
    round_number = latest_round_number(conn, task_id) + 1
    retry_number = next_retry_number(conn, task_id, lane_id)
    submission_dir = ensure_task_dirs(task_id) / "submissions" / submission_id
    submission_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = submission_dir / "output.md"
    artifact_path.write_text(body.strip() + "\n", encoding="utf-8")
    metadata = {
        "submission_id": submission_id,
        "task_id": task_id,
        "lane_id": lane_id,
        "phase": phase,
        "round_number": round_number,
        "retry_number": retry_number,
        "summary": summary,
    }
    if meta_extra:
        metadata["runtime"] = meta_extra
    (submission_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    conn.execute(
        """
        INSERT INTO submissions(
            submission_id, task_id, lane_id, phase, round_number, retry_number,
            status, summary, artifact_path, created_at, meta_json
        )
        VALUES(?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?)
        """,
        (
            submission_id,
            task_id,
            lane_id,
            phase,
            round_number,
            retry_number,
            summary,
            str(artifact_path),
            now_utc(),
            json.dumps(metadata, ensure_ascii=True),
        ),
    )
    conn.execute(
        """
        UPDATE lanes
        SET status = 'waiting_eval', active_submission_id = ?, notes = '', updated_at = ?
        WHERE lane_id = ?
        """,
        (submission_id, now_utc(), lane_id),
    )
    conn.execute(
        "UPDATE tasks SET status = 'awaiting_evaluation', updated_at = ? WHERE task_id = ?",
        (now_utc(), task_id),
    )
    conn.execute(
        """
        UPDATE lanes
        SET status = 'assigned', active_task_id = ?, active_submission_id = NULL, notes = '', updated_at = ?
        WHERE lane_id = 'evaluator'
        """,
        (task_id, now_utc()),
    )
    conn.commit()
    sync_task_state(conn, task_id)
    append_event("submission_recorded", task_id=task_id, submission_id=submission_id, lane_id=lane_id)
    return {"submission_id": submission_id, "summary": summary, "artifact_path": str(artifact_path)}


def score_task(
    conn: sqlite3.Connection,
    task_id: str,
    left_submission_id: str,
    right_submission_id: str,
    rationale: str,
    loser_brief: str,
    left_score: float | None = None,
    right_score: float | None = None,
    left_rubric: dict[str, Any] | None = None,
    right_rubric: dict[str, Any] | None = None,
    scorecard_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = fetch_task(conn, task_id)
    left = fetch_submission(conn, left_submission_id)
    right = fetch_submission(conn, right_submission_id)
    if left["task_id"] != task_id or right["task_id"] != task_id:
        raise SystemExit("Both submissions must belong to the target task")
    left_rubric = normalize_rubric(left_rubric or {}, left_score)
    right_rubric = normalize_rubric(right_rubric or {}, right_score)
    left_total = rubric_total(left_rubric)
    right_total = rubric_total(right_rubric)
    if float(left_total) == float(right_total):
        raise SystemExit("Scores must not tie")

    winner = left if left_total > right_total else right
    loser = right if winner["submission_id"] == left["submission_id"] else left
    winner_score = float(left_total if winner["submission_id"] == left["submission_id"] else right_total)
    loser_score = float(right_total if winner["submission_id"] == left["submission_id"] else left_total)
    winner_rubric = left_rubric if winner["submission_id"] == left["submission_id"] else right_rubric
    loser_rubric = right_rubric if winner["submission_id"] == left["submission_id"] else left_rubric
    initial_duel = task["champion_submission_id"] is None
    evaluation_id = allocate_id(conn, "next_evaluation_id", "E")
    swap_occurred = 0
    total_evaluations = int(task["total_evaluations"] or 0) + 1
    role_swaps = int(task["role_swaps"] or 0)
    masterpiece_locked = 0
    challenger_failed_attempts = int(task["challenger_failed_attempts"] or 0)

    if initial_duel:
        champion_submission_id = winner["submission_id"]
        challenger_submission_id = loser["submission_id"]
        champion_lane_id = winner["lane_id"]
        challenger_lane_id = loser["lane_id"]
        champion_score = winner_score
        challenger_score = loser_score
        challenger_failed_attempts = 0
    else:
        if task["champion_submission_id"] not in (left["submission_id"], right["submission_id"]):
            raise SystemExit("One scored submission must be the current champion")
        current_champion_id = task["champion_submission_id"]
        if winner["submission_id"] == current_champion_id:
            champion_submission_id = current_champion_id
            champion_lane_id = task["champion_lane_id"]
            champion_score = winner_score
            challenger_submission_id = loser["submission_id"]
            challenger_lane_id = loser["lane_id"]
            challenger_score = loser_score
            challenger_failed_attempts += 1
        else:
            swap_occurred = 1
            role_swaps += 1
            champion_submission_id = winner["submission_id"]
            champion_lane_id = winner["lane_id"]
            champion_score = winner_score
            challenger_submission_id = task["champion_submission_id"]
            challenger_lane_id = task["champion_lane_id"]
            challenger_score = loser_score
            challenger_failed_attempts = 0

    if (
        challenger_failed_attempts >= int(task["max_retry_failures"])
        or total_evaluations >= int(task["max_total_evaluations"])
        or role_swaps >= int(task["max_role_swaps"])
    ):
        masterpiece_locked = 1

    task_status = "masterpiece_locked" if masterpiece_locked else "challenger_retrying"
    scorecard = {
        "left_submission_id": left["submission_id"],
        "right_submission_id": right["submission_id"],
        "left_lane_id": left["lane_id"],
        "right_lane_id": right["lane_id"],
        "left_rubric": left_rubric,
        "right_rubric": right_rubric,
        "left_total": left_total,
        "right_total": right_total,
        "rubric_weights": RUBRIC_WEIGHTS,
        "winner_submission_id": winner["submission_id"],
        "winner_lane_id": winner["lane_id"],
        "loser_submission_id": loser["submission_id"],
        "loser_lane_id": loser["lane_id"],
        "winner_score": winner_score,
        "loser_score": loser_score,
        "winner_rubric": winner_rubric,
        "loser_rubric": loser_rubric,
        "delta": winner_score - loser_score,
        "rationale": rationale,
        "loser_brief": loser_brief,
        "swap_occurred": bool(swap_occurred),
    }
    if scorecard_extra:
        scorecard["runtime"] = scorecard_extra
    conn.execute(
        """
        INSERT INTO evaluations(
            evaluation_id, task_id, left_submission_id, right_submission_id,
            winner_submission_id, loser_submission_id, winner_lane_id, loser_lane_id,
            winner_score, loser_score, score_delta, rationale, loser_brief,
            swap_occurred, masterpiece_locked, created_at, scorecard_json
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evaluation_id,
            task_id,
            left["submission_id"],
            right["submission_id"],
            winner["submission_id"],
            loser["submission_id"],
            winner["lane_id"],
            loser["lane_id"],
            winner_score,
            loser_score,
            winner_score - loser_score,
            rationale,
            loser_brief,
            swap_occurred,
            masterpiece_locked,
            now_utc(),
            json.dumps(scorecard, ensure_ascii=True),
        ),
    )
    conn.execute(
        """
        UPDATE submissions
        SET published = CASE WHEN submission_id = ? THEN 1 ELSE published END,
            superseded_by = CASE WHEN submission_id = ? THEN ? ELSE superseded_by END
        WHERE task_id = ?
        """,
        (champion_submission_id, task["champion_submission_id"], champion_submission_id, task_id),
    )
    conn.execute(
        """
        UPDATE tasks
        SET status = ?,
            updated_at = ?,
            published_submission_id = ?,
            champion_submission_id = ?,
            challenger_submission_id = ?,
            champion_lane_id = ?,
            challenger_lane_id = ?,
            champion_score = ?,
            challenger_score = ?,
            challenger_failed_attempts = ?,
            total_evaluations = ?,
            role_swaps = ?,
            masterpiece_locked = ?
        WHERE task_id = ?
        """,
        (
            task_status,
            now_utc(),
            champion_submission_id,
            champion_submission_id,
            challenger_submission_id,
            champion_lane_id,
            challenger_lane_id,
            champion_score,
            challenger_score,
            challenger_failed_attempts,
            total_evaluations,
            role_swaps,
            masterpiece_locked,
            task_id,
        ),
    )

    release_lane_and_promote(conn, winner["lane_id"], finished_task_id=task_id)
    if masterpiece_locked:
        release_lane_and_promote(conn, challenger_lane_id, finished_task_id=task_id)
    else:
        assign_or_reserve_task(conn, challenger_lane_id, task_id, "duel_retry", "challenger retry after evaluation")

    conn.execute(
        """
        UPDATE lanes
        SET status = 'idle', active_task_id = NULL, active_submission_id = NULL, notes = '', updated_at = ?
        WHERE lane_id = 'evaluator'
        """,
        (now_utc(),),
    )
    conn.commit()
    sync_task_state(conn, task_id)

    evaluation_file = ensure_task_dirs(task_id) / "evaluations" / f"{evaluation_id}.json"
    evaluation_file.write_text(json.dumps(scorecard | {"evaluation_id": evaluation_id}, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    append_event(
        "task_scored",
        task_id=task_id,
        evaluation_id=evaluation_id,
        winner_submission_id=winner["submission_id"],
        loser_submission_id=loser["submission_id"],
        swap_occurred=bool(swap_occurred),
        masterpiece_locked=bool(masterpiece_locked),
    )
    return {
        "evaluation_id": evaluation_id,
        "winner_submission_id": winner["submission_id"],
        "winner_lane_id": winner["lane_id"],
        "loser_submission_id": loser["submission_id"],
        "loser_lane_id": loser["lane_id"],
        "masterpiece_locked": bool(masterpiece_locked),
        "swap_occurred": bool(swap_occurred),
    }


def ready_evaluation_pair(conn: sqlite3.Connection, task_id: str) -> tuple[sqlite3.Row, sqlite3.Row] | None:
    task = fetch_task(conn, task_id)
    if task["masterpiece_locked"]:
        return None
    if task["champion_submission_id"] is None:
        ready: list[sqlite3.Row] = []
        for lane_id in WORKER_LANES:
            lane = fetch_lane(conn, lane_id)
            if lane["active_task_id"] == task_id and lane["active_submission_id"]:
                ready.append(fetch_submission(conn, lane["active_submission_id"]))
        if len(ready) < 2:
            return None
        ready.sort(key=lambda row: row["lane_id"])
        return ready[0], ready[1]

    challenger_lane_id = task["challenger_lane_id"]
    if not challenger_lane_id:
        return None
    challenger_lane = fetch_lane(conn, challenger_lane_id)
    if challenger_lane["active_task_id"] != task_id or not challenger_lane["active_submission_id"]:
        return None
    champion = fetch_submission(conn, task["champion_submission_id"])
    challenger = fetch_submission(conn, challenger_lane["active_submission_id"])
    return champion, challenger


def runnable_worker_lanes(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM lanes
        WHERE lane_type = 'worker'
          AND status IN ('assigned', 'retrying')
          AND active_task_id IS NOT NULL
          AND active_submission_id IS NULL
          AND active_run_id IS NULL
        ORDER BY lane_id ASC
        """
    ).fetchall()


def evaluator_runnable(conn: sqlite3.Connection) -> tuple[sqlite3.Row, tuple[sqlite3.Row, sqlite3.Row] | None]:
    lane = fetch_lane(conn, "evaluator")
    if lane["status"] != "assigned" or lane["active_task_id"] is None or lane["active_run_id"] is not None:
        return lane, None
    return lane, ready_evaluation_pair(conn, lane["active_task_id"])


def run_worker_once(conn: sqlite3.Connection, lane_id: str, executor: str, exec_timeout: float) -> dict[str, Any]:
    lane = fetch_lane(conn, lane_id)
    task = fetch_task(conn, lane["active_task_id"])
    paths = lane_paths(lane_id)
    workspace = prepare_task_workspace(conn, lane_id, task["task_id"])
    if executor == "mock":
        command = ["mock-worker", lane_id, task["task_id"]]
    elif executor == "codex":
        command = [REAL_CODEX, "--ask-for-approval", "never", "exec", "--skip-git-repo-check"]
    else:
        raise SystemExit(f"Unsupported executor: {executor}")
    handle = start_run(conn, lane_id, task["task_id"], f"{executor}:worker", command, workspace, paths["home"])
    try:
        if executor == "mock":
            payload = mock_worker_output(conn, task, lane_id)
        else:
            payload = codex_worker_output(conn, task, lane_id, handle, exec_timeout)
        submission = record_submission(
            conn,
            task["task_id"],
            lane_id,
            summary=str(payload["summary"]).strip(),
            body=str(payload["body"]).strip(),
            meta_extra=payload.get("meta"),
        )
        finish_run(conn, handle, "completed", 0, submission["submission_id"])
        return {
            "type": "worker_submission",
            "lane_id": lane_id,
            "task_id": task["task_id"],
            "submission_id": submission["submission_id"],
        }
    except Exception as exc:
        exit_code = exc.exit_code if isinstance(exc, LaneExecutionError) else 1
        run_status = exc.run_status if isinstance(exc, LaneExecutionError) else "failed"
        finish_run(conn, handle, run_status, exit_code)
        set_lane_error(conn, lane_id, str(exc))
        return {
            "type": "worker_error",
            "lane_id": lane_id,
            "task_id": task["task_id"],
            "error": str(exc),
        }


def run_evaluator_once(conn: sqlite3.Connection, executor: str, exec_timeout: float) -> dict[str, Any] | None:
    evaluator_lane, pair = evaluator_runnable(conn)
    if pair is None:
        return None
    left, right = pair
    task = fetch_task(conn, evaluator_lane["active_task_id"])
    paths = lane_paths("evaluator")
    workspace = prepare_task_workspace(conn, "evaluator", task["task_id"])
    if executor == "mock":
        command = ["mock-evaluator", task["task_id"], left["submission_id"], right["submission_id"]]
    elif executor == "codex":
        command = [REAL_CODEX, "--ask-for-approval", "never", "exec", "--skip-git-repo-check"]
    else:
        raise SystemExit(f"Unsupported executor: {executor}")
    handle = start_run(conn, "evaluator", task["task_id"], f"{executor}:evaluator", command, workspace, paths["home"])
    try:
        if executor == "mock":
            payload = mock_evaluator_output(conn, task, left, right)
        else:
            payload = codex_evaluator_output(conn, task, left, right, handle, exec_timeout)
        result = score_task(
            conn,
            task["task_id"],
            left["submission_id"],
            right["submission_id"],
            rationale=str(payload["rationale"]).strip(),
            loser_brief=str(payload["loser_brief"]).strip(),
            left_score=float(payload["left_total"]),
            right_score=float(payload["right_total"]),
            left_rubric=payload["left_rubric"],
            right_rubric=payload["right_rubric"],
            scorecard_extra=payload.get("meta"),
        )
        finish_run(conn, handle, "completed", 0)
        return {"type": "evaluation", "task_id": task["task_id"], **result}
    except Exception as exc:
        exit_code = exc.exit_code if isinstance(exc, LaneExecutionError) else 1
        run_status = exc.run_status if isinstance(exc, LaneExecutionError) else "failed"
        finish_run(conn, handle, run_status, exit_code)
        set_lane_error(conn, "evaluator", str(exc))
        return {"type": "evaluation_error", "task_id": task["task_id"], "error": str(exc)}


def run_tick(conn: sqlite3.Connection, executor: str, exec_timeout: float) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    promote_idle_lanes(conn)
    workers = runnable_worker_lanes(conn)
    for lane in workers:
        actions.append(run_worker_once(conn, lane["lane_id"], executor, exec_timeout))
    evaluation = run_evaluator_once(conn, executor, exec_timeout)
    if evaluation is not None:
        actions.append(evaluation)
    return actions


def has_runnable_work(conn: sqlite3.Connection) -> bool:
    if runnable_worker_lanes(conn):
        return True
    _lane, pair = evaluator_runnable(conn)
    return pair is not None


def daemon_state_payload(
    *,
    executor: str,
    interval: float,
    cycle_count: int,
    started_at: str,
    last_heartbeat: str,
    last_progress: bool,
    runnable_after: bool,
    stop_requested: bool,
    reason: str,
    last_actions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "pid": os.getpid(),
        "executor": executor,
        "interval": interval,
        "cycle_count": cycle_count,
        "started_at": started_at,
        "last_heartbeat": last_heartbeat,
        "last_progress": last_progress,
        "runnable_after": runnable_after,
        "stop_requested": stop_requested,
        "reason": reason,
        "last_actions": last_actions or [],
    }


def format_status(snapshot: dict[str, Any], task_id: str | None) -> str:
    lines: list[str] = []
    lines.append(f"Root: {snapshot['root']}")
    lines.append(f"DB: {snapshot['db_path']}")
    lines.append(f"Executor: {snapshot['executor']}")
    lines.append("")
    lines.append("Lanes:")
    for lane in snapshot["lanes"]:
        queue_desc = ", ".join(
            f"{entry['task_id']}:{entry['reservation_type']}" for entry in lane["queued_reservations"]
        ) or "-"
        notes = lane["notes"] or "-"
        lines.append(
            f"- {lane['lane_id']}: status={lane['status']} active_task={lane['active_task_id'] or '-'} "
            f"active_submission={lane['active_submission_id'] or '-'} active_run={lane['active_run_id'] or '-'} "
            f"queue={queue_desc} notes={notes}"
        )
    lines.append("")
    lines.append("Tasks:")
    tasks = snapshot["tasks"]
    if task_id:
        tasks = [task for task in tasks if task["task_id"] == task_id]
    for task in tasks[:10]:
        lines.append(
            f"- {task['task_id']}: status={task['status']} champion={task['champion_lane_id'] or '-'} "
            f"published={task['published_submission_id'] or '-'} retries={task['challenger_failed_attempts']} "
            f"evals={task['total_evaluations']} swaps={task['role_swaps']} masterpiece={bool(task['masterpiece_locked'])}"
        )
    if not tasks:
        lines.append("- none")
    return "\n".join(lines)


def format_dashboard(snapshot: dict[str, Any]) -> str:
    lines: list[str] = []
    daemon_state = snapshot["daemon"].get("state") or {}
    lines.append("CodexLab Dashboard")
    lines.append(f"Root: {snapshot['root']}")
    lines.append(
        f"Daemon: running={snapshot['daemon']['running']} reason={daemon_state.get('reason', '-')} "
        f"cycle_count={daemon_state.get('cycle_count', '-')}"
    )
    summary = snapshot["summary"]
    lines.append(
        "Summary: "
        f"tasks={summary['task_total']} in_progress={summary['task_in_progress']} "
        f"retrying={summary['task_retrying']} locked={summary['task_locked']} "
        f"lane_busy={summary['lane_busy']} queued={summary['queued_reservations']} "
        f"stale_runs={summary['stale_runs']} repairable_lanes={summary['repairable_lanes']}"
    )
    lines.append("")
    lines.append("Lanes:")
    for lane in snapshot["lanes"]:
        queue_desc = ", ".join(
            f"{entry['task_id']}:{entry['reservation_type']}" for entry in lane["queued_reservations"]
        ) or "-"
        lines.append(
            f"- {lane['lane_id']}: status={lane['status']} active_task={lane['active_task_id'] or '-'} "
            f"active_submission={lane['active_submission_id'] or '-'} queue={queue_desc}"
        )
    lines.append("")
    lines.append("Tasks:")
    if not snapshot["tasks"]:
        lines.append("- none")
        return "\n".join(lines)
    for task in snapshot["tasks"][:10]:
        champion = task["champion_submission"]
        challenger = task["challenger_submission"]
        latest_eval = task["latest_evaluation"]
        queue_desc = ", ".join(f"{entry['lane_id']}:{entry['reservation_type']}" for entry in task["queued_on_lanes"]) or "-"
        lines.append(
            f"- {task['task_id']}: status={task['status']} champion={task['champion_lane_id'] or '-'} "
            f"challenger={task['challenger_lane_id'] or '-'} evals={task['total_evaluations']} "
            f"retries={task['challenger_failed_attempts']} swaps={task['role_swaps']} "
            f"masterpiece={bool(task['masterpiece_locked'])}"
        )
        lines.append(f"  Prompt: {task['prompt_preview']}")
        if champion:
            lines.append(
                f"  Champion: {champion['submission_id']} {champion['lane_id']} "
                f"score={task['champion_score'] if task['champion_score'] is not None else '-'} "
                f"summary={champion['summary']}"
            )
        else:
            lines.append("  Champion: -")
        if challenger:
            lines.append(
                f"  Challenger: {challenger['submission_id']} {challenger['lane_id']} "
                f"score={task['challenger_score'] if task['challenger_score'] is not None else '-'} "
                f"summary={challenger['summary']}"
            )
        else:
            lines.append("  Challenger: -")
        lines.append(f"  Queue: {queue_desc}")
        if latest_eval:
            lines.append(
                f"  Latest Eval: {latest_eval['evaluation_id']} winner={latest_eval['winner_lane_id']} "
                f"delta={latest_eval['score_delta']} swap={latest_eval['swap_occurred']} "
                f"rationale={latest_eval['rationale']}"
            )
            lines.append(f"  Loser brief: {latest_eval['loser_brief']}")
        else:
            lines.append("  Latest Eval: -")
    return "\n".join(lines)


def cmd_doctor(args: argparse.Namespace) -> int:
    conn = connect()
    findings: list[dict[str, str]] = []
    required_paths = [
        ROOT,
        CONTROL_DIR,
        TASKS_DIR,
        AGENTS_DIR,
        LAB_HOME,
        ROOT / "AGENTS.md",
        ROOT / "docs" / "project-plan.md",
        TEMPLATES_DIR / "worker_prompt.md",
        TEMPLATES_DIR / "evaluator_prompt.md",
    ]
    for path in required_paths:
        findings.append(
            {
                "status": "ok" if path.exists() else "error",
                "message": f"{path} {'exists' if path.exists() else 'is missing'}",
            }
        )

    wrapper = shutil.which("codexlab")
    expected_wrapper = "/home/usow/bin/codexlab"
    findings.append(
        {
            "status": "ok" if wrapper == expected_wrapper else "warn",
            "message": f"codexlab on PATH -> {wrapper or 'missing'}",
        }
    )
    findings.append(
        {
            "status": "ok" if Path(REAL_CODEX).exists() else "warn",
            "message": f"real codex binary -> {REAL_CODEX}",
        }
    )
    repo_probe = git_probe(target_repo_path())
    repo_status = "ok" if repo_probe["is_repo"] and repo_probe["has_head"] else "warn"
    findings.append(
        {
            "status": repo_status,
            "message": (
                f"target repo -> {repo_probe['path']} "
                f"(is_repo={repo_probe['is_repo']} has_head={repo_probe['has_head']} "
                f"top={repo_probe['top_level'] or '-'} error={repo_probe['error'] or '-'})"
            ),
        }
    )

    agents_text = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    findings.append(
        {
            "status": "ok" if str(ROOT) in agents_text else "warn",
            "message": "AGENTS.md points at the active codexlab root",
        }
    )

    config_text = (LAB_HOME / "config.toml").read_text(encoding="utf-8") if (LAB_HOME / "config.toml").exists() else ""
    findings.append(
        {
            "status": "ok" if str(ROOT) in config_text else "warn",
            "message": "lab config.toml trusts the active codexlab root",
        }
    )

    lane_count = conn.execute("SELECT COUNT(*) FROM lanes").fetchone()[0]
    findings.append(
        {
            "status": "ok" if lane_count == 3 else "error",
            "message": f"lane registry contains {lane_count} rows",
        }
    )
    conn.close()

    if args.json:
        print(json.dumps({"findings": findings}, indent=2, ensure_ascii=True))
    else:
        for finding in findings:
            print(f"[{finding['status']}] {finding['message']}")
    return 0 if all(item["status"] != "error" for item in findings) else 1


def cmd_submit(args: argparse.Namespace) -> int:
    conn = connect()
    task_id = allocate_id(conn, "next_task_id", "T")
    title = args.title or textwrap.shorten(args.prompt.strip().replace("\n", " "), width=72, placeholder="...")
    created_at = now_utc()
    conn.execute(
        """
        INSERT INTO tasks(
            task_id, title, prompt, status, created_at, updated_at,
            masterpiece_locked, max_retry_failures, max_total_evaluations, max_role_swaps
        )
        VALUES(?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?)
        """,
        (task_id, title, args.prompt, created_at, created_at, MAX_RETRY_FAILURES, MAX_TOTAL_EVALUATIONS, MAX_ROLE_SWAPS),
    )
    base = ensure_task_dirs(task_id)
    (base / "brief.md").write_text(
        f"# {task_id}: {title}\n\n## Prompt\n\n{args.prompt.strip()}\n",
        encoding="utf-8",
    )
    for lane_id in WORKER_LANES:
        assign_or_reserve_task(conn, lane_id, task_id, "new_task", "initial task submission")
    conn.execute("UPDATE tasks SET status = 'in_progress', updated_at = ? WHERE task_id = ?", (now_utc(), task_id))
    conn.commit()
    sync_task_state(conn, task_id)
    append_event("task_submitted", task_id=task_id, title=title)
    print(f"task_id={task_id}")
    print(f"title={title}")
    conn.close()
    return 0


def cmd_record_submission(args: argparse.Namespace) -> int:
    conn = connect()
    submission = record_submission(conn, args.task_id, args.lane_id, args.summary, args.body or args.summary)
    print(f"submission_id={submission['submission_id']}")
    conn.close()
    return 0


def cmd_score(args: argparse.Namespace) -> int:
    conn = connect()
    left_rubric = parse_json_object_arg("--left-rubric-json", args.left_rubric_json)
    right_rubric = parse_json_object_arg("--right-rubric-json", args.right_rubric_json)
    if left_rubric is None and args.left_score is None:
        raise SystemExit("Provide either --left-score or --left-rubric-json")
    if right_rubric is None and args.right_score is None:
        raise SystemExit("Provide either --right-score or --right-rubric-json")
    result = score_task(
        conn,
        args.task_id,
        args.left,
        args.right,
        rationale=args.rationale,
        loser_brief=args.loser_brief,
        left_score=float(args.left_score) if args.left_score is not None else None,
        right_score=float(args.right_score) if args.right_score is not None else None,
        left_rubric=left_rubric,
        right_rubric=right_rubric,
    )
    print(f"evaluation_id={result['evaluation_id']}")
    print(f"winner_submission_id={result['winner_submission_id']}")
    print(f"masterpiece_locked={result['masterpiece_locked']}")
    conn.close()
    return 0


def cmd_tick(args: argparse.Namespace) -> int:
    conn = connect()
    actions = run_tick(conn, args.executor, args.exec_timeout)
    snapshot = status_snapshot(conn)
    conn.close()
    payload = {
        "executor": args.executor,
        "exec_timeout": args.exec_timeout,
        "actions": actions,
        "progress": bool(actions),
        "status": snapshot,
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
    else:
        print(f"executor={args.executor}")
        if actions:
            for action in actions:
                print(json.dumps(action, ensure_ascii=True))
        else:
            print("no_actions")
    return 0


def resume_recovered_work(
    conn: sqlite3.Connection,
    *,
    executor: str,
    exec_timeout: float,
    interval: float,
    max_ticks: int,
    until_idle: bool,
) -> dict[str, Any]:
    ticks: list[dict[str, Any]] = []
    snapshot = status_snapshot(conn)
    for tick_index in range(max_ticks):
        actions = run_tick(conn, executor, exec_timeout)
        runnable_after = has_runnable_work(conn)
        snapshot = status_snapshot(conn)
        tick_result = {
            "tick": tick_index + 1,
            "actions": actions,
            "progress": bool(actions),
            "runnable_after": runnable_after,
        }
        ticks.append(tick_result)
        if until_idle and not runnable_after:
            break
        if tick_index + 1 < max_ticks and interval > 0:
            time.sleep(interval)
    return {
        "executor": executor,
        "exec_timeout": exec_timeout,
        "interval": interval,
        "until_idle": until_idle,
        "ticks": ticks,
        "status": snapshot,
    }


def cmd_run_loop(args: argparse.Namespace) -> int:
    all_ticks: list[dict[str, Any]] = []
    for tick_index in range(args.max_ticks):
        conn = connect()
        actions = run_tick(conn, args.executor, args.exec_timeout)
        runnable_after = has_runnable_work(conn)
        snapshot = status_snapshot(conn)
        conn.close()
        tick_result = {
            "tick": tick_index + 1,
            "actions": actions,
            "progress": bool(actions),
            "runnable_after": runnable_after,
            "exec_timeout": args.exec_timeout,
        }
        all_ticks.append(tick_result)
        if not args.json:
            print(f"tick={tick_index + 1} progress={bool(actions)} actions={len(actions)} runnable_after={runnable_after}")
            for action in actions:
                print(json.dumps(action, ensure_ascii=True))
        if args.until_idle and not runnable_after:
            if args.json:
                print(json.dumps({"ticks": all_ticks, "status": snapshot}, indent=2, ensure_ascii=True))
            return 0
        if tick_index + 1 < args.max_ticks and args.interval > 0:
            time.sleep(args.interval)
    if args.json:
        print(json.dumps({"ticks": all_ticks, "status": snapshot}, indent=2, ensure_ascii=True))
    return 0


def cmd_daemon_run(args: argparse.Namespace) -> int:
    ensure_layout()
    stop_requested = False
    started_at = now_utc()
    cycle_count = 0

    def handle_stop(_signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    write_json(
        DAEMON_PID_FILE,
        {
            "pid": os.getpid(),
            "started_at": started_at,
            "executor": args.executor,
            "interval": args.interval,
            "exec_timeout": args.exec_timeout,
        },
    )
    append_event("daemon_started", pid=os.getpid(), executor=args.executor, interval=args.interval, exec_timeout=args.exec_timeout)

    try:
        while True:
            conn = connect()
            actions = run_tick(conn, args.executor, args.exec_timeout)
            runnable_after = has_runnable_work(conn)
            snapshot = status_snapshot(conn)
            conn.close()
            cycle_count += 1
            heartbeat = now_utc()
            reason = "running"
            if stop_requested:
                reason = "stop_requested"
            elif args.until_idle and not runnable_after:
                reason = "idle"
            elif args.max_cycles and cycle_count >= args.max_cycles:
                reason = "max_cycles"
            state = daemon_state_payload(
                executor=args.executor,
                interval=args.interval,
                cycle_count=cycle_count,
                started_at=started_at,
                last_heartbeat=heartbeat,
                last_progress=bool(actions),
                runnable_after=runnable_after,
                stop_requested=stop_requested,
                reason=reason,
                last_actions=actions,
            )
            state["exec_timeout"] = args.exec_timeout
            state["status_snapshot"] = snapshot
            write_json(DAEMON_STATE_FILE, state)
            append_event(
                "daemon_tick",
                pid=os.getpid(),
                cycle_count=cycle_count,
                executor=args.executor,
                action_count=len(actions),
                runnable_after=runnable_after,
                reason=reason,
                exec_timeout=args.exec_timeout,
            )
            if args.json:
                print(json.dumps({"tick": cycle_count, "actions": actions, "runnable_after": runnable_after}, ensure_ascii=True))
                sys.stdout.flush()
            if stop_requested or (args.until_idle and not runnable_after) or (args.max_cycles and cycle_count >= args.max_cycles):
                break
            time.sleep(args.interval)
    finally:
        final_state = read_json(DAEMON_STATE_FILE) or {}
        final_state.update(
            {
                "pid": os.getpid(),
                "stopped_at": now_utc(),
                "stop_requested": stop_requested,
                "running": False,
            }
        )
        write_json(DAEMON_STATE_FILE, final_state)
        try:
            current_pid = read_json(DAEMON_PID_FILE)
            if current_pid and int(current_pid.get("pid", 0) or 0) == os.getpid():
                DAEMON_PID_FILE.unlink()
        except FileNotFoundError:
            pass
        append_event("daemon_stopped", pid=os.getpid(), cycle_count=cycle_count)
    return 0


def start_daemon_process(
    *,
    executor: str,
    exec_timeout: float,
    interval: float,
    max_cycles: int,
    until_idle: bool,
) -> dict[str, Any]:
    ensure_layout()
    active = daemon_runtime_snapshot()
    if active["running"]:
        return {
            "started": False,
            "already_running": True,
            "pid": active["pid"],
            "log": str(DAEMON_LOG_FILE),
            "state_file": str(DAEMON_STATE_FILE),
            "command": active.get("pid_payload", {}).get("command"),
            "executor": executor,
            "interval": interval,
            "exec_timeout": exec_timeout,
            "max_cycles": max_cycles,
            "until_idle": until_idle,
        }
    command = [
        "python3",
        str(SCRIPT_PATH),
        "daemon",
        "run",
        "--executor",
        executor,
        "--interval",
        str(interval),
        "--exec-timeout",
        str(exec_timeout),
    ]
    if until_idle:
        command.append("--until-idle")
    if max_cycles:
        command.extend(["--max-cycles", str(max_cycles)])
    env = os.environ.copy()
    env["CODEXLAB_ROOT"] = str(ROOT)
    with DAEMON_LOG_FILE.open("a", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=env,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
        )
    write_json(
        DAEMON_PID_FILE,
        {
            "pid": process.pid,
            "started_at": now_utc(),
            "executor": executor,
            "interval": interval,
            "exec_timeout": exec_timeout,
            "command": command,
        },
    )
    return {
        "started": True,
        "already_running": False,
        "pid": process.pid,
        "log": str(DAEMON_LOG_FILE),
        "state_file": str(DAEMON_STATE_FILE),
        "command": command,
        "executor": executor,
        "interval": interval,
        "exec_timeout": exec_timeout,
        "max_cycles": max_cycles,
        "until_idle": until_idle,
    }


def cmd_daemon_start(args: argparse.Namespace) -> int:
    result = start_daemon_process(
        executor=args.executor,
        exec_timeout=args.exec_timeout,
        interval=args.interval,
        max_cycles=args.max_cycles,
        until_idle=args.until_idle,
    )
    if result["already_running"]:
        print(f"already_running pid={result['pid']}")
        return 0
    print(f"started pid={result['pid']}")
    print(f"log={result['log']}")
    return 0


def cmd_daemon_status(args: argparse.Namespace) -> int:
    snapshot = daemon_runtime_snapshot()
    if args.json:
        print(json.dumps(snapshot, indent=2, ensure_ascii=True))
        return 0
    print(f"running={snapshot['running']}")
    print(f"pid={snapshot['pid'] or '-'}")
    print(f"pid_file={snapshot['pid_file']}")
    print(f"state_file={snapshot['state_file']}")
    print(f"log_file={snapshot['log_file']}")
    state = snapshot["state"] or {}
    if state:
        print(f"cycle_count={state.get('cycle_count', '-')}")
        print(f"last_heartbeat={state.get('last_heartbeat', '-')}")
        print(f"last_progress={state.get('last_progress', '-')}")
        print(f"runnable_after={state.get('runnable_after', '-')}")
        print(f"reason={state.get('reason', '-')}")
        print(f"exec_timeout={state.get('exec_timeout', '-')}")
    return 0


def cmd_daemon_stop(args: argparse.Namespace) -> int:
    snapshot = daemon_runtime_snapshot()
    if not snapshot["running"]:
        print("not_running")
        return 0
    pid = int(snapshot["pid"])
    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        if not process_alive(pid):
            break
        time.sleep(0.1)
    running = process_alive(pid)
    print(f"stopped={not running}")
    print(f"pid={pid}")
    if running:
        return 1
    return 0


def cmd_runs_list(args: argparse.Namespace) -> int:
    conn = connect()
    runs = runs_snapshot(conn, limit=args.limit)
    conn.close()
    if args.json:
        print(json.dumps({"runs": runs}, indent=2, ensure_ascii=True))
        return 0
    if not runs:
        print("no_runs")
        return 0
    for run in runs:
        print(
            f"- {run['run_id']}: lane={run['lane_id']} task={run['task_id'] or '-'} "
            f"status={run['status']} mode={run['mode']} started={run['started_at']} exit={run['exit_code']}"
        )
    return 0


def cmd_runs_show(args: argparse.Namespace) -> int:
    conn = connect()
    row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (args.run_id,)).fetchone()
    conn.close()
    if row is None:
        raise SystemExit(f"Unknown run: {args.run_id}")
    run = dict(row)
    run_path = run_dir(args.run_id)
    payload = {
        "run": run,
        "run_dir": str(run_path),
        "files": {
            "command": str(run_path / "command.json"),
            "prompt": str(run_path / "prompt.txt"),
            "stdout": str(run_path / "stdout.log"),
            "stderr": str(run_path / "stderr.log"),
        },
        "artifacts": {},
    }
    for artifact_name in ("command.json", "prompt.txt", "stdout.log", "stderr.log", "worker-output.json", "evaluator-output.json"):
        path = run_path / artifact_name
        if path.exists():
            text = path.read_text(encoding="utf-8")
            payload["artifacts"][artifact_name] = text if args.full else text[-args.tail :]
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
        return 0
    print(f"run_id={run['run_id']}")
    print(f"lane_id={run['lane_id']}")
    print(f"task_id={run['task_id'] or '-'}")
    print(f"status={run['status']}")
    print(f"mode={run['mode']}")
    print(f"started_at={run['started_at']}")
    print(f"finished_at={run['finished_at'] or '-'}")
    print(f"exit_code={run['exit_code'] if run['exit_code'] is not None else '-'}")
    print(f"run_dir={run_path}")
    for artifact_name, content in payload["artifacts"].items():
        print(f"\n[{artifact_name}]")
        print(content.rstrip())
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    conn = connect()
    snapshot = status_snapshot(conn)
    conn.close()
    if args.json:
        if args.task_id:
            snapshot["tasks"] = [task for task in snapshot["tasks"] if task["task_id"] == args.task_id]
        print(json.dumps(snapshot, indent=2, ensure_ascii=True))
        return 0
    print(format_status(snapshot, args.task_id))
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    conn = connect()
    snapshot = dashboard_snapshot(conn, args.task_id)
    conn.close()
    if args.json:
        print(json.dumps(snapshot, indent=2, ensure_ascii=True))
        return 0
    print(format_dashboard(snapshot))
    return 0


def cmd_workspace_status(args: argparse.Namespace) -> int:
    conn = connect()
    snapshot = workspace_status_snapshot(conn)
    conn.close()
    if args.json:
        print(json.dumps(snapshot, indent=2, ensure_ascii=True))
        return 0
    print(f"Root: {snapshot['root']}")
    print(f"Target Repo: {snapshot['target_repo']}")
    print("")
    print("Workspaces:")
    if not snapshot["items"]:
        print("- none")
        return 0
    for item in snapshot["items"]:
        print(
            f"- lane={item.get('lane_id')} task={item.get('task_id')} kind={item.get('workspace_kind')} "
            f"path={item.get('workspace_path')} task_status={item.get('task_status') or '-'} "
            f"lane_status={item.get('lane_status') or '-'} queued={item.get('task_is_queued')} "
            f"fallback={item.get('fallback_reason') or '-'}"
        )
    return 0


def should_clean_workspace(item: dict[str, Any], *, include_finished: bool, include_orphans: bool, force: bool) -> tuple[bool, str]:
    task_id = item.get("task_id")
    task_status = item.get("task_status")
    lane_active_task_id = item.get("lane_active_task_id")
    task_is_queued = bool(item.get("task_is_queued"))
    if force:
        return True, "force"
    if lane_active_task_id == task_id:
        return False, "active"
    if task_is_queued:
        return False, "queued"
    if include_orphans and not task_status:
        return True, "orphan"
    if include_finished and task_status in {"masterpiece_locked", "cancelled"}:
        return True, "finished"
    return False, "ineligible"


def remove_workspace(item: dict[str, Any]) -> None:
    workspace_path = Path(item["workspace_path"])
    workspace_kind = item.get("workspace_kind")
    if workspace_kind == "git-worktree":
        repo_root = item.get("repo_root")
        if repo_root and workspace_path.exists():
            result = subprocess.run(
                ["git", "-C", repo_root, "worktree", "remove", "--force", str(workspace_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                message = result.stderr.strip() or result.stdout.strip() or "git worktree remove failed"
                raise LaneExecutionError(message)
    else:
        if workspace_path.exists():
            shutil.rmtree(workspace_path)
    for parent in (workspace_path.parent, workspace_path.parent.parent):
        if parent.exists() and parent != lane_paths(item["lane_id"])["workspace"]:
            try:
                parent.rmdir()
            except OSError:
                pass


def cmd_workspace_clean(args: argparse.Namespace) -> int:
    conn = connect()
    snapshot = workspace_status_snapshot(conn)
    conn.close()
    include_finished = args.finished or (not args.finished and not args.orphans)
    include_orphans = args.orphans or (not args.finished and not args.orphans)
    candidates = []
    for item in snapshot["items"]:
        if args.task_id and item.get("task_id") != args.task_id:
            continue
        allowed, reason = should_clean_workspace(
            item,
            include_finished=include_finished,
            include_orphans=include_orphans,
            force=args.force,
        )
        item_copy = dict(item)
        item_copy["clean_reason"] = reason
        if allowed:
            candidates.append(item_copy)

    removed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for item in candidates:
        if args.dry_run:
            removed.append({"task_id": item["task_id"], "lane_id": item["lane_id"], "workspace_path": item["workspace_path"], "dry_run": True, "reason": item["clean_reason"]})
            continue
        try:
            remove_workspace(item)
            removed.append({"task_id": item["task_id"], "lane_id": item["lane_id"], "workspace_path": item["workspace_path"], "reason": item["clean_reason"]})
            append_event(
                "workspace_removed",
                lane_id=item["lane_id"],
                task_id=item["task_id"],
                workspace_path=item["workspace_path"],
                workspace_kind=item.get("workspace_kind"),
                reason=item["clean_reason"],
            )
        except Exception as exc:
            errors.append({"task_id": item["task_id"], "lane_id": item["lane_id"], "workspace_path": item["workspace_path"], "error": str(exc)})

    payload = {
        "removed": removed,
        "errors": errors,
        "matched": len(candidates),
        "dry_run": bool(args.dry_run),
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
    else:
        print(f"matched={payload['matched']}")
        print(f"removed={len(removed)}")
        print(f"errors={len(errors)}")
        for item in removed:
            print(f"removed lane={item['lane_id']} task={item['task_id']} path={item['workspace_path']} reason={item['reason']}")
        for item in errors:
            print(f"error lane={item['lane_id']} task={item['task_id']} path={item['workspace_path']} message={item['error']}")
    return 0 if not errors else 1


def cmd_recover(args: argparse.Namespace) -> int:
    conn = connect()
    snapshot = recovery_snapshot(conn)
    payload = {
        **snapshot,
        "applied": False,
        "abandoned_run_count": len(snapshot["stale_runs"]),
        "repaired_lane_count": len(snapshot["lane_repairs"]),
    }
    if args.resume and args.restart_daemon:
        conn.close()
        raise SystemExit("--resume and --restart-daemon are mutually exclusive")
    if args.resume and not args.apply:
        conn.close()
        raise SystemExit("--resume requires --apply")
    if args.restart_daemon and not args.apply:
        conn.close()
        raise SystemExit("--restart-daemon requires --apply")
    if args.apply:
        result = apply_recovery_plan(conn, snapshot, requeue=args.requeue)
        payload["applied"] = True
        payload["abandoned_runs"] = result["abandoned_runs"]
        payload["repaired_lanes"] = result["repaired_lanes"]
        payload["requeued_reservations"] = result["requeued_reservations"]
        payload["abandoned_run_count"] = len(result["abandoned_runs"])
        payload["repaired_lane_count"] = len(result["repaired_lanes"])
        payload["requeued_reservation_count"] = len(result["requeued_reservations"])
        payload["post_status"] = status_snapshot(conn)
        if args.resume:
            daemon_state = snapshot["daemon"].get("state") or {}
            resume_executor = args.executor or daemon_state.get("executor") or DEFAULT_EXECUTOR
            resume_timeout = args.exec_timeout if args.exec_timeout is not None else float(daemon_state.get("exec_timeout") or DEFAULT_EXEC_TIMEOUT)
            resume_interval = args.interval if args.interval is not None else float(daemon_state.get("interval") or 0.0)
            resume_payload = resume_recovered_work(
                conn,
                executor=resume_executor,
                exec_timeout=float(resume_timeout),
                interval=float(resume_interval),
                max_ticks=args.max_ticks,
                until_idle=args.until_idle,
            )
            payload["resume"] = resume_payload
            payload["post_status"] = resume_payload["status"]
        if args.restart_daemon:
            daemon_state = snapshot["daemon"].get("state") or {}
            restart_executor = args.executor or daemon_state.get("executor") or DEFAULT_EXECUTOR
            restart_timeout = args.exec_timeout if args.exec_timeout is not None else float(daemon_state.get("exec_timeout") or DEFAULT_EXEC_TIMEOUT)
            restart_interval = args.interval if args.interval is not None else float(daemon_state.get("interval") or 1.0)
            daemon_restart = start_daemon_process(
                executor=restart_executor,
                exec_timeout=float(restart_timeout),
                interval=float(restart_interval),
                max_cycles=args.daemon_max_cycles,
                until_idle=args.until_idle,
            )
            payload["daemon_restart"] = daemon_restart
    conn.close()
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
        return 0
    print(f"daemon_running={payload['daemon']['running']}")
    print(f"stale_runs={payload['abandoned_run_count']}")
    print(f"lane_repairs={payload['repaired_lane_count']}")
    print(f"requeued={payload.get('requeued_reservation_count', 0)}")
    if payload["stale_runs"]:
        for run in payload["stale_runs"]:
            print(
                f"- run {run['run_id']}: lane={run['lane_id']} task={run['task_id'] or '-'} "
                f"status={run['status']} referenced_by_lane={run['referenced_by_lane']}"
            )
    if payload["lane_repairs"]:
        for action in payload["lane_repairs"]:
            print(
                f"- lane {action['lane_id']}: {action['current_status']} -> {action['next_status']} "
                f"task={action['next_task_id'] or '-'} run={action['run_id'] or '-'} reason={action['reason']}"
            )
    if args.apply:
        print("applied=True")
        for item in payload.get("requeued_reservations", []):
            print(
                f"- requeued {item['reservation_id']}: lane={item['lane_id']} "
                f"task={item['task_id']} type={item['reservation_type']}"
            )
    if args.resume:
        resume_payload = payload.get("resume", {})
        ticks = resume_payload.get("ticks", [])
        print(f"resume_executor={resume_payload.get('executor', '-')}")
        print(f"resume_ticks={len(ticks)}")
        for tick in ticks:
            print(
                f"- resume tick {tick['tick']}: progress={tick['progress']} "
                f"actions={len(tick['actions'])} runnable_after={tick['runnable_after']}"
            )
    if args.restart_daemon:
        daemon_restart = payload.get("daemon_restart", {})
        print(f"daemon_restarted={daemon_restart.get('started', False)}")
        print(f"daemon_pid={daemon_restart.get('pid', '-')}")
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    while True:
        conn = connect()
        snapshot = dashboard_snapshot(conn, args.task_id) if args.dashboard else status_snapshot(conn)
        conn.close()
        if sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print(format_dashboard(snapshot) if args.dashboard else format_status(snapshot, args.task_id))
        if args.once:
            return 0
        time.sleep(args.interval)


def cmd_tui(args: argparse.Namespace) -> int:
    ensure_layout()
    env = os.environ.copy()
    env["CODEX_HOME"] = str(LAB_HOME)
    command = [
        REAL_CODEX,
        "--cd",
        str(ROOT),
        "--sandbox",
        "workspace-write",
        "--ask-for-approval",
        "never",
        *args.extra_args,
    ]
    append_event("tui_launch", command=command, codex_home=str(LAB_HOME))
    os.execvpe(REAL_CODEX, command, env)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CodexLab control plane CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor", help="Validate the codexlab control plane layout")
    doctor.add_argument("--json", action="store_true")
    doctor.set_defaults(func=cmd_doctor)

    submit = subparsers.add_parser("submit", help="Create a new task and queue it for both workers")
    submit.add_argument("--title")
    submit.add_argument("--prompt", required=True)
    submit.set_defaults(func=cmd_submit)

    record = subparsers.add_parser("record-submission", help="Record a worker submission artifact")
    record.add_argument("task_id")
    record.add_argument("lane_id", choices=WORKER_LANES)
    record.add_argument("--summary", required=True)
    record.add_argument("--body")
    record.set_defaults(func=cmd_record_submission)

    score = subparsers.add_parser("score", help="Record evaluator scoring between two submissions")
    score.add_argument("task_id")
    score.add_argument("--left", required=True)
    score.add_argument("--right", required=True)
    score.add_argument("--left-score", type=float)
    score.add_argument("--right-score", type=float)
    score.add_argument("--left-rubric-json")
    score.add_argument("--right-rubric-json")
    score.add_argument("--rationale", required=True)
    score.add_argument("--loser-brief", default="Revise to beat the published champion on the next retry.")
    score.set_defaults(func=cmd_score)

    tick = subparsers.add_parser("tick", help="Execute one scheduler tick")
    tick.add_argument("--executor", choices=("mock", "codex"), default=DEFAULT_EXECUTOR)
    tick.add_argument("--exec-timeout", type=float, default=DEFAULT_EXEC_TIMEOUT)
    tick.add_argument("--json", action="store_true")
    tick.set_defaults(func=cmd_tick)

    run_loop = subparsers.add_parser("run-loop", help="Run the scheduler for multiple ticks")
    run_loop.add_argument("--executor", choices=("mock", "codex"), default=DEFAULT_EXECUTOR)
    run_loop.add_argument("--exec-timeout", type=float, default=DEFAULT_EXEC_TIMEOUT)
    run_loop.add_argument("--interval", type=float, default=0.0)
    run_loop.add_argument("--max-ticks", type=int, default=20)
    run_loop.add_argument("--until-idle", action="store_true")
    run_loop.add_argument("--json", action="store_true")
    run_loop.set_defaults(func=cmd_run_loop)

    daemon = subparsers.add_parser("daemon", help="Manage the background scheduler")
    daemon_subparsers = daemon.add_subparsers(dest="daemon_command", required=True)

    daemon_run = daemon_subparsers.add_parser("run", help="Run the scheduler in the foreground")
    daemon_run.add_argument("--executor", choices=("mock", "codex"), default=DEFAULT_EXECUTOR)
    daemon_run.add_argument("--exec-timeout", type=float, default=DEFAULT_EXEC_TIMEOUT)
    daemon_run.add_argument("--interval", type=float, default=1.0)
    daemon_run.add_argument("--max-cycles", type=int, default=0)
    daemon_run.add_argument("--until-idle", action="store_true")
    daemon_run.add_argument("--json", action="store_true")
    daemon_run.set_defaults(func=cmd_daemon_run)

    daemon_start = daemon_subparsers.add_parser("start", help="Start the scheduler in the background")
    daemon_start.add_argument("--executor", choices=("mock", "codex"), default=DEFAULT_EXECUTOR)
    daemon_start.add_argument("--exec-timeout", type=float, default=DEFAULT_EXEC_TIMEOUT)
    daemon_start.add_argument("--interval", type=float, default=1.0)
    daemon_start.add_argument("--max-cycles", type=int, default=0)
    daemon_start.add_argument("--until-idle", action="store_true")
    daemon_start.set_defaults(func=cmd_daemon_start)

    daemon_status = daemon_subparsers.add_parser("status", help="Show background scheduler state")
    daemon_status.add_argument("--json", action="store_true")
    daemon_status.set_defaults(func=cmd_daemon_status)

    daemon_stop = daemon_subparsers.add_parser("stop", help="Stop the background scheduler")
    daemon_stop.add_argument("--timeout", type=float, default=5.0)
    daemon_stop.set_defaults(func=cmd_daemon_stop)

    runs = subparsers.add_parser("runs", help="Inspect recorded lane runs")
    runs_subparsers = runs.add_subparsers(dest="runs_command", required=True)

    runs_list = runs_subparsers.add_parser("list", help="List recent runs")
    runs_list.add_argument("--limit", type=int, default=20)
    runs_list.add_argument("--json", action="store_true")
    runs_list.set_defaults(func=cmd_runs_list)

    runs_show = runs_subparsers.add_parser("show", help="Show one recorded run")
    runs_show.add_argument("run_id")
    runs_show.add_argument("--tail", type=int, default=4000)
    runs_show.add_argument("--full", action="store_true")
    runs_show.add_argument("--json", action="store_true")
    runs_show.set_defaults(func=cmd_runs_show)

    status = subparsers.add_parser("status", help="Show task and lane state")
    status.add_argument("task_id", nargs="?")
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=cmd_status)

    dashboard = subparsers.add_parser("dashboard", help="Show the operator dashboard with latest evaluation context")
    dashboard.add_argument("task_id", nargs="?")
    dashboard.add_argument("--json", action="store_true")
    dashboard.set_defaults(func=cmd_dashboard)

    workspace = subparsers.add_parser("workspace", help="Inspect prepared task workspaces")
    workspace_subparsers = workspace.add_subparsers(dest="workspace_command", required=True)

    workspace_status = workspace_subparsers.add_parser("status", help="Show workspace manifests")
    workspace_status.add_argument("--json", action="store_true")
    workspace_status.set_defaults(func=cmd_workspace_status)

    workspace_clean = workspace_subparsers.add_parser("clean", help="Remove finished or orphaned workspaces")
    workspace_clean.add_argument("--task-id")
    workspace_clean.add_argument("--finished", action="store_true", help="Clean finished task workspaces")
    workspace_clean.add_argument("--orphans", action="store_true", help="Clean manifests with no backing task row")
    workspace_clean.add_argument("--force", action="store_true", help="Ignore active/queued safety checks")
    workspace_clean.add_argument("--dry-run", action="store_true")
    workspace_clean.add_argument("--json", action="store_true")
    workspace_clean.set_defaults(func=cmd_workspace_clean)

    recover = subparsers.add_parser("recover", help="Recover stale run handles and optionally resume scheduling")
    recover.add_argument("--apply", action="store_true")
    recover.add_argument("--requeue", action="store_true", help="Requeue repaired worker tasks instead of reopening the lane directly")
    recover.add_argument("--resume", action="store_true", help="Resume scheduler work after applying recovery")
    recover.add_argument("--restart-daemon", action="store_true", help="Restart the background daemon after applying recovery")
    recover.add_argument("--executor", choices=("mock", "codex"), default=None)
    recover.add_argument("--exec-timeout", type=float)
    recover.add_argument("--interval", type=float)
    recover.add_argument("--max-ticks", type=int, default=20)
    recover.add_argument("--daemon-max-cycles", type=int, default=0)
    recover.add_argument("--until-idle", action="store_true")
    recover.add_argument("--json", action="store_true")
    recover.set_defaults(func=cmd_recover)

    watch = subparsers.add_parser("watch", help="Continuously print status")
    watch.add_argument("task_id", nargs="?")
    watch.add_argument("--interval", type=float, default=2.0)
    watch.add_argument("--dashboard", action="store_true")
    watch.add_argument("--once", action="store_true")
    watch.set_defaults(func=cmd_watch)

    tui = subparsers.add_parser("tui", help="Launch Codex inside the lab workspace")
    tui.add_argument("extra_args", nargs=argparse.REMAINDER)
    tui.set_defaults(func=cmd_tui)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
