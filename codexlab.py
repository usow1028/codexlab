#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
from collections import deque
import concurrent.futures
import json
import os
import re
import signal
import shlex
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path(__file__).resolve()
if str(SCRIPT_PATH.parent) not in sys.path:
    sys.path.insert(0, str(SCRIPT_PATH.parent))
REPO_VENDOR_DIR = SCRIPT_PATH.parent / ".vendor"
if REPO_VENDOR_DIR.exists():
    sys.path.insert(0, str(REPO_VENDOR_DIR))

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.styles import Style
except ImportError:
    PromptSession = None
    Completer = None
    Completion = None
    Style = None

from codexlab_resilience import (
    CredentialVault,
    ResilientRunner,
    VaultError,
    atomic_write_json,
    is_quota_text,
    parse_shell_command,
    probe_auth_rate_limits,
)
from codexlab_asymptote import AsymptoteController


ROOT = Path(os.environ.get("CODEXLAB_ROOT", SCRIPT_PATH.parent))
CONTROL_DIR = ROOT / "control"
DB_PATH = CONTROL_DIR / "control.db"
EVENTS_DIR = CONTROL_DIR / "events"
EVENTS_FILE = EVENTS_DIR / "events.jsonl"
RUNS_DIR = CONTROL_DIR / "runs"
DAEMON_DIR = CONTROL_DIR / "daemon"
ASYMPTOTE_DIR = ROOT / "asymptote"
DAEMON_PID_FILE = DAEMON_DIR / "daemon.pid"
DAEMON_STATE_FILE = DAEMON_DIR / "daemon-state.json"
LEGACY_ASYMPTOTE_STATE_FILE = DAEMON_DIR / "asymptote-state.json"
ASYMPTOTE_STATE_FILE = ASYMPTOTE_DIR / "state.json"
DAEMON_LOG_FILE = DAEMON_DIR / "daemon.log"
TASKS_DIR = ROOT / "tasks"
AGENTS_DIR = ROOT / "agents"
LAB_HOME = ROOT / ".codex-home"
TEMPLATES_DIR = ROOT / "templates"
LEGACY_USER_PREFS_FILE = ROOT / "user_prefs.md"
LEGACY_AI_PREFS_FILE = ROOT / "ai_prefs.md"
LEGACY_LETTERS_FILE = ROOT / "letters.md"
USER_PREFS_FILE = ASYMPTOTE_DIR / "user_prefs.md"
AI_PREFS_FILE = ASYMPTOTE_DIR / "ai_prefs.md"
LETTERS_FILE = ASYMPTOTE_DIR / "letters.md"
REAL_CODEX = os.environ.get("CODEXLAB_CODEX_BIN", "/usr/bin/codex")
LOGIN_CODEX_HOME = Path(os.environ.get("CODEXLAB_LOGIN_CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
RESILIENCE_POOL_PATH = Path(os.environ.get("CODEXLAB_POOL_PATH", str(Path.home() / ".codexlab" / "pool.json"))).expanduser()
RESILIENCE_AUTH_PATH = LOGIN_CODEX_HOME / "auth.json"
RESILIENCE_SCRATCH_DIR = RESILIENCE_POOL_PATH.parent / "probe-homes"
DEFAULT_EXECUTOR = os.environ.get("CODEXLAB_EXECUTOR", "mock")
LIVE_DEFAULT_EXECUTOR = os.environ.get("CODEXLAB_LIVE_EXECUTOR", "codex")
_BASE_EXEC_TIMEOUT = os.environ.get("CODEXLAB_EXEC_TIMEOUT")
DEFAULT_MOCK_EXEC_TIMEOUT = float(os.environ.get("CODEXLAB_MOCK_EXEC_TIMEOUT", _BASE_EXEC_TIMEOUT or "120"))
DEFAULT_CODEX_EXEC_TIMEOUT = float(os.environ.get("CODEXLAB_CODEX_EXEC_TIMEOUT", _BASE_EXEC_TIMEOUT or "600"))
DEFAULT_EXEC_TIMEOUT = DEFAULT_MOCK_EXEC_TIMEOUT
DEFAULT_WATCH_INTERVAL = float(os.environ.get("CODEXLAB_WATCH_INTERVAL", "1.0"))
DEFAULT_EVENTS_LIMIT = int(os.environ.get("CODEXLAB_EVENTS_LIMIT", "12"))
DEFAULT_QUOTA_RECHECK_INTERVAL = float(os.environ.get("CODEXLAB_QUOTA_RECHECK_INTERVAL", "60"))
DEFAULT_RESILIENCE_RECHECK_INTERVAL = float(os.environ.get("CODEXLAB_RESILIENCE_RECHECK_INTERVAL", "60"))
DEFAULT_ASYMPTOTE_INTERVAL_SECONDS = float(os.environ.get("CODEXLAB_ASYMPTOTE_INTERVAL_SECONDS", "3600"))
_USAGE_PROBE_WORKERS_OVERRIDE = os.environ.get("CODEXLAB_USAGE_PROBE_WORKERS")
DEFAULT_USAGE_PROBE_WORKERS = (
    max(1, int(_USAGE_PROBE_WORKERS_OVERRIDE))
    if _USAGE_PROBE_WORKERS_OVERRIDE is not None
    else None
)
LIVE_CODEX_SANDBOX = os.environ.get("CODEXLAB_LIVE_SANDBOX", "danger-full-access").strip() or "danger-full-access"
MAX_DAEMON_STATE_FILE_BYTES = int(os.environ.get("CODEXLAB_MAX_DAEMON_STATE_FILE_BYTES", str(1024 * 1024)))
AUTO_QUOTA_RECOVERY = os.environ.get("CODEXLAB_AUTO_RECOVER_QUOTA", "1").strip().lower() not in {"0", "false", "no", "off"}
SCHEMA_VERSION = 5
PATCH_MODE_STRONG_HINTS = (
    "code change",
    "file",
    "files",
    "folder",
    "directory",
    "module",
    "plugin",
    "workspace",
    "repo",
    "repository",
    "test",
    "tests",
    "space/",
    ".py",
    "wire it into the repo",
    "파일",
    "폴더",
    "디렉토리",
    "모듈",
    "플러그인",
    "테스트",
)
PATCH_MODE_WEAK_HINTS = (
    "implement",
    "implementation",
    "patch",
    "fix",
    "create",
    "build",
    "modify",
    "update",
    "add ",
    "구현",
    "수정",
    "변경",
    "추가",
    "생성",
    "만들",
)
PROPOSAL_MODE_HINTS = (
    "plan",
    "design",
    "proposal",
    "blueprint",
    "architecture",
    "outline",
    "strategy",
    "spec",
    "설계",
    "계획",
    "제안",
    "청사진",
)
STREAM_EVENT_TYPES = {
    "task_submitted",
    "run_started",
    "submission_recorded",
    "task_scored",
    "evaluation_tied",
    "task_masterpiece_locked",
    "lane_error",
    "lane_recovered",
    "run_recovered",
    "run_resume_fallback",
    "quota_probe",
    "quota_auto_resume",
    "resilience_paused",
    "resilience_resumed",
    "task_applied",
    "task_apply_failed",
    "asymptote_started",
    "asymptote_stopped",
    "asymptote_pulse",
    "asymptote_human_note",
    "asymptote_error",
    "daemon_started",
    "daemon_stopped",
}
ASYMPTOTE_CONTROLLER: AsymptoteController | None = None


def default_exec_timeout_for(executor: str) -> float:
    return DEFAULT_CODEX_EXEC_TIMEOUT if executor == "codex" else DEFAULT_MOCK_EXEC_TIMEOUT


def current_animation_phase() -> int:
    return int(time.time() * 2)


def animated_state_icon(state: str, *, phase: int | None = None) -> str:
    frame = current_animation_phase() if phase is None else max(0, int(phase))
    if state == "RUNNING":
        return ("|", "/", "-", "\\")[frame % 4]
    if state == "RECOVERING":
        return (".", "o", "O", "o")[frame % 4]
    if state == "BLOCKED":
        return ("!", ".", "!", ".")[frame % 4]
    if state == "WAITING":
        return (".", "o", ".", "o")[frame % 4]
    if state == "READY":
        return "+"
    if state == "DONE":
        return "*"
    if state == "PAUSED":
        return ";"
    return "-"


def render_state_label(state: str, *, phase: int | None = None) -> str:
    return f"{animated_state_icon(state, phase=phase)} {state}"
WORKER_LANES = ("worker-a", "worker-b")
ALL_LANES = (
    ("worker-a", "worker"),
    ("worker-b", "worker"),
    ("evaluator", "evaluator"),
)
MAX_RETRY_FAILURES = 1
MAX_TOTAL_EVALUATIONS = 12
MAX_ROLE_SWAPS = 6
FINISHED_TASK_STATUSES = {"masterpiece_locked", "cancelled"}
EVALUATOR_TIERS = ("primary", "elder", "absolute")
UI_LANE_LABELS = {
    "worker-a": "Red Corner",
    "worker-b": "Blue Corner",
    "evaluator": "Score Judge",
}
UI_EVALUATOR_LABELS = {
    "primary": "score judge",
    "elder": "chief judge",
    "absolute": "final arbiter",
}
UI_ROLE_LABELS = {
    "champion": "champion",
    "challenger": "challenger",
    "published": "published card",
    "archive": "archive",
}
UI_MOMENTUM_LABELS = {
    "pending": "awaiting card",
    "new": "new card",
    "up": "rising",
    "down": "slipping",
    "same": "steady",
}
UI_RESERVATION_LABELS = {
    "new_task": "opening bout",
    "duel_retry": "title reply",
    "tie_rematch": "rematch",
}
RUBRIC_WEIGHTS = {
    "correctness": 35.0,
    "completeness": 25.0,
    "risk": 15.0,
    "maintainability": 15.0,
    "verification": 10.0,
}
RUBRIC_CRITERIA = tuple(RUBRIC_WEIGHTS.keys())
PROMPT_SESSIONS: dict[str, Any] = {}
SIMULTANEOUS_WORKER_PAIR_MODES = {"initial_duel", "full_rematch"}
CONSOLE_SLASH_COMMANDS: tuple[tuple[str, str], ...] = (
    ("/focus T-0001", "focus one task"),
    ("/all", "show every task"),
    ("/status", "refresh the live status panel"),
    ("/refresh", "reload the operator snapshot"),
    ("/clear-tasks", "wipe runtime task history"),
    ("/profile register", "login and save the next numbered profile"),
    ("/profile list", "list registered profiles"),
    ("/profile current", "show the active profile"),
    ("/profile activate <account_key|alias>", "switch to a stored profile"),
    ("/profile disable <account_key|alias>", "exclude a profile from rotation"),
    ("/profile enable <account_key|alias>", "return a profile to rotation"),
    ("/profile renumber", "rename stored aliases to 1, 2, 3..."),
    ("/profile reset-exhausted", "mark exhausted profiles ready again"),
    ("/auto-switch on", "enable quota-triggered profile rotation"),
    ("/auto-switch off", "disable quota-triggered profile rotation"),
    ("/sync", "sync the current auth file back into the vault"),
    ("/asymptote on", "open the epsilon interface and start the hourly pulse"),
    ("/asymptote off", "close the epsilon interface"),
    ("/useage", "show the current profile's weekly limit"),
    ("/useage all", "show the weekly limit for every profile"),
    ("/run <command...>", "run an ad-hoc command through the resilience layer"),
    ("/quit", "leave the shell-style console"),
    ("/exit", "leave the shell-style console"),
)


def credential_vault() -> CredentialVault:
    return CredentialVault(RESILIENCE_POOL_PATH, RESILIENCE_AUTH_PATH)


def asymptote_controller() -> AsymptoteController:
    global ASYMPTOTE_CONTROLLER
    if ASYMPTOTE_CONTROLLER is None:
        ASYMPTOTE_CONTROLLER = AsymptoteController(
            state_path=ASYMPTOTE_STATE_FILE,
            user_prefs_path=USER_PREFS_FILE,
            ai_prefs_path=AI_PREFS_FILE,
            letters_path=LETTERS_FILE,
            interval_seconds=DEFAULT_ASYMPTOTE_INTERVAL_SECONDS,
            event_callback=append_event,
            legacy_state_path=LEGACY_ASYMPTOTE_STATE_FILE,
            legacy_user_prefs_path=LEGACY_USER_PREFS_FILE,
            legacy_ai_prefs_path=LEGACY_AI_PREFS_FILE,
            legacy_letters_path=LEGACY_LETTERS_FILE,
        )
    return ASYMPTOTE_CONTROLLER


def asymptote_snapshot() -> dict[str, Any]:
    try:
        return asymptote_controller().snapshot()
    except Exception as exc:
        return {
            "active": False,
            "owner_pid": 0,
            "status": "BLOCKED",
            "reason": f"두 세계의 주파수가 어긋나 계면이 불안정해졌습니다 (Interface Instability): {exc}",
            "progress_text": "[----------------] 0m left to horizon",
            "interface_state": "unstable",
            "last_error": str(exc),
            "human_anchor": "-",
            "ai_anchor": "-",
            "letters_anchor": "-",
        }


def stop_owned_asymptote_engine() -> None:
    try:
        payload = asymptote_snapshot()
        if payload.get("active") and int(payload.get("owner_pid") or 0) == os.getpid():
            asymptote_controller().stop()
    except Exception:
        return


def preferred_terminal_launcher() -> str | None:
    desktop = str(os.environ.get("XDG_CURRENT_DESKTOP") or "")
    if (os.environ.get("KONSOLE_VERSION") or "KDE" in desktop) and shutil.which("konsole"):
        return "konsole"
    for candidate in ("gnome-terminal", "kitty", "wezterm", "alacritty", "x-terminal-emulator", "xterm"):
        if shutil.which(candidate):
            return candidate
    return None


def external_asymptote_console_supported() -> bool:
    return preferred_terminal_launcher() is not None


def spawn_asymptote_console(*, activate: bool) -> str:
    launcher = preferred_terminal_launcher()
    if launcher is None:
        raise VaultError("no supported terminal launcher found for asymptote-console")
    inner_args = [sys.executable, str(SCRIPT_PATH), "asymptote-console"]
    if activate:
        inner_args.append("--activate")
    else:
        inner_args.append("--attach")
    command_text = f"cd {shlex.quote(str(ROOT))} && exec {' '.join(shlex.quote(part) for part in inner_args)}"
    if launcher == "konsole":
        command = [
            launcher,
            "--new-tab",
            "-p",
            "tabtitle=CodexLab Asymptote",
            "-e",
            "/usr/bin/bash",
            "-lc",
            command_text,
        ]
        destination = "new Konsole tab"
    elif launcher == "gnome-terminal":
        command = [launcher, "--", "/usr/bin/bash", "-lc", command_text]
        destination = "new terminal window"
    elif launcher == "kitty":
        command = [launcher, "--title", "CodexLab Asymptote", "/usr/bin/bash", "-lc", command_text]
        destination = "new terminal window"
    elif launcher == "wezterm":
        command = [launcher, "start", "--cwd", str(ROOT), "/usr/bin/bash", "-lc", command_text]
        destination = "new terminal window"
    elif launcher == "alacritty":
        command = [launcher, "--title", "CodexLab Asymptote", "-e", "/usr/bin/bash", "-lc", command_text]
        destination = "new terminal window"
    else:
        command = [launcher, "-e", "/usr/bin/bash", "-lc", command_text]
        destination = "new terminal window"
    subprocess.Popen(
        command,
        cwd=str(ROOT),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    mode = "activated" if activate else "attached"
    return f"opened asymptote console in a {destination} ({mode})"


def resilience_summary() -> dict[str, Any]:
    try:
        return credential_vault().summary()
    except Exception as exc:
        return {
            "error": str(exc),
            "auto_switch": False,
            "current_account_key": None,
            "current_alias": None,
            "counts": {status: 0 for status in ("ready", "active", "exhausted", "disabled")},
            "profiles": [],
            "last_rotation": {},
        }


def resilience_auto_switch_enabled() -> bool:
    try:
        return credential_vault().auto_switch_enabled()
    except Exception:
        return False


def lane_display_name(lane_id: Any) -> str:
    if not lane_id:
        return "-"
    if isinstance(lane_id, dict):
        lane_id = lane_id.get("lane_id")
        if not lane_id:
            return "-"
    return UI_LANE_LABELS.get(str(lane_id), str(lane_id))


def advanced_prompt_available() -> bool:
    return PromptSession is not None and interactive_terminal_available()


def prompt_session(name: str) -> Any | None:
    if not advanced_prompt_available():
        return None
    session = PROMPT_SESSIONS.get(name)
    if session is None:
        session = PromptSession()
        PROMPT_SESSIONS[name] = session
    return session


class SlashCommandCompleter(Completer if Completer is not None else object):
    def __init__(self, commands: tuple[tuple[str, str], ...]) -> None:
        self.commands = commands

    def get_completions(self, document: Any, complete_event: Any) -> Any:
        if Completion is None:
            return
        text = str(getattr(document, "text_before_cursor", "") or "")
        query = text.lstrip()
        if not query.startswith("/"):
            return
        lowered = query.lower()
        for command_text, description in self.commands:
            if command_text.lower().startswith(lowered):
                yield Completion(
                    command_text,
                    start_position=-len(query),
                    display=command_text,
                    display_meta=description,
                )


CONSOLE_SLASH_COMPLETER = SlashCommandCompleter(CONSOLE_SLASH_COMMANDS) if Completer is not None else None


def read_prompt_line(
    prompt_text: str,
    *,
    session_name: str,
    refresh_interval: float | None = None,
    message: Any = None,
    bottom_toolbar: Any = None,
    rprompt: Any = None,
    style: Any = None,
    completer: Any = None,
    complete_while_typing: bool = False,
) -> str:
    session = prompt_session(session_name)
    if session is not None:
        prompt_message = message if message is not None else prompt_text
        return str(
            session.prompt(
                prompt_message,
                refresh_interval=refresh_interval,
                bottom_toolbar=bottom_toolbar,
                rprompt=rprompt,
                style=style,
                completer=completer,
                complete_while_typing=complete_while_typing,
                reserve_space_for_menu=8 if completer is not None else 0,
            )
        )
    return input(prompt_text)


def evaluator_display_label(tier: str | None) -> str:
    if not tier:
        return "score judge"
    return UI_EVALUATOR_LABELS.get(str(tier), str(tier))


def scoreboard_role_label(role: str | None) -> str:
    if not role:
        return "-"
    return UI_ROLE_LABELS.get(str(role), str(role))


def scoreboard_momentum_label(movement: str | None) -> str:
    if not movement:
        return "-"
    return UI_MOMENTUM_LABELS.get(str(movement), str(movement))


def reservation_display_label(reservation_type: str | None) -> str:
    if not reservation_type:
        return "-"
    return UI_RESERVATION_LABELS.get(str(reservation_type), str(reservation_type))


def lane_submission_ref(lane_id: str | None, submission_id: str | None) -> str:
    if submission_id:
        return f"{lane_display_name(lane_id)}/{submission_id}"
    return lane_display_name(lane_id)


def slugify_label(text: str, *, fallback: str = "item", max_length: int = 80) -> str:
    lowered = re.sub(r"[^a-z0-9]+", "-", str(text or "").lower()).strip("-")
    if not lowered:
        lowered = fallback
    return lowered[:max_length].strip("-") or fallback


def numeric_id_text(value: str | None, prefix: str) -> str:
    text = str(value or "")
    if text.startswith(f"{prefix}-"):
        return text.split("-", 1)[1]
    return text


def lane_slug(lane_id: str | None) -> str:
    return slugify_label(lane_display_name(lane_id), fallback="corner")


def evaluator_slug(tier: str | None) -> str:
    return slugify_label(evaluator_display_label(tier), fallback="judge")


def boxing_stage_info(
    *,
    pair_mode: str | None,
    duel_stage: str | None,
    evaluator_tier: str | None = None,
    tier_phase: str | None = None,
) -> tuple[str, str]:
    mode = str(pair_mode or "initial_duel")
    stage = str(duel_stage or "initial")
    judge = str(evaluator_tier or "primary")
    judge_phase = str(tier_phase or "base")
    if mode == "initial_duel":
        return "championship-bout", "Championship bout"
    if mode == "challenger_retry":
        if stage == "defense":
            return "title-defense", "Title defense"
        return "challenger-rematch", "Challenger rematch"
    if mode == "full_rematch":
        return "draw-rematch", "Draw rematch"
    if mode == "review_only":
        if judge == "absolute" and judge_phase == "post_rematch":
            return "final-rereview", "Final re-review"
        if judge == "absolute":
            return "final-arbiter-review", "Final arbiter review"
        if judge == "elder":
            return "chief-judge-review", "Chief judge review"
        return "judge-review", "Judge review"
    return "open-bout", "Open bout"


def lane_names_display(lane_ids: list[str] | tuple[str, ...]) -> str:
    return ", ".join(lane_display_name(lane_id) for lane_id in lane_ids)


def queued_entries_display(entries: list[dict[str, Any]]) -> str:
    return ", ".join(
        f"{lane_display_name(entry['lane_id'])}:{reservation_display_label(entry['reservation_type'])}"
        for entry in entries
    )


def crowd_reaction(task: dict[str, Any]) -> str:
    if task.get("masterpiece_locked"):
        champion_lane = task.get("champion_lane_id")
        if champion_lane:
            return f"The crowd has settled behind the {lane_display_name(champion_lane)}."
        return "The crowd has settled after the final bell."
    champion_lane = task.get("champion_lane_id")
    if not champion_lane:
        return "The crowd is still split."
    latest_eval = task.get("latest_evaluation") or {}
    try:
        delta = abs(float(latest_eval.get("score_delta") or 0.0))
    except (TypeError, ValueError):
        delta = 0.0
    lane_label = lane_display_name(champion_lane)
    if delta >= 15:
        return f"The crowd is firmly behind the {lane_label}."
    if delta >= 5:
        return f"The crowd is leaning toward the {lane_label}."
    return f"The crowd sounds split, but the {lane_label} has a slight edge."

DEFAULT_WORKER_TEMPLATE = textwrap.dedent(
    """\
    You are {lane_id}, a CodexLab worker agent.

    Task ID: {task_id}
    Title: {task_title}
    Task mode: {task_mode}

    Primary task brief:
    {task_prompt}

    Current scoring benchmark summary:
    {champion_summary}

    Current scoring benchmark body:
    {champion_body}

    Corner notes from the latest judging:
    {guidance_brief}

    Your latest submission summary:
    {own_previous_summary}

    Your latest submission body:
    {own_previous_body}

    Round context:
    {round_context}

    Mode expectations:
    {mode_expectations}

    Instructions:
    - Produce the strongest possible standalone answer for the task.
    - Your goal is to improve your own weighted-rubric score, not to write a debate transcript.
    - Use the scoring benchmark and corner notes only as reference material.
    - Do not write a rebuttal, dialogue, or point-by-point response to the other corner.
    - If a pressure point from the other corner seems persuasive, you may absorb it into your own answer. If it does not persuade you, ignore it.
    - In a title-defense round, this is the final chance to sharpen your own answer before the winner locks.
    - In a tie rematch, you may improve your latest draft or resubmit it unchanged if you are confident it already wins.
    - Keep the response concrete and high signal.
    - Return only JSON matching the required schema.
    """
)

DEFAULT_EVALUATOR_TEMPLATE = textwrap.dedent(
    """\
    You are the CodexLab {evaluator_tier_label}.

    Task ID: {task_id}
    Title: {task_title}
    Task mode: {task_mode}

    Original task brief:
    {task_prompt}

    Compare the two submissions below.
    Score each submission on a 0-5 rubric for correctness, completeness, risk, maintainability, and verification.
    Compute the weighted total using these exact weights:
    - correctness = 35
    - completeness = 25
    - risk = 15
    - maintainability = 15
    - verification = 10
    Derive the overall winner from the weighted totals, not from an unweighted sum or intuition alone.
    {tie_policy}
    {mode_scoring_guidance}
    Always return both loser_brief and rematch_brief.
    If you choose a winner, loser_brief must help the lower-scoring worker improve its own submission on the weighted rubric. It may mention the strongest pressure points raised by the other corner, but only as optional considerations, not as a script for a rebuttal. rematch_brief must be an empty string.
    If the weighted totals truly tie, loser_brief must be an empty string and rematch_brief must give both workers shared improvement notes. Those notes may mention persuasive pressure points from the opposite corner, but both workers should still submit standalone answers rather than debate replies.

    LEFT SUBMISSION
    ID: {left_submission_id}
    Lane: {left_lane_id}
    Summary:
    {left_summary}

    Body:
    {left_body}

    Workspace evidence:
    {left_evidence}

    RIGHT SUBMISSION
    ID: {right_submission_id}
    Lane: {right_lane_id}
    Summary:
    {right_summary}

    Body:
    {right_body}

    Workspace evidence:
    {right_evidence}

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
    task_id: str
    mode: str
    run_dir: Path
    started_at: str


@dataclass
class CodexWorkerLaunch:
    handle: RunHandle
    lane_id: str
    task_id: str
    workspace: Path
    output_path: Path
    prompt_style: str
    used_resume: bool
    session_id: str | None
    process: subprocess.Popen[str]
    started_monotonic: float


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def jwt_payload(token: str | None) -> dict[str, Any]:
    if not token:
        return {}
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        parsed = json.loads(decoded.decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def codex_login_identity() -> dict[str, Any]:
    auth_path = LOGIN_CODEX_HOME / "auth.json"
    payload = {
        "login_home": str(LOGIN_CODEX_HOME),
        "email": None,
        "account_id": None,
        "plan": None,
        "user_id": None,
    }
    auth_json = read_json(auth_path) or {}
    tokens = auth_json.get("tokens")
    if not isinstance(tokens, dict):
        return payload
    token_payload = jwt_payload(tokens.get("id_token") or tokens.get("access_token"))
    auth_claims = token_payload.get("https://api.openai.com/auth")
    if not isinstance(auth_claims, dict):
        auth_claims = {}
    payload["email"] = token_payload.get("email")
    payload["account_id"] = auth_claims.get("chatgpt_account_id") or tokens.get("account_id")
    payload["plan"] = auth_claims.get("chatgpt_plan_type")
    payload["user_id"] = auth_claims.get("user_id")
    return payload


def login_identity_fingerprint(identity: dict[str, Any]) -> str:
    account_id = identity.get("account_id")
    email = identity.get("email")
    if account_id:
        return f"account:{account_id}"
    if email:
        return f"email:{email}"
    return f"login-home:{identity.get('login_home', '-')}"


def resilience_counts_display(counts: dict[str, Any]) -> str:
    return (
        f"ready={counts.get('ready', 0)} "
        f"active={counts.get('active', 0)} "
        f"exhausted={counts.get('exhausted', 0)} "
        f"disabled={counts.get('disabled', 0)}"
    )


def resilience_current_label(summary: dict[str, Any]) -> str:
    current_key = summary.get("current_account_key")
    current_alias = summary.get("current_alias")
    if not current_key:
        return "-"
    if current_alias and current_alias != current_key:
        return f"{current_key}/{current_alias}"
    return str(current_key)


def infer_task_mode(prompt: str, title: str | None = None) -> str:
    haystack = " ".join(part for part in (title or "", prompt) if part).lower()
    if any(hint in haystack for hint in PATCH_MODE_STRONG_HINTS):
        return "patch"
    if any(hint in haystack for hint in PROPOSAL_MODE_HINTS):
        return "proposal"
    if any(hint in haystack for hint in PATCH_MODE_WEAK_HINTS):
        return "patch"
    return "proposal"


def task_mode_label(mode: str | None) -> str:
    return "Patch bout" if str(mode or "proposal") == "patch" else "Proposal bout"


def task_apply_status_label(status: str | None) -> str:
    mapping = {
        "not_requested": "Not requested",
        "pending": "Pending apply",
        "applied": "Applied to target repo",
        "not_applied": "Not applied",
    }
    return mapping.get(str(status or "not_requested"), str(status or "not_requested"))


def asymptote_current_label(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "OFF")
    reason = str(payload.get("reason") or "-")
    progress = str(payload.get("progress_text") or "-")
    return f"{render_state_label(status)} | {reason} | {progress}"


def resilience_guard_snapshot(summary: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        vault = credential_vault()
        return vault.reserve_guard(vault.load() if summary is None else None) if summary is None else {
            **vault.reserve_guard(
                {
                    "version": 1,
                    "current_account_key": summary.get("current_account_key"),
                    "auto_switch": summary.get("auto_switch", True),
                    "last_rotation": summary.get("last_rotation") or {},
                    "reserve_percent_threshold": summary.get("reserve_percent_threshold"),
                    "accounts": {
                        profile.get("account_key"): {
                            "alias": profile.get("alias"),
                            "status": profile.get("status"),
                            "fingerprint": {"email": profile.get("email"), "account_id": None},
                            "auth_data": {},
                            "last_sync": profile.get("last_sync"),
                            "usage_state": profile.get("usage_state"),
                            "last_quota_blocked_at": profile.get("last_quota_blocked_at"),
                            "last_quota_reason": profile.get("last_quota_reason"),
                            "last_activated_at": None,
                            "last_verified_at": profile.get("last_verified_at"),
                            "usage_percent_remaining": profile.get("usage_percent_remaining"),
                            "usage_percent_used": profile.get("usage_percent_used"),
                            "usage_window_minutes": profile.get("usage_window_minutes"),
                            "usage_resets_at": profile.get("usage_resets_at"),
                            "usage_plan_type": profile.get("usage_plan_type"),
                            "usage_limit_id": profile.get("usage_limit_id"),
                            "usage_checked_at": profile.get("usage_checked_at"),
                        }
                        for profile in summary.get("profiles", [])
                    },
                }
            ),
            "summary_cached": True,
        }
    except Exception as exc:
        return {
            "active": False,
            "reason": f"resilience guard unavailable: {exc}",
            "current_account_key": None,
            "current_email": None,
            "current_remaining": None,
            "reserve_percent_threshold": None,
            "available_alternatives": [],
            "checked_at": now_utc(),
        }


def refresh_resilience_usage_summary(*, all_profiles: bool) -> dict[str, Any]:
    try:
        return probe_resilience_usage(all_profiles=all_profiles)
    except Exception:
        return resilience_summary()


def resilience_execution_guard(*, refresh_usage: bool = False) -> dict[str, Any]:
    summary = refresh_resilience_usage_summary(all_profiles=True) if refresh_usage else resilience_summary()
    return resilience_guard_snapshot(summary)


def usage_probe_worker_count(account_count: int) -> int:
    if account_count <= 1:
        return 1
    if DEFAULT_USAGE_PROBE_WORKERS is None:
        return account_count
    return max(1, min(DEFAULT_USAGE_PROBE_WORKERS, account_count))


def try_sync_current_resilience_profile() -> None:
    try:
        vault = credential_vault()
        current_key = vault.current_account_key()
        if current_key:
            vault.sync_auth(current_key)
    except VaultError:
        return


def ensure_selected_resilience_profile() -> str | None:
    try:
        vault = credential_vault()
        current_key = vault.current_account_key()
        if not current_key:
            return None
        resolved_key = vault.resolve_account_ref(current_key)
        vault.inject_auth(resolved_key)
        return resolved_key
    except VaultError:
        return None


def ensure_live_codex_home_auth() -> None:
    ensure_codex_home_config(LAB_HOME, [ROOT])
    auth_payload: dict[str, Any] | None = None
    try:
        vault = credential_vault()
        current_key = vault.current_account_key()
        if current_key:
            auth_payload = vault.account_auth_data(current_key)
    except VaultError:
        auth_payload = None
    if not auth_payload:
        auth_payload = read_json(RESILIENCE_AUTH_PATH) or read_json(LAB_HOME / "auth.json")
    if auth_payload:
        atomic_write_json(LAB_HOME / "auth.json", auth_payload)


def run_resilience_profile_login(alias: str | None = None) -> str:
    LOGIN_CODEX_HOME.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["CODEX_HOME"] = str(LOGIN_CODEX_HOME)
    requested_alias = (alias or "").strip()
    print("")
    if requested_alias:
        print(f"Starting codex login for profile alias={requested_alias}")
    else:
        print("Starting codex login for the next numbered profile")
    print(f"Login home: {LOGIN_CODEX_HOME}")
    subprocess.run([REAL_CODEX, "login"], check=True, env=env)
    account_key = credential_vault().register_current(requested_alias or None)
    return account_key


def rotate_resilience_account(*, reason: str) -> str | None:
    try:
        return credential_vault().rotate_account(reason=reason)
    except VaultError:
        return None


def run_dir_quota_blocked(run_dir_path: Path) -> bool:
    combined = "\n".join(
        part
        for part in (
            read_text_file(run_dir_path / "stderr.log"),
            read_text_file(run_dir_path / "stdout.log"),
        )
        if part
    )
    return is_quota_text(combined)


def is_usage_limit_text(text: str | None) -> bool:
    normalized = (text or "").lower()
    return bool(normalized) and (
        "usage limit" in normalized
        and ("codex" in normalized or "plus" in normalized or "try again at" in normalized)
    )


def trusted_project_block(path: Path) -> str:
    return f'[projects."{path.resolve()}"]\ntrust_level = "trusted"\n'


def ensure_codex_home_config(home: Path, trusted_paths: list[Path]) -> None:
    home.mkdir(parents=True, exist_ok=True)
    config_path = home / "config.toml"
    content = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    updated = content
    changed = False
    for trusted_path in trusted_paths:
        block = trusted_project_block(trusted_path)
        header = block.splitlines()[0]
        if header in updated:
            continue
        if updated and not updated.endswith("\n"):
            updated += "\n"
        if updated and not updated.endswith("\n\n"):
            updated += "\n"
        updated += block
        changed = True
    if changed:
        config_path.write_text(updated, encoding="utf-8")


def ensure_layout() -> None:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    DAEMON_DIR.mkdir(parents=True, exist_ok=True)
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
    ensure_codex_home_config(LAB_HOME, [ROOT])
    for lane_id, _lane_type in ALL_LANES:
        paths = lane_paths(lane_id)
        paths["home"].mkdir(parents=True, exist_ok=True)
        paths["workspace"].mkdir(parents=True, exist_ok=True)
        ensure_codex_home_config(paths["home"], [paths["workspace"]])


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
            task_mode TEXT NOT NULL DEFAULT 'proposal',
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
            max_role_swaps INTEGER NOT NULL DEFAULT 6,
            duel_stage TEXT NOT NULL DEFAULT 'initial',
            pair_mode TEXT NOT NULL DEFAULT 'initial_duel',
            evaluator_tier TEXT NOT NULL DEFAULT 'primary',
            tier_phase TEXT NOT NULL DEFAULT 'base',
            rematch_brief TEXT NOT NULL DEFAULT '',
            apply_status TEXT NOT NULL DEFAULT 'not_requested',
            applied_submission_id TEXT,
            applied_at TEXT,
            apply_notes TEXT NOT NULL DEFAULT '',
            worker_a_session_id TEXT,
            worker_b_session_id TEXT,
            evaluator_session_id TEXT
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
            evaluator_tier TEXT NOT NULL DEFAULT 'primary',
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
            session_id TEXT,
            prompt_bytes INTEGER,
            prompt_style TEXT NOT NULL DEFAULT 'full',
            used_resume INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            exit_code INTEGER
        );
        """
    )
    current_schema_version_row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    current_schema_version = int(current_schema_version_row[0]) if current_schema_version_row else 0
    for key, value in (
        ("schema_version", "5"),
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
    ensure_schema_v3(conn, current_schema_version)
    ensure_schema_v4(conn, current_schema_version)
    ensure_schema_v5(conn, current_schema_version)
    conn.commit()


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    if column_name in table_columns(conn, table_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {definition}")


def infer_task_control_state(conn: sqlite3.Connection, task: sqlite3.Row) -> tuple[str, str, str, str]:
    if bool(task["masterpiece_locked"]):
        return "defense", "challenger_retry", "primary", "base"

    active_submissions = 0
    for lane_id in WORKER_LANES:
        lane = query_lane(conn, lane_id)
        if lane and lane["active_task_id"] == task["task_id"] and lane["active_submission_id"]:
            active_submissions += 1

    if not task["champion_submission_id"]:
        if active_submissions >= 2:
            return "initial", "initial_duel", "primary", "base"
        return "initial", "initial_duel", "primary", "base"

    if active_submissions >= 2:
        return "tiebreak", "full_rematch", "primary", "post_rematch"

    if int(task["total_evaluations"] or 0) <= 1:
        return "counterattack", "challenger_retry", "primary", "base"
    return "defense", "challenger_retry", "primary", "base"


def ensure_schema_v3(conn: sqlite3.Connection, current_schema_version: int) -> None:
    ensure_column(conn, "tasks", "duel_stage", "duel_stage TEXT NOT NULL DEFAULT 'initial'")
    ensure_column(conn, "tasks", "pair_mode", "pair_mode TEXT NOT NULL DEFAULT 'initial_duel'")
    ensure_column(conn, "tasks", "evaluator_tier", "evaluator_tier TEXT NOT NULL DEFAULT 'primary'")
    ensure_column(conn, "tasks", "tier_phase", "tier_phase TEXT NOT NULL DEFAULT 'base'")
    ensure_column(conn, "tasks", "rematch_brief", "rematch_brief TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "evaluations", "evaluator_tier", "evaluator_tier TEXT NOT NULL DEFAULT 'primary'")

    conn.execute("UPDATE tasks SET max_retry_failures = ?", (MAX_RETRY_FAILURES,))
    if current_schema_version >= 3:
        return

    for task in conn.execute("SELECT * FROM tasks ORDER BY created_at ASC, task_id ASC").fetchall():
        duel_stage, pair_mode, evaluator_tier, tier_phase = infer_task_control_state(conn, task)
        conn.execute(
            """
            UPDATE tasks
            SET duel_stage = ?,
                pair_mode = ?,
                evaluator_tier = ?,
                tier_phase = ?,
                rematch_brief = COALESCE(rematch_brief, '')
            WHERE task_id = ?
            """,
            (duel_stage, pair_mode, evaluator_tier, tier_phase, task["task_id"]),
        )
    conn.execute("UPDATE meta SET value = '3' WHERE key = 'schema_version'")


def ensure_schema_v4(conn: sqlite3.Connection, current_schema_version: int) -> None:
    ensure_column(conn, "tasks", "worker_a_session_id", "worker_a_session_id TEXT")
    ensure_column(conn, "tasks", "worker_b_session_id", "worker_b_session_id TEXT")
    ensure_column(conn, "tasks", "evaluator_session_id", "evaluator_session_id TEXT")
    ensure_column(conn, "runs", "session_id", "session_id TEXT")
    ensure_column(conn, "runs", "prompt_bytes", "prompt_bytes INTEGER")
    ensure_column(conn, "runs", "prompt_style", "prompt_style TEXT NOT NULL DEFAULT 'full'")
    ensure_column(conn, "runs", "used_resume", "used_resume INTEGER NOT NULL DEFAULT 0")
    if current_schema_version >= 4:
        return
    conn.execute("UPDATE meta SET value = '4' WHERE key = 'schema_version'")


def ensure_schema_v5(conn: sqlite3.Connection, current_schema_version: int) -> None:
    ensure_column(conn, "tasks", "task_mode", "task_mode TEXT NOT NULL DEFAULT 'proposal'")
    ensure_column(conn, "tasks", "apply_status", "apply_status TEXT NOT NULL DEFAULT 'not_requested'")
    ensure_column(conn, "tasks", "applied_submission_id", "applied_submission_id TEXT")
    ensure_column(conn, "tasks", "applied_at", "applied_at TEXT")
    ensure_column(conn, "tasks", "apply_notes", "apply_notes TEXT NOT NULL DEFAULT ''")
    if current_schema_version >= 5:
        return
    for task in conn.execute("SELECT task_id, title, prompt, task_mode, masterpiece_locked FROM tasks").fetchall():
        inferred_mode = infer_task_mode(str(task["prompt"] or ""), str(task["title"] or ""))
        apply_status = "not_requested"
        if inferred_mode == "patch" and bool(task["masterpiece_locked"]):
            apply_status = "not_applied"
        conn.execute(
            """
            UPDATE tasks
            SET task_mode = COALESCE(NULLIF(task_mode, ''), ?),
                apply_status = CASE
                    WHEN apply_status IS NULL OR apply_status = '' THEN ?
                    ELSE apply_status
                END,
                apply_notes = COALESCE(apply_notes, '')
            WHERE task_id = ?
            """,
            (inferred_mode, apply_status, task["task_id"]),
        )
    conn.execute("UPDATE meta SET value = '5' WHERE key = 'schema_version'")


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
    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    os.replace(temp_path, path)


def read_json(path: Path, *, tolerate_empty: bool = False, tolerate_invalid: bool = False) -> dict[str, Any] | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        if tolerate_empty:
            return None
        return json.loads(text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        if tolerate_invalid:
            return None
        raise


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def daemon_pid_payload() -> dict[str, Any] | None:
    payload = read_json(DAEMON_PID_FILE, tolerate_empty=True, tolerate_invalid=True)
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


def truncate_text(value: Any, limit: int = 240) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= limit:
        return value
    return value[: max(limit - 3, 0)] + "..."


def compact_quota_monitor(monitor: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(monitor, dict):
        return {}
    blocked_lanes = []
    for item in monitor.get("blocked_lanes") or []:
        if not isinstance(item, dict):
            continue
        blocked_lanes.append(
            {
                "lane_id": item.get("lane_id"),
                "lane_type": item.get("lane_type"),
                "task_id": item.get("task_id"),
                "run_id": item.get("run_id"),
                "mode": item.get("mode"),
                "message": truncate_text(item.get("message")),
            }
        )
    probe = monitor.get("last_probe") if isinstance(monitor.get("last_probe"), dict) else {}
    return {
        "enabled": bool(monitor.get("enabled", False)),
        "status": monitor.get("status"),
        "login_identity": monitor.get("login_identity") if isinstance(monitor.get("login_identity"), dict) else {},
        "current_identity_fingerprint": monitor.get("current_identity_fingerprint"),
        "recheck_interval_seconds": monitor.get("recheck_interval_seconds"),
        "blocked_lanes": blocked_lanes,
        "identity_changed": bool(monitor.get("identity_changed", False)),
        "last_probe_at": monitor.get("last_probe_at"),
        "last_probe_identity": monitor.get("last_probe_identity"),
        "last_probe": {
            "ok": probe.get("ok"),
            "quota_blocked": probe.get("quota_blocked"),
            "exit_code": probe.get("exit_code"),
            "message": truncate_text(probe.get("message")),
        }
        if probe
        else {},
        "last_recovered_at": monitor.get("last_recovered_at"),
    }


def compact_daemon_actions(actions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for action in (actions or [])[:8]:
        if not isinstance(action, dict):
            continue
        compacted.append({key: truncate_text(value) for key, value in action.items()})
    return compacted


def compact_daemon_state_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        return {}
    resilience_guard = payload.get("resilience_guard") if isinstance(payload.get("resilience_guard"), dict) else {}
    return {
        "pid": int(payload.get("pid", 0) or 0),
        "executor": payload.get("executor"),
        "interval": payload.get("interval"),
        "cycle_count": payload.get("cycle_count"),
        "started_at": payload.get("started_at"),
        "last_heartbeat": payload.get("last_heartbeat"),
        "last_progress": bool(payload.get("last_progress", False)),
        "last_progress_at": payload.get("last_progress_at"),
        "runnable_after": bool(payload.get("runnable_after", False)),
        "stop_requested": bool(payload.get("stop_requested", False)),
        "reason": payload.get("reason"),
        "last_actions": compact_daemon_actions(payload.get("last_actions")),
        "quota_monitor": compact_quota_monitor(payload.get("quota_monitor")),
        "resilience_guard": {
            "active": bool(resilience_guard.get("active", False)),
            "reason": truncate_text(resilience_guard.get("reason")),
            "current_account_key": resilience_guard.get("current_account_key"),
            "current_email": resilience_guard.get("current_email"),
            "current_remaining": resilience_guard.get("current_remaining"),
            "reserve_percent_threshold": resilience_guard.get("reserve_percent_threshold"),
            "available_alternatives": list(resilience_guard.get("available_alternatives") or [])[:8],
            "checked_at": resilience_guard.get("checked_at"),
        }
        if resilience_guard
        else {},
        "exec_timeout": payload.get("exec_timeout"),
        "stopped_at": payload.get("stopped_at"),
        "running": bool(payload.get("running", False)),
        "state_compacted": bool(payload.get("state_compacted", False) or ("status_snapshot" in payload)),
    }


def oversized_daemon_state_payload(*, pid_payload: dict[str, Any] | None, state_file: Path) -> dict[str, Any]:
    stat = state_file.stat()
    pid = int(pid_payload.get("pid", 0) or 0) if pid_payload else 0
    return compact_daemon_state_payload(
        {
            "pid": pid,
            "executor": pid_payload.get("executor") if pid_payload else None,
            "interval": pid_payload.get("interval") if pid_payload else None,
            "started_at": pid_payload.get("started_at") if pid_payload else None,
            "last_heartbeat": datetime.fromtimestamp(stat.st_mtime, timezone.utc).replace(microsecond=0).isoformat(),
            "last_progress": False,
            "runnable_after": False,
            "stop_requested": False,
            "reason": "oversized_state_file",
            "last_actions": [],
            "quota_monitor": {},
            "exec_timeout": pid_payload.get("exec_timeout") if pid_payload else None,
            "running": bool(pid_payload and process_alive(pid)),
            "state_compacted": True,
        }
    )


def read_daemon_state_payload(pid_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if not DAEMON_STATE_FILE.exists():
        return {}
    pid = int(pid_payload.get("pid", 0) or 0) if pid_payload else 0
    running = bool(pid_payload and process_alive(pid))
    try:
        size = DAEMON_STATE_FILE.stat().st_size
    except OSError:
        size = 0
    if size > MAX_DAEMON_STATE_FILE_BYTES:
        compacted = oversized_daemon_state_payload(pid_payload=pid_payload, state_file=DAEMON_STATE_FILE)
        if not running:
            write_json(DAEMON_STATE_FILE, compacted)
        return compacted
    payload = read_json(DAEMON_STATE_FILE, tolerate_empty=True, tolerate_invalid=True) or {}
    compacted = compact_daemon_state_payload(payload)
    if compacted != payload and not running:
        write_json(DAEMON_STATE_FILE, compacted)
    return compacted


def daemon_runtime_snapshot() -> dict[str, Any]:
    pid_payload = daemon_pid_payload()
    state_payload = read_daemon_state_payload(pid_payload)
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


def query_lane(conn: sqlite3.Connection, lane_id: str | None) -> sqlite3.Row | None:
    if not lane_id:
        return None
    return conn.execute("SELECT * FROM lanes WHERE lane_id = ?", (lane_id,)).fetchone()


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
    add_command = ["git", "-C", repo_top, "worktree", "add", "--detach", str(workspace), "HEAD"]
    result = subprocess.run(
        add_command,
        capture_output=True,
        text=True,
        check=False,
    )
    failure_text = "\n".join(part for part in (result.stderr.strip(), result.stdout.strip()) if part)
    if result.returncode != 0 and "missing but already registered worktree" in failure_text:
        subprocess.run(
            ["git", "-C", repo_top, "worktree", "prune"],
            capture_output=True,
            text=True,
            check=False,
        )
        result = subprocess.run(
            add_command,
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


def workspace_metadata(workspace: Path) -> dict[str, Any]:
    manifest_path = workspace / "codexlab-task.json"
    if not manifest_path.exists():
        return {
            "workspace_path": str(workspace),
            "workspace_kind": "unknown",
            "repo_root": None,
            "head_sha": None,
            "fallback_reason": "missing codexlab-task.json",
        }
    return hydrate_workspace_metadata(manifest_path)


def parse_git_status_line(raw_line: str) -> tuple[str, str, str | None] | None:
    line = raw_line.rstrip()
    if len(line) < 4:
        return None
    status = line[:2]
    payload = line[3:].strip()
    if not payload:
        return None
    if " -> " in payload:
        old_path, new_path = payload.split(" -> ", 1)
        return status, new_path.strip(), old_path.strip()
    return status, payload, None


def workspace_change_evidence(workspace: Path) -> dict[str, Any]:
    metadata = workspace_metadata(workspace)
    evidence: dict[str, Any] = {
        "workspace_path": str(workspace),
        "workspace_kind": metadata.get("workspace_kind"),
        "repo_root": metadata.get("repo_root"),
        "head_sha": metadata.get("head_sha"),
        "fallback_reason": metadata.get("fallback_reason"),
        "status_lines": [],
        "changed_files": [],
        "modified_files": [],
        "untracked_files": [],
        "deleted_files": [],
        "renamed_files": [],
        "tests_run": [],
        "has_changes": False,
        "workspace_summary": "No repository changes detected.",
    }
    if shutil.which("git") is None:
        evidence["workspace_summary"] = "Git is unavailable, so workspace changes could not be inspected."
        return evidence
    result = subprocess.run(
        ["git", "-C", str(workspace), "status", "--short", "--untracked-files=all"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "workspace is not a git repository"
        evidence["workspace_summary"] = f"Workspace change inspection unavailable: {message}"
        return evidence

    status_lines = [line for line in result.stdout.splitlines() if line.strip()]
    changed_files: list[str] = []
    modified_files: list[str] = []
    untracked_files: list[str] = []
    deleted_files: list[str] = []
    renamed_files: list[dict[str, str]] = []

    for raw_line in status_lines:
        parsed = parse_git_status_line(raw_line)
        if parsed is None:
            continue
        status, path_text, previous_path = parsed
        if path_text == "codexlab-task.json":
            continue
        changed_files.append(path_text)
        if status == "??":
            untracked_files.append(path_text)
            continue
        if "D" in status:
            deleted_files.append(path_text)
        elif "R" in status and previous_path:
            renamed_files.append({"from": previous_path, "to": path_text})
            modified_files.append(path_text)
        else:
            modified_files.append(path_text)

    changed_files = sorted(dict.fromkeys(changed_files))
    modified_files = sorted(dict.fromkeys(modified_files))
    untracked_files = sorted(dict.fromkeys(untracked_files))
    deleted_files = sorted(dict.fromkeys(deleted_files))
    evidence.update(
        {
            "status_lines": status_lines,
            "changed_files": changed_files,
            "modified_files": modified_files,
            "untracked_files": untracked_files,
            "deleted_files": deleted_files,
            "renamed_files": renamed_files,
            "has_changes": bool(changed_files or deleted_files),
        }
    )
    if evidence["has_changes"]:
        fragments = []
        if untracked_files:
            fragments.append(f"{len(untracked_files)} new")
        if modified_files:
            fragments.append(f"{len(modified_files)} modified")
        if deleted_files:
            fragments.append(f"{len(deleted_files)} deleted")
        evidence["workspace_summary"] = (
            f"{len(changed_files)} file changes detected"
            + (f" ({', '.join(fragments)})" if fragments else "")
            + "."
        )
    return evidence


def submission_runtime_and_evidence(
    *,
    workspace: Path,
    runtime: dict[str, Any],
) -> dict[str, Any]:
    return {
        "runtime": runtime,
        "evidence": workspace_change_evidence(workspace),
    }


def task_dir(task_id: str) -> Path:
    return TASKS_DIR / task_id


def ensure_task_dirs(task_id: str) -> Path:
    base = task_dir(task_id)
    (base / "submissions").mkdir(parents=True, exist_ok=True)
    (base / "evaluations").mkdir(parents=True, exist_ok=True)
    (base / "current_best").mkdir(parents=True, exist_ok=True)
    (base / "submission-cards").mkdir(parents=True, exist_ok=True)
    (base / "decision-cards").mkdir(parents=True, exist_ok=True)
    return base


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id


def latest_failed_run_for_lane(conn: sqlite3.Connection, lane_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM runs
        WHERE lane_id = ?
          AND status IN ('failed', 'timeout')
        ORDER BY started_at DESC, run_id DESC
        LIMIT 1
        """,
        (lane_id,),
    ).fetchone()


def read_text_file(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def task_session_column(lane_id: str) -> str:
    if lane_id not in {item[0] for item in ALL_LANES}:
        raise SystemExit(f"Unknown lane for session storage: {lane_id}")
    return f"{lane_id.replace('-', '_')}_session_id"


def task_lane_session_id(task: sqlite3.Row | dict[str, Any], lane_id: str) -> str | None:
    key = task_session_column(lane_id)
    if isinstance(task, dict):
        value = task.get(key)
    else:
        value = task[key]
    return str(value).strip() if value else None


def store_task_lane_session_id(conn: sqlite3.Connection, task_id: str, lane_id: str, session_id: str) -> None:
    session_id = str(session_id).strip()
    if not session_id:
        return
    column = task_session_column(lane_id)
    conn.execute(f"UPDATE tasks SET {column} = ?, updated_at = ? WHERE task_id = ?", (session_id, now_utc(), task_id))
    conn.commit()


def clear_task_lane_session_id(conn: sqlite3.Connection, task_id: str, lane_id: str) -> None:
    column = task_session_column(lane_id)
    conn.execute(f"UPDATE tasks SET {column} = NULL, updated_at = ? WHERE task_id = ?", (now_utc(), task_id))
    conn.commit()


def update_run_metadata(conn: sqlite3.Connection, run_id: str, **fields: Any) -> None:
    assignments: list[str] = []
    params: list[Any] = []
    for key, value in fields.items():
        if value is None:
            continue
        assignments.append(f"{key} = ?")
        params.append(int(value) if isinstance(value, bool) else value)
    if not assignments:
        return
    params.append(run_id)
    conn.execute(f"UPDATE runs SET {', '.join(assignments)} WHERE run_id = ?", tuple(params))
    conn.commit()


def session_id_from_jsonl(path: Path, *, expected_cwd: Path | None = None) -> str | None:
    if not path.exists():
        return None
    expected_cwd_text = str(expected_cwd) if expected_cwd is not None else None
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict) or payload.get("type") != "session_meta":
                    continue
                meta = payload.get("payload")
                if not isinstance(meta, dict):
                    continue
                if expected_cwd_text is not None and str(meta.get("cwd") or "") != expected_cwd_text:
                    continue
                session_id = str(meta.get("id") or "").strip()
                if session_id:
                    return session_id
    except OSError:
        return None
    return None


def codex_session_roots() -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for home in (LAB_HOME, LOGIN_CODEX_HOME):
        session_root = home / "sessions"
        key = str(session_root.resolve()) if session_root.exists() else str(session_root)
        if key in seen:
            continue
        seen.add(key)
        roots.append(session_root)
    return roots


def session_file_exists(session_id: str) -> bool:
    session_id = str(session_id or "").strip()
    if not session_id:
        return False
    candidates: list[tuple[float, Path]] = []
    for sessions_root in codex_session_roots():
        if not sessions_root.exists():
            continue
        for session_path in sessions_root.rglob("rollout-*.jsonl"):
            try:
                stat = session_path.stat()
            except OSError:
                continue
            candidates.append((stat.st_mtime, session_path))
    candidates.sort(key=lambda item: item[0], reverse=True)
    for _mtime, session_path in candidates[:400]:
        if session_id_from_jsonl(session_path) == session_id:
            return True
    return False


def live_task_lane_session_id(conn: sqlite3.Connection, task: sqlite3.Row, lane_id: str) -> str | None:
    session_id = task_lane_session_id(task, lane_id)
    if not session_id:
        return None
    if session_file_exists(session_id):
        return session_id
    clear_task_lane_session_id(conn, task["task_id"], lane_id)
    return None


def run_stdout_session_id(run_output_dir: Path) -> str | None:
    return session_id_from_jsonl(run_output_dir / "stdout.log")


def discover_session_id_from_disk(workspace: Path, started_at: str) -> str | None:
    started_dt = parse_timestamp(started_at)
    cutoff = started_dt - timedelta(minutes=5) if started_dt else None
    candidates: list[tuple[float, Path]] = []
    for sessions_root in codex_session_roots():
        if not sessions_root.exists():
            continue
        for session_path in sessions_root.rglob("rollout-*.jsonl"):
            try:
                stat = session_path.stat()
            except OSError:
                continue
            candidates.append((stat.st_mtime, session_path))
    candidates.sort(key=lambda item: item[0], reverse=True)
    for mtime, session_path in candidates[:200]:
        if cutoff is not None and datetime.fromtimestamp(mtime, timezone.utc) < cutoff:
            break
        session_id = session_id_from_jsonl(session_path, expected_cwd=workspace)
        if session_id:
            return session_id
    return None


def capture_run_session_id(run_output_dir: Path, workspace: Path, started_at: str) -> str | None:
    session_id = run_stdout_session_id(run_output_dir)
    if session_id:
        return session_id
    return discover_session_id_from_disk(workspace, started_at)


def finalize_task_lane_session(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    lane_id: str,
    handle: RunHandle,
    workspace: Path,
    known_session_id: str | None = None,
) -> str | None:
    session_id = (known_session_id or "").strip() or capture_run_session_id(handle.run_dir, workspace, handle.started_at)
    if not session_id:
        return None
    update_run_metadata(conn, handle.run_id, session_id=session_id)
    store_task_lane_session_id(conn, task_id, lane_id, session_id)
    return session_id


def quota_blocked_lane_info(conn: sqlite3.Connection, lane_id: str) -> dict[str, Any] | None:
    lane = fetch_lane(conn, lane_id)
    if lane["status"] != "error":
        return None
    run = latest_failed_run_for_lane(conn, lane_id)
    if run is None or not str(run["mode"]).startswith("codex:"):
        return None
    stderr_text = read_text_file(run_dir(run["run_id"]) / "stderr.log")
    stdout_text = read_text_file(run_dir(run["run_id"]) / "stdout.log")
    combined = "\n".join(part for part in (lane["notes"], stderr_text, stdout_text) if part)
    if not is_usage_limit_text(combined):
        return None
    message = textwrap.shorten(" ".join(combined.split()), width=220, placeholder="...")
    return {
        "lane_id": lane["lane_id"],
        "lane_type": lane["lane_type"],
        "task_id": lane["active_task_id"],
        "run_id": run["run_id"],
        "mode": run["mode"],
        "message": message,
    }


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
    started_at = now_utc()
    target = run_dir(run_id)
    target.mkdir(parents=True, exist_ok=True)
    (target / "command.json").write_text(json.dumps(command, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    conn.execute(
        """
        INSERT INTO runs(
            run_id, lane_id, task_id, submission_id, mode, status, command, cwd, codex_home,
            session_id, prompt_bytes, prompt_style, used_resume, started_at, finished_at, exit_code
        )
        VALUES(?, ?, ?, NULL, ?, 'running', ?, ?, ?, NULL, NULL, 'full', 0, ?, NULL, NULL)
        """,
        (run_id, lane_id, task_id, mode, json.dumps(command, ensure_ascii=True), str(cwd), str(codex_home), started_at),
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
    return RunHandle(run_id=run_id, lane_id=lane_id, task_id=task_id, mode=mode, run_dir=target, started_at=started_at)


def update_run_command(conn: sqlite3.Connection, handle: RunHandle, command: list[str]) -> None:
    (handle.run_dir / "command.json").write_text(json.dumps(command, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    conn.execute(
        "UPDATE runs SET command = ? WHERE run_id = ?",
        (json.dumps(command, ensure_ascii=True), handle.run_id),
    )
    conn.commit()


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
    append_event(
        "run_finished",
        run_id=handle.run_id,
        lane_id=handle.lane_id,
        task_id=handle.task_id,
        mode=handle.mode,
        status=status,
        exit_code=exit_code,
        submission_id=submission_id,
    )


def set_lane_error(conn: sqlite3.Connection, lane_id: str, message: str, task_id: str | None = None) -> None:
    conn.execute(
        """
        UPDATE lanes
        SET status = 'error', notes = ?, updated_at = ?
        WHERE lane_id = ?
        """,
        (message[:400], now_utc(), lane_id),
    )
    conn.commit()
    append_event("lane_error", lane_id=lane_id, task_id=task_id, message=message[:400])


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
        "tie_rematch": 0,
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
    status = "retrying" if reservation["reservation_type"] in {"duel_retry", "tie_rematch"} else "assigned"
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
    desired_status = "retrying" if reservation_type in {"duel_retry", "tie_rematch"} else "assigned"
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

    if lane["active_task_id"] == task_id and reservation_type in {"duel_retry", "tie_rematch"}:
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


def parse_json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def submission_metadata(submission: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    if isinstance(submission, dict):
        return parse_json_object(submission.get("meta_json"))
    return parse_json_object(submission["meta_json"])


def submission_runtime_meta(submission: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    meta = submission_metadata(submission)
    runtime = meta.get("runtime")
    if isinstance(runtime, dict):
        return runtime
    if meta:
        return meta
    return {}


def submission_evidence_meta(submission: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    meta = submission_metadata(submission)
    evidence = meta.get("evidence")
    return evidence if isinstance(evidence, dict) else {}


def submission_evidence_prompt_block(submission: sqlite3.Row | dict[str, Any]) -> str:
    evidence = submission_evidence_meta(submission)
    if not evidence:
        return "No recorded workspace evidence."
    changed_files = evidence.get("changed_files") or []
    deleted_files = evidence.get("deleted_files") or []
    tests_run = evidence.get("tests_run") or []
    lines = [
        f"Workspace summary: {evidence.get('workspace_summary') or 'No workspace summary.'}",
        f"Changed files: {', '.join(changed_files) if changed_files else '-'}",
    ]
    if deleted_files:
        lines.append(f"Deleted files: {', '.join(deleted_files)}")
    if tests_run:
        lines.append(f"Tests run: {', '.join(tests_run)}")
    return "\n".join(lines)


def submission_stage_info(submission: dict[str, Any]) -> tuple[str, str]:
    meta = parse_json_object(submission.get("meta_json"))
    stage_slug = str(meta.get("stage_slug") or "").strip()
    stage_label = str(meta.get("stage_label") or "").strip()
    if stage_slug and stage_label:
        return stage_slug, stage_label
    phase = str(submission.get("phase") or "initial")
    if phase == "initial":
        return "championship-bout", "Championship bout"
    if phase == "retry":
        return "challenger-rematch", "Challenger rematch"
    if phase == "rematch":
        return "draw-rematch", "Draw rematch"
    if phase == "review":
        return "judge-review", "Judge review"
    return "open-bout", "Open bout"


def evaluation_stage_info(evaluation: dict[str, Any]) -> tuple[str, str]:
    scorecard = parse_json_object(evaluation.get("scorecard_json"))
    return boxing_stage_info(
        pair_mode=scorecard.get("pair_mode"),
        duel_stage=scorecard.get("duel_stage"),
        evaluator_tier=evaluation.get("evaluator_tier"),
        tier_phase=scorecard.get("tier_phase"),
    )


def clear_generated_markdown(directory: Path) -> None:
    if not directory.exists():
        return
    for child in directory.iterdir():
        if child.is_file() and child.suffix == ".md":
            child.unlink()


def reset_generated_dir(directory: Path) -> None:
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)


def write_task_boxing_views(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    submissions: list[dict[str, Any]],
    evaluations: list[dict[str, Any]],
) -> None:
    task_id = str(task["task_id"])
    base = ensure_task_dirs(task_id)
    stage_slug, stage_label = boxing_stage_info(
        pair_mode=task["pair_mode"],
        duel_stage=task["duel_stage"],
        evaluator_tier=task["evaluator_tier"],
        tier_phase=task["tier_phase"],
    )
    if task["masterpiece_locked"]:
        stage_slug, stage_label = "champion-confirmed", "Champion confirmed"
    champion = fetch_submission(conn, task["champion_submission_id"]) if task["champion_submission_id"] else None
    challenger = fetch_submission(conn, task["challenger_submission_id"]) if task["challenger_submission_id"] else None

    bout_lines = [
        f"# Bout {task_id}",
        "",
        f"Title: {task['title']}",
        f"Prompt: {task['prompt']}",
        f"Task mode: {task_mode_label(task['task_mode'])}",
        f"Current stage: {stage_label}",
        f"Status: {'Champion confirmed' if task['masterpiece_locked'] else task['status']}",
        f"Apply status: {task_apply_status_label(task['apply_status'])}",
        "",
        "ID legend:",
        "- T = Task",
        "- S = Submission",
        "- E = Evaluation",
        "",
        f"Champion: {lane_submission_ref(task['champion_lane_id'], task['champion_submission_id'])}",
        f"Challenger: {lane_submission_ref(task['challenger_lane_id'], task['challenger_submission_id'])}",
        f"Judge tier: {evaluator_display_label(task['evaluator_tier'])}",
        "",
        "Readable artifact folders:",
        "- submission-cards/",
        "- decision-cards/",
    ]
    (base / "bout.md").write_text("\n".join(bout_lines).rstrip() + "\n", encoding="utf-8")

    champion_card = base / "champion-card.md"
    if champion:
        champion_lines = [
            f"# Champion Card {task_id}",
            "",
            f"Champion: {lane_display_name(champion['lane_id'])}",
            f"Submission: {champion['submission_id']}",
            f"Score: {task['champion_score']}",
            f"Apply status: {task_apply_status_label(task['apply_status'])}",
            f"Summary: {champion['summary']}",
            f"Canonical output: submissions/{champion['submission_id']}/output.md",
            f"Canonical metadata: submissions/{champion['submission_id']}/metadata.json",
        ]
        if task["apply_notes"]:
            champion_lines.append(f"Apply notes: {task['apply_notes']}")
        if task["masterpiece_locked"]:
            champion_lines.insert(2, "Status: Champion confirmed")
        champion_card.write_text("\n".join(champion_lines).rstrip() + "\n", encoding="utf-8")
    elif champion_card.exists():
        champion_card.unlink()

    submission_cards_dir = base / "submission-cards"
    decision_cards_dir = base / "decision-cards"
    clear_generated_markdown(submission_cards_dir)
    clear_generated_markdown(decision_cards_dir)
    submissions_by_corner_dir = base / "submissions" / "by-corner"
    evaluations_by_judge_dir = base / "evaluations" / "by-judge"
    reset_generated_dir(submissions_by_corner_dir)
    reset_generated_dir(evaluations_by_judge_dir)

    submission_groups: dict[str, list[dict[str, Any]]] = {}
    for submission in submissions:
        submission_groups.setdefault(str(submission["lane_id"]), []).append(submission)

    submissions_index_lines = [
        f"# Submission Index {task_id}",
        "",
        "Canonical IDs:",
        "- S = Submission",
        "",
        "By corner:",
    ]
    for lane_id in WORKER_LANES:
        corner_dir = submissions_by_corner_dir / lane_slug(lane_id)
        corner_dir.mkdir(parents=True, exist_ok=True)
        entries = submission_groups.get(lane_id, [])
        submissions_index_lines.append(f"- {lane_display_name(lane_id)} -> by-corner/{lane_slug(lane_id)}/")
        latest_entry = entries[-1] if entries else None
        latest_lines = [
            f"# {lane_display_name(lane_id)}",
            "",
            f"Corner: {lane_display_name(lane_id)}",
            f"Task: {task_id}",
        ]
        if latest_entry:
            latest_stage_slug, latest_stage_label = submission_stage_info(latest_entry)
            latest_lines.extend(
                [
                    f"Latest card: {latest_entry['submission_id']}",
                    f"Round: {int(latest_entry['round_number']):02d}",
                    f"Stage: {latest_stage_label}",
                    f"Canonical output: submissions/{latest_entry['submission_id']}/output.md",
                    f"Canonical metadata: submissions/{latest_entry['submission_id']}/metadata.json",
                ]
            )
        else:
            latest_lines.append("Latest card: none")
        (corner_dir / "latest.md").write_text("\n".join(latest_lines).rstrip() + "\n", encoding="utf-8")

    for submission in submissions:
        stage_slug, stage_label = submission_stage_info(submission)
        evidence = submission_evidence_meta(submission)
        filename = (
            f"round-{int(submission['round_number']):02d}__{lane_slug(submission['lane_id'])}"
            f"__{stage_slug}__{submission['submission_id']}.md"
        )
        card_lines = [
            f"# Round {int(submission['round_number']):02d} | {lane_display_name(submission['lane_id'])} | {stage_label}",
            "",
            f"Submission ID: {submission['submission_id']}",
            f"Task ID: {submission['task_id']}",
            f"Corner: {lane_display_name(submission['lane_id'])}",
            f"Phase: {submission['phase']}",
            f"Retry number: {submission['retry_number']}",
            f"Summary: {submission['summary']}",
            f"Published: {bool(submission.get('published'))}",
            f"Canonical output: submissions/{submission['submission_id']}/output.md",
            f"Canonical metadata: submissions/{submission['submission_id']}/metadata.json",
            f"Workspace summary: {evidence.get('workspace_summary') or '-'}",
            f"Changed files: {', '.join(evidence.get('changed_files') or []) or '-'}",
        ]
        (submission_cards_dir / filename).write_text("\n".join(card_lines).rstrip() + "\n", encoding="utf-8")
        corner_dir = submissions_by_corner_dir / lane_slug(submission["lane_id"])
        corner_filename = (
            f"round-{int(submission['round_number']):02d}__{stage_slug}__{submission['submission_id']}.md"
        )
        (corner_dir / corner_filename).write_text("\n".join(card_lines).rstrip() + "\n", encoding="utf-8")

    (base / "submissions" / "index.md").write_text("\n".join(submissions_index_lines).rstrip() + "\n", encoding="utf-8")

    evaluation_groups: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for index, evaluation in enumerate(evaluations, start=1):
        evaluation_groups.setdefault(str(evaluation.get("evaluator_tier") or "primary"), []).append((index, evaluation))

    evaluations_index_lines = [
        f"# Decision Index {task_id}",
        "",
        "Canonical IDs:",
        "- E = Evaluation",
        "",
        "By judge:",
    ]

    for index, evaluation in enumerate(evaluations, start=1):
        stage_slug, stage_label = evaluation_stage_info(evaluation)
        filename = (
            f"decision-{index:02d}__{evaluator_slug(evaluation.get('evaluator_tier'))}"
            f"__{stage_slug}__{evaluation['evaluation_id']}.md"
        )
        scorecard = parse_json_object(evaluation.get("scorecard_json"))
        if scorecard.get("pair_mode") == "initial_duel":
            result_label = "Opening champion established"
        elif bool(evaluation.get("swap_occurred")):
            result_label = "Champion changed hands"
        elif bool(evaluation.get("masterpiece_locked")):
            result_label = "Title defense successful"
        else:
            result_label = "Champion retained"
        card_lines = [
            f"# Decision {index:02d} | {evaluator_display_label(evaluation.get('evaluator_tier'))} | {stage_label}",
            "",
            f"Evaluation ID: {evaluation['evaluation_id']}",
            f"Task ID: {evaluation['task_id']}",
            f"Result: {result_label}",
            f"Champion side: {lane_submission_ref(evaluation.get('winner_lane_id'), evaluation.get('winner_submission_id'))}",
            f"Champion score: {evaluation.get('winner_score')}",
            f"Challenger side: {lane_submission_ref(evaluation.get('loser_lane_id'), evaluation.get('loser_submission_id'))}",
            f"Challenger score: {evaluation.get('loser_score')}",
            f"Decision delta: {evaluation.get('score_delta')}",
            f"Canonical file: evaluations/{evaluation['evaluation_id']}.json",
        ]
        rationale = str(evaluation.get("rationale") or "").strip()
        if rationale:
            card_lines.extend(["", "Rationale:", rationale])
        (decision_cards_dir / filename).write_text("\n".join(card_lines).rstrip() + "\n", encoding="utf-8")
        judge_dir = evaluations_by_judge_dir / evaluator_slug(evaluation.get("evaluator_tier"))
        judge_dir.mkdir(parents=True, exist_ok=True)
        judge_filename = f"decision-{index:02d}__{stage_slug}__{evaluation['evaluation_id']}.md"
        (judge_dir / judge_filename).write_text("\n".join(card_lines).rstrip() + "\n", encoding="utf-8")

    for tier in EVALUATOR_TIERS:
        judge_dir = evaluations_by_judge_dir / evaluator_slug(tier)
        judge_dir.mkdir(parents=True, exist_ok=True)
        evaluations_index_lines.append(f"- {evaluator_display_label(tier)} -> by-judge/{evaluator_slug(tier)}/")
        latest_entry = evaluation_groups.get(tier, [])[-1] if evaluation_groups.get(tier) else None
        latest_lines = [
            f"# {evaluator_display_label(tier)}",
            "",
            f"Judge tier: {evaluator_display_label(tier)}",
            f"Task: {task_id}",
        ]
        if latest_entry:
            latest_index, latest_eval = latest_entry
            latest_stage_slug, latest_stage_label = evaluation_stage_info(latest_eval)
            latest_lines.extend(
                [
                    f"Latest decision: {latest_eval['evaluation_id']}",
                    f"Decision number: {latest_index:02d}",
                    f"Stage: {latest_stage_label}",
                    f"Canonical file: evaluations/{latest_eval['evaluation_id']}.json",
                ]
            )
        else:
            latest_lines.append("Latest decision: none")
        (judge_dir / "latest.md").write_text("\n".join(latest_lines).rstrip() + "\n", encoding="utf-8")

    (base / "evaluations" / "index.md").write_text("\n".join(evaluations_index_lines).rstrip() + "\n", encoding="utf-8")


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
    write_task_boxing_views(conn, task, submissions, evaluations)


def submission_brief(conn: sqlite3.Connection, submission_id: str | None) -> dict[str, Any] | None:
    if not submission_id:
        return None
    row = query_submission(conn, submission_id)
    body = read_submission_body(row["artifact_path"])
    evidence = submission_evidence_meta(row)
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
        "workspace_summary": evidence.get("workspace_summary") or "",
        "changed_files": evidence.get("changed_files") or [],
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
        "evaluator_tier": row["evaluator_tier"],
        "left_submission_id": scorecard.get("left_submission_id"),
        "right_submission_id": scorecard.get("right_submission_id"),
        "left_total": scorecard.get("left_total"),
        "right_total": scorecard.get("right_total"),
        "winner_rubric": scorecard.get("winner_rubric"),
        "loser_rubric": scorecard.get("loser_rubric"),
        "pair_mode": scorecard.get("pair_mode"),
        "duel_stage": scorecard.get("duel_stage"),
    }


def active_run_brief(conn: sqlite3.Connection, run_id: str | None) -> dict[str, Any] | None:
    if not run_id:
        return None
    row = query_run(conn, run_id)
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "lane_id": row["lane_id"],
        "task_id": row["task_id"],
        "submission_id": row["submission_id"],
        "mode": row["mode"],
        "status": row["status"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "exit_code": row["exit_code"],
        "cwd": row["cwd"],
    }


def blank_scoreboard_entry(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "submission_id": row["submission_id"],
        "lane_id": row["lane_id"],
        "phase": row["phase"],
        "round_number": row["round_number"],
        "retry_number": row["retry_number"],
        "summary": row["summary"],
        "published": bool(row["published"]),
        "created_at": row["created_at"],
        "latest_score": None,
        "latest_rubric": None,
        "last_scored_at": None,
        "rank": None,
        "movement": "pending",
        "role": "archive",
    }


def scoreboard_role_priority(entry: dict[str, Any]) -> int:
    role = entry.get("role", "archive")
    return {
        "champion": 0,
        "challenger": 1,
        "published": 2,
        "archive": 3,
    }.get(role, 4)


def scoreboard_sort_key(entry: dict[str, Any]) -> tuple[Any, ...]:
    return (
        scoreboard_role_priority(entry),
        entry["latest_score"] is None,
        -(float(entry["latest_score"]) if entry["latest_score"] is not None else 0.0),
        entry["created_at"],
        entry["submission_id"],
    )


def evaluation_scoreboard_entries(
    submissions: list[sqlite3.Row],
    evaluations: list[sqlite3.Row],
) -> dict[str, dict[str, Any]]:
    entries = {row["submission_id"]: blank_scoreboard_entry(row) for row in submissions}
    for row in evaluations:
        try:
            scorecard = json.loads(row["scorecard_json"])
        except json.JSONDecodeError:
            continue
        for side in ("left", "right"):
            submission_id = scorecard.get(f"{side}_submission_id")
            total = scorecard.get(f"{side}_total")
            rubric = scorecard.get(f"{side}_rubric")
            if submission_id not in entries or total is None:
                continue
            entries[submission_id]["latest_score"] = float(total)
            entries[submission_id]["latest_rubric"] = rubric
            entries[submission_id]["last_scored_at"] = row["created_at"]
    return entries


def task_scoreboard(conn: sqlite3.Connection, task_id: str) -> list[dict[str, Any]]:
    submissions = conn.execute(
        """
        SELECT * FROM submissions
        WHERE task_id = ?
        ORDER BY created_at ASC, submission_id ASC
        """,
        (task_id,),
    ).fetchall()
    if not submissions:
        return []
    evaluations = conn.execute(
        """
        SELECT * FROM evaluations
        WHERE task_id = ?
        ORDER BY created_at ASC, evaluation_id ASC
        """,
        (task_id,),
    ).fetchall()
    current_entries = evaluation_scoreboard_entries(submissions, evaluations)
    task = fetch_task(conn, task_id)
    latest_eval = latest_evaluation_brief(conn, task_id)

    for entry in current_entries.values():
        submission_id = entry["submission_id"]
        if submission_id == task["champion_submission_id"]:
            entry["role"] = "champion"
        elif submission_id == task["challenger_submission_id"]:
            entry["role"] = "challenger"
        elif entry["published"]:
            entry["role"] = "published"

    ranked = sorted(current_entries.values(), key=scoreboard_sort_key)
    display_rank = 1
    for entry in ranked:
        if entry["latest_score"] is None:
            entry["rank"] = None
            entry["movement"] = "pending"
        else:
            entry["rank"] = display_rank
            display_rank += 1
            if latest_eval is None:
                entry["movement"] = "new"
            elif entry["submission_id"] == latest_eval["winner_submission_id"]:
                entry["movement"] = "up" if latest_eval["swap_occurred"] else "new"
            elif entry["submission_id"] == latest_eval["loser_submission_id"]:
                entry["movement"] = "down"
            else:
                entry["movement"] = "same"
    return ranked


def recent_events(limit: int = DEFAULT_EVENTS_LIMIT, task_id: str | None = None) -> list[dict[str, Any]]:
    if limit <= 0 or not EVENTS_FILE.exists():
        return []
    events: deque[dict[str, Any]] = deque(maxlen=max(limit, 1))
    with EVENTS_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if task_id and payload.get("task_id") != task_id:
                continue
            events.append(payload)
    return list(events)


def read_new_events(offset: int, task_id: str | None = None) -> tuple[int, list[dict[str, Any]]]:
    if not EVENTS_FILE.exists():
        return 0, []
    size = EVENTS_FILE.stat().st_size
    if offset < 0 or offset > size:
        offset = 0
    events: list[dict[str, Any]] = []
    with EVENTS_FILE.open("r", encoding="utf-8") as handle:
        handle.seek(offset)
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if task_id and payload.get("task_id") != task_id:
                continue
            events.append(payload)
        next_offset = handle.tell()
    return next_offset, events


def events_file_size() -> int:
    if not EVENTS_FILE.exists():
        return 0
    return EVENTS_FILE.stat().st_size


def compact_timestamp(ts: str | None) -> str:
    if not ts:
        return "-"
    text = str(ts)
    if "T" in text:
        time_part = text.split("T", 1)[1]
        for token in ("+", "Z"):
            if token in time_part:
                time_part = time_part.split(token, 1)[0]
        return time_part
    return text


def format_event_summary(event: dict[str, Any]) -> str:
    ts = event.get("ts", "-")
    event_type = event.get("type", "event")
    if event_type == "task_submitted":
        return f"{ts} submit {event.get('task_id')} mode={event.get('task_mode') or 'proposal'} title={event.get('title')}"
    if event_type == "lane_reserved":
        return (
            f"{ts} reserve {lane_display_name(event.get('lane_id'))} -> {event.get('task_id')} "
            f"type={reservation_display_label(event.get('reservation_type'))}"
        )
    if event_type == "lane_assigned":
        return (
            f"{ts} assign {lane_display_name(event.get('lane_id'))} -> {event.get('task_id')} "
            f"type={reservation_display_label(event.get('reservation_type'))}"
        )
    if event_type == "lane_promoted":
        return (
            f"{ts} promote {lane_display_name(event.get('lane_id'))} task={event.get('task_id')} "
            f"type={reservation_display_label(event.get('reservation_type'))}"
        )
    if event_type == "lane_retrying":
        return f"{ts} retry {lane_display_name(event.get('lane_id'))} task={event.get('task_id')}"
    if event_type == "workspace_prepared":
        return (
            f"{ts} workspace {lane_display_name(event.get('lane_id'))} task={event.get('task_id')} "
            f"kind={event.get('workspace_kind')}"
        )
    if event_type == "run_started":
        return f"{ts} start {lane_display_name(event.get('lane_id'))} {event.get('mode')} task={event.get('task_id')} run={event.get('run_id')}"
    if event_type == "submission_recorded":
        return f"{ts} submission {event.get('submission_id')} from {lane_display_name(event.get('lane_id'))} task={event.get('task_id')}"
    if event_type == "run_finished":
        return (
            f"{ts} finish {lane_display_name(event.get('lane_id'))} {event.get('mode')} task={event.get('task_id')} "
            f"status={event.get('status')} exit={event.get('exit_code')}"
        )
    if event_type == "run_resume_fallback":
        return (
            f"{ts} fallback {lane_display_name(event.get('lane_id'))} task={event.get('task_id')} "
            f"mode={event.get('mode')} from resume to cold exec"
        )
    if event_type == "task_scored":
        marker = " CHAMPION_CONFIRMED" if event.get("masterpiece_locked") else ""
        return (
            f"{ts} decision {event.get('evaluation_id')} task={event.get('task_id')} "
            f"champion={lane_submission_ref(event.get('winner_lane_id'), event.get('winner_submission_id'))} "
            f"challenger={lane_submission_ref(event.get('loser_lane_id'), event.get('loser_submission_id'))} "
            f"delta={event.get('score_delta')} judge={evaluator_display_label(event.get('evaluator_tier'))}{marker}"
        )
    if event_type == "evaluation_tied":
        return (
            f"{ts} draw task={event.get('task_id')} judge={evaluator_display_label(event.get('evaluator_tier'))} "
            f"next={event.get('next_step')} pair={lane_submission_ref(event.get('left_lane_id'), event.get('left_submission_id'))} "
            f"vs {lane_submission_ref(event.get('right_lane_id'), event.get('right_submission_id'))}"
        )
    if event_type == "task_masterpiece_locked":
        return (
            f"{ts} CHAMPION CONFIRMED task={event.get('task_id')} "
            f"submission={event.get('submission_id')} lane={lane_display_name(event.get('lane_id'))} score={event.get('score')}"
        )
    if event_type == "task_applied":
        changed = ",".join(event.get("changed_files", [])) or "-"
        return (
            f"{ts} applied task={event.get('task_id')} "
            f"submission={event.get('submission_id')} lane={lane_display_name(event.get('lane_id'))} "
            f"changed={changed}"
        )
    if event_type == "task_apply_failed":
        return (
            f"{ts} apply-failed task={event.get('task_id')} "
            f"submission={event.get('submission_id')} lane={lane_display_name(event.get('lane_id'))} "
            f"reason={event.get('reason')}"
        )
    if event_type == "lane_error":
        return f"{ts} ERROR lane={lane_display_name(event.get('lane_id'))} task={event.get('task_id') or '-'} message={event.get('message')}"
    if event_type == "lane_recovered":
        return (
            f"{ts} recover lane={lane_display_name(event.get('lane_id'))} next_status={event.get('next_status')} "
            f"task={event.get('next_task_id') or '-'}"
        )
    if event_type == "workspace_removed":
        return (
            f"{ts} workspace removed lane={lane_display_name(event.get('lane_id'))} task={event.get('task_id')} "
            f"reason={event.get('reason')}"
        )
    if event_type == "daemon_started":
        return f"{ts} daemon start executor={event.get('executor')} interval={event.get('interval')}"
    if event_type == "quota_probe":
        blocked = ",".join(lane_display_name(item) for item in event.get("blocked_lanes", [])) or "-"
        return (
            f"{ts} quota probe ok={event.get('ok')} quota_blocked={event.get('quota_blocked')} "
            f"email={event.get('email') or '-'} lanes={blocked} message={event.get('message')}"
        )
    if event_type == "quota_auto_resume":
        repaired = ",".join(lane_display_name(item) for item in event.get("repaired_lanes", [])) or "-"
        blocked = ",".join(lane_display_name(item) for item in event.get("blocked_lanes", [])) or "-"
        return (
            f"{ts} quota auto-resume email={event.get('email') or '-'} "
            f"blocked={blocked} repaired={repaired}"
        )
    if event_type == "asymptote_started":
        return f"{ts} asymptote start message={event.get('message')}"
    if event_type == "asymptote_stopped":
        return f"{ts} asymptote stop message={event.get('message')}"
    if event_type == "asymptote_pulse":
        return f"{ts} asymptote pulse trigger={event.get('trigger')} question={event.get('question')}"
    if event_type == "asymptote_human_note":
        return f"{ts} asymptote human note heading={event.get('entry_heading')}"
    if event_type == "asymptote_error":
        return f"{ts} asymptote error trigger={event.get('trigger')} message={event.get('message')}"
    if event_type == "daemon_tick":
        return (
            f"{ts} daemon tick cycle={event.get('cycle_count')} actions={event.get('action_count')} "
            f"runnable_after={event.get('runnable_after')} reason={event.get('reason')}"
        )
    if event_type == "daemon_stopped":
        return f"{ts} daemon stop cycle_count={event.get('cycle_count')}"
    return f"{ts} {event_type}"


def format_stream_event(event: dict[str, Any], *, task_id: str | None) -> str | None:
    event_type = event.get("type", "event")
    if event_type not in STREAM_EVENT_TYPES:
        return None
    ts = compact_timestamp(event.get("ts"))
    task_ref = event.get("task_id") or "-"
    if event_type == "task_submitted":
        return f"[{ts}] {task_ref} submitted | mode={event.get('task_mode') or 'proposal'} | title={event.get('title')}"
    if event_type == "run_started":
        mode = event.get("mode", "run")
        role = "judging" if str(mode).endswith("evaluator") else "boxing"
        return f"[{ts}] {task_ref} {lane_display_name(event.get('lane_id'))} started {role} run {event.get('run_id')}"
    if event_type == "submission_recorded":
        return f"[{ts}] {task_ref} {lane_display_name(event.get('lane_id'))} submitted {event.get('submission_id')}"
    if event_type == "task_scored":
        winner_score = event.get("winner_score")
        loser_score = event.get("loser_score")
        marker = " | CHAMPION CONFIRMED" if event.get("masterpiece_locked") else ""
        return (
            f"[{ts}] {task_ref} decision | champion={lane_submission_ref(event.get('winner_lane_id'), event.get('winner_submission_id'))}"
            f" {winner_score} | challenger={lane_submission_ref(event.get('loser_lane_id'), event.get('loser_submission_id'))} {loser_score}"
            f" | delta={event.get('score_delta')} | judge={evaluator_display_label(event.get('evaluator_tier'))}{marker}"
        )
    if event_type == "evaluation_tied":
        return (
            f"[{ts}] {task_ref} draw | judge={evaluator_display_label(event.get('evaluator_tier'))} "
            f"next={event.get('next_step')} | {lane_submission_ref(event.get('left_lane_id'), event.get('left_submission_id'))} "
            f"vs {lane_submission_ref(event.get('right_lane_id'), event.get('right_submission_id'))}"
        )
    if event_type == "task_masterpiece_locked":
        return (
            f"[{ts}] {task_ref} CHAMPION CONFIRMED | "
            f"{lane_submission_ref(event.get('lane_id'), event.get('submission_id'))} score={event.get('score')}"
        )
    if event_type == "task_applied":
        return (
            f"[{ts}] {task_ref} applied | "
            f"{lane_submission_ref(event.get('lane_id'), event.get('submission_id'))} -> target repo"
        )
    if event_type == "task_apply_failed":
        return f"[{ts}] {task_ref} apply blocked | {event.get('reason')}"
    if event_type == "lane_error":
        return f"[{ts}] {task_ref} ERROR {lane_display_name(event.get('lane_id'))} | {event.get('message')}"
    if event_type == "lane_recovered":
        return (
            f"[{ts}] {task_ref} recovered {lane_display_name(event.get('lane_id'))} | "
            f"next={event.get('next_status')} task={event.get('next_task_id') or '-'}"
        )
    if event_type == "run_recovered":
        return f"[{ts}] {task_ref} recovered run {event.get('run_id')} on {lane_display_name(event.get('lane_id'))}"
    if event_type == "run_resume_fallback":
        return f"[{ts}] {task_ref} fallback | {lane_display_name(event.get('lane_id'))} resumed session was rejected, retrying cold"
    if event_type == "quota_probe":
        if task_id:
            return None
        return (
            f"[{ts}] quota probe | ok={event.get('ok')} quota_blocked={event.get('quota_blocked')} "
            f"email={event.get('email') or '-'}"
        )
    if event_type == "quota_auto_resume":
        if task_id:
            return None
        repaired = ",".join(lane_display_name(item) for item in event.get("repaired_lanes", [])) or "-"
        blocked = ",".join(lane_display_name(item) for item in event.get("blocked_lanes", [])) or "-"
        return (
            f"[{ts}] quota auto-resume | email={event.get('email') or '-'} "
            f"blocked={blocked} repaired={repaired}"
        )
    if event_type == "asymptote_started":
        if task_id:
            return None
        return f"[{ts}] asymptote on | {event.get('message')}"
    if event_type == "asymptote_stopped":
        if task_id:
            return None
        return f"[{ts}] asymptote off | {event.get('message')}"
    if event_type == "asymptote_pulse":
        if task_id:
            return None
        return f"[{ts}] asymptote pulse | trigger={event.get('trigger')} heading={event.get('entry_heading')}"
    if event_type == "asymptote_human_note":
        if task_id:
            return None
        return f"[{ts}] asymptote human note | heading={event.get('entry_heading')}"
    if event_type == "asymptote_error":
        if task_id:
            return None
        return f"[{ts}] asymptote instability | {event.get('message')}"
    if event_type == "daemon_started":
        if task_id:
            return None
        return f"[{ts}] daemon started | executor={event.get('executor')} interval={event.get('interval')}"
    if event_type == "daemon_stopped":
        if task_id:
            return None
        return f"[{ts}] daemon stopped | cycle_count={event.get('cycle_count')}"
    return None


def task_is_finished(task: dict[str, Any]) -> bool:
    return task.get("status") in FINISHED_TASK_STATUSES


def task_stage_label(task: dict[str, Any]) -> str:
    if task.get("masterpiece_locked"):
        if str(task.get("task_mode") or "proposal") == "patch":
            return f"Champion confirmed | {task_apply_status_label(task.get('apply_status'))}"
        return "Champion confirmed"
    pair_mode = str(task.get("pair_mode") or "initial_duel")
    evaluator_tier_name = evaluator_display_label(str(task.get("evaluator_tier") or "primary"))
    if pair_mode == "challenger_retry":
        if direct_reply_stage(task) == "defense":
            return "Title defense in progress"
        return "Challenger rematch in progress"
    if pair_mode == "full_rematch":
        return f"Draw rematch queued for the {evaluator_tier_name}"
    if pair_mode == "review_only":
        if task.get("evaluator_tier") == "absolute" and task.get("tier_phase") == "post_rematch":
            return "Final arbiter is reviewing the drawn pair again"
        return f"Draw under review by the {evaluator_tier_name}"
    scoreboard = task.get("scoreboard") or []
    if len(scoreboard) >= 2:
        return "Championship bout ready for the score judge"
    if len(scoreboard) == 1:
        return "Waiting for the other corner"
    if task.get("queued_on_lanes"):
        return "Queued for the corners"
    return "Waiting for the corners"


def task_next_action(task: dict[str, Any], snapshot: dict[str, Any]) -> str:
    if task.get("masterpiece_locked"):
        champion = task.get("champion_submission")
        apply_status = str(task.get("apply_status") or "not_requested")
        if champion:
            if str(task.get("task_mode") or "proposal") == "patch":
                return (
                    f"{lane_display_name(champion['lane_id'])} champion confirmed at score {task.get('champion_score')} "
                    f"| {task_apply_status_label(apply_status)}"
                )
            return f"{lane_display_name(champion['lane_id'])} champion confirmed at score {task.get('champion_score')}"
        return "Task is complete"
    pair_mode = str(task.get("pair_mode") or "initial_duel")
    evaluator_tier_name = evaluator_display_label(str(task.get("evaluator_tier") or "primary"))
    ready_tasks = snapshot.get("ready_evaluation_tasks", [])
    if pair_mode == "challenger_retry":
        reply_stage = direct_reply_stage(task)
        round_label = "title defense" if reply_stage == "defense" else "rematch"
        draft_label = "title-defense draft" if reply_stage == "defense" else "improved rematch draft"
        if task["task_id"] in ready_tasks:
            return f"{evaluator_tier_name} should score the {round_label}"
        return f"{lane_display_name(task.get('challenger_lane_id'))} should submit the {draft_label}"
    if pair_mode == "full_rematch":
        if task["task_id"] in ready_tasks:
            return f"{evaluator_tier_name} should score the rematch pair"
        return "Both corners should submit improved rematch drafts"
    if pair_mode == "review_only":
        if task["task_id"] in ready_tasks:
            if task.get("evaluator_tier") == "absolute" and task.get("tier_phase") == "post_rematch":
                return "Final arbiter should keep reviewing the drawn pair until a champion emerges"
            return f"{evaluator_tier_name} should re-review the drawn pair"
        return f"Waiting for the {evaluator_tier_name} to re-open the drawn pair"
    scoreboard = task.get("scoreboard") or []
    pending_entries = [entry for entry in scoreboard if entry.get("latest_score") is None]
    if task.get("latest_evaluation") and pending_entries:
        latest_pending = pending_entries[-1]
        return (
            f"Score judge should score {lane_display_name(task.get('champion_lane_id'))} "
            f"against {lane_submission_ref(latest_pending['lane_id'], latest_pending['submission_id'])}"
        )
    if len(scoreboard) >= 2 and not task.get("latest_evaluation"):
        worker_labels = ", ".join(lane_submission_ref(entry["lane_id"], entry["submission_id"]) for entry in scoreboard)
        return f"Score judge should score the championship bout: {worker_labels}"
    if len(scoreboard) == 1:
        only = scoreboard[0]
        missing = [lane_id for lane_id in WORKER_LANES if lane_id != only["lane_id"]]
        return f"Waiting for {lane_display_name(missing[0])} to submit"
    if task.get("queued_on_lanes"):
        queue_desc = queued_entries_display(task["queued_on_lanes"])
        return f"Queued behind active lane work: {queue_desc}"
    if task["task_id"] in ready_tasks:
        return "Score judge can pick this task now"
    return "Waiting for the scheduler to start the corners"


def task_display_state(task: dict[str, Any], snapshot: dict[str, Any]) -> tuple[str, str]:
    task_id = task.get("task_id")
    if task_is_finished(task):
        return "DONE", "champion confirmed"

    lanes = snapshot.get("lanes", [])
    quota_monitor = (snapshot.get("daemon", {}).get("state") or {}).get("quota_monitor") or {}
    blocked_lane_infos = quota_monitor.get("blocked_lanes") or []

    blocked_lanes = []
    for lane in lanes:
        if lane.get("active_task_id") != task_id:
            continue
        if lane.get("status") == "error":
            blocked_lanes.append(lane_display_name(lane["lane_id"]))
        active_run = lane.get("active_run")
        if active_run:
            return "RUNNING", f"{lane_display_name(lane['lane_id'])} running {active_run.get('mode') or lane.get('status')}"

    quota_blocked = [lane_display_name(item["lane_id"]) for item in blocked_lane_infos if item.get("task_id") == task_id]
    if quota_blocked:
        joined = ", ".join(quota_blocked)
        return "BLOCKED", f"quota blocked on {joined}"
    if blocked_lanes:
        joined = ", ".join(blocked_lanes)
        return "BLOCKED", f"lane error on {joined}"

    ready_tasks = snapshot.get("ready_evaluation_tasks", [])
    if task_id in ready_tasks:
        tier = evaluator_display_label(str(task.get("evaluator_tier") or "primary"))
        return "READY", f"{tier} can score now"

    ready_workers = [
        lane_display_name(lane["lane_id"])
        for lane in lanes
        if lane.get("active_task_id") == task_id and lane.get("status") in {"assigned", "retrying"}
    ]
    if ready_workers:
        joined = ", ".join(ready_workers)
        return "READY", f"waiting for scheduler to start {joined}"

    waiting_eval = [
        lane_display_name(lane["lane_id"])
        for lane in lanes
        if lane.get("active_task_id") == task_id and lane.get("status") == "waiting_eval"
    ]
    if waiting_eval:
        joined = ", ".join(waiting_eval)
        return "WAITING", f"submitted and waiting for the score judge via {joined}"

    if task.get("queued_on_lanes"):
        queue_desc = queued_entries_display(task["queued_on_lanes"])
        return "WAITING", f"queued on {queue_desc}"

    return "WAITING", task_next_action(task, snapshot)


def execution_state(snapshot: dict[str, Any]) -> tuple[str, str]:
    tasks = snapshot.get("tasks") or []
    if not tasks:
        return "IDLE", "no tasks"
    guard = snapshot.get("resilience_guard") or {}
    if guard.get("active"):
        return "PAUSED", str(guard.get("reason") or "resilience guard paused codex work")

    states = [task_display_state(task, snapshot) for task in tasks]
    if any(state == "RUNNING" for state, _reason in states):
        reason = next(reason for state, reason in states if state == "RUNNING")
        return "RUNNING", reason
    if any(state == "BLOCKED" for state, _reason in states):
        reason = next(reason for state, reason in states if state == "BLOCKED")
        return "BLOCKED", reason
    if all(state == "DONE" for state, _reason in states):
        return "DONE", "all visible tasks are finished"
    if any(state == "READY" for state, _reason in states):
        reason = next(reason for state, reason in states if state == "READY")
        return "READY", reason

    daemon = snapshot.get("daemon", {})
    if snapshot.get("summary", {}).get("task_in_progress", 0) and not daemon.get("running", False):
        return "PAUSED", "tasks exist but the daemon is not running"
    return "WAITING", "tasks are queued or waiting on the next transition"


def lane_display_state(lane: dict[str, Any]) -> str:
    active_run = lane.get("active_run") or {}
    if active_run and active_run.get("status") == "running":
        return "RUNNING"
    status = str(lane.get("status") or "")
    if status == "error":
        return "BLOCKED"
    if status in {"assigned", "retrying"}:
        return "RUNNING"
    if status in {"waiting_eval", "locked"}:
        return "WAITING"
    if status == "idle":
        return "IDLE"
    return "WAITING"


def task_active_summary(task: dict[str, Any], snapshot: dict[str, Any]) -> str:
    active_parts: list[str] = []
    for lane in snapshot.get("lanes", []):
        if lane.get("active_task_id") != task.get("task_id"):
            continue
        active_run = lane.get("active_run")
        if active_run:
            mode = active_run.get("mode") or lane.get("status")
            active_parts.append(f"{lane_display_name(lane['lane_id'])}:{mode}")
        else:
            active_parts.append(f"{lane_display_name(lane['lane_id'])}:{lane.get('status')}")
    return ", ".join(active_parts) or "-"


def task_scoreboard_summary(task: dict[str, Any], limit: int = 3) -> str:
    fragments: list[str] = []
    for entry in task.get("scoreboard") or []:
        score = entry.get("latest_score")
        rank = entry.get("rank")
        rank_label = f"#{rank}" if rank is not None else "pending"
        score_label = f"{float(score):.1f}" if score is not None else "pending"
        fragments.append(f"{rank_label} {lane_display_name(entry['lane_id'])}/{entry['submission_id']} {score_label}")
        if len(fragments) >= limit:
            break
    return " | ".join(fragments) or "-"


def format_progress_stream(snapshot: dict[str, Any], task_id: str | None) -> str:
    tasks = snapshot.get("tasks", [])
    if task_id:
        tasks = [task for task in tasks if task.get("task_id") == task_id]
    if not tasks:
        if task_id:
            return f"Progress:\n- {task_id} not found"
        return "Progress:\n- no tasks"

    visible_tasks = [task for task in tasks if not task_is_finished(task)]
    if task_id and not visible_tasks:
        visible_tasks = tasks

    execution_label = snapshot.get("execution_state")
    execution_reason = snapshot.get("execution_reason")
    if not execution_label or not execution_reason:
        execution_label, execution_reason = execution_state(snapshot)
    phase = current_animation_phase()

    lines = ["Progress:", f"- Execution: {render_state_label(execution_label, phase=phase)} | {execution_reason}"]
    if not visible_tasks:
        lines.append("- no tasks in progress")
        return "\n".join(lines)

    for task in visible_tasks:
        state_label = task.get("display_state")
        state_reason = task.get("display_reason")
        if not state_label or not state_reason:
            state_label, state_reason = task_display_state(task, snapshot)
        lines.append(f"- {task['task_id']} [{render_state_label(state_label, phase=phase)}] {task_stage_label(task)}")
        lines.append(f"  state: {render_state_label(state_label, phase=phase)} | {state_reason}")
        lines.append(f"  mode: {task_mode_label(task.get('task_mode'))} | apply: {task_apply_status_label(task.get('apply_status'))}")
        lines.append(f"  next: {task_next_action(task, snapshot)}")
        active = task_active_summary(task, snapshot)
        if active != "-":
            lines.append(f"  active: {active}")
        scoreboard = task_scoreboard_summary(task)
        if scoreboard != "-":
            lines.append(f"  scorecards: {scoreboard}")
        lines.append(f"  crowd: {crowd_reaction(task)}")
    return "\n".join(lines)


def status_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    resilience = resilience_summary()
    resilience_guard = resilience_guard_snapshot(resilience)
    asymptote = asymptote_snapshot()
    ready_eval_tasks = [item["task"]["task_id"] for item in ready_evaluation_candidates(conn)]
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
        item["active_run"] = active_run_brief(conn, item["active_run_id"])
        if item["lane_id"] == "evaluator":
            item["ready_evaluation_tasks"] = ready_eval_tasks
        lanes.append(item)

    tasks = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM tasks ORDER BY created_at DESC, task_id DESC"
        ).fetchall()
    ]
    summary = {
        "task_total": len(tasks),
        "task_in_progress": sum(1 for task in tasks if task["status"] not in FINISHED_TASK_STATUSES),
        "ready_evaluations": len(ready_eval_tasks),
    }
    return {
        "root": str(ROOT),
        "db_path": str(DB_PATH),
        "executor": DEFAULT_EXECUTOR,
        "daemon": daemon_runtime_snapshot(),
        "resilience": resilience,
        "resilience_guard": resilience_guard,
        "asymptote": asymptote,
        "summary": summary,
        "lanes": lanes,
        "tasks": tasks,
        "ready_evaluation_tasks": ready_eval_tasks,
    }


def dashboard_snapshot(
    conn: sqlite3.Connection,
    task_id: str | None = None,
    *,
    events_limit: int = DEFAULT_EVENTS_LIMIT,
) -> dict[str, Any]:
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
        item["scoreboard"] = task_scoreboard(conn, item["task_id"])
        item["queued_on_lanes"] = sorted(task_queue_map.get(item["task_id"], []), key=lambda entry: (entry["priority"], entry["lane_id"]))
        tasks.append(item)

    summary = {
        "task_total": len(tasks),
        "task_locked": sum(1 for task in tasks if task["masterpiece_locked"]),
        "task_retrying": sum(1 for task in tasks if task["status"] in {"challenger_retrying", "worker_rematching"}),
        "task_in_progress": sum(1 for task in tasks if task["status"] not in FINISHED_TASK_STATUSES),
        "lane_idle": sum(1 for lane in base["lanes"] if lane["status"] == "idle"),
        "lane_busy": sum(1 for lane in base["lanes"] if lane["status"] != "idle"),
        "queued_reservations": sum(len(lane["queued_reservations"]) for lane in base["lanes"]),
        "ready_evaluations": len(base.get("ready_evaluation_tasks", [])),
        "stale_runs": len(recovery["stale_runs"]),
        "repairable_lanes": len(recovery["lane_repairs"]),
    }
    execution_snapshot = {
        "daemon": daemon,
        "resilience": base.get("resilience"),
        "resilience_guard": base.get("resilience_guard"),
        "asymptote": base.get("asymptote"),
        "summary": summary,
        "lanes": base["lanes"],
        "tasks": tasks,
        "ready_evaluation_tasks": base.get("ready_evaluation_tasks", []),
    }
    for task in tasks:
        display_state, display_reason = task_display_state(task, execution_snapshot)
        task["display_state"] = display_state
        task["display_reason"] = display_reason
    execution_label, execution_reason = execution_state(execution_snapshot)
    return {
        "root": base["root"],
        "db_path": base["db_path"],
        "executor": base["executor"],
        "generated_at": now_utc(),
        "daemon": daemon,
        "resilience": base.get("resilience"),
        "resilience_guard": base.get("resilience_guard"),
        "asymptote": base.get("asymptote"),
        "execution_state": execution_label,
        "execution_reason": execution_reason,
        "summary": summary,
        "lanes": base["lanes"],
        "tasks": tasks,
        "ready_evaluation_tasks": base.get("ready_evaluation_tasks", []),
        "recent_events": [
            {**event, "summary": format_event_summary(event)}
            for event in recent_events(events_limit, task_id)
        ],
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
        if task is not None and task["pair_mode"] in {"challenger_retry", "full_rematch"}:
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


def apply_recovery_plan(
    conn: sqlite3.Connection,
    snapshot: dict[str, Any],
    *,
    requeue: bool = False,
    allow_daemon_running: bool = False,
) -> dict[str, Any]:
    if snapshot["daemon"]["running"] and not allow_daemon_running:
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
            target_task = query_task(conn, action["next_task_id"])
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
            reservation_type = "recovered_task"
            if action["next_status"] == "retrying":
                reservation_type = "tie_rematch" if target_task and target_task["pair_mode"] == "full_rematch" else "duel_retry"
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


def daemon_quota_monitor(
    conn: sqlite3.Connection,
    *,
    executor: str,
    exec_timeout: float,
    previous: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    monitor = dict(previous or {})
    identity = codex_login_identity()
    identity_fingerprint = login_identity_fingerprint(identity)
    monitor.update(
        {
            "enabled": bool(AUTO_QUOTA_RECOVERY and executor == "codex"),
            "login_identity": identity,
            "current_identity_fingerprint": identity_fingerprint,
            "recheck_interval_seconds": DEFAULT_QUOTA_RECHECK_INTERVAL,
        }
    )
    if not monitor["enabled"]:
        monitor["status"] = "disabled"
        monitor["blocked_lanes"] = []
        return monitor, None

    recovery = recovery_snapshot(conn)
    blocked_lanes = [
        info
        for info in (
            quota_blocked_lane_info(conn, action["lane_id"])
            for action in recovery["lane_repairs"]
        )
        if info is not None
    ]
    monitor["blocked_lanes"] = blocked_lanes
    if not blocked_lanes:
        monitor["status"] = "clear"
        return monitor, None

    last_probe_identity = monitor.get("last_probe_identity")
    identity_changed = bool(last_probe_identity) and last_probe_identity != identity_fingerprint
    last_probe_at = parse_timestamp(monitor.get("last_probe_at"))
    now = datetime.now(timezone.utc)
    probe_due = (
        last_probe_at is None
        or (now - last_probe_at).total_seconds() >= DEFAULT_QUOTA_RECHECK_INTERVAL
    )
    monitor["status"] = "waiting_for_available_codex"
    monitor["identity_changed"] = identity_changed
    if not (identity_changed or probe_due):
        return monitor, None

    probe = codex_availability_probe(exec_timeout)
    monitor["last_probe_at"] = now_utc()
    monitor["last_probe_identity"] = identity_fingerprint
    monitor["last_probe"] = probe
    append_event(
        "quota_probe",
        email=identity.get("email"),
        account_id=identity.get("account_id"),
        ok=probe["ok"],
        quota_blocked=probe.get("quota_blocked", False),
        blocked_lanes=[item["lane_id"] for item in blocked_lanes],
        message=probe["message"],
    )
    if not probe["ok"]:
        monitor["status"] = "quota_blocked" if probe.get("quota_blocked") else "probe_failed"
        return monitor, None

    applied = apply_recovery_plan(conn, recovery_snapshot(conn), allow_daemon_running=True)
    action = {
        "type": "auto_recover",
        "trigger": "quota_ready",
        "repaired_lanes": applied["repaired_lanes"],
        "abandoned_runs": applied["abandoned_runs"],
        "email": identity.get("email"),
        "account_id": identity.get("account_id"),
    }
    monitor["status"] = "recovered"
    monitor["last_recovered_at"] = now_utc()
    append_event(
        "quota_auto_resume",
        email=identity.get("email"),
        account_id=identity.get("account_id"),
        repaired_lanes=applied["repaired_lanes"],
        blocked_lanes=[item["lane_id"] for item in blocked_lanes],
    )
    return monitor, action


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


def next_evaluation_attempt_index(conn: sqlite3.Connection, task_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM runs
        WHERE task_id = ?
          AND lane_id = 'evaluator'
        """,
        (task_id,),
    ).fetchone()
    count = int(row[0] or 0)
    return max(count - 1, 0)


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


def last_submission_for_lane(
    conn: sqlite3.Connection,
    task_id: str,
    lane_id: str,
    phases: tuple[str, ...] | None = None,
) -> sqlite3.Row | None:
    query = [
        "SELECT * FROM submissions",
        "WHERE task_id = ? AND lane_id = ?",
    ]
    params: list[Any] = [task_id, lane_id]
    if phases:
        placeholders = ", ".join("?" for _ in phases)
        query.append(f"AND phase IN ({placeholders})")
        params.extend(phases)
    query.append("ORDER BY created_at DESC, submission_id DESC")
    query.append("LIMIT 1")
    return conn.execute("\n".join(query), tuple(params)).fetchone()


def submission_phases_for_pair_mode(pair_mode: str) -> tuple[str, ...]:
    if pair_mode == "initial_duel":
        return ("initial",)
    if pair_mode == "challenger_retry":
        return ("retry",)
    if pair_mode in {"full_rematch", "review_only"}:
        return ("rematch",)
    return ("retry",)


def pair_submission_for_lane(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    lane_id: str,
    phases: tuple[str, ...],
) -> sqlite3.Row | None:
    lane = fetch_lane(conn, lane_id)
    if lane["active_task_id"] == task["task_id"]:
        active_submission = query_submission(conn, lane["active_submission_id"])
        if active_submission is not None and active_submission["phase"] in phases:
            return active_submission
        if lane["status"] in {"assigned", "retrying"}:
            return None
    return last_submission_for_lane(conn, task["task_id"], lane_id, phases=phases)


def evaluator_tier_label(tier: str) -> str:
    return {
        "primary": "primary evaluator",
        "elder": "elder evaluator",
        "absolute": "absolute evaluator",
    }.get(str(tier or "primary"), "primary evaluator")


def evaluator_tie_policy(task: sqlite3.Row) -> str:
    tier = str(task["evaluator_tier"] or "primary")
    pair_mode = str(task["pair_mode"] or "initial_duel")
    tier_phase = str(task["tier_phase"] or "base")
    if tier == "absolute" and pair_mode == "review_only" and tier_phase == "post_rematch":
        return (
            "This is the final absolute re-review. Re-examine the pair until one side edges ahead. "
            "Do not stop at a tie unless the weighted totals are still exactly equal after your most careful pass."
        )
    if tier == "absolute":
        return "A tie is allowed only if the weighted totals are exactly equal after the strictest possible review."
    if tier == "elder":
        return "Only return a tie if the weighted totals are genuinely identical after a stricter second look."
    return "Only return a tie if the weighted totals are genuinely identical."


def direct_reply_stage(task: sqlite3.Row | dict[str, Any]) -> str:
    if isinstance(task, dict):
        duel_stage = str(task.get("duel_stage") or "counterattack")
    else:
        try:
            duel_stage = str(task["duel_stage"] or "counterattack")
        except (KeyError, TypeError, IndexError):
            duel_stage = "counterattack"
    if duel_stage == "defense":
        return "defense"
    return "counterattack"


def task_round_context(task: sqlite3.Row, lane_id: str) -> str:
    duel_stage = str(task["duel_stage"] or "initial")
    pair_mode = str(task["pair_mode"] or "initial_duel")
    tier = evaluator_tier_label(str(task["evaluator_tier"] or "primary"))
    if pair_mode == "challenger_retry":
        if direct_reply_stage(task) == "defense":
            return (
                "Title-defense improvement round. The trailing corner gets one final chance to sharpen its own answer "
                "against the current scoring benchmark. Use any pressure points only if they genuinely improve your "
                "answer. After this round, the winner locks as the masterpiece."
            )
        return (
            "Counterattack improvement round. The opening loser gets one informed chance to strengthen its own answer "
            "against the current scoring benchmark. Use any pressure points only if they genuinely improve your answer. "
            "If the current benchmark still wins here, the higher-scoring output locks as the masterpiece."
        )
    if pair_mode == "full_rematch":
        return (
            f"Tie rematch under the {tier}. You may improve your previous draft or resubmit it unchanged if "
            "you believe it already wins."
        )
    if pair_mode == "review_only":
        return f"The {tier} is re-reviewing the current tied pair without new worker submissions."
    if duel_stage == "initial":
        return "Initial duel. Establish the strongest opening draft."
    return "Produce the strongest possible answer for the current round."


def task_mode_worker_expectations(task: sqlite3.Row) -> str:
    if str(task["task_mode"] or "proposal") == "patch":
        return (
            "This is a patch bout. Make the real workspace changes needed to satisfy the brief. "
            "Your JSON body should summarize the implemented result, but the harness will inspect actual changed files in the workspace."
        )
    return (
        "This is a proposal bout. Focus on the strongest standalone answer. Workspace changes are optional and are not required to score well."
    )


def task_mode_evaluator_guidance(task: sqlite3.Row) -> str:
    if str(task["task_mode"] or "proposal") == "patch":
        return (
            "This is a patch bout. Heavily reward submissions that made concrete workspace changes aligned with the brief, and penalize answers that are mostly prose with little or no implementation evidence."
        )
    return (
        "This is a proposal bout. Judge the written submission itself; workspace evidence is secondary."
    )


def worker_guidance_brief(conn: sqlite3.Connection, task: sqlite3.Row, lane_id: str) -> str:
    rematch_brief = str(task["rematch_brief"] or "").strip()
    if rematch_brief:
        return rematch_brief
    if task["pair_mode"] == "challenger_retry" and task["challenger_lane_id"] == lane_id:
        return last_loser_brief(conn, task["task_id"], lane_id)
    return "No retry brief yet. Produce the strongest draft you can."


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
            "rematch_brief": {"type": "string"},
        },
        "required": ["left_rubric", "right_rubric", "rationale", "loser_brief", "rematch_brief"],
    }


def build_worker_prompt(conn: sqlite3.Connection, task: sqlite3.Row, lane_id: str) -> str:
    champion_summary = "No published champion yet."
    champion_body = "No published champion yet."
    if task["published_submission_id"]:
        champion = fetch_submission(conn, task["published_submission_id"])
        champion_summary = champion["summary"]
        champion_body = read_submission_body(champion["artifact_path"]) or champion["summary"]
    previous_submission = last_submission_for_lane(conn, task["task_id"], lane_id)
    own_previous_summary = "No previous submission yet."
    own_previous_body = "No previous submission yet."
    if previous_submission is not None:
        own_previous_summary = previous_submission["summary"]
        own_previous_body = read_submission_body(previous_submission["artifact_path"]) or previous_submission["summary"]
    template = load_template("worker_prompt.md", DEFAULT_WORKER_TEMPLATE)
    return template.format(
        lane_id=lane_id,
        task_id=task["task_id"],
        task_title=task["title"],
        task_mode=task_mode_label(task["task_mode"]),
        task_prompt=task["prompt"],
        champion_summary=champion_summary,
        champion_body=champion_body,
        guidance_brief=worker_guidance_brief(conn, task, lane_id),
        own_previous_summary=own_previous_summary,
        own_previous_body=own_previous_body,
        round_context=task_round_context(task, lane_id),
        mode_expectations=task_mode_worker_expectations(task),
    )


def build_worker_resume_prompt(conn: sqlite3.Connection, task: sqlite3.Row, lane_id: str) -> str:
    champion_summary = "No published champion yet."
    champion_body = "No published champion yet."
    if task["published_submission_id"]:
        champion = fetch_submission(conn, task["published_submission_id"])
        champion_summary = champion["summary"]
        champion_body = read_submission_body(champion["artifact_path"]) or champion["summary"]
    return textwrap.dedent(
        f"""\
        Continue the same task and boxer session.

        Task: {task["task_id"]} | {task["title"]}
        Task mode: {task_mode_label(task["task_mode"])}
        Corner: {lane_display_name(lane_id)}
        Round context: {task_round_context(task, lane_id)}
        Corner notes from the latest judging: {worker_guidance_brief(conn, task, lane_id)}

        Current scoring benchmark summary:
        {champion_summary}

        Current scoring benchmark body:
        {champion_body}

        {task_mode_worker_expectations(task)}

        Submit a standalone answer that improves your own weighted-rubric score.
        Do not write a rebuttal, dialogue, or point-by-point answer to the other corner.
        If any pressure point from the other corner is persuasive, you may absorb it into your own answer. Otherwise ignore it.

        Return only JSON with this shape:
        {{"summary":"concise summary","body":"full answer"}}
        """
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
        task_mode=task_mode_label(task["task_mode"]),
        task_prompt=task["prompt"],
        evaluator_tier_label=evaluator_tier_label(str(task["evaluator_tier"] or "primary")),
        tie_policy=evaluator_tie_policy(task),
        mode_scoring_guidance=task_mode_evaluator_guidance(task),
        left_submission_id=left["submission_id"],
        left_lane_id=left["lane_id"],
        left_summary=left["summary"],
        left_body=read_submission_body(left["artifact_path"]) or left["summary"],
        left_evidence=submission_evidence_prompt_block(left),
        right_submission_id=right["submission_id"],
        right_lane_id=right["lane_id"],
        right_summary=right["summary"],
        right_body=read_submission_body(right["artifact_path"]) or right["summary"],
        right_evidence=submission_evidence_prompt_block(right),
    )


def build_evaluator_resume_prompt(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    left: sqlite3.Row,
    right: sqlite3.Row,
) -> str:
    return textwrap.dedent(
        f"""\
        Continue the same judging session for this task.

        Task: {task["task_id"]} | {task["title"]}
        Task mode: {task_mode_label(task["task_mode"])}
        Judge tier: {evaluator_tier_label(str(task["evaluator_tier"] or "primary"))}
        Weighted rubric remains:
        - correctness = 35
        - completeness = 25
        - risk = 15
        - maintainability = 15
        - verification = 10
        Use weighted totals, not a raw sum. {evaluator_tie_policy(task)}
        {task_mode_evaluator_guidance(task)}
        When you give winner/loser notes, focus on how each corner can improve its own standalone answer. You may mention persuasive pressure points from the opposite corner, but do not ask for a rebuttal or debate transcript.

        LEFT SUBMISSION
        ID: {left["submission_id"]}
        Lane: {left["lane_id"]}
        Summary:
        {left["summary"]}

        Body:
        {read_submission_body(left["artifact_path"]) or left["summary"]}

        Evidence:
        {submission_evidence_prompt_block(left)}

        RIGHT SUBMISSION
        ID: {right["submission_id"]}
        Lane: {right["lane_id"]}
        Summary:
        {right["summary"]}

        Body:
        {read_submission_body(right["artifact_path"]) or right["summary"]}

        Evidence:
        {submission_evidence_prompt_block(right)}

        Return only JSON with this shape:
        {{"left_rubric":{{"correctness":0,"completeness":0,"risk":0,"maintainability":0,"verification":0}},"right_rubric":{{"correctness":0,"completeness":0,"risk":0,"maintainability":0,"verification":0}},"rationale":"...","loser_brief":"...","rematch_brief":"..."}}
        """
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
    rationale = str(payload.get("rationale", "")).strip()
    loser_brief = str(payload.get("loser_brief", "")).strip()
    rematch_brief = str(payload.get("rematch_brief", "")).strip()
    if not rationale:
        raise LaneExecutionError("Evaluator output is missing rationale")
    is_tie = float(left_total) == float(right_total)
    if is_tie:
        rematch_brief = rematch_brief or loser_brief or rationale
    elif not loser_brief:
        raise LaneExecutionError("Evaluator output is missing loser_brief")
    return {
        "left_rubric": left_rubric,
        "right_rubric": right_rubric,
        "left_total": left_total,
        "right_total": right_total,
        "rationale": rationale,
        "loser_brief": loser_brief,
        "rematch_brief": rematch_brief,
        "is_tie": is_tie,
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
    eval_index = next_evaluation_attempt_index(conn, task["task_id"])
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
        loser_brief = str(entry.get("loser_brief", "Revise your own answer to raise its weighted score on the next round."))
        rematch_brief = str(entry.get("rematch_brief", loser_brief or "Break the tie with a clearer edge while keeping both drafts standalone."))
    else:
        if task["champion_submission_id"] is None:
            lane_scores = {"worker-a": 91.0, "worker-b": 82.0}
        else:
            lane_scores = {
                task["champion_lane_id"]: 92.0,
                task["challenger_lane_id"]: 87.0,
            }
        left_rubric = rubric_from_total(lane_scores.get(left["lane_id"], 80.0))
        right_rubric = rubric_from_total(lane_scores.get(right["lane_id"], 80.0))
        if rubric_total(left_rubric) == rubric_total(right_rubric):
            right_rubric["verification"] = clamp_rubric_score(right_rubric["verification"] - 0.1)
        rationale = "Mock evaluator compared the two submissions and kept the stronger one ahead."
        loser_brief = (
            "Focus on correctness gaps and increase detail to raise your own score. "
            "If the other corner exposed a persuasive weakness, absorb that insight into your answer, but do not write a rebuttal."
        )
        rematch_brief = (
            "Break the tie with a sharper edge in correctness and verification. "
            "Use any persuasive pressure point only as optional input; submit a stronger standalone answer."
        )
    left_total = rubric_total(left_rubric)
    right_total = rubric_total(right_rubric)
    return {
        "left_rubric": left_rubric,
        "right_rubric": right_rubric,
        "left_total": left_total,
        "right_total": right_total,
        "rationale": rationale,
        "loser_brief": loser_brief,
        "rematch_brief": rematch_brief,
        "is_tie": float(left_total) == float(right_total),
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
        result = ResilientRunner(credential_vault()).execute(
            command,
            auto_switch=resilience_auto_switch_enabled(),
            input_text=prompt,
            cwd=work_dir,
            env=env,
            timeout_seconds=timeout_seconds,
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
    completed = result.completed
    (run_output_dir / "stdout.log").write_text(completed.stdout, encoding="utf-8")
    (run_output_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
    return completed


def codex_exec_config_args(lane_home: Path) -> list[str]:
    log_dir = lane_home / "log"
    sqlite_home = lane_home / "sqlite"
    log_dir.mkdir(parents=True, exist_ok=True)
    sqlite_home.mkdir(parents=True, exist_ok=True)
    return [
        "-c",
        f"log_dir={json.dumps(str(log_dir))}",
        "-c",
        f"sqlite_home={json.dumps(str(sqlite_home))}",
    ]


def codex_exec_env() -> dict[str, str]:
    ensure_selected_resilience_profile()
    ensure_live_codex_home_auth()
    env = os.environ.copy()
    env["CODEX_HOME"] = str(LAB_HOME)
    return env


def codex_probe_output_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "status": {"type": "string"},
        },
        "required": ["status"],
    }


def codex_availability_probe(timeout_seconds: float) -> dict[str, Any]:
    probe_dir = DAEMON_DIR / "quota-probe"
    probe_dir.mkdir(parents=True, exist_ok=True)
    schema_path = probe_dir / "probe-output.schema.json"
    output_path = probe_dir / "probe-output.json"
    command = [
        REAL_CODEX,
        *codex_exec_config_args(LAB_HOME),
        "--ask-for-approval",
        "never",
        "exec",
        "--skip-git-repo-check",
        "--ephemeral",
        "--sandbox",
        LIVE_CODEX_SANDBOX,
        "--cd",
        str(ROOT),
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--color",
        "never",
        "-",
    ]
    (probe_dir / "command.json").write_text(json.dumps(command, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    schema_path.write_text(json.dumps(codex_probe_output_schema(), indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    prompt = 'Return exactly {"status":"ok"} as JSON.\n'
    try:
        completed = run_codex_command(
            command,
            prompt,
            codex_exec_env(),
            ROOT,
            probe_dir,
            min(timeout_seconds, 30.0),
        )
    except FileNotFoundError:
        return {
            "ok": False,
            "quota_blocked": False,
            "message": f"missing codex binary: {REAL_CODEX}",
            "exit_code": 127,
        }
    except LaneExecutionError as exc:
        return {
            "ok": False,
            "quota_blocked": False,
            "message": str(exc),
            "exit_code": exc.exit_code,
        }
    if completed.returncode != 0:
        message_text = (completed.stderr or completed.stdout or "").strip()
        return {
            "ok": False,
            "quota_blocked": is_usage_limit_text(message_text),
            "message": textwrap.shorten(" ".join(message_text.split()), width=220, placeholder="...") or f"exit code {completed.returncode}",
            "exit_code": completed.returncode,
        }
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "ok": False,
            "quota_blocked": False,
            "message": "probe did not produce valid JSON output",
            "exit_code": completed.returncode,
        }
    if not isinstance(payload, dict) or str(payload.get("status", "")).lower() != "ok":
        return {
            "ok": False,
            "quota_blocked": False,
            "message": "probe returned an unexpected payload",
            "exit_code": completed.returncode,
        }
    return {
        "ok": True,
        "quota_blocked": False,
        "message": "codex exec probe succeeded",
        "exit_code": completed.returncode,
    }


def codex_run_json_payload(output_path: Path, label: str) -> dict[str, Any]:
    try:
        raw = output_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise LaneExecutionError(f"{label} codex exec did not write an output file") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LaneExecutionError(f"{label} codex exec wrote invalid JSON output") from exc
    if not isinstance(payload, dict):
        raise LaneExecutionError(f"{label} codex exec output must decode to a JSON object")
    return payload


def configure_run_prompt(
    conn: sqlite3.Connection,
    handle: RunHandle,
    *,
    prompt: str,
    prompt_style: str,
    used_resume: bool,
    session_id: str | None,
) -> None:
    conn.execute(
        """
        UPDATE runs
        SET prompt_bytes = ?, prompt_style = ?, used_resume = ?, session_id = ?
        WHERE run_id = ?
        """,
        (
            len(prompt.encode("utf-8")),
            prompt_style,
            int(used_resume),
            session_id,
            handle.run_id,
        ),
    )
    conn.commit()


def is_resume_rejection_text(text: str | None) -> bool:
    normalized = (text or "").lower()
    if not normalized:
        return False
    markers = (
        "thread/resume failed",
        "no rollout found for thread id",
        "failed to resume",
        "resume failed",
        "invalid session",
    )
    if any(marker in normalized for marker in markers):
        return True
    return "usage: codex exec resume" in normalized and "unexpected argument" in normalized


def run_resume_rejected(run_dir_path: Path) -> bool:
    combined = "\n".join(
        part
        for part in (
            read_text_file(run_dir_path / "stderr.log"),
            read_text_file(run_dir_path / "stdout.log"),
        )
        if part
    )
    return is_resume_rejection_text(combined)


def preserve_resume_attempt_artifacts(run_dir_path: Path) -> None:
    artifact_map = {
        "command.json": "resume-attempt-command.json",
        "prompt.txt": "resume-attempt-prompt.txt",
        "stdout.log": "resume-attempt-stdout.log",
        "stderr.log": "resume-attempt-stderr.log",
        "worker-output.json": "resume-attempt-worker-output.json",
        "evaluator-output.json": "resume-attempt-evaluator-output.json",
    }
    for source_name, target_name in artifact_map.items():
        source = run_dir_path / source_name
        target = run_dir_path / target_name
        if not source.exists() or target.exists():
            continue
        shutil.copyfile(source, target)


def build_codex_resume_command(home: Path, output_path: Path, session_id: str) -> list[str]:
    return [
        REAL_CODEX,
        *codex_exec_config_args(home),
        "-c",
        f"sandbox_mode={json.dumps(LIVE_CODEX_SANDBOX)}",
        "--ask-for-approval",
        "never",
        "exec",
        "resume",
        "--skip-git-repo-check",
        "--output-last-message",
        str(output_path),
        "--json",
        session_id,
        "-",
    ]


def worker_codex_command_spec(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    lane_id: str,
    workspace: Path,
    run_dir_path: Path,
) -> tuple[str, list[str], Path, str, bool, str | None]:
    paths = lane_paths(lane_id)
    output_path = run_dir_path / "worker-output.json"
    session_id = live_task_lane_session_id(conn, task, lane_id)
    if session_id:
        prompt = build_worker_resume_prompt(conn, task, lane_id)
        command = build_codex_resume_command(paths["home"], output_path, session_id)
        return prompt, command, output_path, "resume-delta", True, session_id
    schema_path = run_dir_path / "worker-output.schema.json"
    schema_path.write_text(json.dumps(worker_output_schema(), indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    prompt = build_worker_prompt(conn, task, lane_id)
    command = [
        REAL_CODEX,
        *codex_exec_config_args(paths["home"]),
        "--ask-for-approval",
        "never",
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        LIVE_CODEX_SANDBOX,
        "--cd",
        str(workspace),
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--color",
        "never",
        "--json",
        "-",
    ]
    return prompt, command, output_path, "full", False, None


def evaluator_codex_command_spec(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    left: sqlite3.Row,
    right: sqlite3.Row,
    workspace: Path,
    run_dir_path: Path,
) -> tuple[str, list[str], Path, str, bool, str | None]:
    paths = lane_paths("evaluator")
    output_path = run_dir_path / "evaluator-output.json"
    session_id = live_task_lane_session_id(conn, task, "evaluator")
    if session_id:
        prompt = build_evaluator_resume_prompt(conn, task, left, right)
        command = build_codex_resume_command(paths["home"], output_path, session_id)
        return prompt, command, output_path, "resume-delta", True, session_id
    schema_path = run_dir_path / "evaluator-output.schema.json"
    schema_path.write_text(json.dumps(evaluator_output_schema(), indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    prompt = build_evaluator_prompt(conn, task, left, right)
    command = [
        REAL_CODEX,
        *codex_exec_config_args(paths["home"]),
        "--ask-for-approval",
        "never",
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        LIVE_CODEX_SANDBOX,
        "--cd",
        str(workspace),
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--color",
        "never",
        "--json",
        "-",
    ]
    return prompt, command, output_path, "full", False, None


def launch_codex_worker(conn: sqlite3.Connection, lane_id: str) -> CodexWorkerLaunch:
    lane = fetch_lane(conn, lane_id)
    task = fetch_task(conn, lane["active_task_id"])
    paths = lane_paths(lane_id)
    workspace = prepare_task_workspace(conn, lane_id, task["task_id"])
    handle = start_run(conn, lane_id, task["task_id"], "codex:worker", [REAL_CODEX, "--ask-for-approval", "never", "exec", "--skip-git-repo-check"], workspace, paths["home"])
    prompt, command, output_path, prompt_style, used_resume, session_id = worker_codex_command_spec(conn, task, lane_id, workspace, handle.run_dir)
    prompt_path = handle.run_dir / "prompt.txt"
    stdout_path = handle.run_dir / "stdout.log"
    stderr_path = handle.run_dir / "stderr.log"
    prompt_path.write_text(prompt, encoding="utf-8")
    update_run_command(conn, handle, command)
    configure_run_prompt(conn, handle, prompt=prompt, prompt_style=prompt_style, used_resume=used_resume, session_id=session_id)
    try:
        with prompt_path.open("r", encoding="utf-8") as stdin_handle, stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                stdin=stdin_handle,
                stdout=stdout_handle,
                stderr=stderr_handle,
                text=True,
                env=codex_exec_env(),
            )
    except FileNotFoundError as exc:
        raise LaneExecutionError(f"missing codex binary: {REAL_CODEX}", exit_code=127) from exc
    return CodexWorkerLaunch(
        handle=handle,
        lane_id=lane_id,
        task_id=task["task_id"],
        workspace=workspace,
        output_path=output_path,
        prompt_style=prompt_style,
        used_resume=used_resume,
        session_id=session_id,
        process=process,
        started_monotonic=time.monotonic(),
    )


def await_codex_worker_launch(launch: CodexWorkerLaunch, timeout_seconds: float) -> dict[str, Any]:
    elapsed = time.monotonic() - launch.started_monotonic
    remaining = max(timeout_seconds - elapsed, 0.0)
    try:
        return_code = launch.process.wait(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        launch.process.kill()
        try:
            launch.process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            pass
        raise LaneExecutionError(
            f"codex exec timed out after {timeout_seconds:g}s",
            exit_code=124,
            run_status="timeout",
        ) from exc
    if return_code != 0:
        raise LaneExecutionError(f"worker codex exec failed with exit code {return_code}", return_code)
    payload = codex_run_json_payload(launch.output_path, "worker")
    if not isinstance(payload, dict) or not isinstance(payload.get("summary"), str) or not isinstance(payload.get("body"), str):
        raise LaneExecutionError("worker codex exec wrote invalid JSON output")
    return payload


def collect_codex_worker_launch(conn: sqlite3.Connection, launch: CodexWorkerLaunch, timeout_seconds: float) -> dict[str, Any]:
    try:
        payload = await_codex_worker_launch(launch, timeout_seconds)
        if launch.used_resume and run_resume_rejected(launch.handle.run_dir):
            raise LaneExecutionError("worker codex resume was rejected; retrying cold", exit_code=75)
        session_id = finalize_task_lane_session(
            conn,
            task_id=launch.task_id,
            lane_id=launch.lane_id,
            handle=launch.handle,
            workspace=launch.workspace,
            known_session_id=launch.session_id,
        )
        submission = record_submission(
            conn,
            launch.task_id,
            launch.lane_id,
            summary=str(payload["summary"]).strip(),
            body=str(payload["body"]).strip(),
            meta_extra=submission_runtime_and_evidence(
                workspace=launch.workspace,
                runtime={
                    "executor": "codex",
                    "run_id": launch.handle.run_id,
                    "session_id": session_id,
                    "prompt_style": launch.prompt_style,
                    "used_resume": launch.used_resume,
                },
            ),
        )
        finish_run(conn, launch.handle, "completed", 0, submission["submission_id"])
        return {
            "type": "worker_submission",
            "lane_id": launch.lane_id,
            "task_id": launch.task_id,
            "submission_id": submission["submission_id"],
        }
    except Exception as exc:
        if launch.used_resume and run_resume_rejected(launch.handle.run_dir):
            preserve_resume_attempt_artifacts(launch.handle.run_dir)
            clear_task_lane_session_id(conn, launch.task_id, launch.lane_id)
            append_event(
                "run_resume_fallback",
                run_id=launch.handle.run_id,
                lane_id=launch.lane_id,
                task_id=launch.task_id,
                mode=launch.handle.mode,
            )
            try:
                task = fetch_task(conn, launch.task_id)
                payload = codex_worker_output(
                    conn,
                    task,
                    launch.lane_id,
                    launch.workspace,
                    launch.handle,
                    timeout_seconds,
                )
                submission = record_submission(
                    conn,
                    launch.task_id,
                    launch.lane_id,
                    summary=str(payload["summary"]).strip(),
                    body=str(payload["body"]).strip(),
                    meta_extra=payload.get("meta"),
                )
                finish_run(conn, launch.handle, "completed", 0, submission["submission_id"])
                return {
                    "type": "worker_submission",
                    "lane_id": launch.lane_id,
                    "task_id": launch.task_id,
                    "submission_id": submission["submission_id"],
                }
            except Exception as fallback_exc:
                exc = fallback_exc
        if run_dir_quota_blocked(launch.handle.run_dir) and resilience_auto_switch_enabled():
            rotated_to = rotate_resilience_account(reason=f"quota blocked {launch.lane_id} worker run")
            if rotated_to:
                append_event(
                    "quota_auto_resume",
                    lane_id=launch.lane_id,
                    task_id=launch.task_id,
                    recovered=True,
                    account_key=rotated_to,
                )
                try:
                    task = fetch_task(conn, launch.task_id)
                    payload = codex_worker_output(
                        conn,
                        task,
                        launch.lane_id,
                        launch.workspace,
                        launch.handle,
                        timeout_seconds,
                    )
                    submission = record_submission(
                        conn,
                        launch.task_id,
                        launch.lane_id,
                        summary=str(payload["summary"]).strip(),
                        body=str(payload["body"]).strip(),
                        meta_extra=payload.get("meta"),
                    )
                    finish_run(conn, launch.handle, "completed", 0, submission["submission_id"])
                    return {
                        "type": "worker_submission",
                        "lane_id": launch.lane_id,
                        "task_id": launch.task_id,
                        "submission_id": submission["submission_id"],
                    }
                except Exception as rotated_exc:
                    exc = rotated_exc
        exit_code = exc.exit_code if isinstance(exc, LaneExecutionError) else 1
        run_status = exc.run_status if isinstance(exc, LaneExecutionError) else "failed"
        finish_run(conn, launch.handle, run_status, exit_code)
        set_lane_error(conn, launch.lane_id, str(exc), launch.task_id)
        return {
            "type": "worker_error",
            "lane_id": launch.lane_id,
            "task_id": launch.task_id,
            "error": str(exc),
        }


def codex_worker_output(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    lane_id: str,
    workspace: Path,
    handle: RunHandle,
    timeout_seconds: float,
) -> dict[str, Any]:
    prompt, command, output_path, prompt_style, used_resume, known_session_id = worker_codex_command_spec(
        conn,
        task,
        lane_id,
        workspace,
        handle.run_dir,
    )
    update_run_command(conn, handle, command)
    configure_run_prompt(conn, handle, prompt=prompt, prompt_style=prompt_style, used_resume=used_resume, session_id=known_session_id)
    env = codex_exec_env()
    completed = run_codex_command(command, prompt, env, workspace, handle.run_dir, timeout_seconds)
    if completed.returncode != 0:
        if used_resume and run_resume_rejected(handle.run_dir):
            preserve_resume_attempt_artifacts(handle.run_dir)
            clear_task_lane_session_id(conn, task["task_id"], lane_id)
            append_event(
                "run_resume_fallback",
                run_id=handle.run_id,
                lane_id=lane_id,
                task_id=task["task_id"],
                mode=handle.mode,
            )
            prompt, command, output_path, prompt_style, used_resume, known_session_id = worker_codex_command_spec(
                conn,
                fetch_task(conn, task["task_id"]),
                lane_id,
                workspace,
                handle.run_dir,
            )
            update_run_command(conn, handle, command)
            configure_run_prompt(conn, handle, prompt=prompt, prompt_style=prompt_style, used_resume=used_resume, session_id=known_session_id)
            completed = run_codex_command(command, prompt, env, workspace, handle.run_dir, timeout_seconds)
        if completed.returncode != 0:
            raise LaneExecutionError(f"worker codex exec failed with exit code {completed.returncode}", completed.returncode)
    payload = codex_run_json_payload(output_path, "worker")
    if not isinstance(payload, dict) or not isinstance(payload.get("summary"), str) or not isinstance(payload.get("body"), str):
        raise LaneExecutionError("worker codex exec wrote invalid JSON output")
    session_id = finalize_task_lane_session(
        conn,
        task_id=task["task_id"],
        lane_id=lane_id,
        handle=handle,
        workspace=workspace,
        known_session_id=known_session_id,
    )
    payload["meta"] = {
        "runtime": {
            "executor": "codex",
            "run_id": handle.run_id,
            "session_id": session_id,
            "prompt_style": prompt_style,
            "used_resume": used_resume,
        },
        "evidence": workspace_change_evidence(workspace),
    }
    return payload


def codex_evaluator_output(
    conn: sqlite3.Connection,
    task: sqlite3.Row,
    left: sqlite3.Row,
    right: sqlite3.Row,
    workspace: Path,
    handle: RunHandle,
    timeout_seconds: float,
) -> dict[str, Any]:
    prompt, command, output_path, prompt_style, used_resume, known_session_id = evaluator_codex_command_spec(
        conn,
        task,
        left,
        right,
        workspace,
        handle.run_dir,
    )
    update_run_command(conn, handle, command)
    configure_run_prompt(conn, handle, prompt=prompt, prompt_style=prompt_style, used_resume=used_resume, session_id=known_session_id)
    env = codex_exec_env()
    completed = run_codex_command(command, prompt, env, workspace, handle.run_dir, timeout_seconds)
    if completed.returncode != 0:
        if used_resume and run_resume_rejected(handle.run_dir):
            preserve_resume_attempt_artifacts(handle.run_dir)
            clear_task_lane_session_id(conn, task["task_id"], "evaluator")
            append_event(
                "run_resume_fallback",
                run_id=handle.run_id,
                lane_id="evaluator",
                task_id=task["task_id"],
                mode=handle.mode,
            )
            prompt, command, output_path, prompt_style, used_resume, known_session_id = evaluator_codex_command_spec(
                conn,
                fetch_task(conn, task["task_id"]),
                left,
                right,
                workspace,
                handle.run_dir,
            )
            update_run_command(conn, handle, command)
            configure_run_prompt(conn, handle, prompt=prompt, prompt_style=prompt_style, used_resume=used_resume, session_id=known_session_id)
            completed = run_codex_command(command, prompt, env, workspace, handle.run_dir, timeout_seconds)
        if completed.returncode != 0:
            raise LaneExecutionError(f"evaluator codex exec failed with exit code {completed.returncode}", completed.returncode)
    payload = codex_run_json_payload(output_path, "evaluator")
    parsed = parse_evaluator_payload(payload)
    session_id = finalize_task_lane_session(
        conn,
        task_id=task["task_id"],
        lane_id="evaluator",
        handle=handle,
        workspace=workspace,
        known_session_id=known_session_id,
    )
    parsed["meta"] = {
        "executor": "codex",
        "run_id": handle.run_id,
        "session_id": session_id,
        "prompt_style": prompt_style,
        "used_resume": used_resume,
    }
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
    pair_mode = str(task["pair_mode"] or "initial_duel")
    phase = {
        "initial_duel": "initial",
        "challenger_retry": "retry",
        "full_rematch": "rematch",
        "review_only": "review",
    }.get(pair_mode, "retry")
    stage_slug, stage_label = boxing_stage_info(
        pair_mode=pair_mode,
        duel_stage=task["duel_stage"],
        evaluator_tier=task["evaluator_tier"],
        tier_phase=task["tier_phase"],
    )
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
        "corner_label": lane_display_name(lane_id),
        "stage_slug": stage_slug,
        "stage_label": stage_label,
    }
    if meta_extra:
        runtime = meta_extra.get("runtime") if isinstance(meta_extra, dict) else None
        evidence = meta_extra.get("evidence") if isinstance(meta_extra, dict) else None
        if isinstance(runtime, dict):
            metadata["runtime"] = runtime
        elif isinstance(meta_extra, dict):
            metadata["runtime"] = meta_extra
        if isinstance(evidence, dict):
            metadata["evidence"] = evidence
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
    conn.commit()
    sync_task_state(conn, task_id)
    append_event("submission_recorded", task_id=task_id, submission_id=submission_id, lane_id=lane_id)
    return {"submission_id": submission_id, "summary": summary, "artifact_path": str(artifact_path)}


def reset_evaluator_lane(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE lanes
        SET status = 'idle', active_task_id = NULL, active_submission_id = NULL, notes = '', updated_at = ?
        WHERE lane_id = 'evaluator'
        """,
        (now_utc(),),
    )


def schedule_tie_rematch(conn: sqlite3.Connection, task_id: str, reason: str) -> None:
    for lane_id in WORKER_LANES:
        assign_or_reserve_task(conn, lane_id, task_id, "tie_rematch", reason)


def handle_tied_evaluation(
    conn: sqlite3.Connection,
    task_id: str,
    left_submission_id: str,
    right_submission_id: str,
    rationale: str,
    rematch_brief: str,
    left_rubric: dict[str, Any],
    right_rubric: dict[str, Any],
    scorecard_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = fetch_task(conn, task_id)
    left = fetch_submission(conn, left_submission_id)
    right = fetch_submission(conn, right_submission_id)
    left_total = rubric_total(normalize_rubric(left_rubric))
    right_total = rubric_total(normalize_rubric(right_rubric))
    if float(left_total) != float(right_total):
        raise SystemExit("Tied evaluation handler requires equal totals")

    duel_stage = str(task["duel_stage"] or ("initial" if not task["champion_submission_id"] else "retry"))
    pair_mode = str(task["pair_mode"] or ("initial_duel" if not task["champion_submission_id"] else "challenger_retry"))
    evaluator_tier = str(task["evaluator_tier"] or "primary")
    tier_phase = str(task["tier_phase"] or "base")
    shared_brief = rematch_brief.strip() or rationale.strip()
    next_status = "awaiting_evaluation"
    next_pair_mode = pair_mode
    next_tier = evaluator_tier
    next_tier_phase = tier_phase
    next_duel_stage = duel_stage
    next_step = "absolute_rereview"
    worker_rematch = False

    if pair_mode in {"initial_duel", "challenger_retry"}:
        next_pair_mode = "full_rematch"
        next_tier_phase = "post_rematch"
        next_status = "worker_rematching"
        next_step = "worker_rematch"
        worker_rematch = True
        if duel_stage != "initial":
            next_duel_stage = "tiebreak"
    elif pair_mode == "full_rematch" and tier_phase == "post_rematch":
        next_pair_mode = "review_only"
        next_status = "awaiting_evaluation"
        if evaluator_tier == "primary":
            next_tier = "elder"
            next_tier_phase = "base"
            next_step = "elder_review"
        elif evaluator_tier == "elder":
            next_tier = "absolute"
            next_tier_phase = "base"
            next_step = "absolute_review"
        else:
            next_tier = "absolute"
            next_tier_phase = "post_rematch"
            next_step = "absolute_rereview"
    elif pair_mode == "review_only" and tier_phase == "base":
        next_pair_mode = "full_rematch"
        next_tier_phase = "post_rematch"
        next_status = "worker_rematching"
        next_step = "absolute_final_worker_rematch" if evaluator_tier == "absolute" else "worker_rematch"
        worker_rematch = True
        if duel_stage != "initial":
            next_duel_stage = "tiebreak"
    elif pair_mode == "review_only" and evaluator_tier == "absolute" and tier_phase == "post_rematch":
        next_pair_mode = "review_only"
        next_tier = "absolute"
        next_tier_phase = "post_rematch"
        next_status = "awaiting_evaluation"
        next_step = "absolute_rereview"
    else:
        next_pair_mode = "full_rematch"
        next_tier_phase = "post_rematch"
        next_status = "worker_rematching"
        next_step = "worker_rematch"
        worker_rematch = True
        if duel_stage != "initial":
            next_duel_stage = "tiebreak"

    conn.execute(
        """
        UPDATE tasks
        SET status = ?,
            updated_at = ?,
            duel_stage = ?,
            pair_mode = ?,
            evaluator_tier = ?,
            tier_phase = ?,
            rematch_brief = ?
        WHERE task_id = ?
        """,
        (
            next_status,
            now_utc(),
            next_duel_stage,
            next_pair_mode,
            next_tier,
            next_tier_phase,
            shared_brief,
            task_id,
        ),
    )
    if worker_rematch:
        schedule_tie_rematch(
            conn,
            task_id,
            f"{evaluator_tier_label(evaluator_tier)} declared a tie; rematch required",
        )
    reset_evaluator_lane(conn)
    conn.commit()
    sync_task_state(conn, task_id)
    append_event(
        "evaluation_tied",
        task_id=task_id,
        evaluator_tier=evaluator_tier,
        next_evaluator_tier=next_tier,
        pair_mode=pair_mode,
        next_pair_mode=next_pair_mode,
        next_step=next_step,
        left_submission_id=left["submission_id"],
        right_submission_id=right["submission_id"],
        left_lane_id=left["lane_id"],
        right_lane_id=right["lane_id"],
        left_total=left_total,
        right_total=right_total,
        rationale=rationale,
        rematch_brief=shared_brief,
        worker_rematch=worker_rematch,
    )
    return {
        "task_id": task_id,
        "evaluator_tier": evaluator_tier,
        "next_evaluator_tier": next_tier,
        "pair_mode": pair_mode,
        "next_pair_mode": next_pair_mode,
        "next_step": next_step,
        "left_submission_id": left["submission_id"],
        "right_submission_id": right["submission_id"],
        "left_lane_id": left["lane_id"],
        "right_lane_id": right["lane_id"],
        "left_total": left_total,
        "right_total": right_total,
        "worker_rematch": worker_rematch,
    }


def set_task_apply_state(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    apply_status: str,
    submission_id: str | None,
    notes: str,
) -> None:
    applied_at = now_utc() if apply_status == "applied" else None
    conn.execute(
        """
        UPDATE tasks
        SET apply_status = ?,
            applied_submission_id = ?,
            applied_at = ?,
            apply_notes = ?,
            updated_at = ?
        WHERE task_id = ?
        """,
        (apply_status, submission_id, applied_at, notes[:2000], now_utc(), task_id),
    )
    conn.commit()
    sync_task_state(conn, task_id)


def repo_conflicting_paths(repo_path: Path, paths: list[str]) -> list[str]:
    if not paths or shutil.which("git") is None:
        return []
    probe = git_probe(repo_path)
    if not probe["is_repo"]:
        return []
    result = subprocess.run(
        ["git", "-C", str(repo_path), "status", "--short", "--", *paths],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    conflicts: list[str] = []
    for raw_line in result.stdout.splitlines():
        parsed = parse_git_status_line(raw_line)
        if parsed is None:
            continue
        _status, path_text, _previous_path = parsed
        if path_text != "codexlab-task.json":
            conflicts.append(path_text)
    return sorted(dict.fromkeys(conflicts))


def promote_patch_winner(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    winner_submission_id: str,
    winner_lane_id: str,
) -> dict[str, Any]:
    task = fetch_task(conn, task_id)
    if str(task["task_mode"] or "proposal") != "patch":
        return {"apply_status": "not_requested", "notes": "task mode is proposal"}

    workspace = existing_task_workspace(winner_lane_id, task_id)
    if workspace is None:
        notes = "winner workspace was not found"
        set_task_apply_state(conn, task_id, apply_status="not_applied", submission_id=winner_submission_id, notes=notes)
        append_event("task_apply_failed", task_id=task_id, submission_id=winner_submission_id, lane_id=winner_lane_id, reason=notes)
        return {"apply_status": "not_applied", "notes": notes}

    evidence = workspace_change_evidence(workspace)
    changed_files = [item for item in evidence.get("changed_files") or [] if item != "codexlab-task.json"]
    deleted_files = [item for item in evidence.get("deleted_files") or [] if item != "codexlab-task.json"]
    touched_paths = sorted(dict.fromkeys(changed_files + deleted_files))
    if not touched_paths:
        notes = "winner workspace had no repo changes to apply"
        set_task_apply_state(conn, task_id, apply_status="not_applied", submission_id=winner_submission_id, notes=notes)
        append_event("task_apply_failed", task_id=task_id, submission_id=winner_submission_id, lane_id=winner_lane_id, reason=notes)
        return {"apply_status": "not_applied", "notes": notes}

    repo_path = target_repo_path()
    conflicts = repo_conflicting_paths(repo_path, touched_paths)
    if conflicts:
        notes = f"target repo has local changes on: {', '.join(conflicts)}"
        set_task_apply_state(conn, task_id, apply_status="not_applied", submission_id=winner_submission_id, notes=notes)
        append_event("task_apply_failed", task_id=task_id, submission_id=winner_submission_id, lane_id=winner_lane_id, reason=notes)
        return {"apply_status": "not_applied", "notes": notes, "conflicts": conflicts}

    copied: list[str] = []
    removed: list[str] = []
    for relative_path in changed_files:
        source = workspace / relative_path
        target = repo_path / relative_path
        if not source.exists() or source.is_dir():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied.append(relative_path)
    for relative_path in deleted_files:
        target = repo_path / relative_path
        if target.is_dir():
            shutil.rmtree(target)
            removed.append(relative_path)
        elif target.exists():
            target.unlink()
            removed.append(relative_path)

    notes = (
        f"applied {len(copied)} file(s)"
        + (f" and removed {len(removed)} file(s)" if removed else "")
        + f" from {lane_display_name(winner_lane_id)}"
    )
    set_task_apply_state(conn, task_id, apply_status="applied", submission_id=winner_submission_id, notes=notes)
    append_event(
        "task_applied",
        task_id=task_id,
        submission_id=winner_submission_id,
        lane_id=winner_lane_id,
        changed_files=copied,
        deleted_files=removed,
    )
    return {
        "apply_status": "applied",
        "notes": notes,
        "changed_files": copied,
        "deleted_files": removed,
    }


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

    duel_stage = str(task["duel_stage"] or ("initial" if not task["champion_submission_id"] else "retry"))
    pair_mode = str(task["pair_mode"] or ("initial_duel" if not task["champion_submission_id"] else "challenger_retry"))
    evaluator_tier = str(task["evaluator_tier"] or "primary")
    reply_stage = direct_reply_stage(task) if pair_mode == "challenger_retry" else ""
    winner = left if left_total > right_total else right
    loser = right if winner["submission_id"] == left["submission_id"] else left
    winner_score = float(left_total if winner["submission_id"] == left["submission_id"] else right_total)
    loser_score = float(right_total if winner["submission_id"] == left["submission_id"] else left_total)
    winner_rubric = left_rubric if winner["submission_id"] == left["submission_id"] else right_rubric
    loser_rubric = right_rubric if winner["submission_id"] == left["submission_id"] else left_rubric
    initial_duel = duel_stage == "initial"
    evaluation_id = allocate_id(conn, "next_evaluation_id", "E")
    current_champion_id = task["champion_submission_id"]
    swap_occurred = int(bool(current_champion_id and winner["submission_id"] != current_champion_id))
    total_evaluations = int(task["total_evaluations"] or 0) + 1
    role_swaps = int(task["role_swaps"] or 0)
    challenger_failed_attempts = int(task["challenger_failed_attempts"] or 0)
    masterpiece_locked = 0
    next_duel_stage = duel_stage
    next_pair_mode = pair_mode
    next_tier = "primary"
    next_tier_phase = "base"
    next_rematch_brief = ""
    task_status = "awaiting_evaluation"
    next_reply_reason = ""
    stage_slug, stage_label = boxing_stage_info(
        pair_mode=pair_mode,
        duel_stage=duel_stage,
        evaluator_tier=evaluator_tier,
        tier_phase=task["tier_phase"],
    )

    if initial_duel:
        champion_submission_id = winner["submission_id"]
        challenger_submission_id = loser["submission_id"]
        champion_lane_id = winner["lane_id"]
        challenger_lane_id = loser["lane_id"]
        champion_score = winner_score
        challenger_score = loser_score
        challenger_failed_attempts = 0
        task_status = "challenger_retrying"
        next_duel_stage = "counterattack"
        next_pair_mode = "challenger_retry"
        next_reply_reason = "counterattack after opening evaluation"
    elif pair_mode == "challenger_retry" and reply_stage == "counterattack" and swap_occurred:
        champion_submission_id = winner["submission_id"]
        challenger_submission_id = loser["submission_id"]
        champion_lane_id = winner["lane_id"]
        challenger_lane_id = loser["lane_id"]
        champion_score = winner_score
        challenger_score = loser_score
        challenger_failed_attempts = 0
        task_status = "challenger_retrying"
        next_duel_stage = "defense"
        next_pair_mode = "challenger_retry"
        next_reply_reason = "defense round after champion swap"
        role_swaps += 1
    else:
        masterpiece_locked = 1
        task_status = "masterpiece_locked"
        champion_submission_id = winner["submission_id"]
        champion_lane_id = winner["lane_id"]
        champion_score = winner_score
        challenger_submission_id = loser["submission_id"]
        challenger_lane_id = loser["lane_id"]
        challenger_score = loser_score
        if pair_mode == "challenger_retry":
            challenger_failed_attempts = 1
        elif duel_stage != "initial" and current_champion_id == champion_submission_id:
            challenger_failed_attempts = 1
        else:
            challenger_failed_attempts = 0
        if swap_occurred:
            role_swaps += 1

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
        "duel_stage": duel_stage,
        "pair_mode": pair_mode,
        "evaluator_tier": evaluator_tier,
        "tier_phase": task["tier_phase"],
        "task_mode": task["task_mode"],
        "stage_slug": stage_slug,
        "stage_label": stage_label,
        "judge_label": evaluator_display_label(evaluator_tier),
    }
    if scorecard_extra:
        scorecard["runtime"] = scorecard_extra
    conn.execute(
        """
        INSERT INTO evaluations(
            evaluation_id, task_id, left_submission_id, right_submission_id,
            winner_submission_id, loser_submission_id, winner_lane_id, loser_lane_id,
            winner_score, loser_score, score_delta, rationale, loser_brief,
            swap_occurred, masterpiece_locked, evaluator_tier, created_at, scorecard_json
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            evaluator_tier,
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
            masterpiece_locked = ?,
            duel_stage = ?,
            pair_mode = ?,
            evaluator_tier = ?,
            tier_phase = ?,
            rematch_brief = ?
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
            next_duel_stage,
            next_pair_mode,
            next_tier,
            next_tier_phase,
            next_rematch_brief,
            task_id,
        ),
    )

    if masterpiece_locked:
        release_lane_and_promote(conn, winner["lane_id"], finished_task_id=task_id)
        release_lane_and_promote(conn, loser["lane_id"], finished_task_id=task_id)
    else:
        release_lane_and_promote(conn, winner["lane_id"], finished_task_id=task_id)
        assign_or_reserve_task(conn, challenger_lane_id, task_id, "duel_retry", next_reply_reason or "direct reply round after evaluation")

    reset_evaluator_lane(conn)
    conn.commit()
    sync_task_state(conn, task_id)

    apply_result: dict[str, Any] | None = None
    if masterpiece_locked:
        apply_result = promote_patch_winner(
            conn,
            task_id=task_id,
            winner_submission_id=champion_submission_id,
            winner_lane_id=champion_lane_id,
        )

    evaluation_file = ensure_task_dirs(task_id) / "evaluations" / f"{evaluation_id}.json"
    evaluation_file.write_text(json.dumps(scorecard | {"evaluation_id": evaluation_id}, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    append_event(
        "task_scored",
        task_id=task_id,
        evaluation_id=evaluation_id,
        winner_submission_id=winner["submission_id"],
        winner_score=winner_score,
        loser_submission_id=loser["submission_id"],
        loser_score=loser_score,
        winner_lane_id=winner["lane_id"],
        loser_lane_id=loser["lane_id"],
        score_delta=winner_score - loser_score,
        swap_occurred=bool(swap_occurred),
        masterpiece_locked=bool(masterpiece_locked),
        evaluator_tier=evaluator_tier,
    )
    if masterpiece_locked:
        append_event(
            "task_masterpiece_locked",
            task_id=task_id,
            evaluation_id=evaluation_id,
            submission_id=champion_submission_id,
            lane_id=champion_lane_id,
            score=champion_score,
        )
    return {
        "evaluation_id": evaluation_id,
        "winner_submission_id": winner["submission_id"],
        "winner_lane_id": winner["lane_id"],
        "loser_submission_id": loser["submission_id"],
        "loser_lane_id": loser["lane_id"],
        "masterpiece_locked": bool(masterpiece_locked),
        "swap_occurred": bool(swap_occurred),
        "apply_result": apply_result,
    }


def ready_evaluation_pair(conn: sqlite3.Connection, task_id: str) -> tuple[sqlite3.Row, sqlite3.Row] | None:
    task = fetch_task(conn, task_id)
    if task["masterpiece_locked"]:
        return None
    pair_mode = str(task["pair_mode"] or ("initial_duel" if not task["champion_submission_id"] else "challenger_retry"))
    phases = submission_phases_for_pair_mode(pair_mode)
    if pair_mode in {"initial_duel", "full_rematch", "review_only"}:
        ready: list[sqlite3.Row] = []
        for lane_id in WORKER_LANES:
            submission = pair_submission_for_lane(conn, task, lane_id, phases)
            if submission is not None:
                ready.append(submission)
        if len(ready) < 2:
            return None
        ready.sort(key=lambda row: row["lane_id"])
        return ready[0], ready[1]

    challenger_lane_id = task["challenger_lane_id"]
    if not challenger_lane_id:
        return None
    challenger = pair_submission_for_lane(conn, task, challenger_lane_id, phases)
    if challenger is None or not task["champion_submission_id"]:
        return None
    champion = fetch_submission(conn, task["champion_submission_id"])
    return champion, challenger


def ready_evaluation_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for task in conn.execute(
        """
        SELECT * FROM tasks
        WHERE status IN ('in_progress', 'awaiting_evaluation', 'challenger_retrying', 'worker_rematching')
          AND masterpiece_locked = 0
        ORDER BY created_at ASC, task_id ASC
        """
    ).fetchall():
        pair = ready_evaluation_pair(conn, task["task_id"])
        if pair is None:
            continue
        candidates.append({"task": task, "pair": pair})
    return candidates


def rebalance_waiting_worker_lanes(conn: sqlite3.Connection) -> list[str]:
    released: list[str] = []
    for lane in conn.execute(
        """
        SELECT * FROM lanes
        WHERE lane_type = 'worker'
          AND status = 'waiting_eval'
          AND active_task_id IS NOT NULL
          AND active_submission_id IS NOT NULL
          AND active_run_id IS NULL
        ORDER BY lane_id ASC
        """
    ).fetchall():
        if not lane_queue(conn, lane["lane_id"]):
            continue
        if query_submission(conn, lane["active_submission_id"]) is None:
            continue
        conn.execute(
            """
            UPDATE lanes
            SET status = 'idle', active_task_id = NULL, active_submission_id = NULL, notes = '', updated_at = ?
            WHERE lane_id = ?
            """,
            (now_utc(), lane["lane_id"]),
        )
        released.append(lane["lane_id"])
        promote_lane_from_queue(conn, lane["lane_id"])
    return released


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


def evaluator_runnable(conn: sqlite3.Connection) -> tuple[sqlite3.Row, sqlite3.Row | None, tuple[sqlite3.Row, sqlite3.Row] | None]:
    lane = fetch_lane(conn, "evaluator")
    if lane["status"] == "error" or lane["active_run_id"] is not None:
        return lane, None, None
    if lane["active_task_id"] is not None:
        preferred_task = fetch_task(conn, lane["active_task_id"])
        preferred_pair = ready_evaluation_pair(conn, preferred_task["task_id"])
        if preferred_pair is not None:
            return lane, preferred_task, preferred_pair
    candidates = ready_evaluation_candidates(conn)
    if not candidates:
        return lane, None, None
    selected = candidates[0]
    return lane, selected["task"], selected["pair"]


def run_worker_once(conn: sqlite3.Connection, lane_id: str, executor: str, exec_timeout: float) -> dict[str, Any]:
    if executor == "mock":
        lane = fetch_lane(conn, lane_id)
        task = fetch_task(conn, lane["active_task_id"])
        paths = lane_paths(lane_id)
        workspace = prepare_task_workspace(conn, lane_id, task["task_id"])
        command = ["mock-worker", lane_id, task["task_id"]]
        handle = start_run(conn, lane_id, task["task_id"], f"{executor}:worker", command, workspace, paths["home"])
        try:
            payload = mock_worker_output(conn, task, lane_id)
            submission = record_submission(
                conn,
                task["task_id"],
                lane_id,
                summary=str(payload["summary"]).strip(),
                body=str(payload["body"]).strip(),
                meta_extra=submission_runtime_and_evidence(
                    workspace=workspace,
                    runtime=payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
                ),
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
            set_lane_error(conn, lane_id, str(exc), task["task_id"])
            return {
                "type": "worker_error",
                "lane_id": lane_id,
                "task_id": task["task_id"],
                "error": str(exc),
            }
    elif executor == "codex":
        try:
            launch = launch_codex_worker(conn, lane_id)
        except Exception as exc:
            exit_code = exc.exit_code if isinstance(exc, LaneExecutionError) else 1
            run_status = exc.run_status if isinstance(exc, LaneExecutionError) else "failed"
            lane = fetch_lane(conn, lane_id)
            task_id = lane["active_task_id"]
            if lane["active_run_id"]:
                run = query_run(conn, lane["active_run_id"])
                if run is not None:
                    handle = RunHandle(
                        run_id=run["run_id"],
                        lane_id=lane_id,
                        task_id=str(task_id or ""),
                        mode=run["mode"],
                        run_dir=run_dir(run["run_id"]),
                        started_at=str(run["started_at"] or now_utc()),
                    )
                    finish_run(conn, handle, run_status, exit_code)
            set_lane_error(conn, lane_id, str(exc), task_id)
            return {
                "type": "worker_error",
                "lane_id": lane_id,
                "task_id": task_id,
                "error": str(exc),
            }
        return collect_codex_worker_launch(conn, launch, exec_timeout)
    else:
        raise SystemExit(f"Unsupported executor: {executor}")


def codex_concurrent_worker_lanes(conn: sqlite3.Connection, workers: list[sqlite3.Row]) -> list[sqlite3.Row]:
    if len(workers) < 2:
        return []
    task_ids = {lane["active_task_id"] for lane in workers if lane["active_task_id"]}
    if len(task_ids) != 1:
        return []
    task = fetch_task(conn, next(iter(task_ids)))
    if str(task["pair_mode"] or "initial_duel") not in SIMULTANEOUS_WORKER_PAIR_MODES:
        return []
    return sorted(workers, key=lambda row: str(row["lane_id"]))


def run_codex_workers_concurrently(conn: sqlite3.Connection, lane_ids: list[str], exec_timeout: float) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    launches: list[CodexWorkerLaunch] = []
    for lane_id in lane_ids:
        try:
            launches.append(launch_codex_worker(conn, lane_id))
        except Exception as exc:
            exit_code = exc.exit_code if isinstance(exc, LaneExecutionError) else 1
            run_status = exc.run_status if isinstance(exc, LaneExecutionError) else "failed"
            lane = fetch_lane(conn, lane_id)
            task_id = lane["active_task_id"]
            if lane["active_run_id"]:
                run = query_run(conn, lane["active_run_id"])
                if run is not None:
                    handle = RunHandle(
                        run_id=run["run_id"],
                        lane_id=lane_id,
                        task_id=str(task_id or ""),
                        mode=run["mode"],
                        run_dir=run_dir(run["run_id"]),
                        started_at=str(run["started_at"] or now_utc()),
                    )
                    finish_run(conn, handle, run_status, exit_code)
            set_lane_error(conn, lane_id, str(exc), task_id)
            actions.append(
                {
                    "type": "worker_error",
                    "lane_id": lane_id,
                    "task_id": task_id,
                    "error": str(exc),
                }
            )
    for launch in launches:
        actions.append(collect_codex_worker_launch(conn, launch, exec_timeout))
    return actions


def run_evaluator_once(conn: sqlite3.Connection, executor: str, exec_timeout: float) -> dict[str, Any] | None:
    evaluator_lane, task, pair = evaluator_runnable(conn)
    if pair is None or task is None:
        return None
    left, right = pair
    conn.execute(
        """
        UPDATE lanes
        SET status = 'assigned', active_task_id = ?, active_submission_id = NULL, notes = '', updated_at = ?
        WHERE lane_id = 'evaluator'
        """,
        (task["task_id"], now_utc()),
    )
    conn.commit()
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
            payload = codex_evaluator_output(conn, task, left, right, workspace, handle, exec_timeout)
        if payload.get("is_tie"):
            result = handle_tied_evaluation(
                conn,
                task["task_id"],
                left["submission_id"],
                right["submission_id"],
                rationale=str(payload["rationale"]).strip(),
                rematch_brief=str(payload.get("rematch_brief", "")).strip(),
                left_rubric=payload["left_rubric"],
                right_rubric=payload["right_rubric"],
                scorecard_extra=payload.get("meta"),
            )
            finish_run(conn, handle, "completed", 0)
            return {"type": "evaluation_tied", **result}
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
        set_lane_error(conn, "evaluator", str(exc), task["task_id"])
        return {"type": "evaluation_error", "task_id": task["task_id"], "error": str(exc)}


def run_tick(
    conn: sqlite3.Connection,
    executor: str,
    exec_timeout: float,
    *,
    resilience_guard: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if executor == "codex":
        guard = resilience_guard or resilience_execution_guard()
        if guard.get("active"):
            return []
    actions: list[dict[str, Any]] = []
    rebalance_waiting_worker_lanes(conn)
    promote_idle_lanes(conn)
    workers = runnable_worker_lanes(conn)
    concurrent_workers = codex_concurrent_worker_lanes(conn, workers) if executor == "codex" else []
    if concurrent_workers:
        actions.extend(run_codex_workers_concurrently(conn, [lane["lane_id"] for lane in concurrent_workers], exec_timeout))
    else:
        for lane in workers:
            actions.append(run_worker_once(conn, lane["lane_id"], executor, exec_timeout))
    evaluation = run_evaluator_once(conn, executor, exec_timeout)
    if evaluation is not None:
        actions.append(evaluation)
    return actions


def has_runnable_work(conn: sqlite3.Connection) -> bool:
    if conn.execute("SELECT 1 FROM reservations WHERE status = 'queued' LIMIT 1").fetchone() is not None:
        return True
    if runnable_worker_lanes(conn):
        return True
    _lane, _task, pair = evaluator_runnable(conn)
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
    quota_monitor: dict[str, Any] | None = None,
    resilience_guard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "pid": os.getpid(),
        "executor": executor,
        "interval": interval,
        "cycle_count": cycle_count,
        "started_at": started_at,
        "last_heartbeat": last_heartbeat,
        "last_progress": last_progress,
        "last_progress_at": last_heartbeat if last_progress else None,
        "runnable_after": runnable_after,
        "stop_requested": stop_requested,
        "reason": reason,
        "last_actions": compact_daemon_actions(last_actions),
        "quota_monitor": compact_quota_monitor(quota_monitor),
        "resilience_guard": compact_daemon_state_payload({"resilience_guard": resilience_guard or {}}).get("resilience_guard", {}),
    }


def format_status(snapshot: dict[str, Any], task_id: str | None) -> str:
    lines: list[str] = []
    execution_label, execution_reason = execution_state(snapshot)
    phase = current_animation_phase()
    resilience = snapshot.get("resilience") or resilience_summary()
    resilience_guard = snapshot.get("resilience_guard") or resilience_guard_snapshot(resilience)
    asymptote = snapshot.get("asymptote") or asymptote_snapshot()
    lines.append(f"Root: {snapshot['root']}")
    lines.append(f"DB: {snapshot['db_path']}")
    lines.append(f"Executor: {snapshot['executor']}")
    lines.append(f"Execution: {render_state_label(execution_label, phase=phase)} | {execution_reason}")
    lines.append(
        f"Resilience: auto_switch={'on' if resilience.get('auto_switch') else 'off'} "
        f"selected={resilience_current_label(resilience)} reserve_threshold={resilience.get('reserve_percent_threshold', '-')}"
    )
    if resilience_guard.get("active"):
        lines.append(f"Resilience guard: {render_state_label('PAUSED', phase=phase)} | {resilience_guard.get('reason')}")
    lines.append(
        f"Asymptote: {render_state_label(str(asymptote.get('status') or 'OFF'), phase=phase)} "
        f"| {asymptote.get('reason') or '-'}"
    )
    lines.append(
        f"Asymptote pulse: {asymptote.get('progress_text') or '-'} "
        f"| interface={asymptote.get('interface_state') or '-'}"
    )
    if asymptote.get("last_error"):
        lines.append(f"Asymptote warning: {asymptote.get('last_error')}")
    lines.append("")
    lines.append("Lanes:")
    for lane in snapshot["lanes"]:
        queue_desc = ", ".join(
            f"{entry['task_id']}:{entry['reservation_type']}" for entry in lane["queued_reservations"]
        ) or "-"
        notes = lane["notes"] or "-"
        lane_state = lane_display_state(lane)
        lines.append(
            f"- {lane['lane_id']}: state={render_state_label(lane_state, phase=phase)} raw_status={lane['status']} "
            f"active_task={lane['active_task_id'] or '-'} "
            f"active_submission={lane['active_submission_id'] or '-'} active_run={lane['active_run_id'] or '-'} "
            f"queue={queue_desc} notes={notes}"
        )
    lines.append("")
    lines.append("Tasks:")
    tasks = snapshot["tasks"]
    if task_id:
        tasks = [task for task in tasks if task["task_id"] == task_id]
    for task in tasks:
        display_state, display_reason = task_display_state(task, snapshot)
        lines.append(
            f"- {task['task_id']}: status={task['status']} display={render_state_label(display_state, phase=phase)} "
            f"champion={lane_display_name(task['champion_lane_id']) if task['champion_lane_id'] else '-'} "
            f"published={task['published_submission_id'] or '-'} retries={task['challenger_failed_attempts']} "
            f"evals={task['total_evaluations']} swaps={task['role_swaps']} masterpiece={bool(task['masterpiece_locked'])} "
            f"pair_mode={task.get('pair_mode')} judge={evaluator_display_label(task.get('evaluator_tier'))} "
            f"mode={task_mode_label(task.get('task_mode'))} apply={task_apply_status_label(task.get('apply_status'))}"
        )
        lines.append(f"  state: {render_state_label(display_state, phase=phase)} | {display_reason}")
        if task.get("apply_notes"):
            lines.append(f"  apply: {task['apply_notes']}")
    if not tasks:
        lines.append("- none")
    return "\n".join(lines)


def format_dashboard(snapshot: dict[str, Any]) -> str:
    lines: list[str] = []
    daemon_state = snapshot["daemon"].get("state") or {}
    quota_monitor = daemon_state.get("quota_monitor") or {}
    resilience = snapshot.get("resilience") or resilience_summary()
    resilience_guard = snapshot.get("resilience_guard") or resilience_guard_snapshot(resilience)
    asymptote = snapshot.get("asymptote") or asymptote_snapshot()
    execution_label = snapshot.get("execution_state")
    execution_reason = snapshot.get("execution_reason")
    if not execution_label or not execution_reason:
        execution_label, execution_reason = execution_state(snapshot)
    phase = current_animation_phase()
    lines.append("CodexLab Dashboard")
    lines.append(f"Root: {snapshot['root']}")
    lines.append(f"Updated: {snapshot.get('generated_at', '-')}")
    lines.append(
        f"Daemon: running={snapshot['daemon']['running']} reason={daemon_state.get('reason', '-')} "
        f"cycle_count={daemon_state.get('cycle_count', '-')}"
    )
    lines.append(f"Execution: {render_state_label(execution_label, phase=phase)} | {execution_reason}")
    summary = snapshot["summary"]
    lines.append(
        "Summary: "
        f"tasks={summary['task_total']} in_progress={summary['task_in_progress']} "
        f"retrying={summary['task_retrying']} locked={summary['task_locked']} "
        f"lane_busy={summary['lane_busy']} queued={summary['queued_reservations']} "
        f"ready_evals={summary['ready_evaluations']} "
        f"stale_runs={summary['stale_runs']} repairable_lanes={summary['repairable_lanes']}"
    )
    if quota_monitor:
        blocked = ", ".join(lane_display_name(item["lane_id"]) for item in quota_monitor.get("blocked_lanes", [])) or "-"
        identity = quota_monitor.get("login_identity") or {}
        probe = quota_monitor.get("last_probe") or {}
        lines.append(
            f"Quota monitor: status={quota_monitor.get('status', '-')} "
            f"email={identity.get('email') or '-'} blocked_lanes={blocked} "
            f"last_probe_ok={probe.get('ok', '-')}"
        )
        if probe:
            lines.append(
                f"Quota detail: at={quota_monitor.get('last_probe_at', '-')} "
                f"message={probe.get('message', '-')}"
            )
    lines.append(
        f"Resilience: auto_switch={'on' if resilience.get('auto_switch') else 'off'} "
        f"selected={resilience_current_label(resilience)} "
        f"reserve_threshold={resilience.get('reserve_percent_threshold', '-')}"
    )
    if resilience_guard.get("active"):
        lines.append(f"Resilience guard: {render_state_label('PAUSED', phase=phase)} | {resilience_guard.get('reason')}")
    lines.append(
        f"Asymptote: {render_state_label(str(asymptote.get('status') or 'OFF'), phase=phase)} "
        f"| {asymptote.get('reason') or '-'}"
    )
    lines.append(
        f"Horizon: {asymptote.get('progress_text') or '-'} "
        f"| interface={asymptote.get('interface_state') or '-'}"
    )
    if asymptote.get("last_error"):
        lines.append(f"Asymptote warning: {asymptote.get('last_error')}")
    lines.append("")
    lines.append("Corners and Officials:")
    for lane in snapshot["lanes"]:
        queue_desc = queued_entries_display(lane["queued_reservations"]) or "-"
        active_run = lane.get("active_run")
        run_desc = "-"
        if active_run:
            run_desc = (
                f"{active_run['run_id']} {active_run['mode']} status={active_run['status']} "
                f"started={active_run['started_at']}"
            )
        lane_role = lane_display_name(lane["lane_id"])
        lane_state = lane_display_state(lane)
        lines.append(
            f"- {lane_role}: {render_state_label(lane_state, phase=phase)} raw_status={lane['status']} "
            f"active_task={lane['active_task_id'] or '-'} "
            f"active_submission={lane['active_submission_id'] or '-'} run={run_desc}"
        )
        lines.append(f"  Queue: {queue_desc}")
        if lane["lane_id"] == "evaluator":
            ready_eval_desc = ", ".join(lane.get("ready_evaluation_tasks", [])) or "-"
            lines.append(f"  Ready judging: {ready_eval_desc}")
    lines.append("")
    lines.append("Tasks:")
    if not snapshot["tasks"]:
        lines.append("- none")
    else:
        for task in snapshot["tasks"]:
            champion = task["champion_submission"]
            challenger = task["challenger_submission"]
            latest_eval = task["latest_evaluation"]
            queue_desc = queued_entries_display(task["queued_on_lanes"]) or "-"
            status_label = "CHAMPION CONFIRMED" if task["masterpiece_locked"] else task["status"]
            display_state = task.get("display_state")
            display_reason = task.get("display_reason")
            if not display_state or not display_reason:
                display_state, display_reason = task_display_state(task, snapshot)
            lines.append(
                f"- {task['task_id']}: {render_state_label(display_state, phase=phase)} {status_label} "
                f"champion={lane_display_name(task['champion_lane_id']) if task['champion_lane_id'] else '-'} "
                f"challenger={lane_display_name(task['challenger_lane_id']) if task['challenger_lane_id'] else '-'} evals={task['total_evaluations']} "
                f"retries={task['challenger_failed_attempts']} swaps={task['role_swaps']}"
            )
            lines.append(f"  Prompt: {task['prompt_preview']}")
            lines.append(f"  Stage: {task_stage_label(task)}")
            lines.append(f"  Mode: {task_mode_label(task.get('task_mode'))}")
            lines.append(f"  State: {render_state_label(display_state, phase=phase)} | {display_reason}")
            lines.append(f"  Apply: {task_apply_status_label(task.get('apply_status'))}")
            if task.get("apply_notes"):
                lines.append(f"  Apply notes: {task['apply_notes']}")
            lines.append(
                f"  Resolution: duel_stage={task.get('duel_stage')} pair_mode={task.get('pair_mode')} "
                f"judge={evaluator_display_label(task.get('evaluator_tier'))} tier_phase={task.get('tier_phase')}"
            )
            lines.append(f"  Next: {task_next_action(task, snapshot)}")
            lines.append(f"  Crowd reaction: {crowd_reaction(task)}")
            if task.get("rematch_brief"):
                lines.append(f"  Shared notes: {task['rematch_brief']}")
            if champion:
                lines.append(
                    f"  Champion: {champion['submission_id']} {lane_display_name(champion['lane_id'])} "
                    f"score={task['champion_score'] if task['champion_score'] is not None else '-'} "
                    f"summary={champion['summary']}"
                )
                if champion.get("workspace_summary"):
                    lines.append(f"  Champion workspace: {champion['workspace_summary']}")
            else:
                lines.append("  Champion: -")
            if challenger:
                lines.append(
                    f"  Challenger: {challenger['submission_id']} {lane_display_name(challenger['lane_id'])} "
                    f"score={task['challenger_score'] if task['challenger_score'] is not None else '-'} "
                    f"summary={challenger['summary']}"
                )
                if challenger.get("workspace_summary"):
                    lines.append(f"  Challenger workspace: {challenger['workspace_summary']}")
            else:
                lines.append("  Challenger: -")
            lines.append(f"  Queue: {queue_desc}")
            if task["scoreboard"]:
                lines.append("  Scorecards:")
                for entry in task["scoreboard"]:
                    rank = f"#{entry['rank']}" if entry["rank"] is not None else "--"
                    score = f"{entry['latest_score']:.2f}" if entry["latest_score"] is not None else "pending"
                    lines.append(
                        f"  - {rank} {entry['submission_id']} {lane_display_name(entry['lane_id'])} "
                        f"score={score} role={scoreboard_role_label(entry['role'])} momentum={scoreboard_momentum_label(entry['movement'])} "
                        f"summary={entry['summary']}"
                    )
            else:
                lines.append("  Scorecards: -")
            if latest_eval:
                lines.append(
                    f"  Latest Decision: {latest_eval['evaluation_id']} champion={lane_display_name(latest_eval['winner_lane_id'])} "
                    f"champion_score={latest_eval['winner_score']} challenger={lane_display_name(latest_eval['loser_lane_id'])} "
                    f"challenger_score={latest_eval['loser_score']} delta={latest_eval['score_delta']} "
                    f"title_changed={latest_eval['swap_occurred']} judge={evaluator_display_label(latest_eval.get('evaluator_tier'))}"
                )
                lines.append(f"  Rationale: {latest_eval['rationale']}")
                lines.append(f"  Improvement notes: {latest_eval['loser_brief']}")
            else:
                lines.append("  Latest Decision: -")
            if task["masterpiece_locked"]:
                lines.append("  Outcome: CHAMPION CONFIRMED")
    lines.append("")
    lines.append("Recent Activity:")
    if not snapshot.get("recent_events"):
        lines.append("- none")
    else:
        for event in snapshot["recent_events"]:
            lines.append(f"- {event['summary']}")
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
    if Path(REAL_CODEX).exists():
        login_status = subprocess.run(
            [REAL_CODEX, "login", "status"],
            capture_output=True,
            text=True,
            check=False,
            env=codex_exec_env(),
        )
        findings.append(
            {
                "status": "ok" if login_status.returncode == 0 else "warn",
                "message": f"codex login status via {LOGIN_CODEX_HOME} -> {(login_status.stdout or login_status.stderr).strip() or 'unknown'}",
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


def submit_task(conn: sqlite3.Connection, prompt: str, title: str | None = None) -> dict[str, Any]:
    normalized_prompt = prompt.strip()
    if not normalized_prompt:
        raise SystemExit("Task prompt must not be empty")
    task_id = allocate_id(conn, "next_task_id", "T")
    normalized_title = title or textwrap.shorten(normalized_prompt.replace("\n", " "), width=72, placeholder="...")
    task_mode = infer_task_mode(normalized_prompt, normalized_title)
    created_at = now_utc()
    conn.execute(
        """
        INSERT INTO tasks(
            task_id, title, prompt, task_mode, status, created_at, updated_at,
            masterpiece_locked, max_retry_failures, max_total_evaluations, max_role_swaps, apply_status
        )
        VALUES(?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?)
        """,
        (
            task_id,
            normalized_title,
            normalized_prompt,
            task_mode,
            created_at,
            created_at,
            MAX_RETRY_FAILURES,
            MAX_TOTAL_EVALUATIONS,
            MAX_ROLE_SWAPS,
            "pending" if task_mode == "patch" else "not_requested",
        ),
    )
    base = ensure_task_dirs(task_id)
    (base / "brief.md").write_text(
        f"# {task_id}: {normalized_title}\n\n## Task Mode\n\n{task_mode_label(task_mode)}\n\n## Prompt\n\n{normalized_prompt}\n",
        encoding="utf-8",
    )
    for lane_id in WORKER_LANES:
        assign_or_reserve_task(conn, lane_id, task_id, "new_task", "initial task submission")
    conn.execute("UPDATE tasks SET status = 'in_progress', updated_at = ? WHERE task_id = ?", (now_utc(), task_id))
    conn.commit()
    sync_task_state(conn, task_id)
    append_event("task_submitted", task_id=task_id, title=normalized_title, task_mode=task_mode)
    return {"task_id": task_id, "title": normalized_title, "task_mode": task_mode}


def cmd_submit(args: argparse.Namespace) -> int:
    conn = connect()
    created = submit_task(conn, args.prompt, args.title)
    print(f"task_id={created['task_id']}")
    print(f"title={created['title']}")
    print(f"task_mode={created['task_mode']}")
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
    previous_state = read_daemon_state_payload()
    quota_monitor_state = previous_state.get("quota_monitor") or {}
    resilience_guard_state = previous_state.get("resilience_guard") or {}

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
            auto_recover_action: dict[str, Any] | None = None
            refresh_guard = False
            if args.executor == "codex":
                checked_at = parse_timestamp(resilience_guard_state.get("checked_at"))
                now = datetime.now(timezone.utc)
                refresh_guard = (
                    bool(resilience_guard_state.get("active"))
                    and (
                    checked_at is None
                    or (now - checked_at).total_seconds() >= DEFAULT_RESILIENCE_RECHECK_INTERVAL
                    )
                )
            resilience_guard_state = (
                resilience_execution_guard(refresh_usage=refresh_guard)
                if args.executor == "codex"
                else {}
            )
            if args.executor == "codex" and resilience_guard_state.get("active"):
                tick_actions = []
                actions = []
                quota_monitor_state = compact_quota_monitor(quota_monitor_state)
            else:
                quota_monitor_state, auto_recover_action = daemon_quota_monitor(
                    conn,
                    executor=args.executor,
                    exec_timeout=args.exec_timeout,
                    previous=quota_monitor_state,
                )
                tick_actions = run_tick(
                    conn,
                    args.executor,
                    args.exec_timeout,
                    resilience_guard=resilience_guard_state,
                )
                actions = ([auto_recover_action] if auto_recover_action else []) + tick_actions
            runnable_after = has_runnable_work(conn)
            conn.close()
            cycle_count += 1
            heartbeat = now_utc()
            reason = "running"
            if stop_requested:
                reason = "stop_requested"
            elif args.executor == "codex" and resilience_guard_state.get("active"):
                reason = "resilience_paused"
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
                quota_monitor=quota_monitor_state,
                resilience_guard=resilience_guard_state,
            )
            state["exec_timeout"] = args.exec_timeout
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
        final_state = read_daemon_state_payload()
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
            current_pid = read_json(DAEMON_PID_FILE, tolerate_empty=True, tolerate_invalid=True)
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
    snapshot = dashboard_snapshot(conn, args.task_id, events_limit=args.events_limit)
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


def clear_directory_contents(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def stop_daemon_process(timeout: float = 5.0) -> dict[str, Any]:
    snapshot = daemon_runtime_snapshot()
    if not snapshot["running"]:
        return {"was_running": False, "stopped": True, "pid": None}
    pid = int(snapshot["pid"])
    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not process_alive(pid):
            break
        time.sleep(0.1)
    stopped = not process_alive(pid)
    return {"was_running": True, "stopped": stopped, "pid": pid}


def clear_tasks_runtime(*, daemon_timeout: float = 5.0) -> dict[str, Any]:
    ensure_layout()
    daemon_result = stop_daemon_process(daemon_timeout)
    if daemon_result["was_running"] and not daemon_result["stopped"]:
        raise SystemExit(f"Failed to stop daemon pid={daemon_result['pid']} before clearing tasks")

    conn = connect()
    counts = {
        "tasks": conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
        "submissions": conn.execute("SELECT COUNT(*) FROM submissions").fetchone()[0],
        "evaluations": conn.execute("SELECT COUNT(*) FROM evaluations").fetchone()[0],
        "reservations": conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0],
        "runs": conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0],
    }
    conn.execute("DELETE FROM evaluations")
    conn.execute("DELETE FROM submissions")
    conn.execute("DELETE FROM reservations")
    conn.execute("DELETE FROM runs")
    conn.execute("DELETE FROM tasks")
    conn.execute(
        """
        UPDATE lanes
        SET status = 'idle',
            active_task_id = NULL,
            active_submission_id = NULL,
            active_run_id = NULL,
            notes = '',
            updated_at = ?
        """,
        (now_utc(),),
    )
    for key in ("next_task_id", "next_submission_id", "next_evaluation_id", "next_reservation_id", "next_run_id"):
        conn.execute("UPDATE meta SET value = '1' WHERE key = ?", (key,))
    conn.commit()
    conn.close()

    clear_directory_contents(TASKS_DIR)
    clear_directory_contents(RUNS_DIR)
    clear_directory_contents(EVENTS_DIR)
    clear_directory_contents(DAEMON_DIR)
    for lane_id, _lane_type in ALL_LANES:
        workspace_root = lane_paths(lane_id)["workspace"]
        clear_directory_contents(workspace_root / "tasks")
        clear_directory_contents(workspace_root / "worktrees")

    payload = {
        "cleared": True,
        "counts": counts,
        "daemon": daemon_result,
        "root": str(ROOT),
        "tasks_dir": str(TASKS_DIR),
        "runs_dir": str(RUNS_DIR),
    }
    append_event("tasks_cleared", counts=counts)
    return payload


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


def cmd_clear_tasks(args: argparse.Namespace) -> int:
    payload = clear_tasks_runtime(daemon_timeout=args.timeout)
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
        return 0
    print("cleared=True")
    print(f"daemon_was_running={payload['daemon']['was_running']}")
    print(f"daemon_stopped={payload['daemon']['stopped']}")
    print(f"tasks_removed={payload['counts']['tasks']}")
    print(f"submissions_removed={payload['counts']['submissions']}")
    print(f"evaluations_removed={payload['counts']['evaluations']}")
    print(f"reservations_removed={payload['counts']['reservations']}")
    print(f"runs_removed={payload['counts']['runs']}")
    return 0


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
            resume_timeout = (
                args.exec_timeout
                if args.exec_timeout is not None
                else max(float(daemon_state.get("exec_timeout") or 0.0), default_exec_timeout_for(resume_executor))
            )
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
            restart_timeout = (
                args.exec_timeout
                if args.exec_timeout is not None
                else max(float(daemon_state.get("exec_timeout") or 0.0), default_exec_timeout_for(restart_executor))
            )
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


def resolve_live_prompt(args: argparse.Namespace) -> str:
    if args.prompt:
        return str(args.prompt).strip()
    if args.prompt_words:
        return " ".join(args.prompt_words).strip()
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    try:
        return read_prompt_line("codexlab> ", session_name="live").strip()
    except EOFError:
        return ""


def task_finished_in_snapshot(snapshot: dict[str, Any], task_id: str | None) -> bool:
    if not task_id:
        return False
    for task in snapshot.get("tasks", []):
        if task.get("task_id") == task_id:
            return task_is_finished(task)
    return False


def ensure_live_daemon(executor: str, exec_timeout: float, interval: float) -> dict[str, Any]:
    runtime = daemon_runtime_snapshot()
    if runtime["running"]:
        active_executor = (
            runtime.get("state", {}).get("executor")
            or runtime.get("pid_payload", {}).get("executor")
            or executor
        )
        if active_executor != executor:
            raise SystemExit(
                f"Daemon already running with executor={active_executor}; stop it or rerun with --executor {active_executor}"
            )
        return {
            "started": False,
            "already_running": True,
            "pid": runtime["pid"],
            "state_file": runtime["state_file"],
            "log": runtime["log_file"],
        }
    return start_daemon_process(
        executor=executor,
        exec_timeout=exec_timeout,
        interval=interval,
        max_cycles=0,
        until_idle=True,
    )


def watch_loop(
    *,
    task_id: str | None,
    dashboard: bool,
    interval: float,
    once: bool,
    until_finished: bool,
    events_limit: int,
) -> int:
    while True:
        conn = connect()
        snapshot = dashboard_snapshot(conn, task_id, events_limit=events_limit) if dashboard else status_snapshot(conn)
        conn.close()
        if sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print(format_dashboard(snapshot) if dashboard else format_status(snapshot, task_id))
        if once:
            return 0
        if until_finished and task_finished_in_snapshot(snapshot, task_id):
            return 0
        time.sleep(interval)


def watch_stream_loop(
    *,
    task_id: str | None,
    interval: float,
    once: bool,
    until_finished: bool,
    events_limit: int,
) -> int:
    start_offset = events_file_size()
    conn = connect()
    snapshot = dashboard_snapshot(conn, task_id, events_limit=events_limit)
    conn.close()
    if task_id:
        print(f"CodexLab Event Stream | task={task_id}")
    else:
        print("CodexLab Event Stream")
    progress_text = format_progress_stream(snapshot, task_id)
    print(progress_text)
    if once:
        return 0
    if until_finished and task_finished_in_snapshot(snapshot, task_id):
        return 0

    last_progress_text = progress_text
    event_offset = start_offset
    while True:
        event_offset, new_events = read_new_events(event_offset, task_id)
        printed_any = False
        for event in new_events:
            line = format_stream_event(event, task_id=task_id)
            if not line:
                continue
            print(line)
            printed_any = True

        conn = connect()
        snapshot = dashboard_snapshot(conn, task_id, events_limit=events_limit)
        conn.close()
        progress_text = format_progress_stream(snapshot, task_id)
        if progress_text != last_progress_text:
            if printed_any:
                print("")
            print(progress_text)
            last_progress_text = progress_text
            printed_any = True

        if until_finished and task_finished_in_snapshot(snapshot, task_id):
            return 0
        time.sleep(interval)


def cmd_watch(args: argparse.Namespace) -> int:
    if args.dashboard:
        return watch_loop(
            task_id=args.task_id,
            dashboard=True,
            interval=args.interval,
            once=args.once,
            until_finished=args.until_finished,
            events_limit=args.events_limit,
        )
    return watch_stream_loop(
        task_id=args.task_id,
        interval=args.interval,
        once=args.once,
        until_finished=args.until_finished,
        events_limit=args.events_limit,
    )


def cmd_live(args: argparse.Namespace) -> int:
    prompt = resolve_live_prompt(args)
    if not prompt:
        raise SystemExit("Task prompt must not be empty")
    ensure_selected_resilience_profile()
    conn = connect()
    created = submit_task(conn, prompt, args.title)
    conn.close()

    print(f"task_id={created['task_id']}")
    print(f"title={created['title']}")
    daemon_result = ensure_live_daemon(args.executor, args.exec_timeout, args.interval)
    if daemon_result.get("started"):
        print(f"daemon_pid={daemon_result['pid']}")
    elif daemon_result.get("already_running"):
        print(f"daemon_pid={daemon_result['pid']} (reused)")

    if args.no_watch or not sys.stdout.isatty():
        print(
            f"watch_hint=codexlab watch {created['task_id']} --until-finished --interval {args.interval:g}"
        )
        return 0

    if args.dashboard:
        return watch_loop(
            task_id=created["task_id"],
            dashboard=True,
            interval=args.interval,
            once=False,
            until_finished=True,
            events_limit=args.events_limit,
        )
    return watch_stream_loop(
        task_id=created["task_id"],
        interval=args.interval,
        once=False,
        until_finished=True,
        events_limit=args.events_limit,
    )


def format_console_snapshot(
    snapshot: dict[str, Any],
    *,
    focus_task_id: str | None,
    max_tasks: int = 4,
    max_events: int = 6,
) -> str:
    execution_label = snapshot.get("execution_state")
    execution_reason = snapshot.get("execution_reason")
    if not execution_label or not execution_reason:
        execution_label, execution_reason = execution_state(snapshot)
    phase = current_animation_phase()

    lines: list[str] = []
    focus_label = focus_task_id or "all"
    lines.append(f"CodexLab Console | focus={focus_label}")
    lines.append(f"Execution: {render_state_label(execution_label, phase=phase)} | {execution_reason}")
    summary = snapshot.get("summary", {})
    lines.append(
        "Summary: "
        f"tasks={summary.get('task_total', 0)} "
        f"in_progress={summary.get('task_in_progress', 0)} "
        f"queued={summary.get('queued_reservations', 0)} "
        f"ready_evals={summary.get('ready_evaluations', 0)} "
        f"repairable_lanes={summary.get('repairable_lanes', 0)}"
    )
    daemon_state = snapshot.get("daemon", {}).get("state") or {}
    lines.append(
        f"Daemon: running={snapshot.get('daemon', {}).get('running')} "
        f"executor={daemon_state.get('executor', snapshot.get('executor', '-'))} "
        f"reason={daemon_state.get('reason', '-')}"
    )
    quota_monitor = daemon_state.get("quota_monitor") or {}
    quota_status = str(quota_monitor.get("status") or "")
    if quota_monitor and (
        quota_status not in {"", "disabled"}
        or quota_monitor.get("blocked_lanes")
        or quota_monitor.get("last_probe")
    ):
        blocked = ", ".join(lane_display_name(item["lane_id"]) for item in quota_monitor.get("blocked_lanes", [])) or "-"
        identity = quota_monitor.get("login_identity") or {}
        lines.append(
            f"Quota monitor: status={quota_monitor.get('status', '-')} "
            f"email={identity.get('email') or '-'} blocked={blocked}"
        )
    resilience = snapshot.get("resilience") or resilience_summary()
    resilience_guard = snapshot.get("resilience_guard") or resilience_guard_snapshot(resilience)
    asymptote = snapshot.get("asymptote") or asymptote_snapshot()
    lines.append(
        f"Resilience: auto_switch={'on' if resilience.get('auto_switch') else 'off'} "
        f"selected={resilience_current_label(resilience)} "
        f"reserve_threshold={resilience.get('reserve_percent_threshold', '-')}"
    )
    if resilience_guard.get("active"):
        lines.append(f"Resilience guard: {render_state_label('PAUSED', phase=phase)} | {resilience_guard.get('reason')}")
    asymptote_owner_pid = int(asymptote.get("owner_pid") or 0)
    asymptote_summary = str(asymptote.get("reason") or "-")
    if str(asymptote.get("status") or "OFF").upper() != "OFF":
        if asymptote_owner_pid and asymptote_owner_pid != os.getpid():
            asymptote_summary = "separate console active"
        elif asymptote_owner_pid == os.getpid():
            asymptote_summary = "attached to this console"
    lines.append(
        f"Asymptote: {render_state_label(str(asymptote.get('status') or 'OFF'), phase=phase)} "
        f"| {asymptote_summary}"
    )
    lines.append(f"Profiles: {resilience_counts_display(resilience.get('counts') or {})}")
    for profile in list(resilience.get("profiles") or [])[:4]:
        current_marker = " current" if profile.get("is_current") else ""
        lines.append(
            f"- {profile.get('account_key')} alias={profile.get('alias') or '-'} "
            f"status={profile.get('status')}{current_marker}"
        )

    lines.append("")
    lines.append("Corners and Officials:")
    for lane in snapshot.get("lanes", []):
        queue_desc = queued_entries_display(lane.get("queued_reservations", [])) or "-"
        active_run = lane.get("active_run")
        run_desc = "-"
        if active_run:
            run_desc = f"{active_run['run_id']} {active_run['mode']} {active_run['status']}"
        lane_state = lane_display_state(lane)
        lines.append(
            f"- {render_state_label(lane_state, phase=phase)} {lane_display_name(lane['lane_id'])}: "
            f"raw_status={lane['status']} task={lane['active_task_id'] or '-'} "
            f"submission={lane['active_submission_id'] or '-'} run={run_desc} queue={queue_desc}"
        )

    tasks = list(snapshot.get("tasks", []))
    if focus_task_id:
        tasks = [task for task in tasks if task.get("task_id") == focus_task_id]
    else:
        unfinished = [task for task in tasks if not task_is_finished(task)]
        finished = [task for task in tasks if task_is_finished(task)]
        tasks = (unfinished + finished)[:max_tasks]

    lines.append("")
    lines.append("Tasks:")
    if not tasks:
        lines.append("- none")
    else:
        for task in tasks:
            state_label = task.get("display_state")
            state_reason = task.get("display_reason")
            if not state_label or not state_reason:
                state_label, state_reason = task_display_state(task, snapshot)
            lines.append(f"- {task['task_id']} [{render_state_label(state_label, phase=phase)}] {task_stage_label(task)}")
            lines.append(f"  mode: {task_mode_label(task.get('task_mode'))} | apply: {task_apply_status_label(task.get('apply_status'))}")
            lines.append(f"  next: {task_next_action(task, snapshot)}")
            lines.append(f"  state: {render_state_label(state_label, phase=phase)} | {state_reason}")
            if task.get("apply_notes"):
                lines.append(f"  apply: {task['apply_notes']}")
            lines.append(f"  crowd: {crowd_reaction(task)}")
            scoreboard = task_scoreboard_summary(task)
            if scoreboard != "-":
                lines.append(f"  scorecards: {scoreboard}")

    lines.append("")
    lines.append("Recent:")
    recent = snapshot.get("recent_events", [])
    if focus_task_id:
        recent = [event for event in recent if event.get("task_id") == focus_task_id]
    rendered_events = 0
    for event in recent:
        line = format_stream_event(event, task_id=focus_task_id)
        if not line:
            continue
        lines.append(f"- {line}")
        rendered_events += 1
        if rendered_events >= max_events:
            break
    if rendered_events == 0:
        lines.append("- no recent events")
    return "\n".join(lines)


def wrap_console_lines(text: str, width: int) -> list[str]:
    usable_width = max(width, 1)
    wrapped: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n")
        if not line:
            wrapped.append("")
            continue
        while len(line) > usable_width:
            wrapped.append(line[:usable_width])
            line = line[usable_width:]
        wrapped.append(line)
    if not wrapped:
        wrapped.append("")
    return wrapped


def console_panel_width() -> int:
    columns = shutil.get_terminal_size((100, 24)).columns
    return max(48, min(columns - 2, 108))


def console_prompt_width() -> int:
    columns = shutil.get_terminal_size((100, 24)).columns
    return max(48, columns)


def ascii_panel_border(title: str, width: int) -> str:
    label = f"| {title} |"
    if len(label) >= width:
        return label[:width]
    remaining = width - len(label)
    left = remaining // 2
    right = remaining - left
    return f"{'-' * left}{label}{'-' * right}"


def render_ascii_panel(title: str, body: str, *, width: int | None = None) -> str:
    panel_width = max(width or console_panel_width(), 24)
    inner_width = max(panel_width - 4, 1)
    lines = [ascii_panel_border(title, panel_width)]
    for raw_line in wrap_console_lines(body, inner_width):
        lines.append(f"| {raw_line.ljust(inner_width)} |")
    lines.append("-" * panel_width)
    return "\n".join(lines)


CONSOLE_PROMPT_STYLE = (
    Style.from_dict(
        {
            "bottom-toolbar": "noreverse bg:default fg:default",
            "bottom-toolbar.text": "noreverse bg:default fg:default",
            "rprompt": "noreverse bg:default fg:default",
        }
    )
    if Style is not None
    else None
)


def trim_console_input(input_buffer: str, width: int) -> str:
    usable_width = max(width, 1)
    if len(input_buffer) <= usable_width:
        return input_buffer
    if usable_width <= 3:
        return input_buffer[-usable_width:]
    return "..." + input_buffer[-(usable_width - 3) :]


def format_console_screen(
    snapshot: dict[str, Any],
    *,
    focus_task_id: str | None,
    status_message: str,
    input_buffer: str,
    width: int,
    height: int,
) -> list[str]:
    body = format_console_snapshot(snapshot, focus_task_id=focus_task_id)
    body_lines = wrap_console_lines(body, width)
    help_line = "Enter submits a task | /focus T-0001 | /status | /profile ... | /auto-switch on|off | /asymptote on|off | /sync | /useage [all] | /run ... | /quit"
    status_line = f"Status: {status_message}" if status_message else "Status: ready"
    prompt_prefix = "Prompt> "
    prompt_line = f"{prompt_prefix}{trim_console_input(input_buffer, width - len(prompt_prefix))}"
    footer_lines = wrap_console_lines(help_line, width) + wrap_console_lines(status_line, width) + wrap_console_lines(prompt_line, width)
    available_body_lines = max(height - len(footer_lines), 0)
    return body_lines[:available_body_lines] + footer_lines


def console_input_timeout_ms(*, now_monotonic: float, last_refresh_monotonic: float, interval: float, snapshot_loaded: bool) -> int:
    if not snapshot_loaded:
        return 0
    remaining = max(float(interval) - (now_monotonic - last_refresh_monotonic), 0.0)
    if remaining <= 0:
        return 0
    return max(1, min(int(remaining * 1000), 5000))


def console_startup_daemon(
    snapshot: dict[str, Any],
    *,
    executor: str,
    exec_timeout: float,
    interval: float,
) -> str:
    summary = snapshot.get("summary", {})
    if snapshot.get("daemon", {}).get("running"):
        return ""
    if summary.get("task_in_progress", 0) <= 0 and summary.get("queued_reservations", 0) <= 0:
        return ""
    result = ensure_live_daemon(executor, exec_timeout, interval)
    if result.get("started"):
        return f"resumed queued work with daemon {result['pid']}"
    if result.get("already_running"):
        return f"reused daemon {result['pid']}"
    return ""


def console_live_panel_text(
    focus_task_id: str | None,
    *,
    status_message: str = "",
    events_limit: int = 6,
) -> str:
    try:
        conn = connect()
        snapshot = dashboard_snapshot(conn, focus_task_id, events_limit=events_limit)
        live_body = format_console_snapshot(snapshot, focus_task_id=focus_task_id, max_events=max(1, events_limit))
    except Exception:
        live_body = "live dashboard unavailable"
    finally:
        try:
            conn.close()
        except Exception:
            pass

    panel_width = console_panel_width()
    prompt_width = console_prompt_width()
    live_lines = [
        live_body,
        "",
        "Commands: /focus T-0001 | /all | /status | /refresh | /clear-tasks | /profile ... | /auto-switch on|off | /asymptote on|off | /sync | /useage [all] | /run ... | /quit",
        f"Status: {status_message or 'ready'}",
    ]
    prompt_header = ascii_panel_border("prompt", prompt_width)
    return "\n\n".join([render_ascii_panel("live panel", "\n".join(live_lines), width=panel_width), prompt_header])


def console_prompt_bottom_border() -> str:
    return "-" * console_prompt_width()


def console_live_toolbar_text(
    focus_task_id: str | None,
    *,
    status_message: str = "",
    events_limit: int = 6,
) -> str:
    return console_live_panel_text(
        focus_task_id,
        status_message=status_message,
        events_limit=events_limit,
    )


def format_asymptote_console_snapshot(payload: dict[str, Any], *, max_lines: int = 8) -> str:
    phase = current_animation_phase()
    lines = [
        "Asymptote Console",
        f"Execution: {render_state_label(str(payload.get('status') or 'OFF'), phase=phase)} | {payload.get('reason') or '-'}",
        f"Horizon: {payload.get('progress_text') or '-'}",
        f"Interface: {payload.get('interface_state') or '-'}",
        f"Human anchor: {payload.get('human_anchor') or '-'}",
        f"AI anchor: {payload.get('ai_anchor') or '-'}",
        f"Letters: {payload.get('letters_anchor') or '-'}",
        f"Files: {USER_PREFS_FILE.name}, {AI_PREFS_FILE.name}, {LETTERS_FILE.name}",
    ]
    if payload.get("last_error"):
        lines.append(f"Warning: {payload.get('last_error')}")
    recent = [event for event in recent_events(max_lines) if str(event.get("type", "")).startswith("asymptote_")]
    lines.append("")
    lines.append("Recent:")
    if not recent:
        lines.append("- no recent asymptote events")
    else:
        rendered = 0
        for event in recent:
            summary = format_stream_event(event, task_id=None)
            if not summary:
                continue
            lines.append(f"- {summary}")
            rendered += 1
            if rendered >= max_lines:
                break
        if rendered == 0:
            lines.append("- no recent asymptote events")
    return "\n".join(lines)


def asymptote_console_panel_text(*, status_message: str = "", events_limit: int = 6) -> str:
    payload = asymptote_snapshot()
    panel_width = console_panel_width()
    prompt_width = console_prompt_width()
    live_lines = [
        format_asymptote_console_snapshot(payload, max_lines=max(1, events_limit)),
        "",
        "Commands: /status | /sync | /asymptote off | /quit",
        f"Status: {status_message or 'ready'}",
    ]
    prompt_header = ascii_panel_border("prompt", prompt_width)
    return "\n\n".join([render_ascii_panel("asymptote", "\n".join(live_lines), width=panel_width), prompt_header])


def print_resilience_profiles(summary: dict[str, Any]) -> None:
    print("")
    print("Registered profiles:")
    for profile in summary.get("profiles", []):
        current_marker = " current" if profile.get("is_current") else ""
        print(
            f"- {profile.get('account_key')}: alias={profile.get('alias') or '-'} "
            f"status={profile.get('status')}{current_marker}"
        )
    if not summary.get("profiles"):
        print("- none")


def usage_state_label(profile: dict[str, Any]) -> str:
    state = str(profile.get("usage_state") or "unknown")
    return {
        "available": "available",
        "quota_blocked": "quota_blocked",
        "unknown": "unknown",
    }.get(state, state)


def format_usage_remaining(profile: dict[str, Any]) -> str:
    remaining = profile.get("usage_percent_remaining")
    if isinstance(remaining, (int, float)):
        if float(remaining).is_integer():
            return f"{int(remaining)}% left"
        return f"{remaining:.1f}% left"
    return "unknown"


def format_usage_reset(profile: dict[str, Any]) -> str:
    resets_at = profile.get("usage_resets_at")
    if not isinstance(resets_at, (int, float)):
        return "-"
    return datetime.fromtimestamp(float(resets_at), timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M %Z")


def probe_resilience_usage(*, all_profiles: bool) -> dict[str, Any]:
    vault = credential_vault()
    payload = vault.load()
    accounts = payload.get("accounts") or {}
    account_keys: list[str]
    if all_profiles:
        account_keys = sorted(accounts)
    else:
        current_key = payload.get("current_account_key")
        account_keys = [current_key] if current_key else []
    jobs: list[tuple[str, dict[str, Any]]] = []
    for account_key in account_keys:
        if not account_key or account_key not in accounts:
            continue
        auth_data = accounts[account_key].get("auth_data")
        if isinstance(auth_data, dict) and auth_data:
            jobs.append((account_key, auth_data))
    if not jobs:
        return vault.summary()

    results: dict[str, dict[str, Any]] = {}
    worker_count = usage_probe_worker_count(len(jobs))
    if worker_count <= 1:
        for account_key, auth_data in jobs:
            try:
                results[account_key] = probe_auth_rate_limits(
                    auth_data,
                    codex_bin=REAL_CODEX,
                    scratch_root=RESILIENCE_SCRATCH_DIR,
                    timeout_seconds=8.0,
                )
            except Exception:
                continue
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    probe_auth_rate_limits,
                    auth_data,
                    codex_bin=REAL_CODEX,
                    scratch_root=RESILIENCE_SCRATCH_DIR,
                    timeout_seconds=8.0,
                ): account_key
                for account_key, auth_data in jobs
            }
            for future in concurrent.futures.as_completed(futures):
                account_key = futures[future]
                try:
                    results[account_key] = future.result()
                except Exception:
                    continue
    for account_key, _auth_data in jobs:
        usage_summary = results.get(account_key)
        if usage_summary is None:
            continue
        try:
            vault.record_usage_probe(account_key, usage_summary)
        except Exception:
            continue
    return vault.summary()


def print_resilience_usage(summary: dict[str, Any], *, all_profiles: bool) -> None:
    profiles = list(summary.get("profiles") or [])
    if not all_profiles:
        profiles = [profile for profile in profiles if profile.get("is_current")]
    print("")
    print("Usage status:")
    if not profiles:
        print("- no matching profile")
        return
    for profile in profiles:
        print(
            f"- {profile.get('account_key')}: "
            f"email={profile.get('email') or '-'} "
            f"({format_usage_remaining(profile)})"
        )


def handle_resilience_console_command(
    command: str,
    *,
    allow_run: bool = True,
    allow_profile_login: bool = True,
    prefer_external_asymptote: bool = False,
) -> str | None:
    text = command.strip()
    if not text.startswith(("/profile", "/auto-switch", "/sync", "/run", "/useage", "/usage", "/asymptote")):
        return None
    try:
        parts = parse_shell_command(text)
    except VaultError as exc:
        return f"parse error: {exc}"
    if not parts:
        return None
    vault = credential_vault()
    cmd_name = parts[0]
    try:
        if cmd_name == "/profile":
            if len(parts) < 2:
                return "usage: /profile register [alias] | list | current | activate <account> | disable <account> | enable <account> | renumber | reset-exhausted"
            action = parts[1]
            if action == "register":
                if len(parts) > 3:
                    return "usage: /profile register [alias]"
                if not allow_profile_login:
                    return "/profile register is only available in the shell-style console"
                alias = parts[2] if len(parts) == 3 else None
                account_key = run_resilience_profile_login(alias)
                return f"registered {account_key}"
            if action == "list":
                print_resilience_profiles(vault.summary())
                return f"profiles={len(vault.summary().get('profiles', []))}"
            if action == "current":
                summary = vault.summary()
                print("")
                print(
                    "Current profile: "
                    f"{resilience_current_label(summary)} auto_switch={'on' if summary.get('auto_switch') else 'off'}"
                )
                return "current profile shown"
            if action == "activate":
                if len(parts) < 3:
                    return "usage: /profile activate <account_key|alias>"
                resolved_key = vault.resolve_account_ref(parts[2])
                vault.activate(resolved_key)
                return f"activated {resolved_key}"
            if action == "disable":
                if len(parts) < 3:
                    return "usage: /profile disable <account_key|alias>"
                resolved_key = vault.resolve_account_ref(parts[2])
                vault.disable(resolved_key)
                return f"disabled {resolved_key}"
            if action == "enable":
                if len(parts) < 3:
                    return "usage: /profile enable <account_key|alias>"
                resolved_key = vault.resolve_account_ref(parts[2])
                vault.enable(resolved_key)
                return f"enabled {resolved_key}"
            if action == "renumber":
                mapping = vault.renumber_aliases()
                return f"renumbered profiles={len(mapping)}"
            if action == "reset-exhausted":
                changed = vault.reset_exhausted()
                return f"reset exhausted profiles={changed}"
            return "unknown /profile action"
        if cmd_name == "/auto-switch":
            if len(parts) != 2 or parts[1] not in {"on", "off"}:
                return "usage: /auto-switch <on|off>"
            enabled = vault.set_auto_switch(parts[1] == "on")
            return f"auto-switch={'on' if enabled else 'off'}"
        if cmd_name == "/asymptote":
            if len(parts) != 2 or parts[1] not in {"on", "off"}:
                return "usage: /asymptote <on|off>"
            if parts[1] == "on" and prefer_external_asymptote and external_asymptote_console_supported():
                payload = asymptote_snapshot()
                return spawn_asymptote_console(activate=not bool(payload.get("active")))
            result = asymptote_controller().start() if parts[1] == "on" else asymptote_controller().stop()
            return result.message
        if cmd_name == "/sync":
            synced = vault.sync_auth()
            fingerprint = synced.get("fingerprint") or {}
            message = f"sync complete account_id={fingerprint.get('account_id') or '-'}"
            asymptote = asymptote_snapshot()
            if asymptote.get("active"):
                pulse_result = asymptote_controller().sync_now()
                if pulse_result.ok:
                    message += " | asymptote pulse triggered"
                elif pulse_result.message != "Asymptote inactive":
                    message += f" | {pulse_result.message}"
            return message
        if cmd_name in {"/useage", "/usage"}:
            if len(parts) == 1:
                print_resilience_usage(probe_resilience_usage(all_profiles=False), all_profiles=False)
                return "usage shown for current profile"
            if len(parts) == 2 and parts[1] == "all":
                print_resilience_usage(probe_resilience_usage(all_profiles=True), all_profiles=True)
                return "usage shown for all profiles"
            return "usage: /useage [all]"
        if cmd_name == "/run":
            if not allow_run:
                return "/run is only available in the shell-style console"
            if len(parts) < 2:
                return "usage: /run <command...>"
            print("")
            print(f"Running: {' '.join(parts[1:])}")
            result = ResilientRunner(vault).execute(
                parts[1:],
                auto_switch=vault.auto_switch_enabled(),
                stdout_consumer=lambda chunk: print(chunk, end="", flush=True),
                stderr_consumer=lambda chunk: print(chunk, end="", file=sys.stderr, flush=True),
            )
            return (
                f"run exit={result.completed.returncode} "
                f"attempts={result.attempts} rotations={len(result.rotations)}"
            )
    except (VaultError, FileNotFoundError, subprocess.SubprocessError) as exc:
        return f"resilience error: {exc}"
    return None


def handle_console_command(
    command: str,
    *,
    focus_task_id: str | None,
) -> tuple[str, str | None, bool]:
    text = command.strip()
    if text in {"/quit", "/exit"}:
        return "leaving console", focus_task_id, True
    if text == "/status":
        return "status refreshed", focus_task_id, False
    if text == "/refresh":
        return "refreshed", focus_task_id, False
    if text == "/all":
        return "showing all tasks", None, False
    if text == "/clear-tasks":
        return "cleared tasks", None, False
    if text.startswith("/focus "):
        requested = text.split(None, 1)[1].strip()
        if requested:
            return f"focused {requested}", requested, False
        return "usage: /focus T-0001", focus_task_id, False
    if text == "/help":
        return (
            "commands: /focus T-0001, /all, /status, /refresh, /clear-tasks, /profile ..., "
            "/auto-switch on|off, /asymptote on|off, /sync, /useage [all], /run ..., /quit",
            focus_task_id,
            False,
        )
    return "unknown command; use /help", focus_task_id, False


def cmd_console(args: argparse.Namespace) -> int:
    ensure_layout()
    ensure_selected_resilience_profile()
    focus_task_id = args.task_id
    selected_profile = resilience_summary().get("current_account_key")
    status_message = (
        f"profile {selected_profile} ready | type a task and press Enter"
        if selected_profile
        else "type a task and press Enter"
    )
    live_panel_mode = advanced_prompt_available()

    try:
        while True:
            conn = connect()
            snapshot = dashboard_snapshot(conn, focus_task_id, events_limit=args.events_limit)
            conn.close()
            startup_message = console_startup_daemon(
                snapshot,
                executor=args.executor,
                exec_timeout=args.exec_timeout,
                interval=args.interval,
            )
            if startup_message:
                status_message = startup_message
                conn = connect()
                snapshot = dashboard_snapshot(conn, focus_task_id, events_limit=args.events_limit)
                conn.close()

            if not live_panel_mode:
                print(format_console_snapshot(snapshot, focus_task_id=focus_task_id))
                print("")
                print("Commands: /focus T-0001 | /all | /status | /refresh | /clear-tasks | /profile ... | /auto-switch on|off | /asymptote on|off | /sync | /run ... | /quit")
                print(f"Status: {status_message or 'ready'}")
                print("")

            try:
                text = read_prompt_line(
                    "",
                    session_name="console",
                    refresh_interval=max(float(args.interval), 0.25),
                    message=(
                        (lambda: console_live_panel_text(focus_task_id, status_message=status_message, events_limit=args.events_limit))
                        if live_panel_mode
                        else None
                    ),
                    bottom_toolbar=(console_prompt_bottom_border if live_panel_mode else None),
                    style=(CONSOLE_PROMPT_STYLE if live_panel_mode else None),
                    completer=(CONSOLE_SLASH_COMPLETER if live_panel_mode else None),
                    complete_while_typing=live_panel_mode,
                )
            except EOFError:
                return 0
            except KeyboardInterrupt:
                print("")
                return 0

            text = text.strip()
            if not text:
                status_message = "refreshed"
                continue
            if text.startswith("/"):
                if text == "/clear-tasks":
                    payload = clear_tasks_runtime()
                    focus_task_id = None
                    status_message = (
                        f"cleared tasks | tasks={payload['counts']['tasks']} "
                        f"runs={payload['counts']['runs']}"
                    )
                    continue
                resilience_message = handle_resilience_console_command(text, prefer_external_asymptote=True)
                if resilience_message is not None:
                    status_message = resilience_message
                    continue
                status_message, focus_task_id, should_exit = handle_console_command(
                    text,
                    focus_task_id=focus_task_id,
                )
                if should_exit:
                    return 0
                continue

            conn = connect()
            created = submit_task(conn, text, None)
            conn.close()
            daemon_result = ensure_live_daemon(args.executor, args.exec_timeout, args.interval)
            daemon_bits = []
            if daemon_result.get("started"):
                daemon_bits.append(f"daemon {daemon_result['pid']} started")
            elif daemon_result.get("already_running"):
                daemon_bits.append(f"daemon {daemon_result['pid']} reused")
            focus_task_id = created["task_id"]
            status_message = f"submitted {created['task_id']}" + (f" | {'; '.join(daemon_bits)}" if daemon_bits else "")
    finally:
        stop_owned_asymptote_engine()


def cmd_asymptote_console(args: argparse.Namespace) -> int:
    ensure_layout()
    status_message = "asymptote console ready"
    if args.activate:
        result = asymptote_controller().start()
        status_message = result.message
    elif args.attach:
        status_message = "attached to existing asymptote interface"

    live_panel_mode = advanced_prompt_available()
    try:
        while True:
            payload = asymptote_snapshot()

            if not live_panel_mode:
                print(format_asymptote_console_snapshot(payload))
                print("")
                print("Commands: /status | /sync | /asymptote off | /quit")
                print(f"Status: {status_message or 'ready'}")
                print("")

            try:
                text = read_prompt_line(
                    "",
                    session_name="asymptote-console",
                    refresh_interval=1.0,
                    message=((lambda: asymptote_console_panel_text(status_message=status_message, events_limit=args.events_limit)) if live_panel_mode else None),
                    bottom_toolbar=(console_prompt_bottom_border if live_panel_mode else None),
                    style=(CONSOLE_PROMPT_STYLE if live_panel_mode else None),
                )
            except EOFError:
                return 0
            except KeyboardInterrupt:
                print("")
                return 0

            text = text.strip()
            if not text or text == "/status":
                status_message = "status refreshed"
                continue
            if text in {"/quit", "/exit"}:
                return 0
            if text == "/sync":
                result = asymptote_controller().sync_now()
                status_message = result.message
                continue
            if text == "/asymptote off":
                result = asymptote_controller().stop()
                status_message = result.message
                if result.ok:
                    return 0
                continue
            if text.startswith("/"):
                status_message = "commands: /status, /sync, /asymptote off, /quit"
                continue
            result = asymptote_controller().record_human_note(text)
            status_message = result.message
    finally:
        stop_owned_asymptote_engine()


def cmd_tui(args: argparse.Namespace) -> int:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise SystemExit("tui requires an interactive terminal")
    try:
        import curses
    except ImportError as exc:
        raise SystemExit(f"tui requires curses support: {exc}") from exc

    ensure_layout()
    ensure_selected_resilience_profile()

    def _run(stdscr: Any) -> int:
        try:
            curses.curs_set(1)
        except curses.error:
            pass
        stdscr.keypad(True)

        focus_task_id = args.task_id
        input_buffer = ""
        status_message = "type a task and press Enter"
        last_refresh = 0.0
        snapshot: dict[str, Any] = {}
        dirty = True

        while True:
            now = time.monotonic()
            if not snapshot or (now - last_refresh) >= args.interval:
                conn = connect()
                snapshot = dashboard_snapshot(conn, focus_task_id, events_limit=args.events_limit)
                conn.close()
                last_refresh = now
                dirty = True
                startup_message = console_startup_daemon(
                    snapshot,
                    executor=args.executor,
                    exec_timeout=args.exec_timeout,
                    interval=args.interval,
                )
                if startup_message:
                    status_message = startup_message
                    conn = connect()
                    snapshot = dashboard_snapshot(conn, focus_task_id, events_limit=args.events_limit)
                    conn.close()
                    last_refresh = time.monotonic()
                    dirty = True

            if dirty:
                height, width = stdscr.getmaxyx()
                stdscr.erase()
                lines = format_console_screen(
                    snapshot,
                    focus_task_id=focus_task_id,
                    status_message=status_message,
                    input_buffer=input_buffer,
                    width=max(width - 1, 1),
                    height=max(height, 1),
                )
                for row, line in enumerate(lines[:height]):
                    try:
                        stdscr.addnstr(row, 0, line, max(width - 1, 1))
                    except curses.error:
                        pass

                prompt_y = max(min(len(lines) - 1, height - 1), 0)
                cursor_x = min(len("Prompt> ") + len(trim_console_input(input_buffer, max(width - len("Prompt> ") - 1, 1))), max(width - 1, 0))
                try:
                    stdscr.move(prompt_y, cursor_x)
                except curses.error:
                    pass
                stdscr.refresh()
                dirty = False

            stdscr.timeout(
                console_input_timeout_ms(
                    now_monotonic=time.monotonic(),
                    last_refresh_monotonic=last_refresh,
                    interval=args.interval,
                    snapshot_loaded=bool(snapshot),
                )
            )

            try:
                key = stdscr.get_wch()
            except curses.error:
                continue

            if isinstance(key, str):
                if key in {"\n", "\r"}:
                    text = input_buffer.strip()
                    input_buffer = ""
                    if not text:
                        status_message = "refreshed"
                        snapshot = {}
                        dirty = True
                        continue
                    if text.startswith("/"):
                        if text == "/clear-tasks":
                            payload = clear_tasks_runtime()
                            focus_task_id = None
                            status_message = (
                                f"cleared tasks | tasks={payload['counts']['tasks']} "
                                f"runs={payload['counts']['runs']}"
                            )
                            snapshot = {}
                            dirty = True
                            continue
                        resilience_message = handle_resilience_console_command(
                            text,
                            allow_run=False,
                            allow_profile_login=False,
                            prefer_external_asymptote=False,
                        )
                        if resilience_message is not None:
                            status_message = resilience_message
                            snapshot = {}
                            dirty = True
                            continue
                        status_message, focus_task_id, should_exit = handle_console_command(
                            text,
                            focus_task_id=focus_task_id,
                        )
                        if should_exit:
                            return 0
                        snapshot = {}
                        dirty = True
                        continue
                    conn = connect()
                    created = submit_task(conn, text, None)
                    conn.close()
                    daemon_result = ensure_live_daemon(args.executor, args.exec_timeout, args.interval)
                    daemon_bits = []
                    if daemon_result.get("started"):
                        daemon_bits.append(f"daemon {daemon_result['pid']} started")
                    elif daemon_result.get("already_running"):
                        daemon_bits.append(f"daemon {daemon_result['pid']} reused")
                    focus_task_id = created["task_id"]
                    status_message = f"submitted {created['task_id']}" + (f" | {'; '.join(daemon_bits)}" if daemon_bits else "")
                    snapshot = {}
                    dirty = True
                    continue
                if key in {"\b", "\x7f"}:
                    input_buffer = input_buffer[:-1]
                    dirty = True
                    continue
                if key == "\x1b":
                    input_buffer = ""
                    status_message = "cleared input"
                    dirty = True
                    continue
                if key.isprintable():
                    input_buffer += key
                    dirty = True
                    continue
            else:
                if key == curses.KEY_BACKSPACE:
                    input_buffer = input_buffer[:-1]
                    dirty = True
                    continue
                if key == curses.KEY_RESIZE:
                    dirty = True
                    continue

        return 0

    try:
        return int(curses.wrapper(_run))
    except KeyboardInterrupt:
        return 0
    finally:
        stop_owned_asymptote_engine()


def cmd_codex(args: argparse.Namespace) -> int:
    ensure_layout()
    ensure_live_codex_home_auth()
    env = os.environ.copy()
    env["CODEX_HOME"] = str(LAB_HOME)
    command = [
        REAL_CODEX,
        "--cd",
        str(ROOT),
        "--sandbox",
        LIVE_CODEX_SANDBOX,
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

    console = subparsers.add_parser("console", help="Open the shell-style dashboard + prompt console")
    console.add_argument("--task-id")
    console.add_argument("--executor", choices=("mock", "codex"), default=LIVE_DEFAULT_EXECUTOR)
    console.add_argument("--exec-timeout", type=float, default=default_exec_timeout_for(LIVE_DEFAULT_EXECUTOR))
    console.add_argument("--interval", type=float, default=DEFAULT_WATCH_INTERVAL)
    console.add_argument("--events-limit", type=int, default=DEFAULT_EVENTS_LIMIT)
    console.set_defaults(func=cmd_console)

    live = subparsers.add_parser("live", help="Submit a task like stock codex and follow the live event stream")
    live.add_argument("--title")
    live.add_argument("--prompt")
    live.add_argument("--executor", choices=("mock", "codex"), default=LIVE_DEFAULT_EXECUTOR)
    live.add_argument("--exec-timeout", type=float, default=default_exec_timeout_for(LIVE_DEFAULT_EXECUTOR))
    live.add_argument("--interval", type=float, default=DEFAULT_WATCH_INTERVAL)
    live.add_argument("--events-limit", type=int, default=DEFAULT_EVENTS_LIMIT)
    live.add_argument("--dashboard", action="store_true", help="Use the full screen dashboard instead of the event stream")
    live.add_argument("--no-watch", action="store_true")
    live.add_argument("prompt_words", nargs="*")
    live.set_defaults(func=cmd_live)

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
    tick.add_argument("--exec-timeout", type=float, default=default_exec_timeout_for(DEFAULT_EXECUTOR))
    tick.add_argument("--json", action="store_true")
    tick.set_defaults(func=cmd_tick)

    run_loop = subparsers.add_parser("run-loop", help="Run the scheduler for multiple ticks")
    run_loop.add_argument("--executor", choices=("mock", "codex"), default=DEFAULT_EXECUTOR)
    run_loop.add_argument("--exec-timeout", type=float, default=default_exec_timeout_for(DEFAULT_EXECUTOR))
    run_loop.add_argument("--interval", type=float, default=0.0)
    run_loop.add_argument("--max-ticks", type=int, default=20)
    run_loop.add_argument("--until-idle", action="store_true")
    run_loop.add_argument("--json", action="store_true")
    run_loop.set_defaults(func=cmd_run_loop)

    daemon = subparsers.add_parser("daemon", help="Manage the background scheduler")
    daemon_subparsers = daemon.add_subparsers(dest="daemon_command", required=True)

    daemon_run = daemon_subparsers.add_parser("run", help="Run the scheduler in the foreground")
    daemon_run.add_argument("--executor", choices=("mock", "codex"), default=DEFAULT_EXECUTOR)
    daemon_run.add_argument("--exec-timeout", type=float, default=default_exec_timeout_for(DEFAULT_EXECUTOR))
    daemon_run.add_argument("--interval", type=float, default=1.0)
    daemon_run.add_argument("--max-cycles", type=int, default=0)
    daemon_run.add_argument("--until-idle", action="store_true")
    daemon_run.add_argument("--json", action="store_true")
    daemon_run.set_defaults(func=cmd_daemon_run)

    daemon_start = daemon_subparsers.add_parser("start", help="Start the scheduler in the background")
    daemon_start.add_argument("--executor", choices=("mock", "codex"), default=DEFAULT_EXECUTOR)
    daemon_start.add_argument("--exec-timeout", type=float, default=default_exec_timeout_for(DEFAULT_EXECUTOR))
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
    dashboard.add_argument("--events-limit", type=int, default=DEFAULT_EVENTS_LIMIT)
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

    clear_tasks = subparsers.add_parser("clear-tasks", help="Stop the daemon and clear runtime task history for a fresh queue")
    clear_tasks.add_argument("--timeout", type=float, default=5.0)
    clear_tasks.add_argument("--json", action="store_true")
    clear_tasks.set_defaults(func=cmd_clear_tasks)

    watch = subparsers.add_parser("watch", help="Stream meaningful task events; use --dashboard for full screen redraw")
    watch.add_argument("task_id", nargs="?")
    watch.add_argument("--interval", type=float, default=DEFAULT_WATCH_INTERVAL)
    watch.add_argument("--dashboard", action="store_true", help="Redraw the full dashboard instead of streaming changes")
    watch.add_argument("--events-limit", type=int, default=DEFAULT_EVENTS_LIMIT)
    watch.add_argument("--until-finished", action="store_true")
    watch.add_argument("--once", action="store_true")
    watch.set_defaults(func=cmd_watch)

    tui = subparsers.add_parser("tui", help="Open the full-screen curses dashboard + prompt console")
    tui.add_argument("--task-id")
    tui.add_argument("--executor", choices=("mock", "codex"), default=LIVE_DEFAULT_EXECUTOR)
    tui.add_argument("--exec-timeout", type=float, default=default_exec_timeout_for(LIVE_DEFAULT_EXECUTOR))
    tui.add_argument("--interval", type=float, default=DEFAULT_WATCH_INTERVAL)
    tui.add_argument("--events-limit", type=int, default=DEFAULT_EVENTS_LIMIT)
    tui.set_defaults(func=cmd_tui)

    asymptote_console = subparsers.add_parser("asymptote-console", help="Open the dedicated Asymptote console")
    asymptote_console.add_argument("--activate", action="store_true", help="Start the asymptote engine before opening the console")
    asymptote_console.add_argument("--attach", action="store_true", help="Open the console without starting a new engine")
    asymptote_console.add_argument("--events-limit", type=int, default=DEFAULT_EVENTS_LIMIT)
    asymptote_console.set_defaults(func=cmd_asymptote_console)

    codex = subparsers.add_parser("codex", help="Launch stock Codex inside the lab workspace")
    codex.add_argument("extra_args", nargs=argparse.REMAINDER)
    codex.set_defaults(func=cmd_codex)

    return parser


CLI_COMMANDS = {
    "console",
    "live",
    "doctor",
    "submit",
    "record-submission",
    "score",
    "tick",
    "run-loop",
    "daemon",
    "runs",
    "status",
    "dashboard",
    "workspace",
    "recover",
    "clear-tasks",
    "watch",
    "tui",
    "asymptote-console",
    "codex",
}


def interactive_terminal_available() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def normalize_main_argv(argv: list[str]) -> list[str]:
    if not argv:
        if interactive_terminal_available():
            return ["console"]
        return ["live"]
    if argv[0] in CLI_COMMANDS or argv[0] in {"-h", "--help"}:
        return argv
    return ["live", *argv]


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(normalize_main_argv(list(argv if argv is not None else sys.argv[1:])))
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
