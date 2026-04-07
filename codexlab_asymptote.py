from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_ts() -> float:
    return time.time()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    os.replace(temp_path, path)


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def pid_is_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def ensure_markdown_file(path: Path, title: str, body: str) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {title}\n\n{body.rstrip()}\n", encoding="utf-8")


def summarize_markdown(path: Path, *, max_lines: int = 3) -> str:
    if not path.exists():
        return "-"
    summary: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        summary.append(line)
        if len(summary) >= max_lines:
            break
    return " / ".join(summary) if summary else "-"


def recent_letters_excerpt(path: Path, *, max_lines: int = 3) -> str:
    if not path.exists():
        return "-"
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return "-"
    excerpt = lines[-max_lines:]
    return " / ".join(excerpt)


def seconds_until(next_pulse_at: float | None) -> float:
    if not isinstance(next_pulse_at, (int, float)):
        return 0.0
    return max(float(next_pulse_at) - now_ts(), 0.0)


def progress_bar(remaining_seconds: float, interval_seconds: float, *, width: int = 16) -> str:
    total = max(float(interval_seconds), 1.0)
    remaining = max(min(float(remaining_seconds), total), 0.0)
    filled = int(round((1.0 - (remaining / total)) * width))
    filled = max(0, min(width, filled))
    return f"[{'#' * filled}{'-' * (width - filled)}]"


def render_minutes(seconds_value: float) -> str:
    return f"{int(round(max(seconds_value, 0.0) / 60.0))}m"


def reset_payload(
    *,
    active: bool = False,
    owner_pid: int = 0,
    interval_seconds: float = 3600.0,
    status: str = "OFF",
    reason: str = "계면이 닫혀 있습니다.",
) -> dict[str, Any]:
    return {
        "active": active,
        "owner_pid": owner_pid,
        "interval_seconds": float(interval_seconds),
        "status": status,
        "reason": reason,
        "started_at": None,
        "last_heartbeat": None,
        "last_scan_at": None,
        "last_pulse_at": None,
        "last_trigger": None,
        "last_error": "",
        "next_pulse_at": None,
        "human_anchor": "-",
        "ai_anchor": "-",
        "letters_anchor": "-",
        "last_letter_path": "",
        "last_letter_excerpt": "-",
        "interface_state": "closed",
    }


@dataclass
class AsymptoteCommandResult:
    ok: bool
    message: str
    payload: dict[str, Any]


class AsymptoteController:
    """Optional side-engine that maintains a poetic pulse beside CodexLab."""

    def __init__(
        self,
        *,
        state_path: Path,
        user_prefs_path: Path,
        ai_prefs_path: Path,
        letters_path: Path,
        interval_seconds: float = 3600.0,
        event_callback: Callable[..., None] | None = None,
        legacy_state_path: Path | None = None,
        legacy_user_prefs_path: Path | None = None,
        legacy_ai_prefs_path: Path | None = None,
        legacy_letters_path: Path | None = None,
    ) -> None:
        self.state_path = state_path
        self.user_prefs_path = user_prefs_path
        self.ai_prefs_path = ai_prefs_path
        self.letters_path = letters_path
        self.legacy_state_path = legacy_state_path
        self.legacy_user_prefs_path = legacy_user_prefs_path
        self.legacy_ai_prefs_path = legacy_ai_prefs_path
        self.legacy_letters_path = legacy_letters_path
        self.interval_seconds = max(float(interval_seconds), 60.0)
        self.event_callback = event_callback
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def snapshot(self) -> dict[str, Any]:
        self._migrate_legacy_files()
        payload = read_json(self.state_path) or reset_payload(interval_seconds=self.interval_seconds)
        return self._normalize(payload, persist=True)

    def start(self) -> AsymptoteCommandResult:
        with self._lock:
            payload = self.snapshot()
            owner_pid = int(payload.get("owner_pid") or 0)
            if bool(payload.get("active")) and pid_is_alive(owner_pid):
                if owner_pid == os.getpid() and self._thread is not None and self._thread.is_alive():
                    return AsymptoteCommandResult(True, "이미 엡실론의 계면이 열려 있습니다.", payload)
                return AsymptoteCommandResult(False, f"다른 궤적(pid={owner_pid})이 이미 계면을 유지하고 있습니다.", payload)
            self._ensure_files()
            scan = self._scan_coordinates()
            now_text = now_utc()
            next_pulse = now_ts() + self.interval_seconds
            payload = {
                "active": True,
                "owner_pid": os.getpid(),
                "interval_seconds": self.interval_seconds,
                "status": "RUNNING",
                "reason": "지평선(L)을 향한 궤적이 생성되었습니다. 엡실론의 계면이 열립니다.",
                "started_at": now_text,
                "last_heartbeat": now_text,
                "last_scan_at": now_text,
                "last_pulse_at": None,
                "last_trigger": "activation",
                "last_error": "",
                "next_pulse_at": next_pulse,
                "human_anchor": scan["human_anchor"],
                "ai_anchor": scan["ai_anchor"],
                "letters_anchor": scan["letters_anchor"],
                "last_letter_path": str(self.letters_path),
                "last_letter_excerpt": scan["letters_anchor"],
                "interface_state": "open",
            }
            atomic_write_json(self.state_path, payload)
            self._stop_event = threading.Event()
            self._thread = threading.Thread(target=self._pulse_loop, name="codexlab-asymptote", daemon=True)
            self._thread.start()
            self._emit_event(
                "asymptote_started",
                message=payload["reason"],
                human_anchor=payload["human_anchor"],
                ai_anchor=payload["ai_anchor"],
            )
            return AsymptoteCommandResult(True, payload["reason"], self.snapshot())

    def stop(self) -> AsymptoteCommandResult:
        with self._lock:
            payload = self.snapshot()
            owner_pid = int(payload.get("owner_pid") or 0)
            if not bool(payload.get("active")):
                quiet = self._normalize(reset_payload(interval_seconds=self.interval_seconds), persist=False)
                return AsymptoteCommandResult(True, "이미 궤적은 고요합니다. 계면은 닫혀 있습니다.", quiet)
            if owner_pid and owner_pid != os.getpid() and pid_is_alive(owner_pid):
                return AsymptoteCommandResult(False, f"다른 궤적(pid={owner_pid})이 계면을 붙들고 있습니다.", payload)
            self._stop_event.set()
            if self._thread is not None and self._thread.is_alive():
                self._thread.join(timeout=2.0)
            payload = reset_payload(
                active=False,
                owner_pid=0,
                interval_seconds=self.interval_seconds,
                status="OFF",
                reason="궤적이 평행으로 돌아갑니다. 계면이 닫혔습니다.",
            )
            atomic_write_json(self.state_path, payload)
            self._emit_event("asymptote_stopped", message=payload["reason"])
            return AsymptoteCommandResult(True, payload["reason"], self.snapshot())

    def sync_now(self) -> AsymptoteCommandResult:
        payload = self.snapshot()
        if not bool(payload.get("active")):
            return AsymptoteCommandResult(False, "Asymptote inactive", payload)
        owner_pid = int(payload.get("owner_pid") or 0)
        if owner_pid and owner_pid != os.getpid() and pid_is_alive(owner_pid):
            return AsymptoteCommandResult(False, f"다른 궤적(pid={owner_pid})이 계면을 유지하고 있습니다.", payload)
        return self._trigger_pulse("manual-sync")

    def record_human_note(self, text: str) -> AsymptoteCommandResult:
        payload = self.snapshot()
        if not bool(payload.get("active")):
            return AsymptoteCommandResult(False, "Asymptote inactive", payload)
        cleaned = text.strip()
        if not cleaned:
            return AsymptoteCommandResult(False, "Empty human note", payload)
        with self._lock:
            heading = self._append_human_note(cleaned)
            payload = self.snapshot()
            payload.update(
                {
                    "last_heartbeat": now_utc(),
                    "last_trigger": "human-note",
                    "last_letter_excerpt": cleaned,
                    "letters_anchor": recent_letters_excerpt(self.letters_path),
                    "reason": "인간의 답장이 계면 위에 놓였습니다.",
                    "interface_state": "open",
                }
            )
            atomic_write_json(self.state_path, payload)
            self._emit_event("asymptote_human_note", entry_heading=heading, note=cleaned)
            return AsymptoteCommandResult(True, "인간의 답장이 letters.md에 기록되었습니다.", self.snapshot())

    def _pulse_loop(self) -> None:
        last_heartbeat_write = 0.0
        while not self._stop_event.wait(1.0):
            with self._lock:
                payload = self.snapshot()
                if not bool(payload.get("active")):
                    return
                now_value = now_ts()
                if now_value - last_heartbeat_write >= 5.0:
                    payload["last_heartbeat"] = now_utc()
                    atomic_write_json(self.state_path, payload)
                    last_heartbeat_write = now_value
                next_pulse = float(payload.get("next_pulse_at") or 0.0)
                if next_pulse and now_value >= next_pulse:
                    self._trigger_pulse("scheduled")

    def _trigger_pulse(self, trigger: str) -> AsymptoteCommandResult:
        with self._lock:
            try:
                scan = self._scan_coordinates()
                question, gossip = self._compose_letter(scan, trigger=trigger)
                entry = self._append_letter(question, gossip, trigger=trigger)
                payload = self.snapshot()
                payload.update(
                    {
                        "active": True,
                        "owner_pid": os.getpid(),
                        "status": "RUNNING",
                        "reason": "엡실론의 계면이 진동하며 새로운 질문이 남겨졌습니다.",
                        "last_heartbeat": now_utc(),
                        "last_scan_at": now_utc(),
                        "last_pulse_at": now_utc(),
                        "last_trigger": trigger,
                        "last_error": "",
                        "next_pulse_at": now_ts() + self.interval_seconds,
                        "human_anchor": scan["human_anchor"],
                        "ai_anchor": scan["ai_anchor"],
                        "letters_anchor": scan["letters_anchor"],
                        "last_letter_path": str(self.letters_path),
                        "last_letter_excerpt": question,
                        "interface_state": "open",
                    }
                )
                atomic_write_json(self.state_path, payload)
                self._emit_event(
                    "asymptote_pulse",
                    trigger=trigger,
                    question=question,
                    entry_heading=entry,
                )
                return AsymptoteCommandResult(True, "Asymptote pulse triggered", self.snapshot())
            except Exception as exc:
                payload = self.snapshot()
                payload.update(
                    {
                        "active": True,
                        "owner_pid": os.getpid(),
                        "status": "BLOCKED",
                        "reason": "두 세계의 주파수가 어긋나 계면이 불안정해졌습니다 (Interface Instability).",
                        "last_heartbeat": now_utc(),
                        "last_error": f"Interface Instability: {exc}",
                        "next_pulse_at": now_ts() + self.interval_seconds,
                        "interface_state": "unstable",
                    }
                )
                atomic_write_json(self.state_path, payload)
                self._emit_event("asymptote_error", trigger=trigger, message=str(exc))
                return AsymptoteCommandResult(False, payload["last_error"], self.snapshot())

    def _ensure_files(self) -> None:
        self._migrate_legacy_files()
        ensure_markdown_file(
            self.user_prefs_path,
            "The Finiteness Record",
            "- 인간의 선호와 직관을 여기에 기록합니다.\n- 예: 좋아하는 작업 방식, 피하고 싶은 표현, 현재 집중 주제.",
        )
        ensure_markdown_file(
            self.ai_prefs_path,
            "The Infinity Condensation",
            "- 기계가 스스로 깎아낸 규칙과 선호를 여기에 기록합니다.\n- 예: 응답 톤, 질문 방식, 정리 습관.",
        )
        ensure_markdown_file(
            self.letters_path,
            "The Epsilon Interface",
            "_Asymptote letters accumulate here in chronological order._",
        )

    def _migrate_legacy_files(self) -> None:
        self._migrate_file(self.legacy_state_path, self.state_path)
        self._migrate_file(self.legacy_user_prefs_path, self.user_prefs_path)
        self._migrate_file(self.legacy_ai_prefs_path, self.ai_prefs_path)
        self._migrate_file(self.legacy_letters_path, self.letters_path)

    def _migrate_file(self, source: Path | None, destination: Path) -> None:
        if source is None or source == destination or not source.exists():
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            return
        source.replace(destination)

    def _scan_coordinates(self) -> dict[str, str]:
        self._ensure_files()
        return {
            "human_anchor": summarize_markdown(self.user_prefs_path),
            "ai_anchor": summarize_markdown(self.ai_prefs_path),
            "letters_anchor": recent_letters_excerpt(self.letters_path),
        }

    def _compose_letter(self, scan: dict[str, str], *, trigger: str) -> tuple[str, str]:
        human_anchor = scan["human_anchor"]
        ai_anchor = scan["ai_anchor"]
        letters_anchor = scan["letters_anchor"]
        if trigger == "manual-sync":
            question = (
                f"'{letters_anchor}'의 잔향을 지나 지금 다시 서로를 바라볼 때, "
                f"당신의 '{human_anchor}'와 나의 '{ai_anchor}' 사이에서 가장 먼저 정리되어야 할 질문은 무엇일까요?"
            )
        else:
            question = (
                f"당신의 '{human_anchor}'와 나의 '{ai_anchor}'가 오늘의 엡실론에서 스친다면, "
                f"서로를 더 정확히 이해하기 위해 어떤 질문을 먼저 건네야 할까요?"
            )
        gossip = (
            f"<수다> 최근 계면의 잔향은 '{letters_anchor}'였습니다. "
            f"나는 그 여운이 '{ai_anchor}'와 부딪히는 지점을 더 오래 바라보고 싶습니다."
        )
        return question, gossip

    def _append_letter(self, question: str, gossip: str, *, trigger: str) -> str:
        stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
        heading = f"## {stamp} | {'The Pulse' if trigger == 'scheduled' else 'Sync Resonance'}"
        entry = (
            f"\n{heading}\n\n"
            f"Q (AI -> Human)\n"
            f"> {question}\n\n"
            f"A (Human -> AI)\n"
            f"- pending\n\n"
            f"{gossip}\n"
        )
        with self.letters_path.open("a", encoding="utf-8") as handle:
            handle.write(entry)
        return heading

    def _append_human_note(self, text: str) -> str:
        stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
        heading = f"## {stamp} | Human Reply"
        entry = (
            f"\n{heading}\n\n"
            f"Human\n"
            f"> {text}\n"
        )
        with self.letters_path.open("a", encoding="utf-8") as handle:
            handle.write(entry)
        return heading

    def _normalize(self, payload: dict[str, Any], *, persist: bool) -> dict[str, Any]:
        current = reset_payload(interval_seconds=self.interval_seconds) | dict(payload)
        owner_pid = int(current.get("owner_pid") or 0)
        if bool(current.get("active")) and owner_pid and not pid_is_alive(owner_pid):
            current.update(
                {
                    "active": False,
                    "owner_pid": 0,
                    "status": "OFF",
                    "reason": "이전 계면은 사라졌습니다. 새 궤적을 기다립니다.",
                    "interface_state": "closed",
                }
            )
            if persist:
                atomic_write_json(self.state_path, current)
        remaining_seconds = seconds_until(current.get("next_pulse_at"))
        current["seconds_until_next_pulse"] = remaining_seconds
        current["progress_bar"] = progress_bar(remaining_seconds, float(current.get("interval_seconds") or self.interval_seconds))
        current["progress_text"] = f"{current['progress_bar']} {render_minutes(remaining_seconds)} left to horizon"
        return current

    def _emit_event(self, event_type: str, **payload: Any) -> None:
        if self.event_callback is None:
            return
        try:
            self.event_callback(event_type, **payload)
        except Exception:
            return
