from __future__ import annotations

import base64
import json
import os
import selectors
import shlex
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

import fcntl


PROFILE_STATUSES = {"ready", "active", "exhausted", "disabled"}
QUOTA_MARKERS = (
    "quota exceeded",
    "rate limit",
    "429",
    "usage limit",
)


class VaultError(RuntimeError):
    """Raised when the credential vault cannot satisfy a requested operation."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _jwt_payload(token: str | None) -> dict[str, Any]:
    if not token:
        return {}
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        parsed = json.loads(decoded.decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def extract_fingerprint(auth_data: dict[str, Any]) -> dict[str, str | None]:
    tokens = auth_data.get("tokens")
    if not isinstance(tokens, dict):
        return {"account_id": None, "email": None}
    payload = _jwt_payload(str(tokens.get("id_token") or tokens.get("access_token") or ""))
    auth_claims = payload.get("https://api.openai.com/auth")
    if not isinstance(auth_claims, dict):
        auth_claims = {}
    return {
        "account_id": str(auth_claims.get("chatgpt_account_id") or tokens.get("account_id") or "") or None,
        "email": str(payload.get("email") or "") or None,
    }


def is_quota_text(text: str | None) -> bool:
    normalized = (text or "").lower()
    return any(marker in normalized for marker in QUOTA_MARKERS)


@contextmanager
def advisory_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def atomic_write_json(path: Path, payload: dict[str, Any], *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def normalize_pool(payload: dict[str, Any]) -> dict[str, Any]:
    if "accounts" not in payload:
        accounts = {key: value for key, value in payload.items() if key.startswith("account_") and isinstance(value, dict)}
        payload = {
            "version": 1,
            "current_account_key": None,
            "auto_switch": True,
            "last_rotation": {},
            "accounts": accounts,
        }
    payload.setdefault("version", 1)
    payload.setdefault("current_account_key", None)
    payload.setdefault("auto_switch", True)
    payload.setdefault("last_rotation", {})
    accounts = payload.get("accounts")
    if not isinstance(accounts, dict):
        accounts = {}
    normalized_accounts: dict[str, Any] = {}
    for key, value in accounts.items():
        if not isinstance(value, dict):
            continue
        entry = dict(value)
        status = str(entry.get("status") or "ready")
        if status not in PROFILE_STATUSES:
            status = "ready"
        entry["status"] = status
        entry.setdefault("alias", key)
        entry.setdefault("fingerprint", {"account_id": None, "email": None})
        entry.setdefault("auth_data", {})
        entry.setdefault("last_sync", None)
        normalized_accounts[str(key)] = entry
    payload["accounts"] = normalized_accounts
    current = payload.get("current_account_key")
    if current and current not in normalized_accounts:
        payload["current_account_key"] = None
    return payload


@dataclass
class RotationRecord:
    previous_account_key: str | None
    next_account_key: str
    reason: str
    rotated_at: str


@dataclass
class ExecutionResult:
    completed: subprocess.CompletedProcess[str]
    attempts: int
    rotations: list[RotationRecord]
    quota_detected: bool


class CredentialVault:
    """Manage codex auth profiles stored in pool.json and mirrored auth.json."""

    def __init__(self, pool_path: Path, auth_path: Path) -> None:
        self.pool_path = pool_path
        self.auth_path = auth_path
        self.pool_lock_path = pool_path.with_suffix(pool_path.suffix + ".lock")
        self.auth_lock_path = auth_path.with_suffix(auth_path.suffix + ".lock")

    def load(self) -> dict[str, Any]:
        with advisory_lock(self.pool_lock_path):
            return normalize_pool(read_json(self.pool_path))

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_pool(payload)
        with advisory_lock(self.pool_lock_path):
            atomic_write_json(self.pool_path, normalized)
        return normalized

    def summary(self) -> dict[str, Any]:
        payload = self.load()
        accounts = payload["accounts"]
        counts = {status: 0 for status in PROFILE_STATUSES}
        profiles: list[dict[str, Any]] = []
        for key in sorted(accounts):
            entry = accounts[key]
            counts[entry["status"]] += 1
            profiles.append(
                {
                    "account_key": key,
                    "alias": entry.get("alias") or key,
                    "status": entry["status"],
                    "is_current": key == payload.get("current_account_key"),
                }
            )
        current = payload.get("current_account_key")
        current_entry = accounts.get(current or "", {})
        return {
            "auto_switch": bool(payload.get("auto_switch", True)),
            "current_account_key": current,
            "current_alias": current_entry.get("alias"),
            "counts": counts,
            "profiles": profiles,
            "last_rotation": payload.get("last_rotation") or {},
        }

    def auto_switch_enabled(self) -> bool:
        return bool(self.load().get("auto_switch", True))

    def set_auto_switch(self, enabled: bool) -> bool:
        payload = self.load()
        payload["auto_switch"] = bool(enabled)
        self.save(payload)
        return bool(enabled)

    def current_account_key(self) -> str | None:
        return self.load().get("current_account_key")

    def resolve_account_ref(self, account_ref: str) -> str:
        ref = account_ref.strip()
        if not ref:
            raise VaultError("empty account reference")
        payload = self.load()
        accounts = payload["accounts"]
        if ref in accounts:
            return ref
        matches = [key for key, entry in accounts.items() if str(entry.get("alias") or "").strip() == ref]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise VaultError(f"alias is ambiguous: {ref}")
        raise VaultError(f"unknown account: {ref}")

    def _next_account_key(self, accounts: dict[str, Any]) -> str:
        suffixes = []
        for key in accounts:
            if key.startswith("account_"):
                try:
                    suffixes.append(int(key.split("_", 1)[1]))
                except ValueError:
                    continue
        return f"account_{(max(suffixes) + 1) if suffixes else 1}"

    def _matching_account_key(self, accounts: dict[str, Any], fingerprint: dict[str, str | None]) -> str | None:
        for key, entry in accounts.items():
            known = entry.get("fingerprint") or {}
            if fingerprint.get("account_id") and fingerprint.get("account_id") == known.get("account_id"):
                return key
            if fingerprint.get("email") and fingerprint.get("email") == known.get("email"):
                return key
        return None

    def _mark_active(self, payload: dict[str, Any], account_key: str, *, previous_active_status: str = "ready") -> dict[str, Any]:
        accounts = payload["accounts"]
        previous_key = payload.get("current_account_key")
        if previous_key and previous_key in accounts and previous_key != account_key:
            if accounts[previous_key]["status"] == "active":
                accounts[previous_key]["status"] = previous_active_status
        accounts[account_key]["status"] = "active"
        payload["current_account_key"] = account_key
        return payload

    def register_current(self, alias: str) -> str:
        auth_data = read_json(self.auth_path)
        if not auth_data:
            raise VaultError(f"missing auth file: {self.auth_path}")
        payload = self.load()
        accounts = payload["accounts"]
        fingerprint = extract_fingerprint(auth_data)
        account_key = self._matching_account_key(accounts, fingerprint) or self._next_account_key(accounts)
        entry = accounts.get(account_key, {})
        entry["alias"] = alias.strip() or account_key
        entry["status"] = "active"
        entry["fingerprint"] = fingerprint
        entry["auth_data"] = auth_data
        entry["last_sync"] = utc_now()
        accounts[account_key] = entry
        payload["accounts"] = accounts
        payload = self._mark_active(payload, account_key)
        self.save(payload)
        return account_key

    def inject_auth(self, account_key: str, *, previous_active_status: str = "ready") -> None:
        payload = self.load()
        accounts = payload["accounts"]
        if account_key not in accounts:
            raise VaultError(f"unknown account: {account_key}")
        auth_data = accounts[account_key].get("auth_data")
        if not isinstance(auth_data, dict) or not auth_data:
            raise VaultError(f"account has no auth payload: {account_key}")
        with advisory_lock(self.auth_lock_path):
            atomic_write_json(self.auth_path, auth_data)
        payload = self._mark_active(payload, account_key, previous_active_status=previous_active_status)
        self.save(payload)

    def sync_auth(self, account_key: str | None = None) -> dict[str, Any]:
        payload = self.load()
        accounts = payload["accounts"]
        target_key = account_key or payload.get("current_account_key")
        if not target_key or target_key not in accounts:
            raise VaultError("no active account to sync")
        auth_data = read_json(self.auth_path)
        if not auth_data:
            raise VaultError(f"missing auth file: {self.auth_path}")
        incoming = extract_fingerprint(auth_data)
        known = accounts[target_key].get("fingerprint") or {}
        if known.get("account_id") and incoming.get("account_id") and known["account_id"] != incoming["account_id"]:
            raise VaultError("active auth fingerprint does not match the selected account")
        if known.get("email") and incoming.get("email") and known["email"] != incoming["email"]:
            raise VaultError("active auth email does not match the selected account")
        accounts[target_key]["auth_data"] = auth_data
        accounts[target_key]["fingerprint"] = incoming
        accounts[target_key]["last_sync"] = utc_now()
        payload["accounts"] = accounts
        self.save(payload)
        return accounts[target_key]

    def activate(self, account_key: str) -> None:
        self.inject_auth(account_key, previous_active_status="ready")

    def disable(self, account_key: str) -> None:
        payload = self.load()
        accounts = payload["accounts"]
        if account_key not in accounts:
            raise VaultError(f"unknown account: {account_key}")
        accounts[account_key]["status"] = "disabled"
        if payload.get("current_account_key") == account_key:
            payload["current_account_key"] = None
        payload["accounts"] = accounts
        self.save(payload)

    def enable(self, account_key: str) -> None:
        payload = self.load()
        accounts = payload["accounts"]
        if account_key not in accounts:
            raise VaultError(f"unknown account: {account_key}")
        accounts[account_key]["status"] = "ready" if account_key != payload.get("current_account_key") else "active"
        payload["accounts"] = accounts
        self.save(payload)

    def reset_exhausted(self) -> int:
        payload = self.load()
        changed = 0
        for key, entry in payload["accounts"].items():
            if entry["status"] == "exhausted":
                entry["status"] = "ready" if key != payload.get("current_account_key") else "active"
                changed += 1
        self.save(payload)
        return changed

    def rotate_account(self, *, reason: str = "quota exceeded") -> str:
        payload = self.load()
        accounts = payload["accounts"]
        previous_key = payload.get("current_account_key")
        if previous_key and previous_key in accounts:
            accounts[previous_key]["status"] = "exhausted"
        next_key = None
        for key in sorted(accounts):
            if accounts[key]["status"] == "ready":
                next_key = key
                break
        if not next_key:
            raise VaultError("no ready account available for rotation")
        payload["accounts"] = accounts
        payload["last_rotation"] = {
            "previous_account_key": previous_key,
            "next_account_key": next_key,
            "reason": reason,
            "rotated_at": utc_now(),
        }
        self.save(payload)
        self.inject_auth(next_key, previous_active_status="exhausted")
        return next_key


class ResilientRunner:
    """Run commands with live streaming and optional auth rotation on quota failures."""

    def __init__(self, vault: CredentialVault, *, max_attempts: int = 3) -> None:
        self.vault = vault
        self.max_attempts = max(1, int(max_attempts))

    def execute(
        self,
        cmd_list: list[str],
        auto_switch: bool = True,
        *,
        input_text: str | None = None,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float | None = None,
        stdout_consumer: Callable[[str], None] | None = None,
        stderr_consumer: Callable[[str], None] | None = None,
    ) -> ExecutionResult:
        rotations: list[RotationRecord] = []
        quota_detected = False
        attempts = 0

        while attempts < self.max_attempts:
            attempts += 1
            stdout_chunks: list[str] = []
            stderr_chunks: list[str] = []
            current_key = self.vault.current_account_key()
            process = subprocess.Popen(
                cmd_list,
                cwd=str(cwd) if cwd is not None else None,
                env=env,
                stdin=subprocess.PIPE if input_text is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            if input_text is not None and process.stdin is not None:
                process.stdin.write(input_text)
                process.stdin.close()

            selector = selectors.DefaultSelector()
            if process.stdout is not None:
                selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            if process.stderr is not None:
                selector.register(process.stderr, selectors.EVENT_READ, "stderr")

            try:
                start_monotonic = time.monotonic()
                quota_hit_this_attempt = False

                while selector.get_map():
                    if timeout_seconds is not None and (time.monotonic() - start_monotonic) > timeout_seconds:
                        process.terminate()
                        try:
                            process.wait(timeout=5.0)
                        except subprocess.TimeoutExpired:
                            process.kill()
                        raise subprocess.TimeoutExpired(cmd_list, timeout_seconds, output="".join(stdout_chunks), stderr="".join(stderr_chunks))

                    events = selector.select(timeout=0.1)
                    if not events and process.poll() is not None:
                        for key in list(selector.get_map().values()):
                            stream = key.fileobj
                            tail = stream.read()
                            if tail:
                                if key.data == "stdout":
                                    stdout_chunks.append(tail)
                                    if stdout_consumer:
                                        stdout_consumer(tail)
                                else:
                                    stderr_chunks.append(tail)
                                    if stderr_consumer:
                                        stderr_consumer(tail)
                            selector.unregister(stream)
                        continue

                    for key, _mask in events:
                        stream = key.fileobj
                        line = stream.readline()
                        if line == "":
                            selector.unregister(stream)
                            continue
                        if key.data == "stdout":
                            stdout_chunks.append(line)
                            if stdout_consumer:
                                stdout_consumer(line)
                        else:
                            stderr_chunks.append(line)
                            if stderr_consumer:
                                stderr_consumer(line)
                        if auto_switch and self.vault.auto_switch_enabled() and is_quota_text(line):
                            quota_hit_this_attempt = True
                            quota_detected = True
                            process.terminate()

                return_code = process.wait()
                stdout_text = "".join(stdout_chunks)
                stderr_text = "".join(stderr_chunks)
                combined = "\n".join(part for part in (stdout_text, stderr_text) if part)
                quota_hit_this_attempt = quota_hit_this_attempt or is_quota_text(combined)

                if current_key:
                    try:
                        self.vault.sync_auth(current_key)
                    except VaultError:
                        pass

                completed = subprocess.CompletedProcess(cmd_list, return_code, stdout_text, stderr_text)
                if not (auto_switch and self.vault.auto_switch_enabled() and quota_hit_this_attempt and attempts < self.max_attempts):
                    return ExecutionResult(completed=completed, attempts=attempts, rotations=rotations, quota_detected=quota_detected)

                previous_key = current_key
                next_key = self.vault.rotate_account(reason="quota exceeded")
                rotations.append(
                    RotationRecord(
                        previous_account_key=previous_key,
                        next_account_key=next_key,
                        reason="quota exceeded",
                        rotated_at=utc_now(),
                    )
                )
                if stderr_consumer:
                    stderr_consumer(f"[resilience] switched account {previous_key or '-'} -> {next_key}\n")
            finally:
                selector.close()
                if process.stdin is not None and not process.stdin.closed:
                    process.stdin.close()
                if process.stdout is not None and not process.stdout.closed:
                    process.stdout.close()
                if process.stderr is not None and not process.stderr.closed:
                    process.stderr.close()

        return ExecutionResult(
            completed=subprocess.CompletedProcess(cmd_list, 1, "", ""),
            attempts=attempts,
            rotations=rotations,
            quota_detected=quota_detected,
        )


def parse_shell_command(command_text: str) -> list[str]:
    try:
        return shlex.split(command_text)
    except ValueError as exc:
        raise VaultError(str(exc)) from exc
