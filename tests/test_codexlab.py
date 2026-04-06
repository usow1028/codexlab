from __future__ import annotations

import argparse
import base64
import fcntl
import importlib.util
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest import TestCase, mock


SCRIPT = Path("/home/usow/codexlab/codexlab.py")
INSTALL_SCRIPT = Path("/home/usow/codexlab/scripts/install-codexlab.sh")


class CodexLabCliTests(TestCase):
    def load_module(self):
        root_env = os.environ.get("CODEXLAB_ROOT")
        if root_env:
            os.environ["CODEXLAB_POOL_PATH"] = str(Path(root_env) / ".codexlab" / "pool.json")
        spec = importlib.util.spec_from_file_location("codexlab_runtime", SCRIPT)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    def run_cli(
        self,
        root: Path,
        *args: str,
        extra_env: dict[str, str] | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["CODEXLAB_ROOT"] = str(root)
        env.setdefault("CODEXLAB_POOL_PATH", str(root / ".codexlab" / "pool.json"))
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            check=check,
            capture_output=True,
            text=True,
            input=input_text,
            env=env,
        )

    def status_json(self, root: Path, task_id: str | None = None) -> dict:
        args = ["status", "--json"]
        if task_id:
            args.insert(1, task_id)
        result = self.run_cli(root, *args)
        return json.loads(result.stdout)

    def connect_db(self, root: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(root / "control" / "control.db")
        conn.row_factory = sqlite3.Row
        return conn

    def now_utc(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def queue_reservation(
        self,
        conn: sqlite3.Connection,
        *,
        lane_id: str,
        task_id: str,
        reservation_type: str,
        reason: str = "test queued reservation",
    ) -> str:
        current = int(conn.execute("SELECT value FROM meta WHERE key = 'next_reservation_id'").fetchone()[0])
        reservation_id = f"R-{current:04d}"
        priority = 0 if reservation_type in {"duel_retry", "tie_rematch"} else 10
        now = self.now_utc()
        conn.execute("UPDATE meta SET value = ? WHERE key = 'next_reservation_id'", (str(current + 1),))
        conn.execute(
            """
            INSERT INTO reservations(
                reservation_id, lane_id, task_id, reservation_type, priority, status, reason, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, 'queued', ?, ?, ?)
            """,
            (reservation_id, lane_id, task_id, reservation_type, priority, reason, now, now),
        )
        return reservation_id

    def wait_for(self, predicate, timeout: float = 5.0, interval: float = 0.1) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(interval)
        return False

    def init_git_repo(self, repo: Path) -> None:
        repo.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "init", str(repo)], check=True, capture_output=True, text=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.name", "CodexLab Test"], check=True, capture_output=True, text=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.email", "codexlab@example.com"], check=True, capture_output=True, text=True)
        (repo / "README.md").write_text("test repo\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True, capture_output=True, text=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-m", "init"], check=True, capture_output=True, text=True)

    def seed_doctor_files(self, root: Path) -> None:
        (root / "docs").mkdir(parents=True, exist_ok=True)
        (root / "templates").mkdir(parents=True, exist_ok=True)
        (root / "AGENTS.md").write_text(f"Work only inside {root}\n", encoding="utf-8")
        (root / "docs" / "project-plan.md").write_text("# Plan\n", encoding="utf-8")
        (root / "templates" / "worker_prompt.md").write_text("worker prompt\n", encoding="utf-8")
        (root / "templates" / "evaluator_prompt.md").write_text("evaluator prompt\n", encoding="utf-8")

    def write_fake_auth(self, login_home: Path, *, email: str, account_id: str, plan: str = "plus") -> None:
        login_home.mkdir(parents=True, exist_ok=True)

        def encode(payload: dict) -> str:
            raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

        jwt_payload = {
            "email": email,
            "name": "CodexLab Test",
            "sub": f"google-oauth2|{account_id}",
            "https://api.openai.com/auth": {
                "chatgpt_account_id": account_id,
                "chatgpt_plan_type": plan,
                "user_id": f"user-{account_id}",
                "organizations": [],
            },
        }
        token = f"{encode({'alg': 'none', 'typ': 'JWT'})}.{encode(jwt_payload)}."
        auth_payload = {
            "OPENAI_API_KEY": None,
            "auth_mode": "chatgpt",
            "last_refresh": self.now_utc(),
            "tokens": {
                "access_token": token,
                "account_id": account_id,
                "id_token": token,
                "refresh_token": "refresh-token",
            },
        }
        (login_home / "auth.json").write_text(json.dumps(auth_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    def write_fake_session_file(self, login_home: Path, *, session_id: str, cwd: Path) -> Path:
        session_dir = login_home / "sessions" / "2026" / "04" / "06"
        session_dir.mkdir(parents=True, exist_ok=True)
        session_path = session_dir / f"rollout-{session_id}.jsonl"
        session_path.write_text(
            json.dumps(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": session_id,
                        "cwd": str(cwd),
                        "source": "exec",
                    },
                },
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return session_path

    def make_fake_codex(self, root: Path, entries: list[dict]) -> dict[str, str]:
        helper_dir = root / "fake-codex"
        helper_dir.mkdir(parents=True, exist_ok=True)
        plan_path = helper_dir / "plan.json"
        counter_path = helper_dir / "counter.txt"
        script_path = helper_dir / "fake_codex.py"
        plan_path.write_text(json.dumps(entries, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        counter_path.write_text("0\n", encoding="utf-8")
        script_path.write_text(
            """#!/usr/bin/env python3
from __future__ import annotations
import json
import os
import sys
import time
import fcntl
from pathlib import Path

plan = json.loads(Path(os.environ["FAKE_CODEX_PLAN"]).read_text(encoding="utf-8"))
counter_path = Path(os.environ["FAKE_CODEX_COUNTER"])
with counter_path.open("a+", encoding="utf-8") as counter_handle:
    counter_handle.seek(0)
    fcntl.flock(counter_handle.fileno(), fcntl.LOCK_EX)
    index = int(counter_handle.read().strip() or "0")
    counter_handle.seek(0)
    counter_handle.truncate()
    counter_handle.write(str(index + 1) + "\\n")
    counter_handle.flush()
    os.fsync(counter_handle.fileno())
    fcntl.flock(counter_handle.fileno(), fcntl.LOCK_UN)
entry = plan[index]
argv = sys.argv[1:]
output_path = None
for i, arg in enumerate(argv):
    if arg == "--output-last-message" and i + 1 < len(argv):
        output_path = Path(argv[i + 1])
        break
if output_path is None:
    raise SystemExit("missing --output-last-message")
capture_started_path = entry.get("capture_started_path")
if capture_started_path:
    started_path = Path(capture_started_path)
    started_path.parent.mkdir(parents=True, exist_ok=True)
    started_path.write_text(f"{time.time():.6f}\\n", encoding="utf-8")
sleep_seconds = float(entry.get("sleep", 0))
if sleep_seconds:
    time.sleep(sleep_seconds)
stdout_text = entry.get("stdout", "")
stderr_text = entry.get("stderr", "")
session_id = entry.get("session_id")
if session_id:
    session_meta = json.dumps(
        {
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "cwd": os.getcwd(),
                "source": "exec",
            },
        },
        ensure_ascii=True,
    )
    stdout_text = session_meta + "\\n" + stdout_text
write_file = entry.get("write_file")
if write_file:
    file_path = Path(write_file)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(entry.get("write_text", ""), encoding="utf-8")
capture_env_path = entry.get("capture_env_path")
if capture_env_path:
    env_payload = {name: os.environ.get(name) for name in entry.get("capture_env", [])}
    capture_path = Path(capture_env_path)
    capture_path.parent.mkdir(parents=True, exist_ok=True)
    capture_path.write_text(json.dumps(env_payload, ensure_ascii=True) + "\\n", encoding="utf-8")
if stdout_text:
    print(stdout_text, end="")
if stderr_text:
    print(stderr_text, file=sys.stderr, end="")
if "payload" in entry:
    output_path.write_text(json.dumps(entry["payload"], ensure_ascii=True) + "\\n", encoding="utf-8")
raise SystemExit(int(entry.get("exit_code", 0)))
""",
            encoding="utf-8",
        )
        script_path.chmod(0o755)
        return {
            "CODEXLAB_CODEX_BIN": str(script_path),
            "FAKE_CODEX_PLAN": str(plan_path),
            "FAKE_CODEX_COUNTER": str(counter_path),
        }

    def test_install_script_creates_wrapper_and_wrapper_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            temp = Path(tmpdir)
            home = temp / "home"
            install_dir = home / "bin"
            shell_rc = home / ".bashrc"
            root = temp / "lab-root"
            home.mkdir(parents=True, exist_ok=True)

            env = os.environ.copy()
            env["HOME"] = str(home)
            env["SHELL"] = "/bin/bash"
            subprocess.run(
                ["bash", str(INSTALL_SCRIPT), "--install-dir", str(install_dir), "--shell-rc", str(shell_rc)],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )

            wrapper = install_dir / "codexlab"
            self.assertTrue(wrapper.is_symlink())
            self.assertEqual(shell_rc.read_text(encoding="utf-8").strip(), 'export PATH="$HOME/bin:$PATH"')

            wrapped = subprocess.run(
                [str(wrapper), "status", "--json"],
                check=True,
                capture_output=True,
                text=True,
                env={**env, "CODEXLAB_ROOT": str(root)},
            )
            payload = json.loads(wrapped.stdout)
            self.assertEqual(payload["root"], str(root))
            self.assertEqual(len(payload["lanes"]), 3)

    def test_install_script_writes_fish_conf_snippet_for_fish_shell(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            temp = Path(tmpdir)
            home = temp / "home"
            install_dir = home / "bin"
            fish_conf = home / ".config" / "fish" / "conf.d" / "codexlab_path.fish"
            root = temp / "lab-root"
            home.mkdir(parents=True, exist_ok=True)

            env = os.environ.copy()
            env["HOME"] = str(home)
            env["SHELL"] = "/bin/fish"
            subprocess.run(
                ["bash", str(INSTALL_SCRIPT), "--install-dir", str(install_dir)],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )

            wrapper = install_dir / "codexlab"
            self.assertTrue(wrapper.is_symlink())
            self.assertTrue(fish_conf.exists())
            self.assertEqual(
                fish_conf.read_text(encoding="utf-8"),
                'if not contains -- "$HOME/bin" $PATH\n    set -gx PATH "$HOME/bin" $PATH\nend\n',
            )

            wrapped = subprocess.run(
                [str(wrapper), "status", "--json"],
                check=True,
                capture_output=True,
                text=True,
                env={**env, "CODEXLAB_ROOT": str(root)},
            )
            payload = json.loads(wrapped.stdout)
            self.assertEqual(payload["root"], str(root))
            self.assertEqual(len(payload["lanes"]), 3)

    def test_doctor_bootstraps_repo_local_codex_home(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.seed_doctor_files(root)

            payload = json.loads(self.run_cli(root, "doctor", "--json").stdout)
            self.assertFalse(any(item["status"] == "error" for item in payload["findings"]))

            config_path = root / ".codex-home" / "config.toml"
            self.assertTrue(config_path.exists())
            self.assertIn(str(root), config_path.read_text(encoding="utf-8"))

    def test_submit_assigns_both_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Build feature alpha")
            status = self.status_json(root)
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            self.assertEqual(lanes["worker-a"]["active_task_id"], "T-0001")
            self.assertEqual(lanes["worker-b"]["active_task_id"], "T-0001")
            self.assertEqual(status["tasks"][0]["task_id"], "T-0001")

    def test_live_shorthand_accepts_prompt_words_without_explicit_subcommand(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            result = self.run_cli(
                root,
                "--executor",
                "mock",
                "--no-watch",
                "Explain",
                "a",
                "ranking",
                "battle",
            )
            self.assertIn("task_id=T-0001", result.stdout)
            self.assertIn("watch_hint=codexlab watch T-0001 --until-finished", result.stdout)
            self.assertTrue(
                self.wait_for(
                    lambda: (
                        (payload := json.loads(self.run_cli(root, "daemon", "status", "--json").stdout))
                    )
                    and (not payload["running"])
                    and payload["state"].get("reason") == "idle",
                    timeout=5.0,
                )
            )
            status = self.status_json(root, "T-0001")
            self.assertEqual(status["tasks"][0]["status"], "masterpiece_locked")

    def test_live_default_mode_accepts_prompt_from_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            result = self.run_cli(
                root,
                input_text="Describe a worker and evaluator pipeline simply\n",
                extra_env={"CODEXLAB_LIVE_EXECUTOR": "mock"},
            )
            self.assertIn("task_id=T-0001", result.stdout)
            self.assertIn("watch_hint=codexlab watch T-0001 --until-finished", result.stdout)
            self.assertTrue(
                self.wait_for(
                    lambda: (
                        (payload := json.loads(self.run_cli(root, "daemon", "status", "--json").stdout))
                    )
                    and (not payload["running"])
                    and payload["state"].get("reason") == "idle",
                    timeout=5.0,
                )
            )
            status = self.status_json(root, "T-0001")
            self.assertEqual(status["tasks"][0]["title"], "Describe a worker and evaluator pipeline simply")

    def test_normalize_main_argv_routes_bare_tty_to_console(self) -> None:
        module = self.load_module()
        with mock.patch.object(module, "interactive_terminal_available", return_value=True):
            self.assertEqual(module.normalize_main_argv([]), ["console"])

    def test_normalize_main_argv_routes_bare_non_tty_to_live(self) -> None:
        module = self.load_module()
        with mock.patch.object(module, "interactive_terminal_available", return_value=False):
            self.assertEqual(module.normalize_main_argv([]), ["live"])

    def test_format_console_screen_combines_dashboard_and_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Explain the duel flow simply")
            snapshot = json.loads(self.run_cli(root, "dashboard", "--json").stdout)
            module = self.load_module()
            lines = module.format_console_screen(
                snapshot,
                focus_task_id=None,
                status_message="ready",
                input_buffer="New task prompt",
                width=100,
                height=30,
            )
            rendered = "\n".join(lines)
            self.assertIn("CodexLab Console | focus=all", rendered)
            self.assertIn("Execution:", rendered)
            self.assertIn("Tasks:", rendered)
            self.assertIn("T-0001", rendered)
            self.assertIn("Status: ready", rendered)
            self.assertIn("Prompt> New task prompt", rendered)

    def test_console_input_timeout_waits_until_next_refresh(self) -> None:
        module = self.load_module()
        timeout_ms = module.console_input_timeout_ms(
            now_monotonic=10.25,
            last_refresh_monotonic=10.0,
            interval=1.0,
            snapshot_loaded=True,
        )
        self.assertGreaterEqual(timeout_ms, 700)
        self.assertLessEqual(timeout_ms, 800)
        self.assertEqual(
            module.console_input_timeout_ms(
                now_monotonic=11.1,
                last_refresh_monotonic=10.0,
                interval=1.0,
                snapshot_loaded=True,
            ),
            0,
        )
        self.assertEqual(
            module.console_input_timeout_ms(
                now_monotonic=10.25,
                last_refresh_monotonic=10.0,
                interval=1.0,
                snapshot_loaded=False,
            ),
            0,
        )

    def test_handle_console_command_supports_refresh(self) -> None:
        module = self.load_module()
        status_message, focus_task_id, should_exit = module.handle_console_command(
            "/refresh",
            focus_task_id="T-0007",
        )
        self.assertEqual(status_message, "refreshed")
        self.assertEqual(focus_task_id, "T-0007")
        self.assertFalse(should_exit)

    def test_handle_console_command_mentions_clear_tasks(self) -> None:
        module = self.load_module()
        status_message, focus_task_id, should_exit = module.handle_console_command(
            "/clear-tasks",
            focus_task_id="T-0007",
        )
        self.assertEqual(status_message, "cleared tasks")
        self.assertIsNone(focus_task_id)
        self.assertFalse(should_exit)

    def test_build_parser_routes_tui_to_cmd_tui(self) -> None:
        module = self.load_module()
        parser = module.build_parser()
        args = parser.parse_args(["tui"])
        self.assertIs(args.func, module.cmd_tui)

    def test_read_prompt_line_uses_prompt_session_when_available(self) -> None:
        module = self.load_module()
        module.PROMPT_SESSIONS.clear()

        class FakeSession:
            def __init__(self) -> None:
                self.calls: list[tuple[str, dict[str, object]]] = []

            def prompt(self, message: object = None, **kwargs: object) -> str:
                self.calls.append((message, kwargs))
                return "한글 입력"

        with mock.patch.object(module, "PromptSession", FakeSession), mock.patch.object(
            module, "interactive_terminal_available", return_value=True
        ):
            message = lambda: "live panel"
            value = module.read_prompt_line(
                "Prompt> ",
                session_name="console",
                refresh_interval=0.5,
                message=message,
                completer=module.CONSOLE_SLASH_COMPLETER,
                complete_while_typing=True,
            )
            self.assertEqual(value, "한글 입력")
            self.assertIn("console", module.PROMPT_SESSIONS)
            self.assertEqual(len(module.PROMPT_SESSIONS["console"].calls), 1)
            prompt_message, kwargs = module.PROMPT_SESSIONS["console"].calls[0]
            self.assertIs(prompt_message, message)
            self.assertEqual(kwargs["refresh_interval"], 0.5)
            self.assertTrue(kwargs["complete_while_typing"])
            self.assertIs(kwargs["completer"], module.CONSOLE_SLASH_COMPLETER)
            self.assertEqual(kwargs["reserve_space_for_menu"], 8)
            self.assertNotIn("message", kwargs)

    def test_console_slash_completer_suggests_matching_commands(self) -> None:
        module = self.load_module()
        completer = module.CONSOLE_SLASH_COMPLETER
        self.assertIsNotNone(completer)
        slash_matches = [item.text for item in completer.get_completions(mock.Mock(text_before_cursor="/"), None)]
        self.assertIn("/profile register <alias>", slash_matches)
        self.assertIn("/auto-switch on", slash_matches)
        self.assertIn("/useage", slash_matches)
        self.assertIn("/useage all", slash_matches)
        self.assertIn("/quit", slash_matches)

        profile_matches = [item.text for item in completer.get_completions(mock.Mock(text_before_cursor="/profile a"), None)]
        self.assertEqual(profile_matches, ["/profile activate <account_key|alias>"])

    def test_console_live_toolbar_text_reflects_current_task_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Toolbar should show live task state")
            self.run_cli(root, "tick", "--executor", "mock")
            with mock.patch.dict(os.environ, {"CODEXLAB_ROOT": str(root)}):
                module = self.load_module()
                toolbar = module.console_live_toolbar_text("T-0001", status_message="ready", events_limit=3)
            self.assertIn("| live panel |", toolbar)
            self.assertIn("CodexLab Console | focus=T-0001", toolbar)
            self.assertIn("T-0001", toolbar)
            self.assertIn("Commands: /focus T-0001 | /all | /refresh | /clear-tasks", toolbar)
            self.assertIn("/profile ...", toolbar)
            self.assertIn("/auto-switch on|off", toolbar)
            self.assertIn("/sync | /useage [all] | /run ... | /quit", toolbar)
            self.assertIn("Status: ready", toolbar)
            self.assertIn("| prompt |", toolbar)
            self.assertTrue(toolbar.rstrip().endswith("-"))

    def test_console_prompt_style_disables_bottom_toolbar_reverse(self) -> None:
        module = self.load_module()
        style = module.CONSOLE_PROMPT_STYLE
        self.assertIsNotNone(style)
        rules = dict(style.style_rules)
        self.assertEqual(rules["bottom-toolbar"], "noreverse bg:default fg:default")
        self.assertEqual(rules["bottom-toolbar.text"], "noreverse bg:default fg:default")

    def test_resilience_vault_register_rotate_and_sync_current_auth(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            pool_path = root / ".codexlab" / "pool.json"
            self.write_fake_auth(login_home, email="alpha@example.com", account_id="acc-alpha")
            with mock.patch.dict(
                os.environ,
                {
                    "CODEXLAB_ROOT": str(root),
                    "CODEXLAB_POOL_PATH": str(pool_path),
                    "CODEXLAB_LOGIN_CODEX_HOME": str(login_home),
                },
            ):
                module = self.load_module()
                vault = module.credential_vault()

                first_key = vault.register_current("alpha")
                self.assertEqual(first_key, "account_1")

                self.write_fake_auth(login_home, email="beta@example.com", account_id="acc-beta")
                second_key = vault.register_current("beta")
                self.assertEqual(second_key, "account_2")

                summary = vault.summary()
                self.assertEqual(summary["current_account_key"], "account_2")
                self.assertEqual(summary["counts"]["active"], 1)
                self.assertEqual(summary["counts"]["ready"], 1)

                vault.activate("account_1")
                injected_auth = json.loads((login_home / "auth.json").read_text(encoding="utf-8"))
                self.assertEqual(injected_auth["tokens"]["account_id"], "acc-alpha")

                injected_auth["tokens"]["refresh_token"] = "rotated-refresh-token"
                (login_home / "auth.json").write_text(
                    json.dumps(injected_auth, indent=2, ensure_ascii=True) + "\n",
                    encoding="utf-8",
                )
                synced_entry = vault.sync_auth("account_1")
                self.assertEqual(synced_entry["auth_data"]["tokens"]["refresh_token"], "rotated-refresh-token")

                rotated_to = vault.rotate_account(reason="quota exceeded")
                self.assertEqual(rotated_to, "account_2")

                summary = vault.summary()
                self.assertEqual(summary["current_account_key"], "account_2")
                self.assertEqual(summary["counts"]["active"], 1)
                self.assertEqual(summary["counts"]["exhausted"], 1)
                rotated_auth = json.loads((login_home / "auth.json").read_text(encoding="utf-8"))
                self.assertEqual(rotated_auth["tokens"]["account_id"], "acc-beta")

    def test_resilient_runner_rotates_on_quota_text_and_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            pool_path = root / ".codexlab" / "pool.json"
            script_path = root / "quota-runner.py"
            script_path.write_text(
                """#!/usr/bin/env python3
from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path

auth = json.loads(Path(os.environ["CODEXLAB_AUTH_PATH"]).read_text(encoding="utf-8"))
account_id = auth["tokens"]["account_id"]
if account_id == "acc-beta":
    print("429 Rate limit", file=sys.stderr, flush=True)
    time.sleep(10)
    raise SystemExit(1)
print(f"ok:{account_id}", flush=True)
""",
                encoding="utf-8",
            )
            script_path.chmod(0o755)

            self.write_fake_auth(login_home, email="alpha@example.com", account_id="acc-alpha")
            with mock.patch.dict(
                os.environ,
                {
                    "CODEXLAB_ROOT": str(root),
                    "CODEXLAB_POOL_PATH": str(pool_path),
                    "CODEXLAB_LOGIN_CODEX_HOME": str(login_home),
                },
            ):
                module = self.load_module()
                vault = module.credential_vault()
                vault.register_current("alpha")
                self.write_fake_auth(login_home, email="beta@example.com", account_id="acc-beta")
                vault.register_current("beta")

                env = os.environ.copy()
                env["CODEXLAB_AUTH_PATH"] = str(login_home / "auth.json")
                result = module.ResilientRunner(vault).execute(
                    ["python3", str(script_path)],
                    auto_switch=True,
                    env=env,
                )

                self.assertEqual(result.completed.returncode, 0)
                self.assertEqual(result.completed.stdout.strip(), "ok:acc-alpha")
                self.assertEqual(result.attempts, 2)
                self.assertTrue(result.quota_detected)
                self.assertEqual(len(result.rotations), 1)
                self.assertEqual(result.rotations[0].previous_account_key, "account_2")
                self.assertEqual(result.rotations[0].next_account_key, "account_1")
                self.assertEqual(vault.current_account_key(), "account_1")

    def test_resilience_console_commands_manage_profiles_and_run_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            pool_path = root / ".codexlab" / "pool.json"
            with mock.patch.dict(
                os.environ,
                {
                    "CODEXLAB_ROOT": str(root),
                    "CODEXLAB_POOL_PATH": str(pool_path),
                    "CODEXLAB_LOGIN_CODEX_HOME": str(login_home),
                },
            ):
                module = self.load_module()

                def fake_login_run(argv, **kwargs):
                    self.assertEqual(argv, [module.REAL_CODEX, "login"])
                    self.assertEqual(kwargs["env"]["CODEX_HOME"], str(login_home))
                    self.write_fake_auth(login_home, email="alpha@example.com", account_id="acc-alpha")
                    return subprocess.CompletedProcess(argv, 0, "", "")

                with mock.patch.object(module.subprocess, "run", side_effect=fake_login_run):
                    registered = module.handle_resilience_console_command("/profile register alpha")
                self.assertEqual(registered, "registered account_1")

                list_output = io.StringIO()
                with mock.patch("sys.stdout", list_output):
                    list_status = module.handle_resilience_console_command("/profile list")
                self.assertEqual(list_status, "profiles=1")
                self.assertIn("account_1", list_output.getvalue())
                self.assertIn("alias=alpha", list_output.getvalue())

                self.assertEqual(module.handle_resilience_console_command("/auto-switch off"), "auto-switch=off")
                self.assertEqual(module.handle_resilience_console_command("/auto-switch on"), "auto-switch=on")

                auth_payload = json.loads((login_home / "auth.json").read_text(encoding="utf-8"))
                auth_payload["tokens"]["refresh_token"] = "synced-refresh-token"
                (login_home / "auth.json").write_text(
                    json.dumps(auth_payload, indent=2, ensure_ascii=True) + "\n",
                    encoding="utf-8",
                )
                sync_status = module.handle_resilience_console_command("/sync")
                self.assertIn("sync complete", sync_status)

                run_output = io.StringIO()
                with mock.patch("sys.stdout", run_output):
                    run_status = module.handle_resilience_console_command('/run python3 -c "print(\'ok from resilience run\')"')
                self.assertIn("run exit=0", run_status)
                self.assertIn("ok from resilience run", run_output.getvalue())

    def test_useage_commands_show_live_weekly_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            pool_path = root / ".codexlab" / "pool.json"
            self.write_fake_auth(login_home, email="alpha@example.com", account_id="acc-alpha")
            with mock.patch.dict(
                os.environ,
                {
                    "CODEXLAB_ROOT": str(root),
                    "CODEXLAB_POOL_PATH": str(pool_path),
                    "CODEXLAB_LOGIN_CODEX_HOME": str(login_home),
                },
            ):
                module = self.load_module()
                vault = module.credential_vault()
                vault.register_current("alpha")
                self.write_fake_auth(login_home, email="beta@example.com", account_id="acc-beta")
                vault.register_current("beta")
                vault.activate("account_1")
                vault.rotate_account(reason="quota exceeded")

                def fake_probe(auth_data, *, codex_bin, scratch_root, timeout_seconds):
                    account_id = auth_data["tokens"]["account_id"]
                    if account_id == "acc-alpha":
                        return {
                            "limit_id": "codex",
                            "limit_name": None,
                            "used_percent": 100.0,
                            "remaining_percent": 0.0,
                            "window_minutes": 10080,
                            "resets_at": 1776045971,
                            "plan_type": "free",
                            "credits": {"hasCredits": False, "unlimited": False, "balance": None},
                        }
                    if account_id == "acc-beta":
                        return {
                            "limit_id": "codex",
                            "limit_name": None,
                            "used_percent": 69.0,
                            "remaining_percent": 31.0,
                            "window_minutes": 10080,
                            "resets_at": 1776045971,
                            "plan_type": "free",
                            "credits": {"hasCredits": False, "unlimited": False, "balance": None},
                        }
                    raise AssertionError(f"unexpected account id: {account_id}")

                current_output = io.StringIO()
                with mock.patch.object(module, "probe_auth_rate_limits", side_effect=fake_probe):
                    with mock.patch("sys.stdout", current_output):
                        current_status = module.handle_resilience_console_command("/useage")
                self.assertEqual(current_status, "usage shown for current profile")
                rendered_current = current_output.getvalue()
                self.assertIn("Usage status:", rendered_current)
                self.assertIn("- account_2: email=beta@example.com (31% left)", rendered_current)
                self.assertNotIn("alias=", rendered_current)
                self.assertNotIn("weekly_limit=", rendered_current)

                all_output = io.StringIO()
                with mock.patch.object(module, "probe_auth_rate_limits", side_effect=fake_probe):
                    with mock.patch("sys.stdout", all_output):
                        all_status = module.handle_resilience_console_command("/useage all")
                self.assertEqual(all_status, "usage shown for all profiles")
                rendered_all = all_output.getvalue()
                self.assertIn("- account_1: email=alpha@example.com (0% left)", rendered_all)
                self.assertIn("- account_2: email=beta@example.com (31% left)", rendered_all)
                self.assertNotIn("alias=", rendered_all)
                self.assertNotIn("last_checked_at=", rendered_all)

    def test_extract_rate_limit_summary_computes_remaining_percent(self) -> None:
        resilience_spec = importlib.util.spec_from_file_location(
            "codexlab_resilience_runtime",
            Path("/home/usow/codexlab/codexlab_resilience.py"),
        )
        assert resilience_spec is not None and resilience_spec.loader is not None
        resilience_module = importlib.util.module_from_spec(resilience_spec)
        sys.modules[resilience_spec.name] = resilience_module
        resilience_spec.loader.exec_module(resilience_module)
        summary = resilience_module.extract_rate_limit_summary(
            {
                "rateLimits": {
                    "limitId": "codex",
                    "limitName": None,
                    "primary": {
                        "usedPercent": 69,
                        "windowDurationMins": 10080,
                        "resetsAt": 1776045971,
                    },
                    "secondary": None,
                    "credits": {"hasCredits": False, "unlimited": False, "balance": None},
                    "planType": "free",
                },
                "rateLimitsByLimitId": None,
            }
        )
        self.assertEqual(summary["limit_id"], "codex")
        self.assertEqual(summary["plan_type"], "free")
        self.assertEqual(summary["window_minutes"], 10080)
        self.assertEqual(summary["remaining_percent"], 31.0)

    def test_ensure_selected_resilience_profile_reinjects_current_auth(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            pool_path = root / ".codexlab" / "pool.json"
            self.write_fake_auth(login_home, email="alpha@example.com", account_id="acc-alpha")
            with mock.patch.dict(
                os.environ,
                {
                    "CODEXLAB_ROOT": str(root),
                    "CODEXLAB_POOL_PATH": str(pool_path),
                    "CODEXLAB_LOGIN_CODEX_HOME": str(login_home),
                },
            ):
                module = self.load_module()
                module.credential_vault().register_current("alpha")
                self.write_fake_auth(login_home, email="beta@example.com", account_id="acc-beta")

                injected = module.ensure_selected_resilience_profile()
                self.assertEqual(injected, "account_1")
                auth_payload = json.loads((login_home / "auth.json").read_text(encoding="utf-8"))
                self.assertEqual(auth_payload["tokens"]["account_id"], "acc-alpha")

    def test_format_console_snapshot_includes_resilience_profile_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            pool_path = root / ".codexlab" / "pool.json"
            self.write_fake_auth(login_home, email="alpha@example.com", account_id="acc-alpha")
            with mock.patch.dict(
                os.environ,
                {
                    "CODEXLAB_ROOT": str(root),
                    "CODEXLAB_POOL_PATH": str(pool_path),
                    "CODEXLAB_LOGIN_CODEX_HOME": str(login_home),
                },
            ):
                module = self.load_module()
                module.credential_vault().register_current("alpha")
                snapshot = json.loads(self.run_cli(root, "dashboard", "--json").stdout)
                rendered = module.format_console_snapshot(snapshot, focus_task_id=None)
                self.assertIn("Resilience: auto_switch=on selected=account_1/alpha", rendered)
                self.assertIn("Profiles: ready=0 active=1 exhausted=0 disabled=0", rendered)

    def test_daemon_status_tolerates_empty_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            daemon_dir = root / "control" / "daemon"
            daemon_dir.mkdir(parents=True, exist_ok=True)
            (daemon_dir / "daemon-state.json").write_text("", encoding="utf-8")
            payload = json.loads(self.run_cli(root, "daemon", "status", "--json").stdout)
            self.assertFalse(payload["running"])
            self.assertEqual(payload["state"], {})

    def test_run_loop_mock_locks_masterpiece_after_single_failed_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Draft a durable implementation plan")
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10")

            status = self.status_json(root)
            task = next(task for task in status["tasks"] if task["task_id"] == "T-0001")
            self.assertEqual(task["status"], "masterpiece_locked")
            self.assertEqual(task["masterpiece_locked"], 1)
            self.assertEqual(task["champion_lane_id"], "worker-a")
            self.assertEqual(task["challenger_failed_attempts"], 1)
            self.assertEqual(task["total_evaluations"], 2)

            conn = self.connect_db(root)
            run_count = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            conn.close()
            self.assertEqual(run_count, 5)
            self.assertTrue((root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())
            self.assertTrue((root / "agents" / "worker-b" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())
            self.assertTrue((root / "agents" / "evaluator" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())

    def test_clear_tasks_command_resets_runtime_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Task to clear")
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10")

            payload = json.loads(self.run_cli(root, "clear-tasks", "--json").stdout)
            self.assertTrue(payload["cleared"])
            self.assertEqual(payload["counts"]["tasks"], 1)
            self.assertGreaterEqual(payload["counts"]["runs"], 1)

            status = self.status_json(root)
            self.assertEqual(status["summary"]["task_total"], 0)
            self.assertFalse(status["tasks"])
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            self.assertTrue(all(lane["status"] == "idle" for lane in lanes.values()))
            self.assertTrue(all(lane["active_task_id"] is None for lane in lanes.values()))

            runs = json.loads(self.run_cli(root, "runs", "list", "--json").stdout)["runs"]
            self.assertFalse(runs)
            conn = self.connect_db(root)
            next_task_id = conn.execute("SELECT value FROM meta WHERE key = 'next_task_id'").fetchone()[0]
            next_run_id = conn.execute("SELECT value FROM meta WHERE key = 'next_run_id'").fetchone()[0]
            conn.close()
            self.assertEqual(next_task_id, "1")
            self.assertEqual(next_run_id, "1")

            self.assertFalse(any((root / "tasks").iterdir()))
            self.assertFalse(any((root / "control" / "runs").iterdir()))
            self.assertFalse(any((root / "agents" / "worker-a" / "workspace" / "tasks").iterdir()))
            self.assertFalse(any((root / "agents" / "worker-a" / "workspace" / "worktrees").iterdir()))

    def test_console_plain_text_is_submitted_as_a_task(self) -> None:
        module = self.load_module()
        submitted: list[tuple[str, str | None]] = []
        dummy_conn = mock.Mock()

        def fake_submit_task(_conn, prompt: str, title: str | None) -> dict[str, str]:
            submitted.append((prompt, title))
            return {"task_id": "T-0001", "title": prompt}

        with mock.patch.object(module, "connect", return_value=dummy_conn), mock.patch.object(
            module, "dashboard_snapshot", return_value={"tasks": [], "root": "/tmp", "generated_at": "-", "daemon": {"running": False, "state": {}}, "summary": {"task_total": 0, "task_in_progress": 0, "task_retrying": 0, "task_locked": 0, "lane_busy": 0, "queued_reservations": 0, "ready_evaluations": 0, "stale_runs": 0, "repairable_lanes": 0}, "lanes": [], "recent_events": []}
        ), mock.patch.object(module, "console_startup_daemon", return_value=""), mock.patch.object(
            module, "format_console_snapshot", return_value="snapshot"
        ), mock.patch.object(module, "read_prompt_line", side_effect=["새 작업", EOFError]), mock.patch.object(
            module, "submit_task", side_effect=fake_submit_task
        ):
            result = module.cmd_console(
                argparse.Namespace(task_id=None, executor="mock", exec_timeout=1.0, interval=1.0, events_limit=5)
            )
        self.assertEqual(result, 0)
        self.assertEqual(submitted, [("새 작업", None)])

    def test_task_artifacts_include_boxing_readable_cards(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Draft a durable implementation plan")
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10")

            bout_card = root / "tasks" / "T-0001" / "bout.md"
            champion_card = root / "tasks" / "T-0001" / "champion-card.md"
            submission_cards = root / "tasks" / "T-0001" / "submission-cards"
            decision_cards = root / "tasks" / "T-0001" / "decision-cards"
            submissions_by_corner = root / "tasks" / "T-0001" / "submissions" / "by-corner"
            evaluations_by_judge = root / "tasks" / "T-0001" / "evaluations" / "by-judge"

            self.assertTrue(bout_card.exists())
            self.assertTrue(champion_card.exists())
            self.assertTrue((submission_cards / "round-01__red-corner__championship-bout__S-0001.md").exists())
            self.assertTrue((submission_cards / "round-02__blue-corner__championship-bout__S-0002.md").exists())
            self.assertTrue((submission_cards / "round-03__blue-corner__challenger-rematch__S-0003.md").exists())
            self.assertTrue((decision_cards / "decision-01__score-judge__championship-bout__E-0001.md").exists())
            self.assertTrue((decision_cards / "decision-02__score-judge__challenger-rematch__E-0002.md").exists())
            self.assertTrue((root / "tasks" / "T-0001" / "submissions" / "index.md").exists())
            self.assertTrue((submissions_by_corner / "red-corner" / "latest.md").exists())
            self.assertTrue((submissions_by_corner / "blue-corner" / "latest.md").exists())
            self.assertTrue((submissions_by_corner / "red-corner" / "round-01__championship-bout__S-0001.md").exists())
            self.assertTrue((submissions_by_corner / "blue-corner" / "round-03__challenger-rematch__S-0003.md").exists())
            self.assertTrue((root / "tasks" / "T-0001" / "evaluations" / "index.md").exists())
            self.assertTrue((evaluations_by_judge / "score-judge" / "latest.md").exists())
            self.assertTrue((evaluations_by_judge / "score-judge" / "decision-01__championship-bout__E-0001.md").exists())

            self.assertIn("T = Task", bout_card.read_text(encoding="utf-8"))
            self.assertIn("S = Submission", bout_card.read_text(encoding="utf-8"))
            self.assertIn("E = Evaluation", bout_card.read_text(encoding="utf-8"))
            self.assertIn("Champion confirmed", champion_card.read_text(encoding="utf-8"))
            self.assertIn("Red Corner", (submissions_by_corner / "red-corner" / "latest.md").read_text(encoding="utf-8"))
            self.assertIn("score judge", (evaluations_by_judge / "score-judge" / "latest.md").read_text(encoding="utf-8"))

    def test_tick_mock_counterattack_upset_requires_symmetric_defense_before_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            plan = {
                "evaluation_scores": {
                    "T-0001": [
                        {"worker-a": 91, "worker-b": 82, "rationale": "worker-a wins initial round"},
                        {"worker-a": 89, "worker-b": 94, "rationale": "worker-b overtakes worker-a"},
                        {"worker-a": 96, "worker-b": 93, "rationale": "worker-a reclaims the lead in the defense round"},
                    ]
                }
            }
            env = {"CODEXLAB_MOCK_PLAN": json.dumps(plan)}

            self.run_cli(root, "submit", "--prompt", "Task one", extra_env=env)
            self.run_cli(root, "tick", "--executor", "mock", extra_env=env)

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(second_tick["actions"][-1]["type"], "evaluation")

            status = self.status_json(root)
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            task = next(task for task in status["tasks"] if task["task_id"] == "T-0001")

            self.assertEqual(task["status"], "challenger_retrying")
            self.assertEqual(task["champion_lane_id"], "worker-b")
            self.assertEqual(task["challenger_lane_id"], "worker-a")
            self.assertEqual(task["duel_stage"], "defense")
            self.assertEqual(lanes["worker-a"]["active_task_id"], "T-0001")
            self.assertEqual(lanes["worker-a"]["status"], "retrying")
            self.assertIsNone(lanes["worker-b"]["active_task_id"])
            self.assertFalse(lanes["worker-a"]["queued_reservations"])
            self.assertFalse(lanes["worker-b"]["queued_reservations"])
            self.assertTrue((root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())

            third_tick = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(third_tick["actions"][-1]["type"], "evaluation")
            self.assertTrue(third_tick["actions"][-1]["masterpiece_locked"])

            final_status = self.status_json(root)
            final_task = next(task for task in final_status["tasks"] if task["task_id"] == "T-0001")
            self.assertEqual(final_task["status"], "masterpiece_locked")
            self.assertEqual(final_task["champion_lane_id"], "worker-a")
            self.assertEqual(final_task["total_evaluations"], 3)
            self.assertEqual(final_task["role_swaps"], 2)

    def test_retry_tie_escalates_to_elder_then_absolute_then_re_reviews_until_decisive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            tie_left = {
                "correctness": 5.0,
                "completeness": 5.0,
                "risk": 5.0,
                "maintainability": 4.0,
                "verification": 4.0,
            }
            tie_right = {
                "correctness": 5.0,
                "completeness": 4.0,
                "risk": 5.0,
                "maintainability": 5.0,
                "verification": 5.0,
            }
            plan = {
                "evaluation_scores": {
                    "T-0001": [
                        {"worker-a": 91, "worker-b": 82, "rationale": "worker-a wins the opening duel"},
                        {
                            "worker-a_rubric": tie_left,
                            "worker-b_rubric": tie_right,
                            "rationale": "Primary retry review is tied",
                            "rematch_brief": "Both workers should either sharpen their edge or resubmit unchanged if confident.",
                        },
                        {
                            "worker-a_rubric": tie_left,
                            "worker-b_rubric": tie_right,
                            "rationale": "Primary rematch review is still tied",
                            "rematch_brief": "Escalate to an elder review.",
                        },
                        {
                            "worker-a_rubric": tie_left,
                            "worker-b_rubric": tie_right,
                            "rationale": "Elder review is tied",
                            "rematch_brief": "Run an elder rematch.",
                        },
                        {
                            "worker-a_rubric": tie_left,
                            "worker-b_rubric": tie_right,
                            "rationale": "Elder rematch is tied",
                            "rematch_brief": "Escalate to the absolute evaluator.",
                        },
                        {
                            "worker-a_rubric": tie_left,
                            "worker-b_rubric": tie_right,
                            "rationale": "Absolute review is tied",
                            "rematch_brief": "Allow one final worker rematch.",
                        },
                        {
                            "worker-a_rubric": tie_left,
                            "worker-b_rubric": tie_right,
                            "rationale": "Absolute final worker rematch is tied",
                            "rematch_brief": "Absolute evaluator must re-review until one side edges ahead.",
                        },
                        {
                            "worker-a": 93,
                            "worker-b": 96,
                            "rationale": "Absolute re-review finally gives worker-b the edge",
                            "loser_brief": "Not used because the task locks now.",
                        },
                        {
                            "worker-a": 93,
                            "worker-b": 96,
                            "rationale": "Absolute re-review finally gives worker-b the edge",
                            "loser_brief": "Not used because the task locks now.",
                        },
                    ]
                }
            }
            env = {"CODEXLAB_MOCK_PLAN": json.dumps(plan)}

            self.run_cli(root, "submit", "--prompt", "Tie escalation path", extra_env=env)

            tick1 = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(tick1["actions"][-1]["type"], "evaluation")

            tick2 = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(tick2["actions"][-1]["type"], "evaluation_tied")
            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["status"], "worker_rematching")
            self.assertEqual(task["pair_mode"], "full_rematch")
            self.assertEqual(task["evaluator_tier"], "primary")

            tick3 = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(tick3["actions"][-1]["type"], "evaluation_tied")
            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["pair_mode"], "review_only")
            self.assertEqual(task["evaluator_tier"], "elder")
            self.assertEqual(task["tier_phase"], "base")

            tick4 = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(tick4["actions"], [tick4["actions"][0]])
            self.assertEqual(tick4["actions"][0]["type"], "evaluation_tied")
            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["pair_mode"], "full_rematch")
            self.assertEqual(task["evaluator_tier"], "elder")
            self.assertEqual(task["tier_phase"], "post_rematch")

            tick5 = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(tick5["actions"][-1]["type"], "evaluation_tied")
            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["pair_mode"], "review_only")
            self.assertEqual(task["evaluator_tier"], "absolute")
            self.assertEqual(task["tier_phase"], "base")

            tick6 = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(tick6["actions"][0]["type"], "evaluation_tied")
            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["pair_mode"], "full_rematch")
            self.assertEqual(task["evaluator_tier"], "absolute")
            self.assertEqual(task["tier_phase"], "post_rematch")

            tick7 = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(tick7["actions"][-1]["type"], "evaluation_tied")
            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["pair_mode"], "review_only")
            self.assertEqual(task["evaluator_tier"], "absolute")
            self.assertEqual(task["tier_phase"], "post_rematch")

            final_tick = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            if final_tick["actions"][-1]["type"] != "evaluation":
                final_tick = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json", extra_env=env).stdout)
            self.assertEqual(final_tick["actions"][-1]["type"], "evaluation")
            self.assertTrue(final_tick["actions"][-1]["masterpiece_locked"])

            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["status"], "masterpiece_locked")
            self.assertEqual(task["champion_lane_id"], "worker-b")

    def test_evaluator_prefers_ready_retry_pair_over_newer_incomplete_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            self.run_cli(root, "submit", "--prompt", "Older task should finish its retry first")
            first_tick = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json").stdout)
            self.assertEqual(first_tick["actions"][2]["type"], "evaluation")
            self.assertEqual(first_tick["actions"][2]["task_id"], "T-0001")

            self.run_cli(root, "submit", "--prompt", "Newer task should not steal evaluator focus")
            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json").stdout)

            worker_actions = [action for action in second_tick["actions"] if action["type"] == "worker_submission"]
            self.assertEqual({action["task_id"] for action in worker_actions}, {"T-0001", "T-0002"})
            self.assertEqual(second_tick["actions"][2]["type"], "evaluation")
            self.assertEqual(second_tick["actions"][2]["task_id"], "T-0001")

            dashboard = json.loads(self.run_cli(root, "dashboard", "--json").stdout)
            self.assertEqual(dashboard["summary"]["ready_evaluations"], 0)
            t2 = next(task for task in dashboard["tasks"] if task["task_id"] == "T-0002")
            self.assertEqual(len(t2["scoreboard"]), 1)
            evaluator_lane = next(lane for lane in dashboard["lanes"] if lane["lane_id"] == "evaluator")
            self.assertEqual(evaluator_lane["ready_evaluation_tasks"], [])

    def test_daemon_run_until_idle_writes_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Background daemon task")
            self.run_cli(root, "daemon", "run", "--executor", "mock", "--interval", "0", "--until-idle", "--max-cycles", "10")

            daemon_state_path = root / "control" / "daemon" / "daemon-state.json"
            self.assertTrue(daemon_state_path.exists())
            state = json.loads(daemon_state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["reason"], "idle")
            self.assertFalse(state["running"])
            self.assertGreaterEqual(state["cycle_count"], 1)
            workspace_status = json.loads(self.run_cli(root, "workspace", "status", "--json").stdout)
            self.assertTrue(any(item["workspace_kind"] == "directory" for item in workspace_status["items"]))

    def test_daemon_start_status_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "daemon", "start", "--executor", "mock", "--interval", "0.1", "--max-cycles", "100")

            self.assertTrue(
                self.wait_for(
                    lambda: json.loads(self.run_cli(root, "daemon", "status", "--json").stdout)["running"],
                    timeout=5.0,
                )
            )

            stop_result = self.run_cli(root, "daemon", "stop", "--timeout", "5")
            self.assertIn("stopped=True", stop_result.stdout)

            status = json.loads(self.run_cli(root, "daemon", "status", "--json").stdout)
            self.assertFalse(status["running"])

    def test_daemon_auto_recovers_quota_blocked_lane_when_probe_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            self.write_fake_auth(login_home, email="codex-a@example.com", account_id="acct-a")
            env = self.make_fake_codex(
                root,
                [
                    {"payload": {"summary": "worker-a proposal", "body": "A body"}},
                    {
                        "stderr": (
                            "ERROR: You've hit your usage limit. Upgrade to Plus to continue using Codex, "
                            "or try again at Apr 13th, 2026 2:43 AM.\n"
                        ),
                        "exit_code": 1,
                    },
                    {"payload": {"status": "ok"}},
                    {"payload": {"summary": "worker-b retry", "body": "B repaired body"}},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.6,
                                "risk": 4.6,
                                "maintainability": 4.6,
                                "verification": 4.7,
                            },
                            "right_rubric": {
                                "correctness": 4.0,
                                "completeness": 4.0,
                                "risk": 4.0,
                                "maintainability": 4.0,
                                "verification": 4.0,
                            },
                            "rationale": "worker-a remains the clearer answer",
                            "loser_brief": "Add more structure and verification detail",
                        }
                    },
                ],
            )
            env["CODEXLAB_LOGIN_CODEX_HOME"] = str(login_home)

            self.run_cli(root, "submit", "--prompt", "Daemon quota auto resume path", extra_env=env)
            self.run_cli(root, "daemon", "run", "--executor", "codex", "--interval", "0", "--max-cycles", "2", extra_env=env)

            daemon_state = json.loads((root / "control" / "daemon" / "daemon-state.json").read_text(encoding="utf-8"))
            self.assertEqual(daemon_state["quota_monitor"]["login_identity"]["email"], "codex-a@example.com")
            self.assertTrue(daemon_state["quota_monitor"]["last_probe"]["ok"])
            self.assertEqual(daemon_state["last_actions"][0]["type"], "auto_recover")
            self.assertEqual(daemon_state["last_actions"][1]["type"], "worker_submission")
            self.assertEqual(daemon_state["last_actions"][2]["type"], "evaluation")

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertEqual(task["champion_lane_id"], "worker-a")
            self.assertEqual(task["status"], "challenger_retrying")

    def test_dashboard_json_includes_latest_evaluation_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Dashboard should expose evaluation context")
            self.run_cli(root, "tick", "--executor", "mock")

            dashboard = json.loads(self.run_cli(root, "dashboard", "--json").stdout)
            self.assertEqual(dashboard["summary"]["task_total"], 1)
            self.assertEqual(dashboard["summary"]["queued_reservations"], 0)
            self.assertEqual(dashboard["summary"]["lane_busy"], 1)
            self.assertTrue(dashboard["recent_events"])
            task = dashboard["tasks"][0]
            self.assertEqual(task["champion_submission"]["lane_id"], "worker-a")
            self.assertEqual(task["challenger_submission"]["lane_id"], "worker-b")
            self.assertEqual(task["latest_evaluation"]["winner_lane_id"], "worker-a")
            self.assertEqual(task["scoreboard"][0]["rank"], 1)
            self.assertEqual(task["scoreboard"][0]["role"], "champion")
            self.assertIn("Mock evaluator compared", task["latest_evaluation"]["rationale"])
            self.assertIn("correctness gaps", task["latest_evaluation"]["loser_brief"])
            self.assertTrue(dashboard["execution_state"])
            self.assertTrue(dashboard["execution_reason"])
            self.assertTrue(task["display_state"])
            self.assertTrue(task["display_reason"])

    def test_evaluator_output_schema_requires_all_properties(self) -> None:
        module = self.load_module()
        schema = module.evaluator_output_schema()
        self.assertEqual(set(schema["required"]), set(schema["properties"]))
        self.assertIn("loser_brief", schema["required"])
        self.assertIn("rematch_brief", schema["required"])

    def test_default_worker_template_targets_self_improvement_not_rebuttal(self) -> None:
        module = self.load_module()
        template = module.DEFAULT_WORKER_TEMPLATE
        self.assertIn("standalone answer", template)
        self.assertIn("Do not write a rebuttal", template)
        self.assertNotIn("explicitly answer the current published champion", template)

    def test_default_evaluator_template_frames_briefs_as_optional_pressure_notes(self) -> None:
        module = self.load_module()
        template = module.DEFAULT_EVALUATOR_TEMPLATE
        self.assertIn("improve its own submission", template)
        self.assertIn("optional considerations", template)
        self.assertIn("standalone answers", template)

    def test_is_resume_rejection_text_treats_resume_cli_parse_errors_as_recoverable(self) -> None:
        module = self.load_module()
        self.assertTrue(
            module.is_resume_rejection_text(
                "error: unexpected argument '--color' found\n\nUsage: codex exec resume [OPTIONS] [SESSION_ID] [PROMPT]\n"
            )
        )

    def test_tick_rebalances_waiting_lanes_with_cross_task_tie_rematches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Cross-task tie rematch A")
            self.run_cli(root, "submit", "--prompt", "Cross-task tie rematch B")

            conn = self.connect_db(root)
            now = self.now_utc()
            conn.execute(
                """
                UPDATE tasks
                SET status = 'worker_rematching',
                    duel_stage = 'tiebreak',
                    pair_mode = 'full_rematch',
                    evaluator_tier = 'primary',
                    tier_phase = 'post_rematch',
                    updated_at = ?
                WHERE task_id IN ('T-0001', 'T-0002')
                """,
                (now,),
            )
            conn.execute(
                """
                UPDATE lanes
                SET status = 'retrying', active_task_id = 'T-0001', active_submission_id = NULL, active_run_id = NULL, notes = '', updated_at = ?
                WHERE lane_id = 'worker-a'
                """,
                (now,),
            )
            conn.execute(
                """
                UPDATE lanes
                SET status = 'retrying', active_task_id = 'T-0002', active_submission_id = NULL, active_run_id = NULL, notes = '', updated_at = ?
                WHERE lane_id = 'worker-b'
                """,
                (now,),
            )
            conn.commit()
            conn.close()

            self.run_cli(root, "record-submission", "T-0001", "worker-a", "--summary", "worker-a rematch seed", "--body", "worker-a rematch body")
            self.run_cli(root, "record-submission", "T-0002", "worker-b", "--summary", "worker-b rematch seed", "--body", "worker-b rematch body")

            conn = self.connect_db(root)
            self.queue_reservation(conn, lane_id="worker-a", task_id="T-0002", reservation_type="tie_rematch")
            self.queue_reservation(conn, lane_id="worker-b", task_id="T-0001", reservation_type="tie_rematch")
            conn.commit()
            conn.close()

            tick = json.loads(self.run_cli(root, "tick", "--executor", "mock", "--json").stdout)
            action_types = [item["type"] for item in tick["actions"]]
            self.assertEqual(action_types[:2], ["worker_submission", "worker_submission"])
            self.assertIn(action_types[-1], {"evaluation", "evaluation_tied"})

    def test_watch_once_dashboard_renders_operator_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Watch dashboard once")
            self.run_cli(root, "tick", "--executor", "mock")

            result = self.run_cli(root, "watch", "--dashboard", "--once")
            self.assertIn("CodexLab Dashboard", result.stdout)
            self.assertIn("Execution:", result.stdout)
            self.assertIn("Stage:", result.stdout)
            self.assertIn("State:", result.stdout)
            self.assertIn("Next:", result.stdout)
            self.assertIn("Ready judging:", result.stdout)
            self.assertIn("Scorecards:", result.stdout)
            self.assertIn("Latest Decision:", result.stdout)
            self.assertIn("Improvement notes:", result.stdout)
            self.assertIn("Crowd reaction:", result.stdout)
            self.assertIn("Recent Activity:", result.stdout)

    def test_watch_once_stream_renders_progress_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Watch stream once")
            self.run_cli(root, "tick", "--executor", "mock")

            result = self.run_cli(root, "watch", "T-0001", "--once")
            self.assertIn("CodexLab Event Stream | task=T-0001", result.stdout)
            self.assertIn("Progress:", result.stdout)
            self.assertIn("Execution:", result.stdout)
            self.assertRegex(result.stdout, r"\[(READY|WAITING|RUNNING|BLOCKED|DONE)\]")
            self.assertIn("state:", result.stdout)
            self.assertIn("next:", result.stdout)
            self.assertIn("scorecards:", result.stdout)
            self.assertIn("crowd:", result.stdout)

    def test_watch_once_stream_marks_done_for_finished_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Watch stream done state")
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10")

            result = self.run_cli(root, "watch", "T-0001", "--once")
            self.assertIn("CodexLab Event Stream | task=T-0001", result.stdout)
            self.assertIn("[DONE]", result.stdout)
            self.assertIn("Champion confirmed", result.stdout)

    def test_watch_once_dashboard_marks_masterpiece_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Masterpiece should be explicit in the dashboard")
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10")

            result = self.run_cli(root, "watch", "--dashboard", "--once")
            self.assertIn("CHAMPION CONFIRMED", result.stdout)
            self.assertIn("Outcome: CHAMPION CONFIRMED", result.stdout)

    def test_watch_once_dashboard_renders_quota_monitor_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Quota dashboard visibility")

            daemon_state = {
                "pid": 99999,
                "executor": "codex",
                "interval": 1.0,
                "cycle_count": 7,
                "started_at": self.now_utc(),
                "last_heartbeat": self.now_utc(),
                "last_progress": False,
                "runnable_after": False,
                "stop_requested": False,
                "reason": "running",
                "last_actions": [],
                "quota_monitor": {
                    "status": "quota_blocked",
                    "login_identity": {"email": "quota@example.com"},
                    "blocked_lanes": [{"lane_id": "worker-b", "task_id": "T-0001"}],
                    "last_probe_at": self.now_utc(),
                    "last_probe": {
                        "ok": False,
                        "quota_blocked": True,
                        "message": "You've hit your usage limit. Upgrade to Plus to continue using Codex.",
                    },
                },
            }
            daemon_state_path = root / "control" / "daemon" / "daemon-state.json"
            daemon_state_path.parent.mkdir(parents=True, exist_ok=True)
            daemon_state_path.write_text(json.dumps(daemon_state, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

            result = self.run_cli(root, "watch", "--dashboard", "--once")
            self.assertIn("Quota monitor:", result.stdout)
            self.assertIn("quota@example.com", result.stdout)
            self.assertIn("Blue Corner", result.stdout)
            self.assertIn("usage limit", result.stdout.lower())

    def test_git_worktree_workspace_is_used_when_target_repo_has_head(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "project"
            self.init_git_repo(repo)
            env = {"CODEXLAB_TARGET_REPO": str(repo)}

            self.run_cli(root, "submit", "--prompt", "Use git worktree isolation", extra_env=env)
            self.run_cli(root, "tick", "--executor", "mock", extra_env=env)

            manifest_path = root / "agents" / "worker-a" / "workspace" / "worktrees" / "T-0001" / "codexlab-task.json"
            self.assertTrue(manifest_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["workspace_kind"], "git-worktree")
            self.assertEqual(manifest["repo_root"], str(repo.resolve()))
            git_check = subprocess.run(
                ["git", "-C", str(manifest_path.parent), "rev-parse", "--is-inside-work-tree"],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(git_check.stdout.strip(), "true")
            workspace_status = json.loads(self.run_cli(root, "workspace", "status", "--json", extra_env=env).stdout)
            self.assertTrue(any(item["workspace_kind"] == "git-worktree" for item in workspace_status["items"]))

    def test_workspace_clean_removes_finished_directory_workspaces(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Cleanup finished directories")
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10")

            manifest = root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json"
            self.assertTrue(manifest.exists())

            dry_run = json.loads(self.run_cli(root, "workspace", "clean", "--task-id", "T-0001", "--dry-run", "--json").stdout)
            self.assertEqual(dry_run["matched"], 3)
            self.assertTrue(manifest.exists())

            result = json.loads(self.run_cli(root, "workspace", "clean", "--task-id", "T-0001", "--json").stdout)
            self.assertEqual(len(result["removed"]), 3)
            self.assertFalse(manifest.exists())

    def test_workspace_clean_removes_finished_git_worktrees(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "project"
            self.init_git_repo(repo)
            env = {"CODEXLAB_TARGET_REPO": str(repo)}

            self.run_cli(root, "submit", "--prompt", "Cleanup git worktrees", extra_env=env)
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10", extra_env=env)

            worktree_manifest = root / "agents" / "worker-a" / "workspace" / "worktrees" / "T-0001" / "codexlab-task.json"
            self.assertTrue(worktree_manifest.exists())
            result = json.loads(self.run_cli(root, "workspace", "clean", "--task-id", "T-0001", "--json", extra_env=env).stdout)
            self.assertEqual(len(result["removed"]), 3)
            self.assertFalse(worktree_manifest.exists())
            listing = subprocess.run(
                ["git", "-C", str(repo), "worktree", "list", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertNotIn(str(worktree_manifest.parent), listing.stdout)

    def test_workspace_clean_skips_active_task_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Active task should not be cleaned")
            self.run_cli(root, "tick", "--executor", "mock")

            result = json.loads(self.run_cli(root, "workspace", "clean", "--task-id", "T-0001", "--json").stdout)
            self.assertEqual(result["matched"], 0)
            manifest = root / "agents" / "worker-b" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json"
            self.assertTrue(manifest.exists())

    def test_recover_marks_stale_runs_abandoned_and_reopens_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Recover stranded worker lane")

            conn = self.connect_db(root)
            conn.execute(
                """
                INSERT INTO runs(
                    run_id, lane_id, task_id, submission_id, mode, status, command, cwd, codex_home, started_at, finished_at, exit_code
                )
                VALUES(?, ?, ?, NULL, ?, 'running', ?, ?, ?, ?, NULL, NULL)
                """,
                (
                    "RUN-9999",
                    "worker-a",
                    "T-0001",
                    "codex:worker",
                    json.dumps(["fake-codex"], ensure_ascii=True),
                    str(root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001"),
                    str(root / "agents" / "worker-a" / "home"),
                    self.now_utc(),
                ),
            )
            conn.execute(
                """
                UPDATE lanes
                SET active_run_id = ?, status = 'assigned', updated_at = ?
                WHERE lane_id = 'worker-a'
                """,
                ("RUN-9999", self.now_utc()),
            )
            conn.commit()
            conn.close()

            preview = json.loads(self.run_cli(root, "recover", "--json").stdout)
            self.assertEqual(preview["abandoned_run_count"], 1)
            self.assertEqual(preview["repaired_lane_count"], 1)
            self.assertEqual(preview["lane_repairs"][0]["next_status"], "assigned")

            applied = json.loads(self.run_cli(root, "recover", "--apply", "--json").stdout)
            self.assertTrue(applied["applied"])
            self.assertEqual(applied["abandoned_runs"], ["RUN-9999"])
            self.assertEqual(applied["repaired_lanes"], ["worker-a"])

            status = self.status_json(root)
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            self.assertIsNone(lanes["worker-a"]["active_run_id"])
            self.assertEqual(lanes["worker-a"]["status"], "assigned")

            conn = self.connect_db(root)
            recovered_run = conn.execute("SELECT * FROM runs WHERE run_id = 'RUN-9999'").fetchone()
            conn.close()
            self.assertEqual(recovered_run["status"], "abandoned")
            self.assertEqual(recovered_run["exit_code"], 125)

    def test_codex_executor_uses_fake_binary_and_records_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self.make_fake_codex(
                root,
                [
                    {"payload": {"summary": "worker-a proposal", "body": "A body"}, "stdout": "worker-a ok\n"},
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}, "stdout": "worker-b ok\n"},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.2,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "left is stronger",
                            "loser_brief": "Improve specificity and verification",
                        },
                        "stdout": "evaluator ok\n",
                    },
                ],
            )
            self.run_cli(root, "submit", "--prompt", "Codex executor success path", extra_env=env)
            tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(len(tick["actions"]), 3)

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertEqual(task["champion_lane_id"], "worker-a")
            self.assertEqual(task["published_submission_id"], "S-0001")

            runs = json.loads(self.run_cli(root, "runs", "list", "--json", extra_env=env).stdout)["runs"]
            self.assertEqual(len(runs), 3)
            self.assertTrue(all(run["status"] == "completed" for run in runs))

            worker_runs = [run for run in runs if run["lane_id"] in {"worker-a", "worker-b"}]
            worker_details = [
                json.loads(self.run_cli(root, "runs", "show", run["run_id"], "--json", extra_env=env).stdout)
                for run in worker_runs
            ]
            worker_stdout = "\n".join(detail["artifacts"]["stdout.log"] for detail in worker_details)
            self.assertIn("worker-a ok", worker_stdout)
            self.assertIn("worker-b ok", worker_stdout)
            self.assertTrue(all("--cd" in detail["artifacts"]["command.json"] for detail in worker_details))
            self.assertTrue(all("--output-last-message" in detail["artifacts"]["command.json"] for detail in worker_details))

    def test_codex_executor_uses_login_home_override_and_lane_runtime_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            capture_path = root / "captures" / "worker-a-env.json"
            login_home = root / "login-home"
            env = self.make_fake_codex(
                root,
                [
                    {
                        "payload": {"summary": "worker-a proposal", "body": "A body"},
                        "capture_env": ["CODEX_HOME"],
                        "capture_env_path": str(capture_path),
                    },
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.2,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "left is stronger",
                            "loser_brief": "Improve specificity and verification",
                        }
                    },
                ],
            )
            env["CODEXLAB_LOGIN_CODEX_HOME"] = str(login_home)

            self.run_cli(root, "submit", "--prompt", "Login home override path", extra_env=env)
            self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env)

            captured = json.loads(capture_path.read_text(encoding="utf-8"))
            self.assertEqual(captured["CODEX_HOME"], str(login_home))

            run_detail = json.loads(self.run_cli(root, "runs", "show", "RUN-0001", "--json", "--full", extra_env=env).stdout)
            command_artifact = run_detail["artifacts"]["command.json"]
            self.assertIn(str(root / "agents" / "worker-a" / "home" / "log"), command_artifact)
            self.assertIn(str(root / "agents" / "worker-a" / "home" / "sqlite"), command_artifact)
            self.assertIn("--cd", command_artifact)

    def test_codex_executor_starts_opening_workers_concurrently(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            worker_a_started = root / "captures" / "worker-a.started"
            worker_b_started = root / "captures" / "worker-b.started"
            env = self.make_fake_codex(
                root,
                [
                    {
                        "sleep": 0.2,
                        "capture_started_path": str(worker_a_started),
                        "payload": {"summary": "worker-a proposal", "body": "A body"},
                    },
                    {
                        "sleep": 0.2,
                        "capture_started_path": str(worker_b_started),
                        "payload": {"summary": "worker-b proposal", "body": "B body"},
                    },
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.2,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "left is stronger",
                            "loser_brief": "Improve specificity and verification",
                        }
                    },
                ],
            )
            self.run_cli(root, "submit", "--prompt", "Concurrent opening bout", extra_env=env)
            tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual([item["type"] for item in tick["actions"]], ["worker_submission", "worker_submission", "evaluation"])

            start_a = float(worker_a_started.read_text(encoding="utf-8").strip())
            start_b = float(worker_b_started.read_text(encoding="utf-8").strip())
            self.assertLess(abs(start_a - start_b), 0.15)

    def test_codex_executor_reuses_task_scoped_lane_sessions_after_opening_bout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            env = self.make_fake_codex(
                root,
                [
                    {
                        "session_id": "sess-worker-a",
                        "payload": {"summary": "worker-a proposal", "body": "A body"},
                    },
                    {
                        "session_id": "sess-worker-b",
                        "payload": {"summary": "worker-b proposal", "body": "B body"},
                    },
                    {
                        "session_id": "sess-evaluator",
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.2,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "left is stronger",
                            "loser_brief": "Improve specificity and verification",
                            "rematch_brief": "",
                        },
                    },
                    {
                        "payload": {"summary": "worker-b retry", "body": "B revised body"},
                    },
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.6,
                                "risk": 4.5,
                                "maintainability": 4.6,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.3,
                                "completeness": 4.2,
                                "risk": 4.1,
                                "maintainability": 4.2,
                                "verification": 4.1,
                            },
                            "rationale": "left still wins after the rematch",
                            "loser_brief": "Not used because the bout locks now",
                            "rematch_brief": "",
                        },
                    },
                ],
            )
            env["CODEXLAB_LOGIN_CODEX_HOME"] = str(login_home)

            self.run_cli(root, "submit", "--prompt", "Session reuse path", extra_env=env)
            first_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual([item["type"] for item in first_tick["actions"]], ["worker_submission", "worker_submission", "evaluation"])

            task = self.status_json(root, "T-0001")["tasks"][0]
            worker_a_session_id = task["worker_a_session_id"]
            worker_b_session_id = task["worker_b_session_id"]
            self.assertIn(worker_a_session_id, {"sess-worker-a", "sess-worker-b"})
            self.assertIn(worker_b_session_id, {"sess-worker-a", "sess-worker-b"})
            self.assertNotEqual(worker_a_session_id, worker_b_session_id)
            self.assertEqual(task["evaluator_session_id"], "sess-evaluator")
            self.write_fake_session_file(
                login_home,
                session_id=worker_a_session_id,
                cwd=root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001",
            )
            self.write_fake_session_file(
                login_home,
                session_id=worker_b_session_id,
                cwd=root / "agents" / "worker-b" / "workspace" / "tasks" / "T-0001",
            )
            self.write_fake_session_file(
                login_home,
                session_id="sess-evaluator",
                cwd=root / "agents" / "evaluator" / "workspace" / "tasks" / "T-0001",
            )

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual([item["type"] for item in second_tick["actions"]], ["worker_submission", "evaluation"])

            retry_run = json.loads(self.run_cli(root, "runs", "show", "RUN-0004", "--json", "--full", extra_env=env).stdout)
            self.assertEqual(retry_run["run"]["used_resume"], 1)
            self.assertEqual(retry_run["run"]["session_id"], worker_b_session_id)
            self.assertEqual(retry_run["run"]["prompt_style"], "resume-delta")
            self.assertIn("resume", retry_run["artifacts"]["command.json"])
            self.assertNotIn("--color", retry_run["artifacts"]["command.json"])
            self.assertIn(worker_b_session_id, retry_run["artifacts"]["command.json"])

            evaluator_run = json.loads(self.run_cli(root, "runs", "show", "RUN-0005", "--json", "--full", extra_env=env).stdout)
            self.assertEqual(evaluator_run["run"]["used_resume"], 1)
            self.assertEqual(evaluator_run["run"]["session_id"], "sess-evaluator")
            self.assertEqual(evaluator_run["run"]["prompt_style"], "resume-delta")
            self.assertIn("resume", evaluator_run["artifacts"]["command.json"])
            self.assertNotIn("--color", evaluator_run["artifacts"]["command.json"])
            self.assertIn("sess-evaluator", evaluator_run["artifacts"]["command.json"])

            final_task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(final_task["status"], "masterpiece_locked")

    def test_codex_executor_cold_falls_back_when_stored_session_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            env = self.make_fake_codex(
                root,
                [
                    {
                        "session_id": "old-worker-a",
                        "payload": {"summary": "worker-a proposal", "body": "A body"},
                    },
                    {
                        "session_id": "old-worker-b",
                        "payload": {"summary": "worker-b proposal", "body": "B body"},
                    },
                    {
                        "session_id": "old-evaluator",
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.2,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "left is stronger",
                            "loser_brief": "Improve specificity and verification",
                            "rematch_brief": "",
                        },
                    },
                    {
                        "session_id": "new-worker-b",
                        "payload": {"summary": "worker-b retry", "body": "B revised body"},
                    },
                    {
                        "session_id": "new-evaluator",
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.6,
                                "risk": 4.5,
                                "maintainability": 4.6,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.3,
                                "completeness": 4.2,
                                "risk": 4.1,
                                "maintainability": 4.2,
                                "verification": 4.1,
                            },
                            "rationale": "left still wins after the rematch",
                            "loser_brief": "Not used because the bout locks now",
                            "rematch_brief": "",
                        },
                    },
                ],
            )
            env["CODEXLAB_LOGIN_CODEX_HOME"] = str(login_home)

            self.run_cli(root, "submit", "--prompt", "Missing session should cold fall back", extra_env=env)
            self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env)

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual([item["type"] for item in second_tick["actions"]], ["worker_submission", "evaluation"])

            retry_run = json.loads(self.run_cli(root, "runs", "show", "RUN-0004", "--json", "--full", extra_env=env).stdout)
            self.assertEqual(retry_run["run"]["used_resume"], 0)
            self.assertEqual(retry_run["run"]["prompt_style"], "full")
            self.assertNotIn("resume", retry_run["artifacts"]["command.json"])

            evaluator_run = json.loads(self.run_cli(root, "runs", "show", "RUN-0005", "--json", "--full", extra_env=env).stdout)
            self.assertEqual(evaluator_run["run"]["used_resume"], 0)
            self.assertEqual(evaluator_run["run"]["prompt_style"], "full")
            self.assertNotIn("resume", evaluator_run["artifacts"]["command.json"])

            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["worker_b_session_id"], "new-worker-b")
            self.assertEqual(task["evaluator_session_id"], "new-evaluator")

    def test_codex_executor_cold_falls_back_when_resume_is_rejected_internally(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            login_home = root / "login-home"
            env = self.make_fake_codex(
                root,
                [
                    {
                        "session_id": "old-worker-a",
                        "payload": {"summary": "worker-a proposal", "body": "A body"},
                    },
                    {
                        "session_id": "old-worker-b",
                        "payload": {"summary": "worker-b proposal", "body": "B body"},
                    },
                    {
                        "session_id": "old-evaluator",
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.2,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "left is stronger",
                            "loser_brief": "Improve specificity and verification",
                            "rematch_brief": "",
                        },
                    },
                    {
                        "exit_code": 1,
                        "stderr": "Error: thread/resume: thread/resume failed: no rollout found for thread id old-worker-b\n",
                    },
                    {
                        "session_id": "new-worker-b",
                        "payload": {"summary": "worker-b retry", "body": "B revised body"},
                    },
                    {
                        "exit_code": 1,
                        "stderr": "Error: thread/resume: thread/resume failed: no rollout found for thread id old-evaluator\n",
                    },
                    {
                        "session_id": "new-evaluator",
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.6,
                                "risk": 4.5,
                                "maintainability": 4.6,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.3,
                                "completeness": 4.2,
                                "risk": 4.1,
                                "maintainability": 4.2,
                                "verification": 4.1,
                            },
                            "rationale": "left still wins after the rematch",
                            "loser_brief": "Not used because the bout locks now",
                            "rematch_brief": "",
                        },
                    },
                ],
            )
            env["CODEXLAB_LOGIN_CODEX_HOME"] = str(login_home)

            self.run_cli(root, "submit", "--prompt", "Rejected resume should cold fall back", extra_env=env)
            self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env)

            self.write_fake_session_file(
                login_home,
                session_id="old-worker-a",
                cwd=root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001",
            )
            self.write_fake_session_file(
                login_home,
                session_id="old-worker-b",
                cwd=root / "agents" / "worker-b" / "workspace" / "tasks" / "T-0001",
            )
            self.write_fake_session_file(
                login_home,
                session_id="old-evaluator",
                cwd=root / "agents" / "evaluator" / "workspace" / "tasks" / "T-0001",
            )

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual([item["type"] for item in second_tick["actions"]], ["worker_submission", "evaluation"])

            retry_run = json.loads(self.run_cli(root, "runs", "show", "RUN-0004", "--json", "--full", extra_env=env).stdout)
            self.assertEqual(retry_run["run"]["used_resume"], 0)
            self.assertEqual(retry_run["run"]["prompt_style"], "full")
            self.assertEqual(retry_run["run"]["session_id"], "new-worker-b")
            self.assertTrue((root / "control" / "runs" / "RUN-0004" / "resume-attempt-command.json").exists())
            self.assertIn("resume", (root / "control" / "runs" / "RUN-0004" / "resume-attempt-command.json").read_text(encoding="utf-8"))
            self.assertIn("old-worker-b", (root / "control" / "runs" / "RUN-0004" / "resume-attempt-command.json").read_text(encoding="utf-8"))
            self.assertIn("thread/resume failed", (root / "control" / "runs" / "RUN-0004" / "resume-attempt-stderr.log").read_text(encoding="utf-8"))

            evaluator_run = json.loads(self.run_cli(root, "runs", "show", "RUN-0005", "--json", "--full", extra_env=env).stdout)
            self.assertEqual(evaluator_run["run"]["used_resume"], 0)
            self.assertEqual(evaluator_run["run"]["prompt_style"], "full")
            self.assertEqual(evaluator_run["run"]["session_id"], "new-evaluator")
            self.assertTrue((root / "control" / "runs" / "RUN-0005" / "resume-attempt-command.json").exists())
            self.assertIn("resume", (root / "control" / "runs" / "RUN-0005" / "resume-attempt-command.json").read_text(encoding="utf-8"))
            self.assertIn("old-evaluator", (root / "control" / "runs" / "RUN-0005" / "resume-attempt-command.json").read_text(encoding="utf-8"))
            self.assertIn("thread/resume failed", (root / "control" / "runs" / "RUN-0005" / "resume-attempt-stderr.log").read_text(encoding="utf-8"))

            task = self.status_json(root, "T-0001")["tasks"][0]
            self.assertEqual(task["status"], "masterpiece_locked")
            self.assertEqual(task["worker_b_session_id"], "new-worker-b")
            self.assertEqual(task["evaluator_session_id"], "new-evaluator")

    def test_codex_executor_uses_git_worktree_cwd_when_target_repo_has_head(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "project"
            self.init_git_repo(repo)
            env = self.make_fake_codex(
                root,
                [
                    {
                        "payload": {"summary": "worker-a proposal", "body": "A body"},
                        "write_file": "worker-a.marker",
                        "write_text": "worker-a wrote inside the prepared workspace\n",
                    },
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.8,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.2,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "left is stronger",
                            "loser_brief": "Improve specificity and verification",
                        }
                    },
                ],
            )
            env["CODEXLAB_TARGET_REPO"] = str(repo)

            self.run_cli(root, "submit", "--prompt", "Codex executor should use a git worktree", extra_env=env)
            tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(len(tick["actions"]), 3)

            worktree_root = root / "agents" / "worker-a" / "workspace" / "worktrees" / "T-0001"
            self.assertTrue((worktree_root / "codexlab-task.json").exists())
            self.assertTrue((worktree_root / "worker-a.marker").exists())
            self.assertFalse((root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001" / "worker-a.marker").exists())

    def test_codex_executor_timeout_marks_run_and_lane_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self.make_fake_codex(
                root,
                [
                    {"sleep": 0.2, "payload": {"summary": "slow worker", "body": "slow body"}},
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}},
                ],
            )
            self.run_cli(root, "submit", "--prompt", "Timeout path", extra_env=env)
            tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "0.05", "--json", extra_env=env).stdout)
            worker_errors = [action for action in tick["actions"] if action["type"] == "worker_error"]
            self.assertEqual(len(worker_errors), 1)
            self.assertIn("timed out", worker_errors[0]["error"])

            status = self.status_json(root)
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            self.assertEqual(lanes["worker-a"]["status"], "error")

            runs = json.loads(self.run_cli(root, "runs", "list", "--json", extra_env=env).stdout)["runs"]
            timeout_run = next(run for run in runs if run["lane_id"] == "worker-a")
            self.assertEqual(timeout_run["status"], "timeout")

            run_detail = json.loads(self.run_cli(root, "runs", "show", timeout_run["run_id"], "--json", extra_env=env).stdout)
            self.assertIn("prompt.txt", run_detail["files"]["prompt"])

    def test_recover_can_resume_worker_after_malformed_worker_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self.make_fake_codex(
                root,
                [
                    {"payload": {"summary": "broken worker-a payload"}},
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}},
                    {"payload": {"summary": "worker-a retry", "body": "A repaired body"}},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.7,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.2,
                                "completeness": 4.1,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.1,
                            },
                            "rationale": "worker-a recovered with the stronger draft",
                            "loser_brief": "Add more verification detail",
                        }
                    },
                ],
            )
            self.run_cli(root, "submit", "--prompt", "Worker malformed output recovery path", extra_env=env)
            first_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual({action["type"] for action in first_tick["actions"]}, {"worker_error", "worker_submission"})

            preview = json.loads(self.run_cli(root, "recover", "--json", extra_env=env).stdout)
            self.assertEqual(preview["repaired_lane_count"], 1)
            repaired_lane = preview["lane_repairs"][0]["lane_id"]
            self.assertIn(repaired_lane, {"worker-a", "worker-b"})
            self.assertEqual(preview["lane_repairs"][0]["repair_source"], "error_state")

            applied = json.loads(self.run_cli(root, "recover", "--apply", "--json", extra_env=env).stdout)
            self.assertIn(repaired_lane, applied["repaired_lanes"])

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(second_tick["actions"][0]["type"], "worker_submission")
            self.assertEqual(second_tick["actions"][1]["type"], "evaluation")

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertIn(task["champion_lane_id"], {"worker-a", "worker-b"})
            self.assertEqual(task["status"], "challenger_retrying")

    def test_recover_apply_requeue_defers_worker_until_next_tick(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self.make_fake_codex(
                root,
                [
                    {"payload": {"summary": "broken worker-a payload"}},
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}},
                    {"payload": {"summary": "worker-a retry", "body": "A repaired body"}},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.7,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.2,
                                "completeness": 4.1,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.1,
                            },
                            "rationale": "worker-a recovered with the stronger draft",
                            "loser_brief": "Add more verification detail",
                        }
                    },
                ],
            )
            self.run_cli(root, "submit", "--prompt", "Recover and requeue worker lane", extra_env=env)
            first_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual({action["type"] for action in first_tick["actions"]}, {"worker_error", "worker_submission"})

            recovered = json.loads(
                self.run_cli(
                    root,
                    "recover",
                    "--apply",
                    "--requeue",
                    "--json",
                    extra_env=env,
                ).stdout
            )
            self.assertTrue(recovered["applied"])
            self.assertEqual(len(recovered["repaired_lanes"]), 1)
            repaired_lane = recovered["repaired_lanes"][0]
            self.assertIn(repaired_lane, {"worker-a", "worker-b"})
            self.assertEqual(recovered["requeued_reservation_count"], 1)
            self.assertEqual(recovered["requeued_reservations"][0]["reservation_type"], "recovered_task")

            status_after_recover = self.status_json(root)
            lanes_after_recover = {lane["lane_id"]: lane for lane in status_after_recover["lanes"]}
            self.assertEqual(lanes_after_recover[repaired_lane]["status"], "idle")
            self.assertTrue(
                any(
                    entry["task_id"] == "T-0001" and entry["reservation_type"] == "recovered_task"
                    for entry in lanes_after_recover[repaired_lane]["queued_reservations"]
                )
            )

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(second_tick["actions"][0]["type"], "worker_submission")
            self.assertEqual(second_tick["actions"][1]["type"], "evaluation")

            final_status = self.status_json(root, "T-0001")
            task = final_status["tasks"][0]
            self.assertIn(task["champion_lane_id"], {"worker-a", "worker-b"})
            self.assertEqual(task["status"], "challenger_retrying")

    def test_recover_can_restart_daemon_after_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Restart daemon after repair")
            self.run_cli(root, "record-submission", "T-0001", "worker-b", "--summary", "worker-b seed", "--body", "worker-b body")

            conn = self.connect_db(root)
            conn.execute(
                """
                UPDATE lanes
                SET status = 'error', notes = 'simulated worker failure', updated_at = ?
                WHERE lane_id = 'worker-a'
                """,
                (self.now_utc(),),
            )
            conn.commit()
            conn.close()

            recovered = json.loads(
                self.run_cli(
                    root,
                    "recover",
                    "--apply",
                    "--restart-daemon",
                    "--executor",
                    "mock",
                    "--interval",
                    "0",
                    "--daemon-max-cycles",
                    "20",
                    "--until-idle",
                    "--json",
                ).stdout
            )
            self.assertTrue(recovered["applied"])
            self.assertEqual(recovered["repaired_lanes"], ["worker-a"])
            self.assertTrue(recovered["daemon_restart"]["started"])

            self.assertTrue(
                self.wait_for(
                    lambda: (
                        (payload := json.loads(self.run_cli(root, "daemon", "status", "--json").stdout))
                    )
                    and (not payload["running"])
                    and payload["state"].get("reason") == "idle",
                    timeout=5.0,
                )
            )

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertEqual(task["status"], "masterpiece_locked")
            self.assertEqual(task["champion_lane_id"], "worker-a")

    def test_recover_apply_resume_continues_scheduler_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self.make_fake_codex(
                root,
                [
                    {"payload": {"summary": "broken worker-a payload"}},
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}},
                    {"payload": {"summary": "worker-a retry", "body": "A repaired body"}},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.7,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.5,
                                "verification": 4.6,
                            },
                            "right_rubric": {
                                "correctness": 4.2,
                                "completeness": 4.1,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.1,
                            },
                            "rationale": "worker-a recovered with the stronger draft",
                            "loser_brief": "Add more verification detail",
                        }
                    },
                ],
            )
            self.run_cli(root, "submit", "--prompt", "Recover and resume in one command", extra_env=env)
            first_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual({action["type"] for action in first_tick["actions"]}, {"worker_error", "worker_submission"})

            recovered = json.loads(
                self.run_cli(
                    root,
                    "recover",
                    "--apply",
                    "--resume",
                    "--executor",
                    "codex",
                    "--exec-timeout",
                    "2",
                    "--max-ticks",
                    "1",
                    "--json",
                    extra_env=env,
                ).stdout
            )
            self.assertTrue(recovered["applied"])
            self.assertEqual(len(recovered["repaired_lanes"]), 1)
            self.assertIn(recovered["repaired_lanes"][0], {"worker-a", "worker-b"})
            self.assertEqual(len(recovered["resume"]["ticks"]), 1)
            self.assertEqual(recovered["resume"]["ticks"][0]["actions"][0]["type"], "worker_submission")
            self.assertEqual(recovered["resume"]["ticks"][0]["actions"][1]["type"], "evaluation")

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertIn(task["champion_lane_id"], {"worker-a", "worker-b"})

    def test_recover_can_resume_evaluator_after_malformed_evaluator_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self.make_fake_codex(
                root,
                [
                    {"payload": {"summary": "worker-a proposal", "body": "A body"}},
                    {"payload": {"summary": "worker-b proposal", "body": "B body"}},
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.4,
                                "completeness": 4.3,
                                "risk": 4.2,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "right_rubric": {
                                "correctness": 4.1,
                                "completeness": 4.0,
                                "risk": 4.0,
                                "maintainability": 4.1,
                                "verification": 4.0,
                            },
                            "rationale": "missing loser brief should fail"
                        }
                    },
                    {
                        "payload": {
                            "left_rubric": {
                                "correctness": 4.6,
                                "completeness": 4.5,
                                "risk": 4.4,
                                "maintainability": 4.4,
                                "verification": 4.5,
                            },
                            "right_rubric": {
                                "correctness": 4.2,
                                "completeness": 4.1,
                                "risk": 4.0,
                                "maintainability": 4.0,
                                "verification": 4.0,
                            },
                            "rationale": "worker-a remains ahead",
                            "loser_brief": "Strengthen risk handling and verification"
                        }
                    },
                ],
            )
            self.run_cli(root, "submit", "--prompt", "Evaluator malformed output recovery path", extra_env=env)
            first_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(first_tick["actions"][2]["type"], "evaluation_error")
            self.assertIn("loser_brief", first_tick["actions"][2]["error"])

            preview = json.loads(self.run_cli(root, "recover", "--json", extra_env=env).stdout)
            self.assertEqual(preview["repaired_lane_count"], 1)
            self.assertEqual(preview["lane_repairs"][0]["lane_id"], "evaluator")
            self.assertEqual(preview["lane_repairs"][0]["repair_source"], "error_state")

            self.run_cli(root, "recover", "--apply", "--json", extra_env=env)
            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(len(second_tick["actions"]), 1)
            self.assertEqual(second_tick["actions"][0]["type"], "evaluation")

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertEqual(task["champion_lane_id"], "worker-a")
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            self.assertEqual(lanes["evaluator"]["status"], "idle")

    def test_manual_score_accepts_rubric_json_and_persists_scorecard(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Manual rubric score path")
            self.run_cli(root, "record-submission", "T-0001", "worker-a", "--summary", "left candidate", "--body", "left body")
            self.run_cli(root, "record-submission", "T-0001", "worker-b", "--summary", "right candidate", "--body", "right body")

            left_rubric = json.dumps(
                {
                    "correctness": 5,
                    "completeness": 4.8,
                    "risk": 4.5,
                    "maintainability": 4.6,
                    "verification": 4.7,
                }
            )
            right_rubric = json.dumps(
                {
                    "correctness": 4.1,
                    "completeness": 4.0,
                    "risk": 4.2,
                    "maintainability": 4.0,
                    "verification": 4.1,
                }
            )
            self.run_cli(
                root,
                "score",
                "T-0001",
                "--left",
                "S-0001",
                "--right",
                "S-0002",
                "--left-rubric-json",
                left_rubric,
                "--right-rubric-json",
                right_rubric,
                "--rationale",
                "left wins on correctness and verification",
            )

            conn = self.connect_db(root)
            evaluation = conn.execute("SELECT * FROM evaluations WHERE evaluation_id = 'E-0001'").fetchone()
            conn.close()
            scorecard = json.loads(evaluation["scorecard_json"])
            self.assertEqual(scorecard["winner_submission_id"], "S-0001")
            self.assertEqual(scorecard["left_rubric"]["correctness"], 5.0)
            self.assertGreater(scorecard["left_total"], scorecard["right_total"])
            self.assertEqual(scorecard["rubric_weights"]["verification"], 10.0)
