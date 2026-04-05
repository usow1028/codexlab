from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path
from unittest import TestCase


SCRIPT = Path("/home/usow/codexlab/codexlab.py")
INSTALL_SCRIPT = Path("/home/usow/codexlab/scripts/install-codexlab.sh")


class CodexLabCliTests(TestCase):
    def run_cli(self, root: Path, *args: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["CODEXLAB_ROOT"] = str(root)
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["python3", str(SCRIPT), *args],
            check=True,
            capture_output=True,
            text=True,
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
from pathlib import Path

plan = json.loads(Path(os.environ["FAKE_CODEX_PLAN"]).read_text(encoding="utf-8"))
counter_path = Path(os.environ["FAKE_CODEX_COUNTER"])
index = int(counter_path.read_text(encoding="utf-8").strip() or "0")
counter_path.write_text(str(index + 1) + "\\n", encoding="utf-8")
entry = plan[index]
argv = sys.argv[1:]
output_path = None
for i, arg in enumerate(argv):
    if arg == "--output-last-message" and i + 1 < len(argv):
        output_path = Path(argv[i + 1])
        break
if output_path is None:
    raise SystemExit("missing --output-last-message")
sleep_seconds = float(entry.get("sleep", 0))
if sleep_seconds:
    time.sleep(sleep_seconds)
stdout_text = entry.get("stdout", "")
stderr_text = entry.get("stderr", "")
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

    def test_submit_assigns_both_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Build feature alpha")
            status = self.status_json(root)
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            self.assertEqual(lanes["worker-a"]["active_task_id"], "T-0001")
            self.assertEqual(lanes["worker-b"]["active_task_id"], "T-0001")
            self.assertEqual(status["tasks"][0]["task_id"], "T-0001")

    def test_run_loop_mock_locks_masterpiece_after_three_failed_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Draft a durable implementation plan")
            self.run_cli(root, "run-loop", "--executor", "mock", "--until-idle", "--max-ticks", "10")

            status = self.status_json(root)
            task = next(task for task in status["tasks"] if task["task_id"] == "T-0001")
            self.assertEqual(task["status"], "masterpiece_locked")
            self.assertEqual(task["masterpiece_locked"], 1)
            self.assertEqual(task["champion_lane_id"], "worker-a")
            self.assertEqual(task["challenger_failed_attempts"], 3)
            self.assertEqual(task["total_evaluations"], 4)

            conn = self.connect_db(root)
            run_count = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            conn.close()
            self.assertEqual(run_count, 9)
            self.assertTrue((root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())
            self.assertTrue((root / "agents" / "worker-b" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())
            self.assertTrue((root / "agents" / "evaluator" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())

    def test_tick_mock_handles_role_swap_and_retry_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            plan = {
                "evaluation_scores": {
                    "T-0001": [
                        {"worker-a": 91, "worker-b": 82, "rationale": "worker-a wins initial round"},
                        {"worker-a": 89, "worker-b": 94, "rationale": "worker-b overtakes worker-a"},
                    ]
                }
            }
            env = {"CODEXLAB_MOCK_PLAN": json.dumps(plan)}

            self.run_cli(root, "submit", "--prompt", "Task one", extra_env=env)
            self.run_cli(root, "tick", "--executor", "mock", extra_env=env)

            self.run_cli(root, "submit", "--prompt", "Task two", extra_env=env)
            self.run_cli(root, "tick", "--executor", "mock", extra_env=env)

            status = self.status_json(root)
            lanes = {lane["lane_id"]: lane for lane in status["lanes"]}
            task = next(task for task in status["tasks"] if task["task_id"] == "T-0001")

            self.assertEqual(task["champion_lane_id"], "worker-b")
            self.assertEqual(task["challenger_lane_id"], "worker-a")
            self.assertEqual(lanes["worker-a"]["active_task_id"], "T-0002")
            self.assertTrue(
                any(
                    entry["task_id"] == "T-0001" and entry["reservation_type"] == "duel_retry"
                    for entry in lanes["worker-a"]["queued_reservations"]
                )
            )
            self.assertEqual(lanes["worker-b"]["active_task_id"], "T-0002")
            self.assertTrue((root / "agents" / "worker-a" / "workspace" / "tasks" / "T-0002" / "codexlab-task.json").exists())
            self.assertTrue((root / "agents" / "worker-b" / "workspace" / "tasks" / "T-0001" / "codexlab-task.json").exists())

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

    def test_dashboard_json_includes_latest_evaluation_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Dashboard should expose evaluation context")
            self.run_cli(root, "tick", "--executor", "mock")

            dashboard = json.loads(self.run_cli(root, "dashboard", "--json").stdout)
            self.assertEqual(dashboard["summary"]["task_total"], 1)
            self.assertEqual(dashboard["summary"]["queued_reservations"], 0)
            self.assertEqual(dashboard["summary"]["lane_busy"], 1)
            task = dashboard["tasks"][0]
            self.assertEqual(task["champion_submission"]["lane_id"], "worker-a")
            self.assertEqual(task["challenger_submission"]["lane_id"], "worker-b")
            self.assertEqual(task["latest_evaluation"]["winner_lane_id"], "worker-a")
            self.assertIn("Mock evaluator compared", task["latest_evaluation"]["rationale"])
            self.assertIn("correctness gaps", task["latest_evaluation"]["loser_brief"])

    def test_watch_once_dashboard_renders_operator_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.run_cli(root, "submit", "--prompt", "Watch dashboard once")
            self.run_cli(root, "tick", "--executor", "mock")

            result = self.run_cli(root, "watch", "--dashboard", "--once")
            self.assertIn("CodexLab Dashboard", result.stdout)
            self.assertIn("Latest Eval:", result.stdout)
            self.assertIn("Loser brief:", result.stdout)

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

            run_detail = json.loads(self.run_cli(root, "runs", "show", "RUN-0001", "--json", extra_env=env).stdout)
            self.assertIn("worker-a ok", run_detail["artifacts"]["stdout.log"])

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
            self.assertEqual(tick["actions"][0]["type"], "worker_error")
            self.assertIn("timed out", tick["actions"][0]["error"])

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
            self.assertEqual(first_tick["actions"][0]["type"], "worker_error")
            self.assertEqual(first_tick["actions"][1]["type"], "worker_submission")

            preview = json.loads(self.run_cli(root, "recover", "--json", extra_env=env).stdout)
            self.assertEqual(preview["repaired_lane_count"], 1)
            self.assertEqual(preview["lane_repairs"][0]["lane_id"], "worker-a")
            self.assertEqual(preview["lane_repairs"][0]["repair_source"], "error_state")

            applied = json.loads(self.run_cli(root, "recover", "--apply", "--json", extra_env=env).stdout)
            self.assertIn("worker-a", applied["repaired_lanes"])

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(second_tick["actions"][0]["type"], "worker_submission")
            self.assertEqual(second_tick["actions"][1]["type"], "evaluation")

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertEqual(task["champion_lane_id"], "worker-a")
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
            self.assertEqual(first_tick["actions"][0]["type"], "worker_error")
            self.assertEqual(first_tick["actions"][1]["type"], "worker_submission")

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
            self.assertEqual(recovered["repaired_lanes"], ["worker-a"])
            self.assertEqual(recovered["requeued_reservation_count"], 1)
            self.assertEqual(recovered["requeued_reservations"][0]["reservation_type"], "recovered_task")

            status_after_recover = self.status_json(root)
            lanes_after_recover = {lane["lane_id"]: lane for lane in status_after_recover["lanes"]}
            self.assertEqual(lanes_after_recover["worker-a"]["status"], "idle")
            self.assertTrue(
                any(
                    entry["task_id"] == "T-0001" and entry["reservation_type"] == "recovered_task"
                    for entry in lanes_after_recover["worker-a"]["queued_reservations"]
                )
            )

            second_tick = json.loads(self.run_cli(root, "tick", "--executor", "codex", "--exec-timeout", "2", "--json", extra_env=env).stdout)
            self.assertEqual(second_tick["actions"][0]["type"], "worker_submission")
            self.assertEqual(second_tick["actions"][1]["type"], "evaluation")

            final_status = self.status_json(root, "T-0001")
            task = final_status["tasks"][0]
            self.assertEqual(task["champion_lane_id"], "worker-a")
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
            self.assertEqual(first_tick["actions"][0]["type"], "worker_error")
            self.assertEqual(first_tick["actions"][1]["type"], "worker_submission")

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
            self.assertEqual(recovered["repaired_lanes"], ["worker-a"])
            self.assertEqual(len(recovered["resume"]["ticks"]), 1)
            self.assertEqual(recovered["resume"]["ticks"][0]["actions"][0]["type"], "worker_submission")
            self.assertEqual(recovered["resume"]["ticks"][0]["actions"][1]["type"], "evaluation")

            status = self.status_json(root, "T-0001")
            task = status["tasks"][0]
            self.assertEqual(task["champion_lane_id"], "worker-a")

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
