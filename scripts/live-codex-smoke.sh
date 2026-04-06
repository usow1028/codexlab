#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
SMOKE_TIMEOUT="${CODEXLAB_SMOKE_TIMEOUT:-180}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SMOKE_ROOT="$REPO_ROOT/control/smoke/$STAMP"
LAB_ROOT="$SMOKE_ROOT/lab-root"
TARGET_REPO="$SMOKE_ROOT/target-repo"

mkdir -p "$SMOKE_ROOT" "$LAB_ROOT" "$TARGET_REPO"

git init "$TARGET_REPO" >/dev/null
git -C "$TARGET_REPO" config user.name "CodexLab Smoke"
git -C "$TARGET_REPO" config user.email "codexlab-smoke@example.com"
printf 'CodexLab live smoke target\n' > "$TARGET_REPO/README.md"
git -C "$TARGET_REPO" add README.md
git -C "$TARGET_REPO" commit -m "init smoke target" >/dev/null

export CODEXLAB_ROOT="$LAB_ROOT"
export CODEXLAB_TARGET_REPO="$TARGET_REPO"

python3 "$REPO_ROOT/codexlab.py" submit \
  --prompt "Live codex smoke: summarize the task in 2-3 concise sentences, do not modify repository files, and return only the required JSON." \
  > "$SMOKE_ROOT/submit.txt"

python3 "$REPO_ROOT/codexlab.py" tick --executor codex --exec-timeout "$SMOKE_TIMEOUT" --json \
  > "$SMOKE_ROOT/tick.json"
python3 "$REPO_ROOT/codexlab.py" dashboard --json > "$SMOKE_ROOT/dashboard.json"
python3 "$REPO_ROOT/codexlab.py" runs list --json > "$SMOKE_ROOT/runs.json"

python3 - "$SMOKE_ROOT" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

smoke_root = Path(sys.argv[1])
tick = json.loads((smoke_root / "tick.json").read_text(encoding="utf-8"))
actions = tick.get("actions", [])
errors = [action for action in actions if action.get("type", "").endswith("error")]
if errors:
    print(json.dumps({"ok": False, "reason": "executor_error", "actions": actions}, indent=2, ensure_ascii=True))
    raise SystemExit(1)
if not any(action.get("type") == "evaluation" for action in actions):
    print(json.dumps({"ok": False, "reason": "missing_evaluation", "actions": actions}, indent=2, ensure_ascii=True))
    raise SystemExit(1)

dashboard = json.loads((smoke_root / "dashboard.json").read_text(encoding="utf-8"))
tasks = dashboard.get("tasks", [])
if len(tasks) != 1:
    print(json.dumps({"ok": False, "reason": "unexpected_task_count", "task_count": len(tasks)}, indent=2, ensure_ascii=True))
    raise SystemExit(1)
task = tasks[0]
if not task.get("champion_lane_id"):
    print(json.dumps({"ok": False, "reason": "missing_champion", "task": task}, indent=2, ensure_ascii=True))
    raise SystemExit(1)

runs = json.loads((smoke_root / "runs.json").read_text(encoding="utf-8")).get("runs", [])
if len(runs) < 3 or any(run.get("status") != "completed" for run in runs[:3]):
    print(json.dumps({"ok": False, "reason": "runs_not_completed", "runs": runs}, indent=2, ensure_ascii=True))
    raise SystemExit(1)

summary = {
    "ok": True,
    "task_id": task["task_id"],
    "task_status": task["status"],
    "champion_lane_id": task["champion_lane_id"],
    "published_submission_id": task["published_submission_id"],
    "run_ids": [run["run_id"] for run in runs[:3]],
    "smoke_root": str(smoke_root),
}
print(json.dumps(summary, indent=2, ensure_ascii=True))
PY

echo "artifacts_root=$SMOKE_ROOT"
