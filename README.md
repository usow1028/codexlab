  codex login
  cd ~
  git clone git@github.com:usow1028/codexlab.git
  cd codexlab
  bash scripts/install-codexlab.sh
  source ~/.bashrc
  codexlab doctor

  설치 후 바로 이어서 작업하려면:

  cd ~/codexlab
  codex


# CodexLab

CodexLab is a local control plane for a `2 workers + 1 evaluator` Codex harness.

The staged delivery plan lives in [docs/project-plan.md](/home/usow/codexlab/docs/project-plan.md).

Stage 1 adds:

- persistent SQLite state under `control/control.db`
- append-only JSONL events under `control/events/events.jsonl`
- per-run artifacts under `control/runs/RUN-*`
- task directories under `tasks/T-*`
- task-scoped lane workspaces under `agents/*/workspace/tasks/T-*`
- editable worker/evaluator prompt templates under `templates/`
- a `codexlab` CLI for submit/status/watch/score/tick flows

## Commands

```bash
codexlab doctor
codexlab submit --prompt "Implement feature X"
codexlab status
codexlab status T-0001
codexlab dashboard
codexlab dashboard T-0001 --json
codexlab workspace status
codexlab workspace clean --task-id T-0001 --dry-run
codexlab workspace clean --task-id T-0001
codexlab record-submission T-0001 worker-a --summary "first draft"
codexlab record-submission T-0001 worker-b --summary "alternative draft"
codexlab score T-0001 --left S-0001 --right S-0002 \
  --left-rubric-json '{"correctness":4.8,"completeness":4.5,"risk":4.3,"maintainability":4.4,"verification":4.6}' \
  --right-rubric-json '{"correctness":4.1,"completeness":4.0,"risk":4.0,"maintainability":4.1,"verification":4.0}' \
  --rationale "worker-a is stronger"
codexlab tick --executor mock
codexlab run-loop --executor mock --until-idle
codexlab daemon run --executor mock --until-idle
codexlab daemon start --executor mock
codexlab daemon status
codexlab daemon stop
codexlab runs list
codexlab runs show RUN-0001
codexlab recover --json
codexlab recover --apply
codexlab recover --apply --requeue
codexlab recover --apply --resume --until-idle
codexlab recover --apply --restart-daemon --until-idle
codexlab tick --executor codex
codexlab watch
codexlab watch --dashboard --once
codexlab tui
```

## Install on another machine

Git clone alone cannot safely auto-install a wrapper into your shell environment.
Use the bundled installer once after cloning:

```bash
git clone git@github.com:usow1028/codexlab.git
cd codexlab
bash scripts/install-codexlab.sh
source ~/.bashrc
codexlab doctor
```

If you do not want shell rc changes, install the wrapper only:

```bash
bash scripts/install-codexlab.sh --skip-path
~/bin/codexlab doctor
```

## Current scope

The current stage now covers both the control plane and a first automated runner:

- lanes
- tasks
- submissions
- evaluations
- reservations
- champion/challenger state
- `mock` executor for repeatable automated tests
- optional `codex` executor using `codex exec` with JSON schema output
- daemon lifecycle commands for continuous background scheduling
- task-scoped workspace isolation as groundwork for future git worktrees
- operator dashboard output with latest evaluation rationale and loser brief

## Executor model

- `mock` is the safe default for testing the scheduler end to end.
- `codex` launches real `codex exec` runs inside each lane workspace with a lane-specific `CODEX_HOME`.
- `CODEXLAB_TARGET_REPO=/path/to/repo` lets CodexLab create real git worktrees when the target repository has a committed `HEAD`.
- if the target repo is missing, not a git repo, or has no commit yet, CodexLab falls back to task-scoped directories automatically.
- `workspace clean` safely removes finished or orphaned workspaces and skips active or queued tasks unless `--force` is used.
- `CODEXLAB_EXEC_TIMEOUT` or `--exec-timeout` controls how long a real `codex exec` lane may run before it is marked as a timeout.
- `runs list` and `runs show` expose recorded run rows plus saved stdout/stderr artifacts for debugging.
- `recover` finds stale `running` rows, stranded `active_run_id` handles, and recoverable `error` lanes, then can mark the runs `abandoned` and reopen the lanes when the daemon is stopped.
- `recover --apply --requeue` clears the broken lane but re-inserts the repaired worker task into that lane's reservation queue, letting the next scheduler tick promote it normally.
- `recover --apply --resume` can immediately continue scheduler work after repair, using explicit `--executor` values or the last daemon state when available.
- `recover --apply --restart-daemon` can relaunch the background daemon after repair, again reusing explicit runtime flags or the last daemon state when available.
- `dashboard` shows daemon health, lane queues, champion/challenger summaries, and the latest evaluation rationale in one snapshot.
- `watch --dashboard` re-renders that operator view continuously.
- Worker output contract: JSON with `summary` and `body`.
- Evaluator output contract: JSON with `left_rubric`, `right_rubric`, `rationale`, and `loser_brief`.
- Each rubric contains `correctness`, `completeness`, `risk`, `maintainability`, and `verification` on a `0-5` scale.
- Manual `score` still accepts `--left-score/--right-score` for compatibility and backfills a uniform rubric from those totals.

`CODEXLAB_MOCK_PLAN` can override mock scoring and worker output for tests or demos. It accepts either an inline JSON string or a path to a JSON file.

Example:

```json
{
  "evaluation_scores": {
    "T-0001": [
      {
        "worker-a_rubric": {
          "correctness": 4.8,
          "completeness": 4.5,
          "risk": 4.4,
          "maintainability": 4.5,
          "verification": 4.6
        },
        "worker-b_rubric": {
          "correctness": 4.1,
          "completeness": 4.2,
          "risk": 4.0,
          "maintainability": 4.1,
          "verification": 4.0
        },
        "rationale": "worker-a wins initial round"
      },
      { "worker-a": 89, "worker-b": 94, "rationale": "legacy numeric totals still work" }
    ]
  }
}
```

## What is next

- replace plain lane workspaces with per-task git worktrees
- finish live `codex` executor hardening against real target repos
- extend the dashboard into a richer interactive TUI with live run artifact tails
