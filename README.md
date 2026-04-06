- codex login
- cd ~
- git clone git@github.com:usow1028/codexlab.git
- cd codexlab
- bash scripts/install-codexlab.sh
- bash/zsh: source your shell rc file
- fish: source ~/.config/fish/conf.d/codexlab_path.fish
- codexlab doctor

설치 후 바로 이어서 작업하려면:

cd ~/codexlab
codexlab "작업 설명"


# CodexLab

CodexLab is a local control plane for a `2 workers + 1 evaluator` Codex harness.

The staged delivery plan lives in [docs/project-plan.md](/home/usow/codexlab/docs/project-plan.md).
Daily operations and release smoke steps live in [docs/OPERATIONS.md](/home/usow/codexlab/docs/OPERATIONS.md).

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
codexlab "Implement feature X"
codexlab
codexlab live --prompt "Implement feature X"
codexlab submit --prompt "Implement feature X"
codexlab status
codexlab status T-0001
codexlab dashboard
codexlab dashboard T-0001
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
codexlab clear-tasks
bash scripts/live-codex-smoke.sh
codexlab recover --json
codexlab recover --apply
codexlab recover --apply --requeue
codexlab recover --apply --resume --until-idle
codexlab recover --apply --restart-daemon --until-idle
codexlab tick --executor codex
codexlab watch
codexlab watch T-0001 --until-finished
codexlab watch T-0001 --dashboard --until-finished
codexlab watch --dashboard --once
codexlab console
codexlab tui
codexlab codex
```

## Install on another machine

Git clone alone cannot safely auto-install a wrapper into your shell environment.
Use the bundled installer once after cloning:

```bash
git clone git@github.com:usow1028/codexlab.git
cd codexlab
bash scripts/install-codexlab.sh
# bash/zsh: source ~/.bashrc or ~/.zshrc
# fish: source ~/.config/fish/conf.d/codexlab_path.fish
codexlab doctor
```

`codexlab doctor` bootstraps the repo-local `.codex-home/config.toml` and validates the runtime layout for a fresh clone.
For live `codex` lanes, CodexLab uses your authenticated Codex home from `~/.codex` by default. If your login lives elsewhere, export `CODEXLAB_LOGIN_CODEX_HOME=/path/to/codex-home` before running `codexlab`.

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
- prompt-style `codexlab` entry that submits work like stock `codex`
- operator dashboard output with active runs, live scorecards, crowd reaction, recent activity, and champion confirmation
- a symmetric improvement duel: opening champion, challenger rematch, optional title defense, draw rematches, chief/final judge escalation, and repeated final re-review until a winner emerges

## Operator terminology

The operator UI now uses boxing-flavored labels while the internal control plane still keeps the original worker/evaluator data model.

- `worker-a` -> `Red Corner`
- `worker-b` -> `Blue Corner`
- primary evaluator -> `score judge`
- elder evaluator -> `chief judge`
- absolute evaluator -> `final arbiter`
- current leader -> `champion`
- current trailing direct-reply opponent -> `challenger`
- locked final output -> `CHAMPION CONFIRMED`
- non-binding momentum hint -> `Crowd reaction`

Task artifacts now also generate readable boxing companion files beside the canonical `T/S/E` records:

- `bout.md` explains `T = Task`, `S = Submission`, `E = Evaluation`
- `champion-card.md` points at the current champion output
- `submission-cards/round-..__red-corner__...__S-....md` gives readable aliases for submission folders
- `decision-cards/decision-..__score-judge__...__E-....md` gives readable aliases for evaluation files
- `submissions/by-corner/red-corner/` and `submissions/by-corner/blue-corner/` group each boxer's cards directly inside the canonical submissions tree
- `evaluations/by-judge/score-judge/`, `.../chief-judge/`, and `.../final-arbiter/` group decision cards by judging tier

## Executor model

- `mock` is the safe default for testing the scheduler end to end.
- `codex` launches real `codex exec` runs inside each lane workspace while keeping lane-specific log and SQLite state under `agents/<lane>/home/`.
- after the first real `codex` run on a given task/lane, later rounds on that same lane now resume the stored Codex session instead of cold-starting a fresh `exec` every time.
- if an operator or external cleanup removes a stored Codex session file, CodexLab now notices before the next round and falls back to a fresh cold `exec` for that lane instead of breaking the task.
- if a stored session file still exists but Codex rejects the `resume` internally, CodexLab preserves that failed resume artifact, clears the stale session id, and retries that round once as a fresh cold `exec`.
- live `codex` authentication is resolved from `CODEXLAB_LOGIN_CODEX_HOME` or, by default, `~/.codex`.
- when a real `codex` lane fails because the authenticated account is out of usage, the daemon now records the blocked lanes, watches the active login identity, and automatically reopens those lanes after a later probe succeeds. Switching the underlying `~/.codex` login to a usable account is enough; a manual `recover` is no longer required for that quota-blocked path.
- `CODEXLAB_TARGET_REPO=/path/to/repo` lets CodexLab create real git worktrees when the target repository has a committed `HEAD`.
- if the target repo is missing, not a git repo, or has no commit yet, CodexLab falls back to task-scoped directories automatically.
- `workspace clean` safely removes finished or orphaned workspaces and skips active or queued tasks unless `--force` is used.
- `CODEXLAB_EXEC_TIMEOUT` or `--exec-timeout` controls how long a real `codex exec` lane may run before it is marked as a timeout.
- `runs list` and `runs show` expose recorded run rows plus saved stdout/stderr artifacts for debugging.
- run rows now also record whether a lane used `resume`, which session id was attached, and how large the submitted prompt artifact was.
- `recover` finds stale `running` rows, stranded `active_run_id` handles, and recoverable `error` lanes, then can mark the runs `abandoned` and reopen the lanes when the daemon is stopped.
- `recover --apply --requeue` clears the broken lane but re-inserts the repaired worker task into that lane's reservation queue, letting the next scheduler tick promote it normally.
- `recover --apply --resume` can immediately continue scheduler work after repair, using explicit `--executor` values or the last daemon state when available.
- `recover --apply --restart-daemon` can relaunch the background daemon after repair, again reusing explicit runtime flags or the last daemon state when available.
- `codexlab "..."` still behaves like a prompt-first shorthand: it submits a task, starts or reuses the scheduler, and follows that task until the duel finishes.
- bare `codexlab` now opens a shell-style console in TTY sessions so the operator snapshot stays above a normal prompt instead of re-rendering while you type.
- `codexlab console` is that shell-style entrypoint explicitly; it accepts plain task text plus `/focus T-0001`, `/all`, `/refresh`, `/clear-tasks`, `/profile ...`, `/auto-switch on|off`, `/sync`, `/run ...`, and `/quit`.
- plain text entered at the shell-style console prompt is submitted as a new task immediately; management commands must start with `/`.
- `codexlab clear-tasks` stops the daemon if needed and clears runtime task history, runs, reservations, events, and task-scoped workspaces so the next task starts again from `T-0001`.
- `codexlab tui` keeps the old full-screen curses console when you want in-place redraws instead of shell-like input behavior.
- when `prompt_toolkit` is available, the shell-style console uses it for the prompt line so CJK/IME input behaves more like a normal shell and less like a redrawn TTY app.
- the shell-style console intentionally leaves the input line bare instead of printing `Prompt>` inline, to reduce IME composition interference.
- when that prompt path is active, typing `/` now opens a live slash-command completion list, and the menu narrows immediately as you keep typing.
- resilience profile state now lives in `~/.codexlab/pool.json` by default. When a current profile is selected, `codexlab` injects that profile back into `~/.codex/auth.json` before live work starts.
- `/profile register` now launches `codex login` inside the same terminal, then captures the resulting `~/.codex/auth.json` into the vault and marks that profile current with the next numbered alias (`1`, `2`, `3`, ...). You can still pass an optional alias override.
- `/profile activate <account_key|alias>` switches the selected stored profile back into `~/.codex/auth.json` immediately.
- `/auto-switch on|off` and `/run ...` keep using the same quota-aware account rotation layer.
- `/useage` probes the current profile's live Codex weekly limit and prints a compact `email + remaining percent` summary.
- `/useage all` probes every stored profile in isolation and prints the same compact weekly-limit summary for each one.
- `codexlab codex` launches raw stock Codex inside the lab workspace when you want the underlying tool directly.
- `dashboard` shows daemon health, corner queues, active runs, current stage, next action, champion/challenger summaries, live scorecards, crowd reaction, recent activity, and explicit `CHAMPION CONFIRMED` outcomes.
- the dashboard also surfaces the daemon quota monitor so you can see the active login email, blocked lanes, and the latest auto-resume probe result when Codex usage is exhausted.
- `watch` now defaults to an append-only event stream that prints only meaningful changes such as boxer starts, submissions, judging decisions, title changes, errors, and `CHAMPION CONFIRMED`.
- the duel logic now uses a symmetric improvement path: the opening loser gets one rematch, and if that overturns the champion, the previous champion gets one matching title defense before the winner locks.
- worker prompts now treat the other corner as a scoring benchmark and optional source of pressure notes, not as a debate opponent; each submission is supposed to remain a standalone answer.
- if a retry or rematch draws on weighted rubric totals, both boxers rematch, then judging escalates from score judge -> chief judge -> final arbiter, and the final arbiter keeps re-reviewing the final pair until a winner is chosen.
- the stream also reprints a compact `Progress:` block only when the tracked task state changes, so `IN PROGRESS` tasks disappear from that block once they lock.
- `watch --dashboard --until-finished` still re-renders the full operator view continuously for a specific task until the task locks or is cancelled.
- in real `codex` opening bouts and worker rematches that require both corners, CodexLab now launches both worker runs before waiting so the bout starts concurrently instead of serializing Red then Blue.
- Worker output contract: JSON with `summary` and `body`.
- Evaluator output contract: JSON with `left_rubric`, `right_rubric`, `rationale`, plus `loser_brief` for decisive wins or `rematch_brief` for true ties. These briefs should help corners improve their own submissions, while any pressure from the opposite corner stays optional rather than forcing a rebuttal.
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

- optional richer interactive TUI with live run artifact tails
- future throughput work if true concurrent lane execution becomes a release requirement
