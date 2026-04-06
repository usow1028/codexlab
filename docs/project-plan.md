# CodexLab Project Plan

This roadmap tracks the path from the current prototype to a fully operational `2 workers + 1 evaluator` Codex harness.

## Current checkpoint

- Date: `2026-04-06`
- Overall completion: `100%` for the current release scope
- Current state:
  - `done`: state model, task/submission/evaluation persistence, champion-challenger logic, symmetric improvement duel plus tie-rematch escalation, `mock` executor, real `codex` executor path, prompt templates, status/doctor/watch/tick/run-loop CLI, daemon lifecycle commands, workspace cleanup, timeout-aware codex execution, run inspection commands, rubric-based evaluator scoring, cross-machine wrapper installer, repo-local doctor bootstrap, operations runbook, and live `codex` smoke verification against a real target repo
  - `in progress`: none inside the current release scope
  - `pending`: none inside the current release scope

## Phase 1. Control Plane Foundation

Status: `done`

Goals:
- define lanes, tasks, submissions, evaluations, reservations, runs
- persist all state in SQLite
- emit append-only event logs
- provide baseline CLI commands for submit, status, watch, manual submission, and manual score

Acceptance criteria:
- new tasks can be created and assigned to both workers
- state survives process restarts
- each task has a directory with brief, state, submissions, and evaluations

## Phase 2. Automated Champion-Challenger Runner

Status: `done`

Goals:
- auto-run workers and evaluator through `tick` and `run-loop`
- support a deterministic `mock` executor
- add first-pass `codex exec` integration
- allow prompt customization through editable template files

Acceptance criteria:
- a full task cycle can complete without manual `record-submission` or `score`
- the opening duel can establish a champion even after tied rematches and evaluator escalation
- the opening loser gets one counterattack, and if that overturns the champion, the previous champion gets one matching defense round before the task locks or enters tie-break escalation
- tests cover initial duel, retry loss, retry win, tie rematch escalation, and queued reservations

## Phase 3. Continuous Daemon Scheduler

Status: `done`

Goals:
- run the scheduler continuously in the background
- expose `daemon start`, `daemon run`, `daemon status`, `daemon stop`
- write daemon heartbeat and runtime metadata under `control/daemon/`

Acceptance criteria:
- the user can start the scheduler once and keep submitting tasks
- queued retries continue without manual ticks
- daemon state can be inspected and stopped cleanly

## Phase 4. Task-Level Workspace Isolation

Status: `done`

Goals:
- replace plain lane workspaces with per-task git worktrees
- keep worker changes isolated from each other and from the control-plane repo
- record worktree paths per active task and lane
- completed: automatically use real git worktrees when `CODEXLAB_TARGET_REPO` points to a repository with a committed `HEAD`
- completed: `workspace clean` can safely remove finished or orphaned workspace trees
- completed: live smoke now verifies that prepared worktrees flow through the real `codex exec` path end to end

Acceptance criteria:
- each concurrent task attempt has its own writable workspace
- role swaps and queued tasks do not corrupt another active workspace
- cleanup and recovery commands can remove stale worktrees safely

## Phase 5. Real Codex Executor Hardening

Status: `done`

Goals:
- verify the `codex` executor against real tasks
- improve JSON-schema prompt contracts and error handling
- capture stdout, stderr, output schema files, and final artifacts more robustly
- current slice done: prepared task workspaces now flow through the real `codex exec` path, so git-worktree targets and directory fallbacks execute in the same resolved workspace
- current slice done: timeout-aware `codex exec` handling and run inspection commands
- current slice done: malformed worker and evaluator outputs now degrade into recoverable lane errors covered by automated tests
- completed: live `codex` smoke succeeds against a real temporary git target repo using the authenticated Codex home plus lane-scoped runtime directories

Acceptance criteria:
- worker and evaluator lanes can complete real `codex exec` runs
- malformed output, timeouts, and non-zero exits degrade into recoverable lane errors
- run artifacts make failures easy to diagnose

## Phase 6. Evaluator Quality Model

Status: `done`

Goals:
- expand evaluator scoring beyond one flat score
- add rubric fields such as correctness, completeness, risk, maintainability, and verification
- derive total scores and loser briefs from structured evaluation output

Acceptance criteria:
- evaluator output is machine-readable and traceable
- scorecards justify ranking changes clearly
- user can inspect why a champion was retained or replaced

## Phase 7. Recovery and Operations

Status: `done`

Goals:
- add `doctor`, `recover`, and `clean` style maintenance commands
- detect stale runs, stale pid files, and broken lane state
- support safe resumption after crashes or reboots
- current slice done: `recover` can detect stale `running` rows, clear stranded `active_run_id` handles, reopen recoverable `error` lanes, mark runs `abandoned`, and reopen lanes when the daemon is stopped
- current slice done: `recover --apply --requeue` can place repaired worker tasks back into lane reservations so the next scheduler cycle promotes them cleanly
- current slice done: `recover --apply --resume` can immediately continue scheduler work after repairing lanes
- current slice done: `recover --apply --restart-daemon` can relaunch the background daemon after repair

Acceptance criteria:
- a crashed daemon can be restarted without manual database surgery
- stale active runs can be marked failed or resumed cleanly
- operational commands explain the real blocking state

## Phase 8. Parallelism and Throughput

Status: `deferred beyond the current release scope`

Goals:
- move from sequential tick execution toward true concurrent lane execution
- allow the leader worker to start a new task while the challenger continues retries
- ensure evaluator scheduling does not starve active workers

Acceptance criteria:
- multiple lanes can progress independently
- reservation fairness still matches the champion-challenger rules
- throughput improves without breaking determinism or state integrity

Release decision:
- the current release keeps the deterministic sequential scheduler
- true concurrent subprocess execution is a future throughput enhancement, not a release blocker

## Phase 9. Operator Experience

Status: `done`

Goals:
- add a richer TUI or dashboard view
- surface queue depth, active runs, retry counts, and current champions
- show evaluator rationale and loser brief directly in the operator view
- current slice done: `codexlab` itself now works as a prompt-style live entrypoint, and `dashboard` / `watch --dashboard` expose daemon health, lane queues, active runs, per-task scoreboards, recent activity, evaluator rationale, loser briefs, and explicit masterpiece confirmation
- completed: `docs/OPERATIONS.md` adds setup, daily operations, recovery, cleanup, and live smoke procedures

Acceptance criteria:
- the user can understand the whole system without digging through SQLite or JSON files
- task, lane, and duel status are visible at a glance

## Phase 10. Release Readiness

Status: `done`

Goals:
- validate end-to-end flows with real Codex lanes
- document setup, recovery, and daily operations
- remove obvious footguns and stabilize CLI ergonomics

Completed:
- `doctor` now validates live Codex login readiness via the authenticated login home
- `scripts/live-codex-smoke.sh` provides a repeatable end-to-end validation path
- setup, recovery, cleanup, and smoke procedures are documented in `README.md` and `docs/OPERATIONS.md`

Acceptance criteria:
- the system can run unattended for extended periods
- documentation covers setup, execution, debugging, and cleanup
- completion reaches practical `100%` for the current project scope

## Post-release options

1. Add a richer optional TUI with live run artifact tails if operator ergonomics become a priority.
2. Revisit Phase 8 only if true concurrent subprocess execution becomes worth the added scheduler complexity.
3. Expand smoke coverage into a longer unattended soak test if production usage needs it.
