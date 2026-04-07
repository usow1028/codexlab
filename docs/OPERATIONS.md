# CodexLab Operations

This runbook covers the current release-ready operator flow for CodexLab.

Failure taxonomy and rare-case definitions live in [FAILURE_CATEGORIES.md](/home/usow/codexlab/docs/FAILURE_CATEGORIES.md).

## Setup

```bash
cd ~/codexlab
bash scripts/install-codexlab.sh
codexlab doctor
```

After the installer:

- bash/zsh: `source ~/.bashrc` or `source ~/.zshrc`
- fish: `source ~/.config/fish/conf.d/codexlab_path.fish`

`codexlab doctor` should report:

- the runtime layout exists
- repo-local `.codex-home/config.toml` is bootstrapped
- the real `codex` binary exists
- `codex login status` is healthy via the authenticated Codex home

If your authenticated Codex home is not the default `~/.codex`, set:

```bash
export CODEXLAB_LOGIN_CODEX_HOME=/path/to/your/codex-home
```

CodexLab reads authentication from that home, mirrors the selected auth into the repo-local `.codex-home/auth.json`, and then runs live `codex exec` work from the repo-local `.codex-home/` while still keeping lane logs and SQLite runtime state under each lane's `agents/<lane>/home/`.

## Daily flow

```bash
codexlab "Implement feature X"
codexlab watch T-0001 --until-finished
codexlab watch T-0001 --dashboard --until-finished
codexlab runs list
codexlab runs show RUN-0001
```

`codexlab` now has a prompt-style entrypoint:

- `codexlab "..."` submits a new task, starts or reuses the background scheduler, and follows that task live.
- bare `codexlab` opens the shell-style console so the operator snapshot stays above a normal terminal prompt without live redraw while you type.
- `codexlab console` opens that shell-style console explicitly.
- `codexlab tui` opens the older full-screen curses console explicitly.
- the shell-style console accepts plain task text plus `/focus T-0001`, `/all`, `/status`, `/refresh`, `/clear-tasks`, `/profile ...`, `/auto-switch on|off`, `/asymptote on|off`, `/sync`, `/run ...`, and `/quit`.
- plain text in that prompt submits a new task immediately; only slash-prefixed entries are treated as console commands.
- resilience profile data is stored in `~/.codexlab/pool.json` by default. When a current profile exists, `codexlab` injects it back into `~/.codex/auth.json` when the main console/TUI opens and mirrors it into `.codex-home/auth.json` before live Codex work starts.
- `/profile register` launches `codex login` in the same terminal, then stores the resulting `~/.codex/auth.json` as the next numbered profile alias and marks it current. An explicit alias override is still accepted when needed.
- `/profile list` shows the vault, `/profile activate <account_key|alias>` switches manually, `/auto-switch on|off` controls quota-triggered rotation, and `/sync` writes refreshed tokens back into the vault.
- live Codex lanes now run with `danger-full-access` so memory files and built-in tools remain available, while the repo `AGENTS.md` still blocks modifying files outside `/home/usow/codexlab`.
- `/asymptote on` opens the optional epsilon interface, starts the hourly pulse thread, and scans `asymptote/user_prefs.md` plus `asymptote/ai_prefs.md` immediately. From the shell-style `codexlab` console it prefers a separate dedicated terminal tab/window when the desktop supports one, so the main console stays uncluttered. `/asymptote off` closes it gracefully.
- while Asymptote is active, `/sync` keeps its vault-sync behavior and then triggers an immediate extra pulse so `asymptote/letters.md` advances without waiting for the next hour boundary.
- `/useage` probes the current profile's live Codex weekly limit and prints a compact `email + remaining percent` summary.
- `/useage all` runs the same live probe for every stored profile in isolation and prints the same compact weekly-limit summary for each profile.
- `/run <command...>` executes an ad-hoc command through the resilience layer, streaming stdout/stderr live while re-injecting the next ready profile if quota/rate-limit text is detected.
- when `prompt_toolkit` is installed, that shell-style console uses it for the prompt line so Korean/CJK IME input behaves more like a regular shell prompt.
- the shell-style console also leaves the active input line bare instead of prefixing it with `Prompt>`.
- typing `/` in that shell-style console now opens a slash-command suggestion menu, and the list keeps narrowing as you type more of the command.
- `codexlab codex` launches raw stock Codex inside the lab workspace when you need the underlying tool directly.
- the operator UI uses boxing-flavored labels: `worker-a` = `Red Corner`, `worker-b` = `Blue Corner`, the primary evaluator = `score judge`, the elder evaluator = `chief judge`, the absolute evaluator = `final arbiter`, and the final lock = `CHAMPION CONFIRMED`.
- `watch --until-finished` is the quieter default: it appends only meaningful changes such as boxer starts, submissions, judging decisions, title changes, errors, and final champion confirmation.
- the stream also prints a compact `Progress:` block only when the tracked task state changes, including live scorecard ranks, crowd reaction, and an explicit `IN PROGRESS` marker.
- `watch --dashboard --until-finished` is still available when you want the full continuously redrawn operator view.
- each task now uses a symmetric improvement duel: the opening loser gets one rematch, and if that overturns the champion, the previous champion gets one matching title defense before lock.
- each boxer should still submit a standalone answer; the opposing corner only contributes optional pressure notes that can be borrowed if persuasive.
- each task now has a mode: `proposal` or `patch`. Patch bouts expect real workspace changes, and the operator views now show both task mode and apply state.
- if a retry or later rematch draws on weighted totals, CodexLab schedules a boxer rematch, then escalates judging from score judge to chief judge to final arbiter.
- after the final boxer rematch under the final arbiter, that arbiter keeps re-reviewing the same final pair until a decisive winner is produced.
- when a patch bout locks a champion, CodexLab tries to promote the winner workspace into the target repo. If the target repo already has conflicting local changes on the same paths, the task remains locked but `apply` becomes `Not applied` with an explicit reason.
- under the real `codex` executor, any bout stage that needs both corners on the same task now starts both worker runs before waiting, so opening exchanges begin concurrently instead of Red then Blue.
- after each lane's first real `codex` round on a task, later rounds reuse that task-scoped session with `codex exec resume` so rematches and judging reviews avoid repeated cold starts.
- if those stored session files disappear under `~/.codex/sessions/`, CodexLab clears the stale reference and cold-starts that round once instead of leaving the lane stuck on a broken resume id.
- if Codex itself rejects a still-present session during `resume`, CodexLab keeps the failed resume artifacts under the same `RUN-xxxx/` directory and retries that round once as a cold run.

For continuous scheduling:

```bash
codexlab daemon start --executor codex
codexlab daemon status
codexlab daemon stop
```

To wipe runtime task history before starting a fresh batch:

```bash
codexlab clear-tasks
```

That command stops the daemon if needed, clears tasks/submissions/evaluations/reservations/runs, resets the next IDs back to `1`, and removes task-scoped workspaces and run artifacts. It does not touch your code, docs, or lane home logs.

If a live `codex` run hits a usage limit, keep the daemon running. CodexLab now tracks the blocked lanes, watches the authenticated login under `CODEXLAB_LOGIN_CODEX_HOME` or `~/.codex`, and automatically applies recovery after a later probe succeeds. If you switch the underlying Codex login to a different usable account, the daemon will reuse that login on the next probe and continue without a manual `recover`.

## Live smoke

Run the built-in end-to-end smoke against a real temporary git target repo:

```bash
bash scripts/live-codex-smoke.sh
```

The script creates an isolated smoke lab root under `control/smoke/<timestamp>/`, runs:

1. `submit`
2. `tick --executor codex`
3. `dashboard --json`
4. `runs list --json`

It exits non-zero if any worker or evaluator action errors, if no evaluation occurs, or if the first three runs do not complete successfully.

## Recovery

Inspect stale state:

```bash
codexlab recover --json
```

Repair worker/evaluator lanes:

```bash
codexlab recover --apply
codexlab recover --apply --requeue
codexlab recover --apply --resume --executor codex --until-idle
codexlab recover --apply --restart-daemon --executor codex --until-idle
```

`recover` is still the right tool for malformed output, timeouts, stale runs, or any other non-quota lane failure. The quota-blocked case is now handled automatically by the daemon.

If the failure does not look like quota, timeout, malformed output, or stale resume state, check [FAILURE_CATEGORIES.md](/home/usow/codexlab/docs/FAILURE_CATEGORIES.md) before treating it as a generic lane error. The runtime boundary mismatch class in particular needs a different fix path than ordinary recoverable run failures.

## Cleanup

Check lingering workspaces:

```bash
codexlab workspace status
codexlab workspace clean --dry-run
codexlab workspace clean
```

Finished and orphaned workspace trees are safe to remove; active and queued tasks are skipped unless `--force` is used.
