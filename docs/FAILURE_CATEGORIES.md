# CodexLab Failure Categories

This document defines the failure classes that CodexLab should recognize and discuss explicitly.

The goal is not only recovery. The goal is also operator clarity:

- what kind of failure this is
- what boundary actually broke
- whether it is common or rare
- what the correct fix path is

## Why This Exists

Some failures are routine:

- quota exhaustion
- timeout
- malformed worker/evaluator output
- stale resume session

Some failures are rare and easy to misread if they are not named.

This document exists so those rare cases are not treated as generic "lane error" incidents.

## Category List

### 1. Quota Exhaustion

Definition:
- A live Codex run hits account usage or rate-limit exhaustion.

Typical signals:
- `Quota Exceeded`
- `429`
- `Rate limit`
- weekly limit probe reports `0% left`

Expected operator state:
- `quota_blocked`
- auto-switch or reserve-guard pause may engage

Primary response:
- rotate to another ready profile
- or wait for recharge

### 2. Reserve-Threshold Pause

Definition:
- The last usable profile falls to the reserve threshold, so CodexLab pauses before burning the final fallback account.

Typical signals:
- `PAUSED`
- reserve-protected status text
- selected/current profile remains usable for stock Codex, but CodexLab-managed work does not start new runs

Primary response:
- register a new profile
- wait for recharge
- or raise the threshold intentionally if policy changes

### 3. Timeout

Definition:
- A worker or judge run does not finish before the configured execution timeout.

Typical signals:
- run exits at the configured timeout
- large source material or long-form drafting tasks consistently die near the same wall-clock limit

Primary response:
- raise timeout if the task is legitimately long
- otherwise inspect whether the model is stuck before extending limits further

### 4. Malformed Output Contract

Definition:
- A worker or judge returns output that violates the expected JSON or rubric contract.

Typical signals:
- missing required fields
- invalid JSON
- schema mismatch

Primary response:
- fix prompt/schema contract
- or treat as recoverable lane failure and rerun

### 5. Stale Or Rejected Resume Session

Definition:
- A stored Codex session id cannot be resumed cleanly.

Typical signals:
- missing session file
- `resume` CLI parse error
- Codex rejects a still-present session internally

Primary response:
- clear stale session reference
- retry once as a cold run

### 6. Runtime Boundary Mismatch

Definition:
- The live Codex runtime can start, but tool or memory access breaks because the execution boundary is inconsistent.

This is the category for the rare case we hit when:

- live lanes tried to use Codex memories or built-in tools
- the live sandbox/runtime boundary did not match those expectations
- inherited repo policy also constrained outside access in a conflicting way

Typical signals:
- `legacy sandbox policy must match split sandbox policies`
- `exec_command failed`
- `js_repl kernel exited unexpectedly`
- workers appear to be running, but they are not progressing into real task work

Boundary that breaks:
- `CODEX_HOME`
- sandbox mode
- writable roots / tool policy
- inherited `AGENTS.md` instructions

Primary response:
- align live `CODEX_HOME` and live session roots
- use one consistent sandbox policy for cold and resume paths
- make memory/tool access policy explicit
- keep outside-write restrictions in policy, not via a contradictory runtime boundary

Notes:
- This is not a quota problem.
- This is not a simple timeout.
- This is not a malformed output problem.
- It should be called out by name when it happens.

### 7. Daemon State Drift Or Display Recursion

Definition:
- The daemon state file or display layer no longer reflects a small current runtime state and instead drifts, bloats, or becomes stale.

Typical signals:
- dashboard becomes unexpectedly slow
- daemon state file grows abnormally
- displayed daemon metadata does not match the real running daemon

Primary response:
- keep daemon state minimal
- store events separately
- compute dashboard snapshots from DB + events instead of recursively persisting full snapshots

### 8. Auth Ownership Drift

Definition:
- CodexLab's selected vault profile and stock `~/.codex/auth.json` fall out of sync, so operators believe one account is active while live work or stock Codex is actually using another.

Typical signals:
- stock `codex` appears to jump back to an older account
- `selected=` in CodexLab does not match the effective login identity
- auto-switch rotates, but stock Codex still feels blocked on a different profile

Boundary that breaks:
- vault `current_account_key`
- injected `~/.codex/auth.json`
- live `CODEX_HOME` mirror

Primary response:
- keep CodexLab as the authority for selected profiles
- mirror the selected vault profile into stock `auth.json`
- only change the active account through CodexLab profile commands or explicit vault sync flows

Why it is easy to misread:
- it looks like quota or login instability, but the real bug is ownership of the active auth state

### 9. Proposal/Patch Contract Mismatch

Definition:
- The user expects a real repo change, but the harness still judges mostly the written submission body as if the task were a proposal bout.

Typical signals:
- a task locks a champion with a strong Markdown answer, but the target repo did not change
- worker worktrees contain real files, yet nothing is promoted into the main repo
- evaluator rationale rewards prose quality while concrete repo changes are missing or under-weighted

Boundary that breaks:
- task mode classification
- worker submission artifact contract
- evaluator scoring expectations
- winner promotion/apply step

Primary response:
- mark the task as `patch`
- capture actual workspace evidence with each submission
- judge patch bouts against implementation evidence
- promote the winning worktree into the target repo or record an explicit apply failure

Why it is easy to misread:
- it looks like the model ignored the prompt, but the real problem is that the harness was rewarding the wrong artifact

## Rare-Case Rule

When a new failure does not fit the routine set above, do not collapse it into a generic `lane error`.

Add it here if all of the following are true:

- it crosses a distinct system boundary
- it changes the correct recovery path
- it is likely to be misdiagnosed as a more common class

## Template For New Rare Categories

Use this shape:

```text
Name:
Definition:
Typical signals:
Boundary that breaks:
Primary response:
Why it is easy to misread:
```

## Current Guidance

For day-to-day operator work:

- use [OPERATIONS.md](/home/usow/codexlab/docs/OPERATIONS.md) for commands and recovery steps
- use this document to decide what kind of failure you are actually looking at
