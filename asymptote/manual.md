# Asymptote Manual

Asymptote is an optional side-engine for CodexLab.

Its role is simple:

- keep a separate reflective interface outside the main `codexlab` task console
- read the current human/AI preference anchors
- append periodic or manual letters to `letters.md`
- preserve its own runtime state in `state.json`

## Start

From the main `codexlab` shell:

```text
/asymptote on
```

On desktop environments that support it, this opens a dedicated Asymptote console in a separate terminal tab or window.

If no supported terminal launcher is available, CodexLab falls back to starting Asymptote inline.

## Stop

From the Asymptote console:

```text
/asymptote off
```

or:

```text
/quit
```

If the dedicated Asymptote console owns the running engine, closing that console also stops the engine cleanly.

## Files

All Asymptote files live under this directory:

- `user_prefs.md`
  Human-side preferences and anchors.
- `ai_prefs.md`
  AI-side preferences and anchors.
- `letters.md`
  The chronological conversation and pulse log.
- `state.json`
  Runtime state for the current Asymptote interface.
- `manual.md`
  This short operator guide.

## Dedicated console behavior

The dedicated Asymptote console supports these commands:

```text
/status
/sync
/asymptote off
/quit
```

Any plain text you type there is treated as a human note and appended to `letters.md`.

## Sync behavior

From the main `codexlab` console:

```text
/sync
```

still performs the normal resilience/auth sync first.

If Asymptote is active, the same `/sync` also triggers one immediate extra Asymptote pulse so `letters.md` advances without waiting for the next scheduled interval.

## Pulse model

When active, Asymptote:

- scans `user_prefs.md`
- scans `ai_prefs.md`
- derives human and AI anchors
- appends a new question block to `letters.md`
- keeps the next pulse horizon in `state.json`

Scheduled pulses run roughly once per configured interval.
Manual sync pulses run immediately.

## Recommended workflow

1. Start `codexlab`
2. Run `/asymptote on`
3. Keep the main CodexLab console for tasks
4. Use the separate Asymptote console for reflection and notes
5. Use `/sync` when you want an immediate extra pulse
6. Stop Asymptote with `/asymptote off` when finished
