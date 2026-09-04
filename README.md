# Codex Resumer

Codex Resumer is a local Linux CLI and daemon for coordinating Codex work across
temporary usage interruptions. It provides a capability-checked daemon
lifecycle and runs a durable global FIFO Queue across new Workspace Threads and
explicitly imported Managed Threads.

## Requirements

- Node.js 20 or newer
- A current `codex` CLI available on `PATH`
- An existing ChatGPT login created by `codex login`

Codex Resumer starts `codex app-server` with the current Codex environment. It
does not ask for, read, or save a separate API key. See the
[official App Server documentation](https://learn.chatgpt.com/docs/app-server)
for the protocol Codex Resumer checks at startup.

## Install and run

From a source checkout:

```sh
npm install
npm run build
npm link
codex-resumer daemon start
```

Without linking, run the TypeScript source directly:

```sh
npm run dev -- daemon start
```

The daemon commands are:

```sh
codex-resumer daemon start
codex-resumer daemon status
codex-resumer daemon stop
```

Add Tasks for new Threads in a Workspace, start the Queue, and inspect its state:

```sh
codex-resumer task add --workspace ./my-workspace "Implement the requested change"
codex-resumer queue start --until-idle
codex-resumer queue status
```

Every Queue start or resume selects a Run Policy. Use `--until-idle` to keep
starting Tasks and Continuations until the Queue is empty, or provide a one-time
absolute Cutoff Time with an explicit timezone:

```sh
codex-resumer queue start --cutoff 2026-09-05T02:00:00+08:00
codex-resumer queue pause
codex-resumer queue resume --until-idle
```

Manual pause lets an active Turn finish but prevents the next Task or
Continuation from starting. Resume creates a new Queue Run and requires a new
Run Policy selection. At Cutoff Time, the Queue is paused without interrupting
an active Turn. A Cutoff that expires during a Quota Pause prevents the
Continuation, and an expired Cutoff remains enforced after daemon restart.
`queue status` shows the persisted Queue Run ID, Run Policy, normalized Cutoff
Time, start/end times, and pause reason.

Inspect or replace the global Continuation prompt:

```sh
codex-resumer config show
codex-resumer config set continuationPrompt \
  "Inspect the current work, finish what remains, and do not repeat completed work."
```

The default is:

> Inspect the current Thread and Workspace state, continue the unfinished Task,
> and do not repeat work that is already complete.

Import an existing Thread, add a Task to it, or supply a multiline prompt on
stdin:

```sh
codex-resumer thread import <thread-id>
codex-resumer task add --thread <thread-id> "Continue the existing work"
codex-resumer task add --workspace ./my-workspace <<'PROMPT'
Implement the requested change.
Run the focused tests when finished.
PROMPT
```

Tasks run one at a time in displayed order. While a Task is active, queued
Tasks can be added, moved, or cancelled without changing the active Turn:

```sh
codex-resumer task list
codex-resumer task move 4 --before 2
codex-resumer task move 4 --after 2
codex-resumer task cancel 3
```

Only queued Tasks can be moved or cancelled, and a queued Task cannot be moved
before the active Task. Cancelling a Task keeps its terminal metadata but
removes its prompt. `task list` and `queue status` show Queue order, state,
Workspace, Thread, and Turn identifiers without displaying prompts.

Workspace paths are stored as canonical absolute paths. A successful Codex Turn
completes the Task and starts the next queued Task; no marker is required in the
model output. Every Workspace target creates a new Thread. Imported Threads are
validated with App Server and permanently bound to their recorded Workspace.
New Threads and Turns omit model, reasoning, and personality overrides, so Codex
uses the user's current defaults.

When App Server reports a structured `usageLimitExceeded` error, the active Task
enters `waiting_for_quota` while retaining its Managed Thread and failed Turn.
Codex Resumer selects the reached rate-limit bucket, waits until its server-provided
reset time plus a two-second safety margin, and reads the limits again before it
continues. If no reset time is available, it polls with exponential delays from one
minute up to a fifteen-minute cap. A recovered Task starts a new Turn in the same
Managed Thread with the current global Continuation prompt; the original Task prompt
is never submitted again. Repeated Quota Pauses follow the same process until the
Task completes, the Queue is manually paused, or its Cutoff Time is reached.

`daemon start` detaches from the terminal. Starting it again is safe and keeps a
single daemon instance. At startup, Codex Resumer checks the installed protocol
schema for the quota, Thread, Turn, interruption, approval, and streamed-error
capabilities it needs. It then verifies the current ChatGPT account and reads
the current rate-limit state. An incompatible App Server or missing ChatGPT
login prevents the daemon from accepting work, and `daemon status` reports the
reason together with the installed Codex version.

## Local data and permissions

Codex Resumer follows the Linux XDG base-directory convention:

- Configuration directory: `$XDG_CONFIG_HOME/codex-resumer`, or
  `~/.config/codex-resumer`
- State directory: `$XDG_STATE_HOME/codex-resumer`, or
  `~/.local/state/codex-resumer`
- Socket directory: `$XDG_RUNTIME_DIR/codex-resumer`; if `XDG_RUNTIME_DIR` is
  unavailable, `/tmp/codex-resumer-<uid>`

Application directories are mode `0700`; the Unix domain socket and daemon
status file are mode `0600`. Queue, Task, Managed Thread, and Turn state is kept
in `state.sqlite3` inside the state directory. Complete prompts are removed from
the database when their Task completes.
