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
codex-resumer queue start
codex-resumer queue status
```

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
