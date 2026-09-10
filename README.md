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

Install the published npm package globally:

```sh
npm install --global codex-resumer
codex-resumer --help
```

To verify a package built from a source checkout in an isolated temporary
installation, run `npm run verify:package`.

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
codex-resumer daemon stop --force
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

The global Access Mode defaults to Configured Access. On every Turn, Configured
Access reads the effective Codex sandbox and approval settings for that
Workspace and sends them explicitly to App Server. Manual Queue starts and
resumes warn that an approval can block unattended work.

Full Access removes Codex sandbox and approval restrictions for every Task and
Continuation in the Queue Run. It can modify files outside the Workspace and
use the network without asking. Select it globally, then acknowledge that risk
on every manual start or resume. `--yes` is the explicit non-interactive
acknowledgement:

```sh
codex-resumer config set accessMode full
codex-resumer queue start --until-idle --yes
```

To return to the user's effective Codex permissions for the next Queue Run:

```sh
codex-resumer config set accessMode configured
codex-resumer queue resume --until-idle
```

The selected Access Mode is stored with the Queue Run. Automatic Continuations
and unexpected daemon recovery reuse that selection without asking again. A
permission policy rejected by App Server fails clearly and is never replaced
with a different policy.

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

Only queued Tasks can be moved, and a queued Task cannot be moved before the
active Task. Cancelling a queued or Needs Attention Task keeps its terminal
metadata, preserves Workspace edits, and removes its prompt. Cancelling the
active Task also interrupts its Turn and pauses the whole Queue.

Transient network and service failures retry the same Managed Thread up to
three times with exponential backoff. Authentication, configuration, model,
process, permission, and unknown failures do not become Quota Pauses or advance
the Queue. Approval, permission, MCP elicitation, and user-input requests are
never approved unattended: Codex Resumer returns a protocol-safe cancellation
or empty response, marks the Task `needs_attention`, and pauses the Queue.

Resolve a Needs Attention Task explicitly, then resume the Queue separately:

```sh
codex-resumer task retry 3 "A new prompt based on the current Workspace state"
codex-resumer task complete 3
codex-resumer task cancel 3
codex-resumer queue resume --until-idle
```

`task retry` is available only for a Needs Attention Task and requires a new
prompt; the stored old prompt is never reused. `task complete` records manual
completion. Every manual resolution leaves the Queue paused until an explicit
resume. External activity on a Managed Thread also pauses the Queue for review
without interrupting a Turn that Codex Resumer already owns. `task list` and
`queue status` show Queue order, Task and Thread states, Workspace, and
Thread/Turn identifiers without displaying prompts.

Workspace paths are stored as canonical absolute paths. A successful Codex Turn
completes the Task and starts the next queued Task; no marker is required in the
model output. Every Workspace target creates a new Thread. Imported Threads are
validated with App Server and permanently bound to their recorded Workspace.
New Threads and Turns omit model, reasoning, and personality overrides, so Codex
uses the user's current defaults. Codex Resumer also leaves service tier,
credits, API keys, and authentication unchanged.

When App Server reports a structured `usageLimitExceeded` error, the active Task
enters `waiting_for_quota` while retaining its Managed Thread and failed Turn.
Codex Resumer selects the reached rate-limit bucket, waits until its server-provided
reset time plus a two-second safety margin, and reads the limits again before it
continues. If no reset time is available, it polls with exponential delays from one
minute up to a fifteen-minute cap. A recovered Task starts a new Turn in the same
Managed Thread with the current global Continuation prompt; the original Task prompt
is never submitted again. Repeated Quota Pauses follow the same process until the
Task completes, the Queue is manually paused, or its Cutoff Time is reached. If
App Server rejects the confirmed Access Mode during an automatic Continuation,
Codex Resumer pauses the Queue as `Needs Attention` and persists the structured
RPC error for `queue status`; it does not retry with weaker permissions.

`daemon start` detaches from the terminal. Starting it again is safe and keeps a
single daemon instance. At startup, Codex Resumer checks the installed protocol
schema for the quota, Thread, Turn, interruption, approval, and streamed-error
capabilities it needs. It then verifies the current ChatGPT account and reads
the current rate-limit state. An incompatible App Server or missing ChatGPT
login prevents the daemon from accepting work, and `daemon status` reports the
reason together with the installed Codex version.

After an unexpected daemon exit, startup reconciles the active Task against the
Turn history reported by App Server. A still-active Turn is resumed for event
listening without submitting another prompt. A successful Turn completes the
Task, and a structured usage-limit failure returns to Quota Pause recovery.
Interrupted, failed, idle-without-success, unavailable, or otherwise uncertain
state becomes Needs Attention. The existing Queue Run and its confirmed Access
Mode remain in effect only for this unexpected-restart recovery.

A normal `daemon stop` pauses the Queue and ends its Queue Run, so later work
requires an explicit `queue resume` and a new Access Mode confirmation. Normal
stop refuses while a Turn is active and explains how to let it finish or force
the stop. `daemon stop --force` interrupts the active Turn, records the Task as
Needs Attention, pauses the Queue, and exits.

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
the database when their Task completes or is cancelled. The global
configuration exposes only `accessMode` and `continuationPrompt`.

The append-only event log is `events.jsonl` in the state directory. Read the
current log or follow new records with:

```sh
codex-resumer logs read
codex-resumer logs follow
```

Each JSONL record contains a timestamp and event type, plus the relevant Task,
Thread, Turn, state transition, Quota Pause reset time, or sanitized error code.
It never records complete Task or Continuation prompts, Codex output,
environment variables, credentials, or complete command output. Automatic log
rotation is intentionally not part of the MVP.

Configuration, state, and runtime directories are mode `0700`. Configuration,
database, daemon status, socket, and event-log files are mode `0600`; the parent
directories provide the same current-user-only protection to transient SQLite
files.

## Troubleshooting

- If `daemon status` reports that the daemon is stopped, run
  `codex-resumer daemon start`.
- If startup reports an unauthenticated daemon, run `codex login`, confirm with
  `codex login status`, then start the daemon again.
- If startup reports missing App Server capabilities, update Codex CLI using
  the same installation method originally used, check `codex --version`, and
  run `codex-resumer daemon start` again.
- If the Queue is in Needs Attention, inspect `codex-resumer queue status` and
  `codex-resumer logs read`; resolve the named Task with `task retry`,
  `task complete`, or `task cancel`, then explicitly resume the Queue.
- If a Task is in Quota Pause, leave the daemon running. It uses the applicable
  server reset time and rechecks the account before starting a Continuation.
- A normal stop refuses during an active Turn. Let it finish, or use
  `codex-resumer daemon stop --force` knowing that the Task will require review.

## Optional login startup

Codex Resumer never installs a login service automatically. On a Linux system
using systemd user services, first find the absolute executable path with
`command -v codex-resumer`. Create `~/.config/systemd/user/codex-resumer.service`
with that path substituted for `/absolute/path/to/codex-resumer`:

```ini
[Unit]
Description=Codex Resumer daemon

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/absolute/path/to/codex-resumer daemon start
ExecStop=/absolute/path/to/codex-resumer daemon stop

[Install]
WantedBy=default.target
```

Then opt in and inspect its status:

```sh
systemctl --user daemon-reload
systemctl --user enable --now codex-resumer.service
systemctl --user status codex-resumer.service
```

Disable it with `systemctl --user disable --now codex-resumer.service`.

## Real Configured Access smoke test

The release smoke test uses an isolated XDG data directory, checks the real
Codex login and App Server capabilities, submits one harmless Task under
Configured Access, verifies status, and restarts the daemon. It never selects
Full Access. Because it consumes one real Codex Turn, it requires explicit
opt-in:

```sh
CODEX_RESUMER_RUN_REAL_SMOKE=1 npm run smoke:configured
```
