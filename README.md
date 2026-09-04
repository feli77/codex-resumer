# Codex Resumer

Codex Resumer is a local Linux CLI and daemon for coordinating Codex work across
temporary usage interruptions. This initial slice provides a capability-checked
daemon lifecycle; Queue and Task commands will be added separately.

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
status file are mode `0600`.
