# Codex Resumer

Codex Resumer coordinates local Codex work across temporary usage interruptions. Its language distinguishes user intent, conversation context, filesystem scope, and quota-related waiting.

## Language

**Task**:
A unit of user intent expressed as a natural-language prompt for Codex and scoped to a Thread and Workspace.
_Avoid_: Command, shell command, job

**Queued Task**:
A Task waiting for an earlier Task to finish before it can begin.
_Avoid_: Queued command, script

**Queue**:
An ordered collection of Tasks in which at most one Task is active at a time.
_Avoid_: Command list, batch

**Continuation**:
A concise request asking Codex to continue an unfinished Task in its existing Thread after a Quota Pause.
_Avoid_: Replay, retry, rerun

**Thread**:
A Codex conversation whose accumulated context is preserved across Tasks and Continuations.
_Avoid_: Session, chat

**Managed Thread**:
A Thread created by Codex Resumer or explicitly placed under its control by the user. Codex Resumer is its sole expected writer while a Task is active.
_Avoid_: Current terminal, discovered session

**Turn**:
A single Codex response to a prompt within a Thread.
_Avoid_: Round, run

**Workspace**:
The local directory in which a Task is carried out.
_Avoid_: Project, repository

**Quota Pause**:
A temporary inability to continue Codex work because an applicable usage limit has been reached.
_Avoid_: Crash, reset, five-hour lockout

**Run Policy**:
The user's rule for how long a Queue may continue starting Tasks and Continuations without further input.
_Avoid_: Retry policy, schedule

**Queue Run**:
The period beginning with an explicit Queue start or resume and ending when the Queue becomes idle or paused. Its Access Mode confirmation remains valid across automatic Continuations and unexpected daemon restarts.
_Avoid_: Daemon lifetime, session

**Until Idle**:
A Run Policy that continues until the Queue is empty or the user explicitly pauses it.
_Avoid_: Unlimited run

**Cutoff Time**:
A one-time absolute time after which no new Task or Continuation may begin; a Turn already in progress may finish.
_Avoid_: Timeout, deadline, recurring schedule

**Needs Attention**:
A Queue condition that requires user action before any further Task or Continuation can begin.
_Avoid_: Failed, retrying

**Access Mode**:
The global permission boundary applied to every Task and Continuation when a Queue starts or resumes.
_Avoid_: Per-task permission, sandbox toggle

**Configured Access**:
An Access Mode that explicitly reapplies the user's current Codex permission configuration to each Turn.
_Avoid_: Default access, safe mode

**Full Access**:
An Access Mode that lets Codex operate without sandbox or approval restrictions after the user acknowledges the risk.
_Avoid_: Automatic mode, trusted mode
