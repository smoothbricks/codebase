# Driving cowshed from coding agents

cowshed's CLI is designed to be **self-driving**: an agent that can run shell commands can operate cowshed correctly
without reading this document, because every command narrates its effects and next steps on stderr. This document makes
the implicit contract explicit.

## The two-stream rule

- **Parse stdout.** It contains exactly one thing: the answer (a path, a name, TSV rows, or a `--json` envelope). Piping
  cowshed into `grep`, `jq`, `xargs`, or a variable never captures prose.
- **Read stderr.** All guidance lives there, in greppable, prefixed lines:
  - `cowshed: ...` — what just happened, or why something failed
  - `next: <command>` — concrete follow-up commands, valid to run as-is

An agent's loop is: capture stdout for data, scan stderr for `next:` lines when deciding what to do, and switch on the
exit code when something fails. Never parse prose out of stdout — there is none — and never expect data on stderr.

```sh
WS_PATH=$(cowshed new task-4821)          # stdout only: the mountpoint
# stderr (seen by the agent, not captured): cowshed: ... / next: cowshed exec task-4821 -- ...
```

Prefer `--json` when you want structure:

```sh
cowshed new task-4821 --json | jq -r .result.path
```

## Warm workspaces for coding agents

Pattern: one cowshed workspace per agent task, instead of `git worktree add`. You get full isolation (the agent can
trash anything inside; the host is safe), warm caches (builds and installs are incremental from main's state), and O(1)
cleanup.

```sh
# spawn
WS=task-$RANDOM
WS_PATH=$(cowshed new "$WS")

# work — either cd into it (inherits your session's permissions)...
cd "$WS_PATH" && bun test

# ...or run sandboxed (recommended for autonomous work):
cowshed exec "$WS" -- bun install
cowshed exec "$WS" -- bun test
cowshed exec "$WS" -- cargo clippy --workspace

# ship & destroy
cowshed exec "$WS" -- git commit -am "implement feature"
cowshed push "$WS"
cowshed rm "$WS"
```

Notes for harness integration:

- `cowshed new` is fast (~250 ms) and safe to call per-task. Names must be unique per project; exit 4 means the name is
  taken — pick another, don't retry the same one.
- The workspace branch is `cowshed/<name>`; `cowshed push` delivers it to the main repo's refs, where a human (or a
  merge queue) takes over. Never push to main's checked-out branch.
- `cowshed rm` refuses (exit 4) if the branch was never pushed — that's your signal work would be lost. Push first or
  `--force` deliberately.
- If a task needs a _snapshot to retry from_, `cowshed checkpoint <ws> <label>` before the risky step and
  `cowshed restore <ws> <label>` on failure — both are milliseconds, and far cheaper than re-creating the workspace.
- Parallel exploration: `cowshed fork <ws> <ws>-alt` mid-task gives you two divergent futures from the same state. Forks
  start with a closed sandbox regardless of the source's grants.

This is cowshed's layered capability model: the trusted coordinator holds policy authority; workers run closed and ask.
A worker handle controls exactly one workspace's exec, shells, jobs, quota-limited checkpoints, push, and read-only
grants. It cannot grant/revoke, restore/destroy/rebase/land, run gc, mirror repositories, or select another workspace.

## Binary stdin without shell interpolation

Agent harnesses should use cowshed's structured stdin paths rather than quoting binary or untrusted data into a command:

```sh
producer | cowshed exec "$WS" --stdin -- ./consume
cowshed exec "$WS" --stdin-file fixtures/request.bin -- ./consume
```

Inline bytes, backpressured streams, and workspace-relative file sources remain separate from argv/shell text. File
sources are regular files opened beneath the workspace with no-follow traversal. Job metadata reports source kind,
delivered bytes, EOF completion, and an optional relative path, never inline contents. Canceling an input stream closes
stdin without implicitly killing the job.

A trivial `>`/`2>` shell form may take an optimized path only when a real shell AST parser proves the narrow eligible
shape; every ambiguity falls back to ordinary shell execution and capture. Harnesses must not depend on whether the
optimization fires. The combined stdout+stderr job quota defaults to 1 GiB; crossing it yields an explicit
`output-limit` terminal state after process-group termination and pipe drain, never silent truncation.

## Sandbox etiquette: start closed, earn grants

Every workspace starts closed: writes limited to its own volume, designated cache subtrees, and temp; network limited to
your own gateway listener (whose registry mirrors — npm, crates.io — are baseline policy, warm and credentialed, so
builds and installs work out of the box with zero grants). Third-party repositories arrive through
`cowshed repo clone <url>`, which the gateway mirrors on your behalf. Need a dev server? Bind it to `$PORT` — each
workspace owns a block of ports above its gateway base (`$COWSHED_PORT_BASE`), reachable from the host browser
container-style and guaranteed not to collide with sibling workspaces.

**Exit code 6 is not an error to retry — it is a request to negotiate.** When cowshed reports 6 it has authoritative
evidence of the denial (egress denials always — the gateway logged the decision; filesystem denials when the kernel
sandbox telemetry can be correlated) and prints the exact grant that would allow it:

```
$ cowshed exec task-4821 -- ./scripts/sync-fixtures.sh
cowshed: sandbox denied network-outbound fixtures.internal.example:443
cowshed: egress from 'task-4821' is limited to the gateway allowlist
next: cowshed grant task-4821 --egress fixtures.internal.example
```

The correct agent behavior on exit 6:

1. Do **not** silently self-grant if you hold the authority to; surface the `next:` line to the operator or the
   coordinating agent as a permission request.
2. Once granted, retry the _original_ command (filesystem grants apply from the next exec).
3. Treat grants as scoped to the task: they die with the workspace, and `cowshed revoke <ws> --all` returns to closed if
   you widened temporarily.

Not every denial is diagnosable: a deep child process blocked by the sandbox may surface only as its own nonzero exit,
passed through unchanged. If a command fails writing outside your volume or reaching the network and there was no exit
6, suspect the sandbox anyway — `cowshed doctor` shows recent gateway denials, and the troubleshooting guide has the
Seatbelt log incantation.

This is cowshed's layered capability model: the trusted coordinator holds policy authority; workers run closed and ask.

## Concurrency and hygiene

- Workspaces are cheap; don't pool or reuse them across tasks. Fresh clone per task means reproducible starting state
  (whatever main was at that moment).
- Workspace enumeration and attachment state derive from disk; the persistent supervisor owns only live process/job
  control and recovers from durable job ids plus controller telemetry. If a harness crashes, `cowshed ls` still tells
  the truth, clients reconnect to running jobs, and `cowshed rm` cleans up completely.
- Run `cowshed ensure` at task start if your harness may outlive reboots; it is a no-op (~20 ms) when healthy and
  repairs mounts when not.
- `cowshed gc` is safe to run between tasks; it never touches live workspaces.

## What agents must not expect

- No writes to `$HOME`, no reading `~/.ssh`, `~/.aws`, keychains, or other projects — closed means closed, and
  `cowshed grant` is the only door.
- No direct internet. The gateway's mirrors make registry traffic invisible to you; arbitrary hosts need an egress
  grant. Credentials are injected upstream by the gateway — there are no tokens inside the workspace to find, and no
  `.env` files.
- No remote git. Your repo has exactly the local `host` remote (main) and whatever mirrors `cowshed repo clone` brought
  in; pushing to origin/GitHub is the coordinator's job, host-side.
- No reaching the human's simulator. iOS test loops run on **dev-side headless simulators** (`--preset simulator`;
  `simctl`, XCUITest, `simctl io` screenshots). The personal-session simulator is reachable only through `--sim` grants
  the coordinator gives you — `install` is human-gated per artifact by design ([ios.md](ios.md)); ship builds with
  `cowshed sim export` and let the human pull them in.
- Desktop apps: test them **in dev's own session** — run the built `.app` and drive it via accessibility APIs /
  AppleScript (no grant, same uid). You cannot launch an app in the human's session; there is no verb for it. Ship a
  build with `cowshed app export` — the human runs `cowshed app promote` to use it ([desktop.md](desktop.md)).
- No mutating cowshed's own state files: markers are informational, grants live outside your volume.

## Coordinator and worker connections

MCP coordinator authority is transferred only on a dedicated inherited FD/socketpair — never stderr, argv, environment,
or a workspace file. A coordinator mints a worker a 256-bit descriptor bound to one workspace and the expected peer and
socket; it is memory-only, one-use, atomically consumed, expires in 30 seconds, and dies on server restart. Presenting a
worker capability to a coordinator-only tool returns the dedicated authorization RPC error before dispatch, not a
sandbox-denied outcome. Escalation remains an agent-to-agent policy decision; there is no interactive consent protocol.

Workspace enumeration and attachment state derive from disk, while a persistent per-workspace supervisor provides job
control. Its permission-checked socket accepts concurrent clients and survives individual disconnects; reconnect by the
workspace-local numeric job id to resume status, logs, or attachment. The durable key is
`(repoId, workspaceIncarnation, jobId)`. A client disconnect never unlinks the socket or stops the job. Authoritative
job state comes from controller-owned immutable per-writer telemetry segments. Workspace-local records travel with
checkpoints but are editable reproduction data and must never drive authorization, denial, quota, or success decisions.
