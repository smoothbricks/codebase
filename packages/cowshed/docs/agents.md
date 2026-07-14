# Driving cowshed from coding agents

cowshed's CLI is designed to be **self-driving**: an agent that can run shell commands can operate cowshed correctly
without reading this document, because every command narrates its effects and next steps on stderr. This document makes
the implicit contract explicit.

Start with [usage.md](usage.md) for adoption, the multi-repository mental model, repository selection, and the current
command map. This guide focuses on deeper automation contracts.

## Select the repository before the workspace

`main` is repository-scoped. cwd or `--project <git-root>` selects one adopted repository; then `new` clones that
repository's `main` unless `--from <workspace>` selects another source inside the same repository. A host can therefore
run agents against many warm repositories without a host-global main.

Harnesses should pass `--project` explicitly rather than make correctness depend on their process cwd:

```sh
PROJECT=~/src/api
cowshed new task-4821 --project "$PROJECT" --json
```

`--repo-id` is only an adoption-time identity input. It is not how ordinary commands select a repository.

## The two-stream rule

- **Parse ordinary control stdout.** It contains one answer: a path, a name, TSV rows, or a bounded `--json` envelope.
  Job/status JSON is bounded control transport: it carries lifecycle fields, `StreamInfo`, hashes, summaries, and may
  carry a small `Inline.data` value tagged as `utf8` or `base64`. Unbounded output is available only through explicit
  foreground streaming, `cowshed job logs`, `cowshed job attach`, or artifact reads.
- **Read stderr.** All guidance lives there, in greppable, prefixed lines:
  - `cowshed: ...` — what just happened, or why something failed
  - `next: <command>` — concrete follow-up commands, valid to run as-is

An agent's loop is: parse the control answer, scan stderr for `next:` lines when deciding what to do, and switch on the
exit code when something fails. Treat explicit raw streams as opaque bytes; do not decode them merely to move or retain
them.

```sh
PROJECT=~/src/api
WS_PATH=$(cowshed new task-4821 --project "$PROJECT") # stdout only: the mountpoint
# stderr remains visible to the agent: cowshed: ... / next: cowshed exec task-4821 -- ...
```

Prefer `--json` when you want structure:

```sh
cowshed new task-4821 --project ~/src/api --json | jq -r .result.mount
```

## Warm workspaces for coding agents

Pattern: one cowshed workspace per agent task, instead of `git worktree add`. You get full isolation (the agent can
trash anything inside; the host is safe), warm caches (builds and installs are incremental from main's state), and O(1)
cleanup.

```sh
# spawn
PROJECT=~/src/api
WS=task-4821
WS_PATH=$(cowshed new "$WS" --project "$PROJECT")

# work — either cd into it with the caller's ordinary host permissions...
cd "$WS_PATH" && bun test

# ...or run sandboxed (recommended for autonomous work):
cowshed exec "$WS" --project "$PROJECT" -- bun install
cowshed exec "$WS" --project "$PROJECT" -- bun test
cowshed exec "$WS" --project "$PROJECT" -- cargo clippy --workspace

# ship & destroy
cowshed exec "$WS" --project "$PROJECT" -- git commit -am "implement feature"
cowshed push "$WS" --project "$PROJECT"
cowshed rm "$WS" --project "$PROJECT"
```

Notes for harness integration:

- `cowshed new` uses copy-on-write storage and is safe to call per task. Names are unique within the selected
  repository; exit 4 means the name is taken — pick another instead of retrying the same name.
- The workspace branch is `cowshed/<name>`; `cowshed push` delivers it to the main repo's refs, where a human (or a
  merge queue) takes over. Never push to main's checked-out branch.
- `cowshed rm` refuses (exit 4) if the branch was never pushed — that's your signal work would be lost. Push first or
  `--force` deliberately.
- If a task needs a _snapshot to retry from_, `cowshed checkpoint <ws> <label>` before the risky step and
  `cowshed restore <ws> <label>` on failure — both are milliseconds, and far cheaper than re-creating the workspace.
  Checkpoint publication first crosses a supervisor barrier: complete Arrow batches and spill files are sealed, and a
  manifest commits every checkpoint-resident job byte. A restored checkpoint starts a new workspace incarnation; the
  captured content remains authoritative only for the snapshot that created it, while controller commitments preserve
  existence, ordering, terminal state, hashes, and fork/restore lineage across that boundary.
- Parallel exploration: `cowshed fork <ws> <ws>-alt` mid-task gives you two divergent futures from the same state. Forks
  start with a closed sandbox regardless of the source's grants.

This is cowshed's layered capability model: the trusted coordinator holds policy authority; workers run closed and ask.
A worker handle controls exactly one workspace's exec, shells, jobs, quota-limited checkpoints, push, and read-only
grants. It cannot grant/revoke, restore/destroy/rebase/land, run gc, mirror repositories, or select another workspace.

## Bounded job results and raw artifacts

Every job has separate `StreamInfo { storage, bytes, sha256, summary }` handles for stdout and stderr. `storage` is
`Captured { artifact }` or `Redirect { source, artifact }`; the protected `artifact` is either
`Inline { data: BinaryData }` or `File { path: WorkspacePath }`. Small terminal streams may remain inline as Arrow
Binary, so a short job need not create `out` or `err` files. Protected files spill lazily. Raw reads always resolve
`artifact`, making logs and attachment representation-transparent.

`Redirect` exists only when a real shell AST proves a narrow literal `>`/`2>` workspace destination. Its
workspace-relative `source` is the live caller-visible path written by the shell and is never authoritative. After
terminal state cowshed snapshots the admitted bytes into an independent sealed `artifact`: inline when small, otherwise
a protected clone/reflink/copy file. It never hardlinks writable and protected paths. Ambiguous or unrecognized shell
text retains ordinary shell behavior; bytes redirected away from cowshed's pipes then do not appear in the job handle.

Separate `stdout_copy`/`stderr_copy` request fields publish a workspace-visible copy post-terminal from the canonical
artifact. That export does not alter `StreamInfo.storage` and is never used for reads or authority. Its destination is
also an independent clone/reflink/copy, never a hardlink.

Ordinary agent responses remain bounded and control-only. They carry lifecycle metadata, `StreamInfo`, and redacted
summaries. A small `Inline.data` artifact may appear in the response's tagged `utf8`/`base64` representation; the bound
prevents an unbounded byte array, base64 blob, or decoded stdout/stderr copy. To consume full-fidelity output of any
size, use `cowshed job logs <ws> <id>`, `cowshed job attach <ws> <id>`, or the frontend's raw artifact stream. These
paths preserve arbitrary binary bytes and keep stdout and stderr separate.

The supervisor is the sole writer under protected `.cowshed/job/**`. Every executed shell, named session, and descendant
receives a child restriction that denies writes there before repository-controlled startup; completed batches and files
are immutable. Recovery may discard only an incomplete trailing batch or file. A missing or hash-mismatched committed
artifact is an integrity failure, not an invitation to trust a newer-looking copy.

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

Shell `>`, `2>`, pipelines, expansions, and descriptor operations keep ordinary shell semantics. Bytes redirected away
from the supervisor's pipes are not captured output, and cowshed never aliases a protected artifact into a writable
destination. The combined stdout+stderr job quota defaults to 1 GiB; crossing it yields an explicit `output-limit`
terminal state after process-group termination and pipe drain, never silent truncation.

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
workspace-local numeric job id to resume status, raw logs, or attachment. The durable key is
`(repo_id, workspace_incarnation, job_id)`. A client disconnect never unlinks the socket or stops the job.

Protected in-volume Arrow records and canonical `Inline`/`File` artifacts are the captured-content authority within
their originating incarnation and checkpoint snapshot; a `Redirect.source` never is. Controller-owned immutable Arrow
segments carry compact continuity commitments for job existence, lifecycle, ordering, lineage, terminal state, byte
counts, stream hashes, and terminal-batch digest. They carry no artifact payload or path authority and never duplicate
raw stdout/stderr. Neither tier silently wins a mismatch: missing committed content or a digest disagreement is an
integrity failure.
