# Warm Shell Layer

`cowshed-shell` is the process-management layer between `cowshed-core` (which owns _what may run where_ — substrate,
mounts, sandbox profiles, grants) and every client that runs commands (CLI, MCP, CI, NAPI). It generalizes jcode's
shell-supervisor pattern: one long-lived supervisor per workspace holding a pool of warm shells with the composed
environment already loaded, a framed stdio protocol over a unix socket, job control, and the single exec-record capture
that all clients consume.

## Why a pool at all — honest calibration

A fresh workspace is a CoW clone of main, so it inherits main's `.direnv`/`.devenv` caches; whether that makes a fresh
clone's _first_ evaluation cheap is a listed verification item (04_sandbox.md, devenv section — it needs a real
`cowshed new` to test). What is already measured is the steady state: **a warm `direnv exec` on a devenv repo costs
~1.3–2.9 s per invocation** — `direnv export` dominates, dwarfing `sandbox-exec` startup and shell init. That makes the
pool load-bearing, not a nicety: per-command environment composition (04's exec pipeline wraps every command in
fail-closed `direnv export`) is too slow to pay per exec across the dozens of commands an agent or CI job runs. The
pool's value:

- **The per-command residual, measured ~1.5–3 s on a devenv repo** — paid once per warm shell instead of once per
  command.
- **Persistent shell state** — a named session keeps cwd, shell variables, and running jobs across calls, so an agent's
  multi-step task is one shell, not N independent `sandbox-exec` spawns.
- **The framed stdio protocol** — multiplexed, backpressured, language-neutral I/O for the NAPI and MCP clients, instead
  of each reinventing pipe plumbing.
- **Job control** — timeouts, auto-backgrounding, re-attach, structured capture.

The spec still states plainly: the pool is not what makes cowshed fast — the CoW clone and warm caches are that thing.
The pool is what keeps the _per-command_ cost from eating those wins.

## Supervisor

One supervisor process per attached workspace, spawned on first exec (or by `cowshed ensure` for warmed workspaces). It
is launched **once** under the workspace's launch-time sandbox profile, so its whole process tree — every shell it
forks, every child those shells spawn — inherits the Seatbelt/Landlock boundary transparently, with no per-command
wrapping (the jcode supervisor model, 04_sandbox.md).

- Holds K warm shells (default 2, `.cowshed.toml` `[shell] pool`), each an interactive shell that has already sourced
  the workspace environment via `direnv export`. Anonymous shells are reset (`cd` to mount root, environment re-applied
  from the cached snapshot, shell vars cleared) and returned to the pool after each anonymous exec.
- **Named sessions** (`--session <name>`, `WorkspaceHandle::shell(Some(name))`) are persistent shells outside the pool:
  state survives across calls until explicitly closed or the supervisor stops. This is how a coordinator gives one
  subagent a stable shell for a multi-step task.
- Idle timeout: the supervisor exits after `[shell] idle` (default 30 min) with no sessions and no running jobs, freeing
  memory; the next exec respawns it. `cowshed detach`/`cowshed rm` stop it immediately (teardown below).

## Protocol

A unix socket per supervisor at `<runtime dir>/<project_id>/<workspace>.sock` (the parent dir's write grant is in the
launch profile so the supervisor can `bind(2)`; the socket is unlinked on exit — jcode's short-lived-socket pattern).
Framing is deliberately minimal and binary so the NAPI and MCP clients need no parser beyond a length read:

```
frame = channel:u8  length:u32-le  payload:[u8; length]
channel = 0 control(JSON)  1 stdin  2 stdout  3 stderr  4 events(JSON)
```

- **control** — request/response JSON: `open-session`, `run`, `signal`, `resize`, `close`.
- **stdin/stdout/stderr** — raw bytes, backpressured (the reader's socket buffer is the flow-control signal; the
  supervisor pauses the child's stdout when the client stops reading).
- **events** — asynchronous JSON notifications: `job-backgrounded`, `job-exited`, `job-output-truncated`,
  `session-closed`. A client that only wants completion can ignore stdout/stderr and watch events.

The protocol is transport for both the CLI (which renders it to the stdout/stderr contract, 06_cli.md) and the MCP
server (which renders it to tool results, 12_mcp.md). One protocol, three front-ends.

## Job control

Every command runs as a _job_ with an id, whether foreground or backgrounded:

- **Soft timeout → auto-background.** A foreground command still running at its soft timeout (`--timeout`, default
  `[shell] soft_timeout` = 120 s) is detached: it keeps running, its stdout and stderr spool to
  `.cowshed/jobs/<id>/{stdout,stderr}` **inside the volume**, and an `event: job-backgrounded` fires. The client gets
  the job id and can walk away — the classic agent pattern of "this build is slow, background it and poll." Because the
  spool is in-volume, a `cowshed checkpoint` taken later captures the job's output alongside the filesystem it produced.
- **`--background`** forces detachment immediately.
- **Hard timeout** (`[shell] hard_timeout`, unset by default, set by CI — 10_ci.md) → SIGTERM, then SIGKILL after a
  grace, then the job is marked `killed:timeout`. This is how CI bounds a hung step.
- **Re-attach**: `cowshed jobs attach` / `Job::attach` re-opens stdio to a running job (replaying buffered-but-unread
  output, then live). `jobs logs` streams the spool (`--follow` to tail).

### Exec records — the single capture

Every job, on completion, appends one line to `.cowshed/jobs/records.ndjson`:

```json
{
  "id": "j-7f3a",
  "argv": ["bun", "test"],
  "cwd": ".",
  "envHash": "…",
  "grantRevision": 7,
  "started": "2026-07-11T12:34:56Z",
  "durationMs": 8421,
  "exit": 1,
  "signal": null,
  "stdout": ".cowshed/jobs/j-7f3a/stdout",
  "stderr": ".cowshed/jobs/j-7f3a/stderr",
  "truncated": false
}
```

This is the _only_ capture implementation in cowshed. The CLI's `jobs`, the MCP `job_status`/`job_logs` tools, and CI's
failure-diagnosis artifact (10_ci.md) all read these records and spools — no client re-implements capture, and every
consumer sees the same `grantRevision` and `envHash` needed to reproduce a run. Spools are size-capped
(`[shell] max_spool`, default 32 MiB per stream) with a `truncated` flag rather than unbounded growth.

**Not audit-grade.** These records live _inside_ the workspace volume, which is workspace-writable by construction — a
job can rewrite its own `records.ndjson` and spools. They are a reproduction/observability aid, not a tamper-evident
audit trail. The authoritative record of what authority a workspace held and what egress it made lives **outside** the
volume: the grant files and their revisions (controller-owned, 01/04) and the gateway's append-only audit log (05). When
those disagree with an in-volume record, the outside-the-volume source wins.

## Grant-change propagation

When a coordinator calls `grant`/`revoke` (04_sandbox.md, 07_api.md), the sandbox profile changes. The supervisor:

1. marks its pooled shells stale and drains them (finishes in-flight execs, then closes);
2. relaunches shells under the regenerated profile, **re-applying the cached environment snapshot** (no Nix
   re-evaluation — the env is unchanged, only the sandbox boundary moved);
3. reports the enforced `grant_revision` on subsequent execs so the coordinator knows which snapshot took effect.

Running jobs launched under the old profile continue under it until they end or are killed — matching 04_sandbox.md's
"filesystem grants bind at next exec." A coordinator needing a hard cut kills the tree (`ExecHandle::kill`) and re-runs.

**Named sessions refuse to run under stale authority.** A named session is a persistent shell pinned to the profile it
launched with. When a grant/revoke moves the revision, that shell's profile is now stale — so the next `run` on it is
**rejected** (a `Conflict` naming the revision mismatch: enforced _rev_ vs current _rev_), not silently executed under
the old boundary. Silently running would violate "next exec gets the grant" and, on a _revoke_, would run with authority
the coordinator just removed. The caller closes and reopens the session (or the coordinator kills the tree) to pick up
the regenerated profile. Anonymous pooled shells need no such rule — they are drained and relaunched under the new
profile automatically (above), so an anonymous next-exec always sees the current revision.

## Teardown ordering

`cowshed rm` / `cowshed detach` must stop the supervisor **before** unmounting or destroying the substrate, because live
children hold the mount busy and would force `-force` detaches or `zfs destroy` retries. The daemon/long-lived-child
population is large and project-specific — Nx daemon, Gradle/Kotlin daemons, watchman, Metro, `workerd`/miniflare,
Verdaccio, DynamoDB Local, MinIO, Jupyter kernel gateways, LSP servers, DAP adapters, `devenv up` service trees, and
anything a job double-forks — so the supervisor never enumerates by name; it tracks its **process group** and tears down
the whole descendant tree regardless of what those processes are. Order:

1. supervisor SIGTERMs its session/job tree, waits the grace, SIGKILLs stragglers;
2. supervisor exits, unlinking its socket;
3. only then does `cowshed-core` detach/destroy (02_workspaces.md, 09_substrates.md).

The supervisor tracks its descendants (process group) precisely so this is deterministic — another reason the shell
layer is not optional glue but core to correct lifecycle.

## ensure / doctor awareness

- `cowshed ensure` (workspace attached) checks supervisor liveness and restarts it if the socket is dead; it never
  starts one proactively on the ≤25 ms fast path unless a stale socket is found.
- `cowshed doctor` reports orphaned sockets (no live supervisor), stuck jobs (running past hard timeout with a dead
  supervisor), and spool directories exceeding budget, each with a `fix:` hint.

## Tradeoffs

**Per-command sandbox wrapping rejected as the default.** Wrapping every command in its own `sandbox-exec` (the fallback
path, still available for one-shot `cowshed exec` with no supervisor) pays the full startup cost per command and cannot
hold persistent shell state. Launch-time supervisor sandboxing pays it once; the cost — grants bind only at shell
relaunch — is surfaced, not hidden.

**A cross-workspace shared supervisor rejected.** One supervisor serving many workspaces would straddle sandbox
boundaries (the exact sccache-daemon bug jcode hit) and couple unrelated workspaces' lifecycles. One supervisor per
workspace keeps the boundary and the teardown clean.

**Text-log capture rejected.** Scraping merged terminal text is what forces agents into brittle `grep` parsing.
Structured exec records with separated, spooled streams and the grant/env context make a failed run reproducible instead
of merely readable (10_ci.md).
