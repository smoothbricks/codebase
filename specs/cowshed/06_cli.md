# CLI Contract

The `cowshed` binary is self-driving: an agent that has never seen it can operate it from its own output. Two rules make
that possible and they are absolute:

1. **stdout is machine-readable only.** A bare value (usually a path) or, with `--json`, one JSON envelope. Nothing else
   — ever. `cowshed path raven | xargs ls` and `cowshed ls | grep …` must never see prose.
2. **Everything for humans and agents goes to stderr**: progress, warnings, and — after every command — actionable
   hints. Guidance can never contaminate pipes or grep. Guidance lines are prefixed `cowshed:`; trailing hint lines are
   prefixed `next:`.

```
$ cowshed new raven
~/.cowshed/mnt/acme/widget/raven        ← stdout (bare mount path)
cowshed: created workspace raven from main @ 8f31c2d (612ms)     ← stderr
next: cd "$(cowshed path raven)"                                 ← stderr
next: cowshed exec raven -- bun install                          ← stderr
next: cowshed grant raven --egress registry.npmjs.org            ← stderr (if gateway saw no grants)
```

## JSON envelope (frozen)

`--json` on any command emits **exactly this discriminated envelope**, one line on stdout:

```json
{"ok":true,"result":{"workspace":"raven","mount":"…","baseCommit":"8f31c2d"}}
{"ok":false,"error":{"code":"conflict","message":"workspace raven already exists","hint":"cowshed ls"}}
```

`ok` is the discriminant; success carries `result`, failure carries `error` with `code` (taxonomy name), `message`, and
`hint`. No other top-level keys — `cmd`, `detail`, and a numeric `code` are **not** part of the envelope. This is the
single frozen shape; contract goldens (08_testing.md) enforce it, and any package doc showing a different arrangement is
stale, not authoritative. Long operations additionally emit NDJSON progress events on stderr with `--json`
(`{"event":"attach","ms":233}`) — NDJSON here is a **wire encoding on a pipe to a live consumer**, the same role it
plays for `--ndjson` export flags; cowshed never writes NDJSON to disk (telemetry storage is Arrow, 13_telemetry.md).

### Exec and job JSON (frozen)

Every `cowshed exec` submission, foreground or background, atomically allocates a positive, workspace-local,
monotonically increasing numeric `JobId`; ids are never reused. In normal mode the child's raw stdout remains CLI
stdout, so the control-plane job id and state are reported on stderr (a forced or automatic background prints the bare
numeric id on stdout instead). With `--json`, stdout contains only the final JSON envelope: `result` carries the numeric
`jobId`, `JobInfo` lifecycle/result metadata, and the bounded summaries described below. Live progress on stderr remains
control-only NDJSON. No child byte, base64 field, byte array, or UTF-8-decoded log fragment is ever mixed into either
JSON channel.

`JobInfo` also carries structured stdin metadata (`kind`, delivered `bytes`, `complete`, and an optional normalized
workspace-relative `path`) and the explicit `output-limit` terminal state. The configurable combined stdout+stderr
capture quota defaults to 1 GiB. Crossing counts persisted plus in-flight bytes, then TERM/grace/KILLs the process
group, drains both pipes, and publishes `output-limit`; cowshed never silently truncates output while a job continues.
Authoritative state comes from controller-owned immutable per-writer telemetry segments, not the workspace-writable
record projection.

Each job's retained captured streams are independent raw byte files inside its workspace: `.cowshed/job/<id>/out` and
`.cowshed/job/<id>/err`. JSON carries `stdout` and `stderr` `StreamInfo` objects; each has `path`, `bytes`, and a
`summary` object with `version`, `text`, and `truncated`. Summaries are deterministic, size-bounded, versioned, and
redacted text projections; they are identical in control messages, CLI/NAPI DTOs, and Arrow exec records. They exist for
orientation only: no denial, exit status, output-limit, policy decision, or build-success decision may inspect or depend
on a summary. The raw files contain every byte admitted before any combined-quota trip and are the sole full-fidelity
source for those bytes. Consequently `cowshed job logs` without `--json` may copy raw bytes to its selected output
stream, while `--json` returns only metadata, paths, and summaries in the frozen envelope.

## Exit codes (stable API)

Two disjoint ranges. **cowshed's own outcome** uses 0–6; a **child process run under `cowshed exec`** has its exit code
passed through _unchanged_, and cowshed-wrapper failures that occur while trying to exec use the **100–105** range so
they can never be confused with a child that legitimately exits 1–6.

| Code | Meaning                       | Example                                                                   |
| ---- | ----------------------------- | ------------------------------------------------------------------------- |
| 0    | success                       |                                                                           |
| 1    | internal error (bug — report) | panic, unexpected hdiutil failure                                         |
| 2    | usage                         | unknown flag, bad workspace name                                          |
| 3    | not found                     | no such workspace/project/checkpoint                                      |
| 4    | conflict / busy               | name taken, mount busy, push to checked-out branch                        |
| 5    | environment missing           | not adopted, gateway down when required, no APFS                          |
| 6    | sandbox-denied                | grant intersects secret denies; denial proven pre-spawn or by the gateway |

**`cowshed exec` exit semantics (frozen):**

- The child's exit code passes through **exactly** — a child that exits 6 reports 6 as _its own_ status, not a cowshed
  denial.
- cowshed-wrapper failures during exec use a separate range: **100** internal, **101** usage, **102** not-found, **103**
  conflict, **104** env-missing, **105** sandbox-denied-pre-spawn (a grant that intersects a secret deny, or a profile
  that fails to apply, caught _before_ the child runs).
- Exit/typed **6** is only ever emitted on **authoritative denial evidence**: pre-spawn validation, profile-application
  failure, gateway policy denial, or unified-log enforcement telemetry correlated by pid (schema in 04_sandbox.md).
  cowshed **never** synthesizes a denial — or a suggested grant — by scanning child stdout/stderr for strings like
  "operation not permitted". With no authoritative evidence, the child's ordinary non-zero exit is preserved untouched.
- NAPI/MCP surface these as structured errors/outcomes rather than overloading a numeric code (07_api.md, 12_mcp.md).

## Commands

Global flags: `--json`, `--project <git-root>` (default: cwd's repo), `-q`/`--quiet` (aliases; suppress stderr guidance,
never errors).

| Command                                       | stdout                                           | Notes                                                                                                                                                                                                                                                                                                                                                                                                              |
| --------------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cowshed adopt [path]`                        | mount path                                       | Convert checkout → main workspace (02). `--capacity <size>`.                                                                                                                                                                                                                                                                                                                                                       |
| `cowshed new <name>`                          | mount path                                       | Clone main → session. `--ref <rev>`, `--from <ws>`, `--browse`, `--slot <n>`.                                                                                                                                                                                                                                                                                                                                      |
| `cowshed ls`                                  | one name per line (`--json`: full records)       | Includes `main`, mount state, branch, base commit, age. Detached rows degrade — see below.                                                                                                                                                                                                                                                                                                                         |
| `cowshed path <ws>`                           | mount path                                       | Exit 3 if unknown; attaches if detached (unless `--no-attach`).                                                                                                                                                                                                                                                                                                                                                    |
| `cowshed exec <ws> -- <cmd…>`                 | child's stdout                                   | Sandboxed argv exec (04). Every submission creates a numeric `JobId`; `--stdin` reads binary caller stdin, `--stdin-file <rel>` reads a safe workspace-relative regular file, and `--stdin-base64 <data>` supplies inline bytes without shell interpolation. Control reports the id on stderr (`--json`: result metadata). `--ro`, `--cwd <rel>`. Child exit passes through unchanged; wrapper errors use 100–105. |
| `cowshed shell <ws>`                          | — (interactive)                                  | Sandboxed login shell inside the mount.                                                                                                                                                                                                                                                                                                                                                                            |
| `cowshed repo mirror <url>`                   | mirror path                                      | Gateway fetches `<url>` into a read-only bare mirror on the cache volume (02/05). Repo-scoped egress grant required.                                                                                                                                                                                                                                                                                               |
| `cowshed repo clone <url> [dir]`              | clone path                                       | `repo mirror` then a local `git clone --dissociate` into the workspace (default dir: repo basename).                                                                                                                                                                                                                                                                                                               |
| `cowshed sim export <ws> [artifact]`          | drop path                                        | Copy a built iOS `.app` to the one-way drop dir for the personal-session simulator (02/14). Default: newest built app.                                                                                                                                                                                                                                                                                             |
| `cowshed app export <ws> [artifact]`          | drop path                                        | Mac-target sibling of `sim export`: copy a built macOS `.app` to the drop dir (02/14).                                                                                                                                                                                                                                                                                                                             |
| `cowshed app promote [artifact]`              | installed path                                   | **Personal-session, human-only** (no `<ws>`, not agent-invokable): verify signature (Developer-ID default), install a drop-dir build into `~/Applications`, clear quarantine (14_nix.md). `--force` for ad-hoc, `--system` for `/Applications`.                                                                                                                                                                    |
| `cowshed ensure`                              | `--envrc`: export lines (≤2 load-bearing; 03)    | ≤25 ms fast path; heals mounts (02). `--attach` for stubs.                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed grant <ws> …`                        | new grant revision                               | `--read/--write <path>`, `--egress <host[:port]> [--opaque] [--impersonate <p>]`, `--repo <host/org[/repo]>`, `--sim <verb>`, `--preset simulator` (04/05).                                                                                                                                                                                                                                                        |
| `cowshed revoke <ws> …`                       | new grant revision                               | Same selectors + `--all`.                                                                                                                                                                                                                                                                                                                                                                                          |
| `cowshed push <ws>`                           | pushed ref                                       | `--branch <name>`. Host-side fetch from the mount (02); refuses checked-out branch (exit 4).                                                                                                                                                                                                                                                                                                                       |
| `cowshed rebase <ws>`                         | new head sha                                     | Rebase branch onto host/main (02). `--fresh` sheds divergence.                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed land <ws>`                           | landed sha                                       | Rebase + validate + host ff-merge + retire (02). `--check <cmd>`, `--no-retire`, `--push-only`.                                                                                                                                                                                                                                                                                                                    |
| `cowshed fork <src> <dst>`                    | mount path                                       | Mid-flight CoW copy; closed grants.                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed checkpoint <ws> [label]`             | label                                            | Crash-consistent snapshot; omitted label → generated UTC-timestamp label. `--keep` exempts from gc.                                                                                                                                                                                                                                                                                                                |
| `cowshed restore <ws> <label>`                | mount path                                       | Label required. Previous image kept as `pre-restore-…` (02).                                                                                                                                                                                                                                                                                                                                                       |
| `cowshed ps <ws>`                             | familiar `ps` table (`--json`: `JobInfo[]`)      | All jobs, foreground and background: numeric ID, PID, state, elapsed time, exit/signal, and command. Primary interactive overview (11).                                                                                                                                                                                                                                                                            |
| `cowshed job list <ws>`                       | one numeric job id per line (`--json`: records)  | Deliberately retained although `ps` also lists jobs: completes the `job` command family and gives scripts a minimal discovery surface.                                                                                                                                                                                                                                                                             |
| `cowshed job status <ws> <id>`                | one status record                                | Current `JobInfo`, including stream paths and summaries when available.                                                                                                                                                                                                                                                                                                                                            |
| `cowshed job logs <ws> <id>`                  | raw spooled stdout                               | `--stderr`, `--follow`; reads `.cowshed/job/<id>/out` or `err`. With `--json`, returns metadata and summaries only.                                                                                                                                                                                                                                                                                                |
| `cowshed job attach <ws> <id>`                | streamed raw I/O                                 | Attach to a running job, replaying unread bytes before live bytes (11).                                                                                                                                                                                                                                                                                                                                            |
| `cowshed job detach <ws> <id>`                | — (no stdout)                                    | Detach the client; the job and its raw stream capture continue.                                                                                                                                                                                                                                                                                                                                                    |
| `cowshed job kill <ws> <id>`                  | — (no stdout)                                    | Kill the complete process tree; id and record remain queryable.                                                                                                                                                                                                                                                                                                                                                    |
| `cowshed job wait <ws> <id>`                  | final status                                     | Wait for a terminal state; `--json` returns final `JobInfo`.                                                                                                                                                                                                                                                                                                                                                       |
| `cowshed rm <ws>`                             | — (no stdout)                                    | Perceived-instant (02). Refuses unpushed branch without `--force`; `--force` also required for main / dirty.                                                                                                                                                                                                                                                                                                       |
| `cowshed attach <ws>` / `cowshed detach <ws>` | mount path / —                                   | Explicit mount lifecycle. `--browse`.                                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed du [ws]`                             | `--json`: written/referenced per ws + checkpoint | CoW-aware usage; lists checkpoints per workspace (01).                                                                                                                                                                                                                                                                                                                                                             |
| `cowshed logs`                                | human table (`--json`/`--ndjson`: events)        | Controller telemetry (13). `--ws`, `--kind`, `--since`, `--follow`. Wraps `lmao-inspect` over the store segments.                                                                                                                                                                                                                                                                                                  |
| `cowshed audit`                               | human table (`--json`/`--ndjson`: events)        | Gateway audit events (05/13). `--denied`, `--host`, `--ws`, `--follow` (live tail via the control plane).                                                                                                                                                                                                                                                                                                          |
| `cowshed trace <trace-id>`                    | human waterfall (`--json`: span tree)            | Terminal waterfall of a lifecycle op, exec, or land (13).                                                                                                                                                                                                                                                                                                                                                          |
| `cowshed mcp serve`                           | — (stdio/socket server)                          | Coordinator authority arrives only on an inherited FD/socketpair; it is never printed or placed in argv/environment (12).                                                                                                                                                                                                                                                                                          |
| `cowshed gateway run`                         | — (foreground daemon)                            | launchd runs this.                                                                                                                                                                                                                                                                                                                                                                                                 |
| `cowshed gateway status`                      | `--json` status                                  | Cache stats, per-workspace counters, telemetry segment stats.                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed gc`                                  | freed bytes                                      | Trash drain, checkpoint/retention pruning, orphan mountpoints, compaction (01). `--dry-run`.                                                                                                                                                                                                                                                                                                                       |
| `cowshed doctor`                              | `--json` findings                                | Invariant checks; each finding carries a `fix:` hint.                                                                                                                                                                                                                                                                                                                                                              |

### `cowshed ls` detached-row degradation

`ls` must never attach an image to read it (that would blow the ≤50 ms budget and mutate mount state), but the base
commit, branch, and age live in the in-image marker. So for a **detached** workspace those fields come from the snapshot
cowshed cached in the grants sidecar at detach time (01_storage.md). If that snapshot is absent (e.g. a workspace
detached by an older cowshed, or a crash before the snapshot was written), the marker-derived columns are emitted
**empty** rather than by attaching — `state` and `name` are always accurate because they derive from readdir +
getmntinfo alone.

### Checkpoint listing

Checkpoints are **not** listed by a bare `cowshed checkpoint <ws>` (that form generates a timestamped snapshot). List
them with `cowshed ls --json` (per-workspace `checkpoints` array) or `cowshed du <ws>` (which reports each checkpoint's
written/referenced bytes).

### Egress grant modes

`--egress <host>` grants an intercepted host by default: the gateway terminates TLS under the workspace CA and injects
the Keychain credential + trace context (05_gateway.md). `--opaque` reverts a host to an opaque CONNECT tunnel (pinned
clients, no injection); `--impersonate <profile>` presents a browser-shaped outbound fingerprint and suppresses header
injection. A bare `cowshed grant <ws>` (no flags) prints the current set with `mode` and `impersonate` columns on the
egress rows.

`--sim <verb>` grants personal-session simulator broker verbs (`openurl`, `install` — 04/05/14); dev-side headless
simulators need the `--preset simulator` profile class instead (CoreSimulator IPC), not a `--sim` grant. `install` is
additionally bound to drop-dir artifacts and the human-gating rule (14_nix.md).

`cowshed exec` and `cowshed shell` accept `--session <name>` to bind to a named persistent shell in the workspace
supervisor (state — cwd, env, jobs — persists across calls); without it, exec uses an anonymous pooled shell
(11_shell.md). Long commands auto-background on the soft timeout; `--timeout <dur>` sets it and `--background` forces it
immediately, printing the numeric `JobId` on stdout. Foregrounding versus backgrounding changes attachment only: both
are jobs with the same id, raw backing files, status, wait, kill, and retention contract.

`--slot <n>` (on `cowshed new`) mounts the workspace at a stable, recycled path
(`~/.cowshed/mnt/<owner>/<repo>/slot-<n>`) instead of a name-derived one. `owner` and `repo` are the separately
validated and encoded components of the primary `repo_id`, never an unsplit path value. Successive workspaces in the
same slot inherit each other's **path-keyed** cache warmth (cargo incremental, Xcode DerivedData) — opt-in, because it
trades workspace-path uniqueness for warmth and only one workspace may hold a slot at a time (exit 4 if occupied).

## Self-driving conventions

- Every failure's stderr includes the exact command that would resolve it
  (`cowshed: workspace not mounted — run: cowshed attach raven`), and `--json` errors carry the same in `hint`. Agents
  recover without documentation.
- `cowshed` with no args prints a compact command map to stderr and exits 2 — safe for probing.
- Destructive operations (`rm`, `restore`) state on stderr precisely what will be destroyed and which flag confirmed it;
  there are no interactive prompts, ever (agents can't answer them). Missing confirmation flags are exit 2 with the
  completed command line in the hint.
- Output stability: bare-stdout shapes and JSON keys are covered by CLI contract tests (08_testing.md); changing them is
  a breaking change.

## Configuration

None required. `cowshed adopt` through daily use works with zero files. Optional `.cowshed.toml` at the repo root, all
keys optional:

```toml
capacity = "100g"              # image cap
[checkpoints]
keep = 5
[gateway]
port = 7644                    # macOS host control plane; Linux private-netns connector; never a service port
port_range = "40960-49151"    # macOS only: reserved range workspace port blocks are carved from
block = 16                     # macOS only: base = data-plane listener; base+1.. = workspace dev servers
[cache]                        # extend (never replace) the convention table
extra_workspace_dirs = ["build-out"]
[shell]
output_limit = "1GiB"         # combined stdout+stderr per job; explicit output-limit terminal state
```

On macOS, `cowshed ensure --envrc` additionally exports `PORT` and `COWSHED_PORT_BASE` (= the block base) as dev-server
conventions, so `vite`/`astro`/`metro`/`devenv up` bind inside the workspace's own block instead of colliding with
siblings (04_sandbox.md). Linux allocates no port block: dev servers bind its private loopback, while ordinary package
tools retain their configured `http://127.0.0.1:7644/…` proxy and registry URLs through exactly one trusted minimal
connector launched inside that workspace's private loopback-only network namespace under a distinct, controller-owned,
non-signalable identity/cgroup. It binds only `127.0.0.1:7644` and forwards bytes only to the workspace's mounted
`/run/cowshed/gateway.sock`; it holds no policy or credentials and exposes no general TCP or Unix-socket forwarding API.
The socket inode plus network namespace selects the workspace and the opaque token still authenticates it. Detach or
restore drains and kills the connector and unlinks the old socket. On both platforms `ensure` exports `GOENV` pointing
at the in-image Go environment file — Go's one load-bearing export, since Go has no directory-scoped config to carry the
per-workspace `GOPROXY` (03_caches.md).

## Tradeoffs

**Prose-on-stdout rejected.** Mixed streams are the root cause of agents parsing with `grep` and breaking on wording
changes. The stdout/stderr split costs nothing for humans (terminals merge the streams visually) and makes every cowshed
invocation composable. `-q` exists for callers that want silence, not a different contract.

**Interactive prompts rejected.** A confirmation prompt is an API that only humans can call. Explicit flags (`--force`)
plus precise stderr statements provide the same safety with a uniform caller model.

### Structured stdin

`cowshed exec` accepts exactly one stdin source. With no stdin flag it inherits interactive stdin as usual; `--stdin`
explicitly streams opaque bytes from the invoking process, `--stdin-base64 <data>` decodes inline bytes strictly before
submission, and `--stdin-file <rel>` asks the supervisor to open a workspace-relative regular file. These flags populate
`ExecRequest.stdin`; they never rewrite argv, interpolate shell text, or synthesize `< file`. File paths must remain
beneath the workspace and are opened read-only with no-follow traversal; absolute paths, symlinks, devices, sockets,
directories, and races fail closed. Backpressure reaches the caller/file reader, EOF closes child stdin once, and
interrupted input records incomplete delivery in `JobInfo` without implicitly killing the job.

### Shell redirection optimization

When an explicit shell command contains a trivial redirection, cowshed may optimize it only with a real shell AST
parser. The eligible form is one top-level simple command with literal, in-workspace, same-filesystem nonexistent `>`
and/or `2>` destinations under default clobber semantics. Append, fd duplication, pipelines, subshells, expansions,
symlinks, `noclobber`, existing targets, ambiguity, or a failed check always use ordinary shell execution and capture.
For an eligible form, the supervisor may create one inode and hardlink the destination with the corresponding job
`out`/`err` path; streaming, summaries, and quota account that inode once. Parsing is never an authorization, denial, or
correctness dependency.
