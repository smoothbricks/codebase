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
The CLI output module serializes `cowshed_core::api::JsonEnvelope<T>` and `CowshedError` directly. It has no local JSON
value tree, error record, or envelope encoder. The sealed `ResultBody` bound rejects `()` and anonymous adapter maps;
empty success is exactly the named `EmptyResult {}` body, never `null`.

### JSON result bodies (frozen)

Every command uses a named `cowshed-core` DTO as its `result`; adapters never assemble anonymous maps. `adopt`, `new`,
`fork`, `restore`, `attach`, and `path` return `MountResult { workspace, mount, baseCommit? }`; lifecycle commands fill
`baseCommit`, while query/attachment paths may omit it when no marker snapshot was requested. `detach`, `rm`, job
detach/kill, and successful policy mutations with no additional observation return the literal empty object `{}` through
`EmptyResult` (never `null`, `true`, or a message string). `doctor` returns `DoctorReport { healthy, findings }`; each
`Finding` has `code`, `severity`, `message`, `hint`, and optional `path`. `ls` returns `WorkspaceInfo[]`; `ensure`
returns `EnsureReport`; `gc` returns `GcReport`. Exec and job commands return the frozen job DTOs in 07_api.md. Commands
whose normal stdout is a scalar use a named one-field body (`CheckpointResult { label }`, `RevisionResult { oid }`,
`SlotResult { slot }`) so JSON never changes shape when another field is added.

The controller commitment transport never carries output payload or artifact paths. Ordinary `JobInfo` JSON describes
protected content through the frozen `StreamInfo` union and may include a bounded inline artifact as the exact
`BinaryData` wire union `{encoding:"utf8",data:"…"} | {encoding:"base64",data:"…"}`. The serializer selects `utf8`
exactly when the bytes are valid UTF-8; both forms are bounded by decoded byte length. Detached workspaces still omit
unavailable marker fields rather than emitting `null` or attaching as a side effect.

### Exec and job JSON (frozen)

Child command arguments never pass through `String`. The CLI collects each post-`--` value as an `OsString` and moves it
into the canonical `CommandArg`; the controller request and `JobInfo.argv` serialize every element as exactly
`{encoding:"utf8",data}` when its Unix bytes are valid UTF-8 or `{encoding:"base64",data}` otherwise. Base64 is strict
and canonical; unknown fields/encodings, malformed data, NUL, an argument above 128 KiB, total decoded argv above 1 MiB,
an empty argv or `argv[0]`, and non-representable platform bytes fail as usage errors before RPC, job-id/artifact
effects, or spawn. The supervisor consumes each `CommandArg` into `OsString`, and protected Arrow records argv as
`List<Binary>`; neither the CLI nor runtime uses lossy text conversion or a second wire shape.

Every `cowshed exec` submission, foreground or background, atomically allocates a positive, workspace-local,
monotonically increasing numeric `JobId`; ids are never reused. In normal mode captured child stdout remains CLI stdout,
so the control-plane job id and state are reported on stderr (a forced or automatic background prints the bare numeric
id on stdout instead). With `--json`, stdout contains only the final JSON envelope: `result` carries the numeric
`jobId`, `JobInfo` lifecycle/result metadata, hashes, bounded summaries, and—only when the canonical artifact is
inline—the bounded `BinaryData` `utf8|base64` tagged union. Live progress on stderr remains control-only NDJSON.
Unbounded child output never enters JSON, and controller commitments never receive either inline encoding.

`JobInfo` also carries structured stdin metadata (`kind`, delivered `bytes`, `complete`, and an optional normalized
workspace-relative `path`) and the explicit `output-limit` terminal state. The configurable combined stdout+stderr
capture quota defaults to 1 GiB. Crossing counts protected plus in-flight bytes, then TERM/grace/KILLs the process
group, drains both pipes without retaining bytes beyond the exact boundary, and publishes `output-limit`; cowshed never
silently truncates output while a job continues.

Each stream is `StreamInfo { storage, bytes, sha256, summary }`. `storage` is `{kind:"captured",artifact}` or
`{kind:"redirect",source,artifact}`; the protected artifact is `{kind:"inline",data}` or `{kind:"file",path}`. A small
terminal stream is inline Arrow Binary and has no path. A protected `.cowshed/job/<id>/out|err` file appears only after
lazy promotion. `Redirect.source` is an AST-proven live shell destination and never authority;
representation-transparent logs always read its independent protected `artifact`. `summary` remains deterministic,
bounded, versioned, redacted diagnostic text and drives no denial, exit, quota, policy, or build-success decision.

Protected content artifacts are authoritative within their origin incarnation/checkpoint boundary. Compact controller
commitments are authoritative for job existence/status/order/lineage and expected counts/hashes, but contain no output
payload. A mismatch is typed `integrity`; neither tier silently overwrites the other.

## Exit codes (stable API)

Two mappings. **cowshed's own outcome** uses 0–7; a child process run under `cowshed exec` has its exit code passed
through unchanged. Wrapper failures that occur while trying to exec use **100–106**. Because a real child may also exit
100–106, shell status alone is intentionally ambiguous; structured job/error output identifies whether cowshed failed
before exec or the child itself returned that status.

| Code | Meaning                       | Example                                                                     |
| ---- | ----------------------------- | --------------------------------------------------------------------------- |
| 0    | success                       |                                                                             |
| 1    | internal error (bug — report) | panic, unexpected substrate failure                                         |
| 2    | usage                         | unknown flag, bad workspace name                                            |
| 3    | not found                     | no such workspace/project/checkpoint                                        |
| 4    | conflict / busy               | name taken, mount busy, CAS moved                                           |
| 5    | environment missing           | not adopted, gateway down when required, no supported substrate             |
| 6    | sandbox-denied                | denial proven pre-spawn, by kernel evidence, or by gateway                  |
| 7    | integrity                     | missing/altered committed artifact, invalid complete batch, digest mismatch |

**`cowshed exec` exit semantics (frozen):**

- The child's exit code passes through exactly; a child that exits 6 or 7 reports its own status, not a cowshed wrapper
  denial/integrity result.
- Wrapper failures are **100** internal, **101** usage, **102** not-found, **103** conflict, **104** env-missing,
  **105** sandbox-denied-pre-spawn, and **106** integrity.
- Exit/typed **6** is emitted only on authoritative denial evidence (04_sandbox.md), never output string scanning.
- Exit/typed **7** covers missing/altered committed content, invalid complete Arrow batches, and commitment
  count/hash/batch/lineage mismatch. Discarding an incomplete trailing batch is successful recovery with a structured
  recovery report, not exit 7.
- NAPI/MCP surface the same taxonomy as structured errors/outcomes (07_api.md, 12_mcp.md).

## Commands

Global flags: `--json`, `--project <git-root>` (default: cwd's repo), `-q`/`--quiet` (aliases; suppress stderr guidance,
never errors).

| Command                                       | stdout                                           | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --------------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cowshed adopt [path]`                        | mount path                                       | Convert checkout → main workspace (02). `--capacity <size>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cowshed new <name>`                          | mount path                                       | Clone main → session. `--ref <rev>`, `--from <ws>`, `--browse`, `--slot <n>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `cowshed ls`                                  | one name per line (`--json`: full records)       | Includes `main`, mount state, branch, base commit, age. Detached rows degrade — see below.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed path <ws>`                           | mount path                                       | Exit 3 if unknown; attaches if detached (unless `--no-attach`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed exec <ws> -- <cmd…>`                 | child's stdout                                   | Sandboxed byte-exact argv exec (04). Post-`--` arguments remain `OsString`; each is capped at 128 KiB and their decoded total at 1 MiB. Every submission creates a numeric `JobId`; structured binary stdin is separate from shell text. `--stdout-copy <rel>` / `--stderr-copy <rel>` request independent post-terminal publication; default policy is `CreateNew`, and `--replace-output` changes all requested copies to `Replace`. `--ro`, `--cwd <rel>`. Child exit passes through unchanged; wrapper errors use 100–106. |
| `cowshed shell <ws>`                          | — (interactive)                                  | Sandboxed login shell inside the mount.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed repo mirror <url>`                   | mirror path                                      | Gateway fetches `<url>` into a read-only bare mirror on the cache volume (02/05). Repo-scoped egress grant required.                                                                                                                                                                                                                                                                                                                                                                                                           |
| `cowshed repo clone <url> [dir]`              | clone path                                       | `repo mirror` then a local `git clone --dissociate` into the workspace (default dir: repo basename).                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `cowshed sim export <ws> [artifact]`          | drop path                                        | Copy a built iOS `.app` to the one-way drop dir for the personal-session simulator (02/14). Default: newest built app.                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed app export <ws> [artifact]`          | drop path                                        | Mac-target sibling of `sim export`: copy a built macOS `.app` to the drop dir (02/14).                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed app promote [artifact]`              | installed path                                   | **Personal-session, human-only** (no `<ws>`, not agent-invokable): verify signature (Developer-ID default), install a drop-dir build into `~/Applications`, clear quarantine (14_nix.md). `--force` for ad-hoc, `--system` for `/Applications`.                                                                                                                                                                                                                                                                                |
| `cowshed ensure`                              | `--envrc`: export lines (≤2 load-bearing; 03)    | ≤25 ms fast path; heals mounts (02). `--attach` for stubs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed grant <ws> …`                        | new grant revision                               | `--read/--write <path>`, `--egress <host[:port]> [--opaque] [--impersonate <p>]`, `--repo <host/org[/repo]>`, `--sim <verb>`, `--preset simulator` (04/05).                                                                                                                                                                                                                                                                                                                                                                    |
| `cowshed revoke <ws> …`                       | new grant revision                               | Same selectors + `--all`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed push <ws>`                           | preserved ref, source sha                        | `--branch <name>` plus optional incarnation/source/destination CAS expectations. Preserves under `refs/cowshed/<ws>/…`; never advances an integration branch or checkout (02).                                                                                                                                                                                                                                                                                                                                                 |
| `cowshed rebase <ws>`                         | new head sha                                     | Rebase onto `--onto <branch-or-ref>` (default `main`); `--fresh` sheds divergence. Optional incarnation/source/base CAS expectations (02).                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed land <ws>`                           | target branch, landed sha, checkout state        | Rebase + validate + ff `--target <branch>` (default `main`) + retire. If target is checked out in the main workspace, advances its HEAD/index/tree; otherwise updates only that branch ref. `--check <cmd>`, `--no-retire`, `--push-only`, and optional incarnation/source/target CAS expectations (02).                                                                                                                                                                                                                       |
| `cowshed fork <src> <dst>`                    | mount path                                       | Mid-flight CoW copy; closed grants.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `cowshed checkpoint <ws> [label]`             | label                                            | Supervisor artifact barrier + manifest/commitment, then crash-consistent image snapshot; omitted label generates UTC timestamp. `--keep` exempts from gc.                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed restore <ws> <label>`                | mount path                                       | Label required; validates manifest/commitment before the fresh-incarnation publication fence. Previous image remains `pre-restore-…` (02).                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed ps <ws>`                             | familiar `ps` table (`--json`: `JobInfo[]`)      | All jobs, foreground and background: numeric ID, PID, state, elapsed time, exit/signal, and command. Primary interactive overview (11).                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed job list <ws>`                       | one numeric job id per line (`--json`: records)  | Deliberately retained although `ps` also lists jobs: completes the `job` command family and gives scripts a minimal discovery surface.                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed job status <ws> <id>`                | one status record                                | Current `JobInfo`, including storage discriminants, byte counts, hashes, and summaries; a file path exists only after promotion.                                                                                                                                                                                                                                                                                                                                                                                               |
| `cowshed job logs <ws> <id>`                  | raw captured stdout                              | `--stderr`, `--follow`; resolves the protected inline/file artifact. With `--json`, bounded inline data uses `BinaryData` tagged `utf8` or `base64`; no unbounded stream enters JSON.                                                                                                                                                                                                                                                                                                                                          |
| `cowshed job attach <ws> <id>`                | streamed raw I/O                                 | Attach to a running job, replaying unread bytes before live bytes (11).                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed job detach <ws> <id>`                | — (no stdout)                                    | Detach the client; the job and its raw stream capture continue.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed job kill <ws> <id>`                  | — (no stdout)                                    | Kill the complete process tree; id and record remain queryable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed job wait <ws> <id>`                  | final status                                     | Wait for a terminal state; `--json` returns final `JobInfo`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cowshed rm <ws>`                             | — (no stdout)                                    | Perceived-instant (02). Refuses unpushed branch without `--force`; `--force` also required for main / dirty.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cowshed attach <ws>` / `cowshed detach <ws>` | mount path / —                                   | Explicit mount lifecycle. `--browse`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `cowshed du [ws]`                             | `--json`: written/referenced per ws + checkpoint | CoW-aware usage; lists checkpoints per workspace (01).                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed logs`                                | human table (`--json`/`--ndjson`: events)        | Controller telemetry (13). `--ws`, `--kind`, `--since`, `--follow`. Wraps `lmao-inspect` over the store segments.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed audit`                               | human table (`--json`/`--ndjson`: events)        | Gateway audit events (05/13). `--denied`, `--host`, `--ws`, `--follow` (live tail via the control plane).                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed trace <trace-id>`                    | human waterfall (`--json`: span tree)            | Terminal waterfall of a lifecycle op, exec, or land (13).                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed mcp serve`                           | — (stdio/socket server)                          | Coordinator authority arrives only on an inherited FD/socketpair; it is never printed or placed in argv/environment (12).                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed gateway run`                         | — (foreground daemon)                            | launchd runs this.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `cowshed gateway status`                      | `--json` status                                  | Cache stats, per-workspace counters, telemetry segment stats.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `cowshed gc`                                  | freed bytes                                      | Two-phase exact candidate plan (01); `--dry-run` lists typed candidates and sums bytes with zero mutation.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed doctor`                              | `--json` findings                                | Invariant checks; each finding carries a `fix:` hint.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

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
supervisor; without it, exec uses an anonymous pooled shell (11_shell.md). Long commands auto-background on the soft
timeout; `--timeout <dur>` sets it and `--background` forces it immediately, printing the numeric `JobId` on stdout.
Foregrounding versus backgrounding changes attachment only. Backgrounding forces memory-only prefixes into lazy
protected files so later reattach/checkpoint reads are durable; it does not imply every terminal job has stream paths.

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

On macOS, `cowshed ensure --envrc` exports `COWSHED_PORT_BASE` (= the authoritative detached-metadata block base) as the
dev-server convention, so `vite`/`astro`/`metro`/`devenv up` can select ports inside the workspace's own block instead
of colliding with siblings (04_sandbox.md). Linux allocates no port block and emits no port sentinel: dev servers bind
its private loopback, while ordinary package tools retain their configured `http://127.0.0.1:7644/…` proxy and registry
URLs through exactly one trusted minimal connector launched inside that workspace's private loopback-only network
namespace. On both platforms `ensure` exports `GOENV` pointing at the authoritative mounted workspace's in-image Go
environment file and `COWSHED_WORKSPACE_TOKEN` containing the value read from its controller-minted token path. These,
plus the macOS-only base, are the complete load-bearing export set; the CLI receives typed paths and `PortBlock` in
`EnsureReport` and never guesses from cwd, a marker, a slot, or a mount-path convention.

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
`ExecRequest.stdin`; they never rewrite or text-normalize byte-exact `CommandArg` argv, interpolate shell text, or
synthesize `< file`. File paths must remain beneath the workspace and are opened read-only with no-follow traversal;
absolute paths, symlinks, devices, sockets, directories, and races fail closed. Backpressure reaches the caller/file
reader, EOF closes child stdin once, and interrupted input records incomplete delivery in `JobInfo` without implicitly
killing the job.

### Shell redirection and explicit publication

Cowshed never uses a shell AST to create a hardlink alias. An optional real-AST path may classify a proven literal
`>`/`2>` destination as `OutputStorage::Redirect` only when the supervisor controls the actual writable descriptor and
accounts admitted bytes at the identical combined-quota boundary. It snapshots the terminal source into an independent
protected inline or clone/reflink/copied file artifact. Polling/tailing or reopening a path is forbidden. If those
conditions are unavailable, ordinary shell semantics run and `StreamInfo` claims only bytes that reached the captured
pipe.

`--stdout-copy <rel>` / `--stderr-copy <rel>` are separate explicit post-terminal publication requests. Each defaults to
`PublicationPolicy::CreateNew`; one `--replace-output` changes every requested copy in that invocation to
`PublicationPolicy::Replace` and is a usage error unless at least one copy option is present. Publication materializes
from the sealed canonical protected artifact using clone/reflink/copy plus fsync and atomic rename. It never hardlinks,
changes `StreamInfo.storage`, becomes a read/authority source, or promises a destination while the command runs.
