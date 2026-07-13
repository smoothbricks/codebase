# cowshed CLI guide

## The contract

Every cowshed command follows the same I/O discipline:

- **Ordinary stdout** carries one control answer: a bare value, TSV rows, or a bounded JSON envelope with `--json`.
  Job/status JSON contains lifecycle fields, typed artifact handles, byte counts, SHA-256 digests, bounded summaries,
  and may contain small `Inline.data` bytes tagged as `utf8` or `base64`. Foreground exec, `cowshed job logs`,
  `cowshed job attach`, and artifact reads are the explicit interfaces for unbounded raw bytes.
- **stderr** carries progress, explanations, warnings, and self-driving guidance. Guidance lines are prefixed
  `cowshed:`; suggested follow-up commands are prefixed `next:`. Agents and humans read the same hints.
- **Exit codes** are stable:

| Code | Meaning                          | Typical cause                                                                                                                       |
| ---- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| 0    | ok                               | —                                                                                                                                   |
| 1    | internal error (bug — report it) | panic, unexpected hdiutil/diskutil failure                                                                                          |
| 2    | usage                            | unknown flag, missing argument                                                                                                      |
| 3    | not-found                        | no such workspace/project/checkpoint                                                                                                |
| 4    | conflict                         | name in use, workspace busy, restore over unsaved state without `--force`                                                           |
| 5    | env-missing                      | gateway not running, caches volume absent, direnv missing                                                                           |
| 6    | sandbox-denied                   | command blocked by the sandbox, confirmed by authoritative evidence; stderr names the path/domain and the grant that would allow it |
| 7    | integrity                        | committed job content missing, mutated, rolled back, or inconsistent with its controller commitment                                 |

`cowshed exec` passes the child's exit code through **unchanged**; failures of cowshed's own exec wrapper (mount gone
mid-run, profile generation failed, integrity verification failed, …) use 100–106 so they can never collide with a child
that legitimately exits 1–7. Exit 6 is reported only when cowshed has authoritative evidence of a denial — the gateway
logged the egress decision, or the kernel sandbox telemetry names the blocked operation; otherwise the child's ordinary
exit passes through untouched. Exit 7 is reported only for an established content/commitment integrity failure, never
for a summary mismatch or ordinary child output.

The JSON envelope is uniform:

```
$ cowshed new raven --json
{"ok":true,"result":{"workspace":"raven","mount":"/Users/me/.cowshed/mnt/acme/widget/raven","baseCommit":"6f3a2c1"}}

$ cowshed path nonesuch --json
{"ok":false,"error":{"code":"not-found",
 "message":"no workspace 'nonesuch' in project 'example-project'",
 "hint":"cowshed ls"}}
```

Errors with `--json` still exit with their code; stderr stays human-readable either way. A bounded `JobInfo` may encode
small `Inline.data` bytes in a tagged `utf8` or `base64` form. The size bound is load-bearing: larger and live artifacts
remain handles, and unbounded bytes require `cowshed job logs`, `cowshed job attach`, or an explicit artifact read.

## Global flags

- `--json` — JSON envelope on stdout instead of bare values/TSV. Available on every command.
- `--project <path|name>` — operate on a project you're not standing in. Default: the project owning the current
  directory (walking up to a `.cowshed/workspace.json` marker or an adopted git root). Exit 3 if neither resolves.
- `-q` / `--quiet` — suppress `cowshed:` progress lines on stderr; errors and `next:` hints still print.

Workspace names are `[a-z0-9][a-z0-9-]*`, unique per project. Commands taking `<name>` accept `main` wherever it makes
sense. `cowshed exec main -- ...` uses the same closed sandbox and explicit grants as every other workspace.

## Lifecycle

### `cowshed adopt`

Run once, inside an existing checkout. Converts it into an image-backed **main workspace** at the same path. This is the
only cowshed operation that copies data (one-time; clonefile cannot cross volumes into a new image).

```
$ cd <project-root> && cowshed adopt
cowshed: created dedicated volumes cowshed.store, cowshed.caches (space-sharing, excluded from backup)
cowshed: creating image ~/.cowshed/acme/widget/main.asif (capacity 100g, sparse)
cowshed: copying 8,357,293 objects into the image (this is the one-time cost)
cowshed: verifying tree against source ... ok
cowshed: swapping <project-root> -> mountpoint (stub .envrc written beneath)
next: eval "$(cowshed ensure --envrc)"   # add to .envrc
next: cowshed new <name>
<project-root>
```

### `cowshed new <name> [--ref <rev>] [--browse]`

Clones **main's live image** (there are no templates) and mounts it.

```
$ cowshed new raven
cowshed: cloned acme/widget main image (copy-on-write)
cowshed: verified clone (fsck, pre-mount) and attached at ~/.cowshed/mnt/acme/widget/raven
cowshed: branch cowshed/raven created from main @ 6f3a2c1
cowshed: port block 40960–40975 (macOS: gateway at 40960; your dev servers at 40961+)
cowshed: sandbox: closed (write: own volume, designated cache subtrees, temp; egress: gateway only)
next: cowshed exec raven -- bun test
~/.cowshed/mnt/acme/widget/raven
```

The clone is crash-consistent (like a power cut) and fsck-verified on its device _before_ mounting (attach `-nomount` →
verify → mount). `--ref` starts the branch elsewhere than main's HEAD; source catches up via git, caches stay warm
regardless. `--browse` makes the volume visible in Finder (default is nobrowse).

### `cowshed ls`

TSV on stdout: name, state, branch, mountpoint (empty when detached).

```
$ cowshed ls
main	mounted	main	<project-root>
raven	mounted	cowshed/raven	~/.cowshed/mnt/acme/widget/raven
fox	detached	cowshed/fox
```

### `cowshed path <name>`

Bare mountpoint on stdout. Exit 3 if the workspace doesn't exist. A detached workspace is attached first, so the printed
path is always live; pass `--no-attach` to skip the healing and get the would-be path with a `cowshed:` note instead.

### `cowshed rm <name> [--force]`

Marks the workspace deleted and returns immediately; detach and image deletion happen in the background. Refuses
(exit 4) when the branch has commits not pushed anywhere unless `--force`.

```
$ cowshed rm raven
cowshed: cowshed/raven is pushed (host has 6f3a2c1..9b2e77d)
cowshed: detaching and deleting in background
next: cowshed gc   # reclaim space from old checkpoints too
```

Nothing on stdout — `rm` has no answer to give.

## Daily work

### `cowshed exec <name> -- <cmd...>`

Runs a command inside the workspace's sandbox, cwd at the workspace root. In foreground raw mode, child stdout/stderr
pass through as opaque bytes and the child exit code passes through. With `--json`, stdout is instead the bounded final
control envelope; retrieve full bytes explicitly through job logs, attachment, or the typed artifact reader.

```
$ cowshed exec raven -- cargo build -p cowshed-core
   Compiling cowshed-core v0.1.0
...
$ echo $?
0
```

When a command fails _because of_ the sandbox and cowshed has authoritative evidence — the gateway logged the egress
denial, or the kernel sandbox telemetry (unified log, correlated by pid) names the blocked operation — cowshed reports
exit 6 with the diagnosis:

```
$ cowshed exec raven -- ./scripts/render-video.sh
cowshed: sandbox denied file-write <project-root>/renders
cowshed: workspace 'raven' starts closed; this path is outside its writable set
next: cowshed grant raven --write <project-root>/renders
$ echo $?
6
```

Without such evidence, the child's ordinary exit code passes through untouched — cowshed never guesses a denial from
output text. Failures of the exec wrapper itself use exit codes 100–106.

### `cowshed shell <name>`

Interactive shell inside the sandbox, same wiring as `exec`. Your prompt, direnv, and toolchains work normally; writes
outside the granted set fail with EPERM.

### Dev servers inside workspaces

On macOS, each workspace owns a **16-port block** allocated at creation; the gateway data plane sits on the base port,
and base+1 through base+15 are workspace service ports. Linux allocates no port block: every workspace instead gets
private loopback in its own network namespace, so fixed service ports do not collide with siblings. Ordinary package
tools still use `http://127.0.0.1:7644/…`: exactly one controller-owned, non-signalable connector in that namespace
binds that address and forwards bytes only to the workspace's mounted `/run/cowshed/gateway.sock`. It holds no policy or
credentials and is not a general TCP/Unix-socket forwarder; the socket inode, namespace, and opaque token retain the
authority boundary. Detach or restore drains and kills it. Tools must use cowshed's platform-specific configuration
rather than assuming host-wide loopback.

On macOS, `cowshed ensure --envrc` exports `PORT` (base+1) and `COWSHED_PORT_BASE` for tools that need several ports;
devenv offsets can derive from the block. Linux configuration contains no block or sentinel values.

```
$ cowshed shell raven
raven$ echo $PORT
40961
raven$ bun run dev          # vite reads $PORT; open http://localhost:40961 in your browser
```

### `cowshed job list <name>` — background work

Long commands auto-background at the soft timeout (default 120 s; `--timeout <dur>` tunes it, `--background` forces it
immediately) and keep running under the workspace supervisor. `cowshed exec`/`cowshed shell` accept `--session <name>`
for a persistent named shell whose cwd, variables, and jobs survive across calls.

Every job has separate stdout/stderr `StreamInfo { storage, bytes, sha256, summary }` handles. `storage` is
`Captured { artifact }` or `Redirect { source, artifact }`; `artifact` is `Inline { data: BinaryData }` or
`File { path: WorkspacePath }`. Small terminal streams remain inline and protected files spill lazily, so consumers must
not assume every short job creates `out` and `err` files.

```sh
$ cowshed exec raven --background -- bun run build:everything
42
$ cowshed job list raven
42	running	bun run build:everything	2m14s
$ cowshed job logs raven 42 --follow      # raw stdout bytes; --stderr selects the other stream
$ cowshed job attach raven 42             # re-attach live raw stdio
```

Control/status JSON is bounded; it may carry tagged bytes only for a small inline artifact. Logs, attachments, and
artifact reads resolve the canonical artifact independently of whether it is inline or spilled and preserve arbitrary
binary output without UTF-8 assumptions or response-size growth.

### `cowshed ensure [--envrc]`

The fast auto-fix. Healthy fast-path is a marker read plus a statfs (~15–25 ms, silent, exit 0). Otherwise it reattaches
images after reboot or Finder ejects, repairs mount flags, re-arms the autosave agent, and reconciles anything drifted —
synchronously, so when it returns you are standing in a valid workspace. `--envrc` additionally prints exports for the
current workspace on stdout, for `eval` in `.envrc`:

```
$ cowshed ensure --envrc
export COWSHED_GATEWAY_TOKEN=cw1_r4v3n…
export GOENV=~/.cowshed/mnt/acme/widget/raven/.cowshed/cache/go/env
export COWSHED_PORT_BASE=40960 PORT=40961
export COWSHED_REPO_ID=acme/widget COWSHED_WORKSPACE=raven
```

Deliberately short: wiring is carried by **files, not environment**. The registry URL (the macOS workspace gateway base
port, or Linux's fixed private-loopback connector at `127.0.0.1:7644`) and the bun cache dir live in the committed
`bunfig.toml` — bun honors a _relative_ `[install.cache] dir`, verified, so there is no cache export at all; cargo's
source replacement and `SCCACHE_NO_DAEMON` live in the in-image `.cargo/config.toml` (cargo's `[env]` verifiably reaches
rustc-wrapper invocations); the read-at-build caches (cargo registry, Go module/build caches, sccache, zig, gradle) are
reached through their tools' _default_ host paths, relocated once onto the caches volume at first adopt — except Go,
which has no directory-scoped config: its in-image env file (carrying the per-workspace `GOPROXY`, the shared caches,
in-image `GOPATH`/`GOBIN`, and `GOTOOLCHAIN=local`) is reached via the `GOENV` export, so `~/go` is never created. The
two load-bearing exports above each exist only until their verification passes (token-via-config kills the first; a
file-based `GOENV` alternative — none known — would kill the second); on macOS, `PORT`/`COWSHED_PORT_BASE` wire dev
servers into the workspace's port block (see "Dev servers" above); Linux has no block. The `COWSHED_*` identity lines
are prompt conveniences, never load-bearing.

`ensure` never does slow or surprising work — no fetches, no compaction, no installs. Main gets the same wiring (that's
the "main shares caches like sandboxes do" rule; the only difference is main isn't sandboxed).

### `cowshed attach <name>` / `cowshed detach <name>`

Suspend and resume a workspace without destroying it. Detached workspaces cost one closed file.

### Simulators (iOS) — `cowshed sim export <name> [artifact]`

Copies a built `.app` to the one-way drop dir (`<shared-drop-root>/<owner>/<repo>/`, using the separately validated
components of the primary `repo_id`; stdout = the drop path) so the personal session can install it into the human's
native Simulator.app — the artifact handoff for posture B. The in-image `xcrun` wrapper handles the rest of the
simulator story (dev-local headless simulators by default; personal-session devices via `--sim` grants). The full
walkthrough, Expo included, is [ios.md](ios.md).

## Sandbox grants

Workspaces start **closed**: write access to their own volume, `~/.cowshed/caches`, and temp; read access to the
toolchains and system; egress to the localhost gateway only. Widen per workspace:

```
$ cowshed grant raven --read <project-root>/reference-corpus
$ cowshed grant raven --write <project-root>/shared-assets --egress api.github.com
cowshed: grants for raven now: +read <project-root>/reference-corpus, +write <project-root>/shared-assets, +egress api.github.com
cowshed: filesystem grants apply from the next exec; egress applies immediately (gateway allowlist)
next: cowshed exec raven -- <retry your command>
```

- Besides `--read`/`--write`/`--egress` there are `--repo <host/org[/repo]>` (gateway repo mirrors), `--sim <verb>`
  (personal-session simulator broker: `openurl` freely, `install` drop-dir-bound and human-gated — [ios.md](ios.md)),
  and `--preset simulator` (dev-side headless CoreSimulator IPC).
- Grants are recorded in `<image>.grants.json`, **outside the volume** — a sandboxed process cannot edit its own grants.
- Filesystem grants take effect at the next `exec`/`shell` (Seatbelt profiles are fixed at process launch; every exec
  carries the current grant snapshot). Egress grants are enforced by the gateway and apply to running processes
  immediately.
- `cowshed grant <name>` with no flags prints the current grant set (TSV; `--json` for the envelope):

```
$ cowshed grant raven
read	<project-root>/reference-corpus
write	<project-root>/shared-assets
egress	api.github.com
```

- `cowshed revoke raven --write <project-root>/shared-assets` narrows again; `cowshed revoke raven --all` resets to
  closed. Revocation of egress is immediate; filesystem revocation applies from the next exec.
- The closed baseline is a floor, not a grant: you cannot revoke a workspace's access to its own volume, the caches
  volume, or the gateway.
- **Egress is intercepted by default.** `--egress api.github.com` lets the gateway terminate TLS under the workspace's
  CA and inject the Keychain credential + trace context — the workspace reaches the API authenticated while holding no
  secret. Add `--opaque` for a cert-pinning host (plain tunnel, no injection) or `--impersonate <profile>` for a
  browser-shaped fingerprint (also no injection). A bare `cowshed grant raven` prints the set with `mode`/`impersonate`
  columns; `--repo github.com/org/*` grants which repos the gateway will mirror (see Git).

## Authority boundaries

Project lookup is discovery-only. Workspace inspection may safely ensure or attach. A worker capability controls one
workspace's exec, shell, jobs, quota-limited checkpoints, push, and grant reads. Only a trusted coordinator may
grant/revoke, restore/destroy/rebase/land, run gc, or mirror repositories. The persistent per-workspace supervisor
socket is permission- and peer-checked, supports concurrent clients and reconnect, and is never unlinked merely because
one client disconnects.

Protected in-volume Arrow records, inline bytes, and spill files are captured-content authority within their origin
incarnation/checkpoint snapshot. Controller Arrow is continuity authority for job existence, lifecycle, order, lineage,
terminal state, byte counts, hashes, and batch digest; it carries no artifact payload or path authority and never
duplicates raw bytes. Every shell/session/descendant is restricted from writing `.cowshed/job/**` before
repository-controlled startup. A content/commitment mismatch is a typed integrity failure.

MCP coordinator authority is delivered only through an inherited FD/socketpair, never stderr, argv, environment, or a
workspace file. Worker descriptors are 256-bit, one-use, expire after 30 seconds, are atomically consumed, restart-
invalidated, and bound to the intended peer/socket/workspace. Authorization uses its own RPC error and is not a sandbox
denial.

## Git

Workspace git is **local-paths-only**: every workspace has the `host` remote (main's repository, a mounted path) and can
clone from read-only mirrors under `~/.cowshed/caches/git` — nothing else. No remote URLs, no credentials, no credential
helpers exist inside a workspace; pushing to real remotes (origin, GitHub) is coordinator work, done host-side with your
normal git setup.

### `cowshed push <name> [--branch <b>]`

Delivers the workspace branch to main's repository. Under the hood this is a _host-side fetch from the workspace mount_
— the trusted side runs git, so nothing inside the workspace (hooks, `.git/config`) ever executes outside the sandbox.
Never touches main's checked-out branch.

```
$ cowshed push raven
cowshed: pushed cowshed/raven -> host (9 commits, 6f3a2c1..9b2e77d)
next: merge in main when ready; new workspaces are warm from whatever main has built
cowshed/raven
```

A background autosave (a per-project launchd agent, host-side like `push`) fetches every workspace into
`refs/cowshed/<name>/wip` every 10 minutes — uncommitted work is the only thing at risk between autosaves, because the
store volume that holds the images is excluded from backup (durability = git).

### `cowshed repo mirror <url>` / `cowshed repo clone <url> [dir]`

How third-party code gets into a workspace — the `gh repo clone` of the sandbox. `mirror` asks the gateway to fetch the
repository (with its Keychain credentials, subject to the workspace's repo grants, one audit line) into a shared bare
mirror on the caches volume, and prints the mirror path. `clone` is the sugar: mirror, then a local `git clone` from
that path into the workspace. Mirrors are fetch-only, deduplicated fleet-wide, and read-only for sandboxes; re-run
`mirror` to refresh.

```
$ cowshed exec raven -- cowshed repo clone https://github.com/tinylibs/tinybench
cowshed: mirror ~/.cowshed/caches/git/github.com/tinylibs/tinybench.git (fetched via gateway)
tinybench
```

### `cowshed rebase <name> [--fresh]`

Brings the workspace branch up to current main (`git fetch host && git rebase host/main`, run inside the sandbox).
Conflicts abort cleanly and exit 4 naming the conflicted paths. `--fresh` sheds accumulated image divergence: replay the
branch onto a brand-new clone of current main and transplant the workspace's identity onto it — refused (exit 4) if the
tree is dirty.

### `cowshed land <name> [--check <cmd>]`

The full close-out in one primitive: rebase onto main, validate (`--check`, or `.cowshed.toml` `[land] check`) inside
the sandbox, fast-forward main's repo from the workspace, retire the workspace. Any failing step exits 4 with the
workspace intact. `--no-retire` keeps the workspace; `--push-only` stops after validation for review-gated flows.

## Time travel

### `cowshed fork <src> <dst>`

Clones a _running_ workspace — two divergent futures from the same mid-flight state, in milliseconds. Grants are **not**
inherited; forks start closed.

### `cowshed checkpoint <name> [label]` / `cowshed restore <name> <label>`

Checkpoint clonefiles the workspace image (crash-consistent, fsck-verified) under a label — generated from the UTC
timestamp when you don't give one. Before publication, a supervisor barrier seals complete Arrow batches and spill
files; a manifest commits every checkpoint-resident job byte. Recovery may discard only incomplete trailing data.

Restore swaps the current image for the checkpoint (detach → clone → reattach, ~500 ms) and mints a new workspace
incarnation. Protected content remains authoritative for the restored snapshot's origin boundary; compact controller
commitments preserve lifecycle/order/lineage and hashes across the restore. Restore refuses over unsaved work without
`--force` (exit 4), and the displaced image is kept as a `pre-restore-<ts>` checkpoint, so a restore is itself undoable.
List checkpoints with `cowshed ls --json` or `cowshed du`.

```
$ cowshed checkpoint raven pre-refactor
pre-refactor
$ cowshed restore raven pre-refactor
cowshed: raven restored to pre-refactor (previous image kept as pre-restore-2026-07-11T14-22-09Z)
next: cowshed exec raven -- git status
```

## Infrastructure

### `cowshed gateway run` / `cowshed gateway status`

`run` starts the gateway in the foreground (use launchd for login-time start; see [gateway.md](gateway.md)). `status`
prints `running <pid>` plus the control port and per-workspace listener count, or exits 5 with setup hints.

### `cowshed du`

Copy-on-write-aware usage: written vs referenced bytes per workspace and per checkpoint — "written" is the true cost,
"referenced" is shared with main. `--json` for dashboards; this is also how a coordinator spots long-lived workspaces
worth `cowshed rebase --fresh`.

### `cowshed logs` / `cowshed audit` / `cowshed trace`

cowshed's telemetry is distributed tracing into Arrow columns, not a text logfile (see [telemetry.md](telemetry.md)) —
these three verbs read it, human tables by default, `--json`/`--ndjson` to pipe:

```
$ cowshed logs --ws raven --kind lifecycle --since 1h   # lifecycle/op spans for one workspace
$ cowshed audit --denied --follow                       # live egress denials across the fleet
$ cowshed trace 4bf92f35a3…                             # terminal waterfall of one op/exec/land
```

There is no `.ndjson` or `.log` file to `tail`; `--ndjson` is an export encoding on the pipe. Under the hood these wrap
the generic `lmao-inspect` reader over the Arrow segments in `~/.cowshed/telemetry/`.

### `cowshed mcp serve`

Runs the MCP server (stdio, or a shared Unix socket) exposing workspaces as tools for agent harnesses. Coordinator
authority arrives only through a dedicated inherited FD/socketpair and is never printed on stderr or placed in argv or
environment. Worker connections redeem short-lived one-use descriptors and can run or observe only their bound
workspace.

### `cowshed gc`

Deletes orphaned images and stale mountpoint dirs, prunes expired checkpoints, compacts detached images, and reports
what it reclaimed. Safe to run anytime; also runs opportunistically from other commands.

### `cowshed doctor`

Invariant checks: every image has a marker, every mount matches an image, grants files parse, caches volume and gateway
reachable, autosave fresh. Exit 0 when healthy; otherwise the code of the most severe finding (3/4/5) with one
`cowshed:` line per issue and a `next:` fix for each.

```
$ cowshed doctor
cowshed: gateway not running (last audit event 2d ago)
next: cowshed gateway run   # or: launchctl kickstart -k gui/501/dev.cowshed.gateway
$ echo $?
5
```

### Binary stdin

Use structured stdin instead of interpolating input into shell text:

```sh
producer | cowshed exec raven --stdin -- ./binary-consumer
cowshed exec raven --stdin-file fixtures/input.bin -- ./binary-consumer
cowshed exec raven --stdin-base64 AAEC/w== -- ./binary-consumer
```

`--stdin` streams opaque caller bytes with backpressure; `--stdin-file` accepts only a workspace-relative regular file
opened read-only with no-follow traversal; `--stdin-base64` strictly decodes inline bytes. Absolute/escaping paths,
symlinks, devices, sockets, and directories fail closed. EOF closes child stdin once. Canceling input closes stdin and
records incomplete delivery; it does not implicitly kill the job. Job JSON reports stdin kind, delivered byte count,
completion, and the normalized relative file path when applicable, never inline contents.

A real shell AST may recognize a proven narrow literal `>`/`2>` workspace destination as `OutputStorage::Redirect`. The
shell writes the live caller-visible `source`; after terminal state cowshed snapshots the admitted bytes into an
independent protected `artifact` using inline Arrow Binary or clone/reflink/copy file storage. The source is never
authoritative and is never hardlinked to the artifact. Ambiguous or unrecognized shell text keeps ordinary shell
semantics, and bytes redirected away from the supervisor's pipes are then absent from the job handle.

`cowshed exec` exposes post-terminal publication as `--stdout-copy <rel>` and `--stderr-copy <rel>`. Each requested
workspace-relative destination defaults to `CreateNew`, so an existing path is an operational conflict rather than an
implicit overwrite. `--replace-output` upgrades every requested copy in that invocation to `Replace`; using it without
either copy destination is a usage error. Structured API/JSON requests retain a separate publication policy per stream
instead of inheriting the CLI-wide switch.

Copies are published only after the canonical artifact is drained, fsynced, closed, and sealed. A destination is an
independent clone/reflink when supported, otherwise an ordinary copy, and is atomically renamed under the selected
policy. Publication does not alter `StreamInfo.storage` and is never used for reads or authority. Failure is a typed
operational error and does not change the already-established process result.

### Job artifact storage and output limit

`stdout` and `stderr` each use typed captured/redirect storage with a canonical protected `Inline`/`File` artifact. The
stream handle always reports bytes, SHA-256, and a bounded redacted summary. `Redirect.source` is never authority.
Representation-transparent logs, attachment, and artifact reads always resolve the canonical `artifact`; small terminal
bytes may stay inline, and a protected file is spilled lazily only when needed.

The streams share a configurable per-job capture quota (default 1 GiB). The quota includes persisted and in-flight
bytes. Crossing it terminates the whole process group with TERM, grace, then KILL, drains both pipes, and records the
explicit `output-limit` terminal state. cowshed never silently truncates output while the command continues. Diagnostic
summary truncation is separate.
