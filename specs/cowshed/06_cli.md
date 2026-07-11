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
/Users/danny/.cowshed/mnt/conloca-3f2a9c1b/raven        ← stdout (bare mount path)
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
(`{"event":"attach","ms":233}`).

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

| Command                                       | stdout                                           | Notes                                                                                                                |
| --------------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `cowshed adopt [path]`                        | mount path                                       | Convert checkout → main workspace (02). `--capacity <size>`.                                                         |
| `cowshed new <name>`                          | mount path                                       | Clone main → session. `--ref <rev>`, `--from <ws>`, `--browse`, `--slot <n>`.                                        |
| `cowshed ls`                                  | one name per line (`--json`: full records)       | Includes `main`, mount state, branch, base commit, age. Detached rows degrade — see below.                           |
| `cowshed path <ws>`                           | mount path                                       | Exit 3 if unknown; attaches if detached (unless `--no-attach`).                                                      |
| `cowshed exec <ws> -- <cmd…>`                 | child's stdout                                   | Sandboxed exec (04). `--ro`, `--cwd <rel>`. Child exit passes through unchanged; wrapper errors use 100–105.         |
| `cowshed shell <ws>`                          | — (interactive)                                  | Sandboxed login shell inside the mount.                                                                              |
| `cowshed repo mirror <url>`                   | mirror path                                      | Gateway fetches `<url>` into a read-only bare mirror on the cache volume (02/05). Repo-scoped egress grant required. |
| `cowshed repo clone <url> [dir]`              | clone path                                       | `repo mirror` then a local `git clone --dissociate` into the workspace (default dir: repo basename).                 |
| `cowshed ensure`                              | `--envrc`: export lines (≤2 load-bearing; 03)    | ≤25 ms fast path; heals mounts (02). `--attach` for stubs.                                                           |
| `cowshed grant <ws> …`                        | new grant revision                               | `--read/--write <path>`, `--egress <host[:port]>`, `--repo <host/org[/repo]>` (04).                                  |
| `cowshed revoke <ws> …`                       | new grant revision                               | Same selectors + `--all`.                                                                                            |
| `cowshed push <ws>`                           | pushed ref                                       | `--branch <name>`. Host-side fetch from the mount (02); refuses checked-out branch (exit 4).                         |
| `cowshed rebase <ws>`                         | new head sha                                     | Rebase branch onto host/main (02). `--fresh` sheds divergence.                                                       |
| `cowshed land <ws>`                           | landed sha                                       | Rebase + validate + host ff-merge + retire (02). `--check <cmd>`, `--no-retire`, `--push-only`.                      |
| `cowshed fork <src> <dst>`                    | mount path                                       | Mid-flight CoW copy; closed grants.                                                                                  |
| `cowshed checkpoint <ws> [label]`             | label                                            | Crash-consistent snapshot; omitted label → generated UTC-timestamp label. `--keep` exempts from gc.                  |
| `cowshed restore <ws> <label>`                | mount path                                       | Label required. Previous image kept as `pre-restore-…` (02).                                                         |
| `cowshed jobs <ws>`                           | one job id per line (`--json`: records)          | List backgrounded jobs (11).                                                                                         |
| `cowshed jobs logs <ws> <id>`                 | spooled stdout                                   | `--stderr`, `--follow`. Reads the in-volume spool.                                                                   |
| `cowshed jobs attach <ws> <id>`               | streamed io                                      | Re-attach to a running job's stdio (11).                                                                             |
| `cowshed rm <ws>`                             | — (no stdout)                                    | Perceived-instant (02). Refuses unpushed branch without `--force`; `--force` also required for main / dirty.         |
| `cowshed attach <ws>` / `cowshed detach <ws>` | mount path / —                                   | Explicit mount lifecycle. `--browse`.                                                                                |
| `cowshed du [ws]`                             | `--json`: written/referenced per ws + checkpoint | CoW-aware usage; lists checkpoints per workspace (01).                                                               |
| `cowshed mcp serve`                           | — (stdio/socket server)                          | Prints the coordinator token to stderr exactly once (12).                                                            |
| `cowshed gateway run`                         | — (foreground daemon)                            | launchd runs this.                                                                                                   |
| `cowshed gateway status`                      | `--json` status                                  | Cache stats, per-workspace counters, audit path.                                                                     |
| `cowshed gc`                                  | freed bytes                                      | Trash drain, checkpoint/retention pruning, orphan mountpoints, compaction (01). `--dry-run`.                         |
| `cowshed doctor`                              | `--json` findings                                | Invariant checks; each finding carries a `fix:` hint.                                                                |

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

`cowshed exec` and `cowshed shell` accept `--session <name>` to bind to a named persistent shell in the workspace
supervisor (state — cwd, env, jobs — persists across calls); without it, exec uses an anonymous pooled shell
(11_shell.md). Long commands auto-background on the soft timeout; `--timeout <dur>` sets it and `--background` forces it
immediately, printing a job id on stdout.

`--slot <n>` (on `cowshed new`) mounts the workspace at a stable, recycled path (`~/.cowshed/mnt/<project_id>/slot-<n>`)
instead of a name-derived one, so successive workspaces in the same slot inherit each other's **path-keyed** cache
warmth (cargo incremental, Xcode DerivedData) — opt-in, because it trades workspace-path uniqueness for warmth and only
one workspace may hold a slot at a time (exit 4 if occupied).

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
port = 7644                    # host control plane only — never a workspace data-plane port
port_range = "40960-49151"    # reserved range workspace port blocks are carved from
block = 16                     # ports per workspace (base = data-plane listener; base+1.. = its dev servers)
[cache]                        # extend (never replace) the convention table
extra_workspace_dirs = ["build-out"]
```

`cowshed ensure --envrc` additionally exports `PORT` and `COWSHED_PORT_BASE` (= the block base) as dev-server
conventions, so `vite`/`astro`/`metro`/`devenv up` bind inside the workspace's own block instead of colliding with
siblings (04_sandbox.md).

## Tradeoffs

**Prose-on-stdout rejected.** Mixed streams are the root cause of agents parsing with `grep` and breaking on wording
changes. The stdout/stderr split costs nothing for humans (terminals merge the streams visually) and makes every cowshed
invocation composable. `-q` exists for callers that want silence, not a different contract.

**Interactive prompts rejected.** A confirmation prompt is an API that only humans can call. Explicit flags (`--force`)
plus precise stderr statements provide the same safety with a uniform caller model.
