# CLI Contract

The `cowshed` binary is self-driving: an agent that has never seen it can operate it from its own output. Three rules
make that possible and they are absolute:

1. **stdout is machine-readable only.** A bare value (usually a path) or, with `--json`, one JSON envelope. Nothing else
   — ever. `cowshed path raven | xargs ls` and `cowshed ls | grep …` must never see prose.
2. **Everything for humans and agents goes to stderr**: progress, warnings, and — after every command — actionable
   hints. Guidance can never contaminate pipes or grep. Guidance lines are prefixed `cowshed:`; trailing hint lines are
   prefixed `next:`.
3. **A command that may trigger an authorization prompt says so first.** Any operation that can escalate — volume
   provisioning, a remount macOS classifies as privileged, plist installation — emits a `cowshed:` line naming the
   action and the prompt before the dialog appears
   (`cowshed: one administrator authorization will remount cowshed.store at /private/cowshed/store`). A silent
   SecurityAgent dialog behind an unrelated read-only verb is a contract violation, not a platform quirk to tolerate.
   Where the platform permits (fstab-pinned mounts), cowshed prefers designs in which no prompt can occur at all
   (01_storage.md).
4. **Diagnosis is canonical and self-contained.** The same host state yields the same verdict from any directory,
   regardless of unrelated details (a stray file in a mountpoint dir must change the wording, not the verdict). Every
   finding names the observed evidence (which volume, mounted where, expected where, which stub files) and a `next:`
   command that exists in the parser. Hinted verbs are contract-tested against the parser so guidance can never
   reference a command that does not exist.

## Launcher

The npm package's `cowshed` bin is `bin/cowshed`, a POSIX shell script that execs the native binary for the host:
`sccache` (the one daemon-control verb) runs the host-stable install launchd runs when it exists; everything else runs
`dist/bin/<platform>/cowshed`, restoring an execute bit a publish dropped, or else `target/release/cowshed` of the
workspace a linked checkout sits in. A host with none of them gets exit 5 naming every path looked in. It is a shell
script rather than Node because it runs before every command, and Node's own start (~30 ms) costs more than the fastest
verbs take in total; `exec` hands the binary the command's signals, exit status and standard streams unchanged. The
library's `runCli` runs the CLI through the same script, so one rule picks the binary.

## Onboarding and repair

Two verbs own the host story, both runnable from any directory:

- **`doctor`** is the universal diagnostician: version and install source, per-volume state (present/absent, current
  versus canonical mountpoint, marker validity), service status, workspace inventory summary, project checks when an
  adopted checkout resolves. It never mutates: its project open finishes no unfinished lifecycle operation, interrupted
  publication or restore, retired-image reclamation, identity change or binding heal, and reports each as a finding for
  the next opening command to finish. `doctor --repair` opens the way every other verb does. While the retired caches
  volume still exists it reports the `caches-volume` warning, whose hint is `cowshed setup --retire-caches-volume` once
  only the volume's marker remains and `cowshed setup, then cowshed setup --retire-caches-volume` before that.
- **`setup`** is idempotent host repair: provision absent volumes, repair detached or mis-mounted ones,
  FileVault-encrypt unencrypted ones in place and store passphrases in System.keychain, validate markers precisely, pin
  `/etc/fstab`, and install the `dev.cowshed.storage` system LaunchDaemon that unlocks and mounts before login
  (01_storage.md). It announces an authorization prompt before raising one, then performs everything that can require
  elevation inside that single session. Once storage repair succeeds, it refreshes every adopted main's build state
  through the same runtime path jobs use (16_build_volumes.md). Migration is rebuild-only: it announces the discard of
  contributed incremental directories before acting, protects tracked source files, and links an empty build volume for
  the next build to repopulate. No build state is copied. A project refusal is reported on stderr without hiding later
  projects; the command exits with the first typed failure and the number of failed projects, never a success JSON
  envelope first. Uninstall and a failed storage repair never run migration. A fully migrated healthy host changes
  nothing and says so. The non-destructive storage promise applies to the host store volume, layer-3 caches and source
  data, not to rebuildable incremental state. On a host that still has the retired caches volume, setup moves what the
  volume holds to the host HOME without authorization, and `setup --retire-caches-volume` deletes the emptied volume,
  its `/etc/fstab` pin and its mount-service entry inside that same single session, refusing while anything but the
  marker is left (03_caches.md "Retiring the caches volume").

The stranded-user journey this contract exists for: after a reboot with locked or unmounted volumes, `cowshed doctor`
explains the exact divergence (volume present but locked, or at macOS's default `/Volumes/<name>` instead of its
canonical path, stubs listed by name, service status) and `cowshed setup` fixes it in one step.

```
$ cowshed new raven
/Users/you/.cowshed/mnt/acme/widget/raven           ← stdout (bare mount path)
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
`Finding` has `code`, `severity`, `message`, `hint`, and optional `path`. `ls` returns `WorkspaceInfo[]`; `attach`
returns `MountResult` for one workspace and `WorkspaceInfo[]` for several; `gc` returns `GcReport`; `identity add`
returns `IdentityReport { repoId, added, identity, identities }`. Exec and job commands return the frozen job DTOs in
07_api.md. Commands whose normal stdout is a scalar use a named one-field body (`CheckpointResult { label }`,
`RevisionResult { oid }`, `SlotResult { slot }`) so JSON never changes shape when another field is added.

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
so the control-plane job id and state are reported on stderr (`--background` prints the bare numeric id on stdout
instead). With `--json`, stdout contains only the final JSON envelope: `result` carries the numeric `jobId`, `JobInfo`
lifecycle/result metadata, hashes, bounded summaries, and—only when the canonical artifact is inline—the bounded
`BinaryData` `utf8|base64` tagged union. Live progress on stderr remains control-only NDJSON. Unbounded child output
never enters JSON, and controller commitments never receive either inline encoding.

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
| `cowshed new <name>`                          | mount path                                       | Clone main → session, minted with the `main` remote (02). `--ref <rev>`, `--from <ws>`, `--browse`, `--slot <n>`, `--register`, `--git-worktree`.                                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed ls`                                  | one name per line (`--json`: full records)       | Includes `main`, mount state, branch, base commit, age. Detached rows degrade — see below.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed path <ws>`                           | mount path                                       | Exit 3 if unknown; attaches if detached (unless `--no-attach`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed exec <ws> -- <cmd…>`                 | child's stdout                                   | Sandboxed byte-exact argv exec (04). Post-`--` arguments remain `OsString`; each is capped at 128 KiB and their decoded total at 1 MiB. Every submission creates a numeric `JobId`; structured binary stdin is separate from shell text. `--stdout-copy <rel>` / `--stderr-copy <rel>` request independent post-terminal publication; default policy is `CreateNew`, and `--replace-output` changes all requested copies to `Replace`. `--ro`, `--cwd <rel>`. Child exit passes through unchanged; wrapper errors use 100–106. |
| `cowshed shell <ws>`                          | — (interactive)                                  | Sandboxed login shell inside the mount.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed repo mirror <url>`                   | mirror path                                      | Gateway fetches `<url>` into a read-only bare mirror in cowshed's own cache directory (02/05). Repo-scoped egress grant required.                                                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed repo clone <url> [dir]`              | clone path                                       | `repo mirror` then a local `git clone --dissociate` into the workspace (default dir: repo basename).                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `cowshed sim export <ws> [artifact]`          | drop path                                        | Copy a built iOS `.app` to the one-way drop dir for the personal-session simulator (02/14). Default: newest built app.                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed app export <ws> [artifact]`          | drop path                                        | Mac-target sibling of `sim export`: copy a built macOS `.app` to the drop dir (02/14).                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed attach [ws] [--all]`                 | mount path(s)                                    | `<ws>` one; cwd in a project = that project's sessions; `--all` store-wide. Mains always mounted (05).                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed detach [ws] [--all]`                 | — (no stdout)                                    | `<ws>` one from store readdir (sidecar identity, no cwd/git); `--project` overrides; `--all` store-wide attached sessions. Mains never targets.                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed mount main [--repo-id <owner/repo>]` | mount path                                       | Mount a project's main from store records — the repository binding and the checkout-path record — rather than a live git checkout, so a stub directory left by a broken workspace still mounts; a volume mounted with other flags is remounted with the canonical ones.                                                                                                                                                                                                                                                        |
| `cowshed grant <ws> …`                        | new grant revision                               | `--read/--write <path>`, `--egress <host> [--opaque]`, `--repo <host/org[/repo]>`, `--sim <verb>`, `--preset simulator` (04/05); `--ports <N>` grows a macOS port block to at least N service ports, gateway excluded (04).                                                                                                                                                                                                                                                                                                    |
| `cowshed grant --project-wide …`              | new standing-grant revision                      | `--read <path>`, `--egress <host> [--opaque]`: the project's standing grants every workspace runs under (04); bare prints them. Takes no `<ws>` and no `--write`.                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed identity add <remote>`               | the project's bound identities                   | Bind a remote main's checkout configures as a non-primary repository identity: from the next exec every workspace that can read the checkout fetches that URL (and its SSH form without the login) from the local clone (02). Refused when another adopted project binds the URL; an already-bound remote reports the binding unchanged.                                                                                                                                                                                       |
| `cowshed revoke <ws> …`                       | new grant revision                               | Same selectors + `--all`, except `--ports`: a port block never shrinks.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed push <ws>`                           | preserved ref, source sha                        | `--branch <name>` plus optional incarnation/source/destination CAS expectations. Preserves under `refs/cowshed/<ws>/…`; never advances an integration branch or checkout (02).                                                                                                                                                                                                                                                                                                                                                 |
| `cowshed rebase <ws>`                         | new head sha                                     | Rebase onto `--onto <branch-or-ref>` (default `main`); `--fresh` sheds divergence. Optional incarnation/source/base CAS expectations (02).                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed land <ws>`                           | target branch, landed sha, checkout state        | Refuse a dirty tree, validate, ff `--target <branch>` (default `main`), retire. Never rebases: a target main has moved past is refused (exit 4, `next: cowshed rebase <ws>`). The target must be the branch main's checkout has checked out, whose HEAD/index/tree it advances; any other target is refused. `--check <cmd>`, `--no-retire`, `--push-only`, and optional incarnation/source/target CAS expectations (02).                                                                                                      |
| `cowshed fork <src> <dst>`                    | mount path                                       | Mid-flight CoW copy; closed grants.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `cowshed checkpoint <ws> [label]`             | label                                            | Supervisor artifact barrier + manifest/commitment, then crash-consistent image snapshot; omitted label generates UTC timestamp. `--keep` exempts from gc.                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed restore <ws> <label>`                | mount path                                       | Label required; validates manifest/commitment before the fresh-incarnation publication fence. Previous image remains `pre-restore-…` (02).                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed rm <ws>`                             | — (no stdout)                                    | Perceived-instant (02). Requires HEAD contained in live main; `--force` does not waive that check. `--abandon` preserves unlanded commits in a verified bundle. Named removal also handles unpublished clones without activating them.                                                                                                                                                                                                                                                                                         |
| `cowshed mv <ws> <new-name>`                  | new mount path                                   | Rename a workspace, or move the project checkout with `cowshed mv main <new-path>` — the source decides whether the destination reads as a name or a path. Owns the unmount/republish/remount cycle, symmetric with `rm` (02). The rename refuses a dirty tree or a running job; the checkout move refuses a relative, occupied, parentless, or storage-overlapping destination — each naming the command that clears it (02).                                                                                                 |
| `cowshed du [ws]`                             | `--json`: written/referenced per ws + checkpoint | CoW-aware usage; lists checkpoints per workspace (01).                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `cowshed logs`                                | human table (`--json`/`--ndjson`: events)        | Controller telemetry (13). `--ws`, `--kind`, `--since`, `--follow`. Wraps `lmao-inspect` over the store segments.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed audit`                               | human table (`--json`/`--ndjson`: events)        | Gateway audit events (05/13). `--denied`, `--host`, `--ws`, `--follow` (live tail via the control plane).                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed trace <trace-id>`                    | human waterfall (`--json`: span tree)            | Terminal waterfall of a lifecycle op, exec, or land (13).                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed mcp serve`                           | — (stdio/socket server)                          | Coordinator authority arrives only on an inherited FD/socketpair; it is never printed or placed in argv/environment (12).                                                                                                                                                                                                                                                                                                                                                                                                      |
| `cowshed controller`                          | — (serves stdin)                                 | One coordinator controller connection for an embedding process, on standard input, which must be a Unix socket (else exit 2 before the project is resolved); exits 0 when the peer closes its end. A program that links cowshed is another build, and the daemon starts supervisors only for its own (11), so it runs its controller as this verb. Reconciles the project's gateway sessions before each exec, shell and checked land, as `exec` and `land --check` do (05).                                                   |
| `cowshed gateway run`                         | — (foreground daemon)                            | launchd runs this.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `cowshed gateway status`                      | `--json` status                                  | Cache stats, per-workspace counters, telemetry segment stats.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `cowshed resize <ws\|main> <size>`            | new capacity                                     | Grow the image (01, "Detached growth"); with `--build <size>` instead, grow the workspace's build volume and its seed (16, "Substrate"). The supervisor stops for the resize and restarts after; a held volume refuses before anything changes. `--json`: `ResizeResult { workspace, volume: "workspace"\|"build", previousCapacity, capacity }`.                                                                                                                                                                              |
| `cowshed defrag <ws>`                         | extent count after the rewrite                   | Rewrite the image contiguously (01, "Clone cost follows extents, not size"): detached like `resize`, refused while busy or short of free space, copied without cloning, renamed over, verified, remounted. `--json`: `DefragmentResult { workspace, previousExtents, extents, bytes }`.                                                                                                                                                                                                                                        |
| `cowshed reseed <ws\|main>`                   | `reseeded`, `fresh`, `skipped`, `noBuildVolume`  | Refreeze the workspace's build-volume seed from its live build volume when the seed is behind it (16, "Targets and seeds"), under the workspace's image lock, as every `new`/`fork` does first. The Nx daemon stops as at a land; another Nx task-database holder or a held Cargo `.cargo-lock` skips, named. Needs the workspace attached. `--json`: `ReseedResult { workspace, outcome }`.                                                                                                                                   |
| `cowshed rekey <ws\|main>`                    | workspace name (`--json`: `RekeyResult`)         | Rebuild the CA identity of a workspace whose companion key is missing or invalid: republish its quarantined sidecar beside the still-in-place image (revision + 1), mint fresh credentials into the live mount, consume the quarantine entry. Invalidates in-flight job certificates.                                                                                                                                                                                                                                          |
| `cowshed gc`                                  | freed bytes                                      | Two-phase exact candidate plan (01); `--dry-run` lists typed candidates and sums bytes with zero mutation.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed doctor`                              | `--json` findings                                | Invariant checks; each finding carries a `fix:` hint.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

`gc` never lets a candidate-local failure prevent later candidates from being tried; it reports each deferred path and
diagnostic on stderr and in the structured result, alongside actual bytes freed. `doctor` diagnoses sidecarless session
images by name rather than treating them as live workspaces or failing the inventory. Store-wide `ls --all` and
attach/detach skip an unreadable project with a named warning and still process other projects; identity collisions
remain errors. `doctor` retains a finding for each unreadable project while inspecting the others.

Store-wide attach and detach likewise try the other workspaces after one target or project fails and name each skipped
target; successful mount changes still reconcile with the gateway.

### `cowshed ls` detached rows

`ls` must never attach an image to read it (that would blow the ≤50 ms budget and mutate mount state), but the base
commit, branch, and age live in the in-image marker. So for a **detached** workspace those fields come from the snapshot
every grants sidecar carries (01_storage.md); a sidecar without one is unreadable, not a row with empty columns. `state`
and `name` are always accurate because they derive from readdir + getmntinfo alone.

### Checkpoint listing

Checkpoints are **not** listed by a bare `cowshed checkpoint <ws>` (that form generates a timestamped snapshot). List
them with `cowshed ls --json` (per-workspace `checkpoints` array) or `cowshed du <ws>` (which reports each checkpoint's
written/referenced bytes).

### Egress grant modes

`--egress <host>` grants an intercepted host by default: the gateway terminates TLS under the workspace CA and injects
the Keychain credential + trace context (05_gateway.md). `--opaque` grants every `--egress` host of the same invocation
as an opaque CONNECT tunnel instead (pinned clients, Go on macOS; no injection), and is a usage error without one. A
host holds one rule, so granting a host again restates its mode — `--egress <host>` alone turns an opaque host back to
intercepted. Per-port narrowing is a further field of that rule, set through the coordinator API's `EgressRule`; the CLI
takes no flag for it. A bare `cowshed grant <ws>` (no flags) prints the current set with a `mode` column on the egress
rows.

`cowshed grant --project-wide` addresses the project's standing policy: read paths, egress hosts, and workspace-relative
`--deny-write` and `--deny` paths every workspace (including main) runs under. The policy lives outside the workspaces
(04_sandbox.md, "Project-standing grants"), so a workspace grant cannot remove a project deny. The project comes from
ordinary discovery — the cwd, or `--project <git-root>`. `--write` and a `<ws>` alongside `--project-wide` are usage
errors; a write allow remains a per-workspace decision. A bare `cowshed grant --project-wide` prints the standing set,
and `--json` includes `{ revision, read, denyWrite, deny, egress }`.

`--sim <verb>` grants personal-session simulator broker verbs (`openurl`, `install` — 04/05/14); dev-side headless
simulators need the `--preset simulator` profile class instead (CoreSimulator IPC), not a `--sim` grant. `install` is
additionally bound to drop-dir artifacts and the human-gating rule (14_nix.md).

`cowshed grant <ws> --ports <N>` (macOS) is the workspace's port-capacity request: the block must hold at least N
service ports, the gateway port at its base excluded, so it becomes the smallest aligned power-of-two block with N+1
ports (`--ports 80` turns the initial 64-port block into a 128-port one). It sets `GrantDelta.servicePorts` (07_api.md);
growth is monotone, so a count the block already holds leaves the block, the grant revision, and the supervisor
unchanged, and `revoke` takes no `--ports`. `--ports 0`, a malformed count, and `--project-wide --ports` are usage
errors; Linux, whose workspaces each own a private loopback, refuses the grant rather than recording a block. A larger
block keeps its base when it can grow in place and otherwise moves to a disjoint one; the block it left stays reserved
to the workspace as `retainedPortBlocks` until the workspace is removed (04_sandbox.md). Either way growth needs an idle
workspace: while any of its jobs is active the grant is refused immediately — it never waits or queues — so services
that need the ports are launched after the grant. The next exec starts a fresh supervisor whose jobs get the new block's
Seatbelt profile and `COWSHED_PORT_BASE`/`COWSHED_PORT_BLOCK_SIZE` (04_sandbox.md). The reserved range is a finite host
resource shared by every workspace: a block that has no free aligned place is refused.

`cowshed exec` and `cowshed shell` accept `--session <name>` to bind to a named session in the workspace supervisor,
whose cwd and environment overlay apply to each of its commands; with or without it, the command runs in a warm exec
host of the workspace shell (11_shell.md). `cowshed exec` waits for its command however long it runs and exits with the
command's status. `--timeout <dur>` stops waiting after that long: a command still running then keeps running under the
supervisor, and `cowshed exec` exits **103** with an error naming its job and how to reach it, because it has no output
or status of the command's to give. `--background` asks not to wait at all: the numeric `JobId` is stdout, the exit is
0, and stderr names how to reach the job. The CLI has no verb that reattaches to a job; the API does
(`WorkspaceHandle::job`, 07_api.md). Leaving a job running changes attachment only. It forces memory-only prefixes into
lazy protected files so later reads and checkpoints are durable; it does not imply every terminal job has stream paths.

`--register` (on `cowshed new`) additionally adds a `cowshed/<ws>` remote in the **main** workspace pointing at the new
workspace's canonical mount, so a human in main can fetch and review the workspace's branch in place. Off by default
because it is host-side state that accumulates; `cowshed rm` and `cowshed land` drop it at retirement (02).

`--git-worktree` (on `cowshed new`) mints a git-worktree workspace: a registered linked worktree of main's repository
sharing its object store and refs, instead of the default standalone clone. Requires main mounted, then and thereafter;
gets no `main` remote (nothing to fetch from); and refuses `cowshed checkpoint` and `cowshed restore`, because its
history lives outside its image. Each refusal names the command that resolves it (02).

`--slot <n>` (on `cowshed new`) mounts the workspace at a stable, recycled path (`<mount-root>/<owner>/<repo>/slot@<n>`,
default mount root `~/.cowshed/mnt`) instead of a name-derived one. `owner` and `repo` are the separately validated and
encoded components of the primary `repo_id`, never an unsplit path value. Successive workspaces in the same slot inherit
each other's **path-keyed** cache warmth (Xcode DerivedData, and whatever a build records by absolute path; cargo's own
fingerprints are package-relative and need no slot) — opt-in, because it trades workspace-path uniqueness for warmth and
only one workspace may hold a slot at a time (exit 4 if occupied).

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

### Inferring `<ws>` from the working directory

A command run inside a mounted workspace may omit that workspace's name when the verb infers from cwd. Resolution is
containment of the canonical cwd in exactly one currently mounted workspace (01_storage.md), which is exact because
mount identity is keyed off the in-image marker, and which refuses an ambiguous match rather than picking one. An
explicit argument always wins; inference never overrides what the caller named. Outside any workspace those verbs refuse
and name both ways out: name one, or run the command from inside one.

The split is by what the verb does to the workspace, not by convenience:

| Infers from cwd                        | Requires the name                     |
| -------------------------------------- | ------------------------------------- |
| `rebase`, `push`, `checkpoint`, `path` | `rm`, `land`, `restore`, `mv`, `exec` |

Verbs that act on a workspace **in place** infer it. Verbs that **retire it** (`rm`, `land`), **replace it** (`restore`
mints a fresh incarnation over the running one), or **rename or move it** (`mv`) require it to be named, so that losing
the workspace you are standing in is always something you asked for by name rather than something the working directory
decided for you. `exec` is excluded for a second reason as well: its workspace argument is positionally ambiguous with
the command it runs.

`attach` and `detach` change mount state but do not infer a single workspace from cwd. `attach [ws] [--all]`: `<ws>` one
session; no name while cwd is in a project checkout or session = that project's detached sessions; `--all` every
detached session store-wide. Mains are always mounted and are never attach targets. `detach [ws] [--all]`: `<ws>` one
session resolved from the store readdir (`<owner>/<repo>/sessions/<ws>.image` plus sidecar/marker identity) without cwd
or git discovery; `--project` still selects the project; `--all` every attached session store-wide. Mains are never
detach targets. A bare `detach` with neither a name nor `--all` is usage.

Detach stops the workspace's supervisor, then stops the Nx daemon the checkout's own `.nx/workspace-data` record names,
as a land's quiesce does (`nx daemon --stop`'s `SIGTERM`): the daemon's cwd is the checkout, whichever shell started it,
and Nx starts a fresh one on the next run. A volume the kernel still refuses to unmount is a `conflict` naming every
holder, pid and argv (an open file, a working directory or an executable on the volume), with stopping them as the next
move. It is not a storage failure.

### Resident workspaces

`path <ws>` and `exec <ws> -- …` name one workspace, and when that workspace is mounted and its daemon-owned supervisor
(11_shell.md) already serves its current authority, every fact they depend on is live. They are answered from that state
without opening the project controller. The answer reads, fresh on every call, only the records that decide this
workspace: the in-image marker at the invocation's Git root (which names the project), the named workspace's marker at
its mount and its active sidecar, the project's policy (whose revision is part of the effective grant revision), slot
records (which place the mount), and the lifecycle-intent journal. It asks the host two things: whether a filesystem is
mounted exactly at the mount path, and which authority the supervisor's socket reports. `exec` also asks the gateway
whether the workspace's session is installed at that revision — exactly the case in which the controller's pre-exec
reconcile would install nothing for it. The job then runs through the supervisor's socket, relayed as the controller
path relays it.

Any disagreement opens the controller exactly as before, which is what does the work: a workspace that is detached,
unserved, served under an older grant or incarnation, a gateway session not yet at the served revision, a linked
worktree, or unfinished lifecycle work on the workspace, on `main`, or on the project identity. Git discovery steered by
`GIT_DIR`, `GIT_WORK_TREE`, `GIT_COMMON_DIR`, `GIT_CEILING_DIRECTORIES` or `GIT_DISCOVERY_ACROSS_FILESYSTEM`, an omitted
`<ws>`, `path --slot`, and `exec --session` always go through the controller. No directory is listed: a project with a
thousand retired sessions answers as fast as one with a single workspace. Once the supervisor admits the job, every
later failure is that job's and is reported, never retried through the controller.

What a resident answer does not re-check is what the serving supervisor proved when it opened: the project binding
against Git's remotes. The recorded binding changes only through cowshed verbs, which change the records read above; a
remote edited by hand is reconciled by the next verb that opens the controller. `COWSHED_TIMING=1` prints each step of
either path, including why a resident answer declined, on stderr (13_telemetry.md).

## Configuration

None required. `cowshed adopt` through daily use works with zero files. Optional `.cowshed.toml` at the repo root, all
keys optional:

```toml
capacity = "100g"              # image cap
[build]
capacity = "100g"              # build volume cap for a volume created from nothing; clones inherit theirs
[checkpoints]
keep = 5
[gateway]
port = 7644                    # macOS host control plane; Linux private-netns connector; never a service port
port_range = "32768-49151"    # macOS only: reserved range workspace port blocks are carved from
[cache]                        # extend (never replace) the convention table
extra_workspace_dirs = ["build-out"]
[shell]
output_limit = "1GiB"         # combined stdout+stderr per job; explicit output-limit terminal state
```

Workspace environment lives inside the image as `.cowshed/env`: it exports `COWSHED_WORKSPACE_TOKEN` (controller-minted
token), and on macOS `COWSHED_PORT_BASE` and `COWSHED_PORT_BLOCK_SIZE` (= the authoritative detached-metadata block base
and size) so `vite`/`astro`/`metro`/`devenv up` select ports inside the workspace's own block instead of colliding with
siblings (04_sandbox.md). Block size is not configuration: a block's size is persisted with it, new workspaces get 64
ports, and `cowshed grant <ws> --ports <N>` grows it (04_sandbox.md). Linux allocates no port block and emits no port
sentinel: dev servers bind its private loopback, while ordinary package tools retain their configured
`http://127.0.0.1:7644/…` proxy and registry URLs through exactly one trusted minimal connector launched inside that
namespace. Cowshed publishes `.cowshed/env` whenever it mints credentials and whenever it starts the workspace's
supervisor; `.envrc` sources it (see 02_workspaces.md). No CLI verb prints these values on demand.

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
