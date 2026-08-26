# cowshed CLI guide

## The contract

Every cowshed command follows the same I/O discipline:

- **Ordinary stdout** carries one control answer: a bare value, aligned table rows, or a bounded JSON envelope with
  `--json`. Job/status JSON contains lifecycle fields, typed artifact handles, byte counts, SHA-256 digests, bounded
  summaries, and may contain small `Inline.data` bytes tagged as `utf8` or `base64`. Foreground exec,
  supervisor-captured streams, `cowshed exec --session`, and artifact reads are the explicit interfaces for unbounded
  raw bytes.
- **stderr** carries progress, explanations, warnings, and self-driving guidance. Guidance lines are prefixed
  `cowshed:`; suggested follow-up commands are prefixed `next:`. Agents and humans read the same hints.
- **Exit codes** are stable:

| Code | Meaning                          | Typical cause                                                                                                                       |
| ---- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| 0    | ok                               | —                                                                                                                                   |
| 1    | internal error (bug — report it) | panic, unexpected hdiutil/diskutil failure                                                                                          |
| 2    | usage                            | unknown flag, missing argument                                                                                                      |
| 3    | not-found                        | no such workspace/project/checkpoint                                                                                                |
| 4    | conflict                         | name in use, workspace busy, restore over unsaved work                                                                              |
| 5    | env-missing                      | gateway, storage, mount, or executable unavailable; configured devenv refresh failed                                                |
| 6    | sandbox-denied                   | command blocked by the sandbox, confirmed by authoritative evidence; stderr names the path/domain and the grant that would allow it |
| 7    | integrity                        | committed job content missing, mutated, rolled back, or from outside the workspace's lineage                                        |

`cowshed exec` passes the child's exit code through **unchanged**; failures of cowshed's own exec wrapper (mount gone
mid-run, profile generation failed, integrity verification failed, …) use 100–106 so they can never collide with a child
that legitimately exits 1–7. Exit 6 is reported only when cowshed has authoritative evidence of a denial — the gateway
logged the egress decision, or the kernel sandbox telemetry names the blocked operation; otherwise the child's ordinary
exit passes through untouched. Exit 7 is reported only for an established content integrity failure, never for a summary
mismatch or ordinary child output.

The JSON envelope is uniform:

```
$ cowshed new raven --json
{"ok":true,"result":{"workspace":"raven","mount":"<project-root>/.cowshed/raven","baseCommit":"6f3a2c1"}}

$ cowshed path nonesuch --json
{"ok":false,"error":{"code":"not-found",
 "message":"no workspace 'nonesuch' in project 'example-project'",
 "hint":"cowshed ls"}}
```

Errors with `--json` still exit with their code; stderr stays human-readable either way. A bounded `JobInfo` may encode
small `Inline.data` bytes in a tagged `utf8` or `base64` form. The size bound is load-bearing: larger and live artifacts
remain handles, and unbounded bytes require `cowshed exec --session`, a `--background` job id, or an explicit artifact read.

## Global flags

- `--json` — JSON envelope on stdout instead of bare values/tables. Available on every command.
- `--project <git-root>` — select an adopted repository explicitly. Default: discover the standalone Git root owning the
  current directory, then validate its cowshed repository binding. Exit 3 if neither resolves.
- `-q` / `--quiet` — suppress `cowshed:` progress lines on stderr; errors and `next:` hints still print.
- `--help` / `-h` — the command map, or one command's full usage when a verb precedes it. `cowshed help [<command>]` is
  the same request. Help is an answer, not a diagnostic: it goes to stdout and exits 0, and `--help` anywhere in a
  command's own arguments answers instead of refusing the half-typed grammar it appears in. Past `--` the argument
  belongs to the child, so `cowshed exec raven -- cargo --help` runs cargo's help inside the workspace.

Usage lines are generated from the same option table the parser reads, so a flag a command accepts is a flag its usage
line and its `--help` page name, with one line saying what it does. A mistyped verb is corrected against the command
list: `cowshed sscache` refuses with `did you mean: sccache`.

Workspace names are `[a-z0-9][a-z0-9-]*`, unique per project. Commands taking `<name>` accept `main` wherever it makes
sense. `cowshed exec main -- ...` uses the same closed sandbox and explicit grants as every other workspace.

A host may adopt many repositories. Each repository binding owns one `main` and an independent namespace of workspace
names and checkpoints. cwd or `--project` selects the repository before a command interprets its workspace argument;
`main` is therefore repository-scoped, not host-global. `--repo-id` is used only while adopting a repository whose
identity cannot be derived unambiguously from its remotes.

## Lifecycle

### `cowshed setup [--uninstall] [--force]`

Idempotent host repair, runnable from any directory and needing no repository: its subject is the machine. It creates
absent volumes, remounts detached or mis-mounted ones at their canonical paths, validates each volume marker, and pins
the boot mounts in `/etc/fstab`. It never deletes a volume. On a healthy host it changes nothing and says so. Every
storage error in the CLI points here — a host with no volumes has no checkout to adopt.

Anything that can escalate happens inside one authorization session, and **the exact intent for every volume is printed
before the dialog appears** — name, UUID, size, and where it is going. When the plan creates and deletes nothing, that
is stated outright, because the macOS dialog gives you no way to tell a mount from a reformat. A run with nothing to
escalate raises no prompt at all. `-q` drops the per-volume outcome rows, but never the pre-dialog disclosure: hiding
what you are about to authorize is not a thing `--quiet` may do.

The common repair — volumes that already exist but have lost their boot pins:

```
$ cowshed setup
cowshed: setup will request administrator authorization once, for the actions below
cowshed: no volumes will be created or deleted; existing data is untouched
cowshed: cowshed.store exists (UUID 1D6F0E1A-…-AAAA, 1.0 TB) and will be mounted at /private/cowshed/store
cowshed: cowshed.caches exists (UUID 1D6F0E1A-…-BBBB, 2.0 TB) and will be mounted at /private/cowshed/caches
cowshed: /etc/fstab will pin UUID 1D6F0E1A-…-AAAA at /private/cowshed/store so it mounts at every boot
cowshed: cowshed.store (store): present but not mounted -> mounted
cowshed: cowshed.caches (caches): present but not mounted -> mounted
cowshed: pinned the boot mounts in /etc/fstab
cowshed: host storage is set up (one administrator authorization was used)
next: cowshed doctor
```

Sizes are decimal, as `diskutil` and the hardware state them, so the number matches what Disk Utility shows.
Reclaimable leftover files are listed by name rather than counted — "3 files will be deleted" is not something anyone
can agree to.

Dismissing the authorization dialog is an answer, not a failure. Nothing is changed and the run exits **6**:

```
$ cowshed setup
cowshed: setup will request administrator authorization once, for the actions below
…
cowshed: administrator authorization was declined, so nothing on this host was changed
next: cowshed setup
```

The stranded-user recovery, after a reboot left the volumes unmounted:

```
$ cowshed new raven
cowshed: cowshed.store is not mounted at /private/cowshed/store
next: cowshed setup

$ cowshed setup
…
cowshed: cowshed.store (store): present but not mounted -> mounted
cowshed: cowshed.caches (caches): mis-mounted at /Volumes/cowshed.caches -> remounted
cowshed: /etc/fstab already pins the boot mounts
cowshed: host storage is set up
next: cowshed doctor

$ cowshed setup
cowshed: cowshed.store (store): mounted at its canonical path -> already-current
cowshed: cowshed.caches (caches): mounted at its canonical path -> already-current
cowshed: /etc/fstab already pins the boot mounts
cowshed: everything already set up
next: cowshed doctor
```

A volume that exists **outside this host's container** — a `cowshed.store` on another disk — is reported as its own
state with its container, device, and current mount point named, and left exactly as it is. It is never reported as
missing and never re-created, because re-creating means `diskutil apfs deleteVolume`:

```
cowshed: cowshed.store (store): found outside this host's container (container disk4, device disk4s7, mounted at /Volumes/cowshed.store) -> reported
cowshed: data is safe on disk4s7; cowshed left it untouched
cowshed: host storage is partially set up: 1 volume lives outside this host's container and left untouched
```

`--uninstall` is the same transaction backwards, and narrower on purpose. It removes cowshed's **machine presence** —
the cowshed-tagged `/etc/fstab` pins, the `dev.cowshed.gateway` and `dev.cowshed.sccache` LaunchAgents, and the
installed binaries they ran — and touches no volume, no image, and no workspace. Nothing it removes holds data;
everything it leaves does. It therefore refuses while the volumes still hold workspaces, or while their occupancy
cannot be established at all (an unmounted store looks empty to every cheap check), until `--force` says the caller
means it anyway. There is no interactive prompt — the refusal is the prompt, and its hint is the completed command
line:

```
$ cowshed setup --uninstall
cowshed: 5 workspaces still exist on this host's volumes across 2 adopted projects; uninstall removes no volume and no
image, so they would be left unmanaged
next: cowshed setup --uninstall --force
```

With `--json`, `setup` emits the frozen envelope carrying the per-volume report; `--uninstall` reports the fstab
outcome and every service artifact it touched, in the order it touched them (both agents, then both binaries). A
teardown that found nothing installed reports an empty `services` list rather than omitting the field:

```
$ cowshed setup --json
{"ok":true,"result":{"volumes":[{"name":"cowshed.store","role":"store","stateBefore":"absent","action":"created"}],"fstab":"pinned","authorized":true}}

$ cowshed setup --uninstall --force --json
{"ok":true,"result":{"fstab":"removed","services":[{"what":"dev.cowshed.gateway agent","outcome":"removed"},{"what":"dev.cowshed.sccache agent","outcome":"already-absent"},{"what":"installed cowshed binary","outcome":"removed"},{"what":"installed sccache binary","outcome":"already-absent"}]}}
```

`outcome` is `removed` or `already-absent`; the stderr rendering of the same value reads `already absent`.

### `cowshed adopt`

Run once inside each existing checkout you want cowshed to manage. Adoption converts that repository into an
image-backed **main workspace** at the same path. A host may have any number of adopted repositories and therefore any
number of repository-scoped mains. Adoption is the only operation that copies the source tree into a new image.

On macOS, `cowshed adopt` and `cowshed setup` are the only commands allowed to create native storage. The first
adopt on a machine may display one administrator authorization prompt from `diskutil` while cowshed creates and mounts
the space-sharing `cowshed.store` and `cowshed.caches` APFS volumes. Once both volumes are present and correctly
mounted, later adopts only validate them and do not prompt.

Every other command (`new`, `ls`, `path`, `exec`, `rm`, `attach`, `detach`, and `doctor`) opens storage in existing-only
mode. If either volume is absent or needs mounting, the command exits with `environment-missing`, lists the required
setup actions, and prints `next: cowshed setup`; it never creates a volume, repairs a mount, or requests administrator
authorization. Launchd agents and future background services use the same existing-only entrypoint, so a background
process can report missing setup but can never cause a macOS authorization prompt.

Storage guidance points at `cowshed setup`, never at adopting a directory: a host with no volumes has no checkout to
adopt, and `adopt` would ask for one.

```
$ cd <project-root> && cowshed adopt
cowshed: created dedicated volumes cowshed.store, cowshed.caches (space-sharing, excluded from backup)
cowshed: creating image /private/cowshed/store/acme/widget/main.asif (capacity 100g, asif)
cowshed: copying 8,357,293 objects into the image (this is the one-time cost)
cowshed: verifying tree against source ... ok
cowshed: swapping <project-root> -> mountpoint (stub .envrc written beneath)
next: eval "$(cowshed ensure --envrc)"   # direnv repositories: add to .envrc
next: cowshed new <name>
<project-root>
```

The `.envrc` line is direnv wiring; cowshed does not authorize it. Devenv-native repositories instead reattach an
unmounted workspace with `cowshed ensure --attach`, evaluate the same exports in the human shell with
`eval "$(cowshed ensure --envrc)"`, and run the repository's `devenv:allow` command once for that workspace. Cowshed
never modifies either tool's trust database.

### `cowshed new <name> [--ref <rev> | --from <workspace>] [--browse] [--slot <n>]`

Clones a live image from the repository selected by cwd or `--project`, then mounts it. The source is that repository's
`main` by default:

```sh
cd ~/src/api
cowshed new raven
```

From another cwd, select the same repository explicitly:

```sh
cowshed new raven --project ~/src/api
```

Use `--from` to clone another workspace inside the selected repository:

```sh
cowshed new raven-alt --from raven --project ~/src/api
```

Cross-repository `--from` is forbidden; select another repository with cwd or `--project` instead. `--ref` starts the
new Git branch at another revision and is mutually exclusive with `--from`. The storage clone remains warm regardless.
`--browse` makes the volume visible in Finder; the default mount is nobrowse.

`--slot <n>` binds the workspace to a build slot, so it mounts at that slot's stable path instead of one named after it
— see [`cowshed path --slot`](#cowshed-path---slot-n--build-slots-and-compiler-cache-reuse). A slot already held by
another workspace is a conflict (exit 4).

### `cowshed ls [--all]`

Bare `ls` remains scoped to the repository selected by cwd or `--project`. Its stdout is a space-aligned table:
workspace name, state, branch, mountpoint (empty when detached).

```
$ cowshed ls
main   mounted   main           <project-root>
raven  mounted   cowshed/raven  <project-root>/.cowshed/raven
fox    detached  cowshed/fox
```

If that scoped list is empty, stderr reports how many other adopted projects exist and points to `cowshed ls --all`.
`--all` discovers every validated `<store>/<owner>/<repo>/repository.json`, then uses each project's normal listing
path. Plain output adds `repoId` as the first column and keeps projects contiguous:

```
$ cowshed ls --all
acme/api  main   mounted  main           ~/src/api
acme/api  raven  mounted  cowshed/raven  ~/src/api/.cowshed/raven
acme/web  main   mounted  main           ~/src/web
```

With `--json`, the result is grouped explicitly as
`[{"repoId":"acme/api","workspaces":[...]},{"repoId":"acme/web","workspaces":[...]}]`.

### `cowshed path <name>`

Bare mountpoint on stdout. Exit 3 if the workspace doesn't exist. A detached workspace is attached first, so the printed
path is always live; pass `--no-attach` to skip the remount and get the would-be path with a `cowshed:` note instead.

### `cowshed path --slot <n>` — build slots and compiler-cache reuse

A **build slot** is one stable mount path, occupied by one workspace at a time. `cowshed new <name> --slot 3` binds slot
3, and that workspace mounts at `<project-root>/.cowshed/slot-3` instead of `.../<name>`. When it is removed or
renamed the slot is released, and the next workspace to take slot 3 mounts at exactly the same absolute path.

That path identity is the entire feature, because compiler caches key on absolute paths:

- Cargo derives `-C metadata` and `-C extra-filename` from a package id that carries the **absolute manifest
  directory**, so a local crate compiled at two paths is two different compilations.
- sccache additionally hashes the compiler's **physical** working directory.

Measured on this hardware with sccache 0.16 over a ten-crate workspace, second checkout of identical sources:

| build path                         | Rust units hit |
| ---------------------------------- | -------------- |
| same absolute path (slot)          | **22/22**      |
| sibling paths                      | 9/22           |
| sibling paths + `SCCACHE_BASEDIRS` | 9/22           |
| symlink + `cargo --manifest-path`  | 12/22          |

The 9 that hit without a slot are registry crates, whose package ids carry no path. Note what does _not_ work: a symlink
is inert, because cargo resolves its working directory with `getcwd` (`cargo metadata` through a symlinked checkout
reports the physical path), and `SCCACHE_BASEDIRS` cannot help either, because `-C metadata` is a hash sccache never
sees. Only the mount path itself can be the stable thing.

`cowshed path --slot <n>` prints the tenant's mountpoint without you knowing which workspace holds the slot (exit 4 if
nobody does); `cowshed path <name>` prints the same path from the other direction. **Builds must run through the slot
path to benefit** — that is where a slot-bound workspace is mounted, so `cd $(cowshed path --slot 3)` and `cowshed exec`
both land there, but a build reached through some other route to the same files (a symlink you made, a `--manifest-path`
into one) is a different compilation.

The trade: a workspace mounted at a slot path gets `RUSTC_WRAPPER=sccache` **and `CARGO_INCREMENTAL=0`**, from
`ensure --envrc` and from `cowshed exec` alike. Incremental compilation is per-unit local state sccache cannot cache and
cargo prefers when both are available, so a slot tenant is choosing the shared cross-generation cache over local
incrementality. Name-mounted workspaces are never opted in: they get the cache endpoints (`SCCACHE_SERVER_UDS`,
`SCCACHE_DIR`) but nothing that routes rustc through a cache their path cannot share.

`main` cannot take a slot — its mount is fixed by the project's checkout layout.

### `cowshed rm <name> [--force] [--restore] [--abandon]`

Marks the workspace deleted and returns immediately; detach and image deletion happen in the background.

Removing a workspace destroys its image, and the image is where its commits live — so `rm` refuses (exit 4) unless
`main` already contains the workspace's `HEAD`. The refusal names main's tip and points at `cowshed land <ws>`.

The two overrides authorize different losses and neither substitutes for the other:

| Flag        | Overrides                                                         | Does **not** override    |
| ----------- | ----------------------------------------------------------------- | ------------------------ |
| `--force`   | transient state: a dirty tree, an in-progress merge, a busy mount | the landed-ancestry gate |
| `--abandon` | the landed-ancestry gate — commits `main` does not contain        | transient state          |

`--abandon` has no short spelling, and it is the only way to delete unlanded commits. Before deleting, it writes a Git
bundle of `main..HEAD` beside the retired image in `sessions/.trash/<ws>-<tip>.bundle` and prints what it destroyed, so
even a deliberate abandonment is recoverable:

```
$ cowshed rm raven
cowshed: workspace raven head 9b2e77d… is not contained by main (main is at 6f3a2c1…)
next: land the workspace: cowshed land raven

$ cowshed rm raven --abandon
cowshed: abandoned 9 commits at 9b2e77d… that main (at 6f3a2c1…) did not contain
cowshed: bundled to /private/cowshed/store/acme/api/sessions/.trash/raven-9b2e77d….bundle
next: cowshed gc   # free space from old checkpoints too
```

Recover an abandoned bundle from main's repository, which holds its one prerequisite:

```sh
git fetch <bundle> HEAD:refs/heads/recovered-raven
```

`cowshed rm main --restore` is the reverse of `adopt`: it puts the pre-adoption checkout back and unbinds the project.
Plain `cowshed rm main` throws the warm main image away instead, and needs `--force`.

Nothing on stdout — `rm` has no answer to give. With `--json`, the result is `{}`, or
`{"abandoned":{"head":…,"targetBranch":"main","targetHead":…,"unlandedCommits":9,"bundle":…}}` after an abandonment.

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

### Background work

Long commands auto-background at the soft timeout (default 120 s; `--timeout <dur>` tunes it, `--background` forces it
immediately) and keep running under the workspace supervisor. `cowshed exec` accepts `--session <name>` for a persistent
named shell whose cwd, variables, and jobs survive across calls. There is no `cowshed job` verb; reattach with
`cowshed exec --session` or print the numeric job id from `--background`.

Every job has separate stdout/stderr `StreamInfo { storage, bytes, sha256, summary }` handles. `storage` is
`Captured { artifact }` or `Redirect { source, artifact }`; `artifact` is `Inline { data: BinaryData }` or
`File { path: WorkspacePath }`. Small terminal streams remain inline and protected files spill lazily, so consumers must
not assume every short job creates `out` and `err` files.

```sh
$ cowshed exec raven --background -- bun run build:everything
42
$ cowshed exec raven --session build -- bun run build:everything
```

Control/status JSON is bounded; it may carry tagged bytes only for a small inline artifact. Supervisor-captured streams
and artifact reads resolve the canonical artifact independently of whether it is inline or spilled and preserve arbitrary
binary output without UTF-8 assumptions or response-size growth.


### `cowshed ensure [--envrc]`

The fast auto-fix. Healthy fast-path is a marker read plus a statfs (~15–25 ms, silent, exit 0). Otherwise it reattaches
images after reboot or Finder ejects, repairs mount flags, re-arms the autosave agent, and reconciles anything drifted —
synchronously, so when it returns you are standing in a valid workspace. Devenv-native repositories use
`cowshed ensure --attach` as the explicit remount spelling. `--envrc` additionally prints POSIX shell exports for the
current workspace and must be run from inside that workspace — each directory has its own exports:

```sh
$ cowshed ensure --envrc
export GOENV='<project-root>/.cowshed/raven/.cowshed/cache/go/env'
export SCCACHE_SERVER_UDS='/private/cowshed/store/sccache.sock'
export COWSHED_WORKSPACE_TOKEN='cw1_r4v3n…'
export COWSHED_PORT_BASE='40960'
```

Direnv repositories evaluate that output from `.envrc`. Devenv-native repositories may evaluate it after an explicit
attach; sandboxed `cowshed exec` processes receive the cowshed-owned exports directly. If `[devenv] dir` is configured
in `.cowshed.toml`, devenv's exported variables form the base environment for each new sandbox process, while
controller-filtered values and cowshed's own `GOENV`/`SCCACHE_SERVER_UDS`/`COWSHED_*` values win on conflicts. The
devenv-provided `PATH` is discarded in favor of cowshed's admitted, profile-first PATH.

Deliberately short: wiring is carried by **files, not environment**. The registry URL (the macOS workspace gateway base
port, or Linux's fixed private-loopback connector at `127.0.0.1:7644`) and the bun cache dir live in the committed
`bunfig.toml` — bun honors a _relative_ `[install.cache] dir`, verified, so there is no cache export at all; cargo's
source replacement and `SCCACHE_SERVER_UDS` live in the in-image `.cargo/config.toml` (cargo's `[env]` verifiably
reaches rustc-wrapper invocations); the read-at-build caches (cargo registry, Go module/build caches, sccache, zig,
gradle) are reached through their tools' _default_ host paths, relocated once onto the caches volume at first adopt —
except Go, which has no directory-scoped config: its in-image env file (carrying the per-workspace `GOPROXY`, the shared
caches, in-image `GOPATH`/`GOBIN`, and `GOTOOLCHAIN=local`) is reached via the `GOENV` export, so `~/go` is never
created. The load-bearing exports above are few by design (token-via-config would kill the first; a file-based `GOENV`
alternative — none known — would kill the second; `SCCACHE_SERVER_UDS` stays, as the host sccache daemon's endpoint has
no per-tool config file); on macOS, `PORT`/`COWSHED_PORT_BASE` wire dev servers into the workspace's port block (see
"Dev servers" above); Linux has no block. The `COWSHED_*` identity lines are prompt conveniences, never load-bearing.

`ensure` never does slow or surprising work — no fetches, no compaction, no installs. Main gets the same wiring (that's
the "main shares caches like sandboxes do" rule; the only difference is main isn't sandboxed).

### `cowshed attach <name>` / `cowshed detach <name>`

Suspend and resume a workspace without destroying it. Detached workspaces cost one closed file.

### `cowshed resize <name|main> <size>`

Grow one workspace's image. Sizes are `100g`, `200g`, `1t` — binary units, at least a mebibyte, and a whole number of
the 4 KiB blocks the image tools resize in.

```
$ cowshed resize raven 200g
200g
cowshed: workspace raven grew from 100g to 200g
```

Resize only ever grows: a size that does not exceed what the image already holds is refused, with the current capacity
named, before anything is touched. A mounted workspace is detached, grown, and put back on its mount; a workspace whose
volume is busy refuses the resize rather than being torn out from under running work.

Capacity itself is chosen once, at `cowshed adopt --capacity <size>` (default `100g`), because that is the only verb
that mints an image — `new` and `fork` clone main's and inherit its capacity, so `resize` is how a clone gets a bigger
one.

### Simulators (iOS) — `cowshed sim export <name> [artifact]`

Copies a built `.app` to the one-way drop dir (`<shared-drop-root>/<owner>/<repo>/`, using the separately validated
components of the primary `repo_id`; stdout = the drop path) so the personal session can install it into the human's
native Simulator.app — the artifact handoff for posture B. The in-image `xcrun` wrapper handles the rest of the
simulator story (dev-local headless simulators by default; personal-session devices via `--sim` grants). The full
walkthrough, Expo included, is [ios.md](ios.md).

## Sandbox grants

Workspaces start **closed**: write access to their own volume, `/private/cowshed/caches`, and temp; read access to the
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
incarnation/checkpoint snapshot. The image's marker carries the incarnation and its lineage, which is what authorizes
records an ancestor wrote into a cloned image; the controller's audit records (telemetry, never read for a decision)
carry job existence, lifecycle, order, lineage, terminal state, byte counts, hashes, and batch digest without payload or
path. Every shell/session/descendant is restricted from writing `.cowshed/job/**` before repository-controlled startup.
A frame from outside the workspace's lineage, or content that fails its own digests, is a typed integrity failure.

MCP coordinator authority is delivered only through an inherited FD/socketpair, never stderr, argv, environment, or a
workspace file. Worker descriptors are 256-bit, one-use, expire after 30 seconds, are atomically consumed, restart-
invalidated, and bound to the intended peer/socket/workspace. Authorization uses its own RPC error and is not a sandbox
denial.

## Git

Workspace git is **local-paths-only**: every workspace has the `host` remote (main's repository, a mounted path) and can
clone from read-only mirrors under `/private/cowshed/caches/repo-mirrors` — nothing else. No remote URLs, no credentials, no credential
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
cowshed: mirror /private/cowshed/caches/repo-mirrors/github.com/tinylibs/tinybench.git (fetched via gateway)
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
incarnation. Protected content remains authoritative for the restored snapshot's origin boundary; the restored marker
records the lineage, and the controller's audit record of the restore carries the hashes. Restore refuses over unsaved
work (exit 4); the displaced image is kept as a `pre-restore-<timestamp>` checkpoint, so a restore is
itself undoable. List checkpoints with `cowshed ls --json` or `cowshed du`.

```
$ cowshed checkpoint raven pre-refactor
pre-refactor
$ cowshed restore raven pre-refactor
cowshed: raven restored to pre-refactor (previous image kept as pre-restore-2026-07-11T14-22-09Z)
next: cowshed exec raven -- git status
```

## Infrastructure

### `cowshed gateway start` / `stop [--purge]` / `status`

`start` installs and loads the per-user macOS LaunchAgent `dev.cowshed.gateway`, then waits until its authenticated Unix
control socket is healthy. The generated mode-0600 plist names `~/Library/Application Support/dev.cowshed/bin/cowshed`,
`RunAtLoad`, `KeepAlive`, and stable pre-tracer stderr at `~/Library/Logs/cowshed/daemon-stderr.log`. That path is on
the volume carrying `~/Library/LaunchAgents` itself, so launchd can still reach the program after a reboot: `start`
copies the running executable there when the bytes differ, and refuses a running executable inside cowshed's own storage
rather than baking in a path that only exists once cowshed has mounted it. `stop` boots out the agent and removes the
plist, leaving the installed binary — that copy is host state rather than agent state, and keeping it makes the next
`start` a plist write instead of a fresh multi-megabyte copy. `stop --purge` deletes it too, for a host that is done
with the gateway rather than pausing it; `cowshed setup --uninstall` does the same for both services at once. All of
these are idempotent, and a `--purge` with nothing installed says so rather than failing.

`status` reports health without starting the service. Its JSON result is the standard frozen envelope:

```json
{
  "ok": true,
  "result": {
    "running": true,
    "socket": "/private/cowshed/store/gateway.sock",
    "cacheEntries": 0,
    "cacheBytes": 0,
    "activeWorkspaces": 3
  }
}
```

`gateway run` is the LaunchAgent's internal foreground entrypoint. It validates already-mounted host storage and
creates none, restores every authoritative attached workspace session, and drains on SIGTERM or SIGINT. Ordinary `exec`,
`ensure`, and `doctor` commands reconcile the current project's attached sessions before admission; lifecycle commands
reconcile again before reporting success. If the service is absent they fail with exit 5 and the exact
`launchctl kickstart -k gui/<uid>/dev.cowshed.gateway` next hint.

### `cowshed sccache start [--capacity <size>]` / `stop` / `status`

The gateway daemon starts this agent itself, so a healthy host already has it: `run_daemon` heals every project's mounts
and then the compile cache. A host without sccache on PATH logs one line and serves normally. The verbs are for repair,
inspection, and resizing.

`start` installs and loads the per-user macOS LaunchAgent `dev.cowshed.sccache`, then waits until the server answers on
its unix socket at `/private/cowshed/store/sccache.sock`. The mode-0600 plist runs the _sccache binary itself_ — a copy at
`~/Library/Application Support/dev.cowshed/bin/sccache`, installed by `start` from the sccache it resolves on the
invoking shell's PATH, so run it from a shell with the devenv/nix sccache available — as a foreground unix-socket server:
`SCCACHE_START_SERVER=1` selects server mode, `SCCACHE_NO_DAEMON=1` keeps it under launchd supervision,
`SCCACHE_IDLE_TIMEOUT=0` disables idle exit, and `SCCACHE_DIR` pins the shared store at `/private/cowshed/caches/sccache`.
Stderr lands at `~/Library/Logs/cowshed/sccache-stderr.log`. `stop` boots out the agent and removes the plist; both
operations are idempotent. The copy is what keeps the daemon alive across a devenv update or nix garbage collection: an
sccache upgrade is picked up by rerunning `cowshed sccache start`, which recopies on byte drift and rewrites the plist
only on drift.

Two more variables are in that plist because sccache reads them once, at server start, and no client can supply them:

- `SCCACHE_CACHE_SIZE` — the cap. sccache's own default is 10 GiB, which is smaller than one debug graph of a project
  cowshed hosts, so the default evicts the entries a second slot tenant came for. The derived default is the summed
  allocated size of every adopted project's `main` image, floored at **40 GiB** and rounded up to a whole gibibyte;
  `--capacity 120g` overrides it (same size grammar as `cowshed adopt`/`resize`).
- `SCCACHE_BASEDIRS` — **plural**. sccache 0.16 has no `SCCACHE_BASEDIR` at all and ignores it silently, which is how a
  host can look configured while `--show-stats` reports `Base directories (none)`. It is set to the store root. Do not
  expect it to buy cross-path Rust reuse: measured, it changes nothing there, because cargo's `-C metadata` is a hash
  sccache never sees. Build slots are what fix that.

`status` reports launchd and socket health without starting anything, and surfaces the daemon's own `--show-stats`
whenever it answers:

```json
{
  "ok": true,
  "result": {
    "installed": true,
    "running": true,
    "socket": "/private/cowshed/store/sccache.sock",
    "stats": {
      "maxCacheSize": 42949672960,
      "baseDirectories": ["/private/cowshed/store"],
      "compileRequests": 1204,
      "requestsExecuted": 1204,
      "hits": { "C/C++": 39, "Rust": 22 },
      "misses": { "Rust": 0 }
    }
  }
}
```

Hits and misses are per language on purpose: cross-workspace C and C++ reuse works without any slot, so a healthy
aggregate hit rate routinely hides a Rust hit rate of zero — which is the number a slot host is managing.

Workspaces reach the daemon through `SCCACHE_SERVER_UDS` (supervisor-injected, `ensure --envrc`-exported, and carried by
the cargo `[env]` guidance); the Seatbelt profile admits exactly that socket and keeps the sccache store
daemon-write-only. `sccache --show-stats` works from any shell with the export set — it speaks to the same server.

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
the generic `lmao-inspect` reader over the Arrow segments in `/private/cowshed/store/telemetry/`.

### `cowshed mcp serve`

Runs the MCP server (stdio, or a shared Unix socket) exposing workspaces as tools for agent harnesses. Coordinator
authority arrives only through a dedicated inherited FD/socketpair and is never printed on stderr or placed in argv or
environment. Worker connections redeem short-lived one-use descriptors and can run or observe only their bound
workspace.

### `cowshed gc`

Deletes orphaned images and stale mountpoint dirs, prunes expired checkpoints, compacts detached images, and reports
what it freed. Safe to run anytime; `rm`, `land`, and `restore` also run it opportunistically.

### `cowshed doctor`

Invariant checks: every image has a marker, every mount matches an image, grants files parse, caches volume and gateway
reachable, autosave fresh. Exit 0 when healthy; otherwise the code of the most severe finding (3/4/5) with one
`cowshed:` line per issue and a `next:` fix for each.

```
$ cowshed doctor
cowshed: gateway not running (last audit event 2d ago)
next: launchctl kickstart -k gui/501/dev.cowshed.gateway
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
