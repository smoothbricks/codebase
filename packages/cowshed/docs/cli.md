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

| Code | Meaning                          | Typical cause                                                                                                                                                                                                                                                                                                                        |
| ---- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 0    | ok                               | —                                                                                                                                                                                                                                                                                                                                    |
| 1    | internal error (bug — report it) | panic, unexpected hdiutil/diskutil failure                                                                                                                                                                                                                                                                                           |
| 2    | usage                            | unknown flag, missing argument                                                                                                                                                                                                                                                                                                       |
| 3    | not-found                        | no such workspace/project/checkpoint                                                                                                                                                                                                                                                                                                 |
| 4    | conflict                         | name in use, workspace busy, restore over unsaved work                                                                                                                                                                                                                                                                               |
| 5    | env-missing                      | gateway, storage, mount, or executable unavailable; configured devenv refresh failed                                                                                                                                                                                                                                                 |
| 6    | sandbox-denied                   | command blocked by the sandbox, confirmed by authoritative evidence, with stderr naming the path/domain and the grant that would allow it; or the shell running cowshed is itself sandboxed away from cowshed's store (EPERM on a store path), where no grant helps and the hint is to rerun from a shell allowed to write the store |
| 7    | integrity                        | committed job content missing, mutated, rolled back, or from outside the workspace's lineage                                                                                                                                                                                                                                         |

`cowshed exec` passes the child's exit code through **unchanged**; failures of cowshed's own exec wrapper (mount gone
mid-run, profile generation failed, integrity verification failed, …) use 100–106 so they can never collide with a child
that legitimately exits 1–7. Exit 6 is reported only when cowshed has authoritative evidence of a denial — the gateway
logged the egress decision, the kernel sandbox telemetry names the blocked operation, or cowshed's own write to a
storage path it owns failed with EPERM; otherwise the child's ordinary exit passes through untouched. Exit 7 is reported
only for an established content integrity failure, never for a summary mismatch or ordinary child output.

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
remain handles, and unbounded bytes require `cowshed exec --session`, a `--background` job id, or an explicit artifact
read.

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
`main` is therefore repository-scoped, not host-global. `--repo-id` selects an explicit identity while adopting and
repairs an adopted identity with `cowshed mv main --repo-id`.

## Lifecycle

### `cowshed setup [--uninstall] [--force] [--mount-root <dir>]`

Idempotent host repair, runnable from any directory and needing no repository: its subject is the machine. It creates
absent volumes, remounts detached or mis-mounted ones at their canonical paths, FileVault-encrypts unencrypted volumes
in place, stores each independent random passphrase in `/Library/Keychains/System.keychain`, validates each volume
marker, pins the boot mounts in `/etc/fstab`, and installs the root-owned `dev.cowshed.storage` system LaunchDaemon.
That daemon runs a fixed `/bin/sh` script of the volume UUIDs and canonical paths
(`/Library/Application Support/dev.cowshed/mount-volumes.sh`); before login and without invoking the cowshed binary, it
reads each passphrase from System.keychain, runs `diskutil apfs unlockVolume -nomount`, then mounts the volume with
`-nobrowse` at its canonical path. fstab keeps `noauto` so Disk Arbitration does not race it. Setup never deletes or
recreates a volume: existing volumes are encrypted in place. On a healthy host it changes nothing and says so. Every
storage error in the CLI points here — a host with no volumes has no checkout to adopt.

`--mount-root <dir>` sets the host workspace mount root (default `~/.cowshed/mnt`). Session workspaces mount at
`<mount-root>/<owner>/<repo>/<ws>`. The path must be absolute. The root can change only while every workspace is
detached; otherwise setup names the attached workspaces and refuses. Stdout is the configured root.

After a repair, and after `--mount-root`, setup probes git identity for every adopted checkout: it runs
`git config --list --show-origin` in the checkout and in a throwaway repository under the mount root
(`<mount-root>/<owner>/<repo>/`), then diffs the observed origins. A config file included only in the checkout is
reported on stderr, prefixed with its project, by name with the `includeIf` condition that pulled it in, plus the
remedy: add a pattern covering the mount root. A file the repository's own config includes by a relative path inside the
repository (`include.path = ../tooling/workspace.gitconfig`) travels with every clone and is not reported.
`cowshed doctor` reports the same finding for the current project. `new` does not probe: the answer depends only on your
git configuration and the mount root.

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
cowshed: cowshed.store exists (UUID 1D6F0E1A-…-AAAA, 1.0 TB) and will be FileVault-encrypted in place; passphrase stored in System.keychain
cowshed: cowshed.caches exists (UUID 1D6F0E1A-…-BBBB, 2.0 TB) and will be FileVault-encrypted in place; passphrase stored in System.keychain
cowshed: /etc/fstab will pin UUID 1D6F0E1A-…-AAAA at /private/cowshed/store so it mounts at every boot
cowshed: /etc/fstab will pin UUID 1D6F0E1A-…-BBBB at /private/cowshed/caches so it mounts at every boot
cowshed: system LaunchDaemon dev.cowshed.storage will be installed to unlock and mount cowshed volumes before login
cowshed: cowshed.store (store): present but not mounted -> mounted
cowshed: cowshed.caches (caches): present but not mounted -> mounted
cowshed: pinned the boot mounts in /etc/fstab
cowshed: host storage is set up (one administrator authorization was used)
```

Sizes are decimal, as `diskutil` and the hardware state them, so the number matches what Disk Utility shows. Reclaimable
leftover files are listed by name rather than counted — "3 files will be deleted" is not something anyone can agree to.

Dismissing the authorization dialog is an answer, not a failure. Nothing is changed and the run exits **6**:

```
$ cowshed setup
cowshed: setup will request administrator authorization once, for the actions below
…
cowshed: administrator authorization was declined, so nothing on this host was changed
next: cowshed setup
```

A sequence that stops partway is reported action by action, and **never** as a success. Core hands the CLI the progress
it made alongside the failure that stopped it, so the run says which actions completed, which one failed and why, and
which were never reached — then exits with the failing action's own code:

```
$ cowshed setup
cowshed: setup will request administrator authorization once, for the actions below
cowshed: no volumes will be created or deleted; existing data is untouched
cowshed: cowshed.store exists (UUID …-A, 1.0 TB) and will be mounted at /private/cowshed/store
cowshed: cowshed.caches exists (UUID …-B, 2.0 TB) and will be mounted at /private/cowshed/caches
cowshed: cowshed.store exists (UUID …-A, 1.0 TB) and will be FileVault-encrypted in place; passphrase stored in System.keychain
cowshed: cowshed.caches exists (UUID …-B, 2.0 TB) and will be FileVault-encrypted in place; passphrase stored in System.keychain
cowshed: /etc/fstab will pin UUID …-A at /private/cowshed/store so it mounts at every boot
cowshed: /etc/fstab will pin UUID …-B at /private/cowshed/caches so it mounts at every boot
cowshed: system LaunchDaemon dev.cowshed.storage will be installed to unlock and mount cowshed volumes before login
cowshed: cowshed.store exists (UUID …-A, 1.0 TB) and will be mounted at /private/cowshed/store: done
cowshed: cowshed.caches exists (UUID …-B, 2.0 TB) and will be mounted at /private/cowshed/caches: FAILED — resource busy
cowshed: cowshed.store exists (UUID …-A, 1.0 TB) and will be FileVault-encrypted in place; passphrase stored in System.keychain: not attempted
cowshed: cowshed.caches exists (UUID …-B, 2.0 TB) and will be FileVault-encrypted in place; passphrase stored in System.keychain: not attempted
cowshed: /etc/fstab will pin UUID …-A at /private/cowshed/store so it mounts at every boot: not attempted
cowshed: /etc/fstab will pin UUID …-B at /private/cowshed/caches so it mounts at every boot: not attempted
cowshed: system LaunchDaemon dev.cowshed.storage will be installed to unlock and mount cowshed volumes before login: not attempted
cowshed: host storage is NOT set up: 1 action done, 1 failed, 5 not attempted
cowshed: cowshed.caches could not be mounted: resource busy
next: cowshed doctor
```

Each outcome line repeats its intent sentence verbatim, so the line you authorized and the line reporting it are
recognisably the same action. A run that completes prints no outcome lines — they would only repeat the volume rows.
With `--json` a partial run answers `ok:false` with the failing action's error; the per-action evidence stays on stderr,
because the frozen envelope has no partial state and answering `ok:true` over a failure is the one thing it must not do.

A dialog dismissed _partway_ through says so, and pointedly does not claim nothing changed — earlier actions had already
succeeded. It still exits 6.

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
cowshed: wrote ~/Library/Application Support/Mozilla.sccache/config: an sccache client that inherited no cowshed environment now caches in /private/cowshed/caches/sccache
cowshed: host storage is set up

$ cowshed setup
cowshed: cowshed.store (store): mounted at its canonical path -> already-current
cowshed: cowshed.caches (caches): mounted at its canonical path -> already-current
cowshed: /etc/fstab already pins the boot mounts
cowshed: ~/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache
cowshed: everything already set up
```

A volume that exists **outside this host's container** — a `cowshed.store` on another disk — is reported as its own
state with its container, device, and current mount point named, and left exactly as it is. It is never reported as
missing and never re-created, because re-creating means `diskutil apfs deleteVolume`:

```
cowshed: cowshed.store (store): found outside this host's container (container disk4, device disk4s7, mounted at /Volumes/cowshed.store) -> reported
cowshed: data is safe on disk4s7; cowshed left it untouched
cowshed: host storage is partially set up: 1 volume lives outside this host's container and left untouched
```

`setup` also writes **sccache's own config file** — the one host-level thing that decides where a compile cache lands
for a client that has no cowshed environment at all. Every workspace gets `SCCACHE_DIR` from its supervisor, so a build
inside a workspace already reaches the daemon; a `cargo` invoked anywhere else is still wrapped (`RUSTC_WRAPPER=sccache`
survives in any shell that once loaded a project environment) and would otherwise fall back to sccache's private
per-user directory, where nothing is shared and the hit rate reads as zero. The file names
`/private/cowshed/caches/sccache` and the same cap the daemon uses, because a client that finds no daemon starts a
server of its own over that directory and sccache's 10 GiB default would evict the shared store down to it.

The destination is whichever config path sccache itself would load — `~/Library/Application Support/Mozilla.sccache/` on
macOS, `$XDG_CONFIG_HOME/sccache/` elsewhere, and the legacy `~/Library/Preferences/Mozilla.sccache/` when a config
already lives there, since writing the modern path over an existing old one would shadow rather than replace it. cowshed
owns exactly the `[cache.disk]` table and says so in a comment at the top of the block (a comment, because sccache
rejects unknown _keys_ and would refuse to start over one). Nothing else in that file is ever rewritten: the block is
appended below whatever is already there, and a `cache.disk.dir` cowshed did not write is left alone and reported rather
than overwritten —

```
cowshed: left ~/Library/Application Support/Mozilla.sccache/config alone: it already sets cache.disk.dir to ~/Library/Caches/Mozilla.sccache; a store-less sccache client will not share /private/cowshed/caches/sccache until cache.disk.dir names it
```

which is a report, not a failure: the host's storage is set up either way. A block cowshed _did_ write is refreshed on
every run, so a cap that has drifted upward as projects were adopted is repaired by running `setup` again; a foreign
`dir` never is, and has to be resolved by hand. Nothing is written at all when the caches volume is not mounted, since a
config naming a directory beneath an empty mountpoint would resolve onto the boot disk and become one more orphaned
cache.

`--uninstall` is the same transaction backwards, and narrower on purpose. It removes cowshed's **machine presence** —
the cowshed-tagged `/etc/fstab` pins, the `dev.cowshed.storage` system LaunchDaemon, the `cowshed.store` and
`cowshed.caches` items in `/Library/Keychains/System.keychain`, the `dev.cowshed.gateway` and `dev.cowshed.sccache`
LaunchAgents, and the installed binaries they ran — and touches no volume, no image, and no workspace. Nothing it
removes holds data; everything it leaves does. It therefore refuses while the volumes still hold workspaces, or while
their occupancy cannot be established at all (an unmounted store looks empty to every cheap check), until `--force` says
the caller means it anyway. There is no interactive prompt — the refusal is the prompt, and its hint is the completed
command line:

```
$ cowshed setup --uninstall
cowshed: 5 workspaces still exist on this host's volumes across 2 adopted projects; uninstall removes no volume and no
image, so they would be left unmanaged
next: cowshed setup --uninstall --force
```

With `--json`, `setup` emits the frozen envelope carrying the per-volume report; `--uninstall` reports the fstab outcome
and every service artifact it touched, in the order it touched them (system daemon, then both System.keychain items,
then both user agents, then both binaries). A teardown that found nothing installed reports an empty `services` list
rather than omitting the field:

```
$ cowshed setup --json
{"ok":true,"result":{"volumes":[{"name":"cowshed.store","role":"store","stateBefore":"absent","action":"created"}],"fstab":"pinned","authorized":true}}

$ cowshed setup --uninstall --force --json
{"ok":true,"result":{"fstab":"removed","services":[{"what":"dev.cowshed.storage system LaunchDaemon","outcome":"removed"},{"what":"cowshed.store System.keychain item","outcome":"removed"},{"what":"cowshed.caches System.keychain item","outcome":"already-absent"},{"what":"dev.cowshed.gateway agent","outcome":"removed"},{"what":"dev.cowshed.sccache agent","outcome":"already-absent"},{"what":"installed cowshed binary","outcome":"removed"},{"what":"installed sccache binary","outcome":"already-absent"}]}}
```

`outcome` is `removed` or `already-absent`; the stderr rendering of the same value reads `already absent`.

### `cowshed adopt`

Run once inside each existing checkout you want cowshed to manage. Adoption converts that repository into an
image-backed **main workspace** at the same path. A host may have any number of adopted repositories and therefore any
number of repository-scoped mains. Adoption is the only operation that copies the source tree into a new image.

On macOS, `cowshed adopt` and `cowshed setup` are the only commands allowed to create native storage. The first adopt on
a machine may display one administrator authorization prompt from `diskutil` while cowshed creates and mounts the
space-sharing `cowshed.store` and `cowshed.caches` APFS volumes. Once both volumes are present and correctly mounted,
later adopts only validate them and do not prompt.

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
next: cowshed new <name>
<project-root>
```

Workspace environment lives in the image at `.cowshed/env` and is rewritten on token rotation. The repository `.envrc`
is a two-liner that sources that file. Cowshed does not authorize direnv or devenv trust databases. Reattach a detached
session workspace with `cowshed attach`.

### `cowshed mv main --repo-id <owner/repo>`

Change the adopted project's repository identity without changing its Git remotes or copying the checkout. Cowshed moves
the store namespace and the session mount namespace, rewrites the repository binding, and moves every workspace's
store-side detached sidecar onto the new identity. The target identity is validated with the same `owner/repo` grammar
as adoption.

The binding records the identity it leaves as a former one, and every later identity check asks whether the project owns
the identity it is shown rather than whether that identity is the current one. That is what keeps the stamps this
operation cannot reach valid: a detached session's in-image marker, a workspace's CA certificate subject, and artifact
records already appended. A detached workspace's marker is brought onto the current identity the next time it is
attached.

The operation refuses if any live adopted project already owns the target identity — as its current identity or as one
it recorded holding — if a session workspace is attached, or if main is detached. Uniqueness is scoped to live projects:
retirement deletes the binding, so a retired project's former identities are free again.

The identity transaction writes a store-root intent before its first mutation and marks it `mutating` only once the
project's volumes are unmounted. Reopening any project completes a started transaction from what is on disk rather than
from how far the record says it got, so a crash leaves a project that finishes renaming, never one that is half-renamed.
Two namespaces both present, or a malformed intent, are refused by name instead of guessed at.

`--repo-id` is an identity override, not a remote check. `origin` may already point at the renamed repository, may still
use the old path, or may be absent; cowshed does not inspect or rewrite it.

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

A symlink the source left pointing outside its tree, such as a `bun install` `link:` dependency, is rewritten in the new
workspace to the absolute path it named in the source: `main`, or the `--from` workspace, whatever its depth. In-tree
relative links keep their bytes. A link whose target is missing from the source refuses the clone by name and rewrites
nothing; repair it in the source, then rerun the same `cowshed new` to resume.

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
`--all` discovers every validated `<store>/<owner>/<repo>/repository.json` and derives every workspace from its image,
detached sidecar, and current kernel mount facts. It never opens project checkouts or reads Git remotes, so a missing
direct-mounted main is reported as detached instead of aborting the store-wide list. Plain output adds `repoId` as the
first column and keeps projects contiguous:

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
3, and that workspace mounts at `<project-root>/.cowshed/slot-3` instead of `.../<name>`. When it is removed or renamed
the slot is released, and the next workspace to take slot 3 mounts at exactly the same absolute path.

Path identity was once the entire feature, because both halves of the compiler cache keyed on absolute paths. Neither
does any more:

- Cargo derived `-C metadata` and `-C extra-filename` from a package id carrying the **absolute manifest directory**, so
  a local crate compiled at two paths was two different compilations. From cargo 1.97 both are path-independent for
  workspace members (measured: identical hashes for one workspace checked out at two paths).
- sccache additionally hashes the compiler's **physical** working directory. The bundled build keys that, the blanket
  `CARGO_*` values and the argument bytes relative to the request cwd for a client that sets `SCCACHE_BASEDIR_CWD=1` —
  which every workspace does — and compiles such a request with the cwd remapped away, so the working directory rustc
  records in rmeta and debuginfo is workspace-relative.

So cross-path sharing is the default, and a slot is what is left for the cases those two do not reach. A unit whose
output still names its checkout — an `env-dep:` value (`env!("CARGO_MANIFEST_DIR")`, never normalized), a proc macro
that reads the manifest directory, an include recorded by absolute path — is found by reading the outputs before they
are stored, and is stored for that path alone: it hits again at the same path and fails closed at every other. Tooling
that persists absolute paths across tenant generations keeps working, and an unpatched sccache or a cargo older than
1.97 still needs the path to be identical. The table below is that older world, measured on this hardware with sccache
0.16 over a ten-crate workspace, second checkout of identical sources:

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

Every workspace child cowshed launches — `cowshed exec` and every supervisor-run command alike — gets `RUSTC_WRAPPER`
naming the sccache that `cowshed setup --sccache` pinned (the program the host daemon runs, by its store path, so a
repository shell that ships no sccache still builds), `SCCACHE_BASEDIR_CWD=1` and the cache endpoints
(`SCCACHE_SERVER_UDS`, `SCCACHE_DIR`). A host without a pinned sccache gets no wrapper. Name mounts are not excluded:
the bundled sccache normalizes the residual path-bearing key inputs against the request cwd, so sibling paths share
entries with each other. A slot buys the one input normalization cannot reach — cargo's `-C metadata`, a hash sccache
never sees — and same-path reuse of the units whose output names their checkout.

`CARGO_INCREMENTAL` is not set, at any mount and for any command, `cowshed land --check` included. Cargo decides it per
profile, which serves both halves of a build: workspace crates in `dev` and `test` stay incremental and local (a
one-line edit rebuilds in ~1.7s, against ~20-32s with incremental forced off), while their dependencies are always
non-incremental and reach the cache without anyone forcing anything. A land check therefore builds the same units an
interactive command builds; forcing `CARGO_INCREMENTAL=0` there would compile every workspace crate a second time.

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

`--force` stops that workspace's running jobs (TERM, then KILL after the grace period) before detaching; without it,
removal lets the supervisor finish its active jobs. A leftover `.git/AUTO_MERGE` alone is not an in-progress operation
and does not require `--force`.

A dirty tree is a tracked change, or an untracked file that is not hidden, not ignored by main's current gitignore
rules, and not held by main's checkout with the same content at the same path: an untracked file left in main arrives in
every clone and is not the workspace's work.

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

Bundle verification uses a temporary bare repository. Interrupting `rm --abandon` removes that repository before the
command exits; a retry revalidates any bundle already published.

`cowshed rm main --restore` is the reverse of `adopt`: it puts the pre-adoption checkout back and unbinds the project.
Plain `cowshed rm main` throws the warm main image away instead, and needs `--force`.

A restore refuses (exit 4) when main's head is in neither the retained checkout nor a remote ref, because restoring
would drop commits made in main after adoption. `cowshed rm main --restore --abandon` restores anyway, and first bundles
main's history beyond the retained head into the restored checkout's Git directory —
`.git/cowshed/abandoned-main-<tip>.bundle` for an ordinary checkout; the report names the path. `--abandon` on main
needs `--restore`. Recover from the restored checkout:

```sh
git fetch .git/cowshed/abandoned-main-<tip>.bundle HEAD:refs/heads/recovered-main
```

A restore interrupted after main was retired is finished by running the same command again from the checkout. A checkout
with no remote is still found then: removing main records the checkout's path in the project store first. The finishing
run reports no abandoned work; the bundle the interrupted run reported stays where it said.

Nothing on stdout — `rm` has no answer to give. With `--json`, the result is `{}`, or
`{"abandoned":{"head":…,"targetBranch":"main","targetHead":…,"unlandedCommits":9,"bundle":…}}` after an abandonment. For
main, `targetBranch` is the retained checkout's branch (`HEAD` when it is detached).

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

### Dev servers inside workspaces

On macOS, each workspace owns a **port block** — 64 ports when the workspace is created — and the gateway data plane
sits on the base port, with base+1 through base+size-1 as workspace service ports: 63 of them in a new block. A
sandboxed process can connect only to its own block, so a service set that needs more ports asks for them with
[`cowshed grant <name> --ports <N>`](#sandbox-grants). Linux allocates no port block: every workspace instead gets
private loopback in its own network namespace, so fixed service ports do not collide with siblings. Ordinary package
tools still use `http://127.0.0.1:7644/…`: exactly one controller-owned, non-signalable connector in that namespace
binds that address and forwards bytes only to the workspace's mounted `/run/cowshed/gateway.sock`. It holds no policy or
credentials and is not a general TCP/Unix-socket forwarder; the socket inode, namespace, and opaque token retain the
authority boundary. Detach or restore drains and kills it. Tools must use cowshed's platform-specific configuration
rather than assuming host-wide loopback.

On macOS, `.cowshed/env` and every job's environment carry `COWSHED_PORT_BASE` and `COWSHED_PORT_BLOCK_SIZE`: the block
is `base … base+size-1`. Its size is 64 for a new workspace, may be larger after a `--ports` grant, and older workspaces
may hold 16-port blocks, so read it rather than assume it. devenv offsets can derive from the block. Linux configuration
contains no block or sentinel values.

```
$ cowshed exec raven -- sh -c 'echo $((COWSHED_PORT_BASE + 1))'
40961
$ cowshed exec raven -- sh -c 'exec bun run dev --port $((COWSHED_PORT_BASE + 1))'
                            # open http://localhost:40961 in your browser
```

### Background work

A foreground `cowshed exec` shows the command's output as it is written, waits for the command however long it runs, and
exits with the command's status. `--timeout <dur>` stops waiting after that long: a command still running keeps running
under the workspace supervisor, and `cowshed exec` exits 103 with an error naming its job, because it has no output or
status of the command's to give. A command that finishes just as the timeout fires is waited for; `cowshed exec` prints
the rest of its output and exits with its code. `--background` prints the numeric job id and returns at once, exit 0,
naming on stderr how to reach the job. No CLI verb reattaches to a job; the cowshed API does (`worker("<ws>").job(<id>)`
waits for it, reads its logs or kills it — see [integrations.md](integrations.md)). `cowshed exec` accepts
`--session <name>` for a named session whose cwd and variables carry across calls; opening a name the workspace already
holds rejoins that session and never closes it.

Commands under the workspace `.envrc` run in a warm shell: the supervisor activates the `.envrc` once and starts each
command from that activated environment, so only the first command after the workspace shell's inputs change pays for
direnv/devenv evaluation. Those inputs are exactly what direnv itself watches — the `.envrc`, `devenv.nix`,
`devenv.lock`, anything named with `watch_file`. Edit one and the next command re-activates, showing direnv's output on
its stderr; every other command starts warm with only its own output. A `cd` or `export` inside one command never
reaches the next. The warm shells belong to the workspace supervisor, a process the cowshed daemon (`cowshed gateway`)
starts on the first command a workspace gets and keeps: every later `cowshed exec`, from any terminal or tool, runs in
the same warm shells, and a job keeps running and capturing after the `cowshed` that started it exits. The supervisor
retires after 30 minutes with no named session and no running job, and `cowshed detach`/`cowshed rm` retire it at once;
the next command starts another, which activates once. A `cowshed grant` or `revoke` takes effect for the very next
command, which gets a shell activated under the new grants; jobs already running keep the access they started with.

Interrupting `cowshed` itself — Ctrl-C, a closed terminal, SIGTERM — while it still runs a command ends that command and
every job it started and still runs: SIGTERM, then SIGKILL after a short grace. `cowshed` then dies by the same signal,
so a loop around it stops too. A job that already went to the background when the invocation returned is untouched, and
a signal the process inherited as ignored (`nohup`, a background job of a script) stays ignored.

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
and artifact reads resolve the canonical artifact independently of whether it is inline or spilled and preserve
arbitrary binary output without UTF-8 assumptions or response-size growth.

### `cowshed attach [ws] [--all]`

Mount detached session workspaces and print their mount path(s). `<ws>` attaches one session workspace. With no name, a
cwd inside a project checkout or session attaches that project's detached session workspaces. `--all` attaches every
detached session workspace store-wide. Mains are always mounted and are never attach targets.

Workspace environment lives in the image as `.cowshed/env`, rewritten on token rotation and sourced by the repository
`.envrc` two-liner. Sandboxed `cowshed exec` processes receive the cowshed-owned exports directly. Wiring is carried by
**files, not a CLI env printer**.

### `cowshed detach [ws] [--all]`

Unmount session workspace(s) and stop their supervisors without destroying anything. Detached workspaces cost one closed
file. `attach` and `path` bring them back. `<ws>` detaches one session, resolved from the store
(`<owner>/<repo>/sessions/<ws>.image` plus the image sidecar / marker identity) so it does not require cwd or git
discovery. `--project` still selects the project. `--all` detaches every attached session workspace store-wide. Mains
are always mounted and are never detach targets.

The all-session forms continue after one workspace or project cannot mount or unmount, naming the skipped target on
stderr; successful changes to other workspaces remain effective.

### `cowshed mount main --repo-id <owner/repo>`

Mount main for the named project and print its mount path. Resolution reads store records — the repository binding and
the checkout-path record — rather than a live git checkout, so it works from an empty stub directory left by a broken
workspace and never requires cwd or git discovery. The mount uses the gateway-canonical flags; a volume mounted with
other flags is remounted rather than refused.

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

### `cowshed defrag <name|main>`

Rewrite one workspace's image so its data is contiguous again, and print the extent count it ends with.

```
$ cowshed defrag main
6
cowshed: workspace main rewritten contiguously: 2110000 extents -> 6, 239059476480 bytes copied; a clone's first write now copies a map predicted at 72.0µs instead of 25.3s
```

`new` and `fork` clone an image in milliseconds because the clone shares the source's extent map, but the first write to
the clone copies that map, at about 12 µs per extent. Main fragments with every write it takes while clones share its
blocks, so a long-used main can hold millions of extents and make every new workspace wait tens of seconds inside its
attach. `cowshed doctor` reports main's extent count and the first-write cost it predicts as `main-extents`, and warns
with this verb as the hint once that cost reaches a second.

The workspace leaves the kernel exactly as it does for `resize`: a volume with a file open or a job running refuses
before the image is touched, so run it while the checkout is idle. The data is then copied with plain reads and writes
(never a clone, which would share the old extent map) into `<image>.defrag` beside the image, flushed, renamed over the
image, verified by attaching it, and mounted again if it was mounted. A copy that fails leaves the image untouched and
the workspace back on its mount.

The copy needs as much free space on the store volume as the image has allocated, checked before anything is detached,
and it keeps that space: clones and checkpoints taken earlier still share the old blocks, so the space comes back only
as they are removed. Main fragments again as it takes writes while new clones share it; rerun the verb when `doctor`
warns.

### `cowshed rekey <name|main>`

Rebuild one keyless workspace's CA identity and print its name. The quarantined grants sidecar is republished beside the
still-in-place image with the revision bumped by one; fresh credentials are minted into the live mount, so the workspace
must be mounted (a degraded mount is enough — the mount proof needs no CA). The quarantine entry is consumed when the
rotation completes. When the sidecar never left, the revision is preserved. Rotation invalidates in-flight job
certificates: they were signed by the lost CA generation.

```
$ cowshed rekey raven
raven
cowshed: workspace raven rekeyed at revision 8; quarantine entry <project>/quarantine/raven-1756944000 consumed
next: cowshed attach raven
```

## Sandbox grants

### `cowshed grant <name> [--read <path...>] [--write <path...>] [--deny-write <relative-path...>] [--deny <relative-path...>] [--egress <host>] [--opaque] [--ports <N>]`

Workspaces start **closed**: write access to their own volume, `/private/cowshed/caches`, and temp; read access to the
toolchains and system; egress to the localhost gateway only. Widen filesystem and network access per workspace:

```
$ cowshed grant raven --read <project-root>/reference-corpus
$ cowshed grant raven --write <project-root>/shared-assets
cowshed: grants for raven now: 1 read, 1 write, 0 denied writes, 0 egress
cowshed: grants apply from the next exec or shell
next: cowshed exec raven -- <retry your command>
```

- `--read` and `--write` are repeatable and each occurrence accepts one or more paths. Paths must be absolute; cowshed
  normalizes, deduplicates, and sorts them before persistence.
- A grant that contains or falls beneath the workspace mount, another cowshed mount, controller state, the project
  policy root, or a credential-bearing hard-deny is rejected before the grants file changes.
- Grants are recorded in `<image>.grants.json`, **outside the volume** — a sandboxed process cannot edit its own grants.
- A path is recorded under its resolved spelling; `grant` says so when that differs from what was typed. A symlink
  planted beside the workspace mount so `../<name>` resolves (`<shed>/<org>/<project>/<name>` pointing at a sibling
  repository) sits inside the mount-root deny, and the profile carves the link back as a readable literal exactly when
  its target is granted — grant the target, and the workspace reaches it through the link.
- Filesystem grants take effect at the next `exec`: Seatbelt profiles are fixed at process launch, and every launch
  carries the current persisted grant snapshot.
- `--deny-write` protects a path relative to the workspace root and everything below it. The final Seatbelt rule
  overrides the workspace's broad write allow; link creation and parent renames cannot bypass it.
  `cowshed grant --project-wide --deny-write .git/hooks .git/config` puts the same restriction on main and every current
  and future workspace. Workspace grants cannot remove a project deny. Set this from the trusted host, not an
  in-workspace job.
- `--deny` hides a path relative to the workspace root and everything below it: reads and writes there fail with EPERM,
  with the same link and rename protection as `--deny-write`. `--project-wide --deny .runtime` applies it to main and
  every workspace. Main's `.cowshed.toml` can declare it durably as `[sandbox] deny = [".runtime"]`; cowshed reads that
  section only from main's checkout, never from a workspace's copy.
- `--egress <host>` is repeatable and admits one host through the gateway, intercepted, on the default ports (443 and
  80). Network reach is a separate decision from filesystem reach and a separately auditable one: the gateway records
  every admission in its Arrow audit telemetry ([telemetry.md](telemetry.md)). Holding a credential for a registry does
  not grant reach to it, and granting reach does not hand the workspace a credential — see
  [`cowshed credential`](#cowshed-credential-addlsstatusrm).
- `--opaque` grants the invocation's `--egress` hosts as opaque tunnels instead: the gateway forwards encrypted bytes
  with host-only audit and no injection, and the client verifies the real certificate. It is for clients that cannot
  trust the workspace CA — certificate-pinning clients, and Go on macOS
  (`cowshed grant --project-wide --egress proxy.golang.org --egress sum.golang.org --opaque`). A host holds one rule, so
  granting it again restates its mode: `--egress <host>` alone turns it back to intercepted.
- Egress grants apply immediately: the gateway reads the current policy per request, with no re-exec.
- `--ports <N>` (macOS) grows the workspace's port block to hold at least N service ports, not counting the gateway port
  at its base. Blocks are power-of-two sized and aligned, so the block becomes the smallest one with N+1 ports:
  `cowshed grant raven --ports 80` turns the initial 64-port block (63 service ports) into a 128-port block (127). The
  count is a minimum and a block never shrinks: a count the block already holds, on its own, leaves the block, the grant
  revision, and the workspace's supervisor as they are, and `--ports 0`, a malformed count, and `--project-wide --ports`
  are usage errors.
  - cowshed prefers a larger block that contains the current one: it grows in place, keeping the base when that base is
    aligned to the larger size. When no free containing block exists, the workspace moves to a free larger block and
    keeps the block it moved from reserved to itself: a background process left behind by a finished job may still hold
    old-block ports, so no other workspace is handed them, and new jobs may still connect to them. The grant set lists
    such blocks as `retainedPortBlocks` (`--json`); they appear only after such a move, and no grant request can add
    one. A later growth whose block contains a retained block drops that entry without releasing any port; other
    retained blocks stay until the workspace is removed, when they are released like any retired block. The gateway
    endpoint and the advertised `COWSHED_PORT_BASE`/`COWSHED_PORT_BLOCK_SIZE` come from the current `portBlock` alone.
  - Growth, whether or not the base moves, needs an idle workspace: while any of its jobs is running the grant is
    refused at once, never queued or waited out. Stop those jobs, grant, then start them again — or better, grant ports
    before launching the services that need them.
  - The next exec starts a fresh supervisor under the new block: its jobs get the new
    `COWSHED_PORT_BASE`/`COWSHED_PORT_BLOCK_SIZE` and a Seatbelt profile that allows the new block and any retained
    ones.
  - Ports are a host resource, not a per-workspace quota: every workspace's block, current or retained, comes from the
    one reserved range (`32768–49151` by default, 16384 ports in all, so 256 workspaces at the initial 64-port size),
    and a workspace keeps its blocks until it is removed, detached or not. A block that cannot be placed because the
    range has no free aligned block that large is refused rather than waited for.
  - Linux refuses `--ports`: each workspace's private network namespace already gives it every loopback port.

  ```
  $ cowshed grant raven --ports 80
  cowshed: grants for raven now: 0 read, 0 write, 0 denied writes, 0 egress
  cowshed: port block for raven now: COWSHED_PORT_BASE=40960 COWSHED_PORT_BLOCK_SIZE=128 (127 service ports)
  cowshed: grants apply from the next exec or shell
  next: cowshed exec raven -- <retry your command>
  ```

- `cowshed grant <name>` with no flags prints the current grant set (TSV; `--json` for the envelope):

```
$ cowshed grant raven
read	<project-root>/reference-corpus
write	<project-root>/shared-assets
egress	registry.internal.example	443,80	intercept
```

## Authority boundaries

Project lookup is discovery-only. Workspace inspection may safely attach. A worker capability controls one workspace's
exec, shell, jobs, quota-limited checkpoints, push, and grant reads. Only a trusted coordinator may grant/revoke,
restore/destroy/rebase/land, run gc, or mirror repositories. The persistent per-workspace supervisor socket is
permission- and peer-checked, supports concurrent clients and reconnect, and is never unlinked merely because one client
disconnects.

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

Workspace git fetches from the `main` remote (main's repository, a mounted path), from the local clone an adopted
repository's URLs are rewritten to, and over HTTPS from a host the workspace holds an intercepted egress grant for —
fetch only: the gateway admits `git-upload-pack` and refuses `git-receive-pack`. No credentials or credential helpers
exist inside a workspace; pushing to real remotes (origin, GitHub) is coordinator work, done host-side with your normal
git setup.

### `cowshed push <name> [--branch <b>]`

Delivers the workspace branch to main's repository. Under the hood this is a _host-side fetch from the workspace mount_
— the trusted side runs git, so nothing inside the workspace (hooks, `.git/config`) ever executes outside the sandbox.
Never touches main's checked-out branch.

```
$ cowshed push raven
refs/cowshed/raven/heads/cowshed/raven	9b2e77d4c1…
```

Stdout is the preserved ref and the commit it names. Nothing preserves a workspace's work but `push` and `land`: the
store volume that holds the images is excluded from backup (durability = git).

### `cowshed rebase [<name>] [--onto <rev> | --into <lane>]`

Brings the workspace branch up to current main: cowshed points the workspace's `main` remote at main's checkout
(`cowshed-main` in a workspace where something else already holds that name), fetches it, and runs
`git rebase --no-autostash main/main` in the workspace; `--onto` names another revision. `--into <lane>` rebases onto
what the workspace lands into instead: the branch the lane base `<lane>` has checked out, fetched from its mount, so a
unit catches up with what its lane-mates have landed there. `--onto` and `--into` together are refused (exit 2). A dirty
tree is refused before anything moves (exit 4: commit or discard the uncommitted work first). Autostash stays off even
when the repository's config turns it on, because a stash re-apply that conflicts leaves unmerged paths behind and still
exits 0. A conflict aborts the rebase, restores the branch to the commit it had before, and exits 4 with git's output
naming the conflicted paths.

### `cowshed land <name> [--into <lane>] [--check <cmd>]`

The full close-out in one primitive: validate (`--check`) inside the sandbox, fast-forward the target's checkout to the
workspace's branch, start the target's warm step, retire the workspace. The target is main, or with `--into <lane>` the
lane base `<lane>`. A dirty tree, read the way `rm` reads it, is refused before the check runs: the check sees the
working tree but only the commit lands. Land does not rebase; when the target has moved past the workspace's base the
fast-forward is refused, and `cowshed rebase <name>` (with the same `--into`) is the next step. Any failing step before
the fast-forward exits 4 with the workspace intact. A retire refused after the fast-forward keeps the workspace and
exits with that refusal, whose message starts with what landed, so the retry is `cowshed rm`, not another land. The
retire measures the workspace against the branch it just landed on, so a lane unit retires once its lane base holds its
commits. The target branch (`--target`, default the branch the target's checkout has checked out) must be the
checked-out one. `--into` refuses the workspace itself (exit 2). `--no-retire` keeps the workspace; `--push-only` stops
after validation for review-gated flows.

**The warm step.** A clone starts warm because its source's build outputs copy with it, so after the fast-forward land
builds the target at what landed, with the argv the target's `.cowshed.toml` declares:

```toml
[land]
warm = ["tooling/warm-main"]   # one argv, never a shell string
```

It runs as a background job of the target's workspace supervisor, from the target's root, with `COWSHED_LAND_BASE` (the
target's head before the land) and `COWSHED_LAND_HEAD` (the landed head) in its environment. Land does not wait for it:
it prints the job on stderr and returns. At most one warm job runs per target; lands that arrive while one runs coalesce
into the one run waiting behind it, from the oldest waiting base to the newest head, which starts when the running job
ends. Landing a unit into a lane base warms the lane base, so units forked later in the lane start with their
lane-mates' work built.

```
$ cowshed land raven
main	<landed head>	true
cowshed: main's warm step builds <main's previous head>..<landed head> as job 12
```

`--json` carries it as `warm`: `{ "state": "started", "jobId", "range": { "base", "head" } }`, or `"queued"` with
`behind` naming the running warm job. The job's record (`JobInfo.warm`, and the protected records' `warm_base`,
`warm_head`, `exit_code` and `duration_ms` columns) says which landed commits it built, how it exited and how long it
took. No `[land] warm` starts nothing.

## Time travel

### `cowshed fork <src> <dst>`

Clones a _running_ workspace — two divergent futures from the same mid-flight state, in milliseconds. Grants are **not**
inherited; forks start closed.

Escaping symlinks are resolved against `<src>` before Git or environment preparation, including when the source is
`main`. In-tree links keep their relative bytes; a refused source target writes no link changes, so the same fork can
resume after its source is repaired. This repair does not change the fork's inherited branch or remotes.

### `cowshed checkpoint <name> [label]` / `cowshed restore <name> <label>`

Checkpoint clonefiles the workspace image (crash-consistent, fsck-verified) under a label — generated from the UTC
timestamp when you don't give one. Before publication, a supervisor barrier seals complete Arrow batches and spill
files; a manifest commits every checkpoint-resident job byte. Recovery may discard only incomplete trailing data.

Restore swaps the current image for the checkpoint (detach → clone → reattach, ~500 ms) and mints a new workspace
incarnation. Protected content remains authoritative for the restored snapshot's origin boundary; the restored marker
records the lineage, and the controller's audit record of the restore carries the hashes. Restore refuses over unsaved
work (exit 4); the displaced image is kept as a `pre-restore-<timestamp>` checkpoint, so a restore is itself undoable.
List checkpoints with `cowshed ls --json`.

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
copies the running executable there when the bytes differ, whatever volume that executable came from. A build inside a
workspace or the nix store is copied rather than refused — the copy is precisely what makes the agent independent of a
path that only exists once cowshed has mounted it. `stop` boots out the agent and removes the plist, leaving the
installed binary — that copy is host state rather than agent state, and keeping it makes the next `start` a plist write
instead of a fresh multi-megabyte copy. `stop --purge` deletes it too, for a host that is done with the gateway rather
than pausing it; `cowshed setup --uninstall` removes the system storage daemon, both user agents, and both installed
binaries at once. All of these are idempotent, and a `--purge` with nothing installed says so rather than failing.

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

`gateway run` is the LaunchAgent's internal foreground entrypoint. It validates already-mounted host storage and creates
none, restores every authoritative attached workspace session, and drains on SIGTERM or SIGINT. Ordinary `exec`,
`attach`, and `doctor` commands reconcile the current project's attached sessions before admission; lifecycle commands
reconcile again before reporting success. If the service is absent they fail with exit 5 and the exact
`launchctl kickstart -k gui/<uid>/dev.cowshed.gateway` next hint.

### `cowshed credential add|ls|status|rm`

A workspace reaches a private registry through the gateway, which attaches a credential **the host holds** and strips
whatever the client sent; nothing inside a sandbox ever receives the secret. This verb is how the host comes to hold
one. It is per project — a record is bound to the discovered `repo_id` — and it is host-side: a workspace manifest
cannot ask for a credential.

```
$ export REGISTRY_READ_TOKEN=<token>
$ cowshed credential add --origin https://registry.internal.example \
    --path-prefix /api/packages/acme/npm/ --secret-env REGISTRY_READ_TOKEN
cowshed: enrolled https://registry.internal.example:443 for acme/site with scope /api/packages/acme/npm/
cowshed: REGISTRY_READ_TOKEN is now withheld from every child of this project
next: cowshed grant <workspace> --egress registry.internal.example
https://registry.internal.example:443  credential installed
cowshed: scope: /api/packages/acme/npm/
cowshed: withheld from every child: REGISTRY_READ_TOKEN
```

- The secret is **named, never typed**: `--secret-env <NAME>` reads a variable from this shell,
  `--secret-command '["op","read","op://vault/item/field"]'` runs a helper directly with no shell, `--secret-stdin`
  reads a pipe. A non-empty named variable wins and the helper is not run. There is deliberately no flag carrying the
  credential itself — argv is visible to every process on the host and lands in shell history.
- `--origin` is host and port only (the port defaults to 443, and is always explicit in the stored record). The
  registry's namespace belongs in `--path-prefix`, which is repeatable and **required**: a credential admitted for a
  whole host is not a scope, and a bare `/` is refused. `--method` defaults to `GET` and `HEAD`, which is what an
  intercept grant admits.
- Enrolment records the environment variable NAME it read in host state, and every job in the project then has that name
  withheld from its environment — including the second `exec` in a long-lived named session. A credential the gateway
  holds has no business also riding into the sandbox.
- `ls`/`status` report each route, the scope it was enrolled with, the names withheld, and whether a usable credential
  is installed. They never print the secret, and never a digest of it. A record that no longer decodes, or whose scope
  could never match a request, reports as missing rather than silently never attaching.
- Enrolling the same origin again is rotation, and is the supported path: the record is replaced with no window in which
  the credential is absent. `rm --origin <origin>` forgets one.
- Network reach stays separate: `cowshed grant <ws> --egress <host>` is what lets a workspace reach the registry at all,
  and it is audited on its own.
- On Linux the store is read-only by construction — the service manager supplies records through
  `$CREDENTIALS_DIRECTORY` — so `add`/`rm`/`status` say that, with the mechanism named, instead of reporting a success
  that wrote nothing.

### `cowshed identity add <remote>`

A dependency can name a repository by more than one URL — the forge it is pushed to as well as the host it was adopted
from. Workspaces fetch every URL their project bindings name from the local clone instead of the network (the generated,
ignored `.cowshed/git-fetch.inc` every child includes), and this verb is how a binding gains another URL: it binds a
remote the project's main checkout already configures as an additional, non-primary identity.

```
$ cd ~/src/widget && git remote -v | grep forge
forge  ssh://git@forge.example.test:2223/acme/widget.git (fetch)
$ cowshed identity add forge
cowshed: bound remote forge (ssh://git@forge.example.test:2223/acme/widget.git) as acme/widget of acme/widget
acme/widget  primary  origin  https://github.com/acme/widget.git
acme/widget  bound  forge  ssh://git@forge.example.test:2223/acme/widget.git
cowshed: workspaces that can read this checkout fetch these URLs from it from their next exec
```

- Routes come only from bindings, never from a checkout's remotes: a workspace can write its own `.git/config`, and a
  remote it adds must not redirect another repository's URL. A URL another adopted project already binds is refused,
  because a fetch URL routes to exactly one clone.
- An SSH URL is recorded with its login name and routes both with and without it (Cargo pins
  `ssh://git@forge.example.test:2223/…`, uv drops the login); an HTTPS username or password is never recorded.
- The workspace still needs read access to the checkout (`cowshed grant --project-wide --read <checkout>`); routing
  grants nothing.
- The remote must stay configured under the same name, like the adopted one: opening the project refuses a bound remote
  that was removed or now names another repository, and follows one that moved servers. Binding a remote that is already
  bound reports the binding unchanged; `--json` returns `{ repoId, added, identity, identities }`.

### `cowshed sccache start [--capacity <size>]` / `stop` / `status`

The gateway daemon starts this agent itself, so a healthy host already has it: `run_daemon` repairs/reattaches every
project's mounts and then the compile cache. A host without sccache on PATH logs one line and serves normally. The verbs
are for repair, inspection, and resizing.

`start` installs and loads the per-user macOS LaunchAgent `dev.cowshed.sccache`, then waits until the server answers on
its unix socket at `/private/cowshed/store/sccache.sock`. The mode-0600 plist runs the _sccache binary itself_ — a copy
at `~/Library/Application Support/dev.cowshed/bin/sccache`, installed by `start` from the sccache it resolves on the
invoking shell's PATH, so run it from a shell with the devenv/nix sccache available — as a foreground unix-socket
server: `SCCACHE_START_SERVER=1` selects server mode, `SCCACHE_NO_DAEMON=1` keeps it under launchd supervision,
`SCCACHE_IDLE_TIMEOUT=0` disables idle exit, and `SCCACHE_DIR` pins the shared store at
`/private/cowshed/caches/sccache`. Stderr lands at `~/Library/Logs/cowshed/sccache-stderr.log`. `stop` boots out the
agent and removes the plist; both operations are idempotent. The copy is what keeps the daemon alive across a devenv
update or nix garbage collection: an sccache upgrade is picked up by rerunning `cowshed sccache start`, which recopies
on byte drift and rewrites the plist only on drift.

Cross-path Rust reuse — every workspace hitting one cache regardless of its mount path — requires an sccache that
carries `nix/sccache/sccache-rust-basedir-cwd.patch`. It extends `SCCACHE_BASEDIRS` normalization to the Rust hasher and
honors the per-request `SCCACHE_BASEDIR_CWD=1` client variable cowshed exports in every workspace, keying the cwd, the
blanket `CARGO_*` environment values, and the argument bytes relative to the request cwd. Values rustc records as
`# env-dep:` are never normalized. A normalized key is only as good as the claim that the output does not depend on the
path it omits, so the patch makes that claim true and then checks it:

- such a compile runs under `--remap-path-prefix=<cwd>=`, so the working directory rustc writes into rmeta and debuginfo
  is workspace-relative and identical at every mount;
- before the entry is stored, its outputs (and the dependency list of its dep-info) are searched for every normalized
  path. An output that names one — a proc macro that read `CARGO_MANIFEST_DIR` through `std::env`, which dep-info cannot
  see, or an `env!` value — is stored under the verbatim per-path key, with a marker under the normalized key that sends
  later lookups to their own per-path key. That checkout keeps reusing it; no other checkout is ever served it.

Cargo's own `-C metadata` is path-independent for workspace members from cargo 1.97. An unpatched sccache still serves
same-path (slot-tenant) reuse, nothing more. Prove the build with `strings sccache | grep cowshed-path-v2`.

Concurrent misses of one cache key wait for the first compile (`patches/sccache-singleflight.patch`; prove with
`nm sccache | grep inflight_join`). Without it, parallel `cargo` processes compile the same crate N times.

Two more variables are in that plist because sccache reads them once, at server start, and no client can supply them:

- `SCCACHE_CACHE_SIZE` — the cap. sccache's own default is 10 GiB, which is smaller than one debug graph of a project
  cowshed hosts, so the default evicts the entries a second slot tenant came for. The derived default is the summed
  allocated size of every adopted project's `main` image, floored at **40 GiB** and rounded up to a whole gibibyte;
  `--capacity 120g` overrides it (same size grammar as `cowshed adopt`/`resize`).
- `SCCACHE_BASEDIRS` — **plural**; set to the store root. With the patched binary it also participates in Rust key
  normalization; the per-request cwd from `SCCACHE_BASEDIR_CWD` is what makes workspace paths interchangeable.

The agent is launchd `ProcessType` **Interactive**. Background is Darwin background QoS (PRI 4) and Standard is the
utility band (PRI 20); both throttle the agent's IO behind the host's. sccache hashes every miss and runs rustc as its
own child, both at the agent's priority, while every wrapped `sccache rustc` client stays interactive (PRI 31), so
either clamp turns the shared daemon into a niced compile queue. The gateway agent is Interactive for the same reason:
its supervisor manager starts every workspace supervisor, and so every shell host and `cowshed exec` job, and a
descendant cannot leave its agent's QoS band.

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

Workspaces reach the daemon through `SCCACHE_SERVER_UDS` (supervisor-injected, `.cowshed/env`-exported, and carried by
the cargo `[env]` guidance); the Seatbelt profile admits exactly that socket and keeps the sccache store
daemon-write-only. `sccache --show-stats` works from any shell with the export set — it speaks to the same server.

A client with no export set at all is `cowshed setup`'s business rather than this verb's: it reads sccache's own config
file, which `setup` writes and owns (see [`cowshed setup`](#cowshed-setup---uninstall---force---mount-root-dir) above).
`sccache --show-stats` run from such a shell — no `SCCACHE_DIR`, no `SCCACHE_CONF`, outside every workspace — is the
check that the file took effect: `Cache location` must read `Local disk: "/private/cowshed/caches/sccache"`. It reports
the resolved configuration without starting a server, so it is safe to run against a live host.

### `cowshed controller`

Serves one coordinator controller connection, for the project selected by the cwd or `--project`, to the embedding
process that started it with one end of a Unix socketpair as its standard input. A program that links cowshed is another
cowshed build, and the daemon starts workspace supervisors only for its own, so such a program runs its controller as
this command of the host's `cowshed` and speaks the controller protocol on its end of the socket
([integrations](integrations.md#embedding-the-controller)). Nothing is written to stdout; the command exits 0 when the
embedding process closes its end. A terminal or a pipe on standard input is refused with exit 2 before the project is
resolved. The project's gateway sessions are reconciled before each exec, shell and checked land.

### `cowshed gc`

Deletes retired and orphaned images and stale mountpoint dirs, prunes expired checkpoints, and reports what it freed.
Safe to run anytime; `rm`, `land`, and `restore` also run it opportunistically.

An image under `sessions/` without its grants sidecar is skipped by other commands with a named warning; `doctor`
reports it, and `gc` can reclaim it as an orphan after mount and lock checks. `gc` keeps going when an individual
candidate cannot be reclaimed, names it as deferred on stderr and in the `deferred` JSON array, and reports only space
actually freed by successful candidates.

### `cowshed doctor`

Invariant checks: every image has a marker, every mount matches an image, grants files parse, caches volume and gateway
reachable, and git identity at the workspace mount root matches the checkout (`includeIf gitdir:` files that would not
apply under the mount root). Exit 0 when healthy; otherwise the code of the most severe finding (3/4/5). Stdout is
`healthy` or `unhealthy`. Stderr is every finding, then the distinct `next:` commands those findings carry — findings
first, hints after, never interleaved.

Store-wide listing and diagnosis continue past an unreadable project's records: listing warns with the project name, and
doctor records a finding for it while checking other projects.

If a selected project's checks cannot run, doctor records an error finding named `project-checks-skipped`; it never
turns missing evidence into `healthy: true`.

Opening the project for its checks finishes nothing, unlike every other command's open: no unfinished lifecycle
operation is replayed (`main`'s included), no interrupted publication or restore is completed, no retired image is
reclaimed, no repository identity change is finished and no moved remote URL is written to the binding. Doctor reports
each instead — `unfinished-intent`, `interrupted-publication`, `pending-publication`, `retired-trash`,
`identity-change-unfinished` and `binding-moved` — and the next command that opens the project finishes them.
`cowshed doctor --repair` opens the project the way every other command does.

`cowshed doctor --repair` handles one narrow, mechanically provable failure: valid artifact frames whose job sequence
numbers are not strictly increasing because concurrent writers raced. It first validates every frame header, length
complement, SHA-256 digest, trailer, Arrow payload, record invariant, and checkpoint prefix. If anything except sequence
ordering is invalid, it refuses without replacing the log. Otherwise it writes a byte-for-byte
`records.arrow.pre-repair-<digest>` backup beside the log, resequences the records in physical append order, recomputes
checkpoint prefix digests, atomically replaces the log, verifies it again, and then runs the ordinary checks.

```
$ cowshed doctor
unhealthy
cowshed: [error mount] cowshed.store: present, not mounted; expected /private/cowshed/store
cowshed: [error gateway-down] gateway: launchd loaded; control socket does not answer
next: cowshed setup
next: cowshed gateway stop && cowshed gateway start
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
