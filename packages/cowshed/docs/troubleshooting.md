# Troubleshooting

First move for anything weird: `cowshed doctor`. It checks every invariant (images ↔ markers ↔ mounts ↔ grants, gateway,
a retired caches volume still present) and prints one `cowshed:` line per problem with a `next:` fix. Because cowshed
has no database, doctor isn't reconciling state — it's _deriving_ it from disk and the mount table, so what it reports
is the truth.

## Mounts

**A setup mount probe runs out of budget or cannot start.** Its error preserves the named command, admission/execute
budget and actual termination outcome. An admission wait, spawn failure or child deadline is not proof that Disk
Arbitration is wedged. Setup propagates that failure; it does not signal or restart a system daemon on that evidence.

**Workspace missing after reboot.** Mounts don't survive reboots; images do. Reattach explicitly:

```sh
$ cowshed attach
cowshed: attached acme/widget/raven
```

An unmounted main workspace exposes an empty mountpoint, not an auto-attach `.envrc`. Cowshed does not create or rewrite
repository shell hooks. After attaching, a project that uses direnv interactively can authorize its own hook with
`direnv allow`; sandboxed exec uses a private approval store. The login LaunchAgent may attach permanent workspaces
proactively, but explicit `cowshed attach` remains the recovery command.

**Finder ejected a volume** (or `diskutil eject` by hand): use the same explicit `cowshed attach` recovery.

**direnv says `.envrc is blocked` in a workspace.** This is expected until that clone path is authorized. Run
`direnv allow`; `cowshed attach` repairs mounts but deliberately does not change trust.

**A command does not see an edited shell file.** A warm workspace shell re-activates when a file direnv recorded as an
input changes. A file your `.envrc` reads without telling direnv (a sourced script, a lockfile an install step uses) is
not on that list: add `watch_file <path>` to the `.envrc`, and edits to it re-activate the next command.

**`cowshed adopt` or `cowshed push` refused with exit 4 naming files.** The secrets gate found credential-shaped content
(`.env*`, key files, known token prefixes, `.envrc` secret exports). For adopt: move each value into the gateway
Keychain (see gateway.md) and delete the file, or `cowshed adopt --quarantine` to relocate findings outside the image so
dependent tooling fails loudly. For push: the offending hunks are named — remove the secret and push again.

False positive on adopt? The refusal itself prints the exact controller-owned `waivers.json` path and a valid entry
example. Entries form a JSON array whose `path` matches the finding's repository-relative path exactly and whose
`reason` must be non-empty; malformed, blank-reason, or duplicate entries fail closed with the same pointer. Waivers
exist only for intentionally committed synthetic or public detector fixtures that can never hold live credentials —
never for live, temporary, copied, developer-local, deployment, or recoverable credentials — and a waiver suppresses
blocking while every waived finding remains retained for audit.

**Attach fails.** `cowshed doctor` distinguishes: image/dataset missing, image verification failure on macOS, occupied
mountpoint, or Linux attachment wiring failure. On Linux an attachment is healthy only when its private netns contains
exactly one trusted connector bound to `127.0.0.1:7644` and that connector can open the mounted per-incarnation
`/run/cowshed/gateway.sock`. `attach` recreates missing runtime wiring; it never invents a Linux `portBlock`.

**Repository identity conflict.** cowshed records the selected remote URL and its normalized lowercase `owner/repo`
`repo_id`. If the URL no longer normalizes to that identity, open fails instead of mounting another repository's data.
Fix the remote or explicitly select/rebind the intended identity; discovery only proposes candidates. Local-only
repositories must be adopted with `--repo-id owner/repo`. Moving a checkout is not a conflict. Trusted policy is read
only from `/private/cowshed/store/<owner>/<repo>/policy.json`, never from the checkout.

**An abandoned project's checkout or marker is gone, so ordinary removal cannot open it.** Use
`cowshed forget <owner/repo> --abandon` only when you explicitly intend to stop serving that exact registration. It
records positive abandonment authority, drains owned supervisors, refuses busy images without forcing, and preserves the
complete project store tree at the reported `.retired-projects/<operation-id>` archive before unbinding. Retry the same
command after a refusal; startup also resumes already-authorized records. A missing fixture marker or PID is not
permission to retire anything. This frees live mounts/registration ownership, **not the retained disk bytes**. It does
not delete the missing checkout's siblings or substitute for normal fixture resource-retirement proofs.

**`cowshed resize` or `cowshed defrag` says the image `is still attached as /dev/diskN with nothing mounted`.** The
kernel still holds the workspace's image, but nothing is mounted at its mount point — an attachment an interrupted
process or an outside tool left behind. Neither verb can grow or rewrite an image the kernel holds (the image file stays
exclusively locked while attached), and that attachment is not the workspace mount's to release, so the verb refuses
before touching anything. Device names are reused: confirm that the named device still belongs to this exact image
before manually running `hdiutil detach /dev/diskN`, then retry the verb. A resize that fails after a mounted workspace
was detached puts it back on its mount.

## Sandbox denials (exit 6)

When cowshed reports exit 6, it comes with the diagnosis and the fix on stderr:

```
cowshed: sandbox denied file-write <external-path>/gen.lock
next: cowshed grant raven --write <external-path>
```

Exit 6 is only ever reported on authoritative evidence: egress denials always (the gateway logged the decision),
filesystem denials when the kernel sandbox telemetry can be correlated to your command, and EPERM on one of cowshed's
own storage paths (see the first case below). A denial deep in a child process may instead surface as the child's own
nonzero exit, passed through unchanged — when a failure smells like the sandbox but there was no exit 6, check the raw
Seatbelt log around the failure:

```sh
log show --last 2m --predicate 'sender == "Sandbox"' | grep deny
```

and the gateway audit events for egress (Arrow segments under `/private/cowshed/store/telemetry/gateway/`,
[telemetry.md](telemetry.md)). Common cases:

- **A lifecycle verb (`fork`, `rm`, …) exits 6 with "the process running cowshed is sandboxed away from cowshed's
  store"**: the shell that ran cowshed — an agent harness shell, or a `cowshed exec` child calling cowshed again — is
  itself sandboxed, and the kernel answered EPERM on a store path (such a sandbox typically lets the store's existing
  lock files open but refuses the temp file every durable write starts with). The store is intact and no grant helps:
  rerun the verb from a shell whose sandbox allows writing the store.
- **Tool writes to `$HOME` dotfiles** (some CLIs insist on `~/.toolrc`): grant narrowly (`--write ~/.toolrc`, not
  `--write ~`), or set the tool's env override to a path inside the workspace — fix its config once with `cowshed exec`;
  it's in the image and every fork inherits it.
- **A tool reads something under `$HOME` and gets "Operation not permitted"**: reads under `$HOME` deny by default.
  System paths (`/usr`, `/System`, `/Applications`, `/nix/store`, `/opt`) stay readable; under `$HOME` a sandbox reads
  only its own workspace, the shared tool caches (`~/.cargo` registry/git, `~/.bun/install/cache`, `~/.cache/uv`), the
  Nix profile links (`~/.nix-profile`, `~/.local/state/nix/profile`) with each generation link they resolve through
  under `~/.local/state/nix/profiles`, the sccache GC root, and what `--read`/`--write` grants name. Point the tool's
  config at a path inside the workspace, or grant exactly the file it needs (`--read ~/.toolrc`, never `--read ~`).
- **A workspace cannot read main's checkout**: by design. Main mounts at your original checkout path, and every other
  workspace denies it exactly as it denies a sibling workspace; jobs in main itself still read it. Land or fetch what
  you need instead of reading main's files.
- **Egress to an unmirrored host**: `cowshed grant <ws> --egress <host>`; see the
  [grant-file contract](../../../specs/cowshed/04_sandbox.md#grant-file-imagegrantsjson).
- **Exec rejects an invalid recorded egress host**: inspect `cowshed grant <ws>`, then run
  `cowshed revoke <ws> --egress api.example.com:443`. If access is needed, run
  `cowshed grant <ws> --egress api.example.com`. For a project-standing key, use `--project-wide` in place of `<ws>`.
  The [widening and narrowing contract](../../../specs/cowshed/04_sandbox.md#widening-and-narrowing) owns recovery.
- **Linux package/proxy client gets connection refused at `127.0.0.1:7644`**: do not point it at the Unix socket or a
  macOS block base. Run `cowshed attach`; `doctor` distinguishes a detached workspace, absent/dead connector, missing or
  wrong per-incarnation socket mount, and a dead host gateway. A healthy workspace uses
  `http://cowshed:<token>@127.0.0.1:7644` in `HTTP_PROXY`/`HTTPS_PROXY` (plus lowercase forms), the token as userinfo.
  Detach and restore intentionally drain old connections; retry only after the new attachment is admitted.
- **407 versus 403**: 407 means the endpoint/credential pair did not authenticate — most often proxy variables that lost
  their userinfo, or stale pre-restore wiring. A client without the credential gets one 407 with
  `Proxy-Authenticate: Basic realm="cowshed"` and stops; cargo instead reads a bare tunnel failure as a spurious network
  error and grinds through its retry ladder, so a cargo command that hangs for minutes on `CONNECT tunnel failed` is a
  credential problem, not a slow network. 403 means endpoint and credential authenticated but policy denied the
  destination; use the gateway's grant hint. Port 7644 by itself is not workspace identity: the private netns plus
  mounted socket inode selects the workspace.
- **`go` denied writing under `~/go`**: a Go project shares exactly `~/go/pkg/mod` (and the user cache directory's
  `go-build`); `GOPATH`, and with it `go install`'s binaries, stays at Go's default under the private `HOME`. A write
  anywhere else in the host's `~/go` means something passed the host `GOPATH` explicitly. Find that setting; never grant
  `~/go`.
- **`nix` fails on `/etc/nix/<file>` with "Operation not permitted"**: the sandbox answered a config stat with EPERM
  instead of ENOENT (Determinate Nix probes an optional `sentry-endpoint`). Workspace profiles grant read-only metadata
  on `/etc/nix` — both spellings, since `/etc` resolves to `/private/etc` — and on nothing else in `/etc`, so a cowshed
  that still shows this predates that rule. Never grant `/etc` to work around it.
- **Denial persists after a grant**: filesystem grants apply from the _next_ exec; a long-running process (watcher, dev
  server) keeps its launch-time profile. Restart that process.

## Artifact integrity (exit 7)

Exit 7 means protected content is missing, mutated, rolled back, or written by an incarnation outside the workspace's
lineage for `(repo_id, workspace_incarnation, job_id)`. It is not a child exit, sandbox denial, or summary mismatch, and
retries or grants do not repair it. Preserve the workspace/checkpoint and follow the `cowshed doctor` integrity report;
cowshed fails closed rather than choosing the caller-visible redirect source, a publication copy, or whichever record
looks newer.

An `artifact ordering failure` is different: every frame may still be complete and readable while two independent
writers allocated the same sequence. Run `cowshed doctor --repair`; it repairs only that fully validated ordering case
and preserves the exact original beside the log. A digest, trailer, Arrow, record, or checkpoint-prefix failure is
content corruption and the repair refuses to rewrite it.

**Checkpoint was not pruned.** GC keeps the union of three sets: explicit pins, every checkpoint younger than 14 days,
and the newest five checkpoints per workspace. A user label and `cowshed checkpoint --keep` both pin; age or count does
not override a pin. Unpin explicitly before expecting GC to remove it.

## Disk usage

Images are sparse files that grow with churn; deleting files inside a volume does not shrink the image file. Sparse
creation reserves no host space for capacity (01a_apfs.md, "Capacity and allocation"). Guest-freed space is not returned
automatically: in the RAW measurement, a deleted 1 GiB payload kept its host allocation after guest sync, image fsync,
clean unmount and detach. `hdiutil compact` does not support RAW. To reclaim the space, run a verified re-adoption
(`cowshed adopt <checkout> --re-adopt`, 02_workspaces.md): the filesystem is copied into a fresh sparse RAW image,
verified, and the old image is kept until you remove it after verification. The host frees the old blocks only after the
retained image and every clone or checkpoint sharing them are released.

`cowshed gc` reclaims retired images, removes orphans, and prunes expired checkpoints (`--dry-run` lists each candidate
first):

```
$ cowshed gc --dry-run
44023414784
cowshed: would delete /private/cowshed/store/acme/widget/sessions/.trash/fox-3f2a….dmg (18203238400 bytes; reason: workspace was retired)
…
cowshed: dry run examined 12 objects; 4 candidates, 44023414784 bytes deletable
$ cowshed gc
44023414784
```

A main that has taken years of writes under clones is slow to clone, not large: `cowshed doctor` reports `main-extents`
and names `cowshed defrag main` when the next `fork` would pay for it.

Attribution: `du` on the images directory tells you per-workspace cost; _inside_ a mounted workspace, normal `du` works
— it's just APFS. Remember clones share extents: ten fresh workspaces cost ~zero until they diverge, so "sum of image
sizes" overstates real usage. `df -h /private/cowshed/store` covers the store volume. The shared tool caches are
ordinary directories at each tool's default in your HOME, so `du -sh` on one (`~/.cargo/registry`,
`~/.bun/install/cache`, `~/Library/Caches/Mozilla.sccache`) attributes it; both share the container's free space with
everything else.

Cargo's shared writable caches are its own `~/.cargo/{registry,git}`; gateway-owned bare repository mirrors are separate
at `~/Library/Caches/dev.cowshed/repo-mirrors` and are never reachable from a sandbox.

**A workspace rebuilds every dependency its copied `target/` already holds, refetches a crate the host has, or its
`bun install` relinks all of `node_modules`.** Cargo fingerprints a registry or git dependency by the absolute path of
its source under `$CARGO_HOME`, and Bun's isolated linker writes its cache path into every `node_modules/.bun` link, so
every checkout must use one literal path for each. Every sandboxed child of a project that uses the tool already gets
the host's own default — `CARGO_HOME=~/.cargo`, `BUN_INSTALL_CACHE_DIR=~/.bun/install/cache`, `UV_CACHE_DIR=~/.cache/uv`
— and a caller's own value for these never reaches it. The other side of the comparison is what differs: a host shell
that sets one of these variables to another path built main against that path. Unset it, or warm main through
`cowshed exec main --`.

**Nix cache/state.** A Nix project's sandbox uses the host's own `~/.cache/nix` and `~/.local/state/nix`: the supervisor
links the private `$XDG_CACHE_HOME/nix` and `$XDG_STATE_HOME/nix` to them, so nothing on the host is relocated and a
declarative configuration has nothing to own. A home-manager, NixOS or nix-darwin module that still links either path
into the retired caches volume is a conflict `cowshed setup` names, with the module option to remove, and leaves alone;
it never rewrites module-owned paths. Remove that option, switch, then rerun `cowshed setup`.

## Path-sensitive caches (why a fresh workspace rebuilds more than expected)

Cargo keys a workspace crate on its path relative to the package, so a workspace at
`<mount-root>/<owner>/<repo>/<workspace>` finds the units main built fresh (measured: a dev build of a mid-size crate in
a fresh clone, 208 of 208 units fresh, the incremental workspace crates included). When a fresh workspace still
rebuilds, run the build with `CARGO_LOG=cargo::compiler::fingerprint=info` and read the `dirty:` reason of the first
unit that rebuilt:

- **No fingerprint at all** — the selected seed never held that unit (for example, another Cargo profile or toolchain),
  so the fork builds that delta once. If the target built it after freezing its seed, the next fork refreshes
  automatically unless a named holder refuses writer exclusion. Close that holder and fork again; no manual seed-refresh
  or warm step exists. `cowshed reseed` only discards a workspace's build state for an existing seed clone, so it cannot
  make that seed warmer and may require a cold rebuild. `doctor` reports target-only `seed-age`, `seed-tree-behind` and
  `target-moved-outside-land`; after an external Git fast-forward, keep the warm landing workspace alive until the next
  fork can capture its exactly matching tree.
- **`PathToSourceChanged`** — a dependency was built under another `$CARGO_HOME`; see the shared cargo caches above.
- **`the rerun-if-changed instructions changed`** — a build script watches a path outside its package by absolute path;
  print it relative to the package instead, which is how cargo resolves it.
- **`StaleItem(MissingFile { .. })`**, on every build — a build script watches a path that does not exist, such as a
  `.git/packed-refs` a fresh clone or a cargo git checkout never wrote. Cargo counts a missing watched path as changed,
  so the script reruns and everything above its crate recompiles each time, in main as much as in a workspace. Watch
  only paths that exist, plus the nearest existing directory where one may later appear.
- **Incremental on one side only** — the host shell exports `CI`, which turns incremental off for workspace crates
  whatever the profile says, while a sandbox child never inherits it; build through `cowshed exec`.

The one path problem cargo never reports is the opposite one: a unit that compiled `env!("CARGO_MANIFEST_DIR")` in stays
fresh in every clone, so a test built in main reads main's files from inside the workspace. Read the variable at run
time (cargo and nextest set it for every test).

Xcode DerivedData does key on absolute paths: slot mounts (`fork --slot`) recycle a stable path for it. `bun install`,
`node_modules`, zig, and gradle caches are path-independent.

## A workspace misses Nx entries main has, and `.nx/workspace-data` is a directory

Every build-state path is a fixed link onto the checkout's build volume
(`.nx/workspace-data -> ../.cowshed/build/nx/workspace-data`). A tool that removes the path itself — `nx reset`, an
`rm -rf .nx/workspace-data` — leaves the next Nx run to make a real directory there: its task database and graph then
live outside the volume, so no fork, land or seed sees them, and every `cowshed fork` clones the directory from main.
`cowshed doctor` reports each such path as `build-link`; `cowshed exec <ws> -- true` relinks it (the refresh before
every exec discards what is there), and `cowshed setup` does the same for every workspace. A script that means to clear
Nx's state should empty the directory, never remove the path: `cowshed cache clear <ws>` does that for `.nx/cache` and
`.nx/workspace-data` together, and clears the project's gateway Nx store too.

## Parallel test fixture collisions

Keep the normal Nx and nextest parallelism. Gateway fixtures reserve a block's next port in the kernel for the test
process's lifetime; PID-modulo selection and file locks under different `TMPDIR` roots do not establish global
ownership. The gateway endpoint itself remains available for the session being tested. APFS fixtures use unique per-run
roots and remove a root even when a corruption test replaced its directory with a regular file.

## sccache reports a 0% hit rate and the shared cache never grows

A misdirected cache looks exactly like a broken one. Check where the client is actually writing before believing
anything about hit rates: from a directory outside every workspace, with `SCCACHE_DIR` and `SCCACHE_CONF` unset,

```
$ sccache --show-stats | head -3
Cache location                  Local disk: "/Users/you/Library/Caches/Mozilla.sccache"
Max cache size                     200 GiB
```

`Cache location` must be `~/Library/Caches/Mozilla.sccache` (`~/.cache/sccache` on Linux), sccache's own default and the
directory the daemon serves. If it names anything else, that client is filling a cache the daemon never reads, the
shared store is serving nothing, and the 0% is the consequence rather than the fault. `--show-stats` reports the
resolved configuration without starting a server, so this is safe to run against a live host.

The fix is `cowshed setup`, which writes and owns the `[cache.disk]` table in sccache's own config file (see
[cli.md](cli.md#cowshed-setup---uninstall---force---downgrade)). Run it and look at the line it prints about that file.
If it says it **left the file alone**, cowshed found a `cache.disk.dir` it did not write and refused to overwrite it;
the line names the directory it found. Point that `dir` at `~/Library/Caches/Mozilla.sccache` yourself, or delete the
`[cache.disk]` table and re-run `setup`. cowshed never resolves this one for you — a cache directory somebody chose
deliberately is not cowshed's to move.

Two symptoms of the same cause worth recognising. **Orphaned stores**: every directory that was ever a wrong destination
keeps whatever it accumulated, so a host that ran misconfigured for a while has gigabytes in the directories its config
named doing nothing. They are disposable by contract — delete them once `--show-stats` names the shared store. **A
shrinking shared cache**: a client that finds no daemon starts a server of its own over whatever directory its config
names, with sccache's 10 GiB default cap unless the config says otherwise, and that server will evict a larger shared
store down to 10 GiB. This is why the file `setup` writes carries a `size` beside the `dir`, and why a hand-edited
`[cache.disk]` should carry one too.

A build _inside_ a workspace never depends on any of this: the supervisor exports `SCCACHE_DIR` and
`SCCACHE_SERVER_UDS`, and the environment beats the config file. The config governs exactly the store-less case — which
is also why you cannot verify it from a shell that has the project environment loaded.

## Backup and durability (read once, remember forever)

**The store volume is excluded from backup** — deliberately. Multi-gigabyte images with constant internal churn would
bloat every backup (and, on the Data volume, every hourly local snapshot — that is why they live on a dedicated volume
at all; see 01a_apfs.md). Source follows the durability rules below; the shared tool caches are the tools' own
directories in your HOME, rebuildable from their registries and never anything to restore. Protected job content is
authoritative within its origin incarnation/checkpoint snapshot, but a workspace image is still not an off-machine
backup.

- Committed + pushed (`cowshed push`, or merged in main): it's in main's repo — and main's off-machine durability is its
  **origin remote**, exactly as before adoption. Keep pushing main to origin as usual; the store volume is not a backup.
- Committed, unpushed: `cowshed push <ws>` preserves the branch in main's repo under `refs/cowshed/<ws>/…`; until then
  the commits exist only in the workspace image.
- **Uncommitted work exists only in the workspace image.** Nothing saves it in the background.

Restoring a machine: clone main's repo from its origin remote, `cowshed adopt` again; workspaces are recreated from
their pushed branches (`cowshed fork x --ref refs/cowshed/x/heads/<branch>`). Checkpoints and images are not backup
artifacts — never treat them as one. Export any terminal job stream you need to retain independently; cowshed
materializes a clone, reflink, or copy, never a hardlink to protected content.

## ZFS pool and hierarchy

A ZFS host uses exactly two sibling datasets under the `<pool>/cowshed` root: `<pool>/cowshed/store` at
`/private/cowshed/store` and `<pool>/cowshed/projects` for `<owner>/<repo>/{main,ws/...}`. A `<pool>/cowshed/caches`
dataset from an earlier release is kept mounted at `/private/cowshed/caches` until `cowshed setup` has moved what it
holds and `cowshed setup --retire-caches-volume` destroys it. If `statfs` does not locate a suitable delegated ZFS
dataset, configure `[substrate] kind = "zfs"` and `pool = "<pool>"`; cowshed deliberately refuses to scan pools or
guess. `cowshed doctor` reports the selected pool and any missing sibling, mountpoint, or delegation.

**Restore interrupted.** Before detached metadata publication, recovery restores the displaced workspace, old
incarnation, and old token. After publication, recovery completes the replacement forward; it never rolls back across
the incarnation fence. A healthy restore always drains the old supervisor, stages and verifies the replacement, mints
the new incarnation then token, swaps and mounts, publishes metadata atomically, revokes the old token, and only then
admits a supervisor or job. No state should accept both tokens; `cowshed doctor` reports a publication mismatch.

## `cowshed exec` says the cowshed daemon is not reachable

Workspace supervisors are processes the cowshed daemon starts and keeps, through its supervisor manager at
`/private/cowshed/store/run/manager.sock`; a command that runs work in a workspace asks it for the workspace's
supervisor. `cowshed gateway status` shows whether the daemon runs, and `cowshed gateway start` installs and starts it.
A supervisor that could not start leaves its reason in `~/Library/Logs/cowshed/daemon-stderr.log`.

## A command says "the cowshed daemon is build X; this cowshed is build Y"

The daemon and the `cowshed` you ran are different builds. Every command that would change a workspace or run work in
one asks the daemon first and stops before it changes anything, so nothing was half done. Run `cowshed setup` from the
cowshed you mean to use: it installs that build as both the daemon and the `cowshed` on your `PATH`. To install a
checkout's build, run its own `packages/cowshed/bin/cowshed setup`; a plain `cowshed setup` runs the installed copy and
installs nothing new. A `cowshed` on `PATH` that is a link into a checkout runs whatever that checkout last built, which
is how the two drift apart; setup repoints such a link at the installed copy.

Setup exits 4 without installing or restarting anything while a restart would cut work off: a land holding its
repository's turn, or a gate job holding or wanting CPU tokens. Its storage repair, which runs first, stays done. The
refusal names each blocker; rerun setup once they have ended (`cowshed gateway status` lists the land turns and gate
jobs). No install window is needed and setup never waits for them. If an earlier install failed and left the gateway
agent unloaded, the error says so and names the `launchctl bootstrap gui/<uid> '<plist>'` that loads it;
`launchctl kickstart` cannot load an agent launchd does not hold ("Could not find service").

## `cowshed exec` says a workspace supervisor is another cowshed build

After an install, a workspace's supervisor may still be the previous build's: the daemon asks it to drain, and it keeps
serving until its running jobs end. Until then a command of the new build cannot use it. `cowshed detach <ws>` (and
`rm`) stops it: the supervisor is signalled, the jobs it left running are ended, and its socket is removed. The next
`cowshed exec` starts a supervisor of the new build. Detaching ends that supervisor's jobs, so let a job you still need
finish first.

## A cargo build inside a workspace does not use sccache

A workspace's cargo is wrapped with the sccache `cowshed setup --sccache` pinned, named by its store path. A host that
never ran it, or whose pinned store path was collected (`cowshed doctor` reports `sccache-unpinned`), builds without a
wrapper. Run `cowshed setup --sccache`; the next command in the workspace uses it.

## `cowshed exec` fails with `cannot run direnv from PATH …`

The workspace supervisor looks for `direnv` (and `devenv` and `nix`) in the host's Nix profiles — `~/.nix-profile`,
`~/.local/state/nix/profile`, `/etc/profiles/per-user/<you>`, `/run/current-system/sw` and the default profile — never
on your shell's PATH, and puts only the store package each one resolves to on the job's PATH, never a whole profile.
Install `direnv` into one of those profiles (`nix profile install nixpkgs#direnv`, or home-manager/nix-darwin), then run
the command again. The message names the PATH that was searched. A different tool your shell has but a job lacks belongs
in the workspace's own `.envrc` activation.

## When cowshed itself misbehaves

`cowshed doctor --json` is the bounded bug-report payload: it includes versions, invariant results, continuity metadata,
hashes, and the last few operations from the telemetry store, never raw job stdout/stderr. Workspace lifecycle can be
re-derived after detach, but protected job content exists only in its origin incarnation/checkpoint or an independent
export. To reset attachment state, detach each workspace with `cowshed detach`; subsequent commands re-derive mounts and
controller wiring. There is no cache to clear and no database to reset.

A corrupt shared cache is the tool's own problem, solved the tool's own way: nothing unique lives in one, so delete that
tool's cache directory (`~/.cargo/registry`, `~/.bun/install/cache`, `~/.cache/uv`, …) and the tool refetches or
rebuilds it. sccache's `~/Library/Caches/Mozilla.sccache` and the gateway mirror under `~/Library/Caches/dev.cowshed`
belong to running daemons: stop the daemon first (`cowshed sccache stop`, `cowshed gateway stop`), delete, then start it
again. (Never delete anything under `/private/cowshed/store` — that volume holds your images.)

## Every verb prints `could not install gateway session …: (EndpointConflict)`

`EndpointConflict` means the gateway already has a session on the port block this project's inventory assigns to one of
its workspaces, under a different workspace identity. The gateway's session table is a cache of host inventory, never an
authority: the owner is a session left behind by a project that was deleted out of band
(`rm -rf /private/cowshed/store/<owner>/<repo>` without ever running a verb against it again), and the host-global
port-block allocator has since handed that block to a new workspace. Reconcile — which every `exec`, `attach`, and
`doctor` runs first — evicts such a session itself once the host inventory confirms no live workspace anywhere still
carries that identity, then installs the workspace; the message does not recur. If `cowshed doctor --json` instead
refuses with
`gateway endpoint 127.0.0.1:<base> is assigned to workspace <id> by this project and still claimed by live workspace <id> of another project`,
two live workspaces hold one block: that is an inventory fault, not a stale session, and cowshed never resolves it by
evicting a live session. Retire one of the two (`cowshed rm`, or `detach` and re-create it so it takes a fresh block)
and rerun `doctor`. One workspace that cannot be installed no longer stops the rest of the project from being installed;
the error names every failed identity.

## `cowshed ls` takes tens of seconds; `cowshed fork` or `doctor` takes a minute

Per-command cost does not grow with history or with the number of warm workspaces: no command reads the audit segments
under `/private/cowshed/store/telemetry/` (they are write-only telemetry; authority is the image inventory); the host
APFS inventory is one IORegistry snapshot plus the kernel mount table, never `diskutil apfs list`, and the two reserved
cowshed volumes' FileVault state comes from that same snapshot, never `diskutil info`; attaching an image takes its
volume from the attach's own report, asking Disk Arbitration nothing after it (`diskutil` queues behind every
arbitration client on the host); one project open validates the repository binding and reads the inventory once for
every workspace it recovers. When a command is still slow on a wait in a host process,
`ps -o pid,ppid,etime,args -ax | grep -E 'diskutil|hdiutil|git'` names it while it runs.

`cowshed fork` prints a `cowshed: apfs canonical/<step>` or `cowshed: fork <step>` span with its elapsed time for every
step, so the slow step is named on stderr. On a large repository two steps carry nearly all of the time:

- **The first write into the fresh clone**, `apfs canonical/first-write`. The clone call shares main's image extent map;
  the first write to either file makes APFS copy that map. Cowshed rewrites the clone's first block before attach so the
  cost is a step of its own rather than part of the mount's recovery write. Main's image fragments when writes move
  blocks shared by clones into new runs, so first-write cost scales with extent count. A one-byte write into a plain
  `cp -c` clone reproduces that map copy without an attached disk image. Deleting a written clone also incurs extent-map
  work; `fork <ws> <name>` incurs it for the source workspace's image. `cowshed doctor` reports the extent count and
  predicted cost as `main-extents` and warns when it reaches the cold-fork budget (08_testing.md). `cowshed defrag main`
  rewrites main's image file contiguously while the checkout is idle; a busy volume refuses. It needs as much free space
  as main's image has allocated and keeps it while older clones still share the old blocks.
- **`fork links`**, the walk over the whole tree for symlinks that point outside it; its cost scales with the entries
  visited.

`cowshed doctor` prints `cowshed: doctor <probe> start` for every probe, each project check per workspace as
`doctor <workspace>/<check>`, and a `done elapsed=…` line for each probe that took 2 s or more, then one
`doctor workspace-checks done` summary naming the slowest workspace. A probe that waits for another party says so before
it asks with `cowshed: doctor <probe> waits-on=<party>`. A doctor that does not finish is in the probe it last started,
waiting for the party that line names; the `done` lines say where a slow doctor's minute went. A gateway or sccache
server that accepts doctor's request and stays silent for a minute is reported as `gateway-unanswered` or
`sccache-unanswered` instead of holding doctor.

## A disk child `did not finish within 120s`

Every `hdiutil`, `diskutil` and `mount_apfs` child cowshed starts is killed at a two-minute deadline, and the refusal
says the child ran and hung — distinct from `could not run executable`, which means it never started. One host cause
cowshed can see is a saturated kernel vnode table. `kern.num_vnodes` at `kern.maxvnodes` is not that: the kernel caches
vnodes up to the limit and recycles the free ones (`kern.free_vnodes`), so a busy host sits there healthily. The table
is saturated when the vnodes _in use_ — `kern.num_vnodes` minus `kern.free_vnodes` — reach the limit: nothing is left to
recycle, and a mount that takes a second can outlast the deadline. When that is the state of the host the refusal names
it (`… vnodes in use of kern.maxvnodes …`) and its hint is the limit to set; `cowshed doctor` reports it as
`vnode-table-saturated`. Raising the limit is the operator's call: `sudo sysctl kern.maxvnodes=<n>`.

## An attach is slow, or stderr says `disk-lease … unleased=`

Every disk tool cowshed runs takes the gateway's host disk-lifecycle lease first: `diskutil`, `hdiutil` and `newfs_apfs`
share one phase, `mount_apfs` and `umount` the other, and the two never overlap, because an attach does not finish while
the mount table keeps changing. Each command prints `cowshed: disk-lease <class> wait` with the time it queued and
`cowshed: disk-lease <class> phase` around the command itself. A long wait is time spent behind the other class's phase;
a long phase is the command.

`status=err` on the wait, with an `unleased=<reason>` line, means the command ran without a lease. "predates disk
leases" means the running gateway is from an older build: `cowshed setup` from this build restarts it. A gateway that is
not running is said once per process and every disk command runs unleased until it is back. The gateway's own stderr
names a holder it evicted, `disk-lease evicted a <class> holder after <held> while <class> waited`, with the pid and
command line: a command that kept its phase ten seconds while the other class waited is the stuck one, and
`ps -o pid,etime,args -p <pid>` shows whether it still runs.

## Every attach fails with `error code 150`

`Failed to initialize IO manager … error code 150` (`kIOReturnNoResources`) from every image attach,
`diskutil image info`, `cowshed fork` and cold mount, while images already attached keep working, means the host's
disk-image attach pool is spent. A command that meets it fails with `AttachPoolExhausted`, and `cowshed doctor` says
which of two causes spent it.

`disk-image-attach-budget` reports leaked disk-image devices: `AppleDiskImageDevice` and `DIDeviceIOUserClient`
instances (`ioclasscount`) that no attached image accounts for, which only a reboot frees. It also prints the
`AppleAPFSVolume` and `IOMedia` counts, whose departed instances take no pool slot (`departed-volume-references`,
below), the ceiling at which this host's pool last ran out (1024 leaked devices until one is recorded: an observation
from one host and boot, not a documented limit), the headroom left, and any disk-image command an earlier cowshed killed
at its 15-minute hard deadline this boot, each a probable leaked device; cowshed no longer kills a disk-image command at
all. "attach pool exhausted, reboot required" means an attach failed with error code 150 this boot with the leaked
devices at the ceiling: reboot. A recorded ceiling only moves down and survives reboots; if the lane gate refuses
because that ceiling alone is below the attaches it needs, and the ceiling no longer holds, `attach-budget --clear`
forgets it.

The other cause is helpers that outlived their image. Every attached image has its own `diskimagesiod`, which maps 72
MiB into the kernel; detach does not always end it, and a helper that outlived its image keeps its mapping.
`cowshed doctor` reports them as `orphaned-disk-image-helpers`: the count, the pids (running helpers that two reads in a
row find no attached `AppleDiskImageDevice`'s user client naming as its creator, with no `diskutil` or `hdiutil` running
during either), the mapping they hold, and the root command that frees it, `sudo kill -9 <pids>`. `launchctl kill`
refuses them, and no reboot is needed for them. While a disk-image tool runs, a helper may still be attaching, so no pid
is named until a later `cowshed doctor`. Killing orphans frees their mappings, never a leaked device. Once they are
gone, `attach-budget --clear` (or any successful cowshed attach) ends the recorded exhaustion. cowshed never signals a
helper itself.

## An attach is refused with `MediaCapReached`, or names IOKit's media match-notification queue

IOKit drops IOMedia match notifications past 1024, and DiskImages2 waits for its new device's match with no overall
timeout, so an attach past the cap hangs forever. Every attached APFS image registers three IOMedia, and a workspace
with its build volume six. cowshed refuses an attach that would cross the cap instead of starting it, and a verb that
may attach first detaches idle workspaces (no supervisor serving them), least recently used first; each detach is said
on stderr, and the workspace attaches again on its next use. When nothing idle frees enough, the verb refuses and names
the attached workspaces with the IOMedia each holds and why each stays. `cowshed doctor`'s `media-notification-cap`
reports the registered media, the attaches running and how many more images fit. Free media with `cowshed rm` for sheds
you are done with or `cowshed detach` for ones you will return to. A reboot does not raise this cap.

## `cowshed doctor` warns `departed-volume-references`

`departed-volume-references` counts the `AppleAPFSVolume` and `IOMedia` instances that outlive their registry entries:
departed volumes and media the kernel has not freed. It warns from the first one and never makes the host unhealthy. It
is not a cause of `error code 150`: a departed volume takes no attach-pool slot, so it neither lowers the headroom
`disk-image-attach-budget` reports nor refuses an attach, and the finding asks for neither a reboot nor a restart.
`cowshed doctor` reads the registry first and the instances after, so a disk command in flight between the two reads can
account for a few.

The count does not say what holds them, and holders have differed: Spotlight's `mds` 2418.6.3.9.400 (arm64e) keeps a
send right to each mounted `AppleAPFSVolume` it admits, while on one earlier boot `diskarbitrationd` held volumes and
media alike. Neither is assumed for another build or class; `mds` is not shown to hold `IOMedia`. The hint attributes by
exact registry ID and changes nothing: as root, count per process the rights in `lsmp -a` whose `AppleAPFSVolume` or
`IOMedia` ID is absent from `ioreg -r -c IOMedia -d 1 -w0`. A count matching the finding names the holder by exact pid;
re-read that it still holds those IDs before anything is done to it, and never act on a daemon family (`01a_apfs.md`,
"Release and device custody"). IDs no process holds are held in the kernel. The defect, the volumes `mds` admits, and
why neither an unmount nor an indexing exclusion releases a right are specified in `01a_apfs.md`, "Spotlight media
references".

## `cowshed path` or `cowshed exec` is slow

`COWSHED_TIMING=1 cowshed exec <ws> -- true` prints one `cowshed: timing +<since start> <scope> <step> <elapsed>` line
per step on stderr. A named workspace that is mounted and served answers from live state: expect only
`resident resolve`, `resident gateway`, `resident submit` and `resident relay` lines, all in milliseconds. Otherwise a
`resident declined: <reason>` line says why the project controller opened instead — `the workspace is not mounted`,
`the supervisor serves another authority` after a grant change, `lifecycle work is unfinished` — and the `open`,
`recover`, `route` and `reconcile` lines that follow name the step that spent the time. The command that follows a
decline leaves the workspace resident again (attached, supervisor current, gateway reconciled), so only the first one
after a change pays for it.

## "cowshed volumes owned by another user"

The cowshed volumes belong to exactly one uid. If `doctor` reports a foreign-uid volume, you are running cowshed as the
wrong account — most commonly you set up the dedicated-`dev`-uid posture (specs' 14_nix.md) and then ran cowshed from
your personal account. Run it as dev instead: `ssh dev@localhost` or `sudo -u dev -i` (a dev shell via ssh/sudo is the
expected, healthy shape — doctor recognizes it). Cross-uid file access to another account's cowshed tree is deliberately
unsupported; there is no `--force` for this one. On nix hosts, `programs.cowshed` (home-manager) and `services.cowshed`
(nix-darwin, for the dev-uid posture) own the host setup declaratively — `doctor` hints name the option to enable rather
than a command to run.

## Simulator brokering (posture B — see ios.md)

- **A tool only lists dev-local simulators, never the personal-session device.** It spawned `/usr/bin/xcrun` by absolute
  path, bypassing the in-image wrapper (`.cowshed/bin/xcrun`). That degradation is the safe default — the personal
  session is unreachable except through the wrapper → gateway → broker path. Fix the tool's PATH resolution, or hand the
  artifact over manually (copy the `.app` to the drop dir, then your side's `simctl install`).
- **`cowshed: sim broker unreachable` (exit 5).** The session broker is a launchd agent in the _personal_ GUI session —
  it isn't running if nobody is logged in or the agent isn't loaded; the `next:` hint names the `launchctl` kickstart.
  Exit 5 (environment) is deliberately distinct from exit 6 (a denial: missing `--sim` grant, non-drop-dir install,
  unregistered URL scheme).
- **`install` refused despite a `--sim install` grant.** The broker only installs drop-dir artifacts and only under the
  human-gating rule — that refusal is the design, not a bug (ios.md explains why: simulator apps run as _you_).

## Desktop apps (posture B — see desktop.md)

- **"I want the app running as dev but visible in my session."** macOS can't show one uid's window in another's session
  (Screen Sharing streams a whole session, it doesn't relocate a window). Pick a lane: test/debug as dev (view via
  Screen Sharing into dev's session), or install a copy for yourself and run it as you (desktop.md).
- **Gatekeeper blocks a copied app.** It's ad-hoc-signed. Sign with Developer-ID on the dev side (dev holds the signing
  identity) so it launches cleanly; or right-click-open once.
- **An agent can't launch a desktop app in my session.** Correct — there is no agent verb for it (unlike `--sim`, there
  is deliberately no `--app open`). Agents test desktop apps as dev in dev's session; only the human installs a copy.
