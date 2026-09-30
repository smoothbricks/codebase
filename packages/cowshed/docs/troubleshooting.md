# Troubleshooting

First move for anything weird: `cowshed doctor`. It checks every invariant (images ↔ markers ↔ mounts ↔ grants, caches
volume, gateway, autosave freshness) and prints one `cowshed:` line per problem with a `next:` fix. Because cowshed has
no database, doctor isn't reconciling state — it's _deriving_ it from disk and the mount table, so what it reports is
the truth.

## Mounts

**Workspace missing after reboot.** Mounts don't survive reboots; images do. Direnv repositories normally reattach when
their allowed `.envrc` runs:

```sh
$ cowshed attach
cowshed: attached acme/widget/raven
```

Adopted main workspaces retain the byte-stable stub `.envrc` cowshed wrote underneath the mountpoint. When unmounted,
`cd <project-root>` exposes that stub, which runs `cowshed attach`; after the real workspace is mounted its own `.envrc`
shadows the stub and direnv reloads. Cowshed does **not** authorize either file: run `direnv allow` once at each
workspace path.

Devenv's hook cannot activate from the bare mountpoint stub. For a devenv-native repository, run `cowshed attach`, then
run the repository's `devenv:allow` command once at that workspace path. The mounted image's `.envrc` sources
`.cowshed/env`; no command prints those exports on demand. Cowshed never reads or writes devenv's trust database. The
login LaunchAgent may attach permanent workspaces proactively, but explicit `attach` remains the recovery command.

**Finder ejected a volume** (or `diskutil eject` by hand): use the same explicit `cowshed attach` recovery.

**direnv says `.envrc is blocked` in a workspace.** This is expected until that clone path is authorized. Run
`direnv allow`; `cowshed attach` repairs mounts but deliberately does not change trust.

**A command does not see an edited shell file.** A warm workspace shell re-activates when a file direnv recorded as an
input changes. A file your `.envrc` reads without telling direnv (a sourced script, a lockfile an install step uses) is
not on that list: add `watch_file <path>` to the `.envrc`, and edits to it re-activate the next command.

**devenv refresh fails during `cowshed exec`.** With `[devenv] dir` in `.cowshed.toml` (or a root `devenv.nix`), cowshed
watches the configuration inputs and refreshes the environment before the next sandbox process. A missing configured
`devenv.nix`, missing `devenv` executable, or evaluation error fails closed with exit 5 and devenv's stderr; cowshed
never reuses a stale snapshot. Existing long-running processes keep their launch environment.

**`cowshed adopt` or `cowshed push` refused with exit 4 naming files.** The secrets gate found credential-shaped content
(`.env*`, key files, known token prefixes, `.envrc` secret exports). For adopt: move each value into the gateway
Keychain (see gateway.md) and delete the file, or `cowshed adopt --quarantine` to relocate findings outside the image so
dependent tooling fails loudly. For push: the offending hunks are named — remove the secret and push again; autosave
meanwhile skips (never propagates findings) and warns.

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

and the gateway audit events for egress (`cowshed audit --denied | tail`). Common cases:

- **A lifecycle verb (`new`, `rm`, …) exits 6 with "the process running cowshed is sandboxed away from cowshed's
  store"**: the shell that ran cowshed — an agent harness shell, or a `cowshed exec` child calling cowshed again — is
  itself sandboxed, and the kernel answered EPERM on a store path (such a sandbox typically lets the store's existing
  lock files open but refuses the temp file every durable write starts with). The store is intact and no grant helps:
  rerun the verb from a shell whose sandbox allows writing the store.
- **Tool writes to `$HOME` dotfiles** (some CLIs insist on `~/.toolrc`): grant narrowly (`--write ~/.toolrc`, not
  `--write ~`), or set the tool's env override to a path inside the workspace — `cowshed shell` and fix its config once;
  it's in the image and every fork inherits it.
- **Egress to an unmirrored host**: `cowshed grant <ws> --egress <host>` — applies immediately, no re-exec.
- **Linux package/proxy client gets connection refused at `127.0.0.1:7644`**: do not point it at the Unix socket or a
  macOS block base. Run `cowshed attach`; `doctor` distinguishes a detached workspace, absent/dead connector, missing or
  wrong per-incarnation socket mount, and a dead host gateway. A healthy workspace uses
  `http://127.0.0.1:7644/{npm,cargo,go}` and the same base, with the token as userinfo
  (`http://cowshed:<token>@127.0.0.1:7644`), in `HTTP_PROXY`/`HTTPS_PROXY` (plus lowercase forms). Detach and restore
  intentionally drain old connections; retry only after the new attachment is admitted.
- **407 versus 403**: 407 means the endpoint/credential pair did not authenticate — most often proxy variables that lost
  their userinfo, or stale pre-restore wiring. A client without the credential gets one 407 with
  `Proxy-Authenticate: Basic realm="cowshed"` and stops; cargo instead reads a bare tunnel failure as a spurious network
  error and grinds through its retry ladder, so a cargo command that hangs for minutes on `CONNECT tunnel failed` is a
  credential problem, not a slow network. 403 means endpoint and credential authenticated but policy denied the
  destination; use the gateway's grant hint. Port 7644 by itself is not workspace identity: the private netns plus
  mounted socket inode selects the workspace.
- **`go` denied writing `~/go`**: that deny is a deliberate tripwire, not a bug — it means a go invocation ran without
  the workspace's `GOENV` wiring (an unwrapped spawn, or an editor without direnv integration). Run it through
  `cowshed exec`/a direnv shell, or fix the editor's direnv plugin; never grant `~/go`. `cowshed doctor` prints the same
  hint, and checks the host for a stray `~/go` that predates adoption (safe to delete — it is only cache).
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

Images are sparse files that grow with churn; deleting files inside a volume does not shrink the image file. `cowshed gc`
reclaims retired images, removes orphans, and prunes expired checkpoints (`--dry-run` lists each candidate first):

```
$ cowshed gc --dry-run
44023414784
cowshed: would delete /private/cowshed/store/acme/widget/sessions/.trash/fox-3f2a….asif (18203238400 bytes; reason: workspace was retired)
…
cowshed: dry run examined 12 objects; 4 candidates, 44023414784 bytes deletable
$ cowshed gc
44023414784
```

A main that has taken years of writes under clones is slow to clone, not large: `cowshed doctor` reports `main-extents`
and names `cowshed defrag main` when the next `new` would pay for it.

Attribution: `du` on the images directory tells you per-workspace cost; _inside_ a mounted workspace, normal `du` works
— it's just APFS. Remember clones share extents: ten fresh workspaces cost ~zero until they diverge, so "sum of image
sizes" overstates real usage. `df -h /private/cowshed/caches` covers the shared cache volume; it shares the container's
free space with everything else.

Cargo's shared writable caches are `/private/cowshed/caches/cargo/{registry,git}`; gateway-owned bare repository mirrors
are separate at `/private/cowshed/caches/repo-mirrors` and must remain sandbox-read-only.

**A workspace rebuilds every dependency its copied `target/` already holds, refetches a crate the host has, or its
`bun install` relinks all of `node_modules`.** Cargo fingerprints a registry or git dependency by the absolute path of
its source under `$CARGO_HOME`, and Bun's isolated linker writes its cache path into every `node_modules/.bun` link, so
every checkout must use one literal path for each. A sandbox uses the host's own `~/.cargo` only once
`~/.cargo/registry` and `~/.cargo/git` both link to `/private/cowshed/caches/cargo/{registry,git}`, the host's
`~/.bun/install/cache` only once it links to `/private/cowshed/caches/bun/install/cache`, and the host's `~/.cache/uv`
only once it links to `/private/cowshed/caches/uv`; until then that tool keeps a private cache in the sandbox's own
HOME. `cowshed doctor` reports each unshared cache as `host-cache-unshared`; `cowshed setup --imperative-host-setup`
moves the host's caches onto the caches volume and links them back. It holds cargo's own package-cache locks while
cargo's caches move and refuses while a cargo process holds one; Bun and uv have no such lock, so run it once builds and
installs are idle. A host cache and a shared directory that both already hold a cache are a conflict it leaves
untouched: keep one, delete the other, and rerun.

**Nix cache/state points at the host filesystem.** On declarative hosts the module must own
`~/.cache/nix → /private/cowshed/caches/nix/cache` and `~/.local/state/nix → /private/cowshed/caches/nix/state`; `setup`
and `doctor` only validate. Fix the declarative configuration rather than allowing cowshed to mutate it. The explicit
`cowshed setup --imperative-host-setup` fallback is only for a host with no supported declarative owner; it is never an
automatic recovery from mixed or broken ownership.

## Path-sensitive caches (why a fresh workspace rebuilds more than expected)

Cargo keys a workspace crate on its path relative to the package, so a workspace at
`<mount-root>/<owner>/<repo>/<workspace>` finds the units main built fresh (measured: a dev build of a mid-size crate in
a fresh clone, 208 of 208 units fresh, the incremental workspace crates included). When a fresh workspace still
rebuilds, run the build with `CARGO_LOG=cargo::compiler::fingerprint=info` and read the `dirty:` reason of the first
unit that rebuilt:

- **No fingerprint at all** — main never built that unit. The usual cause is a `test` profile that differs from `dev`
  (every dependency then has two units, and main held the other one) or a toolchain bump since main was last warmed: let
  `test` inherit `dev`, and warm main with `cowshed exec main -- <canonical build>`.
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

Xcode DerivedData does key on absolute paths: slot mounts (`new --slot`) recycle a stable path for it. `bun install`,
`node_modules`, zig, and gradle caches are path-independent.

## sccache reports a 0% hit rate and the shared cache never grows

A misdirected cache looks exactly like a broken one. Check where the client is actually writing before believing
anything about hit rates: from a directory outside every workspace, with `SCCACHE_DIR` and `SCCACHE_CONF` unset,

```
$ sccache --show-stats | head -3
Cache location                  Local disk: "/private/cowshed/caches/sccache"
Max cache size                     200 GiB
```

`Cache location` must be `/private/cowshed/caches/sccache`. If it names something under `~/Library/Caches` or
`~/.cache`, that client is filling a private cache nobody reads, the shared store is serving nothing, and the 0% is the
consequence rather than the fault. `--show-stats` reports the resolved configuration without starting a server, so this
is safe to run against a live host.

The fix is `cowshed setup`, which writes and owns the `[cache.disk]` table in sccache's own config file (see
[cli.md](cli.md#cowshed-setup---uninstall---force---mount-root-dir)). Run it and look at the line it prints about that
file. If it says it **left the file alone**, cowshed found a `cache.disk.dir` it did not write and refused to overwrite
it; the line names the directory it found. Point that `dir` at `/private/cowshed/caches/sccache` yourself, or delete the
`[cache.disk]` table and re-run `setup`. cowshed never resolves this one for you — a cache directory somebody chose
deliberately is not cowshed's to move.

Two symptoms of the same cause worth recognising. **Orphaned stores**: every directory that was ever a wrong destination
keeps whatever it accumulated, so a host that ran misconfigured for a while has gigabytes in
`~/Library/Caches/Mozilla.sccache` (and possibly older per-home paths) doing nothing. They are disposable by contract —
delete them once `--show-stats` names the shared store. **A shrinking shared cache**: a client that finds no daemon
starts a server of its own over whatever directory its config names, with sccache's 10 GiB default cap unless the config
says otherwise, and that server will evict a larger shared store down to 10 GiB. This is why the file `setup` writes
carries a `size` beside the `dir`, and why a hand-edited `[cache.disk]` should carry one too.

A build _inside_ a workspace never depends on any of this: the supervisor exports `SCCACHE_DIR` and
`SCCACHE_SERVER_UDS`, and the environment beats the config file. The config governs exactly the store-less case — which
is also why you cannot verify it from a shell that has the project environment loaded.

## Backup and durability (read once, remember forever)

**The store and caches volumes are excluded from backup** — deliberately. Multi-gigabyte images with constant internal
churn would bloat every backup (and, on the Data volume, every hourly local snapshot — that is why they live on
dedicated volumes at all; see 01_storage.md). Source and caches follow the durability rules below. Protected job content
is authoritative within its origin incarnation/checkpoint snapshot, but a workspace image is still not an off-machine
backup.

- Committed + pushed (`cowshed push`, or merged in main): it's in main's repo — and main's off-machine durability is its
  **origin remote**, exactly as before adoption. Keep pushing main to origin as usual; the store volume is not a backup.
- Committed, unpushed: the autosave agent (host-side, like `push`) fetches every workspace into `refs/cowshed/<ws>/wip`
  every 10 minutes.
- **Uncommitted work is at risk between autosaves.** `cowshed doctor` warns when any workspace's autosave is stale.

Restoring a machine: clone main's repo from its origin remote, `cowshed adopt` again; workspaces are recreated from
their saved branches (`cowshed new x --ref refs/cowshed/x/wip`). Checkpoints and images are not backup artifacts — never
treat them as one. Export any terminal job stream you need to retain independently; cowshed materializes a clone,
reflink, or copy, never a hardlink to protected content.

## ZFS pool and hierarchy

A ZFS host uses exactly three sibling datasets under the configured root: `<pool>/cowshed/store` at
`/private/cowshed/store`, `<pool>/cowshed/caches` at `/private/cowshed/caches`, and `<pool>/cowshed/projects` for
`<owner>/<repo>/{main,ws/...}`. If `statfs` does not locate a suitable delegated ZFS dataset, configure
`[substrate] kind = "zfs"` and `pool = "<pool>"`; cowshed deliberately refuses to scan pools or guess. `cowshed doctor`
reports the selected pool and any missing sibling, mountpoint, or delegation.

**Restore interrupted.** Before detached metadata publication, recovery restores the displaced workspace, old
incarnation, and old token. After publication, recovery completes the replacement forward; it never rolls back across
the incarnation fence. A healthy restore always drains the old supervisor, stages and verifies the replacement, mints
the new incarnation then token, swaps and mounts, publishes metadata atomically, revokes the old token, and only then
admits a supervisor or job. No state should accept both tokens; `cowshed doctor` reports a publication mismatch.

## `cowshed exec` says the cowshed daemon is not reachable

Workspace supervisors are processes the cowshed daemon starts and keeps, through its supervisor manager at
`/private/cowshed/store/run/manager.sock`; a command that runs work in a workspace asks it for the workspace's
supervisor. `cowshed gateway status` shows whether the daemon runs, and `cowshed gateway start` installs and starts it.
A supervisor that could not start leaves its reason in `~/Library/Logs/cowshed/daemon-stderr.log`. A refusal naming two
cowshed builds means the daemon and the `cowshed` you ran are different binaries: run `cowshed gateway start` from the
one you mean to use.

## `cowshed exec` fails with `cannot run direnv from PATH …`

The workspace supervisor looks for `direnv` (and `devenv`) in the workspace's own devenv profile and in the host's Nix
profiles — `~/.nix-profile`, `~/.local/state/nix/profile`, `/etc/profiles/per-user/<you>`, `/run/current-system/sw` and
the default profile — never on your shell's PATH, because the daemon starts supervisors with launchd's. Install `direnv`
into one of those profiles (`nix profile install nixpkgs#direnv`, or home-manager/nix-darwin), then run the command
again. The message names the PATH that was searched.

## When cowshed itself misbehaves

`cowshed doctor --json` is the bounded bug-report payload: it includes versions, invariant results, continuity metadata,
hashes, and the last few operations from the telemetry store (`cowshed logs --since 1h` shows the same thing), never raw
job stdout/stderr. Workspace lifecycle can be re-derived after detach, but protected job content exists only in its
origin incarnation/checkpoint or an independent export. To reset attachment state, detach each workspace with
`cowshed detach`; subsequent commands re-derive mounts and controller wiring. There is no cache to clear and no database
to reset.

For cache-volume corruption specifically there is a bigger, equally safe hammer: nothing unique lives on
`cowshed.caches`, so `diskutil apfs deleteVolume` and letting cowshed lazily recreate it is always an option — the
mirror refetches, sccache and registries rebuild. `cowshed doctor` suggests it when the caches volume fails its checks.
(Never do this to `cowshed.store` — that volume holds your images.)

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

## `cowshed ls` takes tens of seconds; `cowshed new` or `doctor` takes a minute

Per-command cost does not grow with history or with the number of warm workspaces: no command reads the audit segments
under `/private/cowshed/store/telemetry/` (they are write-only telemetry; authority is the image inventory); the host
APFS inventory is queried only for the cowshed container, and attaching an image lists only that image's container; one
project open validates the repository binding and reads the inventory once for every workspace it recovers. When a
command is still slow on a wait in a host process, `ps -o pid,ppid,etime,args -ax | grep -E 'diskutil|hdiutil|git'`
names it while it runs.

`cowshed new` prints a `cowshed: apfs canonical/<step>` or `cowshed: new <step>` span with its elapsed time for every
step, so the slow step is named on stderr. On a large repository two steps carry nearly all of the time:

- **The first write into the fresh clone**, inside `apfs canonical/attach` or `apfs canonical/mount`, whichever writes
  first. The clone call shares main's image extent map and returns in milliseconds; the first write to either file makes
  APFS copy that map, at about 12 µs per extent. Main's image fragments with every write it takes while clones share its
  blocks, so the cost grows with how long main has been in use: an image with 2.1 million extents costs 25 s on a quiet
  host and 45–70 s on a busy one, one with 470 thousand costs 4 s, and a freshly written 4 GiB file with 256 extents
  costs 5 ms. A one-byte write into a plain `cp -c` clone of the image file reproduces the cost with no disk image
  attached, so it is not the mount itself and does not depend on how many images are attached. Deleting a written clone
  (`cowshed rm`, `cowshed gc`) pays about half as much per extent. `new --from <ws>` pays the same cost for the source
  workspace's image. `cowshed doctor` reports main's extent count and the cost it predicts as `main-extents`, and warns
  once that cost reaches a second. Only rewriting main's image file contiguously lowers it: `cowshed defrag main`, run
  while the checkout is idle, since main has to leave the kernel for the copy and a busy volume refuses. It needs as
  much free space as main's image has allocated and keeps it while older clones still share the old blocks.
- **`new links`**, the walk over the whole tree for symlinks that point outside it, which costs about 5 s over a million
  entries.

## A disk child `did not finish within 120s`

Every `hdiutil`, `diskutil` and `mount_apfs` child cowshed starts is killed at a two-minute deadline, and the refusal
says the child ran and hung — distinct from `could not run executable`, which means it never started. One host cause
cowshed can see is a saturated kernel vnode table. `kern.num_vnodes` at `kern.maxvnodes` is not that: the kernel caches
vnodes up to the limit and recycles the free ones (`kern.free_vnodes`), so a busy host sits there healthily. The table
is saturated when the vnodes _in use_ — `kern.num_vnodes` minus `kern.free_vnodes` — reach the limit: nothing is left to
recycle, and a mount that takes a second can outlast the deadline. When that is the state of the host the refusal names
it (`… vnodes in use of kern.maxvnodes …`) and its hint is the limit to set; `cowshed doctor` reports it as
`vnode-table-saturated`. Raising the limit is the operator's call: `sudo sysctl kern.maxvnodes=<n>`.

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
  artifact over manually (`cowshed sim export` + your side's `simctl install`).
- **`cowshed: sim broker unreachable` (exit 5).** The session broker is a launchd agent in the _personal_ GUI session —
  it isn't running if nobody is logged in or the agent isn't loaded; the `next:` hint names the `launchctl` kickstart.
  Exit 5 (environment) is deliberately distinct from exit 6 (a denial: missing `--sim` grant, non-drop-dir install,
  unregistered URL scheme).
- **`install` refused despite a `--sim install` grant.** The broker only installs drop-dir artifacts and only under the
  human-gating rule — that refusal is the design, not a bug (ios.md explains why: simulator apps run as _you_).

## Desktop apps (posture B — see desktop.md)

- **"I want the app running as dev but visible in my session."** macOS can't show one uid's window in another's session
  (Screen Sharing streams a whole session, it doesn't relocate a window). Pick a lane: test/debug as dev (view via
  Screen Sharing into dev's session), or `cowshed app promote` and run it as yourself.
- **Gatekeeper blocks a promoted app.** It's ad-hoc-signed and `promote` needed `--force`. Sign with Developer-ID on the
  dev side (dev holds the signing identity) so it installs and launches cleanly; or right-click-open once.
- **An agent can't launch a desktop app in my session.** Correct — there is no agent verb for it (unlike `--sim`, there
  is deliberately no `--app open`). Agents test desktop apps as dev in dev's session; only the human `promote`s.
