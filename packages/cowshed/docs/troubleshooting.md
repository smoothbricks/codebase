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
`cd <project-root>` exposes that stub, which runs `cowshed attach`; after the real workspace is mounted its own
`.envrc` shadows the stub and direnv reloads. Cowshed does **not** authorize either file: run `direnv allow` once at
each workspace path.

Devenv's hook cannot activate from the bare mountpoint stub. For a devenv-native repository, run `cowshed attach`,
then run the repository's `devenv:allow` command once at that workspace path. The mounted image's `.envrc` sources
`.cowshed/env`; no command prints those exports on demand. Cowshed never reads or writes devenv's trust database.
The login LaunchAgent may attach permanent workspaces proactively, but explicit `attach` remains the recovery command.

**Finder ejected a volume** (or `hdiutil detach` by hand): use the same explicit `cowshed attach` recovery.

**direnv says `.envrc is blocked` in a workspace.** This is expected until that clone path is authorized. Run
`direnv allow`; `cowshed attach` repairs mounts but deliberately does not change trust.

**devenv refresh fails during `cowshed exec`.** With `[devenv] dir` in `.cowshed.toml` (or a root `devenv.nix`), cowshed
watches the configuration inputs and refreshes the environment before the next sandbox process. A missing configured
`devenv.nix`, missing `devenv` executable, or evaluation error fails closed with exit 5 and devenv's stderr; cowshed
never reuses a stale snapshot. Existing long-running processes keep their launch environment.

**`cowshed adopt` or `cowshed push` refused with exit 4 naming files.** The secrets gate found credential-shaped content
(`.env*`, key files, known token prefixes, `.envrc` secret exports). For adopt: move each value into the gateway
Keychain (see gateway.md) and delete the file, or `cowshed adopt --quarantine` to relocate findings outside the image so
dependent tooling fails loudly. For push: the offending hunks are named — remove the secret and push again; autosave
meanwhile skips (never propagates findings) and warns. False positive? Add a reasoned waiver (shown by `cowshed doctor`
forever after) rather than working around the gate.

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
filesystem denials when the kernel sandbox telemetry can be correlated to your command. A denial deep in a child process
may instead surface as the child's own nonzero exit, passed through unchanged — when a failure smells like the sandbox
but there was no exit 6, check the raw Seatbelt log around the failure:

```sh
log show --last 2m --predicate 'sender == "Sandbox"' | grep deny
```

and the gateway audit events for egress (`cowshed audit --denied | tail`). Common cases:

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

**Checkpoint was not pruned.** GC keeps the union of three sets: explicit pins, every checkpoint younger than 14 days,
and the newest five checkpoints per workspace. A user label and `cowshed checkpoint --keep` both pin; age or count does
not override a pin. Unpin explicitly before expecting GC to remove it.

## Disk usage

Images are sparse and grow with churn; deleted files inside a volume don't shrink the image file until compaction.
`cowshed gc` compacts detached images, removes orphans, and prunes expired checkpoints:

```
$ cowshed gc
cowshed: compacted fox.asif 18.2g -> 6.1g
cowshed: pruned 3 checkpoints of rm'd workspaces (41.0g)
next: cowshed ls   # nothing live was touched
```

Attribution: `du` on the images directory tells you per-workspace cost; _inside_ a mounted workspace, normal `du` works
— it's just APFS. Remember clones share extents: ten fresh workspaces cost ~zero until they diverge, so "sum of image
sizes" overstates real usage. `df -h /private/cowshed/caches` covers the shared cache volume; it shares the container's free
space with everything else.

Cargo's shared writable caches are `/private/cowshed/caches/cargo/{registry,git}`; gateway-owned bare repository mirrors are
separate at `/private/cowshed/caches/repo-mirrors` and must remain sandbox-read-only.

**A sandbox refetches a crate the host already has.** `$CARGO_HOME` follows the private `HOME`, so each workspace has
its own `.cargo`. Its `registry/index` and `registry/cache` are symlinks to the host's, read-only, and `registry/src` —
where cargo unpacks — is real and writable inside the mount. A crate already downloaded on the host therefore builds
offline in a workspace. If those links are missing, the exec predates them or the host has no `~/.cargo/registry` yet;
the next `cowshed exec` plants them. A real directory sitting where a link belongs is left alone on purpose: that is a
workspace's own registry state, and cowshed will not delete it to share the host's.

**Nix cache/state points at the host filesystem.** On declarative hosts the module must own
`~/.cache/nix → /private/cowshed/caches/nix/cache` and `~/.local/state/nix → /private/cowshed/caches/nix/state`; `adopt` and
`doctor` only validate. Fix the declarative configuration rather than allowing cowshed to mutate it. The explicit
`cowshed adopt --imperative-host-setup` fallback is only for a host with no supported declarative owner; it is never an
automatic recovery from mixed or broken ownership.

## Path-sensitive caches (why a fresh workspace rebuilds more than expected)

Cargo incremental state and Xcode DerivedData key on **absolute paths**. Main (fixed path) reuses them perfectly; a
workspace at `<mount-root>/<owner>/<repo>/<workspace>` does not, so first builds there redo path-keyed work even though everything else is
warm. This is physics, not breakage. Mitigations, in order: let sccache absorb it (shared, path-tolerant for most rustc
invocations; already wired); add `--remap-path-prefix`/`trim-paths` to your cargo config if the rebuild tax bothers you;
keep long-lived personal workspaces (their own paths stay stable, so their incremental state stays valid).
`bun install`, `node_modules`, zig, and gradle caches are path-independent — unaffected.

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

A ZFS host uses exactly three sibling datasets under the configured root: `<pool>/cowshed/store` at `/private/cowshed/store`,
`<pool>/cowshed/caches` at `/private/cowshed/caches`, and `<pool>/cowshed/projects` for `<owner>/<repo>/{main,ws/...}`. If
`statfs` does not locate a suitable delegated ZFS dataset, configure `[substrate] kind = "zfs"` and `pool = "<pool>"`;
cowshed deliberately refuses to scan pools or guess. `cowshed doctor` reports the selected pool and any missing sibling,
mountpoint, or delegation.

**Restore interrupted.** Before detached metadata publication, recovery restores the displaced workspace, old
incarnation, and old token. After publication, recovery completes the replacement forward; it never rolls back across
the incarnation fence. A healthy restore always drains the old supervisor, stages and verifies the replacement, mints
the new incarnation then token, swaps and mounts, publishes metadata atomically, revokes the old token, and only then
admits a supervisor or job. No state should accept both tokens; `cowshed doctor` reports a publication mismatch.

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
(`rm -rf /private/cowshed/store/<owner>/<repo>` without ever running a verb against it again), and the host-global port-block
allocator has since handed that block to a new workspace. Reconcile — which every `exec`, `attach`, and `doctor` runs
first — evicts such a session itself once the host inventory confirms no live workspace anywhere still carries that
identity, then installs the workspace; the message does not recur. If `cowshed doctor --json` instead refuses with
`gateway endpoint 127.0.0.1:<base> is assigned to workspace <id> by this project and still claimed by live workspace <id> of another project`,
two live workspaces hold one block: that is an inventory fault, not a stale session, and cowshed never resolves it by
evicting a live session. Retire one of the two (`cowshed rm`, or `detach` and re-create it so it takes a fresh block)
and rerun `doctor`. One workspace that cannot be installed no longer stops the rest of the project from being installed;
the error names every failed identity.

## `cowshed ls` takes tens of seconds; `cowshed new` or `doctor` takes a minute

Per-command cost does not grow with history or with the number of warm workspaces: no command reads the audit segments
under `/private/cowshed/store/telemetry/` (they are write-only telemetry; authority is the image inventory); the host APFS
inventory is queried only for the cowshed container, and attaching an image lists only that image's container; one project
open validates the repository binding and reads the inventory once for every workspace it recovers. When a command is
still slow, the wait is in a host process, not in cowshed — while it runs,
`ps -o pid,ppid,etime,args -ax | grep -E 'diskutil|hdiutil|git'` names it. The first `diskutil mount` of a freshly
cloned image takes seconds on a host with dozens of attached images (the same volume re-mounts in a fraction of that
after publication); that is DiskArbitration's cost per attached image, so `cowshed rm` what you no longer need and
`cowshed gc` the trash.

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
