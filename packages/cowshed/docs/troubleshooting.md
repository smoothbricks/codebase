# Troubleshooting

First move for anything weird: `cowshed doctor`. It checks every invariant (images ↔ markers ↔ mounts ↔ grants, caches
volume, gateway, autosave freshness) and prints one `cowshed:` line per problem with a `next:` fix. Because cowshed has
no database, doctor isn't reconciling state — it's _deriving_ it from disk and the mount table, so what it reports is
the truth.

## Mounts

**Workspace missing after reboot.** Mounts don't survive reboots; images do. Any cowshed command heals on contact — most
naturally:

```
$ cowshed ensure          # in an .envrc: this already ran when you cd'd in
cowshed: reattached conloca/raven (image was healthy, mount was gone)
```

Adopted main workspaces self-heal through the stub `.envrc` cowshed wrote _underneath_ the mountpoint during `adopt`:
when unmounted, `cd ~/Dev/conloca` hits the stub, which runs `cowshed ensure --attach`; the real `.envrc` shadows the
stub once mounted, and direnv reloads. You `direnv allow` each file once. The login LaunchAgent attaches permanent
workspaces proactively; `ensure` is the belt-and-braces.

**Finder ejected a volume** (or `hdiutil detach` by hand): same story — `cowshed ensure`, or just run the command you
wanted; `exec`/`shell`/`path` all verify the mount first and reattach.

**direnv says `.envrc is blocked` in a workspace.** Shouldn't happen: direnv trust is keyed by absolute path, so cowshed
trusts each clone's `.envrc` at `new`/`fork`/`restore` and re-asserts it in `ensure` healing. If you see it anyway (e.g.
the allow entry was cleaned), `cowshed ensure` fixes it; `direnv allow` works too. devenv inside workspaces needs
multi-user (daemon) Nix — `cowshed doctor` flags single-user installs.

**`cowshed adopt` or `cowshed push` refused with exit 4 naming files.** The secrets gate found credential-shaped content
(`.env*`, key files, known token prefixes, `.envrc` secret exports). For adopt: move each value into the gateway
Keychain (see gateway.md) and delete the file, or `cowshed adopt --quarantine` to relocate findings outside the image so
dependent tooling fails loudly. For push: the offending hunks are named — remove the secret and push again; autosave
meanwhile skips (never propagates findings) and warns. False positive? Add a reasoned waiver (shown by `cowshed doctor`
forever after) rather than working around the gate.

**Attach fails.** `cowshed doctor` distinguishes: image file missing (exit 3 — was it `cowshed rm`'d from another
session?), image fails verification (restore a checkpoint or re-clone from main), or mountpoint dir occupied by real
files (exit 4 — something wrote to the unmounted path; cowshed never deletes those files for you; move them and re-run).

## Sandbox denials (exit 6)

When cowshed reports exit 6, it comes with the diagnosis and the fix on stderr:

```
cowshed: sandbox denied file-write /Users/danny/Dev/other-repo/gen.lock
next: cowshed grant raven --write /Users/danny/Dev/other-repo
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
- **`go` denied writing `~/go`**: that deny is a deliberate tripwire, not a bug — it means a go invocation ran without
  the workspace's `GOENV` wiring (an unwrapped spawn, or an editor without direnv integration). Run it through
  `cowshed exec`/a direnv shell, or fix the editor's direnv plugin; never grant `~/go`. `cowshed doctor` prints the same
  hint, and checks the host for a stray `~/go` that predates adoption (safe to delete — it is only cache).
- **Denial persists after a grant**: filesystem grants apply from the _next_ exec; a long-running process (watcher, dev
  server) keeps its launch-time profile. Restart that process.

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
sizes" overstates real usage. `df -h ~/.cowshed/caches` covers the shared cache volume; it shares the container's free
space with everything else.

## Path-sensitive caches (why a fresh workspace rebuilds more than expected)

Cargo incremental state and Xcode DerivedData key on **absolute paths**. Main (fixed path) reuses them perfectly; a
workspace at `~/.cowshed/mnt/...` does not, so first builds there redo path-keyed work even though everything else is
warm. This is physics, not breakage. Mitigations, in order: let sccache absorb it (shared, path-tolerant for most rustc
invocations; already wired); add `--remap-path-prefix`/`trim-paths` to your cargo config if the rebuild tax bothers you;
keep long-lived personal workspaces (their own paths stay stable, so their incremental state stays valid).
`bun install`, `node_modules`, zig, and gradle caches are path-independent — unaffected.

## Backup and durability (read once, remember forever)

**The store and caches volumes are excluded from backup** — deliberately. Multi-gigabyte images with constant internal
churn would bloat every backup (and, on the Data volume, every hourly local snapshot — that is why they live on
dedicated volumes at all; see 01_storage.md), and everything inside them is either reproducible (caches, builds) or
better protected by git. The durability contract:

- Committed + pushed (`cowshed push`, or merged in main): it's in main's repo — and main's off-machine durability is its
  **origin remote**, exactly as before adoption. Keep pushing main to origin as usual; the store volume is not a backup.
- Committed, unpushed: the autosave agent (host-side, like `push`) fetches every workspace into `refs/cowshed/<ws>/wip`
  every 10 minutes.
- **Uncommitted work is at risk between autosaves.** `cowshed doctor` warns when any workspace's autosave is stale.

Restoring a machine: clone main's repo from its origin remote, `cowshed adopt` again; workspaces are recreated from
their saved branches (`cowshed new x --ref refs/cowshed/x/wip`). Checkpoints and images are not backup artifacts — never
treat them as one.

## When cowshed itself misbehaves

`cowshed doctor --json` is the bug-report payload: it includes versions, invariant results, and the last few operations
from the telemetry store (`cowshed logs --since 1h` shows the same thing). State is fully derivable, so the nuclear
option is safe and small: detach everything (`cowshed detach` per workspace), and every subsequent command re-derives
reality. There is no cache to clear and no database to reset.

For cache-volume corruption specifically there is a bigger, equally safe hammer: nothing unique lives on
`cowshed.caches`, so `diskutil apfs deleteVolume` and letting cowshed lazily recreate it is always an option — the
mirror refetches, sccache and registries rebuild. `cowshed doctor` suggests it when the caches volume fails its checks.
(Never do this to `cowshed.store` — that volume holds your images.)

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
