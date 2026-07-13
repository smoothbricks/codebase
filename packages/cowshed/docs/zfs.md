# cowshed on ZFS (Linux)

On Linux, cowshed workspaces are ZFS datasets instead of disk images. Commands, grants, gateway policy, and cache
semantics match macOS, while enforcement and data-plane topology are platform-specific. On ZFS almost everything gets
faster: there is no attach step, no fsck pass, and checkpoints are literal `zfs snapshot`s.

cowshed picks the substrate by looking at the filesystem your project root sits on: APFS → image substrate, ZFS → zfs
substrate.

Cowshed first uses `statfs` on the project root. APFS selects images. ZFS is selected only when the containing pool has
a suitable delegated cowshed root. If `statfs` cannot identify a suitable ZFS dataset, configure both
`[substrate] kind = "zfs"` and `pool = "<pool>"` in `.cowshed.toml`; cowshed never scans imported pools or silently
chooses one.

## What lives where

| What                                                     | Where                                                                                           |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Main workspace                                           | dataset `<pool>/cowshed/projects/<owner>/<repo>/main`, mounted at the original path             |
| Session workspaces                                       | `<pool>/cowshed/projects/<owner>/<repo>/ws/<name>`, mounted at `~/.cowshed/mnt/<owner>/<repo>/` |
| Checkpoints                                              | `zfs snapshot`s on the workspace dataset                                                        |
| Shared caches (Cargo, sccache, zig, Gradle, Go, Nix)     | `<pool>/cowshed/caches`, mounted at `~/.cowshed/caches`                                         |
| Gateway registry/repository mirrors                      | `mirror/` and `repo-mirrors/` on caches; gateway-owned, sandbox-read-only                       |
| Bindings, trusted policy, grants, waivers, gateway state | `<pool>/cowshed/store` at `~/.cowshed`; policy is `<owner>/<repo>/policy.json`                  |
| Telemetry + gateway audit (Arrow segments)               | `~/.cowshed/telemetry/` (`cowshed logs`/`audit`/`trace`) — same as macOS                        |
| Linux gateway data plane                                 | per-incarnation Unix socket plus private-netns connector at `127.0.0.1:7644`; no `portBlock`    |
| Secrets                                                  | secret-service (GNOME Keyring/KWallet), service `dev.cowshed.gateway`                           |

Mountpoints are ZFS properties and normally return at pool import. cowshed still writes the same underlying stub
`.envrc` as APFS because an unimported pool also makes a mount disappear; `cowshed ensure` verifies the mount, identity,
wiring, and grants and heals it when possible.

The dataset hierarchy is fixed, with three siblings:

```text
<pool>/cowshed
├── store       mountpoint=~/.cowshed
├── caches      mountpoint=~/.cowshed/caches
└── projects    mountpoint=none
    └── <owner>/<repo>/{main,ws/...}
```

The primary `repo_id` is stable lowercase `owner/repo`, normalized from the explicitly selected remote and validated
against its recorded URL. Its components are encoded independently for filesystem and dataset use. Discovery proposes
candidates but never silently selects or mints one; local-only repositories require `--repo-id owner/repo`.

## Setup

One dataset delegation, once, as root:

```sh
zfs create -o mountpoint=none rpool/cowshed
zfs allow <user> create,snapshot,clone,destroy,send,receive,userprop rpool/cowshed
```

Linux ignores ZFS _mount_ delegation (`mount(2)` wants `CAP_SYS_ADMIN`), so cowshed ships a minimal root helper for
exactly the mount/unmount calls — installed by the NixOS module (`services.cowshed-runner` or `programs.cowshed`) or
`cowshed doctor` prints the manual install line. The helper refuses anything outside `<pool>/cowshed`; every other
cowshed operation is unprivileged. If the helper is missing, commands that need it exit 5 with the install hint on
stderr.

Then, from your checkout:

```sh
cd <project-root>
cowshed adopt --remote origin
```

Adopt on ZFS copies the tree once into `rpool/cowshed/projects/acme/widget/main` and sets `mountpoint=<project-root>`.
Same path before and after, like macOS.

## What's different from macOS (all of it pleasant)

```
$ cowshed new raven
cowshed: snapshot rpool/cowshed/projects/acme/widget/main@cowshed:raven
cowshed: cloned to ws/raven (copy-on-write)
cowshed: branch cowshed/raven created from main @ 6f3a2c1
next: cowshed shell raven
~/.cowshed/mnt/acme/widget/raven
```

- **`cowshed new` is tens of milliseconds.** Snapshot + clone are O(1) metadata operations, and the clone mounts as part
  of creation. No 250 ms attach, and no fsck pass — ZFS snapshots are transactionally consistent by construction, so the
  crash-consistency dance the image substrate does (sync, clone, verify) simply doesn't exist.
- **`cowshed checkpoint raven before-refactor` is instant and pinned.** Publication first crosses the same supervisor
  barrier as APFS: complete Arrow batches and protected spill files are sealed, and a manifest commits every
  checkpoint-resident job byte. Small terminal streams may remain inline in Arrow, so checkpointing does not force one
  output file per stream. Every supplied label and `--keep` creates an explicit pin. GC retains all pinned checkpoints,
  all checkpoints younger than 14 days, and always the newest five per workspace; only an explicit unpin makes a pinned
  checkpoint eligible.
- **`cowshed restore`** is a fenced clone-swap by default: drain the supervisor and trusted connector, prepare and
  verify the hidden replacement, mint the new incarnation then its token, swap, mount and validate, create its new Unix
  gateway socket/netns connector, atomically publish detached metadata, revoke the old token, then admit jobs. Protected
  job content is authoritative only for its origin snapshot; controller commitments carry lifecycle/order/lineage and
  hashes across the new-incarnation fence without duplicating raw bytes. Failure before publication restores the old
  dataset/token; after publication recovery completes forward. Recovery may discard only incomplete trailing job data; a
  missing manifest entry or hash mismatch is an integrity failure. `--discard-newer` may use `zfs rollback`, but follows
  the identical fence and ordering.
- **Quotas are real:** each workspace dataset carries a `refquota` instead of a sparse-image capacity cap.
- **Dev servers bind their default ports.** Every exec joins a private-loopback network namespace, so
  vite/metro/`devenv up` listen on their usual ports with no cross-workspace collisions — the macOS port-block scheme
  isn't needed for isolation here. Linux sidecars omit `portBlock`; there is no synthetic base and no
  `COWSHED_PORT_BASE`. Ordinary package and proxy clients use the namespace-local trusted connector at
  `http://127.0.0.1:7644`; it binds only that loopback address and forwards unchanged bytes only to the workspace's
  mounted per-incarnation Unix gateway socket. The socket inode plus netns remains identity, and detach/restore drain
  and remove the connector before releasing the old attachment.

## The origin-snapshot lifecycle

One honest difference from APFS: a ZFS clone depends on the snapshot it was cloned from. cowshed contains this
completely — every workspace owns exactly one origin snapshot (`main@cowshed:<name>`), and `cowshed rm` destroys clone
and origin together. Main is never blocked: you keep working in it, snapshots just pin the blocks that existed at clone
time.

Forks extend the same rule one level: `cowshed fork raven raven-alt` snapshots _raven_
(`ws/raven@cowshed:fork-raven-alt`), so the fork's origin lives on its source workspace. While a fork exists,
`cowshed rm raven` exits 4 naming the dependent forks — remove (or land) them first. cowshed never uses `zfs promote` to
shuffle the dependency away; the graph stays explicit so teardown order is always knowable.

The cost is space, not semantics: a long-lived workspace pins main's old blocks as main diverges. `cowshed doctor` shows
it per workspace:

```
cowshed: raven pins 1.2g of main@cowshed:raven (workspace 9 days old)
next: cowshed push raven && cowshed rm raven   # if it's done
```

`cowshed gc` prunes `cowshed:*` snapshots whose workspace is gone (crash leftovers).

## send/receive: warm workspaces travel

The image substrate has no answer to this; ZFS makes it native. cowshed's dataset naming is stable, so plain zfs tooling
composes with it.

Back up main incrementally (this is real backup, unlike macOS where the store volume is excluded from backup and git is
the durability story):

```sh
zfs snapshot rpool/cowshed/projects/acme/widget/main@backup-2026-07-11
zfs send -i @backup-2026-07-04 rpool/cowshed/projects/acme/widget/main@backup-2026-07-11 |
  ssh nas zfs receive -u tank/backup/widget
```

Ship a warm workspace to another cowshed host that already has main (a fleet box, a beefier builder). Only the
divergence travels:

```sh
zfs snapshot rpool/cowshed/projects/acme/widget/ws/raven@ship
zfs send -i main@cowshed:raven rpool/cowshed/projects/acme/widget/ws/raven@ship |
  ssh builder zfs receive rpool/cowshed/projects/acme/widget/ws/raven
```

On the receiving side, `cowshed ls` sees the dataset immediately (there is no database to register it in);
`cowshed ensure` mounts it and re-mints grants at the closed baseline — grants never travel, by design.

## ZFS on macOS?

If you run OpenZFS on OS X, the zfs substrate works there too — cowshed only cares that `zfs` answers. But it is never
the default on macOS: o3x is a kernel extension, which on Apple Silicon means reduced-security boot policy, and cowshed
won't steer your daily driver there. APFS images remain the blessed macOS substrate; ZFS is the blessed Linux one.
