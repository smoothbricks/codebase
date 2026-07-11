# cowshed on ZFS (Linux)

On Linux, cowshed workspaces are ZFS datasets instead of disk images. The commands, the sandbox grants, the gateway, the
cache model — all identical to macOS. What changes is the substrate, and on ZFS almost everything gets faster: there is
no attach step, no fsck pass, and checkpoints are literal `zfs snapshot`s.

cowshed picks the substrate by looking at the filesystem your project root sits on: APFS → image substrate, ZFS → zfs
substrate. Override in `.cowshed.toml` (`[substrate] kind = "zfs"`) only if you need to fight the convention.

## What lives where

| What                                                               | Where                                                                                 |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| Main workspace                                                     | dataset `<pool>/cowshed/<project_id>/main`, mounted at the original path              |
| Session workspaces                                                 | `<pool>/cowshed/<project_id>/ws/<name>`, mounted under `~/.cowshed/mnt/<project_id>/` |
| Checkpoints                                                        | `zfs snapshot`s on the workspace dataset                                              |
| Shared caches (sccache, zig, gradle, Go mod/build, gateway mirror) | `<pool>/cowshed/caches`, mounted at `~/.cowshed/caches`                               |
| Grants, waivers, quarantine, gateway state                         | `<pool>/cowshed/store` dataset at `~/.cowshed` — same path as macOS                   |
| Telemetry + gateway audit (Arrow segments)                         | `~/.cowshed/telemetry/` (`cowshed logs`/`audit`/`trace`) — same as macOS              |
| Secrets                                                            | secret-service (GNOME Keyring/KWallet), service `dev.cowshed.gateway`                 |

Mountpoints are ZFS properties, so they survive reboots without a LaunchAgent, a stub `.envrc`, or any healing:
`zfs-mount.service` mounts everything at boot exactly where cowshed put it. `cowshed ensure` still runs in your `.envrc`
— it verifies wiring and grants — it just has less to heal here.

## Setup

One dataset delegation, once, as root:

```sh
zfs create -o mountpoint=none rpool/cowshed
zfs allow danny create,snapshot,clone,destroy,send,receive,userprop rpool/cowshed
```

Linux ignores ZFS _mount_ delegation (`mount(2)` wants `CAP_SYS_ADMIN`), so cowshed ships a minimal root helper for
exactly the mount/unmount calls — installed by the NixOS module (`services.cowshed-runner` or `programs.cowshed`) or
`cowshed doctor` prints the manual install line. The helper refuses anything outside `<pool>/cowshed`; every other
cowshed operation is unprivileged. If the helper is missing, commands that need it exit 5 with the install hint on
stderr.

Then, from your checkout:

```sh
cd ~/dev/conloca
cowshed adopt
```

Adopt on ZFS copies the tree once into `rpool/cowshed/conloca-3f2a9c1b/main` and sets
`mountpoint=/home/danny/dev/conloca`. Same path before and after, like macOS — minus the stub-`.envrc` dance, because
ZFS remounts it at boot by itself.

## What's different from macOS (all of it pleasant)

```
$ cowshed new raven
cowshed: snapshot rpool/cowshed/conloca-3f2a9c1b/main@cowshed:raven
cowshed: cloned to ws/raven (copy-on-write)
cowshed: branch cowshed/raven created from main @ 6f3a2c1
next: cowshed shell raven
/home/danny/.cowshed/mnt/conloca-3f2a9c1b/raven
```

- **`cowshed new` is tens of milliseconds.** Snapshot + clone are O(1) metadata operations, and the clone mounts as part
  of creation. No 250 ms attach, and no fsck pass — ZFS snapshots are transactionally consistent by construction, so the
  crash-consistency dance the image substrate does (sync, clone, verify) simply doesn't exist.
- **`cowshed checkpoint raven before-refactor` is instant and free** — it's a snapshot, not an image copy. Checkpoint as
  often as you like.
- **`cowshed restore`** clone-swaps by default (later checkpoints survive); `--discard-newer` does a `zfs rollback`
  instead when you genuinely want history truncated.
- **Quotas are real:** each workspace dataset carries a `refquota` instead of a sparse-image capacity cap.
- **Dev servers bind their default ports.** Every exec joins a private-loopback network namespace, so
  vite/metro/`devenv up` listen on their usual ports with no cross-workspace collisions — the macOS port-block scheme
  isn't needed for isolation here. `PORT` and `COWSHED_PORT_BASE` are still exported for cross-platform parity.

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
zfs snapshot rpool/cowshed/conloca-3f2a9c1b/main@backup-2026-07-11
zfs send -i @backup-2026-07-04 rpool/cowshed/conloca-3f2a9c1b/main@backup-2026-07-11 |
  ssh nas zfs receive -u tank/backup/conloca
```

Ship a warm workspace to another cowshed host that already has main (a fleet box, a beefier builder). Only the
divergence travels:

```sh
zfs snapshot rpool/cowshed/conloca-3f2a9c1b/ws/raven@ship
zfs send -i main@cowshed:raven rpool/cowshed/conloca-3f2a9c1b/ws/raven@ship |
  ssh builder zfs receive rpool/cowshed/conloca-3f2a9c1b/ws/raven
```

On the receiving side, `cowshed ls` sees the dataset immediately (there is no database to register it in);
`cowshed ensure` mounts it and re-mints grants at the closed baseline — grants never travel, by design.

## ZFS on macOS?

If you run OpenZFS on OS X, the zfs substrate works there too — cowshed only cares that `zfs` answers. But it is never
the default on macOS: o3x is a kernel extension, which on Apple Silicon means reduced-security boot policy, and cowshed
won't steer your daily driver there. APFS images remain the blessed macOS substrate; ZFS is the blessed Linux one.
