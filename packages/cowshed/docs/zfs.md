# cowshed on ZFS (Linux)

On Linux, cowshed workspaces are ZFS datasets instead of disk images. Commands, grants, gateway policy, and cache
semantics match macOS, while enforcement and data-plane topology are platform-specific. On ZFS almost everything gets
faster: there is no attach step and no fsck pass.

cowshed picks the substrate by looking at the filesystem your project root sits on: APFS → image substrate, ZFS → zfs
substrate.

Cowshed first uses `statfs` on the project root. APFS selects images. ZFS is selected only when a cowshed root that
names you (`cowshed:owner=<your uid>`, set by whoever administers its parent dataset) lies in the containing dataset's
chain. If `statfs` cannot identify one, configure both `[substrate] kind = "zfs"` and `parent = "<parent>"` in
`.cowshed.toml`, where `<parent>` is the pool or any dataset in it and `<parent>/cowshed` is the root; cowshed never
scans imported pools or silently chooses one. A Linux host with no such root gets a directory store instead: each
workspace is a real directory on whatever filesystem holds the store, with no capacity, no clone and no volume of its
own. A project's `substrate` (the `project.open` answer, `Project.substrate` in the library) says which one its store
is: `apfs`, `zfs` or `directory`.

## What lives where

| What                                                     | Where                                                                                               |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Main workspace                                           | dataset `<pool>/cowshed/projects/<owner>/<repo>/main`, mounted at the original path                 |
| Session workspaces                                       | `<pool>/cowshed/projects/<owner>/<repo>/ws/<name>`, mounted at `<mount-root>/<owner>/<repo>/<name>` |
| Shared caches (Cargo, zig, Gradle, Go, Nix)              | each tool's own default in your HOME, e.g. `~/.cargo/registry`, `~/.cache/zig`, `~/go/pkg/mod`      |
| Gateway registry/repository mirrors                      | `$XDG_CACHE_HOME/cowshed/{mirror,repo-mirrors}` (default `~/.cache/cowshed`); gateway-only          |
| Bindings, trusted policy, grants, waivers, gateway state | `<pool>/cowshed/store` at `/private/cowshed/store`; policy is `<owner>/<repo>/policy.json`          |
| Telemetry + gateway audit (Arrow segments)               | `/private/cowshed/store/telemetry/` — same as macOS                                                 |
| Linux gateway data plane                                 | per-incarnation Unix socket plus private-netns connector at `127.0.0.1:7644`; no `portBlock`        |
| Secrets                                                  | one 0600 record file per binding in the 0700 `credentials` directory of cowshed's support directory |

Mountpoints are ZFS properties and normally return at pool import. cowshed still writes the same underlying stub
`.envrc` as APFS because an unimported pool also makes a mount disappear; `cowshed attach` verifies the mount, identity,
wiring, and grants and repairs it when possible.

The dataset hierarchy is fixed, with two siblings:

```text
<pool>/cowshed
├── store       mountpoint=/private/cowshed/store
└── projects    mountpoint=none
    └── <owner>/<repo>/{main,ws/...}
```

The primary `repo_id` is stable lowercase `owner/repo`, normalized from the explicitly selected remote and validated
against its recorded URL. Its components are encoded independently for filesystem and dataset use. Discovery proposes
candidates but never silently selects or mints one; local-only repositories require `--repo-id owner/repo`.

## Setup

One root dataset, once, as root. It is named `cowshed` and may sit directly in the pool or below any dataset of it, and
`cowshed:owner` names the uid cowshed serves there:

```sh
zfs create -o mountpoint=none -o cowshed:owner=$(id -u <user>) rpool/cowshed
```

Every change cowshed makes below it goes through `cowshed-helper`, a small root binary with a fixed verb set (`create`,
`clone`, `snapshot`, `destroy`, `rename`, `promote`, `mount`, `unmount`, `set`, and `check`, which changes nothing).
`zfs allow` cannot stand in for it: Linux ignores ZFS _mount_ delegation (`mount(2)` wants `CAP_SYS_ADMIN`), and inside
a container whose datasets are zoned to its user namespace delegation grants nothing. The helper refuses any dataset
outside the root, any root whose `cowshed:owner` is not you, and any mountpoint outside a directory you own; reads
(`zfs list`, `zfs get`) are unprivileged.

The helper ships beside `cowshed` in every build, and `cowshed setup` installs it beside the installed cowshed
(`~/Library/Application Support/dev.cowshed/bin/cowshed-helper`). That copy is yours, not root's, so cowshed runs it
through `sudo -n`, and the gateway daemon has no terminal to type a password at: a sudoers entry has to admit it.
`cowshed setup` checks this once the helper is installed, by running `cowshed-helper <root> check` the way the daemon
will, and when the check fails it exits non-zero with the one command to run as root on its `next:` line:

```
$ cowshed setup
…
cowshed: …/bin/cowshed-helper did not act as root for the ZFS root rpool/cowshed, so nothing can change its datasets: … sudo: a password is required; …
next: printf '%s\n' '#1000 ALL=(root) NOPASSWD: sha256:<digest> /home/you/Library/Application\ Support/dev.cowshed/bin/cowshed-helper' | sudo sh -c 'umask 227 && cat >/etc/sudoers.d/cowshed-helper-1000.new && visudo -cf /etc/sudoers.d/cowshed-helper-1000.new && mv /etc/sudoers.d/cowshed-helper-1000.new /etc/sudoers.d/cowshed-helper-1000' && cowshed setup
```

The entry admits exactly the installed helper: it names its path and pins its SHA-256, because that path is in a
directory you can write and an entry naming the path alone would make root of any file put there. `visudo` checks the
entry before it takes effect. A build that changes the helper changes the digest, and that build's setup prints the
command again. On a host that reads no `/etc/sudoers.d` (NixOS), add the same entry, printed in the message, through its
own sudoers configuration, or install the helper setuid root in a root-owned directory on `PATH` (a `security.wrappers`
entry on NixOS): cowshed runs a setuid root helper directly, in preference to the installed one.

Then, from your checkout:

```sh
cd <project-root>
cowshed adopt --remote origin
```

Adopt on ZFS copies the tree once into `rpool/cowshed/projects/acme/widget/main` and sets `mountpoint=<project-root>`.
Same path before and after, like macOS.

## What's different from macOS (all of it pleasant)

```
$ cowshed fork raven
cowshed: snapshot rpool/cowshed/projects/acme/widget/main@cowshed:raven
cowshed: cloned to ws/raven (copy-on-write)
cowshed: branch cowshed/raven created from main @ 6f3a2c1
next: cowshed exec raven -- <cmd>
<mount-root>/acme/widget/raven
```

- **`cowshed fork` is tens of milliseconds.** Snapshot + clone are O(1) metadata operations, and the clone mounts as
  part of creation. No 250 ms attach, and no fsck pass — ZFS snapshots are transactionally consistent by construction,
  so the crash-consistency dance the image substrate does (sync, clone, verify) simply doesn't exist.
- **Quotas are real:** each workspace dataset carries a `refquota` instead of a sparse-image capacity cap.
  `cowshed adopt --capacity <size>` sets main's, every clone keeps its source's, and `cowshed resize <ws> <size>` (and
  `--build` for the build volume and its seed) raises it in place, mounted, through the helper's `set` verb. Like the
  image's, it only grows. `df` on the workspace shows it: the dataset's size is its `refquota`.
- **Nothing fragments, nothing needs reclaiming.** `cowshed defrag` and `cowshed adopt --re-adopt` exist to rewrite and
  replace an image file. A dataset has none, so both are refused before anything is stopped, as a `usage` error whose
  `unsupported` field names the substrate and the operation
  (`{ "substrate": "zfs", "operation": "defragmentWorkspace" }`). A directory store refuses `resize` the same way,
  having no capacity to raise.
- **Job volume samples are the dataset's.** A job's `volumes` reading is the change in the bytes its workspace dataset
  and its build dataset reference, from `statvfs` on each mountpoint, as of the pool's last transaction group. A
  directory store's workspace is no filesystem of its own, so its samples say `unconfigured`.
- **Dev servers bind their default ports.** Every exec joins a private-loopback network namespace, so
  vite/metro/`devenv up` listen on their usual ports with no cross-workspace collisions — the macOS port-block scheme
  isn't needed for isolation here. Linux sidecars omit `portBlock`; there is no synthetic base and no
  `COWSHED_PORT_BASE` or `COWSHED_PORT_BLOCK_SIZE`. Ordinary package and proxy clients use the namespace-local trusted
  connector at `http://127.0.0.1:7644`; it binds only that loopback address and forwards unchanged bytes only to the
  workspace's mounted per-incarnation Unix gateway socket. The socket inode plus netns remains identity, and detach
  drains and removes the connector before releasing the old attachment.

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
next: cowshed land raven   # if it's done
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
`cowshed attach` mounts it and re-mints grants at the closed baseline — grants never travel, by design.

## ZFS on macOS?

If you run OpenZFS on OS X, the zfs substrate works there too — cowshed only cares that `zfs` answers. But it is never
the default on macOS: o3x is a kernel extension, which on Apple Silicon means reduced-security boot policy, and cowshed
won't steer your daily driver there. APFS images remain the blessed macOS substrate; ZFS is the blessed Linux one.
