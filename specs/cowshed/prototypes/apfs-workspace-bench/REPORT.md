Project created

```text
  /Users/danny/Dev/apfs-workspace-bench/
  ├── package.json
  ├── bun.lock
  ├── bench.ts
  ├── scan-inodes.ts
  ├── classify-inodes.ts
  └── results/
      ├── latest.json
      └── 2026-07-11T10-23-19.550Z.json
```

It uses:

- Bun Shell for hdiutil, /bin/cp -c, Git, GNU du, and direct I/O;
- tinybench 6.0.2 (https://github.com/tinylibs/tinybench) for sampling and statistical analysis;
- Bun’s nanosecond timer;
- 20 retained samples per task;
- means, medians, p99, standard deviations, standard errors, margins of error, and RME;
- Darwin dd direct-I/O flags, which use F_NOCACHE.

Run it with:

```bash
  cd ~/Dev/apfs-workspace-bench

  bun run bench
  bun run scan ~/Dev/Conloca --depth 2 --limit 40 --threshold 10000
  bun run classify ~/Dev/Conloca/.jcode-worktrees/session_raven_1779838230248_e95b74ac46e7dd0a
```

The benchmark cleans its /private/tmp workspace by default. Add --keep to retain the generated images.

────────────────────────────────────────────────────────────────────────────────

Benchmark methodology

The tested hot synthetic workspace contained:

┌─────────────────────────────────────┬─────────────────────────┐ │ Data │ Count │
├─────────────────────────────────────┼─────────────────────────┤ │ Total files │ 100,000 │
├─────────────────────────────────────┼─────────────────────────┤ │ Tracked source files │ 20,000 │
├─────────────────────────────────────┼─────────────────────────┤ │ node_modules-style files │ 50,000 │
├─────────────────────────────────────┼─────────────────────────┤ │ Rust target/incremental-style files │ 30,000 │
├─────────────────────────────────────┼─────────────────────────┤ │ Direct sequential I/O │ 128 MiB per sample │
├─────────────────────────────────────┼─────────────────────────┤ │ Metadata creation/deletion │ 10,000 files per sample
│ ├─────────────────────────────────────┼─────────────────────────┤ │ Samples │ 20 per task │
└─────────────────────────────────────┴─────────────────────────┘

The base contained a real Git repository with:

- main;
- bench-feature;
- 1,000 files differing between branches;
- ignored node_modules and Rust cache trees.

This was intentionally close to one of the observed Conloca session workspaces, where node_modules contains roughly
88,000–96,000 objects.

The “fresh session” measurements are fresh shadow/clone session creation while macOS is already running, not a machine
reboot or fully purged unified-buffer-cache test. Global filesystem caches were not purged. Direct sequential I/O did
bypass normal caching.

One-time template preparation was:

┌───────────────────────────────────┬─────────┐ │ Setup operation │ Time │
├───────────────────────────────────┼─────────┤ │ Create 32 GiB sparse image │ 1.030 s │
├───────────────────────────────────┼─────────┤ │ Attach template │ 262 ms │
├───────────────────────────────────┼─────────┤ │ Populate 100,000 files │ 7.513 s │
├───────────────────────────────────┼─────────┤ │ Initialize Git and two branches │ 934 ms │
├───────────────────────────────────┼─────────┤ │ Create 128 MiB direct-I/O payload │ 51 ms │
└───────────────────────────────────┴─────────┘

Template preparation is not part of per-session startup.

────────────────────────────────────────────────────────────────────────────────

Session startup results

Medians are the better summary here because mount timings had occasional long outliers.

┌─────────────────────────────┬───────────┬───────────┬──────────┐ │ Operation │ Median │ p99 │ Mean RME │
├─────────────────────────────┼───────────┼───────────┼──────────┤ │ A: new shadow + attach │ 273.77 ms │ 586.89 ms │
13.93% │ ├─────────────────────────────┼───────────┼───────────┼──────────┤ │ B: APFS clonefile only │ 1.71 ms │ 3.87 ms
│ 16.16% │ ├─────────────────────────────┼───────────┼───────────┼──────────┤ │ B: attach a fresh clone │ 249.69 ms │
272.77 ms │ 2.62% │ ├─────────────────────────────┼───────────┼───────────┼──────────┤ │ B: clonefile + attach │ 236.45
ms │ 271.48 ms │ 2.86% │ ├─────────────────────────────┼───────────┼───────────┼──────────┤ │ A: reattach existing
shadow │ 257.96 ms │ 292.39 ms │ 3.44% │ ├─────────────────────────────┼───────────┼───────────┼──────────┤ │ B:
reattach existing clone │ 226.86 ms │ 283.92 ms │ 6.12% │
└─────────────────────────────┴───────────┴───────────┴──────────┘

Relevant comparisons:

- New B session: 236.45 ms versus 273.77 ms.
  - B used 13.6% less time.
  - The actual /bin/cp -c clone contributed only about 1.7 ms.
  - hdiutil attach dominates startup.
- Existing B session reattach: 226.86 ms versus 257.96 ms.
  - B used 12.1% less time.

The fresh-clone attach-only measurement being slightly slower than the combined clone-and-attach task is
test-order/cache variation. The complete lifecycle measurement is the relevant one.

So the practical session startup for B is approximately:

```text
  APFS clonefile:       ~2 ms
  hdiutil attach:     ~235 ms
  --------------------------------
  ready filesystem:   ~237 ms median
```

File count did not create a multi-second mount. APFS mounts the inner volume from its filesystem metadata; it does not
walk all 100,000 files during attach.

────────────────────────────────────────────────────────────────────────────────

Mounted workspace performance

┌──────────────────────────────────┬───────────────┬──────────────┬──────────────────────────┐ │ Operation │ Shadow
median │ Clone median │ Clone relative to shadow │
├──────────────────────────────────┼───────────────┼──────────────┼──────────────────────────┤ │ git status │ 32.74 ms │
34.37 ms │ 5.0% slower │ ├──────────────────────────────────┼───────────────┼──────────────┼──────────────────────────┤
│ Checkout feature and back │ 268.26 ms │ 215.09 ms │ 19.8% less time │
├──────────────────────────────────┼───────────────┼──────────────┼──────────────────────────┤ │ Direct 128 MiB
sequential read │ 30.49 ms │ 34.04 ms │ 11.6% slower │
├──────────────────────────────────┼───────────────┼──────────────┼──────────────────────────┤ │ Direct synchronous 128
MiB write │ 154.10 ms │ 65.92 ms │ 57.2% less time │
├──────────────────────────────────┼───────────────┼──────────────┼──────────────────────────┤ │ Create 10,000 files │
1,122.92 ms │ 1,023.99 ms │ 8.8% less time │
├──────────────────────────────────┼───────────────┼──────────────┼──────────────────────────┤ │ Delete 10,000 files │
214.07 ms │ 222.74 ms │ 4.0% slower │
└──────────────────────────────────┴───────────────┴──────────────┴──────────────────────────┘

Interpretation:

### B’s important wins

- Direct synchronous write completed in 0.428× the time:
  - 154.10 ms shadow;
  - 65.92 ms clone;
  - about 2.34× the effective write throughput for this fixed-size operation.
- Branch checkout took 19.8% less time.
- Creating dependency/build-tree-style files took 8.8% less time.
- Session creation and reattachment were both about 12–14% faster.

### A’s limited wins

- Direct sequential reads were about 11.6% faster.
- File deletion was about 4% faster.
- Warm git status was about 5% faster, but both git status distributions had one very large outlier:
  - shadow median 32.74 ms, one 417 ms sample;
  - clone median 34.37 ms, one 227 ms sample.
  - The corresponding mean RME values were 77% and 46%, so that small median difference is not a sound basis for
    choosing A.

A shadow adds an extra resolution layer: unchanged blocks fall through to the base image, while writes are redirected to
a separate shadow file. B presents one independently writable image file to the disk-image stack. The observed write
results are consistent with that architectural difference.

For Node, Rust, package installation, branch checkout, and build output workloads, B wins overall.

────────────────────────────────────────────────────────────────────────────────

Real Conloca inode profile

The new Bun scripts scanned the actual Conloca trees.

Whole repository

```text
  8,357,293  ~/Dev/Conloca
  6,152,355  ~/Dev/Conloca/.jcode-worktrees
  1,891,789  ~/Dev/Conloca/.claude/worktrees
    254,599  ~/Dev/Conloca/node_modules
     42,461  ~/Dev/Conloca/.nx/cache
```

jCode session tree

```text
  Root objects:          6,152,354
  Immediate workspaces:         69
  node_modules objects:  5,980,072
  node_modules share:       97.20%
```

Largest remaining source categories across all 69 sessions:

```text
  privpkgs       86,482
  packages       43,096
  targets        13,746
  specs           9,384
  example         6,141
  tooling         4,811
  project         2,484
  .nx             1,656
```

Claude worktree tree

```text
  Root objects:          1,891,789
  Immediate workspaces:         31
  node_modules objects:  1,830,726
  node_modules share:       96.77%
```

Combined result

The two session systems contain:

```text
  5,980,072 + 1,830,726 = 7,810,798 node_modules objects
```

That is:

```text
  7,810,798 / 8,357,293 = 93.46%
```

So moving only session node_modules trees into image-backed files would remove approximately 93.5% of all Conloca
filesystem objects from the host Data volume, while leaving:

- the main checkout;
- worktree source files;
- Git’s shared object database;
- worktree administrative metadata;

exactly where they are now.

That materially changes the recommended migration order.

────────────────────────────────────────────────────────────────────────────────

Recommended initial architecture: Git outside, caches in B clones

Use ordinary linked Git worktrees for source and one cloned cache image per session.

```text
  ~/Dev/
  └── Conloca/                       ordinary hot main checkout
      └── .git/                      authoritative shared Git repository

  ~/WorkspaceImages/
  └── Conloca/
      ├── cache-base.sparseimage     hot node_modules/build-cache template
      └── sessions/
          ├── raven.sparseimage      /bin/cp -c clone
          ├── fox.sparseimage
          └── bug-123.sparseimage

  ~/DevSessions/
  └── Conloca/
      ├── raven/
      │   ├── src/                   ordinary linked Git worktree
      │   └── cache/                 mounted image
      │       ├── node_modules/
      │       ├── target/
      │       ├── .nx/
      │       └── other generated data
      └── fox/
          ├── src/
          └── cache/
```

Session creation becomes:

```bash
  SESSION=raven
  REPO="$HOME/Dev/Conloca"
  SESSION_ROOT="$HOME/DevSessions/Conloca/$SESSION"
  SOURCE="$SESSION_ROOT/src"
  CACHE="$SESSION_ROOT/cache"
  BASE="$HOME/WorkspaceImages/Conloca/cache-base.sparseimage"
  IMAGE="$HOME/WorkspaceImages/Conloca/sessions/$SESSION.sparseimage"

  mkdir -p "$SESSION_ROOT" "$CACHE"

  git -C "$REPO" worktree add \
    -b "agent/$SESSION" \
    "$SOURCE" \
    main

  /bin/cp -c "$BASE" "$IMAGE"

  hdiutil attach \
    -nobrowse \
    -owners on \
    -mountpoint "$CACHE" \
    "$IMAGE"

  ln -s ../cache/node_modules "$SOURCE/node_modules"
  ln -s ../cache/target "$SOURCE/target"
  ln -s ../cache/.nx "$SOURCE/.nx"
```

The exact cache set should be project-specific:

- node_modules;
- Rust target;
- .nx/cache;
- .devenv outputs that are safe to recreate;
- language/build caches whose path can be configured;
- generated artifacts.

Do not automatically move repository state, credentials, or non-reproducible data.

Why this is the best first step

1.  It removes approximately 7.81 million host filesystem objects for Conloca.
2.  It retains native git worktree behavior.
3.  The branch is already visible to the main repository; there is no local push step.
4.  Git’s object database remains shared.
5.  Cache-image creation is approximately 2 ms plus a roughly 235 ms attach.
6.  Image deletion replaces recursively unlinking tens of thousands of dependency files.
7.  It can be piloted before changing agent source-workspace creation.

The main unknown is tool behavior across cache-root symlinks. Bun, Nx, Rust, language servers, watchers, and editors
should be exercised against this layout. If a tool rejects symlinked roots, the image volume can be mounted directly at
one specific cache path—such as src/node_modules—but a single volume root cannot simultaneously be mounted at several
unrelated paths. A single cache mount plus symlinks is the more flexible layout.

────────────────────────────────────────────────────────────────────────────────

Full-workspace architecture

If maximum isolation is desired later, put the complete independent repository inside the image:

```text
  ~/WorkspaceImages/Conloca/base.sparseimage
  └── repo/
      ├── .git/
      ├── source
      ├── node_modules/
      ├── target/
      └── .nx/

  ~/WorkspaceImages/Conloca/sessions/raven.sparseimage
  └── APFS clone of the complete base image
```

Session creation:

```bash
  /bin/cp -c \
    "$HOME/WorkspaceImages/Conloca/base.sparseimage" \
    "$HOME/WorkspaceImages/Conloca/sessions/raven.sparseimage"

  hdiutil attach \
    -nobrowse \
    -owners on \
    -mountpoint "$HOME/DevSessions/Conloca/raven" \
    "$HOME/WorkspaceImages/Conloca/sessions/raven.sparseimage"

  git -C "$HOME/DevSessions/Conloca/raven/repo" \
    switch -c agent/raven host/main
```

The image repository should have the ordinary checkout as a local remote:

```bash
  git -C "$MOUNT/repo" remote add host "$HOME/Dev/Conloca"
```

Return the branch with:

```bash
  git -C "$MOUNT/repo" push \
    host \
    HEAD:refs/heads/agent/raven
```

Do not push onto the branch currently checked out by ~/Dev/Conloca; push a separate session branch.

────────────────────────────────────────────────────────────────────────────────

Should .git live outside a full-workspace image?

It can, but it should not by default.

When .git is inside the image:

- its millions or thousands of inner objects are not host Data-volume inodes;
- the host sees the base sparse image and session clone files;
- Option B shares the base image’s Git extents using outer APFS CoW;
- the session repository is self-contained;
- the image has no absolute dependency on a particular host path;
- deleting or moving the session does not leave linked-worktree registrations.

Therefore, keeping .git inside the full-workspace template does not defeat the inode-isolation objective.

External Git directory is technically possible

Git officially supports:

```bash
  git init \
    --separate-git-dir "$HOME/GitSessions/Conloca/raven.git" \
    "$MOUNT/repo"
```

That writes a small .git text file in the worktree pointing at an external Git directory. No bind mount is required. Git
describes this as a “filesystem-agnostic Git symbolic link.”

But a pre-populated hot template would then need per-session initialization:

1.  clone the base image;
2.  attach it;
3.  initialize a unique external Git directory;
4.  configure the host repository as a local remote;
5.  populate or share objects;
6.  create the branch and index;
7.  verify that the base snapshot matches the selected commit.

Sharing host objects through alternates is possible, but git clone --shared
(https://git-scm.com/docs/git-clone#Documentation/git-clone.txt---shared) carries an explicit corruption warning: if the
source prunes objects which are only referenced by the borrowing repository, the borrowing repository can become
corrupt.

Externalizing .git therefore adds:

- absolute path dependencies;
- external lifecycle state;
- index-creation work at session startup;
- object-alternate and garbage-collection hazards;
- cleanup outside the image;
- little or no host-inode advantage over keeping .git inside the inner APFS filesystem.

Do not clone an already-linked Git worktree

A normal linked worktree contains a .git file pointing to something like:

```text
  ~/Dev/Conloca/.git/worktrees/<registration>
```

Cloning an image containing that linked worktree would duplicate the pointer. Multiple mounted sessions would claim the
same:

- HEAD;
- index;
- worktree registration;
- per-worktree configuration.

That is invalid.

If source lives inside a full image, use a standalone repository with a local remote. If source stays outside and only
caches live in the image, continue using normal git worktree add.

Git provides git worktree lock for linked worktrees on media that may be temporarily unmounted, but locking does not
make duplicated registrations safe.

────────────────────────────────────────────────────────────────────────────────

Can the host checkout be mounted “inside” the image?

Not in a way that gives it independent copy-on-write branch semantics.

A host source directory exposed through a symlink, null mount, or equivalent is still the same host source directory.
Editing it changes the original files. The APFS image cannot interpose CoW semantics on an arbitrary directory outside
the image.

The correct relationship is one of:

1.  Host linked worktree + image-backed caches — recommended first.
2.  Independent repository inside a cloned full-workspace image + local Git remote — maximum isolation.
3.  Per-session external Git directory + worktree inside image — feasible but unnecessarily complex.

────────────────────────────────────────────────────────────────────────────────

Decision

For this machine:

1.  Use Option B, not shadows, for writable session images.
2.  First move only session caches—especially node_modules—into cloned images.

- Measured host-inode reduction opportunity: 7,810,798 objects, or 93.46% of the complete Conloca tree.

3.  Keep source as ordinary linked worktrees during the pilot.
4.  If cache-root symlinks behave reliably, this likely solves the APFS object-count problem without changing
    source-worktree semantics.
5.  If complete session isolation is later required, clone the entire hot workspace image and keep its standalone .git
    inside the image, with ~/Dev/Conloca configured as a local remote.
6.  Do not use a cloned linked-worktree .git pointer, and do not externalize .git merely to save host inodes—it does not
    provide a meaningful inode advantage once the repository is inside the image.

Sources:

- Tinybench: statistical benchmarking library (https://github.com/tinylibs/tinybench)
- Git worktree: linked worktree behavior and locking (https://git-scm.com/docs/git-worktree)
- Git init --separate-git-dir (https://git-scm.com/docs/git-init)
- Git local/shared clone behavior and alternates warning (https://git-scm.com/docs/git-clone)
- Apple APFS file cloning and copy-on-write behavior
  (https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/APFS_Guide/Features/
  Features.html)
- hdiutil shadow and image attachment manual (https://keith.github.io/xcode-man-pages/hdiutil.1.html)
