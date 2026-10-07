# Storage Layout

cowshed stores all durable artifacts on one dedicated APFS volume and derives all runtime state from the filesystem and
the kernel. Nothing cowshed creates is visible in Finder, the Desktop, or the user's home directory listing — and no
image or store churn lands on the Data volume, so the user's unique data keeps its metadata, fsck time, and snapshots to
itself.

## Repository identity

A _project_ is a checkout bound to one or more stable repository identities. A `repo_id` is machine-independent and has
exactly two validated components. A Cowshed installation may adopt any number of projects. Each project's primary
`repo_id` owns exactly one warm `main` workspace plus its own session/checkpoint namespace; `main` is never a
machine-global singleton.

```
repo_id = lowercase(owner) + "/" + lowercase(repo)
```

For a repository with remotes, discovery proposes candidates from configured remote URLs and the user or trusted policy
selects one. URL normalization removes transport syntax, credentials, query/fragment, a trailing `.git`, and redundant
slashes before extracting `owner/repo`; it never guesses across ambiguous remotes and never silently mints an identity.
The binding records the chosen remote name and normalized URL and every open validates that its normalized `owner/repo`
still equals `repo_id`. Multiple identities may be bound to one checkout (for example upstream and fork), with exactly
one primary identity used for storage paths. A local-only repository requires an explicit `repo_id` at adopt time.
Moving or recloning a checkout does not change its identity.

Each `owner` and `repo` component is encoded independently as one filesystem component: lowercase ASCII
`[a-z0-9][a-z0-9._-]*`, with percent-encoding for every byte outside that set and for literal `%`, `.` and `..`. At the
layout root, an owner component equal to a host namespace (`gateway`, `telemetry`, `caches`, `mnt`, or
`.cowshed-volume.json`) is also percent-escaped, so a valid remote identity can never alias controller infrastructure.
The `/` in `repo_id` is the one layout separator, never untrusted path text. Thus `acme/widget` maps to
`/private/cowshed/store/acme/widget/`; containment is checked after joining and symlinks are refused. The repository
binding is `/private/cowshed/store/acme/widget/repository.json`; trusted project policy is
`/private/cowshed/store/acme/widget/policy.json`. Both are controller-owned, mode 0600, and never visible to a sandbox.
Every path below uses the primary `repo_id` through this component-safe mapping.

## Directory layout

`/private/cowshed/store` **is** the `cowshed.store` volume — the path is a mountpoint pinned in `/etc/fstab`, not a
directory tree on Data and not inside any user's home. There is no intermediate `store/` level; the volume root is the
layout root:

```
/private/cowshed/store/                          # ← the cowshed.store volume, fstab-pinned here (see "Dedicated store volume")
  .cowshed-volume.json               # volume marker: its ABSENCE means "not mounted" (see mount ordering below)
  host.json                          # host configuration: mount root, credential routes
  .staging/                          # port-block reservations held while a workspace's grants are minted
  .blank/                            # blank templates every new image is cloned from (see "Images")
    <bytes>-<uid>-<gid>.asif         # one formatted, verified, detached image per capacity and owner
    <bytes>-<uid>-<gid>.lock         # flock held only while that template is minted
    <bytes>-<uid>-<gid>-minting-<nonce>.asif # a template being minted, under a name no minter used before
  <owner>/<repo>/                    # primary repo_id, encoded one component at a time
    repository.json                  # chosen remote binding, alternate identities, and primary designation
    checkout-root.json               # where the checkout was when main was last removed (written then, read to reopen)
    slot-bindings.json               # build slot → workspace bindings (`new --slot`, 06_cli.md)
    policy.json                      # trusted project policy (checkpoint quotas, standing grants); controller-owned, 0600
    waivers.json                     # reasoned secret-scan waivers (02_workspaces.md)
    lifecycle-intents.json           # bounded persist-before-mutate create/fork/remove recovery journal, mode 0600
    lifecycle-intents.json.lock      # flock held for each read-modify-write of the journal
    deletion-log.jsonl               # one line per artifact the controller unlinked, one per build volume released (storage/deletion_log.rs)
    main.asif                        # adopted main image (created here behind a PendingFence sidecar)
    main.asif.grants.json            # controller-owned grants + detached metadata
    main.asif.ca.key                 # main's workspace CA private key, 0600
    main.asif.lock                   # flock target for lifecycle operations
    .staging/                        # restore stages, never enumerated
    sessions/
      <workspace>.asif               # one image per workspace
      <workspace>.asif.grants.json   # grants + detached metadata (see 04_sandbox.md)
      <workspace>.asif.ca.key        # the workspace's CA private key, 0600
      <workspace>.asif.lock          # flock target for lifecycle operations
      <workspace>.intent.lock        # flock the process executing <workspace>'s lifecycle intent holds
      .trash/                        # removed images awaiting reclamation (`gc`)
    checkpoints/
      <workspace>/<label>.asif       # clonefile snapshot
    build/
      <id>.asif                      # one build volume: Cargo target dirs + Nx cache and task DB (16_build_volumes.md)
      <id>.asif.json                 # its sidecar: built-at tree, linking checkout, seed flag, created
    tmp/
      <workspace>/                   # its TMPDIR, 0700: writable by its own sandbox only; reclaimed with its retired image,
                                     #   read-only directories its tools sealed made owner-writable first (`go clean -modcache`)
    quarantine/                      # secrets relocated by `cowshed adopt --quarantine`, and keyless sidecars awaiting `rekey`
  gateway.sock                       # gateway unix socket (control plane; root-level keeps sun_path short)
  sccache.sock                       # the host sccache daemon's socket (03_caches.md)
  run/
    manager.sock                     # the daemon's supervisor manager (11_shell.md)
    <digest>.sock                    # one workspace supervisor's socket
    <digest>.groups                  # that supervisor's process-group ledger
    gateway/                         # Linux only, 0700: per-incarnation data-plane Unix sockets, each mode 0600
      <workspaceIncarnation>.sock    # bind-mounted into exactly one attached workspace; absent after detach/restore fence
  telemetry/                         # ALL telemetry: Arrow IPC segments, day-partitioned (13_telemetry.md)
    <yyyy-mm-dd>/*.arrow             #   lifecycle spans, gateway audit, grant mutations, command debug
  (rebuildable caches are not beneath this tree: each tool keeps its own in the host HOME; see 03_caches.md)
  (workspaces mount under a host-configured mount root — default `~/.cowshed/mnt`; see 02_workspaces.md)
```

Workspace mountpoints live under one host-configured **mount root** (default `~/.cowshed/mnt`, settable via
`cowshed setup --mount-root <dir>`), laid out `<mount-root>/<owner>/<repo>/<workspace>`. The root is a plain directory
on Data: no volume mounts there, so the masked-mountpoint failure class cannot exist. Git identity inheritance follows
the anchor of each `includeIf gitdir:` rule — rules anchored at an ancestor of the mount root (for example
`gitdir:~/Dev/`) match workspaces with no additional configuration; rules anchored exactly at a project root do not, and
`cowshed setup`/`doctor` detect that divergence empirically by diffing `git config --show-origin` between each adopted
checkout and a probe repository under the mount root. Changing the root requires every workspace detached; `setup`
refuses while any are attached because absolute paths are baked into detached metadata and sandbox profiles. This
directory is distinct from the in-image `.cowshed/` namespace inside every clone.

<!-- prettier-ignore -->
```
~/Library/LaunchAgents/dev.cowshed.*.plist   # launchd: gateway is a user agent; boot mounts are system/dev.cowshed.storage
~/Library/Logs/cowshed/*.log                 # launchd stderr for pre-tracer-init crashes; kept off /private/cowshed/store
```

Workspace names match `[a-z0-9][a-z0-9-]{0,63}`; `main` is reserved. Repository paths are safe because both `repo_id`
components are validated and encoded independently; the slash between them is structural and percent-decoding is never
performed during path lookup.

Sidecar suffixes append to the complete image filename (`<workspace>.asif.grants.json`). `portBlock` in detached sidecar
metadata is optional and platform-specific: it is present only for macOS workspaces and is omitted on Linux; Linux does
not synthesize a base port in persistent metadata.

The Data-volume footprint is limited to the empty `/private/cowshed/store` mountpoint directory, the per-user workspace
mount root (default `~/.cowshed/mnt`), and cowshed's user cache directory `~/Library/Caches/dev.cowshed`, which holds
the gateway's registry and repository mirrors. Durable evidence lives on the dedicated store volume, outside Data's
snapshots, backups, and fsck domain. Layer-3 caches belong to the tools and stay where the tools keep them in HOME
(03_caches.md).

### Two `.cowshed` namespaces

The name appears in two namespaces that must not be confused. **Host-absolute `/private/cowshed/store`** exists once —
the herd's store volume holds images, grants, and telemetry. **In-image relative `.cowshed/`** exists once _per
workspace_, at each workspace volume root — the marker, token, CA certificate, in-image cache roots, and job spools —
and travels with every clone. The wiring rule follows the split: shared state resolves to a host-absolute path — under
`/private/cowshed/store/...`, or a shared cache's own directory in the host HOME (03_caches.md) — that is the same
string in every context, while workspace-keyed state resolves to in-image relative `.cowshed/...` paths (correct in
every clone automatically). Main and sessions use identical wiring; only the sandbox's permission mask differs.

## Images

- **Format**: one — an ASIF image (`.asif`) holding one case-sensitive APFS volume. There is no second format, no
  fallback, and no format field in any metadata: `.asif` is the only image extension anything enumerates, and macOS 26,
  which introduced ASIF (`diskutil` documents it as the replacement for the legacy `.sparseimage`), is the floor. Every
  image starts as a clone. Each store keeps one **blank template** per capacity and owner,
  `.blank/<capacity-bytes>-<uid>-<gid>.asif`, and a **mint** — adopt's main image, a build volume's first touch — is one
  `clonefile` of it to the image's canonical name plus one `diskutil image attach --nobrowse --noMount --plist`,
  verified with `fsck_apfs -q` like every attach. The clone takes the canonical name whole, so the name only ever holds
  a complete, formatted volume. A template is minted lazily, the first time a store mints at its capacity, in five
  unprivileged steps:
  `diskutil image create blank --format ASIF --size <capacity-bytes> --volumeName [cowshed] --fs None <stem>-minting-<nonce>.asif`,
  `diskutil image attach --nobrowse --noMount --plist <image>`,
  `newfs_apfs -U <uid> -G <gid> -e -v [cowshed] <whole-device>`, a `fsck_apfs -q` of the new volume, and
  `hdiutil detach <whole-device>`; only then is it renamed to its name. Only minting takes the template's lock, and a
  minter that finds the template once it holds the lock returns it, so concurrent first mints make one template and a
  mint that finds it needs no lock at all. Each minter stages under a name of its own (`-minting-<nonce>`), so a path is
  attached at most once: a minter killed with its attach still queued on `storagekitd` can have that attach land after
  the next minter removed its file, and on the single reused `-minting` name it landed beside the next minter's attach,
  leaving one path attached twice. Before it starts, a minter releases every attachment the kernel holds for any of the
  template's staging names (old reused name included) — the inventory decides, since an attachment outlives its file and
  keeps its path — and removes their files. No attach runs for a path the kernel already holds: the image driver refuses
  a second attach of one file, but not of a new file at the same path. The attaching user owns the image's device nodes,
  so formatting needs no privilege, and `-U`/`-G` make the volume root the invoking user's from the start — which is why
  the owner is part of the template's name. `diskutil`'s own `--fs APFS` is not used: it cannot ask for case
  sensitivity, and it leaves a root-owned volume root that an `owners` mount cannot write and only root can hand over.
  Like every clone, a minted volume shares its template's APFS volume and container UUIDs (nothing records or reads
  them; see "Ownership, identity, and the volume label") and carries its label. A capacity gets a template of its own
  rather than a clone of another grown to fit: growing the container is a `diskutil apfs resizeContainer`, a second
  `storagekitd` call on every such mint (0.93–1.0 s measured on a loaded host), where a template costs one mint per
  store and capacity (0.57–0.63 s measured).
- **Case-sensitive, always**: `-e` is the entire cost — one flag at creation. Clones copy the volume as it is, so `new`,
  `fork`, `checkpoint`, and `restore` never repeat it, and case-sensitive ASIF measures the same as case-insensitive
  ASIF on every axis below within noise. A case-sensitive volume holds every path a repository can contain, including
  paths that differ only in case, which a case-insensitive volume merges. The checkout's own volume does not matter:
  adopt copies from it and sets the copied repository's `core.ignorecase` to `false` (02_workspaces.md).
- **Capacity**: 100 GiB sparse. Capacity is a cap, not an allocation; images occupy only written blocks. Override per
  project via `.cowshed.toml` `capacity`.
- **Clone cost follows extents, not size**: `clonefile` of an image returns in milliseconds because the clone shares the
  source file's extent map, and the first write to either file copies that map: about 3.5 µs per extent for maps of
  thousands of extents, rising to about 12 µs for maps of millions, on the store volume. A long-used main is slow to
  clone even though the clone call is instant: a one-byte write into a plain `cp -c` clone of a 2.1M-extent image took
  25.7 s with no image attached, a 473k-extent image 4.4 s, an 8,871-extent image 31 ms, and a freshly written
  256-extent file 5 ms. `new` and `fork` pay it in their own `first-write` step, before the clone is attached. Those
  figures are a quiet host's; host load multiplies them, and the map copy is not over when the write returns. At load
  averages of 60–120 on 18 cores, `first-write` measured 31–59 µs per extent on a 154k-extent main (4.8–9.1 s) and 56–96
  µs on a 1.13M-extent main (62–109 s). After each 1.13M-extent first write, the next metadata operation on the APFS
  container blocked for 35–43 s, whatever volume or directory it named. A file create probing an unrelated
  `/private/tmp` directory stalled 43.1 s in the same window as the clone's next step, which creates its image-lease
  file: the lease file's birth time fell 34.6 s into that step's 35 s, so no holder existed to contend with. A
  fragmented main therefore stalls every process on the host, not only the clone. The 154k-extent main showed no such
  stall: the probe's worst create took 0.35 s. Deleting a written clone costs half to all of that again (half at 2.1M
  extents, about the same at 9k). Fragmentation comes from clones, not from the format: an image takes rewrites in place
  until a clone or checkpoint shares its blocks, after which every block it rewrites moves to a new run. Under the fixed
  churn of "Format measurements" below, three rounds moved a fresh image from 468 to 655 extents with no clone held and
  from 385 to 8,871 with four clones held — about 4,000 extents a round once the volume reuses space earlier rounds
  freed — and SPARSE fragments the same way (583 → 888 and 593 → 9,441). The same three rounds run inside a clone left
  the source at its 356 extents, untouched (the clone itself went to 8,699). What keeps `new` fast is therefore where
  writes land: every write into main while anything shares its blocks is paid again by every later clone, and a write
  inside a workspace costs main nothing. Build churn is most of main's writes, which is why build state lives on build
  volumes that are replaced at each land rather than in main's image (16_build_volumes.md "Why two volumes"). `doctor`
  counts main's extents with one `F_LOG2PHYS_EXT` query per contiguous run (2.1M extents read in 2.6 s) and reports them
  as `main-extents`, a warning naming `cowshed defrag main` once the predicted first-write cost reaches the 1 s
  cold-`new` budget (08_testing.md). `defrag` is the one remedy: it detaches the workspace exactly as `resize` does — a
  busy volume refuses before the image is touched — copies the image's data regions with plain `pread`/`pwrite` into
  `<image>.defrag` beside it (never `clonefile`, `copyfile(3)`, or `std::fs::copy`, all of which clone on APFS and would
  share the old map), punches interior holes after each following write and the trailing hole after sizing the copy
  (APFS may allocate zero blocks when extending it), `F_FULLFSYNC`s it, renames it over the image, syncs the directory,
  and verifies the result by attaching it before restoring the mount state it found. The copy runs at 3.1–5.4 GB/s on
  the store volume (8.9 GiB in 1.8–3.1 s, an 8,871-extent image back to 579). Nothing rewrites an attached image: the
  attachment holds an exclusive lock on the file (another `O_SHLOCK` or `O_EXLOCK` open fails with `EAGAIN`), and
  `diskutil image resize` and `diskutil image create from` refuse it as well. The copy needs, and keeps, free space
  equal to the image's allocated bytes while earlier clones and checkpoints still share the old blocks; the verb refuses
  before detaching when the store volume lacks it. Nothing enumerates `<image>.defrag` as an image or sidecar; the next
  `defrag` replaces one an interrupted run left, and `doctor` names it until then. Mains are never detached implicitly
  (the gateway keeps them mounted), so no path rewrites main on its own.
- **Detached growth**: under the image's lease, an `O_EXLOCK | O_NONBLOCK | O_NOFOLLOW` open proves no image driver
  holds the file and excludes attachment until the write is durable. Validate the ASIF v1 `shdw` header, version 1,
  length `0x200`, 4 KiB alignment, and a strictly larger capacity within the header's maximum sector count (`0x38`);
  rewrite only the eight-byte big-endian 512-byte sector count at `0x30`, then power-loss flush before closing.
  Directories and chunk tables are sized from the maximum, not the current count
  ([format specification](https://github.com/huven/asif-format)); Apple publishes no layout.
  `diskutil image resize --plist` only reads limits. Its content resize secretly attaches the image outside cowshed's
  ownership check and pin, and concurrent resizes were observed exiting `ENOENT` after completing. Cowshed instead
  attaches and verifies its own image, grows the container with `diskutil apfs resizeContainer`, and checks kernel
  capacity. A crash between the header and container growth leaves a larger image around the old container:
  equal-capacity resize is refused, while a larger resize completes both steps, as in the previous two-phase path.
- **Volume name**: the repository name for `main`, `<repo> — <workspace>` for every other workspace. The volume name is
  a label and nothing else: Finder shows it in place of the directory name for a mounted volume's directory, so it is
  written for the person looking at it. Nothing parses it, nothing classifies a volume by it, and nothing derives
  identity from it — renaming a volume by hand (`diskutil rename`) changes the label and nothing else. Identity comes
  from where the backing image lives and from the in-image marker; see "Ownership, identity, and the volume label"
  below. A minted image carries its template's label, `[cowshed]`, until its workspace's supervisor relabels it off the
  provisioning path; a build volume keeps it, since nothing mounts one browsable.
- **Spotlight**: nothing to set. `diskutil image create` has no `-nospotlight`, `mdutil -i off` needs root, and a
  `nobrowse` mount is not indexed (measured: no `.Spotlight-V100` store appears after writes, and `mdutil -s` reports no
  indexing state). A `--browse` mount is an ordinary visible volume to Spotlight.
- **Time Machine**: nothing to set. Backup policy is one per-volume decision, not path exclusions, and Time Machine
  already leaves the store volume out: measured on macOS 26.6, `tmutil isexcluded /private/cowshed/store` reports
  `[Excluded]` though cowshed applies no exclusion (it runs no `tmutil`). Durability is git (`cowshed push`), never
  backup.

### Format measurements

The image-format prototype (`specs/cowshed/prototypes/image-format-bench/`: harness, `results/`) compared three
candidates on the store's APFS container, all created and mounted without root: case-sensitive ASIF (the format),
case-insensitive ASIF (`newfs_apfs -i`, otherwise identical), and case-sensitive SPARSE
(`hdiutil create -type SPARSE -fs "Case-sensitive APFS"`, attached with `hdiutil attach -owners on -nomount`). The data
set is one real Bun `node_modules` (3.6 GB, 34k files, 6.5k symlinks), 40,000 further symlinks, 2 GiB of
cargo-target-like files (24 × 64 MiB, 512 × 1 MiB), and a git repository tracking all but the target files (80,744 index
entries, 0.75 GB of loose objects): 6.3 GB, 55,821 files, 46,575 symlinks. One churn round rewrites every target file
(write a new file, rename it over the old) and relinks every symlink — 2 GiB and 46,575 relinks. Extents are counted as
`doctor` counts them; the first write is one byte rewritten in place in a fresh `clonefile` of the image. The host ran a
fleet throughout (load average 10–35), so single timings carry about ±20 %; the I/O rows interleave the three candidates
round by round.

| Metric                                                                   | ASIF, case-sensitive (chosen)                                                     | ASIF, case-insensitive                                                   | SPARSE, case-sensitive                                           |
| ------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| Create, unprivileged (median of 5)                                       | 441 ms                                                                            | 448 ms                                                                   | 1,046 ms                                                         |
| Attach + `fsck_apfs -q` + mount (median of 5)                            | 228 ms                                                                            | 204 ms                                                                   | 308 ms                                                           |
| Detach (median of 5)                                                     | 337 ms                                                                            | 277 ms                                                                   | 360 ms                                                           |
| Fill with the 6.3 GB data set (`ditto`, two runs)                        | 21.7 s / 16.5 s                                                                   | 17.2 s / 20.8 s                                                          | 34.7 s / 57.5 s                                                  |
| Source extents, filled → 3 churn rounds, 4 clones held                   | 385 → 511 → 5,137 → 8,871                                                         | 425 → 550 → 5,323 → 8,952                                                | 593 → 2,244 → 5,822 → 9,441                                      |
| First write into a fresh clone at those counts                           | 2.7 / 3 / 18.4 / 30.7 ms                                                          | 2.5 / 2.4 / 17.3 / 29.3 ms                                               | 2.8 / 9.4 / 20.6 / 36 ms                                         |
| Source extents, same churn, no clone held                                | 468 → 576 → 629 → 655                                                             | 445 → 586 → 621 → 624                                                    | 583 → 778 → 884 → 888                                            |
| Source extents, same churn inside a clone of it                          | 356 → 356 → 356 → 356 (shed: 472 → 5,003 → 8,699)                                 | —                                                                        | —                                                                |
| Allocated after 3 rounds (5.9 GiB data)                                  | 8.87 GiB                                                                          | 8.87 GiB                                                                 | 9.06 GiB                                                         |
| Defrag copy of the 4-clone image (8.9 GiB)                               | 2.0 s, 4.7 GB/s → 579 extents                                                     | 1.8 s, 5.4 GB/s → 576 extents                                            | 3.1 s, 3.1 GB/s → 598 extents                                    |
| `git status`, 80,744 entries (interleaved median of 7)                   | 315 ms                                                                            | 344 ms                                                                   | 319 ms                                                           |
| 20k small files: create / symlink / unlink ops/s                         | 13,811 / 26,684 / 31,149                                                          | 14,516 / 25,955 / 28,200                                                 | 1,331 / 1,690 / 4,382                                            |
| 1 GiB sequential write (`F_FULLFSYNC`) / read (`F_NOCACHE`)              | 3,201 / 15,022 MB/s                                                               | 3,202 / 15,258 MB/s                                                      | 751 / 2,012 MB/s                                                 |
| Space returned after 3.3 GiB written then deleted                        | 1.3 GiB on its own; `diskutil image create from` copy is 15 MiB (0.18 s)          | 1.3 GiB on its own; `diskutil image create from` copy is 15 MiB (0.13 s) | none on its own; `hdiutil compact` 0.7 s returned 57 MiB         |
| Capacity 100 GiB vs 7.3 TiB (the store volume): create                   | 710 vs 463 ms                                                                     | —                                                                        | 1,050 vs 1,032 ms                                                |
| … attach + fsck + mount                                                  | 288 vs 241 ms                                                                     | —                                                                        | 287 vs 525 ms                                                    |
| … image allocated when empty                                             | 14 vs 17 MiB                                                                      | —                                                                        | 15 vs 17 MiB                                                     |
| … `df` inside the volume (size / available)                              | 100 / 100 vs 7,449 / 2,686 GiB                                                    | —                                                                        | 100 / 100 vs 7,449 / 2,674 GiB                                   |
| … filled, then one churn round: allocated, extents                       | 7.93 GiB, 455 vs 7.93 GiB, 932                                                    | —                                                                        | 7.92 GiB, 785 vs 7.93 GiB, 777                                   |
| … grow 100 GiB → 7.3 TiB, detached                                       | 542 ms                                                                            | —                                                                        | 1,171 ms                                                         |
| Rewrite of the 4-clone image: plain copy vs `diskutil image create from` | 2.4 s → 792 extents, 8.83 GiB vs 2.8 s → 217 extents, 7.95 GiB, same content: yes | —                                                                        | —                                                                |
| Rewrite, resize, or convert while attached                               | refused: file held under exclusive lock (`EAGAIN`)                                | refused: file held under exclusive lock (`EAGAIN`)                       | refused: file held under exclusive lock (`EAGAIN`)               |
| Grow 100 → 200 GB, detached                                              | 460 ms; volume grows with it                                                      | 539 ms; volume grows with it                                             | 498 ms; volume grows with it                                     |
| Volume and its clone case-sensitive                                      | yes / yes                                                                         | no / no                                                                  | yes / yes                                                        |
| Work per `new` for case sensitivity                                      | none                                                                              | none                                                                     | none                                                             |
| Largest capacity (documented / reverse-engineered)                       | just under 4 PiB                                                                  | just under 4 PiB                                                         | 128 PB (`man hdiutil`)                                           |
| Host dies mid-write (docs)                                               | allocation directory is versioned and switched atomically                         | allocation directory is versioned and switched atomically                | power loss during `compact` can damage the image (`man hdiutil`) |

The detached-grow timings above measured Apple's content resize, before the native header-growth cutover.

ASIF's 1 MiB chunks, its capacity limit, and its versioned allocation directory come from reverse engineering
(<https://schamper.dev/dissecting-apples-sparse-image-format-asif/>), not from Apple; SPARSE's limits and its compaction
warning come from `man hdiutil`. Both hold a crash-consistent APFS, and cowshed runs `fsck_apfs -q` before every mount
either way. ASIF's chunk table can mark a chunk unmapped, which fits it returning part of deleted space on its own;
SPARSE returns nothing until `hdiutil compact`, and little then. `diskutil image create from` rewrites a detached ASIF
image from its allocated chunks only: on the churned four-clone image it took 2.8 s against the plain copy's 2.4 s and
left 217 extents and 7.95 GiB against 792 and 8.83 GiB, with the same content, case sensitivity, and owner. An ASIF
capacity as large as the store volume measured no cost against 100 GiB: creation, attach, empty allocation, and the
allocation after a fill and a churn round match within noise, the churned image held 932 extents against 455 (about 1.5
ms more first write), and `df` inside the volume reports the store's own free space as available. SPARSE at that
capacity attached slower (525 against 287 ms) and once refused its first detach with `EBUSY`.

Case-sensitive ASIF therefore costs nothing that case-insensitive ASIF does not: the flag is set once at creation, no
step recurs per `new`, and every measured difference between the two sits inside the noise. SPARSE loses on creation,
attach, fill, small-file and sequential I/O, ties on `git status`, fragments under clones exactly as ASIF does, and
gives back less space.

## Mounts

Attach is `diskutil image attach --nobrowse --noMount --plist <image>` (`hdiutil attach` refuses ASIF outright with
_"use 'diskutil image attach'"_), whose machine-readable output names the APFS volume device. Before the first mount,
cowshed runs `fsck_apfs -q <device>`; any non-zero result detaches the image and fails without exposing a workspace
mount. It then mounts with the kernel helper as the invoking user,
`mount_apfs -o nobrowse,owners,noatime <device> <path>` (`--browse` omits `nobrowse`), not `diskutil mount`: Disk
Arbitration serialises every mount on the host, and under a loaded fleet a mount `mount_apfs` completes in about a
second queued there for 68 s at the median. Owned-image detach is `hdiutil detach <whole-device>`, using the disk-image
driver's release interface rather than general `diskutil eject`; the latter spent 11.086 s detaching a mounted staging
image in a hosted arm64 release. A local real-ASIF comparison measured 127–173 ms for the image-driver detach against
257–298 ms for general eject; this does not establish a hosted latency bound. `WhenIdle` returns the observed
EBUSY/resource-busy refusal without forcing. `Release` gives the first refusal its 10 s monotonic wall-clock grace,
including command execution and disk-lease waits between retries, then uses `hdiutil detach -force`; requested poll
sleeps alone never define that deadline. Every other error remains authoritative. There is no general-eject fallback.
Every detach runs `-verbose`, and hdiutil's account becomes an attribute of the `detach image` span: a refused eject
prints `dissent=<reason>`, and an accepted one slower than 1 s prints `waited=<account>`. A forced eject of an unmounted
volume measured 2.57 s on a loaded host with no dissent in its account: the wait is DiskArbitration's own queue (the
unmount and eject callbacks), which `-force` does not bypass. The kernel's I/O Registry is the one host view that maps
an image's path to its devices (`diskutil image info` reports none): one `IOServiceGetMatchingServices("IOMedia")`
snapshot, each node walked up to its `AppleDiskImageDevice` and that device's `DiskImageURL`. `hdiutil info -plist` is
not an inventory: while any other image attaches or detaches it answers a truncated image list with nothing marking it
(a reviewer probe that kept one image attached while churning another saw it missing from 11–89 of every 648–1551 polls,
and from none of the same polls' registry reads). Reading that omission as absence once skipped a post-format release,
leaving the image attached for `diskutil image resize` to refuse as busy, and once lost a mounted workspace's attachment
on restart.

The image driver registers an attach's media before `diskutil image attach` reports them, so creation requires the
reported blank whole device to be the image's exact single-device mapping in one registry read before formatting.
Absence there is a contradiction, not lag: it fails at once, as does an observed conflicting mapping, and an unreadable
inventory is a typed refusal; each leaves both the attachment and backing file intact for diagnosis. No mutation is
retried and no unproved device is formatted. Shared owned-image cleanup distinguishes this unformatted whole-device
state from an APFS volume: it rechecks the exact mapping under the image's lease and releases only that owned device
without deleting the backing file first.

The disk-images framework behind `diskutil image` and `hdiutil` answers through helper daemons over XPC, and under host
contention it can lose them: a test gate at load average ~350 saw ten concurrent attaches fail at once with exit 1 and
_"Error: Couldn’t communicate with a helper application."_, while twelve concurrent attaches at load ~125 all succeeded.
Such a failure is the framework's, not the request's, so it is typed apart from an ordinary command failure
(`DiskImageHelperUnreachable`) and carries the framework's stderr and the host's `getloadavg` at the failure. It is
neither retried nor read as a detach dissent; like every child failure it still names a saturated vnode table when one
is the likelier cause.

Every release rechecks image ownership at the final detach boundary as well as the initial recovery read. Empty
inventory is already released; a nonempty mapping that no longer contains the recorded device is a typed refusal, never
successful release or permission to delete its backing file. An APFS image can expose both its physical and synthesized
whole devices, so ownership is membership in that exact image's current mapping, not an invented single-device rule
after formatting. Real fixture cleanup likewise cannot treat a missing returned attachment handle as release proof: it
recovers the current typed attachment before unlinking and retains unproved media on failure, without retrying a failed
explicit cleanup from its destructor.

Mounted-image release first uses `/sbin/umount <verified-volume-device>`, the kernel's unmount interface. Merely
changing general eject to `hdiutil detach` did not remove the hosted mounted-volume delay: that command still spent
10.694 s in the next release run. The native host reads kernel mount facts, then holds the image's lease and a fresh raw
IOMedia pin while cross-checking exact image/whole-device/volume identity before unmount. It never unmounts a path
selected by an untrusted child. Restart-owned mounts additionally require their incarnation marker and matching kernel
source. `WhenIdle` returns native unmount's observed resource-busy refusal without force; `Release` waits the existing
grace before `umount -f`. Other errors remain errors. Only after filesystem removal does the image driver's detach
release the device. Local real mounted-image probes measured 29–42 ms for native unmount; hosted deadline closure is not
implied.

Concurrent mount-table reads use `getfsstat(MNT_NOWAIT)` into a buffer each call owns: count first, allocate checked
space with room for new mounts, and grow and reread if the returned buffer is full. No reader borrows `getmntinfo`'s
process-static array, which another thread can overwrite during iteration. Every snapshot retains complete mountpoint
and source-device records; an empty mountpoint is a real inventory fault, not an entry to silently discard.

Disk device names are reusable, not image identities. Each image has its own private per-user lease,
`/private/tmp/cowshed-apfs-image-leases-<euid>/<sha256 of the identity>.lock`: an `flock` on a regular owner-only 0600
single-link file, opened without following symlinks, in a 0700 directory this user owns. The identity is the absolute
path produced by `attachment_inventory_path`, matched against the kernel's `DiskImageURL` backing-file path; the lease
hashes that same normalized identity, never a separately resolved alias. An alias with another identity fails closed.
Every cooperating create, attach, format, `fsck_apfs`, mount, unmount, recovery and detach of one image holds that
image's lease, so independent processes serialize on one image while operations on different images overlap. There is no
host-wide device lock. A cooperating process acts on a device only after the inventory positively maps the identity to
it under the lease (or while a raw pin holds it), and a device is freed only by a detach the same lease covers, so no
same-user cowshed process recycles it inside the critical section. A contended lease is awaited on a helper thread that
hands the locked descriptor back when the holder releases it, bounded by the 120 s disk-child deadline; expiry names the
lease file, the image and the holder's recorded pid. The lease does **not** constrain another user's disk tools or a
manual eject during the two mutations that cannot run raw-pinned, `newfs_apfs` and `hdiutil detach`, because a read-only
raw pin makes both fail with EBUSY. Before `fsck_apfs` or `mount_apfs`, cowshed opens the reported raw volume read-only,
then verifies that the attachment inventory maps **both** its whole container and its volume to the exact image. A
verified attachment owns that raw descriptor across the `attach_verified` → `mount` method boundary. It is released
after `mount_apfs` finishes, before intentional detach, or before the exclusive `diskutil apfs resizeContainer` writer,
which cannot run with a raw descriptor open. Independent device ejects during container resize after pin release are not
covered. While held, the descriptor prevents even external `diskutil eject force` or `hdiutil detach -force` from
releasing the image and recycling its device name. If the image lost the reported device before the descriptor could be
pinned, cowshed performs at most one fresh attachment, and only when inventory shows no remaining attachment for that
image. A conflicting or unreadable mapping fails closed, without running fsck on the reported device. No live-fsck
option authorizes touching a foreign mounted container. Blank-image formatting keeps its image-to-whole-device check
under the image's lease; its exclusive formatter cannot share a raw-device descriptor with another opener.

For every mounted attachment:

- Session workspaces mount under the host-configured mount root at `<mount-root>/<owner>/<repo>/<workspace>`.
  `-nobrowse` keeps every cowshed volume out of Finder, the Desktop, and the sidebar regardless of Finder preferences.
- The **main workspace mounts at the checkout's original path** (written `<project-root>` below; 02_workspaces.md). The
  user's path is the real thing, and sibling workspaces live under the shared mount root.
- Mountpoint directories are created before attach and removed by `cowshed gc`. An unmounted main mountpoint is empty:
  cowshed creates no repository shell hook underneath it. Reattachment is an explicit `cowshed attach` operation.
- Cwd resolution is granted only when the canonical input path is contained in exactly one currently mounted,
  repository-owned attachment whose image metadata and kernel mount facts agree. An in-image marker, an empty
  mountpoint, a detached image, a mount owned by another project, or overlapping active mounts never grants a
  `WorkspaceRef`.
- Personal workspaces may opt into Finder visibility with `--browse` at attach time.
- Every workspace and build-volume mount is `noatime`: builds are read-heavy, and access-time updates on read are
  metadata writes no workflow here consults (the Linux copy path carries an existing atime across; nothing decides on
  one). The store and caches volumes were already `noatime` (their fstab entries).

### Mount and per-volume policy

Main/personal workspaces and live build targets share the
[image backend](../../packages/cowshed/crates/cowshed-core/src/apfs.rs) (`MacOsApfsBackend::mount`); the
[build host](../../packages/cowshed/crates/cowshed-core/src/storage/apfs/native/build_volumes.rs) always passes
`browse = false`. **Build seeds remain detached clones** of a quiet live volume
(`runtime/build_volumes.rs::freeze_seed`/`reseed` and `clone_build_volume`): they inherit filesystem contents and
markers, not active mount flags. A
[blank template](../../packages/cowshed/crates/cowshed-core/src/storage/apfs/native/blank_template.rs) is formatted,
verified and detached **without ever being mounted**. The
[bounded-exec RAM volume](../../packages/nx-plugin/src/executors/bounded-exec/ram-temp.ts) is a separate APFS volume,
not an ASIF image.

The table distinguishes options cowshed requests from flags the kernel supplies. Seeing `nodev`, `nosuid` or `journaled`
in `mount` output does not mean cowshed passed an option with that name. Likewise, no indexing or backup command is
hidden behind `nobrowse`.

| Setting                       | Workspace / live-build images                                                                                                                                 | Detached seed / blank template                                                                        | RAM temp volume                                                                          | Dedicated store / retained caches                                                                     | Reason and setter                                                                                                                                                                                                                                                                                    |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Read-write / `rdonly`         | Read-write by default; recovery-marker mounts request `rdonly`.                                                                                               | Neither is mounted.                                                                                   | Read-write by default.                                                                   | `rw` in fstab.                                                                                        | Builds must write and execute their outputs; recovery reads a marker without modifying the image. Image backend `mount`; `storage/apfs/native.rs::detached_image_incarnation`; `storage/fstab.rs::build_fstab`.                                                                                      |
| `nobrowse`                    | Default; workspace `--browse` omits it. Live build volumes stay hidden.                                                                                       | Neither is mounted; template attachment uses `--nobrowse --noMount`.                                  | Requested at `diskutil mount`.                                                           | Requested at bootstrap/boot mount and in fstab.                                                       | Keep temporary/work volumes out of Finder, Desktop and sidebar. Image backend `mount`/attachment; build host `create_build_volume`/`mount_build_volume`; RAM `RamTempVolume.provision`; `storage/bootstrap/native/macos.rs::desired_mount_service` and `storage/fstab.rs::build_fstab`.              |
| `noatime`                     | Requested on every normal read-write/read-only image mount, including `--browse`.                                                                             | Neither is mounted.                                                                                   | Not requested.                                                                           | Requested in fstab.                                                                                   | Suppress access-time updates in read-heavy builds; workflow decisions do not depend on those updates. Image backend `mount`; `storage/fstab.rs::build_fstab`.                                                                                                                                        |
| `owners`                      | Requested on normal mounts, including read-only mounts.                                                                                                       | No mount flags; filesystem ownership is inherited/formatted.                                          | Not requested; unprivileged Disk Arbitration mounting yields `noowners`.                 | Not requested; fstab specifies `noowners`.                                                            | Workspace permissions must be honored. RAM mounting with ownership would require authorization; launchd plists therefore live outside the RAM-backed `TMPDIR`. Image backend `mount`; RAM module's mount/authorization contract.                                                                     |
| `noowners`                    | Only the temporary root-owned event-store repair mount; normal requested options are restored afterward.                                                      | Not mounted.                                                                                          | Kernel/Disk Arbitration property of the unprivileged mount, not an explicit RAM option.  | Explicit in fstab.                                                                                    | Permit marker repair in an inherited root-owned `.fseventsd` without root authorization; it is not the workspace's final permission policy. Image backend `retire_inherited_event_log`; RAM `provision`; `storage/fstab.rs::build_fstab`.                                                            |
| `nodev`                       | Kernel flag on unprivileged image mounts; not in cowshed's option string.                                                                                     | Not mounted.                                                                                          | Kernel flag on the unprivileged mount; not requested explicitly.                         | Not requested by privileged bootstrap.                                                                | Device-special files must not become usable device nodes through an unprivileged mounted image. Applies at the image backend/RAM mount calls; cowshed has no separate `nodev` setter.                                                                                                                |
| `nosuid`                      | Kernel flag on unprivileged image mounts; not in cowshed's option string.                                                                                     | Not mounted.                                                                                          | Kernel flag on the unprivileged mount; not requested explicitly.                         | Not requested by privileged bootstrap.                                                                | Mounted code cannot gain privilege through set-user-ID/set-group-ID bits. Applies at the image backend/RAM mount calls; cowshed has no separate `nosuid` setter.                                                                                                                                     |
| `journaled` / `local`         | APFS/kernel filesystem properties, not requested mount options.                                                                                               | APFS is formatted, but no mount flags exist while detached.                                           | APFS/kernel properties.                                                                  | APFS/kernel properties.                                                                               | Keep the filesystem's normal consistency/durability behavior; do not confuse the kernel's `journaled` label with a separate HFS+ formatting switch. Image backend `attach_and_format_unlocked` calls `newfs_apfs`; RAM `provision` and host provisioning call `diskutil apfs addVolume ... APFS`.    |
| `.fseventsd/no_log`           | Empty marker attempted after every successful read-write mount; read-only mounts do not write it.                                                             | Seed inherits the source's marker; blank template has none. A live clone is opted out when it mounts. | No explicit marker; the fresh `nobrowse` volume was measured without historical logging. | No explicit marker setter in host bootstrap.                                                          | Disable persistent FSEvents history, not live watcher notifications. The image backend `opt_out_of_event_log` owns the policy. A marker planted after mounting governs a later mount; daemon mount processing is asynchronous. See [The event log](#the-event-log).                                  |
| Event-log permissions         | New directory `0700`; new empty marker requested as `0644` (subject to umask). Repaired root-owned directory becomes `0711`; historical records are retained. | Seed inherits the source's directory/marker; template creates neither.                                | No event-log permission setter.                                                          | No event-log permission setter in bootstrap.                                                          | Directory traversal lets later mounts find the marker without exposing a directory listing; root-owned `0600` chunks keep their permissions. `apfs.rs::opt_out_of_event_log` (`mkdirat`), `opt_out_in` (`openat`) and `retire_event_log` (`fchmod`), all fd-relative and no-follow.                  |
| Root-owned event-store repair | One-time `nobrowse,noowners` staging mount (does not request `noatime`), then the caller's original `owners`/`noatime`/visibility options.                    | Neither is mounted; seed preserves its source's on-disk state.                                        | No repair path.                                                                          | This repairs an image's `.fseventsd`, **not** the dedicated store volume.                             | Add the marker without purging historical records; make root's directory traversable (`0711`, no listing) so later ownership-honoring mounts can find it without repeating repair. Image backend `retire_inherited_event_log`/`retire_event_log`; the final policy honors ownership again.           |
| Spotlight indexing            | No `mdutil` or indexing marker setter. Default `nobrowse` mounts were measured unindexed; `--browse` may be indexed.                                          | Neither is mounted, so no index is provisioned.                                                       | No explicit indexing command/marker; mounted `nobrowse`.                                 | No explicit indexing command/marker; mounted `nobrowse`.                                              | Default work volumes avoid indexing through their hidden mount policy. Browsable workspaces remain ordinary visible volumes; disabling their indexing with `mdutil -i off` would require root. The setters are the `nobrowse` mount calls above.                                                     |
| Time Machine exclusion        | No per-image/path exclusion setter.                                                                                                                           | No exclusion setter.                                                                                  | No exclusion setter; volume is temporary.                                                | No `tmutil` setter; the dedicated store was measured excluded by OS policy.                           | Images live on a separate volume rather than the Data volume, avoiding Data-volume snapshot retention of build churn. Durability is git, not a cowshed-applied backup rule; user backup policy remains separate. Host provisioning creates/mounts the dedicated APFS volume; no code calls `tmutil`. |
| Case sensitivity              | One case-sensitive APFS volume, inherited from the template.                                                                                                  | Seed inherits the filesystem; template uses `newfs_apfs -e`.                                          | Plain `APFS` in `addVolume`; no case-sensitive variant requested.                        | New volumes use plain `APFS`; no case-sensitive variant requested.                                    | Preserve distinct case-sensitive source/output names in image-backed trees. Image backend `attach_and_format_unlocked`; RAM `provision`; `storage/bootstrap/native/macos.rs::provision_apfs_volumes_in_session`.                                                                                     |
| Root UID/GID                  | Invoking user's UID/GID at formatting, inherited by clones.                                                                                                   | Seed inherits ownership; template uses `newfs_apfs -U <uid> -G <gid>`.                                | No explicit formatter UID/GID; mount ignores ownership.                                  | Provisioning chowns the root to the invoking UID/GID; fstab ignores ownership.                        | Make image roots writable by their owner without an elevated format. `blank_template.rs::owner`/`mint` supplies UID/GID to the image backend; `provision_apfs_volumes_in_session` issues/attests the host-root chown.                                                                                |
| FileVault/encryption          | No extra image-volume encryption switch; durable image bytes reside on the encrypted store.                                                                   | Backing-store protection only; no per-image `newfs_apfs -E`.                                          | No encryption setter.                                                                    | FileVault enabled; passphrase persisted in System.keychain before encryption, reused for boot unlock. | Protect durable bytes without a second per-image crypto/password layer. `storage/bootstrap/native/macos.rs::encrypt_volume_with` (`diskutil apfs encryptVolume -user disk -stdinpassphrase`) and `desired_mount_service` (`unlockVolume ... -nomount`).                                              |
| `noauto`                      | No image fstab entry.                                                                                                                                         | No fstab entry.                                                                                       | No fstab entry.                                                                          | Explicit in fstab.                                                                                    | Skip generic `mount -a`; cowshed's boot service explicitly unlocks/mounts the known UUID at the canonical path. `storage/fstab.rs::build_fstab`; `desired_mount_service`.                                                                                                                            |
| Execution / `noexec`          | `noexec` is not requested.                                                                                                                                    | Not mounted.                                                                                          | `noexec` is not requested.                                                               | `noexec` is not requested.                                                                            | Builds, test binaries and scripts must run from their filesystem; `nosuid` is not `noexec`. The image/RAM/fstab option lists above contain no execution ban.                                                                                                                                         |

Settings apply when the relevant path actually mounts or formats a volume. Already-mounted workspaces/build volumes are
not retroactively remounted just because the binary changed. In particular, the RAM path does not inherit the image
backend's new `noatime` or event-log marker policy, and detached seeds/templates have no live watcher or mount flags.

### The event log

`fseventsd`, the one root daemon behind every FSEvents stream on the host, also keeps a persistent per-volume event log
under the volume root's `.fseventsd` (Apple, _File System Events Programming Guide_,
[Preventing File System Event Storage](https://developer.apple.com/library/archive/documentation/Darwin/Conceptual/FSEvents_ProgGuide/FileSystemEventSecurity/FileSystemEventSecurity.html)).
Cowshed volumes do not want it: a checkout's watchers (the Nx daemon, editors) subscribe live and never replay history,
while the log costs the daemon work for every event on every logged volume. On a host saturating `fseventsd` — measured
2026-10-07: 100–107 % of a core for days, resident memory 7.06 GB growing to 8.30 GB in 18 minutes, at only 600–720
events/s visible to a user stream across all volumes — 52 of 66 mounted cowshed volumes kept a log.

What decides it, measured on this host without root (`FSEventsCopyUUIDForDevice` returns the historical stream's UUID; a
null result means historical events are unavailable, so subscribers use `kFSEventStreamEventIdSinceNow`):

- `fseventsd` decides when a volume mounts. A browsable mount gets a log, created as root, mode 0700; a fresh volume
  mounted `nobrowse` gets none, even after 100 000 file creations. A log already on the volume is kept.
- A log travels with the volume: a clone carries its source's `.fseventsd`. Every workspace cloned from a main that was
  once mounted browsable, and every build volume forked from such a seed, was logged by inheritance.
- An empty `.fseventsd/no_log` the user created is honored at the next mount: no log, while live delivery to streams is
  unchanged (a sentinel written after a 200-file burst on an opted-out workspace arrived in 330 ms, no drop flags).
  Planting it under a mounted, logged volume — renaming root's `.fseventsd` aside, which the volume's owner may do —
  changes nothing until the volume mounts again.

So every read-write mount opts its volume out, after `mount_apfs` returns and before the mount is handed back:

- No `.fseventsd`: create it (0700) and an empty `no_log` in it, exclusively; nothing is followed through a symlink. It
  governs the next mount and every clone made from the volume; this mount was `nobrowse` and fresh, so `fseventsd` gave
  it no log anyway.
- A `.fseventsd` that is a real directory holding an empty regular `no_log` the user can see: opted out, nothing to do.
- Root's log (the marker lookup is refused): unmount, mount `nobrowse,noowners` — ownership ignored, so root's directory
  is the user's to change — add the marker to root's log, set the directory 0711, unmount, and mount again as asked. The
  log's records stay, as `fseventsd` wrote them and unlisted; the Apple guide reserves purging them for an
  administrator. 0711 rather than root's 0700 lets the owner look the marker up under every later mount that honors
  ownership — under 0700 the lookup is refused, and every mount would retire the log again (measured: 3 runs in 3).
  `fseventsd` can carry the old log across that immediate remount (three runs in four on this host); the volume is
  unlogged from its next mount, and every clone of it from the first.
- A symlink or file holding `.fseventsd`, or anything but an empty regular file holding `no_log`: reported and left as
  found, never written through.
- Failing to opt out is reported on stderr and the mount stands: the mount is what the caller asked for, the log is host
  load. Only failing to mount the volume back is an error.

Templates are never mounted and carry no `.fseventsd`; their clones are opted out at their first mount. Mains are opted
out at their next mount. Seeds are never mounted: a seed is a clone of a live build volume, so it carries the marker
once that volume has mounted, and every fork of it mounts opted out. The bounded-exec RAM volume (nx-plugin) is already
unlogged — a fresh volume mounted `nobrowse` once, never remounted — so it plants nothing.

Opting out removes the log's work, not the daemon's live delivery work, and does not make delivery reliable on a
saturated host: a volume's first events after its mount can go missing with no drop flag, logged or not (the first
50-file burst after a mount delivered 12 of ~80 events, the next all of them; a single update written after a stream
went live on a just-mounted volume, logged or opted out, was missed for its whole bound in 8 of 23 runs before the
regression synchronized on the mount). Even synchronized on `fseventsd`'s own `Mount` event, on this host (2026-10-07)
one update missed a 10 s bound in 3 of 8 runs — on the browsable, logged arm and the opted-out arms alike, before or
after cowshed touched the volume: the host's `fseventsd` losing a just-mounted volume's events, not the opt-out. Live
delivery is therefore a measurement here, not a regression assertion: on an opted-out workspace after detach and attach,
a sentinel arrived in 330 ms with 0 drop flags. The real-APFS regression asserts only what the mount decides: the
`owners` and `noatime` flags, the marker as an empty regular file, root's directory kept at uid 0 mode 0711, and no log
UUID once the retired volume mounts again. It waits for `fseventsd`'s `Mount` event — on a stream watching the directory
the volumes mount under — before reading the log.

## How the APFS host degrades

The image substrate rests on four host services, and each fails in its own way under the load cowshed generates. They
are host facts, not cowshed bugs, and every rule that budgets disk-tool calls exists because of one of them.

1. **`storagekitd` serializes every `diskutil` verb, host-wide.** `diskutil image create`, `attach`, `info` and `resize`
   each make a synchronous XPC call to root `storagekitd` that re-syncs every disk on the host (`syncAllDisks`); an
   attach makes two. `storagekitd` answers one caller at a time. Measured on macOS 26 with about 390 `/dev/disk*` nodes:
   `diskutil info` costs about 40 ms of that serial queue and an APFS ASIF attach about 85 ms, so throughput stays flat
   (about 24 info calls/s, 12 attaches/s) while latency grows with the number of concurrent callers: an attach takes
   0.22 s alone, 1.2 s with 16 concurrent, and 2.5 s with 32. A parallel real-image test suite peaked at 37 concurrent
   disk-tool processes and measured 2.5–8 s for single calls that take 0.2 s idle. CPU load changes none of this; the
   number of concurrent disk-tool calls on the host does. `hdiutil detach`, `newfs_apfs`, `fsck_apfs`, `mount_apfs` and
   IORegistry reads do not go through `storagekitd`. Mount-table churn starves it all the same: an attach spends most of
   its time in `syncAllDisks` after its device already exists (76% of a sampled stuck attach), and that sync does not
   finish while mounts and unmounts keep changing the table. With no attach running, one `mount_apfs`/`umount` loop at
   6.3 cycles/s moved a probe attach's median from 1.15 s to 12.2 s, and four loops starved every probe past 60 s; eight
   attach loops beside two mount loops made 8 attaches in 42 s, each taking 41.7 s.
2. **AppleDiskImages2 runs out of kernel mappings, held by orphaned `diskimagesiod` helpers.** Every attached image has
   its own `diskimagesiod` (launchd job `system/com.apple.diskimagesiod.<UUID>`), which maps its IO request pool into
   the kernel: 36 shared buffers of 2 MiB each (queue depth 36, 2 MiB max IO), 72 MiB per helper, so 100 attached images
   hold about 7 GiB mapped. When the kernel cannot map a new pool the kernel log reads
   `DISharedBuffer::init: Can't map buffer at user address … size 2097152 to kernel` and
   `DIDeviceRequestPool::AllocateRequests: Can't allocate all buffers, allocated 0/36`, and the attach fails with "error
   code 150" ("Failed to initialize IO manager: Driver returned error code -536870210", `kIOReturnNoResources`). So does
   every `diskutil image info`, every new workspace, build volume, checkpoint and cold mount; images already attached
   keep working, and legacy `hdiutil attach` of UDIF still attaches. It is not the user wire limit (17.8 GB wired
   against a 116.8 GB `vm.global_user_wire_limit`). The space leaks through helpers that outlive their image: detach
   does not always end the image's `diskimagesiod`, and an orphaned helper (running, no `AppleDiskImageDevice` behind
   it) keeps its mapping. Measured: a host about 20,000 attaches into its uptime held 330 images at once, then failed at
   128 after about 300 more attach→detach cycles, then at 114, 100 and 98, with ten orphaned helpers running; detaching
   images freed at most one attach each. `launchctl kill` of an orphan's job is refused even to root ("Not privileged to
   signal service"), but `sudo kill -9 <pid>` of the ten orphans freed their mappings and the next attach succeeded with
   no reboot. An attached image is therefore a standing 72 MiB of kernel mapping, an orphaned helper the same until root
   kills it, and every attach a cycle that can orphan one: an operation or test pays the fewest attach cycles that prove
   its behavior, an idle workspace is detached rather than kept, and `doctor` names orphaned helpers (a `diskimagesiod`
   whose pid created no attached device's user client) with the root command that clears them.
3. **Detach waits on the kernel and holders.** A non-forced unmount of a volume something holds is refused with `EBUSY`
   and retried; a land's target unmount took 12.2 s on its first try and then about 45 retries, and the image detach
   that followed 45.3 s. A held file anywhere in a volume keeps its image attached, so a stray process with a cwd inside
   a workspace blocks its retirement.
4. **Shared extents fragment the source, not the clone.** Writing an image while a clone shares its blocks moves every
   rewritten block to a new run, and every later clone pays the extent map on its first write ("Images" above).

What the substrate does about each:

- **Fewest storagekitd round-trips.** Production operations sit at their floor: a mint is one `clonefile` of the store's
  blank template plus one attach ("Images"), a fork or cold mount is one attach, a land with no retire makes no
  disk-tool call, and a detach goes through `hdiutil`. The mint floor was one `image create` plus a formatting attach
  and `newfs_apfs`. Measured on the same host and tests (CLI adopt and build-volume first touch, `apfs <leg>/mint`
  spans), adopt's mint went from 537–705 ms to 303–345 ms and first touch from 565–756 ms to 312–317 ms on a quiet
  queue; under a loaded parallel suite the old adopt mint took 10.6 s (create 2.4 s, attach 7.7 s) against 5.0 s for the
  new one, all of it the one attach. Values the image itself records come from the image, never from a `diskutil` query:
  an ASIF image's capacity is read from its header (through the same `recognized()` gate the grow uses, which refuses
  any layout other than the measured one), and a mounted volume's from IORegistry.
- **No attach→detach→attach on a success path**, and no verify-by-reattach.
- **Storage calls and mount-table changes never overlap.** Every disk tool on the host runs under the gateway's
  disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease"): `diskutil`, `hdiutil` and `newfs_apfs` share one phase,
  `mount_apfs` and `umount` the other, and the two alternate. The same eight attach loops beside two mount loops made
  106 attaches in 43 s with the phases kept apart, at 0.94 s p50 and 1.78 s p95 under load 167–212.
- **Test images are 1 GiB** and a fixture detaches its images on every exit path it survives, panic included; a killed
  run's images are reclaimed by the next run's sweep (08_testing.md). A leaked image costs one slot of the host's finite
  attach budget for as long as it stays attached. Tests mint one blank template per host user with the production
  minter, at `/private/tmp/cowshed-itest-templates-uid<uid>`, and clone every test image from it; a test store that
  mints is seeded with a clone of it. Like a store's template it outlives its minter and its name spells everything its
  bytes depend on, so concurrent lanes and later runs clone the same one instead of each test process tree paying a
  create, a formatting attach, a format and a detach before its first test.
- **A disk-tool timeout is a concurrency measurement first.** Raising the bound or serializing the suite hides the host
  contention instead of reducing it; the fix is fewer calls, and calls that do not starve each other.
- **`doctor`** reports main's extent count (`main-extents`) so fragmentation is visible before it costs a fork.

## Ownership, identity, and the volume label

Three questions, three authorities, none of them the volume label:

- **Which volumes are cowshed's?** Ownership is by location. Every backing image lives under `/private/cowshed/store`,
  and enumeration is a `readdir` of that tree (`sessions/` plus the one canonical `main` image per project) — never a
  scan of `diskutil list`. A volume cowshed did not create has no image there and is therefore invisible to enumeration,
  gc, and crash-window classification regardless of what it is called.
- **Which workspace is a given image?** The sibling `<image>.grants.json` metadata, cross-checked against the image's
  filename stem. This is what makes discovery and attach possible while detached.
- **Is the volume mounted here ours?** The in-image marker `.cowshed/workspace.json`, matched on `repoId`, `workspace`,
  and `workspaceIncarnation`. Every operation that could damage something — healing a mount with wrong flags, detaching
  after a controller restart, joining a kernel mount into enumeration — reads the marker at the mount point and refuses
  when it does not name the expected workspace. A marker that cannot be read is not ours.

An image with no sidecar is not a discoverable workspace: inventory and the store-wide port allocator skip a named
`<workspace>.asif` with a warning rather than failing every project. Sidecarless legacy `<workspace>.sparseimage`
entries are also skipped with a named warning and never opened as workspaces. `doctor` names sidecarless `.asif` and
`.sparseimage` files directly under `sessions/`; `gc` may reclaim them only after checking the exact path, locks, and
mount ownership. A legacy image never becomes a live workspace just because its filename resembles one.

The orphan plan fingerprints the image's inode, device, length, and modification/change times; a same-sized replacement
invalidates the plan before any deletion. For a legacy disk image, `gc` also checks the image-path attachment inventory,
not merely the expected mountpoint: an image attached somewhere else remains in place and is reported as deferred.

A sidecar that exists but is malformed or mismatched is not an orphan; its uncertain grants and identity still require
an integrity finding instead of a guessed admission or deletion.

The marker is the discriminator because it is the one identity that is both authoritative and re-stampable. An APFS
clone inherits its source volume's name **and its volume UUID**, so a volume UUID recorded at creation cannot tell a
workspace from the fork made out of it; that is why cloning rewrites the marker inside the staging fence, before the
clone is ever published. Volume UUIDs are therefore not recorded and not used. The label is not rewritten there: a clone
is published under the label it inherited, and the workspace's supervisor, once it serves, reads the file system's own
name for the volume (`getattrlist`, no Disk Arbitration round trip) and relabels it in the background when it differs. A
rename is a Disk Arbitration round trip, and Disk Arbitration serializes every client on the host — measured at 9.5 s
and 27.8 s under load against 13–22 ms on a quiet queue — so it stays off the provisioning path. Replacement restores
and identity changes still relabel synchronously.

What follows: volume labels are free to be plain, and manual renaming is harmless rather than unsupported. Cowshed still
derives an internal per-workspace key from `repo_id` and workspace name to pair a storage fact with a kernel mount fact
within one project, but that key is computed from metadata on both sides and never read back off a volume.

## Dedicated store volume

All of cowshed's durable bytes live on one dedicated APFS volume, so the Data volume carries no image or store churn:

- **`cowshed.store`**, mounted at `/private/cowshed/store` — images, grant sidecars, waivers, quarantine, gateway config
  and audit, telemetry. Mostly rebuildable, with a small unique window: uncommitted work between autosaves
  (02_workspaces.md); durability is still git. Same-volume clonefile is preserved by construction — main → sessions →
  checkpoints and trash renames all stay within `cowshed.store`.

Rebuildable caches do not live on it. Each tool's cache stays at the tool's own default in the host HOME; the gateway's
npm mirror and bare repository mirrors live in cowshed's user cache directory, `~/Library/Caches/dev.cowshed/mirror` and
`~/Library/Caches/dev.cowshed/repo-mirrors`, written only by the gateway and readable by no sandbox; sccache's store is
sccache's own default directory, written only by its daemon. 03_caches.md gives the placement rule and how sandboxes
reach each cache. A host that still has the `cowshed.caches` volume of an earlier release keeps it mounted, encrypted,
and pinned by the same machinery below — never creating one — until `cowshed setup --retire-caches-volume` deletes it
(03_caches.md, "Retiring the caches volume").

The store is created once by explicit foreground `cowshed setup`
(`diskutil apfs addVolume <container> APFS cowshed.store -nomount`) and shares the container's free-space pool — no
sizing. The complete create/mount/pin transaction uses the one provisioning authorization session described in
14_nix.md.

Host-storage planning takes its APFS listing from the kernel, never from `diskutil apfs list`. One IORegistry snapshot
names every container (BSD name, capacity ceiling) and every volume (BSD name, name, volume UUID, `RoleValue`,
`Encrypted`); the kernel mount table (`getfsstat` into an owned buffer) names where each volume is mounted, and a volume
with no mount entry is detached. That one snapshot selects the container holding the home directory's exact mount-source
volume and is the global reserved-name guard, so the two decisions cannot observe different listings. The snapshot is
read once, with no retry or delay, and fails closed: an unreadable registry, an empty registry, a duplicated container
or volume identifier, a volume outside its container or at snapshot depth, an empty name, a non-canonical volume UUID, a
zero capacity, or two kernel mounts of one volume is an error, never evidence that a reserved volume is absent. The
listing `diskutil apfs list -plist` read back an empty root while an unrelated image detached; the registry has no such
transient projection. A volume mounted somewhere other than its canonical path is attested by `statfs` at that path
before it is reported as mis-mounted.

Neither planning nor read-only validation spawns `diskutil`. `diskutil info` is answered through Disk Arbitration, which
congests when many images are attached: on a host with about 130 images attached, a consumer's test harness saw one
`diskutil info -plist` of the store volume block for 279 s until its own 300 s deadline interrupted it, and gateways and
cache services run the same validation at startup. The same call answered in 0.43–0.64 s on that host once the
congestion passed; validation's latency no longer depends on Disk Arbitration at all.

FileVault is therefore derived from the registry record. No registry property names a volume's crypto users, and
`Encrypted` alone is not FileVault: a volume group's Data volume is encrypted at rest with a hardware-bound key and no
user. That keyless encryption exists only inside a volume group; a role-less volume is encrypted only by adding a crypto
user, which is exactly what setup's `diskutil apfs encryptVolume -user disk` does. This is an observed platform fact,
measured on a FileVault-off host against `diskutil info -plist`'s `FileVault`:

| Volume                            | `RoleValue` | `Encrypted` | `diskutil info` FileVault |
| --------------------------------- | ----------- | ----------- | ------------------------- |
| Data                              | 64          | Yes         | No                        |
| VM                                | 8           | absent      | No                        |
| Nix Store                         | 0           | Yes         | Yes                       |
| `cowshed.store`, `cowshed.caches` | 0           | Yes         | Yes                       |

All 138 volumes registered a `RoleValue`, and none registered `Encrypted = No`, so an absent `Encrypted` is an
unencrypted volume. For the selected reserved `cowshed.store` / `cowshed.caches` records, a role-less volume's FileVault
is its `Encrypted`; a reserved volume with any role, or none registered, is refused (`ReservedVolumeRole`), because for
it `Encrypted` can be keyless encryption. The rule protects the `MissingVolumeKeychain` refusal: a FileVault volume
without a usable System.keychain passphrase can never be remounted at boot, and reading keyless encryption as FileVault,
or FileVault as unencrypted, would let that refusal be skipped or misapplied. Setup shares the same evidence, so its
encrypt-in-place decision reads the same derivation. The execution-time pre-create recheck inside the authorization
session reads a fresh kernel snapshot the same way, and an unreadable or empty snapshot refuses the create.

**Boot mounting is owned by a root system LaunchDaemon, and the store is FileVault-encrypted.** At provision, the same
authorization session:

1. Creates the store when absent (`diskutil apfs addVolume … -nomount`) or mounts an already-created one at its
   canonical path. An existing store is never deleted.
2. Encrypts each volume that is not already FileVaulted, in place:
   `diskutil apfs encryptVolume <uuid> -user disk -stdinpassphrase` after it is mounted. A 32-character random
   passphrase per volume is stored in `/Library/Keychains/System.keychain` (`add-generic-password -a <label> -s <label>`
   with label `cowshed.store`, ACL limited to `/usr/bin/security` and the APFS user agents). A volume that is already
   FileVaulted without a usable keychain item fails closed — setup does not invent a new password and does not
   `deleteVolume`.
3. Appends one idempotent, comment-tagged fstab line per volume:

<!-- prettier-ignore -->
```
UUID=<store-uuid>  /private/cowshed/store   apfs rw,noatime,noauto,nobrowse,noowners  # cowshed created volume labelled cowshed.store
```

4. Installs and loads `dev.cowshed.storage` (`/Library/LaunchDaemons/dev.cowshed.storage.plist`) running a fixed
   `/bin/sh` script at `/Library/Application Support/dev.cowshed/mount-volumes.sh`. The script does not invoke the
   cowshed binary. For each UUID it: no-ops if already at the canonical path; fails if mounted elsewhere; otherwise
   `security find-generic-password -a <label> -s <label> -w | diskutil apfs unlockVolume <uuid> -nomount -stdinpassphrase`
   then `diskutil mount -nobrowse -mountPoint <canonical> <uuid>`. Missing keychain, symlink mountpoint, or nonempty
   stub fails closed.

UUID form is mandatory because labels are mutable (the same lesson nix-installer learned in
DeterminateSystems/nix-installer#212). `noauto` prevents Disk Arbitration from racing the credentialed remounter — Disk
Arbitration cannot supply System.keychain secrets. The script lives outside every user home and cowshed volume, so it
unlocks and mounts the store before login without depending on the cowshed binary or its version. `nobrowse` keeps the
store out of Finder, the Desktop, and the sidebar; nothing lands under `/Volumes`.

**`noowners` is deliberate.** The herd is machine-global and shared by every local account: with ownership honoring off,
every user sees the same bytes as their own, which is exactly right for git checkouts (git tracks mode bits, never
owners). It also removes the entire class of "mounted by another uid" classification failures. FileVault here is at-rest
protection for a stolen disk, not isolation between local accounts. The trust consequence is stated plainly: any local
user can read or write anything on the store once it is mounted. Cowshed is for machines whose local accounts trust each
other; stronger isolation is a different product.

If the store volume is absent, unencrypted, missing its System.keychain item, or its mountpoint holds anything other
than a reclaimable stub, existing-only commands fail before mutation with `environment-missing` and an explanation of
exactly what is missing, detached, unencrypted, or mis-mounted. A volume mounted anywhere but its canonical root —
`/Volumes/<name>`, say — is **mis-mounted**, not missing. `cowshed doctor` reports the observed and canonical paths and
prescribes `cowshed setup`; setup announces the complete repair — including in-place encryption of an existing
unencrypted volume — opens one authorization session, and converges mounts, FileVault, keychain, fstab, and the boot
daemon.

**`cowshed setup` owns this transaction.** It is a host-level verb needing no repository context: gather evidence,
provision an absent store, repair a detached or mis-mounted volume, encrypt an unencrypted one in place, store
passphrases, validate markers, pin fstab, and converge the boot mount LaunchDaemon — reporting each volume's observed
state and the action taken. Storage-error hints across the CLI point at `cowshed setup`, never at adopting a directory.
Diagnosis is canonical: the same volume evidence yields the same verdict regardless of incidental mountpoint contents,
and reclaimable stubs are enumerated (by name) and reclaimed, not treated as fatal masking. A volume that exists but
carries a wrong or missing marker is reported precisely (role, expected versus observed); it is never silently
re-provisioned, because re-provisioning means deleteVolume. `--uninstall` removes fstab pins, the system daemon and
script, user agents, installed binaries, and cowshed's System.keychain items; it never deletes a volume.

**Mount ordering and the unmounted-masking guard.** The store's mountpoint is a plain directory on Data, and its root's
`.cowshed-volume.json` marker distinguishes the mounted cowshed volume from that bare directory. launchd agents write
pre-tracer stderr under `~/Library/Logs/cowshed/`, never under the mountpoint, so a reboot cannot recreate a masking
stub. What does land on a bare mountpoint before its volume is mounted — the sccache agent's socket and compile cache,
the gateway heal's directory-only `mnt/` scaffolding, Finder's `.DS_Store`, empty directories — is reclaimed before
remount; any other entry keeps the mountpoint masked.

Why a volume and not paths on Data: Data takes hourly APFS local snapshots, and a snapshot pins every since-rewritten
block of a multi-GB churning image — ghosts that path-level `tmutil addexclusion` does **not** prevent (exclusion stops
backup, not snapshotting). A dedicated volume gets no local snapshots, collapses backup policy to one per-volume
decision, and isolates the fsck domain and corruption blast radius (store loss = WIP since last autosave; Data untouched
either way). Rebuildable caches are not images: they stay at the tools' own defaults in HOME, where the host's own
builds already keep them (03_caches.md).

### One herd, multiple users

There is one cowshed per machine, anchored to no account. `/private/cowshed/store` is a plain directory on Data created
once at provision; it exists only as a mountpoint and fstab target. The per-user convenience is a `~/.cowshed/mnt`
workspace mount root (plain directories on Data), configurable at setup. No cowshed evidence, sandbox profile, or
metadata path derives from `$HOME`. Workspace images remain on the store volume while their live mounts appear under
`<mount-root>/…`, so per-image `clonefile` semantics, locks, and lifecycle are unchanged by sharing; concurrent
multi-user mutation of one workspace stays serialized by the same `<image>.lock` flocks, and cross-user `gc` policy is
deliberately blunt: any user may retire any shed, because `noowners` already made the trust model explicit.

## Runtime state: derived, never stored

| Question                | Source of truth                                                                                                                              |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Which workspaces exist? | For the selected primary `repo_id`, `readdir` its `sessions/` images plus that project's exactly one `main`                                  |
| What is attached where? | Kernel mount table (`getfsstat` into an owned buffer), matched by mount point, identity confirmed by the in-image marker                     |
| Workspace identity      | In-image marker `.cowshed/workspace.json`                                                                                                    |
| Grants                  | Sibling file `<image>.grants.json`                                                                                                           |
| Concurrency             | `flock` on `<image>.lock` per lifecycle operation; `<workspace>.intent.lock` held by the process executing that workspace's lifecycle intent |

The detached metadata required for discovery and attach lives in `<image>.grants.json`; at minimum it contains the
workspace identity, in addition to the grant schema in 04_sandbox.md, so `cowshed ls` and attach never mount an image to
learn what it is. Marker-derived fields that exist only _inside_ the image remain unreadable while detached.
`cowshed ls` reports name, image mtime, and mount state from host data, and fills `baseCommit`-class fields from a
cached info snapshot in the same sidecar — stale-marked, refreshed on the next attach.

### In-image marker: `.cowshed/workspace.json`

Written at adopt/new/fork/restore, at the volume root; travels with every clone:

```json
{
  "version": 1,
  "repoId": "acme/widget",
  "projectRoot": "<project-root>",
  "workspace": "raven",
  "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80", // fresh controller-minted 128-bit id on create/fork/restore
  "role": "workspace", // "main" | "workspace"
  "baseCommit": "8f31c2d…", // main's HEAD at creation
  "createdAt": "2026-07-11T12:00:00Z",
  "forkedFrom": null, // workspace name when created by `cowshed fork`
  "createdTrace": "4bf92f…" // trace id of the new/fork/restore that created this image (13_telemetry.md)
}
```

`workspaceIncarnation` identifies one mutable workspace timeline. Checkpoints retain the incarnation in their copied job
records; a fork destination and every restore result mint a fresh incarnation, so a reused numeric job id cannot collide
with controller telemetry from a discarded or sibling timeline. It is public identity, not a credential. The sibling
host sidecar carries the current incarnation for detached discovery; each job record carries the incarnation that
actually produced it.

`createdTrace` is the CoW-lineage anchor: `fork`/`restore`/`checkpoint` link the new trace to it, so the clone graph is
a queryable provenance graph (13_telemetry.md). The in-image `.cowshed/` directory also holds `token` (the gateway
identity, 0600, rewritten on new/fork/restore so identities never duplicate), the workspace CA **certificate** (the
public trust anchor for egress interception — the private key stays controller-side, 04_sandbox.md/05_gateway.md), the
in-image cache roots (03_caches.md), and the protected `.cowshed/job/` authority domain.

The trusted supervisor is the only live writer beneath `.cowshed/job/**`; every executed shell, named session, startup
hook, and descendant receives the mandatory child restriction before repository-controlled code runs (04_sandbox.md).
`.cowshed/job/records.arrow` is the framed protected `ProtectedRecord` stream: `Job(JobArtifactRecord)` or
`CheckpointManifest(CheckpointManifestRecord)`. Small terminal stdout/stderr may live as Arrow Binary in a Job row. A
stream promotes lazily to `.cowshed/job/<numeric-id>/out` or `err` only when it exceeds the bounded inline limit or a
checkpoint/background/replay requirement forces residency; no per-job path is promised. Each stream records byte count,
SHA-256, bounded summary, and exact captured/redirect plus inline/file discriminants. Checkpoint manifests carry
`version, repo_id, origin_incarnation, barrier_id, visible_jobs, records_sha256`; the visible stream commitment is
`storage_kind, bytes, sha256, protected_path` with path present iff file. Complete Arrow batches and sealed spills are
immutable. A records file legitimately holds frames its ancestor incarnations wrote, because fork and restore clone the
source image; the image's marker lineage (02_workspaces.md) is what authorizes them, and a frame from any other
incarnation is an integrity fault. The controller additionally emits compact Admission/Terminal/Checkpoint/Fork/Restore
audit records — existence/status/order/lineage and expected hashes/digests, never raw payload or artifact paths — to an
optional sink that nothing reads for a decision (07_api.md/13_telemetry.md).

### Grant files live outside the volume

`<image>.grants.json` sits next to the image on the host filesystem, owned by the invoking user, mode 0600. It is
**never** granted into any sandbox — a sandboxed process that could edit its own grant file could escalate itself. Only
cowshed-core (running unsandboxed as the controller) reads and writes it. Besides grants it carries the workspace's
identity and `workspaceIncarnation` needed while detached, the optional macOS-only `portBlock` binding (`{base, size}`;
base = the gateway's per-workspace data-plane listener, `base+1 … base+size-1` = the workspace's own bindable dev-server
ports; a new block is 64 ports, and a live block keeps its size until a `service_ports` grant grows it — a grant that
moves it keeps the old block in `retainedPortBlocks`, reserved to the workspace until it retires), and the detach-time
info snapshot described above. Linux sidecars omit `portBlock`. The workspace's CA **private key** sits alongside it
(`<image>.ca.key`, 0600, same controller-only, sandbox-denied treatment) — the gateway signs per-host interception
leaves with it; only the public CA cert ever enters the image (04_sandbox.md/05_gateway.md). Schema in 04_sandbox.md.

During macOS restore, `<canonical-image>.restore.json` is the sole recovery fact for an interrupted image/metadata
publication. Its exact v2, unknown-field-denying schema is
`{version, repoId, workspace, sourceCheckpoint, sourceIncarnation, replacedIncarnation, destinationIncarnation}`.
`sourceCheckpoint` and `sourceIncarnation` identify the retained checkpoint and must match its detached metadata;
`replacedIncarnation` must match the displaced image/grant/CA generation; and `destinationIncarnation` must match both
the canonical pending metadata and the `pre-restore-<destinationIncarnation>` undo name. The fact is fsynced before
pending metadata publication and removed, with a parent-directory fsync, only after detached metadata is activated (the
restore's audit record is emitted on the way, and gates nothing). Recovery uses that complete identity tuple to ignore
retained older undo generations and choose rollback or idempotent completion. There is no runtime restore journal,
`.restore-fences` directory, database row, or second mutable source of truth.

## Retention

Copy-on-write divergence only ever accumulates — checkpoints, idle workspaces, ZFS origin pins (09_substrates.md), and
APFS image-size ratchet all grow silently. cowshed keeps "storage efficient" from becoming a burden with retention
_conventions_, enforced by `cowshed gc`, never by a background daemon deleting work unasked:

| Object                             | Default retention                                                                     | Exempt / override                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Checkpoints                        | all younger than 14 days **plus the newest five per workspace, whichever keeps more** | explicit pin from a label or `cowshed checkpoint --keep`; pins never expire until unpinned |
| CI failure checkpoints             | same checkpoint rule as above                                                         | labeled `ci-fail`, therefore explicitly pinned until unpinned; `10_ci.md`                  |
| Idle workspaces                    | _flagged_ by `cowshed doctor` after 14 days no exec, **never auto-destroyed**         | landing/removal is always explicit                                                         |
| Trash images (`sessions/.trash/…`) | drained on next `gc`                                                                  | —                                                                                          |

- `cowshed gc` evaluates both checkpoint floors independently: it deletes only an unpinned checkpoint that is at least
  14 days old **and** is not among the newest five for that workspace. A user-supplied label creates an explicit pin;
  `--keep` pins the generated or supplied label. Pin state is authoritative detached metadata, not inferred from a
  filename, and an explicit unpin is required before such a checkpoint is eligible.
- Checkpoint quota accounting is per workspace. One authoritative `SubstrateStats` read reports the active image's
  logical and allocated bytes, all published checkpoint allocated bytes/count, and the pinned checkpoint byte subset.
  Admission projects `existing checkpoint bytes + active allocated bytes` and `existing count + 1`; pinned and automatic
  checkpoints both consume the cap. Exact `<=` boundaries are admitted, while `>` returns `Conflict` before cloning or
  publishing any checkpoint image, fact, or detached metadata. A sibling workspace never consumes this workspace's
  coordinator-owned quota.
- Garbage collection is two-phase. `preview_gc(repo)` is a read-only enumeration that returns an immutable plan of exact
  candidates with stable SHA-256 identity, host path, allocated bytes, and closed reason. Pinned checkpoints are
  retained and never become candidates. `execute_gc(plan)` acquires all plan locks without waiting, re-enumerates at the
  plan's observation time, and rejects a pin/incarnation/path/byte/concurrency change as stale before any mutation. Only
  an unchanged plan drains trash, orphan staging/session images and mountpoints, and prunes expired checkpoints;
  execution reports actual freed bytes. Nothing compacts an image: ASIF gives back only part of what its volume frees on
  its own (1.3 GiB of 3.3 GiB written and then deleted, measured), so an image's allocation tracks its high-water mark.
  A candidate whose own cleanup cannot finish is deferred by path and diagnostic while the sweep attempts the others.
  Uncertain ownership, invalid identity, or a stale whole plan still refuse: proceeding through those could delete data
  that does not belong to the candidate.
- Explicit workspace retirement is the sole exception to live checkpoint retention: its exact trash metadata authorizes
  one workspace-scoped cleanup plan that includes pinned checkpoints and pre-restore undo generations as well as the
  trash image and empty mountpoint. Preview and execution validate the repository, workspace, incarnation, checkpoint
  facts, and every associated artifact under the workspace lock; any change makes the plan stale before mutation.
  Cleanup deletes the retirement trash metadata last so an interrupted pass retains authority for the next idempotent GC
  pass. A missing canonical image or orphan checkpoint fact alone never authorizes deletion.
- `cowshed du` reports **written vs referenced** bytes per workspace and per checkpoint (the number that matters for CoW
  substrates — referenced is shared with the base, written is the true cost). `--json` for fleet dashboards. This is how
  a coordinator decides which long-lived workspaces to `cowshed rebase --fresh` (02_workspaces.md) to shed accumulated
  divergence.

## Tradeoffs

**No SQLite / state store.** Any database row describing mounts or workspaces is a cache of kernel or filesystem state
that drifts on reboot and Finder ejects, and drift demands reconciliation machinery. Deriving state makes "what cowshed
believes" and "what is on disk" the same thing by construction; `cowshed doctor` shrinks to invariant checks. Commands
read the source directories and owned kernel mount snapshots rather than maintaining a mutable cache of them.

**SPARSE and sparsebundle rejected; one format, no fallback.** Sparsebundle band files reintroduce thousands of host
inodes per workspace for no benefit (network-volume support is irrelevant here). SPARSE (`.sparseimage`) is the legacy
format ASIF replaces, and it loses or ties every row of "Format measurements": 2.4× slower to create, 7–16× slower on
small-file operations, 4–7× slower on sequential I/O, 1.6–3.5× slower to fill, and no better under clones. Keeping it as
a fallback would cost a second creation path, a second attach and detach tool, a format field in every sidecar and
marker, and an extension/metadata agreement check on every attach — for hosts older than macOS 26, which cowshed does
not support. Case-insensitive ASIF was rejected because case sensitivity costs one flag at creation and nothing
afterwards.

**`/Volumes` mount root rejected.** DiskArbitration-managed mountpoints save a mkdir/rmdir pair but put cowshed paths in
a shared namespace where Finder surfaces them and name collisions get renamed (`widget 1`). The configured mount root
gives short, stable, hidden paths cowshed fully owns.

**`~/Library/Application Support` rejected.** The Apple-idiomatic location puts multi-GB churning images on the Data
volume, where local snapshots pin their rewritten blocks and backup policy needs per-path exclusion machinery. A
dedicated volume costs nothing (container space-sharing) and keeps every image, grant, and telemetry segment out of
`~/Library`.

**A sub-mountpoint below the store root rejected.** The store volume mounts at `/private/cowshed/store` itself, not at a
directory beneath a Data-volume wrapper: one empty mountpoint inode on Data, no wrapper level that means nothing to
users, and every path one level shorter. The costs — mount ordering and the bare-directory guard above — are machinery
`attach` already had for the workspaces.

**Caches beside the images rejected.** Co-locating caches with images on the store unlocks no additional sharing: the
reflink boundary that matters is the image's _inner_ filesystem — clonefile cannot cross it regardless of which volume
the cache or the image file sits on (the wall 03_caches.md's reflink-reachability rule names). The only same-volume
relationship images need is with each other. Layer-3 caches therefore stay at the tools' own defaults in HOME, where the
host's own builds already use them; 03_caches.md ("A dedicated caches volume rejected") explains why they get no volume
of their own either.
