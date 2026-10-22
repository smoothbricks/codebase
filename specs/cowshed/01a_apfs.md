# APFS Storage

cowshed stores all durable artifacts on one dedicated APFS volume and derives all runtime state from the filesystem and
the kernel. Nothing cowshed creates is visible in Finder, the Desktop, or the user's home directory listing — and no
image or store churn lands on the Data volume, so the user's unique data keeps its metadata, fsck time, and snapshots to
itself.

Repository identity, logical layout, shared metadata and retention policy belong to [01_storage.md](./01_storage.md).
This companion owns the macOS/APFS implementation.

## Images

- **Format**: one — a RAW (UDRW) image (`.dmg`) holding one case-sensitive APFS volume, stored as a sparse file on the
  store's APFS filesystem. There is no second runtime format, fallback, or format field in metadata: `.dmg` is the only
  image extension anything enumerates. macOS 26 remains the floor. Every image starts as a clone. The sole metadata-only
  GC exception is specified in [16_build_volumes.md §Garbage collection](./16_build_volumes.md#garbage-collection). Each
  store keeps one **blank template** per capacity and owner, `.blank/<capacity-bytes>-<uid>-<gid>.dmg`, and a **mint** —
  adopt's main image, a build volume's first touch — is one `clonefile` of it to the image's canonical name plus one
  in-kernel attach ("Mounts"), verified with pinned `fsck_apfs -q` before `mount_apfs` mounts the exact volume at the
  requested root with `nobrowse,owners,noatime`. The clone takes the canonical name whole, so the name only ever holds a
  complete, formatted volume. A template is minted lazily, the first time a store mints at its capacity, in five
  unprivileged steps:
  `diskutil image create blank --format RAW --size <capacity-bytes> --volumeName [cowshed] --fs None <stem>-minting-<nonce>.dmg`,
  `hdiutil attach -kernel -nomount -noverify -nobrowse -imagekey diskimage-class=CRawDiskImage -plist <image>`,
  `newfs_apfs -U <uid> -G <gid> -e -v [cowshed] <whole-device>`, a `fsck_apfs -q` of the new volume, and owned-registry
  `hdiutil detach -verbose <whole-device>` of that exact whole device; only then is it renamed to its name. Only minting
  takes the template's lock, and a minter that finds the template once it holds the lock returns it, so concurrent first
  mints make one template and a mint that finds it needs no lock at all. Each minter stages under a name of its own
  (`-minting-<nonce>`), so a path is attached at most once: a minter killed with its attach still queued on
  `storagekitd` can have that attach land after the next minter removed its file, and on the single reused `-minting`
  name it landed beside the next minter's attach, leaving one path attached twice. Before it starts, a minter releases
  every attachment the kernel holds for any of the template's staging names (old reused name included) — the inventory
  decides, since an attachment outlives its file and keeps its path — and removes their files. No attach runs for a path
  the kernel already holds: the image driver refuses a second attach of one file, but not of a new file at the same
  path. The attaching user owns the image's device nodes, so formatting needs no privilege, and `-U`/`-G` make the
  volume root the invoking user's from the start — which is why the owner is part of the template's name. `diskutil`'s
  own `--fs APFS` is not used: it cannot ask for case sensitivity, and it leaves a root-owned volume root that an
  `owners` mount cannot write and only root can hand over. Like every clone, a minted volume shares its template's APFS
  volume and container UUIDs (nothing records or reads them; see "Ownership, identity, and the volume label") and
  carries its label. A capacity gets a template of its own rather than a clone of another grown to fit: growing the
  container is a `diskutil apfs resizeContainer`, a second `storagekitd` call on every such mint (0.93–1.0 s measured on
  a loaded host), where a template costs one mint per store and capacity (0.57–0.63 s measured).
- **Case-sensitive, always**: `-e` is the entire cost — one flag at creation. Clones copy the volume as it is, so
  `fork`, `checkpoint`, and `restore` never repeat it. A case-sensitive volume holds every path a repository can
  contain, including paths that differ only in case, which a case-insensitive volume merges. The checkout's own volume
  does not matter: adopt copies from it and sets the copied repository's `core.ignorecase` to `false`
  (02_workspaces.md).
- **Capacity and allocation**: 100 GiB sparse. Capacity is a cap, not a reservation; a 300 GiB image does not reserve
  300 GiB on the host. Override per project via `.cowshed.toml` `capacity`. Creation uses only the sparse `diskutil`
  path above, never `hdiutil create -type UDIF`, which preallocates the nominal capacity. Copies preserve holes with
  `clonefile` or a hole-aware data copy. Allocation checks use `st_blocks * 512` and `du`, not the apparent file size.
  Re-adoption records the current source and build images' capacity from their authoritative kernel IOMedia mappings
  before detaching them; an old image's apparent file length is not its virtual capacity. The fresh RAW image keeps that
  recorded capacity.
- **Live source snapshots**: a RAW file has no outer allocation directories, chunk tables, or bitmap that a clone can
  capture from mixed generations. Source-image snapshots read the source volume's current kernel mount flags, flush the
  volume only when it is read-write, and flush the backing file before descriptor-cloning the live image. A read-only
  mount has no dirty volume writes; its volume flush is skipped, not an EROFS error suppressed. Any flush failure on a
  read-write mount remains an error. Capture never unmounts, detaches, seals, or changes the source's mount flags. The
  clone is verified with pinned `fsck_apfs -q` before its first mount. A shell whose cwd is in main, an editor, or a
  source watcher does not prevent a fork. This is filesystem crash consistency, not an application-state guarantee:
  build volumes still use the sealed seeds and writer exclusion in 16_build_volumes.md.
- **Space reclamation**: sparse creation does not imply automatic return of guest-freed space. The measured RAW producer
  retained a deleted 1 GiB payload's host allocation after guest sync, image fsync, clean unmount, and detach.
  `hdiutil compact` supports SPARSE and SPARSEBUNDLE, not RAW. Reclaim guest-freed space by the verified re-adoption
  transaction in 02_workspaces.md: copy the live filesystem into a fresh sparse RAW image, verify its contents and Git
  state, and retain the old image until verification passes. The host reclaims old blocks only when the retained image
  and every clone or checkpoint sharing them are explicitly released. This costs a filesystem copy; neither `defrag` nor
  a sparse-file copy identifies guest-freed nonzero blocks.
- **Clone cost follows extents, not size**: `clonefile` shares the source file's extent map, and the first write to
  either file copies that map. First-write cost scales with extent count; `fork` pays it in its own `first-write` step
  before attach. When a clone or checkpoint shares an image's blocks, rewritten blocks move to new runs. Writes into a
  workspace do not rewrite main's extent map. Build state lives on volumes replaced at each land rather than in main's
  image (16_build_volumes.md, "Why two volumes"). `doctor` counts main's extents with one `F_LOG2PHYS_EXT` query per
  contiguous run and reports `main-extents`, a warning naming `cowshed defrag main` once the predicted first-write cost
  reaches the cold-fork budget (08_testing.md). `defrag` is the one remedy: it detaches the workspace exactly as
  `resize` does — a busy volume refuses before the image is touched — copies the image's data regions with plain
  `pread`/`pwrite` into `<image>.defrag` beside it (never `clonefile`, `copyfile(3)`, or `std::fs::copy`, all of which
  clone on APFS and would share the old map), punches interior holes after each following write and the trailing hole
  after sizing the copy (APFS may allocate zero blocks when extending it), `F_FULLFSYNC`s it, renames it over the image,
  syncs the directory, and verifies the result by attaching it before restoring the mount state it found. The copy runs
  at 3.1–5.4 GB/s on the store volume (8.9 GiB in 1.8–3.1 s, an 8,871-extent image back to 579). Nothing rewrites an
  attached image: the attachment holds an exclusive lock on the file (another `O_SHLOCK` or `O_EXLOCK` open fails with
  `EAGAIN`), and `diskutil image resize` and `diskutil image create from` refuse it as well. The copy needs, and keeps,
  free space equal to the image's allocated bytes while earlier clones and checkpoints still share the old blocks; the
  verb refuses before detaching when the store volume lacks it. Nothing enumerates `<image>.defrag` as an image or
  sidecar; the next `defrag` replaces one an interrupted run left, and `doctor` names it until then. Mains are never
  detached implicitly (the gateway keeps them mounted), so no path rewrites main on its own.
- **Detached growth**: under the image's lease, an `O_EXLOCK | O_NONBLOCK | O_NOFOLLOW` open proves no image driver
  holds the file and excludes attachment until the write is durable. The RAW file's length is its virtual capacity.
  Refuse a requested length not aligned to 4 KiB or not strictly larger than the existing length; extend with
  `ftruncate` and power-loss flush before closing. Extending creates holes, not a host allocation of the larger
  capacity. There is no image header or allocation map to parse or rewrite. Cowshed then attaches and verifies its own
  image, grows the container with `diskutil apfs resizeContainer`, and checks kernel capacity. A crash between file and
  container growth leaves a larger image around the old container: equal-capacity resize is refused, while a larger
  resize completes both steps.
- **Volume name**: the repository name for `main`, `<repo> — <workspace>` for every other workspace. The volume name is
  a label and nothing else: Finder shows it in place of the directory name for a mounted volume's directory, so it is
  written for the person looking at it. Nothing parses it, nothing classifies a volume by it, and nothing derives
  identity from it — renaming a volume by hand (`diskutil rename`) changes the label and nothing else. Identity comes
  from where the backing image lives and from the in-image marker; see "Ownership, identity, and the volume label"
  below. A minted image carries its template's label, `[cowshed]`, until its workspace's supervisor relabels it off the
  provisioning path; a build volume keeps it, since nothing mounts one browsable.
- **Spotlight**: the root storage service merges the fixed store and per-user mount roots into the Data volume's
  Spotlight Search Privacy `Exclusions` at setup and boot, preserving other entries. These exclusions and the image
  marker express indexing policy, not media-reference ownership. In mds **2418.6.3.9.400 arm64e**, an admitted mounted
  `AppleAPFSVolume` reaches a constructor path that acquires an owned IOKit port and forgets its name without releasing
  the send right. Ordinary unmount/eject cannot recover that ownership; a process reap releases it, not an indexing
  exclusion or reload. The constructor admission, branch evidence, controls and host mitigation are specified in
  [Spotlight media references](#spotlight-media-references). Cowshed does not restart mds as part of an image operation.
- **Time Machine**: nothing to set. Backup policy is one per-volume decision, not path exclusions, and Time Machine
  already leaves the store volume out: measured on macOS 26.6, `tmutil isexcluded /private/cowshed/store` reports
  `[Excluded]` though cowshed applies no exclusion (it runs no `tmutil`). Durability is git (`cowshed push`), never
  backup.

## Historical format measurements

The image-format prototype (`specs/cowshed/prototypes/image-format-bench/`: harness, `results/`) compared three
candidates on the store's APFS container, all created and mounted without root: case-sensitive ASIF (the historical
pre-RAW format), case-insensitive ASIF (`newfs_apfs -i`, otherwise identical), and case-sensitive SPARSE
(`hdiutil create -type SPARSE -fs "Case-sensitive APFS"`, attached with `hdiutil attach -owners on -nomount`). The data
set is one real Bun `node_modules` (3.6 GB, 34k files, 6.5k symlinks), 40,000 further symlinks, 2 GiB of
cargo-target-like files (24 × 64 MiB, 512 × 1 MiB), and a git repository tracking all but the target files (80,744 index
entries, 0.75 GB of loose objects): 6.3 GB, 55,821 files, 46,575 symlinks. One churn round rewrites every target file
(write a new file, rename it over the old) and relinks every symlink — 2 GiB and 46,575 relinks. Extents are counted as
`doctor` counts them; the first write is one byte rewritten in place in a fresh `clonefile` of the image. The host ran a
fleet throughout (load average 10–35), so single timings carry about ±20 %; the I/O rows interleave the three candidates
round by round.

| Metric                                                                   | ASIF, case-sensitive (historical choice)                                          | ASIF, case-insensitive                                                   | SPARSE, case-sensitive                                           |
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
| Work per fork for case sensitivity                                       | none                                                                              | none                                                                     | none                                                             |
| Largest capacity (documented / reverse-engineered)                       | just under 4 PiB                                                                  | just under 4 PiB                                                         | 128 PB (`man hdiutil`)                                           |
| Host dies mid-write (docs)                                               | allocation directory is versioned and switched atomically                         | allocation directory is versioned and switched atomically                | power loss during `compact` can damage the image (`man hdiutil`) |

The detached-grow timings above measured Apple's content resize, before the native header-growth cutover.

ASIF's 1 MiB chunks, its capacity limit, and its versioned allocation directory come from reverse engineering
(<https://schamper.dev/dissecting-apples-sparse-image-format-asif/>), not from Apple; SPARSE's limits and its compaction
warning come from `man hdiutil`. APFS's journal does not make the enclosing image driver's mutable allocation map a
snapshot: `sync_volume_np(WAIT)` followed by image `fsync` and `clonefile` can capture mixed ASIF map generations. The
pre-RAW ASIF implementation therefore captured only detached images, holding each inode with a shared no-follow lock,
validating its allocation map and cloning through that descriptor. It unmounted and detached a mounted source without
force and restored it afterwards; a source holder refused the capture. That policy blocked live-main forks, and the
cause of the observed inconsistent maps was not established by the earlier successful smoke reads. It is historical, not
the current RAW source-capture contract. RAW has no outer map; source captures remain live, while build seeds retain the
separate writer exclusion in "Images" and 16_build_volumes.md. `fsck_apfs -q` still checks the inner filesystem before a
mount, but cannot establish the outer ASIF map's consistency. ASIF's chunk table can mark a chunk unmapped, which fits
it returning part of deleted space on its own; SPARSE returns nothing until `hdiutil compact`, and little then.
`diskutil image create from` rewrites a detached ASIF image from its allocated chunks only: on the churned four-clone
image it took 2.8 s against the plain copy's 2.4 s and left 217 extents and 7.95 GiB against 792 and 8.83 GiB, with the
same content, case sensitivity, and owner. An ASIF capacity as large as the store volume measured no cost against 100
GiB: creation, attach, empty allocation, and the allocation after a fill and a churn round match within noise, the
churned image held 932 extents against 455 (about 1.5 ms more first write), and `df` inside the volume reports the
store's own free space as available. SPARSE at that capacity attached slower (525 against 287 ms) and once refused its
first detach with `EBUSY`.

Case-sensitive ASIF therefore costs nothing that case-insensitive ASIF does not: the flag is set once at creation, no
step recurs per fork, and every measured difference between the two sits inside the noise. SPARSE loses on creation,
attach, fill, small-file and sequential I/O, ties on `git status`, fragments under clones exactly as ASIF does, and
gives back less space.

## Mounts

APFS image attach uses
`hdiutil attach -kernel -nomount -noverify -nobrowse -imagekey diskimage-class=CRawDiskImage -plist <image>` exactly
once, naming the exact backing image. Read-only attachments add `-readonly`; attachment stays `-nobrowse`, and mount
options select workspace visibility. `-kernel` serves the RAW image from the kernel's own file backing store (`KDIURL` →
`KDIFileBackingStore` → `KDIReadWriteDiskImage` → `KDIDiskImageNub` → `IOHDIXHDDriveInKernel` →
`IODiskImageBlockStorageDeviceInKernel`): no userspace image helper serves its I/O, so none can stall, outlive its image
or hold a notification ("The IOMedia match-notification ceiling"). DiskImages2 (`diskutil image attach`, served by
`diskimagesiod`) is not used for cowshed's own images; its attachments are still read and released, since an earlier
build or another tool may hold one. The driver choice and its measurements are in "The two drivers, measured on one 1
GiB RAW image". Cowshed joins the reported whole device and one freshly probed APFS volume to that image in the current
I/O Registry, opens the raw volume read-only and rechecks ownership under that pin. Absence, a conflicting device,
multiple volumes or unreadable ownership refuses. Before the first mount, pinned `fsck_apfs -q <raw-volume>` checks the
filesystem; any nonzero result refuses publication and releases only the new owned attachment or reports its cleanup
refusal. The ordinary mounted path does not retry attach or verification.

Mount is `mount_apfs -o nobrowse,owners,noatime <verified-volume-device> <mountpoint>` as the invoking user; `--browse`
omits `nobrowse`, and read-only access adds `rdonly`. The existing root-owned event-store repair uses the same helper
with its distinct `nobrowse,noowners` policy, without `noatime`, then restores the original workspace policy. The
mountpoint exists before attach. The image lease and raw volume pin remain held across the host namespace lease wait and
the helper: the already-proven exact image/whole-device/volume identity cannot be recycled while that pin is held. There
is no combined `--mountPoint` attach, `diskutil mount`, fallback, retry or StorageKit cached-family selection. A mount
refusal retains the actual command status and diagnostics.

After success, cowshed holds the requested mount root and verifies it is the filesystem root sourced from the pinned
volume before publication or any in-image marker write. A failed attach, malformed reply, mount refusal or wrong
root/device releases only the new owned attachment or reports its cleanup refusal. An already-attached formatted or
recovered volume uses the same pinned `mount_apfs` boundary. Blank formatting, detached checkpoint verification, resize
and fixtures deliberately exercising an unmounted state retain `--noMount` and explicit `fsck_apfs -q` where required.
Namespace-only unmount and owned-image release use `funmount` on the same held, proven `OwnedVolumeRoot` descriptor, not
a mountpoint path, BSD device name or Disk Arbitration view. The root's filesystem identity comes from `fstatfs` on that
descriptor. Each ordinary attempt is unforced. The image lease and registry-proven raw volume pin preserve authority
across namespace admission, the native operation and its compound postcondition. After every owned volume is absent and
pins are released, guarded `hdiutil detach -verbose` releases the exact physical whole device. There is no
general-eject, DA-unmount, path/device substitution or automatic fallback. This does not claim Spotlight releases
departed references.

### Owned-registry mount boundary

A stale or foreign StorageKit descendant cannot authorize a mount or veto mounting the owned volume. On 2026-10-14 at
**21:28:12.102 +04:00**, `storagekitd[90506]` rejected the lease-overlap fixture's combined attach/mount with
`Error Domain=NSPOSIXErrorDomain Code=22 (Multiple disks to mount, cannot specify mount point)`. The fresh image owned
physical **disk177** and APFS volume **disk179s1**, whose OS fsck returned **0**. Failure cleanup nevertheless requested
unmount of stale **disk177s1** twice. The prior native release had succeeded and its exact registered tree departed;
release alone therefore did not repair this second cached-family selection. One exact-image no-mount attach followed by
pinned filesystem verification and `mount_apfs` of the exact freshly probed volume is the owner-layer cutover, not a
cache reset, delayed retry or parser waiver. Acceptance retains the original real lease-overlap lifecycle and its
complete cleanup expectation.

### Spotlight media references

Discovery starts in `MDSVolumeManager`, not in the index store. `-[MDSVolumeManager setUpDiskArb]` registers a Disk
Arbitration appeared callback matching `VolumeMountable == true`. Its dispatched block requires a nonnull `VolumePath`
and a `VolumeKind` other than `autofs`, then calls `+[MDSDisk createWithDADisk:andPathURL:synthetic:]` with
`synthetic=false`. The VolumePath-description-changed callback has no registration match filter: when the changed keys
include `VolumePath`, it applies the same kind/path checks and constructs a missing record. A known record with a
nonnull path is renamed instead; a null path enters unmount bookkeeping. Thus `--noMount` avoids discovery only until a
real mount path appears. Boot-data, requested-volume, synthetic mountpoint and login/home discovery also enter the
common factory.

mds **2418.6.3.9.400 arm64e** (binary UUID `E1E46335-715A-3AA8-A2DB-E41C09923418`) leaks the owned media port inside
`-[MDSDisk initWithDADisk:andPathURL:cPath:synthetic:volumePropertyFlags:stat:statfs:roleBits:]`.
`IOServiceGetMatchingService` at preferred address `0x1000fa588` returns the owned port, saved in `x26`. On the ordinary
`AppleAPFSVolume` success path, `0x1000fa81c` releases the properties CF dictionary and `0x1000fa820` branches over
`IOObjectRelease` at `0x1000fa828/0x1000fa82c`; a missing properties dictionary also bypasses the port release. The port
name is never transferred to an owning field and is later overwritten. The apparent `_parentFsUUID` field owns a CFUUID,
and `_daDisk` owns the original CF DADisk; their destructor releases do not balance this separate IOKit lookup. The
earlier `DADiskCopyIOMedia` lookup is balanced and is not the leaking acquisition.

The release branch is reachable only when conformance to `AppleAPFSVolume` is false, or when obtaining the parent of an
`AppleAPFSSnapshot` fails. It is not a later volume-disappeared, watched-set removal or periodic-reconcile branch for a
successfully identified APFS volume. Once this constructor forgets the port name, those paths have no owner through
which to release it. The right survives that mds process until process teardown. No signal provides selective cleanup:
this executable registers no SIGHUP handler, its SIGUSR1/SIGUSR2 handlers return immediately, and SIGINT/SIGTERM enter
shutdown. A daemon relaunch is process teardown, not a successful reconciliation of one departed volume.

Admission is not unconditional for every mounted APFS volume. The common factory opens the mount path and uses
`fgetattrlist` with `ATTR_CMNEXT_NOFIRMLINKPATH` to obtain its kernel-resolved filesystem path, without firmlinks. It
does not trust the URL spelling or stop at a symlink. Before construction, `strstr` rejects any path containing
`/firmwaresyncd.`, `/var/tmp/livefs`, `/var/folders/`, `/System/Volumes/Update` or `/msu-target-`, with failure reason
`-8`. These are substring tests, not root-prefix tests: `/private/var/folders/cowshed/502/mnt/...` matches;
`/var/cowshed/...` does not.

The factory then requires successful stat/statfs and filesystem-identity checks; an ordinary directory on Data is not a
distinct volume. For an `apfs` filesystem it asks `APFSVolumeRole` about the actual mount-source device. Query failure
refuses construction, and accepted roles are exactly `0`, `1`, `2`, `0x10`, `0x40`, `0x180` and `0x240`; other roles
fail with reason `-5`. An ordinary role-0 workspace outside the excluded canonical paths is admitted. The factory
selects `MDSDiskAPFS`, whose initializer calls the base initializer that loses the port above. There is no image-format,
`nobrowse`, read-only, owners/noatime, Spotlight-marker or Search Privacy admission gate. Cowshed must not impersonate
firmware/update paths or assign a false filesystem role to evade discovery.

The disappeared callback releases the disk record: it calls `copyDiskDisappeared:`, removes the DADisk map entries and
calls `diskUnmountComplete:`. That closes record ownership, not the separate, forgotten matching-service port.
Index-store policy runs after construction and cannot repair that missing release.

The controls on this build identify each object by its attach-reported device and registry ID, then join root `lsmp`
rights against the live I/O Registry after successful release, with the same mds process throughout:

- Seven mounted APFS volume instances each retained one Mach send right with user-reference count **1** after leaving
  the registry. These cover fresh unique-UUID images, read-write and compressed read-only images, `/Volumes` and a
  custom `/private/tmp` mount, reattachment, and two volumes in one container. The two-volume case retained one right
  for each volume independently; duplicate UUIDs are not required.
- Two mounted HFS+ volumes, one writable and one compressed read-only, retained no IOKit media rights. One APFS volume
  attached but never mounted retained none. Image creation/conversion alone added no retained rights.
- A separate single-volume RAW comparison retained the exact departed APFS object through all four sequences: default
  hdiutil attach/detach, diskutil noMount plus manual mount/native unmount/hdiutil detach, combined DA attach/mount plus
  hdiutil detach, and combined DA attach/mount plus diskutil eject. Changing to DA ownership therefore does not itself
  prevent this mds defect.
- The accumulated cohort contained **7,221** departed AppleAPFSVolume objects held by mds and none held by DA. Process
  teardown freed **7,220**, leaving one other departed object; image-device counts were unchanged. This distinguishes
  the APFS port leak from the separate DI2 device/client leak.

- Two fresh APFS volumes mounted under the genuine Darwin per-user temporary root took **no** mds media right: one
  through its direct path, one through an outside symlink resolving into that root. DA recorded
  `/private/var/folders/...` for both. The same mds PID **27210**, launchd generation **5**, remained throughout; no
  daemon restart accounts for the result. This measures canonical-path admission, not marker effectiveness.

Cowshed must not depend on mds releasing these references to finish unmount, eject, reclamation or publication.
Attachment presence is the kernel's current registered image/device mapping, not the class's still-live instance count.
Accumulated departed volumes must be surfaced as a named doctor finding with their counts and an attribution next step,
separately from image-device attach-pool headroom. On the measured host, a separately managed watchdog reaps mds when it
holds more than **500 AppleAPFSVolume rights**, recording `mdsAPFS` and `mdsKilled`. That bounds accumulation; it does
not repair mds or free leaked DI2 image devices. Indexing suppression is not evidence of absent or released media
references.

### Stable workspace mount root

On macOS the session mount root is fixed at `/private/var/folders/cowshed/<uid>/mnt`, where `<uid>` is the invoking
user's numeric UID. `/var/folders` is its familiar spelling; cowshed derives the canonical path, never stores or accepts
a configured alternative. The path deliberately reaches the pre-constructor exclusion above, before mds can acquire the
port. It is a real mount directory, not an index-policy flag, a fake OS-update path or a renamed volume. A main still
mounts at its original checkout path; this policy does not relocate existing checkouts or release references mds already
holds. An indexed custom root also lets Spotlight hold workspace mounts busy during detachment, as observed during host
migration: the fixed physical path must avoid Spotlight admission, not merely have a different spelling.

`cowshed setup` provisions the root in its existing privileged transaction: `/private/var/folders/cowshed` is root:wheel
mode **0755**, and its `<uid>` and `mnt` directories are owned by the invoker with mode **0700**. Setup creates missing
directories idempotently, attests ownership and type, reports the fixed path, and refuses a symlink, non-directory or
foreign owner rather than writing through it. No separate operator mkdir, daemon bounce or automatic move of an attached
workspace is required. Ordinary session and build mountpoints are created beneath `mnt` on demand.

The fixed `mnt/` component is part of the policy. `dirhelper`'s scheduled cleanup addresses the `T` and `TemporaryItems`
children of `/var/folders/<first>/<second>`; boot cleanup also addresses `Cleanup At Startup` and `X`, and `C` in safe
boot. It does not recurse through an arbitrary `mnt` child. Placing repository owners directly under `<uid>` could
collide with those names on case-insensitive Data. An OS-generated per-user `T` path is not a stable path contract
across boots; cowshed therefore provisions its own stable subtree, outside those cleanup slots. Empty mountpoints can be
recreated without moving the backing images. This is not a new Time Machine policy: backup inclusion remains a
per-volume decision.

There is **no mount-root configuration** and `setup` has no `--mount-root` option. Linux likewise derives its fixed
`~/.cowshed/mnt` root. The supported way to use another access path is an operator-created symlink **from the preferred
path to the canonical root**, for example `ln -s /private/var/folders/cowshed/$(id -u)/mnt ~/workspaces`. Never replace
the canonical root with a link to an indexed directory: the kernel-resolved physical mount path is what mds admits.
Cowshed's probe and workspace jobs use canonical mount paths; identity rules anchored only at an alias are not reliable.

An existing `host.json` with a custom `mountRoot` is refused on read, with the configuration file, recorded root and
required fixed root named. The reader neither ignores the value nor rewrites the file, moves mounts or attaches at the
new path. The operator uses the previous installed build to stop workspace jobs/daemons and detach every session and
build volume at the old root, resolves busy holders (including Spotlight), and audits path-bearing detached metadata and
sandbox grants before reattachment. Only after that explicit maintenance boundary does the operator remove `mountRoot`
from `host.json`, preserving credential routes and mode 0600, and run the new build's ordinary `cowshed setup`. There is
no automatic live migration and no requirement to move backing images. A legacy `mountRoot` already equal to the derived
root is harmless to read and is omitted on the next ordinary configuration save.

The 2026-10-14 production-provisioning proof created the root with the ownership/modes above, then ran the real CLI
parser/dispatcher against an isolated scratch host: adopt, doctor, fork, rm, doctor. Fork published the expected payload
beneath the stable root in **0.950 s**; rm completed in **1.094 s**. Across before-fork, after-fork and after-rm
censuses, AppleAPFSVolume instances/registered/departed were **527/79/448 -> 528/80/448 -> 527/79/448**; IOMedia were
**83/74/9 -> 84/75/9 -> 83/74/9**. The same mds PID **27210** remained running and held no right to the exact new APFS
object **0x100081ffa**, either while mounted or after removal. The live host configuration remained byte-identical; only
the scratch host selected the new default, and all owned scratch images/mounts were released.

This proves the session-root APFS prevention, not that every class counter returned to baseline. The DI2
instances-minus-registered readings increased for AppleDiskImageDevice (**528 -> 529**) and DIDeviceIOUserClient (**524
-> 525**), so the manual example failed its unchanged four-class oracle. That failure remains a measured fact; it does
not by itself identify a surviving object or an uncompleted release. The subsequent trace below closes the operational
release question. Other pre-existing build jobs were not cancelled; this receipt does not claim host-wide idleness.

### Release and device custody

The 2026-10-14 **15:43:32–15:43:45 +04:00** scratch-host trace observed release, not merely absence from inventory. DA
received eject for each owned whole device and logged **eject ongoing -> removed whole device -> removed APFS container
-> removed APFS volume -> ejected success**. This covered the blank template and adopted main on
**disk37/disk38/disk38s1**, then the fork on **disk39/disk40/disk40s1**. Each attachment had a fresh diskimagesiod
instance: PIDs **2329**, **3391** and **4276**. Each logged file-descriptor unlock, its exact-device termination
callback, zero remaining client connections, **signal_terminate** cancelling Mach ports and waiting for its barrier,
**DIIOManager::~DIIOManager(): Closing IO user client**, and **Shutdown is complete**. The fork published in **1.503 s**
and rm completed in **1.541 s**.

The traced fork registered device **0x1000825b6** and client **0x1000825b7** under helper **4276**, a different pair
from the earlier proof (**0x100081fe4/0x100081fe5**, helper **38849**). Both pairs left the registry. The earlier helper
was absent and a root all-process Mach-port census found neither earlier ID; owned mount and scratch teardown checks
passed. The trace therefore establishes successful supported eject and helper shutdown, with no remaining operational
device, owned mount or identified holder. A missing cowshed teardown call or surviving helper is not an open item for
this proof, and no additional eject or watchdog reap is justified by these counts.

The traced cycle still increased the DI2 instance totals by one each while registered counts returned to baseline:
AppleDiskImageDevice **591/60 -> 592/61 -> 592/60**, and DIDeviceIOUserClient **587/60 -> 588/61 -> 588/60**.
AppleAPFSVolume and IOMedia returned to their before-fork counts with the same mds process. The unchanged strict manual
oracle consequently failed again. These counters do not decrement in this measured cycle; that observation alone is not
proof of a leaked object. Registry removal, successful eject and helper termination do not observe kernel allocation
destruction or heap release, and this evidence does not claim either. A failed eject or a positively identified
remaining device/holder would be a distinct operational finding.

The generic kernel-allocation leak oracle pairs **sudo ioclasscount** (per-class IOKit instance counts) with **sudo
zprint** (kernel-zone elements): snapshot both, run one owned cycle, snapshot both again, then diff the raw readings.
Retain the corresponding registered-ID sets and release trace; record concurrent host activity rather than attributing
every host-wide change to the fixture. DI2 instance counts climbing while registry sets return to baseline is precisely
the accounting case this class-and-zone comparison must distinguish from an operational device left attached. No zprint
before/after delta was captured for the trace above, so it is not a completed kernel-allocation proof. Neither a counter
delta alone nor a successful eject alone proves kernel heap retention or release.

Owned-image release selects the current physical whole IOMedia and its actual APFS volume descendants from the image's
registered provider tree. Under the image lease it raw-pins and rechecks each exact owned volume. After host namespace
admission it reads current kernel mount facts and, when mounted, proves the held root with the same `OwnedVolumeRoot`
used for naming. `funmount` acts on that descriptor, never a cached device or path. Positive kernel absence is a
no-effect success. The caller supplies no mounted-device snapshot as release authority. After the volumes are absent and
raw pins released, `hdiutil detach -verbose <physical-whole-device>` releases that exact image through the image
driver's interface. No BSD-prefix or StorageKit cached family, `diskutil eject`, DA-unmount or automatic fallback
selects the effect.

`WhenIdle` returns a recognized busy refusal immediately, without force. `Release` gives the first recognized busy
refusal its existing 10 s monotonic wall-clock grace, including native/command execution and disk-lease waits between
attempts, then makes its existing final forced release: `funmount` with `MNT_FORCE` of only a still-owned root, followed
by `hdiutil detach -force -verbose` of the exact physical whole device. Force is never the ordinary operation or a
remedy for cowshed's own held descriptor. `WithinGrace` retries through the same grace without forcing and returns the
refusal still standing. Requested poll sleeps alone never define that deadline. Native unmount recognizes only actual
`EBUSY`; it does not infer an external holder or PID from that errno. The existing `diskutil unmount` named-dissenter
parser and `hdiutil` busy/dissent recognition remain unchanged. Permission, authority, kernel-read, postcondition,
general and unrecognized failures are errors, never grace or force authority. Every detach runs `-verbose`; the
`detach image` span retains `dissent=<reason>` on refusal and `waited=<account>` for an accepted detach slower than 1 s.

The disk-image child policy warns at the caller's deadline and reports progress at the caller's configured cadence,
rather than killing an in-flight `hdiutil detach`; this does not widen the release grace. The kernel's I/O Registry is
the one host view that maps an image's path to its devices (`diskutil image info` reports none): one
`IOServiceGetMatchingServices` snapshot per image driver, each in-kernel drive's `KDIURLPath` (the path its attach was
handed) and each `AppleDiskImageDevice`'s `DiskImageURL`, with the IOMedia of that device's own subtree, reread when a
concurrent attach or detach invalidates a child list. A question about one image names the image's path, and its file
URL as CoreFoundation spells it, in each driver's matching dictionary (`IOPropertyMatch`), so the kernel answers that
image's devices alone and the read costs that image however many others are attached. Walking every registered IOMedia
node up to its image device instead cost 7 ms per question with 54 images attached and 110 ms with 799, and a verb asks
several. `hdiutil info -plist` is not an inventory: while any other image attaches or detaches it answers a truncated
image list with nothing marking it (a reviewer probe that kept one image attached while churning another saw it missing
from 11–89 of every 648–1551 polls, and from none of the same polls' registry reads). Reading that omission as absence
once skipped a post-format release, leaving the image attached for `diskutil image resize` to refuse as busy, and once
lost a mounted workspace's attachment on restart.

The image driver registers an attach's media before the attach reports them, so creation requires the reported blank
whole device to be the image's exact single-device mapping in one registry read before formatting. Absence there is a
contradiction, not lag: it fails at once, as does an observed conflicting mapping, and an unreadable inventory is a
typed refusal; each leaves both the attachment and backing file intact for diagnosis. No mutation is retried and no
unproved device is formatted. Shared owned-image cleanup distinguishes this unformatted whole-device state from an APFS
volume: it rechecks the exact mapping under the image's lease and releases only that owned device without deleting the
backing file first. Every detach attempt, including a dissent retry and the final force, rechecks that mapping in
`run_guarded` after the host storage lease wait, immediately before spawning `hdiutil`. Each volume unmount instead
carries its proven exact image/whole-device/volume identity through the host lease wait under its raw pin. An earlier
unpinned check is not authority after a lease wait; a recycled `diskN` cannot substitute another image. Confirmed image
absence needs no operation; a positive conflicting mapping or unreadable inventory refuses the mutation. A release
deduplicates whole-device views, so the same attachment is never detached twice.

### Owned-registry release boundary

The release owner selects the exact backing image, its registered whole device and that device's current provider-tree
descendants. Only those identities authorize release: a foreign or stale StorageKit cached descendant must neither be
selected for unmount/detach nor veto release of the owned tree. A reused BSD spelling is not ownership. The owner-layer
adapter preserves this boundary through descriptor-native `funmount` of the held owned root and exact-physical-device
`hdiutil detach`, with a registry-proven raw pin across the unmount's namespace lease wait and a positive registry/image
guard after the detach's storage lease wait. An unrelated refusal remains an error; the boundary is not permission to
broaden the dissent parser, force a foreign volume, add an automatic command fallback, or weaken a fixture's complete
cleanup expectation.

On 2026-10-14 at **20:07:36 +04:00**, the real panic/kill cleanup fixture's retained `second.asif` was joined by the I/O
Registry to driver **0x100089e2a**, helper **36162**, physical **disk177**, container **disk179** and unmounted,
read-only volume **disk179s1** (volume UUID **02E5E265-1B57-4996-A2FE-E5B750FBC630**). Root `lsof` found no holder of
those raw device nodes. Nevertheless, `diskutil eject disk177` caused StorageKit/DA to request unmount of cached
**disk177s1**, marked mounted with the different UUID **73578864-29ED-47A4-BEF1-63916CA00560**, outside the image's
registered tree. Three actual requests returned only
`Unmount of disk177 failed: at least one volume could not be unmounted`, with no dissenter PID. The existing
no-dissenter refusal correctly retained the image and root; this was not the `Release` grace/force case.

Operator-authorized, exact-device `hdiutil detach` subsequently ejected that owned, unmounted image. The unchanged
cleanup case then passed **1/1 in 2.626 s**, nextest run **179547fa-7894-4cd8-a2af-31537ecf17a8**. This separates the
fixture's real expectation from stale framework routing, but did not repair the owner: a later full strict gate again
hit that same foreign cached descendant with a new owned retained image. Both the isolated pass and recurring failure
remain evidence. The current-registry-bound exact-volume unmount and exact-device image-driver detach above are the
permanent owner-layer cutover, not an automatic runtime fallback. Its acceptance must exercise real owned cleanup and
preserve the foreign/stale-descendant exclusion and command refusal controls, not borrow the isolated pass as proof.

**An absent registered device does not prove the backing file is unlocked.** On 2026-10-14, a 100 GiB RAW build image
remained locked by an orphan `diskimagesiod`: PID **28160**, born **11:58:37 +04:00**, fd **4u**, device **1,20**, inode
**53550146**. Root `lsof` named that exact current fd; the original and later attach clients/controllers were gone,
neither live image nor client registry named the helper or image, and the historical driver was the departed registry
object **0x10007ca74**. Read-only attachment failed with `Resource temporarily unavailable`. The historical BSD device
name was not cleanup authority.

A broad `diskimagesiod` restart reduced IOMedia instances from **270 to 28**, unmounting active workspaces, yet this
helper and its file lock survived. It is not the remedy. Only after rechecking the exact process birth, executable, UID,
fd/inode, absence of registered image/client ownership and absence of an in-flight attach was **that PID alone** killed.
Every other helper was left alone. Root `lsof` then reported no holder; the same
`diskutil image attach --readOnly --noMount --plist` succeeded in **0.229 s**. Its new whole device was positively
joined to the exact image through registry driver **0x10007f082** and media **0x10007f085**, then the owned probe was
ejected through DA in **0.079 s**. That proves release of this stale file lock, not reclamation of every leaked DI2
kernel object.

Recovery therefore names an exact orphan, not a daemon family: use root `lsof <image>` and process-birth/owner checks,
join current registry identities, and refuse cleanup while any live or in-flight owner remains. Only a proven dead
operation permits an exact-PID clear. Never bounce all per-image helpers, force-eject a remembered `diskN`, clear a live
fork, or infer ownership solely from PPID 1.

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
recovers and releases the exact image's current typed attachment before unlinking, then holds a nonblocking exclusive,
no-follow descriptor of the backing file across unlink. Empty registered mapping is not proof of an unlocked file: an
orphan helper's file lock remains a named refusal and preserves the file. Confirmed absence with an unlocked file, or an
already-absent file, needs no release command. A failed explicit cleanup is never retried from its destructor.

A namespace-only unmount, needed for adopt handoff or a sealed capture without releasing the device, holds the image
lease and raw IOMedia pin across host namespace admission and the operation. After each admission, the owner reads the
kernel table for the exact pinned volume. Confirmed absence is an idempotent no-effect success, including a second
unmount; no native effect runs and nothing is reported forced. A mounted volume's root is held as `O_EVTONLY` with
`O_DIRECTORY|O_NOFOLLOW`, proved by the same `OwnedVolumeRoot`, and its actual `fstatfs` filesystem identity saved and
rechecked on that same descriptor before `funmount`. An unreadable table or descriptor, foreign root, non-root directory
or changed identity is a typed error. A controller's earlier snapshot cannot authorize the effect after admission.
Restart-owned mounts additionally require their incarnation marker and matching kernel source.

The successful native postcondition is compound: `funmount` succeeds, the same held FD's `fstatfs` returns the measured
`ENOTSUP` dead-vnode result retained as `PostUnmountFd::Dead`, and an independent kernel mount-table read positively
establishes absence of the original filesystem identity. `ENOTSUP` alone is never absence. Another descriptor-read
failure, an unreadable post-table or the original filesystem still present is an error, not success or force authority.
`O_RDONLY` held roots make ordinary unmount busy; `O_EVTONLY` permits the measured unforced operation while preserving
same-FD naming and root identity, but does not keep the mount present. Neither a pathname reopen nor an errno waiver
substitutes for the compound proof. `WhenIdle`, `WithinGrace` and `Release` keep the busy/grace policy above. Ordinary
image release uses this same boundary for every owned volume before physical-device `hdiutil detach`; namespace-only
unmount does not release that device.

Attach, verification and every lookup that answers one attachment require exactly one APFS volume in one reported
container, and refuse anything else by naming what was reported. A release reads each of the image's attachments as its
whole device plus every APFS volume in that container, so it can also take an owned image whose container gained a
volume (for example, one a dead test added). Each mounted volume is unmounted under the image lease and its own raw pin,
after proving that volume belongs to the current provider tree; its raw pin preserves that proof across the host lease
wait. The physical whole device is then detached once with `hdiutil`, taking the unmounted volumes with it. A formatted
attachment that reports no volume, reports a volume without its container, or spans two containers is still refused.

Concurrent mount-table reads use `getfsstat(MNT_NOWAIT)` into a buffer each call owns: count first, allocate checked
space with room for new mounts, and grow and reread if the returned buffer is full. No reader borrows `getmntinfo`'s
process-static array, which another thread can overwrite during iteration. Every snapshot retains complete mountpoint
and source-device records; an empty mountpoint is a real inventory fault, not an entry to silently discard.

Disk device names are reusable, not image identities. Each image has its own private per-user lease,
`/private/tmp/cowshed-apfs-image-leases-<euid>/<sha256 of the identity>.lock`: an `flock` on a regular owner-only 0600
single-link file, opened without following symlinks, in a 0700 directory this user owns. The identity is the absolute
path produced by `attachment_inventory_path`, matched against the kernel's `KDIURLPath` (or a DiskImages2 device's
`DiskImageURL`) backing-file path; the lease hashes that same normalized identity, never a separately resolved alias. An
alias with another identity fails closed. Every cooperating create, attach, format, `fsck_apfs`, mount, unmount, rename,
recovery and detach of one image holds that image's lease, so independent processes serialize on one image while
operations on different images overlap. There is no host-wide device lock. A cooperating process acts on a device only
after the inventory positively maps the identity to it under the lease (or while a raw pin holds it), and a device is
freed only by a detach the same lease covers, so no same-user cowshed process recycles it inside the critical section. A
contended lease is awaited on a helper thread that hands the locked descriptor back when the holder releases it, within
the caller's admission budget; expiry names the lease file, the image and the holder's recorded pid, and no disk child
has started. The lease does **not** constrain another user's disk tools or a manual eject during the two mutations that
cannot run raw-pinned, `newfs_apfs` and `hdiutil detach`, because an open raw pin prevents releasing the device. Before
`fsck_apfs` or `mount_apfs`, cowshed opens the reported raw volume read-only, then verifies that the attachment
inventory maps **both** its whole container and its volume to the exact image. A verified attachment owns that raw
descriptor across the `attach_verified` → `mount` method boundary. A mount lookup of an existing attachment likewise
takes the image lease before reading inventory and returns a handle owning its raw pin through the kernel mount-table
read and mount decision. It is released when an already-mounted handle is dropped, after `mount_apfs` finishes, before
intentional detach, or before the exclusive `diskutil apfs resizeContainer` writer, which cannot run with a raw
descriptor open. Independent device ejects during container resize after pin release are not covered. While held, the
descriptor prevents even external `diskutil eject force` or `hdiutil detach -force` from releasing the image and
recycling its device name. Every attach path, including detached verification and retained read-only mounting, attaches
once and refuses a reported device lost before pinning without retrying. A conflicting or unreadable mapping fails
closed, without running fsck on the reported device. No live-fsck option authorizes touching a foreign mounted
container. Blank-image formatting keeps its image-to-whole-device check under the image's lease; its exclusive formatter
cannot share a raw-device descriptor with another opener.

For every mounted attachment:

- Session workspaces mount under the fixed per-user root at `<mount-root>/<owner>/<repo>/<workspace>`. `-nobrowse` keeps
  every cowshed volume out of Finder, the Desktop, and the sidebar regardless of Finder preferences.
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
[image backend](../../packages/cowshed/crates/cowshed-apfs/src/lib.rs) (`MacOsApfsBackend::mount`); the
[build host](../../packages/cowshed/crates/cowshed-core/src/storage/apfs/native/build_volumes.rs) always passes
`browse = false`. **Build seeds remain detached clones** of a quiet live volume, beside it in `build/` with a sidecar
recording the tree it was built at: the landing workspace's at every land, whether through `cowshed land` or a
fast-forward outside it, and otherwise the target's own (`runtime/build_volumes.rs::freeze_seed`/`capture` and
`clone_build_volume`; 16_build_volumes.md "Targets and seeds"). They inherit filesystem contents and markers, not active
mount flags. A [blank template](../../packages/cowshed/crates/cowshed-core/src/storage/apfs/native/blank_template.rs) is
formatted, verified and detached **without ever being mounted**. The
[bounded-exec RAM volume](../../packages/nx-plugin/src/executors/bounded-exec/ram-temp.ts) is a separate APFS volume,
not a `.dmg` image.

The table distinguishes options cowshed requests from flags the kernel supplies. Seeing `nodev`, `nosuid` or `journaled`
in `mount` output does not mean cowshed passed an option with that name. Likewise, no indexing or backup command is
hidden behind `nobrowse`.

| Setting                       | Workspace / live-build images                                                                                                                                 | Detached seed / blank template                                                                           | RAM temp volume                                                                          | Dedicated store / retained caches                                                                                                  | Reason and setter                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Read-write / `rdonly`         | Read-write by default; recovery-marker mounts request `rdonly`.                                                                                               | Neither is mounted.                                                                                      | Read-write by default.                                                                   | `rw` in fstab.                                                                                                                     | Builds must write and execute their outputs; recovery reads a marker without modifying the image. Image backend `mount`; `storage/apfs/native.rs::detached_image_incarnation`; `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `nobrowse`                    | Default; workspace `--browse` omits it. Live build volumes stay hidden.                                                                                       | Neither is mounted; template attachment uses `--nobrowse --noMount`.                                     | Requested at `diskutil mount`.                                                           | Requested at bootstrap/boot mount and in fstab.                                                                                    | Keep temporary/work volumes out of Finder, Desktop and sidebar. Image backend `mount`/attachment; build host `create_build_volume`/`mount_build_volume`; RAM `RamTempVolume.provision`; `storage/bootstrap/native/macos.rs::desired_mount_service` and `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `noatime`                     | Requested on every normal read-write/read-only image mount, including `--browse`.                                                                             | Neither is mounted.                                                                                      | Not requested.                                                                           | Requested in fstab.                                                                                                                | Suppress access-time updates in read-heavy builds; workflow decisions do not depend on those updates. Image backend `mount`; `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `owners`                      | Requested on normal mounts, including read-only mounts.                                                                                                       | No mount flags; filesystem ownership is inherited/formatted.                                             | Not requested; unprivileged Disk Arbitration mounting yields `noowners`.                 | Not requested; fstab specifies `noowners`.                                                                                         | Workspace permissions must be honored. RAM mounting with ownership would require authorization; launchd plists therefore live outside the RAM-backed `TMPDIR`. Image backend `mount`; RAM module's mount/authorization contract.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `noowners`                    | Only the temporary root-owned event-store repair mount; normal requested options are restored afterward.                                                      | Not mounted.                                                                                             | Kernel/Disk Arbitration property of the unprivileged mount, not an explicit RAM option.  | Explicit in fstab.                                                                                                                 | Permit marker repair in an inherited root-owned `.fseventsd` without root authorization; it is not the workspace's final permission policy. Image backend `retire_inherited_event_log`; RAM `provision`; `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `nodev`                       | Kernel flag on unprivileged image mounts; not in cowshed's option string.                                                                                     | Not mounted.                                                                                             | Kernel flag on the unprivileged mount; not requested explicitly.                         | Not requested by privileged bootstrap.                                                                                             | Device-special files must not become usable device nodes through an unprivileged mounted image. Applies at the image backend/RAM mount calls; cowshed has no separate `nodev` setter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `nosuid`                      | Kernel flag on unprivileged image mounts; not in cowshed's option string.                                                                                     | Not mounted.                                                                                             | Kernel flag on the unprivileged mount; not requested explicitly.                         | Not requested by privileged bootstrap.                                                                                             | Mounted code cannot gain privilege through set-user-ID/set-group-ID bits. Applies at the image backend/RAM mount calls; cowshed has no separate `nosuid` setter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `journaled` / `local`         | APFS/kernel filesystem properties, not requested mount options.                                                                                               | APFS is formatted, but no mount flags exist while detached.                                              | APFS/kernel properties.                                                                  | APFS/kernel properties.                                                                                                            | Keep the filesystem's normal consistency/durability behavior; do not confuse the kernel's `journaled` label with a separate HFS+ formatting switch. Image backend `attach_and_format_unlocked` calls `newfs_apfs`; RAM `provision` and host provisioning call `diskutil apfs addVolume ... APFS`.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `.fseventsd/no_log`           | Empty marker attempted after every successful read-write mount; read-only mounts do not write it.                                                             | Seed inherits the source's marker; blank template has none. A live clone is opted out when it mounts.    | No explicit marker; a fresh `nobrowse` volume starts with no historical FSEvents record. | No explicit marker setter in host bootstrap.                                                                                       | Disable persistent FSEvents history, not live watcher notifications. The image backend `opt_out_of_event_log` owns the policy. A marker planted after mounting governs a later mount; daemon mount processing is asynchronous. See [The event log](#the-event-log).                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Event-log permissions         | New directory `0700`; new empty marker requested as `0644` (subject to umask). Repaired root-owned directory becomes `0711`; historical records are retained. | Seed inherits the source's directory/marker; template creates neither.                                   | No event-log permission setter.                                                          | No event-log permission setter in bootstrap.                                                                                       | Directory traversal lets later mounts find the marker without exposing a directory listing; root-owned `0600` chunks keep their permissions. `apfs.rs::opt_out_of_event_log` (`mkdirat`), `opt_out_in` (`openat`) and `retire_event_log` (`fchmod`), all fd-relative and no-follow.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Root-owned event-store repair | One-time `nobrowse,noowners` staging mount (does not request `noatime`), then the caller's original `owners`/`noatime`/visibility options.                    | Neither is mounted; seed preserves its source's on-disk state.                                           | No repair path.                                                                          | This repairs an image's `.fseventsd`, **not** the dedicated store volume.                                                          | Add the marker without purging historical records; make root's directory traversable (`0711`, no listing) so later ownership-honoring mounts can find it without repeating repair. Image backend `retire_inherited_event_log`/`retire_event_log`; the final policy honors ownership again.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Spotlight indexing            | Adopt plants `.metadata_never_index` through `initialize_volume_root`; cloned volumes inherit it. Mounts write nothing.                                       | Seed and live clones inherit the adopted source's marker; blank template has none. Mounts write nothing. | No explicit indexing command/marker; mounted `nobrowse`.                                 | No explicit indexing command/marker; mounted `nobrowse`.                                                                           | Keep Spotlight off cowshed's volumes, browsable ones included. On a browsable mount the marker makes `mdutil -s` report "Indexing and searching disabled" where an unmarked volume reports "Indexing disabled"; a `nobrowse` mount reads "unknown indexing state" in either case; unprivileged `mdutil -i off` is refused ("Try as root"). The marker stops indexing, not the departed-volume leak ("How the APFS host degrades" §2). It is a file at the checkout root, so `.git/info/exclude` carries `/.metadata_never_index`, and copies and cache retirement skip it like the volume's own bookkeeping. Image backend `initialize_volume_root` plants it symlink-safe and exclusively at adopt, like `no_log`. |
| Time Machine exclusion        | No per-image/path exclusion setter.                                                                                                                           | No exclusion setter.                                                                                     | No exclusion setter; volume is temporary.                                                | No `tmutil` setter; OS policy excludes the dedicated store.                                                                        | Images live on a separate volume rather than the Data volume, avoiding Data-volume snapshot retention of build churn. Durability is git, not a cowshed-applied backup rule; user backup policy remains separate. Host provisioning creates/mounts the dedicated APFS volume; no code calls `tmutil`.                                                                                                                                                                                                                                                                                                                                                                                                                |
| Case sensitivity              | One case-sensitive APFS volume, inherited from the template.                                                                                                  | Seed inherits the filesystem; template uses `newfs_apfs -e`.                                             | Plain `APFS` in `addVolume`; no case-sensitive variant requested.                        | New volumes use plain `APFS`; no case-sensitive variant requested.                                                                 | Preserve distinct case-sensitive source/output names in image-backed trees. Image backend `attach_and_format_unlocked`; RAM `provision`; `storage/bootstrap/native/macos.rs::provision_apfs_volumes_in_session`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Root UID/GID                  | Invoking user's UID/GID at formatting, inherited by clones.                                                                                                   | Seed inherits ownership; template uses `newfs_apfs -U <uid> -G <gid>`.                                   | No explicit formatter UID/GID; mount ignores ownership.                                  | Provisioning chowns the root to the invoking UID/GID; fstab ignores ownership.                                                     | Make image roots writable by their owner without an elevated format. `blank_template.rs::owner`/`mint` supplies UID/GID to the image backend; `provision_apfs_volumes_in_session` issues/attests the host-root chown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| FileVault/encryption          | No extra image-volume encryption switch; durable image bytes reside on the encrypted store.                                                                   | Backing-store protection only; no per-image `newfs_apfs -E`.                                             | No encryption setter.                                                                    | FileVault enabled; label-keyed boot and UUID-keyed discovery passwords read back identically in System.keychain before encryption. | Protect durable bytes without a second per-image crypto/password layer. `storage/bootstrap/native/macos.rs::encrypt_volume_with` (`diskutil apfs encryptVolume -user disk -stdinpassphrase`) and `desired_mount_service` (`unlockVolume ... -nomount`).                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `noauto`                      | No image fstab entry.                                                                                                                                         | No fstab entry.                                                                                          | No fstab entry.                                                                          | Explicit in fstab.                                                                                                                 | Skip generic `mount -a`; cowshed's boot service explicitly unlocks/mounts the known UUID at the canonical path. `storage/fstab.rs::build_fstab`; `desired_mount_service`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Execution / `noexec`          | `noexec` is not requested.                                                                                                                                    | Not mounted.                                                                                             | `noexec` is not requested.                                                               | `noexec` is not requested.                                                                                                         | Builds, test binaries and scripts must run from their filesystem; `nosuid` is not `noexec`. The image/RAM/fstab option lists above contain no execution ban.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

Settings apply when the relevant path actually mounts or formats a volume. Already-mounted workspaces/build volumes are
not retroactively remounted just because the binary changed. In particular, the RAM path does not inherit the image
backend's new `noatime` or event-log marker policy, and detached seeds/templates have no live watcher or mount flags.

### The event log

`fseventsd`, the one root daemon behind every FSEvents stream on the host, also keeps a persistent per-volume event log
under the volume root's `.fseventsd` (Apple, _File System Events Programming Guide_,
[Preventing File System Event Storage](https://developer.apple.com/library/archive/documentation/Darwin/Conceptual/FSEvents_ProgGuide/FileSystemEventSecurity/FileSystemEventSecurity.html)).
Cowshed volumes do not want it: a checkout's watchers (the Nx daemon, editors) subscribe live and never replay history,
while the log costs the daemon work for every event on every logged volume. On a host whose `fseventsd` is already
saturated, every logged cowshed volume adds to that work.

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

So every read-write mount opts its volume out after `mount_apfs` returns and the held root/source is verified, before
the mount is handed back:

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
unlogged and plants nothing: a fresh `nobrowse` volume, mounted on its private staging directory to be marked and then
once by Disk Arbitration, carried no `.fseventsd` after both mounts (measured 2026-10-15).

Opting out removes the log's work, not the daemon's live delivery work, and does not make delivery reliable on a
saturated host: a volume's first events after its mount can go missing with no drop flag, logged or not (the first
50-file burst after a mount delivered 12 of ~80 events, the next all of them; a single update written after a stream
went live on a just-mounted volume, logged or opted out, was missed for its whole bound in 8 of 23 runs before the
regression synchronized on the mount). Even synchronized on `fseventsd`'s own `Mount` event, on this host one update
missed a 10 s bound in 3 of 8 runs — on the browsable, logged arm and the opted-out arms alike, before or after cowshed
touched the volume: the host's `fseventsd` losing a just-mounted volume's events, not the opt-out. Live delivery is
therefore a measurement here, not a regression assertion: on an opted-out workspace after detach and attach, a sentinel
arrived in 330 ms with 0 drop flags. The real-APFS regression asserts only what the mount decides: the `owners` and
`noatime` flags, the marker as an empty regular file, root's directory kept at uid 0 mode 0711, and no log UUID once the
retired volume mounts again. It waits for `fseventsd`'s `Mount` event — on a stream watching the directory the volumes
mount under — before reading the log.

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
2. **AppleDiskImages2 runs out, of kernel mappings held by orphaned `diskimagesiod` helpers or of leaked devices.**
   Every attached image has its own `diskimagesiod` (launchd job `system/com.apple.diskimagesiod.<UUID>`), which maps
   its IO request pool into the kernel: 36 shared buffers of 2 MiB each (queue depth 36, 2 MiB max IO), 72 MiB per
   helper, so 100 attached images hold about 7 GiB mapped. When the kernel cannot map a new pool the kernel log reads
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
   its behavior, and `doctor` names orphaned helpers (a `diskimagesiod` whose pid created no attached device's user
   client) with the root command that clears them. Idle-unmount is dropped because it reclaims nothing. Idle workspaces
   remain mounted; source and build images are released by explicit lifecycle operations, not a background idle-unmount
   policy. The one exception is the IOMedia match-notification ceiling (below): a verb whose attach would reach it first
   detaches idle workspaces, then and for that reason, never on a timer.

   The same error has a second cause, and only a reboot clears it: **leaked image devices.** Every attach creates an
   `AppleDiskImageDevice`, its `DIDeviceIOUserClient` and a `DIDeviceRequestPool`. Most leave the kernel with their
   detach; some stay alive after they have left the registry, counted among their class's instances but registered
   nowhere. `ioclasscount` reads the instances unprivileged (the registry root's `IOKitDiagnostics` class counts), the
   registry its live entries, and the difference is the leak. In one observed failure the first 150 struck with 1090
   device instances against 66 attached, exactly 1024 leaked, and no orphaned helper. Root `lsmp -a` showed no userspace
   reference to a leaked device (only the 66 live helpers held `DIDevice` rights, one each); restarting `mds` and
   `diskarbitrationd` freed none of them, and the reboot freed all (178 instances, 178 registered, after it). A clean
   attach→detach cycle leaks nothing: on 2026-10-22, 20 cycles each of cowshed's exact noMount attach, a default-mounted
   attach and a DI1 attach left no device or client behind, and neither did 8 concurrent attaches over 10 rounds or a
   live wave of `rm`, `land` and `detach`. The measured cause is the IOMedia match-notification ceiling below: a helper
   stalled past it stops draining its device-termination notification, which keeps every image device terminated
   afterwards alive. The pool's capacity is in instances, live and leaked alike, and no documented limit names it.
   Departed APFS-volume and IOMedia references are separate from this image-device pool. Their owner must be measured by
   joining exact registry IDs to root `lsmp`, not inferred from the class name. An earlier host attributed 1064
   APFS-volume and 1067 IOMedia rights to DA; the later 7,221-object cohort belonged to mds, with none held by DA. The
   current mds discovery and missing-release mechanism, controls and mount-root policy are specified in
   [Spotlight media references](#spotlight-media-references).

   The pool is bookkept per host user in `/private/var/tmp/cowshed-attach-pool-<euid>/pool.json`, a 0700 directory that
   survives the reboot, written under a lock beside it; the pool itself is host-wide, and each user's record holds what
   that user's commands met. The record is keyed by `kern.bootsessionuuid`, since `kern.boottime` moves within a boot
   whenever the clock is stepped. Every 150 a disk-image command meets is `ApfsError::AttachPoolExhausted`, a host
   finding whose next step is `cowshed doctor`, never "repair storage", and the system runner records the boot's first:
   the boot is spent, with the image devices' counts and the orphaned helpers at that moment when they can be read,
   until the operator, having freed what spent it, ends it with `cowshed doctor --clear-attach-budget`, or the boot
   ends. A later successful attach does not end it: an attach admitted before the first 150 can finish after it.
   Clearing the spent-boot fact retains the last exhaustion as evidence.

   **Admission.** The system runner refuses every DiskImages2 attach (`diskutil image attach`) before it spawns, inside
   the storage lease, with `AttachPoolExhausted` and its `next:` step, in two cases: this boot is spent, or the live
   `AppleDiskImageDevice` instances, registered and leaked alike, plus the storage phase's members
   (`DISK_PHASE_MEMBERS`, each of which may pass the gate before any of them spawns) would reach the pool's ceiling, 963
   instances. That bound is conservative, not a documented limit: in the boot of 2026-10-21 20:12 +04 the first 150
   (06:01:48) struck with 271 images registered and 963 `DIDeviceIOUserClient` and 972 `AppleDiskImageDevice` instances
   allocated, live and departed together. Each IO client maps 36 shared 2 MiB buffers that only its `free()` returns, so
   the allocation, not the leaked count, is what runs out; counting devices, never fewer than their clients, errs low.
   The recorded counts are later samples: the pool record holds 1,042 because its observer met its first 150 after 67
   more failed attaches had each allocated a device. A failed attach leaks one more device, so an attach that would fail
   is never started, and a refused one can run again once the pool is freed. A pool whose instances or record cannot be
   read refuses the same way (`PoolUnread`), since nothing says the attach would not fail, and so does a record whose
   bytes do not parse: it may have held this boot's 150. `doctor` reads such a record as empty with a warning naming the
   file and still reports the kernel, and the next write (a 150, the operator's clear) replaces it. A refusal names the
   recorded exhaustion and the live facts that explain its next step: proven orphaned helpers first when their mappings
   can account for it, otherwise a reboot for leaked image devices, never a guessed daemon restart for unrelated
   departed-volume references. Clearing a record does not repair the kernel and never grants capacity by raising a
   ceiling. `doctor` reports `disk-image-attach-budget`: the four classes' live instances, registered entries and
   differences, the ceiling and the headroom below it, the current boot's exhaustion evidence, and disk-image commands
   an earlier build interrupted, each a probable leaked device (the record keeps them until the boot ends; no current
   build writes one). It is an error while the boot is spent ("attach pool exhausted", with "reboot required" only when
   the image devices account for it), and a warning within a quarter of the ceiling, or when an earlier build
   interrupted a command. Its hint names a reboot for leaked image devices and proven orphaned helpers first for a 150
   those devices do not explain. `departed-volume-references` is a separate warning from the first departed APFS-volume
   or IOMedia instance, with exact counts and a root `lsmp` attribution step; it neither defines attach-pool capacity
   nor prescribes a daemon restart. Orphaned helpers are named only when two reads in a row agree and no `diskutil` or
   `hdiutil` ran during either, since a helper whose attach is under way has no device yet and killing it mid-attach is
   what leaks one; killing orphans frees their mappings, never a leaked device.

   **A disk-image command is detached from its caller.** A disk-image command is one that creates or releases an image
   device: any `diskutil image …`, any `hdiutil`, and `diskutil eject` (Disk Arbitration's release of an image device,
   classed so whatever it targets). It runs in its own session, ignores the hangup, interrupt and terminate signals
   addressed to it, and writes its output to per-run files the runner reads back after its wait, never to the caller's
   pipes. A caller killed mid-attach — an agent tool deadline, a process-group kill of `cowshed exec`, a supervisor
   reaped with its tree, an adapter that gave up on `cowshed disk-command` — therefore cannot interrupt it or wedge it
   on a dead pipe: the command finishes, and the next invocation reconciles the result from the kernel's attachment
   inventory (an attach that completed is registered, one that never started left nothing). Nothing in cowshed kills it.
   The bounded-exec RAM volume's detach follows the same rule on its side: it is never killed at its deadline, its wait
   is bounded and reported by name, and a stray's detach gets a short bound before the creator reports it and gives the
   create claim back, so one wedged stray cannot hold a chain of creators.

   **No disk-image command is killed.** For a disk-image command the caller's deadline is a warning point, not a kill:
   it is waited for, synchronously and still holding its leases, and reports its verb, target, elapsed time and observed
   progress or lack of progress at the caller's configured cadence, until it ends on its own. Each report also reads the
   host: the oldest disk-image command running on it (pid, command line, how long it has run) and every `diskimagesiod`
   serving no device. Attaches stall behind one another: measured 2026-10-21, one attach whose device never got its BSD
   node held every later attach on the host for up to 41 minutes, and each waiter said only that it was still waiting.
   The helpers are named as facts, never as a remedy: one whose attach is under way serves no device yet. Once that
   attach's client was killed, its helper stayed, still serving none, and launchd keys a helper by its image, so the
   next attach of that image reconnected to the wedged helper and hung again. A late child that succeeds is a success;
   one that fails is the caller's failure as always, and an attach's own failure path detaches whatever device it
   produced. Other disk children (`diskutil info`, `diskutil apfs …`, `mount_apfs`, …) use the caller's execution
   budget, never a second fixed disk-child timeout. A hung DiskImages2 verb keeps its own operation and image lease
   pending instead of leaking a device until reboot; the gateway's disk-phase eviction lets unrelated host work continue
   without killing it (05_gateway.md, "Disk-lifecycle lease"). Its caller stays cancellable: a caller that stops waiting
   leaves the detached command to finish. Callers outside cowshed get the same runner through `cowshed disk-command`
   (06_cli.md). The orphaned-helper `kill -9` does not contradict this: an orphan serves no device, so killing it
   strands nothing.

3. **Detach waits on the kernel and holders.** A non-forced unmount of a volume something holds is refused with `EBUSY`
   and retried; a land's target unmount took 12.2 s on its first try and then about 45 retries, and the image detach
   that followed 45.3 s. A held file anywhere in a volume keeps its image attached, so a stray process with a cwd inside
   a workspace blocks its retirement.
4. **Shared extents fragment the source, not the clone.** Writing an image while a clone shares its blocks moves every
   rewritten block to a new run, and every later clone pays the extent map on its first write ("Images" above).

What the substrate does about each:

- **Fewest storagekitd round-trips.** Production operations sit at their floor: a mint is one `clonefile` of the store's
  blank template plus one attach ("Images"), a fork or cold mount is one attach, a land with no retire makes no
  disk-tool call. Release unmounts each exact currently-owned mounted volume through its held root FD, then runs one
  `hdiutil detach` for the physical device. The earlier measurements below used one `diskutil eject` per release and do
  not claim latency for the current release mechanism. The mint floor was one `image create` plus a formatting attach
  and `newfs_apfs`. Measured on the same host and tests (CLI adopt and build-volume first touch, `apfs <leg>/mint`
  spans), adopt's mint went from 537–705 ms to 303–345 ms and first touch from 565–756 ms to 312–317 ms on a quiet
  queue; under a loaded parallel suite the old adopt mint took 10.6 s (create 2.4 s, attach 7.7 s) against 5.0 s for the
  new one, all of it the one attach. Values the image itself records come from the image, never from a `diskutil` query:
  a RAW image's virtual capacity is its file length, read without spawning a storage tool, and an attached image's
  kernel capacity comes from its authoritative IOMedia mapping.
- **No attach→detach→attach on a success path**, and no verify-by-reattach.
- **Storage calls and mount-table changes never overlap.** Every disk tool and native unmount runs under the gateway's
  disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease"): `diskutil`, `hdiutil` and `newfs_apfs` share storage,
  `mount_apfs`, `umount` and owned-root `funmount` share namespace, and the two alternate. The same eight attach loops
  beside two mount loops made 106 attaches in 43 s with the phases kept apart, at 0.94 s p50 and 1.78 s p95 under load
  167–212.
- **Test images are 1 GiB** and a fixture detaches its images on every exit path it survives, panic included; a killed
  run's images are reclaimed by the next run's sweep (08_testing.md). A leaked image costs one slot of the host's finite
  attach budget for as long as it stays attached. Tests mint one blank template per host user with the production
  minter, at `/private/tmp/cowshed-itest-templates-uid<uid>`, and clone every test image from it; a test store that
  mints is seeded with a clone of it. Like a store's template it outlives its minter and its name spells everything its
  bytes depend on, so concurrent lanes and later runs clone the same one instead of each test process tree paying a
  create, a formatting attach, a format and a detach before its first test. Real-APFS tests attach with `noMount` unless
  mounting is under test, reducing the departed-volume references `mds` retains; the separate real-APFS group keeps
  those tests out of ordinary per-crate runs.
- **A disk-tool timeout is a concurrency measurement first.** Raising the bound or serializing the suite hides the host
  contention instead of reducing it; the fix is fewer calls, and calls that do not starve each other.
- **`doctor`** reports main's extent count (`main-extents`) so fragmentation is visible before it costs a fork.

### The IOMedia match-notification ceiling

IOKit queues at most 1024 matches on one user notification and drops the rest without a word
(`IOServiceUserNotification::kMaxOutstanding = 1024`, XNU `iokit/Kernel/IOUserClient.cpp:809`; its `handler` adds a
match to the notification's queue only while the queue holds fewer), and a matching notification is seeded with every
registered service it matches. Every attached APFS image registers three IOMedia (its whole disk, its container's
`AppleAPFSMedia` and its `AppleAPFSVolume`), so a workspace with its build volume holds six, and an unfiltered IOMedia
match on a host with about 334 images attached enumerates 1024.

A DI2 attach waits for its own device's media. Its `diskimagesiod` helper registers an unfiltered `IOServiceMatched`
notification on IOMedia (`-[DIDeviceHandle waitForDeviceWithError:]`) and spins its run loop until the match names its
device. Past the ceiling that match is dropped, and the attach never returns. Measured from the kernel and helper logs
of the boot of 2026-10-21 20:12 +04: all 2,884 attaches that completed ran with at most 333 images attached, and all 23
that stalled started with 334 to 352. Each stalled helper logged `IOServiceWaitQuiet done` and nothing after it, where a
healthy one goes on to log its device's BSD name.

After the stalls began, every image device that terminated stayed allocated. In that boot the 23 stalled helpers were
exactly the ones holding two notification ports (healthy helpers hold one), and every device terminated after them, 702
in all, was never freed, until the first error code 150 at 963 IO clients and 972 devices allocated. The trigger, the
ceiling and the retention are measured, and the retain edge was reproduced live ("The retain edge, reproduced live on
2026-10-22" below): a stalled helper's termination callback deadlocks on the lock its own attach holds and never drains
its queue.

What is established about it: each `diskimagesiod` registers an unfiltered `IOServiceTerminate` notification on
`AppleDiskImageDevice` (static disassembly of `/usr/libexec/diskimagesiod`: `setupTerminationNotificationWithError:`,
called once from `tryAttachWithParams:` before `waitForDeviceWithError:`, on a port whose source is on the main thread's
run loop; `terminationCallback` drains its iterator until empty). The kernel keeps every matched service in a
notification's queue until the client drains it, and an undrained notification keeps a cleanly detached device and its
user client alive with no userspace right to either: a controlled probe holding one undrained `AppleDiskImageDevice`
matched or terminated notification held a detached device and client (+1/+1), and both were freed when it let go. An
undrained IOMedia notification instead keeps only the media, never the device. What the old boot alone could not settle:
in its port census the orphaned helper 69090's notification receive queues were empty, and the sampled stalled helpers'
main run loops were idle, not blocked. The retention also outlived the threshold: after 04:45 all 420 clean shutdowns,
at only 262 to 301 registered images, still left their devices allocated. The old boot is gone, its departed objects are
no longer in the registry, and SIP refuses kernel tracing, so that boot alone could not name the edge.

#### The retain edge, reproduced live on 2026-10-22

With registered IOMedia pushed to 1,027 by throwaway in-kernel images, one attach with cowshed's DI2 argv of a throwaway
RAW image hung, and its helper (pid 95499) logged nothing after `IOServiceWaitQuiet done`. During the next minute two
other workspaces' images detached cleanly, and both devices stayed allocated (188 `AppleDiskImageDevice` instances
against 185 registered, devices `0x100008c22` and `0x100008c46`). `sample 95499` shows the edge in all 175 samples:

- an attach thread inside `attachToNewDeviceWithParams:` (`+0x13fe4`, within its `@synchronized` section) in
  `tryAttachWithParams:` (`+0x13518`) waiting in `waitForDeviceWithError:` (`+0xc9e0`, the NSRunLoop spin) for the
  IOMedia match the cap dropped;
- the main thread inside IOKit's notification callout (`IODispatchCalloutFromCFMessage`) in `terminationCallback`
  (`+0x109c0`, just after its `objc_sync_enter` call), blocked in `objc_sync_enter` → `__ulock_wait2` on the lock the
  attach thread holds.

The first device that terminates anywhere on the host wakes the termination notification; its callback takes the wakeup
and then blocks before `IOIteratorNext`, forever. Its queue is never drained, so the kernel keeps every
`AppleDiskImageDevice` (and, through it, its user client) terminated afterwards, and no receive queue shows a pending
message: the old boot's empty queues and the 23 two-notification helpers fit exactly. A sample taken before any device
terminates finds the main thread idle, as the old boot's samples did. Killing the attach's `diskutil` client frees
nothing; only the helper's death releases the queue, and nothing but a root kill of that one helper can end it (SIP
refuses the DTrace alternative). The in-kernel driver has no helper, and so neither the wait nor the callback: in the
same session 170 in-kernel attaches took registered IOMedia from 574 to 1,084 at 0.14 to 0.27 s each, and their detaches
above the cap left nothing allocated.

#### The boot of 2026-10-21 20:12 +04, from its kernel and helper logs

- Until 02:26:11 every clean helper shutdown was followed by the kernel's `AppleDiskImageDevice::free()` (1,944
  shutdowns, 1,485 frees in the retained log; earlier kernel history was not retained). No helper shut down between
  02:26:11 and 02:46:30.
- The first stall was helper 55068 at 02:30:07.993, whose attach took the attached images from 333 to 334. All 2,884
  attaches that completed in that boot ran with at most 333 images attached; all 23 that stalled started with 334
  to 352. The registered IOMedia family then was the host's own base (about 21 to 25 media) plus three per image.
- From 02:46:30 no helper shutdown was followed by a free: 702 devices left the registry after a clean helper shutdown
  and stayed allocated, 148 of them created before 02:30 (129 of those by the startup heal's burst) and 554 after 04:10.
  Their user clients stayed with them (774 retained clients = 5 orphaned helpers + 702 + 67 failed attaches).
- At 06:10:28 the kernel counted 1,039 `AppleDiskImageDevice` instances: 256 attached, 5 behind orphaned helpers, 702
  retained after clean shutdowns, 67 from attaches that failed with error code 150 and 9 duplicate creations (error
  155). The first 150 struck at 06:01:48 with 271 images registered and 963 IO clients and 972 devices allocated, with
  the kernel's first `DISharedBuffer::init: Can't map buffer`, long after the retention began; each later failed attach
  allocated one more device. cowshed's pool record holds the first 150 its own commands met: 1,042 instances, 256
  registered, 786 leaked and 5 orphaned helpers.
- 238 of the boot's 261 helpers held one IOKit notification port; the 23 stalled ones held two, each with the device
  references of a helper that never completed its attach and no user-client connection. Apart from those helpers, no
  process holding an `IOServiceUserNotification` right was started in the 02:26 to 02:46 window.

#### What holds departed volumes, and what does not

`mds` and `diskarbitrationd` hold departed APFS volumes and media, never image devices. Spotlight's constructor leak
(above) holds one `AppleAPFSVolume` right per mounted, admitted volume. After the reboot only the four adopted mains,
mounted under the user's home, gave `mds` a right; shed and build volumes under `/private/var/folders/cowshed` gave it
none. Disk Arbitration holds rights to media briefly after a detach: all 15 it held right after a measured wave of
detaches were gone 294 s later. Killing both freed 1,795 departed volumes before the reboot but no image device: those
were held in the kernel, by the stalled helpers' notification queues, where `lsmp` shows no right to them.

#### The retention, reproduced

A 40-line probe arms one `IOServiceAddMatchingNotification` on `AppleDiskImageDevice` (matched or terminated), drains
its initial iterator once and then never runs its port. While it lives, one DI2 noMount attach and detach of a throwaway
1 GiB RAW image leaves the departed device and user client at 0 → 1 after the helper's clean shutdown → 1 sixty seconds
later → 0 once the probe releases its iterator and port, for both notification types. The probe's process holds only its
`IOServiceUserNotification` right, and its receive queue holds the one pending ping. An undrained notification on
IOMedia instead retains only the IOMedia, `AppleAPFSMedia` and `AppleAPFSVolume`, never the device or its client. Clean
cycles with no such subscriber leak nothing: 20 noMount cycles of cowshed's exact attach argv, 20 default-mounted
cycles, and 8 concurrent attach and detach pairs over 10 rounds all left 0 departed devices.

#### The two drivers, measured on one 1 GiB RAW image

| Driver and argv                                                                                          | Helper              | 128 MiB write + `F_FULLSYNC` | 1000 × 4 KiB creates | unlinks    | Retained per cycle (20 cycles) |
| -------------------------------------------------------------------------------------------------------- | ------------------- | ---------------------------- | -------------------- | ---------- | ------------------------------ |
| DI2: `diskutil image attach --nobrowse --noMount --plist`                                                | `diskimagesiod`     | 691.2 MiB/s                  | 4,836/s              | 19,248/s   | 0                              |
| DI1 out of kernel: `hdiutil attach -nomount -noverify -nobrowse -imagekey diskimage-class=CRawDiskImage` | `diskimages-helper` | 37.9 MiB/s                   | 62.6/s               | 796/s      | 0                              |
| DI1 in kernel: the same with `-kernel`                                                                   | none                | 769.8 MiB/s                  | 2,435.8/s            | 32,259.1/s | 0                              |

Medians of three rounds on a loaded host, not a causal ratio. Reads ran at 13 to 17 GB/s for all three, which is the
page cache, not the image. The in-kernel path attaches through `KDIURL` → `KDIFileBackingStore` →
`KDIReadWriteDiskImage` → `KDIDiskImageNub` → `IOHDIXHDDriveInKernel` → `IODiskImageBlockStorageDeviceInKernel`, with no
DI2 object, no image helper, and no userspace descriptor on the backing file; `IOHDIXHDDriveInKernel` carries the
backing file as `KDIURLPath` (a string) and `image-path` (bytes, no NUL). Blank formatting, read-write use and a
read-only attach with `fsck_apfs` and mount all pass on it. Images were attached with `hdiutil` until 2026-09-30
(`a7af3559d`), when ASIF, a DI2-only format, replaced SPARSE for speed; that SPARSE row was the out-of-kernel driver.

#### What multiplied the attaches

- The startup heal attached every session: after this reboot it re-attached 154 sheds (4 mains and 150 sessions), 308
  images, 931 registered IOMedia, in 171.5 s, most of them idle or already landed, and no exec or shell on any workspace
  was served until it ended.
- `cowshed detach`, making room and putting a detached workspace back after a refused `rm` released the workspace image
  and left its build volume attached: before the reboot the kernel held 164 build volumes, 88 session images and 4
  mains. A land and an `rm` released both.

Cowshed's own attaches no longer go through DiskImages2: every cowshed image is attached in the kernel ("Mounts"), which
has no image helper to stall at the ceiling or retain devices after it. Whether an in-kernel attach completes with more
than 1024 registered IOMedia is not yet measured, so until it is, cowshed still never starts an attach past the ceiling.

Cowshed therefore never starts an attach past the ceiling. The system runner refuses an image attach whose media would
reach 1024 before it spawns (`MediaCapReached`, `cowshed-apfs` `media_cap`; 02_workspaces.md, "Making room for an
attach"), counting the registered IOMedia, subclasses included, three for every attach running on the host and for every
other member of the storage phase, and 16 for disks the host attaches on its own. A verb that may attach first detaches
idle workspaces, least recently used first: only workspaces with no supervisor serving them, never main, the named
workspaces, or one a land or `rm` holds. If nothing idle frees enough, it detaches nothing and refuses by name, listing
ten attached workspaces with the media each holds and the count of the rest. Attaching every session (`cowshed attach`
without a name, or `--all`) and controller operations get the guard's refusal only. The gateway's startup pass mounts
mains only and leaves sessions to attach on use (05_gateway.md, "Startup contract"), and a detach frees a workspace's
build volume with its image (02_workspaces.md, "`cowshed attach` / `cowshed detach`"). `doctor` reports
`media-notification-cap`. A helper that stalled before these gates is an orphaned helper
(`orphaned-disk-image-helpers`).

### Freeing an image

One `unlink` of a written image frees its whole extent tree in one container transaction, and every writer of the
store's APFS container, the Data volume's log and fork writes included, waits behind it. A thread at background I/O
priority (`IOPOL_THROTTLE`) makes that worse, because its own metadata reads are throttled while the container waits. So
cowshed never throttles an image free and never frees an image in one call: it cuts the image from its end with
`ftruncate(2)` at most 1,024 physical runs per cut, then unlinks the empty file (16_build_volumes.md, "Garbage
collection", for the measurements and the journal).

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
`<workspace>.dmg` with a warning rather than failing every project. Ordinary GC may reclaim only sidecarless RAW images,
under the exact path, lock, attachment and mount-ownership fences. Legacy `.asif` and `.sparseimage` files are reported
as warnings — one line naming every retained image, said by each project open and never by the enumerators that list
workspaces — and retained evidence only; removal requires an explicit transition or retirement, never filename-based
ordinary GC. A legacy image never becomes a live workspace just because its filename resembles one.

The orphan plan fingerprints the image's inode, device, length, and modification/change times; a same-sized replacement
invalidates the plan before any deletion. GC also checks the RAW image's kernel attachment identity, not merely the
expected mountpoint: an image attached elsewhere remains in place and is reported as deferred. Re-adoption's retained
namespace is never an ordinary orphan scan, and its exact forming-intent staging paths remain protected until the
replacement completes.

A sidecar that exists but is malformed or mismatched is not an orphan; its uncertain grants and identity still require
an integrity finding instead of a guessed admission or deletion.

The marker is the discriminator because it is the one identity that is both authoritative and re-stampable. An APFS
clone inherits its source volume's name **and its volume UUID**, so a volume UUID recorded at creation cannot tell a
workspace from the fork made out of it; that is why cloning rewrites the marker inside the staging fence, before the
clone is ever published. Volume UUIDs are therefore not recorded and not used. The label is not rewritten there: a clone
is published under the label it inherited, and the workspace's supervisor, once it serves, reads the file system's own
name for its image's mounted volume (`getattrlist`) and relabels it in the background when it differs. The label is
cosmetic, so nothing on the provisioning path waits on it. Replacement restores and identity changes still relabel
synchronously.

A volume is named only through its own mount root, held open and proven to be owned media (`OwnedVolumeRoot`), with
`fsetattrlist(ATTR_VOL_NAME)` on that descriptor. Nothing names a volume through a path or through Disk Arbitration:

- **A path names whatever volume holds it.** xnu's `setattrlist` sets the name of `vp->v_mount` for any path, mount root
  or not (`vfs_attrlist.c`, `setattrlist_internal`; measured: a plain subdirectory renamed the volume holding it). A
  mountpoint nothing is mounted at (a workspace whose create was refused, one that is detached, one whose supervisor
  outlived its mount) is a plain directory of the host's Data volume.
- **Disk Arbitration renames a device through the path it recorded.** `diskutil rename`/`renameVolume` and
  `DADiskRename` name the disk through the volume path Disk Arbitration recorded for it, resolved when its queue reaches
  the request, seconds to minutes later on a loaded host (9.5 s and 27.8 s measured; mounts queued over 300 s on
  2026-10-15). A volume unmounted in between by anything Disk Arbitration has not yet heard of — a forced unmount, a
  forced detach — leaves that path a bare directory, and Disk Arbitration, as root, names the volume beneath it.
  Measured on nested throwaway images (an outer image volume holding the inner's mountpoint): a kernel-forced unmount of
  the inner volume followed by `DADiskRename` of the inner device renamed the outer volume in 16 of 20 runs, Disk
  Arbitration reporting success. The host's Data volume carried `[cowshed] fixture · intents — slotted` from a rename by
  path, and then `[cowshed] fixture · dispatch — main` twice, the second time on 2026-10-13/14, after the rename by
  device had gained a re-proof under the image's lease, a raw pin and a held mountpoint descriptor. Such a proof cannot
  close this window: it precedes Disk Arbitration's execution rather than binding the eventual attribute write.

So the runner refuses `diskutil rename` and `renameVolume`, in any spelling, for every caller (cowshed's own work and
`cowshed disk-command` alike, `CommandRunFailure::VolumeRename`), before anything spawns. Nothing outside cowshed's
store labels renames a volume at all: the `bounded-exec` RAM volume is created under its final name, marked on a private
`mount_apfs` mount, and only then placed at `/Volumes/<name>` by Disk Arbitration (nx-plugin README). A store image's
volume (the supervisor's relabel, the build volume's after a land, the restore staging relabel, the identity change's)
is named through an `OwnedVolumeRoot`, which only `cowshed-apfs` can prove, by image, never by mountpoint. The native
host refuses an image outside the store root, and the rename acts only on the volume device the kernel's attachment
inventory maps that image to. An image no attachment holds names nothing (`NotAttached`); one held twice, or held
unformatted, is an inventory fault and is refused. One whose volume the kernel mount table does not list names nothing
(`NotMounted`). Both skips are typed outcomes, not errors, and the supervisor says each one in the daemon log. Under the
image's lease and a raw IOMedia pin that re-proves the mapping, the mountpoint is opened as an event-only directory
(`O_EVTONLY|O_DIRECTORY|O_NOFOLLOW`) and the filesystem that descriptor lies on read (`fstatfs`), retaining its actual
filesystem identity. Anything but the image's volume device, or a directory inside it rather than its root, is refused
(`RenameTargetNotMounted`). A host disk is no attachment of any image, so its volumes are never an `OwnedVolumeRoot`.
The same descriptor supports naming and native unmount; it does not prevent another unforced unmount.

The name is then set through the held descriptor. The kernel binds it to that descriptor's vnode, not to a path: a
volume unmounted since the proof, even by force, leaves the vnode dead and the call fails (`ENOTSUP`, measured; typed
`RenameFailed`) instead of reaching the volume beneath, and nothing is retried another way. The call is a kernel
attribute write, not a Disk Arbitration round trip, so the image's lease is held for microseconds; Disk Arbitration
learns the new name from the kernel and reports it.

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

Neither planning nor read-only validation spawns `diskutil`; validation uses the caller's budget for the kernel reads,
not a separate storage-command deadline. Historical evidence: `diskutil info` is answered through Disk Arbitration,
which congests when many images are attached. On a host with about 130 images attached, a consumer's test harness saw
one `diskutil info -plist` of the store volume block for 279 s before the harness interrupted it. The same call answered
in 0.43–0.64 s on that host once congestion passed. Gateways and cache services use the same read-only validation at
startup; its latency no longer depends on Disk Arbitration at all.

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
   `deleteVolume`. Setup also persists that verified passphrase in System.keychain under account and service
   `<volume-uuid>`, with the volume label and `Encrypted Volume Password` description, allowing `/usr/bin/security`,
   APFSUserAgent and CSUserAgent to read it. This is the identity macOS's independent volume-discovery lookup uses. Both
   keychain identities must read back the same bytes before encryption or a successful setup report; a conflicting UUID
   item fails closed rather than silently replacing a credential. Existing FileVault volumes receive the same repair.
3. Appends one idempotent, comment-tagged fstab line per volume:

<!-- prettier-ignore -->
```
UUID=<store-uuid>  /private/cowshed/store   apfs rw,noatime,noauto,nobrowse,noowners  # cowshed created volume labelled cowshed.store
```

4. Installs and loads `dev.cowshed.storage` (`/Library/LaunchDaemons/dev.cowshed.storage.plist`) running a fixed
   `/bin/sh` script at `/Library/Application Support/dev.cowshed/mount-volumes.sh`. The script does not invoke the
   cowshed binary. For each UUID it: no-ops if already at the canonical path; fails if mounted elsewhere; otherwise
   checks `security find-generic-password -a <label> -s <label> -w /Library/Keychains/System.keychain` before feeding
   the secret to `diskutil apfs unlockVolume <uuid> -nomount -stdinpassphrase`, then separately checks
   `diskutil mount -nobrowse -mountPoint <canonical> <uuid>` and verifies the resulting mountpoint. Missing keychain,
   symlink mountpoint, or nonempty stub fails closed. Every refusal names its step, installed script source line, and
   real exit status on stderr; launchd retains it at `/var/log/cowshed-storage.log`, outside the encrypted store. No
   failure diagnostic formats a credential.

UUID form is mandatory because labels are mutable (the same lesson nix-installer learned in
DeterminateSystems/nix-installer#212). `noauto` suppresses generic automatic mounting; it does not suppress
APFSUserAgent's IOKit volume discovery or its login-time DiskUnlock dialog. If a cowshed volume is still locked when the
desktop appears, APFSUserAgent looks for a generic-password item under the volume UUID, first by service and then by
account, not cowshed's label-keyed boot credential. Its default SecItem lookup can find System.keychain items; the
UUID-keyed identity above therefore supplies the password without a user-specific copy. The script lives outside every
user home and cowshed volume, so it unlocks and mounts the store before login without depending on the cowshed binary or
its version. `nobrowse` keeps the store out of Finder, the Desktop, and the sidebar; nothing lands under `/Volumes`.

The root boot script is cowshed's central **physical-volume** command layer, not an image executor. Its disk commands
are limited to `info -plist`, `apfs unlockVolume -nomount -stdinpassphrase`, and `mount` for the configured UUID. It
never attaches, detaches, creates, or resizes an image, and never executes a user-writable cowshed installation as root.
The repository disk-command guard pins those exact commands in this one script and rejects image verbs there. Every
image operation, including tests and benchmark scripts, uses `SystemCommandRunner` or
`cowshed disk-command <hdiutil|diskutil> -- <args>`; callers do not implement their own signal, deadline, or output
handling for disk tools.

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
script, user agents, installed binaries, and both the label-keyed and UUID-keyed cowshed System.keychain items; it never
deletes a volume.

**Mount ordering and the unmounted-masking guard.** The store's mountpoint is a plain directory on Data, and its root's
`.cowshed-volume.json` marker distinguishes the mounted cowshed volume from that bare directory. The system storage
LaunchDaemon writes stderr to `/var/log/cowshed-storage.log`; user launchd agents write pre-tracer stderr under
`~/Library/Logs/cowshed/`. Neither log lives under the mountpoint, so a reboot cannot recreate a masking stub. What does
land on a bare mountpoint before its volume is mounted — the sccache agent's socket and compile cache, the gateway
heal's directory-only `mnt/` scaffolding, Finder's `.DS_Store`, empty directories — is reclaimed before remount; any
other entry keeps the mountpoint masked.

Why a volume and not paths on Data: Data takes hourly APFS local snapshots, and a snapshot pins every since-rewritten
block of a multi-GB churning image — ghosts that path-level `tmutil addexclusion` does **not** prevent (exclusion stops
backup, not snapshotting). A dedicated volume gets no local snapshots, collapses backup policy to one per-volume
decision, and isolates the fsck domain and corruption blast radius (store loss = WIP since last autosave; Data untouched
either way). Rebuildable caches are not images: they stay at the tools' own defaults in HOME, where the host's own
builds already keep them (03_caches.md).

### One herd, multiple users

There is one cowshed per machine, anchored to no account. `/private/cowshed/store` is a plain directory on Data created
once at provision; it exists only as a mountpoint and fstab target. Workspace mounts use the fixed per-user root
`/private/var/folders/cowshed/<uid>/mnt` on macOS; see [Stable workspace mount root](#stable-workspace-mount-root),
including refusal of legacy custom configuration. No cowshed evidence, sandbox profile, or metadata path on macOS
derives from `$HOME`. Workspace images remain on the store volume while their live mounts appear under `<mount-root>/…`,
so per-image `clonefile` semantics, locks, and lifecycle are unchanged by sharing; concurrent multi-user mutation of one
workspace stays serialized by the same `<image>.lock` flocks, and cross-user `gc` policy is deliberately blunt: any user
may retire any shed, because `noowners` already made the trust model explicit.

## Restore publication custody

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

## Retired mountpoint debris

- A logged volume's final `fseventsd` flush can land on the bare host mountpoint after unmount and leave a root-owned
  `0700` `.fseventsd`. When it is the directory's **only** entry, gc does not try to delete it: it holds the parent and
  directory without following symlinks, checks fd-relative metadata and the kernel mount table, then atomically renames
  the user-owned mountpoint to a same-parent `.root-held-<uuid>` name without replacing an existing destination. A
  changed name, another entry (hidden included), or a newly mounted filesystem refuses the move and preserves the data.
  Preview and doctor never rename. They report `root-held-event-debris` as **info**, not an unhealthy invariant; gc
  reports the retained log through its named `deferred` information while reclaiming the retired image and other
  candidates. Subsequent sweeps leave the retained name in place. Privileged cleanup names only the exact, shell-quoted
  `.fseventsd` directory (`sudo rm -rf -- '<path>/.fseventsd'`), followed by ordinary `rmdir` of its retained parent: a
  late writer's added work can make `rmdir` refuse but cannot be recursively deleted by that guidance. **Enforced by**:
  a real kernel-owned `0700` log moved within an ownership-ignoring scratch image and remounted with ownership honored;
  read-only preview, same-parent/inode-preserving retention, successful image reclamation, idempotence, mixed
  hidden/visible work preservation and mounted-volume refusal, plus descriptor/rename collision and
  informational-diagnostic tests.

## Rejected APFS alternatives

**SPARSE and sparsebundle rejected; one format, no fallback.** Sparsebundle band files reintroduce thousands of host
inodes per workspace for no benefit (network-volume support is irrelevant here). SPARSE (`.sparseimage`) is the legacy
format: against ASIF it is slower to create, attach, and fill, several times slower on small-file operations and
sequential I/O, and no better under clones. Keeping it as a fallback would cost a second creation path, a second attach
and detach tool, a format field in every sidecar and marker, and an extension/metadata agreement check on every attach —
for hosts older than macOS 26, which cowshed does not support. ASIF is not the format because a `clonefile` of a live
ASIF image can capture mixed generations of its mutable outer allocation map, so a fork of a live source would need a
detach first; RAW has no outer map ("Images"). A case-insensitive volume was rejected because case sensitivity costs one
flag at creation and nothing afterwards.

**`/Volumes` mount root rejected.** DiskArbitration-managed mountpoints save a mkdir/rmdir pair but put cowshed paths in
a shared namespace where Finder surfaces them and name collisions get renamed (`widget 1`). The fixed canonical root
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
