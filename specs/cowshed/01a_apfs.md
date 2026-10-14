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
  `diskutil image attach --nobrowse --noMount --plist` followed by native DA mount of its exact, pinned volume at the
  requested root with `nobrowse,owners,noatime`. The clone takes the canonical name whole, so the name only ever holds a
  complete, formatted volume. A template is minted lazily, the first time a store mints at its capacity, in five
  unprivileged steps:
  `diskutil image create blank --format RAW --size <capacity-bytes> --volumeName [cowshed] --fs None <stem>-minting-<nonce>.dmg`,
  `diskutil image attach --nobrowse --noMount --plist <image>`,
  `newfs_apfs -U <uid> -G <gid> -e -v [cowshed] <whole-device>`, a `fsck_apfs -q` of the new volume, and owned-registry
  native DA release of that whole device; only then is it renamed to its name. Only minting takes the template's lock,
  and a minter that finds the template once it holds the lock returns it, so concurrent first mints make one template
  and a mint that finds it needs no lock at all. Each minter stages under a name of its own (`-minting-<nonce>`), so a
  path is attached at most once: a minter killed with its attach still queued on `storagekitd` can have that attach land
  after the next minter removed its file, and on the single reused `-minting` name it landed beside the next minter's
  attach, leaving one path attached twice. Before it starts, a minter releases every attachment the kernel holds for any
  of the template's staging names (old reused name included) — the inventory decides, since an attachment outlives its
  file and keeps its path — and removes their files. No attach runs for a path the kernel already holds: the image
  driver refuses a second attach of one file, but not of a new file at the same path. The attaching user owns the
  image's device nodes, so formatting needs no privilege, and `-U`/`-G` make the volume root the invoking user's from
  the start — which is why the owner is part of the template's name. `diskutil`'s own `--fs APFS` is not used: it cannot
  ask for case sensitivity, and it leaves a root-owned volume root that an `owners` mount cannot write and only root can
  hand over. Like every clone, a minted volume shares its template's APFS volume and container UUIDs (nothing records or
  reads them; see "Ownership, identity, and the volume label") and carries its label. A capacity gets a template of its
  own rather than a clone of another grown to fit: growing the container is a `diskutil apfs resizeContainer`, a second
  `storagekitd` call on every such mint (0.93–1.0 s measured on a loaded host), where a template costs one mint per
  store and capacity (0.57–0.63 s measured).
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
  clone passes Disk Arbitration's filesystem checks in its owned-volume native mount. A shell whose cwd is in main, an
  editor, or a source watcher does not prevent a fork. This is filesystem crash consistency, not an application-state
  guarantee: build volumes still use the sealed seeds and writer exclusion in 16_build_volumes.md.
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
- **Spotlight**: the root storage service merges the configured store and mount roots into the Data volume's Spotlight
  Search Privacy `Exclusions` at setup and boot, preserving other entries. These exclusions and the image marker express
  indexing policy, not media-reference ownership. In mds **2418.6.3.9.400 arm64e**, an admitted mounted
  `AppleAPFSVolume` reaches a constructor path that acquires an owned IOKit port and forgets its name without releasing
  the send right. Ordinary unmount/eject cannot recover that ownership; a process reap releases it, not an indexing
  exclusion or reload. The constructor admission, branch evidence, controls and host mitigation are specified in
  [Spotlight media references](#spotlight-media-references). Cowshed does not restart mds as part of an image operation.
- **Time Machine**: nothing to set. Backup policy is one per-volume decision, not path exclusions, and Time Machine
  already leaves the store volume out: measured on macOS 26.6, `tmutil isexcluded /private/cowshed/store` reports
  `[Excluded]` though cowshed applies no exclusion (it runs no `tmutil`). Durability is git (`cowshed push`), never
  backup.

## Mounts

APFS image attach uses `diskutil image attach --nobrowse --noMount --plist <image>` exactly once. Read-only attachments
add `--readOnly`; attachment stays `--nobrowse`, and native mount options select workspace visibility. Before mounting,
cowshed joins the reported whole device and one freshly probed APFS volume to the exact backing image in the current I/O
Registry and pins that volume. After the host storage lease is granted, it captures that owned media again and
constructs a DADisk with `DADiskCreateFromIOMedia`; absence, a conflicting device, multiple volumes or unreadable
ownership refuses.

Mount is `DADiskMountWithArguments` of that exact volume, with a file URL for the requested mountpoint and default DA
options. Workspace policy preserves `owners,noatime`, adds `nobrowse` when requested and `rdonly` for read-only access.
The existing root-owned event-store repair uses the same primitive with its distinct `nobrowse,noowners` policy, without
`noatime`, then restores the original workspace policy. The mountpoint exists before attach. Disk Arbitration owns
filesystem checking and mount bookkeeping; cowshed does not run a separate live fsck, `mount_apfs` fallback, retry or
StorageKit-family mount. Every mount refusal remains the actual native status and detail. The native session and media
stay live through the actual completion callback, with a warning at 120 s and every 30 s afterward rather than an
abandoned operation.

After success, cowshed holds the requested mount root and verifies its source is the pinned volume before publication or
any in-image marker write. A failed attach, malformed reply, native mount refusal or wrong root/device releases only the
new owned attachment or reports its cleanup refusal. An already-attached formatted or recovered volume uses the same
pinned native mount primitive. Blank formatting, detached checkpoint verification, resize and fixtures deliberately
exercising an unmounted state retain `--noMount` and explicit `fsck_apfs -q` where required. Namespace-only unmount
stays `diskutil unmount` through DA. Owned-image release uses current-IOMedia native DA requests: each actual owned
volume is unmounted, then the exact physical whole device is ejected. No kernel-unmount or image-driver fallback exists.
Changing DA ownership does not itself claim Spotlight releases departed media references.

### Owned-registry mount boundary

A stale or foreign StorageKit descendant cannot authorize a mount or veto mounting the owned volume. On 2026-10-14 at
**21:28:12.102 +04:00**, `storagekitd[90506]` rejected the lease-overlap fixture's combined attach/mount with
`Error Domain=NSPOSIXErrorDomain Code=22 (Multiple disks to mount, cannot specify mount point)`. The fresh image owned
physical **disk177** and APFS volume **disk179s1**, whose OS fsck returned **0**. Failure cleanup nevertheless requested
unmount of stale **disk177s1** twice. The prior native release had succeeded and its exact registered tree departed;
release alone therefore did not repair this second cached-family selection. One no-mount attach followed by native mount
of the exact freshly probed and pinned volume is the owner-layer cutover, not a cache reset, delayed retry or parser
waiver. Acceptance retains the original real lease-overlap lifecycle and its complete cleanup expectation.

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

On macOS the default session mount root is `/private/var/folders/cowshed/<uid>/mnt`, where `<uid>` is the invoking
user's numeric UID. `/var/folders` is its familiar spelling; the stored path is canonical. The path deliberately reaches
the pre-constructor exclusion above, before mds can acquire the port. It is a real mount directory, not an index-policy
flag, a fake OS-update path or a renamed volume. A main still mounts at its original checkout path; this default does
not relocate existing checkouts or release references mds already holds. Explicit roots outside the exclusion retain the
measured risk.

`cowshed setup` provisions the root in its existing privileged transaction: `/private/var/folders/cowshed` is root:wheel
mode **0755**, and its `<uid>` and `mnt` directories are owned by the invoker with mode **0700**. Setup creates missing
directories idempotently, attests ownership and type, reports the selected path, and refuses a symlink, non-directory or
foreign owner rather than writing through it. No separate operator mkdir, daemon bounce or automatic move of an attached
workspace is required. Ordinary session and build mountpoints are created beneath `mnt` on demand.

The fixed `mnt/` component is part of the policy. `dirhelper`'s scheduled cleanup addresses the `T` and `TemporaryItems`
children of `/var/folders/<first>/<second>`; boot cleanup also addresses `Cleanup At Startup` and `X`, and `C` in safe
boot. It does not recurse through an arbitrary `mnt` child. Placing repository owners directly under `<uid>` could
collide with those names on case-insensitive Data. An OS-generated per-user `T` path is not a stable path contract
across boots; cowshed therefore provisions its own stable subtree, outside those cleanup slots. Empty mountpoints can be
recreated without moving the backing images. This is not a new Time Machine policy: backup inclusion remains a
per-volume decision.

There is **one configured session root per host**, selected at setup. Existing explicit and legacy root configuration
continues to mean exactly the same path; installing a new binary neither rewrites it nor moves live sessions. The new
default applies when selecting an unconfigured root. An operator who wants an existing host to switch first leaves no
sessions attached, then runs `cowshed setup --mount-root /private/var/folders/cowshed/$(id -u)/mnt`. Setup retains its
attached-session refusal because absolute paths are baked into detached metadata and sandbox profiles. The operator
chooses that maintenance boundary; cowshed does not bypass it. Linux retains its own default.

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
registered provider tree. A native DA session creates every disk with `DADiskCreateFromIOMedia`, unmounts those volumes
individually, and calls `DADiskEject` on that whole media. It never asks DA or StorageKit to enumerate a BSD-prefix or
whole-disk cached family, never uses `kDADiskUnmountOptionWhole`, and has no `diskutil eject` or automatic `hdiutil`
fallback. An already-unmounted owned volume needs no unmount effect.

`WhenIdle` returns a native DA busy refusal immediately, without force. `Release` gives the first dissent its existing
10 s monotonic wall-clock grace, including native execution and disk-lease waits, then makes one release attempt with
forced unmount of only the owned volumes before ordinary eject. Apple's DA API exposes no force-eject flag.
`WithinGrace` waits through the same grace and returns the dissent without forcing. Native `kDAReturnBusy` and encoded
BSD `EBUSY` are typed dissents; unknown/general status, permission, session or I/O failures remain authoritative. The
existing command-unmount named-dissenter parser is not broadened. Native status, detail and slow accepted release remain
attributes of the release span, never fabricated subprocess output.

The native session remains alive until its actual completion callback. The existing disk-image policy warns at 120 s and
every 30 s thereafter rather than abandoning an in-flight release; neither that policy nor the grace is widened. The
kernel's I/O Registry is the one host view that maps an image's path to its devices (`diskutil image info` reports
none): one `IOServiceGetMatchingServices("IOMedia")` snapshot, each node walked up to its `AppleDiskImageDevice` and
that device's `DiskImageURL`. `hdiutil info -plist` is not an inventory: while any other image attaches or detaches it
answers a truncated image list with nothing marking it (a reviewer probe that kept one image attached while churning
another saw it missing from 11–89 of every 648–1551 polls, and from none of the same polls' registry reads). Reading
that omission as absence once skipped a post-format release, leaving the image attached for `diskutil image resize` to
refuse as busy, and once lost a mounted workspace's attachment on restart.

The image driver registers an attach's media before `diskutil image attach` reports them, so creation requires the
reported blank whole device to be the image's exact single-device mapping in one registry read before formatting.
Absence there is a contradiction, not lag: it fails at once, as does an observed conflicting mapping, and an unreadable
inventory is a typed refusal; each leaves both the attachment and backing file intact for diagnosis. No mutation is
retried and no unproved device is formatted. Shared owned-image cleanup distinguishes this unformatted whole-device
state from an APFS volume: it rechecks the exact mapping under the image's lease and releases only that owned device
without deleting the backing file first. Every detach attempt, including a dissent retry and the final force, rechecks
that mapping after the host storage lease is granted, immediately before capturing the current owned media references
for native DA. Those references stay owned through the callback; a check before the lease wait is not authority
afterward, and a recycled `diskN` cannot substitute another image. Confirmed image absence needs no operation; a
positive conflicting mapping or unreadable inventory refuses the mutation. A release deduplicates whole-device views, so
the same attachment is never detached twice.

### Owned-registry release boundary

The release owner selects the exact backing image, its registered whole device and that device's current provider-tree
descendants. Only those identities authorize release: a foreign or stale StorageKit cached descendant must neither be
selected for unmount/eject nor veto release of the owned tree. A reused BSD spelling is not ownership. The owner-layer
adapter must preserve this boundary through the actual DA request, not merely validate the command's top-level argv. An
unrelated refusal remains an error; the boundary is not permission to broaden the dissent parser, force a foreign
volume, add an automatic command fallback, or weaken a fixture's complete cleanup expectation.

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
remain evidence. The native, current-IOMedia-bound release mechanism above is the permanent owner-layer cutover; the
operator action is not an automatic runtime fallback. Its acceptance must exercise real owned cleanup and preserve the
foreign/stale-descendant exclusion and typed refusal controls, not borrow the isolated pass as proof.

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
already-absent file, needs no native release. A failed explicit cleanup is never retried from its destructor.

A namespace-only unmount, needed for adopt handoff or a sealed capture without releasing the device, is
`diskutil unmount <verified-volume-device>`. It holds the image lease and raw IOMedia pin while rechecking the exact
image/whole-device/volume identity, never a path selected by an untrusted child. Restart-owned mounts additionally
require their incarnation marker and matching kernel source. `WhenIdle` returns the observed DA dissent without force;
`Release` waits the existing grace before `diskutil unmount force <verified-volume-device>`. Other errors remain errors.
Ordinary image release instead unmounts only the current owned volumes through native DA before ejecting the whole
media; it does not add a namespace-only command.

Attach, verification and every lookup that answers one attachment require exactly one APFS volume in one reported
container, and refuse anything else by naming what was reported. A release reads each of the image's attachments as its
whole device plus every APFS volume in that container, so it can also take an owned image whose container gained a
volume (for example, one a dead test added). The image lease and current provider-tree identity fence native unmount of
each owned volume followed by one native DA eject of the physical whole media. A formatted attachment that reports no
volume, reports a volume without its container, or spans two containers is still refused.

Concurrent mount-table reads use `getfsstat(MNT_NOWAIT)` into a buffer each call owns: count first, allocate checked
space with room for new mounts, and grow and reread if the returned buffer is full. No reader borrows `getmntinfo`'s
process-static array, which another thread can overwrite during iteration. Every snapshot retains complete mountpoint
and source-device records; an empty mountpoint is a real inventory fault, not an entry to silently discard.

Disk device names are reusable, not image identities. Each image has its own private per-user lease,
`/private/tmp/cowshed-apfs-image-leases-<euid>/<sha256 of the identity>.lock`: an `flock` on a regular owner-only 0600
single-link file, opened without following symlinks, in a 0700 directory this user owns. The identity is the absolute
path produced by `attachment_inventory_path`, matched against the kernel's `DiskImageURL` backing-file path; the lease
hashes that same normalized identity, never a separately resolved alias. An alias with another identity fails closed.
Every cooperating create, attach, format, `fsck_apfs`, mount, unmount, rename, recovery and detach of one image holds
that image's lease, so independent processes serialize on one image while operations on different images overlap. There
is no host-wide device lock. A cooperating process acts on a device only after the inventory positively maps the
identity to it under the lease (or while a raw pin holds it), and a device is freed only by a detach the same lease
covers, so no same-user cowshed process recycles it inside the critical section. A contended lease is awaited on a
helper thread that hands the locked descriptor back when the holder releases it, bounded by the 120 s disk-child
deadline; expiry names the lease file, the image and the holder's recorded pid. The lease does **not** constrain another
user's disk tools or a manual eject during the two mutations that cannot run raw-pinned, `newfs_apfs` and owned-media
native DA release, because an open raw pin prevents releasing the device. Before detached `fsck_apfs` or a
recovered-volume DA mount, cowshed opens the reported raw volume read-only, then verifies that the attachment inventory
maps **both** its whole container and its volume to the exact image. A verified attachment owns that raw descriptor
across the `attach_verified` → `mount` method boundary. A mount lookup of an existing attachment likewise takes the
image lease before reading inventory and returns a handle owning its raw pin through the kernel mount-table read and
mount decision. It is released when an already-mounted handle is dropped, after the DA mount finishes, before
intentional detach, or before the exclusive `diskutil apfs resizeContainer` writer, which cannot run with a raw
descriptor open. Independent device ejects during container resize after pin release are not covered. While held, the
descriptor prevents even external `diskutil eject force` or `hdiutil detach -force` from releasing the image and
recycling its device name. If the image lost the reported device before the descriptor could be pinned, cowshed performs
at most one fresh attachment, and only when inventory shows no remaining attachment for that image. A conflicting or
unreadable mapping fails closed, without running fsck on the reported device. No live-fsck option authorizes touching a
foreign mounted container. Blank-image formatting keeps its image-to-whole-device check under the image's lease; its
exclusive formatter cannot share a raw-device descriptor with another opener.

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

| Setting                       | Workspace / live-build images                                                                                                                                 | Detached seed / blank template                                                                        | RAM temp volume                                                                          | Dedicated store / retained caches                                                                     | Reason and setter                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Read-write / `rdonly`         | Read-write by default; recovery-marker mounts request `rdonly`.                                                                                               | Neither is mounted.                                                                                   | Read-write by default.                                                                   | `rw` in fstab.                                                                                        | Builds must write and execute their outputs; recovery reads a marker without modifying the image. Image backend `mount`; `storage/apfs/native.rs::detached_image_incarnation`; `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `nobrowse`                    | Default; workspace `--browse` omits it. Live build volumes stay hidden.                                                                                       | Neither is mounted; template attachment uses `--nobrowse --noMount`.                                  | Requested at `diskutil mount`.                                                           | Requested at bootstrap/boot mount and in fstab.                                                       | Keep temporary/work volumes out of Finder, Desktop and sidebar. Image backend `mount`/attachment; build host `create_build_volume`/`mount_build_volume`; RAM `RamTempVolume.provision`; `storage/bootstrap/native/macos.rs::desired_mount_service` and `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                |
| `noatime`                     | Requested on every normal read-write/read-only image mount, including `--browse`.                                                                             | Neither is mounted.                                                                                   | Not requested.                                                                           | Requested in fstab.                                                                                   | Suppress access-time updates in read-heavy builds; workflow decisions do not depend on those updates. Image backend `mount`; `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `owners`                      | Requested on normal mounts, including read-only mounts.                                                                                                       | No mount flags; filesystem ownership is inherited/formatted.                                          | Not requested; unprivileged Disk Arbitration mounting yields `noowners`.                 | Not requested; fstab specifies `noowners`.                                                            | Workspace permissions must be honored. RAM mounting with ownership would require authorization; launchd plists therefore live outside the RAM-backed `TMPDIR`. Image backend `mount`; RAM module's mount/authorization contract.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `noowners`                    | Only the temporary root-owned event-store repair mount; normal requested options are restored afterward.                                                      | Not mounted.                                                                                          | Kernel/Disk Arbitration property of the unprivileged mount, not an explicit RAM option.  | Explicit in fstab.                                                                                    | Permit marker repair in an inherited root-owned `.fseventsd` without root authorization; it is not the workspace's final permission policy. Image backend `retire_inherited_event_log`; RAM `provision`; `storage/fstab.rs::build_fstab`.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `nodev`                       | Kernel flag on unprivileged image mounts; not in cowshed's option string.                                                                                     | Not mounted.                                                                                          | Kernel flag on the unprivileged mount; not requested explicitly.                         | Not requested by privileged bootstrap.                                                                | Device-special files must not become usable device nodes through an unprivileged mounted image. Applies at the image backend/RAM mount calls; cowshed has no separate `nodev` setter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `nosuid`                      | Kernel flag on unprivileged image mounts; not in cowshed's option string.                                                                                     | Not mounted.                                                                                          | Kernel flag on the unprivileged mount; not requested explicitly.                         | Not requested by privileged bootstrap.                                                                | Mounted code cannot gain privilege through set-user-ID/set-group-ID bits. Applies at the image backend/RAM mount calls; cowshed has no separate `nosuid` setter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `journaled` / `local`         | APFS/kernel filesystem properties, not requested mount options.                                                                                               | APFS is formatted, but no mount flags exist while detached.                                           | APFS/kernel properties.                                                                  | APFS/kernel properties.                                                                               | Keep the filesystem's normal consistency/durability behavior; do not confuse the kernel's `journaled` label with a separate HFS+ formatting switch. Image backend `attach_and_format_unlocked` calls `newfs_apfs`; RAM `provision` and host provisioning call `diskutil apfs addVolume ... APFS`.                                                                                                                                                                                                                                                                                                                                                                                                      |
| `.fseventsd/no_log`           | Empty marker attempted after every successful read-write mount; read-only mounts do not write it.                                                             | Seed inherits the source's marker; blank template has none. A live clone is opted out when it mounts. | No explicit marker; a fresh `nobrowse` volume starts with no historical FSEvents record. | No explicit marker setter in host bootstrap.                                                          | Disable persistent FSEvents history, not live watcher notifications. The image backend `opt_out_of_event_log` owns the policy. A marker planted after mounting governs a later mount; daemon mount processing is asynchronous. See [The event log](#the-event-log).                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Event-log permissions         | New directory `0700`; new empty marker requested as `0644` (subject to umask). Repaired root-owned directory becomes `0711`; historical records are retained. | Seed inherits the source's directory/marker; template creates neither.                                | No event-log permission setter.                                                          | No event-log permission setter in bootstrap.                                                          | Directory traversal lets later mounts find the marker without exposing a directory listing; root-owned `0600` chunks keep their permissions. `apfs.rs::opt_out_of_event_log` (`mkdirat`), `opt_out_in` (`openat`) and `retire_event_log` (`fchmod`), all fd-relative and no-follow.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Root-owned event-store repair | One-time `nobrowse,noowners` staging mount (does not request `noatime`), then the caller's original `owners`/`noatime`/visibility options.                    | Neither is mounted; seed preserves its source's on-disk state.                                        | No repair path.                                                                          | This repairs an image's `.fseventsd`, **not** the dedicated store volume.                             | Add the marker without purging historical records; make root's directory traversable (`0711`, no listing) so later ownership-honoring mounts can find it without repeating repair. Image backend `retire_inherited_event_log`/`retire_event_log`; the final policy honors ownership again.                                                                                                                                                                                                                                                                                                                                                                                                             |
| Spotlight indexing            | Empty `.metadata_never_index` planted at the volume root after every successful read-write mount, beside `no_log`; read-only mounts do not write it.          | Seed inherits the source's marker; blank template has none. A live clone is opted out when it mounts. | No explicit indexing command/marker; mounted `nobrowse`.                                 | No explicit indexing command/marker; mounted `nobrowse`.                                              | Keep Spotlight off cowshed's volumes, browsable ones included. On a browsable mount the marker makes `mdutil -s` report "Indexing and searching disabled" where an unmarked volume reports "Indexing disabled"; a `nobrowse` mount reads "unknown indexing state" in either case; unprivileged `mdutil -i off` is refused ("Try as root"). The marker stops indexing, not the departed-volume leak ("How the APFS host degrades" §2). It is a file at the checkout root, so `.git/info/exclude` carries `/.metadata_never_index`, and copies and cache retirement skip it like the volume's own bookkeeping. Image backend `opt_out_of_spotlight`, planted symlink-safe and exclusively like `no_log`. |
| Time Machine exclusion        | No per-image/path exclusion setter.                                                                                                                           | No exclusion setter.                                                                                  | No exclusion setter; volume is temporary.                                                | No `tmutil` setter; OS policy excludes the dedicated store.                                           | Images live on a separate volume rather than the Data volume, avoiding Data-volume snapshot retention of build churn. Durability is git, not a cowshed-applied backup rule; user backup policy remains separate. Host provisioning creates/mounts the dedicated APFS volume; no code calls `tmutil`.                                                                                                                                                                                                                                                                                                                                                                                                   |
| Case sensitivity              | One case-sensitive APFS volume, inherited from the template.                                                                                                  | Seed inherits the filesystem; template uses `newfs_apfs -e`.                                          | Plain `APFS` in `addVolume`; no case-sensitive variant requested.                        | New volumes use plain `APFS`; no case-sensitive variant requested.                                    | Preserve distinct case-sensitive source/output names in image-backed trees. Image backend `attach_and_format_unlocked`; RAM `provision`; `storage/bootstrap/native/macos.rs::provision_apfs_volumes_in_session`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Root UID/GID                  | Invoking user's UID/GID at formatting, inherited by clones.                                                                                                   | Seed inherits ownership; template uses `newfs_apfs -U <uid> -G <gid>`.                                | No explicit formatter UID/GID; mount ignores ownership.                                  | Provisioning chowns the root to the invoking UID/GID; fstab ignores ownership.                        | Make image roots writable by their owner without an elevated format. `blank_template.rs::owner`/`mint` supplies UID/GID to the image backend; `provision_apfs_volumes_in_session` issues/attests the host-root chown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| FileVault/encryption          | No extra image-volume encryption switch; durable image bytes reside on the encrypted store.                                                                   | Backing-store protection only; no per-image `newfs_apfs -E`.                                          | No encryption setter.                                                                    | FileVault enabled; passphrase persisted in System.keychain before encryption, reused for boot unlock. | Protect durable bytes without a second per-image crypto/password layer. `storage/bootstrap/native/macos.rs::encrypt_volume_with` (`diskutil apfs encryptVolume -user disk -stdinpassphrase`) and `desired_mount_service` (`unlockVolume ... -nomount`).                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `noauto`                      | No image fstab entry.                                                                                                                                         | No fstab entry.                                                                                       | No fstab entry.                                                                          | Explicit in fstab.                                                                                    | Skip generic `mount -a`; cowshed's boot service explicitly unlocks/mounts the known UUID at the canonical path. `storage/fstab.rs::build_fstab`; `desired_mount_service`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Execution / `noexec`          | `noexec` is not requested.                                                                                                                                    | Not mounted.                                                                                          | `noexec` is not requested.                                                               | `noexec` is not requested.                                                                            | Builds, test binaries and scripts must run from their filesystem; `nosuid` is not `noexec`. The image/RAM/fstab option lists above contain no execution ban.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

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

So every read-write mount opts its volume out after Disk Arbitration returns and before the mount is handed back:

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
   its behavior, an idle workspace is detached rather than kept, and `doctor` names orphaned helpers (a `diskimagesiod`
   whose pid created no attached device's user client) with the root command that clears them.

   The same error has a second cause, and only a reboot clears it: **leaked image devices.** Every attach creates an
   `AppleDiskImageDevice`, its `DIDeviceIOUserClient` and a `DIDeviceRequestPool`. Most leave the kernel with their
   detach; some stay alive after they have left the registry, counted among their class's instances but registered
   nowhere. `ioclasscount` reads the instances unprivileged (the registry root's `IOKitDiagnostics` class counts), the
   registry its live entries, and the difference is the leak. In one observed failure the first 150 struck with 1090
   device instances against 66 attached, exactly 1024 leaked, and no orphaned helper. Root `lsmp -a` showed no userspace
   reference to a leaked device (only the 66 live helpers held `DIDevice` rights, one each); restarting `mds` and
   `diskarbitrationd` freed none of them, and the reboot freed all (178 instances, 178 registered, after it). The likely
   cause is a device stuck in termination with an unfinished request pool because its attach or helper was killed
   mid-operation; the check that confirms it is the leaked-device delta across one real-APFS lane run, which must be 0
   now that cowshed kills no disk-image command at all (below). 1024 is an observation of one host and boot, not a
   documented limit. Departed APFS-volume and IOMedia references are separate from this image-device pool. Their owner
   must be measured by joining exact registry IDs to root `lsmp`, not inferred from the class name. An earlier host
   attributed 1064 APFS-volume and 1067 IOMedia rights to DA; the later 7,221-object cohort belonged to mds, with none
   held by DA. The current mds discovery and missing-release mechanism, controls and mount-root policy are specified in
   [Spotlight media references](#spotlight-media-references).

   The pool is bookkept per host user in `/private/var/tmp/cowshed-attach-pool-<euid>/pool.json`, a 0700 directory that
   survives the reboot, written under a lock beside it; the pool itself is host-wide, and each user's record holds what
   that user's commands met. The record is keyed by `kern.bootsessionuuid`, since `kern.boottime` moves within a boot
   whenever the clock is stepped. Every 150 a disk-image command meets is `ApfsError::AttachPoolExhausted`, a host
   finding whose next step is `cowshed doctor`, never "repair storage", and the system runner records the boot's first:
   the boot is spent, with the image devices' counts and the orphaned helpers at that moment when they can be read,
   until a later attach in the same boot succeeds (killing orphans frees the pool without a reboot) or
   `cowshed doctor --clear-attach-budget` ends it. That first 150's leaked count becomes the ceiling only when no
   orphaned helper could have caused it; until one does, the ceiling is the observed 1024, and headroom is the ceiling
   minus the devices leaked now. A ceiling only moves down and survives reboots, so
   `cowshed doctor --clear-attach-budget` also forgets it, back to the observed 1024, and keeps the last exhaustion as
   evidence. A record that does not parse proves nothing: it reads as empty with a warning naming the file, the kernel
   is still reported, and the next write (a 150, a clear) replaces it. A refusal of the lane gate always names what
   frees enough: the orphaned helpers for a spent boot the leaked devices do not explain,
   `cowshed doctor --clear-attach-budget` when the recorded ceiling alone is below the need, and otherwise a reboot,
   never a guessed daemon restart for unrelated departed-volume references. `doctor` reports `disk-image-attach-budget`:
   the four classes' instances against their registered entries, the ceiling and its source, the headroom, and the
   disk-image commands an earlier build killed at its former 15-minute hard deadline this boot, each a probable leaked
   device (the record keeps them until the boot ends; no current build writes one). It is an error while the boot is
   spent ("attach pool exhausted", with "reboot required" only when the leaked devices account for it), a warning when
   the headroom falls to a quarter of the ceiling or a command was killed. Its hint names a reboot for leaked image
   devices and proven orphaned helpers first for a 150 those devices do not explain. `departed-volume-references` is a
   separate warning from the first departed APFS-volume or IOMedia instance, with exact counts and a root `lsmp`
   attribution step; it neither spends attach-pool headroom nor prescribes a daemon restart. Orphaned helpers are named
   only when two reads in a row agree and no `diskutil` or `hdiutil` ran during either, since a helper whose attach is
   under way has no device yet and killing it mid-attach is what leaks one; killing orphans frees their mappings, never
   a leaked device.

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

   **No disk-image command is killed.** For a disk-image command the 120 s disk-child deadline is a warning point, not a
   kill: it is waited for, synchronously and still holding its leases, and names its verb, its target and the time
   elapsed on stderr every 30 s, until it ends on its own. A late child that succeeds is a success; one that fails is
   the caller's failure as always, and an attach's own failure path detaches whatever device it produced. Every other
   disk child (`diskutil info`, `diskutil apfs …`, `mount_apfs`, …) is still killed at 120 s. The trade-off is explicit:
   a hung DiskImages2 verb wedges its own operation, and the image lease it holds, for as long as it hangs instead of
   leaking a device until reboot (the gateway evicts it from its disk phase after 10 s, so the host's other disk work
   goes on; 05_gateway.md, "Disk-lifecycle lease"). Its caller stays cancellable: a caller that stops waiting leaves the
   detached command to finish. Callers outside cowshed get the same runner through `cowshed disk-command` (06_cli.md).
   The orphaned-helper `kill -9` does not contradict this: an orphan serves no device, so killing it strands nothing.

3. **Detach waits on the kernel and holders.** A non-forced unmount of a volume something holds is refused with `EBUSY`
   and retried; a land's target unmount took 12.2 s on its first try and then about 45 retries, and the image detach
   that followed 45.3 s. A held file anywhere in a volume keeps its image attached, so a stray process with a cwd inside
   a workspace blocks its retirement.
4. **Shared extents fragment the source, not the clone.** Writing an image while a clone shares its blocks moves every
   rewritten block to a new run, and every later clone pays the extent map on its first write ("Images" above).

What the substrate does about each:

- **Fewest storagekitd round-trips.** Production operations sit at their floor: a mint is one `clonefile` of the store's
  blank template plus one attach ("Images"), a fork or cold mount is one attach, a land with no retire makes no
  disk-tool call. Release now makes native requests only for the owned media, with no StorageKit command round-trip; the
  earlier measurements below used one `diskutil eject` per release and do not claim native release latency. The mint
  floor was one `image create` plus a formatting attach and `newfs_apfs`. Measured on the same host and tests (CLI adopt
  and build-volume first touch, `apfs <leg>/mint` spans), adopt's mint went from 537–705 ms to 303–345 ms and first
  touch from 565–756 ms to 312–317 ms on a quiet queue; under a loaded parallel suite the old adopt mint took 10.6 s
  (create 2.4 s, attach 7.7 s) against 5.0 s for the new one, all of it the one attach. Values the image itself records
  come from the image, never from a `diskutil` query: a RAW image's virtual capacity is its file length, read without
  spawning a storage tool, and an attached image's kernel capacity comes from its authoritative IOMedia mapping.
- **No attach→detach→attach on a success path**, and no verify-by-reattach.
- **Storage calls and mount-table changes never overlap.** Every disk tool on the host runs under the gateway's
  disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease"): `diskutil`, `hdiutil` and `newfs_apfs` share one phase,
  external/manual `mount_apfs` and `umount` the other, and the two alternate. Cowshed's own mounts and unmounts now stay
  in the DA storage phase. The same eight attach loops beside two mount loops made 106 attaches in 43 s with the phases
  kept apart, at 0.94 s p50 and 1.78 s p95 under load 167–212.
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
name for its image's mounted volume (`getattrlist`, no Disk Arbitration round trip) and relabels it in the background
when it differs. A rename is a Disk Arbitration round trip, and Disk Arbitration serializes every client on the host —
measured at 9.5 s and 27.8 s under load against 13–22 ms on a quiet queue — so it stays off the provisioning path.
Replacement restores and identity changes still relabel synchronously.

A rename targets a volume device, never a path. `diskutil renameVolume` handed a path renames whatever volume holds that
path, and a mountpoint nothing is mounted at (a workspace whose create was refused, one that is detached, one whose
supervisor outlived its mount) is a plain directory of the host's own Data volume: a supervisor that relabelled by its
checkout's path once renamed the host's Data volume that way. So every rename (the supervisor's, the build volume's
after a land, the restore staging relabel, the identity change's) names an image, never a mountpoint. The native host
refuses an image outside the store root; the backend beneath it renames only through a verified attachment, so its
target is always some image's volume device, never a host disk. The rename acts only on the volume device that the
kernel's attachment inventory maps that image to. An image no attachment holds names nothing (`NotAttached`); one held
twice, or held unformatted, is an inventory fault and is refused. One whose volume the kernel mount table does not list
names nothing (`NotMounted`). Both skips are typed outcomes, not errors, and the supervisor says each one in the daemon
log. The rename runs under the image's lease and a raw IOMedia pin that re-proves the mapping, as a release proves it
before a namespace-only DA unmount ("Mounts"), and the pin is held across the `diskutil` call. The mount the label found
is re-proven under them too, immediately before the rename. A device rename is not enough on its own: Disk Arbitration
names a mounted volume through the mount path it recorded for the disk, not the device node (Apple's DiskArbitration
source; not measured here), and a volume unmounted between the mount table read and the rename leaves that path a bare
directory of the host's Data volume — which once carried a fixture's `[cowshed] fixture · dispatch — main` label. So the
rename opens the mountpoint as a directory and reads the filesystem that descriptor lies on (`fstatfs`): anything but
that image's volume device, or a directory inside it rather than its root, refuses the rename (`RenameTargetNotMounted`)
before `diskutil` runs. The descriptor is held across the call, so an unforced unmount of that volume fails busy until
the rename returns, and every cooperating unmount of the image already waits on its lease; only a forced unmount by
another process can still land inside the call. No internal disk is an attachment of an image under the store, so the
device a rename names is never the host's.

Open item: the rename holds the image's lease for the whole `diskutil renameVolume` call, and Disk Arbitration's queue
sits inside that call (9.5–27.8 s under load, bounded by the 120 s disk-child deadline). Meanwhile a mount, unmount or
detach of the same image waits on the lease. That only delays lifecycle work on one image; it is not a safety problem,
and no lease wait under a queued rename has been measured. Releasing the lease cannot be split from the wait, because
the wait is the command itself. Holding only the raw pin across it would turn that wait into the concurrent detach's
EBUSY refusal, so the trade-off is left as it stands until it is measured.

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
once at provision; it exists only as a mountpoint and fstab target. Workspace mounts use the host-configured root,
defaulting to `/private/var/folders/cowshed/<uid>/mnt` on macOS; see
[Stable workspace mount root](#stable-workspace-mount-root). Existing configured roots remain unchanged. No cowshed
evidence, sandbox profile, or metadata path derives from `$HOME`. Workspace images remain on the store volume while
their live mounts appear under `<mount-root>/…`, so per-image `clonefile` semantics, locks, and lifecycle are unchanged
by sharing; concurrent multi-user mutation of one workspace stays serialized by the same `<image>.lock` flocks, and
cross-user `gc` policy is deliberately blunt: any user may retire any shed, because `noowners` already made the trust
model explicit.

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
