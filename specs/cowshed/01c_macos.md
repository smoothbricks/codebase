# macOS Host Constraints

cowshed meets platform behavior it does not control at the macOS host: the disk-image drivers and Disk Arbitration,
Spotlight, `fseventsd` and Time Machine, FileVault and the keychains, launchd, XProtect and Gatekeeper, Seatbelt, and
the kernel's process, socket and file interfaces. This companion indexes those constraints, so that an agent that meets
one finds its owner instead of rediscovering it. It owns no mechanism: [01a_apfs.md](./01a_apfs.md) owns the APFS image
substrate, [01b_zfs.md](./01b_zfs.md) its ZFS alternative, and every other owner is a section named in the entry.

## Reading an entry

- **Fact** — what macOS does and where that is established: an SDK header or manual page (`<sys/un.h>`, `clonefile(2)`),
  an Apple guide, a source file of the open Darwin stack (Libc, XNU), a measurement an owner section records
  (_measured_), a statement the owner makes with no source beyond itself (_owner text_), or a comment in the code.
- **Rule** — what cowshed does about it, in one sentence.
- **Owners** — the spec sections or user docs that specify the mechanism, then the code that implements it; code paths
  are relative to `packages/cowshed/crates/` unless they start with `packages/`. _No spec owner_ means no spec section
  states the rule: the code, its comments and any user doc the entry links are its only statements. _Unlanded_ means the
  owner text exists only in a change that has not landed on main: the entry names the change by its commit subject and
  links only owner text that is on main, and the land that brings the text replaces the marker with links. _No code
  owner_ means a spec section states the rule and no code implements it.

An entry exists because a macOS behavior forced a rule in cowshed. A new entry names the fact and the rule in one
sentence each and links the owner, which keeps the mechanism, its constants and its evidence.

## Index

Find the symptom in the left column and follow the link.

| You see                                                                                                                                                                                                  | Entry                                                                                                                                           |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `path must be shorter than SUN_LEN` or `InvalidInput` binding or connecting a Unix socket                                                                                                                | [`sun_path` is 104 bytes](#sun_path-is-104-bytes)                                                                                               |
| A rule spelled `/tmp/…` does not match; `Operation not permitted` on a path a profile seems to admit                                                                                                     | [`/tmp` and `/var` are symlinks into `/private`](#tmp-and-var-are-symlinks-into-private)                                                        |
| A temporary or mount directory vanishes; `cowshed setup` refuses the per-user root as foreign                                                                                                            | [`/private/var/folders` is root-owned and swept](#privatevarfolders-is-root-owned-and-swept)                                                    |
| `getconf: confstr: DARWIN_USER_TEMP_DIR: Input/output error` or `xcrun: warning: confstr() failed with code 5` in a job; where a profile admits `dirhelper`, `mktemp` fails `EPERM` under `/var/folders` | [The per-user temp directory comes from `dirhelper`](#the-per-user-temp-directory-comes-from-dirhelper)                                         |
| `xcode-select: error: unable to read data link at '/var/select/developer_dir'` from `git`; `xcrun: error: couldn't create cache file` under a closed profile                                             | [The `/usr/bin` developer-tool shims](#the-usrbin-developer-tool-shims)                                                                         |
| `… is a .sparseimage file, not a RAW .dmg image`; `du` and `ls -l` disagree on an image's size                                                                                                           | [A RAW sparse file is a disk](#a-raw-sparse-file-is-a-disk)                                                                                     |
| `du` of an image stays large after the guest deleted its files                                                                                                                                           | [`hdiutil compact` does not reclaim a RAW image](#hdiutil-compact-does-not-reclaim-a-raw-image)                                                 |
| Two paths that differ only in case collide                                                                                                                                                               | [Case sensitivity is fixed at format time](#case-sensitivity-is-fixed-at-format-time)                                                           |
| A fork's mount takes tens of seconds; `doctor` reports `main-extents` or warns about extents                                                                                                             | [A clone's first write pays for its extent map](#a-clones-first-write-pays-for-its-extent-map)                                                  |
| `clonefile requires source and destination on the same volume: …`                                                                                                                                        | [`clonefile` does not cross a filesystem](#clonefile-does-not-cross-a-filesystem)                                                               |
| Two attached volumes report one `Volume UUID` and one name                                                                                                                                               | [A clone inherits its volume's UUID and name](#a-clone-inherits-its-volumes-uuid-and-name)                                                      |
| A volume rename lands on the Data volume                                                                                                                                                                 | [A volume is named only through its held mount root](#a-volume-is-named-only-through-its-held-mount-root)                                       |
| State is lost on power loss; `sync_all` (`F_FULLFSYNC`) after a build stalls for seconds                                                                                                                 | [`fsync` does not persist on macOS; `F_FULLFSYNC` does](#fsync-does-not-persist-on-macos-f_fullfsync-does)                                      |
| `cowshed rm` of a large image stalls other writers on the store                                                                                                                                          | [Freeing an image is bounded by runs, not by time](#freeing-an-image-is-bounded-by-runs-not-by-time)                                            |
| A granted `flock` or `O_EXLOCK` on an attached image; `Resource temporarily unavailable` (`EAGAIN`) opening an image                                                                                     | [Image-file locks depend on the driver](#image-file-locks-depend-on-the-driver)                                                                 |
| A larger image file leaves the volume the same size; `diskutil apfs resizeContainer` fails with `-69743` or `-69519`                                                                                     | [Growing a RAW image does not grow its container](#growing-a-raw-image-does-not-grow-its-container)                                             |
| `diskimagesiod` or `Error: Couldn’t communicate with a helper application.` in a cowshed operation                                                                                                       | [Every image attach is in-kernel, never DiskImages2](#every-image-attach-is-in-kernel-never-diskimages2)                                        |
| `Failed to initialize IO manager … error code 150`; an `orphaned-disk-image-helpers` or `disk-image-attach-budget` finding                                                                               | [DiskImages2 helpers map kernel memory (error 150)](#diskimages2-helpers-map-kernel-memory-error-150)                                           |
| An attach hangs with no error; `media-notification-cap`; `AppleDiskImageDevice` instances exceed registered ones                                                                                         | [The IOMedia match-notification ceiling](#the-iomedia-match-notification-ceiling)                                                               |
| `diskutil` or attach latency grows with the number of concurrent callers; `disk-lease … unleased=`                                                                                                       | [`storagekitd` serializes `diskutil`](#storagekitd-serializes-diskutil)                                                                         |
| A caller killed mid-attach; `… is still running after Ns, past its Ns deadline; … cowshed never kills one`                                                                                               | [A disk-image command is never killed](#a-disk-image-command-is-never-killed)                                                                   |
| An attached image is missing from `hdiutil info` while another image attaches or detaches                                                                                                                | [`hdiutil info` is not an inventory](#hdiutil-info-is-not-an-inventory)                                                                         |
| `diskutil mount` takes tens of seconds under load; `Multiple disks to mount, cannot specify mount point`                                                                                                 | [An image's volume mounts with `mount_apfs`, not `diskutil mount`](#an-images-volume-mounts-with-mount_apfs-not-diskutil-mount)                 |
| A release waits behind a SecurityAgent prompt; `ReleaseNeedsAuthorization`                                                                                                                               | [Releasing another user's image prompts for authorization](#releasing-another-users-image-prompts-for-authorization)                            |
| `Resource busy`; `dissented by PID`; a detach that waits                                                                                                                                                 | [A busy volume refuses unmount](#a-busy-volume-refuses-unmount)                                                                                 |
| `/dev/diskN` names a different image than it did a moment ago                                                                                                                                            | [BSD device names are reused](#bsd-device-names-are-reused)                                                                                     |
| `kern.maxvnodes` in an error; a `mount_apfs` or `hdiutil` child stalls past its deadline; `vnode-table-saturated`, `vnode-cache-full` or `vnode-limit-below-floor`                                       | [The kernel vnode table saturates](#the-kernel-vnode-table-saturates)                                                                           |
| `departed-volume-references`; `lsmp -a` lists `AppleAPFSVolume` rights after their volumes left                                                                                                          | [Spotlight (`mds`) leaks a media port per admitted volume](#spotlight-mds-leaks-a-media-port-per-admitted-volume)                               |
| A cowshed volume is indexed by Spotlight; `mdutil -s` reports `unknown indexing state`                                                                                                                   | [Spotlight indexing opt-outs](#spotlight-indexing-opt-outs)                                                                                     |
| `dev.cowshed.storage` exits 1 at `merge Spotlight Search Privacy exclusions`; `mkstemp failed on /System/Volumes/Data/.Spotlight-V100/…`                                                                 | [Spotlight indexing opt-outs](#spotlight-indexing-opt-outs)                                                                                     |
| `fseventsd` load; a root-owned `.fseventsd` in a volume root                                                                                                                                             | [`fseventsd` keeps a persistent event log per volume](#fseventsd-keeps-a-persistent-event-log-per-volume)                                       |
| A watcher probe is late or never comes back; `fseventsd` holds gigabytes and its router thread stays busy                                                                                                | [`fseventsd` slows with uptime](#fseventsd-slows-with-uptime)                                                                                   |
| `root-held-event-debris`; a retired mountpoint cowshed cannot delete                                                                                                                                     | [A root-owned `.fseventsd` lands on a bare mountpoint](#a-root-owned-fseventsd-lands-on-a-bare-mountpoint)                                      |
| Rewritten image blocks stay allocated; `tmutil` reports the store excluded                                                                                                                               | [Time Machine and local snapshots](#time-machine-and-local-snapshots)                                                                           |
| After a reboot the store is absent; a DiskUnlock dialog at login; `environment-missing` naming the store                                                                                                 | [The store volume mounts before login](#the-store-volume-mounts-before-login)                                                                   |
| `diskutil info` and the I/O Registry disagree about FileVault; `reserved APFS volume … registers APFS role …`                                                                                            | [`Encrypted` alone is not FileVault](#encrypted-alone-is-not-filevault)                                                                         |
| `refusing nonempty mountpoint`; the store will not mount after a reboot                                                                                                                                  | [The store mountpoint is a plain directory until it mounts](#the-store-mountpoint-is-a-plain-directory-until-it-mounts)                         |
| Another local account's bytes appear in the store                                                                                                                                                        | [`noowners` makes the store machine-global](#noowners-makes-the-store-machine-global)                                                           |
| An `owners` mount of a volume its owner cannot write                                                                                                                                                     | [A `diskutil`-formatted volume has a root-owned root](#a-diskutil-formatted-volume-has-a-root-owned-root)                                       |
| A cowshed volume appears under `/Volumes`; `doctor` reports it mis-mounted                                                                                                                               | [Disk Arbitration mounts an unpinned volume under `/Volumes`](#disk-arbitration-mounts-an-unpinned-volume-under-volumes)                        |
| `Could not find domain for user gui: <uid>` from `launchctl bootstrap`; the gateway is not running after an ssh-only boot                                                                                | [A user LaunchAgent needs a login session](#a-user-launchagent-needs-a-login-session)                                                           |
| The gateway is down after a boot or an install with `last exit code = 78: EX_CONFIG`; launchd starts a half-written binary                                                                               | [`KeepAlive` restarts whatever the plist names](#keepalive-restarts-whatever-the-plist-names)                                                   |
| A bootout kills a draining gateway about 5 s after SIGTERM                                                                                                                                               | [Bootout waits `ExitTimeOut`](#bootout-waits-exittimeout)                                                                                       |
| `launchctl` exits 113 or 37; `bootout` exits non-zero while the job is torn down                                                                                                                         | [`launchctl` answers 113 and 37 at the edges](#launchctl-answers-113-and-37-at-the-edges)                                                       |
| A fresh workspace's supervisor or first `direnv export json` takes many times longer under load                                                                                                          | [`ProcessType` fixes the QoS band of every descendant](#processtype-fixes-the-qos-band-of-every-descendant)                                     |
| A tool that closes inherited descriptors up to `sysconf(_SC_OPEN_MAX)` never finishes, or opens fail at 256                                                                                              | [A launchd job starts with 256 open files](#a-launchd-job-starts-with-256-open-files)                                                           |
| A SecurityAgent dialog from `cowshed setup`; `administrator authorization was declined`                                                                                                                  | [Privileged steps run in one authorization session](#privileged-steps-run-in-one-authorization-session)                                         |
| A credentialed upstream request answers `502`; `macOS Keychain lookup failed with OSStatus`                                                                                                              | [Credentials live in a keychain that may be locked](#credentials-live-in-a-keychain-that-may-be-locked)                                         |
| A fresh executable's first exec takes seconds and its second milliseconds; `first-launch-scan`                                                                                                           | [XProtect scans a program's first launch](#xprotect-scans-a-programs-first-launch)                                                              |
| The scan returns after an install; `first-launch-scan` names the gateway host's path                                                                                                                     | [The Developer Tools exemption keys on path and designated requirement](#the-developer-tools-exemption-keys-on-path-and-designated-requirement) |
| `codesign` raises a keychain dialog                                                                                                                                                                      | [`codesign` prompts for a key whose partition list lacks it](#codesign-prompts-for-a-key-whose-partition-list-lacks-it)                         |
| A copied app is blocked on its first launch                                                                                                                                                              | [Gatekeeper quarantines downloaded apps; copying adds no quarantine](#gatekeeper-quarantines-downloaded-apps-copying-adds-no-quarantine)        |
| A path the profile admits is still denied; a secret stays readable after a wildcard deny                                                                                                                 | [Seatbelt filters by operation first, then by order](#seatbelt-filters-by-operation-first-then-by-order)                                        |
| `realpath` or a `stat` of a parent directory fails `EPERM` under a closed profile                                                                                                                        | [A path's ancestors need metadata access](#a-paths-ancestors-need-metadata-access)                                                              |
| A process dies at startup under a narrow read profile                                                                                                                                                    | [dyld needs broad read access](#dyld-needs-broad-read-access)                                                                                   |
| `ps` fails `EPERM` before it runs in a job                                                                                                                                                               | [A set-user-ID program cannot run sandboxed](#a-set-user-id-program-cannot-run-sandboxed)                                                       |
| A job reads another workspace's command line or start environment with `ps -E`                                                                                                                           | [Seatbelt leaves `kern.procargs2` readable](#seatbelt-leaves-kernprocargs2-readable)                                                            |
| `No user exists for uid …` from `ssh-keygen`; `id` prints no user name; no keychain in a job; a file watcher gets no events                                                                              | [Mach services, OpenDirectory and keychains are denied](#mach-services-opendirectory-and-keychains-are-denied)                                  |
| `echo x > /dev/stdout` fails `EPERM`; Node aborts in process setup with no message                                                                                                                       | [`/dev/stdout` is a link into `/dev/fd`](#devstdout-is-a-link-into-devfd)                                                                       |
| `sandbox_apply` fails with `EPERM` in a test                                                                                                                                                             | [A sandboxed process cannot apply a profile](#a-sandboxed-process-cannot-apply-a-profile)                                                       |
| `invalid port in network address`; a `path-prefix` admits a sibling; `[A-Za-z0-9]{2}` does not match `ab`                                                                                                | [SBPL accepts less than it seems to](#sbpl-accepts-less-than-it-seems-to)                                                                       |
| `EPERM` on `open` or `connect`; `sandbox-exec` exits 71; `log show` refuses                                                                                                                              | [A Seatbelt denial is `EPERM`; the log is closed to non-admins](#a-seatbelt-denial-is-eperm-the-log-is-closed-to-non-admins)                    |
| A `DYLD_INSERT_LIBRARIES` interposer covers some processes and not others                                                                                                                                | [SIP strips `DYLD_*` from launchers](#sip-strips-dyld_-from-launchers)                                                                          |
| A lease reports `Conflict` after its holder closed it; a client connects to a closed listener and reads end of stream, after a spawn returned                                                            | [`posix_spawnp` leaves transient descriptor copies](#posix_spawnp-leaves-transient-descriptor-copies)                                           |
| A job's output never reaches end of stream after its command ends; a listener stays attached in a child                                                                                                  | [Darwin has no `pipe2` and no `SOCK_CLOEXEC`](#darwin-has-no-pipe2-and-no-sock_cloexec)                                                         |
| A copied workspace stays in use because a child holds a file the copy opened                                                                                                                             | [`copyfile(3)` opens its descriptors without `O_CLOEXEC`](#copyfile3-opens-its-descriptors-without-o_cloexec)                                   |
| A signal or lease check hits a process that reused the pid; `ESRCH` after an `exec`                                                                                                                      | [A process is its pid and its unique id](#a-process-is-its-pid-and-its-unique-id)                                                               |
| A job leader that stops its group is read as exited or reaped early                                                                                                                                      | [A stopped child is reported to `waitid`](#a-stopped-child-is-reported-to-waitid)                                                               |
| `EPERM` creating a process group; a worker survives a group `SIGTERM`                                                                                                                                    | [Process groups race their creators and their signals](#process-groups-race-their-creators-and-their-signals)                                   |
| A macOS job's process tree in which a process forked is never `Complete`; a short-lived grandchild is missing                                                                                            | [kqueue cannot name a child](#kqueue-cannot-name-a-child)                                                                                       |
| A process is `uninspected`; `ps -E` prints no environment for another user's process                                                                                                                     | [libproc describes this user's processes; there is no `/proc`](#libproc-describes-this-users-processes-there-is-no-proc)                        |
| A job's CPU total falls between two reads; a macOS job total misses descendants                                                                                                                          | [`proc_pid_rusage` counters can fall between reads](#proc_pid_rusage-counters-can-fall-between-reads)                                           |
| A test host sleeps at 0% CPU on a connected control-socket pair after its peer shut it down                                                                                                              | [A peer `shutdown(2)` can lose a sleeping reader's wakeup](#a-peer-shutdown2-can-lose-a-sleeping-readers-wakeup)                                |
| `EPERM` connecting to a listener a job bound on port 0; a service port collides with an outbound connection                                                                                              | [The ephemeral port range is host-global](#the-ephemeral-port-range-is-host-global)                                                             |
| Cargo or a Go program fails TLS verification against an intercepted host                                                                                                                                 | [macOS-native TLS ignores environment trust anchors](#macos-native-tls-ignores-environment-trust-anchors)                                       |
| Open files on a RAM volume read `EIO` after a job ends; `a RAM disk of … does not fit the host's … bytes of memory`                                                                                      | [A `ram://` disk lives in a forked helper process](#a-ram-disk-lives-in-a-forked-helper-process)                                                |
| `rsync` never converges on an unchanged tree; `mkstempsock: Invalid argument` copying a checkout                                                                                                         | [`/usr/bin/rsync` is openrsync](#usrbinrsync-is-openrsync)                                                                                      |
| A dataset mounts through `/dev/diskN`, reports `apfs`, or appears in Finder                                                                                                                              | [OpenZFS on macOS decides per dataset whether to publish a disk](#openzfs-on-macos-decides-per-dataset-whether-to-publish-a-disk)               |

## Names, paths and temporary space

### `sun_path` is 104 bytes

- **Fact.** `struct sockaddr_un` ends in `char sun_path[104]` on Darwin (`<sys/un.h>`), so a pathname socket address
  holds 103 bytes and the terminating NUL.
- **Rule.** Every socket cowshed binds sits at a short fixed-length path — `gateway.sock` and `listener-handoff.sock` at
  the store's top, `run/manager.sock`, supervisor sockets at `<store>/run/<32-hex digest>.sock` with their
  `<store>/run/<32-hex digest>.members.sock` twins, a workspace's runtime directory behind its `/tmp/cs-<12-hex>` link —
  and a test fixture whose socket path is used as spelled takes its root from the short scratch parent
  (`COWSHED_RUNTIME_LINK`, else `/tmp`), never from `TMPDIR`, while a fixture rooted under `TMPDIR` reaches its sockets
  through a short alias in the runtime link.
- **Owners.** [04_sandbox.md §Fixture roots under /tmp](./04_sandbox.md#fixture-roots-under-tmp) (states the bound);
  [11_shell.md §Protocol](./11_shell.md#protocol) (supervisor socket digest);
  [01_storage.md §Directory layout](./01_storage.md#directory-layout) (root-level sockets);
  [01_storage.md §Shared temporary scratch](./01_storage.md#shared-temporary-scratch) (the fixture suffix length);
  [08_testing.md §Fixtures](./08_testing.md#fixtures) (fixture roots);
  [08_testing.md §Integration tests (real substrate)](./08_testing.md#integration-tests-real-substrate) (scratch roots);
  [05_gateway.md §The installed cowshed](./05_gateway.md#the-installed-cowshed) (`listener-handoff.sock`). Code:
  [`cowshed-core/src/sandbox.rs`] `workspace_runtime_link`; [`cowshed-core/src/runtime/supervisor_socket.rs`]
  `socket_path`; [`cowshed-gateway-types/src/lib.rs`] `production_control_socket`; [`cowshed-apfs/src/disk_lease.rs`]
  `connect`. See also [`/usr/bin/rsync` is openrsync](#usrbinrsync-is-openrsync).

### `/tmp` and `/var` are symlinks into `/private`

- **Fact.** `/tmp`, `/var` and `/etc` are symbolic links to `/private/tmp`, `/private/var` and `/private/etc` (_owner
  text_), and Seatbelt matches the kernel-resolved path, so a `/tmp`-spelled rule silently denies what a `/private/tmp`
  rule allows (_measured_).
- **Rule.** Profile paths are validated as absolute and lexically normalized and rules name the canonical `/private/…`
  spelling, a name looked up through a link — a workspace's `/tmp/cs-<12-hex>` runtime link, `/tmp` itself, `/etc/nix`,
  `/var/select` — is admitted under both spellings, and the workspace mount root is derived, never configured or
  aliased.
- **Owners.** [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [01a_apfs.md §Stable workspace mount root](./01a_apfs.md#stable-workspace-mount-root);
  [08_testing.md §Fixtures](./08_testing.md#fixtures) (the runtime link is never canonicalized back to the long path).
  Code: [`cowshed-core/src/sandbox.rs`] `validate_path`, `resolve_deny`; [`cowshed-core/src/exec.rs`] `plan_exec`;
  [`cowshed-core/src/storage/host_config.rs`] `canonical_mount_root`. See also
  [`/dev/stdout` is a link into `/dev/fd`](#devstdout-is-a-link-into-devfd).

### `/private/var/folders` is root-owned and swept

- **Fact.** `/private/var/folders` is root-owned and the system sweeps the per-user directories beneath it — files in
  `T` unused for three days, files in `C` in safe boot (`confstr(3)`) — and the owner names `dirhelper`'s slots as `T`,
  `TemporaryItems`, `Cleanup At Startup`, `X` and `C` (_owner text_).
- **Rule.** `cowshed setup` provisions a subtree of its own, `/private/var/folders/cowshed` (root:wheel 0755), holding
  the invoking user's `<uid>` directory and its `mnt` mount root (0700) under names the sweeps do not address, and each
  provisioner of it — `cowshed setup`, the `cowshed:apfs-test-root` target and the fixtures' scratch parent — refuses a
  symlink, a non-directory or a foreign owner rather than write through it. A mountpoint under it needs a user-owned
  parent — a root mount, or an `rmdir` directly under the root-owned `cowshed` directory, is refused — so the
  build-cache volume mounts at `shared/build-cache` under a `shared` parent the provisioning user owns (group `nixbld`,
  0750; the owner's ruling, 2026-10-27).
- **Owners.** [01a_apfs.md §Stable workspace mount root](./01a_apfs.md#stable-workspace-mount-root);
  [01a_apfs.md §Dedicated build-cache volume](./01a_apfs.md#dedicated-build-cache-volume) (the user-owned parent);
  [01_storage.md §Directory layout](./01_storage.md#directory-layout);
  [08_testing.md §Integration tests (real substrate)](./08_testing.md#integration-tests-real-substrate) (the scratch
  parent `t`). Code: [`cowshed-core/src/storage/bootstrap/native/macos.rs`] `mount_root_operations`;
  [`cowshed-core/src/storage/host_config.rs`] `MACOS_MOUNT_PREFIX`; [`cowshed-os/src/scratch.rs`] `macos_scratch_root`.
  See also
  [Spotlight (`mds`) leaks a media port per admitted volume](#spotlight-mds-leaks-a-media-port-per-admitted-volume).

### The per-user temp directory comes from `dirhelper`

- **Fact.** `mktemp` with no template, or with `-t`, prefers `_CS_DARWIN_USER_TEMP_DIR` to `TMPDIR` (`mktemp(1)`), and
  `confstr(3)` answers it through `com.apple.bsd.dirhelper` with the host user's `/var/folders/<x>/<y>/T` (_owner
  text_).
- **Rule.** The profile does not admit `com.apple.bsd.dirhelper`, so the lookup fails (`EIO`) and tools fall back to
  `TMPDIR`, the job's own directory under the store, or to `/tmp`, which the profile does not let a job write.
- **Owners.** [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [04_sandbox.md §Evaluating an edited `devenv.nix` inside its workspace](./04_sandbox.md#evaluating-an-edited-devenvnix-inside-its-workspace).
  Code: [`cowshed-core/src/sandbox.rs`] `every_role_has_entropy_and_no_user_directory_helper`;
  [`cowshed-core/src/storage/mod.rs`] `exec_temp_dir`.

### The `/usr/bin` developer-tool shims

- **Fact.** `/usr/bin/git`, `/usr/bin/xcrun` and the other developer tools are `xcode-select` shims that run the tool of
  the selected developer directory, which `DEVELOPER_DIR` overrides (`xcode-select(1)`) and the link
  `/var/select/developer_dir` records, and a shim writes caches under the host user's temporary directory (_owner
  text_).
- **Rule.** Controller Git execs the developer directory's `usr/bin/git` directly with `/var/select` readable, the
  supervisor takes the developer directory of its sandbox `PATH` from a fixed system install, never from the
  `DEVELOPER_DIR` it started with, and the repository's linker wrapper takes `SDKROOT` from the environment and asks
  `xcrun` only when it is absent.
- **Owners.**
  [04_sandbox.md §Trusted supervisor and child profiles](./04_sandbox.md#trusted-supervisor-and-child-profiles) (the Git
  role);
  [04_sandbox.md §Evaluating an edited `devenv.nix` inside its workspace](./04_sandbox.md#evaluating-an-edited-devenvnix-inside-its-workspace)
  (the `xcrun` cache); [08_testing.md §Host-controller authority](./08_testing.md#host-controller-authority) (the
  developer directory);
  [16_build_volumes.md §One Cargo environment on every path](./16_build_volumes.md#one-cargo-environment-on-every-path)
  (the linker). Code: [`cowshed-core/src/sandbox.rs`] `GIT_DEVELOPER_DIR`; [`cowshed-core/src/runtime/supervisor.rs`]
  `developer_directory`; [`packages/cowshed/scripts/macos-linker.sh`].

## Images and clones

### A RAW sparse file is a disk

- **Fact.** A headerless file attaches as a disk of its length through the RAW disk-image class (`hdiutil(1)` shows
  `-imagekey diskimage-class=CRawDiskImage` for forcing a raw blob to attach), so a sparse file of the capacity is a
  blank disk (_measured_: a zero-block 1 GiB file attaches, takes `newfs_apfs`, passes `fsck_apfs -q` and detaches), and
  `st_blocks` counts allocated 512-byte blocks while the file length is only the cap (`stat(2)`).
- **Rule.** Every image is a sparse `.dmg`: the blank template is created at its capacity with no disk tool and every
  image is a clone of it, attached by the RAW class and judged for allocation by `st_blocks * 512`, never by apparent
  length, and an image in any other format is refused.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Format”, “Capacity and allocation”;
  [09_substrates.md §APFS image substrate (reference)](./09_substrates.md#apfs-image-substrate-reference). Code:
  [`cowshed-apfs/src/lib.rs`] `create_blank_unlocked`, `IN_KERNEL_ATTACH`; [`cowshed-apfs/src/image.rs`]
  `IMAGE_EXTENSION`.

### `hdiutil compact` does not reclaim a RAW image

- **Fact.** `hdiutil compact` scans the bands of a SPARSE or SPARSEBUNDLE image, not a RAW file (`hdiutil(1)`), and a
  deleted payload's host allocation survives sync, image fsync, clean unmount and detach on a RAW image (_measured_).
- **Rule.** Guest-freed space returns through the verified re-adoption copy into a fresh sparse RAW image, which keeps
  the old image until the user removes it after verification, never through `compact`, `defrag` or a sparse-file copy.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Space reclamation”;
  [02_workspaces.md §Re-adoption: space reclamation](./02_workspaces.md#re-adoption-space-reclamation);
  [docs/cli.md §`cowshed adopt`](../../packages/cowshed/docs/cli.md#cowshed-adopt) (the retained old image);
  [docs/troubleshooting.md §Disk usage](../../packages/cowshed/docs/troubleshooting.md#disk-usage). Code:
  [`cowshed-core/src/storage/apfs/native/re_adopt.rs`].

### Case sensitivity is fixed at format time

- **Fact.** `newfs_apfs` makes a case-insensitive volume unless given `-e` (`newfs_apfs(8)`), and a case-insensitive
  volume merges paths that differ only in case.
- **Rule.** Every image descends from a blank template formatted `newfs_apfs -U <uid> -G <gid> -e` by the invoking user,
  so a repository's paths survive verbatim, and the checkout's own volume is never consulted: adopt sets the copied
  repository's `core.ignorecase` to `false`.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Case-sensitive, always”;
  [01a_apfs.md §Mount and per-volume policy](./01a_apfs.md#mount-and-per-volume-policy) (the Case sensitivity row);
  [02_workspaces.md §`cowshed adopt` — create the main workspace](./02_workspaces.md#cowshed-adopt--create-the-main-workspace)
  (step 4). Code: [`cowshed-apfs/src/lib.rs`] `attach_and_format_unlocked`; [`cowshed-apfs/src/template.rs`];
  [`cowshed-core/src/git.rs`] `record_case_sensitive_filesystem`.

### A clone's first write pays for its extent map

- **Fact.** `clonefile(2)` shares the source's data blocks and a later write to either file is copy-on-write
  (`clonefile(2)`); the first mutation of a clone, or of the source it still shares, copies the shared extent map
  wherever it lands (a write at any offset, an append, even a metadata-only `ftruncate`), so the cost follows the extent
  count, not the size or the offset, and every write after it is free (_measured_). `hdiutil attach -nomount` does not
  trigger the copy; the read-write `mount_apfs` does, because APFS writes its checkpoint (_measured_).
- **Rule.** A fork measures each fresh clone's extents before it mounts the clone, warns past 7,140 extents naming
  `cowshed defrag`, and never refuses on the count; it forces no write ahead of the mount, whose first mutation pays for
  the map. `defrag` rewrites an image with plain `pread`/`pwrite` because `clonefile`, `copyfile(3)` and `std::fs::copy`
  would share the old map.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Clone cost follows extents, not size”;
  [08_testing.md §APFS / Seatbelt (macOS)](./08_testing.md#apfs--seatbelt-macos);
  [16_build_volumes.md §Why two volumes](./16_build_volumes.md#why-two-volumes). Code:
  [`cowshed-core/src/storage/apfs/extents.rs`] `count_extents`, `prove_forkable`, `rewrite_contiguously`;
  [`cowshed-core/src/storage/lifecycle.rs`] `FORKABLE_MAX`; [`cowshed-core/src/storage/apfs/native.rs`] `defragment`.

### `clonefile` does not cross a filesystem

- **Fact.** A clone shares blocks only inside one APFS volume (`clonefile(2)` fails `EXDEV` across volumes), so files
  inside two separately mounted images cannot be cloned into each other, while whole image files on one store volume
  can.
- **Rule.** A fork clones the backing image file, and the store is one volume so every image clone stays inside it.
- **Owners.** [09_substrates.md §APFS image substrate (reference)](./09_substrates.md#apfs-image-substrate-reference);
  [03_caches.md §The reflink-reachability rule](./03_caches.md#the-reflink-reachability-rule);
  [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume) (same-volume clones);
  [01a_apfs.md §Rejected APFS alternatives](./01a_apfs.md#rejected-apfs-alternatives), “Caches beside the images
  rejected”. Code: [`cowshed-apfs/src/lib.rs`] `clone_image`, `classify_clone_error`.

### A clone inherits its volume's UUID and name

- **Fact.** An APFS clone carries its source volume's name and volume UUID, so neither tells a workspace from the fork
  made of it (_owner text_).
- **Rule.** A mounted volume's identity is the in-image `.cowshed/workspace.json` marker, matched on repository,
  workspace and incarnation and re-stamped inside the staging fence, so a volume UUID is never identity and the label is
  cosmetic.
- **Owners.**
  [01a_apfs.md §Ownership, identity, and the volume label](./01a_apfs.md#ownership-identity-and-the-volume-label);
  [01_storage.md §In-image marker: `.cowshed/workspace.json`](./01_storage.md#in-image-marker-cowshedworkspacejson).
  Code: [`cowshed-core/src/storage/apfs/native.rs`] `write_marker`, `validate_marker`.

### A volume is named only through its held mount root

- **Fact.** xnu's `setattrlist` names the volume that holds any path, mount root or not (`vfs_attrlist.c`,
  `setattrlist_internal`; _measured_), and Disk Arbitration renames through the volume path it recorded, resolved
  seconds to minutes later, so a volume unmounted in between by anything Disk Arbitration has not heard of (a forced
  unmount or detach) hands the name to the volume beneath its bare mountpoint, the host's Data volume (_measured_).
- **Rule.** A volume is named only by `fsetattrlist` on a held, proven mount-root descriptor (`OwnedVolumeRoot`), and
  the runner refuses `diskutil rename` and `renameVolume` for every caller before anything spawns.
- **Owners.**
  [01a_apfs.md §Ownership, identity, and the volume label](./01a_apfs.md#ownership-identity-and-the-volume-label);
  [05_gateway.md §Disk-lifecycle lease](./05_gateway.md#disk-lifecycle-lease) (the refusal);
  [docs/cli.md §`cowshed disk-command <diskutil|hdiutil> -- <args...>`](../../packages/cowshed/docs/cli.md#cowshed-disk-command-diskutilhdiutil----args)
  (exit 101). Code: [`cowshed-apfs/src/disk_lease.rs`] `refuse_volume_rename`; [`cowshed-apfs/src/mounts.rs`]
  `set_volume_name`.

### `fsync` does not persist on macOS; `F_FULLFSYNC` does

- **Fact.** `fsync(2)` on macOS does not flush the drive's cache and `fcntl(F_FULLFSYNC)` does (`fsync(2)`, `fcntl(2)`),
  Rust's `File::sync_all` is `F_FULLFSYNC` on Apple (code comment), on a disk image it flushes every dirty block of the
  whole image (_owner text_), and `F_BARRIERFSYNC` orders writes and makes no durability promise (`fcntl(2)`;
  `<sys/fcntl.h>`: “fsync + issue barrier to drive”).
- **Rule.** A job's own records take one `fsync` because they survive the death of their writer, not power loss, while
  `F_FULLFSYNC` (through `sync_all`) is spent on lifecycle and authority state and on sealed telemetry segments.
- **Owners.** [11_shell.md §Job control](./11_shell.md#job-control), “Durability of a job's own records”;
  [13_telemetry.md §Storage: one authority tier, one audit trail, one Arrow substrate](./13_telemetry.md#storage-one-authority-tier-one-audit-trail-one-arrow-substrate);
  [13_telemetry.md §Process-tree spans](./13_telemetry.md#process-tree-spans) (sealed segments). Code:
  [`cowshed-os/src/fsio.rs`] `Durability`, `full_sync`.

### Freeing an image is bounded by runs, not by time

- **Fact.** One `unlink` of a written image can reclaim its whole extent tree in a single kernel operation that other
  writers on the container can wait behind, and a clone's first mutation pays its whole map inside one syscall that
  userspace cannot split (_measured_).
- **Rule.** Release cuts an image from its end with `ftruncate`, each cut sized toward 250 ms from the last cut's rate
  (1,024 runs first, at least 64, at most twice the last) at normal disk priority, then unlinks the empty file; the file
  length is the restart journal and the bound limits reclamation work, not wall time.
- **Owners.** [01a_apfs.md §Freeing an image](./01a_apfs.md#freeing-an-image);
  [16_build_volumes.md §Garbage collection](./16_build_volumes.md#garbage-collection). Code:
  [`cowshed-core/src/storage/apfs/release.rs`] `STEP_EXTENTS`.

### Image-file locks depend on the driver

- **Fact.** A DiskImages2 helper holds an exclusive lock on its image (_owner text_), so another `O_SHLOCK` or
  `O_EXLOCK` open with `O_NONBLOCK` fails with `EWOULDBLOCK` (`EAGAIN`; `open(2)`), while an in-kernel attachment takes
  none: `flock` and `O_EXLOCK` are all granted with one attached (_measured_).
- **Rule.** No granted lock proves an image is detached: attachment is read from the I/O Registry, where each in-kernel
  drive names its backing file by `backingstore-id`, and detached growth and capture take the file lock only to exclude
  a DiskImages2 attach.
- **Owners.** [01a_apfs.md §Mounts](./01a_apfs.md#mounts); [01a_apfs.md §Images](./01a_apfs.md#images), “Detached
  growth”; [01a_apfs.md §Owned-registry release boundary](./01a_apfs.md#owned-registry-release-boundary) (an orphaned
  helper keeps the lock). Code: [`cowshed-apfs/src/lib.rs`] `refuse_attached`, `ImageSource`.

### Growing a RAW image does not grow its container

- **Fact.** Extending a RAW file only enlarges the disk the kernel presents: the APFS container keeps its old size until
  `diskutil apfs resizeContainer` grows it (_owner text_), and the call errors when the container already spans the
  image (`-69743`, `-69519`; code comment).
- **Rule.** Growth extends the file under the image lease, attaches and verifies the image, then grows the container, so
  a crash between the two leaves a larger image around the old container that an equal-capacity resize refuses and a
  larger resize completes.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Detached growth”. Code: [`cowshed-apfs/src/lib.rs`]
  `grow_container`, `CONTAINER_ALREADY_SPANS_IMAGE`.

## Attach, mount and release

### Every image attach is in-kernel, never DiskImages2

- **Fact.** `hdiutil attach -kernel` serves an image with no helper process (`hdiutil(1)` names only UDRW, UDRO, UDZO,
  ULFO and UDSP as in-kernel formats; the RAW class attaches that way too, _measured_), while every DiskImages2 attach
  (`diskutil image attach`) is served by its own `diskimagesiod` (_owner text_); on one 1 GiB RAW image a 128 MiB write
  plus `F_FULLFSYNC` reaches 769.8 MiB/s in-kernel, 691.2 MiB/s through DiskImages2 and 37.9 MiB/s through the
  out-of-kernel `hdiutil` driver (_measured_: medians of three rounds on a loaded host), and the framework behind
  `hdiutil` and `diskutil image` answers through XPC helper daemons that a loaded host loses, which prints
  `Error: Couldn’t communicate with a helper application.` (_measured_).
- **Rule.** Every image attach is
  `hdiutil attach -kernel -nomount -noverify -nobrowse -imagekey diskimage-class=CRawDiskImage -plist` of the exact
  image (plus `-readonly` for a read-only attachment), cowshed runs no DiskImages2 verb of its own, no flag lowers the
  attach floor, and a command whose framework helper is unreachable fails as `DiskImageHelperUnreachable` with the host
  load, never retried and never read as a detach dissent.
- **Owners.** [01a_apfs.md §Mounts](./01a_apfs.md#mounts);
  [01a_apfs.md §The two drivers, measured on one 1 GiB RAW image](./01a_apfs.md#the-two-drivers-measured-on-one-1-gib-raw-image);
  [01a_apfs.md §Owned-registry release boundary](./01a_apfs.md#owned-registry-release-boundary) (the helper-unreachable
  failure); [09_substrates.md §APFS image substrate (reference)](./09_substrates.md#apfs-image-substrate-reference);
  [08_testing.md §APFS / Seatbelt (macOS)](./08_testing.md#apfs--seatbelt-macos). Code: [`cowshed-apfs/src/lib.rs`]
  `IN_KERNEL_ATTACH`, `DISK_IMAGE_HELPER_UNREACHABLE`. See also
  [DiskImages2 helpers map kernel memory (error 150)](#diskimages2-helpers-map-kernel-memory-error-150),
  [The IOMedia match-notification ceiling](#the-iomedia-match-notification-ceiling).

### DiskImages2 helpers map kernel memory (error 150)

- **Fact.** Each DiskImages2 attachment has its own `diskimagesiod` launchd job that maps 36 shared 2 MiB buffers (72
  MiB) into the kernel; when the kernel cannot map a pool the attach fails with `error code 150`, the driver's
  `kIOReturnNoResources` (-536870210), and the pool is spent by helpers that outlive their image, which keep their
  mapping until root `kill -9`s them because `launchctl kill` of their job is refused even to root, and by image devices
  that a stalled helper keeps allocated (_measured_).
- **Rule.** cowshed runs no DiskImages2 verb of its own, so none of its own operations draws on the pool, `doctor` names
  orphaned helpers with the root command that clears them and reports leaked image devices as
  `disk-image-attach-budget`, and nothing in cowshed kills a helper.
- **Owners.** [01a_apfs.md §How the APFS host degrades](./01a_apfs.md#how-the-apfs-host-degrades) (item 2);
  [06_cli.md §Onboarding and repair](./06_cli.md#onboarding-and-repair) (names the probe);
  [docs/troubleshooting.md §Every attach fails with `error code 150`](../../packages/cowshed/docs/troubleshooting.md#every-attach-fails-with-error-code-150).
  Code: [`cowshed-apfs/src/disk_image_helpers.rs`]; [`cowshed-apfs/src/attach_budget.rs`];
  [`cowshed-cli/src/runtime.rs`] `orphaned_disk_image_helpers_finding`, `disk_image_attach_budget_finding`. See also
  [Every image attach is in-kernel, never DiskImages2](#every-image-attach-is-in-kernel-never-diskimages2),
  [The IOMedia match-notification ceiling](#the-iomedia-match-notification-ceiling).

### The IOMedia match-notification ceiling

- **Fact.** IOKit queues at most 1,024 matches on one user notification and drops the rest silently
  (`IOServiceUserNotification::kMaxOutstanding`, XNU `iokit/Kernel/IOUserClient.cpp`), each attached APFS image
  registers three `IOMedia`, and a DiskImages2 helper that waits for its device past that count never returns, after
  which its stalled termination callback keeps every image device that terminates anywhere on the host allocated until
  the helper dies (_measured_).
- **Rule.** Attaches stay in the kernel, so no helper waits on a notification; a verb whose attach would reach the
  ceiling first detaches the idle workspaces that free enough room (none when they cannot) and attaches anyway, never on
  a timer, and `doctor` reports `media-notification-cap`, and the leaked-device count as `disk-image-attach-budget`.
- **Owners.**
  [01a_apfs.md §The IOMedia match-notification ceiling](./01a_apfs.md#the-iomedia-match-notification-ceiling);
  [01a_apfs.md §How the APFS host degrades](./01a_apfs.md#how-the-apfs-host-degrades) (item 2, never on a timer);
  [02_workspaces.md §Making room for an attach](./02_workspaces.md#making-room-for-an-attach);
  [docs/troubleshooting.md §A verb warns about IOKit's media match-notification queue, or `cowshed doctor` warns `media-notification-cap`](../../packages/cowshed/docs/troubleshooting.md#a-verb-warns-about-iokits-media-match-notification-queue-or-cowshed-doctor-warns-media-notification-cap).
  Code: [`cowshed-apfs/src/media_cap.rs`]; [`cowshed-core/src/media_room.rs`]; [`cowshed-apfs/src/attach_budget.rs`].
  See also [Every image attach is in-kernel, never DiskImages2](#every-image-attach-is-in-kernel-never-diskimages2),
  [DiskImages2 helpers map kernel memory (error 150)](#diskimages2-helpers-map-kernel-memory-error-150).

### `storagekitd` serializes `diskutil`

- **Fact.** `diskutil image create|attach|info|resize` each make a synchronous XPC call to root `storagekitd`, which
  re-syncs every disk (`syncAllDisks`) and answers one caller at a time, and mount-table churn starves that sync, while
  `hdiutil detach`, `newfs_apfs`, `fsck_apfs`, `mount_apfs` and I/O Registry reads bypass it (_measured_).
- **Rule.** cowshed makes the fewest disk-tool calls, plans and validates host storage from the kernel (I/O Registry,
  `getfsstat`) rather than from `diskutil`, and the gateway's disk-lifecycle lease runs storage verbs and mount-table
  changes in alternating classes that overlap only for an evicted holder or an unleased fallback.
- **Owners.** [01a_apfs.md §How the APFS host degrades](./01a_apfs.md#how-the-apfs-host-degrades);
  [05_gateway.md §Disk-lifecycle lease](./05_gateway.md#disk-lifecycle-lease);
  [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume) (host-storage planning from the kernel);
  [docs/troubleshooting.md §An attach is slow, or stderr says `disk-lease … unleased=`](../../packages/cowshed/docs/troubleshooting.md#an-attach-is-slow-or-stderr-says-disk-lease--unleased).
  Code: [`cowshed-gateway/src/disk_lease.rs`]; [`cowshed-gateway-types/src/disk_lease.rs`] `DiskClass`;
  [`cowshed-apfs/src/disk_lease.rs`].

### A disk-image command is never killed

- **Fact.** A DiskImages2 attach whose device never got its BSD node holds every later attach on the host for up to 41
  minutes, and a killed client leaves its `diskimagesiod` wedged for the next attach of that image (_measured_);
  `hdiutil detach` terminates any associated process (`hdiutil(1)`).
- **Rule.** A disk-image command (`hdiutil`, `diskutil image …`, `diskutil eject`) runs in its own session, ignores the
  caller's hangup, interrupt and terminate signals, writes its output to per-run files and is warned about at the
  caller's deadline, never killed, and the next invocation reconciles from the kernel's attachment inventory.
- **Owners.** [01a_apfs.md §How the APFS host degrades](./01a_apfs.md#how-the-apfs-host-degrades), “A disk-image command
  is detached from its caller”; [05_gateway.md §Disk-lifecycle lease](./05_gateway.md#disk-lifecycle-lease);
  [06_cli.md §Commands](./06_cli.md#commands), “cowshed disk-command”;
  [docs/cli.md §`cowshed disk-command <diskutil|hdiutil> -- <args...>`](../../packages/cowshed/docs/cli.md#cowshed-disk-command-diskutilhdiutil----args);
  [07_api.md §Host operations](./07_api.md#host-operations) (`host.diskCommand`). Code: [`cowshed-apfs/src/lib.rs`]
  `is_disk_image_command`, `PastDeadline`, `spawn_disk_child`; [`cowshed-cli/src/disk_command.rs`].

### `hdiutil info` is not an inventory

- **Fact.** While another image attaches or detaches, `hdiutil info -plist` answers a truncated image list with nothing
  marking it (_measured_), and `diskutil image info` reports no device mapping; the kernel's I/O Registry maps an
  image's path to its devices.
- **Rule.** Attachment is read from the I/O Registry — one `IOServiceGetMatchingServices` snapshot per image driver,
  filtered by the image's path — never from `hdiutil info`.
- **Owners.** [01a_apfs.md §Release authority](./01a_apfs.md#release-authority) (the inventory paragraph). Code:
  [`cowshed-apfs/src/lib.rs`] `attached_disk_images`, `registered_matching`.

### An image's volume mounts with `mount_apfs`, not `diskutil mount`

- **Fact.** `diskutil mount` routes the mount through Disk Arbitration, which serializes every client on the host: a
  mount that `mount_apfs` completes in about a second queues 68 s at the median across a loaded fleet (a measurement in
  a code comment), and `storagekitd` rejects a combined attach-and-mount with
  `Multiple disks to mount, cannot specify mount point` while a stale cached descendant outside the image's tree stays
  in its family (_measured_).
- **Rule.** An image's volume mounts with `mount_apfs -o nobrowse,owners,noatime` (`--browse` omits `nobrowse`,
  read-only adds `rdonly`) on the exact verified volume device, with no combined `--mountPoint` attach,
  `diskutil mount`, retry or fallback.
- **Owners.** [01a_apfs.md §Mounts](./01a_apfs.md#mounts);
  [01a_apfs.md §Owned-registry mount boundary](./01a_apfs.md#owned-registry-mount-boundary) (the combined-attach
  refusal); [01a_apfs.md §Mount and per-volume policy](./01a_apfs.md#mount-and-per-volume-policy). Code:
  [`cowshed-apfs/src/lib.rs`] `MOUNT_APFS`, `mount_options`, `mount_request`.

### Releasing another user's image prompts for authorization

- **Fact.** Disk Arbitration passes an unmount or eject outright when the disk's `owner-uid` is the caller's euid;
  otherwise `_DAAuthorize` asks authd for `system.volume.virtual.unmount`, which passes root, and a non-administrator
  gets a SecurityAgent prompt that holds the request (_measured_: a 22-minute stall).
- **Rule.** cowshed attaches only as the invoking user and refuses, before it spawns, a release of a disk-image disk by
  any caller that is neither root nor the disk's owner (a root-owned disk included), and a refusal is never retried or
  forced.
- **Owners.** [01a_apfs.md §Release authority](./01a_apfs.md#release-authority);
  [06_cli.md §CLI Contract](./06_cli.md#cli-contract) (rule 3: no unannounced prompt). Code:
  [`cowshed-apfs/src/release_authority.rs`].

### A busy volume refuses unmount

- **Fact.** A held file anywhere in a volume refuses a non-forced unmount with `EBUSY` (`unmount(2)`), `hdiutil detach`
  unmounts the whole disk through `DADiskUnmount` first, `diskutil unmount` names the holder as `dissented by PID`
  (_owner text_), a held root opened `O_RDONLY` makes an ordinary unmount busy where `O_EVTONLY` does not, and after a
  successful `funmount` the held descriptor's `fstatfs` answers `ENOTSUP`, a dead vnode that is not proof of absence
  (_measured_).
- **Rule.** A release unmounts with `funmount` on the held, proven root descriptor, unforced: `WhenIdle` returns a busy
  refusal at once, `Release` retries it within a 10 s grace and then forces only a volume cowshed owns, and absence is
  read from the kernel mount table.
- **Owners.** [01a_apfs.md §Release authority](./01a_apfs.md#release-authority);
  [01a_apfs.md §Owned-registry release boundary](./01a_apfs.md#owned-registry-release-boundary);
  [01a_apfs.md §How the APFS host degrades](./01a_apfs.md#how-the-apfs-host-degrades) (item 3);
  [02_workspaces.md §Holders cowshed does not run](./02_workspaces.md#holders-cowshed-does-not-run) (what holds a mount;
  event-only opens hold nothing). Code: [`cowshed-apfs/src/mounts.rs`] `funmount`, `HeldMount`;
  [`cowshed-apfs/src/lib.rs`] `detach_was_dissented`, `DetachGrace`.

### BSD device names are reused

- **Fact.** `/dev/diskN` names are reused after a detach, so a name is not an identity (_owner text_).
- **Rule.** cowshed acts on a device only after the I/O Registry maps the image's identity to it under the image lease
  or a raw pin, and never on a remembered name.
- **Owners.** [01a_apfs.md §Owned-registry release boundary](./01a_apfs.md#owned-registry-release-boundary);
  [06_cli.md §Commands](./06_cli.md#commands), “cowshed disk-command hdiutil --registry-entry”. Code:
  [`cowshed-apfs/src/lib.rs`] `pin_raw_device`, `require_attached_mapping`, `reused_device`.

### The kernel vnode table saturates

- **Fact.** macOS caches vnodes up to `kern.maxvnodes`. At the limit no call waits for another to release a vnode: each
  new vnode reclaims a free one, which drops that file's cached pages
  ([01a_apfs.md §The vnode table](./01a_apfs.md#the-vnode-table)). Saturation is vnodes in use (`kern.num_vnodes` −
  `kern.free_vnodes`) reaching the limit, when a `mount_apfs` that takes a second on a healthy host passes a two-minute
  deadline (`sysctl(3)` lists `kern.maxvnodes` and `kern.num_vnodes`, XNU `bsd/kern/kern_sysctl.c` defines
  `kern.free_vnodes`, and the rest is the code's own comment and measurement).
- **Rule.** cowshed names the vnode table as a failure's cause only when vnodes in use reach `kern.maxvnodes`. `doctor`
  also reports a full cache under one vnode per 32 KiB of memory (`vnode-cache-full`) and such a limit with room left
  (`vnode-limit-below-floor`), and prints the raise to that floor as `sudo sysctl -w kern.maxvnodes=<n>` with the
  boot-time job that keeps it, for the operator to decide. A full cache at or over the floor is no finding.
- **Owners.** [01a_apfs.md §The vnode table](./01a_apfs.md#the-vnode-table) (the floor, the full cache and doctor's
  findings); [01a_apfs.md §Owned-registry release boundary](./01a_apfs.md#owned-registry-release-boundary) (a child
  failure names a saturated vnode table);
  [docs/troubleshooting.md §A disk child `did not finish within 120s`](../../packages/cowshed/docs/troubleshooting.md#a-disk-child-did-not-finish-within-120s);
  [06_cli.md §Onboarding and repair](./06_cli.md#onboarding-and-repair) (names the probe). Code:
  [`cowshed-core/src/vnodes.rs`]; [`cowshed-core/src/error.rs`] `storage_failure`; [`cowshed-cli/src/runtime.rs`]
  `vnode_table_finding`.

## Host services that watch volumes

### Spotlight (`mds`) leaks a media port per admitted volume

- **Fact.** In `mds` 2418.6.3.9.400 (arm64e) each mounted `AppleAPFSVolume` it admits leaves one Mach send right that
  only process exit releases; its volume factory rejects any mount whose kernel-resolved path contains `/var/folders/`
  (or one of four other substrings) and applies no `nobrowse`, read-only, marker or Search Privacy gate to any other
  mount (_measured_: disassembly and `lsmp` controls).
- **Rule.** Every session, build, fixture and RAM mount sits under `/private/var/folders/cowshed/<uid>/…` — sessions and
  build volumes under `mnt`, fixtures under `t`, the RAM volume under `ram<revision>` — and the build-cache volume at
  `/private/var/folders/cowshed/shared/build-cache`, and cowshed never impersonates a firmware path or waits for `mds`
  to release a right; a main mounts at its checkout path, which `mds` admits.
- **Owners.** [01a_apfs.md §Spotlight media references](./01a_apfs.md#spotlight-media-references);
  [01a_apfs.md §Stable workspace mount root](./01a_apfs.md#stable-workspace-mount-root);
  [08_testing.md §Integration tests (real substrate)](./08_testing.md#integration-tests-real-substrate);
  [docs/troubleshooting.md §`cowshed doctor` warns `departed-volume-references`](../../packages/cowshed/docs/troubleshooting.md#cowshed-doctor-warns-departed-volume-references);
  [docs/cli.md §`cowshed setup [--uninstall] [--force] [--downgrade]`](../../packages/cowshed/docs/cli.md#cowshed-setup---uninstall---force---downgrade)
  (the fixed physical path and Spotlight admission); [01_storage.md §Directory layout](./01_storage.md#directory-layout)
  (the canonical mount root). Code: [`cowshed-core/src/storage/host_config.rs`] `MACOS_MOUNT_PREFIX`;
  [`cowshed-os/src/scratch.rs`] `macos_scratch_root`; [`cowshed-apfs/src/attach_budget.rs`] `DEPARTED_ATTRIBUTION_HINT`;
  [`cowshed-cli/src/runtime.rs`] `departed_volume_references_finding`;
  [`packages/nx-plugin/src/executors/bounded-exec/ram-temp.ts`] `ramTempPaths`.

### Spotlight indexing opt-outs

- **Fact.** The Data volume's Search Privacy `Exclusions` and an empty `.metadata_never_index` at a volume root are the
  two indexing opt-outs the owner names; on a browsable mount the marker makes `mdutil -s` report “Indexing and
  searching disabled” where an unmarked volume reports “Indexing disabled”, a `nobrowse` mount reads “unknown indexing
  state” either way, and neither opt-out stops the `mds` port leak above (_owner text_). The `Exclusions` live in
  `/System/Volumes/Data/.Spotlight-V100`, which refuses writes even to a root LaunchDaemon: the boot script's `mktemp`
  there failed `Operation not permitted` on each of 26,970 launchd runs, and `mdutil -i off` refuses a `nobrowse` mount
  with `-400` (_measured_, macOS 26.6.2).
- **Rule.** Cowshed plants neither opt-out and writes no Spotlight policy at adopt, setup or boot: the session mount
  root keeps session and build volumes unindexed
  ([01a_apfs.md §Stable workspace mount root](./01a_apfs.md#stable-workspace-mount-root)). Volumes adopted while adopt
  planted `.metadata_never_index` still carry it, and git excludes it there. `mds` is never restarted and indexing never
  toggled.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Spotlight”;
  [01a_apfs.md §Mount and per-volume policy](./01a_apfs.md#mount-and-per-volume-policy) (the Spotlight indexing row);
  [01b_zfs.md §Mount root and Spotlight](./01b_zfs.md#mount-root-and-spotlight). Code: [`cowshed-apfs/src/lib.rs`]
  `SPOTLIGHT_OPT_OUT`; [`cowshed-core/src/git.rs`] `ensure_cowshed_excludes`. See also
  [Spotlight (`mds`) leaks a media port per admitted volume](#spotlight-mds-leaks-a-media-port-per-admitted-volume).

### `fseventsd` keeps a persistent event log per volume

- **Fact.** `fseventsd` keeps a persistent log in each logged volume's `.fseventsd`, and an empty `no_log` file in that
  directory disables it (Apple, _File System Events Programming Guide_,
  [Preventing File System Event Storage](https://developer.apple.com/library/archive/documentation/Darwin/Conceptual/FSEvents_ProgGuide/FileSystemEventSecurity/FileSystemEventSecurity.html));
  it creates the directory as root when a browsable volume mounts, honors `no_log` at the next mount, a clone carries
  its source's log and live delivery to watchers is unchanged, though a volume's first events after its mount can go
  missing with no drop flag, logged or not (_measured_).
- **Rule.** Every read-write image mount opts its volume out with an empty `no_log` marker as soon as `mount_apfs`
  succeeds, retiring a root-owned log through one `nobrowse,noowners` remount that leaves the directory `0711`, and a
  failure to opt out is reported while the mount stands.
- **Owners.** [01a_apfs.md §The event log](./01a_apfs.md#the-event-log);
  [01a_apfs.md §Mount and per-volume policy](./01a_apfs.md#mount-and-per-volume-policy);
  [02_workspaces.md §The safe-to-clone-main invariant](./02_workspaces.md#the-safe-to-clone-main-invariant) (root-owned
  volume bookkeeping in a clone). Code: [`cowshed-apfs/src/lib.rs`] `opt_out_of_event_log`,
  `retire_inherited_event_log`, `EVENT_LOG_OPT_OUT`. See also
  [`fseventsd` slows with uptime](#fseventsd-slows-with-uptime).

### `fseventsd` slows with uptime

- **Fact.** `fseventsd` interns event paths in one shared string table (its `PathStringZone`) created with 4096 buckets
  and never resized, so each lookup under a global mutex walks a chain of about stored strings ÷ 4096 entries
  (_measured_: disassembly of `fseventsd` on macOS 26.6.2), and the cost grows with uptime until `fseventsd` restarts
  (`sudo -n killall fseventsd`; launchd respawns it): an instance up three days held 12.4–12.8 GB, kept its router
  thread busy in every sample and left a test stream's touch undelivered after 89 s, while a restarted instance held
  4.2–4.4 MB at 0–9 % router load and answered probes in 0.2–42 ms with the same clients, so no client causes it
  (_measured_ by thread sampling).
- **Rule.** A daemon answers from the files as they are when its watcher probe comes back late or not at all, never from
  a stream that has gone quiet, and cowshed never restarts `fseventsd`.
- **Owners.** _No spec owner_ for the path table and the restart.
  [16_build_volumes.md §The Nx daemon stays on](./16_build_volumes.md#the-nx-daemon-stays-on), “A daemon answers from
  the files as they are” (the lost-probe rule). Code: [`packages/nx-plugin/src/nx-daemon-watch-stream-host.test.ts`].
  See also [`fseventsd` keeps a persistent event log per volume](#fseventsd-keeps-a-persistent-event-log-per-volume).

### A root-owned `.fseventsd` lands on a bare mountpoint

- **Fact.** A logged volume's final `fseventsd` flush can land on the bare host mountpoint after unmount and leave a
  root-owned `0700` `.fseventsd` that the mountpoint's owner cannot delete (_owner text_).
- **Rule.** When that directory is the mountpoint's only entry, gc renames the user-owned mountpoint to a same-parent
  `.root-held-<uuid>` without replacing anything and names the retained log with its exact privileged cleanup in its
  `deferred` output, while a gc preview lists the same cleanup as `deferred` and `doctor` reports
  `root-held-event-debris` (info), neither renaming.
- **Owners.** [01a_apfs.md §Retired mountpoint debris](./01a_apfs.md#retired-mountpoint-debris);
  [docs/cli.md §`cowshed gc`](../../packages/cowshed/docs/cli.md#cowshed-gc). Code:
  [`cowshed-core/src/storage/apfs/native.rs`] `plan_stray_cleanup`, `retain_root_held_mountpoint`;
  [`cowshed-core/src/storage/recovery.rs`] `ROOT_HELD_MOUNT_PREFIX`; [`cowshed-core/src/runtime/project.rs`]
  `root_held_finding`, `root_held_deferred`.

### Time Machine and local snapshots

- **Fact.** With Time Machine configured, each APFS volume the backup includes, the Data volume among them, takes local
  snapshots (`tmutil(8)`; hourly is _owner text_), and a snapshot pins every block of a churning image rewritten after
  it (_owner text_); a path-level `tmutil addexclusion` stops that path's backup, not its volume's snapshots (_owner
  text_), and Time Machine leaves the store volume out (`tmutil isexcluded /private/cowshed/store` reports `[Excluded]`;
  _measured_).
- **Rule.** All durable bytes live on dedicated volumes (the store, and the build-cache volume for the mbx store). The
  store takes no local snapshots while Time Machine leaves it out — measured above, for `/private/cowshed/store` only —
  and backup is one per-volume decision; cowshed runs no `tmutil` and applies no exclusion. [INFERENCE] The build-cache
  volume's snapshot and Time Machine behaviour is unmeasured; measuring it as the store was measured —
  `tmutil isexcluded` on its mountpoint and a local-snapshot listing for the volume, on a host with Time Machine
  configured — is what settles whether it takes no local snapshots either.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Time Machine”;
  [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume);
  [01a_apfs.md §Rejected APFS alternatives](./01a_apfs.md#rejected-apfs-alternatives);
  [docs/troubleshooting.md §Backup and durability (read once, remember forever)](../../packages/cowshed/docs/troubleshooting.md#backup-and-durability-read-once-remember-forever);
  [01b_zfs.md §One pool, one root](./01b_zfs.md#one-pool-one-root) (the same rule for a ZFS file vdev on Data).

## The store volume and its accounts

### The store volume mounts before login

- **Fact.** A FileVault volume stays locked until its passphrase arrives, and `noauto` in `/etc/fstab` (`fstab(5)`)
  suppresses automatic mounting but not `APFSUserAgent`'s login-time DiskUnlock dialog, which looks for a
  generic-password item named by the volume UUID (_owner text_).
- **Rule.** `cowshed setup` encrypts an unencrypted store in place, stores its passphrase in
  `/Library/Keychains/System.keychain` under the label and the UUID, pins the volume `noauto,nobrowse,noowners` by UUID
  in `/etc/fstab` and installs the root LaunchDaemon `dev.cowshed.storage`, which unlocks and mounts it at
  `/private/cowshed/store` before any login without running the cowshed binary. The build-cache volume gets the same
  treatment by the same daemon, script and keychain scheme, at its own canonical path and with the fstab flags its own
  pin states, `noatime` among them
  ([01a_apfs.md §Dedicated build-cache volume](./01a_apfs.md#dedicated-build-cache-volume)).
- **Owners.** [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume);
  [06_cli.md §Onboarding and repair](./06_cli.md#onboarding-and-repair);
  [01a_apfs.md §Mount and per-volume policy](./01a_apfs.md#mount-and-per-volume-policy) (the `noauto`, `nobrowse` and
  FileVault rows);
  [docs/cli.md §`cowshed setup [--uninstall] [--force] [--downgrade]`](../../packages/cowshed/docs/cli.md#cowshed-setup---uninstall---force---downgrade).
  Code: [`cowshed-core/src/storage/bootstrap/native/macos.rs`] `desired_mount_service`, `MOUNT_SERVICE_LABEL`;
  [`cowshed-core/src/storage/bootstrap/native/macos/keychain.rs`]; [`cowshed-core/src/storage/fstab.rs`] `build_fstab`.

### `Encrypted` alone is not FileVault

- **Fact.** A volume group's Data volume registers `Encrypted` while FileVault is off, because its encryption is keyless
  and bound to the hardware, so `Encrypted` alone does not say a passphrase unlocks a volume; a role-less volume is
  encrypted only by adding a crypto user (_measured_ against `diskutil info`'s `FileVault`).
- **Rule.** FileVault is read from the kernel registry record alone: a volume that registers role 0 (role-less) takes
  its `Encrypted` as its FileVault state, and a reserved volume that registers any other role, or no role, is refused
  (`ReservedVolumeRole`) rather than guessed.
- **Owners.** [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume) (the registry table);
  [docs/troubleshooting.md §`cowshed ls` takes tens of seconds; `cowshed fork` or `doctor` takes a minute](../../packages/cowshed/docs/troubleshooting.md#cowshed-ls-takes-tens-of-seconds-cowshed-fork-or-doctor-takes-a-minute)
  (the reserved volumes' FileVault state). Code: [`cowshed-core/src/storage/bootstrap/native/macos.rs`]
  `reserved_volume_file_vault`.

### The store mountpoint is a plain directory until it mounts

- **Fact.** The store's mountpoint is an ordinary directory on Data, so what a service writes there before the volume
  mounts — a LaunchAgent's socket, a stderr file, Finder's `.DS_Store` — stays in it, and an entry that is not a
  disposable stub keeps it masked (_owner text_).
- **Rule.** Logs live outside the mountpoint (`/var/log/cowshed-storage.log`, `~/Library/Logs/cowshed/`), `setup` and
  the existing-only runtime reclaim only enumerated disposable stubs (an idle daemon socket, directory-only `mnt/`
  scaffolding, empty directories, `.DS_Store`) before they remount, the root boot script refuses any nonempty
  mountpoint, and any other entry keeps the mountpoint masked, which existing-only commands refuse with
  `environment-missing` naming the mountpoint. The build-cache volume's mountpoint follows the same rule, and a foreign
  top-level entry of another owner on that volume is never a reclaimable stub
  ([01a_apfs.md §Dedicated build-cache volume](./01a_apfs.md#dedicated-build-cache-volume)).
- **Owners.** [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume), “Mount ordering and the
  unmounted-masking guard”;
  [14_nix.md §Ownership detection: validate, never mutate](./14_nix.md#ownership-detection-validate-never-mutate). Code:
  [`cowshed-core/src/storage/bootstrap/native/macos.rs`] `reclaimable_stub_paths`; [`cowshed-cli/src/launchd.rs`]
  `LaunchAgentSpec::assemble`.

### `noowners` makes the store machine-global

- **Fact.** A volume mounted `noowners` ignores the ownership field, so every object appears owned by the current
  effective user and every local account sees the same bytes as its own (`mount(8)`).
- **Rule.** The store mounts `noowners` and is shared by every local account, with FileVault protecting only a stolen
  disk.
- **Owners.** [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume), “`noowners` is deliberate”;
  [01a_apfs.md §One herd, multiple users](./01a_apfs.md#one-herd-multiple-users);
  [01a_apfs.md §Mount and per-volume policy](./01a_apfs.md#mount-and-per-volume-policy) (the `owners` and `noowners`
  rows). Code: [`cowshed-core/src/storage/fstab.rs`] `build_fstab`.

### A `diskutil`-formatted volume has a root-owned root

- **Fact.** A volume made with `diskutil image create blank --fs APFS` has a root-owned root that an `owners` mount
  cannot write and only root can hand over, and the flag offers only APFS, ExFAT, MS-DOS or None, so it cannot ask for
  case sensitivity (`diskutil help image create blank`; _owner text_ for the root).
- **Rule.** Each image volume is a clone of a blank template formatted once with `newfs_apfs` as the invoking user
  (`-U`/`-G`, case-sensitive `-e`) and is mounted `owners`.
- **Owners.** [01a_apfs.md §Images](./01a_apfs.md#images), “Format”;
  [01a_apfs.md §Mount and per-volume policy](./01a_apfs.md#mount-and-per-volume-policy) (the Root UID/GID row). Code:
  [`cowshed-apfs/src/lib.rs`] `attach_and_format_unlocked`.

### Disk Arbitration mounts an unpinned volume under `/Volumes`

- **Fact.** Disk Arbitration mounts a volume it discovers at `/Volumes/<name>`, in a namespace Finder shows and where a
  name collision is renamed (`widget 1`) (_owner text_).
- **Rule.** A cowshed volume is mounted only at its canonical path; one found at `/Volumes/<name>` is mis-mounted, not
  missing, and `cowshed setup` remounts it.
- **Owners.** [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume);
  [01a_apfs.md §Rejected APFS alternatives](./01a_apfs.md#rejected-apfs-alternatives);
  [docs/gateway.md §Start at login (launchd)](../../packages/cowshed/docs/gateway.md#start-at-login-launchd).

## launchd, privileges and credentials

### A user LaunchAgent needs a login session

- **Fact.** User LaunchAgents load into the `gui/<uid>` domain, the user-login domain created when the user logs in at
  the GUI (`launchctl(1)`), and never fire for a uid with no graphical login (_owner text_).
- **Rule.** The gateway runs as a LaunchAgent bootstrapped into `gui/<uid>` with fixed `/bin/launchctl` arguments, an
  install whose bootout goes overdue leaves the agent unloaded and prints the exact `launchctl bootstrap` line to run
  once launchd lets go, and a headless dedicated account is specified to run the gateway as a LaunchDaemon instead.
- **Owners.** [05_gateway.md §Placement and identity](./05_gateway.md#placement-and-identity);
  [14_nix.md §Posture B — dedicated `dev` uid (recommended hardening)](./14_nix.md#posture-b--dedicated-dev-uid-recommended-hardening);
  [docs/gateway.md §Start at login (launchd)](../../packages/cowshed/docs/gateway.md#start-at-login-launchd);
  [docs/troubleshooting.md §A command says "the cowshed daemon is build X; this cowshed is build Y"](../../packages/cowshed/docs/troubleshooting.md#a-command-says-the-cowshed-daemon-is-build-x-this-cowshed-is-build-y).
  Code: [`cowshed-cli/src/launchd.rs`] `gui_domain`, `bootstrap_hint`.

### `KeepAlive` restarts whatever the plist names

- **Fact.** launchd restarts a job whose `KeepAlive` is `true` whenever it exits, by default at most once every 10 s
  (`ThrottleInterval`; `launchd.plist(5)`), and a job whose program is missing exits 78 (`EX_CONFIG`) at launch (_owner
  text_).
- **Rule.** The stable name the gateway's launchd job runs moves in a single rename onto a content-addressed copy on the
  Data volume, so launchd never finds a missing or half-written binary and an install restarts the agent onto the new
  build. The root storage daemon `dev.cowshed.storage` has no `KeepAlive`: a boot run that refuses exits once with its
  refusal logged, instead of respawning every 10 s.
- **Owners.** [05_gateway.md §The installed cowshed](./05_gateway.md#the-installed-cowshed);
  [docs/gateway.md §Start at login (launchd)](../../packages/cowshed/docs/gateway.md#start-at-login-launchd);
  [01a_apfs.md §Dedicated store volume](./01a_apfs.md#dedicated-store-volume) (the storage daemon). Code:
  [`cowshed-cli/src/launchd.rs`] `ServiceLifecycle`, `HostStableExecutable`, `replace_symlink`;
  [`cowshed-core/src/storage/bootstrap/native/macos.rs`] `desired_mount_service`; [`cowshed-cli/src/gateway_service.rs`]
  `install_host_stable_executable`.

### Bootout waits `ExitTimeOut`

- **Fact.** launchd SIGKILLs a stopping job `ExitTimeOut` seconds after SIGTERM, a default the system defines
  (`launchd.plist(5)`; 5 s is _measured_).
- **Rule.** A changed plist is booted out, the old daemon is awaited by its kernel exit event and until launchd drops
  the job, and only then is the agent bootstrapped again; the gateway states `ExitTimeOut` 30 s, longer than its 20 s
  drain.
- **Owners.** [05_gateway.md §The installed cowshed](./05_gateway.md#the-installed-cowshed);
  [docs/gateway.md §Start at login (launchd)](../../packages/cowshed/docs/gateway.md#start-at-login-launchd). Code:
  [`cowshed-cli/src/gateway_service.rs`] `deactivate_launch_agent`; [`cowshed-cli/src/agent_cycle.rs`];
  [`cowshed-gateway-types/src/lib.rs`] `GATEWAY_EXIT_TIMEOUT`. See also
  [`launchctl` answers 113 and 37 at the edges](#launchctl-answers-113-and-37-at-the-edges).

### `launchctl` answers 113 and 37 at the edges

- **Fact.** `launchctl kickstart` answers 113 (“Could not find service”) for a job launchd does not hold, `kickstart -k`
  answers 37 (`EALREADY`) while one is still in teardown, and `launchctl bootout` can exit non-zero while launchd tears
  the job down (_owner text_).
- **Rule.** The reload takes the one step launchd's state allows — bootstrap an agent launchd does not hold, kickstart
  one it holds, leave one in teardown alone and name it — and judges a bootout by the process and the job, not by its
  exit status.
- **Owners.** [05_gateway.md §The installed cowshed](./05_gateway.md#the-installed-cowshed);
  [docs/gateway.md §Start at login (launchd)](../../packages/cowshed/docs/gateway.md#start-at-login-launchd). Code:
  [`cowshed-cli/src/gateway_service.rs`] `deactivate_launch_agent`; [`cowshed-cli/src/agent_cycle.rs`]. See also
  [Bootout waits `ExitTimeOut`](#bootout-waits-exittimeout).

### `ProcessType` fixes the QoS band of every descendant

- **Fact.** launchd applies a job's `ProcessType` band to it and every descendant, which cannot leave it: `Background`
  runs at priority 4 and `Standard` at 20, both with I/O throttled behind the host's, and `Interactive` at 31
  (`launchd.plist(5)`; _measured_).
- **Rule.** Every cowshed LaunchAgent is `ProcessType` `Interactive`, so the gateway, every workspace supervisor and
  every job run at the host's own band.
- **Owners.** [05_gateway.md §Placement and identity](./05_gateway.md#placement-and-identity). Code:
  [`cowshed-cli/src/launchd.rs`] `PROCESS_TYPE`.

### A launchd job starts with 256 open files

- **Fact.** A launchd job starts with a soft `maxfiles` of 256 and an unlimited hard limit (`launchctl limit maxfiles`
  prints launchd's own, which its jobs inherit; `launchctl(1)`), every descendant inherits both, and an infinite soft
  limit leaves `sysconf(_SC_OPEN_MAX)` useless to tools that close inherited descriptors (_owner text_).
- **Rule.** The gateway plist sets `SoftResourceLimits.NumberOfFiles` to the finite `245760` and
  `HardResourceLimits.NumberOfFiles` to `RLIM_INFINITY`, and child-spawn code changes neither.
- **Owners.** [05_gateway.md §Placement and identity](./05_gateway.md#placement-and-identity). Code:
  [`cowshed-cli/src/launchd.rs`] `soft_number_of_files`.

### Privileged steps run in one authorization session

- **Fact.** Root operations need an administrator authorization, and `AuthorizationExecuteWithPrivileges`, which lets
  one session run several privileged commands (_owner text_), is marked deprecated in the SDK header (`Authorization.h`:
  use a launchd-launched helper tool or the Service Management framework instead) and reports only that the tool
  launched, not how it exited (code comment).
- **Rule.** `cowshed setup` announces its action on stderr before it raises the one dialog and runs every privileged
  step in that single session through `/bin/sh`, which appends each child's exit status, while a run with nothing to
  escalate prints no authorization announcement.
- **Owners.** _No spec owner_ for the launch-only status of `AuthorizationExecuteWithPrivileges` and the `/bin/sh`
  wrapper that appends each child's exit status: `macos.rs` states them.
  [06_cli.md §CLI Contract](./06_cli.md#cli-contract) (rule 3);
  [06_cli.md §Onboarding and repair](./06_cli.md#onboarding-and-repair);
  [docs/cli.md §`cowshed setup [--uninstall] [--force] [--downgrade]`](../../packages/cowshed/docs/cli.md#cowshed-setup---uninstall---force---downgrade).
  Code: [`cowshed-core/src/storage/bootstrap/native/macos.rs`] `AuthorizationExecuteWithPrivileges`;
  [`cowshed-cli/src/setup_service.rs`] `AUTHORIZATION_ANNOUNCEMENT`.

### Credentials live in a keychain that may be locked

- **Fact.** A user's login keychain is not unlocked automatically in a session with no login, so its generic-password
  items stay unreadable until something unlocks it (_owner text_).
- **Rule.** Registry and git credentials live only as generic-password items under service `dev.cowshed.gateway`, read
  by the gateway after endpoint, token, project, authority and path checks; a lookup the keychain fails answers the
  request `502`, and a dedicated account's gateway is specified to defer credentialed reads until its first shell, or
  `cowshed unlock`, unlocks the keychain.
- **Owners.** [05_gateway.md §Credentials](./05_gateway.md#credentials);
  [14_nix.md §Posture B — dedicated `dev` uid (recommended hardening)](./14_nix.md#posture-b--dedicated-dev-uid-recommended-hardening);
  [docs/gateway.md §Credentials (credential-store-held, never in workspaces)](../../packages/cowshed/docs/gateway.md#credentials-credential-store-held-never-in-workspaces).
  Code: [`cowshed-gateway/src/platform.rs`] `KeychainCredentialProvider`; [`cowshed-gateway/src/proxy.rs`].

## Code signing and first launch

### XProtect scans a program's first launch

- **Fact.** macOS assesses each new executable on its first launch and charges the wait to the run's responsible
  process: a launchd job is its own responsible process and everything it starts inherits that, and a charged launch of
  a program cowshed's size waits hundreds of milliseconds to seconds (_owner text_); for apps Apple documents the check
  as XProtect's, run whenever an app is first launched, changed in the file system, or XProtect's own signatures are
  updated
  ([Apple Platform Security, “Protecting against malware in macOS”](https://support.apple.com/guide/security/sec469d47bd8/web)),
  and no source names the scanner of a bare executable.
- **Rule.** The gateway runs through a fixed-path signed host that is the responsible process of the daemon, every
  workspace supervisor and every job, so one Developer Tools entry exempts them all, and `doctor` times a fresh
  executable's first and second launch to report `first-launch-exempt`, `first-launch-scan` or
  `first-launch-unmeasured`.
- **Owners.** [05_gateway.md §The installed cowshed](./05_gateway.md#the-installed-cowshed), “The gateway host” (the
  responsible process and the host);
  [05_gateway.md §Control plane (Unix socket + optional 7644)](./05_gateway.md#control-plane-unix-socket--optional-7644),
  “{"op":"first-launch"}” (the `first-launch` op);
  [06_cli.md §Onboarding and repair](./06_cli.md#onboarding-and-repair), “`first-launch` reads whether” (the
  `first-launch` check);
  [docs/troubleshooting.md §New binaries start slowly in `cowshed exec`, or `cowshed doctor` warns `first-launch-scan`](../../packages/cowshed/docs/troubleshooting.md#new-binaries-start-slowly-in-cowshed-exec-or-cowshed-doctor-warns-first-launch-scan).
  Code: [`cowshed-cli/src/gateway_host.rs`] `VERB`; [`cowshed-cli/src/first_launch.rs`] `measure`, `judge`;
  [`cowshed-cli/src/runtime.rs`] `first_launch_finding`.

### The Developer Tools exemption keys on path and designated requirement

- **Fact.** A Developer Tools entry (System Settings → Privacy & Security) lets an app run software that does not meet
  the system's security policy (Apple, _Mac User Guide_,
  [Control the ability of apps to run software that doesn’t meet the system’s security policy](https://support.apple.com/guide/mac-help/control-ability-apps-run-software-meet-mchlc5fb7f9c/mac)),
  and it exempts a responsible process and its descendants from the first-launch scan (_owner text_); `tccd` keys a
  non-bundle client's entry to its resolved path and, by one third-party report the owner cites, its designated
  requirement (a second report says the path alone; the TCC database is unreadable to the user), so a content-addressed
  copy (a new path per build) cannot hold one, and the owner infers that an ad-hoc signature, whose designated
  requirement is a cdhash, cannot either.
- **Rule.** The host sits at one fixed path and every install that changes it re-signs it with the user's self-signed
  cowshed code-signing identity, whose designated requirement
  `identifier "dev.cowshed.gateway-host" and certificate leaf = H"<sha1>"` is the same for every build, so the operator
  adds the host to Developer Tools once and `doctor` reports whether the entry holds.
- **Owners.** [05_gateway.md §The installed cowshed](./05_gateway.md#the-installed-cowshed), “The gateway host” (the
  host's path, signature and designated requirement);
  [06_cli.md §Onboarding and repair](./06_cli.md#onboarding-and-repair), “`first-launch` reads whether” (the
  `first-launch` check);
  [docs/troubleshooting.md §New binaries start slowly in `cowshed exec`, or `cowshed doctor` warns `first-launch-scan`](../../packages/cowshed/docs/troubleshooting.md#new-binaries-start-slowly-in-cowshed-exec-or-cowshed-doctor-warns-first-launch-scan).
  Code: [`cowshed-cli/src/code_signing.rs`] `GATEWAY_HOST_IDENTIFIER`; [`cowshed-cli/src/gateway_host.rs`] `VERB`;
  [`cowshed-cli/src/first_launch.rs`] `judge`.

### `codesign` prompts for a key whose partition list lacks it

- **Fact.** A key's partition list limits access by an application's code signature and changing it needs that
  keychain's password, and `codesign` needs `apple:` in it (`security(1)`, `set-key-partition-list`); a key imported
  into the login keychain carries `security`'s partition, and `codesign` then raises a keychain dialog (observed on
  macOS 26.6; _owner text_).
- **Rule.** The signing identity lives in cowshed's own empty-password keychain
  `~/Library/Keychains/cowshed-signing.keychain`, imported with an access list naming `codesign` and a partition list
  set with that password, so signing raises no dialog and the login keychain is never written, while a dedicated
  account's signing keys are specified to follow the `security set-key-partition-list` recipe.
- **Owners.** [05_gateway.md §The installed cowshed](./05_gateway.md#the-installed-cowshed), “The gateway host” (the
  signing identity and its keychain);
  [14_nix.md §iOS, Xcode, and simulators (posture B)](./14_nix.md#ios-xcode-and-simulators-posture-b) (the
  `set-key-partition-list` recipe). Code: [`cowshed-cli/src/code_signing.rs`] `KEYCHAIN_NAME`, `prepare`,
  `create_identity`.

### Gatekeeper quarantines downloaded apps; copying adds no quarantine

- **Fact.** `cp` and `cp -R` add no `com.apple.quarantine` attribute, which downloader apps add, but copy one the source
  carries (`cp(1)`: `-X` skips extended attributes), and an ad-hoc-signed app still trips Gatekeeper on its first launch
  while a Developer-ID-signed one opens cleanly (_owner text_).
- **Rule.** A human promotes a copied app by hand — verifying its signature, copying it into `~/Applications` and
  clearing any quarantine attribute — the steps `14_nix.md` specifies as a `cowshed app promote` verb that requires a
  Developer-ID signature unless forced.
- **Owners.** _No code owner_. [14_nix.md §macOS desktop apps: three lanes](./14_nix.md#macos-desktop-apps-three-lanes);
  [docs/desktop.md §Lane 3: use it yourself](../../packages/cowshed/docs/desktop.md#lane-3-use-it-yourself);
  [docs/desktop.md §Signing](../../packages/cowshed/docs/desktop.md#signing).

## Seatbelt

### Seatbelt filters by operation first, then by order

- **Fact.** In SBPL a rule for the exact operation outranks a wildcard rule whatever their order, and order decides only
  between rules for the same operation: a wildcard `deny file-read*` still permits a read under a broad
  `allow file-read-data`, and an explicit `deny file-read-data` blocks it (_measured_).
- **Rule.** Each profile emits its layers in order — broad allows, the store-volume deny, scoped carve-backs, secret
  denies — with every `file-read*` rule paired with an explicit `file-read-data` twin, and unit tests assert that no
  allow precedes a deny that covers it and that the secret denies follow the grants and carve-backs.
- **Owners.**
  [04_sandbox.md §Trusted supervisor and child profiles](./04_sandbox.md#trusted-supervisor-and-child-profiles) (the
  Notes); [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [08_testing.md §Unit tests (pure, all platforms)](./08_testing.md#unit-tests-pure-all-platforms). Code:
  [`cowshed-core/src/sandbox.rs`] `seatbelt_profile`, `push_line`, `hard_denies`,
  `no_allow_precedes_a_deny_that_covers_it`, `secret_denies_follow_grants_and_carve_backs`.

### A path's ancestors need metadata access

- **Fact.** Seatbelt gates a path's metadata (`file-read-metadata`) separately from its data, and canonicalizing a path
  (`realpath`, as Cargo and libgit2 do for a fetched cache path) reads the metadata of every ancestor, so a profile that
  admits a leaf but not its ancestors makes it fail (_owner text_).
- **Rule.** Every capability grant under HOME gets `file-read-metadata` literals for its ancestors beneath HOME —
  resolvable, never listable — and the workspace mount, an explicit grant and an allowed socket get the exact literal
  ancestors path resolution needs.
- **Owners.** [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [04_sandbox.md §Trusted supervisor and child profiles](./04_sandbox.md#trusted-supervisor-and-child-profiles) (the
  Read policy); [15_capabilities.md §One contribution contract](./15_capabilities.md#one-contribution-contract). Code:
  [`cowshed-core/src/sandbox.rs`] `push_readable_ancestors`, `push_capability_grant`.

### dyld needs broad read access

- **Fact.** dyld and toolchains read libraries, frameworks and tools across `/usr`, `/System`, `/Applications`,
  `/nix/store` and `/opt` to start a process, including the metadata of the separate store entries that hold a tool's
  shared libraries, so a profile cannot deny system reads (_owner text_).
- **Rule.** The baseline allows `file-read-data` on `/`, denies HOME and carves back only named paths, so system and
  toolchain roots stay readable while nothing under HOME is readable because it exists.
- **Owners.** [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [04_sandbox.md §Trusted supervisor and child profiles](./04_sandbox.md#trusted-supervisor-and-child-profiles) (the
  Read policy); [04_sandbox.md §devenv / Nix inside the sandbox](./04_sandbox.md#devenv--nix-inside-the-sandbox);
  [docs/README.md §The cache model in one paragraph](../../packages/cowshed/docs/README.md#the-cache-model-in-one-paragraph)
  (dyld needs the store entries' metadata). Code: [`cowshed-core/src/sandbox.rs`] `seatbelt_profile`.

### A set-user-ID program cannot run sandboxed

- **Fact.** macOS refuses a sandboxed exec of a set-user-ID program whatever the profile allows, `(allow default)`
  included, and `/bin/ps` is set-user-ID root; the one Seatbelt form that admits it is `(with no-sandbox)`, which runs
  the program outside every sandbox (_owner text_).
- **Rule.** Every role that runs workspace commands carries exactly one
  `(allow process-exec (literal "/bin/ps") (with no-sandbox))` after the broad exec allow, and every other set-user-ID
  program stays refused.
- **Owners.** [04_sandbox.md §Process inspection](./04_sandbox.md#process-inspection);
  [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile). Code:
  [`cowshed-core/src/sandbox.rs`] `PROCESS_STATUS_EXEC`.

### Seatbelt leaves `kern.procargs2` readable

- **Fact.** Every job reads the command line, and the start environment unless it belongs to an Apple platform program,
  of every process its user runs, and neither `(deny sysctl*)` nor `(deny process-info*)` closes it on its own (_owner
  text_).
- **Rule.** cowshed adds no rule for it and states the exposure — a secret in one workspace's process environment is
  readable by every workspace of the same user — while upstream credentials stay in the gateway and never enter a
  workspace.
- **Owners.** [04_sandbox.md §Process inspection](./04_sandbox.md#process-inspection);
  [00_overview.md §Goals](./00_overview.md#goals);
  [docs/gateway.md §Client wiring (files, not hand-written env)](../../packages/cowshed/docs/gateway.md#client-wiring-files-not-hand-written-env)
  (the workspace token a sibling reads this way is worth nothing there). Code:
  [`cowshed-core/src/runtime/supervisor.rs`] `gateway_proxy_url`.

### Mach services, OpenDirectory and keychains are denied

- **Fact.** Seatbelt gates Mach lookups by service name: OpenDirectory's `libinfo` service answers `pwd.h` and `grp.h`
  lookups while its API and membership services stay denied, `com.apple.FSEvents` delivers file-system events, and user
  keychains live under `~/Library/Keychains` inside HOME (_owner text_).
- **Rule.** The baseline admits only the `libinfo` and `FSEvents` lookups and denies `~/Library/Keychains` with the
  other secret paths, so a job resolves users and watches files but reads no user keychain and gains no keychain access.
- **Owners.** [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [04_sandbox.md §Escape tests](./04_sandbox.md#escape-tests). Code: [`cowshed-core/src/sandbox.rs`] `seatbelt_profile`,
  `hard_denies`.

### `/dev/stdout` is a link into `/dev/fd`

- **Fact.** `/dev/stdin`, `/dev/stdout` and `/dev/stderr` are links into `/dev/fd` and Seatbelt matches the resolved
  spelling, so a literal write rule on `/dev/stdout` never admits `echo x > /dev/stdout` (_measured_), and `fstat` of a
  write-only descriptor is `file-read-metadata`, so a runtime that `fstat`s its stdio before running a line, Node,
  aborts without a message when that is refused (_owner text_).
- **Rule.** The profile admits `file-read-metadata` on `/dev/null` and the three link names and read-write on the
  `/dev/fd` subtree, which reaches nothing the child could not already access.
- **Owners.** [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile). Code:
  [`cowshed-core/src/sandbox.rs`] `every_role_opens_stdio_through_the_descriptor_directory`. See also
  [`/tmp` and `/var` are symlinks into `/private`](#tmp-and-var-are-symlinks-into-private).

### A sandboxed process cannot apply a profile

- **Fact.** A process inside a `deny default` Seatbelt profile, as every cowshed job is, cannot apply a profile of its
  own (`sandbox_apply` fails `EPERM`), and an enclosing profile cannot grant an inner supervisor independent authority
  (_owner text_).
- **Rule.** A `host_controller_` fixture carries an ignore reason, runs only from an unsandboxed host-controller shell
  through `nx run cowshed:host-controller-test`, and ordinary sandboxed runs report it as ignored.
- **Owners.** [08_testing.md §Integration tests (real substrate)](./08_testing.md#integration-tests-real-substrate);
  [08_testing.md §Host-controller authority](./08_testing.md#host-controller-authority). Code:
  [`cowshed-core/src/sandbox.rs`] `host_controller_seatbelt_runs_process_status_and_no_other_set_user_id_program`.

### SBPL accepts less than it seems to

- **Fact.** SBPL network filters take only `localhost` or `*` host literals and reject port ranges, a `path-prefix` is a
  plain string prefix (`<dir>` admits `<dir>-sibling/x.sock`), and the regex engine does not honor `{n}` intervals
  (`[A-Za-z0-9]{2}` compiles and does not match `ab`) (_measured_).
- **Rule.** A port block is emitted as one literal allow per port, every `path-prefix` ends in `/`, and a fixture-root
  pattern spells a repeated character class twice.
- **Owners.** [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [04_sandbox.md §Fixture roots under /tmp](./04_sandbox.md#fixture-roots-under-tmp);
  [04_sandbox.md §Sandbox-denial evidence](./04_sandbox.md#sandbox-denial-evidence);
  [08_testing.md §Unit tests (pure, all platforms)](./08_testing.md#unit-tests-pure-all-platforms) (port-block rule
  generation). Code: [`cowshed-core/src/sandbox.rs`] `seatbelt_profile`, `push_fixture_root_rules`.

### A Seatbelt denial is `EPERM`; the log is closed to non-admins

- **Fact.** A Seatbelt denial surfaces as `EPERM` on `open` and `connect`, distinguishable from an allowed but unserved
  connection (`ECONNREFUSED`), and `log show` and `log stream` need an administrator (_measured_); `sandbox-exec` exits
  71 both when it cannot exec the program and when the child exits 71 (_owner text_).
- **Rule.** Exit 6 / `SandboxDenied` may be produced only from authoritative evidence — pre-spawn validation, a
  profile-application failure, a gateway 403 or an `EPERM` from a known syscall and target — never from scanning output
  text or the unified log, and a failed start is judged by the supervisor's own lookup before the spawn, never by
  `sandbox-exec`'s exit 71.
- **Owners.** [04_sandbox.md §Sandbox-denial evidence](./04_sandbox.md#sandbox-denial-evidence);
  [11_shell.md §Shell activation and process reuse](./11_shell.md#shell-activation-and-process-reuse), “A command that
  cannot start is named” (the exit 71 ambiguity). Code: [`cowshed-core/src/runtime/command_start.rs`].

### SIP strips `DYLD_*` from launchers

- **Fact.** `DYLD_INSERT_LIBRARIES` is stripped from the children of every SIP launcher (`/bin/sh`, `/usr/bin/env`) and
  ignored by SIP-protected and hardened-runtime binaries (`dyld(1)`; _measured_).
- **Rule.** Port-zero redirection rides each runtime's own option — `NODE_OPTIONS=--require=…`,
  `BUN_OPTIONS=--preload=…`, which every launcher passes on — and never `DYLD_INSERT_LIBRARIES`, which would cover some
  processes and not others.
- **Owners.**
  [04_sandbox.md §Trusted supervisor and child profiles](./04_sandbox.md#trusted-supervisor-and-child-profiles), “A
  sandboxed job binds its listeners in its block”. Code: [`cowshed-core/src/port_zero.rs`] `loader_options`;
  [`cowshed-core/src/port_zero.cjs`].

## Processes and descriptors

### `posix_spawnp` leaves transient descriptor copies

- **Fact.** Libc's `posix_spawnp` calls `posix_spawn` once per `PATH` entry it tries
  ([`sys/posix_spawn.c`](https://github.com/apple-oss-distributions/Libc/blob/main/sys/posix_spawn.c)), the kernel makes
  a child for every try before it looks the program up, and a try that fails holds copies of the spawner's descriptors
  until its own exit, after the call has returned (XNU `fdt_fork`; _owner text_).
- **Rule.** A release is judged by a waiter's grant, never by the spawn's return: the descriptors whose release a peer
  observes (the supervisor's and the gateway's listeners and leases, the image lease and pin, the lifecycle locks) are
  fenced from creation by a process-wide fork lock that their close takes exclusively and that the spawns of
  cowshed-core and the gateway hold shared, and only a descriptor handed across `exec` on purpose keeps custody.
- **Owners.** [11_shell.md §Protocol](./11_shell.md#protocol);
  [02_workspaces.md §Held locks: `cowshed lock` and `cowshed lock-holders`](./02_workspaces.md#held-locks-cowshed-lock-and-cowshed-lock-holders);
  [docs/disk-image-lifetime.md §Apple's expectations and close ownership](../../packages/cowshed/docs/disk-image-lifetime.md#apples-expectations-and-close-ownership)
  (the raw-volume pin). Code: [`cowshed-os/src/fork_lock.rs`]; [`cowshed-core/clippy.toml`]. See also
  [Darwin has no `pipe2` and no `SOCK_CLOEXEC`](#darwin-has-no-pipe2-and-no-sock_cloexec),
  [`copyfile(3)` opens its descriptors without `O_CLOEXEC`](#copyfile3-opens-its-descriptors-without-o_cloexec).

### Darwin has no `pipe2` and no `SOCK_CLOEXEC`

- **Fact.** The macOS SDK declares neither `pipe2` nor `SOCK_CLOEXEC` (`<unistd.h>`, `<sys/socket.h>`), so a pipe or
  socket cannot be made close-on-exec in the call that creates it, and a child forked between `socket` and `fcntl` keeps
  it for life.
- **Rule.** A shell-hosted job's pipes, the supervisor's and the gateway's listeners and the disk lease's client socket
  are created and marked close-on-exec under the exclusive fork lock (`fork_lock::create`), so no concurrent spawn forks
  between the two steps.
- **Owners.** [11_shell.md §Protocol](./11_shell.md#protocol);
  [05_gateway.md §Disk-lifecycle lease](./05_gateway.md#disk-lifecycle-lease) (the connector's descriptor);
  [08_testing.md §Fixtures](./08_testing.md#fixtures) (fixture listeners). Code: [`cowshed-os/src/fork_lock.rs`]
  `create`; [`cowshed-apfs/src/disk_lease.rs`]. See also
  [`posix_spawnp` leaves transient descriptor copies](#posix_spawnp-leaves-transient-descriptor-copies),
  [`copyfile(3)` opens its descriptors without `O_CLOEXEC`](#copyfile3-opens-its-descriptors-without-o_cloexec).

### `copyfile(3)` opens its descriptors without `O_CLOEXEC`

- **Fact.** `copyfile(3)` given paths opens its source and destination without `O_CLOEXEC` and holds them for the whole
  call, so a child spawned meanwhile keeps them past its `exec` for its whole life (code comment, with its measurement).
- **Rule.** Every copy names descriptors this process opened close-on-exec and goes through `fcopyfile(3)`, only a clone
  names paths because `COPYFILE_CLONE_FORCE` is one `clonefileat(2)` that opens none, and the crate's clippy
  configuration refuses `libc::copyfile`.
- **Owners.** _No spec owner_: the module comment and the clippy reason state the rule. Code:
  [`cowshed-core/src/copy/native/macos.rs`]; [`cowshed-core/clippy.toml`]. See also
  [`posix_spawnp` leaves transient descriptor copies](#posix_spawnp-leaves-transient-descriptor-copies),
  [Darwin has no `pipe2` and no `SOCK_CLOEXEC`](#darwin-has-no-pipe2-and-no-sock_cloexec).

### A process is its pid and its unique id

- **Fact.** Darwin gives each process life a unique id (`p_uniqueid`, read through `proc_info` flavors that no public
  SDK header declares) from a 64-bit counter that XNU never repeats while the system runs (`forkproc`), and
  `proc_signal_with_audittoken` (`<libproc.h>`) names a life by the pid and pid version of an audit token, a version
  that `exec` increments (`<EndpointSecurity/ESMessageCore.h>`), so a signal under a stale one answers `ESRCH` (code
  comment; _measured_).
- **Rule.** A lease, a held lock, an `fd-holders` row and the supervisor's ledger name a life by its pid together with
  its unique id (the ledger adds `kern.bootsessionuuid`), and `signal_life` signals a recorded life through the pid
  version read in the same call as its unique id, only while that unique id still matches.
- **Owners.**
  [11_shell.md §Scoped descendant retirement capabilities](./11_shell.md#scoped-descendant-retirement-capabilities);
  [11_shell.md §Supervisor recovery](./11_shell.md#supervisor-recovery);
  [02_workspaces.md §Holders cowshed does not run](./02_workspaces.md#holders-cowshed-does-not-run);
  [02_workspaces.md §Descriptor holders: `cowshed fd-holders`](./02_workspaces.md#descriptor-holders-cowshed-fd-holders).
  Code: [`cowshed-core/src/runtime/job_groups.rs`] `process_record`, `signal_life`;
  [`cowshed-core/src/runtime/process_tree.rs`] `BirthToken`; [`cowshed-core/src/lease_holder.rs`].

### A stopped child is reported to `waitid`

- **Fact.** Darwin reports a stopped child to a `waitid` that asked only for exits (_measured_ on Darwin 25.6).
- **Rule.** The non-reaping exit read (`WEXITED | WNOWAIT`) takes a stop report with a wait for `WSTOPPED | WCONTINUED`
  alone, never `WEXITED`, and reads the exit again, so a stop is never read as an exit and the leader stays unreaped.
- **Owners.** [11_shell.md §Shell activation and process reuse](./11_shell.md#shell-activation-and-process-reuse),
  “Process identity and signal ownership”. Code: [`cowshed-core/src/runtime/job_groups.rs`] `take_stop_report`,
  `read_exit`; [`cowshed-core/src/runtime/child_fence.rs`].

### Process groups race their creators and their signals

- **Fact.** Two creations of one process group race and macOS refuses the loser with `EPERM`, and a group signal does
  not hold a fork back, so a member forked while it goes out survives it, and so does the one that member forks next
  (_owner text_).
- **Rule.** One child creates and leads a script job's group and reports once it exists, and a kill that must leave
  nothing behind is repeated while the kernel still lists members of the group.
- **Owners.** [11_shell.md §Shell activation and process reuse](./11_shell.md#shell-activation-and-process-reuse),
  “Script jobs”;
  [packages/nx-plugin/README.md §Bounded Test Targets](../../packages/nx-plugin/README.md#bounded-test-targets) (the
  repeated kill). Code: [`cowshed-shell/src/script.rs`] `child`; [`cowshed-core/src/runtime/job_groups.rs`]
  `kill_owned`; [`packages/nx-plugin/src/process-group.ts`] `judgeReap`.

### kqueue cannot name a child

- **Fact.** kqueue `NOTE_FORK` coalesces and carries no child pid, `NOTE_TRACK` and `NOTE_CHILD` are refused with
  `ENOTSUP`, `NOTE_EXITSTATUS` is refused with `EACCES` for a life that is neither the supervisor's child nor one it may
  `SIGKILL` (xnu `filt_procattach`, `filt_procevent` and the `NOTE_FORK` comment in `bsd/sys/event.h`, as the owner
  links), and Endpoint Security needs an Apple entitlement (`<EndpointSecurity/ESClient.h>`; _owner text_).
- **Rule.** The supervisor answers each `NOTE_FORK` by listing the member's children with `proc_listchildpids` and
  watching each, watches a life that refuses `NOTE_EXITSTATUS` without it, uses no Endpoint Security observer, and never
  claims `Complete` for a macOS tree in which a member forked.
- **Owners.** [07_api.md §Process-tree observations](./07_api.md#process-tree-observations);
  [11_shell.md §Complete process tree, compact observations](./11_shell.md#complete-process-tree-compact-observations).
  Code: [`cowshed-core/src/runtime/process_events.rs`]; [`cowshed-core/src/runtime/daemon_watch.rs`].

### libproc describes this user's processes; there is no `/proc`

- **Fact.** macOS has no procfs: process facts come from libproc (`proc_listallpids`, `proc_pidinfo`), which lists every
  user's processes but lists another user's descriptors only to root; `ps -E` prints the start environment of this
  user's processes only, and the kernel shows an Apple platform program's start environment to that program alone
  (_owner text_).
- **Rule.** The `fd-holders` census counts a process whose descriptors the kernel withholds as `uninspected`, never as
  holding nothing.
- **Owners.**
  [02_workspaces.md §Descriptor holders: `cowshed fd-holders`](./02_workspaces.md#descriptor-holders-cowshed-fd-holders);
  [02_workspaces.md §Holders cowshed does not run](./02_workspaces.md#holders-cowshed-does-not-run) (the tree and volume
  censuses, which see this user's processes only);
  [04_sandbox.md §Process inspection](./04_sandbox.md#process-inspection). Code: [`cowshed-core/src/build_volume/nx.rs`]
  `object_holders`; [`cowshed-os/src/process.rs`] `Unread`, `descriptor_names`;
  [`cowshed-apfs/src/disk_image_helpers.rs`].

### `proc_pid_rusage` counters can fall between reads

- **Fact.** A `proc_pid_rusage` CPU counter can fall between two reads (a job leader's own user CPU reads 239 µs, then
  222 µs; _measured_), and macOS job CPU is reconciled against the leader's own plus reaped children's rusage, which
  misses running, unreaped and orphaned descendants (_owner text_).
- **Rule.** The leader source and each process row hold every CPU counter at the highest value read, so a cumulative
  total never falls and no window counts negative CPU, and reconciliation names the unattributed remainder.
- **Owners.**
  [07_api.md §Complete job accounting and observation reconciliation](./07_api.md#complete-job-accounting-and-observation-reconciliation);
  [13_telemetry.md §Process-tree spans](./13_telemetry.md#process-tree-spans) (the `job.unattributed` row). Code:
  [`cowshed-core/src/runtime/job_accounting.rs`]; [`cowshed-core/src/runtime/process_usage.rs`];
  [`cowshed-core/src/runtime/job_unattributed.rs`].

### A peer `shutdown(2)` can lose a sleeping reader's wakeup

- **Fact.** Darwin can lose a peer `shutdown(2)` wakeup for a reader already asleep in `recvmsg`: a C model of eight
  socket pairs wedges within 50–750 trials of eight teardowns that end with `shutdown` and never wedges in 3,000 trials
  whose peer closes instead (_measured_).
- **Rule.** A fixture ends an exec host as a supervisor does, by closing the control socket's last outside descriptor,
  and never `shutdown(2)`s it while holding it.
- **Owners.** [08_testing.md §Integration tests (real substrate)](./08_testing.md#integration-tests-real-substrate),
  “Script-host fixture wedge”;
  [11_shell.md §Shell activation and process reuse](./11_shell.md#shell-activation-and-process-reuse) (controller EOF).
  Code: [`cowshed-shell/tests/script_host.rs`] `close_control`.

## Network

### The ephemeral port range is host-global

- **Fact.** macOS has no per-process network namespace, so the default ephemeral range,
  `net.inet.ip.portrange.first`–`last`, 49152–65535 (`<netinet/in.h>` `IPPORT_HIFIRSTAUTO`; `sysctl`), is shared by
  every process, no Seatbelt filter names a port a same-sandbox process bound, and a restricted `network-bind` fails
  `bind(0)` with `EPERM` instead of steering it (_measured_).
- **Rule.** Every workspace gets a power-of-two port block carved from 32768–49151, below the ephemeral floor so no
  outbound connection's local port falls inside it, its profile admits outbound connects to its own ports only unless it
  is granted `--loopback-any`, and Node and Bun map port-0 listeners into the block through a `--require` or `--preload`
  shim.
- **Owners.** [05_gateway.md §Placement and identity](./05_gateway.md#placement-and-identity);
  [04_sandbox.md §Trusted supervisor and child profiles](./04_sandbox.md#trusted-supervisor-and-child-profiles), “A
  sandboxed job binds its listeners in its block”;
  [04_sandbox.md §Baseline (closed) profile](./04_sandbox.md#baseline-closed-profile);
  [04_sandbox.md §Grant file: `<image>.grants.json`](./04_sandbox.md#grant-file-imagegrantsjson) (the range ends below
  the ephemeral range);
  [docs/cli.md §Dev servers inside workspaces](../../packages/cowshed/docs/cli.md#dev-servers-inside-workspaces) (the
  port-0 preload). Code: [`cowshed-gateway-types/src/session.rs`] `MACOS_PORT_MIN`, `MACOS_PORT_MAX`;
  [`cowshed-core/src/metadata.rs`] `PortAllocation`; [`cowshed-core/src/port_zero.rs`].

### macOS-native TLS ignores environment trust anchors

- **Fact.** Security.framework tooling and Go, whose darwin path defers to the platform verifier, ignore `SSL_CERT_FILE`
  and the other environment anchors, and rustup's cargo, which verifies through SecureTransport, ignores `SSL_CERT_FILE`
  (_owner text_).
- **Rule.** Cargo gets its own `CARGO_HTTP_CAINFO`, and a client that cannot take an anchor, Go module traffic included,
  is granted `--opaque` so its tunnel presents the real upstream certificate.
- **Owners.** [04_sandbox.md §Egress-interception trust anchors](./04_sandbox.md#egress-interception-trust-anchors);
  [03_caches.md §Wiring](./03_caches.md#wiring);
  [05_gateway.md §Granted egress — intercept (default) or `--opaque`](./05_gateway.md#granted-egress--intercept-default-or---opaque);
  [docs/gateway.md §Client wiring (files, not hand-written env)](../../packages/cowshed/docs/gateway.md#client-wiring-files-not-hand-written-env).
  Code: [`cowshed-core/src/capabilities/cargo.rs`] `CA_ENV`.

## RAM disks

### A `ram://` disk lives in a forked helper process

- **Fact.** `hdiutil attach ram://<sectors>` (a sector is 512 bytes; `hdiutil(1)`) forks a `diskimages-helper` that
  calls `setsid` and serves the disk until detach, a RAM disk larger than memory is swap, its pages return only on
  detach and no TRIM passes through the RAM device (code comment), and a job's retirement ends every member that
  outlives its leader, `setsid` lives included, so a helper a job forked dies with it and the disk goes with it: open
  files read `EIO` (_owner text_).
- **Rule.** Only the gateway, which no job contains, attaches (`hdiutil attach -nomount ram://<n>`, a protected
  disk-image command) and detaches RAM disks, by exact I/O Registry entry and never forced, and it refuses a size larger
  than `hw.memsize`.
- **Owners.** [05_gateway.md §RAM disks](./05_gateway.md#ram-disks);
  [11_shell.md §Shell activation and process reuse](./11_shell.md#shell-activation-and-process-reuse) (retirement);
  [packages/nx-plugin/README.md §Bounded Test Targets](../../packages/nx-plugin/README.md#bounded-test-targets) (the RAM
  temp volume). Code: [`cowshed-apfs/src/ram_disk.rs`] `SystemRamDisks`; [`cowshed-gateway-types/src/ram_disk.rs`]
  `RamAttachRefusal`; [`packages/nx-plugin/src/executors/bounded-exec/ram-attach.ts`] `attachRamDisk`;
  [`packages/nx-plugin/src/executors/bounded-exec/ram-detach.ts`] `detachGatewayRamDisk`.

## Tools and toolchain

### `/usr/bin/rsync` is openrsync

- **Fact.** `rsync(1)` on macOS is `openrsync(1)`, whose itemized output cannot report an unchanged tree as unchanged
  once extended attributes are preserved (code comment), and whose `-a` includes `-D` (`openrsync(1)`), so it recreates
  sockets under the staging mount, which overruns `sun_path` (code comment).
- **Rule.** Adopt copies in-process with `copyfile(3)` — each leaf requests an APFS clone and falls back to a data copy
  on `EXDEV` or `ENOTSUP` — decides quiescence from two `stat` snapshots of the source, and skips sockets, FIFOs and
  device nodes as runtime artifacts.
- **Owners.** _No spec owner_ for the openrsync findings and the socket skip: `copy.rs` states them.
  [02_workspaces.md §`cowshed adopt` — create the main workspace](./02_workspaces.md#cowshed-adopt--create-the-main-workspace)
  (step 3). Code: [`cowshed-core/src/copy.rs`]; [`cowshed-core/src/copy/native/macos.rs`]. See also
  [`sun_path` is 104 bytes](#sun_path-is-104-bytes).

## ZFS on macOS

### OpenZFS on macOS decides per dataset whether to publish a disk

- **Fact.** OpenZFS for macOS mounts a dataset either through an IOKit proxy disk (`/dev/diskN`) or directly by name,
  and four inheritable properties choose: `com.apple.devdisk` (default `poolonly`, a proxy disk only for the pool's root
  dataset), `com.apple.browse` (default `on`, the mount appears in Finder), `com.apple.mimic` (default `off`, the mount
  reports `zfs` rather than `hfs` or `apfs`) and `com.apple.ignoreowner` (default `off`) (declared in OpenZFS's
  `libzfs/sys/fs/zfs.h`; `zfs set` lists their values; the owner reads their behavior from the port's source).
- **Rule.** `<pool>/cowshed` sets `com.apple.devdisk=off`, `com.apple.browse=off`, `com.apple.mimic=off` and
  `com.apple.ignoreowner=off` locally, `store` sets `ignoreowner=on`, every other dataset inherits them except a
  `--browse` workspace's local `com.apple.browse=on`, and `setup` and `doctor` attest the tree with one `zfs get`.
- **Owners.** _No code owner_. [01b_zfs.md §One pool, one root](./01b_zfs.md#one-pool-one-root);
  [01b_zfs.md §No disk per dataset](./01b_zfs.md#no-disk-per-dataset);
  [01b_zfs.md §Inherited, never set per dataset](./01b_zfs.md#inherited-never-set-per-dataset);
  [01b_zfs.md §Mount root and Spotlight](./01b_zfs.md#mount-root-and-spotlight).

[`cowshed-apfs/src/attach_budget.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/attach_budget.rs
[`cowshed-apfs/src/disk_image_helpers.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/disk_image_helpers.rs
[`cowshed-apfs/src/disk_lease.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/disk_lease.rs
[`cowshed-apfs/src/image.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/image.rs
[`cowshed-apfs/src/lib.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/lib.rs
[`cowshed-apfs/src/media_cap.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/media_cap.rs
[`cowshed-apfs/src/mounts.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/mounts.rs
[`cowshed-apfs/src/ram_disk.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/ram_disk.rs
[`cowshed-apfs/src/release_authority.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/release_authority.rs
[`cowshed-apfs/src/template.rs`]: ../../packages/cowshed/crates/cowshed-apfs/src/template.rs
[`cowshed-cli/src/agent_cycle.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/agent_cycle.rs
[`cowshed-cli/src/code_signing.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/code_signing.rs
[`cowshed-cli/src/disk_command.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/disk_command.rs
[`cowshed-cli/src/first_launch.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/first_launch.rs
[`cowshed-cli/src/gateway_host.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/gateway_host.rs
[`cowshed-cli/src/gateway_service.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/gateway_service.rs
[`cowshed-cli/src/launchd.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/launchd.rs
[`cowshed-cli/src/runtime.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/runtime.rs
[`cowshed-cli/src/setup_service.rs`]: ../../packages/cowshed/crates/cowshed-cli/src/setup_service.rs
[`cowshed-core/clippy.toml`]: ../../packages/cowshed/crates/cowshed-core/clippy.toml
[`cowshed-core/src/build_volume/nx.rs`]: ../../packages/cowshed/crates/cowshed-core/src/build_volume/nx.rs
[`cowshed-core/src/capabilities/cargo.rs`]: ../../packages/cowshed/crates/cowshed-core/src/capabilities/cargo.rs
[`cowshed-core/src/copy.rs`]: ../../packages/cowshed/crates/cowshed-core/src/copy.rs
[`cowshed-core/src/copy/native/macos.rs`]: ../../packages/cowshed/crates/cowshed-core/src/copy/native/macos.rs
[`cowshed-core/src/error.rs`]: ../../packages/cowshed/crates/cowshed-core/src/error.rs
[`cowshed-core/src/exec.rs`]: ../../packages/cowshed/crates/cowshed-core/src/exec.rs
[`cowshed-core/src/git.rs`]: ../../packages/cowshed/crates/cowshed-core/src/git.rs
[`cowshed-core/src/lease_holder.rs`]: ../../packages/cowshed/crates/cowshed-core/src/lease_holder.rs
[`cowshed-core/src/media_room.rs`]: ../../packages/cowshed/crates/cowshed-core/src/media_room.rs
[`cowshed-core/src/metadata.rs`]: ../../packages/cowshed/crates/cowshed-core/src/metadata.rs
[`cowshed-core/src/port_zero.cjs`]: ../../packages/cowshed/crates/cowshed-core/src/port_zero.cjs
[`cowshed-core/src/port_zero.rs`]: ../../packages/cowshed/crates/cowshed-core/src/port_zero.rs
[`cowshed-core/src/runtime/child_fence.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/child_fence.rs
[`cowshed-core/src/runtime/command_start.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/command_start.rs
[`cowshed-core/src/runtime/daemon_watch.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/daemon_watch.rs
[`cowshed-core/src/runtime/job_accounting.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/job_accounting.rs
[`cowshed-core/src/runtime/job_groups.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/job_groups.rs
[`cowshed-core/src/runtime/job_unattributed.rs`]:
  ../../packages/cowshed/crates/cowshed-core/src/runtime/job_unattributed.rs
[`cowshed-core/src/runtime/process_events.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/process_events.rs
[`cowshed-core/src/runtime/process_tree.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/process_tree.rs
[`cowshed-core/src/runtime/process_usage.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/process_usage.rs
[`cowshed-core/src/runtime/project.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/project.rs
[`cowshed-core/src/runtime/supervisor.rs`]: ../../packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs
[`cowshed-core/src/runtime/supervisor_socket.rs`]:
  ../../packages/cowshed/crates/cowshed-core/src/runtime/supervisor_socket.rs
[`cowshed-core/src/sandbox.rs`]: ../../packages/cowshed/crates/cowshed-core/src/sandbox.rs
[`cowshed-core/src/storage/apfs/extents.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/apfs/extents.rs
[`cowshed-core/src/storage/apfs/native.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs
[`cowshed-core/src/storage/apfs/native/re_adopt.rs`]:
  ../../packages/cowshed/crates/cowshed-core/src/storage/apfs/native/re_adopt.rs
[`cowshed-core/src/storage/apfs/release.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/apfs/release.rs
[`cowshed-core/src/storage/bootstrap/native/macos.rs`]:
  ../../packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/macos.rs
[`cowshed-core/src/storage/bootstrap/native/macos/keychain.rs`]:
  ../../packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/macos/keychain.rs
[`cowshed-core/src/storage/fstab.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/fstab.rs
[`cowshed-core/src/storage/host_config.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/host_config.rs
[`cowshed-core/src/storage/lifecycle.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/lifecycle.rs
[`cowshed-core/src/storage/mod.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/mod.rs
[`cowshed-core/src/storage/recovery.rs`]: ../../packages/cowshed/crates/cowshed-core/src/storage/recovery.rs
[`cowshed-core/src/vnodes.rs`]: ../../packages/cowshed/crates/cowshed-core/src/vnodes.rs
[`cowshed-gateway-types/src/disk_lease.rs`]: ../../packages/cowshed/crates/cowshed-gateway-types/src/disk_lease.rs
[`cowshed-gateway-types/src/lib.rs`]: ../../packages/cowshed/crates/cowshed-gateway-types/src/lib.rs
[`cowshed-gateway-types/src/ram_disk.rs`]: ../../packages/cowshed/crates/cowshed-gateway-types/src/ram_disk.rs
[`cowshed-gateway-types/src/session.rs`]: ../../packages/cowshed/crates/cowshed-gateway-types/src/session.rs
[`cowshed-gateway/src/disk_lease.rs`]: ../../packages/cowshed/crates/cowshed-gateway/src/disk_lease.rs
[`cowshed-gateway/src/platform.rs`]: ../../packages/cowshed/crates/cowshed-gateway/src/platform.rs
[`cowshed-gateway/src/proxy.rs`]: ../../packages/cowshed/crates/cowshed-gateway/src/proxy.rs
[`cowshed-os/src/fork_lock.rs`]: ../../packages/cowshed/crates/cowshed-os/src/fork_lock.rs
[`cowshed-os/src/fsio.rs`]: ../../packages/cowshed/crates/cowshed-os/src/fsio.rs
[`cowshed-os/src/process.rs`]: ../../packages/cowshed/crates/cowshed-os/src/process.rs
[`cowshed-os/src/scratch.rs`]: ../../packages/cowshed/crates/cowshed-os/src/scratch.rs
[`cowshed-shell/src/script.rs`]: ../../packages/cowshed/crates/cowshed-shell/src/script.rs
[`cowshed-shell/tests/script_host.rs`]: ../../packages/cowshed/crates/cowshed-shell/tests/script_host.rs
[`packages/cowshed/scripts/macos-linker.sh`]: ../../packages/cowshed/scripts/macos-linker.sh
[`packages/nx-plugin/src/executors/bounded-exec/ram-attach.ts`]:
  ../../packages/nx-plugin/src/executors/bounded-exec/ram-attach.ts
[`packages/nx-plugin/src/executors/bounded-exec/ram-detach.ts`]:
  ../../packages/nx-plugin/src/executors/bounded-exec/ram-detach.ts
[`packages/nx-plugin/src/executors/bounded-exec/ram-temp.ts`]:
  ../../packages/nx-plugin/src/executors/bounded-exec/ram-temp.ts
[`packages/nx-plugin/src/nx-daemon-watch-stream-host.test.ts`]:
  ../../packages/nx-plugin/src/nx-daemon-watch-stream-host.test.ts
[`packages/nx-plugin/src/process-group.ts`]: ../../packages/nx-plugin/src/process-group.ts
