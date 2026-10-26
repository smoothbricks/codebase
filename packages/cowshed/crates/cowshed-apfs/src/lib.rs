//! macOS APFS disk-image substrate.
//!
//! Every external operation crosses [`CommandRunner`]. Commands are represented
//! as an executable plus an argument vector; this crate never invokes a shell.

pub mod attach_budget;
pub mod device;
pub mod disk_image_helpers;
pub mod disk_lease;
pub mod image;
pub mod media_cap;
pub mod mounts;
#[cfg(target_os = "macos")]
pub mod ram_disk;
pub mod release_authority;
pub mod store_lock;
pub mod template;

use crate::image::ImageCapacity;
use crate::mounts::{NativeUnmountError, PostUnmountFd, UnmountEffect, UnmountMode};
use cowshed_os::deadline::{Bound, LatencyClass};
use cowshed_os::fork_lock::{Fenced, Spawn as _};
use cowshed_os::host_load::HostLoad;
use cowshed_os::namespace::HostNamespace;
pub use cowshed_os::process::{CommandOutput, ProcessStatus};
use cowshed_os::process::{fmt_command, fmt_command_failure, fmt_command_spawn};
use cowshed_types::image::{IMAGE_EXTENSION, is_image_path};
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use crate::device::{DISKUTIL, container_of, identifier_depth};

/// The disk-image tool. A RAW image is a sparse file cowshed creates itself
/// ([`MacOsApfsBackend::create_blank_image`]), attached in the kernel ([`in_kernel_attach`]) and
/// released with `hdiutil detach`. Which images are attached is read from the kernel
/// ([`CommandRunner::attached_disk_images`]), never from `hdiutil info`.
pub(crate) const HDIUTIL: &str = "/usr/bin/hdiutil";

/// The flags of every runtime image attach (01a_apfs.md, "Images"): `-kernel` serves the RAW image from
/// the kernel's own file backing store (`IOHDIXHDDriveInKernel`), with no userspace image helper
/// to stall, orphan or leave a notification undrained (01a_apfs.md, "The IOMedia
/// match-notification ceiling"); `-nomount`, since cowshed mounts the exact verified volume itself;
/// `-noverify`, since a RAW image carries no checksum; `-nobrowse`; and the RAW image class.
const IN_KERNEL_ATTACH: [&str; 7] = [
    "-kernel",
    "-nomount",
    "-noverify",
    "-nobrowse",
    "-imagekey",
    "diskimage-class=CRawDiskImage",
    "-plist",
];

/// `hdiutil attach` of `image` in the kernel, read-only when `access` asks for it.
fn in_kernel_attach(image: &Path, access: MountAccess) -> CommandRequest {
    CommandRequest::new(
        HDIUTIL,
        std::iter::once(OsString::from("attach"))
            .chain((access == MountAccess::ReadOnly).then(|| OsString::from("-readonly")))
            .chain(IN_KERNEL_ATTACH.map(OsString::from))
            .chain([image.as_os_str().to_owned()]),
    )
}

const FSCK_APFS: &str = "/sbin/fsck_apfs";
/// The kernel mount helper. Workspace volumes are mounted with it rather than `diskutil mount`
/// because `diskutil` routes the mount through Disk Arbitration, which serialises every client
/// on the host: under a loaded fleet a mount that `mount_apfs` completes in about a second was
/// measured queueing for 68 s at the median and past the 120 s child deadline at the tail.
/// Disk Arbitration still observes the mounted volume, so eject and inventory keep working.
pub(crate) const MOUNT_APFS: &str = "/sbin/mount_apfs";
pub(crate) const NEWFS_APFS: &str =
    "/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs";

/// Unix `st_blocks` units: 512 bytes on Darwin. GC allocated-byte accounting reads them.
pub const SECTOR_BYTES: u64 = 512;

/// `diskutil apfs resizeContainer <ref> 0` asks for grow-to-fit, and macOS reports "there is
/// nothing to grow into" as a failure rather than a no-op: -69743 when the computed size equals
/// the current one, -69519 when no gap follows the physical store. Both say the container
/// already spans the whole image, which is exactly the requested post-condition, so both are
/// accepted. Every other failure propagates, and the caller still reads the resulting capacity
/// back out of the attachment inventory.
const CONTAINER_ALREADY_SPANS_IMAGE: [&str; 2] = ["Error: -69743:", "Error: -69519:"];

/// Where a disk child's standard input comes from. A parameter of the run, not of the
/// [`CommandRequest`]: only [`SystemCommandRunner::run_with_inherited_input`] and
/// [`SystemCommandRunner::run_with_input`] ask for anything but `/dev/null`.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ChildInput {
    /// `/dev/null`: every command cowshed runs on its own behalf.
    Null,
    /// This process's own standard input, for `cowshed disk-command`, whose caller may pipe a
    /// passphrase to `diskutil apfs unlockVolume … -stdinpassphrase`.
    Inherited,
    /// These bytes, written whole and then closed, for a host connection's `host.diskCommand`,
    /// whose own standard input is its protocol socket and never a child's.
    Bytes(Arc<Vec<u8>>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRequest {
    pub program: PathBuf,
    pub args: Vec<OsString>,
}

impl CommandRequest {
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }
}

/// Whether `request`, exactly `hdiutil detach <device>`, still names the whole disk of the image
/// device registered as `entry` ([`SystemCommandRunner::run_detach_of_entry`]).
fn revalidate_detach(request: &CommandRequest, entry: u64) -> Result<(), CommandRunError> {
    let refused = |change: String| CommandRunError {
        request: request.clone(),
        failure: CommandRunFailure::DeviceChanged(change),
    };
    let device = match request.args.as_slice() {
        [verb, device] if request.program == Path::new(HDIUTIL) && verb == "detach" => {
            let device = device.to_string_lossy();
            match device.strip_prefix("/dev/") {
                Some(_) => device.into_owned(),
                None => format!("/dev/{device}"),
            }
        }
        _ => {
            return Err(refused(
                "only `hdiutil detach <device>` binds to a registry entry".to_owned(),
            ));
        }
    };
    #[cfg(target_os = "macos")]
    let whole = io_registry::whole_disk_of_entry(entry);
    #[cfg(not(target_os = "macos"))]
    let whole: io::Result<Option<String>> = Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image devices are read from the macOS I/O Registry",
    ));
    match whole {
        Ok(Some(whole)) if whole == device => Ok(()),
        Ok(Some(whole)) => Err(refused(format!(
            "registry entry {entry:#x} is now {whole}, not {device}"
        ))),
        Ok(None) => Err(refused(format!(
            "registry entry {entry:#x} no longer has a whole disk ({device} was named)"
        ))),
        Err(error) => Err(refused(format!(
            "registry entry {entry:#x} could not be read ({error})"
        ))),
    }
}

/// A disk tool the `cowshed disk-command` facade runs on a caller's behalf, through
/// [`SystemCommandRunner`] exactly as cowshed runs its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskTool {
    Diskutil,
    Hdiutil,
}

impl DiskTool {
    /// The tool a caller names: `diskutil` or `hdiutil`, never a path.
    pub fn named(name: &str) -> Option<Self> {
        match name {
            "diskutil" => Some(Self::Diskutil),
            "hdiutil" => Some(Self::Hdiutil),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Diskutil => "diskutil",
            Self::Hdiutil => "hdiutil",
        }
    }

    pub fn program(self) -> &'static Path {
        Path::new(match self {
            Self::Diskutil => DISKUTIL,
            Self::Hdiutil => HDIUTIL,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountAccess {
    ReadWrite,
    ReadOnly,
}

impl MountAccess {
    pub fn from_kernel_flags(flags: u64) -> Self {
        if flags & 0x0000_0001 == 0 {
            Self::ReadWrite
        } else {
            Self::ReadOnly
        }
    }
}

/// Why a disk child produced no output to judge.
#[derive(Debug)]
pub enum CommandRunFailure {
    /// The executable could not be started.
    Spawn(io::Error),
    /// The child started, but its exit could not be observed.
    Wait(io::Error),
    /// Refused before spawning: a Disk Arbitration rename.
    VolumeRename,
    /// Refused before spawning: a Disk Arbitration release (`hdiutil detach`, `diskutil eject` or
    /// `unmount`) that would wait on an administrator prompt, or whose disk's owner cannot be read
    /// (release_authority.rs).
    Release(release_authority::ReleaseRefusal),
    /// Refused before spawning: a detach bound to a registry entry
    /// ([`SystemCommandRunner::run_detach_of_entry`]) whose entry no longer names the device, or
    /// could not be read, inside the host disk admission. Nothing was detached.
    DeviceChanged(String),
    /// The total budget expired before the native tool executed.
    Unspawned(Duration),
    /// The deadline expired; the owned group `pid` leads was signalled and the child's actual end
    /// observed by reaping it.
    Deadline {
        deadline: Duration,
        pid: u32,
        ended: ProcessStatus,
    },
    /// Signalling the owned group failed. The subsequent wait's outcome is retained too.
    Kill {
        deadline: Duration,
        pid: u32,
        source: io::Error,
        ended: Result<Option<ProcessStatus>, io::Error>,
    },
    /// The group was signalled, but the child was not reaped: waiting for it failed, or it did
    /// not end within the reap bound, which names its pid and the state the kernel shows it in
    /// (an uninterruptible kernel wait no signal ends).
    Reap {
        deadline: Duration,
        failure: Box<cowshed_os::deadline::ReapError>,
    },
    /// The child ended with `ended`, but its output was not collected: a pipe a process it
    /// started still held at the reap bound, or a read of it failed.
    Output {
        ended: ProcessStatus,
        failure: Box<cowshed_os::deadline::OutputError>,
    },
    /// The child ended with `ended`, but the bytes given as its standard input could not be
    /// written to it for a reason other than the child closing its end.
    Input {
        source: io::Error,
        ended: ProcessStatus,
    },
}

#[derive(Debug)]
pub struct CommandRunError {
    pub request: CommandRequest,
    pub failure: CommandRunFailure,
}

impl From<crate::disk_lease::VolumeRenameRefused> for CommandRunError {
    fn from(refused: crate::disk_lease::VolumeRenameRefused) -> Self {
        Self {
            request: CommandRequest {
                program: refused.program,
                args: refused.args,
            },
            failure: CommandRunFailure::VolumeRename,
        }
    }
}

impl fmt::Display for CommandRunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let program = self.request.program.as_os_str();
        let args = &self.request.args;
        match &self.failure {
            CommandRunFailure::Spawn(source) => fmt_command_spawn(f, program, args, source),
            CommandRunFailure::Wait(source) => {
                f.write_str("could not wait for ")?;
                fmt_command(f, program, args)?;
                write!(f, ": {source}")
            }
            CommandRunFailure::Unspawned(deadline) => {
                fmt_command(f, program, args)?;
                write!(
                    f,
                    " exhausted its total {deadline:?} deadline before native execution; no disk tool was started"
                )
            }
            CommandRunFailure::Deadline {
                deadline,
                pid,
                ended,
            } => {
                fmt_command(f, program, args)?;
                write!(
                    f,
                    " did not finish within {deadline:?}; the owned process group {pid} was signalled and the child reaped with {ended}; its item deferred for the next pass"
                )
            }
            CommandRunFailure::Kill {
                deadline,
                pid,
                source,
                ended,
            } => {
                fmt_command(f, program, args)?;
                write!(
                    f,
                    " did not finish within {deadline:?}; could not kill owned process group {pid}: {source}; "
                )?;
                match ended {
                    Ok(Some(status)) => write!(f, "the child was reaped with {status}"),
                    Ok(None) => write!(
                        f,
                        "the child is still running as pid {pid}; it was not reaped"
                    ),
                    Err(error) => write!(f, "could not observe or reap the child: {error}"),
                }
            }
            CommandRunFailure::Reap { deadline, failure } => {
                fmt_command(f, program, args)?;
                write!(
                    f,
                    " did not finish within {deadline:?}; the owned process group was signalled, but could not reap the child: {failure}"
                )
            }
            CommandRunFailure::Output { ended, failure } => {
                fmt_command(f, program, args)?;
                write!(
                    f,
                    " ended with {ended}, but its output was not collected: {failure}"
                )
            }
            CommandRunFailure::Input { source, ended } => {
                fmt_command(f, program, args)?;
                write!(
                    f,
                    " ended with {ended}, but its standard input could not be written: {source}"
                )
            }
            CommandRunFailure::VolumeRename => {
                f.write_str("refused ")?;
                fmt_command(f, program, args)?;
                write!(f, ": {}", crate::disk_lease::VOLUME_RENAME_REFUSAL)
            }
            CommandRunFailure::Release(refusal) => {
                f.write_str("refused ")?;
                fmt_command(f, program, args)?;
                write!(f, ": {refusal}")
            }
            CommandRunFailure::DeviceChanged(change) => {
                f.write_str("refused ")?;
                fmt_command(f, program, args)?;
                write!(f, ": {change}; nothing was detached")
            }
        }
    }
}

impl std::error::Error for CommandRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.failure {
            CommandRunFailure::Spawn(source)
            | CommandRunFailure::Wait(source)
            | CommandRunFailure::Kill { source, .. }
            | CommandRunFailure::Input { source, .. } => Some(source),
            CommandRunFailure::Deadline { .. }
            | CommandRunFailure::Unspawned(_)
            | CommandRunFailure::VolumeRename
            | CommandRunFailure::DeviceChanged(_) => None,
            CommandRunFailure::Release(refusal) => Some(refusal),
            CommandRunFailure::Reap { failure, .. } => Some(failure),
            CommandRunFailure::Output { failure, .. } => Some(failure),
        }
    }
}

pub trait CommandRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError>;
    /// The uid Disk Arbitration sees as this runner's caller (release_authority.rs).
    fn caller_uid(&self) -> libc::uid_t {
        current_euid()
    }
    /// Acquire the command's host disk lease, then check its authority immediately before
    /// spawning. `false` means no child is needed. Every runner opts in explicitly: recording
    /// runners check before recording; production checks inside the granted lifecycle phase.
    fn run_guarded<E>(
        &self,
        request: &CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<CommandOutput>, E>
    where
        E: From<CommandRunError> + fmt::Display;
    /// Hold `identity`'s image lease while that image's device mapping is read and used.
    ///
    /// `identity` is the absolute path produced by `attachment_inventory_path`, the same value
    /// matched against the kernel's `DiskImageURL` backing-file path for every positive
    /// image-to-device mapping. The lease hashes that same normalized path, not a separately
    /// resolved alias. Every cooperating create, attach, format, fsck, mount, unmount, recovery
    /// and detach of one image holds this lease; different image identities overlap.
    ///
    /// The scope is sound because a device is only ever acted on after the inventory positively
    /// maps `identity` to it under this lease (or while a raw pin holds it), and a device only
    /// becomes free for another image when its record is detached — which a cooperating process
    /// does only under the same lease. No same-user cowshed process can therefore recycle the
    /// device inside the critical section. An alias with another identity cannot authorize use
    /// of this record: the kernel mapping must match the lease identity, not merely name the
    /// same inode. The real overlap regression exercises symlinked-parent and hard-link aliases
    /// against that kernel authority while the registered image's lease remains held.
    /// What the lease does not exclude is a non-cooperating eject (another user, a manual
    /// `hdiutil`) inside the two unpinned mutations, `newfs_apfs` and `hdiutil detach`: a raw pin
    /// makes both fail with EBUSY, so neither can run pinned.
    ///
    /// Recording runners must explicitly opt out; production cannot silently omit the lease.
    /// The lease is fenced (`fork_lock`): once it is dropped, no child this process is still
    /// spawning holds it, so the next holder takes it at once.
    fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>>;
    /// Open the raw volume before resolving its image identity. On macOS the open device
    /// prevents even a forced image eject until the descriptor closes. Recording runners
    /// explicitly opt out; production must not fall back to an unpinned pathname. The pin is
    /// fenced like the lease: once it is dropped, an eject is no longer refused.
    fn pin_raw_device(&self, device: &Path) -> io::Result<Option<Fenced<File>>>;
    /// Hold `mount_point` open and say what the kernel has mounted there ([`mounts::hold_mount`]).
    /// A rename proves its volume's root through this ([`OwnedVolumeRoot`]) and names the volume
    /// through the same descriptor. The default reads the kernel, so a recording runner that does
    /// not answer from its script refuses every fake device: it fails closed, never into a rename.
    fn hold_mount(&self, mount_point: &Path) -> io::Result<mounts::HeldMount> {
        mounts::hold_mount(mount_point)
    }
    /// Name the owned volume `root` holds ([`OwnedVolumeRoot::set_name`]): the one way any code
    /// names a volume. A recording runner records it; a stated root it did not override names
    /// nothing and fails.
    fn name_volume(&self, root: &OwnedVolumeRoot, volume_name: &str) -> io::Result<()> {
        root.set_name(volume_name)
    }
    /// The exact pinned volume's current kernel mount, read only inside namespace admission.
    /// Absence is a no-effect result; a mount root is held and proved before any native effect.
    fn volume_mount(&self, volume_device: &str) -> io::Result<Option<mounts::KernelMountSnapshot>> {
        Ok(mounts::kernel_mounts()?
            .into_iter()
            .find(|mount| mount.source_device == volume_device))
    }
    /// Prove the root of one native unmount, then unmount it. The returned root owns the same
    /// descriptor through the syscall and compound postcondition. Recording runners explicitly
    /// record the native operation; an unoverridden stated root has no descriptor and fails
    /// closed. A runner that schedules disk work admits the unmount under its namespace's lease
    /// first ([`SystemCommandRunner`]).
    fn unmount_guarded<E>(
        &self,
        mode: UnmountMode,
        prove: impl FnOnce() -> Result<Option<OwnedVolumeRoot>, E>,
    ) -> Result<UnmountEffect, E>
    where
        E: From<NativeUnmountError> + fmt::Display,
    {
        unmount_proved(mode, prove)
    }
    /// Every disk image the kernel has attached right now, read from the I/O Registry.
    ///
    /// This is the one attachment inventory, and its absences are authoritative. `hdiutil info
    /// -plist` is not: while any other image attaches or detaches it answers a truncated image
    /// list with nothing marking it, and reading that omission as "not attached" skipped a
    /// release that never happened and lost a mounted workspace's identity on restart. The
    /// kernel's matching snapshot is taken under the registry's own consistency check.
    fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>>;
    /// Every attachment the kernel holds for the backing file at `image`, an absolute path:
    /// exactly the entries of [`Self::attached_disk_images`] backed by it, so its absences are as
    /// authoritative. The system runner has the kernel match that image's devices alone, so the
    /// answer costs that image's registry entries however many other images are attached.
    fn attachments_of(&self, image: &Path) -> io::Result<Vec<AttachedDiskImage>> {
        Ok(self
            .attached_disk_images()?
            .into_iter()
            .filter(|attached| attached.is_backed_by(image))
            .collect())
    }
    /// Grow a detached RAW image's virtual disk to `capacity` by extending its file, and nothing
    /// else: the file's length is the virtual capacity, and no header is rewritten.
    ///
    /// `diskutil image resize` is not used: on an image whose container spans the whole device it
    /// also attaches the image outside any image lease, ownership proof or raw pin. The container
    /// inside grows afterwards through cowshed's own verified attachment
    /// ([`ApfsBackend::grow_container`]).
    ///
    /// Recording runners script or refuse it explicitly; production never falls back to diskutil.
    fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()>;
}

/// The bound every spawned disk child (attach/detach, inventory, mount) answers inside.
///
/// Without it a hung `hdiutil`/`diskutil` child wedges the store operation that spawned it
/// forever — one stuck workspace takes down every other workspace's verbs, and the daemon
/// startup pass with them. On expiry a child is killed and the item reports a timeout
/// ("deferred") so the queue continues and the next pass retries it; the per-leg
/// [`timed_apfs_step`] span such a child was stuck inside terminates with `status=err` and
/// the waited elapsed instead of never closing.
///
/// A disk-image command (`diskutil image …`, `hdiutil`) is the exception, and cowshed never kills
/// one: killed mid-operation, its device can stay leaked in the kernel until reboot
/// (01a_apfs.md, "How the APFS host degrades" §2). For it this deadline is a warning point: it
/// is waited for until it ends on its own, still holding its leases, and says what it is waiting
/// on every [`DISK_IMAGE_WARNING_INTERVAL`].
///
/// 120 seconds is generous against normally sub-second disk children and stays inside the
/// enclosing per-operation budgets even when a pass pays it once per workspace; tests
/// override it through [`SystemCommandRunner::run_with_deadline`].
pub const DISK_CHILD_DEADLINE: Duration = Duration::from_secs(120);

/// The wait for an image lease another disk-image command holds (05_gateway.md, "Deadlines"). Its
/// holder holds it for that command's run, so the wait gets the disk child's own deadline,
/// [`DISK_CHILD_DEADLINE`], as class `ffi`: a disk-image command is a call into the system's disk
/// services.
pub const IMAGE_LEASE: Bound = Bound::new(LatencyClass::Ffi, DISK_CHILD_DEADLINE);

/// How often a disk-image command past [`DISK_CHILD_DEADLINE`] says what it is still waiting on.
pub const DISK_IMAGE_WARNING_INTERVAL: Duration = Duration::from_secs(30);

/// One monotonic budget covering admission and the owned SAFE child's lifetime.
/// Constructed before admission (and before the CLI's blocking-runner handoff).
#[derive(Clone, Copy, Debug)]
pub struct CommandDeadline {
    started: std::time::Instant,
    duration: Duration,
}

impl CommandDeadline {
    pub fn new(duration: Duration) -> Self {
        Self {
            started: std::time::Instant::now(),
            duration,
        }
    }

    pub(crate) fn remaining(self) -> Option<Duration> {
        self.duration
            .checked_sub(self.started.elapsed())
            .filter(|remaining| !remaining.is_zero())
    }
}

/// What the waiter does with a disk child still running at its deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PastDeadline {
    /// Kill the child's process group and report [`CommandRunFailure::Deadline`].
    Kill,
    /// Say what is still running this often, and wait until the child ends on its own. A
    /// disk-image command's policy: no deadline of cowshed's own kills one.
    WarnEvery(Duration),
}

/// When a running disk child is warned about, and whether it is ever killed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadlinePolicy {
    pub deadline: Duration,
    pub past_deadline: PastDeadline,
    /// How long the child's exit after SIGKILL, and its output's end after its exit, are waited
    /// for: the reap class (specs/cowshed/05_gateway.md, "Deadlines").
    pub reap: cowshed_os::deadline::Bound,
}

impl DeadlinePolicy {
    /// `request`'s policy at `deadline`: a disk-image command is warned about from `deadline`
    /// every [`DISK_IMAGE_WARNING_INTERVAL`] and never killed; every other disk child is killed
    /// at `deadline`.
    pub fn for_request(request: &CommandRequest, deadline: Duration) -> Self {
        Self {
            deadline,
            past_deadline: if is_disk_image_command(request) {
                PastDeadline::WarnEvery(DISK_IMAGE_WARNING_INTERVAL)
            } else {
                PastDeadline::Kill
            },
            reap: cowshed_os::deadline::REAP,
        }
    }

    /// What the waiter does `elapsed` into the child's run, having warned `warned` times.
    fn step(&self, elapsed: Duration, warned: u32) -> DeadlineStep {
        match self.past_deadline {
            PastDeadline::Kill if elapsed >= self.deadline => DeadlineStep::Kill,
            PastDeadline::WarnEvery(every) if elapsed >= self.deadline + every * warned => {
                DeadlineStep::Warn
            }
            PastDeadline::Kill | PastDeadline::WarnEvery(_) => DeadlineStep::Wait,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeadlineStep {
    Wait,
    Warn,
    Kill,
}

/// Prove the root `prove` names and unmount it (`CommandRunner::unmount_guarded`).
fn unmount_proved<E>(
    mode: UnmountMode,
    prove: impl FnOnce() -> Result<Option<OwnedVolumeRoot>, E>,
) -> Result<UnmountEffect, E>
where
    E: From<NativeUnmountError> + fmt::Display,
{
    let Some(root) = prove()? else {
        return Ok(UnmountEffect::NoEffect);
    };
    cowshed_os::timing::attribute(
        "apfs-command",
        format_args!("unmount verified APFS volume"),
        "filesystem",
        format_args!("{:?}", root.filesystem_id()),
    );
    root.unmount(mode).map_err(E::from)
}

/// The real runner. Every disk tool and native unmount it runs takes its host namespace's
/// disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease"; 01_storage.md, "Host namespace"):
/// production's from production's gateway, a test namespace's from the gateway at that
/// namespace's root, so no test's disk work queues on, or spaces out, production's.
#[derive(Clone, Copy, Debug)]
pub struct SystemCommandRunner {
    namespace: HostNamespace,
}

impl SystemCommandRunner {
    pub fn new(namespace: HostNamespace) -> Self {
        Self { namespace }
    }

    /// Whose disk lease this runner takes.
    pub fn namespace(&self) -> HostNamespace {
        self.namespace
    }

    /// Run a disk command with one total monotonic SAFE budget, including lease admission.
    /// Protected image commands retain their warning-only child policy.
    pub fn run_with_deadline(
        &self,
        request: &CommandRequest,
        deadline: Duration,
    ) -> Result<CommandOutput, CommandRunError> {
        self.run_admitted(
            request,
            CommandDeadline::new(deadline),
            ChildInput::Null,
            |deadline, run| {
                crate::disk_lease::leased(
                    self.namespace,
                    &request.program,
                    &request.args,
                    deadline,
                    run,
                )
            },
        )
    }

    /// `hdiutil detach <device>` of the image device a caller inventoried as registry entry
    /// `entry`, run as [`Self::run_with_inherited_input`] runs a command, except that the device's
    /// identity is read again inside the host disk admission, immediately before the spawn: the
    /// detach runs only while `entry` is still registered with `device` as its whole disk. A name
    /// recycled while the command queued for admission is therefore never detached
    /// ([`CommandRunFailure::DeviceChanged`]). The remaining window is the one any cowshed
    /// release has, from that read to `hdiutil`'s own open of the name, inside the admission.
    pub fn run_detach_of_entry(
        &self,
        request: &CommandRequest,
        entry: u64,
        gateway_socket: Option<&Path>,
    ) -> Result<CommandOutput, CommandRunError> {
        // A detach reads nothing, and a host connection's own input is its protocol socket.
        self.run_admitted(
            request,
            CommandDeadline::new(DISK_CHILD_DEADLINE),
            ChildInput::Null,
            |deadline, run| {
                self.admit(request, gateway_socket, deadline, || {
                    revalidate_detach(request, entry)?;
                    run()
                })
            },
        )
    }

    pub fn run_with_inherited_input(
        &self,
        request: &CommandRequest,
        deadline: CommandDeadline,
        gateway_socket: Option<&Path>,
    ) -> Result<CommandOutput, CommandRunError> {
        self.run_facade(request, deadline, ChildInput::Inherited, gateway_socket)
    }

    /// The facade run with `input` as the child's whole standard input, closed after it: the
    /// same lease, budget and image policy as [`Self::run_with_inherited_input`].
    pub fn run_with_input(
        &self,
        request: &CommandRequest,
        deadline: CommandDeadline,
        input: Vec<u8>,
        gateway_socket: Option<&Path>,
    ) -> Result<CommandOutput, CommandRunError> {
        self.run_facade(
            request,
            deadline,
            ChildInput::Bytes(Arc::new(input)),
            gateway_socket,
        )
    }

    fn run_facade(
        &self,
        request: &CommandRequest,
        deadline: CommandDeadline,
        input: ChildInput,
        gateway_socket: Option<&Path>,
    ) -> Result<CommandOutput, CommandRunError> {
        let deadline = if is_disk_image_command(request) {
            CommandDeadline::new(DISK_CHILD_DEADLINE)
        } else {
            deadline
        };
        self.run_admitted(request, deadline, input, |deadline, run| {
            self.admit(request, gateway_socket, deadline, run)
        })
    }

    fn admit(
        &self,
        request: &CommandRequest,
        gateway_socket: Option<&Path>,
        deadline: Option<CommandDeadline>,
        run: impl FnOnce() -> Result<CommandOutput, CommandRunError>,
    ) -> Result<CommandOutput, CommandRunError> {
        match gateway_socket {
            Some(socket) => {
                crate::disk_lease::leased_on(socket, &request.program, &request.args, deadline, run)
            }
            None => crate::disk_lease::leased(
                self.namespace,
                &request.program,
                &request.args,
                deadline,
                run,
            ),
        }
    }

    /// The same run under custom admission. The callback receives the SAFE budget so a real
    /// test gateway clips its own waits; even arbitrary admission cannot spawn a late child.
    pub fn run_within(
        &self,
        request: &CommandRequest,
        deadline: Duration,
        lease: impl FnOnce(
            Option<CommandDeadline>,
            &mut dyn FnMut() -> Result<CommandOutput, CommandRunError>,
        ) -> Result<CommandOutput, CommandRunError>,
    ) -> Result<CommandOutput, CommandRunError> {
        self.run_admitted(
            request,
            CommandDeadline::new(deadline),
            ChildInput::Null,
            lease,
        )
    }

    /// One runner owns admission, spawn, deadline policy and attach-pool bookkeeping.
    fn run_admitted(
        &self,
        request: &CommandRequest,
        deadline: CommandDeadline,
        input: ChildInput,
        lease: impl FnOnce(
            Option<CommandDeadline>,
            &mut dyn FnMut() -> Result<CommandOutput, CommandRunError>,
        ) -> Result<CommandOutput, CommandRunError>,
    ) -> Result<CommandOutput, CommandRunError> {
        let policy = DeadlinePolicy::for_request(request, deadline.duration);
        let total = matches!(policy.past_deadline, PastDeadline::Kill).then_some(deadline);
        let output = lease(total, &mut || {
            Self::run_unleased(request, input.clone(), policy, total, &mut |elapsed| {
                warn_outlived(request, policy.deadline, elapsed)
            })
        })?;
        Ok(output)
    }

    /// Run `request` reading `input` under `policy`, calling `warn` each time a warning is due.
    /// Every disk child of every entry point spawns here, a caller's own lease
    /// ([`Self::run_within`]) included, so a Disk Arbitration rename is refused here too
    /// ([`crate::disk_lease::refuse_volume_rename`]).
    fn run_unleased(
        request: &CommandRequest,
        input: ChildInput,
        policy: DeadlinePolicy,
        total: Option<CommandDeadline>,
        warn: &mut dyn FnMut(Duration),
    ) -> Result<CommandOutput, CommandRunError> {
        crate::disk_lease::refuse_volume_rename(&request.program, &request.args)?;
        release_authority::refuse_unauthorized_release(request)?;
        if let Some(deadline) = total
            && deadline.remaining().is_none()
        {
            return Err(CommandRunError {
                request: request.clone(),
                failure: CommandRunFailure::Unspawned(deadline.duration),
            });
        }
        let feed = match &input {
            ChildInput::Bytes(bytes) => Some(Arc::clone(bytes)),
            ChildInput::Null | ChildInput::Inherited => None,
        };
        let (mut child, output_files) =
            spawn_disk_child(request, input, total).map_err(|failure| CommandRunError {
                request: request.clone(),
                failure,
            })?;
        // Given bytes are written by this run between its polls, never blocking it, and the pipe is
        // closed once they are written whole, once the child closes its end, or when the run ends
        // whichever way: a descendant that keeps the pipe and never reads holds nothing up.
        let mut feed = feed
            .zip(child.stdin.take())
            .map(|(bytes, stdin)| Feed::new(bytes, stdin));
        // Drain both pipes on dedicated threads while the deadline poll runs, exactly as
        // `Command::output` drains while it waits: a verbose child (fsck) must never wedge
        // on a full pipe just because the parent is polling instead of reading. A disk-image
        // command has no pipes to drain: its output lands in files (spawn_disk_child), so
        // nothing it writes can wedge on a caller that is no longer there.
        let pumps = output_files
            .is_none()
            .then(|| cowshed_os::deadline::OutputPumps::start(&mut child));
        let pid = child.id();
        // Once the child has exited its pipes close with it, unless a process it started still
        // holds one: that is waited for within the reap bound, and named past it.
        let collect = |pumps: Option<cowshed_os::deadline::OutputPumps>| match &output_files {
            Some(directory) => {
                let read = |name: &str| fs::read(directory.join(name)).unwrap_or_default();
                let output = (read("stdout"), read("stderr"));
                let _ = fs::remove_dir_all(directory);
                Ok(output)
            }
            None => pumps.map_or_else(
                || Ok((Vec::new(), Vec::new())),
                |pumps| {
                    pumps.join(
                        DISK_CHILD_ROLE,
                        pid,
                        &cowshed_os::deadline::start("collect disk child output", policy.reap),
                    )
                },
            ),
        };
        let started = total.map_or_else(std::time::Instant::now, |deadline| deadline.started);
        let mut warned = 0;
        loop {
            if let Some(feed) = feed.as_mut() {
                feed.pump();
            }
            match child.try_wait().map_err(|source| CommandRunError {
                request: request.clone(),
                failure: CommandRunFailure::Wait(source),
            })? {
                Some(status) => {
                    // Closed before the output is collected: a descendant that waits for the end
                    // of its input is not left waiting on this run.
                    let input_failure = feed.take().and_then(Feed::close);
                    let collected = collect(pumps);
                    let ended = ProcessStatus::from(status);
                    if let Some(source) = input_failure {
                        if let Err(failure) = &collected {
                            warn_output_failed(request, failure);
                        }
                        return Err(CommandRunError {
                            request: request.clone(),
                            failure: CommandRunFailure::Input { source, ended },
                        });
                    }
                    let (stdout, stderr) = collected.map_err(|failure| CommandRunError {
                        request: request.clone(),
                        failure: CommandRunFailure::Output {
                            ended,
                            failure: Box::new(failure),
                        },
                    })?;
                    let output = std::process::Output {
                        status,
                        stdout,
                        stderr,
                    };
                    return Ok(CommandOutput::from(output));
                }
                None => match policy.step(started.elapsed(), warned) {
                    DeadlineStep::Wait => {
                        let length = total
                            .and_then(CommandDeadline::remaining)
                            .map_or(Duration::from_millis(25), |remaining| {
                                remaining.min(Duration::from_millis(25))
                            });
                        match feed.as_mut() {
                            Some(feed) => feed.wait(length),
                            None => std::thread::sleep(length),
                        }
                    }
                    DeadlineStep::Warn => {
                        warned += 1;
                        warn(started.elapsed());
                    }
                    DeadlineStep::Kill => {
                        // Closed before the group is killed and its output collected, as at a
                        // normal end. The run's failure is its deadline; a feed or output failure
                        // beside it is said here, never dropped.
                        if let Some(failure) = feed.take().and_then(Feed::close) {
                            warn_input_failed(request, &failure);
                        }
                        let failure = match kill_disk_child_group(&mut child) {
                            Ok(()) => match cowshed_os::deadline::reap(
                                DISK_CHILD_ROLE,
                                &mut child,
                                &cowshed_os::deadline::start("reap killed disk child", policy.reap),
                                cowshed_os::deadline::Progress::Signalled {
                                    signal: libc::SIGKILL,
                                },
                            ) {
                                Ok(status) => {
                                    if let Err(failure) = collect(pumps) {
                                        warn_output_failed(request, &failure);
                                    }
                                    CommandRunFailure::Deadline {
                                        deadline: policy.deadline,
                                        pid,
                                        ended: ProcessStatus::from(status),
                                    }
                                }
                                Err(failure) => {
                                    // The child is left as it is, named; its exit is collected
                                    // whenever it comes, and its pumps, dropped here, free what
                                    // they read and drain on without keeping.
                                    cowshed_os::deadline::collect_later(DISK_CHILD_ROLE, child);
                                    CommandRunFailure::Reap {
                                        deadline: policy.deadline,
                                        failure: Box::new(failure),
                                    }
                                }
                            },
                            Err(source) => {
                                let ended = child
                                    .try_wait()
                                    .map(|status| status.map(ProcessStatus::from));
                                CommandRunFailure::Kill {
                                    deadline: policy.deadline,
                                    pid,
                                    source,
                                    ended,
                                }
                            }
                        };
                        return Err(CommandRunError {
                            request: request.clone(),
                            failure,
                        });
                    }
                },
            }
        }
    }
}

/// Say that a disk-image command outlived its deadline and is still waited for, and what the host
/// shows about why ([`disk_image_helpers::DiskImageHost`]): the oldest disk-image command running,
/// which an attach waiting behind a stuck one names, and the helpers serving no device.
///
/// Written without `eprintln!`, which panics when stderr is gone: a caller that gave up and
/// closed this process's stderr (an adapter past its own deadline) must not turn the warning
/// into an unwind that abandons the wait, the lease, and the attach-pool bookkeeping. That
/// caller cannot hear the warning any more; the wait goes on regardless. The line is formatted
/// whole and written once, so it never splices with another process's on the shared stderr.
fn warn_outlived(request: &CommandRequest, deadline: Duration, elapsed: Duration) {
    use std::io::Write;
    let target = request
        .args
        .last()
        .map_or_else(String::new, |target| target.to_string_lossy().into_owned());
    let host = match disk_image_helpers::DiskImageHost::read() {
        Ok(host) => host.to_string(),
        Err(error) => format!("the host's disk-image commands are unreadable: {error}"),
    };
    let line = format!(
        "cowshed: warning: {} on {target} is still running after {}s, past its {}s deadline; \
         a disk-image command killed mid-operation can leak its device until reboot, so cowshed \
         never kills one: it is waited for, holding its leases, until it ends on its own. {host}\n",
        DiskImageVerb(request),
        elapsed.as_secs(),
        deadline.as_secs(),
    );
    let _ = io::stderr().lock().write_all(line.as_bytes());
}

/// The program and verb a disk-image command names: `diskutil image attach`, `diskutil eject`,
/// `hdiutil detach`.
struct DiskImageVerb<'a>(&'a CommandRequest);

impl fmt::Display for DiskImageVerb<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let request = self.0;
        let words = if request.program == Path::new(DISKUTIL)
            && request.args.first().is_some_and(|verb| verb == "image")
        {
            2
        } else {
            1
        };
        write!(f, "{}", request.program.display())?;
        for word in request.args.iter().take(words) {
            write!(f, " {}", word.display())?;
        }
        Ok(())
    }
}

impl CommandRunner for SystemCommandRunner {
    fn unmount_guarded<E>(
        &self,
        mode: UnmountMode,
        prove: impl FnOnce() -> Result<Option<OwnedVolumeRoot>, E>,
    ) -> Result<UnmountEffect, E>
    where
        E: From<NativeUnmountError> + fmt::Display,
    {
        let operation = match mode {
            UnmountMode::Normal => "funmount normal",
            UnmountMode::ForceAfterGrace => "funmount force-after-grace",
        };
        disk_lease::namespace(self.namespace, operation, || unmount_proved(mode, prove))
    }
    fn run_guarded<E>(
        &self,
        request: &CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<CommandOutput>, E>
    where
        E: From<CommandRunError> + fmt::Display,
    {
        /// A guarded run fails by its check's error or by the run's own.
        enum Guarded<E> {
            Check(E),
            Run(CommandRunError),
        }
        impl<E: fmt::Display> fmt::Display for Guarded<E> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Self::Check(error) => error.fmt(f),
                    Self::Run(error) => error.fmt(f),
                }
            }
        }
        impl<E> From<crate::disk_lease::VolumeRenameRefused> for Guarded<E> {
            fn from(refused: crate::disk_lease::VolumeRenameRefused) -> Self {
                Self::Run(refused.into())
            }
        }
        let deadline = CommandDeadline::new(DISK_CHILD_DEADLINE);
        let policy = DeadlinePolicy::for_request(request, DISK_CHILD_DEADLINE);
        let total = matches!(policy.past_deadline, PastDeadline::Kill).then_some(deadline);
        let output = crate::disk_lease::leased(
            self.namespace,
            &request.program,
            &request.args,
            total,
            || {
                if !check().map_err(Guarded::Check)? {
                    return Ok(None);
                }
                Self::run_unleased(request, ChildInput::Null, policy, total, &mut |elapsed| {
                    warn_outlived(request, DISK_CHILD_DEADLINE, elapsed)
                })
                .map(Some)
                .map_err(Guarded::Run)
            },
        )
        .map_err(|error| match error {
            Guarded::Check(error) => error,
            Guarded::Run(error) => E::from(error),
        })?;
        Ok(output)
    }

    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        self.run_with_deadline(request, DISK_CHILD_DEADLINE)
    }

    fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
        let lease = image_lease_path(identity)?;
        let file = open_image_lease(&lease)?;
        acquire_image_lease(file, &lease, identity, IMAGE_LEASE).map(Some)
    }
    fn pin_raw_device(&self, device: &Path) -> io::Result<Option<Fenced<File>>> {
        // A live raw-device descriptor pins this IOMedia, including against forced image detach.
        // Open first, then confirm the image-to-volume mapping while it cannot be recycled.
        let pin = OpenOptions::new().read(true).open(device)?;
        Ok(Some(Fenced::new(pin)))
    }
    fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
        #[cfg(target_os = "macos")]
        {
            io_registry::attached_disk_images()
        }
        #[cfg(not(target_os = "macos"))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "attached disk images are read from the macOS I/O Registry",
            ))
        }
    }
    fn attachments_of(&self, image: &Path) -> io::Result<Vec<AttachedDiskImage>> {
        #[cfg(target_os = "macos")]
        {
            io_registry::attachments_of(image)
        }
        #[cfg(not(target_os = "macos"))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "the attachments of {} are read from the macOS I/O Registry",
                    image.display()
                ),
            ))
        }
    }
    fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            raw::grow(image, capacity)
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (image, capacity);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "RAW disk images exist only on macOS",
            ))
        }
    }
}

/// A RAW image's virtual capacity is its file length. Growth extends the detached sparse file;
/// there is no outer image header or allocation map to decode.
#[cfg(target_os = "macos")]
mod raw {
    use crate::image::ImageCapacity;
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::path::Path;

    const SECTOR_BYTES: u64 = 512;
    const GROW_BLOCK: u64 = 4096;

    fn file_capacity(file: &File) -> io::Result<ImageCapacity> {
        let metadata = file.metadata()?;
        let length = metadata.len();
        if !metadata.is_file() || length == 0 || !length.is_multiple_of(SECTOR_BYTES) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a RAW image must be a nonempty regular file aligned to 512-byte sectors",
            ));
        }
        Ok(ImageCapacity::from_bytes(length))
    }

    pub(super) fn capacity(image: &Path) -> io::Result<ImageCapacity> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(image)?;
        file_capacity(&file)
    }

    /// The nonblocking exclusive open refuses an image a DiskImages2 helper holds and keeps such
    /// an attach excluded until the larger file length is power-loss durable. An in-kernel
    /// attachment takes no lock on its file, so the registry refuses it ([`refuse_attached`]).
    /// Extending a file preserves its holes.
    pub(super) fn grow(image: &Path, capacity: ImageCapacity) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXLOCK | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(image)?;
        refuse_attached(&file, image)?;
        let current = file_capacity(&file)?;
        if !capacity.bytes().is_multiple_of(GROW_BLOCK) || capacity.bytes() <= current.bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{capacity} must exceed {current} and be aligned to {GROW_BLOCK}-byte blocks"
                ),
            ));
        }
        file.set_len(capacity.bytes())?;
        cowshed_os::fsio::Durability::PowerLoss.sync_file(&file)
    }

    /// Refuse (`WouldBlock`, as a held lock does) when a registered in-kernel drive positively
    /// holds the file `file` opened at `image` ([`super::io_registry::kernel_hold`]). The kernel
    /// names each drive's backing file by device and inode, so an alias path, or a path renamed
    /// or unlinked since the attach, is refused as well. A drive that names no file that way is
    /// matched by the path its attach was handed alone and named in a warning; it never refuses
    /// another image. A DiskImages2 attachment is refused by its helper's lock, which the
    /// caller's open meets.
    pub(super) fn refuse_attached(file: &File, image: &Path) -> io::Result<()> {
        use super::io_registry::{KernelHold, in_kernel_backings, kernel_hold};
        let opened = file.metadata()?;
        match kernel_hold(&in_kernel_backings()?, (opened.dev(), opened.ino()), image) {
            KernelHold::Held => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("{} is attached in the kernel", image.display()),
            )),
            KernelHold::Free { unproven } => {
                if !unproven.is_empty() {
                    cowshed_os::eprintln!(
                        "cowshed: warning: {} proceeds unproven against {} in-kernel drive(s) that \
                     name no backing file by device and inode; one is matched only by the path \
                     its attach was handed, so it goes unseen holding this image under a renamed \
                     or alias path, and one that names no path goes unseen holding any image (`ioreg -r -c IOHDIXHDDriveInKernel` lists them): {}",
                        image.display(),
                        unproven.len(),
                        unproven.join("; ")
                    );
                }
                Ok(())
            }
        }
    }
}

/// The lock file of one image identity:
/// `/private/tmp/cowshed-apfs-image-leases-<euid>/<sha256(identity bytes)>.lock`.
///
/// Every process of this user shares the host's device namespace, independent Nx/Nextest
/// workers included, so the lease lives in one host-wide per-user directory rather than in a
/// workspace. The directory must be a real 0700 directory this user owns, so no other user can
/// plant or swap a lock file in it. The digest keeps the name fixed-length whatever the image
/// path. Lock files are never removed: unlinking one another process still has open would split
/// one lease into two.
fn image_lease_path(identity: &Path) -> io::Result<PathBuf> {
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    let uid = unsafe { libc::geteuid() };
    let directory = PathBuf::from(format!("/private/tmp/cowshed-apfs-image-leases-{uid}"));
    private_user_directory(&directory, "APFS image lease directory")?;
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(identity.as_os_str().as_encoded_bytes());
    Ok(directory.join(format!("{digest:x}.lock")))
}

/// Create `directory` 0700 unless it exists, and refuse it unless it is a real directory this
/// user owns with exactly that mode, so no other user can plant or swap a file in it. `what`
/// names it in the refusal.
pub(crate) fn private_user_directory(directory: &Path, what: &str) -> io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};

    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    let uid = unsafe { libc::geteuid() };
    match fs::DirBuilder::new().mode(0o700).create(directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{what} {} is not a private directory owned by this user",
                directory.display()
            ),
        ));
    }
    Ok(())
}

/// Open a lease file without following a link, refusing anything but this user's private
/// regular file.
fn open_image_lease(lease: &Path) -> io::Result<Fenced<File>> {
    open_private_file(lease, "APFS image lease").map(Fenced::new)
}

/// Open, creating it 0600, the file at `path` without following a link, refusing anything but
/// this user's private regular file with one link. `what` names it in the refusal.
fn open_private_file(path: &Path, what: &str) -> io::Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.uid() != uid
        || metadata.mode() & 0o777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{what} {} is not a private regular file owned by this user",
                path.display()
            ),
        ));
    }
    Ok(file)
}

/// Lock `file`, the lease at `lease` for `identity`, within `bound`, and record this process as its
/// holder.
///
/// The wait is [`cowshed_os::flock::exclusive`]'s: at the bound it fails `TIMEOUT` naming the
/// lease, the image and the pid the holder recorded, and a lease its helper takes after the caller
/// left is released as soon as it is taken.
fn acquire_image_lease(
    file: Fenced<File>,
    lease: &Path,
    identity: &Path,
    bound: Bound,
) -> io::Result<Fenced<File>> {
    let deadline = cowshed_os::deadline::start("image-lease", bound);
    let file = cowshed_os::flock::exclusive(file, lease, &deadline, |lease| {
        format!("{} for image {}", lease_holder(lease), identity.display())
    })?;
    record_lease_holder(file)
}

/// Record this process's pid in a lease it now holds, so a waiter that times out can name it.
fn record_lease_holder(file: Fenced<File>) -> io::Result<Fenced<File>> {
    use std::os::unix::fs::FileExt;

    file.set_len(0)?;
    file.write_all_at(format!("{}\n", std::process::id()).as_bytes(), 0)?;
    Ok(file)
}

/// The holder a timed-out wait reports, as the current holder recorded itself.
fn lease_holder(lease: &Path) -> String {
    match fs::read_to_string(lease) {
        Ok(recorded) if recorded.trim().is_empty() => {
            "a holder that has not recorded its pid".into()
        }
        Ok(recorded) => format!("pid {}", recorded.trim()),
        Err(error) => format!("a holder whose record is unreadable ({error})"),
    }
}

/// Spawn a disk child detached into its own session on Unix, so the deadline kill
/// reaps the whole helper tree: a disk-image tool can fork helpers (`hdiutil` forks
/// `diskimages-helper`), and killing only the direct child would leave a helper holding the wedge.
///
/// A disk-image command (`is_disk_image_command`) is detached further, because it is the
/// one child a caller must never interrupt: it starts with the hangup, interrupt and
/// terminate signals a caller's tree-killer addresses to it both ignored and blocked, and
/// writes its output to per-run files under the system temp directory instead of the
/// caller's pipes. Ignoring alone is not enough: `diskutil` installs its own handlers over
/// the inherited SIG_IGN and abandons an attach mid-way on SIGTERM ("diskutil: interrupted",
/// exit 1). It installs them with `signal` and never touches its mask, so a blocked signal
/// stays pending, never delivered, through any handler it installs. A caller killed
/// mid-attach (an agent tool deadline, a process-group kill of `cowshed exec`) then leaves
/// the command running to completion — no full pipe wedges it, no signal reaches it — and
/// the next invocation reconciles the result from the kernel's attachment inventory.
/// Nothing in cowshed kills it either: its deadline only warns ([`PastDeadline::WarnEvery`]).
/// The answered output directory travels with the child; reading it back removes it.
fn spawn_disk_child(
    request: &CommandRequest,
    input: ChildInput,
    total: Option<CommandDeadline>,
) -> Result<(std::process::Child, Option<PathBuf>), CommandRunFailure> {
    spawn_disk_child_as(request, input, is_disk_image_command(request), total).map_err(|source| {
        match total {
            Some(deadline)
                if source.raw_os_error() == Some(libc::ETIMEDOUT)
                    && deadline.remaining().is_none() =>
            {
                CommandRunFailure::Unspawned(deadline.duration)
            }
            _ => CommandRunFailure::Spawn(source),
        }
    })
}

/// Given bytes on their way into a disk child's standard input, owned by the run that spawned it.
/// The pipe is non-blocking, so a write takes what the pipe holds and the run goes on; it is closed
/// once every byte is written, once the child closes its end, or when the run drops it, whatever
/// outcome ends the run.
struct Feed {
    bytes: Arc<Vec<u8>>,
    written: usize,
    stdin: Option<std::process::ChildStdin>,
    failure: Option<io::Error>,
}

impl Feed {
    fn new(bytes: Arc<Vec<u8>>, stdin: std::process::ChildStdin) -> Self {
        let mut feed = Self {
            bytes,
            written: 0,
            stdin: None,
            failure: None,
        };
        match set_nonblocking(&stdin) {
            Ok(()) => {
                feed.stdin = Some(stdin);
                feed.pump();
            }
            Err(error) => feed.failure = Some(error),
        }
        feed
    }

    /// Write what the pipe takes now. A child that closed its end chose not to read; any other
    /// failure closes the pipe and is kept for the run's end.
    fn pump(&mut self) {
        use std::io::Write as _;
        while let Some(stdin) = self.stdin.as_mut() {
            let rest = &self.bytes[self.written..];
            if rest.is_empty() {
                self.stdin = None;
                return;
            }
            match stdin.write(rest) {
                Ok(written) => self.written += written,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::BrokenPipe => self.stdin = None,
                Err(error) => {
                    self.failure = Some(error);
                    self.stdin = None;
                }
            }
        }
    }

    /// Wait `length` before the next poll of the child, waking early once the pipe can take more.
    /// A poll the kernel refuses for any reason but a signal is a failure of the feed, as a refused
    /// write is: kept, and the pipe closed; the wait then takes its whole length.
    fn wait(&mut self, length: Duration) {
        let Some(stdin) = self.stdin.as_ref() else {
            std::thread::sleep(length);
            return;
        };
        let mut ready = libc::pollfd {
            fd: stdin.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        let timeout = libc::c_int::try_from(length.as_millis()).unwrap_or(libc::c_int::MAX);
        // SAFETY: one `pollfd` this frame owns, naming a descriptor the feed owns.
        if unsafe { libc::poll(&mut ready, 1, timeout) } >= 0 {
            return;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            self.failure = Some(error);
            self.stdin = None;
            std::thread::sleep(length);
        }
    }
    /// Close the pipe, unwritten bytes and all, answering a write failure if one was kept.
    fn close(self) -> Option<io::Error> {
        self.failure
    }
}

fn set_nonblocking(stdin: &std::process::ChildStdin) -> io::Result<()> {
    let fd = stdin.as_raw_fd();
    // SAFETY: `fcntl` reads and then sets the status flags of a descriptor this process owns.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    // SAFETY: as above.
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// What a disk child is named as when a wait on it gives up.
const DISK_CHILD_ROLE: &str = "disk child";

/// Say that the output of a command whose run already failed for another reason (its input or
/// its deadline) was not collected either, on the channel the runner says every other warning on.
fn warn_output_failed(request: &CommandRequest, failure: &cowshed_os::deadline::OutputError) {
    use std::io::Write;
    let line = format!(
        "cowshed: warning: the output of {} was not collected: {failure}\\n",
        request.program.display()
    );
    let _ = io::stderr().lock().write_all(line.as_bytes());
}

/// Say that the input given to a command the deadline then killed had failed, on the channel the
/// runner says every other warning on: the run's failure is the deadline, and this is not lost.
fn warn_input_failed(request: &CommandRequest, failure: &io::Error) {
    use std::io::Write;
    let line = format!(
        "cowshed: warning: writing the standard input of {} failed before its deadline: {failure}\n",
        request.program.display()
    );
    let _ = io::stderr().lock().write_all(line.as_bytes());
}

/// `spawn_disk_child` with the detachment decided by the caller, so tests can detach a
/// stand-in child without naming a real disk tool.
fn spawn_disk_child_as(
    request: &CommandRequest,
    input: ChildInput,
    detached: bool,
    total: Option<CommandDeadline>,
) -> io::Result<(std::process::Child, Option<PathBuf>)> {
    let mut command = Command::new(&request.program);
    command.args(&request.args).stdin(match input {
        ChildInput::Null => std::process::Stdio::null(),
        ChildInput::Inherited => std::process::Stdio::inherit(),
        ChildInput::Bytes(_) => std::process::Stdio::piped(),
    });
    let output_files = if detached {
        let directory = std::env::temp_dir().join(format!(
            "cowshed-disk-image-{}",
            uuid::Uuid::new_v4().simple()
        ));
        use std::os::unix::fs::DirBuilderExt as _;
        fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let stdout = File::create(directory.join("stdout"))?;
        let stderr = File::create(directory.join("stderr"))?;
        command.stdout(stdout).stderr(stderr);
        Some(directory)
    } else {
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        None
    };
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: `setsid`, `signal`, `sigemptyset`, `sigaddset` and `sigprocmask` are
        // async-signal-safe and run in the forked child before exec; the child becomes its
        // own session leader, and a disk-image command starts with the caller's hangup,
        // interrupt and terminate ignored and blocked (both survive exec). Fail closed: a
        // child that cannot leave the parent group must never be addressed by group id, so a
        // setsid failure aborts the spawn instead of risking the parent group, and a disk-image
        // command that cannot block the tree-killer's signals is not started.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                if detached {
                    let mut held: libc::sigset_t = std::mem::zeroed();
                    libc::sigemptyset(&mut held);
                    for signal in [libc::SIGHUP, libc::SIGINT, libc::SIGTERM] {
                        libc::signal(signal, libc::SIG_IGN);
                        libc::sigaddset(&mut held, signal);
                    }
                    if libc::sigprocmask(libc::SIG_BLOCK, &held, std::ptr::null_mut()) == -1 {
                        return Err(io::Error::last_os_error());
                    }
                }
                // spawn_locked may have waited behind a descriptor release. Check inside
                // its fork window, after that wait and immediately before native exec.
                // Instant's monotonic read and this errno value allocate nothing.
                if total.is_some_and(|deadline| deadline.remaining().is_none()) {
                    return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
                }
                Ok(())
            });
        }
    }
    let child = command.spawn_locked()?;
    if let Some(directory) = &output_files
        && let Err(error) =
            cowshed_os::scratch::record_owners(directory, &[std::process::id(), child.id()])
    {
        // The disk command is already running and must not be interrupted for bookkeeping.
        // Missing ownership is fail-closed: a reaper keeps this output, naming the failure.
        cowshed_os::eprintln!(
            "cowshed: cannot record disk command scratch owner at {}: {error}",
            directory.display()
        );
    }
    Ok((child, output_files))
}

/// Kill exactly the owned child's process group; retain any failure for the caller.
fn kill_disk_child_group(child: &mut std::process::Child) -> io::Result<()> {
    #[cfg(unix)]
    {
        let pid = libc::pid_t::try_from(child.id()).map_err(io::Error::other)?;
        // SAFETY: the child successfully became its own session leader before exec. This
        // negative, checked PID addresses only its owned group, never the caller's group.
        if unsafe { libc::kill(-pid, libc::SIGKILL) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(unix))]
    child.kill()
}

/// The wait an unforced detach spends before it gives up on politeness.
///
/// Every volume cowshed detaches was just written to, and macOS wakes `mds` and `fseventsd` on a
/// freshly written volume whether or not it was attached `-nobrowse`. The holder is theirs, it is
/// transient, and there is nothing to ask: waiting is the only way to let it go. Forcing is what
/// makes the detach an obligation rather than an attempt — the volume is cowshed's own staging or
/// retiring image, its writes are already synced by the operation that produced them, and a
/// caller that reached a detach has no way to make progress without one.
///
/// Ten seconds is the retirement grace of specs/cowshed/02_workspaces.md, applied to every
/// unforced detach for the same reason it was chosen there.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DetachGrace {
    pub total: Duration,
    pub poll: Duration,
}

impl DetachGrace {
    /// Command and lease waits spend the real grace too. Requested polls also advance the
    /// injected sleeper's logical clock, without counting a real sleep twice.
    fn expired(self, polled: Duration, elapsed: Duration) -> bool {
        polled.max(elapsed) >= self.total
    }
}

impl Default for DetachGrace {
    fn default() -> Self {
        Self {
            total: Duration::from_secs(10),
            poll: Duration::from_millis(250),
        }
    }
}

/// The grace's clock and sleep boundary, injected so unit tests prove escalation without
/// spending wall-clock time or letting scheduler delays advance a mocked clock.
pub trait Sleeper {
    fn sleep(&self, duration: Duration);
    fn elapsed(&self, since: std::time::Instant) -> Duration;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }

    fn elapsed(&self, since: std::time::Instant) -> Duration {
        since.elapsed()
    }
}

/// The wait a just-detached whole device is given to leave the attachment inventory.
///
/// A successful image detach means the image driver let go; the kernel terminating the media it
/// exposed is a second, lagging step. A verb that detaches an image and then attaches it again,
/// as `resize` does, should not start the attach while the old device is still registered.
///
/// After every whole-device detach the backend therefore re-reads the kernel inventory until the
/// image no longer holds the device, logging each outcome. The bound is generous against a
/// normally sub-second departure and the outcome is always soft: at the bound the operation
/// proceeds exactly as it would have without the check, but loudly. No blind sleeps: the first
/// poll runs immediately, and a departed device costs one registry read and no waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DetachSettleGrace {
    pub total: Duration,
    pub poll: Duration,
}

impl Default for DetachSettleGrace {
    fn default() -> Self {
        Self {
            total: Duration::from_secs(10),
            poll: Duration::from_millis(250),
        }
    }
}

/// One backend step inside a [`cowshed_os::timing`] span labelled `apfs <leg>/<step>`, so a slow
/// lifecycle verb names the storage step that spent the time.
pub fn timed_apfs_step<T, E: std::fmt::Display>(
    leg: &str,
    step: &'static str,
    operation: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    cowshed_os::timing::timed("apfs", format_args!("{leg}/{step}"), operation)
}

/// The leg a backend step serves, derived from the image path it touches: staging state lives
/// under `.staging` (the same namespace the storage layer calls STAGING_NAMESPACE; this module
/// cannot name it without a dependency cycle, so the literal is matched here), everything else
/// is canonical. No signature changes — the trait seams stay intact.
pub fn apfs_step_leg(image: &Path) -> &'static str {
    if image
        .components()
        .any(|component| component.as_os_str() == ".staging")
    {
        "staging"
    } else {
        "canonical"
    }
}

/// A blank RAW image holding one case-sensitive APFS volume, staged at `staged_stem.dmg`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateImageRequest {
    /// Staged path without an image extension, e.g. `.staging/main`.
    pub staged_stem: PathBuf,
    /// Capacity the image is created at.
    pub capacity: ImageCapacity,
    pub volume_name: String,
    pub owner_uid: u32,
    pub owner_gid: u32,
}

/// What a detach may do about a volume something else still holds.
///
/// The distinction is not politeness, it is ownership. A volume cowshed has finished with —
/// staging images, retiring mounts, attachments a failed step left behind — is going away
/// whatever a stray `mds` scan thinks, so `Release` waits the holder out and then forces. A
/// volume the user may still be working in is theirs, so `WhenIdle` reports the dissent and lets
/// the verb refuse; `resize` and the explicit `detach`/un-adopt verbs would otherwise tear a
/// live workspace out from under whoever is in it. `WithinGrace` waits the holder out as
/// `Release` does but never forces: it answers the dissent still standing when the grace ends.
/// A build seed's seal detaches with it once the kernel has accepted its unforced unmount, so
/// no file on the volume is open and only a transient device holder can still dissent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetachIntent {
    Release,
    WhenIdle,
    WithinGrace,
}

/// How a `Release` unmount went: through at once or within the grace, or forced once the grace
/// ran out past whatever still held the volume.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unmounted {
    Unforced,
    Forced,
}

#[derive(Debug)]
pub struct AttachedImage {
    image: PathBuf,
    whole_device: String,
    volume_device: String,
    /// An attachment verified or recovered for mounting holds its raw IOMedia open. The
    /// kernel refuses even forced ejects until mount finishes or the handle is dropped;
    /// an observation-only recovered attachment has no pin.
    pin: std::sync::Mutex<Option<Fenced<File>>>,
}

impl AttachedImage {
    pub fn image(&self) -> &Path {
        &self.image
    }
    pub fn whole_device(&self) -> &str {
        &self.whole_device
    }
    pub fn volume_device(&self) -> &str {
        &self.volume_device
    }
    fn release_pin(&self) {
        self.pin
            .lock()
            .expect("attachment pin mutex poisoned")
            .take();
    }
}

/// A volume's mount root, held open and proven to be owned media: the pinned volume device of a
/// verified attachment of an image, never a host disk. It is the only way any code names an APFS
/// volume ([`CommandRunner::name_volume`]), and only this crate can prove one.
///
/// Naming goes through the held descriptor, never a path. Disk Arbitration (`diskutil rename`,
/// `renameVolume`) names a device through the mount path it recorded, resolved when its queue
/// reaches the request, and xnu names whatever volume holds a path: a volume force-unmounted in
/// between leaves a bare directory of the host's Data volume there, and a root Disk Arbitration
/// renames the Data volume. Measured on nested throwaway images: a kernel-forced unmount before
/// `DADiskRename` of the inner device renamed the outer volume holding the mountpoint in 16 of
/// 20 runs. Through the held descriptor, the same forced unmount fails the call instead.
#[derive(Debug)]
pub struct OwnedVolumeRoot {
    volume_device: String,
    mount_point: PathBuf,
    held: mounts::HeldMount,
}

impl OwnedVolumeRoot {
    /// `held`, at `mount_point`, as `attachment`'s volume root: the image's pinned, proven
    /// volume device mounted there, at the root. Anything else is handed back for the refusal.
    fn of_attachment(
        held: mounts::HeldMount,
        mount_point: &Path,
        attachment: &AttachedImage,
    ) -> Result<Self, mounts::HeldMount> {
        if held.is_root() && held.source_device() == attachment.volume_device {
            Ok(Self {
                volume_device: attachment.volume_device.clone(),
                mount_point: mount_point.to_owned(),
                held,
            })
        } else {
            Err(held)
        }
    }

    pub fn volume_device(&self) -> &str {
        &self.volume_device
    }

    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    pub fn filesystem_id(&self) -> mounts::FilesystemId {
        self.held.filesystem_id()
    }

    fn unmount(&self, mode: UnmountMode) -> Result<UnmountEffect, NativeUnmountError> {
        self.held.unmount(mode)
    }

    /// Name this volume through its held root descriptor ([`mounts::HeldMount`]).
    pub fn set_name(&self, volume_name: &str) -> io::Result<()> {
        self.held.set_volume_name(volume_name)
    }
}

/// The authority that names retained inputs: a validated explicit re-adoption record. The
/// workspace layer implements it for its record; this layer only asks whether a path is named.
pub trait RetainedInputs {
    /// Whether this record names `path` as one of the images it retains.
    fn names(&self, path: &Path) -> bool;
}

/// A path named by a validated explicit re-adoption record. Like every image it is RAW: a recorded
/// input in another format is refused, naming that format and its path, before anything runs
/// ([`ApfsError::InvalidImagePath`]). This token enables identity inspection and read-only
/// retained attachment, never image creation or growth.
#[derive(Clone, Copy, Debug)]
pub struct RetainedInput<'a> {
    path: &'a Path,
}

impl<'a> RetainedInput<'a> {
    /// `path` as a retained input, or `None` when `record` does not name it.
    pub fn named_by(record: &impl RetainedInputs, path: &'a Path) -> Option<Self> {
        record.names(path).then_some(Self { path })
    }

    pub fn path(&self) -> &'a Path {
        self.path
    }
}

/// Retained inputs expose no writable-mount capability. Only this module can obtain the inner
/// attachment; callers use its read-only mount and non-forced release methods.
#[cfg(any(target_os = "macos", test))]
#[derive(Debug)]
pub struct RetainedAttachment(AttachedImage);

#[cfg(any(target_os = "macos", test))]
impl RetainedAttachment {
    pub fn volume_device(&self) -> &str {
        self.0.volume_device()
    }
}

/// The slowest an accepted `hdiutil detach` normally takes: measured 35–500ms on idle and
/// loaded developer hosts alike. A slower one keeps hdiutil's account of what it went through.
const SLOW_DETACH: Duration = Duration::from_secs(1);

/// A child's diagnostic output as one trace line: lines joined with ` | `, blank lines dropped.
fn one_line(output: &[u8]) -> String {
    String::from_utf8_lossy(output)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The exact image mapping may be a just-created whole device or a formatted APFS attachment.
pub enum RecoveredImageAttachment {
    Unformatted {
        image: PathBuf,
        whole_device: String,
    },
    Apfs(AttachedImage),
}

impl RecoveredImageAttachment {
    pub fn whole_device(&self) -> &str {
        match self {
            Self::Unformatted { whole_device, .. } => whole_device,
            Self::Apfs(attachment) => &attachment.whole_device,
        }
    }
}

/// What backs an attached disk image, as the image driver registered it with the kernel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiskImageSource {
    /// A local backing file: the path the attach was handed.
    File(PathBuf),
    /// Any other backing store (RAM, network), by its URL. It is never a workspace image.
    Url(String),
}

impl DiskImageSource {
    /// The image driver registers its backing store as a URL (`DiskImageURL`); a local `file:`
    /// URL is the path the attach was handed.
    pub fn from_url(url: &str) -> Self {
        match url::Url::parse(url) {
            Ok(parsed) if parsed.scheme() == "file" => match parsed.to_file_path() {
                Ok(path) => Self::File(path),
                Err(()) => Self::Url(url.to_owned()),
            },
            _ => Self::Url(url.to_owned()),
        }
    }
}

/// One BSD media node an attached image exposes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageMedia {
    /// The `/dev/diskN[sM…]` node.
    pub device: String,
    /// The media's content hint: empty for an image that is its own APFS store, a partition
    /// scheme, or an Apple partition-type GUID (`EF57347C-…` for the synthesized container,
    /// `41504653-…` for its volume).
    pub content: String,
}

/// One attached disk image as the kernel's I/O Registry holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachedDiskImage {
    pub source: DiskImageSource,
    /// The size of the image's own whole device; `None` while that device is not registered.
    pub capacity: Option<ImageCapacity>,
    /// Every BSD media node beneath the image driver, in device order.
    pub media: Vec<ImageMedia>,
    /// The user Disk Arbitration takes as the owner of every disk this attachment exposes: the
    /// image driver's `owner-uid`, which the attach records as the attaching user, or root when
    /// the driver records none (diskarbitrationd's default). Only that user or root unmounts or
    /// ejects those disks without asking authd (01a_apfs.md, "Release authority").
    pub owner_uid: libc::uid_t,
}

impl AttachedDiskImage {
    /// A file-backed attachment exposing `media` as `(device, content hint)` pairs, made by
    /// this process's user, as every cowshed attach is.
    pub fn file<'a>(
        image: impl Into<PathBuf>,
        capacity: Option<ImageCapacity>,
        media: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        Self {
            source: DiskImageSource::File(image.into()),
            capacity,
            media: media
                .into_iter()
                .map(|(device, content)| ImageMedia {
                    device: device.to_owned(),
                    content: content.to_owned(),
                })
                .collect(),
            owner_uid: current_euid(),
        }
    }

    fn is_backed_by(&self, image: &Path) -> bool {
        matches!(&self.source, DiskImageSource::File(path) if path == image)
    }
}

#[derive(Debug)]
pub enum CloneFileError {
    InvalidImagePath {
        path: PathBuf,
    },
    CrossVolume {
        source: PathBuf,
        destination: PathBuf,
    },
    DestinationExists {
        destination: PathBuf,
    },
    UnsupportedPlatform,
    /// A detached capture was requested, but an image driver still holds the source inode.
    SourceAttached {
        source: PathBuf,
    },
    Io {
        source_path: PathBuf,
        destination_path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for CloneFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidImagePath { path } => write!(
                f,
                "{} does not have the .{IMAGE_EXTENSION} image extension",
                path.display()
            ),
            Self::CrossVolume {
                source,
                destination,
            } => write!(
                f,
                "clonefile requires source and destination on the same volume: {} -> {}",
                source.display(),
                destination.display()
            ),
            Self::DestinationExists { destination } => write!(
                f,
                "clone destination already exists: {}",
                destination.display()
            ),
            Self::UnsupportedPlatform => write!(f, "clonefile is available only on macOS"),
            Self::SourceAttached { source } => write!(
                f,
                "{} is attached, but this capture requires a detached source",
                source.display()
            ),
            Self::Io {
                source_path,
                destination_path,
                source,
            } => write!(
                f,
                "clonefile {} -> {} failed: {}",
                source_path.display(),
                destination_path.display(),
                source
            ),
        }
    }
}

impl std::error::Error for CloneFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct AttachmentDetachFailure {
    pub device: String,
    pub error: Box<ApfsError>,
}

#[derive(Debug)]
pub struct AttachmentCleanupFailure {
    pub inventory: Option<Box<ApfsError>>,
    pub detach: Vec<AttachmentDetachFailure>,
    pub remaining_devices: Vec<String>,
}

impl fmt::Display for AttachmentCleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut separator = "";
        if let Some(inventory) = self.inventory.as_ref() {
            write!(f, "inventory: {inventory}")?;
            separator = "; ";
        }
        for failure in &self.detach {
            write!(f, "{separator}detach {}: {}", failure.device, failure.error)?;
            separator = "; ";
        }
        if !self.remaining_devices.is_empty() {
            write!(
                f,
                "{separator}devices still attached: {}",
                self.remaining_devices.join(", ")
            )?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ApfsError {
    InvalidImagePath(PathBuf),
    InvalidStagedStem(PathBuf),
    InvalidCreateRequest(&'static str),
    InvalidDetachTarget(PathBuf),
    InvalidVolumeName(String),
    InvalidVolumeDevice(String),
    /// A rename's volume is not the one mounted at the path it was found mounted at by the time
    /// the rename would run: the path now lies on `found` (the host's own Data volume, once
    /// nothing is mounted there), or inside a volume rather than at its root. The held path is
    /// no [`OwnedVolumeRoot`] of the image, so nothing is named.
    RenameTargetNotMounted {
        image: PathBuf,
        volume_device: String,
        mount_point: PathBuf,
        found: String,
    },
    /// Naming a proven owned volume root through its held descriptor failed: the volume was
    /// unmounted, even by force, since the proof (its vnode is dead, `ENOTSUP`), or the kernel
    /// refused the name. Nothing else was named.
    RenameFailed {
        volume_device: String,
        mount_point: PathBuf,
        source: io::Error,
    },
    /// A current mount lookup did not yield this pinned attachment's own filesystem root.
    UnmountTargetNotMounted {
        image: PathBuf,
        volume_device: String,
        mount_point: PathBuf,
        found: String,
        is_root: bool,
    },
    /// A descriptor-native unmount failure, with the attachment and exact native cause retained.
    UnmountFailed {
        image: PathBuf,
        volume_device: String,
        mode: UnmountMode,
        source: NativeUnmountError,
    },
    CommandRun(CommandRunError),
    CommandFailed {
        operation: &'static str,
        request: CommandRequest,
        output: CommandOutput,
    },
    /// A disk-image command whose framework could not reach its helper daemon over XPC.
    /// Boxed: the evidence is rare, and every `Result` carrying this error would otherwise pay
    /// for it.
    DiskImageHelperUnreachable(Box<DiskImageHelperFailure>),
    InvalidAttachmentPlist(String),
    /// The attach succeeded but reported no APFS volume at all: the image was never formatted,
    /// or its formatting never finished. Distinct from a malformed report, which proves nothing
    /// about the image.
    NoApfsVolume,
    InvalidAttachmentInventory(String),
    AttachmentCleanupFailed {
        image: PathBuf,
        primary: Box<ApfsError>,
        cleanup: AttachmentCleanupFailure,
    },
    VerificationFailed {
        request: CommandRequest,
        output: CommandOutput,
    },
    VerificationAndDetachFailed {
        request: CommandRequest,
        verification: CommandOutput,
        detach: Box<ApfsError>,
    },
    FileOperation {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Clone(CloneFileError),
    ImageCreationAndCleanupFailed {
        primary: Box<ApfsError>,
        detach: Option<Box<ApfsError>>,
        remove: Option<Box<ApfsError>>,
    },
    ImageNotAttached(PathBuf),
    /// The kernel's disk-image inventory could not be read.
    KernelInventory(io::Error),
    /// A clone needed its mounted source detached, and the kernel refused the unforced release.
    /// Nothing is forced; `held` says which step was refused and by what, and `refusal` is the
    /// refused unmount or detach itself.
    CaptureSourceBusy {
        image: PathBuf,
        mount_point: PathBuf,
        held: CaptureHold,
        refusal: Box<ApfsError>,
    },
}

/// Which unforced step of a capture's seal the kernel refused, and what it named.
#[derive(Debug)]
pub enum CaptureHold {
    /// The unmount was refused: each process of this user that had the volume open when it was
    /// refused (empty when none of them is visible: the kernel or another user's process holds
    /// it).
    Volume(Vec<cowshed_os::process::Holder>),
    /// The unmount was accepted, so no file on the volume was open, but the image's device
    /// still refused its unforced detach when the detach grace ended.
    Device,
}

/// The framework's failure, not the request's: it was seen striking a burst of concurrent
/// attaches at once on a host whose load average stood near 350, so the load at the failure is
/// carried and the report names the host condition the command met.
#[derive(Debug)]
pub struct DiskImageHelperFailure {
    pub operation: &'static str,
    pub request: CommandRequest,
    pub output: CommandOutput,
    /// `getloadavg` when the failure was classified; `None` if the kernel would not say.
    pub load: Option<HostLoad>,
}

impl fmt::Display for ApfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidImagePath(path) => match path.extension() {
                Some(format) => write!(
                    f,
                    "{} is a .{} file, not a RAW .{IMAGE_EXTENSION} image: cowshed attaches RAW images only and converts none",
                    path.display(),
                    format.to_string_lossy()
                ),
                None => write!(
                    f,
                    "{} is not a RAW .{IMAGE_EXTENSION} image",
                    path.display()
                ),
            },
            Self::InvalidStagedStem(path) => write!(
                f,
                "staged image stem must not have an extension: {}",
                path.display()
            ),
            Self::InvalidCreateRequest(message) => f.write_str(message),
            Self::InvalidDetachTarget(target) => {
                write!(f, "invalid APFS detach target: {}", target.display())
            }
            Self::InvalidVolumeName(name) => {
                write!(f, "invalid APFS volume name: {name:?}")
            }
            Self::InvalidVolumeDevice(device) => {
                write!(f, "invalid APFS volume device: {device}")
            }
            Self::RenameTargetNotMounted {
                image,
                volume_device,
                mount_point,
                found,
            } => write!(
                f,
                "refusing to rename {volume_device} of {}: {} is no longer its mount root (the \
                 kernel holds {found} there)",
                image.display(),
                mount_point.display()
            ),
            Self::RenameFailed {
                volume_device,
                mount_point,
                source,
            } => write!(
                f,
                "could not rename {volume_device} through its held mount root {}: {source}",
                mount_point.display()
            ),
            Self::UnmountTargetNotMounted {
                image,
                volume_device,
                mount_point,
                found,
                is_root,
            } => write!(
                f,
                "refusing to unmount {volume_device} of {}: {} holds {found} (mount root: {is_root}), not its owned root",
                image.display(),
                mount_point.display()
            ),
            Self::UnmountFailed {
                image,
                volume_device,
                mode,
                source,
            } => write!(
                f,
                "unmount {volume_device} of {} ({mode:?}) failed: {source}",
                image.display()
            ),
            Self::CommandRun(error) => error.fmt(f),
            Self::CommandFailed {
                operation,
                request,
                output,
            } => fmt_command_failure(
                f,
                operation,
                request.program.as_os_str(),
                &request.args,
                output,
            ),
            Self::DiskImageHelperUnreachable(failure) => {
                f.write_str("the disk-images helper daemon could not be reached over XPC (")?;
                match &failure.load {
                    Some(load) => write!(f, "{load}")?,
                    None => f.write_str("load average unavailable")?,
                }
                f.write_str(" at the failure); ")?;
                fmt_command_failure(
                    f,
                    failure.operation,
                    failure.request.program.as_os_str(),
                    &failure.request.args,
                    &failure.output,
                )
            }
            Self::InvalidAttachmentPlist(message) => {
                write!(f, "invalid attachment plist: {message}")
            }
            Self::NoApfsVolume => {
                f.write_str("attach reported no APFS volume: the image was never formatted")
            }
            Self::InvalidAttachmentInventory(message) => {
                write!(f, "invalid attachment inventory: {message}")
            }
            Self::AttachmentCleanupFailed {
                image,
                primary,
                cleanup,
            } => write!(
                f,
                "attachment failed for {}, and cleaning up newly attached devices also failed: primary={primary}; cleanup={cleanup}",
                image.display()
            ),
            Self::VerificationFailed { request, output } => fmt_command_failure(
                f,
                "verify APFS volume",
                request.program.as_os_str(),
                &request.args,
                output,
            ),
            Self::VerificationAndDetachFailed {
                request,
                verification,
                detach,
            } => {
                fmt_command_failure(
                    f,
                    "verify APFS volume",
                    request.program.as_os_str(),
                    &request.args,
                    verification,
                )?;
                write!(f, "; detaching the failed attachment also failed: {detach}")
            }
            Self::FileOperation {
                operation,
                path,
                source,
            } => write!(f, "{} {} failed: {}", operation, path.display(), source),
            Self::Clone(error) => error.fmt(f),
            Self::ImageCreationAndCleanupFailed {
                primary,
                detach,
                remove,
            } => {
                write!(f, "RAW image creation failed: {primary}")?;
                if let Some(detach) = detach {
                    write!(f, "; detach cleanup failed: {detach}")?;
                }
                if let Some(remove) = remove {
                    write!(f, "; file cleanup failed: {remove}")?;
                }
                Ok(())
            }
            Self::ImageNotAttached(image) => write!(
                f,
                "{} reports no attachment to read a capacity from",
                image.display()
            ),
            Self::KernelInventory(source) => {
                write!(f, "read the kernel disk-image inventory: {source}")
            }
            Self::CaptureSourceBusy {
                image,
                mount_point,
                held: CaptureHold::Volume(holders),
                refusal,
            } => {
                write!(
                    f,
                    "{} must be detached to be cloned, and its volume at {} is in use",
                    image.display(),
                    mount_point.display()
                )?;
                match holders.as_slice() {
                    [] => f.write_str(" by nothing this user can see")?,
                    [first, rest @ ..] => {
                        write!(f, " by {first}")?;
                        for holder in rest {
                            write!(f, ", {holder}")?;
                        }
                    }
                }
                write!(
                    f,
                    "; nothing was forced: close what holds it and run the command again (release refused: {refusal})"
                )
            }
            Self::CaptureSourceBusy {
                image,
                mount_point,
                held: CaptureHold::Device,
                refusal,
            } => write!(
                f,
                "{} must be detached to be cloned; its volume at {} unmounted with no file open, but the image's device still refused the unforced detach when the detach grace ended, held by something this user cannot see; nothing was forced: run the command again (detach refused: {refusal})",
                image.display(),
                mount_point.display()
            ),
        }
    }
}

impl std::error::Error for ApfsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnmountFailed { source, .. } => Some(source),
            Self::CommandRun(error) => Some(error),
            Self::KernelInventory(source) => Some(source),
            Self::FileOperation { source, .. } | Self::RenameFailed { source, .. } => Some(source),
            Self::Clone(error) => Some(error),
            Self::VerificationAndDetachFailed { detach, .. } => Some(detach),
            Self::ImageCreationAndCleanupFailed { primary, .. } => Some(primary),
            Self::AttachmentCleanupFailed { primary, .. } => Some(primary),
            Self::CaptureSourceBusy { refusal, .. } => Some(refusal),
            _ => None,
        }
    }
}

impl From<CommandRunError> for ApfsError {
    fn from(value: CommandRunError) -> Self {
        Self::CommandRun(value)
    }
}

impl From<CloneFileError> for ApfsError {
    fn from(value: CloneFileError) -> Self {
        Self::Clone(value)
    }
}

/// Mounted workspace and build sources may be captured live. Runtime retains the named Nx/Cargo
/// guards for build capture; other contributed paths are exempt. Immutable templates and seed
/// source images remain held against attachment (specs/cowshed/16_build_volumes.md).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageCapture {
    /// Descriptor-clone without changing the source's attachment or mount state.
    Live,
    /// Hold the source inode against attachment throughout the descriptor clone.
    Detached,
}

pub trait ApfsBackend {
    fn create_staged_image(&self, request: &CreateImageRequest) -> Result<PathBuf, ApfsError>;
    /// Write the blank, unattached sparse RAW file `staged_stem.dmg` and nothing else: the one step
    /// of creation that needs no attachment, so a crash inside it leaves only a staging file.
    fn create_blank_image(&self, request: &CreateImageRequest) -> Result<PathBuf, ApfsError>;
    /// Attach the blank image at `image`, format its one case-sensitive APFS volume, and hand
    /// the attachment back verified (`fsck_apfs -q`) and pinned, ready to mount. The formatting
    /// attach is the image's only one: no detach and second attach follow it. Every failure
    /// releases the attachment and removes the image.
    fn format_attached(
        &self,
        image: &Path,
        request: &CreateImageRequest,
    ) -> Result<AttachedImage, ApfsError>;
    /// Push the latest writes of a source still in use into its image file: flush its volume
    /// when `mount_point` is mounted read-write, then the image file itself. A read-only mount
    /// holds no dirty volume writes and is not flushed. What the flushed file's modification
    /// time then says is current. Live workspace and guarded build-source captures follow this
    /// flush; the immutable capture mark comes from the clone, not a later live-source clock.
    ///
    /// Only the source is flushed. The host-wide `sync(8)` this replaces also waited for every
    /// other mounted volume's dirty data — measured at 16 s, and at 39 s inside a fork under a
    /// host with dozens of workspaces building, against 3–676 ms for the source volume alone.
    fn sync_for_freshness(&self, image: &Path, mount_point: Option<&Path>)
    -> Result<(), ApfsError>;
    /// Descriptor-clone `source` to a destination that must not exist. Open the source once,
    /// without following symlinks, and flush that descriptor before cloning. A detached capture
    /// additionally holds a nonblocking shared lock excluding image attachment; live source
    /// captures do not change the source's attachment or mount state. The cloned APFS volume is
    /// verified before its first mount.
    fn clone_image(
        &self,
        source: &Path,
        destination: &Path,
        capture: ImageCapture,
    ) -> Result<(), CloneFileError>;
    /// Rename `attachment`'s volume, mounted at `mount_point`, through that mount root held open,
    /// never through a path or Disk Arbitration ([`OwnedVolumeRoot`]). Under the image's lease and
    /// a raw pin, the device is proven still to be that image's, and `mount_point`, held open,
    /// proven to be that device's mount root; the name is then set through the held descriptor,
    /// which an unmount since — even a forced one — fails rather than redirects.
    fn rename_volume(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        volume_name: &str,
    ) -> Result<(), ApfsError>;
    fn attach_verified(&self, image: &Path) -> Result<AttachedImage, ApfsError>;
    /// Attach the exact image once without mounting, then fsck and mount its pinned APFS volume.
    fn attach_mounted(
        &self,
        image: &Path,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<AttachedImage, ApfsError>;
    fn mount(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsError>;
    fn detach(&self, attachment: &AttachedImage, intent: DetachIntent) -> Result<(), ApfsError>;
    fn delete_image(&self, image: &Path) -> Result<(), ApfsError>;
    /// The capacity a RAW image's file length records. Spawns nothing: the length is the record.
    fn image_capacity(&self, image: &Path) -> Result<ImageCapacity, ApfsError>;
    /// Grow a detached image's virtual disk to `capacity`. Only the image grows: the container
    /// inside it grows afterwards through an owned, verified attachment ([`Self::grow_container`]).
    fn resize_image(&self, image: &Path, capacity: ImageCapacity) -> Result<(), ApfsError>;
    /// Grow the APFS container an attachment exposes until it spans the whole image.
    fn grow_container(&self, attachment: &AttachedImage) -> Result<(), ApfsError>;
    /// The capacity the kernel reports for an attached image.
    fn attached_capacity(&self, image: &Path) -> Result<ImageCapacity, ApfsError>;
}

pub struct MacOsApfsBackend<R, S = ThreadSleeper> {
    runner: R,
    sleeper: S,
    grace: DetachGrace,
    settle: DetachSettleGrace,
}

/// A verified volume pinned under its image lease. Keep this value alive through the
/// mount-table decision and any observation of a refused mount, not merely the child command.
pub struct MountLease<'a, R, S> {
    backend: &'a MacOsApfsBackend<R, S>,
    attachment: &'a AttachedImage,
    inherited_pin: std::sync::MutexGuard<'a, Option<Fenced<File>>>,
    _fresh_pin: Option<Fenced<File>>,
    _lease: Option<Fenced<File>>,
}

impl<R, S> Drop for MountLease<'_, R, S> {
    fn drop(&mut self) {
        self.inherited_pin.take();
    }
}

impl<R: CommandRunner, S: Sleeper> MountLease<'_, R, S> {
    pub fn mount(
        &self,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsError> {
        let attachment = self.attachment;
        let backend = self.backend;
        fs::create_dir_all(mount_point).map_err(|source| ApfsError::FileOperation {
            operation: "create mount point",
            path: mount_point.to_owned(),
            source,
        })?;
        // `noatime`: builds are read-heavy, and access-time updates on read are metadata writes no
        // workflow here consults.
        let options = match (access, browse) {
            (MountAccess::ReadWrite, false) => "nobrowse,owners,noatime",
            (MountAccess::ReadWrite, true) => "owners,noatime",
            (MountAccess::ReadOnly, false) => "rdonly,nobrowse,owners,noatime",
            (MountAccess::ReadOnly, true) => "rdonly,owners,noatime",
        };
        let mounted = timed_apfs_step(apfs_step_leg(&attachment.image), "mount", || {
            backend.run_checked(
                "mount verified APFS volume",
                mount_request(attachment, options, mount_point),
            )
        });
        let opted_out = match (&mounted, access) {
            (Ok(_), MountAccess::ReadWrite) => {
                timed_apfs_step(apfs_step_leg(&attachment.image), "event-log", || {
                    backend.opt_out_of_event_log(attachment, mount_point, options)
                })
            }
            _ => Ok(()),
        };
        mounted.and(opted_out)
    }
}

impl<R> MacOsApfsBackend<R> {
    pub fn new(runner: R) -> Self {
        Self {
            runner,
            sleeper: ThreadSleeper,
            grace: DetachGrace::default(),
            settle: DetachSettleGrace::default(),
        }
    }
}

impl<R, S> MacOsApfsBackend<R, S> {
    /// The grace is a named type rather than a pair of `Duration`s so a caller cannot silently
    /// transpose its bound and its poll.
    pub fn with_grace(runner: R, sleeper: S, grace: DetachGrace) -> Self {
        Self {
            runner,
            sleeper,
            grace,
            settle: DetachSettleGrace::default(),
        }
    }
    /// The detach-settle bound, chained after [`MacOsApfsBackend::with_grace`].
    pub fn with_detach_settle(mut self, settle: DetachSettleGrace) -> Self {
        self.settle = settle;
        self
    }
    pub fn runner(&self) -> &R {
        &self.runner
    }
}

impl<R: CommandRunner, S: Sleeper> MacOsApfsBackend<R, S> {
    /// Acquire mount authority before consulting the mount table. A recovered attachment's
    /// pin fences its device across this reacquisition, but another mounter may have won.
    pub fn prepare_mount<'a>(
        &'a self,
        attachment: &'a AttachedImage,
    ) -> Result<MountLease<'a, R, S>, ApfsError> {
        let lease = self.image_lease(&attachment.image)?;
        let inherited_pin = attachment
            .pin
            .lock()
            .expect("attachment pin mutex poisoned");
        let fresh_pin = if inherited_pin.is_some() {
            // The verified IOMedia handle survived fsck and still prevents eject.
            None
        } else {
            self.pin_attached_volume(attachment)?
        };
        Ok(MountLease {
            backend: self,
            attachment,
            inherited_pin,
            _fresh_pin: fresh_pin,
            _lease: lease,
        })
    }

    /// Hold `image`'s lease ([`CommandRunner::image_lease`]), keyed by the identity the
    /// attachment inventory records for it. Source holder censuses use the same exclusion as
    /// recovery mutations, so a cosmetic rename's mount descriptor is released before they run.
    pub fn image_lease(&self, image: &Path) -> Result<Option<Fenced<File>>, ApfsError> {
        let identity = attachment_inventory_path(image)?;
        timed_apfs_step(apfs_step_leg(image), "image-lease", || {
            self.runner
                .image_lease(&identity)
                .map_err(|source| ApfsError::FileOperation {
                    operation: "acquire APFS image lease",
                    path: identity.clone(),
                    source,
                })
        })
    }

    /// Name `root`'s volume through the runner ([`CommandRunner::name_volume`]), the one call
    /// that names any volume. A failure, including the dead descriptor a forced unmount since
    /// the proof leaves, names nothing and says which root it was.
    fn name_owned_volume(
        &self,
        root: &OwnedVolumeRoot,
        volume_name: &str,
    ) -> Result<(), ApfsError> {
        self.runner
            .name_volume(root, volume_name)
            .map_err(|source| ApfsError::RenameFailed {
                volume_device: root.volume_device.clone(),
                mount_point: root.mount_point.clone(),
                source,
            })
    }

    fn pin_attached_volume(
        &self,
        attachment: &AttachedImage,
    ) -> Result<Option<Fenced<File>>, ApfsError> {
        self.pin_volume(attachment, |attachment| {
            self.require_attached_mapping(attachment)
        })
    }

    /// Hold `attachment`'s raw volume open, then prove `identity` of it under that pin.
    fn pin_volume(
        &self,
        attachment: &AttachedImage,
        identity: impl FnOnce(&AttachedImage) -> Result<(), ApfsError>,
    ) -> Result<Option<Fenced<File>>, ApfsError> {
        let raw = PathBuf::from(raw_device_from(&attachment.volume_device));
        let pin = timed_apfs_step(apfs_step_leg(&attachment.image), "pin", || {
            self.runner
                .pin_raw_device(&raw)
                .map_err(|source| ApfsError::FileOperation {
                    operation: "pin attached APFS volume against device reuse",
                    path: raw,
                    source,
                })
        })?;
        // A pre-check without this pin leaves a gap where an unrelated process can eject
        // the image and the kernel can hand diskN to a foreign mounted volume.
        timed_apfs_step(apfs_step_leg(&attachment.image), "identity", || {
            identity(attachment)
        })?;
        Ok(pin)
    }

    /// Whether `attachment`'s devices are still one of its image's attachments. Membership, not
    /// uniqueness: a path the kernel holds twice (a dead process's attach landing on a file that
    /// was replaced under it) still owns each of its devices, and releasing them one by one has to
    /// get past this check.
    fn require_attached_mapping(&self, attachment: &AttachedImage) -> Result<(), ApfsError> {
        let image = attachment_inventory_path(&attachment.image)?;
        let actual = image_attachments(&image, &self.attachments_of(&image)?)?;
        if actual.iter().any(|held| {
            matches!(held, RecoveredImageAttachment::Apfs(held)
                if held.whole_device == attachment.whole_device
                    && held.volume_device == attachment.volume_device)
        }) {
            Ok(())
        } else {
            Err(reused_device(
                attachment,
                actual.iter().map(RecoveredImageAttachment::whole_device),
            ))
        }
    }

    /// Whether `attachment`'s volume is still one of the volumes its image's driver holds under
    /// that whole device: what a release revalidates before each unmount, however many volumes
    /// the driver's container holds.
    fn require_released_volume(&self, attachment: &AttachedImage) -> Result<(), ApfsError> {
        let image = attachment_inventory_path(&attachment.image)?;
        let views = release_views_of(&image, &self.attachments_of(&image)?)?;
        if views.iter().any(|view| {
            view.whole_device == attachment.whole_device
                && view.volumes.contains(&attachment.volume_device)
        }) {
            Ok(())
        } else {
            Err(reused_device(
                attachment,
                views.iter().map(ReleaseView::whole_device),
            ))
        }
    }

    /// Remove an owned volume from the filesystem namespace before releasing its image.
    /// The raw IOMedia pin freezes device identity while the same owned root FD is unmounted.
    pub fn unmount_verified(
        &self,
        attachment: &AttachedImage,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        self.unmount_reporting(attachment, intent).map(|_| ())
    }

    /// [`Self::unmount_verified`] with `Release`, answering whether the grace ran out and the
    /// unmount was forced past whatever still held the volume.
    pub fn unmount_within_grace(&self, attachment: &AttachedImage) -> Result<Unmounted, ApfsError> {
        self.unmount_reporting(attachment, DetachIntent::Release)
    }

    fn unmount_reporting(
        &self,
        attachment: &AttachedImage,
        intent: DetachIntent,
    ) -> Result<Unmounted, ApfsError> {
        let _lease = self.image_lease(&attachment.image)?;
        let _pin = self.pin_attached_volume(attachment)?;
        self.unmount_volume_unlocked(attachment, intent, "unmount verified APFS volume")
    }

    /// Unmount one volume of a released image's driver under the image's lease and a fresh raw
    /// pin of that volume, once the pin proves the driver still holds it
    /// ([`Self::require_released_volume`]): the single-volume identity check would refuse every
    /// volume of a container that holds more than one.
    pub(crate) fn unmount_released(
        &self,
        volume: &ReleasedVolume,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        let attachment = &volume.0;
        let _lease = self.image_lease(&attachment.image)?;
        let _pin = self.pin_volume(attachment, |attachment| {
            self.require_released_volume(attachment)
        })?;
        self.unmount_volume_unlocked(attachment, intent, "unmount verified APFS volume")
            .map(|_| ())
    }

    /// Remove one exact owned root through its descriptor. Only native EBUSY earns the existing
    /// grace; Release then forces once, WithinGrace returns the remaining busy refusal, and
    /// WhenIdle returns it now. The image lease and raw pin outlive every admission and attempt.
    fn unmount_volume_unlocked(
        &self,
        attachment: &AttachedImage,
        intent: DetachIntent,
        operation: &'static str,
    ) -> Result<Unmounted, ApfsError> {
        enum AttemptError {
            Proof(ApfsError),
            Native(NativeUnmountError),
        }
        impl fmt::Display for AttemptError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Self::Proof(error) => error.fmt(f),
                    Self::Native(error) => error.fmt(f),
                }
            }
        }
        impl From<NativeUnmountError> for AttemptError {
            fn from(error: NativeUnmountError) -> Self {
                Self::Native(error)
            }
        }
        let mut waited = Duration::ZERO;
        let mut started = None;
        let mut mode = UnmountMode::Normal;
        loop {
            let result = timed_apfs_step(apfs_step_leg(&attachment.image), "unmount", || {
                cowshed_os::timing::timed("apfs-command", format_args!("{operation}"), || {
                    self.runner
                        .unmount_guarded(mode, || {
                            let mount = self
                                .runner
                                .volume_mount(&attachment.volume_device)
                                .map_err(|source| {
                                    AttemptError::Native(NativeUnmountError::KernelRead {
                                        stage: "read kernel mount before APFS unmount",
                                        source,
                                    })
                                })?;
                            let Some(mount) = mount else {
                                return Ok(None);
                            };
                            let held =
                                self.runner
                                    .hold_mount(&mount.mount_point)
                                    .map_err(|source| {
                                        AttemptError::Native(NativeUnmountError::KernelRead {
                                            stage: "hold APFS unmount root",
                                            source,
                                        })
                                    })?;
                            OwnedVolumeRoot::of_attachment(held, &mount.mount_point, attachment)
                                .map(Some)
                                .map_err(|held| {
                                    AttemptError::Proof(ApfsError::UnmountTargetNotMounted {
                                        image: attachment.image.clone(),
                                        volume_device: attachment.volume_device.clone(),
                                        mount_point: mount.mount_point,
                                        found: held.source_device().to_owned(),
                                        is_root: held.is_root(),
                                    })
                                })
                        })
                        .map_err(|error| match error {
                            AttemptError::Proof(error) => error,
                            AttemptError::Native(source) => ApfsError::UnmountFailed {
                                image: attachment.image.clone(),
                                volume_device: attachment.volume_device.clone(),
                                mode,
                                source,
                            },
                        })
                })
            });
            match result {
                Ok(UnmountEffect::NoEffect) => {
                    cowshed_os::timing::attribute(
                        "apfs-command",
                        format_args!("{operation}"),
                        "effect",
                        "already unmounted",
                    );
                    return Ok(Unmounted::Unforced);
                }
                Ok(UnmountEffect::Unmounted {
                    post_fd: PostUnmountFd::Dead(source),
                }) => {
                    cowshed_os::timing::attribute(
                        "apfs-command",
                        format_args!("{operation}"),
                        "post-fd",
                        source,
                    );
                    return Ok(match mode {
                        UnmountMode::Normal => Unmounted::Unforced,
                        UnmountMode::ForceAfterGrace => Unmounted::Forced,
                    });
                }
                Err(error) if mode == UnmountMode::Normal && detach_was_dissented(&error) => {
                    let started = started.get_or_insert_with(std::time::Instant::now);
                    let expired = self.grace.expired(waited, self.sleeper.elapsed(*started));
                    match (intent, expired) {
                        (DetachIntent::WhenIdle, _) | (DetachIntent::WithinGrace, true) => {
                            return Err(error);
                        }
                        (DetachIntent::Release, true) => mode = UnmountMode::ForceAfterGrace,
                        (DetachIntent::Release | DetachIntent::WithinGrace, false) => {
                            self.sleeper.sleep(self.grace.poll);
                            waited = waited.saturating_add(self.grace.poll);
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Read the attachment inventory for this exact path without constraining its image format.
    /// Creation and ordinary runtime attachment still validate the format.
    pub fn attached_whole_devices(&self, image: &Path) -> Result<BTreeSet<String>, ApfsError> {
        Ok(self.attachment_inventory(image)?.devices)
    }

    fn attachment_inventory(&self, image: &Path) -> Result<AttachmentInventory, ApfsError> {
        let image = attachment_inventory_path(image)?;
        image_inventory(&image, &self.attachments_of(&image)?)
    }

    /// The kernel's disk-image inventory: Tahoe's `diskutil image info --plist` exposes no
    /// attachment devices, and `hdiutil info -plist` truncates its list while other images
    /// attach or detach, so only the I/O Registry answers both presence and absence.
    fn attached_disk_images(&self) -> Result<Vec<AttachedDiskImage>, ApfsError> {
        cowshed_os::timing::timed(
            "apfs-inventory",
            format_args!("attached disk images"),
            || {
                self.runner
                    .attached_disk_images()
                    .map_err(ApfsError::KernelInventory)
            },
        )
    }

    /// Every attachment the kernel holds for `image`, an attachment inventory path: the
    /// inventory's entries backed by it, read without reading any other image.
    fn attachments_of(&self, image: &Path) -> Result<Vec<AttachedDiskImage>, ApfsError> {
        cowshed_os::timing::timed("apfs-inventory", format_args!("image attachments"), || {
            self.runner
                .attachments_of(image)
                .map_err(ApfsError::KernelInventory)
        })
    }

    fn cleanup_new_attachments(
        &self,
        image: &Path,
        before: &BTreeSet<String>,
        intent: DetachIntent,
    ) -> Result<(), AttachmentCleanupFailure> {
        let after =
            self.attached_whole_devices(image)
                .map_err(|error| AttachmentCleanupFailure {
                    inventory: Some(Box::new(error)),
                    detach: Vec::new(),
                    remaining_devices: Vec::new(),
                })?;
        let new_devices: BTreeSet<_> = after.difference(before).cloned().collect();
        let mut detach = Vec::new();
        for device in &new_devices {
            if let Err(error) = self.detach_image_device_unlocked(image, device, intent) {
                detach.push(AttachmentDetachFailure {
                    device: device.clone(),
                    error: Box::new(error),
                });
            }
        }

        let verified = match self.attached_whole_devices(image) {
            Ok(verified) => verified,
            Err(error) => {
                return Err(AttachmentCleanupFailure {
                    inventory: Some(Box::new(error)),
                    detach,
                    remaining_devices: Vec::new(),
                });
            }
        };
        let remaining_devices: Vec<String> = new_devices.intersection(&verified).cloned().collect();
        if detach.is_empty() && remaining_devices.is_empty() {
            Ok(())
        } else {
            Err(AttachmentCleanupFailure {
                inventory: None,
                detach,
                remaining_devices,
            })
        }
    }

    fn failed_attachment(
        &self,
        image: &Path,
        before: &BTreeSet<String>,
        primary: ApfsError,
        intent: DetachIntent,
    ) -> ApfsError {
        match self.cleanup_new_attachments(image, before, intent) {
            Ok(()) => primary,
            Err(cleanup) => ApfsError::AttachmentCleanupFailed {
                image: image.to_owned(),
                primary: Box::new(primary),
                cleanup,
            },
        }
    }

    fn failed_image_attachment(
        &self,
        path: &Path,
        before: &BTreeSet<String>,
        primary: ApfsError,
    ) -> ApfsError {
        match self.cleanup_new_attachments(path, before, DetachIntent::Release) {
            Ok(()) => self.cleanup_failed_image(path, primary),
            Err(cleanup) => ApfsError::AttachmentCleanupFailed {
                image: path.to_owned(),
                primary: Box::new(primary),
                cleanup,
            },
        }
    }
}

impl<R: CommandRunner, S: Sleeper> MacOsApfsBackend<R, S> {
    fn run_checked(
        &self,
        operation: &'static str,
        request: CommandRequest,
    ) -> Result<CommandOutput, ApfsError> {
        cowshed_os::timing::timed("apfs-command", format_args!("{operation}"), || {
            let output = self.runner.run(&request)?;
            if output.succeeded() {
                Ok(output)
            } else {
                Err(command_failed(operation, request, output))
            }
        })
    }

    /// Opt the read-write volume just mounted at `mount_point` out of fseventsd's persistent
    /// event log (01a_apfs.md, "The event log"): an empty `.fseventsd/no_log` at its root.
    /// fseventsd reads the marker when the volume mounts, so it governs the next mount and every
    /// clone. A log a root fseventsd created is root's to write, so only a mount that ignores
    /// ownership can replace it: that volume is mounted once more without ownership, its log
    /// replaced by the marker, and mounted back as asked. The mount is what the caller asked
    /// for; the log is host load, so a failure to opt out is reported and the volume stays
    /// mounted. Only a failure to mount it back is an error.
    fn opt_out_of_event_log(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        options: &str,
    ) -> Result<(), ApfsError> {
        match opt_out_of_event_log(mount_point) {
            Ok(EventLog::OptedOut) => Ok(()),
            Ok(EventLog::Inherited) => {
                self.retire_inherited_event_log(attachment, mount_point, options)
            }
            Ok(EventLog::Squatted) => {
                cowshed_os::eprintln!(
                    "cowshed: apfs {} has a {EVENT_LOG_DIRECTORY} that is not a directory, or a \
                 {EVENT_LOG_OPT_OUT} that is not an empty regular file; left as found, not \
                 written through",
                    mount_point.display()
                );
                Ok(())
            }
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: apfs {} keeps fseventsd's event log: {error}",
                    mount_point.display()
                );
                Ok(())
            }
        }
    }

    fn retire_inherited_event_log(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        options: &str,
    ) -> Result<(), ApfsError> {
        // Both mounts unmounted here are this call's own and not yet handed back, so whatever
        // holds one is a host daemon that opened the fresh volume (measured: `Resource busy`
        // within 40 ms of the mount, on a live host): wait out the detach grace, then force.
        let unmount = |operation| {
            self.unmount_volume_unlocked(attachment, DetachIntent::Release, operation)
                .map(|_| ())
        };
        if let Err(error) = unmount("unmount to retire an inherited event log") {
            cowshed_os::eprintln!(
                "cowshed: apfs {} keeps fseventsd's inherited event log: {error}",
                mount_point.display()
            );
            return Ok(());
        }
        let retired = self
            .run_checked(
                "mount ignoring ownership to retire an inherited event log",
                mount_request(attachment, "nobrowse,noowners", mount_point),
            )
            .and_then(|_| {
                let replaced =
                    retire_event_log(mount_point).map_err(|source| ApfsError::FileOperation {
                        operation: "opt an inherited event log out",
                        path: mount_point.join(EVENT_LOG_DIRECTORY),
                        source,
                    });
                let unmounted = unmount("unmount the ownership-ignoring mount");
                match (replaced, unmounted) {
                    (Ok(()), unmounted) => unmounted,
                    (Err(replaced), Ok(())) => Err(replaced),
                    (Err(replaced), Err(unmounted)) => {
                        cowshed_os::eprintln!(
                            "cowshed: apfs {} keeps fseventsd's inherited event log: {replaced}",
                            mount_point.display()
                        );
                        Err(unmounted)
                    }
                }
            });
        if let Err(error) = &retired {
            cowshed_os::eprintln!(
                "cowshed: apfs {} keeps fseventsd's inherited event log: {error}",
                mount_point.display()
            );
        }
        self.run_checked(
            "mount verified APFS volume",
            mount_request(attachment, options, mount_point),
        )?;
        if retired.is_ok() {
            cowshed_os::eprintln!(
                "cowshed: apfs {} retired fseventsd's inherited event log",
                mount_point.display()
            );
        }
        Ok(())
    }

    /// Create a blank sparse RAW image, attach it without mounting, format its whole device as one
    /// case-sensitive APFS volume (`newfs_apfs -e`) owned by the invoking user (`-U`/`-G`), verify
    /// it and detach it: the blank template mint's sequence (01a_apfs.md, "Images"), so no caller
    /// detaches a volume the template path would have checked first. Nothing here runs as root.
    fn create_raw(&self, path: &Path, request: &CreateImageRequest) -> Result<(), ApfsError> {
        validate_image_path(path)?;
        let _lease = self.image_lease(path)?;
        self.create_blank_unlocked(path, request)?;
        let whole_device = self.attach_and_format_unlocked(path, request)?;
        let attachment = self
            .verify_formatted_unlocked(path)
            .map_err(|primary| self.release_failed_format(path, &whole_device, primary))?;
        attachment.release_pin();
        self.detach_image_device_unlocked(path, &whole_device, DetachIntent::Release)
    }

    /// A blank image is a sparse file of the image's capacity: the kernel's RAW class attaches any
    /// file as a disk of its length, and `newfs_apfs` formats it (01a_apfs.md, "Images"). No disk
    /// tool runs. `create_new` refuses a path that exists: a staged name is minted once.
    fn create_blank_unlocked(
        &self,
        path: &Path,
        request: &CreateImageRequest,
    ) -> Result<(), ApfsError> {
        timed_apfs_step(apfs_step_leg(path), "blank-create", || {
            let file = OpenOptions::new().write(true).create_new(true).open(path)?;
            file.set_len(request.capacity.bytes())?;
            file.sync_all()
        })
        .map_err(|source| ApfsError::FileOperation {
            operation: "create sparse RAW image",
            path: path.to_owned(),
            source,
        })
    }

    /// Attach the blank image at `path` and format its whole device. Returns that whole device,
    /// still attached; every failure releases what this attach added and removes the image.
    fn attach_and_format_unlocked(
        &self,
        path: &Path,
        request: &CreateImageRequest,
    ) -> Result<String, ApfsError> {
        let attached_before = self.refuse_second_attach(path)?;

        let attach = in_kernel_attach(path, MountAccess::ReadWrite);
        let output = match timed_apfs_step(apfs_step_leg(path), "blank-attach", || {
            self.run_checked("attach blank RAW image", attach)
        }) {
            Ok(output) => output,
            Err(primary) => {
                return Err(self.failed_image_attachment(path, &attached_before, primary));
            }
        };
        let whole_device = match parse_blank_image_whole_device(&output.stdout) {
            Ok(device) => device,
            Err(primary) => {
                return Err(self.failed_image_attachment(path, &attached_before, primary));
            }
        };
        self.require_blank_attachment(path, &whole_device)?;
        let format = CommandRequest::new(
            NEWFS_APFS,
            [
                OsString::from("-U"),
                OsString::from(request.owner_uid.to_string()),
                OsString::from("-G"),
                OsString::from(request.owner_gid.to_string()),
                OsString::from("-e"),
                OsString::from("-v"),
                OsString::from(&request.volume_name),
                OsString::from(&whole_device),
            ],
        );
        if let Err(primary) = timed_apfs_step(apfs_step_leg(path), "format", || {
            self.run_checked("format RAW APFS volume", format)
        }) {
            return Err(self.release_failed_format(path, &whole_device, primary));
        }
        Ok(whole_device)
    }

    /// Release a just-formatted image's attachment after a failed step and remove the image.
    fn release_failed_format(
        &self,
        path: &Path,
        whole_device: &str,
        primary: ApfsError,
    ) -> ApfsError {
        match self.detach_image_device_unlocked(path, whole_device, DetachIntent::Release) {
            Ok(()) => self.cleanup_failed_image(path, primary),
            Err(detach) => ApfsError::ImageCreationAndCleanupFailed {
                primary: Box::new(primary),
                detach: Some(Box::new(detach)),
                remove: None,
            },
        }
    }

    /// The APFS volume formatting just put on `image`, read from the kernel's own record of the
    /// image, pinned against device reuse and checked with `fsck_apfs -q` exactly as a verified
    /// attach is, so the volume mounts through the same gate every other mount passes.
    fn verify_formatted_unlocked(&self, image: &Path) -> Result<AttachedImage, ApfsError> {
        let leg = apfs_step_leg(image);
        let attachment = match self.recovered_image_attachment(image)? {
            Some(RecoveredImageAttachment::Apfs(attachment)) => attachment,
            Some(RecoveredImageAttachment::Unformatted { whole_device, .. }) => {
                return Err(ApfsError::InvalidAttachmentInventory(format!(
                    "{} still shows only the unformatted {whole_device} after formatting",
                    image.display()
                )));
            }
            None => {
                return Err(ApfsError::InvalidAttachmentInventory(format!(
                    "{} is not attached after formatting",
                    image.display()
                )));
            }
        };
        let pin = self.pin_attached_volume(&attachment)?;
        let request = CommandRequest::new(
            FSCK_APFS,
            [
                OsString::from("-q"),
                OsString::from(raw_device_from(&attachment.volume_device)),
            ],
        );
        let output = timed_apfs_step(leg, "fsck", || self.runner.run(&request.clone()))?;
        if !output.succeeded() {
            return Err(ApfsError::VerificationFailed { request, output });
        }
        *attachment
            .pin
            .lock()
            .expect("attachment pin mutex poisoned") = pin;
        Ok(attachment)
    }

    fn cleanup_failed_image(&self, path: &Path, primary: ApfsError) -> ApfsError {
        let remove = match fs::remove_file(path) {
            Ok(()) => None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(source) => Some(ApfsError::FileOperation {
                operation: "remove failed RAW image",
                path: path.to_owned(),
                source,
            }),
        };
        if remove.is_none() {
            primary
        } else {
            ApfsError::ImageCreationAndCleanupFailed {
                primary: Box::new(primary),
                detach: None,
                remove: remove.map(Box::new),
            }
        }
    }

    /// Recover the one attachment a killed process may have left for `image`.
    ///
    /// This never attaches a second device. An absent mapping returns `None`; more than one
    /// mapping is ambiguous and fails closed. The caller decides whether an unmounted survivor
    /// must be detached and verified afresh before use.
    pub fn existing_attachment(&self, image: &Path) -> Result<Option<AttachedImage>, ApfsError> {
        match self.recovered_image_attachment(image)? {
            None => Ok(None),
            Some(RecoveredImageAttachment::Apfs(attachment)) => Ok(Some(attachment)),
            Some(RecoveredImageAttachment::Unformatted { image, .. }) => {
                Err(ApfsError::InvalidAttachmentInventory(format!(
                    "{} is still unformatted; an APFS volume is required",
                    image.display(),
                )))
            }
        }
    }

    /// Resolve a mount's existing attachment under its image lease and hand the raw pin to
    /// mount. A reseed may have replaced the device while the lease was being awaited.
    pub fn existing_attachment_for_mount(
        &self,
        image: &Path,
    ) -> Result<Option<AttachedImage>, ApfsError> {
        let _lease = self.image_lease(image)?;
        let Some(mut attachment) = self.existing_attachment(image)? else {
            return Ok(None);
        };
        *attachment
            .pin
            .get_mut()
            .expect("attachment pin mutex poisoned") = self.pin_attached_volume(&attachment)?;
        Ok(Some(attachment))
    }

    pub fn recovered_image_attachment(
        &self,
        image: &Path,
    ) -> Result<Option<RecoveredImageAttachment>, ApfsError> {
        validate_image_path(image)?;
        let image = attachment_inventory_path(image)?;
        existing_image_attachment(&image, &self.attachments_of(&image)?)
    }

    /// Every driver the kernel holds for `image`, however many, each as its whole device and
    /// every volume its container holds: what a release has to undo. Releasing an existing
    /// owned attachment is format-independent, including a retained re-adoption input.
    pub(crate) fn release_views(&self, image: &Path) -> Result<Vec<ReleaseView>, ApfsError> {
        let image = attachment_inventory_path(image)?;
        release_views_of(&image, &self.attachments_of(&image)?)
    }

    /// The backing file of every attached disk image, each once, by the path its attach was
    /// handed. A file deleted since is still listed: the kernel keeps the path it registered.
    pub fn attached_image_files(&self) -> Result<BTreeSet<PathBuf>, ApfsError> {
        Ok(self
            .attached_disk_images()?
            .into_iter()
            .filter_map(|attached| match attached.source {
                DiskImageSource::File(path) => Some(path),
                DiskImageSource::Url(_) => None,
            })
            .collect())
    }

    /// Detach every attachment the kernel holds for `image`, under its lease, each by the whole
    /// device that attachment is detached through, re-reading the image's own mapping before
    /// each one. A path held twice is released twice; a path held by nothing is already
    /// released. One attachment is one detach: target the image driver's physical whole
    /// device, not its synthesized APFS container. Nothing here unmounts first: a mounted
    /// volume's caller unmounts it ([`Self::unmount_released`]); an unmounted template has
    /// nothing to unmount.
    pub fn release_every_attachment(
        &self,
        image: &Path,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        let _lease = self.image_lease(image)?;
        let devices = self
            .release_views(image)?
            .into_iter()
            .map(|view| view.whole_device)
            .collect::<BTreeSet<_>>();
        for device in devices {
            self.detach_image_device_unlocked(image, &device, intent)?;
        }
        Ok(())
    }

    /// Release every currently owned attachment. The owner resolves each volume's current mount
    /// inside namespace admission; no caller's cached mounted-device set authorizes or skips it.
    ///
    /// An attachment another user made is refused before anything is unmounted: its detach
    /// would wait on an administrator prompt (01a_apfs.md, "Release authority").
    pub fn release_image(&self, image: &Path, intent: DetachIntent) -> Result<(), ApfsError> {
        let views = self.release_views(image)?;
        let euid = self.runner.caller_uid();
        for view in &views {
            let disk = view.whole_device.trim_start_matches("/dev/");
            release_authority::refuse_unowned(disk, view.owner_uid, euid).map_err(|refusal| {
                ApfsError::CommandRun(CommandRunError {
                    request: CommandRequest::new(
                        HDIUTIL,
                        ["detach", "-verbose", &view.whole_device],
                    ),
                    failure: CommandRunFailure::Release(refusal),
                })
            })?;
        }
        for view in views {
            for volume in view.release_volumes() {
                self.unmount_released(&volume, intent)?;
            }
        }
        self.release_every_attachment(image, intent)
    }

    /// The devices `image` holds before an attach, which must be none. The image driver refuses
    /// a second attach of one file, but not of a new file at the same path: once the file a live
    /// attachment registered is replaced, attaching the path again gives it two attachments, and
    /// every later lookup of that path (`matching image has multiple inventory entries`) refuses
    /// until both are detached by hand. A path already held is refused before anything attaches.
    fn refuse_second_attach(&self, image: &Path) -> Result<BTreeSet<String>, ApfsError> {
        let held = self.attached_whole_devices(image)?;
        if held.is_empty() {
            Ok(held)
        } else {
            Err(ApfsError::InvalidAttachmentInventory(format!(
                "{} is already attached as {held:?}; refusing to attach it a second time",
                image.display()
            )))
        }
    }

    /// Attach `image` without mounting it, answering the volume and container the attach itself
    /// reports ([`attached_apfs_volume`]). No Disk Arbitration query follows the attach.
    fn attach_without_mounting(
        &self,
        image: &Path,
        request: CommandRequest,
        cleanup: DetachIntent,
    ) -> Result<AttachedImage, ApfsError> {
        let attached_before = self.refuse_second_attach(image)?;
        let output = match self.run_checked("attach image without mounting", request) {
            Ok(output) => output,
            Err(primary) => {
                return Err(self.failed_attachment(image, &attached_before, primary, cleanup));
            }
        };
        let (whole_device, volume_device) = match parse_attachment_plist(&output.stdout) {
            Ok(attachment) => attachment,
            Err(primary) => {
                return Err(self.failed_attachment(image, &attached_before, primary, cleanup));
            }
        };
        Ok(AttachedImage {
            image: image.to_owned(),
            whole_device,
            volume_device,
            pin: std::sync::Mutex::new(None),
        })
    }

    fn verify_attachment(
        &self,
        image: &Path,
        request: CommandRequest,
        cleanup: DetachIntent,
    ) -> Result<AttachedImage, ApfsError> {
        let leg = apfs_step_leg(image);
        let (mut attachment, request, output, pin) = {
            let _lease = self.image_lease(image)?;
            let attachment = timed_apfs_step(leg, "attach", || {
                self.attach_without_mounting(image, request, cleanup)
            })?;
            // The same raw descriptor carries the exact-image proof from fsck through mount.
            // Losing the reported device refuses this attempt; it never authorizes another attach.
            let pin = self.pin_attached_volume(&attachment)?;
            let request = CommandRequest::new(
                FSCK_APFS,
                [
                    OsString::from("-q"),
                    OsString::from(raw_device_from(&attachment.volume_device)),
                ],
            );
            let output = timed_apfs_step(leg, "fsck", || self.runner.run(&request))?;
            (attachment, request, output, pin)
        };
        if output.succeeded() {
            *attachment
                .pin
                .get_mut()
                .expect("attachment pin mutex poisoned") = pin;
            return Ok(attachment);
        }
        drop(pin);

        match self.detach(&attachment, cleanup) {
            Ok(()) => Err(ApfsError::VerificationFailed { request, output }),
            Err(detach) => Err(ApfsError::VerificationAndDetachFailed {
                request,
                verification: output,
                detach: Box::new(detach),
            }),
        }
    }

    #[cfg(target_os = "macos")]
    pub fn existing_retained_attachment(
        &self,
        input: &RetainedInput<'_>,
    ) -> Result<Option<RetainedAttachment>, ApfsError> {
        let image = attachment_inventory_path(input.path())?;
        match existing_image_attachment(&image, &self.attachments_of(&image)?)? {
            None => Ok(None),
            Some(RecoveredImageAttachment::Apfs(attachment)) => {
                Ok(Some(RetainedAttachment(attachment)))
            }
            Some(RecoveredImageAttachment::Unformatted { image, .. }) => {
                Err(ApfsError::InvalidAttachmentInventory(format!(
                    "{} is still unformatted; a retained filesystem input requires APFS",
                    image.display(),
                )))
            }
        }
    }

    #[cfg(any(target_os = "macos", test))]
    pub fn attach_retained_verified(
        &self,
        input: &RetainedInput<'_>,
    ) -> Result<RetainedAttachment, ApfsError> {
        validate_image_path(input.path())?;
        self.verify_attachment(
            input.path(),
            in_kernel_attach(input.path(), MountAccess::ReadOnly),
            DetachIntent::WhenIdle,
        )
        .map(RetainedAttachment)
    }

    #[cfg(target_os = "macos")]
    pub fn mount_retained_read_only(
        &self,
        attachment: &RetainedAttachment,
        mount: &Path,
    ) -> Result<(), ApfsError> {
        self.mount(&attachment.0, mount, MountAccess::ReadOnly, false)
    }

    #[cfg(target_os = "macos")]
    pub fn unmount_retained_when_idle(
        &self,
        attachment: &RetainedAttachment,
    ) -> Result<(), ApfsError> {
        self.unmount_verified(&attachment.0, DetachIntent::WhenIdle)
    }

    #[cfg(target_os = "macos")]
    pub fn detach_retained_when_idle(
        &self,
        attachment: &RetainedAttachment,
    ) -> Result<(), ApfsError> {
        self.detach(&attachment.0, DetachIntent::WhenIdle)
    }

    /// Detach an owned whole device under `intent`.
    ///
    /// A `Release` waits out a dissent and then forces, because the volume is cowshed's own and
    /// is finished with; a `WithinGrace` waits it out the same way and hands back the dissent
    /// still standing when the grace ends; a `WhenIdle` hands the dissent straight back so its
    /// verb can refuse. Only a dissent is retried — an invalid device or a missing tool fails
    /// immediately either way.
    fn detach_device_checked(
        &self,
        image: &Path,
        target: &str,
        intent: DetachIntent,
    ) -> Result<bool, ApfsError> {
        if !is_kernel_device_path(target) {
            return Err(ApfsError::InvalidDetachTarget(PathBuf::from(target)));
        }
        let mut waited = Duration::ZERO;
        let mut started = None;
        loop {
            match self.detach_once(image, target, false) {
                Ok(detached) => return Ok(detached),
                Err(error) if detach_was_dissented(&error) => {
                    let started = started.get_or_insert_with(std::time::Instant::now);
                    let expired = self.grace.expired(waited, self.sleeper.elapsed(*started));
                    match (intent, expired) {
                        (DetachIntent::WhenIdle, _) | (DetachIntent::WithinGrace, true) => {
                            return Err(error);
                        }
                        (DetachIntent::Release, true) => {
                            return self.detach_once(image, target, true);
                        }
                        (DetachIntent::Release | DetachIntent::WithinGrace, false) => {
                            self.sleeper.sleep(self.grace.poll);
                            waited = waited.saturating_add(self.grace.poll);
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// One `hdiutil detach -verbose`. hdiutil's verbose account is the only record of what an
    /// eject went through, so it is kept as an attribute of the detach step whenever the eject
    /// was refused (`dissent=`, DiskArbitration's reason) or slower than the normal floor
    /// (`waited=`, at least [`SLOW_DETACH`]); a fast, accepted eject adds nothing to the trace.
    fn detach_once(&self, image: &Path, target: &str, force: bool) -> Result<bool, ApfsError> {
        let mut args = Vec::with_capacity(3 + usize::from(force));
        args.push(OsString::from("detach"));
        if force {
            args.push(OsString::from("-force"));
        }
        args.push(OsString::from("-verbose"));
        args.push(OsString::from(target));
        let started = std::time::Instant::now();
        let request = CommandRequest::new(HDIUTIL, args);
        cowshed_os::timing::attribute(
            "apfs-command",
            format_args!("detach image"),
            "image",
            image.display(),
        );
        let result =
            cowshed_os::timing::timed("apfs-command", format_args!("detach image"), || {
                let output = self.runner.run_guarded(&request, || {
                    let held = self.attached_whole_devices(image)?;
                    if held.is_empty() {
                        return Ok(false);
                    }
                    if !held.contains(target) {
                        return Err(ApfsError::InvalidAttachmentInventory(format!(
                            "{} still holds {held:?}, not {target}; retaining the attached \
                         image rather than claiming it was released",
                            image.display()
                        )));
                    }
                    Ok(true)
                })?;
                match output {
                    Some(output) if !output.succeeded() => {
                        Err(command_failed("detach image", request, output))
                    }
                    output => Ok(output),
                }
            });
        let account = match &result {
            Ok(Some(output)) => {
                (started.elapsed() >= SLOW_DETACH).then_some(("waited", output.stderr.as_slice()))
            }
            Err(ApfsError::CommandFailed { output, .. }) => {
                Some(("dissent", output.stderr.as_slice()))
            }
            Ok(None) | Err(_) => None,
        };
        if let Some((key, stderr)) = account {
            cowshed_os::timing::attribute(
                "apfs-command",
                format_args!("detach image"),
                key,
                one_line(stderr),
            );
        }
        result.map(|output| output.is_some())
    }

    /// Detach `whole_device` only while the kernel still shows `image` holding it.
    ///
    /// A `/dev/diskN` name read earlier is not an identity: once its image detaches, the kernel
    /// hands the same name to the next attach anywhere on the host, and detaching the recorded
    /// name would take that image away from its owner. The backing file the image driver
    /// registered at attach time is the identity, so the device is re-read against it
    /// immediately before every detach. The kernel inventory's absences are authoritative, so an
    /// image it does not hold is already released; a positive conflicting mapping is a refusal,
    /// never release success. Its caller holds the image lease while the image identity is read
    /// and ejected.
    fn detach_image_device_unlocked(
        &self,
        image: &Path,
        whole_device: &str,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        if self.detach_device_checked(image, whole_device, intent)? {
            self.settle_detached_device(image, whole_device);
        }
        Ok(())
    }

    /// The image driver registers an attach's media with the kernel before `diskutil image
    /// attach` reports them, so formatting proceeds only when the reported whole device is
    /// already this image's, and only this image's. Absence is a contradiction, not lag: it
    /// leaves the image and attachment intact, since an unproven attachment is not authority to
    /// format, eject or remove either.
    fn require_blank_attachment(&self, image: &Path, whole_device: &str) -> Result<(), ApfsError> {
        let inventory = self.attachment_inventory(image)?;
        if !inventory.matched {
            return Err(ApfsError::InvalidAttachmentInventory(format!(
                "{} attach of {whole_device} is not registered to the image in the kernel; retaining the image and attachment",
                image.display(),
            )));
        }
        if inventory.devices.len() == 1 && inventory.devices.contains(whole_device) {
            Ok(())
        } else {
            Err(ApfsError::InvalidAttachmentInventory(format!(
                "{} no longer owns {whole_device} (holds {:?}); refusing to format a foreign device",
                image.display(),
                inventory.devices,
            )))
        }
    }

    /// Bounded, logged confirmation that the image's detached whole device left the kernel
    /// inventory before any later attach. The poll reads the image's own mapping, not the
    /// host-wide device list: image leases let another image's attach take the freed `diskN`
    /// at once, and that reuse is a departure of this image's device, not a lingering one.
    ///
    /// Soft by design: the detach already succeeded, so a lingering or unreadable inventory is
    /// logged and the operation proceeds exactly as it would have without the check. Failing a
    /// detach that succeeded over the media's termination lag would turn a slow departure under
    /// load into a user-facing failure.
    fn settle_detached_device(&self, image: &Path, whole_device: &str) {
        let mut waited = Duration::ZERO;
        loop {
            match self.attached_whole_devices(image) {
                Ok(held) if !held.contains(whole_device) => {
                    cowshed_os::eprintln!(
                        "cowshed: apfs detach-settle {whole_device} departed waited={waited:?}"
                    );
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: apfs detach-settle {whole_device} inventory unreadable ({error}); proceeding without confirmation waited={waited:?}"
                    );
                    return;
                }
            }
            if waited >= self.settle.total {
                cowshed_os::eprintln!(
                    "cowshed: apfs detach-settle {whole_device} STILL PRESENT after {:?}; proceeding without confirmation",
                    self.settle.total
                );
                return;
            }
            self.sleeper.sleep(self.settle.poll);
            waited = waited.saturating_add(self.settle.poll);
        }
    }
}

/// The `.dmg` path a creation request stages its image at, once the request itself is sound.
fn staged_image_path(request: &CreateImageRequest) -> Result<PathBuf, ApfsError> {
    if request.staged_stem.extension().is_some() || request.staged_stem.file_name().is_none() {
        return Err(ApfsError::InvalidStagedStem(request.staged_stem.clone()));
    }
    if !is_valid_apfs_volume_name(&request.volume_name) {
        return Err(ApfsError::InvalidCreateRequest(
            "volume name must be path-safe and at most 255 bytes",
        ));
    }
    Ok(request.staged_stem.with_extension(IMAGE_EXTENSION))
}

impl<R: CommandRunner, S: Sleeper> ApfsBackend for MacOsApfsBackend<R, S> {
    fn create_staged_image(&self, request: &CreateImageRequest) -> Result<PathBuf, ApfsError> {
        let path = staged_image_path(request)?;
        self.create_raw(&path, request)?;
        Ok(path)
    }

    fn create_blank_image(&self, request: &CreateImageRequest) -> Result<PathBuf, ApfsError> {
        let path = staged_image_path(request)?;
        validate_image_path(&path)?;
        let _lease = self.image_lease(&path)?;
        self.create_blank_unlocked(&path, request)?;
        Ok(path)
    }

    fn format_attached(
        &self,
        image: &Path,
        request: &CreateImageRequest,
    ) -> Result<AttachedImage, ApfsError> {
        if !is_valid_apfs_volume_name(&request.volume_name) {
            return Err(ApfsError::InvalidCreateRequest(
                "volume name must be path-safe and at most 255 bytes",
            ));
        }
        validate_image_path(image)?;
        let _lease = self.image_lease(image)?;
        let whole_device = self.attach_and_format_unlocked(image, request)?;
        self.verify_formatted_unlocked(image)
            .map_err(|primary| self.release_failed_format(image, &whole_device, primary))
    }

    fn sync_for_freshness(
        &self,
        image: &Path,
        mount_point: Option<&Path>,
    ) -> Result<(), ApfsError> {
        if let Some(mount_point) = mount_point.filter(|mount_point| is_mount_root(mount_point)) {
            sync_volume(mount_point).map_err(|source| ApfsError::FileOperation {
                operation: "sync source volume",
                path: mount_point.to_owned(),
                source,
            })?;
        }
        fs::File::open(image)
            .and_then(|file| {
                use std::os::fd::AsRawFd;
                // SAFETY: `file` owns a live descriptor for the duration of the call.
                if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            })
            .map_err(|source| ApfsError::FileOperation {
                operation: "sync source image",
                path: image.to_owned(),
                source,
            })
    }

    fn clone_image(
        &self,
        source: &Path,
        destination: &Path,
        capture: ImageCapture,
    ) -> Result<(), CloneFileError> {
        validate_clone_path(source)?;
        validate_clone_path(destination)?;
        #[cfg(target_os = "macos")]
        {
            let leg = apfs_step_leg(destination);
            // Cowshed attaches only under the image's lease, so a detached capture holds the
            // source's lease through the clone: an in-kernel attachment takes no file lock
            // that the source's shared lock could exclude (01a_apfs.md, "Images").
            let _lease = match capture {
                ImageCapture::Detached => image_identity(source)
                    .and_then(|identity| self.runner.image_lease(&identity))
                    .map_err(|error| CloneFileError::Io {
                        source_path: source.to_owned(),
                        destination_path: destination.to_owned(),
                        source: error,
                    })?,
                ImageCapture::Live => None,
            };
            let held = timed_apfs_step(leg, "hold-source", || {
                ImageSource::hold(source, destination, capture)
            })?;
            timed_apfs_step(leg, "clonefile", || held.clone_to(destination))
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = capture;
            Err(CloneFileError::UnsupportedPlatform)
        }
    }

    fn rename_volume(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        volume_name: &str,
    ) -> Result<(), ApfsError> {
        if !is_valid_apfs_volume_name(volume_name) {
            return Err(ApfsError::InvalidVolumeName(volume_name.to_owned()));
        }
        let _lease = self.image_lease(&attachment.image)?;
        let _pin = self.pin_attached_volume(attachment)?;
        let held =
            self.runner
                .hold_mount(mount_point)
                .map_err(|source| ApfsError::FileOperation {
                    operation: "hold the renamed volume's mountpoint",
                    path: mount_point.to_owned(),
                    source,
                })?;
        let root =
            OwnedVolumeRoot::of_attachment(held, mount_point, attachment).map_err(|held| {
                ApfsError::RenameTargetNotMounted {
                    image: attachment.image.clone(),
                    volume_device: attachment.volume_device.clone(),
                    mount_point: mount_point.to_owned(),
                    found: held.source_device().to_owned(),
                }
            })?;
        self.name_owned_volume(&root, volume_name)
    }

    fn attach_verified(&self, image: &Path) -> Result<AttachedImage, ApfsError> {
        validate_image_path(image)?;
        self.verify_attachment(
            image,
            in_kernel_attach(image, MountAccess::ReadWrite),
            DetachIntent::Release,
        )
    }

    fn attach_mounted(
        &self,
        image: &Path,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<AttachedImage, ApfsError> {
        validate_image_path(image)?;
        let _lease = self.image_lease(image)?;
        fs::create_dir_all(mount_point).map_err(|source| ApfsError::FileOperation {
            operation: "create mount point",
            path: mount_point.to_owned(),
            source,
        })?;
        let attachment = timed_apfs_step(apfs_step_leg(image), "attach", || {
            self.attach_without_mounting(
                image,
                in_kernel_attach(image, access),
                DetachIntent::Release,
            )
        })?;
        let mut mount_attempted = false;
        let mounted = (|| {
            let _pin = self.pin_attached_volume(&attachment)?;
            let request = CommandRequest::new(
                FSCK_APFS,
                [
                    OsString::from("-q"),
                    OsString::from(raw_device_from(&attachment.volume_device)),
                ],
            );
            let output =
                timed_apfs_step(apfs_step_leg(image), "fsck", || self.runner.run(&request))?;
            if !output.succeeded() {
                return Err(ApfsError::VerificationFailed { request, output });
            }
            let options = mount_options(access, browse);
            mount_attempted = true;
            timed_apfs_step(apfs_step_leg(image), "mount", || {
                self.run_checked(
                    "mount verified APFS volume",
                    mount_request(&attachment, options, mount_point),
                )
            })?;
            let held =
                self.runner
                    .hold_mount(mount_point)
                    .map_err(|source| ApfsError::FileOperation {
                        operation: "verify pinned APFS mount point",
                        path: mount_point.to_owned(),
                        source,
                    })?;
            if !held.is_root() || held.source_device() != attachment.volume_device {
                return Err(ApfsError::InvalidAttachmentInventory(format!(
                    "mount_apfs did not mount {} at {}: found {} (mount root: {})",
                    attachment.volume_device,
                    mount_point.display(),
                    held.source_device(),
                    held.is_root(),
                )));
            }
            drop(held);
            if access == MountAccess::ReadWrite {
                self.opt_out_of_event_log(&attachment, mount_point, options)?;
            }
            Ok(())
        })();
        match mounted {
            Ok(()) => Ok(attachment),
            Err(primary) => {
                if mount_attempted {
                    // Even a refused mount may have changed the namespace. Re-prove and unmount
                    // only this attachment's exact volume before releasing its physical image.
                    let unmounted = self.pin_attached_volume(&attachment).and_then(|_pin| {
                        self.unmount_volume_unlocked(
                            &attachment,
                            DetachIntent::Release,
                            "unmount failed APFS mount",
                        )
                    });
                    if let Err(error) = unmounted {
                        return Err(ApfsError::AttachmentCleanupFailed {
                            image: image.to_owned(),
                            primary: Box::new(primary),
                            cleanup: AttachmentCleanupFailure {
                                inventory: None,
                                detach: vec![AttachmentDetachFailure {
                                    device: attachment.volume_device.clone(),
                                    error: Box::new(error),
                                }],
                                remaining_devices: vec![attachment.whole_device.clone()],
                            },
                        });
                    }
                }
                Err(self.failed_attachment(image, &BTreeSet::new(), primary, DetachIntent::Release))
            }
        }
    }

    fn mount(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsError> {
        self.prepare_mount(attachment)?
            .mount(mount_point, access, browse)
    }

    fn detach(&self, attachment: &AttachedImage, intent: DetachIntent) -> Result<(), ApfsError> {
        let _lease = self.image_lease(&attachment.image)?;
        attachment.release_pin();
        self.detach_image_device_unlocked(&attachment.image, &attachment.whole_device, intent)
    }

    fn delete_image(&self, image: &Path) -> Result<(), ApfsError> {
        validate_image_path(image)?;
        match fs::remove_file(image) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ApfsError::FileOperation {
                operation: "delete image",
                path: image.to_owned(),
                source,
            }),
        }
    }

    fn image_capacity(&self, image: &Path) -> Result<ImageCapacity, ApfsError> {
        validate_image_path(image)?;
        #[cfg(target_os = "macos")]
        let capacity = raw::capacity(image);
        #[cfg(not(target_os = "macos"))]
        let capacity = Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RAW disk images exist only on macOS",
        ));
        capacity.map_err(|source| ApfsError::FileOperation {
            operation: "read RAW image capacity",
            path: image.to_owned(),
            source,
        })
    }

    fn resize_image(&self, image: &Path, capacity: ImageCapacity) -> Result<(), ApfsError> {
        validate_image_path(image)?;
        let _lease = self.image_lease(image)?;
        timed_apfs_step(apfs_step_leg(image), "grow-image", || {
            self.runner
                .grow_image(image, capacity)
                .map_err(|source| ApfsError::FileOperation {
                    operation: "grow RAW image",
                    path: image.to_owned(),
                    source,
                })
        })
    }

    fn grow_container(&self, attachment: &AttachedImage) -> Result<(), ApfsError> {
        // The container reference is the parent of the APFS volume device — the synthesized
        // container disk — never the image's own whole device, which is the physical store;
        // `resizeContainer` is handed the container reference.
        let container = whole_device_from(&attachment.volume_device)
            .ok_or_else(|| ApfsError::InvalidVolumeDevice(attachment.volume_device.clone()))?;
        // diskutil's writer refuses EBUSY while the raw read descriptor is open.
        attachment.release_pin();
        let request = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("apfs"),
                OsString::from("resizeContainer"),
                OsString::from(&container),
                OsString::from("0"),
            ],
        );
        // Timed like every other disk-tool call: `resizeContainer` queues on storagekitd too.
        cowshed_os::timing::timed("apfs-command", format_args!("grow APFS container"), || {
            let output = self.runner.run(&request)?;
            if output.succeeded() || container_already_spans_image(&output) {
                Ok(())
            } else {
                Err(ApfsError::CommandFailed {
                    operation: "grow APFS container into image",
                    request,
                    output,
                })
            }
        })
    }

    fn attached_capacity(&self, image: &Path) -> Result<ImageCapacity, ApfsError> {
        let image = attachment_inventory_path(image)?;
        attachment_capacity(&image, &self.attachments_of(&image)?)
    }
}

fn container_already_spans_image(output: &CommandOutput) -> bool {
    CONTAINER_ALREADY_SPANS_IMAGE.iter().any(|code| {
        contains_bytes(&output.stdout, code.as_bytes())
            || contains_bytes(&output.stderr, code.as_bytes())
    })
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// The disk-images framework's report that its XPC helper never answered, as `diskutil image`
/// and `hdiutil` print it: "Error: Couldn’t communicate with a helper application." The match
/// skips the apostrophe, which the framework spells U+2019.
const DISK_IMAGE_HELPER_UNREACHABLE: &[u8] = b"t communicate with a helper application";

/// A failed command, typed by what failed: one whose disk-image framework lost its helper carries
/// the host load at the failure; every other failure is the command's own.
fn command_failed(
    operation: &'static str,
    request: CommandRequest,
    output: CommandOutput,
) -> ApfsError {
    if (request.program == Path::new(DISKUTIL) || request.program == Path::new(HDIUTIL))
        && contains_bytes(&output.stderr, DISK_IMAGE_HELPER_UNREACHABLE)
    {
        ApfsError::DiskImageHelperUnreachable(Box::new(DiskImageHelperFailure {
            operation,
            request,
            output,
            load: HostLoad::read(),
        }))
    } else {
        ApfsError::CommandFailed {
            operation,
            request,
            output,
        }
    }
}

/// A command that creates or releases a disk-image device: a DiskImages2 verb (`diskutil image …`
/// or any `hdiutil`) or a `diskutil eject`, which releases an image through Disk Arbitration. An
/// eject is classed this way whatever it targets: a physical disk's eject merely waits too.
fn is_disk_image_command(request: &CommandRequest) -> bool {
    request.program == Path::new(HDIUTIL)
        || (request.program == Path::new(DISKUTIL)
            && request
                .args
                .first()
                .is_some_and(|verb| verb == "image" || verb == "eject"))
}

/// One walk of the kernel inventory for one image: whether it is attached, its whole devices,
/// and its capacity. Every projection reads this walk, so none can drift about which attachment
/// is the image's.
struct AttachmentInventory {
    devices: BTreeSet<String>,
    capacity: Option<ImageCapacity>,
    matched: bool,
}

fn image_inventory(
    image: &Path,
    images: &[AttachedDiskImage],
) -> Result<AttachmentInventory, ApfsError> {
    let mut devices = BTreeSet::new();
    let mut capacity = None;
    let mut matched = false;
    for attached in images
        .iter()
        .filter(|attached| attached.is_backed_by(image))
    {
        matched = true;
        let mut roots = 0usize;
        for media in &attached.media {
            let device = device_path(&media.device).ok_or_else(|| {
                ApfsError::InvalidAttachmentInventory(format!(
                    "matching image has an invalid device {:?}",
                    media.device
                ))
            })?;
            if device_depth(&device) == 0 {
                roots += 1;
                devices.insert(device);
            }
        }
        if roots == 0 {
            return Err(ApfsError::InvalidAttachmentInventory(
                "matching image has no canonical whole device".into(),
            ));
        }
        if let Some(observed) = attached.capacity {
            if capacity.is_some_and(|previous| previous != observed) {
                return Err(ApfsError::InvalidAttachmentInventory(
                    "matching image is attached twice at different capacities".into(),
                ));
            }
            capacity = Some(observed);
        }
    }
    Ok(AttachmentInventory {
        devices,
        capacity,
        matched,
    })
}

fn attachment_capacity(
    image: &Path,
    images: &[AttachedDiskImage],
) -> Result<ImageCapacity, ApfsError> {
    let inventory = image_inventory(image, images)?;
    if !inventory.matched {
        return Err(ApfsError::ImageNotAttached(image.to_owned()));
    }
    inventory.capacity.ok_or_else(|| {
        ApfsError::InvalidAttachmentInventory(
            "matching image has no registered whole-device size".into(),
        )
    })
}

/// The mount helper's already-in-progress refusal is an invitation to observe, not to retry.
/// Unrelated failures remain failures even if a matching mount appears afterwards.
pub fn mount_is_in_progress(error: &ApfsError) -> bool {
    matches!(error, ApfsError::CommandFailed { request, output, .. }
        if request.program == Path::new(MOUNT_APFS)
            && contains_bytes(&output.stderr, b"Operation already in progress"))
}

/// Whether a failed detach failed because something still holds the volume.
///
/// The image driver reports EBUSY plus its resource-busy diagnostic. Anything else — a bad
/// device, a missing tool, a spawn failure — is not a dissent and must not be waited on.
pub fn detach_was_dissented(error: &ApfsError) -> bool {
    if let ApfsError::UnmountFailed { source, .. } = error {
        return matches!(source, NativeUnmountError::Busy(_));
    }
    if let ApfsError::CaptureSourceBusy { refusal, .. } = error {
        return detach_was_dissented(refusal);
    }
    let ApfsError::CommandFailed {
        request, output, ..
    } = error
    else {
        return false;
    };
    (request.program == Path::new(HDIUTIL)
        && request.args.first().is_some_and(|arg| arg == "detach")
        && output.status == ProcessStatus::Exit(libc::EBUSY)
        && contains_bytes(&output.stderr, b"Resource busy"))
        || (request.program == Path::new(DISKUTIL)
            && request.args.first().is_some_and(|arg| arg == "unmount")
            && output.status == ProcessStatus::Exit(1)
            && contains_bytes(&output.stderr, b" failed to unmount: dissented by PID "))
}

fn mount_options(access: MountAccess, browse: bool) -> &'static str {
    match (access, browse) {
        (MountAccess::ReadWrite, false) => "nobrowse,owners,noatime",
        (MountAccess::ReadWrite, true) => "owners,noatime",
        (MountAccess::ReadOnly, false) => "rdonly,nobrowse,owners,noatime",
        (MountAccess::ReadOnly, true) => "rdonly,owners,noatime",
    }
}

fn mount_request(attachment: &AttachedImage, options: &str, mount_point: &Path) -> CommandRequest {
    CommandRequest::new(
        MOUNT_APFS,
        [
            OsString::from("-o"),
            OsString::from(options),
            OsString::from(&attachment.volume_device),
            mount_point.as_os_str().to_owned(),
        ],
    )
}

/// fseventsd's directory at a volume's root: its persistent event log, or, holding an empty
/// [`EVENT_LOG_OPT_OUT`], the volume's refusal of one.
pub const EVENT_LOG_DIRECTORY: &str = ".fseventsd";
/// The marker fseventsd reads when a volume mounts: present, it keeps no log of the volume and
/// still delivers the volume's live events to every stream.
pub const EVENT_LOG_OPT_OUT: &str = "no_log";
/// macOS's Spotlight opt-out marker. Cowshed writes no Spotlight policy, but volumes adopted while
/// it planted this file (until 2026-10-25) carry it at their root and their clones inherit it, so,
/// like [`VOLUME_METADATA`], excludes, copies and retirements pass over it as the volume's, not the
/// repository's.
pub const SPOTLIGHT_OPT_OUT: &str = ".metadata_never_index";

/// What macOS keeps at every volume's root for itself: the event log, the Spotlight index, the
/// trash, temporary items and document revisions. They describe the volume they live on, not
/// the repository, every mount may grow them (some root-owned and not this user's to empty), and
/// a checkout mounted at a volume's root holds them beside its files.
pub const VOLUME_METADATA: [&str; 5] = [
    ".DocumentRevisions-V100",
    ".Spotlight-V100",
    ".TemporaryItems",
    ".Trashes",
    EVENT_LOG_DIRECTORY,
];

/// Where a mounted volume stands with fseventsd's persistent event log.
#[derive(Debug, Eq, PartialEq)]
enum EventLog {
    /// `.fseventsd` is a real directory holding an empty regular `no_log`: no log from the next
    /// mount.
    OptedOut,
    /// `.fseventsd` is a directory this user may not search: a log a root fseventsd created
    /// when the volume, or the one it was cloned from, mounted without the marker.
    Inherited,
    /// A symlink or file holds `.fseventsd`, or anything but an empty regular file holds the
    /// marker's name. Nothing is written through it, and nothing is removed: it is reported.
    Squatted,
}

/// [`EVENT_LOG_DIRECTORY`] and [`EVENT_LOG_OPT_OUT`] as the path components the fd-relative
/// calls take.
const EVENT_LOG_DIRECTORY_C: &std::ffi::CStr = c".fseventsd";
const EVENT_LOG_OPT_OUT_C: &std::ffi::CStr = c"no_log";
/// Opening a directory for lookups alone needs only search permission: root's 0700 refuses
/// it, the 0711 a retirement leaves grants it. Linux names the same access `O_PATH`.
#[cfg(target_vendor = "apple")]
const SEARCH_ONLY: libc::c_int = libc::O_SEARCH;
#[cfg(not(target_vendor = "apple"))]
const SEARCH_ONLY: libc::c_int = libc::O_PATH;

/// Open `name` under `directory` (or the absolute `name` itself) as a directory with `access`,
/// never through a symlink.
fn open_directory_at(
    directory: Option<&File>,
    name: &std::ffi::CStr,
    access: libc::c_int,
) -> io::Result<File> {
    use std::os::fd::FromRawFd;
    let flags = access | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: `name` is NUL-terminated and outlives the call; `directory`, when given, is an
    // open directory fd. A non-negative answer is a new descriptor this File then owns.
    let fd = unsafe {
        match directory {
            Some(directory) => libc::openat(directory.as_raw_fd(), name.as_ptr(), flags),
            None => libc::open(name.as_ptr(), flags),
        }
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a fresh descriptor nothing else owns.
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// The volume root at `mount_point`, opened without following a symlink.
fn open_volume_root(mount_point: &Path) -> io::Result<File> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(mount_point.as_os_str().as_bytes())?;
    open_directory_at(None, &path, libc::O_RDONLY)
}

/// Plant the opt-out marker under `mount_point` unless it is there, answering where the volume
/// stands. Every step is relative to a descriptor opened without following a symlink, so a
/// `.fseventsd` swapped for a symlink mid-way is refused, never written through.
fn opt_out_of_event_log(mount_point: &Path) -> io::Result<EventLog> {
    let root = open_volume_root(mount_point)?;
    // SAFETY: `root` is an open directory fd and the name a NUL-terminated component.
    if unsafe { libc::mkdirat(root.as_raw_fd(), EVENT_LOG_DIRECTORY_C.as_ptr(), 0o700) } != 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error);
        }
    }
    match open_directory_at(Some(&root), EVENT_LOG_DIRECTORY_C, SEARCH_ONLY) {
        Ok(directory) => opt_out_in(&directory),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => Ok(EventLog::Inherited),
        Err(error) if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) => {
            Ok(EventLog::Squatted)
        }
        Err(error) => Err(error),
    }
}

/// Plant the marker in the open `.fseventsd`, exclusively, unless an empty regular one is there.
fn opt_out_in(directory: &File) -> io::Result<EventLog> {
    Ok(match plant_empty_marker(directory, EVENT_LOG_OPT_OUT_C)? {
        Marker::Planted => EventLog::OptedOut,
        Marker::Refused => EventLog::Inherited,
        Marker::Squatted => EventLog::Squatted,
    })
}

/// Where one empty marker file stands.
#[derive(Debug, Eq, PartialEq)]
enum Marker {
    /// An empty regular file holds the name, planted now or before.
    Planted,
    /// The directory refuses this user a new entry.
    Refused,
    /// Anything but an empty regular file holds the name: left as found.
    Squatted,
}

/// Plant the empty `marker` in the open `directory`, exclusively and never through a symlink,
/// unless an empty regular one is there.
fn plant_empty_marker(directory: &File, marker: &std::ffi::CStr) -> io::Result<Marker> {
    let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: `directory` is an open directory fd and `marker` a NUL-terminated component. A
    // non-negative answer is a new descriptor, closed at once.
    let fd = unsafe { libc::openat(directory.as_raw_fd(), marker.as_ptr(), flags, 0o644) };
    if fd >= 0 {
        // SAFETY: `fd` is the fresh descriptor just opened.
        unsafe { libc::close(fd) };
        return Ok(Marker::Planted);
    }
    let error = io::Error::last_os_error();
    match error.kind() {
        io::ErrorKind::PermissionDenied => Ok(Marker::Refused),
        io::ErrorKind::AlreadyExists => {
            // SAFETY: an all-zero `stat` is a valid buffer for `fstatat` to fill.
            let mut stat: libc::stat = unsafe { std::mem::zeroed() };
            // SAFETY: as above; `AT_SYMLINK_NOFOLLOW` reports a symlink as itself.
            let found = unsafe {
                libc::fstatat(
                    directory.as_raw_fd(),
                    marker.as_ptr(),
                    &mut stat,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if found != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(
                if stat.st_mode & libc::S_IFMT == libc::S_IFREG && stat.st_size == 0 {
                    Marker::Planted
                } else {
                    Marker::Squatted
                },
            )
        }
        _ => Err(error),
    }
}

/// Opt root's log out under a mount that ignores ownership, where its directory is the user's
/// to change. The log stays, records and all: the marker joins it, and the directory's mode
/// becomes 0711, so under every later mount that honors ownership its owner can still look the
/// marker up — root's 0700 would refuse the lookup, and each such mount would find the log
/// inherited and retire it again — while its records stay unlisted. The marker goes first: only
/// a log it opted out has its mode relaxed, so a squatted marker leaves root's 0700 as found.
/// The marker and the mode go through one descriptor opened without following a symlink.
fn retire_event_log(mount_point: &Path) -> io::Result<()> {
    let root = open_volume_root(mount_point)?;
    let directory = open_directory_at(Some(&root), EVENT_LOG_DIRECTORY_C, libc::O_RDONLY)?;
    match opt_out_in(&directory)? {
        EventLog::OptedOut => {}
        state => {
            return Err(io::Error::other(format!(
                "the event log stayed {state:?} under a mount that ignores ownership"
            )));
        }
    }
    // SAFETY: `directory` is an open directory fd.
    if unsafe { libc::fchmod(directory.as_raw_fd(), 0o711) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The identity an image's lease and attachment inventory key on: its absolute path, made
/// absolute against the working directory and never resolved through a link. The lease file
/// hashes it verbatim ([`image_lease_path`]), so every lease of one image goes through here.
fn image_identity(image: &Path) -> io::Result<PathBuf> {
    std::path::absolute(image)
}

fn attachment_inventory_path(image: &Path) -> Result<PathBuf, ApfsError> {
    image_identity(image).map_err(|source| ApfsError::FileOperation {
        operation: "resolve attachment inventory path",
        path: image.to_owned(),
        source,
    })
}

/// The one attachment the kernel holds for `image`, if any. A second one is ambiguous: nothing
/// says which device a caller that expects one should use, so it refuses.
fn existing_image_attachment(
    image: &Path,
    images: &[AttachedDiskImage],
) -> Result<Option<RecoveredImageAttachment>, ApfsError> {
    let mut attachments = image_attachments(image, images)?;
    if attachments.len() > 1 {
        return Err(ApfsError::InvalidAttachmentInventory(
            "matching image has multiple inventory entries".into(),
        ));
    }
    Ok(attachments.pop())
}

/// Each media node an attached image reports: its device as a `/dev/` path, and its content hint.
fn attachment_entities(attached: &AttachedDiskImage) -> Vec<(String, String)> {
    attached
        .media
        .iter()
        .map(|media| {
            (
                device_path(&media.device).unwrap_or_else(|| media.device.clone()),
                media.content.clone(),
            )
        })
        .collect()
}

/// The whole device of an attachment that is nothing but one unformatted whole device.
fn unformatted_whole_device(entities: &[(String, String)]) -> Option<&str> {
    match entities {
        [(device, hint)]
            if device_depth(device) == 0
                && is_kernel_device_path(device)
                && (hint.is_empty() || hint == "GUID_partition_scheme") =>
        {
            Some(device)
        }
        _ => None,
    }
}

/// Every attachment the kernel holds for `image`, in inventory order, each as the one volume it
/// exposes ([`attached_apfs_volume`]).
fn image_attachments(
    image: &Path,
    images: &[AttachedDiskImage],
) -> Result<Vec<RecoveredImageAttachment>, ApfsError> {
    images
        .iter()
        .filter(|attached| attached.is_backed_by(image))
        .map(|attached| {
            let entities = attachment_entities(attached);
            Ok(match unformatted_whole_device(&entities) {
                Some(whole_device) => RecoveredImageAttachment::Unformatted {
                    image: image.to_owned(),
                    whole_device: whole_device.to_owned(),
                },
                None => {
                    let (whole_device, volume_device) = attached_apfs_volume(&entities)
                        .map_err(ApfsError::InvalidAttachmentInventory)?;
                    RecoveredImageAttachment::Apfs(AttachedImage {
                        image: image.to_owned(),
                        whole_device,
                        volume_device,
                        pin: std::sync::Mutex::new(None),
                    })
                }
            })
        })
        .collect()
}

/// One attachment of an image as its release undoes it: the whole device whose one detach takes
/// the attachment, and every APFS volume its container holds, none for an unformatted image.
/// Cowshed's images hold one volume, but a release also takes a container something added a
/// volume to: each mounted volume is unmounted, then the whole device detached once.
pub(crate) struct ReleaseView {
    image: PathBuf,
    whole_device: String,
    volumes: Vec<String>,
    /// The attachment's owner ([`AttachedDiskImage::owner_uid`]).
    owner_uid: libc::uid_t,
}

impl ReleaseView {
    pub fn whole_device(&self) -> &str {
        &self.whole_device
    }

    /// Every owned volume is a candidate. Its current kernel mount is resolved by the unmount
    /// owner after admission; an absent mount needs no native effect before whole-device detach.
    pub(crate) fn release_volumes(self) -> impl Iterator<Item = ReleasedVolume> {
        self.volumes.into_iter().map(move |volume_device| {
            ReleasedVolume(AttachedImage {
                image: self.image.clone(),
                whole_device: self.whole_device.clone(),
                volume_device,
                pin: std::sync::Mutex::new(None),
            })
        })
    }
}

/// One volume of a [`ReleaseView`]. Only a release view names one, and only
/// [`MacOsApfsBackend::unmount_released`] acts on it, revalidating it against the driver first.
pub(crate) struct ReleasedVolume(AttachedImage);

/// Every attachment the kernel holds for `image`, in inventory order, as its release sees it.
fn release_views_of(
    image: &Path,
    images: &[AttachedDiskImage],
) -> Result<Vec<ReleaseView>, ApfsError> {
    images
        .iter()
        .filter(|attached| attached.is_backed_by(image))
        .map(|attached| {
            let entities = attachment_entities(attached);
            let (whole_device, volumes) = match unformatted_whole_device(&entities) {
                Some(whole_device) => (whole_device.to_owned(), Vec::new()),
                None => attached_apfs_volumes(&entities)
                    .map_err(ApfsError::InvalidAttachmentInventory)?,
            };
            Ok(ReleaseView {
                image: image.to_owned(),
                whole_device,
                volumes,
                owner_uid: attached.owner_uid,
            })
        })
        .collect()
}

/// This process's effective uid.
fn current_euid() -> libc::uid_t {
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    unsafe { libc::geteuid() }
}

/// A formatted attachment's physical image device and every APFS volume in its one container.
/// Neither a synthesized container nor a slice is authority to release the image driver.
fn attached_apfs_volumes(entities: &[(String, String)]) -> Result<(String, Vec<String>), String> {
    let volumes: Vec<String> = entities
        .iter()
        .filter(|(_, hint)| is_apfs_volume_hint(hint))
        .map(|(device, _)| device.clone())
        .collect();
    let Some((first, rest)) = volumes.split_first() else {
        return Err("expected an APFS volume, reported none".into());
    };
    let container = reported_container(entities, first)?;
    for volume in rest {
        if reported_container(entities, volume)? != container {
            return Err(format!(
                "APFS volumes {volumes:?} are in more than one container"
            ));
        }
    }
    Ok((physical_image_device(entities)?.to_owned(), volumes))
}

fn is_valid_apfs_volume_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name.trim() == name
        && name != "."
        && name != ".."
        && !name.as_bytes().contains(&b'/')
        && !name.as_bytes().contains(&0)
}

/// The name the file system itself reports for the volume containing `path`.
///
/// Read straight from the kernel (`getattrlist`), so asking costs no Disk Arbitration round trip:
/// a relabel that is already right is skipped without touching the queue it would wait in.
#[cfg(target_os = "macos")]
pub fn volume_name(path: &Path) -> io::Result<OsString> {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let mut request = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: 0,
        volattr: libc::ATTR_VOL_INFO | libc::ATTR_VOL_NAME,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    // The answer's total length (u32), one attrreference_t (i32 offset from itself, u32 length),
    // then the name: at most 255 UTF-8 bytes and a NUL. Aligned the way the kernel packs it.
    #[repr(C, align(4))]
    struct Answer([u8; 12 + 256]);
    let mut answer = Answer([0; 12 + 256]);
    // SAFETY: `path` is NUL-terminated, `request` names one volume attribute, and `answer` is
    // writable for its full length, which is what the kernel is told.
    let status = unsafe {
        libc::getattrlist(
            path.as_ptr(),
            (&raw mut request).cast(),
            answer.0.as_mut_ptr().cast(),
            answer.0.len(),
            0,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    let bytes = &answer.0;
    let word = |at: usize| [bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]];
    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed volume name answer");
    let total = usize::try_from(u32::from_ne_bytes(word(0))).map_err(|_| malformed())?;
    let offset = usize::try_from(i32::from_ne_bytes(word(4))).map_err(|_| malformed())?;
    let length = usize::try_from(u32::from_ne_bytes(word(8))).map_err(|_| malformed())?;
    // The reference sits right after the length word, and its offset counts from itself.
    let start = 4 + offset;
    let name = bytes
        .get(start..start + length)
        .filter(|_| start + length <= total)
        .ok_or_else(malformed)?;
    Ok(OsString::from_vec(
        name.strip_suffix(b"\0").unwrap_or(name).to_vec(),
    ))
}

/// Whether a volume is mounted at exactly `path`. An unmounted mount point lies on its parent's
/// volume, and a sync meant for the source must never land on the volume holding the store.
fn is_mount_root(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(canonical) = fs::canonicalize(path) else {
        return false;
    };
    let Some(parent) = canonical.parent() else {
        return true;
    };
    match (fs::symlink_metadata(&canonical), fs::metadata(parent)) {
        (Ok(own), Ok(parent)) => own.dev() != parent.dev(),
        _ => false,
    }
}

#[cfg(any(target_os = "macos", test))]
fn sync_writable_volume(flags: u64, flush: impl FnOnce() -> libc::c_int) -> io::Result<()> {
    match MountAccess::from_kernel_flags(flags) {
        MountAccess::ReadOnly => Ok(()),
        MountAccess::ReadWrite => match flush() {
            0 => Ok(()),
            // It answers the errno itself, or -1 with errno set, depending on where it failed.
            -1 => Err(io::Error::last_os_error()),
            errno => Err(io::Error::from_raw_os_error(errno)),
        },
    }
}

/// Flush a mounted read-write source volume and wait; a read-only mount holds no dirty writes.
#[cfg(target_os = "macos")]
fn sync_volume(mount_point: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        fn sync_volume_np(path: *const libc::c_char, flags: libc::c_int) -> libc::c_int;
    }
    const SYNC_VOLUME_WAIT: libc::c_int = 0x02;
    let path = std::ffi::CString::new(mount_point.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount point contains NUL"))?;
    let mut facts = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is NUL-terminated; `facts` is writable for exactly one statfs value.
    if unsafe { libc::statfs(path.as_ptr(), facts.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful statfs initialized `facts`, and no borrow escaped the call.
    let flags = u64::from(unsafe { facts.assume_init_ref() }.f_flags);
    sync_writable_volume(flags, || {
        // SAFETY: `path` is NUL-terminated and outlives the call; the flags ask only to wait.
        unsafe { sync_volume_np(path.as_ptr(), SYNC_VOLUME_WAIT) }
    })
}

#[cfg(not(target_os = "macos"))]
fn sync_volume(mount_point: &Path) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let volume = fs::File::open(mount_point)?;
    // SAFETY: `volume` owns a live descriptor for the duration of the call.
    if unsafe { libc::syncfs(volume.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn is_kernel_device_path(device: &str) -> bool {
    device
        .strip_prefix("/dev/")
        .and_then(identifier_depth)
        .is_some()
}

fn validate_image_path(path: &Path) -> Result<(), ApfsError> {
    if is_image_path(path) {
        Ok(())
    } else {
        Err(ApfsError::InvalidImagePath(path.to_owned()))
    }
}

fn validate_clone_path(path: &Path) -> Result<(), CloneFileError> {
    if is_image_path(path) {
        Ok(())
    } else {
        Err(CloneFileError::InvalidImagePath {
            path: path.to_owned(),
        })
    }
}

fn parse_blank_image_whole_device(bytes: &[u8]) -> Result<String, ApfsError> {
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| ApfsError::InvalidAttachmentPlist(error.to_string()))?;
    let system_entities = value
        .as_dictionary()
        .and_then(|root| root.get("system-entities"))
        .and_then(plist::Value::as_array)
        .ok_or_else(|| ApfsError::InvalidAttachmentPlist("missing system-entities array".into()))?;
    let mut whole_devices = Vec::new();
    for entity in system_entities {
        let Some(device) = entity
            .as_dictionary()
            .and_then(|dictionary| dictionary.get("dev-entry"))
            .and_then(plist::Value::as_string)
            .and_then(device_path)
        else {
            continue;
        };
        if device_depth(&device) == 0 && is_kernel_device_path(&device) {
            whole_devices.push(device);
        }
    }
    match whole_devices.len() {
        1 => Ok(whole_devices.pop().expect("one whole device was counted")),
        0 => Err(ApfsError::InvalidAttachmentPlist(
            "no canonical whole image device".into(),
        )),
        _ => Err(ApfsError::InvalidAttachmentPlist(
            "multiple whole image devices".into(),
        )),
    }
}

fn parse_attachment_plist(bytes: &[u8]) -> Result<(String, String), ApfsError> {
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| ApfsError::InvalidAttachmentPlist(error.to_string()))?;
    let system_entities = value
        .as_dictionary()
        .and_then(|root| root.get("system-entities"))
        .and_then(plist::Value::as_array)
        .ok_or_else(|| ApfsError::InvalidAttachmentPlist("missing system-entities array".into()))?;
    let entities = collect_attachment_entities(system_entities)?;
    if !entities.iter().any(|(_, hint)| is_apfs_volume_hint(hint)) {
        return Err(ApfsError::NoApfsVolume);
    }
    attached_apfs_volume(&entities).map_err(ApfsError::InvalidAttachmentPlist)
}

/// Apple's fixed APFS partition-type GUID prefixes, as the kernel's IOMedia `Content` labels an
/// attached image's media: the synthesized container (its physical store), then the volume.
const APFS_STORE_TYPE_GUID_PREFIX: &str = "EF57347C-";
const APFS_VOLUME_TYPE_GUID_PREFIX: &str = "41504653-";

fn hint_has_type_guid(hint: &str, prefix: &str) -> bool {
    hint.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn is_apfs_volume_hint(hint: &str) -> bool {
    hint.eq_ignore_ascii_case("Apple_APFS_Volume")
        || hint_has_type_guid(hint, APFS_VOLUME_TYPE_GUID_PREFIX)
}

fn is_apfs_container_hint(hint: &str) -> bool {
    hint.eq_ignore_ascii_case("Apple_APFS_Container")
        || hint_has_type_guid(hint, APFS_STORE_TYPE_GUID_PREFIX)
}

/// The physical image device and its one APFS volume. The synthesized container validates
/// the volume's parentage, but is not the device whose image-driver connection we release.
///
/// Cowshed's images hold one volume in one container, and both reports of an attachment name
/// the two: `diskutil image attach --plist` with string hints (`Apple_APFS_Container`,
/// `Apple_APFS_Volume`), the kernel inventory with Apple's partition-type GUIDs. The answer
/// therefore needs no Disk Arbitration query: `diskutil info` and `diskutil apfs list` queue
/// behind every arbitration client on the host, and under contention answered before Disk
/// Arbitration had announced the attach at all. Anything but exactly one volume inside a
/// reported container is refused, naming what was reported; only a release takes more
/// ([`attached_apfs_volumes`]).
fn attached_apfs_volume(entities: &[(String, String)]) -> Result<(String, String), String> {
    let volumes: Vec<&str> = entities
        .iter()
        .filter(|(_, hint)| is_apfs_volume_hint(hint))
        .map(|(device, _)| device.as_str())
        .collect();
    let [volume] = volumes.as_slice() else {
        return Err(format!("expected one APFS volume, reported {volumes:?}"));
    };
    reported_container(entities, volume)?;
    Ok((
        physical_image_device(entities)?.to_owned(),
        (*volume).to_owned(),
    ))
}

/// The container `volume` is a slice of, once the report names it as an APFS container.
fn reported_container(entities: &[(String, String)], volume: &str) -> Result<String, String> {
    let container = whole_device_from(volume)
        .ok_or_else(|| format!("APFS volume {volume} is not a slice of a container"))?;
    if entities
        .iter()
        .any(|(device, hint)| *device == container && is_apfs_container_hint(hint))
    {
        Ok(container)
    } else {
        Err(format!(
            "APFS volume {volume} was reported without its container {container}"
        ))
    }
}

/// The image driver's whole media, distinct from every synthesized container and slice.
/// RAW exposes it with no content hint; an image another tool partitioned names its partition map.
fn physical_image_device(entities: &[(String, String)]) -> Result<&str, String> {
    let mut physical = entities.iter().filter(|(device, hint)| {
        device_depth(device) == 0
            && is_kernel_device_path(device)
            && (hint.is_empty()
                || hint == "GUID_partition_scheme"
                || hint == "Apple_partition_scheme")
    });
    let Some((device, _)) = physical.next() else {
        return Err("attachment reports no physical whole image device".into());
    };
    if physical.next().is_some() {
        return Err("attachment reports multiple physical whole image devices".into());
    }
    Ok(device)
}

/// The refusal of a device its image no longer holds, naming the whole devices it holds now.
fn reused_device<'a>(attachment: &AttachedImage, held: impl Iterator<Item = &'a str>) -> ApfsError {
    ApfsError::InvalidAttachmentInventory(format!(
        "{} no longer owns {} / {} (holds {:?}); refusing a reused device",
        attachment.image.display(),
        attachment.whole_device,
        attachment.volume_device,
        held.collect::<Vec<_>>()
    ))
}

/// Each reported entity's device, as a `/dev/` path, and its content hint.
fn collect_attachment_entities(
    system_entities: &[plist::Value],
) -> Result<Vec<(String, String)>, ApfsError> {
    let mut entities = Vec::new();
    for entity in system_entities {
        let dictionary = entity.as_dictionary().ok_or_else(|| {
            ApfsError::InvalidAttachmentPlist("system-entities entry is not a dictionary".into())
        })?;
        let Some(device) = dictionary
            .get("dev-entry")
            .and_then(plist::Value::as_string)
        else {
            continue;
        };
        let device = device_path(device).unwrap_or_else(|| device.to_owned());
        let hint = dictionary
            .get("content-hint")
            .and_then(plist::Value::as_string)
            .unwrap_or_default()
            .to_owned();
        entities.push((device, hint));
    }
    Ok(entities)
}

fn device_path(identifier: &str) -> Option<String> {
    let relative = identifier.strip_prefix("/dev/").unwrap_or(identifier);
    identifier_depth(relative)?;
    Some(format!("/dev/{relative}"))
}

/// Slice depth of a `/dev/` device path; a string that is not a valid device path reads as depth
/// zero so plist scans that rank candidates by depth simply never prefer it.
fn device_depth(device: &str) -> usize {
    device
        .strip_prefix("/dev/")
        .and_then(identifier_depth)
        .unwrap_or(0)
}

fn whole_device_from(device: &str) -> Option<String> {
    let container = device.strip_prefix("/dev/").and_then(container_of)?;
    Some(format!("/dev/{container}"))
}

fn raw_device_from(device: &str) -> String {
    match device.strip_prefix("/dev/") {
        Some(relative) => format!("/dev/r{relative}"),
        None => format!("r{device}"),
    }
}

/// One source inode retained from no-follow open through descriptor cloning. Detached captures
/// also retain the shared lock that excludes a DiskImages2 helper, and refuse a source any
/// registered attachment backs, which an in-kernel attach is ([`raw::refuse_attached`]). Live
/// workspace and guarded build captures retain no attachment lock and leave the source mounted;
/// callers own writer admission.
#[cfg(target_os = "macos")]
struct ImageSource<'a> {
    file: File,
    path: &'a Path,
}

#[cfg(target_os = "macos")]
impl<'a> ImageSource<'a> {
    /// Hold and flush exactly the inode that is cloned, without resolving its path again.
    fn hold(
        path: &'a Path,
        destination: &Path,
        capture: ImageCapture,
    ) -> Result<Self, CloneFileError> {
        use std::os::unix::fs::OpenOptionsExt;
        let failed = |error: io::Error| CloneFileError::Io {
            source_path: path.to_owned(),
            destination_path: destination.to_owned(),
            source: error,
        };
        let lock_flags = match capture {
            ImageCapture::Live => 0,
            ImageCapture::Detached => libc::O_SHLOCK | libc::O_NONBLOCK,
        };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(lock_flags | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|error| match error.kind() {
                io::ErrorKind::WouldBlock => CloneFileError::SourceAttached {
                    source: path.to_owned(),
                },
                _ => failed(error),
            })?;
        if capture == ImageCapture::Detached {
            raw::refuse_attached(&file, path).map_err(|error| match error.kind() {
                io::ErrorKind::WouldBlock => CloneFileError::SourceAttached {
                    source: path.to_owned(),
                },
                _ => failed(error),
            })?;
        }
        // SAFETY: `file` owns a live descriptor for the duration of the call.
        if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
            return Err(failed(io::Error::last_os_error()));
        }
        Ok(Self { file, path })
    }

    /// Clone the held file, by its descriptor, to `destination`, which must not exist.
    fn clone_to(&self, destination: &Path) -> Result<(), CloneFileError> {
        use std::os::unix::ffi::OsStrExt;
        let target = std::ffi::CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
            CloneFileError::Io {
                source_path: self.path.to_owned(),
                destination_path: destination.to_owned(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "destination path contains NUL",
                ),
            }
        })?;
        // SAFETY: `self.file` owns a live descriptor and `target` is a NUL-terminated path, both
        // outliving the call. Flags `0` is Darwin's default clone, ownership copied, as the
        // path-based `clonefile` this replaces made it.
        let cloned = unsafe {
            libc::fclonefileat(self.file.as_raw_fd(), libc::AT_FDCWD, target.as_ptr(), 0)
        };
        if cloned == 0 {
            return Ok(());
        }
        Err(classify_clone_error(
            self.path,
            destination,
            io::Error::last_os_error(),
        ))
    }
}

#[cfg(any(target_os = "macos", test))]
fn classify_clone_error(source: &Path, destination: &Path, error: io::Error) -> CloneFileError {
    // Darwin EXDEV=18 and EEXIST=17; raw codes are used because std does not
    // expose a cross-device ErrorKind on all supported Rust versions.
    match error.raw_os_error() {
        Some(18) => CloneFileError::CrossVolume {
            source: source.to_owned(),
            destination: destination.to_owned(),
        },

        Some(17) => CloneFileError::DestinationExists {
            destination: destination.to_owned(),
        },
        _ => CloneFileError::Io {
            source_path: source.to_owned(),
            destination_path: destination.to_owned(),
            source: error,
        },
    }
}

/// One APFS container the kernel has registered: its synthesized whole disk and every volume it
/// hosts, read from one I/O Registry matching snapshot by [`registered_apfs_containers`].
///
/// Mount state is deliberately absent: the kernel mount table is the mount authority.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredApfsContainer {
    /// The container's synthesized whole disk as a bare BSD name, e.g. `disk3`.
    pub reference: String,
    /// The `Size` of that `AppleAPFSMedia`: the ceiling every volume in it shares.
    pub capacity_bytes: u64,
    /// The container's volumes, sorted by identifier. Snapshots are not volumes.
    pub volumes: Vec<RegisteredApfsVolume>,
}

/// One `AppleAPFSVolume` the kernel has published as a BSD device.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredApfsVolume {
    /// The volume's `FullName`.
    pub name: String,
    /// The volume's bare BSD name, e.g. `disk3s8`, always a slice of its container's reference.
    pub identifier: String,
    /// The volume's `UUID`, canonical uppercase hyphenated. Unique within its container only:
    /// a cloned image attached beside its source registers the same volume UUIDs.
    pub volume_uuid: String,
    /// The volume's `RoleValue`, the APFS `APFS_VOL_ROLE_*` code: 0 for a role-less volume,
    /// 64 for a volume group's Data, 8 for VM, and so on. `None` when no `RoleValue` registers;
    /// every one of 138 volumes on the measuring host registered one.
    pub role: Option<u64>,
    /// Whether the volume registers `Encrypted = Yes`. The registry publishes the key only on
    /// encrypted volumes (4 of 138 on the measuring host, never `Encrypted = No`), so its
    /// absence is an unencrypted volume.
    pub encrypted: bool,
}

#[cfg(target_os = "macos")]
pub use io_registry::registered_apfs_containers;
#[cfg(target_os = "macos")]
pub(crate) use io_registry::{
    disk_image_class_counts, disk_image_user_client_creators, registered_media,
};

/// What one `AppleAPFSMedia` registers. `reference` is absent until the media's BSD client has
/// published the device node: such a container is arriving, not yet addressable.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ApfsMediaFacts {
    reference: Option<String>,
    capacity_bytes: Option<u64>,
}

/// What one `AppleAPFSVolume` registers, keyed to the registry entry id of the
/// `AppleAPFSMedia` its `AppleAPFSContainer` provider hangs from.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ApfsVolumeFacts {
    media: u64,
    identifier: Option<String>,
    name: Option<String>,
    uuid: Option<String>,
    role: Option<u64>,
    encrypted: Option<bool>,
}

/// Validate one registry snapshot's APFS facts into the container inventory.
///
/// Volumes are grouped by the registry identity of the media their container provider hangs
/// from, never by parsing BSD names; a volume's name must then agree with that provider. Only a
/// missing BSD Name means "not yet published" and leaves an object out; every other gap,
/// malformation, duplicate or disagreement is an error, so a damaged read can never pass for
/// an inventory with fewer containers or volumes in it.
#[cfg(any(target_os = "macos", test))]
fn project_registered_apfs(
    media: &std::collections::BTreeMap<u64, ApfsMediaFacts>,
    volumes: Vec<ApfsVolumeFacts>,
) -> io::Result<Vec<RegisteredApfsContainer>> {
    use std::collections::BTreeMap;
    use std::collections::btree_map::Entry;

    fn malformed(message: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, message)
    }

    let mut containers: BTreeMap<u64, RegisteredApfsContainer> = BTreeMap::new();
    let mut references: BTreeMap<&str, u64> = BTreeMap::new();
    for (&entry, facts) in media {
        let Some(reference) = facts.reference.as_deref() else {
            continue;
        };
        if identifier_depth(reference) != Some(0) {
            return Err(malformed(format!(
                "APFS container media {entry:#x} registers BSD Name {reference:?}, not a whole disk"
            )));
        }
        let capacity_bytes = match facts.capacity_bytes {
            Some(0) => {
                return Err(malformed(format!(
                    "APFS container {reference} registers a zero Size"
                )));
            }
            Some(bytes) => bytes,
            None => {
                return Err(malformed(format!(
                    "APFS container {reference} registers no Size"
                )));
            }
        };
        if let Some(other) = references.insert(reference, entry) {
            return Err(malformed(format!(
                "APFS container media {other:#x} and {entry:#x} both register BSD Name {reference}"
            )));
        }
        containers.insert(
            entry,
            RegisteredApfsContainer {
                reference: reference.to_owned(),
                capacity_bytes,
                volumes: Vec::new(),
            },
        );
    }

    for volume in volumes {
        let Some(identifier) = volume.identifier else {
            continue;
        };
        let Some(container) = containers.get_mut(&volume.media) else {
            return Err(malformed(match media.get(&volume.media) {
                Some(_) => format!(
                    "APFS volume {identifier} is published before its container media {:#x}",
                    volume.media
                ),
                None => format!(
                    "APFS volume {identifier} hangs from container media {:#x} the snapshot never read",
                    volume.media
                ),
            }));
        };
        if identifier_depth(&identifier) != Some(1)
            || container_of(&identifier) != Some(container.reference.as_str())
        {
            return Err(malformed(format!(
                "APFS volume {identifier} hangs from container {}, whose volume it cannot be",
                container.reference
            )));
        }
        let name = match volume.name {
            Some(name) if !name.is_empty() => name,
            Some(_) => {
                return Err(malformed(format!(
                    "APFS volume {identifier} registers an empty FullName"
                )));
            }
            None => {
                return Err(malformed(format!(
                    "APFS volume {identifier} registers no FullName"
                )));
            }
        };
        let Some(mut volume_uuid) = volume.uuid else {
            return Err(malformed(format!(
                "APFS volume {identifier} registers no UUID"
            )));
        };
        // Only the hyphenated form is 36 bytes long, so uppercasing it in place is canonical.
        if volume_uuid.len() != 36
            || !uuid::Uuid::try_parse(&volume_uuid).is_ok_and(|parsed| !parsed.is_nil())
        {
            return Err(malformed(format!(
                "APFS volume {identifier} registers UUID {volume_uuid:?}, not a hyphenated volume UUID"
            )));
        }
        volume_uuid.make_ascii_uppercase();
        container.volumes.push(RegisteredApfsVolume {
            name,
            identifier,
            volume_uuid,
            role: volume.role,
            encrypted: volume.encrypted.unwrap_or(false),
        });
    }

    let mut inventory: Vec<RegisteredApfsContainer> = containers.into_values().collect();
    for container in &mut inventory {
        container
            .volumes
            .sort_by(|left, right| left.identifier.cmp(&right.identifier));
        if let Some(pair) = container
            .volumes
            .windows(2)
            .find(|pair| pair[0].identifier == pair[1].identifier)
        {
            return Err(malformed(format!(
                "two APFS volumes in container {} both register BSD Name {}",
                container.reference, pair[0].identifier
            )));
        }
        let mut uuids: BTreeMap<&str, &str> = BTreeMap::new();
        for volume in &container.volumes {
            match uuids.entry(&volume.volume_uuid) {
                Entry::Occupied(first) => {
                    return Err(malformed(format!(
                        "APFS volumes {} and {} in container {} both register UUID {}",
                        first.get(),
                        volume.identifier,
                        container.reference,
                        volume.volume_uuid
                    )));
                }
                Entry::Vacant(slot) => {
                    slot.insert(&volume.identifier);
                }
            }
        }
    }
    inventory.sort_by(|left, right| left.reference.cmp(&right.reference));
    Ok(inventory)
}

/// The kernel's I/O Registry view of attached disk images and registered APFS containers.
///
/// Each read starts from one `IOServiceGetMatchingServices` snapshot, which the kernel collects
/// under the registry's own consistency check — it restarts its walk whenever a concurrent attach
/// or detach invalidates it — so the set is complete for one instant, which is exactly what
/// `hdiutil info` and `diskutil apfs list` are not. Disk images are read from their image
/// drivers ([`Driver`]): every registered in-kernel DI1 drive, whose `KDIURLPath` is the path its
/// attach was handed, and every DiskImages2 `AppleDiskImageDevice`, whose `DiskImageURL` names its
/// backing store; for one image, only the drives and devices the kernel matched to that image's
/// path or file URL. Each device's media are read from its own subtree, again
/// whenever a concurrent change invalidated a child list. A question about one image therefore
/// reads that image's registry entries alone, however many other images are attached. APFS is
/// read from a snapshot of every registered IOMedia: every container's `AppleAPFSMedia` and every
/// `AppleAPFSVolume`, grouped by the media its `AppleAPFSContainer` provider hangs from. A live
/// node's provider links do not
/// change; a node already detached from the plane is departing and is not reported, and neither
/// is one the kernel finalizes while the walk holds it: finalizing destroys the entry's port, so
/// every reference to it becomes a dead name that answers `MACH_SEND_INVALID_DEST` to any call
/// and nothing at all to a property read. Any other read failure fails the whole inventory.
///
/// Each `AppleDiskImageDevice` also names, as its user client's creator, the `diskimagesiod`
/// helper serving it: the half of [`crate::disk_image_helpers`] that tells a helper from an
/// orphan.
#[cfg(target_os = "macos")]
mod io_registry {
    use super::{
        ApfsMediaFacts, ApfsVolumeFacts, AttachedDiskImage, DiskImageSource, ImageMedia,
        RegisteredApfsContainer, project_registered_apfs,
    };
    use crate::image::ImageCapacity;
    use std::collections::BTreeMap;
    use std::collections::btree_map::Entry;
    use std::ffi::{CStr, c_char, c_void};
    use std::io;
    use std::ptr;

    type IoObject = libc::mach_port_t;
    type KernReturn = libc::c_int;
    type CfType = *const c_void;
    type CfIndex = isize;

    const KERN_SUCCESS: KernReturn = 0;
    /// `kIOReturnNoDevice`: the entry has no provider in the plane.
    const IO_RETURN_NO_DEVICE: KernReturn = 0xE000_02C0_u32 as KernReturn;
    /// `MACH_SEND_INVALID_DEST`: the call was sent to a dead name. The kernel destroys a registry
    /// entry's port when it finalizes the terminated entry, so a reference this task still holds
    /// to it answers this to every call from then on.
    const MACH_SEND_INVALID_DEST: KernReturn = 0x1000_0003;
    /// `kIOMainPortDefault`.
    const MAIN_PORT_DEFAULT: libc::mach_port_t = 0;
    const SERVICE_PLANE: &CStr = c"IOService";
    const CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
    const CF_NUMBER_SINT64_TYPE: CfIndex = 4;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CfRange {
        location: CfIndex,
        length: CfIndex,
    }

    #[link(name = "IOKit", kind = "framework")]
    unsafe extern "C" {
        fn IOServiceMatching(name: *const c_char) -> CfType;
        fn IORegistryEntryIDMatching(entry_id: u64) -> CfType;
        fn IOServiceGetMatchingService(main_port: libc::mach_port_t, matching: CfType) -> IoObject;
        fn IOServiceGetMatchingServices(
            main_port: libc::mach_port_t,
            matching: CfType,
            existing: *mut IoObject,
        ) -> KernReturn;
        fn IOIteratorNext(iterator: IoObject) -> IoObject;
        fn IOIteratorIsValid(iterator: IoObject) -> libc::c_int;
        fn IOObjectRelease(object: IoObject) -> KernReturn;
        fn IOObjectConformsTo(object: IoObject, class_name: *const c_char) -> libc::c_int;
        fn IORegistryEntryGetParentEntry(
            entry: IoObject,
            plane: *const c_char,
            parent: *mut IoObject,
        ) -> KernReturn;
        fn IORegistryEntryGetRegistryEntryID(entry: IoObject, entry_id: *mut u64) -> KernReturn;
        fn IORegistryEntryGetChildIterator(
            entry: IoObject,
            plane: *const c_char,
            iterator: *mut IoObject,
        ) -> KernReturn;
        fn IORegistryEntryCreateCFProperty(
            entry: IoObject,
            key: CfType,
            allocator: CfType,
            options: u32,
        ) -> CfType;
        fn IORegistryGetRootEntry(main_port: libc::mach_port_t) -> IoObject;
        fn IOObjectGetClass(object: IoObject, class_name: *mut c_char) -> KernReturn;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFRelease(object: CfType);
        fn CFGetTypeID(object: CfType) -> usize;
        fn CFStringGetTypeID() -> usize;
        fn CFNumberGetTypeID() -> usize;
        fn CFBooleanGetTypeID() -> usize;
        fn CFBooleanGetValue(boolean: CfType) -> u8;
        fn CFStringCreateWithBytes(
            allocator: CfType,
            bytes: *const u8,
            length: CfIndex,
            encoding: u32,
            external_representation: u8,
        ) -> CfType;
        fn CFStringGetLength(string: CfType) -> CfIndex;
        fn CFStringGetBytes(
            string: CfType,
            range: CfRange,
            encoding: u32,
            loss_byte: u8,
            external_representation: u8,
            buffer: *mut u8,
            capacity: CfIndex,
            used: *mut CfIndex,
        ) -> CfIndex;
        fn CFNumberGetValue(number: CfType, number_type: CfIndex, value: *mut c_void) -> u8;
        fn CFDictionaryGetTypeID() -> usize;
        fn CFDictionaryGetValue(dictionary: CfType, key: CfType) -> CfType;
        fn CFArrayGetTypeID() -> usize;
        fn CFArrayGetCount(array: CfType) -> CfIndex;
        fn CFArrayGetValueAtIndex(array: CfType, index: CfIndex) -> CfType;
        fn CFDataGetTypeID() -> usize;
        fn CFDataGetLength(data: CfType) -> CfIndex;
        fn CFDataGetBytePtr(data: CfType) -> *const u8;

        fn CFRetain(object: CfType) -> CfType;
        fn CFDictionaryCreateMutable(
            allocator: CfType,
            capacity: CfIndex,
            key_callbacks: *const CfDictionaryCallBacks,
            value_callbacks: *const CfDictionaryCallBacks,
        ) -> CfType;
        fn CFDictionarySetValue(dictionary: CfType, key: CfType, value: CfType);
        fn CFURLCreateFromFileSystemRepresentation(
            allocator: CfType,
            buffer: *const u8,
            length: CfIndex,
            is_directory: u8,
        ) -> CfType;
        fn CFURLGetString(url: CfType) -> CfType;
        #[link_name = "kCFTypeDictionaryKeyCallBacks"]
        static CF_TYPE_DICTIONARY_KEY_CALL_BACKS: CfDictionaryCallBacks;
        #[link_name = "kCFTypeDictionaryValueCallBacks"]
        static CF_TYPE_DICTIONARY_VALUE_CALL_BACKS: CfDictionaryCallBacks;
    }

    /// CoreFoundation's dictionary callbacks, opaque here: only their address is passed.
    #[repr(C)]
    struct CfDictionaryCallBacks {
        _opaque: [u8; 0],
    }
    /// One owned registry reference.
    struct Object(IoObject);

    impl Drop for Object {
        fn drop(&mut self) {
            // SAFETY: the wrapper owns exactly one reference to a non-null registry object.
            unsafe { IOObjectRelease(self.0) };
        }
    }

    /// One owned, non-null CoreFoundation object.
    struct Cf(CfType);

    impl Drop for Cf {
        fn drop(&mut self) {
            // SAFETY: the wrapper owns exactly one reference to a non-null CF object.
            unsafe { CFRelease(self.0) };
        }
    }

    /// An image driver whose attachments the inventory reads, with the property naming each
    /// attachment's backing store.
    #[derive(Clone, Copy)]
    enum Driver {
        /// cowshed's own attach, `hdiutil attach -kernel`: `KDIURLPath` is the path it was handed.
        InKernel,
        /// `diskutil image attach`, an earlier build's or another tool's: `DiskImageURL` is a URL.
        DiskImages2,
    }

    impl Driver {
        const ALL: [Self; 2] = [Self::InKernel, Self::DiskImages2];

        fn class(self) -> &'static CStr {
            match self {
                Self::InKernel => c"IOHDIXHDDriveInKernel",
                Self::DiskImages2 => c"AppleDiskImageDevice",
            }
        }

        fn source_key(self) -> &'static str {
            match self {
                Self::InKernel => "KDIURLPath",
                Self::DiskImages2 => "DiskImageURL",
            }
        }

        /// The backing store a driver's source property names.
        fn source(self, value: &str) -> DiskImageSource {
            match self {
                Self::InKernel if value.starts_with('/') => {
                    DiskImageSource::File(std::path::PathBuf::from(value))
                }
                Self::InKernel => DiskImageSource::Url(value.to_owned()),
                Self::DiskImages2 => DiskImageSource::from_url(value),
            }
        }

        /// The value this driver registers for the local file at `image`.
        fn source_of(self, image: &std::path::Path) -> io::Result<Cf> {
            match self {
                Self::InKernel => key(image
                    .to_str()
                    .ok_or_else(|| invalid(format!("{} is not UTF-8", image.display())))?),
                Self::DiskImages2 => file_url(image),
            }
        }

        /// Whether `entry` is an attachment of any image driver.
        fn any_conforms(entry: &Object) -> bool {
            Self::ALL
                .iter()
                .any(|driver| conforms(entry, driver.class()))
        }
    }

    struct Keys {
        bsd_name: Cf,
        content: Cf,
        size: Cf,
        source: Cf,
        owner: Cf,
        driver: Driver,
    }

    struct ApfsKeys {
        bsd_name: Cf,
        size: Cf,
        full_name: Cf,
        uuid: Cf,
        role: Cf,
        encrypted: Cf,
    }

    /// The registry entries one IOKit iterator yields, as owned references.
    struct Entries(Object);

    impl Entries {
        /// Whether the iteration saw a consistent registry. A child iteration that a concurrent
        /// attach or detach invalidated may have skipped entries and must be walked again.
        fn consistent(&self) -> bool {
            // SAFETY: the wrapped iterator is live.
            unsafe { IOIteratorIsValid(self.0.0) != 0 }
        }
    }

    impl Iterator for Entries {
        type Item = Object;

        fn next(&mut self) -> Option<Object> {
            // SAFETY: the wrapped iterator is live; each object it yields is +1.
            match unsafe { IOIteratorNext(self.0.0) } {
                0 => None,
                next => Some(Object(next)),
            }
        }
    }

    /// Every registered instance of `class` at one instant.
    fn registered(class: &CStr) -> io::Result<Entries> {
        registered_matching(class, None)
    }

    /// Every registered instance of `class` at one instant that has each of `properties`, when
    /// given. The kernel compares them (`IOPropertyMatch`) against the class's own instances, so
    /// no instance that does not match is read.
    fn registered_matching(class: &CStr, properties: Option<&Cf>) -> io::Result<Entries> {
        let named = class.to_string_lossy();
        let property_match = properties.map(|_| key("IOPropertyMatch")).transpose()?;
        // SAFETY: the class name is NUL-terminated; a null answer is refused below.
        let matching = unsafe { IOServiceMatching(class.as_ptr()) };
        if matching.is_null() {
            return Err(io::Error::other(format!(
                "IOServiceMatching({named}) returned no matching dictionary"
            )));
        }
        if let (Some(properties), Some(property_match)) = (properties, &property_match) {
            // SAFETY: `matching` is the live mutable dictionary just created, which only the call
            // below consumes; the dictionary retains the key and the properties it is given.
            unsafe { CFDictionarySetValue(matching, property_match.0, properties.0) };
        }
        let mut iterator: IoObject = 0;
        // SAFETY: `matching` is a +1 dictionary the call consumes whatever it returns, and
        // `iterator` is writable.
        let status =
            unsafe { IOServiceGetMatchingServices(MAIN_PORT_DEFAULT, matching, &mut iterator) };
        if status != KERN_SUCCESS {
            return Err(kernel_error(&format!("match registered {named}"), status));
        }
        Ok(Entries(Object(iterator)))
    }

    /// A new, empty CoreFoundation dictionary of CF-typed keys and values.
    fn mutable_dictionary() -> io::Result<Cf> {
        // SAFETY: the callbacks are CoreFoundation's own for CF-typed keys and values, and the
        // answer is +1 or null.
        let dictionary = unsafe {
            CFDictionaryCreateMutable(
                ptr::null(),
                0,
                &raw const CF_TYPE_DICTIONARY_KEY_CALL_BACKS,
                &raw const CF_TYPE_DICTIONARY_VALUE_CALL_BACKS,
            )
        };
        if dictionary.is_null() {
            Err(io::Error::other(
                "CoreFoundation could not create a matching dictionary",
            ))
        } else {
            Ok(Cf(dictionary))
        }
    }

    /// The file URL CoreFoundation spells for the local path `image`: the `DiskImageURL` the
    /// image framework registers for a file attached by that path, which it spells the same way.
    fn file_url(image: &std::path::Path) -> io::Result<Cf> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = image.as_os_str().as_bytes();
        let length = CfIndex::try_from(bytes.len())
            .map_err(|_| invalid(format!("{} is too long for a file URL", image.display())))?;
        // SAFETY: `bytes` is readable for `length` bytes, which the call copies; the answer is +1
        // or null.
        let url = unsafe {
            CFURLCreateFromFileSystemRepresentation(ptr::null(), bytes.as_ptr(), length, 0)
        };
        if url.is_null() {
            return Err(io::Error::other(format!(
                "CoreFoundation could not spell {} as a file URL",
                image.display()
            )));
        }
        let url = Cf(url);
        // SAFETY: `url` is a live CFURL; the string it answers is borrowed from it, or null.
        let string = unsafe { CFURLGetString(url.0) };
        if string.is_null() {
            return Err(io::Error::other(format!(
                "CoreFoundation spelled no URL string for {}",
                image.display()
            )));
        }
        // SAFETY: `string` is a live CFString borrowed from `url`, retained before `url` goes.
        Ok(Cf(unsafe { CFRetain(string) }))
    }

    /// The entry's children in the service plane, unregistered ones (user clients) included.
    fn children(entry: &Object) -> Read<Entries> {
        let mut iterator: IoObject = 0;
        // SAFETY: `entry` is a registry reference, the plane name is NUL-terminated, and
        // `iterator` is writable.
        let status = unsafe {
            IORegistryEntryGetChildIterator(entry.0, SERVICE_PLANE.as_ptr(), &mut iterator)
        };
        checked("read a registry entry's children", status).map(|()| Entries(Object(iterator)))
    }

    /// Why a read of one held registry entry has no answer.
    enum Unread {
        /// The kernel finalized the entry while the walk held it. It left the registry between
        /// the snapshot and this read: it is departing, exactly like a node already detached from
        /// the plane, and says nothing about the rest of the inventory.
        Gone,
        /// The registry refused, or garbled, a read of an entry that is still registered.
        Failed(io::Error),
    }

    impl From<io::Error> for Unread {
        fn from(error: io::Error) -> Self {
            Self::Failed(error)
        }
    }

    type Read<T> = Result<T, Unread>;

    /// Settle one snapshot node's reading: a node that left the registry mid-read is skipped as
    /// departing; any other failure fails the whole inventory.
    fn settle(read: Read<()>) -> io::Result<()> {
        match read {
            Ok(()) | Err(Unread::Gone) => Ok(()),
            Err(Unread::Failed(error)) => Err(error),
        }
    }

    fn checked(operation: &str, status: KernReturn) -> Read<()> {
        match status {
            KERN_SUCCESS => Ok(()),
            MACH_SEND_INVALID_DEST => Err(Unread::Gone),
            status => Err(Unread::Failed(kernel_error(operation, status))),
        }
    }

    pub(super) fn attached_disk_images() -> io::Result<Vec<AttachedDiskImage>> {
        let mut attached = Vec::new();
        for driver in Driver::ALL {
            attached.extend(image_devices(registered(driver.class())?, driver)?);
        }
        Ok(attached)
    }

    /// What one registered in-kernel drive names as the file it holds.
    #[derive(Debug, PartialEq, Eq)]
    pub(super) enum DriveBacking {
        /// The kernel's own `backingstore-id`, `{"writeable-components" | "readonly-components" =
        /// ("d<dev>:i<ino>", …)}`: each file the drive holds open, as `(device, inode)`, whatever
        /// path names it now.
        Files(Vec<(u64, u64)>),
        /// No `backingstore-id` names the drive's file, for the reason `why`. Its `KDIURLPath`,
        /// the path its attach was handed, still does, but no longer once that file is renamed,
        /// unlinked or reached through an alias.
        Path {
            path: std::path::PathBuf,
            why: String,
        },
        /// Its `KDIURLPath` names a store that is no local file (`ram://<sectors>` for a RAM
        /// disk), so it holds no file. The kernel registers no `backingstore-id` for such a drive.
        Store { url: String },
        /// Neither names the drive's file, for the reason `why`.
        Unidentified { why: String },
    }

    impl DriveBacking {
        /// Whether this drive positively holds the file opened as `(device, inode)` at `image`:
        /// its `backingstore-id` names that file or, failing one, its `KDIURLPath` is `image`.
        fn holds(&self, file: (u64, u64), image: &std::path::Path) -> bool {
            match self {
                Self::Files(files) => files.contains(&file),
                Self::Path { path, .. } => path == image,
                Self::Store { .. } | Self::Unidentified { .. } => false,
            }
        }

        /// How this drive is known when it names no file by device and inode; `None` when it does.
        fn unproven(&self) -> Option<String> {
            match self {
                Self::Files(_) | Self::Store { .. } => None,
                Self::Path { path, why } => Some(format!(
                    "the drive attached from {} ({why})",
                    path.display()
                )),
                Self::Unidentified { why } => Some(format!("a drive that names no file ({why})")),
            }
        }
    }

    /// What the registered in-kernel drives say of one opened file.
    #[derive(Debug, PartialEq, Eq)]
    pub(super) enum KernelHold {
        /// A drive positively holds it.
        Held,
        /// No drive holds it, as far as `unproven`, each drive that names no file by device and
        /// inode, can tell.
        Free { unproven: Vec<String> },
    }

    /// Judge the file opened as `(device, inode)` at `image` against every drive, each by
    /// itself: held when one drive positively holds it, free otherwise. One drive the registry
    /// cannot identify never refuses an image it does not name.
    pub(super) fn kernel_hold(
        drives: &[DriveBacking],
        file: (u64, u64),
        image: &std::path::Path,
    ) -> KernelHold {
        if drives.iter().any(|drive| drive.holds(file, image)) {
            return KernelHold::Held;
        }
        KernelHold::Free {
            unproven: drives.iter().filter_map(DriveBacking::unproven).collect(),
        }
    }

    /// The registry keys a drive's backing file is read under.
    struct BackingKeys {
        backing: Cf,
        lists: [Cf; 2],
        source: Cf,
    }

    impl BackingKeys {
        fn new() -> io::Result<Self> {
            Ok(Self {
                backing: key("backingstore-id")?,
                lists: [key("writeable-components")?, key("readonly-components")?],
                source: key(Driver::InKernel.source_key())?,
            })
        }
    }

    /// What every registered in-kernel drive names as the file it holds, each read by itself
    /// ([`drive_backing`]); a drive that left the registry mid-read is departing and passed over.
    pub(super) fn in_kernel_backings() -> io::Result<Vec<DriveBacking>> {
        let keys = BackingKeys::new()?;
        Ok(registered(Driver::InKernel.class())?
            .filter_map(|drive| drive_backing(|key| property(&drive, key), &keys))
            .collect())
    }

    /// What one in-kernel drive names as its backing file, each property read through
    /// `property`; `None` when the drive left the registry mid-read. A `backingstore-id` that is
    /// absent, unreadable or malformed fails this drive alone over to its `KDIURLPath`, which is
    /// read only then.
    fn drive_backing(
        property: impl Fn(&Cf) -> Read<Option<Cf>>,
        keys: &BackingKeys,
    ) -> Option<DriveBacking> {
        let why = match property(&keys.backing) {
            Ok(Some(identity)) => match backing_components(&identity, &keys.lists) {
                Ok(files) => return Some(DriveBacking::Files(files)),
                Err(error) => error.to_string(),
            },
            Ok(None) => "it registers no backingstore-id".to_owned(),
            Err(Unread::Gone) => return None,
            Err(Unread::Failed(error)) => format!("its backingstore-id is unreadable: {error}"),
        };
        let name = Driver::InKernel.source_key();
        let unidentified = match property(&keys.source) {
            Ok(Some(value)) => match cf_string(value.0, name) {
                Ok(source) => {
                    return Some(match Driver::InKernel.source(&source) {
                        DiskImageSource::File(path) => DriveBacking::Path { path, why },
                        DiskImageSource::Url(url) => DriveBacking::Store { url },
                    });
                }
                Err(error) => error.to_string(),
            },
            Ok(None) => format!("it registers no {name}"),
            Err(Unread::Gone) => return None,
            Err(Unread::Failed(error)) => format!("its {name} is unreadable: {error}"),
        };
        Some(DriveBacking::Unidentified {
            why: format!("{why}; {unidentified}"),
        })
    }

    /// The `d<dev>:i<ino>` components of one drive's `backingstore-id` dictionary.
    fn backing_components(identity: &Cf, lists: &[Cf; 2]) -> io::Result<Vec<(u64, u64)>> {
        // SAFETY: `identity` is a live CF object.
        if unsafe { CFGetTypeID(identity.0) != CFDictionaryGetTypeID() } {
            return Err(invalid(
                "registry property backingstore-id is not a dictionary",
            ));
        }
        let mut files = Vec::new();
        for list_key in lists {
            // SAFETY: `identity` is a live CFDictionary; the answer is borrowed from it, or null.
            let list = unsafe { CFDictionaryGetValue(identity.0, list_key.0) };
            if list.is_null() {
                continue;
            }
            // SAFETY: `list` is a live CF object borrowed from `identity`.
            if unsafe { CFGetTypeID(list) != CFArrayGetTypeID() } {
                return Err(invalid("a backingstore-id component list is not an array"));
            }
            // SAFETY: `list` is a live CFArray.
            for index in 0..unsafe { CFArrayGetCount(list) } {
                // SAFETY: `index` is below the array's count; the value is borrowed from it.
                let component = unsafe { CFArrayGetValueAtIndex(list, index) };
                files.push(parse_backing_component(&cf_string(
                    component,
                    "backingstore-id component",
                )?)?);
            }
        }
        if files.is_empty() {
            return Err(invalid(
                "registry property backingstore-id names no backing file",
            ));
        }
        Ok(files)
    }

    /// One `backingstore-id` component, `d<dev>:i<ino>` in decimal, as `(device, inode)`.
    fn parse_backing_component(component: &str) -> io::Result<(u64, u64)> {
        component
            .strip_prefix('d')
            .and_then(|rest| rest.split_once(":i"))
            .and_then(|(device, inode)| Some((device.parse().ok()?, inode.parse().ok()?)))
            .ok_or_else(|| {
                invalid(format!(
                    "backingstore-id component {component:?} is not d<dev>:i<ino>"
                ))
            })
    }

    /// Every attachment the kernel holds for the local file at `image`: each driver's
    /// attachments whose source the kernel matched to that file's path or URL, each with the
    /// media in its own subtree. No other image is read, however many are attached.
    pub(super) fn attachments_of(image: &std::path::Path) -> io::Result<Vec<AttachedDiskImage>> {
        let mut attachments = Vec::new();
        for driver in Driver::ALL {
            let properties = mutable_dictionary()?;
            let source_key = key(driver.source_key())?;
            let source = driver.source_of(image)?;
            // SAFETY: `properties` is a live mutable dictionary, which retains the key and value.
            unsafe { CFDictionarySetValue(properties.0, source_key.0, source.0) };
            attachments.extend(image_devices(
                registered_matching(driver.class(), Some(&properties))?,
                driver,
            )?);
        }
        // The kernel matched a spelling; the backing path it names decides.
        Ok(attachments
            .into_iter()
            .filter(|attached| attached.is_backed_by(image))
            .collect())
    }

    /// Each attachment of `driver` in one registry snapshot with the BSD media in its own subtree,
    /// in registry order. One that publishes no media yet exposes nothing and is not reported;
    /// one that leaves the registry mid-read is departing and is not reported either.
    fn image_devices(devices: Entries, driver: Driver) -> io::Result<Vec<AttachedDiskImage>> {
        let keys = Keys {
            bsd_name: key("BSD Name")?,
            content: key("Content")?,
            size: key("Size")?,
            source: key(driver.source_key())?,
            owner: key("owner-uid")?,
            driver,
        };
        let mut images: BTreeMap<u64, AttachedDiskImage> = BTreeMap::new();
        for device in devices {
            settle(record_image_device(&mut images, &device, &keys))?;
        }
        Ok(images.into_values().collect())
    }

    /// The media one image device exposes: its whole media's size and node, and every BSD media
    /// node.
    #[derive(Default)]
    struct DeviceMedia {
        capacity: Option<ImageCapacity>,
        whole: Option<String>,
        media: Vec<ImageMedia>,
    }

    /// The whole disk (`/dev/diskN`) of the image device registered as `entry_id`, of any image
    /// driver, a RAM disk's included: `None` once that entry has left the registry or while it
    /// has no whole media. A detach bound to the entry reads it inside the host disk admission
    /// ([`super::SystemCommandRunner::run_detach_of_entry`]).
    pub(super) fn whole_disk_of_entry(entry_id: u64) -> io::Result<Option<String>> {
        // SAFETY: the answer is a +1 matching dictionary or null.
        let matching = unsafe { IORegistryEntryIDMatching(entry_id) };
        if matching.is_null() {
            return Err(io::Error::other(format!(
                "IORegistryEntryIDMatching({entry_id:#x}) returned no matching dictionary"
            )));
        }
        // SAFETY: `matching` is a +1 dictionary the call consumes; the answer is +1 or 0.
        let service = unsafe { IOServiceGetMatchingService(MAIN_PORT_DEFAULT, matching) };
        if service == 0 {
            return Ok(None);
        }
        let device = Object(service);
        // Source and ownership keys are not read here: only the media keys are.
        let keys = Keys {
            bsd_name: key("BSD Name")?,
            content: key("Content")?,
            size: key("Size")?,
            source: key(Driver::InKernel.source_key())?,
            owner: key("owner-uid")?,
            driver: Driver::InKernel,
        };
        let read = (|| -> Read<Option<String>> {
            let mut found = DeviceMedia::default();
            while !read_media(&device, 0, &keys, &mut found)? {
                found = DeviceMedia::default();
            }
            Ok(found.whole)
        })();
        match read {
            Ok(whole) => Ok(whole),
            Err(Unread::Gone) => Ok(None),
            Err(Unread::Failed(error)) => Err(error),
        }
    }

    /// Record one image device with every BSD media node in its own subtree, reading the subtree
    /// again whenever a concurrent attach or detach invalidated a child list. Every read lands
    /// before anything is recorded, so a device that leaves the registry mid-read leaves no trace.
    fn record_image_device(
        images: &mut BTreeMap<u64, AttachedDiskImage>,
        device: &Object,
        keys: &Keys,
    ) -> Read<()> {
        let id = entry_id(device, "read a disk image's registry id")?;
        let source_key = keys.driver.source_key();
        let Some(source) = string_property(device, &keys.source, source_key)? else {
            return Err(invalid(format!("a disk image device registers no {source_key}")).into());
        };
        // Both drivers record the attaching uid on the device itself (measured 2026-10-22);
        // Disk Arbitration takes a disk with none as root's.
        let owner_uid = match u64_property(device, &keys.owner, "owner-uid")? {
            Some(uid) => libc::uid_t::try_from(uid)
                .map_err(|_| invalid(format!("registry property owner-uid {uid} is not a uid")))?,
            None => 0,
        };
        let mut found = DeviceMedia::default();
        while !read_media(device, 0, keys, &mut found)? {
            found = DeviceMedia::default();
        }
        if found.media.is_empty() {
            return Ok(());
        }
        found
            .media
            .sort_by(|left, right| left.device.cmp(&right.device));
        images.insert(
            id,
            AttachedDiskImage {
                source: keys.driver.source(&source),
                capacity: found.capacity,
                media: found.media,
                owner_uid,
            },
        );
        Ok(())
    }

    /// Read the BSD media below `entry` into `found`, `media_above` being how many IOMedia lie
    /// between `entry` and its image device: the device's whole media is the one with none above
    /// it. A child the kernel finalizes mid-read is departing and is skipped with everything below
    /// it, and another image device is never part of this one. Answers whether every child list
    /// read was consistent.
    fn read_media(
        entry: &Object,
        media_above: usize,
        keys: &Keys,
        found: &mut DeviceMedia,
    ) -> Read<bool> {
        let mut walk = children(entry)?;
        let mut consistent = true;
        for child in walk.by_ref() {
            if Driver::any_conforms(&child) {
                continue;
            }
            match read_child(&child, media_above, keys, found) {
                Ok(read) => consistent &= read,
                Err(Unread::Gone) => {}
                Err(failed) => return Err(failed),
            }
        }
        Ok(consistent && walk.consistent())
    }

    /// Record `child` when it is an IOMedia, then read the media below it.
    fn read_child(
        child: &Object,
        media_above: usize,
        keys: &Keys,
        found: &mut DeviceMedia,
    ) -> Read<bool> {
        let below = if conforms(child, c"IOMedia") {
            record_media(child, media_above == 0, keys, found)?;
            media_above + 1
        } else {
            media_above
        };
        read_media(child, below, keys, found)
    }

    /// Record one IOMedia node of an image device if it has a BSD name, with its size when it is
    /// the device's whole media. Every read lands before anything is recorded.
    fn record_media(
        media: &Object,
        is_whole: bool,
        keys: &Keys,
        found: &mut DeviceMedia,
    ) -> Read<()> {
        let Some(name) = string_property(media, &keys.bsd_name, "BSD Name")? else {
            return Ok(());
        };
        let capacity = if is_whole {
            Some(u64_property(media, &keys.size, "Size")?.map(ImageCapacity::from_bytes))
        } else {
            None
        };
        let content = string_property(media, &keys.content, "Content")?.unwrap_or_default();
        if let Some(capacity) = capacity {
            found.capacity = capacity;
            found.whole = Some(format!("/dev/{name}"));
        }
        found.media.push(ImageMedia {
            device: format!("/dev/{name}"),
            content,
        });
        Ok(())
    }

    /// Every APFS container and volume registered at one instant, from one IOMedia snapshot:
    /// `AppleAPFSMedia` is a container's synthesized whole disk,
    /// and each `AppleAPFSVolume` hangs from an `AppleAPFSContainer` that hangs from one. A
    /// container whose volumes are all gone, or not yet started, is still reported. A volume
    /// whose provider chain has already left the plane, or whose entries the kernel finalizes
    /// mid-read, is departing and is not; any other read failure fails the whole inventory,
    /// which [`super::project_registered_apfs`] validates.
    pub fn registered_apfs_containers() -> io::Result<Vec<RegisteredApfsContainer>> {
        let keys = ApfsKeys {
            bsd_name: key("BSD Name")?,
            size: key("Size")?,
            full_name: key("FullName")?,
            uuid: key("UUID")?,
            role: key("RoleValue")?,
            encrypted: key("Encrypted")?,
        };
        let mut media: BTreeMap<u64, ApfsMediaFacts> = BTreeMap::new();
        let mut volumes = Vec::new();
        for entry in registered(c"IOMedia")? {
            settle(record_apfs_entry(&mut media, &mut volumes, &entry, &keys))?;
        }
        project_registered_apfs(&media, volumes)
    }

    pub(crate) fn disk_image_user_client_creators(
        image: Option<&std::path::Path>,
    ) -> io::Result<Vec<String>> {
        let creator_key = key("IOUserClientCreator")?;
        let image_key = image.map(|_| key("DiskImageURL")).transpose()?;
        let mut creators = Vec::new();
        for device in registered(c"AppleDiskImageDevice")? {
            settle((|| {
                if let (Some(image), Some(image_key)) = (image, image_key.as_ref()) {
                    let Some(url) = string_property(&device, image_key, "DiskImageURL")? else {
                        return Err(invalid("a disk image device registers no DiskImageURL").into());
                    };
                    if !matches!(DiskImageSource::from_url(&url), DiskImageSource::File(path) if path == image)
                    {
                        return Ok(());
                    }
                }
                record_user_client_creators(&mut creators, &device, &creator_key)
            })())?;
        }
        Ok(creators)
    }

    /// The image devices, their user clients, APFS volumes and media, each as its live kernel
    /// instances against the registry entries that account for them (attach_budget.rs). The
    /// registry is read first and the instances after, so each count can be off by the disk
    /// commands in flight between the two reads.
    pub(crate) fn disk_image_class_counts() -> io::Result<crate::attach_budget::KernelReading> {
        use crate::attach_budget::{ClassCount, KernelReading};
        let registered = [
            registered_exactly(c"AppleDiskImageDevice")?,
            u64::try_from(disk_image_user_client_creators(None)?.len())
                .map_err(|_| invalid("more user clients than a u64 counts"))?,
            registered_exactly(c"AppleAPFSVolume")?,
            registered_exactly(c"IOMedia")?,
        ];
        let instances = class_instances([
            "AppleDiskImageDevice",
            "DIDeviceIOUserClient",
            "AppleAPFSVolume",
            "IOMedia",
        ])?;
        let [image_devices, user_clients, apfs_volumes, media] =
            std::array::from_fn(|class| ClassCount {
                instances: instances[class],
                registered: registered[class],
            });
        Ok(KernelReading {
            image_devices,
            user_clients,
            apfs_volumes,
            media,
        })
    }

    /// Every registered IOMedia, subclasses included (`AppleAPFSMedia`, `AppleAPFSVolume`): what
    /// an unfiltered IOMedia match notification enumerates, which IOKit caps (media_cap.rs).
    pub(crate) fn registered_media() -> io::Result<u64> {
        u64::try_from(registered(c"IOMedia")?.count())
            .map_err(|_| invalid("more registered media than a u64 counts"))
    }

    /// The user Disk Arbitration takes as the owner of the disk-image disk named `bsd_name`
    /// (`disk9`, `disk9s1`): the first `owner-uid` from its IOMedia up through its providers, as
    /// diskarbitrationd's `DADiskCreateFromIOMedia` searches, ending at its image drive
    /// (`IOHDIXHDDrive`, `AppleDiskImageDevice`), or root when none records one
    /// (release_authority.rs). `None` when no registered IOMedia has that name, when no image
    /// drive lies above it (the host's own disks: their releases are not this check's), or when
    /// it leaves the registry while being read.
    pub(crate) fn disk_image_owner_uid(bsd_name: &str) -> io::Result<Option<libc::uid_t>> {
        let properties = mutable_dictionary()?;
        let name_key = key("BSD Name")?;
        let name = key(bsd_name)?;
        // SAFETY: `properties` is a live mutable dictionary, which retains the key and value.
        unsafe { CFDictionarySetValue(properties.0, name_key.0, name.0) };
        let owner_key = key("owner-uid")?;
        let Some(media) = registered_matching(c"IOMedia", Some(&properties))?.next() else {
            return Ok(None);
        };
        let mut owner = None;
        let mut entry = Some(media);
        while let Some(current) = entry {
            if owner.is_none() {
                owner = match u64_property(&current, &owner_key, "owner-uid") {
                    Ok(uid) => uid,
                    Err(Unread::Gone) => return Ok(None),
                    Err(Unread::Failed(error)) => return Err(error),
                };
            }
            if conforms(&current, c"IOHDIXHDDrive") || conforms(&current, c"AppleDiskImageDevice") {
                let uid = owner.unwrap_or(0);
                return libc::uid_t::try_from(uid).map(Some).map_err(|_| {
                    invalid(format!("registry property owner-uid {uid} is not a uid"))
                });
            }
            entry = match provider(&current) {
                Ok(parent) => parent,
                Err(Unread::Gone) => return Ok(None),
                Err(Unread::Failed(error)) => return Err(error),
            };
        }
        Ok(None)
    }

    /// The registry entry of the legacy image drive (`IOHDIXHDDrive`, a RAM disk's) whose whole
    /// disk is `bsd_name` (`disk9`): the identity a RAM disk keeps until it leaves the registry,
    /// which no later disk reuses. `None` when no registered IOMedia has that name, when no legacy
    /// image drive lies above it, or when it leaves the registry while being read.
    pub(super) fn legacy_image_entry(bsd_name: &str) -> io::Result<Option<u64>> {
        let properties = mutable_dictionary()?;
        let name_key = key("BSD Name")?;
        let name = key(bsd_name)?;
        // SAFETY: `properties` is a live mutable dictionary, which retains the key and value.
        unsafe { CFDictionarySetValue(properties.0, name_key.0, name.0) };
        let Some(media) = registered_matching(c"IOMedia", Some(&properties))?.next() else {
            return Ok(None);
        };
        let mut entry = Some(media);
        while let Some(current) = entry {
            if conforms(&current, c"IOHDIXHDDrive") {
                return match entry_id(&current, "read a RAM disk's registry id") {
                    Ok(id) => Ok(Some(id)),
                    Err(Unread::Gone) => Ok(None),
                    Err(Unread::Failed(error)) => Err(error),
                };
            }
            entry = match provider(&current) {
                Ok(parent) => parent,
                Err(Unread::Gone) => return Ok(None),
                Err(Unread::Failed(error)) => return Err(error),
            };
        }
        Ok(None)
    }

    /// A legacy RAM disk as one registry snapshot lists it: its registry entry, its whole disk once
    /// it has published one, and every BSD media node below it.
    pub(super) struct RegisteredRamDisk {
        pub(super) entry: u64,
        pub(super) whole: Option<String>,
        pub(super) media: Vec<String>,
    }

    /// Every legacy RAM disk of `uid` whose image is `ram://<sectors>`, from one registry snapshot.
    /// One that leaves the registry mid-read is departing and is not listed.
    pub(super) fn ram_disks_of(
        sectors: u64,
        uid: libc::uid_t,
    ) -> io::Result<Vec<RegisteredRamDisk>> {
        let image = format!("ram://{sectors}");
        let keys = Keys {
            bsd_name: key("BSD Name")?,
            content: key("Content")?,
            size: key("Size")?,
            source: key("image-path")?,
            owner: key("owner-uid")?,
            driver: Driver::InKernel,
        };
        let mut disks = Vec::new();
        for device in registered(c"IOHDIXHDDriveOutKernel")? {
            let read = (|| -> Read<Option<RegisteredRamDisk>> {
                let id = entry_id(&device, "read a RAM disk's registry id")?;
                let Some(path) = property(&device, &keys.source)? else {
                    return Ok(None);
                };
                if data_text(path.0, "image-path")? != image
                    || u64_property(&device, &keys.owner, "owner-uid")? != Some(u64::from(uid))
                {
                    return Ok(None);
                }
                let mut found = DeviceMedia::default();
                while !read_media(&device, 0, &keys, &mut found)? {
                    found = DeviceMedia::default();
                }
                let media = found.media.into_iter().map(|media| media.device).collect();
                Ok(Some(RegisteredRamDisk {
                    entry: id,
                    whole: found.whole,
                    media,
                }))
            })();
            match read {
                Ok(Some(disk)) => disks.push(disk),
                Ok(None) | Err(Unread::Gone) => {}
                Err(Unread::Failed(error)) => return Err(error),
            }
        }
        Ok(disks)
    }

    /// Whether `entry_id` is still registered. An entry id is never reused, so its absence alone
    /// proves its device left the registry; an entry with no whole media yet is still there.
    pub(super) fn entry_registered(entry_id: u64) -> io::Result<bool> {
        // SAFETY: the answer is a +1 matching dictionary or null.
        let matching = unsafe { IORegistryEntryIDMatching(entry_id) };
        if matching.is_null() {
            return Err(io::Error::other(format!(
                "IORegistryEntryIDMatching({entry_id:#x}) returned no matching dictionary"
            )));
        }
        // SAFETY: `matching` is a +1 dictionary the call consumes; the answer is +1 or 0.
        let service = unsafe { IOServiceGetMatchingService(MAIN_PORT_DEFAULT, matching) };
        if service == 0 {
            return Ok(false);
        }
        drop(Object(service));
        Ok(true)
    }

    /// The text of `value`, a live CF object borrowed for the call, which must be data holding
    /// UTF-8, as a legacy drive's `image-path` does, a trailing NUL dropped.
    fn data_text(value: CfType, name: &str) -> io::Result<String> {
        // SAFETY: `value` is a live CF object.
        if unsafe { CFGetTypeID(value) != CFDataGetTypeID() } {
            return Err(invalid(format!("registry property {name} is not data")));
        }
        // SAFETY: `value` is a live CFData.
        let (length, bytes) = unsafe { (CFDataGetLength(value), CFDataGetBytePtr(value)) };
        let bytes = match usize::try_from(length) {
            // SAFETY: CFData's byte pointer is valid for its length while `value` lives.
            Ok(length) if length > 0 && !bytes.is_null() => unsafe {
                std::slice::from_raw_parts(bytes, length)
            },
            _ => &[],
        };
        let text = bytes.strip_suffix(&[0]).unwrap_or(bytes);
        String::from_utf8(text.to_vec())
            .map_err(|_| invalid(format!("registry property {name} is not UTF-8")))
    }

    /// Registered entries whose class is exactly `class`, subclasses excluded, as the kernel's
    /// instance counts count them. An entry that leaves the registry mid-read is departing.
    fn registered_exactly(class: &CStr) -> io::Result<u64> {
        let mut count = 0;
        for entry in registered(class)? {
            let mut name: [c_char; 128] = [0; 128];
            // SAFETY: `entry` is a registry reference and `name` is an `io_name_t`, the 128
            // bytes the call writes a NUL-terminated class name into.
            let status = unsafe { IOObjectGetClass(entry.0, name.as_mut_ptr()) };
            match checked("read a registry entry's class", status) {
                Ok(()) => {}
                Err(Unread::Gone) => continue,
                Err(Unread::Failed(error)) => return Err(error),
            }
            let bytes = name.map(|byte| byte.to_ne_bytes()[0]);
            let named = CStr::from_bytes_until_nul(&bytes)
                .map_err(|_| invalid("a registry class name is not NUL-terminated"))?;
            if named == class {
                count += 1;
            }
        }
        Ok(count)
    }

    /// The live kernel instances of each of `classes`, as `ioclasscount` reads them: the
    /// `Classes` dictionary of the registry root's `IOKitDiagnostics`, one exact-class count per
    /// class, registered or not. A class the kernel never loaded counts none.
    fn class_instances<const N: usize>(classes: [&str; N]) -> io::Result<[u64; N]> {
        // SAFETY: the default main port names the kernel's registry; a null answer is refused.
        let root = unsafe { IORegistryGetRootEntry(MAIN_PORT_DEFAULT) };
        if root == 0 {
            return Err(io::Error::other("IORegistryGetRootEntry answered no root"));
        }
        let root = Object(root);
        let diagnostics = match property(&root, &key("IOKitDiagnostics")?) {
            Ok(Some(diagnostics)) => diagnostics,
            Ok(None) => return Err(invalid("the registry root publishes no IOKitDiagnostics")),
            Err(Unread::Gone) => return Err(io::Error::other("the registry root is gone")),
            Err(Unread::Failed(error)) => return Err(error),
        };
        let counts = dictionary_value(diagnostics.0, &key("Classes")?, "IOKitDiagnostics")?
            .ok_or_else(|| invalid("IOKitDiagnostics names no Classes"))?;
        let mut instances = [0; N];
        for (slot, class) in instances.iter_mut().zip(classes) {
            if let Some(count) = dictionary_value(counts, &key(class)?, "IOKitDiagnostics Classes")?
            {
                *slot = borrowed_u64(count, class)?;
            }
        }
        Ok(instances)
    }

    /// `dictionary[key]`, borrowed from `dictionary`: valid while it is, and never released.
    fn dictionary_value(dictionary: CfType, key: &Cf, name: &str) -> io::Result<Option<CfType>> {
        // SAFETY: `dictionary` is a live CF object.
        if unsafe { CFGetTypeID(dictionary) != CFDictionaryGetTypeID() } {
            return Err(invalid(format!("{name} is not a dictionary")));
        }
        // SAFETY: `dictionary` is a live CFDictionary and `key` a CFString; the answer is
        // borrowed or null.
        let value = unsafe { CFDictionaryGetValue(dictionary, key.0) };
        Ok((!value.is_null()).then_some(value))
    }

    /// A borrowed CFNumber as a non-negative 64-bit integer.
    fn borrowed_u64(value: CfType, name: &str) -> io::Result<u64> {
        // SAFETY: `value` is a live CF object borrowed from its dictionary.
        if unsafe { CFGetTypeID(value) != CFNumberGetTypeID() } {
            return Err(invalid(format!(
                "the instance count of {name} is not a number"
            )));
        }
        let mut number = 0i64;
        // SAFETY: `value` is a live CFNumber and `number` is writable storage for an SInt64.
        if unsafe { CFNumberGetValue(value, CF_NUMBER_SINT64_TYPE, (&raw mut number).cast()) } == 0
        {
            return Err(invalid(format!(
                "the instance count of {name} is not an exact 64-bit integer"
            )));
        }
        u64::try_from(number)
            .map_err(|_| invalid(format!("the instance count of {name} is negative")))
    }

    /// Record one device's user client creators, walking its children again whenever a
    /// concurrent registry change invalidated the walk, so no client is skipped.
    fn record_user_client_creators(
        creators: &mut Vec<String>,
        device: &Object,
        key: &Cf,
    ) -> Read<()> {
        loop {
            let mut walk = children(device)?;
            let mut found = Vec::new();
            for child in walk.by_ref() {
                if !conforms(&child, c"DIDeviceIOUserClient") {
                    continue;
                }
                let Some(creator) = string_property(&child, key, "IOUserClientCreator")? else {
                    return Err(invalid(
                        "a disk image's DIDeviceIOUserClient names no IOUserClientCreator",
                    )
                    .into());
                };
                found.push(creator);
            }
            if walk.consistent() {
                creators.append(&mut found);
                return Ok(());
            }
        }
    }

    /// Record one IOMedia node if it is an APFS container's media or an APFS volume. A volume's
    /// own reads all land before its container media is recorded.
    fn record_apfs_entry(
        media: &mut BTreeMap<u64, ApfsMediaFacts>,
        volumes: &mut Vec<ApfsVolumeFacts>,
        entry: &Object,
        keys: &ApfsKeys,
    ) -> Read<()> {
        if conforms(entry, c"AppleAPFSMedia") {
            record_apfs_media(media, entry, keys)?;
        } else if conforms(entry, c"AppleAPFSVolume") {
            let identifier = string_property(entry, &keys.bsd_name, "BSD Name")?;
            let Some(container_media) = apfs_volume_media(entry, identifier.as_deref())? else {
                return Ok(());
            };
            let name = string_property(entry, &keys.full_name, "FullName")?;
            let uuid = string_property(entry, &keys.uuid, "UUID")?;
            let role = u64_property(entry, &keys.role, "RoleValue")?;
            let encrypted = bool_property(entry, &keys.encrypted, "Encrypted")?;
            volumes.push(ApfsVolumeFacts {
                media: record_apfs_media(media, &container_media, keys)?,
                identifier,
                name,
                uuid,
                role,
                encrypted,
            });
        }
        Ok(())
    }

    /// Read one `AppleAPFSMedia` once, however many of its volumes reach it, and answer its
    /// registry entry id.
    fn record_apfs_media(
        media: &mut BTreeMap<u64, ApfsMediaFacts>,
        entry: &Object,
        keys: &ApfsKeys,
    ) -> Read<u64> {
        let id = entry_id(entry, "read an APFS container media's registry id")?;
        if let Entry::Vacant(slot) = media.entry(id) {
            let reference = string_property(entry, &keys.bsd_name, "BSD Name")?;
            let capacity_bytes = u64_property(entry, &keys.size, "Size")?;
            slot.insert(ApfsMediaFacts {
                reference,
                capacity_bytes,
            });
        }
        Ok(id)
    }

    /// The `AppleAPFSMedia` a volume's `AppleAPFSContainer` provider hangs from, or `None` once
    /// either link has left the plane. Any other provider is not APFS and is refused.
    fn apfs_volume_media(volume: &Object, identifier: Option<&str>) -> Read<Option<Object>> {
        let named = identifier.unwrap_or("<unpublished>");
        let Some(container) = provider(volume)? else {
            return Ok(None);
        };
        if !conforms(&container, c"AppleAPFSContainer") {
            alive(&container)?;
            return Err(invalid(format!(
                "APFS volume {named} hangs from a provider that is not an AppleAPFSContainer"
            ))
            .into());
        }
        let Some(media) = provider(&container)? else {
            return Ok(None);
        };
        if !conforms(&media, c"AppleAPFSMedia") {
            alive(&media)?;
            return Err(invalid(format!(
                "the AppleAPFSContainer of APFS volume {named} hangs from a provider that is not an AppleAPFSMedia"
            ))
            .into());
        }
        Ok(Some(media))
    }

    /// The entry's provider in the service plane, or `None` once it is detached from the plane.
    fn provider(entry: &Object) -> Read<Option<Object>> {
        let mut parent: IoObject = 0;
        // SAFETY: `entry` is a registry reference, the plane name is NUL-terminated, and
        // `parent` is writable.
        match unsafe { IORegistryEntryGetParentEntry(entry.0, SERVICE_PLANE.as_ptr(), &mut parent) }
        {
            IO_RETURN_NO_DEVICE => Ok(None),
            // Only a successful call hands back a reference to wrap.
            status => checked("read a registry provider", status).map(|()| Some(Object(parent))),
        }
    }

    /// Whether the entry is an instance of `class`. IOKit folds every failure into `false`, a
    /// finalized entry's included; where `false` would be an error, ask [`alive`] first.
    fn conforms(entry: &Object, class: &CStr) -> bool {
        // SAFETY: `entry` is a registry reference and `class` is NUL-terminated.
        unsafe { IOObjectConformsTo(entry.0, class.as_ptr()) != 0 }
    }

    /// Tell an entry that answered nothing because it left the registry from one still there.
    fn alive(entry: &Object) -> Read<()> {
        entry_id(entry, "confirm a registry entry is still registered")?;
        Ok(())
    }

    fn entry_id(entry: &Object, operation: &str) -> Read<u64> {
        let mut id = 0u64;
        // SAFETY: `entry` is a registry reference and `id` is writable.
        let status = unsafe { IORegistryEntryGetRegistryEntryID(entry.0, &mut id) };
        checked(operation, status).map(|()| id)
    }

    fn key(name: &str) -> io::Result<Cf> {
        let length = CfIndex::try_from(name.len())
            .map_err(|_| invalid(format!("registry key {name:?} is too long")))?;
        // SAFETY: `name` is valid UTF-8 for `length` bytes and is copied by the call.
        let created = unsafe {
            CFStringCreateWithBytes(
                ptr::null(),
                name.as_ptr(),
                length,
                CF_STRING_ENCODING_UTF8,
                0,
            )
        };
        if created.is_null() {
            Err(io::Error::other(format!(
                "CoreFoundation could not create the registry key {name:?}"
            )))
        } else {
            Ok(Cf(created))
        }
    }

    /// The entry's `key` property, or `None` when a still-registered entry does not have it.
    /// IOKit answers null both for an absent property and for any failure, a finalized entry's
    /// included, so a null answer is resolved through [`alive`].
    fn property(entry: &Object, key: &Cf) -> Read<Option<Cf>> {
        // SAFETY: `entry` is a registry reference and `key` is a CFString; the answer is +1 or
        // null.
        let value = unsafe { IORegistryEntryCreateCFProperty(entry.0, key.0, ptr::null(), 0) };
        // A null answer must never be wrapped: the wrapper releases what it holds.
        if value.is_null() {
            alive(entry)?;
            Ok(None)
        } else {
            Ok(Some(Cf(value)))
        }
    }

    fn string_property(entry: &Object, key: &Cf, name: &str) -> Read<Option<String>> {
        let Some(value) = property(entry, key)? else {
            return Ok(None);
        };
        Ok(Some(cf_string(value.0, name)?))
    }

    /// The UTF-8 text of `value`, a live CF object borrowed for the call, which must be a string.
    fn cf_string(value: CfType, name: &str) -> io::Result<String> {
        // SAFETY: `value` is a live CF object.
        if unsafe { CFGetTypeID(value) != CFStringGetTypeID() } {
            return Err(invalid(format!("registry property {name} is not a string")));
        }
        let range = CfRange {
            location: 0,
            // SAFETY: `value` is a live CFString.
            length: unsafe { CFStringGetLength(value) },
        };
        let mut needed: CfIndex = 0;
        // SAFETY: a null buffer asks only for the UTF-8 length of the whole range.
        let measured = unsafe {
            CFStringGetBytes(
                value,
                range,
                CF_STRING_ENCODING_UTF8,
                0,
                0,
                ptr::null_mut(),
                0,
                &mut needed,
            )
        };
        let capacity = usize::try_from(needed)
            .map_err(|_| invalid(format!("registry property {name} has a negative length")))?;
        let mut bytes = vec![0u8; capacity];
        let mut used: CfIndex = 0;
        // SAFETY: `bytes` is writable for `needed` bytes, the capacity the call is given.
        let converted = unsafe {
            CFStringGetBytes(
                value,
                range,
                CF_STRING_ENCODING_UTF8,
                0,
                0,
                bytes.as_mut_ptr(),
                needed,
                &mut used,
            )
        };
        if measured != range.length || converted != range.length || used != needed {
            return Err(invalid(format!(
                "registry property {name} did not convert to UTF-8 whole"
            )));
        }
        String::from_utf8(bytes)
            .map_err(|_| invalid(format!("registry property {name} is not UTF-8")))
    }

    fn u64_property(entry: &Object, key: &Cf, name: &str) -> Read<Option<u64>> {
        let Some(value) = property(entry, key)? else {
            return Ok(None);
        };
        // SAFETY: `value` is a live CF object.
        if unsafe { CFGetTypeID(value.0) != CFNumberGetTypeID() } {
            return Err(invalid(format!("registry property {name} is not a number")).into());
        }
        let mut number = 0i64;
        // SAFETY: `value` is a live CFNumber and `number` is writable storage for an SInt64.
        if unsafe { CFNumberGetValue(value.0, CF_NUMBER_SINT64_TYPE, (&raw mut number).cast()) }
            == 0
        {
            return Err(invalid(format!(
                "registry property {name} is not an exact 64-bit integer"
            ))
            .into());
        }
        let number = u64::try_from(number)
            .map_err(|_| invalid(format!("registry property {name} is negative")))?;
        Ok(Some(number))
    }

    fn bool_property(entry: &Object, key: &Cf, name: &str) -> Read<Option<bool>> {
        let Some(value) = property(entry, key)? else {
            return Ok(None);
        };
        // SAFETY: `value` is a live CF object.
        if unsafe { CFGetTypeID(value.0) != CFBooleanGetTypeID() } {
            return Err(invalid(format!("registry property {name} is not a boolean")).into());
        }
        // SAFETY: `value` is a live CFBoolean.
        Ok(Some(unsafe { CFBooleanGetValue(value.0) } != 0))
    }

    fn invalid(message: impl Into<String>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, message.into())
    }

    fn kernel_error(operation: &str, status: KernReturn) -> io::Error {
        io::Error::other(format!("{operation}: IOKit returned {status:#010x}"))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// The kernel spells a drive's backing file `d<dev>:i<ino>`, as `ioreg` showed it for
        /// a read-write and a read-only in-kernel attach (01a_apfs.md, "Images").
        #[test]
        fn a_backing_component_is_the_device_and_inode_the_kernel_spells() {
            assert_eq!(
                parse_backing_component("d16777231:i1374749620").unwrap(),
                (16_777_231, 1_374_749_620)
            );
            for malformed in ["16777231:i1", "d1:1", "d1:i", "dx:i1", "d1:i1x", ""] {
                assert_eq!(
                    parse_backing_component(malformed).unwrap_err().kind(),
                    io::ErrorKind::InvalidData,
                    "{malformed:?}"
                );
            }
        }

        #[link(name = "CoreFoundation", kind = "framework")]
        unsafe extern "C" {
            fn CFDataCreate(allocator: CfType, bytes: *const u8, length: CfIndex) -> CfType;
            fn CFPropertyListCreateWithData(
                allocator: CfType,
                data: CfType,
                options: usize,
                format: *mut CfIndex,
                error: *mut CfType,
            ) -> CfType;
        }

        /// The identifying properties of a read-write in-kernel drive, as
        /// `ioreg -a -r -c IOHDIXHDDriveInKernel -d 1` printed them on 2026-10-22, the image path
        /// replaced by a neutral one.
        const CLASS: &str = "<key>IOClass</key><string>IOHDIXHDDriveInKernel</string>";
        const SOURCE_PATH: &str = "<key>KDIURLPath</key>\
            <string>/private/cowshed/store/example/codebase/sessions/fixture.dmg</string>";
        const BACKING_ID: &str = "<key>backingstore-id</key><dict><key>writeable-components</key>\
            <array><string>d16777230:i120596975</string></array></dict>";
        const IMAGE: &str = "/private/cowshed/store/example/codebase/sessions/fixture.dmg";
        const HELD: (u64, u64) = (16_777_230, 120_596_975);
        const OTHER_IMAGE: &str = "/private/cowshed/store/smoothbricks/sessions/other.dmg";

        /// One drive judged from its registry properties, given as `ioreg -a` prints them and
        /// parsed by CoreFoundation into the dictionary the registry answers them from.
        fn drive(properties: &[&str]) -> DriveBacking {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><plist version=\"1.0\"><dict>{}</dict></plist>",
                properties.concat()
            );
            let length = CfIndex::try_from(xml.len()).expect("fixture length");
            // SAFETY: `xml` is readable for `length` bytes, which the call copies; the answer is
            // +1 or null.
            let data = unsafe { CFDataCreate(ptr::null(), xml.as_ptr(), length) };
            assert!(!data.is_null(), "copy the fixture plist");
            let data = Cf(data);
            // SAFETY: `data` is a live CFData, and neither the format nor an error is asked for;
            // the answer is +1 or null.
            let parsed = unsafe {
                CFPropertyListCreateWithData(
                    ptr::null(),
                    data.0,
                    0,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            assert!(!parsed.is_null(), "parse the fixture plist");
            let properties = Cf(parsed);
            // SAFETY: `properties` is a live CF object.
            let is_dictionary = unsafe { CFGetTypeID(properties.0) == CFDictionaryGetTypeID() };
            assert!(is_dictionary, "a drive's properties are a dictionary");
            let keys = BackingKeys::new().expect("registry keys");
            drive_backing(
                |key| {
                    // SAFETY: `properties` is a live CFDictionary and `key` a CFString; the value
                    // is borrowed from it, or null.
                    let value = unsafe { CFDictionaryGetValue(properties.0, key.0) };
                    if value.is_null() {
                        return Ok(None);
                    }
                    // SAFETY: `value` is live while `properties` is; retaining it makes it +1.
                    Ok(Some(Cf(unsafe { CFRetain(value) })))
                },
                &keys,
            )
            .expect("a fixture drive never departs")
        }

        fn path(image: &str) -> &std::path::Path {
            std::path::Path::new(image)
        }

        /// A drive that registers its backing file by device and inode holds that file whatever
        /// path reaches it, and no other file, the path its attach was handed included.
        #[test]
        fn a_drive_naming_its_backing_file_holds_that_file_by_device_and_inode() {
            let drives = [drive(&[CLASS, SOURCE_PATH, BACKING_ID])];
            assert_eq!(drives[0], DriveBacking::Files(vec![HELD]));
            assert_eq!(
                kernel_hold(&drives, HELD, path(OTHER_IMAGE)),
                KernelHold::Held
            );
            assert_eq!(
                kernel_hold(&drives, (16_777_230, 1), path(IMAGE)),
                KernelHold::Free { unproven: vec![] }
            );
        }

        /// Audit H5: one registered drive without a `backingstore-id` failed every detached
        /// capture and growth on the host. It is judged by itself now: it refuses the image at
        /// the path its attach was handed, and every other image proceeds, naming it.
        #[test]
        fn a_drive_without_a_backing_id_refuses_only_the_image_at_its_attach_path() {
            let drives = [
                drive(&[CLASS, SOURCE_PATH]),
                drive(&[
                    CLASS,
                    "<key>KDIURLPath</key><string>/private/cowshed/store/a.dmg</string>",
                    "<key>backingstore-id</key><dict><key>readonly-components</key>\
                     <array><string>d1:i2</string></array></dict>",
                ]),
            ];
            assert_eq!(
                drives[0],
                DriveBacking::Path {
                    path: IMAGE.into(),
                    why: "it registers no backingstore-id".to_owned(),
                }
            );
            assert_eq!(drives[1], DriveBacking::Files(vec![(1, 2)]));
            assert_eq!(kernel_hold(&drives, (7, 8), path(IMAGE)), KernelHold::Held);
            assert_eq!(
                kernel_hold(&drives, (1, 2), path(OTHER_IMAGE)),
                KernelHold::Held
            );
            assert_eq!(
                kernel_hold(&drives, (7, 8), path(OTHER_IMAGE)),
                KernelHold::Free {
                    unproven: vec![format!(
                        "the drive attached from {IMAGE} (it registers no backingstore-id)"
                    )],
                }
            );
        }

        /// A `backingstore-id` that does not name a file fails that drive alone over to its
        /// attach path, saying why.
        #[test]
        fn a_malformed_backing_id_falls_back_to_the_attach_path() {
            for (identity, why) in [
                (
                    "<key>backingstore-id</key><string>d16777230:i120596975</string>",
                    "registry property backingstore-id is not a dictionary",
                ),
                (
                    "<key>backingstore-id</key><dict></dict>",
                    "registry property backingstore-id names no backing file",
                ),
                (
                    "<key>backingstore-id</key><dict><key>writeable-components</key>\
                     <array><string>16777230:120596975</string></array></dict>",
                    "backingstore-id component \"16777230:120596975\" is not d<dev>:i<ino>",
                ),
            ] {
                assert_eq!(
                    drive(&[CLASS, SOURCE_PATH, identity]),
                    DriveBacking::Path {
                        path: IMAGE.into(),
                        why: why.to_owned(),
                    },
                    "{identity}"
                );
            }
        }

        /// A drive that names its file neither way refuses no image; it is named as unproven.
        #[test]
        fn a_drive_naming_no_file_refuses_nothing_and_is_named() {
            let drives = [
                drive(&[CLASS]),
                drive(&[CLASS, "<key>KDIURLPath</key><integer>1</integer>"]),
            ];
            assert_eq!(
                drives,
                [
                    DriveBacking::Unidentified {
                        why: "it registers no backingstore-id; it registers no KDIURLPath".to_owned(),
                    },
                    DriveBacking::Unidentified {
                        why: "it registers no backingstore-id; registry property KDIURLPath is not a string"
                            .to_owned(),
                    },
                ]
            );
            assert_eq!(
                kernel_hold(&drives, HELD, path(IMAGE)),
                KernelHold::Free {
                    unproven: vec![
                        "a drive that names no file (it registers no backingstore-id; it registers no KDIURLPath)"
                            .to_owned(),
                        "a drive that names no file (it registers no backingstore-id; registry property \
                         KDIURLPath is not a string)"
                            .to_owned(),
                    ],
                }
            );
        }

        /// The publish runner's failure: a RAM disk (`hdiutil attach -nomount ram://16777218`)
        /// registers `KDIURLPath` `ram://16777218` and no `backingstore-id`, and failed every
        /// detached growth and capture on the host. It holds no file: it refuses nothing and is
        /// not named as unproven.
        #[test]
        fn a_ram_disk_holds_no_file_and_is_not_unproven() {
            let drives = [
                drive(&[
                    CLASS,
                    "<key>KDIURLPath</key><string>ram://16777218</string>",
                ]),
                drive(&[CLASS, SOURCE_PATH, BACKING_ID]),
            ];
            assert_eq!(
                drives[0],
                DriveBacking::Store {
                    url: "ram://16777218".to_owned(),
                }
            );
            assert_eq!(
                kernel_hold(&drives, (7, 8), path(OTHER_IMAGE)),
                KernelHold::Free { unproven: vec![] }
            );
            assert_eq!(
                kernel_hold(&drives, HELD, path(OTHER_IMAGE)),
                KernelHold::Held
            );
        }

        /// A drive that leaves the registry mid-read is departing: it is passed over, neither
        /// holding a file nor named as unproven.
        #[test]
        fn a_drive_departing_mid_read_is_passed_over() {
            let keys = BackingKeys::new().expect("registry keys");
            assert_eq!(drive_backing(|_| Err(Unread::Gone), &keys), None);
        }

        /// `MACH_PORT_RIGHT_DEAD_NAME`.
        const DEAD_NAME: libc::c_uint = 4;

        unsafe extern "C" {
            static mach_task_self_: libc::mach_port_t;
            fn mach_port_allocate(
                task: libc::mach_port_t,
                right: libc::c_uint,
                name: *mut libc::mach_port_t,
            ) -> KernReturn;
        }

        /// A reference to a registry entry the kernel finalized while this task held it.
        /// Finalizing destroys the entry's port, which turns every send right a task holds to it
        /// into a dead name; allocating a dead name directly yields that same right without
        /// racing a real detach, so the vanish lands at a known point instead of by timing.
        fn finalized_entry() -> Object {
            let mut name: libc::mach_port_t = 0;
            // SAFETY: `mach_task_self_` is this task's own port, set before `main`, and `name`
            // is writable.
            let status = unsafe { mach_port_allocate(mach_task_self_, DEAD_NAME, &mut name) };
            assert_eq!(status, KERN_SUCCESS, "allocate a dead name");
            Object(name)
        }

        /// The captured failure: an image detaching elsewhere finalized a provider the walk
        /// was climbing, and `read a registry provider: IOKit returned 0x10000003` failed the
        /// whole inventory.
        #[test]
        fn a_provider_chain_finalized_mid_walk_is_departing_not_an_inventory_failure() {
            let entry = finalized_entry();
            assert!(matches!(provider(&entry), Err(Unread::Gone)));
            assert!(matches!(children(&entry), Err(Unread::Gone)));
            assert!(matches!(apfs_volume_media(&entry, None), Err(Unread::Gone)));
            assert!(matches!(entry_id(&entry, "read"), Err(Unread::Gone)));
        }

        /// A finalized entry answers null to every property read; that null must not pass for
        /// an absent property, which would report a departing image as one with no
        /// `DiskImageURL`, or a departing container as arriving.
        #[test]
        fn a_finalized_entry_has_no_properties_rather_than_absent_ones() {
            let entry = finalized_entry();
            let keys = Keys {
                bsd_name: key("BSD Name").expect("key"),
                content: key("Content").expect("key"),
                size: key("Size").expect("key"),
                source: key("KDIURLPath").expect("key"),
                owner: key("owner-uid").expect("key"),
                driver: Driver::InKernel,
            };
            assert!(matches!(
                string_property(&entry, &keys.source, "KDIURLPath"),
                Err(Unread::Gone)
            ));
            assert!(matches!(
                u64_property(&entry, &keys.size, "Size"),
                Err(Unread::Gone)
            ));
            let mut images = BTreeMap::new();
            assert!(matches!(
                record_image_device(&mut images, &entry, &keys),
                Err(Unread::Gone)
            ));
            assert!(images.is_empty());
            assert!(settle(record_image_device(&mut images, &entry, &keys)).is_ok());
        }

        /// The other half of the boundary: a property a still-registered entry does not have is
        /// absent, not a departure.
        #[test]
        fn a_live_entry_keeps_its_absent_properties() {
            let media = registered(c"IOMedia")
                .expect("match registered IOMedia")
                .next()
                .expect("a host registers at least one IOMedia");
            let absent = key("cowshed-absent-property").expect("key");
            assert!(matches!(
                string_property(&media, &absent, "cowshed-absent-property"),
                Ok(None)
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::namespace::TestNamespace;
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::cell::{Ref, RefCell};
    use std::collections::{BTreeMap, VecDeque};
    use std::ffi::OsStr;

    /// A host namespace of the test's own: no test leases a disk tool as production does, or names
    /// a volume with production's `[cowshed]`.
    fn test_namespace() -> HostNamespace {
        HostNamespace::Test(TestNamespace::mint().expect("a test namespace"))
    }

    /// A runner whose disk tools lease as a test namespace of the test's own.
    fn test_runner() -> SystemCommandRunner {
        SystemCommandRunner::new(test_namespace())
    }

    const BLANK_IMAGE_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
      <dict><key>dev-entry</key><string>disk8</string><key>content-hint</key><string>GUID_partition_scheme</string></dict>
    </array></dict></plist>"#;

    /// `diskutil image attach --nobrowse --noMount --plist` for a formatted image, captured live on
    /// macOS 26.6: the image's whole device carries no content hint, and the synthesized container
    /// and its one case-sensitive volume follow it. The volume is mounted; the physical
    /// whole device, not its synthesized container, is detached.
    const ATTACH_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
      <dict><key>content-hint</key><string></string><key>dev-entry</key><string>disk4</string></dict>
      <dict><key>content-hint</key><string>Apple_APFS_Container</string><key>dev-entry</key><string>disk5</string></dict>
      <dict><key>content-hint</key><string>Apple_APFS_Volume</string><key>dev-entry</key><string>disk5s1</string><key>filesystem-name</key><string>Case-sensitive APFS</string><key>filesystem-type</key><string>apfs</string></dict>
    </array></dict></plist>"#;

    const APFS_CONTAINER_CONTENT: &str = "EF57347C-0000-11AA-AA11-00306543ECAC";
    const APFS_VOLUME_CONTENT: &str = "41504653-0000-11AA-AA11-00306543ECAC";

    /// One scripted host answer, in the order the backend asks for it.
    #[derive(Debug)]
    enum Reply {
        Command(CommandOutput),
        Inventory(io::Result<Vec<AttachedDiskImage>>),
        Grow(io::Result<()>),
        /// The device the kernel has at a held mountpoint, and whether it is that mount's root.
        Mount(&'static str, bool),
        /// What naming a proven owned volume root answers.
        Named(io::Result<()>),
        Unmount(Result<UnmountEffect, NativeUnmountError>),
    }

    /// One thing the backend asked the host, in order.
    #[derive(Clone, Debug, PartialEq)]
    enum Seen {
        Command(CommandRequest),
        Inventory,
        Grow(PathBuf, ImageCapacity),
        HoldMount(PathBuf),
        /// A proven owned root named: its volume device and the name.
        NameVolume(String, String),
        UnmountVolume(String, mounts::FilesystemId, UnmountMode),
    }

    impl Seen {
        fn command(&self) -> &CommandRequest {
            match self {
                Self::Command(request) => request,
                Self::Inventory => {
                    panic!("expected a command, saw a kernel inventory read")
                }
                Self::Grow(image, _) => panic!("expected a command, saw {} grow", image.display()),
                Self::HoldMount(path) => {
                    panic!("expected a command, saw {} held", path.display())
                }
                Self::NameVolume(device, name) => {
                    panic!("expected a command, saw {device} named {name:?}")
                }
                Self::UnmountVolume(device, _, mode) => {
                    panic!("expected a command, saw {device} unmounted {mode:?}")
                }
            }
        }
    }

    fn commands(steps: &[Seen]) -> Vec<CommandRequest> {
        steps
            .iter()
            .filter_map(|step| match step {
                Seen::Command(request) => Some(request.clone()),
                Seen::Inventory
                | Seen::Grow(..)
                | Seen::HoldMount(_)
                | Seen::NameVolume(..)
                | Seen::UnmountVolume(..) => None,
            })
            .collect()
    }

    #[derive(Default)]
    struct RecordingRunner {
        steps: RefCell<Vec<Seen>>,
        replies: RefCell<VecDeque<Reply>>,
        // Scripted native effects and namespace queries are distinct from child output.
        volume_mounts: RefCell<BTreeMap<String, bool>>,
        mount_roots: RefCell<BTreeMap<PathBuf, String>>,
        /// The caller Disk Arbitration would see: this process's euid unless a test sets one.
        caller_uid: libc::uid_t,
    }

    impl RecordingRunner {
        fn with_outputs(replies: impl IntoIterator<Item = Reply>) -> Self {
            Self {
                steps: RefCell::new(Vec::new()),
                replies: RefCell::new(replies.into_iter().collect()),
                volume_mounts: RefCell::default(),
                mount_roots: RefCell::default(),
                caller_uid: current_euid(),
            }
        }
        /// Every command and inventory read, in the order the backend made them.
        fn steps(&self) -> Vec<Seen> {
            self.steps.borrow().clone()
        }
        /// The commands alone.
        fn requests(&self) -> Vec<CommandRequest> {
            commands(&self.steps.borrow())
        }
        fn next_reply(&self, step: Seen) -> Reply {
            self.steps.borrow_mut().push(step.clone());
            self.replies
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| panic!("test supplied no reply for {step:?}"))
        }
    }

    impl CommandRunner for RecordingRunner {
        fn caller_uid(&self) -> libc::uid_t {
            self.caller_uid
        }
        fn run_guarded<E>(
            &self,
            request: &crate::CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<crate::CommandOutput>, E>
        where
            E: From<crate::CommandRunError> + std::fmt::Display,
        {
            if check()? {
                self.run(request).map(Some).map_err(E::from)
            } else {
                Ok(None)
            }
        }
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            match self.next_reply(Seen::Command(request.clone())) {
                Reply::Command(output) => {
                    if output.succeeded() {
                        let effect = if request.program == Path::new(MOUNT_APFS) {
                            request.args.get(2).map(|device| (device, true))
                        } else {
                            None
                        };
                        if let Some((device, mounted)) = effect {
                            self.volume_mounts
                                .borrow_mut()
                                .insert(device.to_string_lossy().into_owned(), mounted);
                        }
                    }
                    Ok(output)
                }
                other => panic!("test scripted {other:?} where {request:?} ran"),
            }
        }
        fn volume_mount(
            &self,
            volume_device: &str,
        ) -> io::Result<Option<mounts::KernelMountSnapshot>> {
            if !self
                .volume_mounts
                .borrow()
                .get(volume_device)
                .copied()
                .unwrap_or(true)
            {
                return Ok(None);
            }
            let mount_point = PathBuf::from("/private/tmp/cowshed-recorded-mount")
                .join(volume_device.trim_start_matches('/'));
            self.mount_roots
                .borrow_mut()
                .insert(mount_point.clone(), volume_device.to_owned());
            Ok(Some(mounts::KernelMountSnapshot::new(
                1,
                mount_point,
                volume_device,
                true,
                true,
            )))
        }
        fn unmount_guarded<E>(
            &self,
            mode: UnmountMode,
            prove: impl FnOnce() -> Result<Option<OwnedVolumeRoot>, E>,
        ) -> Result<UnmountEffect, E>
        where
            E: From<NativeUnmountError> + fmt::Display,
        {
            let Some(root) = prove()? else {
                return Ok(UnmountEffect::NoEffect);
            };
            match self.next_reply(Seen::UnmountVolume(
                root.volume_device().to_owned(),
                root.filesystem_id(),
                mode,
            )) {
                Reply::Unmount(result) => {
                    if result.is_ok() {
                        self.volume_mounts
                            .borrow_mut()
                            .insert(root.volume_device().to_owned(), false);
                    }
                    result.map_err(E::from)
                }
                other => panic!("test scripted {other:?} where a native unmount ran"),
            }
        }
        fn image_lease(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
            Ok(None)
        }
        fn pin_raw_device(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
            Ok(None)
        }
        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            match self.next_reply(Seen::Inventory) {
                Reply::Inventory(images) => images,
                other => panic!("test scripted {other:?} where the kernel inventory was read"),
            }
        }
        fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()> {
            match self.next_reply(Seen::Grow(image.to_owned(), capacity)) {
                Reply::Grow(result) => result,
                other => panic!("test scripted {other:?} where {} grew", image.display()),
            }
        }
        fn hold_mount(&self, mount_point: &Path) -> io::Result<mounts::HeldMount> {
            let scripted = matches!(self.replies.borrow().front(), Some(Reply::Mount(..)));
            if !scripted && let Some(device) = self.mount_roots.borrow().get(mount_point) {
                self.steps
                    .borrow_mut()
                    .push(Seen::HoldMount(mount_point.to_owned()));
                return Ok(mounts::HeldMount::stated(
                    device,
                    true,
                    mounts::FilesystemId::new([7, 26]),
                ));
            }
            match self.next_reply(Seen::HoldMount(mount_point.to_owned())) {
                Reply::Mount(device, root) => Ok(mounts::HeldMount::stated(
                    device,
                    root,
                    mounts::FilesystemId::new([7, 26]),
                )),
                other => panic!(
                    "test scripted {other:?} where {} was held",
                    mount_point.display()
                ),
            }
        }
        fn name_volume(&self, root: &OwnedVolumeRoot, volume_name: &str) -> io::Result<()> {
            match self.next_reply(Seen::NameVolume(
                root.volume_device().to_owned(),
                volume_name.to_owned(),
            )) {
                Reply::Named(result) => result,
                other => panic!(
                    "test scripted {other:?} where {} was named",
                    root.volume_device()
                ),
            }
        }
    }

    /// Records the grace instead of spending it, so escalation order is provable in microseconds.
    #[derive(Default)]
    struct RecordingSleeper(RefCell<Vec<Duration>>);

    impl RecordingSleeper {
        fn waits(&self) -> Ref<'_, Vec<Duration>> {
            self.0.borrow()
        }
    }

    impl Sleeper for RecordingSleeper {
        fn sleep(&self, duration: Duration) {
            self.0.borrow_mut().push(duration);
        }

        fn elapsed(&self, _since: std::time::Instant) -> Duration {
            Duration::ZERO
        }
    }

    #[test]
    fn release_grace_counts_wall_time_without_double_counting_requested_polls() {
        let grace = DetachGrace {
            total: Duration::from_millis(20),
            poll: Duration::from_millis(10),
        };
        assert!(!grace.expired(Duration::ZERO, Duration::from_millis(19)));
        assert!(grace.expired(Duration::ZERO, grace.total));
        assert!(!grace.expired(grace.poll, grace.poll));
        assert!(grace.expired(grace.total, Duration::ZERO));
    }

    #[test]
    fn recorded_grace_polls_do_not_advance_from_wall_clock_or_scheduler_delays() {
        let sleeper = RecordingSleeper::default();
        let since = std::time::Instant::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap();
        assert_eq!(sleeper.elapsed(since), Duration::ZERO);
        sleeper.sleep(Duration::from_millis(10));
        assert_eq!(sleeper.elapsed(since), Duration::ZERO);
        assert_eq!(*sleeper.waits(), [Duration::from_millis(10)]);
    }

    /// A backend whose detach graces each admit exactly two retries before giving up. The sleeper only
    /// records, so both bounds are reached in zero wall-clock time.
    fn graced_backend(
        replies: impl IntoIterator<Item = Reply>,
    ) -> MacOsApfsBackend<RecordingRunner, RecordingSleeper> {
        MacOsApfsBackend::with_grace(
            RecordingRunner::with_outputs(replies),
            RecordingSleeper::default(),
            DetachGrace {
                total: Duration::from_millis(20),
                poll: Duration::from_millis(10),
            },
        )
        .with_detach_settle(DetachSettleGrace {
            total: Duration::from_millis(20),
            poll: Duration::from_millis(10),
        })
    }

    fn ok(stdout: impl Into<Vec<u8>>) -> Reply {
        Reply::Command(CommandOutput::success(stdout))
    }

    fn failed(status: i32, stderr: &str) -> Reply {
        Reply::Command(CommandOutput::failure(status, stderr))
    }

    /// The resource-busy refusal observed from a real held-file image detach.
    fn dissent() -> Reply {
        failed(
            libc::EBUSY,
            "hdiutil: couldn't unmount \"disk5\" - Resource busy",
        )
    }

    /// Native busy is distinct from a disk child's diagnostic output.
    fn unmount_dissent() -> Reply {
        Reply::Unmount(Err(NativeUnmountError::Busy(io::Error::from_raw_os_error(
            libc::EBUSY,
        ))))
    }

    fn unmount_ok() -> Reply {
        Reply::Unmount(Ok(UnmountEffect::Unmounted {
            post_fd: PostUnmountFd::Dead(io::Error::from_raw_os_error(libc::ENOTSUP)),
        }))
    }

    fn unmounts(runner: &RecordingRunner) -> Vec<(String, mounts::FilesystemId, UnmountMode)> {
        runner
            .steps()
            .into_iter()
            .filter_map(|step| match step {
                Seen::UnmountVolume(device, fsid, mode) => Some((device, fsid, mode)),
                _ => None,
            })
            .collect()
    }

    fn capacity(value: &str) -> ImageCapacity {
        ImageCapacity::parse(value).expect("test capacities are well formed")
    }

    /// `hdiutil attach …`: every attach cowshed runs.
    fn is_in_kernel_attach(request: &CommandRequest) -> bool {
        request.program == Path::new(HDIUTIL)
            && request.args.first().is_some_and(|verb| verb == "attach")
    }
    fn argv(request: &CommandRequest) -> Vec<String> {
        request
            .args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    /// The content hint the kernel registers for a fixture device: the volume for a first slice,
    /// the synthesized container for `/dev/disk5`, none for an image's own store.
    fn fixture_content(device: &str) -> &'static str {
        if device.ends_with("s1") {
            APFS_VOLUME_CONTENT
        } else if device == "/dev/disk5" {
            APFS_CONTAINER_CONTENT
        } else {
            ""
        }
    }

    /// Kernel inventory records: each image path holding its devices.
    fn attached_images(entries: &[(&str, &[&str])]) -> Vec<AttachedDiskImage> {
        entries
            .iter()
            .map(|(path, devices)| {
                AttachedDiskImage::file(
                    *path,
                    None,
                    devices
                        .iter()
                        .map(|device| (*device, fixture_content(device))),
                )
            })
            .collect()
    }

    fn images(entries: &[(&str, &[&str])]) -> Reply {
        Reply::Inventory(Ok(attached_images(entries)))
    }

    fn no_images() -> Reply {
        Reply::Inventory(Ok(Vec::new()))
    }

    fn unreadable_inventory() -> Reply {
        Reply::Inventory(Err(io::Error::other("registry unavailable")))
    }

    /// The identity read every device detach makes first: the kernel still shows `image`
    /// (resolved the way the backend resolves it) holding `device`.
    fn holding(image: impl AsRef<Path>, device: &str) -> Reply {
        let image = attachment_inventory_path(image.as_ref()).unwrap();
        images(&[(image.to_str().unwrap(), &[device][..])])
    }

    fn holding_verified_volume(image: impl AsRef<Path>) -> Reply {
        let image = attachment_inventory_path(image.as_ref()).unwrap();
        images(&[(
            image.to_str().unwrap(),
            &["/dev/disk4", "/dev/disk5", "/dev/disk5s1"][..],
        )])
    }

    /// The mapping a blank image's attach on `/dev/disk8` shows once `newfs_apfs` put its
    /// container and volume on it: what the verify before the final detach reads.
    fn formatted_on_disk8(image: impl AsRef<Path>) -> Reply {
        let image = attachment_inventory_path(image.as_ref()).unwrap();
        images(&[(
            image.to_str().unwrap(),
            &["/dev/disk8", "/dev/disk5", "/dev/disk5s1"][..],
        )])
    }

    /// A re-adoption record that names exactly one retained input.
    struct NamesOne(PathBuf);

    impl RetainedInputs for NamesOne {
        fn names(&self, path: &Path) -> bool {
            path == self.0
        }
    }

    /// One command-contract walk covers strict recorded input selection, readonly attach and
    /// fsck, then a retained-source cleanup dissent that must never escalate to force. That the
    /// workspace layer's re-adoption record names exactly its retained images is its own test.
    #[test]
    fn retained_inputs_are_record_bound_read_only_and_never_forced() {
        let retained = temp_path("retained-input", IMAGE_EXTENSION);
        fs::create_dir_all(retained.parent().unwrap()).unwrap();
        let record = NamesOne(retained.clone());
        let input = RetainedInput::named_by(&record, &retained).expect("the record names it");
        let unrelated = retained.with_file_name("unrecorded.dmg");
        assert!(RetainedInput::named_by(&record, &unrelated).is_none());

        // A recorded input that is not RAW is refused by its format and path; nothing runs.
        let foreign = retained.with_extension("asif");
        let foreign_record = NamesOne(foreign.clone());
        let foreign_input =
            RetainedInput::named_by(&foreign_record, &foreign).expect("the record names it");
        let refusing = MacOsApfsBackend::new(RecordingRunner::default());
        let refused = refusing
            .attach_retained_verified(&foreign_input)
            .expect_err("a non-RAW retained input is refused");
        assert!(
            matches!(&refused, ApfsError::InvalidImagePath(path) if path == &foreign),
            "{refused:?}"
        );
        assert!(
            refused.to_string().starts_with(&format!(
                "{} is a .asif file, not a RAW .dmg image",
                foreign.display()
            )),
            "{refused}"
        );
        assert!(refusing.runner().requests().is_empty());

        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(ATTACH_PLIST),
            holding_verified_volume(input.path()),
            ok([]),
        ]));
        let attachment = backend.attach_retained_verified(&input).unwrap();
        assert_eq!(attachment.0.image(), input.path());
        assert_eq!(attachment.volume_device(), "/dev/disk5s1");
        assert_eq!(attachment.0.whole_device(), "/dev/disk4");
        let requests = backend.runner().requests();
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
        assert_eq!(
            argv(&requests[0]),
            [
                "attach",
                "-readonly",
                "-kernel",
                "-nomount",
                "-noverify",
                "-nobrowse",
                "-imagekey",
                "diskimage-class=CRawDiskImage",
                "-plist",
                input.path().to_str().unwrap(),
            ]
        );
        assert_eq!(requests[1].program, Path::new(FSCK_APFS));
        drop(attachment);

        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            failed(1, "attach failed after adding the device"),
            holding(input.path(), "/dev/disk4"),
            holding(input.path(), "/dev/disk4"),
            dissent(),
            holding(input.path(), "/dev/disk4"),
        ]));
        assert!(matches!(
            backend.attach_retained_verified(&input),
            Err(ApfsError::AttachmentCleanupFailed { .. })
        ));
        let requests = backend.runner().requests();
        assert_eq!(argv(&requests[1]), ["detach", "-verbose", "/dev/disk4"]);
        assert!(
            requests.iter().all(|request| {
                request
                    .args
                    .iter()
                    .all(|argument| argument != OsStr::new("-force"))
            }),
            "a retained source is never force-detached, including attach-failure cleanup"
        );
    }

    /// Attaches its one new device to `image` on the first `diskutil image attach`, answers that
    /// attach with a malformed plist, and removes the device again when `hdiutil detach` names it.
    struct StatefulMalformedAttachRunner {
        steps: RefCell<Vec<Seen>>,
        attached: RefCell<BTreeMap<PathBuf, BTreeSet<String>>>,
        image: PathBuf,
        new_device: String,
        fail_detach: bool,
    }

    impl StatefulMalformedAttachRunner {
        fn new(image: &Path, preexisting: &[&str], fail_detach: bool) -> Self {
            let image = attachment_inventory_path(image).unwrap();
            let mut attached = BTreeMap::new();
            attached.insert(
                image.clone(),
                preexisting
                    .iter()
                    .map(|device| (*device).to_owned())
                    .collect(),
            );
            attached.insert(
                PathBuf::from("/tmp/cowshed-unrelated.dmg"),
                BTreeSet::from(["/dev/disk20".into()]),
            );
            Self {
                steps: RefCell::new(Vec::new()),
                attached: RefCell::new(attached),
                image,
                new_device: "/dev/disk8".into(),
                fail_detach,
            }
        }

        fn steps(&self) -> Vec<Seen> {
            self.steps.borrow().clone()
        }

        fn devices_for(&self, image: &Path) -> BTreeSet<String> {
            let image = attachment_inventory_path(image).unwrap();
            self.attached
                .borrow()
                .get(&image)
                .cloned()
                .unwrap_or_default()
        }
    }

    impl CommandRunner for StatefulMalformedAttachRunner {
        fn run_guarded<E>(
            &self,
            request: &crate::CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<crate::CommandOutput>, E>
        where
            E: From<crate::CommandRunError> + std::fmt::Display,
        {
            if check()? {
                self.run(request).map(Some).map_err(E::from)
            } else {
                Ok(None)
            }
        }
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            self.steps.borrow_mut().push(Seen::Command(request.clone()));
            let args = argv(request);
            if request.program == Path::new(HDIUTIL)
                && args.first().is_some_and(|arg| arg == "attach")
            {
                self.attached
                    .borrow_mut()
                    .entry(self.image.clone())
                    .or_default()
                    .insert(self.new_device.clone());
                return Ok(CommandOutput::success("<plist><dict><key>malformed"));
            }
            assert!(
                request.program == Path::new(HDIUTIL)
                    && args == ["detach", "-verbose", self.new_device.as_str()],
                "unexpected command: {request:?}"
            );
            if self.fail_detach {
                return Ok(CommandOutput::failure(16, "busy"));
            }
            self.attached
                .borrow_mut()
                .get_mut(&self.image)
                .expect("target image is inventoried")
                .remove(&self.new_device);
            Ok(CommandOutput::success([]))
        }
        fn image_lease(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
            Ok(None)
        }
        fn pin_raw_device(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
            Ok(None)
        }
        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            self.steps.borrow_mut().push(Seen::Inventory);
            Ok(self
                .attached
                .borrow()
                .iter()
                .filter(|(_, devices)| !devices.is_empty())
                .map(|(path, devices)| {
                    AttachedDiskImage::file(
                        path.clone(),
                        None,
                        devices.iter().map(|device| (device.as_str(), "")),
                    )
                })
                .collect())
        }
        fn grow_image(&self, image: &Path, _: ImageCapacity) -> io::Result<()> {
            panic!("creation never grows an image: {}", image.display());
        }
    }

    fn temp_path(label: &str, extension: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cowshed-apfs-{label}-{}-{:?}.{extension}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    #[test]
    fn system_runner_reports_output_spawn_errors_and_signal_status() {
        let output = test_runner()
            .run(&CommandRequest::new(
                "/bin/sh",
                ["-c", "printf stdout; printf stderr >&2; exit 7"],
            ))
            .unwrap();
        assert_eq!(output.status, ProcessStatus::Exit(7));
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");

        let signaled = test_runner()
            .run(&CommandRequest::new("/bin/sh", ["-c", "kill -TERM $$"]))
            .unwrap();
        assert_eq!(signaled.status, ProcessStatus::Signal(libc::SIGTERM));

        let missing = temp_path("missing-command", "bin");
        let error = test_runner()
            .run(&CommandRequest::new(
                &missing,
                std::iter::empty::<OsString>(),
            ))
            .unwrap_err();
        assert_eq!(error.request.program, missing);
        assert!(error.to_string().contains("could not run"));
        assert!(std::error::Error::source(&error).is_some());
    }
    #[test]
    fn system_runner_hung_disk_child_is_killed_at_the_deadline() {
        // Hang injection: a shell-builtin spin stands in for a hung disk child that never
        // answers. Absolute coreutils paths (`/bin/sleep`, `/bin/echo`) do not exist on all
        // Linux runners (NixOS provides only `/bin/sh`), so the hang and the follow-up probe
        // must use `/bin/sh` builtins only. A child that is no disk-image command is killed at
        // the deadline, and the queue continues; a disk-image command is never killed (the tests
        // below), only warned about. The child's identity comes from the runner that spawned and
        // reaped it, never from the child: a loaded host can kill the shell at 250 ms before it
        // runs a single builtin, and a pid the child had to write itself was then empty. The spin
        // never ends on its own, so the `Deadline` failure with its SIGKILL is the proof the
        // deadline fired; a runner that never fired it would hang to the run's no-progress bound.
        let error = test_runner()
            .run_with_deadline(
                &CommandRequest::new("/bin/sh", ["-c", "while true; do :; done"]),
                Duration::from_millis(250),
            )
            .unwrap_err();
        let CommandRunFailure::Deadline {
            deadline,
            pid,
            ended: ProcessStatus::Signal(libc::SIGKILL),
        } = error.failure
        else {
            panic!("{error:?}");
        };
        assert_eq!(deadline, Duration::from_millis(250));
        // The child ran; the report must say it hung, never that it could not be started.
        let report = error.to_string();
        assert!(!report.contains("could not run"), "{report}");
        assert!(report.contains("did not finish within 250ms"), "{report}");
        assert!(report.contains("deferred"), "{report}");
        assert!(
            report.contains(&format!("process group {pid} was signalled")),
            "{report}"
        );
        assert!(report.contains("reaped with signal"), "{report}");
        let pid = libc::pid_t::try_from(pid).expect("a process id fits pid_t");
        let reaped = cowshed_os::test_backstop::waitpid(pid, libc::WNOHANG)
            .expect_err("the runner actually reaped its child");
        assert_eq!(
            reaped.raw_os_error(),
            Some(libc::ECHILD),
            "the runner actually reaped its child"
        );
        // SAFETY: signal zero observes this exact reaped PID; it sends no signal.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "no owned child remains alive"
        );
        // The queue continues: a fast child right after the kill still runs.
        let output = test_runner()
            .run(&CommandRequest::new("/bin/sh", ["-c", "printf 'next\\n'"]))
            .unwrap();
        assert_eq!(output.stdout, b"next\n");
    }

    fn disk_image_request() -> CommandRequest {
        CommandRequest::new(DISKUTIL, ["image", "attach", "/private/tmp/x.dmg"])
    }

    /// The outlived-deadline warning names the verb, not just its noun: `diskutil image attach`.
    /// A detached child leads its own session, ignores the terminate and hangup a
    /// caller's tree-killer sends it, and its output waits in files, not the caller's
    /// pipes: a caller killed mid-attach leaves it running to completion.
    #[cfg(unix)]
    #[test]
    fn a_detached_disk_child_ignores_caller_signals_and_writes_files() {
        let request = CommandRequest::new(
            "/bin/sh",
            [
                "-c",
                "kill -TERM $$; kill -HUP $$; printf done; printf err >&2",
            ],
        );
        let (mut child, files) =
            spawn_disk_child_as(&request, ChildInput::Null, true, None).expect("spawn detached");
        let directory = files.expect("a detached child writes files");
        let status = child.wait_bounded().expect("reap the detached child");
        assert!(status.success(), "{status:?}");
        assert_eq!(fs::read(directory.join("stdout")).unwrap(), b"done");
        assert_eq!(fs::read(directory.join("stderr")).unwrap(), b"err");
        fs::remove_dir_all(directory).unwrap();

        let request = CommandRequest::new("/bin/sh", ["-c", "sleep 5"]);
        let (mut child, files) =
            spawn_disk_child_as(&request, ChildInput::Null, true, None).expect("spawn sleeper");
        let directory = files.expect("the owned protected fixture writes files");
        let pid = child.id() as libc::pid_t;
        fs::remove_dir_all(directory).expect("unlink the exact fixture's open output files");
        // SAFETY: getpgid only reads the child's group; the child is ours, unreaped.
        assert_eq!(
            unsafe { libc::getpgid(pid) },
            pid,
            "the child leads its session"
        );
        kill_disk_child_group(&mut child).expect("kill exact owned fixture group");
        let status = child.wait_bounded().expect("reap exact owned fixture");
        assert_eq!(
            ProcessStatus::from(status),
            ProcessStatus::Signal(libc::SIGKILL)
        );
    }

    /// A disk tool's own handlers do not reopen it to the caller's tree-killer: `diskutil`
    /// installs a terminate handler over the inherited SIG_IGN and abandons an attach on it, so
    /// the stand-in below does the same, then is sent the terminate. Detached, it survives.
    #[cfg(unix)]
    #[test]
    fn a_detached_disk_child_holds_caller_signals_through_its_own_handlers() {
        let request = CommandRequest::new(
            std::env::current_exe().expect("test binary"),
            [
                "--exact",
                "tests::a_disk_tool_installing_its_own_terminate_handler",
                "--ignored",
                "--nocapture",
            ],
        );
        let (mut child, files) =
            spawn_disk_child_as(&request, ChildInput::Null, true, None).expect("spawn detached");
        let directory = files.expect("a detached child writes files");
        let status = child.wait_bounded().expect("reap the detached child");
        let stdout = fs::read_to_string(directory.join("stdout")).unwrap();
        fs::remove_dir_all(directory).unwrap();
        assert!(
            status.success() && stdout.contains("survived its terminate"),
            "{status:?}: {stdout}"
        );
    }

    /// The disk tool [`a_detached_disk_child_holds_caller_signals_through_its_own_handlers`]
    /// runs detached: like `diskutil`, it installs its own terminate handler, which exits 3.
    #[cfg(unix)]
    #[test]
    #[ignore = "spawned by a_detached_disk_child_holds_caller_signals_through_its_own_handlers"]
    fn a_disk_tool_installing_its_own_terminate_handler() {
        extern "C" fn interrupted(_: libc::c_int) {
            // SAFETY: `_exit` is async-signal-safe and ends the process at once.
            unsafe { libc::_exit(3) };
        }
        // SAFETY: getsid and getpid only read this process's own ids.
        if unsafe { libc::getsid(0) != libc::getpid() } {
            return; // Only a detached disk child leads its own session.
        }
        // SAFETY: the handler only calls `_exit`; `raise` directs the signal at this thread,
        // so an unblocked terminate runs the handler before `raise` returns.
        unsafe {
            libc::signal(
                libc::SIGTERM,
                interrupted as *const () as libc::sighandler_t,
            );
            libc::raise(libc::SIGTERM);
        }
        println!("survived its terminate");
    }

    #[test]
    fn a_disk_image_command_is_named_by_its_program_and_verb() {
        assert_eq!(
            DiskImageVerb(&disk_image_request()).to_string(),
            "/usr/sbin/diskutil image attach"
        );
        assert_eq!(
            DiskImageVerb(&CommandRequest::new(HDIUTIL, ["detach", "/dev/disk9"])).to_string(),
            "/usr/bin/hdiutil detach"
        );
        assert_eq!(
            DiskImageVerb(&CommandRequest::new(DISKUTIL, ["eject", "disk9"])).to_string(),
            "/usr/sbin/diskutil eject"
        );
    }

    /// The policy itself, at its real constants: a disk-image command past the 120 s deadline is
    /// warned about every 30 s and never killed, however long it runs; every other disk child is
    /// killed at the deadline.
    #[test]
    fn a_disk_image_command_is_warned_about_at_the_deadline_and_never_killed() {
        let at = Duration::from_secs;
        for image in [
            disk_image_request(),
            CommandRequest::new(DISKUTIL, ["image", "info", "/private/tmp/x.dmg"]),
            CommandRequest::new(HDIUTIL, ["detach", "/dev/disk9"]),
            CommandRequest::new(HDIUTIL, ["info"]),
            // A Disk Arbitration release of an image device, and of a physical disk alike.
            CommandRequest::new(DISKUTIL, ["eject", "disk9"]),
            CommandRequest::new(DISKUTIL, ["eject", "force", "/dev/disk9"]),
        ] {
            let policy = DeadlinePolicy::for_request(&image, DISK_CHILD_DEADLINE);
            assert_eq!(
                policy.past_deadline,
                PastDeadline::WarnEvery(DISK_IMAGE_WARNING_INTERVAL),
                "{image:?}"
            );
            assert_eq!(policy.step(at(119), 0), DeadlineStep::Wait);
            assert_eq!(policy.step(at(120), 0), DeadlineStep::Warn);
            assert_eq!(policy.step(at(121), 1), DeadlineStep::Wait);
            assert_eq!(policy.step(at(150), 1), DeadlineStep::Warn);
            // Fifteen minutes, a day, a week: each is one more warning, never a kill.
            for (elapsed, warned) in [(900, 26), (86_400, 2_876), (7 * 86_400, 20_156)] {
                assert_eq!(
                    policy.step(at(elapsed), warned),
                    DeadlineStep::Warn,
                    "{image:?} at {elapsed}s"
                );
                assert_eq!(
                    policy.step(at(elapsed), warned + 1),
                    DeadlineStep::Wait,
                    "{image:?} at {elapsed}s"
                );
            }
        }
        for ordinary in [
            CommandRequest::new("/sbin/mount_apfs", ["-o", "nobrowse"]),
            CommandRequest::new(DISKUTIL, ["apfs", "resizeContainer", "disk9s1", "0"]),
            CommandRequest::new(DISKUTIL, ["info", "-plist", "disk9s1"]),
        ] {
            let policy = DeadlinePolicy::for_request(&ordinary, DISK_CHILD_DEADLINE);
            assert_eq!(policy.past_deadline, PastDeadline::Kill, "{ordinary:?}");
            assert_eq!(policy.step(at(119), 0), DeadlineStep::Wait, "{ordinary:?}");
            assert_eq!(policy.step(at(120), 0), DeadlineStep::Kill, "{ordinary:?}");
        }
    }

    /// A slow disk-image command is never signalled: the waiter warns past its deadline again
    /// and again, and the child finishes on its own, exit status and output intact, returned as
    /// its own outcome rather than a deadline failure.
    #[test]
    fn a_slow_disk_image_command_outliving_its_deadline_is_waited_for_never_killed() {
        use cowshed_os::fork_lock::Fenced;
        use std::io::{Read, Write};
        use std::os::unix::fs::OpenOptionsExt;

        struct FifoDirectory(PathBuf);
        impl Drop for FifoDirectory {
            fn drop(&mut self) {
                if let Err(error) = fs::remove_dir_all(&self.0) {
                    cowshed_os::eprintln!(
                        "remove owned disk-child FIFOs {}: {error}",
                        self.0.display()
                    );
                }
            }
        }
        struct ReleaseOnDrop<'a>(Option<&'a File>);
        impl Drop for ReleaseOnDrop<'_> {
            fn drop(&mut self) {
                if let Some(mut pipe) = self.0.take()
                    && let Err(error) = pipe.write_all(b"cancel\n")
                {
                    cowshed_os::eprintln!("release owned disk child during cleanup: {error}");
                }
            }
        }

        let directory = temp_path("disk-child-warnings", "fifo");
        fs::create_dir(&directory).expect("create owned disk-child FIFO directory");
        let directory = FifoDirectory(directory);
        let ready_path = directory.0.join("ready");
        let release_path = directory.0.join("release");
        for path in [&ready_path, &release_path] {
            let name =
                std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("FIFO path");
            // SAFETY: the NUL-terminated path is in this test's owned directory.
            assert_eq!(
                unsafe { libc::mkfifo(name.as_ptr(), 0o600) },
                0,
                "create {}: {}",
                path.display(),
                io::Error::last_os_error()
            );
        }
        let reader = |path: &Path| {
            Fenced::new(
                fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(path)
                    .expect("open owned FIFO reader"),
            )
        };
        let ready = reader(&ready_path);
        let _release_reader = reader(&release_path);
        let release = Fenced::new(
            fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&release_path)
                .expect("open owned child release writer"),
        );

        let deadline = Duration::from_millis(10);
        assert!(matches!(
            DeadlinePolicy::for_request(&disk_image_request(), deadline).past_deadline,
            PastDeadline::WarnEvery(_)
        ));
        let policy = DeadlinePolicy {
            deadline,
            past_deadline: PastDeadline::WarnEvery(Duration::from_millis(50)),
            reap: cowshed_os::deadline::REAP,
        };
        let request = CommandRequest::new(
            "/bin/sh",
            [
                OsStr::new("-c"),
                OsStr::new(
                    "exec 3<\"$2\" || exit 4; printf r >\"$1\" || exit 4; \
                     IFS= read -r release <&3 || exit 4; \
                     [ \"$release\" = release ] || exit 4; printf done; exit 3",
                ),
                OsStr::new("disk-image-child"),
                ready_path.as_os_str(),
                release_path.as_os_str(),
            ],
        );
        let (result, warnings, readiness) = std::thread::scope(|scope| {
            // This guard releases even on unwind, before the scope joins the waiter. The writer
            // stays open outside the scope so a child still starting can open and read its FIFO.
            let mut cleanup = ReleaseOnDrop(Some(&release));
            let (warned, observed) = std::sync::mpsc::channel();
            let running = scope.spawn(move || {
                let mut warnings = 0;
                let result = SystemCommandRunner::run_unleased(
                    &request,
                    ChildInput::Null,
                    policy,
                    None,
                    &mut |_| {
                        warnings += 1;
                        // Disconnect means the observer is done or unwinding; never abandon the child.
                        let _ = warned.send(());
                    },
                );
                (result, warnings)
            });
            let mut observations = 0;
            let readiness = loop {
                if observed.recv().is_err() {
                    break Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "disk-child waiter ended before five warnings and child readiness",
                    ));
                }
                observations += 1;
                if observations < 5 {
                    continue;
                }
                let mut token = [0];
                match (&*ready).read(&mut token) {
                    Ok(1) if token == *b"r" => break Ok(()),
                    Ok(0) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Ok(_) => {
                        break Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unexpected disk-child readiness token {token:?}"),
                        ));
                    }
                    Err(error) => break Err(error),
                }
            };
            let released = if readiness.is_ok() {
                let released = (&*release).write_all(b"release\n");
                if released.is_ok() {
                    cleanup.0 = None;
                }
                released
            } else {
                Ok(())
            };
            drop(cleanup);
            let (result, warnings) = running.join().expect("join the owned disk-child waiter");
            (result, warnings, readiness.and(released))
        });
        let output =
            result.expect("a slow disk-image command ends on its own, never at a deadline");
        readiness.expect("five actual warnings and child readiness precede release");
        assert_eq!(output.status, ProcessStatus::Exit(3));
        assert_eq!(output.stdout, b"done");
        assert!(
            warnings >= 5,
            "only {warnings} warnings before child release"
        );
    }

    #[test]
    fn a_safe_child_cannot_exec_after_the_fork_lock_outlives_its_budget() {
        let marker = temp_path("fork-lock-late-exec", "txt");
        let request = CommandRequest::new(
            "/bin/sh",
            [
                OsString::from("-c"),
                OsString::from("printf executed > \"$1\""),
                OsString::from("owned-fork-lock-child"),
                marker.clone().into_os_string(),
            ],
        );
        let (locked, acquired) = std::sync::mpsc::sync_channel(0);
        let (give_budget, budget) = std::sync::mpsc::sync_channel::<CommandDeadline>(0);
        let owner = std::thread::spawn(move || {
            cowshed_os::fork_lock::create(|| {
                assert!(cowshed_os::fork_lock::held_exclusive());
                locked
                    .send(())
                    .expect("announce the real exclusive fork lock");
                let deadline = budget.recv().expect("the fixture's monotonic budget");
                while let Some(remaining) = deadline.remaining() {
                    std::thread::park_timeout(remaining);
                }
                Ok(())
            })
            .expect("release the owned fork-lock fixture");
        });
        acquired.recv().expect("the real fork lock is held");
        let deadline = CommandDeadline::new(Duration::from_millis(50));
        give_budget
            .send(deadline)
            .expect("hold the fork lock through budget exhaustion");
        // Exercise the production spawn seam directly: an outer admission check cannot make
        // this oracle pass. std reaps a pre_exec-failed fork stub without executing the tool.
        let outcome = spawn_disk_child(&request, ChildInput::Null, Some(deadline));
        owner.join().expect("the exact fork-lock owner finished");
        let (failure, unexpected_status) = match outcome {
            Err(failure) => (Some(failure), None),
            Ok((mut child, files)) => {
                let status = child
                    .wait_bounded()
                    .expect("reap any unexpectedly executed owned fixture");
                if let Some(directory) = files {
                    fs::remove_dir_all(directory).expect("remove only the owned output files");
                }
                (None, Some(ProcessStatus::from(status)))
            }
        };
        let executed = marker.exists();
        if executed {
            fs::remove_file(&marker).expect("remove the unexpected owned marker before asserting");
        }
        assert!(
            matches!(failure, Some(CommandRunFailure::Unspawned(budget)) if budget == deadline.duration),
            "failure={failure:?}; unexpected status={unexpected_status:?}"
        );
        assert!(
            !executed,
            "the native tool must not exec after the fork lock spends its budget"
        );
    }

    #[test]
    fn exhausted_custom_admission_never_spawns_a_late_safe_child() {
        let marker = temp_path("late-safe-spawn", "txt");
        let request = CommandRequest::new(
            "/bin/sh",
            [
                OsString::from("-c"),
                OsString::from("printf spawned > \"$1\""),
                OsString::from("late-safe-child"),
                marker.clone().into_os_string(),
            ],
        );
        let error = test_runner()
            .run_within(&request, Duration::from_millis(25), |deadline, run| {
                let deadline = deadline.expect("SAFE custom admission carries its total budget");
                while let Some(remaining) = deadline.remaining() {
                    std::thread::park_timeout(remaining);
                }
                run()
            })
            .expect_err("an expired admitted command must not spawn");
        assert!(
            matches!(error.failure, CommandRunFailure::Unspawned(deadline) if deadline == Duration::from_millis(25)),
            "{error:?}"
        );
        assert!(
            !marker.exists(),
            "the real child would have written this marker"
        );
    }

    #[test]
    fn a_protected_stand_in_is_alive_at_its_warning_then_finishes_on_fixture_release() {
        let pid_file = temp_path("protected-child-pid", "txt");
        let release = temp_path("protected-child-release", "txt");
        let request = CommandRequest::new(
            "/bin/sh",
            [
                OsString::from("-c"),
                OsString::from(
                    "printf '%s' $$ > \"$1\"; while ! test -f \"$2\"; do :; done; printf done; exit 3",
                ),
                OsString::from("owned-protected-child"),
                pid_file.clone().into_os_string(),
                release.clone().into_os_string(),
            ],
        );
        let policy = DeadlinePolicy {
            deadline: Duration::from_millis(10),
            past_deadline: PastDeadline::WarnEvery(Duration::from_millis(10)),
            reap: cowshed_os::deadline::REAP,
        };
        assert!(matches!(
            DeadlinePolicy::for_request(&disk_image_request(), policy.deadline).past_deadline,
            PastDeadline::WarnEvery(_)
        ));
        let mut warnings = 0;
        let output = SystemCommandRunner::run_unleased(
            &request,
            ChildInput::Null,
            policy,
            None,
            &mut |elapsed| {
                assert!(elapsed >= policy.deadline);
                let pid = loop {
                    match fs::read_to_string(&pid_file) {
                        Ok(text) if !text.is_empty() => {
                            break text.parse::<libc::pid_t>().expect("fixture pid");
                        }
                        Ok(_) => std::thread::yield_now(),
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {
                            std::thread::yield_now()
                        }
                        Err(error) => panic!("read exact fixture pid: {error}"),
                    }
                };
                // SAFETY: observe only our live stand-in; signal zero does not cancel it.
                assert_eq!(
                    unsafe { libc::kill(pid, 0) },
                    0,
                    "the protected stand-in survives its warning point"
                );
                warnings += 1;
                fs::write(&release, b"release").expect("release only this owned fixture");
            },
        )
        .expect("protected fixture ends on its own, not as a timeout");
        assert!(warnings > 0, "the real waiter reported the overrun");
        assert_eq!(output.status, ProcessStatus::Exit(3));
        assert_eq!(output.stdout, b"done");
        fs::remove_file(pid_file).expect("remove owned pid receipt");
        fs::remove_file(release).expect("remove owned release marker");
    }

    /// A disk child that exits while a process it started keeps its stdout open is not waited for
    /// past the reap bound: the run answers the child's exit and names the held pipe, where it
    /// used to park its runner until that process ended.
    #[test]
    fn a_disk_child_whose_own_child_holds_its_output_fails_naming_the_held_pipe() {
        let request = CommandRequest::new("/bin/sh", ["-c", "sleep 600 & printf started; exit 0"]);
        let policy = DeadlinePolicy {
            deadline: DISK_CHILD_DEADLINE,
            past_deadline: PastDeadline::Kill,
            reap: cowshed_os::deadline::Bound::new(
                cowshed_os::deadline::LatencyClass::Reap,
                Duration::from_millis(50),
            ),
        };
        let error = SystemCommandRunner::run_unleased(
            &request,
            ChildInput::Null,
            policy,
            None,
            &mut |_| panic!("no deadline warning for a child that exits at once"),
        )
        .expect_err("sleep keeps the pipes open");
        let CommandRunFailure::Output { ended, failure } = &error.failure else {
            panic!("expected the held pipe, got {error}");
        };
        let cowshed_os::deadline::OutputError::Held { stream, expired } = failure.as_ref() else {
            panic!("expected the held pipe, got {error}");
        };
        // The disk child led its own group (setsid), which its sleep inherited.
        let group = libc::pid_t::try_from(expired.peer.pid).expect("a pid");
        // SAFETY: a negative pid addresses only the group this test's disk child led.
        assert_eq!(unsafe { libc::kill(-group, libc::SIGKILL) }, 0);
        assert_eq!(*ended, ProcessStatus::Exit(0));
        assert_eq!(*stream, cowshed_os::deadline::OutputStream::Stdout);
        assert_eq!(expired.op, "collect disk child output");
        assert_eq!(expired.peer.role, "disk child");
        assert!(expired.elapsed >= policy.reap.within, "{error}");
        let message = error.to_string();
        assert!(
            message.contains("its output was not collected"),
            "{message}"
        );
        assert!(message.contains("stdout is still open"), "{message}");
    }

    const DISK_CHILD_INPUT_ROLE: &str = "COWSHED_TEST_DISK_CHILD_INPUT";

    /// A disk child reads `/dev/null` unless it is run with this process's standard input
    /// ([`SystemCommandRunner::run_with_inherited_input`]), which `cowshed disk-command` asks for
    /// so a caller can pipe a passphrase to `diskutil apfs unlockVolume … -stdinpassphrase`. The
    /// test re-executes itself with a passphrase on its standard input and reports what each
    /// kind of run read.
    #[test]
    fn a_disk_child_reads_dev_null_unless_run_with_inherited_input() {
        let read = CommandRequest::new(
            "/bin/sh",
            [
                "-c",
                "if read line; then printf 'got %s' \"$line\"; else printf eof; fi",
            ],
        );
        if std::env::var_os(DISK_CHILD_INPUT_ROLE).is_some() {
            let null = test_runner().run(&read).expect("null-input child");
            let inherited = test_runner()
                .run_with_inherited_input(&read, CommandDeadline::new(DISK_CHILD_DEADLINE), None)
                .expect("inheriting child");
            println!(
                "null={} inherited={}",
                String::from_utf8_lossy(&null.stdout),
                String::from_utf8_lossy(&inherited.stdout)
            );
            return;
        }
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "tests::a_disk_child_reads_dev_null_unless_run_with_inherited_input",
                "--nocapture",
            ])
            .env(DISK_CHILD_INPUT_ROLE, "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn_locked()
            .expect("re-execute the test");
        {
            use std::io::Write;
            let mut stdin = child.stdin.take().expect("piped stdin");
            stdin
                .write_all(b"passphrase\n")
                .expect("write the passphrase");
        }
        let output = child
            .wait_with_output_bounded()
            .expect("reap the re-executed test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "{output:?}");
        assert!(
            stdout.contains("null=eof inherited=got passphrase"),
            "{stdout}"
        );
    }

    /// A disk child run with given bytes reads exactly them, then end of input; none at all is end
    /// of input at once. Bytes larger than any pipe buffer reach a child that echoes them while its
    /// output drains, and a child that never reads them is no failure: it chose not to.
    #[test]
    fn a_disk_child_reads_exactly_the_given_bytes_then_end_of_input() {
        let deadline = || CommandDeadline::new(DISK_CHILD_DEADLINE);
        let read = CommandRequest::new(
            "sh",
            [
                "-c",
                "if read line; then printf 'got %s' \"$line\"; else printf eof; fi; cat",
            ],
        );
        let given = test_runner()
            .run_with_input(&read, deadline(), b"passphrase\nrest".to_vec(), None)
            .expect("child given bytes");
        assert_eq!(given.stdout, b"got passphraserest");
        let empty = test_runner()
            .run_with_input(&read, deadline(), Vec::new(), None)
            .expect("child given nothing");
        assert_eq!(empty.stdout, b"eof");

        let large: Vec<u8> = (0..(1 << 20))
            .map(|index: u32| index.to_le_bytes()[0])
            .collect();
        let echoed = test_runner()
            .run_with_input(
                &CommandRequest::new("cat", Vec::<OsString>::new()),
                deadline(),
                large.clone(),
                None,
            )
            .expect("cat given a megabyte");
        assert_eq!(echoed.status, ProcessStatus::Exit(0));
        assert!(
            echoed.stdout == large,
            "cat echoed {} bytes",
            echoed.stdout.len()
        );

        let ignored = test_runner()
            .run_with_input(
                &CommandRequest::new("true", Vec::<OsString>::new()),
                deadline(),
                large,
                None,
            )
            .expect("a child that never reads its input");
        assert_eq!(ignored.status, ProcessStatus::Exit(0));
    }

    /// The witness's descendant and its fixture. From construction it holds both ends of the
    /// `ended` fifo, so the descendant's open of it never waits for a reader and the test's read
    /// waits for the descendant's line, not for a writer. Dropped on any outcome, it releases a
    /// descendant still blocked on `release` and removes the fixture; a cleanup step that fails
    /// for any reason but the expected ones (no reader left, nothing left to remove) is said.
    struct Descendant {
        root: PathBuf,
        release: PathBuf,
        ended: PathBuf,
        acknowledged: io::BufReader<File>,
        _writer: File,
    }

    impl Descendant {
        fn new(root: PathBuf) -> Self {
            use std::os::unix::fs::OpenOptionsExt as _;
            fs::create_dir_all(&root).unwrap();
            let fifo = |name: &str| {
                let fifo = root.join(name);
                let path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
                // SAFETY: a NUL-terminated path this frame owns.
                assert_eq!(
                    unsafe { libc::mkfifo(path.as_ptr(), 0o600) },
                    0,
                    "{}",
                    io::Error::last_os_error()
                );
                fifo
            };
            let release = fifo("release");
            let ended = fifo("ended");
            // Opened for reading without waiting for a writer, then made blocking; with this
            // writer held too, a read waits for a line and never meets end of file.
            let reader = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&ended)
                .unwrap();
            let writer = OpenOptions::new().write(true).open(&ended).unwrap();
            // SAFETY: `fcntl` reads and then sets the status flags of a descriptor this frame owns.
            let flags = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_GETFL) };
            // SAFETY: as above.
            let cleared = unsafe {
                libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, flags & !libc::O_NONBLOCK)
            };
            assert!(flags >= 0 && cleared == 0, "{}", io::Error::last_os_error());
            Self {
                root,
                release,
                ended,
                acknowledged: io::BufReader::new(reader),
                _writer: writer,
            }
        }

        /// The line the descendant writes as its last act.
        fn acknowledgment(&mut self) -> String {
            use std::io::BufRead as _;
            let mut line = String::new();
            self.acknowledged
                .read_line(&mut line)
                .expect("the descendant says it ends");
            line
        }
    }

    impl Drop for Descendant {
        fn drop(&mut self) {
            use std::io::Write as _;
            use std::os::unix::fs::OpenOptionsExt as _;
            // A reader still waiting on the fifo takes this at once; with none, the open is
            // refused with ENXIO and there is nobody left to release.
            match OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&self.release)
            {
                Ok(mut release) => {
                    if let Err(error) = release.write_all(b"go\n") {
                        cowshed_os::eprintln!("cannot release the witness's descendant: {error}");
                    }
                }
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => {}
                Err(error) => {
                    cowshed_os::eprintln!("cannot open the witness's release fifo: {error}");
                }
            }
            match fs::remove_dir_all(&self.root) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    cowshed_os::eprintln!("cannot remove {}: {error}", self.root.display());
                }
            }
        }
    }

    /// A child that exits while a descendant keeps its input pipe and never reads ends the run:
    /// the run owns the feed, so a megabyte nobody reads wedges nothing. The descendant, still
    /// holding the pipe when the run returns, is released through a fifo and its exit observed
    /// through another, never a clock.
    #[test]
    fn a_descendant_that_keeps_the_input_and_never_reads_holds_up_no_run() {
        let mut descendant = Descendant::new(temp_path("retained-stdin", "fifo"));
        let request = CommandRequest::new(
            "/bin/sh",
            [
                OsString::from("-c"),
                // An asynchronous list's stdin is /dev/null unless redirected, so the descendant
                // takes the pipe from a descriptor saved before it starts.
                OsString::from(
                    "exec 3<&0; sh -c 'read line < \"$1\"; echo ended > \"$2\"' sh \"$1\" \"$2\" <&3 >/dev/null 2>&1 & echo $!",
                ),
                OsString::from("sh"),
                descendant.release.clone().into_os_string(),
                descendant.ended.clone().into_os_string(),
            ],
        );
        let large = vec![b'x'; 1 << 20];
        let ran = test_runner()
            .run_with_input(
                &request,
                CommandDeadline::new(DISK_CHILD_DEADLINE),
                large,
                None,
            )
            .expect("the child ends the run");
        assert_eq!(ran.status, ProcessStatus::Exit(0));
        let pid: libc::pid_t = String::from_utf8(ran.stdout)
            .unwrap()
            .trim()
            .parse()
            .expect("the descendant's pid");
        // SAFETY: signal zero only asks whether the descendant still runs.
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            0,
            "the descendant still holds the pipe"
        );
        {
            use std::io::Write as _;
            OpenOptions::new()
                .write(true)
                .open(&descendant.release)
                .and_then(|mut release| release.write_all(b"go\n"))
                .expect("release the descendant");
        }
        // The descendant's last act before it exits.
        assert_eq!(descendant.acknowledgment(), "ended\n");
    }

    #[test]
    fn system_runner_verbose_child_output_is_drained_under_deadline() {
        // Pipes are pumped while the deadline poll runs: a child whose stdout exceeds the
        // pipe buffer must complete with its output intact, never wedge on a full pipe. The
        // deadline is not the subject, so it sits at the run's no-progress backstop: a wedged
        // pipe still ends in the deadline error, and a loaded host never does.
        let output = test_runner()
            .run_with_deadline(
                &CommandRequest::new("/bin/sh", ["-c", "seq 1 20000"]),
                cowshed_os::test_backstop::NO_PROGRESS,
            )
            .unwrap();
        assert!(output.succeeded());
        assert_eq!(
            output.stdout.iter().filter(|byte| **byte == b'\n').count(),
            20_000
        );
    }

    #[test]
    fn rejects_a_path_that_is_not_an_image_before_spawning() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for image in ["session.sparseimage", "session.img", "session"] {
            let error = backend.attach_verified(Path::new(image)).unwrap_err();
            assert!(matches!(error, ApfsError::InvalidImagePath(path) if path == Path::new(image)));
        }
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn failed_verification_detaches_the_whole_image_device() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(ATTACH_PLIST),
            holding_verified_volume("session.dmg"),
            failed(8, "not clean"),
            holding("session.dmg", "/dev/disk4"),
            ok([]),
            no_images(),
        ]));
        let error = backend
            .attach_verified(Path::new("session.dmg"))
            .unwrap_err();
        assert!(matches!(
            error,
            ApfsError::VerificationFailed {
                request,
                output: CommandOutput {
                    status: ProcessStatus::Exit(8),
                    ..
                },
            } if request.program == Path::new(FSCK_APFS)
                && argv(&request) == ["-q", "/dev/rdisk5s1"]
        ));
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 7);
        assert_eq!(steps[3].command().program, Path::new(FSCK_APFS));
        assert_eq!(argv(steps[3].command()), ["-q", "/dev/rdisk5s1"]);
        assert_eq!(steps[4], Seen::Inventory);
        assert_eq!(steps[5].command().program, Path::new(HDIUTIL));
        assert_eq!(
            argv(steps[5].command()),
            ["detach", "-verbose", "/dev/disk4"]
        );
        assert!(
            !commands(&steps)
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "mount"))
        );
    }

    /// A path the kernel already holds is never attached again, neither to verify nor to format:
    /// once its file is replaced, a second attach succeeds and the path has two attachments,
    /// which every later lookup refuses as ambiguous. The refusal comes before any attach runs,
    /// and the held device is left alone.
    #[test]
    fn an_image_the_kernel_already_holds_is_never_attached_again() {
        let image = Path::new("/tmp/cowshed-preexisting.dmg");
        let inventory = || images(&[("/tmp/cowshed-preexisting.dmg", &["/dev/disk5"][..])]);
        let refused = |error: ApfsError| {
            matches!(
                error,
                ApfsError::InvalidAttachmentInventory(message)
                    if message == "/tmp/cowshed-preexisting.dmg is already attached as \
                                   {\"/dev/disk5\"}; refusing to attach it a second time"
            )
        };

        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([inventory()]));
        let error = backend.attach_verified(image).unwrap_err();
        assert!(refused(error));
        assert_eq!(backend.runner().steps(), [Seen::Inventory]);

        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([inventory()]));
        let request = CreateImageRequest {
            staged_stem: PathBuf::from("/tmp/cowshed-preexisting"),
            capacity: ImageCapacity::from_gibibytes(1),
            volume_name: "cowshed".to_owned(),
            owner_uid: 501,
            owner_gid: 20,
        };
        let error = backend.format_attached(image, &request).unwrap_err();
        assert!(refused(error));
        assert_eq!(backend.runner().steps(), [Seen::Inventory]);
    }

    /// Two attachments of one path are listed both; only the caller that wants exactly one
    /// refuses them as ambiguous.
    #[test]
    fn every_attachment_of_a_path_held_twice_is_listed() {
        let image = Path::new("/tmp/cowshed-target.dmg");
        let mut second = live_capture();
        for media in &mut second.media {
            media.device = media.device.replace("/dev/disk1", "/dev/disk2");
        }
        let inventory = [live_capture(), second];
        assert!(matches!(
            existing_image_attachment(image, &inventory),
            Err(ApfsError::InvalidAttachmentInventory(message))
                if message == "matching image has multiple inventory entries"
        ));
        let held = image_attachments(image, &inventory).unwrap();
        assert_eq!(
            held.iter()
                .map(RecoveredImageAttachment::whole_device)
                .collect::<Vec<_>>(),
            ["/dev/disk14", "/dev/disk24"]
        );
    }

    /// A malformed attach report releases only the device this attach added, never another
    /// image's. (An image the kernel already holds is not attached at all; see
    /// `an_image_the_kernel_already_holds_is_never_attached_again`.)
    #[test]
    fn malformed_image_attach_detaches_only_the_new_device_and_verifies_absence() {
        let image = Path::new("/tmp/cowshed-malformed-attach.dmg");
        let backend = MacOsApfsBackend::new(StatefulMalformedAttachRunner::new(image, &[], false));

        let error = backend.attach_verified(image).unwrap_err();

        assert!(matches!(error, ApfsError::InvalidAttachmentPlist(_)));
        assert_eq!(backend.runner().devices_for(image), BTreeSet::new());
        assert_eq!(
            backend
                .runner()
                .devices_for(Path::new("/tmp/cowshed-unrelated.dmg")),
            BTreeSet::from(["/dev/disk20".into()])
        );
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 7);
        assert_eq!(steps[0], Seen::Inventory);
        assert_eq!(
            argv(steps[1].command()),
            [
                "attach",
                "-kernel",
                "-nomount",
                "-noverify",
                "-nobrowse",
                "-imagekey",
                "diskimage-class=CRawDiskImage",
                "-plist",
                "/tmp/cowshed-malformed-attach.dmg",
            ]
        );
        assert_eq!(steps[2], Seen::Inventory);
        assert_eq!(steps[3], Seen::Inventory);
        assert_eq!(
            argv(steps[4].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[6], Seen::Inventory);
        assert!(
            !commands(&steps)
                .iter()
                .any(|request| argv(request).iter().any(|arg| arg == "/dev/disk20"))
        );
    }

    #[test]
    fn malformed_blank_image_cleanup_failure_preserves_image_and_typed_context() {
        let stem = temp_path("malformed-blank-cleanup", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(StatefulMalformedAttachRunner::new(&image, &[], true));

        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("5g"),
                volume_name: "main".into(),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(image.exists());
        assert_eq!(
            backend.runner().devices_for(&image),
            BTreeSet::from(["/dev/disk8".into()])
        );
        assert!(
            error
                .to_string()
                .contains("cleaning up newly attached devices also failed")
        );
        assert!(std::error::Error::source(&error).is_some());
        match error {
            ApfsError::AttachmentCleanupFailed {
                image: failed_image,
                primary,
                cleanup,
            } => {
                assert_eq!(failed_image, image);
                assert!(matches!(*primary, ApfsError::InvalidAttachmentPlist(_)));
                assert!(cleanup.inventory.is_none());
                assert_eq!(cleanup.detach.len(), 1);
                assert_eq!(cleanup.detach[0].device, "/dev/disk8");
                assert!(matches!(
                    cleanup.detach[0].error.as_ref(),
                    ApfsError::CommandFailed {
                        operation: "detach image",
                        output: CommandOutput {
                            status: ProcessStatus::Exit(16),
                            ..
                        },
                        ..
                    }
                ));
                assert_eq!(cleanup.remaining_devices, ["/dev/disk8"]);
            }
            other => panic!("unexpected error: {other}"),
        }
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 6);
        assert_eq!(steps[0], Seen::Inventory);
        assert_eq!(steps[2], Seen::Inventory);
        assert_eq!(steps[3], Seen::Inventory);
        assert_eq!(
            argv(steps[4].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[5], Seen::Inventory);
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn failed_detach_remains_a_cleanup_error_when_inventory_reports_absence() {
        let image = Path::new("/tmp/cowshed-failed-detach.dmg");
        let attached = || images(&[("/tmp/cowshed-failed-detach.dmg", &["/dev/disk8"][..])]);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok("<plist><dict><key>malformed"),
            attached(),
            attached(),
            failed(1, "detach refused"),
            no_images(),
        ]));

        let error = backend.attach_verified(image).unwrap_err();

        match error {
            ApfsError::AttachmentCleanupFailed {
                primary, cleanup, ..
            } => {
                assert!(matches!(*primary, ApfsError::InvalidAttachmentPlist(_)));
                assert!(cleanup.inventory.is_none());
                assert_eq!(cleanup.detach.len(), 1);
                assert_eq!(cleanup.detach[0].device, "/dev/disk8");
                assert!(cleanup.remaining_devices.is_empty());
            }
            other => panic!("unexpected error: {other}"),
        }
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 6);
        assert_eq!(
            argv(steps[4].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[5], Seen::Inventory);
    }

    #[test]
    fn malformed_blank_image_inventory_failures_preserve_the_image() {
        for index in 0..2 {
            let stem =
                temp_path(&format!("malformed-inventory-{index}"), "stem").with_extension("");
            let image = stem.with_extension(IMAGE_EXTENSION);
            let cleanup_inventory = if index == 0 {
                unreadable_inventory()
            } else {
                images(&[(image.to_str().unwrap(), &["not-a-device"][..])])
            };
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
                no_images(),
                ok("<plist><dict><key>malformed"),
                cleanup_inventory,
            ]));

            let error = backend
                .create_staged_image(&CreateImageRequest {
                    staged_stem: stem,
                    capacity: capacity("5g"),
                    volume_name: "main".into(),
                    owner_uid: 502,
                    owner_gid: 20,
                })
                .unwrap_err();

            assert!(image.exists());
            match error {
                ApfsError::AttachmentCleanupFailed {
                    primary, cleanup, ..
                } => {
                    assert!(matches!(*primary, ApfsError::InvalidAttachmentPlist(_)));
                    let inventory = cleanup.inventory.expect("inventory failure is retained");
                    if index == 0 {
                        assert!(matches!(*inventory, ApfsError::KernelInventory(_)));
                    } else {
                        assert!(matches!(
                            *inventory,
                            ApfsError::InvalidAttachmentInventory(_)
                        ));
                    }
                    assert!(cleanup.detach.is_empty());
                    assert!(cleanup.remaining_devices.is_empty());
                }
                other => panic!("unexpected error: {other}"),
            }
            assert_eq!(backend.runner().steps().len(), 3);
            fs::remove_file(image).unwrap();
        }
    }

    #[test]
    fn post_create_image_attach_failure_removes_the_image() {
        let stem = temp_path("image-attach-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            failed(1, "unsupported after create"),
            no_images(),
            no_images(),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("5g"),
                volume_name: "main".into(),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "attach blank RAW image",
                ..
            }
        ));
        assert!(!image.exists());
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0], Seen::Inventory);
        assert_eq!(
            steps
                .iter()
                .filter(|step| **step == Seen::Inventory)
                .count(),
            3
        );
    }

    /// The gate failure this type exists for: `diskutil image attach` exiting 1 because the
    /// framework lost its XPC helper. It must arrive typed, with the framework's own stderr and
    /// the host load, not as one more generic exit 1 — and an ordinary refusal must not.
    #[test]
    fn an_attach_whose_framework_lost_its_helper_is_typed_with_the_host_load() {
        let helper_lost = "Error: Couldn\u{2019}t communicate with a helper application.\n";
        let attach = |stderr: &'static str| {
            let stem = temp_path("image-helper-lost", "stem").with_extension("");
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
                no_images(),
                failed(1, stderr),
                no_images(),
                no_images(),
            ]));
            let error = backend
                .create_staged_image(&CreateImageRequest {
                    staged_stem: stem.clone(),
                    capacity: capacity("5g"),
                    volume_name: "main".into(),
                    owner_uid: 502,
                    owner_gid: 20,
                })
                .unwrap_err();
            assert!(!stem.with_extension(IMAGE_EXTENSION).exists());
            error
        };

        let error = attach(helper_lost);
        let ApfsError::DiskImageHelperUnreachable(failure) = &error else {
            panic!("expected the typed helper failure, got {error:?}");
        };
        assert_eq!(failure.operation, "attach blank RAW image");
        assert!(failure.load.is_some(), "the host load travels with it");
        assert_eq!(failure.output.stderr, helper_lost.as_bytes());
        let report = error.to_string();
        assert!(
            report.starts_with(
                "the disk-images helper daemon could not be reached over XPC (loadavg "
            ),
            "{report}"
        );
        assert!(report.contains("helper application"), "{report}");

        assert!(matches!(
            attach("attach failed"),
            ApfsError::CommandFailed {
                operation: "attach blank RAW image",
                ..
            }
        ));
    }

    #[test]
    fn recovered_unformatted_media_is_released_without_deleting_its_backing_file() {
        let image = temp_path("unformatted-release", IMAGE_EXTENSION);
        fs::write(&image, b"unformatted").unwrap();
        let backend = graced_backend([
            holding(&image, "/dev/disk8"),
            holding(&image, "/dev/disk8"),
            ok([]),
            no_images(),
        ]);
        backend
            .release_every_attachment(&image, DetachIntent::Release)
            .unwrap();
        assert_eq!(fs::read(&image).unwrap(), b"unformatted");
        assert!(
            backend.runner().requests().iter().any(|request| {
                request.program == Path::new(HDIUTIL)
                    && argv(request) == ["detach", "-verbose", "/dev/disk8"]
            }),
            "positive fresh image ownership permits only its own device release"
        );
        fs::remove_file(image).unwrap();
    }

    /// The image's devices are read once to choose what to release and again right before each
    /// detach: a device the image no longer holds by then is refused, never detached.
    #[test]
    fn a_second_observation_conflict_cannot_claim_unformatted_media_was_released() {
        let image = temp_path("unformatted-second-map", IMAGE_EXTENSION);
        fs::write(&image, b"unformatted").unwrap();
        let backend =
            graced_backend([holding(&image, "/dev/disk8"), holding(&image, "/dev/disk9")]);
        let error = backend
            .release_every_attachment(&image, DetachIntent::Release)
            .unwrap_err();
        assert!(matches!(error, ApfsError::InvalidAttachmentInventory(_)));
        assert_eq!(fs::read(&image).unwrap(), b"unformatted");
        assert!(
            !backend
                .runner()
                .requests()
                .iter()
                .any(|request| { request.args.first().is_some_and(|arg| arg == "detach") }),
            "a second positive conflicting mapping retains the image without a false release"
        );
        fs::remove_file(image).unwrap();
    }

    fn blank_request(stem: PathBuf) -> CreateImageRequest {
        CreateImageRequest {
            staged_stem: stem,
            capacity: capacity("5g"),
            volume_name: "main".into(),
            owner_uid: 502,
            owner_gid: 20,
        }
    }

    /// The created blank is still exactly the sparse file creation made: its requested length,
    /// no more allocated blocks than a fresh sparse file of the same length beside it, and zero
    /// bytes at its head and tail, so nothing wrote, formatted or truncated it. Allocation is
    /// judged against that control, never a block count only one filesystem reports: a volume
    /// may charge a sparse file a block of its own, as the Linux CI test volume does.
    fn assert_untouched_blank(image: &Path, capacity: ImageCapacity) {
        use std::io::{Read as _, Seek as _, SeekFrom};
        use std::os::unix::fs::MetadataExt as _;
        let metadata = fs::symlink_metadata(image).expect("the created image is kept");
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), capacity.bytes());

        let mut control_name = image.as_os_str().to_owned();
        control_name.push(".sparse-control");
        let control = PathBuf::from(control_name);
        let control_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&control)
            .expect("create the sparse control");
        control_file
            .set_len(capacity.bytes())
            .expect("size the sparse control");
        control_file.sync_all().expect("sync the sparse control");
        let control_blocks = control_file
            .metadata()
            .expect("stat the sparse control")
            .blocks();
        drop(control_file);
        fs::remove_file(&control).expect("remove the sparse control");
        assert!(
            metadata.blocks() <= control_blocks,
            "nothing was written to the blank: {} blocks, a fresh sparse file takes {control_blocks}",
            metadata.blocks()
        );

        let mut file = File::open(image).expect("open the created image");
        let mut head = [0_u8; 4096];
        file.read_exact(&mut head).expect("read the blank's head");
        assert_eq!(head, [0; 4096], "the blank's head is unwritten");
        let mut tail = [0_u8; 4096];
        file.seek(SeekFrom::End(-4096))
            .expect("seek to the blank's tail");
        file.read_exact(&mut tail).expect("read the blank's tail");
        assert_eq!(tail, [0; 4096], "the blank's tail is unwritten");
    }

    /// Creation never takes over a path that exists: the file keeps its bytes and no disk
    /// command runs.
    #[test]
    fn blank_creation_refuses_an_existing_path_and_runs_nothing() {
        let stem = temp_path("image-existing-staged", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"someone else's").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let error = backend
            .create_staged_image(&blank_request(stem))
            .unwrap_err();
        assert!(
            matches!(
                &error,
                ApfsError::FileOperation {
                    operation: "create sparse RAW image",
                    path,
                    source,
                } if *path == image && source.kind() == io::ErrorKind::AlreadyExists
            ),
            "{error:?}"
        );
        assert_eq!(fs::read(&image).unwrap(), b"someone else's");
        assert!(backend.runner().requests().is_empty());
        fs::remove_file(image).unwrap();
    }

    /// The detached-resize root: a blank image is released after formatting only through the
    /// kernel's own mapping. `hdiutil info` once omitted the still-attached image here, the
    /// release read that omission as "already released", and the image stayed attached until
    /// `diskutil image resize` refused it as busy. No step may consult `hdiutil info` at all.
    #[test]
    fn a_formatted_blank_image_is_released_through_the_kernel_mapping() {
        let stem = temp_path("image-kernel-release", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = graced_backend([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk8"),
            ok([]),
            formatted_on_disk8(&image),
            formatted_on_disk8(&image),
            ok([]),
            formatted_on_disk8(&image),
            ok([]),
            no_images(),
        ]);
        backend.create_staged_image(&blank_request(stem)).unwrap();

        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 10);
        assert_eq!(steps[3].command().program, Path::new(NEWFS_APFS));
        assert_eq!(steps[4], Seen::Inventory);
        assert_eq!(steps[5], Seen::Inventory);
        // The formatted volume is verified before its detach, as the template mint does.
        assert_eq!(steps[6].command().program, Path::new(FSCK_APFS));
        assert_eq!(argv(steps[6].command()), ["-q", "/dev/rdisk5s1"]);
        assert_eq!(steps[7], Seen::Inventory);
        assert_eq!(
            argv(steps[8].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[9], Seen::Inventory);
        assert!(
            !commands(&steps)
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "info")),
            "no attachment decision reads `hdiutil info`"
        );
        assert!(backend.sleeper.waits().is_empty());
        fs::remove_file(image).unwrap();
    }

    /// The canonical image adopt creates is formatted on its one attach and handed back verified:
    /// the kernel's own record names the new volume, `fsck_apfs` checks it, and nothing detaches,
    /// so no second attach is needed to mount it.
    #[test]
    fn formatting_keeps_its_one_attach_and_hands_back_a_verified_volume() {
        let image = temp_path("image-format-attached", IMAGE_EXTENSION);
        let formatted = || {
            let path = attachment_inventory_path(&image).unwrap();
            images(&[(
                path.to_str().unwrap(),
                &["/dev/disk8", "/dev/disk5", "/dev/disk5s1"][..],
            )])
        };
        let backend = graced_backend([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk8"),
            ok([]),
            formatted(),
            formatted(),
            ok([]),
        ]);

        let attachment = backend
            .format_attached(&image, &blank_request(image.with_extension("")))
            .unwrap();

        assert_eq!(attachment.volume_device(), "/dev/disk5s1");
        let steps = backend.runner().steps();
        let commands = commands(&steps);
        assert_eq!(
            commands
                .iter()
                .filter(|request| {
                    request.program == Path::new(HDIUTIL)
                        && argv(request).first().is_some_and(|arg| arg == "attach")
                })
                .count(),
            1,
            "one attach formats and verifies the image"
        );
        assert!(
            !commands
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "detach")),
            "nothing detaches the image it formatted"
        );
        assert_eq!(
            commands.last().unwrap().program,
            Path::new(FSCK_APFS),
            "the volume is verified before anything mounts it"
        );
        assert_eq!(argv(commands.last().unwrap()), ["-q", "/dev/rdisk5s1"]);
    }

    /// A format whose volume never appears in the kernel's record is not trusted: the attachment
    /// is released by the image's own mapping and the image removed.
    #[test]
    fn formatting_without_a_registered_volume_releases_the_attachment_and_the_image() {
        let image = temp_path("image-format-no-volume", IMAGE_EXTENSION);
        fs::write(&image, b"blank").unwrap();
        let backend = graced_backend([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk8"),
            ok([]),
            holding(&image, "/dev/disk8"),
            holding(&image, "/dev/disk8"),
            ok([]),
            no_images(),
        ]);

        let error = backend
            .format_attached(&image, &blank_request(image.with_extension("")))
            .unwrap_err();

        assert!(
            matches!(error, ApfsError::InvalidAttachmentInventory(_)),
            "{error}"
        );
        assert!(!image.exists());
        let steps = backend.runner().steps();
        assert_eq!(
            argv(commands(&steps).last().unwrap()),
            ["detach", "-verbose", "/dev/disk8"]
        );
    }

    /// An unreadable kernel inventory at the final release is a typed refusal, never a release.
    #[test]
    fn an_unreadable_inventory_at_release_is_never_treated_as_released() {
        let stem = temp_path("image-unreadable-release", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = graced_backend([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk8"),
            ok([]),
            formatted_on_disk8(&image),
            formatted_on_disk8(&image),
            ok([]),
            unreadable_inventory(),
        ]);
        let error = backend
            .create_staged_image(&blank_request(stem))
            .unwrap_err();

        assert!(matches!(error, ApfsError::KernelInventory(_)), "{error}");
        assert!(
            !backend
                .runner()
                .requests()
                .iter()
                .any(|request| request.args.first().is_some_and(|arg| arg == "detach")),
            "an unknown mapping authorizes no eject"
        );
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn conflicting_blank_attachment_mapping_never_formats_or_removes_the_image() {
        let stem = temp_path("image-foreign-publication", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = graced_backend([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk9"),
        ]);
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("5g"),
                volume_name: "main".into(),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();
        assert!(matches!(error, ApfsError::InvalidAttachmentInventory(_)));
        assert_untouched_blank(&image, capacity("5g"));
        assert!(
            backend.sleeper.waits().is_empty(),
            "conflict is not announcement lag"
        );
        assert!(
            !backend.runner().requests().iter().any(|request| {
                request.program == Path::new(NEWFS_APFS)
                    || request.args.first().is_some_and(|arg| arg == "detach")
            }),
            "an observed foreign mapping authorizes no mutation"
        );
        fs::remove_file(image).unwrap();
    }

    /// The image driver registers an attach's media before `diskutil image attach` reports
    /// them, so a reported device the kernel does not map to the image is a contradiction to
    /// refuse at once, not lag to wait out.
    #[test]
    fn an_unregistered_blank_attachment_fails_closed_without_waiting_or_mutating() {
        let stem = temp_path("image-unregistered-attachment", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = graced_backend([no_images(), ok(BLANK_IMAGE_PLIST), no_images()]);
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("5g"),
                volume_name: "main".into(),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            ApfsError::InvalidAttachmentInventory(message) if message.contains("not registered")
        ));
        assert_untouched_blank(&image, capacity("5g"));
        assert!(backend.sleeper.waits().is_empty());
        assert!(
            !backend.runner().requests().iter().any(|request| {
                request.program == Path::new(NEWFS_APFS)
                    || request.args.first().is_some_and(|arg| arg == "detach")
            }),
            "absence is not permission to format, eject or remove a backing file"
        );
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn failed_newfs_detaches_and_removes_staged_image() {
        let stem = temp_path("image-newfs-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk8"),
            failed(70, "format failed"),
            holding(&image, "/dev/disk8"),
            ok([]),
            no_images(),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("5g"),
                volume_name: "main".into(),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "format RAW APFS volume",
                output: CommandOutput {
                    status: ProcessStatus::Exit(70),
                    ..
                },
                ..
            }
        ));
        assert!(!image.exists());
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 7);
        assert_eq!(steps[3].command().program, Path::new(NEWFS_APFS));
        assert_eq!(steps[4], Seen::Inventory);
        assert_eq!(
            argv(steps[5].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[6], Seen::Inventory);
    }

    #[test]
    fn failed_newfs_preserves_image_when_detach_cleanup_fails() {
        let stem = temp_path("image-detach-cleanup-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk8"),
            failed(70, "format failed"),
            holding(&image, "/dev/disk8"),
            failed(16, "busy"),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("5g"),
                volume_name: "main".into(),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(std::error::Error::source(&error).is_some());
        assert!(matches!(
            error,
            ApfsError::ImageCreationAndCleanupFailed {
                primary,
                detach: Some(detach),
                remove: None,
            } if matches!(
                *primary,
                ApfsError::CommandFailed {
                    operation: "format RAW APFS volume",
                    ..
                }
            ) && matches!(
                *detach,
                ApfsError::CommandFailed {
                    operation: "detach image",
                    output: CommandOutput {
                        status: ProcessStatus::Exit(16),
                        ..
                    },
                    ..
                }
            )
        ));
        assert!(image.exists());
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn failed_final_image_eject_preserves_attached_image() {
        let stem = temp_path("image-final-eject-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(BLANK_IMAGE_PLIST),
            holding(&image, "/dev/disk8"),
            ok([]),
            formatted_on_disk8(&image),
            formatted_on_disk8(&image),
            ok([]),
            formatted_on_disk8(&image),
            failed(16, "busy"),
        ]));
        let error = backend
            .create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("5g"),
                volume_name: "main".into(),
                owner_uid: 502,
                owner_gid: 20,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "detach image",
                output: CommandOutput {
                    status: ProcessStatus::Exit(16),
                    ..
                },
                ..
            }
        ));
        assert!(image.exists());
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn source_sync_skips_read_only_mounts_and_preserves_read_write_errors() {
        for (flags, expected_access) in [
            (0, MountAccess::ReadWrite),
            (1, MountAccess::ReadOnly),
            (0x0010_0000, MountAccess::ReadWrite),
            (0x0010_0001, MountAccess::ReadOnly),
        ] {
            let access = MountAccess::from_kernel_flags(flags);
            assert_eq!(access, expected_access);
            let mut flushed = false;
            let result = sync_writable_volume(flags, || {
                flushed = true;
                libc::EROFS
            });
            match expected_access {
                MountAccess::ReadOnly => {
                    assert!(!flushed, "read-only kernel mount must not be flushed");
                    assert!(result.is_ok());
                }
                MountAccess::ReadWrite => {
                    assert!(flushed, "read-write kernel mount must be flushed");
                    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EROFS));
                }
            }
        }
    }

    #[test]
    fn only_a_mounted_volume_root_is_a_mount_root() {
        let base = std::env::temp_dir().join(format!("cowshed-mount-root-{}", std::process::id()));
        fs::create_dir_all(&base).unwrap();
        assert!(
            !is_mount_root(&base),
            "a plain directory lies on its parent's volume"
        );
        assert!(!is_mount_root(&base.join("missing")));
        assert!(is_mount_root(Path::new("/")));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn clone_validation_requires_both_paths_to_be_image_paths() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for (source, destination, invalid) in [
            ("main.sparseimage", "session.dmg", "main.sparseimage"),
            ("main.dmg", "session.sparseimage", "session.sparseimage"),
        ] {
            let error = backend
                .clone_image(
                    Path::new(source),
                    Path::new(destination),
                    crate::ImageCapture::Detached,
                )
                .unwrap_err();
            assert!(matches!(
                error,
                CloneFileError::InvalidImagePath { path } if path == Path::new(invalid)
            ));
        }

        let source = temp_path("validated-missing-source", IMAGE_EXTENSION);
        let destination = temp_path("validated-missing-destination", IMAGE_EXTENSION);
        let error = backend
            .clone_image(&source, &destination, crate::ImageCapture::Detached)
            .unwrap_err();
        #[cfg(target_os = "macos")]
        assert!(matches!(error, CloneFileError::Io { .. }));
        #[cfg(not(target_os = "macos"))]
        assert!(matches!(error, CloneFileError::UnsupportedPlatform));
    }

    /// An attachment for identity tests: an absolute image path, as the inventory records it.
    fn identity_attachment(label: &str) -> AttachedImage {
        AttachedImage {
            image: temp_path(label, IMAGE_EXTENSION),
            whole_device: "/dev/disk12".into(),
            volume_device: "/dev/disk12s1".into(),
            pin: std::sync::Mutex::new(None),
        }
    }

    /// A device name is only a name for this image while the image holds it. Once the image is
    /// gone, the kernel hands the same `/dev/diskN` to the next attach, and detaching the recorded
    /// name would take another image away from its owner.
    #[test]
    fn detach_leaves_a_recorded_device_that_now_belongs_to_another_image() {
        let attachment = identity_attachment("reused-device");
        let backend = graced_backend([images(&[("/tmp/someone-else.dmg", &["/dev/disk12"][..])])]);

        backend.detach(&attachment, DetachIntent::Release).unwrap();

        let steps = backend.runner().steps();
        assert_eq!(
            steps,
            [Seen::Inventory],
            "no detach of another image's device: {steps:?}"
        );
    }

    #[test]
    fn detach_of_an_image_no_longer_attached_issues_no_detach() {
        let attachment = identity_attachment("already-gone");
        let backend = graced_backend([no_images()]);

        backend.detach(&attachment, DetachIntent::Release).unwrap();

        assert_eq!(backend.runner().steps(), [Seen::Inventory]);
    }

    #[test]
    fn detach_rechecks_the_image_after_the_storage_lease_wait() {
        struct RecycledDuringWait {
            image: PathBuf,
            granted: std::cell::Cell<bool>,
            commands: RefCell<Vec<CommandRequest>>,
        }
        impl CommandRunner for RecycledDuringWait {
            fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
                self.commands.borrow_mut().push(request.clone());
                Ok(CommandOutput::success([]))
            }
            fn run_guarded<E>(
                &self,
                request: &CommandRequest,
                check: impl FnOnce() -> Result<bool, E>,
            ) -> Result<Option<CommandOutput>, E>
            where
                E: From<CommandRunError> + fmt::Display,
            {
                // Waiting for the storage phase let our image depart and RAM reuse disk12.
                self.granted.set(true);
                if check()? {
                    self.run(request).map(Some).map_err(E::from)
                } else {
                    Ok(None)
                }
            }
            fn image_lease(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
                Ok(None)
            }
            fn pin_raw_device(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
                Ok(None)
            }
            fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
                let mut attached =
                    AttachedDiskImage::file(&self.image, None, [("/dev/disk12", "")]);
                if self.granted.get() {
                    attached.source = DiskImageSource::Url("ram://16777218".into());
                }
                Ok(vec![attached])
            }
            fn grow_image(&self, _: &Path, _: ImageCapacity) -> io::Result<()> {
                panic!("detach never grows an image");
            }
        }
        let attachment = identity_attachment("wait-recycled");
        let backend = MacOsApfsBackend::new(RecycledDuringWait {
            image: attachment.image.clone(),
            granted: std::cell::Cell::new(false),
            commands: RefCell::new(Vec::new()),
        });

        backend.detach(&attachment, DetachIntent::Release).unwrap();
        assert!(backend.runner().granted.get());
        assert!(backend.runner().commands.borrow().is_empty());
    }

    #[test]
    fn detach_rechecks_the_image_before_forcing_a_recycled_device() {
        let attachment = identity_attachment("force-recycled");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            dissent(),
            holding(&attachment.image, &attachment.whole_device),
            dissent(),
            holding(&attachment.image, &attachment.whole_device),
            dissent(),
            images(&[("/tmp/foreign.dmg", &["/dev/disk12"][..])]),
        ]);

        backend.detach(&attachment, DetachIntent::Release).unwrap();
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 3);
        assert!(
            requests
                .iter()
                .all(|request| !argv(request).contains(&"-force".into()))
        );
    }

    #[test]
    fn detach_rechecks_the_image_before_retrying_a_recycled_device() {
        let attachment = identity_attachment("retry-recycled");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            dissent(),
            images(&[("/tmp/foreign.dmg", &["/dev/disk12"][..])]),
        ]);

        backend.detach(&attachment, DetachIntent::Release).unwrap();
        assert_eq!(backend.runner().requests().len(), 1);
        assert_eq!(
            argv(&backend.runner().requests()[0]),
            ["detach", "-verbose", "/dev/disk12"]
        );
    }

    #[test]
    fn release_detaches_a_duplicate_image_device_only_once() {
        let image = temp_path("duplicate-release-view", IMAGE_EXTENSION);
        let duplicate = AttachedDiskImage::file(&image, None, [("/dev/disk12", "")]);
        let backend = graced_backend([
            Reply::Inventory(Ok(vec![duplicate.clone(), duplicate])),
            holding(&image, "/dev/disk12"),
            ok([]),
            no_images(),
        ]);

        backend
            .release_every_attachment(&image, DetachIntent::Release)
            .unwrap();
        assert_eq!(backend.runner().requests().len(), 1);
    }

    /// Detach-settle: a device already gone from the inventory costs one read and no waiting.
    #[test]
    fn detach_settle_returns_without_sleep_when_device_already_gone() {
        let attachment = identity_attachment("settle-gone");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            ok([]),
            no_images(),
        ]);
        backend.detach(&attachment, DetachIntent::Release).unwrap();
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[2], Seen::Inventory);
        assert!(
            backend.sleeper.waits().is_empty(),
            "no blind sleeps: departed on the first poll"
        );
    }

    /// Detach-settle: a lingering device is polled until it departs, then the detach succeeds.
    #[test]
    fn detach_settle_polls_until_departure_then_returns() {
        let attachment = identity_attachment("settle-polls");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            ok([]),
            holding(&attachment.image, &attachment.whole_device),
            no_images(),
        ]);
        backend.detach(&attachment, DetachIntent::Release).unwrap();
        assert_eq!(backend.runner().steps().len(), 4);
        assert_eq!(
            *backend.sleeper.waits(),
            [Duration::from_millis(10)],
            "one poll while present, none once departed"
        );
    }

    /// Detach-settle is soft by design: a device that never departs within the bound proceeds
    /// loudly instead of failing a successful detach. Failing here would turn transient DA
    /// slowness under load into user-facing failures; the loud log is the measurement.
    #[test]
    fn detach_settle_proceeds_loudly_at_bound_while_device_lingers() {
        let attachment = identity_attachment("settle-bound");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            ok([]),
            holding(&attachment.image, &attachment.whole_device),
            holding(&attachment.image, &attachment.whole_device),
            holding(&attachment.image, &attachment.whole_device),
        ]);
        backend.detach(&attachment, DetachIntent::Release).unwrap();
        assert_eq!(backend.runner().steps().len(), 5);
        assert_eq!(
            *backend.sleeper.waits(),
            [Duration::from_millis(10), Duration::from_millis(10)],
            "two polls spend the 20ms bound, then the bound stops the wait"
        );
    }

    /// Detach-settle reads the image's own mapping. Image leases let another image's attach take
    /// the freed `diskN` immediately; that reuse proves this image's device departed, so it costs
    /// one read and no waiting rather than the whole bound spent on someone else's attachment.
    #[test]
    fn detach_settle_treats_a_device_reused_by_another_image_as_departed() {
        let attachment = identity_attachment("settle-reused");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            ok([]),
            images(&[("/tmp/someone-else.dmg", &["/dev/disk12"][..])]),
        ]);
        backend.detach(&attachment, DetachIntent::Release).unwrap();
        assert_eq!(backend.runner().steps().len(), 3);
        assert!(
            backend.sleeper.waits().is_empty(),
            "another image now holding the name is not a lingering departure"
        );
    }

    /// The image driver's resource-busy refusal buys the whole grace, and the force lands
    /// exactly once at the end of it.
    #[test]
    fn a_released_detach_spends_its_grace_before_forcing_once() {
        let image = temp_path("detach-grace", IMAGE_EXTENSION);
        let backend = graced_backend([
            holding(&image, "/dev/disk4"),
            dissent(),
            holding(&image, "/dev/disk4"),
            dissent(),
            holding(&image, "/dev/disk4"),
            dissent(),
            holding(&image, "/dev/disk4"),
            ok([]),
        ]);

        backend
            .detach_device_checked(&image, "/dev/disk4", DetachIntent::Release)
            .unwrap();

        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 4, "three polite attempts, then one force");
        for request in requests.iter() {
            assert_eq!(request.program, Path::new(HDIUTIL));
        }
        for request in requests.iter().take(3) {
            assert_eq!(argv(request), ["detach", "-verbose", "/dev/disk4"]);
        }
        assert_eq!(
            argv(&requests[3]),
            ["detach", "-force", "-verbose", "/dev/disk4"]
        );
        assert_eq!(
            *backend.sleeper.waits(),
            [Duration::from_millis(10), Duration::from_millis(10)]
        );
    }

    /// A child's diagnostic output becomes one trace line, its blank lines dropped.
    #[test]
    fn child_output_reads_as_one_trace_line() {
        assert_eq!(
            one_line(b"hdiutil: detach: processing \"/dev/disk4\"\n\n  \"disk4\" ejected.\n"),
            "hdiutil: detach: processing \"/dev/disk4\" | \"disk4\" ejected."
        );
        assert_eq!(one_line(b""), "");
    }

    /// `WhenIdle` is the whole reason the intent exists: `resize`, `unmount`, and the adoption
    /// rollback must hand a live workspace's dissent back to their caller instead of forcing it.
    #[test]
    fn a_when_idle_detach_reports_the_dissent_without_waiting_or_forcing() {
        let image = temp_path("detach-idle", IMAGE_EXTENSION);
        let backend = graced_backend([holding(&image, "/dev/disk4"), dissent()]);

        let error = backend
            .detach_device_checked(&image, "/dev/disk4", DetachIntent::WhenIdle)
            .unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "detach image",
                output: CommandOutput {
                    status: ProcessStatus::Exit(libc::EBUSY),
                    ..
                },
                ..
            }
        ));
        assert_eq!(backend.runner().requests().len(), 1);
        assert!(backend.sleeper.waits().is_empty());
    }

    /// A seal's detach after an accepted unforced unmount: the transient device holder's
    /// dissent is waited out within the grace, and the detach that then goes through was never
    /// forced.
    #[test]
    fn a_within_grace_detach_waits_out_a_transient_dissent_without_forcing() {
        let image = temp_path("detach-within-grace", IMAGE_EXTENSION);
        let backend = graced_backend([
            holding(&image, "/dev/disk4"),
            dissent(),
            holding(&image, "/dev/disk4"),
            ok([]),
        ]);

        assert!(
            backend
                .detach_device_checked(&image, "/dev/disk4", DetachIntent::WithinGrace)
                .unwrap()
        );

        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 2, "one dissent, then the accepted retry");
        for request in requests.iter() {
            assert_eq!(argv(request), ["detach", "-verbose", "/dev/disk4"]);
        }
        assert_eq!(*backend.sleeper.waits(), [Duration::from_millis(10)]);
    }

    /// A dissent that outlasts the grace is handed back as the dissent itself, never forced: the
    /// seal refuses rather than tearing the device away from whatever holds it.
    #[test]
    fn a_within_grace_detach_answers_the_dissent_standing_when_the_grace_ends() {
        let image = temp_path("detach-within-grace-busy", IMAGE_EXTENSION);
        let backend = graced_backend([
            holding(&image, "/dev/disk4"),
            dissent(),
            holding(&image, "/dev/disk4"),
            dissent(),
            holding(&image, "/dev/disk4"),
            dissent(),
        ]);

        let error = backend
            .detach_device_checked(&image, "/dev/disk4", DetachIntent::WithinGrace)
            .unwrap_err();

        assert!(detach_was_dissented(&error), "{error}");
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 3, "three unforced attempts and no force");
        for request in requests.iter() {
            assert_eq!(argv(request), ["detach", "-verbose", "/dev/disk4"]);
        }
        assert_eq!(
            *backend.sleeper.waits(),
            [Duration::from_millis(10), Duration::from_millis(10)]
        );

        // The seal reports it as the device's refusal: the unmount already proved no file open,
        // so it names no holders and does not claim the volume is in use.
        let busy = ApfsError::CaptureSourceBusy {
            image: image.clone(),
            mount_point: PathBuf::from("/tmp/cowshed-build-mount"),
            held: CaptureHold::Device,
            refusal: Box::new(error),
        };
        assert!(detach_was_dissented(&busy));
        let said = busy.to_string();
        assert!(
            said.contains("unmounted with no file open")
                && said.contains("refused the unforced detach when the detach grace ended")
                && said.contains("nothing was forced")
                && !said.contains("is in use"),
            "{said}"
        );
    }

    /// Only a dissent is transient. A tool that failed for any other reason is failing now and
    /// will fail in ten seconds, so waiting on it would only delay the report.
    #[test]
    fn a_detach_that_failed_for_another_reason_is_never_waited_on() {
        for (status, message) in [
            (1, "no such device"),
            (libc::EBUSY, "permission denied"),
            (1, "Resource busy"),
        ] {
            let image = temp_path("detach-failed", IMAGE_EXTENSION);
            let backend = graced_backend([holding(&image, "/dev/disk4"), failed(status, message)]);
            let error = backend
                .detach_device_checked(&image, "/dev/disk4", DetachIntent::Release)
                .unwrap_err();
            assert!(matches!(
                error,
                ApfsError::CommandFailed { output, .. }
                    if output.status == ProcessStatus::Exit(status)
            ));
            assert_eq!(backend.runner().requests().len(), 1);
            assert!(backend.sleeper.waits().is_empty());
        }
    }

    /// What attaching `image` through the verified path asks: no prior attachment, the attach
    /// answering the fixture's devices, the inventory mapping them to the image, and a clean fsck.
    fn verified_attachment_replies(image: &str) -> [Reply; 4] {
        [
            no_images(),
            ok(ATTACH_PLIST),
            holding_verified_volume(image),
            ok([]),
        ]
    }

    /// A verified attachment of `session.dmg`, with `then` scripted for what follows it.
    fn attached_session(
        then: impl IntoIterator<Item = Reply>,
    ) -> (MacOsApfsBackend<RecordingRunner>, AttachedImage) {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs(
            verified_attachment_replies("session.dmg")
                .into_iter()
                .chain(then),
        ));
        let attachment = backend
            .attach_verified(Path::new("session.dmg"))
            .expect("verified attachment");
        (backend, attachment)
    }

    /// Every volume the backend named, as (volume device, name).
    fn names(backend: &MacOsApfsBackend<RecordingRunner>) -> Vec<(String, String)> {
        backend
            .runner()
            .steps()
            .into_iter()
            .filter_map(|step| match step {
                Seen::NameVolume(device, name) => Some((device, name)),
                _ => None,
            })
            .collect()
    }

    /// Where the scripted session's volume is mounted.
    const SESSION_MOUNT: &str = "/private/tmp/cowshed-session-mount";

    /// The session's volume still mounted at the root of [`SESSION_MOUNT`].
    fn session_still_mounted() -> Reply {
        Reply::Mount("/dev/disk5s1", true)
    }

    /// A rename proves the device the image's, then holds its mount root, and names the volume
    /// through that held root: no disk tool runs, so no Disk Arbitration path is ever resolved.
    #[test]
    fn rename_volume_names_only_the_held_root_of_the_attachments_volume() {
        let (backend, attachment) = attached_session([
            holding_verified_volume("session.dmg"),
            session_still_mounted(),
            Reply::Named(Ok(())),
        ]);
        let attached = backend.runner().requests().len();

        backend
            .rename_volume(
                &attachment,
                Path::new(SESSION_MOUNT),
                "cowshed.acme--widget.main",
            )
            .unwrap();

        let steps = backend.runner().steps();
        assert_eq!(
            steps[steps.len() - 3..],
            [
                Seen::Inventory,
                Seen::HoldMount(SESSION_MOUNT.into()),
                Seen::NameVolume("/dev/disk5s1".into(), "cowshed.acme--widget.main".into()),
            ],
            "the device is proven the image's, then its mount root held, then named through it"
        );
        assert_eq!(
            backend.runner().requests().len(),
            attached,
            "a rename runs no disk tool"
        );
    }

    /// The mount a label found can be gone by the time the rename holds the image's lease: the
    /// bare directory left there lies on the host's Data volume. A held mountpoint answering any
    /// other device than the image's volume, or a directory inside that volume rather than its
    /// root, is no owned root, and nothing is named.
    #[test]
    fn rename_volume_refuses_a_mountpoint_its_volume_left_before_any_rename() {
        for (found, root) in [("/dev/disk3s5", true), ("/dev/disk5s1", false)] {
            let (backend, attachment) = attached_session([
                holding_verified_volume("session.dmg"),
                Reply::Mount(found, root),
            ]);

            let renamed = backend.rename_volume(
                &attachment,
                Path::new(SESSION_MOUNT),
                &test_namespace().volume_label(format_args!("fixture · dispatch — main")),
            );

            assert!(
                matches!(
                    &renamed,
                    Err(ApfsError::RenameTargetNotMounted { volume_device, found: held, .. })
                        if volume_device == "/dev/disk5s1" && held == found
                ),
                "{found} root={root}: {renamed:?}"
            );
            assert!(names(&backend).is_empty(), "named {:?}", names(&backend));
        }
    }

    #[test]
    fn rename_volume_accepts_a_255_byte_path_safe_name() {
        let name = "a".repeat(255);
        let (backend, attachment) = attached_session([
            holding_verified_volume("session.dmg"),
            session_still_mounted(),
            Reply::Named(Ok(())),
        ]);

        backend
            .rename_volume(&attachment, Path::new(SESSION_MOUNT), &name)
            .unwrap();

        assert_eq!(names(&backend), [("/dev/disk5s1".to_owned(), name)]);
    }

    #[test]
    fn rename_volume_rejects_unsafe_or_oversized_names_before_spawning() {
        let invalid = [
            String::new(),
            " ".into(),
            ".".into(),
            "..".into(),
            " leading".into(),
            "trailing ".into(),
            "parent/child".into(),
            "nul\0name".into(),
            "a".repeat(256),
            "é".repeat(128),
        ];
        let (backend, attachment) = attached_session([]);
        let attached = backend.runner().steps().len();
        for name in invalid {
            let error = backend
                .rename_volume(&attachment, Path::new(SESSION_MOUNT), &name)
                .unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidVolumeName(rejected) if rejected == name
            ));
        }
        assert_eq!(backend.runner().steps().len(), attached);
    }

    /// A volume unmounted, even by force, after its root was proven leaves the held descriptor
    /// dead, and the kernel fails the name (`ENOTSUP`, measured). That failure is the rename's,
    /// named by device and mountpoint; it is not retried through any other route.
    #[test]
    fn rename_volume_reports_a_naming_failure_on_its_held_root() {
        let (backend, attachment) = attached_session([
            holding_verified_volume("session.dmg"),
            session_still_mounted(),
            Reply::Named(Err(io::Error::from_raw_os_error(libc::ENOTSUP))),
        ]);

        let error = backend
            .rename_volume(&attachment, Path::new(SESSION_MOUNT), "main")
            .unwrap_err();

        assert!(
            matches!(
                &error,
                ApfsError::RenameFailed { volume_device, mount_point, source }
                    if volume_device == "/dev/disk5s1"
                        && mount_point == Path::new(SESSION_MOUNT)
                        && source.raw_os_error() == Some(libc::ENOTSUP)
            ),
            "{error:?}"
        );
        assert_eq!(names(&backend).len(), 1, "one naming, never a second route");
    }

    /// Disk Arbitration's rename is refused for every caller of the system runner, in every case
    /// spelling diskutil accepts and whatever path names diskutil, before anything spawns. The
    /// program here does not exist: a rename that got past the refusal would fail to spawn, and
    /// any other verb of it does.
    #[test]
    fn the_system_runner_refuses_disk_arbitration_renames_before_spawning() {
        let label = test_namespace().volume_label(format_args!("fixture · dispatch — main"));
        let request = |verb: &str| {
            CommandRequest::new(
                "/nonexistent/cowshed-test/diskutil",
                [verb, "/dev/disk3s5", label.as_str()],
            )
        };
        let system = test_runner();
        for verb in ["rename", "renameVolume", "RENAMEVOLUME", "ReNaMe"] {
            for refused in [
                system.run(&request(verb)),
                system.run_with_inherited_input(
                    &request(verb),
                    CommandDeadline::new(DISK_CHILD_DEADLINE),
                    None,
                ),
                system
                    .run_guarded(&request(verb), || Ok::<_, ApfsError>(true))
                    .map_err(|error| match error {
                        ApfsError::CommandRun(error) => error,
                        other => panic!("{other}"),
                    })
                    .map(|output| output.expect("a guarded run that was allowed")),
            ] {
                let error = refused.expect_err("a Disk Arbitration rename never runs");
                assert!(
                    matches!(error.failure, CommandRunFailure::VolumeRename),
                    "{verb}: {error}"
                );
                assert!(error.to_string().contains("/dev/disk3s5"), "{error}");
            }
        }
        let other = system
            .run(&request("info"))
            .expect_err("the stand-in diskutil does not exist");
        assert!(
            matches!(other.failure, CommandRunFailure::Spawn(_)),
            "only renames are refused: {other}"
        );
    }

    /// A device name is reusable, not an identity. Once the image no longer holds the device its
    /// attachment recorded (ejected, or the name handed to another image's volume, or to the
    /// host's own), no rename may reach that device.
    #[test]
    fn rename_volume_never_reaches_a_device_no_store_image_holds() {
        let foreign = || {
            images(&[(
                "/private/tmp/foreign.dmg",
                &["/dev/disk4", "/dev/disk5", "/dev/disk5s1"][..],
            )])
        };
        for now in [no_images(), foreign()] {
            let (backend, attachment) = attached_session([now]);

            let renamed = backend.rename_volume(
                &attachment,
                Path::new(SESSION_MOUNT),
                &test_namespace().volume_label(format_args!("fixture · intents — slotted")),
            );

            assert!(
                matches!(renamed, Err(ApfsError::InvalidAttachmentInventory(_))),
                "{renamed:?}"
            );
            assert!(names(&backend).is_empty(), "named {:?}", names(&backend));
        }
    }

    /// Pins with a real `flock`ed descriptor, one file per pin, and notes whether the newest pin
    /// is still held when the volume is named: a second descriptor's non-blocking lock on the
    /// same file fails only while the pin's own descriptor is open.
    struct PinWitness {
        recorded: RecordingRunner,
        pins: PathBuf,
        taken: std::cell::Cell<usize>,
        newest: RefCell<Option<PathBuf>>,
        held_at_rename: RefCell<Option<bool>>,
    }

    impl PinWitness {
        fn new(recorded: RecordingRunner) -> Self {
            let pins = std::env::temp_dir().join(format!(
                "cowshed-pin-witness-{}",
                uuid::Uuid::new_v4().simple()
            ));
            fs::create_dir(&pins).expect("pin directory");
            Self {
                recorded,
                pins,
                taken: std::cell::Cell::new(0),
                newest: RefCell::new(None),
                held_at_rename: RefCell::new(None),
            }
        }

        fn newest_pin_held(&self) -> bool {
            let newest = self.newest.borrow();
            let pin = newest.as_deref().expect("a pin was taken");
            let probe = File::open(pin).expect("open the pin's file");
            probe.try_lock().is_err()
        }
    }

    impl CommandRunner for PinWitness {
        fn run_guarded<E>(
            &self,
            request: &crate::CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<crate::CommandOutput>, E>
        where
            E: From<crate::CommandRunError> + std::fmt::Display,
        {
            if check()? {
                self.run(request).map(Some).map_err(E::from)
            } else {
                Ok(None)
            }
        }
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            self.recorded.run(request)
        }
        fn name_volume(&self, root: &OwnedVolumeRoot, volume_name: &str) -> io::Result<()> {
            self.held_at_rename.replace(Some(self.newest_pin_held()));
            self.recorded.name_volume(root, volume_name)
        }
        fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
            self.recorded.image_lease(identity)
        }
        fn pin_raw_device(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
            let pin = self.pins.join(format!("pin-{}", self.taken.get()));
            self.taken.set(self.taken.get() + 1);
            let file = File::create(&pin)?;
            if let Err(error) = file.try_lock() {
                return Err(error.into());
            }
            self.newest.replace(Some(pin));
            Ok(Some(Fenced::new(file)))
        }
        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            self.recorded.attached_disk_images()
        }
        fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()> {
            self.recorded.grow_image(image, capacity)
        }
        fn hold_mount(&self, mount_point: &Path) -> io::Result<mounts::HeldMount> {
            self.recorded.hold_mount(mount_point)
        }
        fn volume_mount(
            &self,
            volume_device: &str,
        ) -> io::Result<Option<mounts::KernelMountSnapshot>> {
            self.recorded.volume_mount(volume_device)
        }
        fn unmount_guarded<E>(
            &self,
            mode: UnmountMode,
            prove: impl FnOnce() -> Result<Option<OwnedVolumeRoot>, E>,
        ) -> Result<UnmountEffect, E>
        where
            E: From<NativeUnmountError> + fmt::Display,
        {
            self.recorded.unmount_guarded(mode, prove)
        }
    }

    /// The rename's raw pin outlives the naming call. Dropped any earlier, the kernel could
    /// recycle the device between the mapping proof and the rename.
    #[test]
    fn rename_volume_holds_its_device_pin_across_the_rename() {
        let backend = MacOsApfsBackend::new(PinWitness::new(RecordingRunner::with_outputs(
            verified_attachment_replies("session.dmg")
                .into_iter()
                .chain([
                    holding_verified_volume("session.dmg"),
                    session_still_mounted(),
                    Reply::Named(Ok(())),
                ]),
        )));
        let attachment = backend
            .attach_verified(Path::new("session.dmg"))
            .expect("verified attachment");

        let renamed = backend.rename_volume(
            &attachment,
            Path::new(SESSION_MOUNT),
            "cowshed.acme--widget.main",
        );
        let held = backend.runner().held_at_rename.take();
        drop(attachment);
        fs::remove_dir_all(&backend.runner().pins).expect("remove the pins");

        renamed.expect("rename");
        assert_eq!(held, Some(true), "the rename ran without its device pin");
    }

    /// A target reseed finishes while mount waits for its image lease. The old device then
    /// disappears; a descriptor-backed pin excludes the next reseed until mount consumes it.
    struct ReseedBeforeMount {
        pins: PinWitness,
        reseeded: std::cell::Cell<bool>,
    }

    impl ReseedBeforeMount {
        fn new() -> Self {
            Self {
                pins: PinWitness::new(RecordingRunner::with_outputs([ok([])])),
                reseeded: std::cell::Cell::new(false),
            }
        }

        fn reseed(&self) -> io::Result<()> {
            if self.pins.newest.borrow().is_some() && self.pins.newest_pin_held() {
                return Err(io::Error::from(io::ErrorKind::ResourceBusy));
            }
            self.reseeded.set(true);
            Ok(())
        }
    }

    impl CommandRunner for ReseedBeforeMount {
        fn run_guarded<E>(
            &self,
            request: &crate::CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<crate::CommandOutput>, E>
        where
            E: From<crate::CommandRunError> + std::fmt::Display,
        {
            if check()? {
                self.run(request).map(Some).map_err(E::from)
            } else {
                Ok(None)
            }
        }
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            assert_eq!(request.program, Path::new(MOUNT_APFS));
            assert!(request.args.iter().any(|arg| arg == "/dev/disk25s1"));
            assert!(
                self.pins.newest_pin_held(),
                "mount must keep the recovered pin"
            );
            self.pins.run(request)
        }

        fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
            if !self.reseeded.get() {
                self.reseed()?;
            }
            self.pins.image_lease(identity)
        }

        fn pin_raw_device(&self, device: &Path) -> io::Result<Option<Fenced<File>>> {
            let current = if self.reseeded.get() {
                "/dev/rdisk25s1"
            } else {
                "/dev/rdisk15s1"
            };
            if device != Path::new(current) {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            self.pins.pin_raw_device(device)
        }

        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            let media = if self.reseeded.get() {
                [
                    ("/dev/disk24", ""),
                    ("/dev/disk25", APFS_CONTAINER_CONTENT),
                    ("/dev/disk25s1", APFS_VOLUME_CONTENT),
                ]
            } else {
                [
                    ("/dev/disk14", ""),
                    ("/dev/disk15", APFS_CONTAINER_CONTENT),
                    ("/dev/disk15s1", APFS_VOLUME_CONTENT),
                ]
            };
            Ok(vec![AttachedDiskImage::file(
                "/tmp/cowshed-target.dmg",
                None,
                media,
            )])
        }

        fn grow_image(&self, image: &Path, _: ImageCapacity) -> io::Result<()> {
            panic!("mount grew {}", image.display());
        }
        fn volume_mount(
            &self,
            volume_device: &str,
        ) -> io::Result<Option<mounts::KernelMountSnapshot>> {
            self.pins.volume_mount(volume_device)
        }
    }

    #[test]
    fn recovered_mount_survives_a_reseed_before_its_image_lease() {
        let backend = MacOsApfsBackend::new(ReseedBeforeMount::new());
        let mount = temp_path("recovered-mount-reseed", "mount");
        let attachment = backend
            .existing_attachment_for_mount(Path::new("/tmp/cowshed-target.dmg"))
            .expect("recover the target")
            .expect("the target is attached");
        assert_eq!(attachment.volume_device(), "/dev/disk25s1");
        assert_eq!(
            backend.runner().reseed().unwrap_err().kind(),
            io::ErrorKind::ResourceBusy,
            "the recovered pin must exclude a reseed before mount"
        );
        let mounted = backend.mount(&attachment, &mount, MountAccess::ReadOnly, false);
        assert!(
            !backend.runner().pins.newest_pin_held(),
            "mount must release the recovered pin"
        );
        drop(attachment);
        if mount.exists() {
            fs::remove_dir(&mount).expect("remove mountpoint");
        }
        fs::remove_dir_all(&backend.runner().pins.pins).expect("remove pins");
        mounted.expect("a reseed must not leave mount pinning the departed device");
    }

    #[test]
    fn recovered_mount_pin_is_released_when_the_handle_is_dropped() {
        let backend = MacOsApfsBackend::new(PinWitness::new(RecordingRunner::with_outputs([
            Reply::Inventory(Ok(vec![live_capture()])),
            Reply::Inventory(Ok(vec![live_capture()])),
        ])));
        let attachment = backend
            .existing_attachment_for_mount(Path::new("/tmp/cowshed-target.dmg"))
            .unwrap()
            .unwrap();
        assert!(backend.runner().newest_pin_held());
        drop(attachment);
        assert!(!backend.runner().newest_pin_held());
        assert!(backend.runner().recorded.requests().is_empty());
        fs::remove_dir_all(&backend.runner().pins).unwrap();
    }

    #[test]
    fn recovered_mount_conflicting_identity_drops_the_pin_without_mounting() {
        let backend = MacOsApfsBackend::new(PinWitness::new(RecordingRunner::with_outputs([
            Reply::Inventory(Ok(vec![live_capture()])),
            no_images(),
        ])));
        let result = backend.existing_attachment_for_mount(Path::new("/tmp/cowshed-target.dmg"));
        assert!(matches!(
            result,
            Err(ApfsError::InvalidAttachmentInventory(_))
        ));
        assert!(!backend.runner().newest_pin_held());
        assert!(backend.runner().recorded.requests().is_empty());
        fs::remove_dir_all(&backend.runner().pins).unwrap();
    }

    #[test]
    fn delete_image_removes_files_ignores_absence_and_reports_other_io() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let image = temp_path("delete", IMAGE_EXTENSION);
        fs::write(&image, b"image").unwrap();
        backend.delete_image(&image).unwrap();
        assert!(!image.exists());
        backend.delete_image(&image).unwrap();

        let directory = temp_path("delete-directory", IMAGE_EXTENSION);
        fs::create_dir(&directory).unwrap();
        let error = backend.delete_image(&directory).unwrap_err();
        assert!(matches!(
            error,
            ApfsError::FileOperation {
                operation: "delete image",
                ..
            }
        ));
        fs::remove_dir(directory).unwrap();
    }

    /// A relative and an absolute spelling of one image are one identity, and so one lease file:
    /// a capture handed a relative source excludes the attach handed its absolute path.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_relative_and_an_absolute_spelling_take_one_image_lease() {
        let relative = Path::new("lease-relative.dmg");
        let absolute = std::env::current_dir().unwrap().join(relative);
        assert_eq!(image_identity(relative).unwrap(), absolute);
        assert_eq!(attachment_inventory_path(relative).unwrap(), absolute);
        assert_eq!(
            image_lease_path(&image_identity(relative).unwrap()).unwrap(),
            image_lease_path(&absolute).unwrap()
        );
    }

    /// One identity's lease excludes every other holder of that identity — another descriptor
    /// in this very process included — while other identities stay free, a contended holder is
    /// handed the lease only after its release, and a wait that outlives its bound fails naming
    /// the holder instead of hanging.
    #[cfg(target_os = "macos")]
    #[test]
    fn image_lease_excludes_its_identity_leaves_others_free_and_bounds_the_wait() {
        use std::os::unix::fs::MetadataExt;

        let own = attachment_inventory_path(&temp_path("lease-own", IMAGE_EXTENSION)).unwrap();
        let other = attachment_inventory_path(&temp_path("lease-other", IMAGE_EXTENSION)).unwrap();
        let own_lease = image_lease_path(&own).unwrap();
        let other_lease = image_lease_path(&other).unwrap();
        assert_ne!(own_lease, other_lease);
        let directory = own_lease.parent().expect("lease directory");
        assert_eq!(other_lease.parent(), Some(directory));
        assert_eq!(
            fs::symlink_metadata(directory).unwrap().mode() & 0o777,
            0o700
        );

        let held = test_runner()
            .image_lease(&own)
            .unwrap()
            .expect("production takes a real lease");
        assert_eq!(held.metadata().unwrap().mode() & 0o777, 0o600);
        assert_eq!(
            fs::read_to_string(&own_lease).unwrap(),
            format!("{}\n", std::process::id())
        );
        let probe = open_image_lease(&own_lease).unwrap();
        assert!(
            !cowshed_os::flock::try_exclusive(&probe).unwrap(),
            "the identity is held"
        );
        drop(probe);
        assert!(
            test_runner().image_lease(&other).unwrap().is_some(),
            "another identity is free while this one is held"
        );

        let timed_out = acquire_image_lease(
            open_image_lease(&own_lease).unwrap(),
            &own_lease,
            &own,
            Bound::new(LatencyClass::Ffi, Duration::ZERO),
        )
        .expect_err("a held identity cannot be taken within a zero bound");
        assert_eq!(timed_out.kind(), io::ErrorKind::TimedOut);
        let message = timed_out.to_string();
        assert!(
            message.contains(&format!("by pid {}", std::process::id())),
            "{message}"
        );
        assert!(message.contains(own.to_str().unwrap()), "{message}");

        let order = std::sync::Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            let (own, order) = (&own, &order);
            let (started, waiting) = std::sync::mpsc::channel();
            let contender = scope.spawn(move || {
                started.send(()).expect("the test awaits the contender");
                let lease = test_runner().image_lease(own);
                order.lock().expect("order").push("acquired");
                lease
            });
            waiting.recv().expect("the contender started");
            order.lock().expect("order").push("released");
            drop(held);
            assert!(
                contender
                    .join()
                    .expect("contender thread")
                    .expect("contended lease")
                    .is_some()
            );
        });
        assert_eq!(*order.lock().expect("order"), ["released", "acquired"]);
        // Identities unique to this test: no other process can hold these files open.
        fs::remove_file(own_lease).unwrap();
        fs::remove_file(other_lease).unwrap();
    }

    #[test]
    fn blank_image_plist_requires_one_canonical_whole_device() {
        assert_eq!(
            parse_blank_image_whole_device(BLANK_IMAGE_PLIST.as_bytes()).unwrap(),
            "/dev/disk8"
        );
        let missing = br#"<?xml version="1.0"?><plist><dict>
          <key>system-entities</key><array>
            <dict><key>dev-entry</key><string>disk8s1</string></dict>
          </array>
        </dict></plist>"#;
        assert!(matches!(
            parse_blank_image_whole_device(missing),
            Err(ApfsError::InvalidAttachmentPlist(message))
                if message == "no canonical whole image device"
        ));
        let ambiguous = br#"<?xml version="1.0"?><plist><dict>
          <key>system-entities</key><array>
            <dict><key>dev-entry</key><string>disk8</string></dict>
            <dict><key>dev-entry</key><string>/dev/disk9</string></dict>
          </array>
        </dict></plist>"#;
        assert!(matches!(
            parse_blank_image_whole_device(ambiguous),
            Err(ApfsError::InvalidAttachmentPlist(message))
                if message == "multiple whole image devices"
        ));
    }

    #[test]
    fn attachment_inventory_selects_only_exact_image_path_whole_devices() {
        let images = attached_images(&[
            (
                "/tmp/cowshed-target.dmg",
                &["disk4", "/dev/disk4s1", "/dev/disk5"][..],
            ),
            ("/tmp/cowshed-unrelated.dmg", &["/dev/disk20"][..]),
        ]);

        let parsed = image_inventory(Path::new("/tmp/cowshed-target.dmg"), &images).unwrap();
        assert_eq!(
            parsed.devices,
            BTreeSet::from(["/dev/disk4".into(), "/dev/disk5".into()])
        );
        assert!(parsed.matched);
        assert_eq!(parsed.capacity, None);
        let absent = image_inventory(Path::new("/tmp/cowshed-absent.dmg"), &images).unwrap();
        assert!(!absent.matched);
        assert!(absent.devices.is_empty());
    }

    /// One attached disk image as the I/O Registry records it, with the device numbers and
    /// partition-type GUID contents of a live macOS 26.6 capture: the image's own store, then the
    /// synthesized container and its volume.
    fn live_capture() -> AttachedDiskImage {
        AttachedDiskImage::file(
            "/tmp/cowshed-target.dmg",
            Some(ImageCapacity::from_bytes(125_000 * 512)),
            [
                ("/dev/disk14", ""),
                ("/dev/disk15", APFS_CONTAINER_CONTENT),
                ("/dev/disk15s1", APFS_VOLUME_CONTENT),
            ],
        )
    }

    /// The mounted-restart root: a restarted controller finds its mounted image's attachment in
    /// the kernel inventory with one read and never attaches again. Its capacity probe and its
    /// identity read project the same record, so they cannot disagree about whether the image
    /// is attached — the disagreement `hdiutil info`'s truncated snapshots produced.
    #[test]
    fn existing_attachment_and_capacity_project_one_kernel_record() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            Reply::Inventory(Ok(vec![live_capture()])),
            Reply::Inventory(Ok(vec![live_capture()])),
        ]));

        let attachment = backend
            .existing_attachment(Path::new("/tmp/cowshed-target.dmg"))
            .expect("inventory")
            .expect("existing attachment");
        assert_eq!(attachment.whole_device(), "/dev/disk14");
        assert_eq!(attachment.volume_device(), "/dev/disk15s1");
        assert_eq!(
            backend
                .attached_capacity(Path::new("/tmp/cowshed-target.dmg"))
                .unwrap(),
            ImageCapacity::from_bytes(125_000 * 512)
        );
        assert_eq!(
            backend.runner().steps(),
            [Seen::Inventory, Seen::Inventory],
            "the kernel inventory names the volume; nothing is attached or spawned"
        );
    }

    /// Kernel absence is authoritative: no record for the image is no attachment and no
    /// capacity, never an error to retry or a cache to consult.
    #[test]
    fn kernel_absence_is_no_attachment() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([no_images(), no_images()]));
        assert!(
            backend
                .existing_attachment(Path::new("/tmp/cowshed-target.dmg"))
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            backend.attached_capacity(Path::new("/tmp/cowshed-target.dmg")),
            Err(ApfsError::ImageNotAttached(path)) if path == Path::new("/tmp/cowshed-target.dmg")
        ));
    }

    /// A question about one image asks the kernel for that image alone and never reads every
    /// attached image. Each whole read walked all of the host's media, and a verb asks several
    /// such questions, so with hundreds of images attached every verb's cost grew with them.
    #[test]
    fn one_image_questions_never_read_every_attached_image() {
        struct OneImage(AttachedDiskImage);
        impl CommandRunner for OneImage {
            fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
                panic!("a one-image question spawned {request:?}")
            }
            fn run_guarded<E>(
                &self,
                request: &CommandRequest,
                _: impl FnOnce() -> Result<bool, E>,
            ) -> Result<Option<CommandOutput>, E>
            where
                E: From<CommandRunError> + fmt::Display,
            {
                panic!("a one-image question spawned {request:?}")
            }
            fn image_lease(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
                Ok(None)
            }
            fn pin_raw_device(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
                Ok(None)
            }
            fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
                panic!("a one-image question read every attached image")
            }
            fn attachments_of(&self, image: &Path) -> io::Result<Vec<AttachedDiskImage>> {
                Ok(self
                    .0
                    .is_backed_by(image)
                    .then(|| self.0.clone())
                    .into_iter()
                    .collect())
            }
            fn grow_image(&self, image: &Path, _: ImageCapacity) -> io::Result<()> {
                panic!("a one-image question grew {}", image.display())
            }
        }
        let image = Path::new("/tmp/cowshed-target.dmg");
        let backend = MacOsApfsBackend::new(OneImage(live_capture()));
        assert_eq!(
            backend.attached_whole_devices(image).unwrap(),
            BTreeSet::from(["/dev/disk14".to_owned(), "/dev/disk15".to_owned()])
        );
        let attachment = backend
            .existing_attachment(image)
            .unwrap()
            .expect("attached");
        assert_eq!(
            (attachment.whole_device(), attachment.volume_device()),
            ("/dev/disk14", "/dev/disk15s1")
        );
        assert_eq!(
            backend.attached_capacity(image).unwrap(),
            ImageCapacity::from_bytes(125_000 * 512)
        );
        let views = backend.release_views(image).unwrap();
        assert_eq!(
            views
                .iter()
                .map(ReleaseView::whole_device)
                .collect::<Vec<_>>(),
            ["/dev/disk14"]
        );
        assert!(
            backend
                .recovered_image_attachment(Path::new("/tmp/cowshed-other.dmg"))
                .unwrap()
                .is_none()
        );
    }

    /// Foreign, ambiguous, malformed and unreadable inventories fail closed.
    #[test]
    fn foreign_ambiguous_and_unreadable_inventories_fail_closed() {
        let image = Path::new("/tmp/cowshed-target.dmg");
        let foreign = [
            AttachedDiskImage {
                source: DiskImageSource::Url("ram://2048".into()),
                capacity: None,
                media: live_capture().media,
                owner_uid: current_euid(),
            },
            AttachedDiskImage::file("/tmp/cowshed-other.dmg", None, [("/dev/disk20", "")]),
        ];
        assert!(
            existing_image_attachment(image, &foreign)
                .unwrap()
                .is_none()
        );

        let ambiguous = [live_capture(), live_capture()];
        assert!(matches!(
            existing_image_attachment(image, &ambiguous),
            Err(ApfsError::InvalidAttachmentInventory(message))
                if message == "matching image has multiple inventory entries"
        ));

        let volumeless = [AttachedDiskImage::file(
            image,
            None,
            [("/dev/disk14", ""), ("/dev/disk15", APFS_CONTAINER_CONTENT)],
        )];
        assert!(matches!(
            existing_image_attachment(image, &volumeless),
            Err(ApfsError::InvalidAttachmentInventory(_))
        ));

        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([unreadable_inventory()]));
        assert!(matches!(
            backend.existing_attachment(image),
            Err(ApfsError::KernelInventory(_))
        ));
    }

    #[test]
    fn disk_image_sources_decode_only_local_file_urls_to_paths() {
        assert_eq!(
            DiskImageSource::from_url("file:///private/tmp/a%20b.dmg"),
            DiskImageSource::File(PathBuf::from("/private/tmp/a b.dmg"))
        );
        for url in ["ram://2048", "file://host/share/x.dmg", "not a url"] {
            assert_eq!(
                DiskImageSource::from_url(url),
                DiskImageSource::Url(url.into())
            );
        }
    }

    #[test]
    fn inventory_attachment_entities_select_apfs_guid_hints() {
        let Some(RecoveredImageAttachment::Apfs(attachment)) =
            existing_image_attachment(Path::new("/tmp/cowshed-target.dmg"), &[live_capture()])
                .unwrap()
        else {
            panic!("the live capture is a formatted APFS attachment");
        };
        // The synthesized volume is mounted; the image driver's physical disk is detached.
        assert_eq!(attachment.volume_device(), "/dev/disk15s1");
        assert_eq!(attachment.whole_device(), "/dev/disk14");
    }

    /// An attach that does not report exactly one APFS volume inside a container it also reports
    /// is refused with what it reported: an image holds one volume in one container, so two
    /// volumes, none, or a volume whose container is missing are not an image's attachment.
    #[test]
    fn an_attach_plist_without_exactly_one_contained_volume_is_refused() {
        let plist = |entities: &[(&str, &str)]| {
            let rows: String = entities
                .iter()
                .map(|(device, hint)| {
                    format!(
                        "<dict><key>content-hint</key><string>{hint}</string><key>dev-entry</key><string>{device}</string></dict>"
                    )
                })
                .collect();
            format!(
                r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>{rows}</array></dict></plist>"#
            )
        };
        assert_eq!(
            parse_attachment_plist(ATTACH_PLIST.as_bytes()).unwrap(),
            ("/dev/disk4".into(), "/dev/disk5s1".into())
        );
        for (entities, refusal) in [
            (
                &[
                    ("disk4", ""),
                    ("disk5", "Apple_APFS_Container"),
                    ("disk5s1", "Apple_APFS_Volume"),
                    ("disk5s2", "Apple_APFS_Volume"),
                ][..],
                r#"expected one APFS volume, reported ["/dev/disk5s1", "/dev/disk5s2"]"#,
            ),
            (
                &[("disk4", ""), ("disk5s1", "Apple_APFS_Volume")][..],
                "APFS volume /dev/disk5s1 was reported without its container /dev/disk5",
            ),
            (
                &[
                    ("disk4", ""),
                    ("disk6", "Apple_APFS_Container"),
                    ("disk5s1", "Apple_APFS_Volume"),
                ][..],
                "APFS volume /dev/disk5s1 was reported without its container /dev/disk5",
            ),
        ] {
            assert!(
                matches!(
                    parse_attachment_plist(plist(entities).as_bytes()),
                    Err(ApfsError::InvalidAttachmentPlist(message)) if message == refusal
                ),
                "{entities:?}"
            );
        }
        // No volume at all is a typed answer, not a malformed report: the image was never
        // formatted (a blank whole device), or formatting stopped before its volume existed.
        for entities in [
            &[("disk4", "")][..],
            &[("disk4", ""), ("disk5", "Apple_APFS_Container")][..],
        ] {
            assert!(
                matches!(
                    parse_attachment_plist(plist(entities).as_bytes()),
                    Err(ApfsError::NoApfsVolume)
                ),
                "{entities:?}"
            );
        }
    }

    #[test]
    fn attachment_inventory_rejects_malformed_matching_records() {
        let image = Path::new("/tmp/cowshed-target.dmg");
        let malformed = [
            vec![AttachedDiskImage::file(
                image,
                None,
                Vec::<(&str, &str)>::new(),
            )],
            vec![AttachedDiskImage::file(image, None, [("not-a-device", "")])],
            vec![AttachedDiskImage::file(
                image,
                None,
                [("/dev/disk4s1", APFS_VOLUME_CONTENT)],
            )],
            vec![
                AttachedDiskImage::file(
                    image,
                    Some(ImageCapacity::from_bytes(512)),
                    [("/dev/disk4", "")],
                ),
                AttachedDiskImage::file(
                    image,
                    Some(ImageCapacity::from_bytes(1024)),
                    [("/dev/disk6", "")],
                ),
            ],
        ];
        for images in malformed {
            assert!(
                matches!(
                    image_inventory(image, &images),
                    Err(ApfsError::InvalidAttachmentInventory(_))
                ),
                "{images:?}"
            );
        }
    }

    #[test]
    fn attachment_inventory_returns_devices_and_capacity() {
        let image = Path::new("/tmp/cowshed-target.dmg");
        let images = [AttachedDiskImage::file(
            image,
            Some(ImageCapacity::from_bytes(419_430_400_u64 * 512)),
            [("/dev/disk4", "")],
        )];
        let parsed = image_inventory(image, &images).unwrap();
        assert_eq!(parsed.devices, BTreeSet::from(["/dev/disk4".into()]));
        assert_eq!(
            parsed.capacity,
            Some(ImageCapacity::from_bytes(419_430_400_u64 * 512)),
        );
        assert!(parsed.matched);
        assert_eq!(
            attachment_capacity(image, &images).unwrap(),
            parsed.capacity.unwrap()
        );
        let sizeless = [AttachedDiskImage::file(image, None, [("/dev/disk4", "")])];
        assert!(matches!(
            attachment_capacity(image, &sizeless),
            Err(ApfsError::InvalidAttachmentInventory(_))
        ));
    }

    /// Capacity is the RAW file's length, even while the image driver holds it exclusively.
    /// Neither reading it nor requesting growth spawns a disk command.
    #[cfg(target_os = "macos")]
    #[test]
    fn raw_capacity_comes_from_file_length_and_growth_spawns_no_disk_command() {
        use std::os::unix::fs::OpenOptionsExt;

        let (image, _) = sparse_raw_file("capacity-from-length", capacity("1g"));
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([Reply::Grow(Ok(()))]));
        let holder = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_EXLOCK | libc::O_NONBLOCK)
            .open(&image)
            .unwrap();
        assert_eq!(backend.image_capacity(&image).unwrap(), capacity("1g"));
        drop(holder);
        backend.resize_image(&image, capacity("2g")).unwrap();
        assert_eq!(
            backend.runner().steps(),
            [Seen::Grow(image.clone(), capacity("2g"))]
        );
        fs::write(&image, b"not a whole sector").unwrap();
        assert!(matches!(
            backend.image_capacity(&image),
            Err(ApfsError::FileOperation { operation: "read RAW image capacity", path, source })
                if path == image && source.kind() == io::ErrorKind::InvalidData
        ));
        fs::remove_file(image).unwrap();
    }

    /// A refused grow is the operation's failure, typed and naming the image, never a success.
    #[test]
    fn a_refused_grow_is_a_typed_failure_naming_the_image() {
        let image = Path::new("/tmp/cowshed-resize/main.dmg");
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([Reply::Grow(Err(
            io::Error::from_raw_os_error(libc::EWOULDBLOCK),
        ))]));

        let error = backend
            .resize_image(image, capacity("200g"))
            .expect_err("an attached image cannot grow");
        assert!(
            matches!(
                &error,
                ApfsError::FileOperation { operation: "grow RAW image", path, source }
                    if path == image && source.kind() == io::ErrorKind::WouldBlock
            ),
            "{error}"
        );
        assert!(backend.runner().requests().is_empty());
    }

    #[cfg(target_os = "macos")]
    fn sparse_raw_file(label: &str, capacity: ImageCapacity) -> (PathBuf, Vec<u8>) {
        use std::io::Write;
        let bytes: Vec<u8> = (0..=250_u8).cycle().take(4096).collect();
        let path = temp_path(label, IMAGE_EXTENSION);
        let mut file = File::create(&path).unwrap();
        file.write_all(&bytes).unwrap();
        file.set_len(capacity.bytes()).unwrap();
        (path, bytes)
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_growth_preserves_content_and_extends_with_holes() {
        use std::os::unix::fs::{FileExt, MetadataExt};
        let (image, before) = sparse_raw_file("grow-raw", capacity("1g"));
        let allocated_before = fs::metadata(&image).unwrap().blocks() * 512;
        test_runner().grow_image(&image, capacity("2g")).unwrap();
        let file = File::open(&image).unwrap();
        assert_eq!(file.metadata().unwrap().len(), capacity("2g").bytes());
        let mut prefix = vec![0; before.len()];
        file.read_exact_at(&mut prefix, 0).unwrap();
        assert_eq!(prefix, before);
        let mut tail = [1; 4096];
        file.read_exact_at(&mut tail, capacity("2g").bytes() - 4096)
            .unwrap();
        assert_eq!(tail, [0; 4096]);
        assert!(file.metadata().unwrap().blocks() * 512 <= allocated_before + 65536);
        drop(file);
        fs::remove_file(image).unwrap();
    }

    /// Refused growth leaves the complete small file and its length unchanged.
    #[cfg(target_os = "macos")]
    #[test]
    fn raw_growth_refusals_leave_every_byte_unchanged() {
        for (label, requested) in [
            ("unaligned", ImageCapacity::from_bytes(16384 + 512)),
            ("equal", ImageCapacity::from_bytes(8192)),
            ("shrink", ImageCapacity::from_bytes(4096)),
        ] {
            let (image, _) = sparse_raw_file(label, ImageCapacity::from_bytes(8192));
            let before = fs::read(&image).unwrap();
            let error = test_runner().grow_image(&image, requested).unwrap_err();
            assert_eq!(
                error.kind(),
                io::ErrorKind::InvalidInput,
                "{label}: {error}"
            );
            assert_eq!(fs::read(&image).unwrap(), before, "{label}");
            fs::remove_file(image).unwrap();
        }
        for bytes in [Vec::new(), vec![1; 513]] {
            let image = temp_path("invalid-raw-size", IMAGE_EXTENSION);
            fs::write(&image, &bytes).unwrap();
            let error = test_runner()
                .grow_image(&image, capacity("1g"))
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(fs::read(&image).unwrap(), bytes);
            fs::remove_file(image).unwrap();
        }
    }

    /// The image driver holds an attached image's file under an exclusive lock; another
    /// exclusive holder stands in for it here, and the grow refuses without writing.
    #[cfg(target_os = "macos")]
    #[test]
    fn raw_growth_refuses_a_file_another_descriptor_holds_exclusively() {
        use std::os::unix::fs::OpenOptionsExt;

        let (image, _) = sparse_raw_file("grow-exclusive", ImageCapacity::from_bytes(8192));
        let before = fs::read(&image).unwrap();
        let holder = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_EXLOCK | libc::O_NONBLOCK)
            .open(&image)
            .unwrap();
        let error = test_runner()
            .grow_image(&image, capacity("2g"))
            .expect_err("a held image cannot grow");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
        drop(holder);
        assert_eq!(fs::read(&image).unwrap(), before);
        fs::remove_file(image).unwrap();
    }

    /// Growth happens under the image's own production lease, so no cooperating attach,
    /// format or detach of the same image can interleave with the file-length change.
    #[cfg(target_os = "macos")]
    #[test]
    fn resize_image_grows_while_holding_the_image_lease() {
        struct LeaseProbe {
            held_while_growing: std::cell::Cell<bool>,
        }
        impl CommandRunner for LeaseProbe {
            fn run_guarded<E>(
                &self,
                request: &crate::CommandRequest,
                check: impl FnOnce() -> Result<bool, E>,
            ) -> Result<Option<crate::CommandOutput>, E>
            where
                E: From<crate::CommandRunError> + std::fmt::Display,
            {
                if check()? {
                    self.run(request).map(Some).map_err(E::from)
                } else {
                    Ok(None)
                }
            }
            fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
                panic!("growing spawned {request:?}");
            }
            fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
                test_runner().image_lease(identity)
            }
            fn pin_raw_device(&self, device: &Path) -> io::Result<Option<Fenced<File>>> {
                panic!("growing pinned {}", device.display());
            }
            fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
                panic!("growing read the attachment inventory");
            }
            fn grow_image(&self, image: &Path, _: ImageCapacity) -> io::Result<()> {
                let identity = attachment_inventory_path(image).expect("identity");
                let probe = open_image_lease(&image_lease_path(&identity)?)?;
                let contended = !cowshed_os::flock::try_exclusive(&probe)?;
                self.held_while_growing.set(contended);
                Ok(())
            }
        }

        let image = attachment_inventory_path(&temp_path("grow-leased", IMAGE_EXTENSION)).unwrap();
        let backend = MacOsApfsBackend::new(LeaseProbe {
            held_while_growing: std::cell::Cell::new(false),
        });
        backend.resize_image(&image, capacity("2g")).unwrap();
        assert!(backend.runner().held_while_growing.get());
        fs::remove_file(image_lease_path(&image).unwrap()).unwrap();
    }

    #[test]
    fn resize_refuses_a_path_that_is_not_a_raw_image() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let image = Path::new("/tmp/cowshed-resize/main.sparseimage");
        assert!(matches!(
            backend.resize_image(image, capacity("200g")),
            Err(ApfsError::InvalidImagePath(_))
        ));
        assert!(matches!(
            backend.image_capacity(image),
            Err(ApfsError::InvalidImagePath(_))
        ));
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn growing_a_container_that_already_spans_the_image_is_not_a_failure() {
        for refusal in [
            "Error: -69743: The new size must be different than the existing size",
            "Error: -69519: The target disk is too small for this operation",
        ] {
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
                no_images(),
                ok(ATTACH_PLIST),
                holding_verified_volume("/tmp/cowshed-resize/main.dmg"),
                ok([]),
                failed(1, refusal),
            ]));
            let attachment = backend
                .attach_verified(Path::new("/tmp/cowshed-resize/main.dmg"))
                .unwrap();
            backend.grow_container(&attachment).unwrap();
        }
    }

    #[test]
    fn a_container_growth_failure_that_is_not_an_already_full_refusal_propagates() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(ATTACH_PLIST),
            holding_verified_volume("/tmp/cowshed-resize/main.dmg"),
            ok([]),
            failed(1, "Error: -69620: The given file system is not supported"),
        ]));
        let attachment = backend
            .attach_verified(Path::new("/tmp/cowshed-resize/main.dmg"))
            .unwrap();
        assert!(matches!(
            backend.grow_container(&attachment),
            Err(ApfsError::CommandFailed {
                operation: "grow APFS container into image",
                ..
            })
        ));
    }

    #[test]
    fn capacities_round_trip_through_the_units_the_cli_accepts() {
        assert_eq!(capacity("100g").bytes(), 107_374_182_400);
        assert_eq!(capacity("1t").bytes(), 1_099_511_627_776);
        assert_eq!(capacity("200G"), capacity("204800m"));
        assert_eq!(capacity("100g").to_string(), "100g");
        assert_eq!(capacity("1024g").to_string(), "1t");
        assert_eq!(ImageCapacity::from_bytes(1_500_000).to_string(), "1500000");
        for rejected in ["", "g", "100", "100k", "-1g", "1.5g", "100gb"] {
            assert!(
                ImageCapacity::parse(rejected).is_err(),
                "{rejected} must not parse as a capacity"
            );
        }
    }

    #[test]
    fn device_identifier_helpers_preserve_block_and_raw_volume_identity() {
        assert_eq!(device_path("disk12"), Some("/dev/disk12".into()));
        assert_eq!(device_path("disk12s3"), Some("/dev/disk12s3".into()));
        // Leading zeros never appear in kernel device names; a second spelling of the same
        // device would defeat the textual identity comparisons, so it is rejected here exactly
        // as it always was in `is_kernel_device_path`.
        for invalid in [
            "disks1",
            "disk12s",
            "disk12sx",
            "/dev/not-a-disk",
            "disk01",
            "disk12s03",
        ] {
            assert_eq!(device_path(invalid), None);
        }
        assert_eq!(raw_device_from("/dev/disk12s3"), "/dev/rdisk12s3");
    }

    #[test]
    fn device_helpers_distinguish_whole_disks_slices_and_invalid_names() {
        assert_eq!(device_depth("/dev/disk12"), 0);
        assert_eq!(device_depth("/dev/disk12s3"), 1);
        assert_eq!(device_depth("/dev/disk12s3s1"), 2);
        assert_eq!(
            whole_device_from("/dev/disk12s3"),
            Some("/dev/disk12".into())
        );
        assert_eq!(whole_device_from("/dev/disk"), None);
        assert_eq!(whole_device_from("/dev/not-a-disk"), None);
        // The whole identifier must be well-formed, not just its unit prefix: a malformed tail
        // must not be truncated into a plausible container.
        assert_eq!(whole_device_from("/dev/disk12sx"), None);
        assert_eq!(whole_device_from("/dev/disk01s1"), None);
    }

    #[test]
    fn clonefile_errors_classify_existing_destination_and_other_io() {
        let destination = classify_clone_error(
            Path::new("main.dmg"),
            Path::new("session.dmg"),
            io::Error::from_raw_os_error(17),
        );
        assert!(matches!(
            destination,
            CloneFileError::DestinationExists { destination }
                if destination == Path::new("session.dmg")
        ));

        let other = classify_clone_error(
            Path::new("main.dmg"),
            Path::new("session.dmg"),
            io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
        );
        assert!(matches!(
            &other,
            CloneFileError::Io {
                source_path,
                destination_path,
                source,
            } if source_path == Path::new("main.dmg")
                && destination_path == Path::new("session.dmg")
                && source.kind() == io::ErrorKind::PermissionDenied
        ));
        assert_eq!(
            std::error::Error::source(&other).unwrap().to_string(),
            "denied"
        );
    }

    #[test]
    fn clone_reports_a_missing_source_without_creating_destination() {
        let source = temp_path("missing-clone-source", IMAGE_EXTENSION);
        let destination = temp_path("missing-clone-destination", IMAGE_EXTENSION);
        let error = MacOsApfsBackend::new(RecordingRunner::default())
            .clone_image(&source, &destination, crate::ImageCapture::Detached)
            .unwrap_err();
        #[cfg(target_os = "macos")]
        assert!(matches!(error, CloneFileError::Io { .. }));
        #[cfg(not(target_os = "macos"))]
        assert!(matches!(error, CloneFileError::UnsupportedPlatform));
        assert!(!destination.exists());
    }

    #[test]
    fn clonefile_cross_volume_error_is_typed() {
        let error = classify_clone_error(
            Path::new("main.dmg"),
            Path::new("session.dmg"),
            io::Error::from_raw_os_error(18),
        );
        assert!(matches!(error, CloneFileError::CrossVolume { .. }));
    }

    /// A fresh `.dmg` path pair in the temporary directory, which shares one volume.
    #[cfg(target_os = "macos")]
    fn clone_pair(label: &str) -> (PathBuf, PathBuf) {
        let nonce = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let directory = std::env::temp_dir();
        (
            directory.join(format!("cowshed-{label}-source-{nonce}.{IMAGE_EXTENSION}")),
            directory.join(format!(
                "cowshed-{label}-destination-{nonce}.{IMAGE_EXTENSION}"
            )),
        )
    }

    /// Open `image` the way the image driver holds an attached image's file.
    #[cfg(target_os = "macos")]
    fn lock_as_attached(image: &Path) -> io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXLOCK | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(image)
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_held_image_clones_by_descriptor_into_an_independent_file() {
        let (source, destination) = clone_pair("held-clone");
        fs::write(&source, b"fresh image bytes").unwrap();

        ImageSource::hold(&source, &destination, ImageCapture::Detached)
            .unwrap()
            .clone_to(&destination)
            .unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"fresh image bytes");
        fs::write(&destination, b"changed clone").unwrap();
        assert_eq!(fs::read(&source).unwrap(), b"fresh image bytes");

        fs::remove_file(source).unwrap();
        fs::remove_file(destination).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn an_image_an_attachment_locks_is_refused_and_nothing_is_cloned() {
        let (source, destination) = clone_pair("attached-clone");
        fs::write(&source, b"attached image bytes").unwrap();
        let attached = lock_as_attached(&source).unwrap();

        let error = MacOsApfsBackend::new(RecordingRunner::default())
            .clone_image(&source, &destination, crate::ImageCapture::Detached)
            .unwrap_err();
        assert!(
            matches!(&error, CloneFileError::SourceAttached { source: refused } if *refused == source),
            "{error}"
        );
        assert!(
            !destination.exists(),
            "a detached capture refuses an attached source"
        );

        drop(attached);
        fs::remove_file(source).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_held_image_keeps_attachments_out_and_admits_other_captures() {
        let (source, destination) = clone_pair("held-lock");
        fs::write(&source, b"held image bytes").unwrap();

        let held = ImageSource::hold(&source, &destination, ImageCapture::Detached).unwrap();
        let concurrent = ImageSource::hold(&source, &destination, ImageCapture::Detached)
            .expect("two detached captures of one source do not refuse each other");
        assert_eq!(
            lock_as_attached(&source).unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "no attach takes the image while a capture holds it"
        );
        drop((held, concurrent));
        drop(lock_as_attached(&source).expect("released once every capture lets go"));

        fs::remove_file(source).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_live_capture_preserves_an_attached_sources_bytes_and_lock() {
        let (source, destination) = clone_pair("live-clone");
        let bytes = b"the APFS volume is verified by attach, not by an outer image map";
        fs::write(&source, bytes).unwrap();
        let attached = lock_as_attached(&source).unwrap();
        MacOsApfsBackend::new(RecordingRunner::default())
            .clone_image(&source, &destination, ImageCapture::Live)
            .unwrap();
        assert_eq!(fs::read(&source).unwrap(), bytes);
        assert_eq!(fs::read(&destination).unwrap(), bytes);
        assert_eq!(
            lock_as_attached(&source).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(attached);
        fs::remove_file(source).unwrap();
        fs::remove_file(destination).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_symlinked_source_is_never_followed() {
        let (target, destination) = clone_pair("symlink-target");
        let (source, _) = clone_pair("symlink-source");
        fs::write(&target, b"target bytes").unwrap();
        std::os::unix::fs::symlink(&target, &source).unwrap();

        let error = MacOsApfsBackend::new(RecordingRunner::default())
            .clone_image(&source, &destination, crate::ImageCapture::Detached)
            .unwrap_err();
        assert!(
            matches!(&error, CloneFileError::Io { source, .. } if source.raw_os_error() == Some(libc::ELOOP)),
            "{error}"
        );
        assert!(!destination.exists());

        fs::remove_file(source).unwrap();
        fs::remove_file(target).unwrap();
    }

    const SYSTEM_UUID: &str = "A925F069-4430-489F-8206-41FC42B27763";
    const CACHES_UUID: &str = "2B1BDA0B-603C-497C-AD3D-DBA63F7BFA36";

    fn apfs_media(reference: Option<&str>, capacity_bytes: Option<u64>) -> ApfsMediaFacts {
        ApfsMediaFacts {
            reference: reference.map(str::to_owned),
            capacity_bytes,
        }
    }

    fn apfs_volume(
        media: u64,
        identifier: Option<&str>,
        name: Option<&str>,
        uuid: Option<&str>,
    ) -> ApfsVolumeFacts {
        ApfsVolumeFacts {
            media,
            identifier: identifier.map(str::to_owned),
            name: name.map(str::to_owned),
            uuid: uuid.map(str::to_owned),
            role: Some(0),
            encrypted: None,
        }
    }

    fn registered_volume(name: &str, identifier: &str, uuid: &str) -> RegisteredApfsVolume {
        RegisteredApfsVolume {
            name: name.to_owned(),
            identifier: identifier.to_owned(),
            volume_uuid: uuid.to_owned(),
            role: Some(0),
            encrypted: false,
        }
    }

    /// The registry rows measured on a host with FileVault off, where `diskutil info` answered
    /// FileVault No for Data and VM and Yes for the three role-less passphrase volumes.
    #[test]
    fn registered_apfs_carries_each_volume_role_and_encryption() {
        let media = BTreeMap::from([(0x10, apfs_media(Some("disk3"), Some(7_998_499_225_600)))]);
        let row = |identifier: &str, name: &str, role: Option<u64>, encrypted: Option<bool>| {
            let mut uuid = [0u8; 16];
            uuid[..identifier.len()].copy_from_slice(identifier.as_bytes());
            ApfsVolumeFacts {
                role,
                encrypted,
                ..apfs_volume(
                    0x10,
                    Some(identifier),
                    Some(name),
                    Some(&uuid::Uuid::from_bytes(uuid).hyphenated().to_string()),
                )
            }
        };
        let inventory = project_registered_apfs(
            &media,
            vec![
                row("disk3s5", "Data", Some(64), Some(true)),
                row("disk3s6", "VM", Some(8), None),
                row("disk3s7", "Nix Store", Some(0), Some(true)),
                row("disk3s8", "cowshed.store", Some(0), Some(true)),
                row("disk3s9", "cowshed.caches", Some(0), Some(false)),
                row("disk3s10", "unpublished", None, None),
            ],
        )
        .unwrap();
        let facts = inventory[0]
            .volumes
            .iter()
            .map(|volume| (volume.identifier.as_str(), volume.role, volume.encrypted))
            .collect::<Vec<_>>();
        assert_eq!(
            facts,
            [
                ("disk3s10", None, false),
                ("disk3s5", Some(64), true),
                ("disk3s6", Some(8), false),
                ("disk3s7", Some(0), true),
                ("disk3s8", Some(0), true),
                ("disk3s9", Some(0), false),
            ]
        );
    }

    #[test]
    fn registered_apfs_groups_volumes_by_their_container_provider() {
        let media = BTreeMap::from([
            (0x10, apfs_media(Some("disk3"), Some(7_998_499_225_600))),
            (0x20, apfs_media(Some("disk5"), Some(322_122_547_200))),
            (0x30, apfs_media(None, None)),
            (0x40, apfs_media(Some("disk9"), Some(214_748_364_800))),
        ]);
        let volumes = vec![
            apfs_volume(
                0x10,
                Some("disk3s9"),
                Some("cowshed.caches"),
                Some(&CACHES_UUID.to_ascii_lowercase()),
            ),
            apfs_volume(0x10, None, Some("arriving"), None),
            apfs_volume(0x40, Some("disk9s1"), Some("clone"), Some(SYSTEM_UUID)),
            apfs_volume(
                0x10,
                Some("disk3s1"),
                Some("Macintosh HD"),
                Some(SYSTEM_UUID),
            ),
        ];

        let inventory = project_registered_apfs(&media, volumes).unwrap();

        assert_eq!(
            inventory,
            vec![
                RegisteredApfsContainer {
                    reference: "disk3".to_owned(),
                    capacity_bytes: 7_998_499_225_600,
                    volumes: vec![
                        registered_volume("Macintosh HD", "disk3s1", SYSTEM_UUID),
                        registered_volume("cowshed.caches", "disk3s9", CACHES_UUID),
                    ],
                },
                RegisteredApfsContainer {
                    reference: "disk5".to_owned(),
                    capacity_bytes: 322_122_547_200,
                    volumes: Vec::new(),
                },
                RegisteredApfsContainer {
                    reference: "disk9".to_owned(),
                    capacity_bytes: 214_748_364_800,
                    volumes: vec![registered_volume("clone", "disk9s1", SYSTEM_UUID)],
                },
            ]
        );
    }

    #[test]
    fn registered_apfs_refuses_incomplete_or_conflicting_registry_facts() {
        let disk3 = || apfs_media(Some("disk3"), Some(1 << 30));
        let system = |identifier: &str| {
            apfs_volume(
                0x10,
                Some(identifier),
                Some("Macintosh HD"),
                Some(SYSTEM_UUID),
            )
        };
        let cases: Vec<(&str, BTreeMap<u64, ApfsMediaFacts>, Vec<ApfsVolumeFacts>)> = vec![
            (
                "not a whole disk",
                BTreeMap::from([(0x10, apfs_media(Some("disk3s1"), Some(1 << 30)))]),
                Vec::new(),
            ),
            (
                "not a whole disk",
                BTreeMap::from([(0x10, apfs_media(Some("/dev/disk3"), Some(1 << 30)))]),
                Vec::new(),
            ),
            (
                "registers no Size",
                BTreeMap::from([(0x10, apfs_media(Some("disk3"), None))]),
                Vec::new(),
            ),
            (
                "registers a zero Size",
                BTreeMap::from([(0x10, apfs_media(Some("disk3"), Some(0)))]),
                Vec::new(),
            ),
            (
                "both register BSD Name disk3",
                BTreeMap::from([(0x10, disk3()), (0x11, disk3())]),
                Vec::new(),
            ),
            (
                "published before its container media",
                BTreeMap::from([(0x10, apfs_media(None, None))]),
                vec![system("disk3s1")],
            ),
            (
                "the snapshot never read",
                BTreeMap::from([(0x20, disk3())]),
                vec![system("disk3s1")],
            ),
            (
                "whose volume it cannot be",
                BTreeMap::from([(0x10, disk3())]),
                vec![system("disk7s1")],
            ),
            (
                "whose volume it cannot be",
                BTreeMap::from([(0x10, disk3())]),
                vec![system("disk3s1s1")],
            ),
            (
                "whose volume it cannot be",
                BTreeMap::from([(0x10, disk3())]),
                vec![system("disk3")],
            ),
            (
                "registers no FullName",
                BTreeMap::from([(0x10, disk3())]),
                vec![apfs_volume(0x10, Some("disk3s1"), None, Some(SYSTEM_UUID))],
            ),
            (
                "registers an empty FullName",
                BTreeMap::from([(0x10, disk3())]),
                vec![apfs_volume(
                    0x10,
                    Some("disk3s1"),
                    Some(""),
                    Some(SYSTEM_UUID),
                )],
            ),
            (
                "registers no UUID",
                BTreeMap::from([(0x10, disk3())]),
                vec![apfs_volume(0x10, Some("disk3s1"), Some("Data"), None)],
            ),
            (
                "not a hyphenated volume UUID",
                BTreeMap::from([(0x10, disk3())]),
                vec![apfs_volume(
                    0x10,
                    Some("disk3s1"),
                    Some("Data"),
                    Some("A925F0694430489F820641FC42B27763"),
                )],
            ),
            (
                "not a hyphenated volume UUID",
                BTreeMap::from([(0x10, disk3())]),
                vec![apfs_volume(
                    0x10,
                    Some("disk3s1"),
                    Some("Data"),
                    Some("00000000-0000-0000-0000-000000000000"),
                )],
            ),
            (
                "both register BSD Name disk3s1",
                BTreeMap::from([(0x10, disk3())]),
                vec![
                    system("disk3s1"),
                    apfs_volume(0x10, Some("disk3s1"), Some("Data"), Some(CACHES_UUID)),
                ],
            ),
            (
                "both register UUID",
                BTreeMap::from([(0x10, disk3())]),
                vec![system("disk3s1"), system("disk3s2")],
            ),
        ];
        for (expected, media, volumes) in cases {
            let error = project_registered_apfs(&media, volumes)
                .expect_err(&format!("projection must refuse: {expected}"));
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?} in {error}"
            );
        }
    }

    /// One attachment whose container holds two volumes, as the kernel registry reports it.
    fn two_volume_image(image: &Path) -> Vec<AttachedDiskImage> {
        vec![AttachedDiskImage::file(
            image,
            None,
            [
                ("/dev/disk4", ""),
                ("/dev/disk5", APFS_CONTAINER_CONTENT),
                ("/dev/disk5s1", APFS_VOLUME_CONTENT),
                ("/dev/disk5s2", APFS_VOLUME_CONTENT),
            ],
        )]
    }

    /// A release of an image whose container holds two volumes detaches the container once,
    /// neither refusing it as ambiguous nor detaching a volume on its own.
    #[test]
    fn a_release_detaches_a_two_volume_container_once() {
        let image = temp_path("two-volume-release", IMAGE_EXTENSION);
        let backend = graced_backend([
            Reply::Inventory(Ok(two_volume_image(&image))),
            Reply::Inventory(Ok(two_volume_image(&image))),
            ok([]),
            no_images(),
        ]);
        backend
            .release_every_attachment(&image, DetachIntent::Release)
            .unwrap();
        let commands: Vec<Vec<String>> = backend.runner().requests().iter().map(argv).collect();
        assert_eq!(commands, [["detach", "-verbose", "/dev/disk4"]]);
    }

    /// A release unmounts each volume of the container under its own revalidation against the
    /// driver, and refuses a volume the driver does not hold without unmounting anything.
    #[test]
    fn a_release_revalidates_each_volume_it_unmounts_and_refuses_a_stranger() {
        let image = temp_path("two-volume-unmount", IMAGE_EXTENSION);
        let view = release_views_of(&image, &two_volume_image(&image))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(view.whole_device(), "/dev/disk4");
        let volumes: Vec<ReleasedVolume> = view.release_volumes().collect();
        let backend = graced_backend([
            Reply::Inventory(Ok(two_volume_image(&image))),
            unmount_ok(),
            Reply::Inventory(Ok(two_volume_image(&image))),
            unmount_ok(),
            Reply::Inventory(Ok(two_volume_image(&image))),
        ]);
        for volume in &volumes {
            backend
                .unmount_released(volume, DetachIntent::Release)
                .unwrap();
        }
        let stranger = ReleasedVolume(AttachedImage {
            image: image.clone(),
            whole_device: "/dev/disk4".into(),
            volume_device: "/dev/disk5s3".into(),
            pin: std::sync::Mutex::new(None),
        });
        let refused = backend
            .unmount_released(&stranger, DetachIntent::Release)
            .unwrap_err();
        assert!(
            matches!(&refused, ApfsError::InvalidAttachmentInventory(message)
            if message.contains("no longer owns /dev/disk4 / /dev/disk5s3")),
            "{refused}"
        );
        assert_eq!(
            unmounts(backend.runner()),
            [
                (
                    "/dev/disk5s1".into(),
                    mounts::FilesystemId::new([7, 26]),
                    UnmountMode::Normal
                ),
                (
                    "/dev/disk5s2".into(),
                    mounts::FilesystemId::new([7, 26]),
                    UnmountMode::Normal
                ),
            ]
        );
        assert!(backend.runner().requests().is_empty());
    }

    /// A release unmounts exactly the container's mounted volumes, each under its own
    /// revalidation, and leaves the unmounted one to the physical image's single detach. Cleanup
    /// releases the exact backing path the kernel holds, whatever its extension; that admits no
    /// other format to ordinary runtime use.
    #[test]
    fn a_release_unmounts_only_the_mounted_volumes_then_detaches_the_physical_image_once() {
        for extension in [IMAGE_EXTENSION, "img"] {
            let image = temp_path("two-volume-mixed", extension);
            let backend = graced_backend([
                Reply::Inventory(Ok(two_volume_image(&image))),
                Reply::Inventory(Ok(two_volume_image(&image))),
                Reply::Inventory(Ok(two_volume_image(&image))),
                unmount_ok(),
                Reply::Inventory(Ok(two_volume_image(&image))),
                Reply::Inventory(Ok(two_volume_image(&image))),
                ok([]),
                no_images(),
                no_images(),
                no_images(),
            ]);
            backend
                .runner()
                .volume_mounts
                .borrow_mut()
                .insert("/dev/disk5s1".into(), false);
            for _ in 0..2 {
                backend
                    .release_image(&image, DetachIntent::Release)
                    .unwrap();
            }
            assert_eq!(
                unmounts(backend.runner()),
                [(
                    "/dev/disk5s2".into(),
                    mounts::FilesystemId::new([7, 26]),
                    UnmountMode::Normal
                ),]
            );
            let commands: Vec<Vec<String>> = backend.runner().requests().iter().map(argv).collect();
            assert_eq!(commands, [["detach", "-verbose", "/dev/disk4"]]);
        }
    }

    /// A release of another user's attachment is refused before its mounted volumes are touched,
    /// so nothing is left unmounted but attached; the refusal is the runner's own, naming the
    /// detach it did not spawn. The caller is a fixed non-root uid, so the run holds as root too.
    #[test]
    fn a_release_of_another_users_attachment_refuses_before_unmounting() {
        let image = temp_path("two-volume-foreign", IMAGE_EXTENSION);
        let mut attached = two_volume_image(&image);
        attached[0].owner_uid = 0;
        let mut runner = RecordingRunner::with_outputs([Reply::Inventory(Ok(attached))]);
        runner.caller_uid = 501;
        let backend = MacOsApfsBackend::new(runner);
        let error = backend
            .release_image(&image, DetachIntent::Release)
            .unwrap_err();
        let ApfsError::CommandRun(CommandRunError {
            request,
            failure:
                CommandRunFailure::Release(release_authority::ReleaseRefusal::NeedsAuthorization {
                    disk,
                    owner_uid: 0,
                    euid: 501,
                }),
        }) = &error
        else {
            panic!("expected the release refusal, got {error}");
        };
        assert_eq!(disk, "disk4");
        assert_eq!(argv(request), ["detach", "-verbose", "/dev/disk4"]);
        assert_eq!(backend.runner().steps(), [Seen::Inventory]);
    }

    /// A release under `WhenIdle` hands a held volume's dissent straight back: no wait, no forced
    /// unmount, and no detach of the container.
    #[test]
    fn a_when_idle_release_returns_a_held_volumes_dissent_without_forcing_or_detaching() {
        let image = temp_path("two-volume-held", IMAGE_EXTENSION);
        let backend = graced_backend([
            Reply::Inventory(Ok(two_volume_image(&image))),
            Reply::Inventory(Ok(two_volume_image(&image))),
            Reply::Inventory(Ok(two_volume_image(&image))),
            unmount_dissent(),
        ]);
        backend
            .runner()
            .volume_mounts
            .borrow_mut()
            .insert("/dev/disk5s1".into(), false);
        let error = backend
            .release_image(&image, DetachIntent::WhenIdle)
            .unwrap_err();
        assert!(detach_was_dissented(&error), "{error}");
        assert_eq!(
            unmounts(backend.runner()),
            [(
                "/dev/disk5s2".into(),
                mounts::FilesystemId::new([7, 26]),
                UnmountMode::Normal
            ),]
        );
        assert!(backend.runner().requests().is_empty());
        assert!(backend.sleeper.waits().is_empty());
    }

    #[test]
    fn mounted_attach_uses_one_exact_no_mount_attach_then_the_pinned_volume() {
        for (access, browse, options) in [
            (MountAccess::ReadWrite, false, "nobrowse,owners,noatime"),
            (MountAccess::ReadWrite, true, "owners,noatime"),
            (
                MountAccess::ReadOnly,
                false,
                "rdonly,nobrowse,owners,noatime",
            ),
            (MountAccess::ReadOnly, true, "rdonly,owners,noatime"),
        ] {
            let image = temp_path("mounted-attach-command", IMAGE_EXTENSION);
            let mount = temp_path("mounted-attach-command", "mount");
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
                no_images(),
                ok(ATTACH_PLIST),
                holding_verified_volume(image.to_str().unwrap()),
                ok([]),
                ok([]),
                Reply::Mount("/dev/disk5s1", true),
            ]));
            let attached = backend
                .attach_mounted(&image, &mount, access, browse)
                .unwrap();
            let commands = backend.runner().requests();
            let mut attach = vec!["attach".to_owned()];
            if access == MountAccess::ReadOnly {
                attach.push("-readonly".to_owned());
            }
            attach.extend(IN_KERNEL_ATTACH.map(str::to_owned));
            attach.push(image.display().to_string());
            assert_eq!(commands.len(), 3);
            assert_eq!(commands[0].program, Path::new(HDIUTIL));
            assert_eq!(argv(&commands[0]), attach);
            assert_eq!(commands[1].program, Path::new(FSCK_APFS));
            assert_eq!(argv(&commands[1]), ["-q", "/dev/rdisk5s1"]);
            assert_eq!(commands[2].program, Path::new(MOUNT_APFS));
            assert_eq!(
                argv(&commands[2]),
                ["-o", options, "/dev/disk5s1", mount.to_str().unwrap()]
            );
            assert_eq!(attached.volume_device(), "/dev/disk5s1");
            if access == MountAccess::ReadWrite {
                assert!(
                    mount
                        .join(EVENT_LOG_DIRECTORY)
                        .join(EVENT_LOG_OPT_OUT)
                        .exists()
                );
            } else {
                assert!(!mount.join(EVENT_LOG_DIRECTORY).exists());
            }
            fs::remove_dir_all(mount).unwrap();
        }
    }

    struct UnmountAdmissionRunner {
        recorded: RecordingRunner,
        grants: std::cell::Cell<usize>,
        disappear_on_grant: usize,
        mounted: RefCell<BTreeSet<String>>,
        read_failure: bool,
        queries: RefCell<Vec<String>>,
    }

    impl UnmountAdmissionRunner {
        fn new(
            disappear_on_grant: usize,
            read_failure: bool,
            replies: impl IntoIterator<Item = Reply>,
        ) -> Self {
            Self {
                recorded: RecordingRunner::with_outputs(replies),
                grants: std::cell::Cell::new(0),
                disappear_on_grant,
                mounted: RefCell::new(BTreeSet::from([
                    "/dev/disk5s1".into(),
                    "/dev/disk99s1".into(),
                ])),
                read_failure,
                queries: RefCell::default(),
            }
        }
    }

    impl CommandRunner for UnmountAdmissionRunner {
        fn run_guarded<E>(
            &self,
            request: &CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<CommandOutput>, E>
        where
            E: From<CommandRunError> + fmt::Display,
        {
            self.recorded.run_guarded(request, check)
        }
        fn unmount_guarded<E>(
            &self,
            mode: UnmountMode,
            prove: impl FnOnce() -> Result<Option<OwnedVolumeRoot>, E>,
        ) -> Result<UnmountEffect, E>
        where
            E: From<NativeUnmountError> + fmt::Display,
        {
            self.grants.set(self.grants.get() + 1);
            if self.grants.get() == self.disappear_on_grant {
                self.mounted.borrow_mut().remove("/dev/disk5s1");
            }
            self.recorded.unmount_guarded(mode, prove)
        }
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            self.recorded.run(request)
        }
        fn volume_mount(
            &self,
            volume_device: &str,
        ) -> io::Result<Option<mounts::KernelMountSnapshot>> {
            assert!(
                self.grants.get() > 0,
                "mount state must be read after namespace admission"
            );
            self.queries.borrow_mut().push(volume_device.to_owned());
            if self.read_failure {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected kernel mount read failure",
                ));
            }
            if !self.mounted.borrow().contains(volume_device) {
                return Ok(None);
            }
            self.recorded.volume_mount(volume_device)
        }
        fn hold_mount(&self, mount_point: &Path) -> io::Result<mounts::HeldMount> {
            self.recorded.hold_mount(mount_point)
        }
        fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
            self.recorded.image_lease(identity)
        }
        fn pin_raw_device(&self, device: &Path) -> io::Result<Option<Fenced<File>>> {
            self.recorded.pin_raw_device(device)
        }
        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            self.recorded.attached_disk_images()
        }
        fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()> {
            self.recorded.grow_image(image, capacity)
        }
    }

    fn unmount_test_attachment() -> AttachedImage {
        AttachedImage {
            image: temp_path("unmount-admission", IMAGE_EXTENSION),
            whole_device: "/dev/disk4".into(),
            volume_device: "/dev/disk5s1".into(),
            pin: std::sync::Mutex::new(None),
        }
    }

    #[test]
    fn unmount_twice_succeeds_with_one_namespace_effect() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([unmount_ok()]));
        let attachment = unmount_test_attachment();
        for _ in 0..2 {
            assert_eq!(
                backend
                    .unmount_volume_unlocked(
                        &attachment,
                        DetachIntent::Release,
                        "unmount verified APFS volume"
                    )
                    .unwrap(),
                Unmounted::Unforced
            );
        }
        assert_eq!(
            unmounts(backend.runner()),
            [(
                "/dev/disk5s1".into(),
                mounts::FilesystemId::new([7, 26]),
                UnmountMode::Normal
            )]
        );
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn unmount_reads_exact_kernel_mount_state_after_namespace_admission() {
        let backend = MacOsApfsBackend::new(UnmountAdmissionRunner::new(1, false, []));
        let attachment = unmount_test_attachment();
        assert_eq!(
            backend
                .unmount_volume_unlocked(
                    &attachment,
                    DetachIntent::Release,
                    "unmount verified APFS volume"
                )
                .unwrap(),
            Unmounted::Unforced
        );
        assert!(unmounts(&backend.runner().recorded).is_empty());
        assert!(backend.runner().recorded.requests().is_empty());
        assert_eq!(*backend.runner().queries.borrow(), ["/dev/disk5s1"]);
    }

    #[test]
    fn unmount_kernel_read_failure_is_not_absence_or_dissent() {
        let backend = MacOsApfsBackend::new(UnmountAdmissionRunner::new(1, true, []));
        let error = backend
            .unmount_volume_unlocked(
                &unmount_test_attachment(),
                DetachIntent::Release,
                "unmount verified APFS volume",
            )
            .unwrap_err();
        assert!(
            matches!(&error, ApfsError::UnmountFailed { source: NativeUnmountError::KernelRead { stage: "read kernel mount before APFS unmount", source }, .. } if source.kind() == io::ErrorKind::PermissionDenied)
        );
        assert!(!detach_was_dissented(&error));
        assert!(unmounts(&backend.runner().recorded).is_empty());
        assert!(backend.runner().recorded.requests().is_empty());
    }

    #[test]
    fn a_volume_unmounted_during_grace_is_not_reported_forced() {
        let backend = MacOsApfsBackend::with_grace(
            UnmountAdmissionRunner::new(2, false, [unmount_dissent()]),
            RecordingSleeper::default(),
            DetachGrace {
                total: Duration::ZERO,
                poll: Duration::from_millis(10),
            },
        );
        assert_eq!(
            backend
                .unmount_volume_unlocked(
                    &unmount_test_attachment(),
                    DetachIntent::Release,
                    "unmount verified APFS volume"
                )
                .unwrap(),
            Unmounted::Unforced
        );
        assert_eq!(
            unmounts(&backend.runner().recorded),
            [(
                "/dev/disk5s1".into(),
                mounts::FilesystemId::new([7, 26]),
                UnmountMode::Normal
            )]
        );
        assert!(backend.sleeper.waits().is_empty());
    }

    #[test]
    fn a_busy_native_unmount_waits_its_grace_then_forces_only_the_exact_root() {
        let backend = graced_backend([
            unmount_dissent(),
            unmount_dissent(),
            unmount_dissent(),
            unmount_ok(),
        ]);
        assert_eq!(
            backend
                .unmount_volume_unlocked(
                    &unmount_test_attachment(),
                    DetachIntent::Release,
                    "unmount verified APFS volume"
                )
                .unwrap(),
            Unmounted::Forced
        );
        let effects = unmounts(backend.runner());
        assert_eq!(
            effects.iter().map(|(_, _, mode)| *mode).collect::<Vec<_>>(),
            [
                UnmountMode::Normal,
                UnmountMode::Normal,
                UnmountMode::Normal,
                UnmountMode::ForceAfterGrace
            ]
        );
        assert!(
            effects
                .iter()
                .all(|(device, fsid, _)| device == "/dev/disk5s1"
                    && *fsid == mounts::FilesystemId::new([7, 26]))
        );
        assert!(backend.runner().requests().is_empty());
        assert_eq!(
            *backend.sleeper.waits(),
            [Duration::from_millis(10), Duration::from_millis(10)]
        );
    }

    #[test]
    fn native_busy_intents_preserve_immediate_and_within_grace_refusal() {
        for (intent, replies, attempts, sleeps) in [
            (DetachIntent::WhenIdle, vec![unmount_dissent()], 1, 0),
            (
                DetachIntent::WithinGrace,
                vec![unmount_dissent(), unmount_dissent(), unmount_dissent()],
                3,
                2,
            ),
        ] {
            let backend = graced_backend(replies);
            let error = backend
                .unmount_volume_unlocked(
                    &unmount_test_attachment(),
                    intent,
                    "unmount verified APFS volume",
                )
                .unwrap_err();
            assert!(detach_was_dissented(&error));
            let effects = unmounts(backend.runner());
            assert_eq!(effects.len(), attempts);
            assert!(
                effects
                    .iter()
                    .all(|(_, _, mode)| *mode == UnmountMode::Normal)
            );
            assert_eq!(backend.sleeper.waits().len(), sleeps);
        }
    }

    #[test]
    fn native_authority_read_postcondition_and_other_failures_never_earn_grace() {
        let fsid = mounts::FilesystemId::new([7, 26]);
        for source in [
            NativeUnmountError::Authority {
                expected: fsid,
                found: mounts::FilesystemId::new([8, 26]),
            },
            NativeUnmountError::KernelRead {
                stage: "post-unmount mount table",
                source: io::Error::from_raw_os_error(libc::EIO),
            },
            NativeUnmountError::Postcondition { filesystem: fsid },
            NativeUnmountError::Failed(io::Error::from_raw_os_error(libc::EPERM)),
            NativeUnmountError::Failed(io::Error::from_raw_os_error(libc::EINVAL)),
        ] {
            let backend = graced_backend([Reply::Unmount(Err(source))]);
            let error = backend
                .unmount_volume_unlocked(
                    &unmount_test_attachment(),
                    DetachIntent::Release,
                    "unmount verified APFS volume",
                )
                .unwrap_err();
            assert!(matches!(error, ApfsError::UnmountFailed { .. }));
            assert!(!detach_was_dissented(&error));
            assert_eq!(unmounts(backend.runner()).len(), 1);
            assert!(backend.runner().requests().is_empty());
            assert!(backend.sleeper.waits().is_empty());
        }
    }

    #[test]
    fn unmount_refuses_foreign_and_nonroot_held_paths_before_any_native_effect() {
        for (device, is_root) in [("/dev/disk99s1", true), ("/dev/disk5s1", false)] {
            let backend = graced_backend([Reply::Mount(device, is_root)]);
            let error = backend
                .unmount_volume_unlocked(
                    &unmount_test_attachment(),
                    DetachIntent::Release,
                    "unmount verified APFS volume",
                )
                .unwrap_err();
            assert!(matches!(error, ApfsError::UnmountTargetNotMounted { .. }));
            assert!(unmounts(backend.runner()).is_empty());
            assert!(backend.runner().requests().is_empty());
            assert!(backend.sleeper.waits().is_empty());
        }
    }

    #[test]
    fn command_unmount_dissent_parser_remains_exact_and_never_recognizes_absence_text() {
        for (status, text, expected) in [
            (
                1,
                "Volume fixture failed to unmount: dissented by PID 13258 (/bin/bash)",
                true,
            ),
            (1, "Volume fixture failed to unmount", false),
            (1, "Volume fixture is already unmounted", false),
            (1, "Permission denied", false),
            (libc::EBUSY, "Resource busy", false),
        ] {
            let error = command_failed(
                "unmount",
                CommandRequest::new(DISKUTIL, ["unmount", "/dev/disk5s1"]),
                CommandOutput::failure(status, text),
            );
            assert_eq!(detach_was_dissented(&error), expected, "{text}");
        }
    }

    #[test]
    fn a_refused_or_wrong_root_mount_releases_only_its_exact_volume_without_a_mount_fallback() {
        for wrong_root in [false, true] {
            let image = temp_path("refused-mounted-attach", IMAGE_EXTENSION);
            let mount = temp_path("refused-mounted-attach", "mount");
            let mut replies = vec![
                no_images(),
                ok(ATTACH_PLIST),
                holding_verified_volume(image.to_str().unwrap()),
                ok([]),
            ];
            if wrong_root {
                replies.extend([ok([]), Reply::Mount("/dev/disk99s1", true)]);
            } else {
                replies.push(failed(1, "mount refused"));
            }
            replies.extend([
                holding_verified_volume(image.to_str().unwrap()),
                unmount_ok(),
                holding(&image, "/dev/disk4"),
                holding(&image, "/dev/disk4"),
                ok([]),
                no_images(),
                no_images(),
            ]);
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs(replies));
            let error = backend
                .attach_mounted(&image, &mount, MountAccess::ReadOnly, false)
                .unwrap_err();
            if wrong_root {
                assert!(matches!(error, ApfsError::InvalidAttachmentInventory(_)));
            } else {
                assert!(matches!(
                    error,
                    ApfsError::CommandFailed {
                        operation: "mount verified APFS volume",
                        ..
                    }
                ));
            }
            let commands = backend.runner().requests();
            assert_eq!(commands.len(), 4);
            assert_eq!(
                commands
                    .iter()
                    .filter(|command| is_in_kernel_attach(command))
                    .count(),
                1
            );
            assert_eq!(
                commands
                    .iter()
                    .filter(|command| command.program == Path::new(MOUNT_APFS))
                    .count(),
                1
            );
            assert_eq!(
                unmounts(backend.runner()),
                [(
                    "/dev/disk5s1".into(),
                    mounts::FilesystemId::new([7, 26]),
                    UnmountMode::Normal
                )]
            );
            assert_eq!(commands[3].program, Path::new(HDIUTIL));
            assert_eq!(argv(&commands[3]), ["detach", "-verbose", "/dev/disk4"]);
            fs::remove_dir(mount).unwrap();
        }
    }
    /// Every runtime image attach is the in-kernel DI1 attach of the exact image: `hdiutil attach
    /// -kernel` with the RAW class, never mounted by the tool, read-only exactly when asked, the
    /// image last. It is never a DiskImages2 attach, so no `diskimagesiod` helper is involved; a
    /// retained re-adoption input attaches the same way, read-only.
    #[test]
    fn every_attach_is_the_in_kernel_attach_of_the_exact_image() {
        let image = Path::new("/private/cowshed/store/acme/widget/sessions/raven.dmg");
        for access in [MountAccess::ReadWrite, MountAccess::ReadOnly] {
            let request = in_kernel_attach(image, access);
            assert_eq!(request.program, Path::new(HDIUTIL));
            let args = argv(&request);
            assert_eq!(args.first().map(String::as_str), Some("attach"));
            assert_eq!(args.last().map(String::as_str), image.to_str());
            assert_eq!(
                args.iter().any(|arg| arg == "-readonly"),
                access == MountAccess::ReadOnly
            );
            for flag in IN_KERNEL_ATTACH {
                assert_eq!(args.iter().filter(|arg| *arg == flag).count(), 1, "{flag}");
            }
            assert!(is_in_kernel_attach(&request));
        }
    }

    #[test]
    fn a_reported_device_lost_before_verification_never_authorizes_a_second_attach() {
        let image = temp_path("lost-device-no-reattach", IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(ATTACH_PLIST),
            no_images(),
        ]));
        assert!(matches!(
            backend.attach_verified(&image),
            Err(ApfsError::InvalidAttachmentInventory(_))
        ));
        let commands = backend.runner().requests();
        assert_eq!(commands.len(), 1);
        assert!(is_in_kernel_attach(&commands[0]));
        assert!(argv(&commands[0]).iter().any(|arg| arg == "-nomount"));
    }

    /// What attaches, verifies or looks up one attachment still refuses a container with two
    /// volumes: the attach report, the single-attachment lookup and the unmount identity check.
    #[test]
    fn single_attachment_projections_still_refuse_two_volumes() {
        let image = temp_path("two-volume-single", IMAGE_EXTENSION);
        let refusal = r#"expected one APFS volume, reported ["/dev/disk5s1", "/dev/disk5s2"]"#;
        let entities: Vec<(String, String)> = two_volume_image(&image)[0]
            .media
            .iter()
            .map(|media| (media.device.clone(), media.content.clone()))
            .collect();
        assert_eq!(attached_apfs_volume(&entities).unwrap_err(), refusal);
        assert!(matches!(
            existing_image_attachment(&image, &two_volume_image(&image)),
            Err(ApfsError::InvalidAttachmentInventory(message)) if message == refusal
        ));
        let backend = graced_backend([Reply::Inventory(Ok(two_volume_image(&image)))]);
        let attachment = AttachedImage {
            image: image.clone(),
            whole_device: "/dev/disk5".into(),
            volume_device: "/dev/disk5s1".into(),
            pin: std::sync::Mutex::new(None),
        };
        assert!(matches!(
            backend.unmount_verified(&attachment, DetachIntent::Release),
            Err(ApfsError::InvalidAttachmentInventory(message)) if message == refusal
        ));
        assert!(backend.runner().requests().is_empty());
    }

    /// A release view refuses what it cannot name exactly: a formatted attachment with no
    /// volume, a volume whose container is not reported, and volumes in two containers. An
    /// unformatted image is its whole device with no volume to unmount.
    #[test]
    fn a_release_view_refuses_what_it_cannot_name_and_takes_an_unformatted_image() {
        let image = temp_path("release-view-refusals", IMAGE_EXTENSION);
        let cases: [(&[(&str, &str)], &str); 3] = [
            (
                &[("/dev/disk4", ""), ("/dev/disk5", APFS_CONTAINER_CONTENT)],
                "expected an APFS volume, reported none",
            ),
            (
                &[("/dev/disk4", ""), ("/dev/disk5s1", APFS_VOLUME_CONTENT)],
                "APFS volume /dev/disk5s1 was reported without its container /dev/disk5",
            ),
            (
                &[
                    ("/dev/disk4", ""),
                    ("/dev/disk5", APFS_CONTAINER_CONTENT),
                    ("/dev/disk5s1", APFS_VOLUME_CONTENT),
                    ("/dev/disk6", APFS_CONTAINER_CONTENT),
                    ("/dev/disk6s1", APFS_VOLUME_CONTENT),
                ],
                r#"APFS volumes ["/dev/disk5s1", "/dev/disk6s1"] are in more than one container"#,
            ),
        ];
        for (media, refusal) in cases {
            let images = [AttachedDiskImage::file(&image, None, media.iter().copied())];
            assert!(
                matches!(release_views_of(&image, &images),
                    Err(ApfsError::InvalidAttachmentInventory(message)) if message == refusal),
                "{media:?}"
            );
        }
        let unformatted = [AttachedDiskImage::file(&image, None, [("/dev/disk4", "")])];
        let views = release_views_of(&image, &unformatted).unwrap();
        let view = views
            .into_iter()
            .next()
            .expect("one attachment is one release view");
        assert_eq!(view.whole_device(), "/dev/disk4");
        assert_eq!(view.release_volumes().count(), 0);
    }

    #[test]
    fn physical_image_device_is_neither_its_synthesized_container_nor_a_slice() {
        let entities = [
            ("/dev/disk42".to_owned(), "Apple_APFS_Container".to_owned()),
            ("/dev/disk42s1".to_owned(), "Apple_APFS_Volume".to_owned()),
            ("/dev/disk7".to_owned(), String::new()),
        ];
        assert_eq!(physical_image_device(&entities).unwrap(), "/dev/disk7");
        assert_eq!(
            attached_apfs_volume(&entities).unwrap(),
            ("/dev/disk7".into(), "/dev/disk42s1".into())
        );
        assert_eq!(attached_apfs_volumes(&entities).unwrap().0, "/dev/disk7");
        assert!(physical_image_device(&entities[..2]).is_err());
        let mut ambiguous = entities.to_vec();
        ambiguous.push(("/dev/disk8".into(), "GUID_partition_scheme".into()));
        assert!(physical_image_device(&ambiguous).is_err());
    }

    #[test]
    fn a_never_mounted_release_detaches_the_physical_image_device() {
        let image = temp_path("physical-release", IMAGE_EXTENSION);
        let inventory = || {
            Reply::Inventory(Ok(vec![AttachedDiskImage::file(
                &image,
                None,
                [
                    ("/dev/disk7", ""),
                    ("/dev/disk42", APFS_CONTAINER_CONTENT),
                    ("/dev/disk42s1", APFS_VOLUME_CONTENT),
                ],
            )]))
        };
        let backend = graced_backend([
            inventory(),
            inventory(),
            inventory(),
            inventory(),
            ok([]),
            no_images(),
        ]);
        backend
            .runner()
            .volume_mounts
            .borrow_mut()
            .insert("/dev/disk42s1".into(), false);
        backend
            .release_image(&image, DetachIntent::Release)
            .unwrap();
        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(HDIUTIL));
        assert_eq!(argv(&requests[0]), ["detach", "-verbose", "/dev/disk7"]);
    }
}
