//! macOS APFS disk-image substrate.
//!
//! Every external operation crosses [`CommandRunner`]. Commands are represented
//! as an executable plus an argument vector; this module never invokes a shell.

use crate::fork_lock::{Fenced, Spawn as _};
use crate::host_load::HostLoad;
use crate::metadata::{IMAGE_EXTENSION, ImageCapacity, is_image_path};
pub use crate::process::{CommandOutput, ProcessStatus};
use crate::process::{fmt_command, fmt_command_failure, fmt_command_spawn};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::attach_budget;
use crate::device::{DISKUTIL, container_of, identifier_depth};

/// The disk-image driver's detach interface. Creation and attachment of RAW images use
/// `diskutil image`; releasing an owned image goes directly through `hdiutil detach`. Which
/// images are attached is read from the kernel ([`CommandRunner::attached_disk_images`]), never
/// from `hdiutil info`.
pub(crate) const HDIUTIL: &str = "/usr/bin/hdiutil";
const FSCK_APFS: &str = "/sbin/fsck_apfs";
/// The kernel mount helper. Workspace volumes are mounted with it rather than `diskutil mount`
/// because `diskutil` routes the mount through Disk Arbitration, which serialises every client
/// on the host: under a loaded fleet a mount that `mount_apfs` completes in about a second was
/// measured queueing for 68 s at the median and past the 120 s child deadline at the tail.
/// Disk Arbitration still observes the mounted volume, so eject and inventory keep working.
pub(crate) const MOUNT_APFS: &str = "/sbin/mount_apfs";
pub(crate) const UMOUNT: &str = "/sbin/umount";
pub(crate) const NEWFS_APFS: &str =
    "/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs";

/// Unix `st_blocks` units: 512 bytes on Darwin. GC allocated-byte accounting reads them.
pub(crate) const SECTOR_BYTES: u64 = 512;

/// `diskutil apfs resizeContainer <ref> 0` asks for grow-to-fit, and macOS reports "there is
/// nothing to grow into" as a failure rather than a no-op: -69743 when the computed size equals
/// the current one, -69519 when no gap follows the physical store. Both say the container
/// already spans the whole image, which is exactly the requested post-condition, so both are
/// accepted. Every other failure propagates, and the caller still reads the resulting capacity
/// back out of the attachment inventory.
const CONTAINER_ALREADY_SPANS_IMAGE: [&str; 2] = ["Error: -69743:", "Error: -69519:"];

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountAccess {
    ReadWrite,
    ReadOnly,
}

impl MountAccess {
    pub(crate) fn from_kernel_flags(flags: u64) -> Self {
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
    /// The child was still running at this deadline and was killed. It *ran*: a hung child is
    /// never reported as one that could not be started.
    Deadline(Duration),
}

#[derive(Debug)]
pub struct CommandRunError {
    pub request: CommandRequest,
    pub failure: CommandRunFailure,
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
            CommandRunFailure::Deadline(deadline) => {
                fmt_command(f, program, args)?;
                write!(
                    f,
                    " did not finish within {deadline:?}; the child was killed and its item \
                     deferred for the next pass"
                )
            }
        }
    }
}

impl std::error::Error for CommandRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.failure {
            CommandRunFailure::Spawn(source) | CommandRunFailure::Wait(source) => Some(source),
            CommandRunFailure::Deadline(_) => None,
        }
    }
}

pub trait CommandRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError>;
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
    /// Every disk image the kernel has attached right now, read from the I/O Registry.
    ///
    /// This is the one attachment inventory, and its absences are authoritative. `hdiutil info
    /// -plist` is not: while any other image attaches or detaches it answers a truncated image
    /// list with nothing marking it, and reading that omission as "not attached" skipped a
    /// release that never happened and lost a mounted workspace's identity on restart. The
    /// kernel's matching snapshot is taken under the registry's own consistency check.
    fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>>;
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
/// A disk-image command (`diskutil image …`, `hdiutil`) is the exception: killed mid-operation,
/// its device can stay leaked in the kernel until reboot (01_storage.md, "How the APFS host
/// degrades" §2). For it this deadline is a warning point: it is waited for, still holding its
/// leases, and says what it is waiting on every [`DISK_IMAGE_WARNING_INTERVAL`], until
/// [`DISK_IMAGE_HARD_DEADLINE`] kills it and records a probable leaked device in the attach-pool
/// record ([`crate::attach_budget`]).
///
/// 120 seconds is generous against normally sub-second disk children and stays inside the
/// enclosing per-operation budgets even when a pass pays it once per workspace; tests
/// override it through [`SystemCommandRunner::run_with_deadline`].
pub const DISK_CHILD_DEADLINE: Duration = Duration::from_secs(120);

/// How often a disk-image command past [`DISK_CHILD_DEADLINE`] says what it is still waiting on.
pub const DISK_IMAGE_WARNING_INTERVAL: Duration = Duration::from_secs(30);

/// When a disk-image command is killed after all: past this a hung verb would wedge the gateway
/// and its leases indefinitely, which costs more than the device its kill probably leaks.
pub const DISK_IMAGE_HARD_DEADLINE: Duration = Duration::from_secs(15 * 60);

/// When a running disk child is warned about and when it is killed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadlinePolicy {
    /// Every child is killed here, except a disk-image command, which is warned about.
    pub deadline: Duration,
    pub warn_every: Duration,
    /// Where every child is killed.
    pub hard: Duration,
}

impl DeadlinePolicy {
    /// `request`'s policy at `deadline`: a disk-image command waits up to
    /// [`DISK_IMAGE_HARD_DEADLINE`]; every other disk child is killed at `deadline`.
    pub fn for_request(request: &CommandRequest, deadline: Duration) -> Self {
        Self {
            deadline,
            warn_every: DISK_IMAGE_WARNING_INTERVAL,
            hard: if is_disk_image_command(request) {
                DISK_IMAGE_HARD_DEADLINE.max(deadline)
            } else {
                deadline
            },
        }
    }

    /// What the waiter does `elapsed` into the child's run, having warned `warned` times.
    fn step(&self, elapsed: Duration, warned: u32) -> DeadlineStep {
        if elapsed >= self.hard {
            DeadlineStep::Kill
        } else if elapsed >= self.deadline + self.warn_every * warned {
            DeadlineStep::Warn
        } else {
            DeadlineStep::Wait
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeadlineStep {
    Wait,
    Warn,
    Kill,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCommandRunner;

impl SystemCommandRunner {
    /// Spawn `request` and collect its output under [`DeadlinePolicy::for_request`] at
    /// `deadline`. [`CommandRunner::run`] is this with [`DISK_CHILD_DEADLINE`]; tests pass a
    /// short deadline to prove a hung child is reaped promptly.
    ///
    /// A disk tool runs under the host disk-lifecycle lease of its class ([`crate::disk_lease`]):
    /// the deadline starts once the lease is granted, and the lease is released when the child
    /// has been reaped.
    pub fn run_with_deadline(
        &self,
        request: &CommandRequest,
        deadline: Duration,
    ) -> Result<CommandOutput, CommandRunError> {
        let policy = DeadlinePolicy::for_request(request, deadline);
        let output = crate::disk_lease::leased(&request.program, &request.args, || {
            Self::run_unleased(
                request,
                policy,
                &mut |elapsed| warn_outlived(request, policy, elapsed),
                &mut || record_killed(request),
            )
        })?;
        record_pool_outcome(request, &output);
        Ok(output)
    }

    /// Run `request` under `policy`, calling `warn` each time it is due and `killed` after a
    /// kill.
    fn run_unleased(
        request: &CommandRequest,
        policy: DeadlinePolicy,
        warn: &mut dyn FnMut(Duration),
        killed: &mut dyn FnMut(),
    ) -> Result<CommandOutput, CommandRunError> {
        use std::io::Read;
        let mut child = spawn_disk_child(request).map_err(|source| CommandRunError {
            request: request.clone(),
            failure: CommandRunFailure::Spawn(source),
        })?;
        // Drain both pipes on dedicated threads while the deadline poll runs, exactly as
        // `Command::output` drains while it waits: a verbose child (fsck) must never wedge
        // on a full pipe just because the parent is polling instead of reading. A child
        // killed at the deadline drops its pipes, which ends both pumps.
        let stdout = std::thread::spawn({
            let mut pipe = child.stdout.take();
            move || {
                let mut bytes = Vec::new();
                if let Some(pipe) = pipe.as_mut() {
                    let _ = pipe.read_to_end(&mut bytes);
                }
                bytes
            }
        });
        let stderr = std::thread::spawn({
            let mut pipe = child.stderr.take();
            move || {
                let mut bytes = Vec::new();
                if let Some(pipe) = pipe.as_mut() {
                    let _ = pipe.read_to_end(&mut bytes);
                }
                bytes
            }
        });
        let started = std::time::Instant::now();
        let mut warned = 0;
        loop {
            match child.try_wait().map_err(|source| CommandRunError {
                request: request.clone(),
                failure: CommandRunFailure::Wait(source),
            })? {
                Some(status) => {
                    let output = std::process::Output {
                        status,
                        stdout: stdout.join().unwrap_or_default(),
                        stderr: stderr.join().unwrap_or_default(),
                    };
                    return Ok(CommandOutput::from(output));
                }
                None => match policy.step(started.elapsed(), warned) {
                    DeadlineStep::Wait => std::thread::sleep(Duration::from_millis(25)),
                    DeadlineStep::Warn => {
                        warned += 1;
                        warn(started.elapsed());
                    }
                    DeadlineStep::Kill => {
                        kill_disk_child_group(&mut child);
                        let _ = child.wait();
                        let _ = stdout.join();
                        let _ = stderr.join();
                        if let Some(image) = deferred_image_target(request) {
                            record_deferred_image(image);
                        }
                        killed();
                        return Err(CommandRunError {
                            request: request.clone(),
                            failure: CommandRunFailure::Deadline(policy.hard),
                        });
                    }
                },
            }
        }
    }
}

/// Say that a disk-image command outlived its deadline and is still waited for.
fn warn_outlived(request: &CommandRequest, policy: DeadlinePolicy, elapsed: Duration) {
    let target = request
        .args
        .last()
        .map_or_else(String::new, |target| target.to_string_lossy().into_owned());
    eprintln!(
        "cowshed: warning: {} on {target} is still running after {}s, past its {}s deadline; \
         a disk-image command killed mid-operation can leak its device until reboot, so it is \
         waited for, holding its leases, until {}s",
        DiskImageVerb(request),
        elapsed.as_secs(),
        policy.deadline.as_secs(),
        policy.hard.as_secs()
    );
}

/// The program and verb a disk-image command names: `diskutil image attach`, `hdiutil detach`.
struct DiskImageVerb<'a>(&'a CommandRequest);

impl fmt::Display for DiskImageVerb<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let request = self.0;
        let words = if request.program == Path::new(DISKUTIL) {
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

/// Record a disk-image command killed at its hard deadline as a probable leaked device.
fn record_killed(request: &CommandRequest) {
    if !is_disk_image_command(request) {
        return;
    }
    if let Err(error) = attach_budget::record_killed() {
        eprintln!(
            "cowshed: warning: a disk-image command was killed at its hard deadline, and \
             recording the probable leaked device failed: {error}"
        );
    }
}

/// Tell the attach-pool record what the host answered a disk-image command: a failure with
/// error code 150 spends the boot, a successful attach ends a spent boot. A failed record is
/// said aloud; the command's own outcome still reaches the caller.
fn record_pool_outcome(request: &CommandRequest, output: &CommandOutput) {
    if !is_disk_image_command(request) {
        return;
    }
    let recorded = if !output.succeeded() && attach_budget::reports_pool_exhausted(&output.stderr) {
        attach_budget::record_exhaustion()
    } else if output.succeeded() && is_image_attach(request) {
        attach_budget::record_attach()
    } else {
        return;
    };
    if let Err(error) = recorded {
        eprintln!(
            "cowshed: warning: could not record the disk-image attach pool's answer to {}: \
             {error}",
            request.program.display()
        );
    }
}

impl CommandRunner for SystemCommandRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        self.run_with_deadline(request, DISK_CHILD_DEADLINE)
    }

    fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
        let lease = image_lease_path(identity)?;
        let file = open_image_lease(&lease)?;
        acquire_image_lease(file, &lease, identity, DISK_CHILD_DEADLINE).map(Some)
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
    use crate::metadata::ImageCapacity;
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::unix::fs::OpenOptionsExt;
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

    /// The nonblocking exclusive open refuses any attached image and keeps attachment excluded
    /// until the larger file length is power-loss durable. Extending a file preserves its holes.
    pub(super) fn grow(image: &Path, capacity: ImageCapacity) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXLOCK | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(image)?;
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
        crate::fsio::Durability::PowerLoss.sync_file(&file)
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
    let digest = crate::api::dto::Sha256Digest::compute(identity.as_os_str().as_encoded_bytes());
    Ok(directory.join(format!("{}.lock", digest.to_hex())))
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

/// [`open_private_file`] at `path`, held under an exclusive `flock` until the answer is dropped.
/// Fenced like the image leases, so no child forked meanwhile keeps the lock.
pub(crate) fn locked_private_file(path: &Path) -> io::Result<Fenced<File>> {
    let file = Fenced::new(open_private_file(path, "lock file")?);
    lock_file(&file, libc::LOCK_EX)?;
    Ok(file)
}

/// Lock `file`, the lease at `lease` for `identity`, within `deadline`.
///
/// An uncontended lease is taken without blocking. A contended one is waited for on a helper
/// thread that hands the locked descriptor back the moment the holder releases it; nothing
/// polls. At the deadline the wait fails naming the lease, the image and the holder's recorded
/// pid. The helper keeps its descriptor, so the lock it takes after a timed-out caller left is
/// released again as soon as it is taken.
fn acquire_image_lease(
    file: Fenced<File>,
    lease: &Path,
    identity: &Path,
    deadline: Duration,
) -> io::Result<Fenced<File>> {
    match lock_file(&file, libc::LOCK_EX | libc::LOCK_NB) {
        Ok(()) => return record_lease_holder(file),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }
    let (locked, waiter) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("cowshed-apfs-image-lease".into())
        .spawn(move || {
            // A caller that timed out has dropped `waiter`: the failed send returns the
            // descriptor inside its error, and dropping that releases the lock just taken.
            let _ = locked.send(lock_file(&file, libc::LOCK_EX).map(|()| file));
        })?;
    match waiter.recv_timeout(deadline) {
        Ok(result) => record_lease_holder(result?),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "APFS image lease {} for {} is still held after {deadline:?} by {}",
                lease.display(),
                identity.display(),
                lease_holder(lease)
            ),
        )),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(io::Error::other(format!(
            "APFS image lease waiter for {} exited without an answer",
            identity.display()
        ))),
    }
}

fn lock_file(file: &File, operation: libc::c_int) -> io::Result<()> {
    loop {
        // SAFETY: `file` owns this descriptor for the whole call; flock does not consume it.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
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

/// Spawn a disk child detached into its own process group on Unix, so the deadline kill
/// reaps the whole helper tree: a disk-image tool can fork helpers (`hdiutil` forks
/// `diskimages-helper`), and killing only the direct child would leave a helper holding the wedge.
fn spawn_disk_child(request: &CommandRequest) -> io::Result<std::process::Child> {
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: `setsid` is async-signal-safe and runs in the forked child before
        // exec; the child becomes its own group leader. Fail closed: a child that
        // cannot leave the parent group must never be addressed by group id, so a
        // setsid failure aborts the spawn instead of risking the parent group.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    command.spawn_locked()
}

/// Kill a disk child and, on Unix, its whole process group.
fn kill_disk_child_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        // A negative pid addresses the group, which the child leads after setsid, so
        // this reaches `diskimages-helper` too. Fail-open is safe here: the direct kill
        // below still runs, and a group that is already gone reports ESRCH, not damage.
        // SAFETY: `kill` with SIGKILL takes no callbacks and touches no Rust state.
        unsafe {
            libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
        }
    }
    let _ = child.kill();
}

/// Images whose disk child was killed at its deadline ([`DISK_CHILD_DEADLINE`], or
/// [`DISK_IMAGE_HARD_DEADLINE`] for a disk-image command), oldest first.
///
/// The daemon startup pass drains this at the start of each pass (see
/// [`take_deferred_images`]) and heals the drained images before the rest, so a
/// deferred workspace is retried first rather than in inventory order.
static DEFERRED_IMAGES: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// Cap on retained deferrals, so passes that never drain cannot grow memory without
/// limit; entries past the cap evict the oldest first.
const DEFERRED_IMAGES_CAP: usize = 1024;

/// Record `path` for first-in-next-pass retry. Duplicate records collapse: one image
/// is one early retry no matter how many of its children hit the deadline.
pub(crate) fn record_deferred_image(path: PathBuf) {
    let mut deferred = DEFERRED_IMAGES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if !deferred.contains(&path) {
        if deferred.len() >= DEFERRED_IMAGES_CAP {
            deferred.remove(0);
        }
        deferred.push(path);
    }
}

/// Drain the images deferred since the last pass, oldest first. The startup pass calls
/// this once per pass and heals the drained images before the rest.
pub(crate) fn take_deferred_images() -> Vec<PathBuf> {
    std::mem::take(
        &mut *DEFERRED_IMAGES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()),
    )
}

/// The image a disk child was working, when the argv names one.
///
/// Attach/detach argv carries its target last (the same convention the bootstrap mount
/// path relies on); only an image path — not a `/dev/diskN` device — is a deferral key
/// the startup pass can map back to a workspace.
fn deferred_image_target(request: &CommandRequest) -> Option<PathBuf> {
    if request.program != Path::new(DISKUTIL) {
        return None;
    }
    let path = PathBuf::from(request.args.last()?);
    is_image_path(&path).then_some(path)
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

/// One backend step inside a [`crate::timing`] span labelled `apfs <leg>/<step>`, so a slow
/// lifecycle verb names the storage step that spent the time.
pub(crate) fn timed_apfs_step<T, E: std::fmt::Display>(
    leg: &str,
    step: &'static str,
    operation: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    crate::timing::timed("apfs", format_args!("{leg}/{step}"), operation)
}

/// The leg a backend step serves, derived from the image path it touches: staging state lives
/// under `.staging` (the same namespace the storage layer calls STAGING_NAMESPACE; this module
/// cannot name it without a dependency cycle, so the literal is matched here), everything else
/// is canonical. No signature changes — the trait seams stay intact.
pub(crate) fn apfs_step_leg(image: &Path) -> &'static str {
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
/// live workspace out from under whoever is in it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetachIntent {
    Release,
    WhenIdle,
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
    /// An attachment verified but not yet mounted holds its raw IOMedia open. The kernel
    /// refuses even forced ejects until mount finishes; no second process can reuse diskN
    /// between the two backend method calls. Existing mounted attachments have no pin.
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

/// A path named by a validated explicit re-adoption record, not another runtime image format.
/// Ordinary operations still accept only RAW. This token enables identity inspection and
/// read-only retained attachment, never image creation or growth.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetainedInput<'a> {
    path: &'a Path,
}

#[cfg(any(target_os = "macos", test))]
impl<'a> RetainedInput<'a> {
    pub(crate) fn from_record(
        source: &'a crate::storage::recovery::ReAdoptSource,
        path: &'a Path,
    ) -> crate::Result<Self> {
        source.validate()?;
        let named = path == source.image.original.as_path()
            || path == source.image.retained.as_path()
            || source.build.as_ref().is_some_and(|build| {
                path == build.image.original.as_path() || path == build.image.retained.as_path()
            })
            || source
                .old_seeds
                .iter()
                .any(|seed| path == seed.original.as_path() || path == seed.retained.as_path());
        if !named {
            return Err(crate::CowshedError::integrity(
                format!(
                    "{} is not an input named by this re-adoption record",
                    path.display()
                ),
                "keep the source images and repair the replacement record",
            ));
        }
        Ok(Self { path })
    }

    pub(crate) fn path(&self) -> &'a Path {
        self.path
    }
}

/// Retained inputs expose no writable-mount capability. Only this module can obtain the inner
/// attachment; callers use its read-only mount and non-forced release methods.
#[cfg(any(target_os = "macos", test))]
#[derive(Debug)]
pub(crate) struct RetainedAttachment(AttachedImage);

#[cfg(any(target_os = "macos", test))]
impl RetainedAttachment {
    pub(crate) fn volume_device(&self) -> &str {
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
pub(crate) enum RecoveredImageAttachment {
    Unformatted {
        image: PathBuf,
        whole_device: String,
    },
    Apfs(AttachedImage),
}

impl RecoveredImageAttachment {
    pub(crate) fn whole_device(&self) -> &str {
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
}

impl AttachedDiskImage {
    /// A file-backed attachment exposing `media` as `(device, content hint)` pairs.
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
    /// A disk-image command the host refused with error code 150 because its attach pool is
    /// spent (attach_budget.rs): a host finding that only a reboot clears, not the request's
    /// failure. Boxed like the helper failure.
    AttachPoolExhausted(Box<AttachPoolFailure>),
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
    /// A clone needed its mounted source detached, and the kernel refused the unforced release:
    /// something still has the volume at `mount_point` open. Nothing is forced; `holders` names
    /// each process of this user that had it open when it was refused (empty when none of them
    /// is visible: the kernel or another user's process holds it), and `refusal` is the refused
    /// unmount or detach itself.
    CaptureSourceBusy {
        image: PathBuf,
        mount_point: PathBuf,
        holders: Vec<crate::build_volume::nx::Holder>,
        refusal: Box<ApfsError>,
    },
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

/// The command the spent attach pool refused.
#[derive(Debug)]
pub struct AttachPoolFailure {
    pub operation: &'static str,
    pub request: CommandRequest,
    pub output: CommandOutput,
}

impl fmt::Display for ApfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidImagePath(path) => {
                write!(f, "{} is not a .{IMAGE_EXTENSION} image", path.display())
            }
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
            Self::AttachPoolExhausted(failure) => {
                f.write_str(
                    "AttachPoolExhausted: the host's disk-image attach pool is spent (error code \
                     150, kIOReturnNoResources); ",
                )?;
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
                holders,
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
        }
    }
}

impl std::error::Error for ApfsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CommandRun(error) => Some(error),
            Self::KernelInventory(source) => Some(source),
            Self::FileOperation { source, .. } => Some(source),
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

/// Source images may be captured live; immutable templates and build seeds are captured only
/// while their source inode is held against attachment (specs/cowshed/16_build_volumes.md).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageCapture {
    Live,
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
    /// time then says is current. Live source captures may follow the flush; build-state captures
    /// still require detached writer exclusion.
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
    /// Rename `attachment`'s volume through its device, never through a path: `diskutil
    /// renameVolume` handed a path renames whatever volume holds it, and a path nothing is
    /// mounted at lies on the host's own volume. Under the image's lease and a raw pin, the device
    /// is proven still to be that image's before the rename runs, as an unmount proves it.
    fn rename_volume(&self, attachment: &AttachedImage, volume_name: &str)
    -> Result<(), ApfsError>;
    fn attach_verified(&self, image: &Path) -> Result<AttachedImage, ApfsError>;
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
    /// Hold `image`'s lease ([`CommandRunner::image_lease`]), keyed by the identity the
    /// attachment inventory records for it.
    fn image_lease(&self, image: &Path) -> Result<Option<Fenced<File>>, ApfsError> {
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
        let actual = image_attachments(&image, &self.attached_disk_images()?)?;
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
        let views = release_views_of(&image, &self.attached_disk_images()?)?;
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
    /// The raw IOMedia pin freezes the device identity while native unmount bypasses DA.
    pub(crate) fn unmount_verified(
        &self,
        attachment: &AttachedImage,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        self.unmount_reporting(attachment, intent).map(|_| ())
    }

    /// [`Self::unmount_verified`] with `Release`, answering whether the grace ran out and the
    /// unmount was forced past whatever still held the volume.
    pub(crate) fn unmount_within_grace(
        &self,
        attachment: &AttachedImage,
    ) -> Result<Unmounted, ApfsError> {
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

    /// `umount` the attachment's volume, the caller holding its image lease and pin. A volume
    /// something still holds is waited on for the detach grace and then, under
    /// [`DetachIntent::Release`], forced; [`DetachIntent::WhenIdle`] answers the first dissent.
    fn unmount_volume_unlocked(
        &self,
        attachment: &AttachedImage,
        intent: DetachIntent,
        operation: &'static str,
    ) -> Result<Unmounted, ApfsError> {
        let mut waited = Duration::ZERO;
        let mut started = None;
        let mut force = false;
        loop {
            let mut args = Vec::with_capacity(1 + usize::from(force));
            if force {
                args.push(OsString::from("-f"));
            }
            args.push(OsString::from(&attachment.volume_device));
            let result = timed_apfs_step(apfs_step_leg(&attachment.image), "unmount", || {
                self.run_checked(operation, CommandRequest::new(UMOUNT, args))
            });
            match result {
                Ok(_) if force => return Ok(Unmounted::Forced),
                Ok(_) => return Ok(Unmounted::Unforced),
                Err(error) if !force && detach_was_dissented(&error) => {
                    if intent == DetachIntent::WhenIdle {
                        return Err(error);
                    }
                    let started = started.get_or_insert_with(std::time::Instant::now);
                    if self.grace.expired(waited, self.sleeper.elapsed(*started)) {
                        force = true;
                    } else {
                        self.sleeper.sleep(self.grace.poll);
                        waited = waited.saturating_add(self.grace.poll);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Read the attachment inventory for this exact path without constraining its image format.
    /// Mutation entry points still validate the format before changing an image.
    pub(crate) fn attached_whole_devices(
        &self,
        image: &Path,
    ) -> Result<BTreeSet<String>, ApfsError> {
        Ok(self.attachment_inventory(image)?.devices)
    }

    fn attachment_inventory(&self, image: &Path) -> Result<AttachmentInventory, ApfsError> {
        let image = attachment_inventory_path(image)?;
        image_inventory(&image, &self.attached_disk_images()?)
    }

    /// The kernel's disk-image inventory: Tahoe's `diskutil image info --plist` exposes no
    /// attachment devices, and `hdiutil info -plist` truncates its list while other images
    /// attach or detach, so only the I/O Registry answers both presence and absence.
    fn attached_disk_images(&self) -> Result<Vec<AttachedDiskImage>, ApfsError> {
        crate::timing::timed(
            "apfs-inventory",
            format_args!("attached disk images"),
            || {
                self.runner
                    .attached_disk_images()
                    .map_err(ApfsError::KernelInventory)
            },
        )
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
        crate::timing::timed("apfs-command", format_args!("{operation}"), || {
            let output = self.runner.run(&request)?;
            if output.succeeded() {
                Ok(output)
            } else {
                Err(command_failed(operation, request, output))
            }
        })
    }

    /// Opt the read-write volume just mounted at `mount_point` out of fseventsd's persistent
    /// event log (01_storage.md, "The event log"): an empty `.fseventsd/no_log` at its root.
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
                eprintln!(
                    "cowshed: apfs {} has a {EVENT_LOG_DIRECTORY} that is not a directory, or a \
                     {EVENT_LOG_OPT_OUT} that is not an empty regular file; left as found, not \
                     written through",
                    mount_point.display()
                );
                Ok(())
            }
            Err(error) => {
                eprintln!(
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
            eprintln!(
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
                        eprintln!(
                            "cowshed: apfs {} keeps fseventsd's inherited event log: {replaced}",
                            mount_point.display()
                        );
                        Err(unmounted)
                    }
                }
            });
        if let Err(error) = &retired {
            eprintln!(
                "cowshed: apfs {} keeps fseventsd's inherited event log: {error}",
                mount_point.display()
            );
        }
        self.run_checked(
            "mount verified APFS volume",
            mount_request(attachment, options, mount_point),
        )?;
        if retired.is_ok() {
            eprintln!(
                "cowshed: apfs {} retired fseventsd's inherited event log",
                mount_point.display()
            );
        }
        Ok(())
    }

    /// Create a blank sparse RAW image, attach it without mounting, format its whole device as one
    /// case-sensitive APFS volume (`newfs_apfs -e`) owned by the invoking user (`-U`/`-G`), verify
    /// it and detach it: the blank template mint's sequence (01_storage.md), so no caller detaches
    /// a volume the template path would have checked first. Nothing here runs as root.
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

    fn create_blank_unlocked(
        &self,
        path: &Path,
        request: &CreateImageRequest,
    ) -> Result<(), ApfsError> {
        let create = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("image"),
                OsString::from("create"),
                OsString::from("blank"),
                OsString::from("--format"),
                OsString::from("RAW"),
                OsString::from("--size"),
                OsString::from(capacity_argument(request.capacity)),
                OsString::from("--volumeName"),
                OsString::from(&request.volume_name),
                OsString::from("--fs"),
                OsString::from("None"),
                path.as_os_str().to_owned(),
            ],
        );
        timed_apfs_step(apfs_step_leg(path), "blank-create", || {
            self.run_checked("create sparse RAW image", create)
        })
        .map(|_| ())
    }

    /// Attach the blank image at `path` and format its whole device. Returns that whole device,
    /// still attached; every failure releases what this attach added and removes the image.
    fn attach_and_format_unlocked(
        &self,
        path: &Path,
        request: &CreateImageRequest,
    ) -> Result<String, ApfsError> {
        let attached_before = self.refuse_second_attach(path)?;

        let attach = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("image"),
                OsString::from("attach"),
                OsString::from("--nobrowse"),
                OsString::from("--noMount"),
                OsString::from("--plist"),
                path.as_os_str().to_owned(),
            ],
        );
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
    pub(crate) fn existing_attachment(
        &self,
        image: &Path,
    ) -> Result<Option<AttachedImage>, ApfsError> {
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

    pub(crate) fn recovered_image_attachment(
        &self,
        image: &Path,
    ) -> Result<Option<RecoveredImageAttachment>, ApfsError> {
        validate_image_path(image)?;
        let image = attachment_inventory_path(image)?;
        existing_image_attachment(&image, &self.attached_disk_images()?)
    }

    /// Every driver the kernel holds for `image`, however many, each as its whole device and
    /// every volume its container holds: what a release has to undo.
    pub(crate) fn release_views(&self, image: &Path) -> Result<Vec<ReleaseView>, ApfsError> {
        validate_image_path(image)?;
        let image = attachment_inventory_path(image)?;
        release_views_of(&image, &self.attached_disk_images()?)
    }

    /// The backing file of every attached disk image, each once, by the path its attach was
    /// handed. A file deleted since is still listed: the kernel keeps the path it registered.
    pub(crate) fn attached_image_files(&self) -> Result<BTreeSet<PathBuf>, ApfsError> {
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
    /// released. One attachment is one detach: an APFS attachment shows two whole devices (the
    /// image's and its synthesized container), and detaching one takes both, with every volume
    /// the container holds. Nothing here unmounts first: a mounted volume's caller unmounts it
    /// ([`Self::unmount_released`]), and an unmounted one, such as a template's staging image,
    /// has nothing to unmount.
    pub(crate) fn release_every_attachment(
        &self,
        image: &Path,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        let _lease = self.image_lease(image)?;
        for view in self.release_views(image)? {
            self.detach_image_device_unlocked(image, view.whole_device(), intent)?;
        }
        Ok(())
    }

    /// Release `image` from every attachment the kernel holds for it: each volume of each
    /// attachment's container whose device is in `mounted` (the kernel's mount sources) is
    /// unmounted through its own pinned identity ([`Self::unmount_released`]), then every whole
    /// device is detached once ([`Self::release_every_attachment`]), taking the unmounted rest.
    pub(crate) fn release_image(
        &self,
        image: &Path,
        mounted: &BTreeSet<String>,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        for view in self.release_views(image)? {
            for volume in view.mounted_volumes(mounted) {
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
        access: MountAccess,
        cleanup: DetachIntent,
    ) -> Result<AttachedImage, ApfsError> {
        let attached_before = self.refuse_second_attach(image)?;
        let request = CommandRequest::new(
            DISKUTIL,
            [OsString::from("image"), OsString::from("attach")]
                .into_iter()
                .chain((access == MountAccess::ReadOnly).then(|| OsString::from("--readOnly")))
                .chain([
                    OsString::from("--nobrowse"),
                    OsString::from("--noMount"),
                    OsString::from("--plist"),
                    image.as_os_str().to_owned(),
                ]),
        );
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
        access: MountAccess,
        cleanup: DetachIntent,
    ) -> Result<AttachedImage, ApfsError> {
        let leg = apfs_step_leg(image);
        let (mut attachment, request, output, pin) = {
            let _lease = self.image_lease(image)?;
            let mut retried_lost_device = false;
            loop {
                let attachment = timed_apfs_step(leg, "attach", || {
                    self.attach_without_mounting(image, access, cleanup)
                })?;
                // A raw descriptor pins the actual IOMedia from identity verification
                // through fsck; its ownership is handed to the attachment until mount.
                let pin = match self.pin_attached_volume(&attachment) {
                    Ok(pin) => pin,
                    Err(error)
                        if !retried_lost_device
                            && matches!(
                                &error,
                                ApfsError::InvalidAttachmentInventory(_)
                                    | ApfsError::FileOperation {
                                        operation: "pin attached APFS volume against device reuse",
                                        ..
                                    }
                            )
                            && self.attached_whole_devices(image)?.is_empty() =>
                    {
                        retried_lost_device = true;
                        eprintln!(
                            "cowshed: apfs {} lost its reported device before verification ({error}); attaching once more",
                            image.display()
                        );
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let request = CommandRequest::new(
                    FSCK_APFS,
                    [
                        OsString::from("-q"),
                        OsString::from(raw_device_from(&attachment.volume_device)),
                    ],
                );
                let output = timed_apfs_step(leg, "fsck", || self.runner.run(&request.clone()))?;
                break (attachment, request, output, pin);
            }
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
    pub(crate) fn existing_retained_attachment(
        &self,
        input: &RetainedInput<'_>,
    ) -> Result<Option<RetainedAttachment>, ApfsError> {
        let image = attachment_inventory_path(input.path())?;
        match existing_image_attachment(&image, &self.attached_disk_images()?)? {
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
    pub(crate) fn attach_retained_verified(
        &self,
        input: &RetainedInput<'_>,
    ) -> Result<RetainedAttachment, ApfsError> {
        self.verify_attachment(input.path(), MountAccess::ReadOnly, DetachIntent::WhenIdle)
            .map(RetainedAttachment)
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn mount_retained_read_only(
        &self,
        attachment: &RetainedAttachment,
        mount: &Path,
    ) -> Result<(), ApfsError> {
        self.mount(&attachment.0, mount, MountAccess::ReadOnly, false)
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn unmount_retained_when_idle(
        &self,
        attachment: &RetainedAttachment,
    ) -> Result<(), ApfsError> {
        self.unmount_verified(&attachment.0, DetachIntent::WhenIdle)
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn detach_retained_when_idle(
        &self,
        attachment: &RetainedAttachment,
    ) -> Result<(), ApfsError> {
        self.detach(&attachment.0, DetachIntent::WhenIdle)
    }

    /// Detach an owned whole device under `intent`.
    ///
    /// A `Release` waits out a dissent and then forces, because the volume is cowshed's own and
    /// is finished with; a `WhenIdle` hands the dissent straight back so its verb can refuse.
    /// Only a dissent is retried — an invalid device or a missing tool fails immediately either
    /// way.
    fn detach_device_checked(&self, target: &str, intent: DetachIntent) -> Result<(), ApfsError> {
        if !is_kernel_device_path(target) {
            return Err(ApfsError::InvalidDetachTarget(PathBuf::from(target)));
        }
        let target = OsStr::new(target);
        let mut waited = Duration::ZERO;
        let mut started = None;
        loop {
            match self.detach_once(target, false) {
                Ok(()) => return Ok(()),
                Err(error) if detach_was_dissented(&error) => {
                    if intent == DetachIntent::WhenIdle {
                        return Err(error);
                    }
                    let started = started.get_or_insert_with(std::time::Instant::now);
                    if self.grace.expired(waited, self.sleeper.elapsed(*started)) {
                        return self.detach_once(target, true);
                    }
                    self.sleeper.sleep(self.grace.poll);
                    waited = waited.saturating_add(self.grace.poll);
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// One `hdiutil detach -verbose`. hdiutil's verbose account is the only record of what an
    /// eject went through, so it is kept as an attribute of the detach step whenever the eject
    /// was refused (`dissent=`, DiskArbitration's reason) or slower than the normal floor
    /// (`waited=`, at least [`SLOW_DETACH`]); a fast, accepted eject adds nothing to the trace.
    fn detach_once(&self, target: &OsStr, force: bool) -> Result<(), ApfsError> {
        let mut args = Vec::with_capacity(3 + usize::from(force));
        args.push(OsString::from("detach"));
        if force {
            args.push(OsString::from("-force"));
        }
        args.push(OsString::from("-verbose"));
        args.push(target.to_owned());
        let started = std::time::Instant::now();
        let result = self.run_checked("detach image", CommandRequest::new(HDIUTIL, args));
        let account = match &result {
            Ok(output) => {
                (started.elapsed() >= SLOW_DETACH).then_some(("waited", output.stderr.as_slice()))
            }
            Err(ApfsError::CommandFailed { output, .. }) => {
                Some(("dissent", output.stderr.as_slice()))
            }
            Err(_) => None,
        };
        if let Some((key, stderr)) = account {
            crate::timing::attribute(
                "apfs-command",
                format_args!("detach image"),
                key,
                one_line(stderr),
            );
        }
        result.map(|_| ())
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
        let held = self.attached_whole_devices(image)?;
        if held.is_empty() {
            return Ok(());
        }
        if !held.contains(whole_device) {
            return Err(ApfsError::InvalidAttachmentInventory(format!(
                "{} still holds {held:?}, not {whole_device}; retaining the attached \
                 image rather than claiming it was released",
                image.display()
            )));
        }
        self.detach_device_checked(whole_device, intent)?;
        self.settle_detached_device(image, whole_device);
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
                    eprintln!(
                        "cowshed: apfs detach-settle {whole_device} departed waited={waited:?}"
                    );
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    eprintln!(
                        "cowshed: apfs detach-settle {whole_device} inventory unreadable ({error}); proceeding without confirmation waited={waited:?}"
                    );
                    return;
                }
            }
            if waited >= self.settle.total {
                eprintln!(
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
        volume_name: &str,
    ) -> Result<(), ApfsError> {
        if !is_valid_apfs_volume_name(volume_name) {
            return Err(ApfsError::InvalidVolumeName(volume_name.to_owned()));
        }
        let _lease = self.image_lease(&attachment.image)?;
        let _pin = self.pin_attached_volume(attachment)?;
        self.run_checked(
            "rename APFS volume",
            CommandRequest::new(
                DISKUTIL,
                [
                    OsString::from("renameVolume"),
                    OsString::from(&attachment.volume_device),
                    OsString::from(volume_name),
                ],
            ),
        )
        .map(|_| ())
    }

    fn attach_verified(&self, image: &Path) -> Result<AttachedImage, ApfsError> {
        validate_image_path(image)?;
        self.verify_attachment(image, MountAccess::ReadWrite, DetachIntent::Release)
    }

    fn mount(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsError> {
        let _lease = self.image_lease(&attachment.image)?;
        let mut inherited_pin = attachment
            .pin
            .lock()
            .expect("attachment pin mutex poisoned");
        let _fresh_pin = if inherited_pin.is_some() {
            // The same verified IOMedia handle survived fsck and still prevents eject.
            // A second registry read cannot add identity evidence that the held kernel pin
            // lacks.
            None
        } else {
            self.pin_attached_volume(attachment)?
        };
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
            self.run_checked(
                "mount verified APFS volume",
                mount_request(attachment, options, mount_point),
            )
        });
        let opted_out = match (&mounted, access) {
            (Ok(_), MountAccess::ReadWrite) => {
                timed_apfs_step(apfs_step_leg(&attachment.image), "event-log", || {
                    self.opt_out_of_event_log(attachment, mount_point, options)
                })
                .inspect(|()| report_spotlight_opt_out(mount_point))
            }
            _ => Ok(()),
        };
        inherited_pin.take();
        mounted.and(opted_out)
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
        crate::timing::timed("apfs-command", format_args!("grow APFS container"), || {
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
        attachment_capacity(&image, &self.attached_disk_images()?)
    }
}

/// Every size handed to `diskutil` is a plain byte count: it reads unit letters as decimal SI,
/// while cowshed's capacities are binary, and a byte count is the one spelling that cannot be
/// misread.
fn capacity_argument(capacity: ImageCapacity) -> String {
    capacity.bytes().to_string()
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

/// A failed command, typed by what failed: a disk-image command the spent attach pool refused
/// is the host's finding; one whose framework lost its helper carries the host load at the
/// failure; every other failure is the command's own.
fn command_failed(
    operation: &'static str,
    request: CommandRequest,
    output: CommandOutput,
) -> ApfsError {
    if is_disk_image_command(&request) && attach_budget::reports_pool_exhausted(&output.stderr) {
        ApfsError::AttachPoolExhausted(Box::new(AttachPoolFailure {
            operation,
            request,
            output,
        }))
    } else if (request.program == Path::new(DISKUTIL) || request.program == Path::new(HDIUTIL))
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

/// A DiskImages2 verb: `diskutil image …` or any `hdiutil`.
fn is_disk_image_command(request: &CommandRequest) -> bool {
    request.program == Path::new(HDIUTIL)
        || (request.program == Path::new(DISKUTIL)
            && request.args.first().is_some_and(|verb| verb == "image"))
}

/// `diskutil image attach …` or `hdiutil attach …`.
fn is_image_attach(request: &CommandRequest) -> bool {
    let verb = if request.program == Path::new(HDIUTIL) {
        request.args.first()
    } else {
        request.args.get(1)
    };
    is_disk_image_command(request) && verb.is_some_and(|verb| verb == "attach")
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

/// Whether a failed detach failed because something still holds the volume.
///
/// The image driver reports EBUSY plus its resource-busy diagnostic. Anything else — a bad
/// device, a missing tool, a spawn failure — is not a dissent and must not be waited on.
pub(crate) fn detach_was_dissented(error: &ApfsError) -> bool {
    if let ApfsError::CaptureSourceBusy { refusal, .. } = error {
        return detach_was_dissented(refusal);
    }
    let ApfsError::CommandFailed {
        request, output, ..
    } = error
    else {
        return false;
    };
    contains_bytes(&output.stderr, b"Resource busy")
        && ((request.program == Path::new(HDIUTIL)
            && request.args.first().is_some_and(|arg| arg == "detach")
            && output.status == ProcessStatus::Exit(libc::EBUSY))
            || (request.program == Path::new(UMOUNT) && output.status == ProcessStatus::Exit(1)))
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
/// The marker at a volume's root that keeps Spotlight off it. Measured on macOS 26: a
/// browsable mount of a marked volume reads `mdutil -s` "Indexing and searching disabled"
/// against "Indexing disabled" unmarked; a `nobrowse` mount reads "unknown indexing state"
/// either way. It does not stop the departed-volume leak (01_storage.md, "How the APFS host
/// degrades" §2).
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
const SPOTLIGHT_OPT_OUT_C: &std::ffi::CStr = c".metadata_never_index";
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

/// Plant [`SPOTLIGHT_OPT_OUT`] at the root of the volume mounted at `mount_point`.
fn opt_out_of_spotlight(mount_point: &Path) -> io::Result<Marker> {
    plant_empty_marker(&open_volume_root(mount_point)?, SPOTLIGHT_OPT_OUT_C)
}

/// Keep Spotlight off the read-write volume just mounted at `mount_point` ([`SPOTLIGHT_OPT_OUT`]).
/// Like the event log, indexing is host load and the mount is what the caller asked for, so a
/// failure to opt out is reported and the volume stays mounted.
fn report_spotlight_opt_out(mount_point: &Path) {
    match opt_out_of_spotlight(mount_point) {
        Ok(Marker::Planted) => {}
        Ok(Marker::Refused) => eprintln!(
            "cowshed: apfs {} refuses this user a {SPOTLIGHT_OPT_OUT}; Spotlight may index it",
            mount_point.display()
        ),
        Ok(Marker::Squatted) => eprintln!(
            "cowshed: apfs {} has a {SPOTLIGHT_OPT_OUT} that is not an empty regular file; left \
             as found, not written through",
            mount_point.display()
        ),
        Err(error) => eprintln!(
            "cowshed: apfs {} keeps Spotlight indexing: {error}",
            mount_point.display()
        ),
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

fn attachment_inventory_path(image: &Path) -> Result<PathBuf, ApfsError> {
    std::path::absolute(image).map_err(|source| ApfsError::FileOperation {
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
}

impl ReleaseView {
    pub(crate) fn whole_device(&self) -> &str {
        &self.whole_device
    }

    /// Each volume of the container that `mounted` (the kernel's mount sources) names, as the
    /// release unmounts it. An unmounted volume goes with the container's detach.
    pub(crate) fn mounted_volumes<'a>(
        &'a self,
        mounted: &'a BTreeSet<String>,
    ) -> impl Iterator<Item = ReleasedVolume> + 'a {
        self.volumes
            .iter()
            .filter(|volume| mounted.contains(*volume))
            .map(|volume| {
                ReleasedVolume(AttachedImage {
                    image: self.image.clone(),
                    whole_device: self.whole_device.clone(),
                    volume_device: volume.clone(),
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
            })
        })
        .collect()
}

/// A formatted attachment's container and every APFS volume it reports in it. Refused when it
/// reports no volume, a volume without its container, or volumes in two containers.
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
    Ok((container, volumes))
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

/// The APFS volume an attached image exposes, and the synthesized container it hangs from — the
/// whole device whose detach releases the image — read from the image's own system entities.
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
    Ok((reported_container(entities, volume)?, (*volume).to_owned()))
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
/// also retain the shared lock that excludes an image driver; live source captures retain no
/// attachment lock and leave the source mounted.
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
pub(crate) struct RegisteredApfsContainer {
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
pub(crate) struct RegisteredApfsVolume {
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
pub(crate) use io_registry::registered_apfs_containers;
#[cfg(target_os = "macos")]
pub(crate) use io_registry::{disk_image_class_counts, disk_image_user_client_creators};

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
/// One `IOServiceGetMatchingServices` call answers every IOMedia node the kernel has registered.
/// The kernel collects those matches under the registry's own consistency check — it restarts
/// its walk whenever a concurrent attach or detach invalidates it — so the set is complete for
/// one instant, which is exactly what `hdiutil info` and `diskutil apfs list` are not. For disk
/// images each node is walked up its provider chain to the image driver's
/// `AppleDiskImageDevice`, whose `DiskImageURL` names the backing store; for APFS the same
/// snapshot yields every container's `AppleAPFSMedia` and every `AppleAPFSVolume`, grouped by
/// the media its `AppleAPFSContainer` provider hangs from. A live node's provider links do not
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
    use crate::metadata::ImageCapacity;
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
        fn IOServiceMatching(name: *const c_char) -> *mut c_void;
        fn IOServiceGetMatchingServices(
            main_port: libc::mach_port_t,
            matching: *mut c_void,
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

    struct Keys {
        bsd_name: Cf,
        content: Cf,
        size: Cf,
        url: Cf,
    }

    /// The image driver a media node hangs from, and whether the node is the image's own whole
    /// device: no other media lies between them.
    struct ImageDriver {
        driver: Object,
        id: u64,
        is_whole: bool,
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
        let named = class.to_string_lossy();
        // SAFETY: the class name is NUL-terminated; a null answer is refused below.
        let matching = unsafe { IOServiceMatching(class.as_ptr()) };
        if matching.is_null() {
            return Err(io::Error::other(format!(
                "IOServiceMatching({named}) returned no matching dictionary"
            )));
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
        let keys = Keys {
            bsd_name: key("BSD Name")?,
            content: key("Content")?,
            size: key("Size")?,
            url: key("DiskImageURL")?,
        };
        let mut images: BTreeMap<u64, AttachedDiskImage> = BTreeMap::new();
        for media in registered(c"IOMedia")? {
            settle(record_image_media(&mut images, &media, &keys))?;
        }
        Ok(images
            .into_values()
            .map(|mut image| {
                image
                    .media
                    .sort_by(|left, right| left.device.cmp(&right.device));
                image
            })
            .collect())
    }

    /// Record one IOMedia node under the disk image it hangs from, if any. Every read lands
    /// before anything is recorded, so a node that leaves the registry mid-read leaves no trace.
    fn record_image_media(
        images: &mut BTreeMap<u64, AttachedDiskImage>,
        media: &Object,
        keys: &Keys,
    ) -> Read<()> {
        let Some(name) = string_property(media, &keys.bsd_name, "BSD Name")? else {
            return Ok(());
        };
        let Some(owner) = image_driver_of(media)? else {
            return Ok(());
        };
        let capacity = if owner.is_whole {
            Some(u64_property(media, &keys.size, "Size")?.map(ImageCapacity::from_bytes))
        } else {
            None
        };
        let content = string_property(media, &keys.content, "Content")?.unwrap_or_default();
        let image = match images.entry(owner.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let Some(url) = string_property(&owner.driver, &keys.url, "DiskImageURL")? else {
                    return Err(invalid(format!(
                        "the disk image exposing {name} registers no DiskImageURL"
                    ))
                    .into());
                };
                entry.insert(AttachedDiskImage {
                    source: DiskImageSource::from_url(&url),
                    capacity: None,
                    media: Vec::new(),
                })
            }
        };
        if let Some(capacity) = capacity {
            image.capacity = capacity;
        }
        image.media.push(ImageMedia {
            device: format!("/dev/{name}"),
            content,
        });
        Ok(())
    }

    /// Every APFS container and volume registered at one instant, from the same IOMedia snapshot
    /// [`attached_disk_images`] reads: `AppleAPFSMedia` is a container's synthesized whole disk,
    /// and each `AppleAPFSVolume` hangs from an `AppleAPFSContainer` that hangs from one. A
    /// container whose volumes are all gone, or not yet started, is still reported. A volume
    /// whose provider chain has already left the plane, or whose entries the kernel finalizes
    /// mid-read, is departing and is not; any other read failure fails the whole inventory,
    /// which [`super::project_registered_apfs`] validates.
    pub(crate) fn registered_apfs_containers() -> io::Result<Vec<RegisteredApfsContainer>> {
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

    /// The `IOUserClientCreator` of every `DIDeviceIOUserClient` an attached image's
    /// `AppleDiskImageDevice` holds, `pid N, diskimagesiod`: the helper serving the image's IO.
    /// User clients are never registered, so they are reached as each device's children rather
    /// than matched. A device or client the kernel finalizes mid-walk is departing and names no
    /// helper. A client that names no creator fails the read: its helper would pass for an
    /// orphan.
    pub(crate) fn disk_image_user_client_creators() -> io::Result<Vec<String>> {
        let key = key("IOUserClientCreator")?;
        let mut creators = Vec::new();
        for device in registered(c"AppleDiskImageDevice")? {
            settle(record_user_client_creators(&mut creators, &device, &key))?;
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
            u64::try_from(disk_image_user_client_creators()?.len())
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

    /// The image driver above `media`, if any. A finalized node answers `false` to
    /// [`conforms`], and the walk then asks it for its provider, which reports it gone.
    fn image_driver_of(media: &Object) -> Read<Option<ImageDriver>> {
        let mut intervening_media = 0usize;
        let mut current = provider(media)?;
        while let Some(entry) = current {
            if conforms(&entry, c"AppleDiskImageDevice") {
                let id = entry_id(&entry, "read a disk image's registry id")?;
                return Ok(Some(ImageDriver {
                    driver: entry,
                    id,
                    is_whole: intervening_media == 0,
                }));
            }
            if conforms(&entry, c"IOMedia") {
                intervening_media += 1;
            }
            current = provider(&entry)?;
        }
        Ok(None)
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
        // SAFETY: `value` is a live CF object.
        if unsafe { CFGetTypeID(value.0) != CFStringGetTypeID() } {
            return Err(invalid(format!("registry property {name} is not a string")).into());
        }
        let range = CfRange {
            location: 0,
            // SAFETY: `value` is a live CFString.
            length: unsafe { CFStringGetLength(value.0) },
        };
        let mut needed: CfIndex = 0;
        // SAFETY: a null buffer asks only for the UTF-8 length of the whole range.
        let measured = unsafe {
            CFStringGetBytes(
                value.0,
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
                value.0,
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
            ))
            .into());
        }
        let text = String::from_utf8(bytes)
            .map_err(|_| invalid(format!("registry property {name} is not UTF-8")))?;
        Ok(Some(text))
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
            assert!(matches!(image_driver_of(&entry), Err(Unread::Gone)));
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
                url: key("DiskImageURL").expect("key"),
            };
            assert!(matches!(
                string_property(&entry, &keys.url, "DiskImageURL"),
                Err(Unread::Gone)
            ));
            assert!(matches!(
                u64_property(&entry, &keys.size, "Size"),
                Err(Unread::Gone)
            ));
            let mut images = BTreeMap::new();
            assert!(matches!(
                record_image_media(&mut images, &entry, &keys),
                Err(Unread::Gone)
            ));
            assert!(images.is_empty());
            assert!(settle(record_image_media(&mut images, &entry, &keys)).is_ok());
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
    #[cfg(target_os = "macos")]
    use crate::scratch_apfs::ScratchRoot;
    use std::cell::{Ref, RefCell};
    use std::collections::{BTreeMap, VecDeque};

    const BLANK_IMAGE_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
      <dict><key>dev-entry</key><string>disk8</string><key>content-hint</key><string>GUID_partition_scheme</string></dict>
    </array></dict></plist>"#;

    /// `diskutil image attach --nobrowse --noMount --plist` for a formatted image, captured live on
    /// macOS 26.6: the image's whole device carries no content hint, and the synthesized container
    /// and its one case-sensitive volume follow it. The attach answers with the volume and the
    /// whole disk it hangs from, the synthesized container.
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
    }

    /// One thing the backend asked the host, in order.
    #[derive(Clone, Debug, PartialEq)]
    enum Seen {
        Command(CommandRequest),
        Inventory,
        Grow(PathBuf, ImageCapacity),
    }

    impl Seen {
        fn command(&self) -> &CommandRequest {
            match self {
                Self::Command(request) => request,
                Self::Inventory => panic!("expected a command, saw the kernel inventory read"),
                Self::Grow(image, _) => panic!("expected a command, saw {} grow", image.display()),
            }
        }
    }

    fn commands(steps: &[Seen]) -> Vec<CommandRequest> {
        steps
            .iter()
            .filter_map(|step| match step {
                Seen::Command(request) => Some(request.clone()),
                Seen::Inventory | Seen::Grow(..) => None,
            })
            .collect()
    }

    #[derive(Default)]
    struct RecordingRunner {
        steps: RefCell<Vec<Seen>>,
        replies: RefCell<VecDeque<Reply>>,
    }

    impl RecordingRunner {
        fn with_outputs(replies: impl IntoIterator<Item = Reply>) -> Self {
            Self {
                steps: RefCell::new(Vec::new()),
                replies: RefCell::new(replies.into_iter().collect()),
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
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            match self.next_reply(Seen::Command(request.clone())) {
                Reply::Command(output) => Ok(output),
                other => panic!("test scripted {other:?} where {request:?} ran"),
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

    fn capacity(value: &str) -> ImageCapacity {
        ImageCapacity::parse(value).expect("test capacities are well formed")
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

    /// One command-contract walk covers strict recorded input selection, readonly attach and
    /// fsck, then a retained-source cleanup dissent that must never escalate to force.
    #[test]
    fn retained_inputs_are_record_bound_read_only_and_never_forced() {
        use crate::storage::recovery::re_adopt_tests::Fixture;

        let fixture = Fixture::new();
        let mut source = fixture.source(Some(fixture.build()));
        source.image.original.set_extension("asif");
        source.image.retained.set_extension("asif");
        fs::create_dir_all(source.image.retained.parent().unwrap()).unwrap();
        let input = RetainedInput::from_record(&source, &source.image.retained).unwrap();
        let unrelated = source.image.retained.with_file_name("unrecorded.asif");
        let refused = RetainedInput::from_record(&source, &unrelated).unwrap_err();
        assert_eq!(refused.code, crate::ErrorCode::Integrity);
        assert!(
            matches!(
                validate_image_path(input.path()),
                Err(ApfsError::InvalidImagePath(_))
            ),
            "ordinary runtime still refuses ASIF"
        );

        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(ATTACH_PLIST),
            holding_verified_volume(input.path()),
            ok([]),
        ]));
        let attachment = backend.attach_retained_verified(&input).unwrap();
        assert_eq!(attachment.0.image(), input.path());
        assert_eq!(attachment.volume_device(), "/dev/disk5s1");
        assert_eq!(attachment.0.whole_device(), "/dev/disk5");
        let requests = backend.runner().requests();
        assert_eq!(
            argv(&requests[0]),
            [
                "image",
                "attach",
                "--readOnly",
                "--nobrowse",
                "--noMount",
                "--plist",
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
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            self.steps.borrow_mut().push(Seen::Command(request.clone()));
            let args = argv(request);
            if request.program == Path::new(DISKUTIL)
                && args.starts_with(&["image".into(), "create".into(), "blank".into()])
            {
                return Ok(CommandOutput::success([]));
            }
            if request.program == Path::new(DISKUTIL)
                && args.starts_with(&["image".into(), "attach".into()])
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

    #[cfg(target_os = "macos")]
    struct RealImageCleanup<'a> {
        backend: &'a MacOsApfsBackend<SystemCommandRunner>,
        image: PathBuf,
        attachment: Option<AttachedImage>,
        armed: bool,
    }

    #[cfg(target_os = "macos")]
    impl<'a> RealImageCleanup<'a> {
        fn new(backend: &'a MacOsApfsBackend<SystemCommandRunner>, image: PathBuf) -> Self {
            Self {
                backend,
                image,
                attachment: None,
                armed: true,
            }
        }

        fn track(&mut self, attachment: AttachedImage) -> &AttachedImage {
            assert!(
                self.attachment.is_none(),
                "only one real image attachment may be tracked"
            );
            self.attachment.insert(attachment)
        }

        fn release_attachment(&self) -> Result<(), ApfsError> {
            if let Some(attachment) = self.attachment.as_ref() {
                return self.backend.detach(attachment, DetachIntent::Release);
            }
            // Creation can fail after attach but before the caller receives an AttachedImage.
            // No tracked handle is not proof of a detached fixture or permission to unlink it.
            if self.image.exists() && self.backend.release_views(&self.image)?.is_empty() {
                return Err(ApfsError::InvalidAttachmentInventory(format!(
                    "cannot prove fixture image {} was released; retaining its backing file",
                    self.image.display()
                )));
            }
            self.backend
                .release_every_attachment(&self.image, DetachIntent::Release)
        }

        fn finish(mut self) -> Result<(), ApfsError> {
            // Exactly one cleanup attempt: a failure retains its media and original error.
            self.armed = false;
            self.release_attachment()?;
            if self.image.exists() {
                self.backend.delete_image(&self.image)?;
            }
            Ok(())
        }
    }

    #[cfg(target_os = "macos")]
    impl Drop for RealImageCleanup<'_> {
        fn drop(&mut self) {
            if self.armed {
                let released = self.release_attachment().and_then(|()| {
                    if self.image.exists() {
                        self.backend.delete_image(&self.image)?;
                    }
                    Ok(())
                });
                if let Err(error) = released {
                    eprintln!(
                        "cowshed: retained real APFS fixture {}: {error}",
                        self.image.display()
                    );
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn finish_real_image_test(result: Result<(), ApfsError>, cleanup: RealImageCleanup<'_>) {
        match (result, cleanup.finish()) {
            (Ok(()), Ok(())) => {}
            (Err(error), Ok(())) => panic!("real APFS scenario failed: {error}"),
            (Ok(()), Err(error)) => panic!("real APFS cleanup failed: {error}"),
            (Err(primary), Err(cleanup)) => {
                panic!("real APFS scenario failed: {primary}; cleanup failed: {cleanup}")
            }
        }
    }

    #[test]
    fn system_runner_reports_output_spawn_errors_and_signal_status() {
        let output = SystemCommandRunner
            .run(&CommandRequest::new(
                "/bin/sh",
                ["-c", "printf stdout; printf stderr >&2; exit 7"],
            ))
            .unwrap();
        assert_eq!(output.status, ProcessStatus::Exit(7));
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");

        let signaled = SystemCommandRunner
            .run(&CommandRequest::new("/bin/sh", ["-c", "kill -TERM $$"]))
            .unwrap();
        assert_eq!(signaled.status, ProcessStatus::Signal(libc::SIGTERM));

        let missing = temp_path("missing-command", "bin");
        let error = SystemCommandRunner
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
        // the deadline, its item marked deferred, and the queue continues; a disk-image command
        // is waited for up to its hard deadline and then killed the same way (the tests below)
        // — never wait forever.
        let started = std::time::Instant::now();
        let error = SystemCommandRunner
            .run_with_deadline(
                &CommandRequest::new("/bin/sh", ["-c", "while true; do :; done"]),
                Duration::from_millis(250),
            )
            .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "hung child held the store for {:?}; the deadline never fired",
            started.elapsed()
        );
        assert!(
            matches!(error.failure, CommandRunFailure::Deadline(deadline) if deadline == Duration::from_millis(250)),
            "{error:?}"
        );
        // The child ran; the report must say it hung, never that it could not be started.
        let report = error.to_string();
        assert!(!report.contains("could not run"), "{report}");
        assert!(report.contains("did not finish within 250ms"), "{report}");
        assert!(report.contains("deferred"), "{report}");
        // The queue continues: a fast child right after the kill still runs.
        let output = SystemCommandRunner
            .run(&CommandRequest::new("/bin/sh", ["-c", "printf 'next\\n'"]))
            .unwrap();
        assert_eq!(output.stdout, b"next\n");
    }

    fn disk_image_request() -> CommandRequest {
        CommandRequest::new(DISKUTIL, ["image", "attach", "/private/tmp/x.dmg"])
    }

    /// The outlived-deadline warning names the verb, not just its noun: `diskutil image attach`.
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
    }

    /// The policy itself, at its real constants: a disk-image command past the 120 s deadline
    /// is warned about every 30 s and killed only at 15 minutes; every other disk child is
    /// killed at the deadline.
    #[test]
    fn a_disk_image_command_is_warned_about_at_the_deadline_and_killed_only_at_the_ceiling() {
        let policy = DeadlinePolicy::for_request(&disk_image_request(), DISK_CHILD_DEADLINE);
        let at = Duration::from_secs;
        assert_eq!(policy.step(at(119), 0), DeadlineStep::Wait);
        assert_eq!(policy.step(at(120), 0), DeadlineStep::Warn);
        assert_eq!(policy.step(at(121), 1), DeadlineStep::Wait);
        assert_eq!(policy.step(at(150), 1), DeadlineStep::Warn);
        assert_eq!(policy.step(at(899), 26), DeadlineStep::Wait);
        assert_eq!(policy.step(at(900), 26), DeadlineStep::Kill);
        assert_eq!(DISK_IMAGE_HARD_DEADLINE, at(15 * 60));
        for detach in [
            CommandRequest::new(HDIUTIL, ["detach", "/dev/disk9"]),
            CommandRequest::new(DISKUTIL, ["image", "info", "/private/tmp/x.dmg"]),
        ] {
            assert_eq!(
                DeadlinePolicy::for_request(&detach, DISK_CHILD_DEADLINE).hard,
                DISK_IMAGE_HARD_DEADLINE
            );
        }
        for other in [
            CommandRequest::new("/sbin/mount_apfs", ["-o", "nobrowse"]),
            CommandRequest::new(DISKUTIL, ["apfs", "resizeContainer", "disk9s1", "0"]),
        ] {
            let policy = DeadlinePolicy::for_request(&other, DISK_CHILD_DEADLINE);
            assert_eq!(policy.step(at(120), 0), DeadlineStep::Kill, "{other:?}");
        }
    }

    /// Past its deadline a disk-image command is not signalled: the waiter warns and the child
    /// finishes on its own, exit status and output intact, returned as success. At the hard
    /// deadline it is killed and the kill reported, which the runner records as a probable
    /// leaked device.
    #[test]
    fn a_disk_image_command_outliving_its_deadline_is_waited_for_then_killed_at_the_ceiling() {
        let request = CommandRequest::new(
            "/bin/sh",
            [
                "-c",
                "i=0; while [ $i -lt 20000 ]; do i=$((i+1)); done; printf done",
            ],
        );
        let waiting = DeadlinePolicy {
            deadline: Duration::from_millis(1),
            warn_every: Duration::from_millis(5),
            hard: Duration::from_secs(60),
        };
        let mut warnings = 0;
        let mut kills = 0;
        let output = SystemCommandRunner::run_unleased(
            &request,
            waiting,
            &mut |_| warnings += 1,
            &mut || kills += 1,
        )
        .expect("the child finishes on its own");
        assert_eq!(output.status, ProcessStatus::Exit(0));
        assert_eq!(output.stdout, b"done");
        assert!(warnings >= 1, "the deadline passed without a warning");
        assert_eq!(kills, 0);

        let ceiling = DeadlinePolicy {
            deadline: Duration::from_millis(1),
            warn_every: Duration::from_millis(50),
            hard: Duration::from_millis(300),
        };
        let directory = std::env::temp_dir().join(format!(
            "cowshed-apfs-hard-deadline-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&directory).unwrap();
        let record = directory.join("pool.json");
        let error = SystemCommandRunner::run_unleased(
            &CommandRequest::new("/bin/sh", ["-c", "while true; do :; done"]),
            ceiling,
            &mut |_| {},
            &mut || {
                attach_budget::record_killed_at(&record, "this-boot").unwrap();
            },
        )
        .unwrap_err();
        assert!(
            matches!(error.failure, CommandRunFailure::Deadline(hard) if hard == Duration::from_millis(300)),
            "{error:?}"
        );
        assert_eq!(
            attach_budget::load(&record)
                .unwrap()
                .probable_leaks
                .map(|leaks| leaks.killed),
            Some(1)
        );
        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn every_read_write_mount_plants_the_spotlight_opt_out_and_refuses_a_squatter() {
        let root = temp_path("spotlight-opt-out", "mnt");
        fs::create_dir_all(&root).unwrap();
        assert_eq!(opt_out_of_spotlight(&root).unwrap(), Marker::Planted);
        let marker = root.join(SPOTLIGHT_OPT_OUT);
        assert_eq!(fs::metadata(&marker).unwrap().len(), 0);
        assert_eq!(
            opt_out_of_spotlight(&root).unwrap(),
            Marker::Planted,
            "idempotent"
        );
        fs::remove_file(&marker).unwrap();
        let elsewhere = root.with_extension("elsewhere");
        std::os::unix::fs::symlink(&elsewhere, &marker).unwrap();
        assert_eq!(opt_out_of_spotlight(&root).unwrap(), Marker::Squatted);
        assert!(
            !elsewhere.exists(),
            "nothing is written through the symlink"
        );
        fs::remove_file(&marker).unwrap();
        fs::write(&marker, b"not empty").unwrap();
        assert_eq!(opt_out_of_spotlight(&root).unwrap(), Marker::Squatted);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn system_runner_verbose_child_output_is_drained_under_deadline() {
        // Pipes are pumped while the deadline poll runs: a child whose stdout exceeds the
        // pipe buffer must complete with its output intact, never wedge on a full pipe.
        let output = SystemCommandRunner
            .run_with_deadline(
                &CommandRequest::new("/bin/sh", ["-c", "seq 1 20000"]),
                Duration::from_secs(30),
            )
            .unwrap();
        assert!(output.succeeded());
        assert_eq!(
            output.stdout.iter().filter(|byte| **byte == b'\n').count(),
            20_000
        );
    }

    #[test]
    fn deferred_images_roundtrip_dedup_and_drain_oldest_first() {
        let _ = take_deferred_images();
        let main = PathBuf::from("/store/acme/main.dmg");
        let session = PathBuf::from("/store/acme/session.dmg");
        record_deferred_image(main.clone());
        record_deferred_image(main.clone());
        record_deferred_image(session.clone());
        assert_eq!(take_deferred_images(), vec![main, session]);
        assert!(take_deferred_images().is_empty());
    }

    #[test]
    fn deferred_image_target_only_names_disk_tool_image_argv() {
        assert_eq!(
            deferred_image_target(&CommandRequest::new(
                DISKUTIL,
                [
                    "image",
                    "attach",
                    "--nobrowse",
                    "--noMount",
                    "--plist",
                    "/store/acme/main.dmg"
                ],
            )),
            Some(PathBuf::from("/store/acme/main.dmg"))
        );
        assert_eq!(
            deferred_image_target(&CommandRequest::new(HDIUTIL, ["detach", "/dev/disk9"])),
            None
        );
        assert_eq!(
            deferred_image_target(&CommandRequest::new(HDIUTIL, ["info", "-plist"])),
            None
        );
        assert_eq!(
            deferred_image_target(&CommandRequest::new(
                DISKUTIL,
                ["image", "attach", "/store/acme/main.sparseimage"],
            )),
            None,
            "only a .dmg image path is a workspace image"
        );
        assert_eq!(
            deferred_image_target(&CommandRequest::new("/bin/sleep", ["30"])),
            None
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
            holding("session.dmg", "/dev/disk5"),
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
            ["detach", "-verbose", "/dev/disk5"]
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
            ["/dev/disk15", "/dev/disk25"]
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
                "image",
                "attach",
                "--nobrowse",
                "--noMount",
                "--plist",
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
        fs::write(&image, b"created").unwrap();
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
        assert_eq!(steps.len(), 7);
        assert_eq!(argv(steps[0].command())[..3], ["image", "create", "blank"]);
        assert_eq!(steps[1], Seen::Inventory);
        assert_eq!(steps[3], Seen::Inventory);
        assert_eq!(steps[4], Seen::Inventory);
        assert_eq!(
            argv(steps[5].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[6], Seen::Inventory);
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
            fs::write(&image, b"created").unwrap();
            let cleanup_inventory = if index == 0 {
                unreadable_inventory()
            } else {
                images(&[(image.to_str().unwrap(), &["not-a-device"][..])])
            };
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
                ok([]),
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
            assert_eq!(backend.runner().steps().len(), 4);
            fs::remove_file(image).unwrap();
        }
    }

    #[test]
    fn post_create_image_attach_failure_removes_the_image() {
        let stem = temp_path("image-attach-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
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
        assert_eq!(steps.len(), 5);
        assert_eq!(steps[1], Seen::Inventory);
        assert_eq!(
            steps
                .iter()
                .filter(|step| **step == Seen::Inventory)
                .count(),
            3
        );
    }

    #[test]
    fn post_create_cleanup_treats_a_missing_staged_image_as_already_removed() {
        let stem = temp_path("image-missing-cleanup", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        assert!(!image.exists());
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
            no_images(),
            failed(9, "attach failed"),
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
                output: CommandOutput {
                    status: ProcessStatus::Exit(9),
                    ..
                },
                ..
            }
        ));
        assert_eq!(backend.runner().steps().len(), 5);
    }

    /// The host limit this type exists for: `diskutil image attach` exiting 1 with error code
    /// 150 because the kernel attach pool is spent. It must arrive as the typed host finding that
    /// names the reboot, not as one more generic exit 1, and nothing else may.
    #[test]
    fn an_attach_the_spent_pool_refused_is_the_typed_host_finding() {
        let spent = "Error: Disk image operation failed with error code 150\n";
        let attach = |stderr: &'static str| {
            let stem = temp_path("image-pool-spent", "stem").with_extension("");
            let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
                ok([]),
                no_images(),
                failed(1, stderr),
                no_images(),
                no_images(),
            ]));
            backend
                .create_staged_image(&CreateImageRequest {
                    staged_stem: stem,
                    capacity: capacity("5g"),
                    volume_name: "main".into(),
                    owner_uid: 502,
                    owner_gid: 20,
                })
                .unwrap_err()
        };

        let error = attach(spent);
        let ApfsError::AttachPoolExhausted(failure) = &error else {
            panic!("expected the typed pool exhaustion, got {error:?}");
        };
        assert_eq!(failure.operation, "attach blank RAW image");
        assert_eq!(failure.output.stderr, spent.as_bytes());
        let report = error.to_string();
        assert!(report.starts_with("AttachPoolExhausted: "), "{report}");
        assert!(report.contains("attach pool is spent"), "{report}");
        assert!(report.contains("error code 150"), "{report}");

        assert!(matches!(
            attach("Error: Disk image operation failed with error code 15\n"),
            ApfsError::CommandFailed { .. }
        ));
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
                ok([]),
                no_images(),
                failed(1, stderr),
                no_images(),
                no_images(),
            ]));
            backend
                .create_staged_image(&CreateImageRequest {
                    staged_stem: stem,
                    capacity: capacity("5g"),
                    volume_name: "main".into(),
                    owner_uid: 502,
                    owner_gid: 20,
                })
                .unwrap_err()
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

    /// The detached-resize root: a blank image is released after formatting only through the
    /// kernel's own mapping. `hdiutil info` once omitted the still-attached image here, the
    /// release read that omission as "already released", and the image stayed attached until
    /// `diskutil image resize` refused it as busy. No step may consult `hdiutil info` at all.
    #[test]
    fn a_formatted_blank_image_is_released_through_the_kernel_mapping() {
        let stem = temp_path("image-kernel-release", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = graced_backend([
            ok([]),
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
        assert_eq!(steps.len(), 11);
        assert_eq!(steps[4].command().program, Path::new(NEWFS_APFS));
        assert_eq!(steps[5], Seen::Inventory);
        assert_eq!(steps[6], Seen::Inventory);
        // The formatted volume is verified before its detach, as the template mint does.
        assert_eq!(steps[7].command().program, Path::new(FSCK_APFS));
        assert_eq!(argv(steps[7].command()), ["-q", "/dev/rdisk5s1"]);
        assert_eq!(steps[8], Seen::Inventory);
        assert_eq!(
            argv(steps[9].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[10], Seen::Inventory);
        assert!(
            !commands(&steps)
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "info")),
            "no attachment decision reads `hdiutil info`"
        );
        assert!(backend.sleeper.waits().is_empty());
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
                .filter(|request| argv(request).get(1).is_some_and(|arg| arg == "attach"))
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
            ok([]),
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
    }

    #[test]
    fn conflicting_blank_attachment_mapping_never_formats_or_removes_the_image() {
        let stem = temp_path("image-foreign-publication", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"created").unwrap();
        let backend = graced_backend([
            ok([]),
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
        assert_eq!(fs::read(&image).unwrap(), b"created");
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
        fs::write(&image, b"created").unwrap();
        let backend = graced_backend([ok([]), no_images(), ok(BLANK_IMAGE_PLIST), no_images()]);
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
        assert_eq!(fs::read(&image).unwrap(), b"created");
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
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
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
        assert_eq!(steps.len(), 8);
        assert_eq!(steps[4].command().program, Path::new(NEWFS_APFS));
        assert_eq!(steps[5], Seen::Inventory);
        assert_eq!(
            argv(steps[6].command()),
            ["detach", "-verbose", "/dev/disk8"]
        );
        assert_eq!(steps[7], Seen::Inventory);
    }

    #[test]
    fn failed_newfs_preserves_image_when_detach_cleanup_fails() {
        let stem = temp_path("image-detach-cleanup-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
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
    fn failed_newfs_preserves_remove_cleanup_failure_after_detach() {
        let stem = temp_path("image-remove-cleanup-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::create_dir(&image).unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
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
            ApfsError::ImageCreationAndCleanupFailed {
                primary,
                detach: None,
                remove: Some(remove),
            } if matches!(
                *primary,
                ApfsError::CommandFailed {
                    operation: "format RAW APFS volume",
                    ..
                }
            ) && matches!(
                *remove,
                ApfsError::FileOperation {
                    operation: "remove failed RAW image",
                    ..
                }
            )
        ));
        fs::remove_dir(image).unwrap();
    }

    #[test]
    fn failed_final_image_eject_preserves_attached_image() {
        let stem = temp_path("image-final-eject-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"formatted").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
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
                    crate::apfs::ImageCapture::Detached,
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
            .clone_image(&source, &destination, crate::apfs::ImageCapture::Detached)
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
        let backend = graced_backend([dissent(), dissent(), dissent(), ok([])]);

        backend
            .detach_device_checked("/dev/disk4", DetachIntent::Release)
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
        let backend = graced_backend([dissent()]);

        let error = backend
            .detach_device_checked("/dev/disk4", DetachIntent::WhenIdle)
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

    /// Only a dissent is transient. A tool that failed for any other reason is failing now and
    /// will fail in ten seconds, so waiting on it would only delay the report.
    #[test]
    fn a_detach_that_failed_for_another_reason_is_never_waited_on() {
        for (status, message) in [
            (1, "no such device"),
            (libc::EBUSY, "permission denied"),
            (1, "Resource busy"),
        ] {
            let backend = graced_backend([failed(status, message)]);
            let error = backend
                .detach_device_checked("/dev/disk4", DetachIntent::Release)
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

    fn renames(backend: &MacOsApfsBackend<RecordingRunner>) -> Vec<CommandRequest> {
        backend
            .runner()
            .requests()
            .into_iter()
            .filter(|request| {
                request
                    .args
                    .first()
                    .is_some_and(|verb| verb == "renameVolume")
            })
            .collect()
    }

    #[test]
    fn rename_volume_names_the_attachments_pinned_volume_device() {
        let (backend, attachment) =
            attached_session([holding_verified_volume("session.dmg"), ok([])]);

        backend
            .rename_volume(&attachment, "cowshed.acme--widget.main")
            .unwrap();

        let steps = backend.runner().steps();
        assert_eq!(
            steps[steps.len() - 2],
            Seen::Inventory,
            "the device is proven the image's before the rename"
        );
        let rename = steps[steps.len() - 1].command();
        assert_eq!(rename.program, Path::new(DISKUTIL));
        assert_eq!(
            argv(rename),
            ["renameVolume", "/dev/disk5s1", "cowshed.acme--widget.main"]
        );
    }

    #[test]
    fn rename_volume_accepts_a_255_byte_path_safe_name() {
        let name = "a".repeat(255);
        let (backend, attachment) =
            attached_session([holding_verified_volume("session.dmg"), ok([])]);

        backend.rename_volume(&attachment, &name).unwrap();

        assert_eq!(renames(&backend)[0].args[2], OsString::from(name));
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
            let error = backend.rename_volume(&attachment, &name).unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidVolumeName(rejected) if rejected == name
            ));
        }
        assert_eq!(backend.runner().steps().len(), attached);
    }

    #[test]
    fn rename_volume_propagates_checked_diskutil_failure() {
        let (backend, attachment) = attached_session([
            holding_verified_volume("session.dmg"),
            failed(7, "rename failed"),
        ]);

        let error = backend.rename_volume(&attachment, "main").unwrap_err();

        assert!(matches!(
            error,
            ApfsError::CommandFailed {
                operation: "rename APFS volume",
                request,
                output: CommandOutput {
                    status: ProcessStatus::Exit(7),
                    ..
                },
            } if request.program == Path::new(DISKUTIL)
                && argv(&request) == ["renameVolume", "/dev/disk5s1", "main"]
        ));
        assert_eq!(renames(&backend).len(), 1);
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

            let renamed =
                backend.rename_volume(&attachment, "[cowshed] fixture · intents — slotted");

            assert!(
                matches!(renamed, Err(ApfsError::InvalidAttachmentInventory(_))),
                "{renamed:?}"
            );
            assert!(renames(&backend).is_empty(), "ran {:?}", renames(&backend));
        }
    }

    /// Pins with a real `flock`ed descriptor, one file per pin, and notes whether the newest pin
    /// is still held when `renameVolume` runs: a second descriptor's non-blocking lock on the
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
            use std::os::fd::AsRawFd as _;
            let newest = self.newest.borrow();
            let pin = newest.as_deref().expect("a pin was taken");
            let probe = File::open(pin).expect("open the pin's file");
            // SAFETY: `probe` owns a live descriptor for the duration of the call.
            unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) != 0 }
        }
    }

    impl CommandRunner for PinWitness {
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            if request
                .args
                .first()
                .is_some_and(|verb| verb == "renameVolume")
            {
                self.held_at_rename.replace(Some(self.newest_pin_held()));
            }
            self.recorded.run(request)
        }
        fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
            self.recorded.image_lease(identity)
        }
        fn pin_raw_device(&self, _: &Path) -> io::Result<Option<Fenced<File>>> {
            use std::os::fd::AsRawFd as _;
            let pin = self.pins.join(format!("pin-{}", self.taken.get()));
            self.taken.set(self.taken.get() + 1);
            let file = File::create(&pin)?;
            // SAFETY: `file` owns a live descriptor for the duration of the call.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(io::Error::last_os_error());
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
    }

    /// The rename's raw pin outlives the `diskutil` call. Dropped any earlier, the kernel could
    /// recycle the device between the mapping proof and the rename.
    #[test]
    fn rename_volume_holds_its_device_pin_across_the_rename() {
        let backend = MacOsApfsBackend::new(PinWitness::new(RecordingRunner::with_outputs(
            verified_attachment_replies("session.dmg")
                .into_iter()
                .chain([holding_verified_volume("session.dmg"), ok([])]),
        )));
        let attachment = backend
            .attach_verified(Path::new("session.dmg"))
            .expect("verified attachment");

        let renamed = backend.rename_volume(&attachment, "cowshed.acme--widget.main");
        let held = backend.runner().held_at_rename.take();
        drop(attachment);
        fs::remove_dir_all(&backend.runner().pins).expect("remove the pins");

        renamed.expect("rename");
        assert_eq!(held, Some(true), "the rename ran without its device pin");
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

    /// A real attachment holds its image file exclusively, so growing it is refused outright and
    /// nothing is written; once detached, the grow lands and Apple's own limits report it.
    #[cfg(target_os = "macos")]
    #[test]
    fn real_apfs_grow_refuses_an_attached_image_and_lands_once_detached() {
        let root = ScratchRoot::new("image-grow").expect("scratch root");
        let image = root.path().join("image").with_extension(IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        let mut cleanup = RealImageCleanup::new(&backend, image.clone());
        let result = (|| -> Result<(), ApfsError> {
            crate::blank_image::blank_image(&image);
            let created = image.as_path();
            let attachment = cleanup.track(backend.attach_verified(created)?);
            assert!(attachment.whole_device().starts_with("/dev/disk"));
            assert!(attachment.volume_device().starts_with("/dev/disk"));
            let refused = SystemCommandRunner
                .grow_image(created, capacity("2g"))
                .expect_err("an attached image's file is held exclusively");
            assert_eq!(refused.kind(), io::ErrorKind::WouldBlock, "{refused}");
            assert!(matches!(
                backend.resize_image(created, capacity("2g")),
                Err(ApfsError::FileOperation { operation: "grow RAW image", source, .. })
                    if source.kind() == io::ErrorKind::WouldBlock
            ));
            backend.detach(attachment, DetachIntent::Release)?;
            assert_eq!(backend.image_capacity(created)?, capacity("1g"));
            backend.resize_image(created, capacity("2g"))?;
            assert_eq!(backend.image_capacity(created)?, capacity("2g"));
            Ok(())
        })();
        finish_real_image_test(result, cleanup);
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

        let held = SystemCommandRunner
            .image_lease(&own)
            .unwrap()
            .expect("production takes a real lease");
        assert_eq!(held.metadata().unwrap().mode() & 0o777, 0o600);
        assert_eq!(
            fs::read_to_string(&own_lease).unwrap(),
            format!("{}\n", std::process::id())
        );
        let probe = open_image_lease(&own_lease).unwrap();
        assert_eq!(
            lock_file(&probe, libc::LOCK_EX | libc::LOCK_NB)
                .expect_err("the identity is held")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(probe);
        assert!(
            SystemCommandRunner.image_lease(&other).unwrap().is_some(),
            "another identity is free while this one is held"
        );

        let timed_out = acquire_image_lease(
            open_image_lease(&own_lease).unwrap(),
            &own_lease,
            &own,
            Duration::ZERO,
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
                let lease = SystemCommandRunner.image_lease(own);
                order.lock().expect("order").push("acquired");
                lease
            });
            waiting
                .recv_timeout(DISK_CHILD_DEADLINE)
                .expect("the contender started");
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

    #[cfg(target_os = "macos")]
    fn lease_test_image(staged_stem: PathBuf, volume_name: &str) -> CreateImageRequest {
        CreateImageRequest {
            staged_stem,
            capacity: capacity("64m"),
            volume_name: volume_name.into(),
            // SAFETY: `getuid` reads this process's credentials; it takes no pointers and
            // cannot fail.
            owner_uid: unsafe { libc::getuid() },
            // SAFETY: `getgid` reads this process's credentials; it takes no pointers and
            // cannot fail.
            owner_gid: unsafe { libc::getgid() },
        }
    }

    /// Plain filesystem paths a real-APFS test creates beside its images: alias links and mount
    /// directories. Declared before the image cleanups, so on every exit — success, `?` or
    /// unwind — it drops after them, once scoped threads have joined and devices are released.
    /// A removal failure fails a passing test; during an unwind it is reported beside the
    /// original failure instead of replacing it.
    #[cfg(target_os = "macos")]
    struct TestPaths(Vec<PathBuf>);

    #[cfg(target_os = "macos")]
    impl Drop for TestPaths {
        fn drop(&mut self) {
            let mut retained = Vec::new();
            for path in &self.0 {
                let removed = match fs::symlink_metadata(path) {
                    Ok(metadata) if metadata.is_dir() => fs::remove_dir(path),
                    Ok(_) => fs::remove_file(path),
                    Err(error) => Err(error),
                };
                match removed {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => retained.push(format!("{}: {error}", path.display())),
                }
            }
            if retained.is_empty() {
                return;
            }
            let retained = retained.join("; ");
            if std::thread::panicking() {
                eprintln!("cowshed: retained test paths after the failure above: {retained}");
            } else {
                panic!("remove test paths: {retained}");
            }
        }
    }

    /// The device namespace is shared, but leases are per image: every cooperating step of one
    /// image waits for that image's lease, and nothing of another image does.
    ///
    /// Holding image A's production lease, the test drives image B through a complete real
    /// lifecycle on another thread — blank create, attach, format and detach; attach and fsck;
    /// mount; native unmount; detach — while a real detach of A, started first, waits. Under a
    /// host-wide lease B could not begin; without a per-image lease A's detach would not wait
    /// for the release. Aliases of A's file (a symlinked parent, a hard link) take other leases
    /// yet gain nothing: the inventory never maps them, so they can neither verify nor release
    /// A's device. Every wait is an event; the only deadline is the hang bound, and it names the
    /// step still open.
    #[cfg(target_os = "macos")]
    #[test]
    fn real_apfs_image_leases_overlap_across_images_and_exclude_within_one() {
        use std::sync::mpsc;

        enum Progress {
            Step(&'static str),
            Done,
        }

        let root = ScratchRoot::new("leases").expect("scratch root");
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        let image_a = root
            .path()
            .join("lease-held")
            .with_extension(IMAGE_EXTENSION);
        let stem_b = root.path().join("lease-free");
        let image_b = stem_b.with_extension(IMAGE_EXTENSION);
        let mount_b = root.path().join("lease-free-mount");
        let alias_parent = root.path().join("lease-alias-parent");
        let hard_link = root
            .path()
            .join("lease-hard-link")
            .with_extension(IMAGE_EXTENSION);
        let _paths = TestPaths(vec![
            alias_parent.clone(),
            hard_link.clone(),
            mount_b.clone(),
        ]);
        let order = std::sync::Mutex::new(Vec::new());
        let mut cleanup_a = RealImageCleanup::new(&backend, image_a.clone());
        let result = (|| -> Result<(), ApfsError> {
            crate::blank_image::blank_image(&image_a);
            let attachment = cleanup_a.track(backend.attach_verified(&image_a)?);

            std::os::unix::fs::symlink(image_a.parent().expect("temp parent"), &alias_parent)
                .expect("symlinked-parent alias");
            fs::hard_link(&image_a, &hard_link).expect("hard-link alias");
            let symlinked = alias_parent.join(image_a.file_name().expect("image name"));

            let held = backend
                .image_lease(&image_a)?
                .expect("production takes a real lease");
            let identity = attachment_inventory_path(&image_a)?;
            let probe = open_image_lease(&image_lease_path(&identity).expect("lease path"))
                .expect("lease file");
            assert_eq!(
                lock_file(&probe, libc::LOCK_EX | libc::LOCK_NB)
                    .expect_err("A's lease is held")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            drop(probe);

            // Each alias takes its own lease, so these run while A's is held; none may verify
            // or disturb A's attachment.
            for alias in [&symlinked, &hard_link] {
                assert!(
                    backend.recovered_image_attachment(alias)?.is_none(),
                    "{} has an inventory mapping of its own",
                    alias.display()
                );
                if let Ok(foreign) = backend.attach_verified(alias) {
                    let released = backend.detach(&foreign, DetachIntent::Release);
                    panic!(
                        "{} verified device {}: {released:?}",
                        alias.display(),
                        foreign.whole_device()
                    );
                }
                match backend.recovered_image_attachment(&image_a)? {
                    Some(RecoveredImageAttachment::Apfs(still)) => assert_eq!(
                        (still.whole_device(), still.volume_device()),
                        (attachment.whole_device(), attachment.volume_device())
                    ),
                    _ => panic!("{} took A's attachment away", alias.display()),
                }
            }

            let (detach_started, detach_waiting) = mpsc::channel();
            let (progress, steps) = mpsc::channel();
            std::thread::scope(|scope| -> Result<(), ApfsError> {
                let (backend, order, mount_b) = (&backend, &order, &mount_b);
                let detach_a = scope.spawn(move || {
                    detach_started.send(()).expect("the test awaits A's detach");
                    let detached = backend.detach(attachment, DetachIntent::Release);
                    order.lock().expect("order").push("A detached");
                    detached
                });
                detach_waiting
                    .recv_timeout(DISK_CHILD_DEADLINE)
                    .expect("A's detach started");
                scope.spawn(move || {
                    // A send fails only after the test panicked; the scope reports that panic.
                    let report = |event: Progress| {
                        if let Progress::Step(step) = &event {
                            // On stderr too, so a harness kill still shows the open step.
                            eprintln!("cowshed: lease test image B step `{step}` started");
                        }
                        let _ = progress.send(event);
                    };
                    let mut cleanup = RealImageCleanup::new(backend, image_b);
                    let result = (|| -> Result<(), ApfsError> {
                        report(Progress::Step("create"));
                        let created = backend
                            .create_staged_image(&lease_test_image(stem_b, "cowshed-lease-free"))?;
                        report(Progress::Step("attach"));
                        let attachment = cleanup.track(backend.attach_verified(&created)?);
                        report(Progress::Step("mount"));
                        backend.mount(attachment, mount_b, MountAccess::ReadWrite, false)?;
                        fs::write(mount_b.join("written-beside-a-held-lease"), b"independent")
                            .expect("write into B's volume");
                        report(Progress::Step("unmount"));
                        backend.unmount_verified(attachment, DetachIntent::Release)?;
                        report(Progress::Step("detach"));
                        backend.detach(attachment, DetachIntent::Release)
                    })();
                    report(Progress::Step("cleanup"));
                    finish_real_image_test(result, cleanup);
                    report(Progress::Done);
                });
                let mut open = "spawn";
                loop {
                    match steps.recv_timeout(DISK_CHILD_DEADLINE) {
                        Ok(Progress::Step(step)) => open = step,
                        Ok(Progress::Done) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                            "image B's `{open}` is still open after {DISK_CHILD_DEADLINE:?} \
                             while only A's lease is held"
                        ),
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            panic!("image B's lifecycle stopped inside `{open}`")
                        }
                    }
                }
                {
                    let mut order = order.lock().expect("order");
                    order.push("B finished");
                    order.push("A released");
                }
                drop(held);
                detach_a.join().expect("A's detach thread")
            })?;

            assert_eq!(
                *order.lock().expect("order"),
                ["B finished", "A released", "A detached"]
            );
            assert!(
                backend.recovered_image_attachment(&image_a)?.is_none(),
                "A's detach released its own device"
            );
            Ok(())
        })();
        finish_real_image_test(result, cleanup_a);
    }

    /// How [`real_apfs_fixture_run_ended_by_its_parent`] ends: `panic`, `kill` or `sweep`.
    #[cfg(target_os = "macos")]
    const FIXTURE_ENDING: &str = "COWSHED_TEST_FIXTURE_ENDING";
    /// The line a fixture run prints once each of its images is attached.
    #[cfg(target_os = "macos")]
    const FIXTURE_IMAGE: &str = "cowshed-test-fixture-image=";
    /// The line a fixture run prints once a process it started works in its root: that pid and
    /// the root, space-separated.
    #[cfg(target_os = "macos")]
    const FIXTURE_WORKER: &str = "cowshed-test-fixture-worker=";

    /// A real-image fixture leaves nothing attached and nothing running however its run ends. A
    /// panic unwinds through the scratch root's own teardown. A kill — nextest's
    /// `terminate-after`, a host-wide stop — runs no teardown at all, so only the next run's sweep
    /// can end the run's processes and release its images, and the sweep finds only those working
    /// in or staged under a scratch root that names the dead run's pid. Fixtures staged under
    /// `$TMPDIR` stayed attached until reboot after every timed-out run, and the stock Nx daemon a
    /// timed-out test started kept running in its deleted root. The run's worker stands in for
    /// that daemon: a process the run started that outlives it, selected, as the daemon is, by its
    /// working directory alone. The run attaches two images, as a CLI fixture leaves at least a
    /// main and a build volume, so both teardown and sweep release several at once.
    #[cfg(target_os = "macos")]
    #[test]
    fn real_apfs_fixture_runs_leave_nothing_attached_or_running_however_they_end() {
        use crate::fork_lock::Spawn;
        use std::io::{BufRead, BufReader};
        use std::os::unix::process::ExitStatusExt;
        use std::process::{Child, Command, Stdio};

        // Each fixture run is a run of its own, as the next `cargo nextest run` is: without the
        // parent's `NEXTEST_RUN_ID`, its scratch root's sweep is never one the parent's run
        // already recorded. Its stderr is forwarded line by line, so a hung run still shows its
        // open step, and answers whether the run printed a panic: the run's output is this test's
        // own, and a panic in it reads as this test's assertion failure behind any timeout.
        let run = |ending: &str| -> (Child, std::thread::JoinHandle<bool>) {
            let mut child = Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "apfs::tests::real_apfs_fixture_run_ended_by_its_parent",
                    "--ignored",
                    "--nocapture",
                ])
                .env(FIXTURE_ENDING, ending)
                .env_remove("NEXTEST_RUN_ID")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn_locked()
                .expect("spawn a fixture run");
            let stderr = BufReader::new(child.stderr.take().expect("run stderr"));
            let forwarded = std::thread::spawn(move || {
                stderr.lines().fold(false, |panicked, line| {
                    let line = line.expect("run stderr");
                    eprintln!("{line}");
                    panicked || line.contains("panicked at")
                })
            });
            (child, forwarded)
        };
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        for ending in ["panic", "kill"] {
            let (mut child, forwarded) = run(ending);
            let mut lines = BufReader::new(child.stdout.take().expect("run stdout"))
                .lines()
                .map(|line| line.expect("run stdout"));
            let images: Vec<PathBuf> = lines
                .by_ref()
                .filter_map(|line| line.strip_prefix(FIXTURE_IMAGE).map(PathBuf::from))
                .take(2)
                .collect();
            // A red run must not leak the images it proves leaked.
            let _leaks: Vec<_> = images
                .iter()
                .map(|image| RealImageCleanup::new(&backend, image.clone()))
                .collect();
            assert_eq!(images.len(), 2, "the {ending} run attached {images:?}");
            let (worker, root) = lines
                .find_map(|line| {
                    let (pid, root) = line.strip_prefix(FIXTURE_WORKER)?.split_once(' ')?;
                    Some((pid.parse::<libc::pid_t>().ok()?, PathBuf::from(root)))
                })
                .unwrap_or_else(|| panic!("the {ending} run never started its worker"));
            if ending == "kill" {
                child.kill().expect("kill the run mid-fixture");
            }
            lines.for_each(drop);
            let status = child.wait().expect("reap the run");
            assert!(
                !forwarded.join().expect("forward the run's stderr"),
                "the {ending} run's deliberate failure printed a panic"
            );
            match ending {
                "panic" => assert_eq!(status.code(), Some(101), "the run failed"),
                _ => {
                    assert_eq!(status.signal(), Some(libc::SIGKILL), "the run was killed");
                    let (mut next, forwarded) = run("sweep");
                    let next = next.wait().expect("the next run");
                    assert!(
                        !forwarded.join().expect("forward the next run's stderr"),
                        "the sweep run printed a panic"
                    );
                    assert!(next.success(), "the next run swept: {next}");
                }
            }
            for image in &images {
                assert!(
                    backend
                        .recovered_image_attachment(image)
                        .expect("attachment inventory")
                        .is_none(),
                    "{ending}: {} is still attached",
                    image.display()
                );
                assert!(!image.exists(), "{ending}: {} remains", image.display());
            }
            let left = crate::scratch_apfs::processes_in(|cwd| cwd.starts_with(&root))
                .expect("list the processes working under the run's root");
            assert_eq!(left, [], "{ending}: worker {worker} outlived its run");
            assert!(!root.exists(), "{ending}: {} remains", root.display());
        }
    }

    /// One real-image fixture run, ended as [`FIXTURE_ENDING`] says: it fails or waits to be
    /// killed once its images are attached and its worker started, or (`sweep`) only opens and
    /// drops a scratch root, whose teardown waits for its run's sweep of dead runs. Its images
    /// are the root's to release, as a CLI fixture's are.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "spawned by real_apfs_fixture_runs_leave_nothing_attached_or_running_however_they_end"]
    fn real_apfs_fixture_run_ended_by_its_parent() {
        let Some(ending) = std::env::var_os(FIXTURE_ENDING) else {
            return;
        };
        let root = ScratchRoot::new("fixture-run").expect("scratch root");
        if ending == "sweep" {
            return;
        }
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        for name in ["first", "second"] {
            let image = root.path().join(name).with_extension(IMAGE_EXTENSION);
            crate::blank_image::blank_image(&image);
            let attachment = backend
                .attach_verified(&image)
                .expect("attach a fixture image");
            let mount = root.path().join(format!("{name}-volume"));
            fs::create_dir(&mount).expect("a fixture mountpoint");
            backend
                .mount(&attachment, &mount, MountAccess::ReadWrite, false)
                .expect("mount a fixture image");
            println!("{FIXTURE_IMAGE}{}", image.display());
        }
        // Bounded, so a worker no teardown or sweep ends still ends; a passing run ends it within
        // seconds.
        #[expect(
            clippy::zombie_processes,
            reason = "the worker must outlive this run; the scratch root or the next sweep ends it"
        )]
        let worker = crate::fork_lock::Spawn::spawn_locked(
            std::process::Command::new("/bin/sleep")
                .arg("600")
                .current_dir(root.path())
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null()),
        )
        .expect("start a worker in the root");
        println!("{FIXTURE_WORKER}{} {}", worker.id(), root.path().display());
        if ending == "panic" {
            // Unwinds through the scratch root's teardown as a failed assertion does, without the
            // panic hook: its `panicked at` would read as the parent's own assertion failure.
            std::panic::resume_unwind(Box::new("the fixture's run fails with its images attached"));
        }
        // Killed here; the parent's stdin closes only if it failed first.
        let _ = io::stdin().read_line(&mut String::new());
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
        assert_eq!(attachment.whole_device(), "/dev/disk15");
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

    /// Foreign, ambiguous, malformed and unreadable inventories fail closed.
    #[test]
    fn foreign_ambiguous_and_unreadable_inventories_fail_closed() {
        let image = Path::new("/tmp/cowshed-target.dmg");
        let foreign = [
            AttachedDiskImage {
                source: DiskImageSource::Url("ram://2048".into()),
                capacity: None,
                media: live_capture().media,
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
        for url in ["ram://2048", "file://host/share/x.asif", "not a url"] {
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
        // The synthesized APFS volume, not the image's own device or the synthesized store, and
        // the whole disk that volume hangs from.
        assert_eq!(attachment.volume_device(), "/dev/disk15s1");
        assert_eq!(attachment.whole_device(), "/dev/disk15");
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
            ("/dev/disk5".into(), "/dev/disk5s1".into())
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
        SystemCommandRunner
            .grow_image(&image, capacity("2g"))
            .unwrap();
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
            let error = SystemCommandRunner
                .grow_image(&image, requested)
                .unwrap_err();
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
            let error = SystemCommandRunner
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
        let error = SystemCommandRunner
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
            fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
                panic!("growing spawned {request:?}");
            }
            fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
                SystemCommandRunner.image_lease(identity)
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
                let contended = lock_file(&probe, libc::LOCK_EX | libc::LOCK_NB)
                    .err()
                    .is_some_and(|error| error.kind() == io::ErrorKind::WouldBlock);
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
            .clone_image(&source, &destination, crate::apfs::ImageCapture::Detached)
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
            .clone_image(&source, &destination, crate::apfs::ImageCapture::Detached)
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
            .clone_image(&source, &destination, crate::apfs::ImageCapture::Detached)
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
        assert_eq!(commands, [["detach", "-verbose", "/dev/disk5"]]);
    }

    /// A release unmounts each volume of the container under its own revalidation against the
    /// driver, and refuses a volume the driver does not hold without unmounting anything.
    #[test]
    fn a_release_revalidates_each_volume_it_unmounts_and_refuses_a_stranger() {
        let image = temp_path("two-volume-unmount", IMAGE_EXTENSION);
        let views = release_views_of(&image, &two_volume_image(&image)).unwrap();
        let [view] = views.as_slice() else {
            panic!("one attachment is one release view");
        };
        assert_eq!(view.whole_device(), "/dev/disk5");
        let both = BTreeSet::from(["/dev/disk5s1".to_owned(), "/dev/disk5s2".to_owned()]);
        let volumes: Vec<ReleasedVolume> = view.mounted_volumes(&both).collect();
        let backend = graced_backend([
            Reply::Inventory(Ok(two_volume_image(&image))),
            ok([]),
            Reply::Inventory(Ok(two_volume_image(&image))),
            ok([]),
            Reply::Inventory(Ok(two_volume_image(&image))),
        ]);
        for volume in &volumes {
            backend
                .unmount_released(volume, DetachIntent::Release)
                .unwrap();
        }
        let stranger = ReleasedVolume(AttachedImage {
            image: image.clone(),
            whole_device: "/dev/disk5".into(),
            volume_device: "/dev/disk5s3".into(),
            pin: std::sync::Mutex::new(None),
        });
        let refused = backend
            .unmount_released(&stranger, DetachIntent::Release)
            .unwrap_err();
        assert!(
            matches!(&refused, ApfsError::InvalidAttachmentInventory(message)
                if message.contains("no longer owns /dev/disk5 / /dev/disk5s3")),
            "{refused}"
        );
        let commands: Vec<Vec<String>> = backend.runner().requests().iter().map(argv).collect();
        assert_eq!(commands, [["/dev/disk5s1"], ["/dev/disk5s2"]]);
    }

    /// A release unmounts exactly the container's mounted volumes, each under its own
    /// revalidation, and leaves the unmounted one to the container's single detach.
    #[test]
    fn a_release_unmounts_only_the_mounted_volumes_then_detaches_the_container_once() {
        let image = temp_path("two-volume-mixed", IMAGE_EXTENSION);
        let backend = graced_backend([
            Reply::Inventory(Ok(two_volume_image(&image))),
            Reply::Inventory(Ok(two_volume_image(&image))),
            ok([]),
            Reply::Inventory(Ok(two_volume_image(&image))),
            Reply::Inventory(Ok(two_volume_image(&image))),
            ok([]),
            no_images(),
        ]);
        let mounted = BTreeSet::from(["/dev/disk5s2".to_owned(), "/dev/disk9s1".to_owned()]);
        backend
            .release_image(&image, &mounted, DetachIntent::Release)
            .unwrap();
        let commands: Vec<Vec<String>> = backend.runner().requests().iter().map(argv).collect();
        assert_eq!(
            commands,
            [
                vec!["/dev/disk5s2".to_owned()],
                vec!["detach".into(), "-verbose".into(), "/dev/disk5".into()],
            ]
        );
    }

    /// A release under `WhenIdle` hands a held volume's dissent straight back: no wait, no forced
    /// unmount, and no detach of the container.
    #[test]
    fn a_when_idle_release_returns_a_held_volumes_dissent_without_forcing_or_detaching() {
        let image = temp_path("two-volume-held", IMAGE_EXTENSION);
        let backend = graced_backend([
            Reply::Inventory(Ok(two_volume_image(&image))),
            Reply::Inventory(Ok(two_volume_image(&image))),
            failed(1, "umount: unmount(/private/tmp/sibling): Resource busy"),
        ]);
        let mounted = BTreeSet::from(["/dev/disk5s2".to_owned()]);
        let error = backend
            .release_image(&image, &mounted, DetachIntent::WhenIdle)
            .unwrap_err();
        assert!(detach_was_dissented(&error), "{error}");
        let commands: Vec<Vec<String>> = backend.runner().requests().iter().map(argv).collect();
        assert_eq!(commands, [["/dev/disk5s2"]]);
        assert!(backend.sleeper.waits().is_empty());
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
        let [view] = views.as_slice() else {
            panic!("one attachment is one release view");
        };
        assert_eq!(view.whole_device(), "/dev/disk4");
        let whole = BTreeSet::from(["/dev/disk4".to_owned()]);
        assert_eq!(view.mounted_volumes(&whole).count(), 0);
    }
}
