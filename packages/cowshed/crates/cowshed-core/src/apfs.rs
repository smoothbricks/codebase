//! macOS APFS disk-image substrate.
//!
//! Every external operation crosses [`CommandRunner`]. Commands are represented
//! as an executable plus an argument vector; this module never invokes a shell.

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

use crate::device::{DISKUTIL, container_of, identifier_depth};

/// The disk-image driver's detach interface. Creation and attachment of ASIF images use
/// `diskutil image`; releasing an owned image goes directly through `hdiutil detach`. Which
/// images are attached is read from the kernel ([`CommandRunner::attached_disk_images`]), never
/// from `hdiutil info`.
const HDIUTIL: &str = "/usr/bin/hdiutil";
const FSCK_APFS: &str = "/sbin/fsck_apfs";
/// The kernel mount helper. Workspace volumes are mounted with it rather than `diskutil mount`
/// because `diskutil` routes the mount through Disk Arbitration, which serialises every client
/// on the host: under a loaded fleet a mount that `mount_apfs` completes in about a second was
/// measured queueing for 68 s at the median and past the 120 s child deadline at the tail.
/// Disk Arbitration still observes the mounted volume, so eject and inventory keep working.
const MOUNT_APFS: &str = "/sbin/mount_apfs";
const UMOUNT: &str = "/sbin/umount";
const NEWFS_APFS: &str = "/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs";

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
    /// Hold the host's APFS device namespace while an image's device is read and used.
    /// Recording runners must explicitly opt out; production cannot silently omit the lease.
    fn host_device_lease(&self) -> io::Result<Option<File>>;
    /// Open the raw volume before resolving its image identity. On macOS the open device
    /// prevents even a forced image eject until the descriptor closes. Recording runners
    /// explicitly opt out; production must not fall back to an unpinned pathname.
    fn pin_raw_device(&self, device: &Path) -> io::Result<Option<File>>;
    /// Every disk image the kernel has attached right now, read from the I/O Registry.
    ///
    /// This is the one attachment inventory, and its absences are authoritative. `hdiutil info
    /// -plist` is not: while any other image attaches or detaches it answers a truncated image
    /// list with nothing marking it, and reading that omission as "not attached" skipped a
    /// release that never happened and lost a mounted workspace's identity on restart. The
    /// kernel's matching snapshot is taken under the registry's own consistency check.
    fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>>;
}

/// The bound every spawned disk child (attach/detach, inventory, mount) answers inside.
///
/// Without it a hung `hdiutil`/`diskutil` child wedges the store operation that spawned it
/// forever — one stuck workspace takes down every other workspace's verbs, and the daemon
/// startup pass with them. On expiry the child is killed and the item reports a timeout
/// ("deferred") so the queue continues and the next pass retries it; the per-leg
/// [`timed_apfs_step`] span such a child was stuck inside terminates with `status=err` and
/// the waited elapsed instead of never closing.
///
/// 120 seconds is generous against normally sub-second disk children and stays inside the
/// enclosing per-operation budgets even when a pass pays it once per workspace; tests
/// override it through [`SystemCommandRunner::run_with_deadline`].
pub const DISK_CHILD_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCommandRunner;

impl SystemCommandRunner {
    /// Spawn `request` and collect its output, killing the child when `deadline` passes
    /// first. [`CommandRunner::run`] is this with [`DISK_CHILD_DEADLINE`]; tests pass a
    /// short deadline to prove a hung child is reaped promptly.
    pub fn run_with_deadline(
        &self,
        request: &CommandRequest,
        deadline: Duration,
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
                None => {
                    if started.elapsed() >= deadline {
                        kill_disk_child_group(&mut child);
                        let _ = child.wait();
                        let _ = stdout.join();
                        let _ = stderr.join();
                        if let Some(image) = deferred_image_target(request) {
                            record_deferred_image(image);
                        }
                        return Err(CommandRunError {
                            request: request.clone(),
                            failure: CommandRunFailure::Deadline(deadline),
                        });
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
            }
        }
    }
}

impl CommandRunner for SystemCommandRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        self.run_with_deadline(request, DISK_CHILD_DEADLINE)
    }

    fn host_device_lease(&self) -> io::Result<Option<File>> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

        // Every process of this user sees the same Disk Arbitration device namespace, including
        // independent Nx/Nextest workers. No workspace-owned lock can coordinate those workers.
        let uid = unsafe { libc::geteuid() };
        let path = PathBuf::from(format!("/private/tmp/cowshed-apfs-device-{uid}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != uid
            || metadata.mode() & 0o777 != 0o600
            || metadata.nlink() != 1
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "APFS host device lock {} is not a private regular file owned by this user",
                    path.display()
                ),
            ));
        }
        loop {
            // SAFETY: file owns this descriptor for the entire operation; flock does not consume it.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(Some(file));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
    fn pin_raw_device(&self, device: &Path) -> io::Result<Option<File>> {
        // A live raw-device descriptor pins this IOMedia, including against forced image detach.
        // Open first, then confirm the image-to-volume mapping while it cannot be recycled.
        OpenOptions::new().read(true).open(device).map(Some)
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
    command.spawn()
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

/// Images whose disk child was killed at [`DISK_CHILD_DEADLINE`], oldest first.
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

impl Default for DetachGrace {
    fn default() -> Self {
        Self {
            total: Duration::from_secs(10),
            poll: Duration::from_millis(250),
        }
    }
}

/// The grace's only side effect, injected so unit tests spend no wall-clock time proving the
/// escalation order.
pub trait Sleeper {
    fn sleep(&self, duration: Duration);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// The wait a just-detached whole device is given to leave the attachment inventory.
///
/// A successful image detach means the image driver let go; the kernel terminating the media it
/// exposed is a second, lagging step. A verb that detaches an image and then attaches it again,
/// as `resize` does, should not start the attach while the old device is still registered.
///
/// After every whole-device detach the backend therefore re-reads the kernel inventory until the
/// device is gone, logging each outcome. The bound is generous against a normally sub-second
/// departure and the outcome is always soft: at the bound the operation proceeds exactly as it
/// would have without the check, but loudly. No blind sleeps: the first poll runs immediately,
/// and a departed device costs one registry read and no waiting.
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

/// A blank ASIF image holding one case-sensitive APFS volume, staged at `staged_stem.asif`.
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

#[derive(Debug)]
pub struct AttachedImage {
    image: PathBuf,
    whole_device: String,
    volume_device: String,
    /// An attachment verified but not yet mounted holds its raw IOMedia open. The kernel
    /// refuses even forced ejects until mount finishes; no second process can reuse diskN
    /// between the two backend method calls. Existing mounted attachments have no pin.
    pin: std::sync::Mutex<Option<File>>,
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

/// The exact image mapping may be a just-created whole device or a formatted APFS attachment.
pub(crate) enum RecoveredImageAttachment {
    Unformatted {
        image: PathBuf,
        whole_device: String,
    },
    Apfs(AttachedImage),
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
    InvalidMountPoint(PathBuf),
    InvalidVolumeName(String),
    InvalidVolumeDevice(String),
    CommandRun(CommandRunError),
    CommandFailed {
        operation: &'static str,
        request: CommandRequest,
        output: CommandOutput,
    },
    InvalidAttachmentPlist(String),
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
    AsifCreationAndCleanupFailed {
        primary: Box<ApfsError>,
        detach: Option<Box<ApfsError>>,
        remove: Option<Box<ApfsError>>,
    },
    InvalidResizeLimits(String),
    ImageNotAttached(PathBuf),
    /// The kernel's disk-image inventory could not be read.
    KernelInventory(io::Error),
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
            Self::InvalidMountPoint(path) => {
                write!(f, "invalid APFS mount point: {}", path.display())
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
            Self::InvalidAttachmentPlist(message) => {
                write!(f, "invalid attachment plist: {message}")
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
            Self::AsifCreationAndCleanupFailed {
                primary,
                detach,
                remove,
            } => {
                write!(f, "ASIF creation failed: {primary}")?;
                if let Some(detach) = detach {
                    write!(f, "; detach cleanup failed: {detach}")?;
                }
                if let Some(remove) = remove {
                    write!(f, "; file cleanup failed: {remove}")?;
                }
                Ok(())
            }
            Self::InvalidResizeLimits(message) => {
                write!(f, "invalid image resize limits: {message}")
            }
            Self::ImageNotAttached(image) => write!(
                f,
                "{} reports no attachment to read a capacity from",
                image.display()
            ),
            Self::KernelInventory(source) => {
                write!(f, "read the kernel disk-image inventory: {source}")
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
            Self::AsifCreationAndCleanupFailed { primary, .. } => Some(primary),
            Self::AttachmentCleanupFailed { primary, .. } => Some(primary),
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

pub trait ApfsBackend {
    fn create_staged_image(&self, request: &CreateImageRequest) -> Result<PathBuf, ApfsError>;
    /// Make the source's latest writes part of the image a clone is about to be cut from:
    /// its volume when `mount_point` is mounted, then the image file itself.
    ///
    /// Freshness, not consistency (specs/cowshed/02_workspaces.md, `cowshed new` step 1): a live
    /// clone is always crash-consistent, but without this it can miss the source's last writes.
    /// Only the source is flushed. The host-wide `sync(8)` this replaces also waited for every
    /// other mounted volume's dirty data — measured at 16 s, and at 39 s inside a fork under a
    /// host with dozens of workspaces building, against 3–676 ms for the source volume alone.
    fn sync_for_freshness(&self, image: &Path, mount_point: Option<&Path>)
    -> Result<(), ApfsError>;
    fn clone_image(&self, source: &Path, destination: &Path) -> Result<(), CloneFileError>;
    /// Clone `source` to `destination` after [`Self::sync_for_freshness`]; `source_mount` is the
    /// source's mount point when it is a live workspace, `None` for an image nothing mounts.
    fn sync_and_clone(
        &self,
        source: &Path,
        source_mount: Option<&Path>,
        destination: &Path,
    ) -> Result<(), ApfsError>;
    fn rename_volume(&self, mount_point: &Path, volume_name: &str) -> Result<(), ApfsError>;
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
    /// The capacity a detached image currently holds, read from its own resize limits.
    fn image_capacity(&self, image: &Path) -> Result<ImageCapacity, ApfsError>;
    /// Grow a detached image, and the partition and container inside it, to `capacity`.
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
    fn host_device_lease(&self, image: &Path) -> Result<Option<File>, ApfsError> {
        timed_apfs_step(apfs_step_leg(image), "device-lease", || {
            self.runner
                .host_device_lease()
                .map_err(|source| ApfsError::FileOperation {
                    operation: "acquire host APFS device lease for image",
                    path: image.to_owned(),
                    source,
                })
        })
    }

    fn pin_attached_volume(&self, attachment: &AttachedImage) -> Result<Option<File>, ApfsError> {
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
            self.require_attached_mapping(attachment)
        })?;
        Ok(pin)
    }

    fn require_attached_mapping(&self, attachment: &AttachedImage) -> Result<(), ApfsError> {
        let actual = self.existing_attachment(&attachment.image)?;
        if actual.as_ref().is_some_and(|held| {
            held.whole_device == attachment.whole_device
                && held.volume_device == attachment.volume_device
        }) {
            Ok(())
        } else {
            Err(ApfsError::InvalidAttachmentInventory(format!(
                "{} no longer owns {} / {} (holds {actual:?}); refusing a reused device",
                attachment.image.display(),
                attachment.whole_device,
                attachment.volume_device
            )))
        }
    }

    /// Remove an owned volume from the filesystem namespace before releasing its image.
    /// The raw IOMedia pin freezes the device identity while native unmount bypasses DA.
    pub(crate) fn unmount_verified(
        &self,
        attachment: &AttachedImage,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        let _lease = self.host_device_lease(&attachment.image)?;
        let _pin = self.pin_attached_volume(attachment)?;
        let mut waited = Duration::ZERO;
        let mut force = false;
        loop {
            let mut args = Vec::with_capacity(1 + usize::from(force));
            if force {
                args.push(OsString::from("-f"));
            }
            args.push(OsString::from(&attachment.volume_device));
            let result = timed_apfs_step(apfs_step_leg(&attachment.image), "unmount", || {
                self.run_checked(
                    "unmount verified APFS volume",
                    CommandRequest::new(UMOUNT, args),
                )
            });
            match result {
                Ok(_) => return Ok(()),
                Err(error) if !force && detach_was_dissented(&error) => {
                    if intent == DetachIntent::WhenIdle {
                        return Err(error);
                    }
                    if waited >= self.grace.total {
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
            if let Err(error) =
                self.detach_image_device_unlocked(image, device, DetachIntent::Release)
            {
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
    ) -> ApfsError {
        match self.cleanup_new_attachments(image, before) {
            Ok(()) => primary,
            Err(cleanup) => ApfsError::AttachmentCleanupFailed {
                image: image.to_owned(),
                primary: Box::new(primary),
                cleanup,
            },
        }
    }

    fn failed_asif_attachment(
        &self,
        path: &Path,
        before: &BTreeSet<String>,
        primary: ApfsError,
    ) -> ApfsError {
        match self.cleanup_new_attachments(path, before) {
            Ok(()) => self.cleanup_failed_asif(path, primary),
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
                Err(ApfsError::CommandFailed {
                    operation,
                    request,
                    output,
                })
            }
        })
    }

    /// Create a blank ASIF image, attach it without mounting, and format its whole device as one
    /// case-sensitive APFS volume (`newfs_apfs -e`) owned by the invoking user (`-U`/`-G`).
    /// Nothing here runs as root.
    fn create_asif(&self, path: &Path, request: &CreateImageRequest) -> Result<(), ApfsError> {
        validate_image_path(path)?;
        let _lease = self.host_device_lease(path)?;
        let create = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("image"),
                OsString::from("create"),
                OsString::from("blank"),
                OsString::from("--format"),
                OsString::from("ASIF"),
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
            self.run_checked("create ASIF image", create)
        })?;
        let attached_before = self.attached_whole_devices(path)?;

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
            self.run_checked("attach blank ASIF image", attach)
        }) {
            Ok(output) => output,
            Err(primary) => {
                return Err(self.failed_asif_attachment(path, &attached_before, primary));
            }
        };
        let whole_device = match parse_blank_asif_whole_device(&output.stdout) {
            Ok(device) => device,
            Err(primary) => {
                return Err(self.failed_asif_attachment(path, &attached_before, primary));
            }
        };
        if attached_before.contains(&whole_device) {
            return Err(self.failed_attachment(
                path,
                &attached_before,
                ApfsError::InvalidAttachmentPlist(
                    "attach reported a pre-existing whole image device".into(),
                ),
            ));
        }
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
            self.run_checked("format ASIF APFS volume", format)
        }) {
            return match self.detach_image_device_unlocked(
                path,
                &whole_device,
                DetachIntent::Release,
            ) {
                Ok(()) => Err(self.cleanup_failed_asif(path, primary)),
                Err(detach) => Err(ApfsError::AsifCreationAndCleanupFailed {
                    primary: Box::new(primary),
                    detach: Some(Box::new(detach)),
                    remove: None,
                }),
            };
        }
        self.detach_image_device_unlocked(path, &whole_device, DetachIntent::Release)?;
        Ok(())
    }

    fn cleanup_failed_asif(&self, path: &Path, primary: ApfsError) -> ApfsError {
        let remove = match fs::remove_file(path) {
            Ok(()) => None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(source) => Some(ApfsError::FileOperation {
                operation: "remove failed ASIF image",
                path: path.to_owned(),
                source,
            }),
        };
        if remove.is_none() {
            primary
        } else {
            ApfsError::AsifCreationAndCleanupFailed {
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

    pub(crate) fn detach_unformatted_image(
        &self,
        image: &Path,
        whole_device: &str,
        intent: DetachIntent,
    ) -> Result<(), ApfsError> {
        let _lease = self.host_device_lease(image)?;
        match self.recovered_image_attachment(image)? {
            None => Ok(()),
            Some(RecoveredImageAttachment::Unformatted {
                whole_device: current,
                ..
            }) if current == whole_device => {
                self.detach_image_device_unlocked(image, whole_device, intent)
            }
            Some(_) => Err(ApfsError::InvalidAttachmentInventory(format!(
                "{} no longer has the observed unformatted attachment {whole_device}; retaining the image",
                image.display(),
            ))),
        }
    }

    /// Attach `image` without mounting it, answering the volume and container the attach itself
    /// reports ([`attached_apfs_volume`]). No Disk Arbitration query follows the attach.
    fn attach_without_mounting(&self, image: &Path) -> Result<AttachedImage, ApfsError> {
        validate_image_path(image)?;
        let attached_before = self.attached_whole_devices(image)?;
        let request = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("image"),
                OsString::from("attach"),
                OsString::from("--nobrowse"),
                OsString::from("--noMount"),
                OsString::from("--plist"),
                image.as_os_str().to_owned(),
            ],
        );
        let output = match self.run_checked("attach image without mounting", request) {
            Ok(output) => output,
            Err(primary) => {
                return Err(self.failed_attachment(image, &attached_before, primary));
            }
        };
        let (whole_device, volume_device) = match parse_attachment_plist(&output.stdout) {
            Ok(attachment) => attachment,
            Err(primary) => {
                return Err(self.failed_attachment(image, &attached_before, primary));
            }
        };
        if attached_before.contains(&whole_device) {
            return Err(self.failed_attachment(
                image,
                &attached_before,
                ApfsError::InvalidAttachmentPlist(
                    "attach reported a pre-existing whole image device".into(),
                ),
            ));
        }
        Ok(AttachedImage {
            image: image.to_owned(),
            whole_device,
            volume_device,
            pin: std::sync::Mutex::new(None),
        })
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
        loop {
            match self.detach_once(target, false) {
                Ok(()) => return Ok(()),
                Err(error) if detach_was_dissented(&error) => {
                    if intent == DetachIntent::WhenIdle {
                        return Err(error);
                    }
                    if waited >= self.grace.total {
                        return self.detach_once(target, true);
                    }
                    self.sleeper.sleep(self.grace.poll);
                    waited = waited.saturating_add(self.grace.poll);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn detach_once(&self, target: &OsStr, force: bool) -> Result<(), ApfsError> {
        let mut args = Vec::with_capacity(2 + usize::from(force));
        args.push(OsString::from("detach"));
        if force {
            args.push(OsString::from("-force"));
        }
        args.push(target.to_owned());
        self.run_checked("detach image", CommandRequest::new(HDIUTIL, args))
            .map(|_| ())
    }

    /// Detach `whole_device` only while the kernel still shows `image` holding it.
    ///
    /// A `/dev/diskN` name read earlier is not an identity: once its image detaches, the kernel
    /// hands the same name to the next attach anywhere on the host, and detaching the recorded
    /// name would take that image away from its owner. The backing file the image driver
    /// registered at attach time is the identity, so the device is re-read against it
    /// immediately before every detach. The kernel inventory's absences are authoritative, so an
    /// image it does not hold is already released; a positive conflicting mapping is a refusal,
    /// never release success. Its caller holds the host lease while the image identity is read
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
        self.settle_detached_device(whole_device);
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

    /// Bounded, logged confirmation that a detached whole device left the kernel inventory
    /// before any later attach. The poll is keyed on the device rather than an image path: after
    /// a detach there may be no image to scope by, and the question is whether the device is
    /// gone at all.
    ///
    /// Soft by design: the detach already succeeded, so a lingering or unreadable inventory is
    /// logged and the operation proceeds exactly as it would have without the check. Failing a
    /// detach that succeeded over the media's termination lag would turn a slow departure under
    /// load into a user-facing failure.
    fn settle_detached_device(&self, whole_device: &str) {
        let mut waited = Duration::ZERO;
        loop {
            match self.detached_device_departed(whole_device) {
                Ok(true) => {
                    eprintln!(
                        "cowshed: apfs detach-settle {whole_device} departed waited={waited:?}"
                    );
                    return;
                }
                Ok(false) => {}
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

    fn detached_device_departed(&self, whole_device: &str) -> Result<bool, ApfsError> {
        Ok(!inventory_devices(&self.attached_disk_images()?).contains(whole_device))
    }
}

impl<R: CommandRunner, S: Sleeper> ApfsBackend for MacOsApfsBackend<R, S> {
    fn create_staged_image(&self, request: &CreateImageRequest) -> Result<PathBuf, ApfsError> {
        if request.staged_stem.extension().is_some() {
            return Err(ApfsError::InvalidStagedStem(request.staged_stem.clone()));
        }
        if request.staged_stem.file_name().is_none() {
            return Err(ApfsError::InvalidStagedStem(request.staged_stem.clone()));
        }
        if !is_valid_apfs_volume_name(&request.volume_name) {
            return Err(ApfsError::InvalidCreateRequest(
                "volume name must be path-safe and at most 255 bytes",
            ));
        }

        let path = request.staged_stem.with_extension(IMAGE_EXTENSION);
        self.create_asif(&path, request)?;
        Ok(path)
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

    fn clone_image(&self, source: &Path, destination: &Path) -> Result<(), CloneFileError> {
        validate_clone_path(source)?;
        validate_clone_path(destination)?;
        clonefile_native(source, destination)
    }

    fn sync_and_clone(
        &self,
        source: &Path,
        source_mount: Option<&Path>,
        destination: &Path,
    ) -> Result<(), ApfsError> {
        validate_clone_path(source).map_err(ApfsError::from)?;
        validate_clone_path(destination).map_err(ApfsError::from)?;
        let leg = apfs_step_leg(destination);
        timed_apfs_step(leg, "sync", || {
            self.sync_for_freshness(source, source_mount)
        })?;
        timed_apfs_step(leg, "clonefile", || {
            self.clone_image(source, destination)
                .map_err(ApfsError::from)
        })?;
        Ok(())
    }

    fn rename_volume(&self, mount_point: &Path, volume_name: &str) -> Result<(), ApfsError> {
        if !is_canonical_mount_point(mount_point) {
            return Err(ApfsError::InvalidMountPoint(mount_point.to_owned()));
        }
        if !is_valid_apfs_volume_name(volume_name) {
            return Err(ApfsError::InvalidVolumeName(volume_name.to_owned()));
        }
        self.run_checked(
            "rename APFS volume",
            CommandRequest::new(
                DISKUTIL,
                [
                    OsString::from("renameVolume"),
                    mount_point.as_os_str().to_owned(),
                    OsString::from(volume_name),
                ],
            ),
        )
        .map(|_| ())
    }

    fn attach_verified(&self, image: &Path) -> Result<AttachedImage, ApfsError> {
        let leg = apfs_step_leg(image);
        let (mut attachment, request, output, pin) = {
            let _lease = self.host_device_lease(image)?;
            let mut retried_lost_device = false;
            loop {
                let attachment =
                    timed_apfs_step(leg, "attach", || self.attach_without_mounting(image))?;
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

        match self.detach(&attachment, DetachIntent::Release) {
            Ok(()) => Err(ApfsError::VerificationFailed { request, output }),
            Err(detach) => Err(ApfsError::VerificationAndDetachFailed {
                request,
                verification: output,
                detach: Box::new(detach),
            }),
        }
    }

    fn mount(
        &self,
        attachment: &AttachedImage,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsError> {
        let _lease = self.host_device_lease(&attachment.image)?;
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
        let options = match (access, browse) {
            (MountAccess::ReadWrite, false) => "nobrowse,owners",
            (MountAccess::ReadWrite, true) => "owners",
            (MountAccess::ReadOnly, false) => "rdonly,nobrowse,owners",
            (MountAccess::ReadOnly, true) => "rdonly,owners",
        };
        let request = CommandRequest::new(
            MOUNT_APFS,
            [
                OsString::from("-o"),
                OsString::from(options),
                OsString::from(&attachment.volume_device),
                mount_point.as_os_str().to_owned(),
            ],
        );
        let mounted = timed_apfs_step(apfs_step_leg(&attachment.image), "mount", || {
            self.run_checked("mount verified APFS volume", request)
        });
        inherited_pin.take();
        mounted.map(|_| ())
    }

    fn detach(&self, attachment: &AttachedImage, intent: DetachIntent) -> Result<(), ApfsError> {
        let _lease = self.host_device_lease(&attachment.image)?;
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
        let output = self.run_checked(
            "read ASIF image resize limits",
            CommandRequest::new(
                DISKUTIL,
                [
                    OsString::from("image"),
                    OsString::from("resize"),
                    OsString::from("--plist"),
                    image.as_os_str().to_owned(),
                ],
            ),
        )?;
        parse_asif_resize_limits(&output.stdout)
    }

    fn resize_image(&self, image: &Path, capacity: ImageCapacity) -> Result<(), ApfsError> {
        validate_image_path(image)?;
        let request = CommandRequest::new(
            DISKUTIL,
            [
                OsString::from("image"),
                OsString::from("resize"),
                OsString::from("--size"),
                OsString::from(capacity_argument(capacity)),
                image.as_os_str().to_owned(),
            ],
        );
        self.run_checked("resize image", request).map(|_| ())
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

/// `diskutil image resize --plist` reports limits in bytes under `current`.
fn parse_asif_resize_limits(bytes: &[u8]) -> Result<ImageCapacity, ApfsError> {
    let value = plist::Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| ApfsError::InvalidResizeLimits(error.to_string()))?;
    value
        .as_dictionary()
        .and_then(|root| root.get("current"))
        .and_then(plist::Value::as_unsigned_integer)
        .map(ImageCapacity::from_bytes)
        .ok_or_else(|| ApfsError::InvalidResizeLimits("missing unsigned current".to_owned()))
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
fn detach_was_dissented(error: &ApfsError) -> bool {
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

fn attachment_inventory_path(image: &Path) -> Result<PathBuf, ApfsError> {
    std::path::absolute(image).map_err(|source| ApfsError::FileOperation {
        operation: "resolve attachment inventory path",
        path: image.to_owned(),
        source,
    })
}

fn existing_image_attachment(
    image: &Path,
    images: &[AttachedDiskImage],
) -> Result<Option<RecoveredImageAttachment>, ApfsError> {
    let mut attachment = None;
    for attached in images
        .iter()
        .filter(|attached| attached.is_backed_by(image))
    {
        let entities: Vec<(String, String)> = attached
            .media
            .iter()
            .map(|media| {
                (
                    device_path(&media.device).unwrap_or_else(|| media.device.clone()),
                    media.content.clone(),
                )
            })
            .collect();
        let found = match entities.as_slice() {
            [(device, hint)]
                if device_depth(device) == 0
                    && is_kernel_device_path(device)
                    && (hint.is_empty() || hint == "GUID_partition_scheme") =>
            {
                RecoveredImageAttachment::Unformatted {
                    image: image.to_owned(),
                    whole_device: device.clone(),
                }
            }
            _ => {
                let (whole_device, volume_device) = attached_apfs_volume(&entities)
                    .map_err(ApfsError::InvalidAttachmentInventory)?;
                RecoveredImageAttachment::Apfs(AttachedImage {
                    image: image.to_owned(),
                    whole_device,
                    volume_device,
                    pin: std::sync::Mutex::new(None),
                })
            }
        };
        if attachment.replace(found).is_some() {
            return Err(ApfsError::InvalidAttachmentInventory(
                "matching image has multiple inventory entries".into(),
            ));
        }
    }
    Ok(attachment)
}

/// Every whole device the kernel currently attaches for any disk image. The detach-settle
/// check keys on the device rather than scoping by image: after a detach there may be no image
/// to scope by, and the question is whether the device is gone at all. A node that is not a
/// kernel device name names no device to wait on.
fn inventory_devices(images: &[AttachedDiskImage]) -> BTreeSet<String> {
    images
        .iter()
        .flat_map(|attached| &attached.media)
        .filter_map(|media| device_path(&media.device))
        .filter(|device| device_depth(device) == 0)
        .collect()
}

fn is_canonical_mount_point(path: &Path) -> bool {
    let bytes = path.as_os_str().as_encoded_bytes();
    bytes.starts_with(b"/")
        && bytes != b"/"
        && !bytes.contains(&0)
        && !path.starts_with("/dev")
        && bytes[1..]
            .split(|byte| *byte == b'/')
            .all(|segment| !segment.is_empty() && segment != b"." && segment != b"..")
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

/// Flush one mounted volume and wait for it: the source's dirty data, and nobody else's.
#[cfg(target_os = "macos")]
fn sync_volume(mount_point: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        fn sync_volume_np(path: *const libc::c_char, flags: libc::c_int) -> libc::c_int;
    }
    const SYNC_VOLUME_WAIT: libc::c_int = 0x02;
    let path = std::ffi::CString::new(mount_point.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount point contains NUL"))?;
    // SAFETY: `path` is NUL-terminated and outlives the call; the flags ask only to wait.
    match unsafe { sync_volume_np(path.as_ptr(), SYNC_VOLUME_WAIT) } {
        0 => Ok(()),
        // It answers the errno itself, or -1 with errno set, depending on where it failed.
        -1 => Err(io::Error::last_os_error()),
        errno => Err(io::Error::from_raw_os_error(errno)),
    }
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

fn parse_blank_asif_whole_device(bytes: &[u8]) -> Result<String, ApfsError> {
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
    attached_apfs_volume(&collect_attachment_entities(system_entities)?)
        .map_err(ApfsError::InvalidAttachmentPlist)
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
/// Every image holds one volume in one container, and both reports of an attachment name the
/// two: `diskutil image attach --plist` with string hints (`Apple_APFS_Container`,
/// `Apple_APFS_Volume`), the kernel inventory with Apple's partition-type GUIDs. The answer
/// therefore needs no Disk Arbitration query: `diskutil info` and `diskutil apfs list` queue
/// behind every arbitration client on the host, and under contention answered before Disk
/// Arbitration had announced the attach at all. Anything but exactly one volume inside a
/// reported container is refused, naming what was reported.
fn attached_apfs_volume(entities: &[(String, String)]) -> Result<(String, String), String> {
    let volumes: Vec<&str> = entities
        .iter()
        .filter(|(_, hint)| is_apfs_volume_hint(hint))
        .map(|(device, _)| device.as_str())
        .collect();
    let [volume] = volumes.as_slice() else {
        return Err(format!("expected one APFS volume, reported {volumes:?}"));
    };
    let container = whole_device_from(volume)
        .ok_or_else(|| format!("APFS volume {volume} is not a slice of a container"))?;
    if !entities
        .iter()
        .any(|(device, hint)| *device == container && is_apfs_container_hint(hint))
    {
        return Err(format!(
            "APFS volume {volume} was reported without its container {container}"
        ));
    }
    Ok((container, (*volume).to_owned()))
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

fn clonefile_native(source: &Path, destination: &Path) -> Result<(), CloneFileError> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        // SAFETY: `clonefile(2)` is Darwin libc; the signature matches
        // `<sys/clonefile.h>` (`src`, `dst` as NUL-terminated paths, `flags` as
        // `uint32_t`). We only call it with `CString` pointers that outlive the
        // invocation.
        unsafe extern "C" {
            fn clonefile(
                src: *const std::ffi::c_char,
                dst: *const std::ffi::c_char,
                flags: u32,
            ) -> std::ffi::c_int;
        }
        let src = CString::new(source.as_os_str().as_bytes()).map_err(|_| CloneFileError::Io {
            source_path: source.to_owned(),
            destination_path: destination.to_owned(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "source path contains NUL"),
        })?;
        let dst =
            CString::new(destination.as_os_str().as_bytes()).map_err(|_| CloneFileError::Io {
                source_path: source.to_owned(),
                destination_path: destination.to_owned(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "destination path contains NUL",
                ),
            })?;
        // SAFETY: `src`/`dst` are `CString`s, so both pointers are valid
        // NUL-terminated paths for the duration of the call. flags `0` is
        // Darwin's default clonefile (follow symlinks, copy ownership).
        let result = unsafe { clonefile(src.as_ptr(), dst.as_ptr(), 0) };
        if result == 0 {
            return Ok(());
        }
        Err(classify_clone_error(
            source,
            destination,
            io::Error::last_os_error(),
        ))
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = (source, destination);
        Err(CloneFileError::UnsupportedPlatform)
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

/// The kernel's I/O Registry view of attached disk images.
///
/// One `IOServiceGetMatchingServices` call answers every IOMedia node the kernel has registered.
/// The kernel collects those matches under the registry's own consistency check — it restarts
/// its walk whenever a concurrent attach or detach invalidates it — so the set is complete for
/// one instant, which is exactly what `hdiutil info` is not. Each node is then walked up its
/// provider chain to the image driver's `AppleDiskImageDevice`, whose `DiskImageURL` names the
/// backing store. A live node's provider links do not change; a node already detached from the
/// plane is departing and is not reported.
#[cfg(target_os = "macos")]
mod io_registry {
    use super::{AttachedDiskImage, DiskImageSource, ImageMedia};
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
        fn IOObjectRelease(object: IoObject) -> KernReturn;
        fn IOObjectConformsTo(object: IoObject, class_name: *const c_char) -> libc::c_int;
        fn IORegistryEntryGetParentEntry(
            entry: IoObject,
            plane: *const c_char,
            parent: *mut IoObject,
        ) -> KernReturn;
        fn IORegistryEntryGetRegistryEntryID(entry: IoObject, entry_id: *mut u64) -> KernReturn;
        fn IORegistryEntryCreateCFProperty(
            entry: IoObject,
            key: CfType,
            allocator: CfType,
            options: u32,
        ) -> CfType;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFRelease(object: CfType);
        fn CFGetTypeID(object: CfType) -> usize;
        fn CFStringGetTypeID() -> usize;
        fn CFNumberGetTypeID() -> usize;
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

    pub(super) fn attached_disk_images() -> io::Result<Vec<AttachedDiskImage>> {
        let keys = Keys {
            bsd_name: key("BSD Name")?,
            content: key("Content")?,
            size: key("Size")?,
            url: key("DiskImageURL")?,
        };
        // SAFETY: the class name is NUL-terminated; a null answer is refused below.
        let matching = unsafe { IOServiceMatching(c"IOMedia".as_ptr()) };
        if matching.is_null() {
            return Err(io::Error::other(
                "IOServiceMatching(IOMedia) returned no matching dictionary",
            ));
        }
        let mut iterator: IoObject = 0;
        // SAFETY: `matching` is a +1 dictionary the call consumes whatever it returns, and
        // `iterator` is writable.
        let status =
            unsafe { IOServiceGetMatchingServices(MAIN_PORT_DEFAULT, matching, &mut iterator) };
        if status != KERN_SUCCESS {
            return Err(kernel_error("match registered IOMedia", status));
        }
        let iterator = Object(iterator);
        let mut images: BTreeMap<u64, AttachedDiskImage> = BTreeMap::new();
        loop {
            // SAFETY: `iterator` is a live matching iterator; each object it yields is +1.
            let next = unsafe { IOIteratorNext(iterator.0) };
            if next == 0 {
                break;
            }
            let media = Object(next);
            let Some(name) = string_property(&media, &keys.bsd_name, "BSD Name")? else {
                continue;
            };
            let Some(owner) = image_driver_of(&media)? else {
                continue;
            };
            let image = match images.entry(owner.id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let url = string_property(&owner.driver, &keys.url, "DiskImageURL")?
                        .ok_or_else(|| {
                            invalid(format!(
                                "the disk image exposing {name} registers no DiskImageURL"
                            ))
                        })?;
                    entry.insert(AttachedDiskImage {
                        source: DiskImageSource::from_url(&url),
                        capacity: None,
                        media: Vec::new(),
                    })
                }
            };
            if owner.is_whole {
                image.capacity = size_property(&media, &keys.size)?.map(ImageCapacity::from_bytes);
            }
            let content = string_property(&media, &keys.content, "Content")?.unwrap_or_default();
            image.media.push(ImageMedia {
                device: format!("/dev/{name}"),
                content,
            });
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

    fn image_driver_of(media: &Object) -> io::Result<Option<ImageDriver>> {
        let mut intervening_media = 0usize;
        let mut current = provider(media)?;
        while let Some(entry) = current {
            if conforms(&entry, c"AppleDiskImageDevice") {
                let mut id = 0u64;
                // SAFETY: `entry` is a live registry reference and `id` is writable.
                let status = unsafe { IORegistryEntryGetRegistryEntryID(entry.0, &mut id) };
                if status != KERN_SUCCESS {
                    return Err(kernel_error("read a disk image's registry id", status));
                }
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

    fn provider(entry: &Object) -> io::Result<Option<Object>> {
        let mut parent: IoObject = 0;
        // SAFETY: `entry` is live, the plane name is NUL-terminated, and `parent` is writable.
        match unsafe { IORegistryEntryGetParentEntry(entry.0, SERVICE_PLANE.as_ptr(), &mut parent) }
        {
            KERN_SUCCESS => Ok(Some(Object(parent))),
            IO_RETURN_NO_DEVICE => Ok(None),
            status => Err(kernel_error("read a registry provider", status)),
        }
    }

    fn conforms(entry: &Object, class: &CStr) -> bool {
        // SAFETY: `entry` is live and `class` is NUL-terminated.
        unsafe { IOObjectConformsTo(entry.0, class.as_ptr()) != 0 }
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

    fn property(entry: &Object, key: &Cf) -> Option<Cf> {
        // SAFETY: `entry` is live and `key` is a CFString; the answer is +1 or null.
        let value = unsafe { IORegistryEntryCreateCFProperty(entry.0, key.0, ptr::null(), 0) };
        // A null answer must never be wrapped: the wrapper releases what it holds.
        if value.is_null() {
            None
        } else {
            Some(Cf(value))
        }
    }

    fn string_property(entry: &Object, key: &Cf, name: &str) -> io::Result<Option<String>> {
        let Some(value) = property(entry, key) else {
            return Ok(None);
        };
        // SAFETY: `value` is a live CF object.
        if unsafe { CFGetTypeID(value.0) != CFStringGetTypeID() } {
            return Err(invalid(format!("registry property {name} is not a string")));
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
            )));
        }
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| invalid(format!("registry property {name} is not UTF-8")))
    }

    fn size_property(entry: &Object, key: &Cf) -> io::Result<Option<u64>> {
        let Some(value) = property(entry, key) else {
            return Ok(None);
        };
        // SAFETY: `value` is a live CF object.
        if unsafe { CFGetTypeID(value.0) != CFNumberGetTypeID() } {
            return Err(invalid("registry property Size is not a number"));
        }
        let mut size = 0i64;
        // SAFETY: `value` is a live CFNumber and `size` is writable storage for an SInt64.
        if unsafe { CFNumberGetValue(value.0, CF_NUMBER_SINT64_TYPE, (&raw mut size).cast()) } == 0
        {
            return Err(invalid(
                "registry property Size is not an exact 64-bit integer",
            ));
        }
        u64::try_from(size)
            .map(Some)
            .map_err(|_| invalid("registry property Size is negative"))
    }

    fn invalid(message: impl Into<String>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, message.into())
    }

    fn kernel_error(operation: &str, status: KernReturn) -> io::Error {
        io::Error::other(format!("{operation}: IOKit returned {status:#010x}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Ref, RefCell};
    use std::collections::{BTreeMap, VecDeque};

    const BLANK_ASIF_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
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
    }

    /// One thing the backend asked the host, in order.
    #[derive(Clone, Debug, PartialEq)]
    enum Seen {
        Command(CommandRequest),
        Inventory,
    }

    impl Seen {
        fn command(&self) -> &CommandRequest {
            match self {
                Self::Command(request) => request,
                Self::Inventory => panic!("expected a command, saw the kernel inventory read"),
            }
        }
    }

    fn commands(steps: &[Seen]) -> Vec<CommandRequest> {
        steps
            .iter()
            .filter_map(|step| match step {
                Seen::Command(request) => Some(request.clone()),
                Seen::Inventory => None,
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
        fn host_device_lease(&self) -> io::Result<Option<File>> {
            Ok(None)
        }
        fn pin_raw_device(&self, _: &Path) -> io::Result<Option<File>> {
            Ok(None)
        }
        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            match self.next_reply(Seen::Inventory) {
                Reply::Inventory(images) => images,
                other => panic!("test scripted {other:?} where the kernel inventory was read"),
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
                PathBuf::from("/tmp/cowshed-unrelated.asif"),
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
                    && args == ["detach", self.new_device.as_str()],
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
        fn host_device_lease(&self) -> io::Result<Option<File>> {
            Ok(None)
        }
        fn pin_raw_device(&self, _: &Path) -> io::Result<Option<File>> {
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
            match self.backend.recovered_image_attachment(&self.image)? {
                Some(RecoveredImageAttachment::Apfs(attachment)) => {
                    self.backend.detach(&attachment, DetachIntent::Release)
                }
                Some(RecoveredImageAttachment::Unformatted {
                    image,
                    whole_device,
                }) => self.backend.detach_unformatted_image(
                    &image,
                    &whole_device,
                    DetachIntent::Release,
                ),
                None if !self.image.exists() => Ok(()),
                None => Err(ApfsError::InvalidAttachmentInventory(format!(
                    "cannot prove fixture image {} was released; retaining its backing file",
                    self.image.display()
                ))),
            }
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
        // Hang injection: a shell-builtin spin stands in for a hung disk-image child
        // that never answers. Absolute coreutils paths (`/bin/sleep`, `/bin/echo`)
        // do not exist on all Linux runners (NixOS provides only `/bin/sh`), so
        // the hang and the follow-up probe must use `/bin/sh` builtins only.
        // The store must kill the child at the deadline, mark the item deferred,
        // and let the queue continue — never wait forever.
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
        let main = PathBuf::from("/store/acme/main.asif");
        let session = PathBuf::from("/store/acme/session.asif");
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
                    "/store/acme/main.asif"
                ],
            )),
            Some(PathBuf::from("/store/acme/main.asif"))
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
            "only an ASIF image path is a workspace image"
        );
        assert_eq!(
            deferred_image_target(&CommandRequest::new("/bin/sleep", ["30"])),
            None
        );
    }

    #[test]
    fn rejects_a_path_that_is_not_an_asif_image_before_spawning() {
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
            holding_verified_volume("session.asif"),
            failed(8, "not clean"),
            holding("session.asif", "/dev/disk5"),
            ok([]),
            no_images(),
        ]));
        let error = backend
            .attach_verified(Path::new("session.asif"))
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
        assert_eq!(argv(steps[5].command()), ["detach", "/dev/disk5"]);
        assert!(
            !commands(&steps)
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "mount"))
        );
    }

    #[test]
    fn parsed_attach_never_detaches_a_preexisting_same_image_device() {
        let image = Path::new("/tmp/cowshed-preexisting.asif");
        let inventory = || images(&[("/tmp/cowshed-preexisting.asif", &["/dev/disk5"][..])]);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            inventory(),
            ok(ATTACH_PLIST),
            inventory(),
            inventory(),
        ]));

        let error = backend.attach_verified(image).unwrap_err();

        assert!(matches!(
            error,
            ApfsError::InvalidAttachmentPlist(message)
                if message == "attach reported a pre-existing whole image device"
        ));
        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0], Seen::Inventory);
        assert_eq!(steps[2], Seen::Inventory);
        assert_eq!(steps[3], Seen::Inventory);
        assert!(
            !commands(&steps)
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "detach"))
        );
    }

    #[test]
    fn malformed_asif_attach_detaches_only_the_new_device_and_verifies_absence() {
        let image = Path::new("/tmp/cowshed-malformed-attach.asif");
        let backend = MacOsApfsBackend::new(StatefulMalformedAttachRunner::new(
            image,
            &["/dev/disk4"],
            false,
        ));

        let error = backend.attach_verified(image).unwrap_err();

        assert!(matches!(error, ApfsError::InvalidAttachmentPlist(_)));
        assert_eq!(
            backend.runner().devices_for(image),
            BTreeSet::from(["/dev/disk4".into()])
        );
        assert_eq!(
            backend
                .runner()
                .devices_for(Path::new("/tmp/cowshed-unrelated.asif")),
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
                "/tmp/cowshed-malformed-attach.asif",
            ]
        );
        assert_eq!(steps[2], Seen::Inventory);
        assert_eq!(steps[3], Seen::Inventory);
        assert_eq!(argv(steps[4].command()), ["detach", "/dev/disk8"]);
        assert_eq!(steps[6], Seen::Inventory);
        assert!(!commands(&steps).iter().any(|request| {
            let args = argv(request);
            args.iter()
                .any(|arg| arg == "/dev/disk4" || arg == "/dev/disk20")
        }));
    }

    #[test]
    fn malformed_blank_asif_cleanup_failure_preserves_image_and_typed_context() {
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
        assert_eq!(argv(steps[5].command()), ["detach", "/dev/disk8"]);
        assert_eq!(steps[6], Seen::Inventory);
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn failed_detach_remains_a_cleanup_error_when_inventory_reports_absence() {
        let image = Path::new("/tmp/cowshed-failed-detach.asif");
        let attached = || images(&[("/tmp/cowshed-failed-detach.asif", &["/dev/disk8"][..])]);
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
        assert_eq!(argv(steps[4].command()), ["detach", "/dev/disk8"]);
        assert_eq!(steps[5], Seen::Inventory);
    }

    #[test]
    fn malformed_blank_asif_inventory_failures_preserve_the_image() {
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
    fn post_create_asif_attach_failure_removes_the_image() {
        let stem = temp_path("asif-attach-failure", "stem").with_extension("");
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
                operation: "attach blank ASIF image",
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
        let stem = temp_path("asif-missing-cleanup", "stem").with_extension("");
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
                operation: "attach blank ASIF image",
                output: CommandOutput {
                    status: ProcessStatus::Exit(9),
                    ..
                },
                ..
            }
        ));
        assert_eq!(backend.runner().steps().len(), 5);
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
            .detach_unformatted_image(&image, "/dev/disk8", DetachIntent::Release)
            .unwrap();
        assert_eq!(fs::read(&image).unwrap(), b"unformatted");
        assert!(
            backend.runner().requests().iter().any(|request| {
                request.program == Path::new(HDIUTIL) && argv(request) == ["detach", "/dev/disk8"]
            }),
            "positive fresh image ownership permits only its own device release"
        );
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn changed_unformatted_media_mapping_never_releases_another_device() {
        let image = temp_path("unformatted-changed-map", IMAGE_EXTENSION);
        fs::write(&image, b"unformatted").unwrap();
        let backend = graced_backend([holding(&image, "/dev/disk9")]);
        let error = backend
            .detach_unformatted_image(&image, "/dev/disk8", DetachIntent::Release)
            .unwrap_err();
        assert!(matches!(error, ApfsError::InvalidAttachmentInventory(_)));
        assert_eq!(fs::read(&image).unwrap(), b"unformatted");
        assert!(
            !backend
                .runner()
                .requests()
                .iter()
                .any(|request| { request.args.first().is_some_and(|arg| arg == "detach") }),
            "a cached device name is not ownership evidence"
        );
        fs::remove_file(image).unwrap();
    }

    #[test]
    fn a_second_observation_conflict_cannot_claim_unformatted_media_was_released() {
        let image = temp_path("unformatted-second-map", IMAGE_EXTENSION);
        fs::write(&image, b"unformatted").unwrap();
        let backend =
            graced_backend([holding(&image, "/dev/disk8"), holding(&image, "/dev/disk9")]);
        let error = backend
            .detach_unformatted_image(&image, "/dev/disk8", DetachIntent::Release)
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
        let stem = temp_path("asif-kernel-release", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = graced_backend([
            ok([]),
            no_images(),
            ok(BLANK_ASIF_PLIST),
            holding(&image, "/dev/disk8"),
            ok([]),
            holding(&image, "/dev/disk8"),
            ok([]),
            no_images(),
        ]);
        backend.create_staged_image(&blank_request(stem)).unwrap();

        let steps = backend.runner().steps();
        assert_eq!(steps.len(), 8);
        assert_eq!(steps[4].command().program, Path::new(NEWFS_APFS));
        assert_eq!(steps[5], Seen::Inventory);
        assert_eq!(argv(steps[6].command()), ["detach", "/dev/disk8"]);
        assert_eq!(steps[7], Seen::Inventory);
        assert!(
            !commands(&steps)
                .iter()
                .any(|request| argv(request).first().is_some_and(|arg| arg == "info")),
            "no attachment decision reads `hdiutil info`"
        );
        assert!(backend.sleeper.waits().is_empty());
    }

    /// An unreadable kernel inventory at the final release is a typed refusal, never a release.
    #[test]
    fn an_unreadable_inventory_at_release_is_never_treated_as_released() {
        let stem = temp_path("asif-unreadable-release", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = graced_backend([
            ok([]),
            no_images(),
            ok(BLANK_ASIF_PLIST),
            holding(&image, "/dev/disk8"),
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
        let stem = temp_path("asif-foreign-publication", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"created").unwrap();
        let backend = graced_backend([
            ok([]),
            no_images(),
            ok(BLANK_ASIF_PLIST),
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
        let stem = temp_path("asif-unregistered-attachment", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"created").unwrap();
        let backend = graced_backend([ok([]), no_images(), ok(BLANK_ASIF_PLIST), no_images()]);
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
    fn failed_newfs_detaches_and_removes_staged_asif() {
        let stem = temp_path("asif-newfs-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
            no_images(),
            ok(BLANK_ASIF_PLIST),
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
                operation: "format ASIF APFS volume",
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
        assert_eq!(argv(steps[6].command()), ["detach", "/dev/disk8"]);
        assert_eq!(steps[7], Seen::Inventory);
    }

    #[test]
    fn failed_newfs_preserves_image_when_detach_cleanup_fails() {
        let stem = temp_path("asif-detach-cleanup-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"partial").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
            no_images(),
            ok(BLANK_ASIF_PLIST),
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
            ApfsError::AsifCreationAndCleanupFailed {
                primary,
                detach: Some(detach),
                remove: None,
            } if matches!(
                *primary,
                ApfsError::CommandFailed {
                    operation: "format ASIF APFS volume",
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
        let stem = temp_path("asif-remove-cleanup-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::create_dir(&image).unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
            no_images(),
            ok(BLANK_ASIF_PLIST),
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
            ApfsError::AsifCreationAndCleanupFailed {
                primary,
                detach: None,
                remove: Some(remove),
            } if matches!(
                *primary,
                ApfsError::CommandFailed {
                    operation: "format ASIF APFS volume",
                    ..
                }
            ) && matches!(
                *remove,
                ApfsError::FileOperation {
                    operation: "remove failed ASIF image",
                    ..
                }
            )
        ));
        fs::remove_dir(image).unwrap();
    }

    #[test]
    fn failed_final_asif_eject_preserves_attached_image() {
        let stem = temp_path("asif-final-eject-failure", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        fs::write(&image, b"formatted").unwrap();
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok([]),
            no_images(),
            ok(BLANK_ASIF_PLIST),
            holding(&image, "/dev/disk8"),
            ok([]),
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
    fn an_invalid_clone_is_refused_before_anything_is_flushed() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        let error = backend
            .sync_and_clone(
                Path::new("main.asif"),
                None,
                Path::new("session.sparseimage"),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ApfsError::Clone(CloneFileError::InvalidImagePath { .. })
        ));
        assert!(
            backend.runner().requests().is_empty(),
            "the freshness flush is two syscalls on the source, never a host-wide sync child"
        );
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
    fn clone_validation_requires_both_paths_to_be_asif_images() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for (source, destination, invalid) in [
            ("main.sparseimage", "session.asif", "main.sparseimage"),
            ("main.asif", "session.sparseimage", "session.sparseimage"),
        ] {
            let error = backend
                .clone_image(Path::new(source), Path::new(destination))
                .unwrap_err();
            assert!(matches!(
                error,
                CloneFileError::InvalidImagePath { path } if path == Path::new(invalid)
            ));
        }

        let source = temp_path("validated-missing-source", IMAGE_EXTENSION);
        let destination = temp_path("validated-missing-destination", IMAGE_EXTENSION);
        let error = backend.clone_image(&source, &destination).unwrap_err();
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
        let backend = graced_backend([images(&[("/tmp/someone-else.asif", &["/dev/disk12"][..])])]);

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
        let lingering = || images(&[("/tmp/cowshed-lingering.asif", &["/dev/disk12"][..])]);
        let attachment = identity_attachment("settle-polls");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            ok([]),
            lingering(),
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
        let lingering = || images(&[("/tmp/cowshed-lingering.asif", &["/dev/disk12"][..])]);
        let attachment = identity_attachment("settle-bound");
        let backend = graced_backend([
            holding(&attachment.image, &attachment.whole_device),
            ok([]),
            lingering(),
            lingering(),
            lingering(),
        ]);
        backend.detach(&attachment, DetachIntent::Release).unwrap();
        assert_eq!(backend.runner().steps().len(), 5);
        assert_eq!(
            *backend.sleeper.waits(),
            [Duration::from_millis(10), Duration::from_millis(10)],
            "two polls spend the 20ms bound, then the bound stops the wait"
        );
    }

    #[test]
    fn inventory_devices_collects_whole_devices_across_images() {
        let devices = inventory_devices(&attached_images(&[
            ("/tmp/a.asif", &["/dev/disk4"][..]),
            ("/tmp/b.asif", &["disk5s1", "/dev/disk5"][..]),
            // A node that is not a kernel device name names nothing to wait on.
            ("/tmp/c.asif", &["not-a-device"][..]),
        ]));
        assert_eq!(
            devices,
            BTreeSet::from(["/dev/disk4".to_owned(), "/dev/disk5".to_owned()])
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
            assert_eq!(argv(request), ["detach", "/dev/disk4"]);
        }
        assert_eq!(argv(&requests[3]), ["detach", "-force", "/dev/disk4"]);
        assert_eq!(
            *backend.sleeper.waits(),
            [Duration::from_millis(10), Duration::from_millis(10)]
        );
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

    #[test]
    fn rename_volume_records_checked_diskutil_rename_volume_command() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([ok([])]));

        backend
            .rename_volume(
                Path::new("/Volumes/cowshed-stage"),
                "cowshed.acme--widget.main",
            )
            .unwrap();

        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, Path::new(DISKUTIL));
        assert_eq!(
            argv(&requests[0]),
            [
                "renameVolume",
                "/Volumes/cowshed-stage",
                "cowshed.acme--widget.main",
            ]
        );
    }

    #[test]
    fn rename_volume_accepts_a_255_byte_path_safe_name() {
        let name = "a".repeat(255);
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([ok([])]));

        backend
            .rename_volume(Path::new("/Volumes/cowshed-stage"), &name)
            .unwrap();

        let requests = backend.runner().requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].args[2], OsString::from(name));
    }

    #[test]
    fn rename_volume_rejects_noncanonical_mountpoints_before_spawning() {
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for mount_point in [
            "",
            ".",
            "Volumes/main",
            "/",
            "/dev",
            "/dev/disk12s3",
            "/Volumes/../private/tmp",
            "/Volumes/./main",
            "/Volumes//main",
            "/Volumes/main/",
            "/Volumes/\0main",
        ] {
            let error = backend
                .rename_volume(Path::new(mount_point), "main")
                .unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidMountPoint(path) if path == Path::new(mount_point)
            ));
        }
        assert!(backend.runner().requests().is_empty());
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
        let backend = MacOsApfsBackend::new(RecordingRunner::default());
        for name in invalid {
            let error = backend
                .rename_volume(Path::new("/Volumes/cowshed-stage"), &name)
                .unwrap_err();
            assert!(matches!(
                error,
                ApfsError::InvalidVolumeName(rejected) if rejected == name
            ));
        }
        assert!(backend.runner().requests().is_empty());
    }

    #[test]
    fn rename_volume_propagates_checked_diskutil_failure() {
        let backend =
            MacOsApfsBackend::new(RecordingRunner::with_outputs([failed(7, "rename failed")]));

        let error = backend
            .rename_volume(Path::new("/Volumes/cowshed-stage"), "main")
            .unwrap_err();

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
                && argv(&request) == ["renameVolume", "/Volumes/cowshed-stage", "main"]
        ));
        assert_eq!(backend.runner().requests().len(), 1);
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

    #[cfg(target_os = "macos")]
    #[test]
    fn real_apfs_asif_attach_normalizes_bare_devices_and_verifies_the_volume() {
        let stem = temp_path("real-asif-resolution", "stem").with_extension("");
        let image = stem.with_extension(IMAGE_EXTENSION);
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        let mut cleanup = RealImageCleanup::new(&backend, image);
        let result = (|| -> Result<(), ApfsError> {
            let created = backend.create_staged_image(&CreateImageRequest {
                staged_stem: stem,
                capacity: capacity("64m"),
                volume_name: "cowshed-asif-resolution".into(),
                // SAFETY: `getuid`/`getgid` read this process's credentials;
                // they take no pointers and cannot fail.
                owner_uid: unsafe { libc::getuid() },
                // SAFETY: `getgid` reads this process's credentials; it takes no
                // pointers and cannot fail.
                owner_gid: unsafe { libc::getgid() },
            })?;
            let attachment = backend.attach_verified(&created)?;
            let attachment = cleanup.track(attachment);
            assert!(attachment.whole_device().starts_with("/dev/disk"));
            assert!(attachment.volume_device().starts_with("/dev/disk"));
            Ok(())
        })();
        finish_real_image_test(result, cleanup);
    }

    #[test]
    fn blank_asif_plist_requires_one_canonical_whole_device() {
        assert_eq!(
            parse_blank_asif_whole_device(BLANK_ASIF_PLIST.as_bytes()).unwrap(),
            "/dev/disk8"
        );
        let missing = br#"<?xml version="1.0"?><plist><dict>
          <key>system-entities</key><array>
            <dict><key>dev-entry</key><string>disk8s1</string></dict>
          </array>
        </dict></plist>"#;
        assert!(matches!(
            parse_blank_asif_whole_device(missing),
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
            parse_blank_asif_whole_device(ambiguous),
            Err(ApfsError::InvalidAttachmentPlist(message))
                if message == "multiple whole image devices"
        ));
    }

    #[test]
    fn attachment_inventory_selects_only_exact_image_path_whole_devices() {
        let images = attached_images(&[
            (
                "/tmp/cowshed-target.asif",
                &["disk4", "/dev/disk4s1", "/dev/disk5"][..],
            ),
            ("/tmp/cowshed-unrelated.asif", &["/dev/disk20"][..]),
        ]);

        let parsed = image_inventory(Path::new("/tmp/cowshed-target.asif"), &images).unwrap();
        assert_eq!(
            parsed.devices,
            BTreeSet::from(["/dev/disk4".into(), "/dev/disk5".into()])
        );
        assert!(parsed.matched);
        assert_eq!(parsed.capacity, None);
        let absent = image_inventory(Path::new("/tmp/cowshed-absent.asif"), &images).unwrap();
        assert!(!absent.matched);
        assert!(absent.devices.is_empty());
    }

    /// One attached ASIF image as the I/O Registry records it, with the device numbers and
    /// partition-type GUID contents of a live macOS 26.6 capture: the image's own store, then the
    /// synthesized container and its volume.
    fn live_capture() -> AttachedDiskImage {
        AttachedDiskImage::file(
            "/tmp/cowshed-target.asif",
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
            .existing_attachment(Path::new("/tmp/cowshed-target.asif"))
            .expect("inventory")
            .expect("existing attachment");
        assert_eq!(attachment.whole_device(), "/dev/disk15");
        assert_eq!(attachment.volume_device(), "/dev/disk15s1");
        assert_eq!(
            backend
                .attached_capacity(Path::new("/tmp/cowshed-target.asif"))
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
                .existing_attachment(Path::new("/tmp/cowshed-target.asif"))
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            backend.attached_capacity(Path::new("/tmp/cowshed-target.asif")),
            Err(ApfsError::ImageNotAttached(path)) if path == Path::new("/tmp/cowshed-target.asif")
        ));
    }

    /// Foreign, ambiguous, malformed and unreadable inventories fail closed.
    #[test]
    fn foreign_ambiguous_and_unreadable_inventories_fail_closed() {
        let image = Path::new("/tmp/cowshed-target.asif");
        let foreign = [
            AttachedDiskImage {
                source: DiskImageSource::Url("ram://2048".into()),
                capacity: None,
                media: live_capture().media,
            },
            AttachedDiskImage::file("/tmp/cowshed-other.asif", None, [("/dev/disk20", "")]),
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
            DiskImageSource::from_url("file:///private/tmp/a%20b.asif"),
            DiskImageSource::File(PathBuf::from("/private/tmp/a b.asif"))
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
            existing_image_attachment(Path::new("/tmp/cowshed-target.asif"), &[live_capture()])
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
                &[("disk4", ""), ("disk5", "Apple_APFS_Container")][..],
                "expected one APFS volume, reported []",
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
    }

    #[test]
    fn attachment_inventory_rejects_malformed_matching_records() {
        let image = Path::new("/tmp/cowshed-target.asif");
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
        let image = Path::new("/tmp/cowshed-target.asif");
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

    const ASIF_RESIZE_LIMITS_PLIST: &str = r#"<?xml version="1.0"?><plist version="1.0"><dict>
      <key>current</key><integer>107374182400</integer>
      <key>max</key><integer>4503599626321920</integer>
      <key>min</key><integer>20971520</integer>
    </dict></plist>"#;

    #[test]
    fn asif_resize_uses_the_diskutil_image_verbs_for_both_limits_and_growth() {
        let image = Path::new("/tmp/cowshed-resize/main.asif");
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            ok(ASIF_RESIZE_LIMITS_PLIST),
            ok([]),
        ]));

        assert_eq!(backend.image_capacity(image).unwrap(), capacity("100g"));
        backend.resize_image(image, capacity("200g")).unwrap();

        let requests = backend.runner().requests();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.program.as_path())
                .collect::<Vec<_>>(),
            [Path::new(DISKUTIL), Path::new(DISKUTIL)]
        );
        assert_eq!(
            argv(&requests[0]),
            [
                "image",
                "resize",
                "--plist",
                "/tmp/cowshed-resize/main.asif"
            ]
        );
        assert_eq!(
            argv(&requests[1]),
            [
                "image",
                "resize",
                "--size",
                "214748364800",
                "/tmp/cowshed-resize/main.asif"
            ]
        );
    }

    #[test]
    fn resize_refuses_a_path_that_is_not_an_asif_image() {
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
                holding_verified_volume("/tmp/cowshed-resize/main.asif"),
                ok([]),
                failed(1, refusal),
            ]));
            let attachment = backend
                .attach_verified(Path::new("/tmp/cowshed-resize/main.asif"))
                .unwrap();
            backend.grow_container(&attachment).unwrap();
        }
    }

    #[test]
    fn a_container_growth_failure_that_is_not_an_already_full_refusal_propagates() {
        let backend = MacOsApfsBackend::new(RecordingRunner::with_outputs([
            no_images(),
            ok(ATTACH_PLIST),
            holding_verified_volume("/tmp/cowshed-resize/main.asif"),
            ok([]),
            failed(1, "Error: -69620: The given file system is not supported"),
        ]));
        let attachment = backend
            .attach_verified(Path::new("/tmp/cowshed-resize/main.asif"))
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
            Path::new("main.asif"),
            Path::new("session.asif"),
            io::Error::from_raw_os_error(17),
        );
        assert!(matches!(
            destination,
            CloneFileError::DestinationExists { destination }
                if destination == Path::new("session.asif")
        ));

        let other = classify_clone_error(
            Path::new("main.asif"),
            Path::new("session.asif"),
            io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
        );
        assert!(matches!(
            &other,
            CloneFileError::Io {
                source_path,
                destination_path,
                source,
            } if source_path == Path::new("main.asif")
                && destination_path == Path::new("session.asif")
                && source.kind() == io::ErrorKind::PermissionDenied
        ));
        assert_eq!(
            std::error::Error::source(&other).unwrap().to_string(),
            "denied"
        );
    }

    #[test]
    fn clonefile_reports_a_missing_source_without_creating_destination() {
        let source = temp_path("missing-clone-source", IMAGE_EXTENSION);
        let destination = temp_path("missing-clone-destination", IMAGE_EXTENSION);
        let error = clonefile_native(&source, &destination).unwrap_err();
        #[cfg(target_os = "macos")]
        assert!(matches!(error, CloneFileError::Io { .. }));
        #[cfg(not(target_os = "macos"))]
        assert!(matches!(error, CloneFileError::UnsupportedPlatform));
        assert!(!destination.exists());
    }

    #[test]
    fn clonefile_cross_volume_error_is_typed() {
        let error = classify_clone_error(
            Path::new("main.asif"),
            Path::new("session.asif"),
            io::Error::from_raw_os_error(18),
        );
        assert!(matches!(error, CloneFileError::CrossVolume { .. }));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn clonefile_creates_an_independent_same_volume_image_file() {
        let nonce = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let source = std::env::temp_dir().join(format!("cowshed-clone-source-{nonce}.asif"));
        let destination =
            std::env::temp_dir().join(format!("cowshed-clone-destination-{nonce}.asif"));
        fs::write(&source, b"fresh image bytes").unwrap();

        clonefile_native(&source, &destination).unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"fresh image bytes");
        fs::write(&destination, b"changed clone").unwrap();
        assert_eq!(fs::read(&source).unwrap(), b"fresh image bytes");

        fs::remove_file(source).unwrap();
        fs::remove_file(destination).unwrap();
    }
}
