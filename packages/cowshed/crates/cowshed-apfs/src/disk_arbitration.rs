//! Owned-media requests through DiskArbitration: the owned-registry image release and the exact
//! volume mount (01a_apfs.md, "Mounts", "Release and device custody" and "Owned-registry release
//! boundary").
//!
//! The owner hands this module the exact IOMedia it selected from the I/O Registry. Each one
//! becomes a `DADisk` through `DADiskCreateFromIOMedia`, so DiskArbitration is asked about exactly
//! those kernel objects and nothing it may have cached under a reused BSD spelling.
//!
//! A release is given the image's registered whole device and that device's current APFS volume
//! media. Each owned volume is unmounted on its own (never `kDADiskUnmountOptionWhole`, never
//! `DADiskCopyWholeDisk`, never a BSD-prefix enumeration), then the exact whole device is ejected.
//! A foreign or stale cached descendant is therefore neither selected nor consulted:
//! diskarbitrationd's eject (`__DARequestEject`, DiskArbitration-535.120.1.0.1
//! `diskarbitrationd/DARequest.c`) runs the approval round and the device eject for the one disk it
//! is given.
//!
//! `force` is the existing `Release` force after its grace: the owned volumes are unmounted with
//! `kDADiskUnmountOptionForce`. The SDK defines no forced eject (`DADiskEjectOptions` has only
//! `kDADiskEjectOptionDefault`), so the eject that follows is always the ordinary one.
//!
//! A mount is given the one APFS volume of a freshly attached image and asks
//! `DADiskMountWithArguments` to mount that volume, and no sibling (never
//! `kDADiskMountOptionWhole`), at the requested mount point. DiskArbitration keeps the filesystem
//! check and mount bookkeeping it runs for every mount; the request adds only the policy's
//! `/sbin/mount -o` words. Every answer but approval is the mount's refusal: a mount has no
//! already-done answer and earns no grace.
//!
//! Every request acts on an image device, the class `diskutil eject` belongs to, so it keeps that
//! class's deadline policy (01a_apfs.md, "How the APFS host degrades" §2;
//! [`crate::DISK_CHILD_DEADLINE`]): a request still unanswered at 120 s is warned about then and
//! every [`crate::DISK_IMAGE_WARNING_INTERVAL`] after, and waited for until DiskArbitration
//! answers. A request is never abandoned: its answer is the only proof of what happened to the
//! device, and the callback context it was given must stay live while it may still arrive.

use std::error::Error;
use std::fmt;
use std::io;

#[cfg(any(target_os = "macos", test))]
use crate::{MountAccess, MountPolicy};

/// `err_local | err_sub(0x368)` (DADissenter.h): the DiskArbitration status space.
const fn da_return(code: u32) -> i32 {
    i32::from_ne_bytes((0xF8DA_0000 | code).to_ne_bytes())
}

/// `kDAReturnError`: DiskArbitration's generic failure. Not a dissent.
pub const DA_RETURN_ERROR: i32 = da_return(0x01);
/// `kDAReturnBusy`: a dissent, the one native answer the `Release` grace waits through. To a mount
/// it means the volume is already mounted (DiskArbitration.h, `DADiskMountCallback`): a refusal.
pub const DA_RETURN_BUSY: i32 = da_return(0x02);
/// `kDAReturnNotMounted`: an unmount request found the volume already unmounted.
const DA_RETURN_NOT_MOUNTED: i32 = da_return(0x07);

/// Every `DAReturn` DADissenter.h names, by value.
const DA_RETURN_NAMES: [(i32, &str); 13] = [
    (0, "kDAReturnSuccess"),
    (DA_RETURN_ERROR, "kDAReturnError"),
    (DA_RETURN_BUSY, "kDAReturnBusy"),
    (da_return(0x03), "kDAReturnBadArgument"),
    (da_return(0x04), "kDAReturnExclusiveAccess"),
    (da_return(0x05), "kDAReturnNoResources"),
    (da_return(0x06), "kDAReturnNotFound"),
    (DA_RETURN_NOT_MOUNTED, "kDAReturnNotMounted"),
    (da_return(0x08), "kDAReturnNotPermitted"),
    (da_return(0x09), "kDAReturnNotPrivileged"),
    (da_return(0x0A), "kDAReturnNotReady"),
    (da_return(0x0B), "kDAReturnNotWritable"),
    (da_return(0x0C), "kDAReturnUnsupported"),
];

/// `unix_err(0)` (mach/error.h: `err_kern | err_sub(3)`): the space a dissenter encodes a BSD errno
/// in, as `DADissenterGetStatus` documents.
const UNIX_ERR: i32 = 3 << 14;
/// The errno bits under [`UNIX_ERR`] (`err_get_code`).
const UNIX_ERR_CODE: i32 = 0x3FFF;

/// `unix_err(errno)`.
const fn unix_err(errno: i32) -> i32 {
    UNIX_ERR | errno
}

/// The SDK function a mount request calls: the `operation` of its refusal.
const MOUNT_OPERATION: &str = "DADiskMountWithArguments";

/// Why a native DiskArbitration request did not complete. Every variant is an answer the host gave:
/// nothing here is inferred from a missing answer, and no request is reported before
/// DiskArbitration answered it.
#[derive(Debug)]
pub enum OperationFailure {
    /// The native call could not be made or could not deliver its answer: a null object, an
    /// unusable run loop.
    Unavailable {
        operation: &'static str,
        source: io::Error,
    },
    /// DiskArbitration answered with a dissenter: its status and the dissenter's own status string.
    Refused {
        operation: &'static str,
        status: i32,
        detail: Option<String>,
    },
}

impl OperationFailure {
    /// Whether DiskArbitration dissented from a release request because the media is busy:
    /// `kDAReturnBusy`, or a BSD `EBUSY` it encoded with `unix_err`. Only this answer earns the
    /// `Release` grace; a generic or unknown status does not, and neither does any mount refusal,
    /// whose busy answer means the volume is already mounted.
    pub fn is_dissent(&self) -> bool {
        match self {
            Self::Refused {
                operation, status, ..
            } => {
                *operation != MOUNT_OPERATION
                    && (*status == DA_RETURN_BUSY || *status == unix_err(libc::EBUSY))
            }
            Self::Unavailable { .. } => false,
        }
    }
}

impl fmt::Display for OperationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { operation, source } => {
                write!(f, "DiskArbitration {operation} could not run: {source}")
            }
            Self::Refused {
                operation,
                status,
                detail,
            } => {
                write!(
                    f,
                    "DiskArbitration {operation} was refused: {}",
                    Status(*status)
                )?;
                if let Some(detail) = detail {
                    write!(f, ": {detail}")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for OperationFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unavailable { source, .. } => Some(source),
            Self::Refused { .. } => None,
        }
    }
}

/// A `DAReturn` named as DADissenter.h names it, or decoded as the BSD errno it encodes, with its
/// raw value.
struct Status(i32);

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = self.0;
        match DA_RETURN_NAMES.iter().find(|(value, _)| *value == code) {
            Some((_, name)) => f.write_str(name)?,
            None if code & !UNIX_ERR_CODE == UNIX_ERR => {
                let errno = code & UNIX_ERR_CODE;
                write!(
                    f,
                    "unix error {errno} ({})",
                    io::Error::from_raw_os_error(errno)
                )?;
            }
            None => f.write_str("unrecognised DiskArbitration status")?,
        }
        // Signed hex prints the two's-complement bits, the spelling the SDK header comments use.
        write!(f, " ({code:#010X})")
    }
}

/// `kDADiskUnmountOptionDefault`.
#[cfg(any(target_os = "macos", test))]
const UNMOUNT_OPTION_DEFAULT: u32 = 0x0000_0000;
/// `kDADiskUnmountOptionForce`: unmount the volume even if files are still active.
#[cfg(any(target_os = "macos", test))]
const UNMOUNT_OPTION_FORCE: u32 = 0x0008_0000;
/// `kDADiskEjectOptionDefault`, the only eject option the SDK defines.
#[cfg(any(target_os = "macos", test))]
const EJECT_OPTION_DEFAULT: u32 = 0x0000_0000;
/// `kDADiskMountOptionDefault`: neither `kDADiskMountOptionWhole` (0x1, every volume of the whole
/// disk) nor `kDADiskMountOptionNoFollow` (0x2).
#[cfg(any(target_os = "macos", test))]
const MOUNT_OPTION_DEFAULT: u32 = 0x0000_0000;

/// A DiskArbitration request cowshed makes, by the SDK function it calls.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Unmount,
    Eject,
    Mount,
}

#[cfg(any(target_os = "macos", test))]
impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Unmount => "DADiskUnmount",
            Self::Eject => "DADiskEject",
            Self::Mount => MOUNT_OPERATION,
        }
    }

    /// What DiskArbitration's answer means for this request. An owned volume already unmounted is
    /// the state an unmount asks for, so `kDAReturnNotMounted` completes an unmount and nothing
    /// else; every other dissent is the caller's value, a mount's `kDAReturnBusy` (already mounted)
    /// and `kDAReturnNotMounted` included. A dissenter carrying `kDAReturnSuccess` contradicts the
    /// header ("a dissenter object on failure") and is reported as the refusal it was delivered as.
    fn conclude(self, answer: Answer) -> Result<(), OperationFailure> {
        match (self, answer) {
            (_, Answer::Approved) => Ok(()),
            (_, Answer::Unavailable { operation, source }) => {
                Err(OperationFailure::Unavailable { operation, source })
            }
            (
                Self::Unmount,
                Answer::Dissented {
                    status: DA_RETURN_NOT_MOUNTED,
                    ..
                },
            ) => Ok(()),
            (_, Answer::Dissented { status, detail }) => Err(OperationFailure::Refused {
                operation: self.name(),
                status,
                detail,
            }),
        }
    }
}

/// One DiskArbitration request of a release, against one owned IOMedia (an `io_service_t` the
/// caller holds).
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Request {
    Unmount { volume: u32, options: u32 },
    Eject { whole: u32, options: u32 },
}

#[cfg(any(target_os = "macos", test))]
impl Request {
    fn operation(self) -> Operation {
        match self {
            Self::Unmount { .. } => Operation::Unmount,
            Self::Eject { .. } => Operation::Eject,
        }
    }

    /// The IOMedia this request's `DADisk` is created from.
    fn media(self) -> u32 {
        match self {
            Self::Unmount { volume, .. } => volume,
            Self::Eject { whole, .. } => whole,
        }
    }

    fn role(self) -> &'static str {
        match self {
            Self::Unmount { .. } => "volume",
            Self::Eject { .. } => "whole",
        }
    }
}

/// A release, in order: each owned volume unmounted on its own, then the exact whole device
/// ejected.
#[cfg(any(target_os = "macos", test))]
fn requests(
    whole: u32,
    volumes: impl IntoIterator<Item = u32>,
    force: bool,
) -> impl Iterator<Item = Request> {
    let options = if force {
        UNMOUNT_OPTION_FORCE
    } else {
        UNMOUNT_OPTION_DEFAULT
    };
    volumes
        .into_iter()
        .map(move |volume| Request::Unmount { volume, options })
        .chain(std::iter::once(Request::Eject {
            whole,
            options: EJECT_OPTION_DEFAULT,
        }))
}

/// The `/sbin/mount -o` words a mount under `policy` passes, one CFString each. A workspace volume
/// mounts with its owners honoured and no access-time writes, hidden from the Finder unless
/// browsable, read-only on request; an event-store repair mounts hidden and ignoring ownership, and
/// nothing else.
#[cfg(any(target_os = "macos", test))]
fn mount_arguments(policy: MountPolicy) -> &'static [&'static str] {
    match policy {
        MountPolicy::Workspace {
            access: MountAccess::ReadWrite,
            browse: false,
        } => &["nobrowse", "owners", "noatime"],
        MountPolicy::Workspace {
            access: MountAccess::ReadWrite,
            browse: true,
        } => &["owners", "noatime"],
        MountPolicy::Workspace {
            access: MountAccess::ReadOnly,
            browse: false,
        } => &["rdonly", "nobrowse", "owners", "noatime"],
        MountPolicy::Workspace {
            access: MountAccess::ReadOnly,
            browse: true,
        } => &["rdonly", "owners", "noatime"],
        MountPolicy::EventLogRepair => &["nobrowse", "noowners"],
    }
}

/// A request's completion callback, as DiskArbitration delivered it.
#[cfg(any(target_os = "macos", test))]
#[derive(Debug)]
enum Answer {
    /// The callback's dissenter was null.
    Approved,
    Unavailable {
        operation: &'static str,
        source: io::Error,
    },
    Dissented {
        status: i32,
        detail: Option<String>,
    },
}

/// How long into an unanswered request its `warned`-th warning falls due: the disk-image deadline,
/// then each warning interval, the schedule `PastDeadline::WarnEvery` gives `diskutil eject`.
#[cfg(any(target_os = "macos", test))]
fn warning_due(warned: u32) -> std::time::Duration {
    crate::DISK_CHILD_DEADLINE + crate::DISK_IMAGE_WARNING_INTERVAL * warned
}

#[cfg(target_os = "macos")]
pub(crate) use native::{mount, release};

#[cfg(target_os = "macos")]
mod native {
    use super::{
        Answer, MOUNT_OPTION_DEFAULT, Operation, OperationFailure, Request, mount_arguments,
        requests, warning_due,
    };
    use crate::MountPolicy;
    use std::cell::Cell;
    use std::ffi::{CStr, c_char, c_void};
    use std::io::{self, Write as _};
    use std::mem::ManuallyDrop;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::Path;
    use std::ptr;
    use std::time::{Duration, Instant};

    type CfType = *const c_void;
    type CfIndex = isize;
    /// `DADiskMountCallback`, `DADiskUnmountCallback` and `DADiskEjectCallback`:
    /// `(disk, dissenter, context)`.
    type Completed = unsafe extern "C" fn(CfType, CfType, *mut c_void);

    const CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
    /// `kCFRunLoopRunFinished`: the mode has no sources or timers.
    const RUN_FINISHED: i32 = 1;
    /// `kCFRunLoopRunStopped`: `CFRunLoopStop` was called on this thread's run loop.
    const RUN_STOPPED: i32 = 2;
    /// `kCFRunLoopRunTimedOut`.
    const RUN_TIMED_OUT: i32 = 3;
    /// `kCFRunLoopRunHandledSource`.
    const RUN_HANDLED_SOURCE: i32 = 4;
    /// The run-loop mode a request's session is scheduled in. No other source is in it, so running
    /// it delivers this session's answers and nothing of the thread's other run-loop clients.
    const MODE: &str = "cowshed.apfs.disk-arbitration";

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CfRange {
        location: CfIndex,
        length: CfIndex,
    }

    #[link(name = "DiskArbitration", kind = "framework")]
    unsafe extern "C" {
        fn DASessionCreate(allocator: CfType) -> CfType;
        fn DASessionScheduleWithRunLoop(session: CfType, run_loop: CfType, mode: CfType);
        fn DASessionUnscheduleFromRunLoop(session: CfType, run_loop: CfType, mode: CfType);
        fn DADiskCreateFromIOMedia(
            allocator: CfType,
            session: CfType,
            media: libc::mach_port_t,
        ) -> CfType;
        fn DADiskGetBSDName(disk: CfType) -> *const c_char;
        fn DADiskMountWithArguments(
            disk: CfType,
            path: CfType,
            options: u32,
            callback: Completed,
            context: *mut c_void,
            arguments: *mut CfType,
        );
        fn DADiskUnmount(disk: CfType, options: u32, callback: Completed, context: *mut c_void);
        fn DADiskEject(disk: CfType, options: u32, callback: Completed, context: *mut c_void);
        fn DADissenterGetStatus(dissenter: CfType) -> i32;
        fn DADissenterGetStatusString(dissenter: CfType) -> CfType;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFRelease(object: CfType);
        fn CFRunLoopGetCurrent() -> CfType;
        fn CFRunLoopRunInMode(mode: CfType, seconds: f64, return_after_source_handled: u8) -> i32;
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
        fn CFURLCreateFromFileSystemRepresentation(
            allocator: CfType,
            buffer: *const u8,
            length: CfIndex,
            is_directory: u8,
        ) -> CfType;
    }

    /// One owned, non-null CoreFoundation reference (a session, a disk, a string, a URL), released
    /// on every exit.
    struct Owned(CfType);

    impl Owned {
        /// Take ownership of a Create-rule answer; null is `operation`'s failure, said with `what`.
        fn created(
            object: CfType,
            operation: &'static str,
            what: impl FnOnce() -> String,
        ) -> Result<Self, OperationFailure> {
            if object.is_null() {
                return Err(OperationFailure::Unavailable {
                    operation,
                    source: io::Error::other(what()),
                });
            }
            Ok(Self(object))
        }

        fn session() -> Result<Self, OperationFailure> {
            // SAFETY: a null allocator is the default allocator; a null answer is refused by
            // `created`.
            let session = unsafe { DASessionCreate(ptr::null()) };
            Self::created(session, "DASessionCreate", || {
                "returned no session".to_owned()
            })
        }

        fn string(text: &'static str) -> Result<Self, OperationFailure> {
            let length =
                CfIndex::try_from(text.len()).map_err(|error| OperationFailure::Unavailable {
                    operation: "CFStringCreateWithBytes",
                    source: io::Error::new(io::ErrorKind::InvalidInput, format!("{text}: {error}")),
                })?;
            // SAFETY: `text` is readable for `length` bytes; a null answer is refused by `created`.
            let string = unsafe {
                CFStringCreateWithBytes(
                    ptr::null(),
                    text.as_ptr(),
                    length,
                    CF_STRING_ENCODING_UTF8,
                    0,
                )
            };
            Self::created(string, "CFStringCreateWithBytes", || {
                format!("returned no CFString for {text}")
            })
        }

        /// A directory URL holding `path`'s file-system bytes as they are, so a mount point that is
        /// not UTF-8 is named as spelled.
        fn directory(path: &Path) -> Result<Self, OperationFailure> {
            let bytes = path.as_os_str().as_bytes();
            let length =
                CfIndex::try_from(bytes.len()).map_err(|error| OperationFailure::Unavailable {
                    operation: "CFURLCreateFromFileSystemRepresentation",
                    source: io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("mount point {}: {error}", path.display()),
                    ),
                })?;
            // SAFETY: `bytes` is readable for `length` bytes; a null answer is refused by
            // `created`.
            let url = unsafe {
                CFURLCreateFromFileSystemRepresentation(ptr::null(), bytes.as_ptr(), length, 1)
            };
            Self::created(url, "CFURLCreateFromFileSystemRepresentation", || {
                format!("returned no URL for the mount point {}", path.display())
            })
        }

        /// Hand the one reference to a caller that releases it itself.
        fn into_raw(self) -> CfType {
            ManuallyDrop::new(self).0
        }
    }

    impl Drop for Owned {
        fn drop(&mut self) {
            // SAFETY: the wrapper owns exactly one reference to a non-null CF object.
            unsafe { CFRelease(self.0) };
        }
    }

    /// The null-terminated `arguments` list `DADiskMountWithArguments` takes: one owned CFString
    /// per `/sbin/mount -o` word, then null. Every string is released on every exit.
    struct Arguments(Vec<CfType>);

    impl Arguments {
        fn new(words: &[&'static str]) -> Result<Self, OperationFailure> {
            let mut arguments = Self(Vec::with_capacity(words.len() + 1));
            for &word in words {
                arguments.0.push(Owned::string(word)?.into_raw());
            }
            arguments.0.push(ptr::null());
            Ok(arguments)
        }
    }

    impl Drop for Arguments {
        fn drop(&mut self) {
            for &string in self.0.iter().filter(|string| !string.is_null()) {
                // SAFETY: every non-null entry is one reference `new` took from `Owned`.
                unsafe { CFRelease(string) };
            }
        }
    }

    /// A `DADisk` of one owned IOMedia, with the role and `io_service_t` it was created from, so an
    /// unanswered request can name its media when DiskArbitration has no BSD name for it.
    struct Disk {
        disk: Owned,
        role: &'static str,
        media: u32,
    }

    impl Disk {
        fn create(
            session: &Owned,
            role: &'static str,
            media: u32,
        ) -> Result<Self, OperationFailure> {
            // SAFETY: `media` is an IOMedia service the caller holds; `session` is live; a null
            // answer is refused by `created`.
            let disk = unsafe { DADiskCreateFromIOMedia(ptr::null(), session.0, media) };
            let disk = Owned::created(disk, "DADiskCreateFromIOMedia", || {
                format!("returned no disk for the owned {role} IOMedia (io_service_t {media:#x})")
            })?;
            Ok(Self { disk, role, media })
        }
    }

    /// The answer slot a request's callback writes. It is the callback context.
    #[derive(Default)]
    struct Completion(Cell<Option<Answer>>);

    /// The session scheduled on this thread's run loop in [`MODE`], for as long as this value
    /// lives.
    ///
    /// The context-lifetime proof: DiskArbitration delivers a request's callback from the session's
    /// run-loop source, so only inside `CFRunLoopRunInMode(MODE)` on this thread while the session
    /// is scheduled. `Scheduled` borrows the [`Completion`] it hands out as context, so the borrow
    /// checker keeps that slot alive until after `Drop` has unscheduled the session; from then on
    /// no callback can run, answered or not.
    struct Scheduled<'a> {
        session: &'a Owned,
        run_loop: CfType,
        mode: &'a Owned,
        completion: &'a Completion,
    }

    impl<'a> Scheduled<'a> {
        fn new(session: &'a Owned, mode: &'a Owned, completion: &'a Completion) -> Self {
            // SAFETY: Get rule: the current thread's run loop, non-null and live as long as this
            // thread.
            let run_loop = unsafe { CFRunLoopGetCurrent() };
            // SAFETY: `session`, `run_loop` and `mode` are live; `Drop` undoes exactly this
            // schedule.
            unsafe { DASessionScheduleWithRunLoop(session.0, run_loop, mode.0) };
            Self {
                session,
                run_loop,
                mode,
                completion,
            }
        }

        /// Issue one `operation` request against `disk` through `issue`, which is handed the
        /// callback context, and wait, event-driven, for its one callback.
        fn perform(
            &self,
            operation: Operation,
            disk: &Disk,
            issue: impl FnOnce(*mut c_void),
        ) -> Result<(), OperationFailure> {
            issue(ptr::from_ref(self.completion).cast_mut().cast::<c_void>());
            let started = Instant::now();
            let mut warned = 0;
            loop {
                if let Some(answer) = self.completion.0.take() {
                    return operation.conclude(answer);
                }
                let elapsed = started.elapsed();
                let Some(wait) = warning_due(warned)
                    .checked_sub(elapsed)
                    .filter(|wait| !wait.is_zero())
                else {
                    warn_outlived(operation, disk, elapsed);
                    warned += 1;
                    continue;
                };
                // SAFETY: `mode` is a live CFString; only this session's source is in it.
                let reason = match unsafe { CFRunLoopRunInMode(self.mode.0, wait.as_secs_f64(), 1) }
                {
                    RUN_HANDLED_SOURCE | RUN_TIMED_OUT | RUN_STOPPED => continue,
                    RUN_FINISHED => "the DiskArbitration run-loop mode has no source".to_owned(),
                    other => format!("returned the undocumented result {other}"),
                };
                // No source in the mode means the session's answer has no route to this thread:
                // waiting longer cannot deliver it. Returning drops `Scheduled`, which unschedules
                // the session first, so the context the request holds is never reached afterward.
                return Err(OperationFailure::Unavailable {
                    operation: "CFRunLoopRunInMode",
                    source: io::Error::other(format!(
                        "{reason} while {} awaits its answer, so DiskArbitration cannot deliver it",
                        operation.name()
                    )),
                });
            }
        }
    }

    impl Drop for Scheduled<'_> {
        fn drop(&mut self) {
            // SAFETY: undoes `new`'s schedule with the same live session, run loop and mode.
            unsafe { DASessionUnscheduleFromRunLoop(self.session.0, self.run_loop, self.mode.0) };
        }
    }

    /// Release one attached image's owned device tree: unmount each owned volume IOMedia on its own
    /// (forced when `force`), then eject the exact whole IOMedia.
    ///
    /// `whole` and `volumes` are `io_service_t` references the caller holds, live and owned, for
    /// the whole call; they are the identities the owner selected from the current I/O Registry and
    /// nothing else is asked about. Every `DADisk` is created before the first request, so media
    /// DiskArbitration cannot name stops the release before any mutation. Each request is answered
    /// before the next is issued; the first refusal ends the release with the native status.
    pub(crate) fn release(
        whole: u32,
        volumes: impl IntoIterator<Item = u32>,
        force: bool,
    ) -> Result<(), OperationFailure> {
        let mode = Owned::string(MODE)?;
        let session = Owned::session()?;
        let disks = requests(whole, volumes, force)
            .map(|request| {
                Disk::create(&session, request.role(), request.media()).map(|disk| (request, disk))
            })
            .collect::<Result<Vec<_>, OperationFailure>>()?;
        let completion = Completion::default();
        let scheduled = Scheduled::new(&session, &mode, &completion);
        disks.iter().try_for_each(|(request, disk)| {
            scheduled.perform(request.operation(), disk, |context| {
                // SAFETY: `disk` is a live DADisk of the scheduled session; `context` is the
                // borrowed completion slot, live while the session stays scheduled (see
                // `Scheduled`); `completed` matches the callback type.
                unsafe {
                    match *request {
                        Request::Unmount { options, .. } => {
                            DADiskUnmount(disk.disk.0, options, completed, context);
                        }
                        Request::Eject { options, .. } => {
                            DADiskEject(disk.disk.0, options, completed, context);
                        }
                    }
                }
            })
        })
    }

    /// Mount one owned APFS volume IOMedia at `mount_point` under `policy`.
    ///
    /// `volume` is an `io_service_t` the caller holds, live and owned, for the whole call: the one
    /// APFS volume of the freshly attached image the owner selected from the current I/O Registry.
    /// Its `DADisk` comes from that IOMedia, never a BSD name, and the request carries
    /// `kDADiskMountOptionDefault`, never `kDADiskMountOptionWhole`, so DiskArbitration mounts that
    /// volume and no sibling. The mount point reaches CoreFoundation as its file-system bytes.
    /// Every object is created before the request is issued, so an unusable argument stops the
    /// mount before any mutation. The one answer is the result: every dissent is a refusal with its
    /// native status, and nothing here retries.
    pub(crate) fn mount(
        volume: u32,
        mount_point: &Path,
        policy: MountPolicy,
    ) -> Result<(), OperationFailure> {
        let mode = Owned::string(MODE)?;
        let session = Owned::session()?;
        let disk = Disk::create(&session, "volume", volume)?;
        let path = Owned::directory(mount_point)?;
        let mut arguments = Arguments::new(mount_arguments(policy))?;
        let completion = Completion::default();
        let scheduled = Scheduled::new(&session, &mode, &completion);
        scheduled.perform(Operation::Mount, &disk, |context| {
            // SAFETY: `disk` is a live DADisk of the scheduled session; `path` is a live CFURL and
            // `arguments` a live null-terminated CFString list, both dropped only after
            // `scheduled`; `context` is the borrowed completion slot, live while the session stays
            // scheduled (see `Scheduled`); `completed` matches the callback type.
            unsafe {
                DADiskMountWithArguments(
                    disk.disk.0,
                    path.0,
                    MOUNT_OPTION_DEFAULT,
                    completed,
                    context,
                    arguments.0.as_mut_ptr(),
                );
            }
        })
    }

    unsafe extern "C" fn completed(_disk: CfType, dissenter: CfType, context: *mut c_void) {
        let answer = if dissenter.is_null() {
            Answer::Approved
        } else {
            // SAFETY: the callback's dissenter and borrowed status string are live for this call.
            let status = unsafe { DADissenterGetStatus(dissenter) };
            match unsafe { copy_string(DADissenterGetStatusString(dissenter)) } {
                Ok(detail) => Answer::Dissented { status, detail },
                Err(error) => Answer::Unavailable {
                    operation: "DADissenterGetStatusString",
                    source: io::Error::new(error.kind(), format!("status {status:#010X}: {error}")),
                },
            }
        };
        // SAFETY: context is the live Completion borrowed by the scheduled session on this thread.
        let completion = unsafe { &*context.cast::<Completion>() };
        completion.0.set(Some(answer));
    }

    unsafe fn copy_string(string: CfType) -> io::Result<Option<String>> {
        if string.is_null() {
            return Ok(None);
        }
        let range = CfRange {
            location: 0,
            // SAFETY: string is a live borrowed CFString for this callback.
            length: unsafe { CFStringGetLength(string) },
        };
        let mut needed: CfIndex = 0;
        // SAFETY: a null buffer requests only the encoded byte count of the complete range.
        let converted = unsafe {
            CFStringGetBytes(
                string,
                range,
                CF_STRING_ENCODING_UTF8,
                b'?',
                0,
                ptr::null_mut(),
                0,
                &mut needed,
            )
        };
        if range.length < 0 || converted != range.length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "CFStringGetBytes sized {converted} of {} characters",
                    range.length
                ),
            ));
        }
        let capacity = usize::try_from(needed).map_err(io::Error::other)?;
        let mut bytes = vec![0u8; capacity];
        let mut used: CfIndex = 0;
        // SAFETY: bytes is writable for the exact nonnegative capacity returned by the sizing call.
        let converted = unsafe {
            CFStringGetBytes(
                string,
                range,
                CF_STRING_ENCODING_UTF8,
                b'?',
                0,
                bytes.as_mut_ptr(),
                needed,
                &mut used,
            )
        };
        let used = usize::try_from(used).map_err(io::Error::other)?;
        if converted != range.length || used > capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "CFStringGetBytes encoded {converted} of {} characters into {used}/{capacity} bytes",
                    range.length
                ),
            ));
        }
        bytes.truncate(used);
        String::from_utf8(bytes).map(Some).map_err(io::Error::other)
    }

    /// Say that a request outlived the disk-image deadline and is still waited for.
    ///
    /// Written without `eprintln!` for the reason `warn_outlived` in lib.rs gives: a caller that
    /// closed this process's stderr must not turn the warning into a panic that abandons an
    /// in-flight request whose callback context is on this stack. That caller cannot hear the
    /// warning; the wait goes on regardless.
    fn warn_outlived(operation: Operation, disk: &Disk, elapsed: Duration) {
        // SAFETY: a live DADisk; the name, when present, is a NUL-terminated string the disk owns.
        let name = unsafe { DADiskGetBSDName(disk.disk.0) };
        let target = if name.is_null() {
            format!("{} IOMedia {:#x}", disk.role, disk.media)
        } else {
            // SAFETY: non-null, NUL-terminated and owned by `disk`, which outlives this borrow.
            unsafe { CStr::from_ptr(name) }
                .to_string_lossy()
                .into_owned()
        };
        let _ = writeln!(
            io::stderr(),
            "cowshed: warning: DiskArbitration {} on {target} is still running after {}s, past \
             its {}s deadline; abandoning a request mid-operation leaves its device's fate \
             unknown, so cowshed never abandons one: it is waited for, holding its leases, until \
             DiskArbitration answers",
            operation.name(),
            elapsed.as_secs(),
            crate::DISK_CHILD_DEADLINE.as_secs(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The values DADissenter.h's comments spell out, and mach/error.h's `unix_err(EBUSY)`.
    #[test]
    fn statuses_are_the_sdk_values() {
        let expected: [(u32, &str); 13] = [
            (0, "kDAReturnSuccess"),
            (0xF8DA_0001, "kDAReturnError"),
            (0xF8DA_0002, "kDAReturnBusy"),
            (0xF8DA_0003, "kDAReturnBadArgument"),
            (0xF8DA_0004, "kDAReturnExclusiveAccess"),
            (0xF8DA_0005, "kDAReturnNoResources"),
            (0xF8DA_0006, "kDAReturnNotFound"),
            (0xF8DA_0007, "kDAReturnNotMounted"),
            (0xF8DA_0008, "kDAReturnNotPermitted"),
            (0xF8DA_0009, "kDAReturnNotPrivileged"),
            (0xF8DA_000A, "kDAReturnNotReady"),
            (0xF8DA_000B, "kDAReturnNotWritable"),
            (0xF8DA_000C, "kDAReturnUnsupported"),
        ];
        for ((value, name), (expected_value, expected_name)) in DA_RETURN_NAMES.iter().zip(expected)
        {
            assert_eq!(
                value.to_be_bytes(),
                expected_value.to_be_bytes(),
                "{expected_name}"
            );
            assert_eq!(*name, expected_name);
        }
        assert_eq!(DA_RETURN_BUSY, -119_930_878);
        assert_eq!(unix_err(libc::EBUSY), 0xC010);
    }

    /// Each owned volume is unmounted on its own, in the owner's order, then the exact whole is
    /// ejected; force reaches only the unmounts, and no request ever carries
    /// `kDADiskUnmountOptionWhole` (0x1).
    #[test]
    fn a_release_unmounts_each_owned_volume_then_ejects_the_whole() {
        for (force, options) in [
            (false, UNMOUNT_OPTION_DEFAULT),
            (true, UNMOUNT_OPTION_FORCE),
        ] {
            let planned: Vec<_> = requests(0x1103, [0x2207, 0x2203], force).collect();
            assert_eq!(
                planned,
                [
                    Request::Unmount {
                        volume: 0x2207,
                        options
                    },
                    Request::Unmount {
                        volume: 0x2203,
                        options
                    },
                    Request::Eject {
                        whole: 0x1103,
                        options: EJECT_OPTION_DEFAULT
                    },
                ],
                "force={force}"
            );
            for request in planned {
                let (Request::Unmount { options, .. } | Request::Eject { options, .. }) = request;
                assert_eq!(options & 0x1, 0, "{request:?}");
            }
        }
        assert_eq!(UNMOUNT_OPTION_FORCE, 0x0008_0000);
        let bare: Vec<_> = requests(0x1103, [], true).collect();
        assert_eq!(
            bare,
            [Request::Eject {
                whole: 0x1103,
                options: EJECT_OPTION_DEFAULT
            }]
        );
        assert_eq!(bare[0].media(), 0x1103);
        assert_eq!(bare[0].operation(), Operation::Eject);
        assert_eq!(bare[0].operation().name(), "DADiskEject");
        assert_eq!(
            requests(0x1103, [0x2207], false)
                .next()
                .map(Request::operation),
            Some(Operation::Unmount)
        );
    }

    fn dissent(status: i32, detail: Option<&str>) -> Answer {
        Answer::Dissented {
            status,
            detail: detail.map(str::to_owned),
        }
    }

    #[test]
    fn an_answer_concludes_its_request() {
        let unmount = Operation::Unmount;
        let eject = Operation::Eject;
        assert!(unmount.conclude(Answer::Approved).is_ok());
        assert!(eject.conclude(Answer::Approved).is_ok());
        let failure = eject
            .conclude(Answer::Unavailable {
                operation: "DADissenterGetStatusString",
                source: io::Error::new(io::ErrorKind::InvalidData, "incomplete UTF8 encoding"),
            })
            .expect_err("unreadable native refusal detail is unavailable, never success");
        assert!(
            !failure.is_dissent(),
            "conversion failure cannot authorize a retry or force"
        );
        assert!(matches!(
            failure,
            OperationFailure::Unavailable { operation: "DADissenterGetStatusString", source }
                if source.kind() == io::ErrorKind::InvalidData
        ));
        // An owned volume already unmounted is the unmount's goal; an eject has no such answer.
        assert!(
            unmount
                .conclude(dissent(DA_RETURN_NOT_MOUNTED, None))
                .is_ok()
        );
        let Err(OperationFailure::Refused {
            operation,
            status,
            detail,
        }) = eject.conclude(dissent(DA_RETURN_NOT_MOUNTED, Some("x")))
        else {
            panic!("an eject's NotMounted is a refusal");
        };
        assert_eq!(
            (operation, status, detail.as_deref()),
            ("DADiskEject", DA_RETURN_NOT_MOUNTED, Some("x"))
        );
        // A dissenter carrying success contradicts the header and stays the refusal it was
        // delivered as.
        assert!(matches!(
            unmount.conclude(dissent(0, None)),
            Err(OperationFailure::Refused {
                status: 0,
                operation: "DADiskUnmount",
                ..
            })
        ));
    }

    /// A mount completes only on approval: busy (already mounted), not-mounted, an encoded `EBUSY`,
    /// the generic error and a dissenter carrying success are each its refusal, with the native
    /// status, and none earns the `Release` grace.
    #[test]
    fn a_mount_completes_only_on_approval() {
        assert!(Operation::Mount.conclude(Answer::Approved).is_ok());
        for status in [
            DA_RETURN_BUSY,
            DA_RETURN_NOT_MOUNTED,
            unix_err(libc::EBUSY),
            DA_RETURN_ERROR,
            da_return(0x09),
            0,
        ] {
            let failure = Operation::Mount
                .conclude(dissent(status, Some("mount control")))
                .expect_err("every mount dissent is a refusal");
            assert!(!failure.is_dissent(), "{status:#X}");
            let OperationFailure::Refused {
                operation,
                status: refused,
                detail,
            } = failure
            else {
                panic!("a mount dissent is a refusal, not unavailability");
            };
            assert_eq!(
                (operation, refused, detail.as_deref()),
                ("DADiskMountWithArguments", status, Some("mount control"))
            );
        }
        let failure = Operation::Mount
            .conclude(Answer::Unavailable {
                operation: "DADissenterGetStatusString",
                source: io::Error::new(io::ErrorKind::InvalidData, "incomplete UTF8 encoding"),
            })
            .expect_err("an unreadable mount answer is unavailable, never success");
        assert!(matches!(
            failure,
            OperationFailure::Unavailable { operation: "DADissenterGetStatusString", source }
                if source.kind() == io::ErrorKind::InvalidData
        ));
    }

    /// The mount policy matrix: owners honoured and no access-time writes on every workspace mount,
    /// `nobrowse` unless browsable, `rdonly` when read-only; the event-store repair mounts hidden
    /// and ignoring ownership with nothing else. Every word is its own CFString, and the request
    /// never asks for every volume of the whole disk.
    #[test]
    fn a_mount_passes_exactly_its_policys_options() {
        let workspace = |access, browse| MountPolicy::Workspace { access, browse };
        let cases: [(MountPolicy, &[&str]); 5] = [
            (
                workspace(MountAccess::ReadWrite, false),
                &["nobrowse", "owners", "noatime"],
            ),
            (
                workspace(MountAccess::ReadWrite, true),
                &["owners", "noatime"],
            ),
            (
                workspace(MountAccess::ReadOnly, false),
                &["rdonly", "nobrowse", "owners", "noatime"],
            ),
            (
                workspace(MountAccess::ReadOnly, true),
                &["rdonly", "owners", "noatime"],
            ),
            (MountPolicy::EventLogRepair, &["nobrowse", "noowners"]),
        ];
        for (policy, expected) in cases {
            let words = mount_arguments(policy);
            assert_eq!(words, expected, "{policy:?}");
            assert!(
                words
                    .iter()
                    .all(|word| !word.is_empty() && !word.contains(',')),
                "{policy:?}"
            );
        }
        assert_eq!(MOUNT_OPTION_DEFAULT, 0);
        assert_eq!(MOUNT_OPTION_DEFAULT & 0x1, 0, "kDADiskMountOptionWhole");
        assert_eq!(Operation::Mount.name(), "DADiskMountWithArguments");
    }

    /// Only a busy release answer is a dissent: `kDAReturnBusy` or a BSD `EBUSY`; never the generic
    /// error, another errno, an unknown status, an unusable native call, or a mount's refusal.
    #[test]
    fn only_busy_is_a_dissent() {
        let refused = |status| OperationFailure::Refused {
            operation: "DADiskEject",
            status,
            detail: None,
        };
        assert!(refused(DA_RETURN_BUSY).is_dissent());
        assert!(refused(unix_err(libc::EBUSY)).is_dissent());
        assert!(
            OperationFailure::Refused {
                operation: "DADiskUnmount",
                status: DA_RETURN_BUSY,
                detail: None,
            }
            .is_dissent()
        );
        for status in [
            DA_RETURN_ERROR,
            DA_RETURN_NOT_MOUNTED,
            da_return(0x08),
            unix_err(libc::EPERM),
            0x1234,
            0,
        ] {
            assert!(!refused(status).is_dissent(), "{status:#X}");
        }
        for status in [DA_RETURN_BUSY, unix_err(libc::EBUSY)] {
            let mount = OperationFailure::Refused {
                operation: MOUNT_OPERATION,
                status,
                detail: None,
            };
            assert!(!mount.is_dissent(), "{status:#X}");
        }
        let unavailable = OperationFailure::Unavailable {
            operation: "DASessionCreate",
            source: io::Error::other("returned no session"),
        };
        assert!(!unavailable.is_dissent());
    }

    #[test]
    fn a_failure_says_the_native_answer() {
        let busy = OperationFailure::Refused {
            operation: "DADiskUnmount",
            status: DA_RETURN_BUSY,
            detail: Some("held open".to_owned()),
        };
        assert_eq!(
            busy.to_string(),
            "DiskArbitration DADiskUnmount was refused: kDAReturnBusy (0xF8DA0002): held open"
        );
        assert!(busy.source().is_none());
        let errno = OperationFailure::Refused {
            operation: "DADiskEject",
            status: unix_err(libc::EBUSY),
            detail: None,
        };
        assert_eq!(
            errno.to_string(),
            format!(
                "DiskArbitration DADiskEject was refused: unix error 16 ({}) (0x0000C010)",
                io::Error::from_raw_os_error(libc::EBUSY)
            )
        );
        let unknown = OperationFailure::Refused {
            operation: "DADiskEject",
            status: 0x7000_0001,
            detail: None,
        };
        assert_eq!(
            unknown.to_string(),
            "DiskArbitration DADiskEject was refused: unrecognised DiskArbitration status (0x70000001)"
        );
        let mounted = OperationFailure::Refused {
            operation: MOUNT_OPERATION,
            status: DA_RETURN_BUSY,
            detail: None,
        };
        assert_eq!(
            mounted.to_string(),
            "DiskArbitration DADiskMountWithArguments was refused: kDAReturnBusy (0xF8DA0002)"
        );
        let unavailable = OperationFailure::Unavailable {
            operation: "DADiskCreateFromIOMedia",
            source: io::Error::other("returned no disk"),
        };
        assert_eq!(
            unavailable.to_string(),
            "DiskArbitration DADiskCreateFromIOMedia could not run: returned no disk"
        );
        assert_eq!(
            unavailable.source().map(ToString::to_string).as_deref(),
            Some("returned no disk")
        );
    }

    /// The disk-image class's schedule: first warning at 120 s, then every 30 s, never a last one.
    #[test]
    fn an_unanswered_request_is_warned_about_on_the_disk_image_schedule() {
        assert_eq!(warning_due(0), Duration::from_secs(120));
        assert_eq!(warning_due(1), Duration::from_secs(150));
        assert_eq!(warning_due(2), Duration::from_secs(180));
        assert_eq!(warning_due(2880), Duration::from_secs(120 + 30 * 2880));
    }
}
