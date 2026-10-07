//! A live FSEvents stream and fseventsd's persistent event log, read through CoreServices itself
//! (01_storage.md, "The event log"). Shared by the test binaries that mount real APFS volumes
//! (`#[path]`-included, not a crate).
//!
//! fseventsd learns of a mount asynchronously, and while it has not, a stream misses the new
//! volume's events and fseventsd still answers the previous mount's log for its device. A
//! [`Watch`] on the directory a volume mounts under sees fseventsd's own `Mount` event for it:
//! the point from which the volume's live events and its log are fseventsd's view of this mount.

use std::ffi::{CStr, CString, c_char, c_void};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{Duration, Instant};

const SINCE_NOW: u64 = u64::MAX;
const NO_DEFER: u32 = 0x2;
const FILE_EVENTS: u32 = 0x10;
const MOUNT: u32 = 0x40;
const UNMOUNT: u32 = 0x80;
/// How long an expected event may take. Only a failure waits it out: every wait ends on the
/// event's own callback.
const DELIVERY_BOUND: Duration = Duration::from_secs(10);

type Callback =
    unsafe extern "C" fn(*const c_void, *mut c_void, usize, *mut c_void, *const u32, *const u64);

#[repr(C)]
struct StreamContext {
    version: isize,
    info: *mut c_void,
    retain: Option<unsafe extern "C" fn(*const c_void) -> *const c_void>,
    release: Option<unsafe extern "C" fn(*const c_void)>,
    copy_description: Option<unsafe extern "C" fn(*const c_void) -> *const c_void>,
}

#[link(name = "CoreServices", kind = "framework")]
unsafe extern "C" {
    static kCFRunLoopDefaultMode: *const c_void;
    fn CFStringCreateWithFileSystemRepresentation(
        allocator: *const c_void,
        path: *const c_char,
    ) -> *const c_void;
    fn CFArrayCreate(
        allocator: *const c_void,
        values: *const *const c_void,
        count: isize,
        callbacks: *const c_void,
    ) -> *const c_void;
    fn CFRelease(value: *const c_void);
    fn CFRunLoopGetCurrent() -> *mut c_void;
    fn CFRunLoopRunInMode(mode: *const c_void, seconds: f64, return_after_source: u8) -> i32;
    fn FSEventStreamCreate(
        allocator: *const c_void,
        callback: Callback,
        context: *mut StreamContext,
        paths: *const c_void,
        since: u64,
        latency: f64,
        flags: u32,
    ) -> *mut c_void;
    fn FSEventStreamScheduleWithRunLoop(
        stream: *mut c_void,
        run_loop: *mut c_void,
        mode: *const c_void,
    );
    fn FSEventStreamStart(stream: *mut c_void) -> u8;
    fn FSEventStreamFlushSync(stream: *mut c_void);
    fn FSEventStreamStop(stream: *mut c_void);
    fn FSEventStreamInvalidate(stream: *mut c_void);
    fn FSEventStreamRelease(stream: *mut c_void);
    fn FSEventsCopyUUIDForDevice(device: i32) -> *const c_void;
}

/// Every event a stream delivered, in order, with its flags.
type Seen = Vec<(PathBuf, u32)>;

unsafe extern "C" fn changed(
    _stream: *const c_void,
    context: *mut c_void,
    count: usize,
    paths: *mut c_void,
    flags: *const u32,
    _ids: *const u64,
) {
    // SAFETY: the stream runs only on its creator's runloop, and its context is the boxed `Seen`
    // its `Watch` owns, which outlives the stream: `Watch::drop` stops, invalidates and releases
    // the stream before the box goes. Without UseCFTypes, CoreServices hands over `count`
    // C-string paths and `count` flags.
    let seen = unsafe { &mut *context.cast::<Seen>() };
    let paths = paths.cast::<*const c_char>();
    for index in 0..count {
        let path = unsafe { CStr::from_ptr(*paths.add(index)) };
        let flag = unsafe { *flags.add(index) };
        seen.push((
            PathBuf::from(std::ffi::OsStr::from_bytes(path.to_bytes())),
            flag,
        ));
    }
}

/// A live stream of file events under one directory, delivered on the creating thread's runloop
/// while that thread waits in [`Watch::wait_until`].
pub struct Watch {
    root: PathBuf,
    raw: *mut c_void,
    seen: Box<Seen>,
}

impl Watch {
    /// Watch `root`, live once the stream has started and flushed synchronously.
    pub fn start(root: &Path) -> Self {
        let root = root.canonicalize().expect("canonical watched root");
        let watched = CString::new(root.as_os_str().as_bytes()).expect("watched C path");
        let mut seen = Box::<Seen>::default();
        let mut context = StreamContext {
            version: 0,
            info: ptr::addr_of_mut!(*seen).cast(),
            retain: None,
            release: None,
            copy_description: None,
        };
        // SAFETY: every C path is NUL-terminated and alive for its call. The stream copies the
        // context and retains the path array, so both CF references are released once it holds
        // them; the context's `info` is the boxed `Seen` the Watch owns.
        let raw = unsafe {
            let path = CFStringCreateWithFileSystemRepresentation(ptr::null(), watched.as_ptr());
            assert!(!path.is_null(), "create the watched CFString");
            let paths = CFArrayCreate(ptr::null(), &path, 1, &kCFTypeArrayCallBacks);
            CFRelease(path);
            assert!(!paths.is_null(), "create the watched CFArray");
            let raw = FSEventStreamCreate(
                ptr::null(),
                changed,
                &mut context,
                paths,
                SINCE_NOW,
                0.01,
                NO_DEFER | FILE_EVENTS,
            );
            CFRelease(paths);
            raw
        };
        assert!(!raw.is_null(), "create a live FSEvents stream");
        // SAFETY: the stream is scheduled on this thread's runloop in the framework's own mode.
        unsafe {
            FSEventStreamScheduleWithRunLoop(raw, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode);
            if FSEventStreamStart(raw) == 0 {
                FSEventStreamInvalidate(raw);
                FSEventStreamRelease(raw);
                panic!("start the live FSEvents stream on {}", root.display());
            }
            FSEventStreamFlushSync(raw);
        }
        Self { root, raw, seen }
    }

    /// Run this thread's runloop until `done` holds for what the stream delivered, or fail
    /// naming `what` and everything delivered instead.
    pub fn wait_until(&mut self, what: &str, done: impl Fn(&Seen) -> bool) {
        // SAFETY: the stream is started and scheduled on this thread's runloop.
        unsafe { FSEventStreamFlushSync(self.raw) };
        let deadline = Instant::now() + DELIVERY_BOUND;
        while !done(&self.seen) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "{what} not delivered under {} within {DELIVERY_BOUND:?}; delivered: {:x?}",
                self.root.display(),
                self.seen
            );
            // SAFETY: the runloop and mode are valid; the callback does not escape this call.
            unsafe { CFRunLoopRunInMode(kCFRunLoopDefaultMode, remaining.as_secs_f64(), 1) };
        }
    }

    /// Wait until the last mount transition fseventsd reported at `mount` is `transition`, so
    /// that transition is fseventsd's view of the volume. Callers alternate — a wait for a
    /// mount, then for the unmount — so the transition waited for is the one just made.
    pub fn wait_for(&mut self, mount: &Path, transition: Transition) {
        let mount = mount.canonicalize().unwrap_or_else(|_| mount.to_owned());
        let flag = match transition {
            Transition::Mounted => MOUNT,
            Transition::Unmounted => UNMOUNT,
        };
        self.wait_until(&format!("{transition:?} at {}", mount.display()), |seen| {
            seen.iter()
                .rev()
                .find(|(path, flags)| *path == mount && flags & (MOUNT | UNMOUNT) != 0)
                .is_some_and(|(_, flags)| flags & flag != 0)
        });
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Transition {
    Mounted,
    Unmounted,
}

impl Drop for Watch {
    fn drop(&mut self) {
        // SAFETY: the Watch exclusively owns its started stream. Stopping precedes invalidation
        // and release, so no callback outlives the boxed `Seen`.
        unsafe {
            FSEventStreamStop(self.raw);
            FSEventStreamInvalidate(self.raw);
            FSEventStreamRelease(self.raw);
        }
    }
}

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    static kCFTypeArrayCallBacks: c_void;
}

/// Whether fseventsd keeps a persistent event log of the volume mounted at `root`: it answers a
/// log's UUID for the volume's device only while it writes one.
pub fn keeps_event_log(root: &Path) -> bool {
    let device = i32::try_from(root.metadata().expect("volume metadata").dev())
        .expect("a Darwin dev_t fits i32");
    // SAFETY: a non-null answer follows CF's Copy rule and is released here.
    let uuid = unsafe { FSEventsCopyUUIDForDevice(device) };
    if uuid.is_null() {
        return false;
    }
    unsafe { CFRelease(uuid) };
    true
}
