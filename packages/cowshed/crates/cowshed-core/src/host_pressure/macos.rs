//! macOS readers: the memory pressure level the kernel publishes and the swap it has in use, each
//! one `sysctlbyname` per reading. The pressure sysctl answers the dispatch level, not XNU's
//! internal `vm_pressure_level`: `sysctl_memorystatus_vm_pressure_level` copies out
//! `convert_internal_pressure_level_to_dispatch_level`, which folds Urgent into Warning, so its
//! whole range is `NOTE_MEMORYSTATUS_PRESSURE_NORMAL` 1, `_WARN` 2 and `_CRITICAL` 4
//! (xnu bsd/kern/kern_memorystatus_notify.c; doc/vm/memorystatus_notify.md). Any other value is
//! `Malformed`, never `Normal`.

use std::ffi::{CStr, c_void};
use std::io;
use std::ptr::NonNull;

use cowshed_gateway_types::host_status::{DarwinPressureLevel, MemoryPressure, SwapUsage};
use tokio::sync::watch;

use super::{HostPressureError, Reading};

const PRESSURE_LEVEL: &CStr = c"kern.memorystatus_vm_pressure_level";
const SWAP_USAGE: &CStr = c"vm.swapusage";

/// The host's memory pressure level now.
pub fn read_memory_pressure() -> Result<MemoryPressure, HostPressureError> {
    let mut level: libc::c_int = 0;
    read(PRESSURE_LEVEL, &mut level)?;
    let level = darwin_level(level).map_err(|raw| HostPressureError::Malformed {
        file: name(PRESSURE_LEVEL),
        text: raw.to_string(),
    })?;
    Ok(MemoryPressure::Darwin { level })
}

/// The level a raw `kern.memorystatus_vm_pressure_level` answer names, or the raw value when it
/// names none: the sysctl answers only the dispatch levels 1, 2 and 4.
fn darwin_level(raw: libc::c_int) -> Result<DarwinPressureLevel, libc::c_int> {
    match raw {
        1 => Ok(DarwinPressureLevel::Normal),
        2 => Ok(DarwinPressureLevel::Warning),
        4 => Ok(DarwinPressureLevel::Critical),
        other => Err(other),
    }
}

/// The swap the host has in use and its current size, in bytes. macOS grows and shrinks its swap
/// files on demand, so the total is the size now, not a configured ceiling.
pub fn read_swap() -> Result<SwapUsage, HostPressureError> {
    let mut usage = libc::xsw_usage {
        xsu_total: 0,
        xsu_avail: 0,
        xsu_used: 0,
        xsu_pagesize: 0,
        xsu_encrypted: 0,
    };
    read(SWAP_USAGE, &mut usage)?;
    SwapUsage::new(usage.xsu_used, usage.xsu_total).map_err(|error| HostPressureError::Malformed {
        file: name(SWAP_USAGE),
        text: format!(
            "used {} of {} bytes: {error}",
            usage.xsu_used, usage.xsu_total
        ),
    })
}

/// A sysctl value made of plain integers only, so every byte pattern the kernel writes into it is
/// a value of the type.
///
/// # Safety
///
/// Implement only for types whose every bit pattern is valid.
unsafe trait Plain: Copy {}
// SAFETY: an integer.
unsafe impl Plain for libc::c_int {}
// SAFETY: three u64, a u32 and a boolean_t, which is an i32.
unsafe impl Plain for libc::xsw_usage {}

/// Read the sysctl `sysctl` into `value`; a kernel answer of any other width is `Malformed`.
fn read<T: Plain>(sysctl: &'static CStr, value: &mut T) -> Result<(), HostPressureError> {
    let width = size_of::<T>();
    let mut size = width;
    // SAFETY: `sysctl` is NUL-terminated; `value` is writable for `size` bytes, every pattern of
    // which is a `T` (`Plain`), and the kernel writes at most `size` and updates it in place.
    // No new value is set.
    let status = unsafe {
        libc::sysctlbyname(
            sysctl.as_ptr(),
            std::ptr::from_mut(value).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(HostPressureError::Read {
            file: name(sysctl),
            cause: io::Error::last_os_error(),
        });
    }
    if size != width {
        return Err(HostPressureError::Malformed {
            file: name(sysctl),
            text: format!("{size} bytes where {width} were expected"),
        });
    }
    Ok(())
}

/// The sysctl's name as the error reports it.
const fn name(sysctl: &'static CStr) -> &'static str {
    match sysctl.to_str() {
        Ok(name) => name,
        Err(_) => panic!("a sysctl name is ASCII"),
    }
}

/// libdispatch's memory-pressure source type, `DISPATCH_SOURCE_TYPE_MEMORYPRESSURE`
/// (dispatch/source.h): libSystem's own, so no dependency.
#[repr(C)]
struct DispatchSourceType {
    _opaque: [u8; 0],
}

unsafe extern "C" {
    static _dispatch_source_type_memorypressure: DispatchSourceType;
    fn dispatch_get_global_queue(identifier: isize, flags: usize) -> *mut c_void;
    fn dispatch_source_create(
        kind: *const DispatchSourceType,
        handle: usize,
        mask: usize,
        queue: *mut c_void,
    ) -> *mut c_void;
    fn dispatch_set_context(object: *mut c_void, context: *mut c_void);
    fn dispatch_source_set_event_handler_f(
        source: *mut c_void,
        handler: unsafe extern "C" fn(*mut c_void),
    );
    fn dispatch_source_set_cancel_handler_f(
        source: *mut c_void,
        handler: unsafe extern "C" fn(*mut c_void),
    );
    fn dispatch_resume(object: *mut c_void);
    fn dispatch_source_cancel(source: *mut c_void);
    fn dispatch_release(object: *mut c_void);
}

/// `DISPATCH_MEMORYPRESSURE_NORMAL | _WARN | _CRITICAL`: every level change, the drop included.
const EVERY_LEVEL: usize = 0x01 | 0x02 | 0x04;
const DISPATCH_QUEUE_PRIORITY_DEFAULT: isize = 0;

/// The kernel's memory-pressure notification, sending a fresh reading of the level at each change.
/// Dropping it cancels the source; the sender goes with the cancel handler, which libdispatch runs
/// only once no event handler is running.
pub struct Notifier {
    source: NonNull<c_void>,
}

// SAFETY: a dispatch source may be cancelled and released from any thread.
unsafe impl Send for Notifier {}

/// Subscribe `sender` to the kernel's memory-pressure notification.
pub fn notify(sender: watch::Sender<Reading>) -> io::Result<Notifier> {
    let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
    // SAFETY: the source type is libdispatch's static; the queue is a global queue that needs no
    // release; the context is the boxed sender, which the cancel handler alone frees.
    unsafe {
        let queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
        let source = dispatch_source_create(
            &raw const _dispatch_source_type_memorypressure,
            0,
            EVERY_LEVEL,
            queue,
        );
        let Some(source) = NonNull::new(source) else {
            drop(Box::from_raw(context.cast::<watch::Sender<Reading>>()));
            return Err(io::Error::other(
                "dispatch_source_create made no memory-pressure source",
            ));
        };
        dispatch_set_context(source.as_ptr(), context);
        dispatch_source_set_event_handler_f(source.as_ptr(), on_change);
        dispatch_source_set_cancel_handler_f(source.as_ptr(), on_cancel);
        dispatch_resume(source.as_ptr());
        Ok(Notifier { source })
    }
}

/// A level change: the level the sysctl publishes now is the report.
unsafe extern "C" fn on_change(context: *mut c_void) {
    // SAFETY: the context is the sender `notify` boxed, freed only by the cancel handler, which
    // runs after the last event handler returned.
    let sender = unsafe { &*context.cast::<watch::Sender<Reading>>() };
    sender.send_replace(super::reading());
}

unsafe extern "C" fn on_cancel(context: *mut c_void) {
    // SAFETY: the context is the sender `notify` boxed; no event handler runs after this one.
    drop(unsafe { Box::from_raw(context.cast::<watch::Sender<Reading>>()) });
}

impl Drop for Notifier {
    fn drop(&mut self) {
        // SAFETY: the source is this notifier's own, cancelled and released once.
        unsafe {
            dispatch_source_cancel(self.source.as_ptr());
            dispatch_release(self.source.as_ptr());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the three dispatch levels are levels; XNU's internal numbering (0 Normal through 4
    /// Jetsam) and every other value are refused, never read as `Normal`.
    #[test]
    fn only_the_dispatch_levels_are_levels() {
        assert_eq!(darwin_level(1), Ok(DarwinPressureLevel::Normal));
        assert_eq!(darwin_level(2), Ok(DarwinPressureLevel::Warning));
        assert_eq!(darwin_level(4), Ok(DarwinPressureLevel::Critical));
        for raw in [0, 3, 8, -1] {
            assert_eq!(darwin_level(raw), Err(raw));
        }
    }

    /// The kernel's own feed: libdispatch makes the memory-pressure source, so the feed's first
    /// report is the level the sysctl publishes, never the subscription's refusal; dropping it
    /// cancels the source.
    #[tokio::test]
    async fn the_kernel_feed_opens_with_the_published_level() {
        let mut feed = super::super::Source::Kernel.open();
        let reading = feed.current();
        assert!(
            matches!(reading, Reading::Known(MemoryPressure::Darwin { .. })),
            "{reading:?}"
        );
        drop(feed);
    }
}
