//! macOS scans a new executable at its first launch and charges the wait to the launching run's
//! responsible process: the probe measures it, here and in the gateway daemon, and the
//! responsible process is named by libquarantine.

use std::io;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cowshed_gateway_types::{FirstLaunch, FirstLaunchProbe};

use super::{DaemonFirstLaunch, measure};

/// What measures a first launch in the calling process: [`measure`].
pub fn host_probe() -> Option<fn(&Path) -> io::Result<FirstLaunch>> {
    let probe: fn(&Path) -> io::Result<FirstLaunch> = measure;
    Some(probe)
}

/// What measures a first launch in the gateway daemon, whose responsible process every workspace
/// job has: [`DaemonFirstLaunch`] over the daemon's own executable.
pub fn daemon_probe(template: &Path) -> Option<Arc<dyn FirstLaunchProbe>> {
    let probe: Arc<dyn FirstLaunchProbe> = Arc::new(DaemonFirstLaunch::new(template.to_path_buf()));
    Some(probe)
}

/// The program macOS charges this process's launches to: its responsible process's executable,
/// as libquarantine's `responsibility_get_pid_responsible_for_pid` and `proc_pidpath` name it.
/// `None` when either does not answer.
pub fn responsible_program() -> Option<PathBuf> {
    type Responsible = unsafe extern "C" fn(libc::pid_t) -> libc::pid_t;
    // SAFETY: a NUL-terminated symbol name looked up in the process's loaded images.
    let symbol = unsafe {
        libc::dlsym(
            libc::RTLD_DEFAULT,
            c"responsibility_get_pid_responsible_for_pid".as_ptr(),
        )
    };
    if symbol.is_null() {
        return None;
    }
    // SAFETY: the symbol is libquarantine's `pid_t (pid_t)` function.
    let responsible = unsafe { std::mem::transmute::<*mut libc::c_void, Responsible>(symbol) };
    // SAFETY: it takes and returns a pid and touches no memory of ours.
    let pid = unsafe { responsible(libc::getpid()) };
    if pid <= 0 {
        return None;
    }
    let capacity = u32::try_from(libc::PROC_PIDPATHINFO_MAXSIZE).ok()?;
    let mut path = vec![0u8; usize::try_from(capacity).ok()?];
    // SAFETY: `path` is a writable buffer of the length passed.
    let length = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), capacity) };
    let length = usize::try_from(length).ok().filter(|length| *length > 0)?;
    path.truncate(length);
    Some(PathBuf::from(std::ffi::OsString::from_vec(path)))
}
