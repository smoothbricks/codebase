//! Whether the host's Nix daemon is building now. The daemon runs each build as one of its build
//! users, the members of the `nixbld` group (`_nixbld1`.. on macOS, `nixbld1`.. on Linux), so a
//! live process of one of them is a build in progress. It is a proxy: it cannot say whose build
//! it is, and a daemon set to `auto-allocate-uids` builds as uids no group lists, which it misses.
//! Each user's processes are listed by the kernel (`proc_listpids(PROC_UID_ONLY)` on macOS, the
//! owner of each `/proc/<pid>` on Linux); nothing is spawned.

use std::ffi::{CStr, CString};
use std::io;
use std::mem::MaybeUninit;

/// The group whose members the Nix daemon builds as.
const BUILD_GROUP: &CStr = c"nixbld";

/// The most a group or user entry's strings may take before a lookup gives up.
const MAX_ENTRY_BYTES: usize = 1 << 20;

/// Whether a process of a Nix build user is alive now. A host without the build group runs no
/// daemon builds, so none is.
pub(super) fn running() -> io::Result<bool> {
    let users = build_users()?;
    if users.is_empty() {
        return Ok(false);
    }
    any_process_of(&users)
}

/// The uids of [`BUILD_GROUP`]'s members; none when the group does not exist. A member without
/// a user entry runs nothing.
fn build_users() -> io::Result<Vec<libc::uid_t>> {
    let mut uids = Vec::new();
    for member in group_members(BUILD_GROUP)?.unwrap_or_default() {
        if let Some(uid) = user_id(&member)? {
            uids.push(uid);
        }
    }
    Ok(uids)
}

/// The members `getgrnam_r` lists for group `name`; `None` when there is no such group.
fn group_members(name: &CStr) -> io::Result<Option<Vec<CString>>> {
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let mut entry = MaybeUninit::<libc::group>::zeroed();
        let mut found = std::ptr::null_mut();
        // SAFETY: entry and buffer are live writable storage; a successful result points into
        // them.
        let status = unsafe {
            libc::getgrnam_r(
                name.as_ptr(),
                entry.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut found,
            )
        };
        match status {
            0 if found.is_null() => return Ok(None),
            0 => {}
            libc::ERANGE if buffer.len() < MAX_ENTRY_BYTES => {
                buffer.resize(buffer.len() * 4, 0);
                continue;
            }
            error => {
                let error = io::Error::from_raw_os_error(error);
                return Err(io::Error::new(
                    error.kind(),
                    format!("getgrnam_r({name:?}): {error}"),
                ));
            }
        }
        // SAFETY: a successful getgrnam_r with a non-null result initialized entry.
        // SAFETY: a successful getgrnam_r with a non-null result initialized entry.
        let mut member = unsafe { entry.assume_init() }.gr_mem;
        let mut members = Vec::new();
        // `gr_mem` is a null-terminated array of NUL-terminated strings in `buffer`, which
        // outlives this walk. Darwin lays the array at whatever offset follows the strings it
        // copied, so its slots are read unaligned (measured: a slot at an odd address).
        while !member.is_null() {
            // SAFETY: `member` points at a slot of that array, the null one at the latest.
            let name = unsafe { member.read_unaligned() };
            if name.is_null() {
                break;
            }
            // SAFETY: a non-null slot names a NUL-terminated string in `buffer`.
            members.push(unsafe { CStr::from_ptr(name) }.to_owned());
            // SAFETY: a non-null slot is followed by another slot of the same array.
            member = unsafe { member.add(1) };
        }
        return Ok(Some(members));
    }
}

/// The uid `getpwnam_r` gives user `name`; `None` when there is no such user.
fn user_id(name: &CStr) -> io::Result<Option<libc::uid_t>> {
    let mut buffer = vec![0_u8; 4 * 1024];
    loop {
        let mut entry = MaybeUninit::<libc::passwd>::zeroed();
        let mut found = std::ptr::null_mut();
        // SAFETY: entry and buffer are live writable storage; a successful result points into
        // them.
        let status = unsafe {
            libc::getpwnam_r(
                name.as_ptr(),
                entry.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut found,
            )
        };
        return match status {
            0 if found.is_null() => Ok(None),
            // SAFETY: a successful getpwnam_r with a non-null result initialized entry.
            0 => Ok(Some(unsafe { entry.assume_init() }.pw_uid)),
            libc::ERANGE if buffer.len() < MAX_ENTRY_BYTES => {
                buffer.resize(buffer.len() * 4, 0);
                continue;
            }
            error => {
                let error = io::Error::from_raw_os_error(error);
                Err(io::Error::new(
                    error.kind(),
                    format!("getpwnam_r({name:?}): {error}"),
                ))
            }
        };
    }
}

/// `PROC_UID_ONLY` of `<sys/proc_info.h>`, which the libc crate does not declare: the processes
/// whose effective uid is the one named.
#[cfg(target_os = "macos")]
const PROC_UID_ONLY: u32 = 4;

/// Whether any process runs as one of `users`. libproc's `proc_listpids` answers the kernel's
/// -1 with 0 and leaves errno set, so a 0 is no process only when errno stayed clear across the
/// call (as `process_events::list_child_pids` reads it).
#[cfg(target_os = "macos")]
fn any_process_of(users: &[libc::uid_t]) -> io::Result<bool> {
    let mut pids: [libc::pid_t; 16] = [0; 16];
    let bytes = libc::c_int::try_from(size_of_val(&pids)).map_err(io::Error::other)?;
    for &uid in users {
        // SAFETY: `__error` names this thread's errno.
        unsafe { *libc::__error() = 0 };
        // SAFETY: `pids` is writable for `bytes` bytes, its whole size.
        let filled =
            unsafe { libc::proc_listpids(PROC_UID_ONLY, uid, pids.as_mut_ptr().cast(), bytes) };
        let error = io::Error::last_os_error();
        match usize::try_from(filled) {
            Ok(filled) if filled > 0 => {
                let listed = filled / size_of::<libc::pid_t>();
                if pids[..listed.min(pids.len())].iter().any(|&pid| pid > 0) {
                    return Ok(true);
                }
            }
            _ if error.raw_os_error() == Some(0) => {}
            _ => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("proc_listpids(PROC_UID_ONLY, {uid}): {error}"),
                ));
            }
        }
    }
    Ok(false)
}

/// Whether any process runs as one of `users`: the owner of its `/proc/<pid>` directory, its
/// effective uid (root for a process made non-dumpable, which a build user's is not). A process
/// that exits during the walk is skipped.
#[cfg(target_os = "linux")]
fn any_process_of(users: &[libc::uid_t]) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt as _;
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        if !entry
            .file_name()
            .as_encoded_bytes()
            .iter()
            .all(u8::is_ascii_digit)
        {
            continue;
        }
        match entry.metadata() {
            Ok(metadata) if users.contains(&metadata.uid()) => return Ok(true),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn any_process_of(_users: &[libc::uid_t]) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "listing a user's processes is implemented for macOS and Linux",
    ))
}

#[cfg(test)]
mod tests {
    /// This test's own uid runs a process (this one); a uid no account uses runs none.
    #[test]
    fn a_users_live_process_is_seen_and_an_unused_uid_has_none() {
        // SAFETY: geteuid reads this process's credentials.
        let own = unsafe { libc::geteuid() };
        assert!(super::any_process_of(&[own]).unwrap());
        assert!(!super::any_process_of(&[0x7fff_fff0]).unwrap());
    }

    /// The probe answers on this host, with or without a Nix daemon's build group.
    #[test]
    fn the_probe_answers() {
        super::running().unwrap();
        if let Some(members) = super::group_members(super::BUILD_GROUP).unwrap() {
            assert_eq!(super::build_users().unwrap().len(), members.len());
        }
    }
}
