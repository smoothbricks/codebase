//! Whether the host's Nix daemon is building now. The daemon runs each build as one of its build
//! users, the members of the `nixbld` group (`_nixbld1`.. on macOS, `nixbld1`.. on Linux), so a
//! live process of one of them is a build in progress. It is a proxy: it cannot say whose build
//! it is, and a daemon set to `auto-allocate-uids` builds as uids no group lists, which it misses.
//! A build user's processes are read from what names them, never from a walk of every process:
//! the kernel lists one uid's processes on macOS (`proc_listpids(PROC_UID_ONLY)`); on Linux the
//! daemon's own systemd cgroup lists its processes, the builds it started among them. Nothing is
//! spawned. A host that shows none of this (no build users, or on Linux no daemon service cgroup:
//! a single-user install, or a daemon systemd does not run) answers that the daemon's builds are
//! unwatched, and why, rather than failing or claiming none runs.

use std::ffi::{CStr, CString};
use std::io;
use std::mem::MaybeUninit;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// Why the daemon's builds cannot be seen: no build users on either platform, and whatever
/// record of the daemon's builds the platform's reader finds missing.
pub(super) use platform::Unwatched;

/// The group whose members the Nix daemon builds as.
const BUILD_GROUP: &CStr = c"nixbld";

/// The most a group or user entry's strings may take before a lookup gives up.
const MAX_ENTRY_BYTES: usize = 1 << 20;

/// What the probe sees of the Nix daemon's builds.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Builds {
    /// A build user runs a process.
    Running,
    /// The build users run nothing.
    Idle,
    /// Nothing on this host shows the daemon's builds, so one may run unseen.
    Unwatched(Unwatched),
}

/// What the Nix daemon's build users run now.
pub(super) fn running() -> io::Result<Builds> {
    let users = build_users()?;
    if users.is_empty() {
        return Ok(Builds::Unwatched(Unwatched::NoBuildUsers));
    }
    platform::builds_of(&users)
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

#[cfg(test)]
mod tests {
    /// The probe answers on this host, whatever it is: a Linux host whose daemon systemd does not
    /// run answers that the daemon's builds are unwatched.
    #[test]
    fn the_probe_answers() -> std::io::Result<()> {
        super::running()?;
        if let Some(members) = super::group_members(super::BUILD_GROUP)? {
            assert_eq!(super::build_users()?.len(), members.len());
        }
        Ok(())
    }
}
