//! The host's system `PATH`: the directories the service manager gives the daemon it starts,
//! which is where the host keeps the programs every session of its user finds.
//!
//! Each platform lists them once (`linux.rs`, `macos.rs`) and the list is read twice. The
//! daemon's definition states it behind the directory `cowshed setup` found `bun` in
//! (05_gateway.md, "The installed cowshed"). A job's `PATH` ends with it (04_sandbox.md,
//! "devenv / Nix inside the sandbox": the initial sandbox `PATH`), so a workspace with no
//! activation of its own still finds the host's `git`, `ls` and `id`. Nothing here reads the
//! `PATH` of whatever started this process.

use std::ffi::OsString;
use std::io;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// The system `PATH` as the service manager's definition of the daemon spells it. On macOS it
/// is the one launchd gives an agent that states none. On Linux it is the user manager's own,
/// with the per-user directories spelt by the specifiers the manager resolves when it loads the
/// unit, so the text is the same for every user.
pub use platform::service_path;

/// The directories a job's `PATH` ends with, for the user this process runs as, whose home is
/// `home`: [`service_path`]'s, each spelt in full, less any a sandboxed job cannot use. An entry
/// the host does not have stays: the list is the same on every host of a platform, a profile the
/// user creates later is found without a restart, and a missing directory costs a lookup that
/// fails.
pub use platform::job_directories;

/// The name of the user this process runs as, which a per-user directory is spelt with. `None`
/// when the account database has no entry for the uid.
pub fn user_name() -> io::Result<Option<OsString>> {
    use std::os::unix::ffi::OsStringExt as _;

    let mut buffer = vec![0_u8; 1024];
    loop {
        let mut entry = std::mem::MaybeUninit::<libc::passwd>::zeroed();
        let mut found = std::ptr::null_mut();
        // SAFETY: entry and buffer are live writable storage of the sizes named; a successful
        // result points into them.
        let status = unsafe {
            libc::getpwuid_r(
                libc::geteuid(),
                entry.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut found,
            )
        };
        match status {
            0 if found.is_null() => return Ok(None),
            0 => {
                // SAFETY: a successful getpwuid_r left a NUL-terminated pw_name within buffer.
                let name = unsafe { std::ffi::CStr::from_ptr((*found).pw_name) };
                return Ok(Some(OsString::from_vec(name.to_bytes().to_vec())));
            }
            // getpwnam(3), "Return value": these too say the uid was not found.
            libc::ENOENT | libc::ESRCH => return Ok(None),
            libc::ERANGE => buffer.resize(buffer.len() * 2, 0),
            error => return Err(io::Error::from_raw_os_error(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A second read of the account this test runs as answers what the first did, and a name
    /// it has is not empty.
    #[test]
    fn the_running_user_has_one_name() {
        let name = user_name().expect("the account database answers");
        assert!(
            name.as_ref().is_none_or(|name| !name.is_empty()),
            "{name:?}"
        );
        assert_eq!(user_name().expect("the account database answers"), name);
    }
}
