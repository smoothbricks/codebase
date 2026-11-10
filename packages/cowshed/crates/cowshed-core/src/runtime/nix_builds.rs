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

/// Why the daemon's builds cannot be seen.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Unwatched {
    /// The `nixbld` group does not exist or lists no user with an account.
    NoBuildUsers,
    /// The daemon's systemd service cgroup does not exist: a single-user install, or a daemon
    /// systemd does not run.
    #[cfg(any(target_os = "linux", test))]
    NoDaemonCgroup(std::path::PathBuf),
}

impl std::fmt::Display for Unwatched {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoBuildUsers => formatter.write_str("the host has no nixbld build users"),
            #[cfg(any(target_os = "linux", test))]
            Self::NoDaemonCgroup(path) => write!(
                formatter,
                "{} does not exist, so systemd runs no Nix daemon here",
                path.display()
            ),
        }
    }
}

/// What the Nix daemon's build users run now.
pub(super) fn running() -> io::Result<Builds> {
    let users = build_users()?;
    if users.is_empty() {
        return Ok(Builds::Unwatched(Unwatched::NoBuildUsers));
    }
    builds_of(&users)
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

/// Whether a build user runs a process, as the kernel lists each one's.
#[cfg(target_os = "macos")]
fn builds_of(users: &[libc::uid_t]) -> io::Result<Builds> {
    Ok(if any_process_of(users)? {
        Builds::Running
    } else {
        Builds::Idle
    })
}

/// The Nix daemon's systemd service cgroup, under the unified hierarchy. Every build the daemon
/// starts is born inside it, in it or in a child cgroup (`use-cgroups`).
#[cfg(any(target_os = "linux", test))]
const DAEMON_CGROUP: &str = "system.slice/nix-daemon.service";

/// Whether a process of one of `users` is in the Nix daemon's cgroup subtree: the daemon's own
/// record names exactly its processes, and only those are read (AGENTS.md, "Proof").
#[cfg(target_os = "linux")]
fn builds_of(users: &[libc::uid_t]) -> io::Result<Builds> {
    use std::os::unix::fs::MetadataExt as _;
    daemon_builds(
        std::path::Path::new(super::job_cgroup::CGROUP_MOUNT),
        users,
        &|pid| match std::fs::metadata(format!("/proc/{pid}")) {
            Ok(metadata) => Ok(Some(metadata.uid())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        },
    )
}

/// [`builds_of`] over the unified hierarchy mounted at `cgroups`. No daemon service cgroup there
/// is an answer naming its path; any other failure to read it is an error.
#[cfg(any(target_os = "linux", test))]
fn daemon_builds(
    cgroups: &std::path::Path,
    users: &[libc::uid_t],
    uid_of: &impl Fn(u32) -> io::Result<Option<libc::uid_t>>,
) -> io::Result<Builds> {
    let daemon = cgroups.join(DAEMON_CGROUP);
    let present = daemon
        .try_exists()
        .map_err(|error| io::Error::new(error.kind(), format!("{}: {error}", daemon.display())))?;
    if !present {
        return Ok(Builds::Unwatched(Unwatched::NoDaemonCgroup(daemon)));
    }
    Ok(if member_runs_as(&daemon, users, uid_of)? {
        Builds::Running
    } else {
        Builds::Idle
    })
}

/// Whether a member of `cgroup` or of a cgroup below it runs as one of `users`, as `uid_of`
/// answers for a pid (`None` once it exited). A child cgroup removed during the walk held
/// nothing; the root missing is an error.
#[cfg(any(target_os = "linux", test))]
fn member_runs_as(
    cgroup: &std::path::Path,
    users: &[libc::uid_t],
    uid_of: &impl Fn(u32) -> io::Result<Option<libc::uid_t>>,
) -> io::Result<bool> {
    let named = |error: io::Error, path: &std::path::Path| {
        io::Error::new(error.kind(), format!("{}: {error}", path.display()))
    };
    let procs = cgroup.join("cgroup.procs");
    let members = std::fs::read_to_string(&procs).map_err(|error| named(error, &procs))?;
    for line in members.lines() {
        let pid = line.parse::<u32>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: invalid member pid {line:?}", procs.display()),
            )
        })?;
        if uid_of(pid)?.is_some_and(|uid| users.contains(&uid)) {
            return Ok(true);
        }
    }
    for entry in std::fs::read_dir(cgroup).map_err(|error| named(error, cgroup))? {
        let entry = entry.map_err(|error| named(error, cgroup))?;
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        match member_runs_as(&entry.path(), users, uid_of) {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn builds_of(_users: &[libc::uid_t]) -> io::Result<Builds> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "listing a user's processes is implemented for macOS and Linux",
    ))
}

#[cfg(test)]
mod tests {
    /// This test's own uid runs a process (this one); a uid no account uses runs none.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_users_live_process_is_seen_and_an_unused_uid_has_none() {
        // SAFETY: geteuid reads this process's credentials.
        let own = unsafe { libc::geteuid() };
        assert!(super::any_process_of(&[own]).unwrap());
        assert!(!super::any_process_of(&[0x7fff_fff0]).unwrap());
    }

    /// A build user's process in a cgroup or a child cgroup is seen, the daemon's own root
    /// process is not a build, and a cgroup missing where the walk reads it is an error.
    #[test]
    fn a_build_users_member_of_the_daemon_cgroup_is_seen() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-nix-cgroup-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let child = root.join("nix-build-uid-351");
        std::fs::create_dir_all(&child).unwrap();
        std::fs::write(root.join("cgroup.procs"), "100\n").unwrap();
        std::fs::write(child.join("cgroup.procs"), "200\n").unwrap();
        let uid_of = |pid: u32| -> std::io::Result<Option<libc::uid_t>> {
            Ok(match pid {
                100 => Some(0),
                200 => Some(351),
                _ => None,
            })
        };
        assert!(super::member_runs_as(&root, &[351], &uid_of).unwrap());
        assert!(!super::member_runs_as(&root, &[352], &uid_of).unwrap());
        std::fs::write(child.join("cgroup.procs"), "").unwrap();
        assert!(!super::member_runs_as(&root, &[351], &uid_of).unwrap());
        let missing = super::member_runs_as(&root.join("absent"), &[351], &uid_of).unwrap_err();
        assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);
        assert!(
            missing.to_string().contains("absent/cgroup.procs"),
            "{missing}"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A host whose Nix daemon systemd does not run (a single-user install, or a daemon started
    /// another way) has no daemon service cgroup: the probe answers that the daemon's builds are
    /// unwatched and names the path, instead of failing. With the cgroup, it answers what runs.
    #[test]
    fn a_host_without_the_daemon_service_cgroup_answers() -> std::io::Result<()> {
        use super::{Builds, Unwatched};
        let cgroups = std::env::temp_dir().join(format!(
            "cowshed-nix-no-daemon-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(cgroups.join("system.slice"))?;
        let unwatched = super::daemon_builds(&cgroups, &[351], &|_| Ok(None));
        let daemon = cgroups.join(super::DAEMON_CGROUP);
        std::fs::create_dir_all(&daemon)?;
        std::fs::write(daemon.join("cgroup.procs"), "200\n")?;
        let uid_of = |_| Ok(Some(351));
        let running = super::daemon_builds(&cgroups, &[351], &uid_of);
        let idle = super::daemon_builds(&cgroups, &[352], &uid_of);
        std::fs::remove_dir_all(&cgroups)?;
        assert_eq!(
            unwatched?,
            Builds::Unwatched(Unwatched::NoDaemonCgroup(daemon))
        );
        assert_eq!(running?, Builds::Running);
        assert_eq!(idle?, Builds::Idle);
        Ok(())
    }

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
