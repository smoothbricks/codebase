//! A build user's processes as the Nix daemon's systemd service cgroup lists them, under the
//! unified hierarchy; each member's uid is its `/proc/<pid>` owner.

use std::io;

use super::Builds;

/// Why the daemon's builds cannot be seen.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Unwatched {
    /// The `nixbld` group does not exist or lists no user with an account.
    NoBuildUsers,
    /// The daemon's systemd service cgroup does not exist: a single-user install, or a daemon
    /// systemd does not run.
    NoDaemonCgroup(std::path::PathBuf),
}

impl std::fmt::Display for Unwatched {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoBuildUsers => formatter.write_str("the host has no nixbld build users"),
            Self::NoDaemonCgroup(path) => write!(
                formatter,
                "{} does not exist, so systemd runs no Nix daemon here",
                path.display()
            ),
        }
    }
}

/// The Nix daemon's systemd service cgroup, under the unified hierarchy. Every build the daemon
/// starts is born inside it, in it or in a child cgroup (`use-cgroups`).
const DAEMON_CGROUP: &str = "system.slice/nix-daemon.service";

/// Whether a process of one of `users` is in the Nix daemon's cgroup subtree: the daemon's own
/// record names exactly its processes, and only those are read (AGENTS.md, "Proof").
pub(super) fn builds_of(users: &[libc::uid_t]) -> io::Result<Builds> {
    use std::os::unix::fs::MetadataExt as _;
    daemon_builds(
        std::path::Path::new(crate::runtime::job_cgroup::CGROUP_MOUNT),
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

#[cfg(test)]
mod tests {
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
}
