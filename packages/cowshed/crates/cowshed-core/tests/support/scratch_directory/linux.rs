//! Linux directory roots: a child of the canonical `/tmp` under a pid-bearing name the shared-temp
//! sweep claims (`cowshed_os::scratch::create_named`).
//!
//! Cleanup ends the root's workers, then removes its tree, several threads unlinking at once. A
//! tree it cannot remove stays where it is: the shared-temp sweep takes it, by the pid in its
//! name, once this run has ended. Linux has no per-user namespace root to retire one under.

use std::fs;
use std::io;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::LABEL_FILE;
use cowshed_os::process::ExitWatch;

#[path = "../../../../cowshed-apfs/tests/support/scratch_tree.rs"]
mod tree;

/// What every root's name opens with; `create_named` adds `-p<pid and nonce>`, the same length
/// for every root.
const PREFIX: &str = "cowshed-dtest";

/// The grace used by `nx daemon --stop`, before force-ending a surviving worker.
const EXIT_GRACE: Duration = Duration::from_secs(10);

pub struct ScratchRoot {
    path: PathBuf,
}

impl ScratchRoot {
    pub fn new(label: &str) -> io::Result<Self> {
        let parent = fs::canonicalize("/tmp")?;
        let root = Self {
            path: cowshed_os::scratch::create_named(&parent, PREFIX)?,
        };
        cowshed_os::eprintln!("directory fixture {}: {label}", root.path.display());
        fs::write(root.path.join(LABEL_FILE), label)?;
        Ok(root)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        let started = Instant::now();
        match reclaim(&self.path) {
            Ok(()) => cowshed_os::eprintln!(
                "directory fixture {}: removed elapsed={:?}",
                self.path.display(),
                started.elapsed()
            ),
            Err(error) => cowshed_os::eprintln!(
                "directory fixture {}: {error}; what is left stays for the shared-temp sweep, \
                 which takes it once this run has ended elapsed={:?}",
                self.path.display(),
                started.elapsed()
            ),
        }
    }
}

/// End the processes working in `path`, then remove it ([`tree::remove`]).
fn reclaim(path: &Path) -> io::Result<()> {
    end_processes_in(path)?;
    tree::remove(path)
}

/// Every process but this one whose working directory is `root` or below it, with that
/// directory, as procfs names it (`/proc/<pid>/cwd`). A process gone since the listing, a zombie
/// and another user's process name no directory, and are left out.
pub(crate) fn processes_in(root: &Path) -> io::Result<Vec<(u32, PathBuf)>> {
    let this = std::process::id();
    let mut found = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == this {
            continue;
        }
        if let Some(cwd) = working_directory(pid).filter(|cwd| cwd.starts_with(root)) {
            found.push((pid, cwd));
        }
    }
    Ok(found)
}

fn working_directory(pid: u32) -> Option<PathBuf> {
    fs::read_link(format!("/proc/{pid}/cwd")).ok()
}

/// `pid`'s executable as procfs names it, for the report of a process being ended.
fn executable(pid: u32) -> String {
    fs::read_link(format!("/proc/{pid}/exe")).map_or_else(
        |_| "an unreadable executable".to_owned(),
        |path| path.display().to_string(),
    )
}

/// End every process but this one working in `root`: `SIGTERM`, as `nx daemon --stop` ends an Nx
/// daemon, then `SIGKILL` to any that outlives [`EXIT_GRACE`]. Each is named on stderr. The tree
/// is read again once they are gone, because a process can start another in it while it ends (a
/// dying shell's job, a daemon's worker), and it is clear only once a reading finds none.
///
/// Each process is held by its pidfd before it is signalled and signalled through that, so a pid
/// handed to another process since the listing is never signalled: the working directory is read
/// once more behind the pidfd.
fn end_processes_in(root: &Path) -> io::Result<()> {
    const ROUNDS: usize = 3;
    for _ in 0..ROUNDS {
        let found = processes_in(root)?;
        if found.is_empty() {
            return Ok(());
        }
        let mut running = Vec::with_capacity(found.len());
        for (pid, cwd) in found {
            let Some(watch) = ExitWatch::of(pid)? else {
                continue;
            };
            if !working_directory(pid).is_some_and(|cwd| cwd.starts_with(root)) {
                continue;
            }
            cowshed_os::eprintln!(
                "scratch root: ending pid {pid} ({}), working in {}",
                executable(pid),
                cwd.display()
            );
            running.push((pid, watch));
        }
        for signal in [libc::SIGTERM, libc::SIGKILL] {
            running = signal_and_wait(running, signal)?;
        }
        if !running.is_empty() {
            let stayed: Vec<u32> = running.iter().map(|(pid, _)| *pid).collect();
            return Err(io::Error::other(format!(
                "pids {stayed:?} still run {EXIT_GRACE:?} after SIGKILL"
            )));
        }
    }
    let found = processes_in(root)?;
    if found.is_empty() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "processes still start in the tree after {ROUNDS} rounds of ending them: {found:?}"
    )))
}

/// Send `signal` to each of `running` through its pidfd and wait [`EXIT_GRACE`] in all for their
/// exits; those still running then.
fn signal_and_wait(
    running: Vec<(u32, ExitWatch)>,
    signal: libc::c_int,
) -> io::Result<Vec<(u32, ExitWatch)>> {
    for (pid, watch) in &running {
        // SAFETY: pidfd_send_signal reads a live descriptor and takes no info pointer.
        let sent = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                watch.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if sent != 0 {
            let error = io::Error::last_os_error();
            // Already exited: its watch answers at once below.
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(io::Error::new(
                    error.kind(),
                    format!("signal {signal} to pid {pid}: {error}"),
                ));
            }
        }
    }
    let deadline = Instant::now() + EXIT_GRACE;
    let mut stayed = Vec::new();
    for (pid, watch) in running {
        let exited = loop {
            match watch.within(deadline.saturating_duration_since(Instant::now())) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                exited => break exited?,
            }
        };
        if !exited {
            stayed.push((pid, watch));
        }
    }
    Ok(stayed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::BufRead as _;
    use std::process::{Command, Stdio};

    #[test]
    fn dropping_a_directory_root_removes_its_own_tree_and_preserves_a_symlinks_target() {
        use std::os::unix::fs::PermissionsExt as _;

        let outside = ScratchRoot::new("outside").unwrap();
        fs::write(outside.path().join("kept"), "outside").unwrap();
        let root = ScratchRoot::new("links").unwrap();
        let path = root.path().to_path_buf();
        std::os::unix::fs::symlink(outside.path(), path.join("linked")).unwrap();
        // Sealed as a tool seals its cache: the removal restores the owner's access first.
        let sealed = path.join("sealed");
        fs::create_dir_all(sealed.join("inner")).unwrap();
        fs::write(sealed.join("inner/file"), "sealed").unwrap();
        for directory in [sealed.join("inner"), sealed] {
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o555)).unwrap();
        }

        drop(root);
        assert!(!path.exists());
        assert_eq!(
            fs::read_to_string(outside.path().join("kept")).unwrap(),
            "outside"
        );
    }

    #[test]
    fn a_long_label_leaves_room_for_the_stores_deepest_sockets() {
        let label = "clean_landed_source_is_physically_deleted_before_reply_and_crash_replay";
        let root = ScratchRoot::new(label).unwrap();
        assert_eq!(
            fs::read_to_string(root.path().join(LABEL_FILE)).unwrap(),
            label
        );
        let run = root.path().join("store/run");
        let pool = run.join("cpu").join("p".repeat(22));
        fs::create_dir_all(&pool).unwrap();
        for socket in [
            run.join(format!("{}.sock", "0".repeat(32))),
            pool.join("e".repeat(22)),
        ] {
            std::os::unix::net::UnixListener::bind(&socket)
                .unwrap_or_else(|error| panic!("bind {}: {error}", socket.display()));
        }
    }

    #[test]
    fn a_failed_directory_fixture_ends_its_worker_without_touching_another_root() {
        let outside = ScratchRoot::new("live-outside").unwrap();
        let root = ScratchRoot::new("live-worker").unwrap();
        let path = root.path().to_path_buf();
        let spawn = |cwd: &Path| {
            let mut child = Command::new("/bin/sh")
                .args(["-c", "printf 'ready\\n'; exec cat"])
                .current_dir(cwd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn_locked()
                .unwrap();
            let mut ready = String::new();
            std::io::BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut ready)
                .unwrap();
            assert_eq!(ready, "ready\n");
            child
        };
        let mut worker = spawn(&path);
        let mut other = spawn(outside.path());
        let failed = std::panic::catch_unwind(|| {
            let _root = root;
            panic!("exercise fixture unwinding");
        });
        let outside_running = other.try_wait().unwrap().is_none();
        other.kill().unwrap();
        other.wait_bounded().unwrap();
        assert!(failed.is_err());
        assert!(!worker.wait_bounded().unwrap().success());
        assert!(outside_running, "another root's worker must survive");
        assert!(!path.exists());
        assert!(processes_in(&path).unwrap().is_empty());
    }

    /// The shared-temp sweep claims a root by its name alone, so a tree a killed run left is
    /// reclaimed without this fixture's help.
    #[test]
    fn a_root_is_named_as_the_shared_temp_sweep_claims_it() {
        let root = ScratchRoot::new("claimable").unwrap();
        let name = root.path().file_name().unwrap().to_str().unwrap();
        let owner = name
            .strip_prefix(&format!("{PREFIX}-p"))
            .and_then(|suffix| suffix.get(..8))
            .and_then(|pid| u32::from_str_radix(pid, 16).ok());
        assert_eq!(owner, Some(std::process::id()), "{name}");
        assert_eq!(
            root.path().parent(),
            Some(fs::canonicalize("/tmp").unwrap().as_path())
        );
    }
}
