//! A rendered Seatbelt profile as the file `sandbox-exec -f` reads (07_api.md, `SandboxSpec::wrap`;
//! 04_sandbox.md, "Exec pipeline" step 4).
//!
//! A profile on the command line (`sandbox-exec -p <profile>`) is one argv element of the
//! launched process. Profiles grow with grants past the 128 KiB a command argument may hold
//! (`MAX_COMMAND_ARG_BYTES`), and a job's process observation reads that argv back: a leader
//! observed before `sandbox-exec` exec'd its command named the whole profile as one argument.
//! The file keeps the profile out of every argv.
//!
//! The file lives in [`profile_directory`], inside the machine-global cowshed root that every
//! rendered profile denies, read and write, to every role: no sandboxed process can replace a
//! profile a later spawn reads. It is written once, mode 0400, and removed when its last holder
//! drops it; a launch holds it until its child has exited, so `sandbox-exec` has long read it.
//! A process killed while holding one leaves its files behind, named by its pid, and the next
//! write in the directory removes the files of every process that no longer runs.

use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::api::dto::Sha256Digest;

/// Where profile files live: beside the supervisors' sockets in the store's `run` directory.
pub fn profile_directory() -> PathBuf {
    Path::new(cowshed_gateway_types::STORE_ROOT)
        .join("run")
        .join("profiles")
}

/// One rendered profile on disk, named `<sha256>.<pid>.<sequence>.sb` and removed on drop.
#[derive(Debug, Eq, PartialEq)]
pub struct ProfileFile {
    path: PathBuf,
}

/// This process's next profile file sequence number.
static NEXT: AtomicU64 = AtomicU64::new(0);

impl ProfileFile {
    /// Write `profile` into [`profile_directory`].
    pub fn write(profile: &str) -> io::Result<Arc<Self>> {
        Self::write_in(&profile_directory(), profile)
    }

    /// Write `profile` into `directory`, first removing the files of processes that no longer
    /// run.
    pub fn write_in(directory: &Path, profile: &str) -> io::Result<Arc<Self>> {
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
        sweep_dead(directory)?;
        let digest = Sha256Digest::compute(profile.as_bytes()).to_hex();
        let path = directory.join(format!(
            "{digest}.{}.{}.sb",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o400)
            .open(&path)?;
        // The holder owns the file from here, so a failed write removes it.
        let holder = Self { path };
        file.write_all(profile.as_bytes())?;
        Ok(Arc::new(holder))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ProfileFile {
    fn drop(&mut self) {
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => eprintln!(
                "cowshed: cannot remove sandbox profile file {}: {error}; it stays until a later \
                 write finds its process gone",
                self.path.display()
            ),
        }
    }
}

/// The pid a profile file's name carries, when it is one this module named.
fn owner(name: &str) -> Option<libc::pid_t> {
    let mut parts = name.strip_suffix(".sb")?.split('.');
    let (digest, pid, sequence) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some()
        || digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || sequence.parse::<u64>().is_err()
    {
        return None;
    }
    pid.parse().ok()
}

/// Remove the profile files of every process that no longer runs. A file another process
/// removed first is already gone.
fn sweep_dead(directory: &Path) -> io::Result<()> {
    let own = libc::pid_t::try_from(std::process::id()).map_err(io::Error::other)?;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let Some(pid) = entry.file_name().to_str().and_then(owner) else {
            continue;
        };
        if pid == own || cowshed_os::process::running(pid)? {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn scratch() -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "cowshed-profile-file-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }

    #[test]
    fn a_profile_is_written_read_only_and_removed_with_its_last_holder() {
        let directory = scratch();
        let file = ProfileFile::write_in(&directory, "(version 1)(allow default)").unwrap();
        assert_eq!(
            fs::read_to_string(file.path()).unwrap(),
            "(version 1)(allow default)"
        );
        assert_eq!(
            fs::metadata(file.path()).unwrap().permissions().mode() & 0o777,
            0o400
        );
        let held = Arc::clone(&file);
        let path = file.path().to_owned();
        drop(file);
        assert!(path.exists(), "a holder keeps the file");
        drop(held);
        assert!(!path.exists(), "the last holder removes it");
        fs::remove_dir(&directory).unwrap();
    }

    #[test]
    fn a_write_removes_the_files_of_processes_that_no_longer_run_and_nothing_else() {
        let directory = scratch();
        use cowshed_os::fork_lock::Spawn as _;
        let mut child = std::process::Command::new("/usr/bin/true")
            .spawn_locked()
            .unwrap();
        let dead = child.id();
        child.wait().unwrap();
        let digest = "a".repeat(64);
        let stale = directory.join(format!("{digest}.{dead}.0.sb"));
        let live = directory.join(format!("{digest}.1.0.sb"));
        let foreign = directory.join("notes.txt");
        for path in [&stale, &live, &foreign] {
            fs::write(path, b"x").unwrap();
        }
        let file = ProfileFile::write_in(&directory, "(version 1)").unwrap();
        assert!(!stale.exists(), "a dead process's file is removed");
        assert!(live.exists(), "launchd's file stays");
        assert!(foreign.exists(), "a file this module never named stays");
        drop(file);
        fs::remove_dir_all(&directory).unwrap();
    }
}
