//! Real filesystem roots for lifecycle tests that own no disk images.
//!
//! Cleanup ends the processes working beneath the root before removing it. It never inventories,
//! attaches or releases disk images; the prefix is separate from the real-APFS fixture's sweep.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "../../../cowshed-os/tests/support/scratch_processes.rs"]
pub(crate) mod processes;

pub(crate) const ROOT_PREFIX: &str = "cowshed-dtest-";

pub struct ScratchRoot {
    path: PathBuf,
}

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        static SWEPT: Mutex<bool> = Mutex::new(false);
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let mut swept = SWEPT.lock().map_err(|error| {
            std::io::Error::other(format!("directory fixture sweep lock: {error}"))
        })?;
        if !*swept {
            sweep_dead_roots()?;
            *swept = true;
        }
        drop(swept);
        let path = PathBuf::from(format!(
            "/private/tmp/{ROOT_PREFIX}{}-{}-{label}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn reclaim(path: &Path) -> std::io::Result<()> {
    processes::end_processes_in(|cwd| cwd.starts_with(path))?;
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn sweep_dead_roots() -> std::io::Result<()> {
    for entry in fs::read_dir("/private/tmp")? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(pid) = name
            .to_str()
            .and_then(|name| name.strip_prefix(ROOT_PREFIX))
            .and_then(|suffix| suffix.split('-').next())
            .and_then(|pid| pid.parse::<libc::pid_t>().ok())
            .filter(|pid| *pid > 0)
        else {
            continue;
        };
        // SAFETY: signal zero checks one positive pid without delivering a signal.
        if unsafe { libc::kill(pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            && entry.file_type()?.is_dir()
        {
            reclaim(&entry.path())?;
        }
    }
    Ok(())
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        if let Err(error) = reclaim(&self.path) {
            cowshed_os::eprintln!(
                "directory fixture {}: {error}; keeping the root for the next run",
                self.path.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::fork_lock::Spawn as _;
    use std::io::BufRead;
    use std::process::{Command, Stdio};

    #[test]
    fn dropping_a_directory_root_preserves_a_symlinks_target() {
        let outside = ScratchRoot::new("outside").unwrap();
        fs::write(outside.path().join("kept"), "outside").unwrap();
        let root = ScratchRoot::new("links").unwrap();
        let path = root.path().to_path_buf();
        std::os::unix::fs::symlink(outside.path(), path.join("linked")).unwrap();
        drop(root);
        assert!(!path.exists());
        assert_eq!(
            fs::read_to_string(outside.path().join("kept")).unwrap(),
            "outside"
        );
    }

    #[test]
    fn a_failed_directory_fixture_ends_its_worker_without_touching_another_root() {
        let outside = ScratchRoot::new("live-outside").unwrap();
        let root = ScratchRoot::new("live-worker").unwrap();
        let path = root.path().to_path_buf();
        let spawn = |cwd: &Path| {
            let mut child = Command::new("/bin/sh")
                .args(["-c", "printf 'ready\\n'; exec /bin/sleep 60"])
                .current_dir(cwd)
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
        other.wait().unwrap();
        assert!(failed.is_err());
        assert!(!worker.wait().unwrap().success());
        assert!(outside_running, "another root's worker must survive");
        assert!(!path.exists());
        assert!(
            processes::processes_in(|cwd| cwd.starts_with(&path))
                .unwrap()
                .is_empty()
        );
    }
}
