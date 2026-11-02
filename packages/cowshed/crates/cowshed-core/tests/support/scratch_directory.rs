//! Real filesystem roots for lifecycle tests that own no disk images.
//!
//! Cleanup ends the root's workers, then removes its tree, several threads unlinking at once, and
//! the namespace root minted beside it. Only a tree it cannot remove is moved whole under that
//! namespace root, for the lane setup's dead-run sweep; the live source path stays short.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::scratch_namespace::processes;

#[path = "../../../cowshed-apfs/tests/support/scratch_directory_cleanup.rs"]
pub(crate) mod cleanup;
use cleanup::{ROOT_PREFIX, reclaim};

/// The file in each root that names the test which made it. The name stays out of the path: the
/// store's Unix sockets (`run/<32 hex>.sock`, the CPU pools' `run/cpu/<22>/<22>`) must fit
/// `sun_path` (104 bytes on macOS), so a root's length is fixed by its pid and counter alone.
pub(crate) const LABEL_FILE: &str = "label";

/// Where in its namespace root a fixture's tree is retired when its Drop cannot remove it.
const RETIRED: &str = "directory";

pub struct ScratchRoot {
    path: PathBuf,
    /// The test namespace root minted with the fixture: the place its tree is retired to when Drop
    /// cannot remove it, holding only its owner record until then.
    namespace_root: PathBuf,
}

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let (_, namespace_root) = super::scratch_namespace::create_owned_root(label)?;
        let path = PathBuf::from(format!(
            "/private/tmp/{ROOT_PREFIX}{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => {}
            // No other live process has this pid and the counter never repeats, so the root is a
            // dead process's under a reused pid, which the sweep keeps because the pid is live.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                reclaim(&path)?;
                fs::create_dir(&path)?;
            }
            Err(error) => return Err(error),
        }
        let root = Self {
            path,
            namespace_root,
        };
        fs::write(root.path.join(LABEL_FILE), label)?;
        Ok(root)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
thread_local! {
    static RETIREMENT_RENAMES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn retirement_rename_attempts() -> usize {
    RETIREMENT_RENAMES.with(std::cell::Cell::get)
}

/// Missing source is already retired; a missing destination must not hide a surviving source.
fn retirement_result(
    renamed: std::io::Result<()>,
    source_exists: impl FnOnce() -> std::io::Result<bool>,
) -> std::io::Result<()> {
    match renamed {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match source_exists() {
            Ok(false) => Ok(()),
            Ok(true) => Err(error),
            Err(probe) => Err(std::io::Error::new(
                error.kind(),
                format!("rename fixture: {error}; inspect its source: {probe}"),
            )),
        },
        result => result,
    }
}

pub(crate) fn retire_directory_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    RETIREMENT_RENAMES.with(|count| count.set(count.get() + 1));
    retirement_result(
        fs::rename(source, destination),
        || match fs::symlink_metadata(source) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        },
    )
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        let started = std::time::Instant::now();
        cowshed_os::eprintln!("directory fixture {}: teardown start", self.path.display());
        let Err(error) = reclaim(&self.path) else {
            // Its namespace root holds nothing but its owner record: the fallback was not needed.
            match super::scratch_namespace::tree::remove(&self.namespace_root) {
                Ok(()) => cowshed_os::eprintln!(
                    "directory fixture {}: removed elapsed={:?}",
                    self.path.display(),
                    started.elapsed()
                ),
                Err(error) => cowshed_os::eprintln!(
                    "directory fixture {}: removed, but not its namespace root {}: {error}; the \
                     dead-run sweep takes that once this run has ended elapsed={:?}",
                    self.path.display(),
                    self.namespace_root.display(),
                    started.elapsed()
                ),
            }
            return;
        };
        let retired = self.namespace_root.join(RETIRED);
        match retire_directory_tree(&self.path, &retired) {
            Ok(()) => cowshed_os::eprintln!(
                "directory fixture {}: {error}; retired what is left under {} for the dead-run \
                 sweep elapsed={:?}",
                self.path.display(),
                retired.display(),
                started.elapsed()
            ),
            Err(retire) => cowshed_os::eprintln!(
                "directory fixture {}: {error}; retiring it under {} failed too: {retire}; \
                 remaining state stays for the existing sweeps elapsed={:?}",
                self.path.display(),
                retired.display(),
                started.elapsed()
            ),
        }
    }
}

#[cfg(test)]
mod retirement_result_tests {
    use super::retirement_result;
    use std::cell::Cell;
    use std::io::{Error, ErrorKind};

    #[test]
    fn rename_completion_preserves_source_absence_and_real_failures() {
        for (rename_error, source, expected, reads) in [
            (None, Err(ErrorKind::Other), None, 0),
            (
                Some(ErrorKind::PermissionDenied),
                Err(ErrorKind::Other),
                Some(ErrorKind::PermissionDenied),
                0,
            ),
            (Some(ErrorKind::NotFound), Ok(false), None, 1),
            (
                Some(ErrorKind::NotFound),
                Ok(true),
                Some(ErrorKind::NotFound),
                1,
            ),
            (
                Some(ErrorKind::NotFound),
                Err(ErrorKind::PermissionDenied),
                Some(ErrorKind::NotFound),
                1,
            ),
        ] {
            let probes = Cell::new(0);
            let result = retirement_result(
                rename_error.map_or(Ok(()), |kind| Err(Error::from(kind))),
                || {
                    probes.set(probes.get() + 1);
                    source.map_err(Error::from)
                },
            );
            assert_eq!(result.as_ref().err().map(Error::kind), expected);
            assert_eq!(probes.get(), reads);
            if source.is_err() && reads == 1 {
                let message = result.unwrap_err().to_string();
                assert!(
                    message.contains("rename fixture") && message.contains("inspect its source")
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::BufRead;
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
        let namespace_root = root.namespace_root.clone();

        let renames = retirement_rename_attempts();
        drop(root);
        assert!(!path.exists());
        assert_eq!(
            retirement_rename_attempts() - renames,
            0,
            "the fixture's Drop removes its tree itself and leaves nothing to the sweep"
        );
        assert!(
            !namespace_root.exists(),
            "the namespace root held only the fallback's place and goes with the tree"
        );
        assert_eq!(
            fs::read_to_string(outside.path().join("kept")).unwrap(),
            "outside"
        );
    }

    /// `chflags(2)` `path` to `flags`; `UF_IMMUTABLE` refuses its unlink with `EPERM`.
    fn set_flags(path: &Path, flags: libc::c_uint) {
        use std::os::unix::ffi::OsStrExt as _;
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `name` is NUL-terminated and outlives the call, which reads nothing else.
        let changed = unsafe { libc::chflags(name.as_ptr(), flags) };
        assert_eq!(
            changed,
            0,
            "chflags {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }

    #[test]
    fn a_tree_its_drop_cannot_remove_is_retired_whole_under_its_namespace() {
        let root = ScratchRoot::new("immutable").unwrap();
        let path = root.path().to_path_buf();
        set_flags(
            &{
                let pinned = path.join("pinned");
                fs::write(&pinned, "pinned").unwrap();
                pinned
            },
            libc::UF_IMMUTABLE,
        );
        let namespace_root = root.namespace_root.clone();
        let retired = namespace_root.join(RETIRED);

        let renames = retirement_rename_attempts();
        drop(root);
        // Lifted before any assertion, so a failed one leaves nothing the sweep cannot remove.
        for pinned in [retired.join("pinned"), path.join("pinned")] {
            if pinned.exists() {
                set_flags(&pinned, 0);
            }
        }
        assert!(!path.exists());
        assert_eq!(retirement_rename_attempts() - renames, 1);
        assert_eq!(
            fs::read_to_string(retired.join("pinned")).unwrap(),
            "pinned"
        );
        let namespace = cowshed_os::namespace::TestNamespace::parse(
            namespace_root.file_name().unwrap().to_str().unwrap(),
        )
        .unwrap();
        assert_eq!(namespace.owner_pid(), std::process::id());
        assert!(
            fs::symlink_metadata(namespace_root.join(".owner"))
                .unwrap()
                .is_file()
        );
        // No sweep takes a live owner's retired tree, and it is no root a keeper reclaims.
        assert_eq!(
            super::super::scratch_namespace::dead_owner(&namespace_root),
            None
        );
        assert!(super::super::scratch_namespace::reclaim_kept_root(&retired).is_err());
        fs::remove_dir_all(namespace_root).unwrap();
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
                .args(["-c", "printf 'ready\\n'; exec /bin/cat"])
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
        assert!(
            processes::processes_in(|cwd| cwd.starts_with(&path))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_short_directory_is_the_sweeps_only_once_the_process_its_name_records_is_gone() {
        let mut run = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .spawn_locked()
            .unwrap();
        let named = std::ffi::OsString::from(format!("{ROOT_PREFIX}{}-templates", run.id()));
        assert_eq!(
            cleanup::dead_recorded_pid(&named),
            None,
            "a live run keeps its directory"
        );
        // Its input's end ends `cat`.
        drop(run.stdin.take());
        assert!(run.wait_bounded().unwrap().success());
        assert_eq!(
            cleanup::dead_recorded_pid(&named),
            libc::pid_t::try_from(run.id()).ok(),
            "an ended run's directory is the sweep's"
        );
        for unclaimed in [
            "cowshed-dtest-0-templates",
            "cowshed-dtest-run-templates",
            "cowshed-other-1-templates",
        ] {
            assert_eq!(
                cleanup::dead_recorded_pid(std::ffi::OsStr::new(unclaimed)),
                None
            );
        }
    }
}
