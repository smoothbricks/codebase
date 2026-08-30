//! Shared filesystem-publication plumbing: the temp-artifact grammar, the directory durability
//! barrier, and the one atomic private-file writer.
//!
//! Everything here is plain POSIX — nothing Darwin-specific — so a Linux port reuses this module
//! unchanged. The one writer deliberately NOT covered is the root-context marker writer in the
//! macOS bootstrap adapter: it anchors on a directory file descriptor because an unprivileged
//! user controls names beside its destination, and that hardening belongs to the platform
//! adapter that carries the privilege.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

/// The one temp-artifact grammar: `.{final_name}.tmp.{discriminator}`.
///
/// Dot-prefixed so crash residue hides from listings, with a `.tmp.` infix so a sweeper needs
/// exactly one recognizer ([`is_temp_artifact`]) instead of per-writer spellings. Applied only in
/// cowshed-owned directories (project roots, `sessions/`, config and trace directories) — never
/// to workspace content, which is what keeps the loose recognizer safe.
pub(crate) fn temp_name(final_name: &OsStr, discriminator: impl fmt::Display) -> OsString {
    let mut name = OsString::from(".");
    name.push(final_name);
    name.push(format!(".tmp.{discriminator}"));
    name
}

/// True for any file name produced by [`temp_name`]. The exact inverse of that grammar, so
/// residue sweeping stays possible without knowing which writer crashed. No production sweeper
/// exists yet — residue is currently reclaimed only by whole-tree removal — so until one lands
/// this predicate is pinned by tests alone.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn is_temp_artifact(file_name: &OsStr) -> bool {
    let Some(name) = file_name.to_str() else {
        return false;
    };
    name.starts_with('.') && name.contains(".tmp.")
}

/// Open a directory and fsync it — the one durability barrier for directory entries. Opened with
/// `O_DIRECTORY` so a path swapped for a file between derivation and sync fails instead of
/// silently syncing the wrong object, and `O_CLOEXEC` so the descriptor never leaks into an
/// exec'd child.
pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC);
    }
    options.open(path)?.sync_all()
}

/// Atomically rename one directory-relative entry without replacing an existing destination.
#[cfg(target_os = "macos")]
pub(crate) fn rename_noreplace(
    directory: std::os::fd::RawFd,
    source: &std::ffi::CStr,
    destination: &std::ffi::CStr,
) -> io::Result<()> {
    // SAFETY: both names are NUL-terminated and remain live for the call; `directory` is an open
    // directory descriptor owned by the caller. RENAME_EXCL makes the destination check atomic.
    let result = unsafe {
        libc::renameatx_np(
            directory,
            source.as_ptr(),
            directory,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Atomically rename one directory-relative entry without replacing an existing destination.
#[cfg(target_os = "linux")]
pub(crate) fn rename_noreplace(
    directory: std::os::fd::RawFd,
    source: &std::ffi::CStr,
    destination: &std::ffi::CStr,
) -> io::Result<()> {
    // SAFETY: both names are NUL-terminated and remain live for the syscall; `directory` is an
    // open directory descriptor owned by the caller. libc supplies the kernel ABI flag value.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            source.as_ptr(),
            directory,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(crate) fn rename_noreplace(
    _directory: std::os::fd::RawFd,
    _source: &std::ffi::CStr,
    _destination: &std::ffi::CStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic create-new rename is unsupported",
    ))
}

/// How [`publish_private_file`] failed: an I/O step (with the path it was about), or the caller's
/// own write closure. Typed so callers keep their structured errors instead of flattening
/// serialization failures into `io::Error`.
pub(crate) enum PublishError<E> {
    Io { path: PathBuf, source: io::Error },
    Write(E),
}

/// Atomically publish a private regular file at `path`: write into a uniquely named temp sibling,
/// fsync it, rename it over `path`, fsync the parent. A failure at any step removes the temp.
///
/// The temp is created with `create_new` — `O_EXCL` refuses a symlink final component, dangling
/// included, so no separate `O_NOFOLLOW` is needed — plus `O_CLOEXEC`. Permissions are `0600`
/// twice on purpose: the open mode is masked by the umask, so the explicit `set_permissions`
/// afterwards is what guarantees the private mode.
pub(crate) fn publish_private_file<E>(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<(), E>,
) -> Result<(), PublishError<E>> {
    let io_at = |at: &Path, source: io::Error| PublishError::Io {
        path: at.to_owned(),
        source,
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path.file_name().ok_or_else(|| PublishError::Io {
        path: path.to_owned(),
        source: io::Error::new(io::ErrorKind::InvalidInput, "publish path has no file name"),
    })?;
    let temp_path = parent.join(temp_name(file_name, uuid::Uuid::new_v4().simple()));

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_CLOEXEC);
    }
    let file = options
        .open(&temp_path)
        .map_err(|source| io_at(&temp_path, source))?;
    let mut cleanup = TempCleanup {
        path: temp_path.clone(),
        armed: true,
    };
    {
        let mut writer = BufWriter::new(file);
        write(&mut writer).map_err(PublishError::Write)?;
        writer.flush().map_err(|source| io_at(&temp_path, source))?;
        #[cfg(unix)]
        writer
            .get_ref()
            .set_permissions({
                use std::os::unix::fs::PermissionsExt;
                fs::Permissions::from_mode(0o600)
            })
            .map_err(|source| io_at(&temp_path, source))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|source| io_at(&temp_path, source))?;
    }

    fs::rename(&temp_path, path).map_err(|source| io_at(path, source))?;
    cleanup.armed = false;
    sync_directory(parent).map_err(|source| io_at(parent, source))?;
    Ok(())
}

/// Removes the temp file unless the rename disarmed it; publication either completes or leaves
/// nothing behind.
struct TempCleanup {
    path: PathBuf,
    armed: bool,
}

impl Drop for TempCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_names_hide_carry_the_final_name_and_are_recognized() {
        let name = temp_name(OsStr::new("metadata.json"), "abc123");
        let rendered = name.to_str().unwrap();
        assert!(rendered.starts_with(".metadata.json.tmp."));
        assert!(is_temp_artifact(&name));
        assert!(!is_temp_artifact(OsStr::new("metadata.json")));
        assert!(!is_temp_artifact(OsStr::new("metadata.json.tmp.7")));
        assert!(!is_temp_artifact(OsStr::new(".gitignore")));
    }

    #[test]
    fn publish_is_atomic_and_failure_leaves_no_residue() {
        let directory = std::env::temp_dir().join(format!("fsio-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("value.json");

        publish_private_file::<io::Error>(&path, |writer| writer.write_all(b"ok"))
            .map_err(|_| "publish failed")
            .unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"ok");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let failure = publish_private_file(&path, |_| Err("write refused"));
        assert!(matches!(failure, Err(PublishError::Write("write refused"))));
        assert_eq!(
            fs::read(&path).unwrap(),
            b"ok",
            "failed publish never touches the destination"
        );
        let residue: Vec<_> = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| is_temp_artifact(name))
            .collect();
        assert!(
            residue.is_empty(),
            "failed publish removes its temp: {residue:?}"
        );
        fs::remove_dir_all(&directory).unwrap();
    }
}
