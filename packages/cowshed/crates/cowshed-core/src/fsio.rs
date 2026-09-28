//! Shared filesystem-publication and anchored directory-preparation primitives.
//! The temp-artifact grammar, durability barrier and atomic private-file writer
//! coexist with a directory capability for host writes below child-mutable names.
//! These are POSIX operations shared by Darwin and Linux. The privileged bootstrap
//! marker adapter retains its own root-context publication policy.

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// A held directory capability. Child preparation never re-resolves a mutable
/// parent path after it has been opened.
pub(crate) struct AnchoredDirectory(File);

impl AnchoredDirectory {
    /// Open/create a canonical absolute chain without following any symlink.
    /// One path buffer supplies all NUL-terminated components to openat.
    pub(crate) fn create(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory must be absolute",
            ));
        }
        let mut components = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "directory contains NUL"))?
            .into_bytes_with_nul();
        for byte in &mut components {
            if *byte == b'/' {
                *byte = 0;
            }
        }
        let mut directory = Self(
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open("/")?,
        );
        for component in components.split_inclusive(|byte| *byte == 0) {
            if component.len() == 1 {
                continue;
            }
            let component = CStr::from_bytes_with_nul(component)
                .expect("split components have exactly one trailing NUL");
            directory = directory.child(component)?;
        }
        Ok(directory)
    }

    pub(crate) fn child(&self, name: &CStr) -> io::Result<Self> {
        validate_directory_leaf(name)?;
        let open = || {
            // SAFETY: the held parent fd and NUL-terminated name outlive the call.
            // NOFOLLOW and DIRECTORY refuse links and non-directory substitutions.
            unsafe {
                libc::openat(
                    self.0.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            }
        };
        let mut fd = open();
        if fd < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
            // SAFETY: mkdirat resolves one leaf beneath the held parent, never a path.
            if unsafe { libc::mkdirat(self.0.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::AlreadyExists {
                    return Err(error);
                }
            }
            fd = open();
        }
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful openat returned a new owned directory descriptor.
        Ok(Self(unsafe { File::from_raw_fd(fd) }))
    }

    /// Keep real private cache entries, preserve matching links, and replace
    /// stale links without following any mutable parent or destination.
    pub(crate) fn ensure_symlink(&self, name: &CStr, target: &Path) -> io::Result<()> {
        validate_directory_leaf(name)?;
        let target = CString::new(target.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "link target contains NUL"))?;
        let mut buffer = [0u8; 4096];
        // SAFETY: the fd/name are live and the buffer is writable for its full length.
        let count = unsafe {
            libc::readlinkat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if count >= 0 {
            let count = usize::try_from(count).expect("non-negative readlink length");
            if count == buffer.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "link target exceeds buffer",
                ));
            }
            if buffer[..count] == *target.as_bytes() {
                return Ok(());
            }
            // SAFETY: unlinkat removes this directory entry, not its link target.
            if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.as_ptr(), 0) } != 0 {
                return Err(io::Error::last_os_error());
            }
        } else {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINVAL) {
                return Ok(());
            }
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
        }
        // SAFETY: the target and leaf are NUL-terminated; the held parent anchors creation.
        if unsafe { libc::symlinkat(target.as_ptr(), self.0.as_raw_fd(), name.as_ptr()) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

fn validate_directory_leaf(name: &CStr) -> io::Result<()> {
    let name = name.to_bytes();
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected one directory entry",
        ))
    } else {
        Ok(())
    }
}

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
    fn directory_capability_survives_parent_rename_without_following_its_replacement() {
        let temporary = std::env::temp_dir().join(format!("fsio-anchor-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&temporary).unwrap();
        let root = fs::canonicalize(&temporary).unwrap();
        let parent = root.join("private");
        let outside = root.join("outside");
        fs::create_dir(&outside).unwrap();
        let directory = AnchoredDirectory::create(&parent).unwrap();
        let moved = root.join("moved");
        fs::rename(&parent, &moved).unwrap();
        std::os::unix::fs::symlink(&outside, &parent).unwrap();

        directory.child(c"registry").unwrap().child(c"src").unwrap();
        directory
            .ensure_symlink(c"cache", &root.join("cache-target"))
            .unwrap();
        assert!(moved.join("registry/src").is_dir());
        assert_eq!(
            fs::read_link(moved.join("cache")).unwrap(),
            root.join("cache-target")
        );
        for name in ["registry", "cache"] {
            assert_eq!(
                fs::symlink_metadata(outside.join(name)).unwrap_err().kind(),
                io::ErrorKind::NotFound
            );
        }
        assert!(AnchoredDirectory::create(&parent).is_err());
        fs::remove_dir_all(temporary).unwrap();
    }

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
