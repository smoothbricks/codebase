//! Lifecycle locks anchored at a store root: `flock` on lock files opened component by
//! component from the root with `O_NOFOLLOW`, so no symlink planted under the store can redirect
//! a lock, and created race-free when several processes ask for a new one at once.

use std::ffi::CString;
use std::fmt;
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Component, Path, PathBuf};

/// Whether taking a lock waits for its holder or answers busy at once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockMode {
    Wait,
    Try,
}

/// Why a store lock could not be taken. A busy lock under [`LockMode::Try`] is not an error.
#[derive(Debug)]
pub enum StoreLockError {
    /// The lock path is not a plain descendant of the store root.
    EscapesStoreRoot,
    /// The lock path names no lock: the store root itself, or no final component.
    InvalidPath(&'static str),
    /// A path component carries a NUL byte, which no system call can name.
    Nul(PathBuf),
    /// A component below the store root is a symbolic link.
    SymlinkComponent(PathBuf),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for StoreLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EscapesStoreRoot => formatter.write_str("lock path escapes the store root"),
            Self::InvalidPath(reason) => formatter.write_str(reason),
            Self::Nul(path) => write!(
                formatter,
                "controller path contains NUL: {}",
                path.display()
            ),
            Self::SymlinkComponent(path) => write!(
                formatter,
                "storage path contains symbolic link component {}",
                path.display()
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl std::error::Error for StoreLockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> StoreLockError {
    StoreLockError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

const ROOT_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY + libc::O_DIRECTORY + libc::O_NOFOLLOW + libc::O_CLOEXEC;
const DIRECTORY_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY + libc::O_DIRECTORY + libc::O_NOFOLLOW + libc::O_CLOEXEC;
/// An existing lock file: opened, never created.
const LOCK_FILE_OPEN_FLAGS: libc::c_int = libc::O_RDWR + libc::O_NOFOLLOW + libc::O_CLOEXEC;
/// A lock file no one has created yet: created, or `EEXIST` when someone just did.
const LOCK_FILE_CREATE_FLAGS: libc::c_int = LOCK_FILE_OPEN_FLAGS + libc::O_CREAT + libc::O_EXCL;
const WAIT_LOCK_OPERATION: libc::c_int = libc::LOCK_EX;
const TRY_LOCK_OPERATION: libc::c_int = libc::LOCK_EX + libc::LOCK_NB;
const LOCK_FILE_MODE: libc::c_uint = 0o600;
const CONTROLLER_DIRECTORY_MODE: libc::mode_t = 0o700;

fn fd_failed(fd: libc::c_int) -> bool {
    fd == -1
}

fn flock_succeeded(result: libc::c_int) -> bool {
    result == 0
}

fn lock_is_busy(mode: LockMode, kind: io::ErrorKind) -> bool {
    matches!((mode, kind), (LockMode::Try, io::ErrorKind::WouldBlock))
}

fn should_create_directory(fd: libc::c_int, kind: io::ErrorKind) -> bool {
    matches!((fd_failed(fd), kind), (true, io::ErrorKind::NotFound))
}

fn mkdir_failed(result: libc::c_int, kind: io::ErrorKind) -> bool {
    !matches!(
        (fd_failed(result), kind),
        (false, _) | (true, io::ErrorKind::AlreadyExists)
    )
}

fn should_retry_lock(kind: io::ErrorKind) -> bool {
    matches!(kind, io::ErrorKind::Interrupted)
}

/// The image lifecycle locks one operation holds. Fenced: a peer's try-lock succeeds as soon as
/// the guard is dropped, whatever this process is spawning (`fork_lock`).
pub struct ImageLockGuard {
    _files: Vec<cowshed_os::fork_lock::Fenced<File>>,
}

fn path_component(value: &std::ffi::OsStr, path: &Path) -> Result<CString, StoreLockError> {
    CString::new(value.as_bytes()).map_err(|_| StoreLockError::Nul(path.to_owned()))
}

/// The lock file `name` in `directory`, created if it does not exist yet.
///
/// Never one `O_CREAT` open: when several processes `openat` the same new name with `O_CREAT` at
/// once, macOS answers some of them `ENOENT` (measured on APFS: thousands of failures in 4,800
/// racing `openat`s, none with path-based `open`). An open of the existing file, then an
/// exclusive create, then an open of the file the `EEXIST` names, measured none in 12,000.
fn open_or_create_lock_at(
    directory: &File,
    name: &CString,
    path: &Path,
) -> Result<File, StoreLockError> {
    let open = |flags: libc::c_int| {
        // SAFETY: `directory` owns a live directory fd; `name` is a `CString` that outlives the
        // call. Both flag sets include `O_NOFOLLOW | O_CLOEXEC`, so the lock file cannot be a
        // symlink and the fd cannot leak; the mode applies only to `O_CREAT`.
        let fd =
            unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags, LOCK_FILE_MODE) };
        if fd_failed(fd) {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: `fd` is a live descriptor we exclusively own from a successful `openat`;
            // `File` takes ownership and will close it.
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    };
    open(LOCK_FILE_OPEN_FLAGS)
        .or_else(|error| match error.kind() {
            io::ErrorKind::NotFound => open(LOCK_FILE_CREATE_FLAGS),
            _ => Err(error),
        })
        .or_else(|error| match error.kind() {
            io::ErrorKind::AlreadyExists => open(LOCK_FILE_OPEN_FLAGS),
            _ => Err(error),
        })
        .map_err(|error| {
            io_error(
                "open lifecycle lock without following symlinks",
                path,
                error,
            )
        })
}

fn open_lock_file(root: &Path, path: &Path) -> Result<File, StoreLockError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| StoreLockError::EscapesStoreRoot)?;
    let mut components = relative.components().peekable();
    if components.peek().is_none() {
        return Err(StoreLockError::InvalidPath("lock path is the store root"));
    }
    let root_name = CString::new(root.as_os_str().as_bytes())
        .map_err(|_| StoreLockError::Nul(root.to_owned()))?;
    // SAFETY: `root_name` is a `CString`, so the pointer is NUL-terminated and
    // lives for the call. `ROOT_OPEN_FLAGS` includes `O_CLOEXEC | O_NOFOLLOW |
    // O_DIRECTORY`, so this never follows a symlink at the store root.
    let root_fd = unsafe { libc::open(root_name.as_ptr(), ROOT_OPEN_FLAGS) };
    if fd_failed(root_fd) {
        return Err(io_error(
            "open controller store without following symlinks",
            root,
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `root_fd` is a live descriptor we exclusively own from a successful
    // `open`; `File` takes ownership and will close it.
    let mut directory = unsafe { File::from_raw_fd(root_fd) };
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(StoreLockError::EscapesStoreRoot);
        };
        let name = path_component(name, path)?;
        if components.peek().is_none() {
            return open_or_create_lock_at(&directory, &name, path);
        }
        // SAFETY: `directory` owns a live directory fd; `name` is a `CString` that
        // outlives the call. `DIRECTORY_OPEN_FLAGS` includes `O_NOFOLLOW | O_DIRECTORY`.
        let mut fd =
            unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), DIRECTORY_OPEN_FLAGS) };
        if should_create_directory(fd, io::Error::last_os_error().kind()) {
            // SAFETY: `directory` owns a live directory fd; `name` is a `CString`
            // that outlives the call. Mode `0o700` is the controller-directory policy.
            let created = unsafe {
                libc::mkdirat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    CONTROLLER_DIRECTORY_MODE,
                )
            };
            if mkdir_failed(created, io::Error::last_os_error().kind()) {
                return Err(io_error(
                    "create controller directory without following symlinks",
                    path,
                    io::Error::last_os_error(),
                ));
            }
            // SAFETY: same as the `openat` above; retry after `mkdirat` raced with another
            // creator. `name` still lives.
            fd =
                unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), DIRECTORY_OPEN_FLAGS) };
        }
        if fd_failed(fd) {
            return Err(io_error(
                "open controller directory without following symlinks",
                path,
                io::Error::last_os_error(),
            ));
        }
        // SAFETY: `fd` is a live directory descriptor we exclusively own from a
        // successful `openat`; `File` takes ownership and will close it. Dropping
        // `directory` closes the previous component fd.
        directory = unsafe { File::from_raw_fd(fd) };
    }
    Err(StoreLockError::InvalidPath(
        "lock path has no final component",
    ))
}

pub fn acquire_image_locks(
    root: &Path,
    paths: &[PathBuf],
    mode: LockMode,
) -> Result<Option<ImageLockGuard>, StoreLockError> {
    let mut paths = paths.to_vec();
    paths.sort();
    paths.dedup();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let file = cowshed_os::fork_lock::Fenced::new(open_lock_file(root, &path)?);
        let operation = match mode {
            LockMode::Wait => WAIT_LOCK_OPERATION,
            LockMode::Try => TRY_LOCK_OPERATION,
        };
        loop {
            // SAFETY: `file` owns the fd for the duration of the call; `flock` does
            // not consume it. `operation` is `LOCK_EX` or `LOCK_EX|LOCK_NB`.
            let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
            if flock_succeeded(result) {
                files.push(file);
                break;
            }
            let error = io::Error::last_os_error();
            if should_retry_lock(error.kind()) {
                continue;
            }
            if lock_is_busy(mode, error.kind()) {
                return Ok(None);
            }
            return Err(io_error("acquire lifecycle lock", &path, error));
        }
    }
    Ok(Some(ImageLockGuard { _files: files }))
}

/// Refuse `candidate` unless it is a plain descendant of the lexically canonical `root` and no
/// existing component from the root down is a symbolic link. A missing component ends the walk:
/// nothing below it exists to be a link.
pub fn verify_no_symlinks(root: &Path, candidate: &Path) -> Result<(), StoreLockError> {
    // A non-canonical root voids the lexical containment below (`..` in the root lets a candidate
    // strip-prefix cleanly while resolving elsewhere), so it is refused the same way.
    let canonical = root.is_absolute()
        && !root
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir));
    if !canonical {
        return Err(StoreLockError::EscapesStoreRoot);
    }
    let relative = candidate
        .strip_prefix(root)
        .map_err(|_| StoreLockError::EscapesStoreRoot)?;
    if relative.components().next().is_none()
        || !relative
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(StoreLockError::EscapesStoreRoot);
    }
    let inspect = "inspect storage path";
    let root_metadata =
        std::fs::symlink_metadata(root).map_err(|source| io_error(inspect, root, source))?;
    if root_metadata.file_type().is_symlink() {
        return Err(StoreLockError::SymlinkComponent(root.to_owned()));
    }
    let mut current = root.to_owned();
    for component in relative.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StoreLockError::SymlinkComponent(current));
            }
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => return Err(io_error(inspect, &current, source)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_flag_and_result_tables_are_exact() {
        assert_eq!(
            ROOT_OPEN_FLAGS,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
        );
        assert_eq!(DIRECTORY_OPEN_FLAGS, ROOT_OPEN_FLAGS);
        assert_eq!(
            LOCK_FILE_OPEN_FLAGS,
            libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC
        );
        assert_eq!(
            LOCK_FILE_CREATE_FLAGS,
            libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_CREAT | libc::O_EXCL
        );
        assert_eq!(WAIT_LOCK_OPERATION, libc::LOCK_EX);
        assert_eq!(TRY_LOCK_OPERATION, libc::LOCK_EX | libc::LOCK_NB);
        assert_eq!(LOCK_FILE_MODE, 0o600);
        assert_eq!(CONTROLLER_DIRECTORY_MODE, 0o700);

        for (fd, failed) in [(-1, true), (0, false), (1, false)] {
            assert_eq!(fd_failed(fd), failed, "fd={fd}");
            assert_eq!(flock_succeeded(fd), fd == 0, "result={fd}");
        }
        for (mode, kind, busy) in [
            (LockMode::Try, io::ErrorKind::WouldBlock, true),
            (LockMode::Wait, io::ErrorKind::WouldBlock, false),
            (LockMode::Try, io::ErrorKind::Other, false),
            (LockMode::Wait, io::ErrorKind::Other, false),
        ] {
            assert_eq!(lock_is_busy(mode, kind), busy, "{mode:?}/{kind:?}");
        }
        for (fd, kind, create) in [
            (-1, io::ErrorKind::NotFound, true),
            (-1, io::ErrorKind::PermissionDenied, false),
            (0, io::ErrorKind::NotFound, false),
            (0, io::ErrorKind::Other, false),
        ] {
            assert_eq!(should_create_directory(fd, kind), create);
        }
        for (result, kind, failed) in [
            (-1, io::ErrorKind::PermissionDenied, true),
            (-1, io::ErrorKind::AlreadyExists, false),
            (0, io::ErrorKind::PermissionDenied, false),
            (0, io::ErrorKind::Other, false),
        ] {
            assert_eq!(mkdir_failed(result, kind), failed);
        }
        for (kind, retry) in [
            (io::ErrorKind::Interrupted, true),
            (io::ErrorKind::WouldBlock, false),
            (io::ErrorKind::Other, false),
        ] {
            assert_eq!(should_retry_lock(kind), retry);
        }
    }
}
