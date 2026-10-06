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
use std::os::unix::ffi::{OsStrExt, OsStringExt};
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

    /// Keep real private cache entries, preserve matching links, and replace stale links and empty
    /// directories without following any mutable parent or destination. An empty directory holds
    /// nothing a link could lose: it is what a tool leaves after a miss it could not fill.
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
                // Not a link. Only an empty directory gives way; anything else is kept.
                // SAFETY: unlinkat removes this empty directory entry and follows nothing.
                if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) }
                    != 0
                {
                    let error = io::Error::last_os_error();
                    return match error.raw_os_error() {
                        Some(libc::ENOTEMPTY | libc::EEXIST | libc::ENOTDIR) => Ok(()),
                        _ => Err(error),
                    };
                }
            } else if error.kind() != io::ErrorKind::NotFound {
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

    /// Publish `contents` as the private regular file `name` beneath this directory.
    ///
    /// A file that already holds exactly these bytes is left in place, so republishing unchanged
    /// wiring before every spawn costs one read. Otherwise the bytes go to an exclusively created
    /// 0600 temp sibling, are synced, and are renamed over `name`. Nothing is followed: a link a
    /// child planted at `name` is replaced by the rename, never written through.
    pub(crate) fn publish_file(&self, name: &CStr, contents: &[u8]) -> io::Result<()> {
        validate_directory_leaf(name)?;
        if self.read_regular_file(name)?.as_deref() == Some(contents) {
            return Ok(());
        }
        let temp = CString::new(
            temp_name(
                OsStr::from_bytes(name.to_bytes()),
                uuid::Uuid::new_v4().simple(),
            )
            .into_vec(),
        )
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name contains NUL"))?;
        // SAFETY: the held directory fd and NUL-terminated leaf outlive the call; EXCL and
        // NOFOLLOW refuse any entry already at the temp name, a link included.
        let fd = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                temp.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful openat returned a new owned file descriptor.
        let mut file = unsafe { File::from_raw_fd(fd) };
        let published = (|| {
            file.write_all(contents)?;
            file.set_permissions(
                <fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
            )?;
            file.sync_all()?;
            // SAFETY: both names are leaves beneath the same held directory fd.
            if unsafe {
                libc::renameat(
                    self.0.as_raw_fd(),
                    temp.as_ptr(),
                    self.0.as_raw_fd(),
                    name.as_ptr(),
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            self.0.sync_all()
        })();
        if published.is_err() {
            // SAFETY: removes this directory's own temp entry, never a link target.
            unsafe { libc::unlinkat(self.0.as_raw_fd(), temp.as_ptr(), 0) };
        }
        published
    }

    /// Remove the entry `name` beneath this directory, whatever it is but a directory: a file a
    /// previous wiring published, or a link a child planted there — never the link's target. An
    /// absent entry is already the state this asks for.
    pub(crate) fn remove_file(&self, name: &CStr) -> io::Result<()> {
        validate_directory_leaf(name)?;
        // SAFETY: unlinkat removes this directory's own entry, not a link target.
        if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.as_ptr(), 0) } == 0 {
            return self.0.sync_all();
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error)
        }
    }

    /// Make this directory's controller-owned links exactly `links`: each `(name, target)` is a
    /// symlink to `target` — whatever stood at `name` before, a regular file or a link elsewhere,
    /// is replaced, never followed — and each name an earlier call published but `links` omits is
    /// removed. The owned names are recorded in the private file `manifest`, so nothing else in
    /// the directory is enumerated or touched.
    ///
    /// The manifest never under-records: it is widened to the union of the old and new names
    /// before any link changes and narrowed to the new names only after every link is in place,
    /// so a crash at any point leaves a manifest the next call can reconcile from. A manifest
    /// naming anything but a plain leaf, or an entry that is a directory, is an error; nothing
    /// is guessed.
    pub(crate) fn reconcile_links<N: AsRef<CStr>>(
        &self,
        manifest: &CStr,
        links: &[(N, &Path)],
    ) -> io::Result<()> {
        validate_directory_leaf(manifest)?;
        let mut current: Vec<&[u8]> = Vec::with_capacity(links.len());
        for (name, _) in links {
            let name = name.as_ref();
            validate_directory_leaf(name)?;
            let name = name.to_bytes();
            if name == manifest.to_bytes() || current.contains(&name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "link {} is the manifest or named twice",
                        String::from_utf8_lossy(name)
                    ),
                ));
            }
            current.push(name);
        }
        let recorded = self.read_regular_file(manifest)?.unwrap_or_default();
        let mut prior: Vec<CString> = Vec::new();
        for entry in recorded
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            let name = CString::new(entry).expect("split entries contain no NUL");
            validate_directory_leaf(&name).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "link manifest names {:?}, which is not one directory entry",
                        String::from_utf8_lossy(entry)
                    ),
                )
            })?;
            prior.push(name);
        }
        let encode = |names: &mut Vec<&[u8]>| {
            names.sort_unstable();
            names.dedup();
            names
                .iter()
                .flat_map(|name| name.iter().copied().chain([0]))
                .collect::<Vec<u8>>()
        };
        let mut union: Vec<&[u8]> = prior.iter().map(|name| name.to_bytes()).collect();
        union.extend(current.iter().copied());
        self.publish_file(manifest, &encode(&mut union))?;
        for name in &prior {
            if !current.contains(&name.to_bytes()) {
                self.unlink_entry(name)?;
            }
        }
        for (name, target) in links {
            self.exact_symlink(name.as_ref(), target)?;
        }
        self.publish_file(manifest, &encode(&mut current))?;
        self.0.sync_all()
    }

    /// `name` as a symlink to exactly `target`: a matching link is kept, anything else but a
    /// directory is unlinked (never followed) and the link created anew.
    fn exact_symlink(&self, name: &CStr, target: &Path) -> io::Result<()> {
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
            if count < buffer.len() && buffer[..count] == *target.as_bytes() {
                return Ok(());
            }
            self.unlink_entry(name)?;
        } else {
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::ENOENT) => {}
                // Not a link: a regular file is replaced; a directory refuses the unlink.
                Some(libc::EINVAL) => self.unlink_entry(name)?,
                _ => return Err(error),
            }
        }
        // SAFETY: the target and leaf are NUL-terminated; the held parent anchors creation.
        if unsafe { libc::symlinkat(target.as_ptr(), self.0.as_raw_fd(), name.as_ptr()) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Unlink the non-directory entry `name`; an absent entry is already gone.
    fn unlink_entry(&self, name: &CStr) -> io::Result<()> {
        // SAFETY: unlinkat removes this directory's own entry, not a link target, and refuses a
        // directory without AT_REMOVEDIR.
        if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.as_ptr(), 0) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error)
        }
    }

    /// The bytes of `name` when it is a regular file; `None` when it is absent or anything else
    /// (a link, a directory, a FIFO), which publication then replaces.
    fn read_regular_file(&self, name: &CStr) -> io::Result<Option<Vec<u8>>> {
        // SAFETY: the held directory fd and NUL-terminated leaf outlive the call. NONBLOCK keeps a
        // FIFO planted at `name` from stalling the open; it is rejected as not regular below.
        let fd = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::ENOENT | libc::ELOOP) => Ok(None),
                _ => Err(error),
            };
        }
        // SAFETY: a successful openat returned a new owned file descriptor.
        let mut file = unsafe { File::from_raw_fd(fd) };
        if !file.metadata()?.is_file() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        io::Read::read_to_end(&mut file, &mut bytes)?;
        Ok(Some(bytes))
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

/// Remove a directory tree a sandboxed process wrote, read-only directories included.
///
/// Tools seal what they write: Go's module cache extracts every directory 0555, and
/// content-addressed caches commonly do the same, so `remove_dir_all` gets `EACCES` unlinking a
/// child of such a directory. As `go clean -modcache` does, a removal refused that way restores the
/// owner's `rwx` bits on every directory in the tree this user owns, then removes it once more.
/// Only this user's own directories are touched: one another user owns keeps its mode, so the
/// second removal reports exactly what still refuses it. Symlinks are never followed.
pub(crate) fn remove_owned_tree(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            grant_owner_directory_access(path)?;
            fs::remove_dir_all(path)
        }
        removed => removed,
    }
}

/// Add `u+rwx` to each directory beneath (and including) `root` that this user owns, top-down so
/// each one is searchable before its children are visited. A directory another user owns is not
/// descended into. An entry gone mid-walk was removed by someone else and is skipped.
pub(crate) fn grant_owner_directory_access(root: &Path) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    // SAFETY: geteuid has no preconditions and reads no caller-owned memory.
    let owner = unsafe { libc::geteuid() };
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let metadata = match fs::symlink_metadata(&directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !metadata.is_dir() || metadata.uid() != owner {
            continue;
        }
        let mode = metadata.mode() & 0o7777;
        if mode & 0o700 != 0o700 {
            let name = CString::new(directory.as_os_str().as_bytes()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "directory contains NUL")
            })?;
            let mode = libc::mode_t::try_from(mode | 0o700)
                .expect("a permission mask below 0o7777 fits mode_t");
            // SAFETY: `name` is NUL-terminated and outlives the call. AT_SYMLINK_NOFOLLOW makes
            // a directory swapped for a symlink since the lstat change the link, never its target.
            if unsafe {
                libc::fchmodat(
                    libc::AT_FDCWD,
                    name.as_ptr(),
                    mode,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::NotFound {
                    return Err(error);
                }
                continue;
            }
        }
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
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
    full_sync(&options.open(path)?)
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

/// How much of a crash a write must survive before what it records is acknowledged.
///
/// The level is a property of the state, not of the writer: lifecycle and authority state
/// (workspace creation and removal, landing, grants, policy revisions) survives power loss, while
/// a per-job record survives the death of the process that wrote it. Power loss ends every job
/// anyway, and recovery treats a job whose record went missing or torn with it as a lost job
/// (11_shell.md "Job control").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Durability {
    /// No sync: the kernel holds the bytes, so they survive the writer's death. For state that a
    /// later open re-derives from whatever did survive.
    Process,
    /// `fsync(2)` on the file: the bytes reach the device, which on macOS may still lose them to
    /// power loss. A new directory entry is not synced: losing it is losing the record.
    Device,
    /// `F_FULLFSYNC` on macOS (Rust's `sync_all`) on the file and on the directory holding a new
    /// entry: survives power loss.
    PowerLoss,
}

impl Durability {
    pub(crate) fn sync_file(self, file: &File) -> io::Result<()> {
        match self {
            Self::Process => Ok(()),
            Self::Device => {
                #[cfg(test)]
                count_sync(|syncs| syncs.fsync += 1);
                // SAFETY: fsync on a descriptor the borrowed `File` keeps open.
                if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            }
            Self::PowerLoss => full_sync(file),
        }
    }

    /// Make a directory entry this write created as durable as the write itself.
    pub(crate) fn sync_new_entry(self, directory: &Path) -> io::Result<()> {
        match self {
            Self::Process | Self::Device => Ok(()),
            Self::PowerLoss => sync_directory(directory),
        }
    }

    /// [`Self::sync_new_entry`] through a directory descriptor the writer already holds.
    pub(crate) fn sync_new_entry_at(self, directory: &File) -> io::Result<()> {
        match self {
            Self::Process | Self::Device => Ok(()),
            Self::PowerLoss => full_sync(directory),
        }
    }
}

/// `F_FULLFSYNC` on macOS (`fsync(2)` elsewhere), the only full-device flush a [`Durability`]
/// issues.
fn full_sync(file: &File) -> io::Result<()> {
    #[cfg(test)]
    count_sync(|syncs| syncs.full += 1);
    file.sync_all()
}

/// The syncs this thread issued through [`Durability`] since the last [`take_syncs`], so a test
/// can hold a path to the flush its state's tier allows.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Syncs {
    /// `fsync(2)`: [`Durability::Device`].
    pub(crate) fsync: usize,
    /// Full-device flushes: [`Durability::PowerLoss`] files and directories.
    pub(crate) full: usize,
}

#[cfg(test)]
thread_local! {
    static SYNCS: std::cell::Cell<Syncs> = const { std::cell::Cell::new(Syncs { fsync: 0, full: 0 }) };
}

#[cfg(test)]
fn count_sync(count: impl FnOnce(&mut Syncs)) {
    SYNCS.with(|cell| {
        let mut syncs = cell.get();
        count(&mut syncs);
        cell.set(syncs);
    });
}

#[cfg(test)]
pub(crate) fn take_syncs() -> Syncs {
    SYNCS.with(std::cell::Cell::take)
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
    publish_private_file_with(path, Durability::PowerLoss, write)
}

/// [`publish_private_file`] at `durability`: the rename is atomic at every level, so a reader
/// sees the old file or the new one, never a torn one; the level decides only which crash the
/// new one survives.
pub(crate) fn publish_private_file_with<E>(
    path: &Path,
    durability: Durability,
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
        durability
            .sync_file(writer.get_ref())
            .map_err(|source| io_at(&temp_path, source))?;
    }

    fs::rename(&temp_path, path).map_err(|source| io_at(path, source))?;
    cleanup.armed = false;
    durability
        .sync_new_entry(parent)
        .map_err(|source| io_at(parent, source))?;
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

    /// An empty private directory is what a tool leaves after a cache miss it could not fill; it
    /// gives way to the shared cache's link. A directory holding anything, or a file, is kept.
    #[test]
    fn a_cache_link_replaces_only_an_empty_private_directory() {
        let temporary = std::env::temp_dir().join(format!("fsio-link-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&temporary).unwrap();
        // AnchoredDirectory follows no link, and the temp dir may be reached through one.
        let temporary = fs::canonicalize(&temporary).unwrap();
        let directory = AnchoredDirectory::create(&temporary).unwrap();
        let target = temporary.join("target");
        fs::create_dir(temporary.join("empty")).unwrap();
        fs::create_dir(temporary.join("filled")).unwrap();
        fs::write(temporary.join("filled/entry"), b"kept").unwrap();
        fs::write(temporary.join("file"), b"kept").unwrap();

        directory.ensure_symlink(c"empty", &target).unwrap();
        directory.ensure_symlink(c"filled", &target).unwrap();
        directory.ensure_symlink(c"file", &target).unwrap();

        assert_eq!(fs::read_link(temporary.join("empty")).unwrap(), target);
        assert_eq!(fs::read(temporary.join("filled/entry")).unwrap(), b"kept");
        assert_eq!(fs::read(temporary.join("file")).unwrap(), b"kept");
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

    /// A sandboxed tool's sealed output — a 0555 directory holding 0444 files under a 0555
    /// parent, as Go's module cache and content-addressed answer caches leave them — is removed
    /// with the tree, and a symlink inside it is unlinked without its target's mode changing.
    #[test]
    fn owned_tree_removal_unseals_read_only_directories_without_following_links() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("fsio-sealed-{}", uuid::Uuid::new_v4()));
        let tree = root.join("tmp/raven");
        let sealed = tree.join(".tmpXYZ/ca96ba1b");
        fs::create_dir_all(sealed.join("inner")).unwrap();
        fs::write(sealed.join("answer.bin"), b"answer").unwrap();
        let outside = root.join("outside");
        fs::create_dir(&outside).unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o500)).unwrap();
        std::os::unix::fs::symlink(&outside, sealed.join("link")).unwrap();
        for (path, mode) in [
            (sealed.join("answer.bin"), 0o444),
            (sealed.join("inner"), 0o555),
            (sealed.clone(), 0o555),
            (tree.join(".tmpXYZ"), 0o555),
        ] {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        }
        assert_eq!(
            fs::remove_dir_all(&tree).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied,
            "the fixture reproduces the refusal"
        );

        remove_owned_tree(&tree).unwrap();

        assert_eq!(
            fs::symlink_metadata(&tree).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            fs::symlink_metadata(&outside).unwrap().permissions().mode() & 0o777,
            0o500,
            "a link's target keeps its mode"
        );
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
        fs::remove_dir_all(root).unwrap();
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

    fn reconcile_fixture(label: &str) -> (PathBuf, PathBuf, AnchoredDirectory) {
        let temporary = std::env::temp_dir().join(format!("fsio-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&temporary).unwrap();
        let root = fs::canonicalize(&temporary).unwrap();
        let bin = root.join("bin");
        let directory = AnchoredDirectory::create(&bin).unwrap();
        (root, bin, directory)
    }

    /// The links are exactly the current set: a name an earlier call published and this one
    /// omits is removed, and nothing the manifest never named is touched.
    #[test]
    fn reconciled_links_are_exactly_the_current_set() {
        let (root, bin, directory) = reconcile_fixture("reconcile");
        let (npm, node) = (root.join("npm-cli.js"), root.join("node"));
        fs::write(bin.join("unowned"), b"someone else's").unwrap();
        directory
            .reconcile_links(c".links", &[(c"npm", &npm), (c"node", &node)])
            .unwrap();
        assert_eq!(fs::read_link(bin.join("npm")).unwrap(), npm);
        assert_eq!(fs::read_link(bin.join("node")).unwrap(), node);
        assert_eq!(fs::read(bin.join(".links")).unwrap(), b"node\0npm\0");

        directory
            .reconcile_links(c".links", &[(c"node", &node)])
            .unwrap();
        assert!(
            fs::symlink_metadata(bin.join("npm")).is_err(),
            "a stale link stays on PATH"
        );
        assert_eq!(fs::read_link(bin.join("node")).unwrap(), node);
        assert_eq!(fs::read(bin.join(".links")).unwrap(), b"node\0");
        assert_eq!(fs::read(bin.join("unowned")).unwrap(), b"someone else's");

        directory.reconcile_links::<&CStr>(c".links", &[]).unwrap();
        assert!(fs::symlink_metadata(bin.join("node")).is_err());
        assert_eq!(fs::read(bin.join(".links")).unwrap(), b"");
        fs::remove_dir_all(root).unwrap();
    }

    /// Whatever a child planted at an owned name — a regular file, a link outside — is replaced
    /// by the exact link and never followed; a directory there is refused, not removed.
    #[test]
    fn planted_entries_are_replaced_without_redirecting_outside() {
        let (root, bin, directory) = reconcile_fixture("planted");
        let target = root.join("program");
        let outside = root.join("outside");
        fs::write(&outside, b"untouched").unwrap();
        fs::write(bin.join("npm"), b"planted file").unwrap();
        std::os::unix::fs::symlink(&outside, bin.join("node")).unwrap();
        directory
            .reconcile_links(c".links", &[(c"npm", &target), (c"node", &target)])
            .unwrap();
        for name in ["npm", "node"] {
            assert_eq!(fs::read_link(bin.join(name)).unwrap(), target, "{name}");
        }
        assert_eq!(fs::read(&outside).unwrap(), b"untouched");

        // A stale owned name that became a link outside: the link goes, its target stays.
        fs::remove_file(bin.join("npm")).unwrap();
        std::os::unix::fs::symlink(&outside, bin.join("npm")).unwrap();
        directory
            .reconcile_links(c".links", &[(c"node", &target)])
            .unwrap();
        assert!(fs::symlink_metadata(bin.join("npm")).is_err());
        assert_eq!(fs::read(&outside).unwrap(), b"untouched");

        fs::create_dir(bin.join("bun")).unwrap();
        assert!(
            directory
                .reconcile_links(c".links", &[(c"bun", &target)])
                .is_err()
        );
        assert!(bin.join("bun").is_dir());
        fs::remove_dir_all(root).unwrap();
    }

    /// A manifest naming anything but one directory entry is refused before any link changes,
    /// as are duplicate names and a link named like the manifest.
    #[test]
    fn an_invalid_manifest_or_link_set_is_refused_before_any_change() {
        let (root, bin, directory) = reconcile_fixture("invalid");
        let target = root.join("program");
        let outside = root.join("victim");
        fs::write(&outside, b"untouched").unwrap();
        fs::write(bin.join(".links"), b"../victim\0").unwrap();
        let error = directory
            .reconcile_links(c".links", &[(c"npm", &target)])
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&outside).unwrap(), b"untouched");
        assert!(fs::symlink_metadata(bin.join("npm")).is_err());

        fs::remove_file(bin.join(".links")).unwrap();
        for links in [
            vec![(c"npm", target.as_path()), (c"npm", target.as_path())],
            vec![(c".links", target.as_path())],
            vec![(c"../npm", target.as_path())],
        ] {
            assert_eq!(
                directory
                    .reconcile_links(c".links", &links)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        assert!(fs::symlink_metadata(bin.join("npm")).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
