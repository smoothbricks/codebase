//! Shared filesystem-publication and anchored directory-preparation primitives.
//! The temp-artifact grammar, durability barrier and atomic private-file writer
//! coexist with a directory capability for host writes below child-mutable names.
//! These are POSIX operations shared by Darwin and Linux. The privileged bootstrap
//! marker adapter retains its own root-context publication policy.

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::ops::ControlFlow;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// A held directory capability. Child preparation never re-resolves a mutable
/// parent path after it has been opened.
#[derive(Debug)]
pub struct AnchoredDirectory(File);

impl AnchoredDirectory {
    /// Open/create a canonical absolute chain without following any symlink.
    /// One path buffer supplies all NUL-terminated components to openat.
    pub fn create(path: &Path) -> io::Result<Self> {
        Self::open_chain(path, true)
    }

    /// Hold an existing canonical chain without creating a component or following a link.
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_chain(path, false)
    }

    fn open_chain(path: &Path, create: bool) -> io::Result<Self> {
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
            directory = directory.child_with(component, create)?;
        }
        Ok(directory)
    }

    pub fn child(&self, name: &CStr) -> io::Result<Self> {
        self.child_with(name, true)
    }

    pub fn existing_child(&self, name: &CStr) -> io::Result<Self> {
        self.child_with(name, false)
    }

    fn child_with(&self, name: &CStr, create: bool) -> io::Result<Self> {
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
            if !create || error.kind() != io::ErrorKind::NotFound {
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

    pub fn metadata(&self) -> io::Result<fs::Metadata> {
        self.0.metadata()
    }

    /// The entry itself, never a symlink's target. No permission on the child is needed.
    pub fn stat_child(&self, name: &CStr) -> io::Result<Option<libc::stat>> {
        validate_directory_leaf(name)?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: the directory/name are held, and fstatat initializes the output on success.
        if unsafe {
            libc::fstatat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                metadata.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } == 0
        {
            // SAFETY: fstatat initialized the stat above.
            return Ok(Some(unsafe { metadata.assume_init() }));
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        }
    }

    /// Enumerate this held directory, never its pathname. Each visit opens a fresh stream so
    /// its offset cannot affect another visit. Break stops early; errors still close the stream.
    pub fn visit_entries(
        &self,
        mut visit: impl FnMut(&CStr) -> io::Result<ControlFlow<()>>,
    ) -> io::Result<()> {
        // SAFETY: the held descriptor and constant leaf outlive openat.
        let fd = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                c".".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd is a new directory descriptor; a successful fdopendir owns it.
        let directory = unsafe { libc::fdopendir(fd) };
        if directory.is_null() {
            let error = io::Error::last_os_error();
            // SAFETY: fdopendir failed, leaving fd owned by this call.
            drop(unsafe { File::from_raw_fd(fd) });
            return Err(error);
        }
        let result = loop {
            #[cfg(target_os = "macos")]
            // SAFETY: errno is thread-local; zero distinguishes readdir EOF from failure.
            unsafe {
                *libc::__error() = 0
            };
            #[cfg(target_os = "linux")]
            // SAFETY: errno is thread-local; zero distinguishes readdir EOF from failure.
            unsafe {
                *libc::__errno_location() = 0
            };
            // SAFETY: the directory stream remains held until closed below.
            let entry = unsafe { libc::readdir(directory) };
            if entry.is_null() {
                let error = io::Error::last_os_error();
                break if error.raw_os_error() == Some(0) {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            // SAFETY: readdir returned a NUL-terminated name live until its next call.
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
            if name == c"." || name == c".." {
                continue;
            }
            match visit(name) {
                Ok(ControlFlow::Continue(())) => {}
                Ok(ControlFlow::Break(())) => break Ok(()),
                Err(error) => break Err(error),
            }
        };
        // SAFETY: this call owns the stream; closedir also closes its descriptor.
        if unsafe { libc::closedir(directory) } == 0 {
            result
        } else {
            let close = io::Error::last_os_error();
            match result {
                Ok(()) => Err(close),
                Err(error) => Err(io::Error::new(
                    error.kind(),
                    format!("{error}; closing directory: {close}"),
                )),
            }
        }
    }

    /// Whether the only entry in this held directory is the expected leaf.
    #[cfg(target_os = "macos")]
    pub fn contains_only(&self, expected: &CStr) -> io::Result<bool> {
        validate_directory_leaf(expected)?;
        let mut found = false;
        self.visit_entries(|name| {
            if name != expected || found {
                found = false;
                return Ok(ControlFlow::Break(()));
            }
            found = true;
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(found)
    }

    /// The kernel's current name for this directory, for a path-based observation only. The
    /// caller must check the path still names this inode around that observation. Mutations
    /// remain descriptor-relative even when the directory has moved.
    pub fn current_path(&self) -> io::Result<PathBuf> {
        #[cfg(target_os = "macos")]
        {
            let mut path =
                vec![0_u8; usize::try_from(libc::PATH_MAX).expect("PATH_MAX fits usize")];
            // SAFETY: F_GETPATH fills at most PATH_MAX bytes of the live directory's path.
            if unsafe { libc::fcntl(self.0.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
                return Err(io::Error::last_os_error());
            }
            let length = CStr::from_bytes_until_nul(&path)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                .to_bytes()
                .len();
            path.truncate(length);
            Ok(PathBuf::from(OsString::from_vec(path)))
        }
        #[cfg(target_os = "linux")]
        {
            fs::read_link(format!("/proc/self/fd/{}", self.0.as_raw_fd()))
        }
    }

    /// Remove the supplied held child's contents and then its name, never following a link.
    /// The child must still be this parent's entry before descent and before its final unlink.
    /// Return allocated file/link bytes, excluding directory metadata, as the release report does.
    pub fn remove_tree(&self, name: &CStr, tree: &Self) -> io::Result<u64> {
        let require_child = || {
            let mut held = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: the child's held descriptor is live and fstat fills held on success.
            if unsafe { libc::fstat(tree.0.as_raw_fd(), held.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: fstat initialized held.
            let held = unsafe { held.assume_init() };
            let named = self.stat_child(name)?.is_some_and(|at| {
                at.st_mode & libc::S_IFMT == libc::S_IFDIR
                    && at.st_dev == held.st_dev
                    && at.st_ino == held.st_ino
            });
            if named {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "{} no longer names the held directory",
                        name.to_string_lossy()
                    ),
                ))
            }
        };
        require_child()?;
        let bytes = tree.remove_contents()?;
        require_child()?;
        // SAFETY: unlinkat removes only this held parent's empty child, never a link target.
        if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(bytes)
    }

    fn remove_contents(&self) -> io::Result<u64> {
        let metadata = self.metadata()?;
        // SAFETY: geteuid reads no caller-owned memory and cannot fail.
        if metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o700 != 0o700 {
            // Tools seal cache directories. Restore only our own directory's access by fd.
            self.0.set_permissions(fs::Permissions::from_mode(
                (metadata.mode() & 0o7777) | 0o700,
            ))?;
        }
        let mut bytes = 0;
        self.visit_entries(|name| {
            let Some(metadata) = self.stat_child(name)? else {
                return Ok(ControlFlow::Continue(()));
            };
            if metadata.st_mode & libc::S_IFMT == libc::S_IFDIR {
                let child = self.existing_child(name)?;
                bytes += self.remove_tree(name, &child)?;
            } else {
                let blocks = u64::try_from(metadata.st_blocks)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                // SAFETY: only this held parent's leaf is unlinked, without following links.
                if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.as_ptr(), 0) } != 0 {
                    return Err(io::Error::last_os_error());
                }
                bytes += blocks.saturating_mul(512);
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(bytes)
    }

    /// Both names resolve against this held parent; an existing destination is never replaced.
    #[cfg(target_os = "macos")]
    pub fn rename_child_noreplace(&self, source: &CStr, destination: &CStr) -> io::Result<()> {
        validate_directory_leaf(source)?;
        validate_directory_leaf(destination)?;
        rename_noreplace(self.0.as_raw_fd(), source, self.0.as_raw_fd(), destination)?;
        self.sync()
    }

    /// Make the entries this held directory changed durable. Callers issue it once after their
    /// last change in the directory, not once per entry.
    pub fn sync(&self) -> io::Result<()> {
        full_sync(&self.0)
    }

    /// Keep real private cache entries, preserve matching links, and replace stale links and empty
    /// directories without following any mutable parent or destination. An empty directory holds
    /// nothing a link could lose: it is what a tool leaves after a miss it could not fill.
    pub fn ensure_symlink(&self, name: &CStr, target: &Path) -> io::Result<()> {
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

    /// Restore one developer alias through held parents, never through a job-mutated path.
    /// A replaced registry directory joins the existing discard lifecycle without overwriting it.
    /// The caller syncs this held directory once after its last restore in it, see [`Self::sync`].
    pub fn restore_symlink(
        &self,
        name: &CStr,
        target: &Path,
        discard: impl FnOnce() -> io::Result<(Self, CString)>,
    ) -> io::Result<bool> {
        validate_directory_leaf(name)?;
        match self.exact_symlink(name, target) {
            Ok(changed) => Ok(changed),
            Err(error) => {
                if !self
                    .stat_child(name)?
                    .is_some_and(|entry| entry.st_mode & libc::S_IFMT == libc::S_IFDIR)
                {
                    return Err(error);
                }
                let (discard, discarded) = discard()?;
                validate_directory_leaf(&discarded)?;
                rename_noreplace(self.0.as_raw_fd(), name, discard.0.as_raw_fd(), &discarded)?;
                self.exact_symlink(name, target)
            }
        }
    }

    /// Publish `contents` as the private regular file `name` beneath this directory.
    ///
    /// A file that already holds exactly these bytes is left in place, so republishing unchanged
    /// wiring before every spawn costs one read. Otherwise the bytes go to an exclusively created
    /// 0600 temp sibling, are synced, and are renamed over `name`. Nothing is followed: a link a
    /// child planted at `name` is replaced by the rename, never written through.
    pub fn publish_file(&self, name: &CStr, contents: &[u8]) -> io::Result<()> {
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
            self.sync()
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
    pub fn remove_file(&self, name: &CStr) -> io::Result<()> {
        validate_directory_leaf(name)?;
        // SAFETY: unlinkat removes this directory's own entry, not a link target.
        if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.as_ptr(), 0) } == 0 {
            return self.sync();
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
    /// The manifest never under-records: before changing links, publish the old/new name union
    /// with one leading NUL (pending). Sync the repaired directory before publishing the requested
    /// names with two leading NULs (clean). Only a clean manifest and matching links skip writes
    /// and syncs. Pending or older unmarked manifests complete the barrier even if the links
    /// already match, so an interrupted repair cannot pass as durable. A name that is not a plain
    /// leaf, or a directory standing at a link name, is an error.
    pub fn reconcile_links<N: AsRef<CStr>>(
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
        let encode = |names: &mut Vec<&[u8]>, prefix: &[u8]| {
            names.sort_unstable();
            names.dedup();
            let mut bytes = Vec::with_capacity(
                prefix.len() + names.iter().map(|name| name.len() + 1).sum::<usize>(),
            );
            bytes.extend_from_slice(prefix);
            for name in names {
                bytes.extend_from_slice(name);
                bytes.push(0);
            }
            bytes
        };
        let clean = encode(&mut current, b"\0\0");
        let recorded = self.read_regular_file(manifest)?.unwrap_or_default();
        if recorded == clean {
            let mut matching = true;
            for (name, target) in links {
                if !self.symlink_matches(name.as_ref(), target)? {
                    matching = false;
                    break;
                }
            }
            if matching {
                return Ok(());
            }
        }
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
        let mut union: Vec<&[u8]> = prior.iter().map(|name| name.to_bytes()).collect();
        union.extend(current.iter().copied());
        self.publish_file(manifest, &encode(&mut union, b"\0"))?;
        for name in &prior {
            if !current.contains(&name.to_bytes()) {
                self.unlink_entry(name)?;
            }
        }
        for (name, target) in links {
            self.exact_symlink(name.as_ref(), target)?;
        }
        // This barrier precedes the clean receipt. A failed or interrupted publication leaves
        // pending evidence, and the next call repeats the barrier rather than trusting the links.
        self.sync()?;
        self.publish_file(manifest, &clean)
    }

    /// Read the target of the symlink `name` through the held directory into `buffer`, returning
    /// the borrowed prefix. A target filling the whole buffer is refused, never truncated.
    pub fn read_link<'a>(&self, name: &CStr, buffer: &'a mut [u8]) -> io::Result<&'a [u8]> {
        validate_directory_leaf(name)?;
        // SAFETY: the held directory fd and the NUL-terminated leaf are live, and readlinkat writes
        // at most `buffer.len()` bytes into the writable buffer.
        let count = unsafe {
            libc::readlinkat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(count).expect("non-negative readlink length");
        if count == buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "link target exceeds buffer",
            ));
        }
        Ok(&buffer[..count])
    }

    fn symlink_matches(&self, name: &CStr, target: &Path) -> io::Result<bool> {
        let mut buffer = [0u8; 4096];
        match self.read_link(name, &mut buffer) {
            Ok(bytes) => Ok(bytes == target.as_os_str().as_bytes()),
            // Absent, or not a link at all (a regular file the rename replaces).
            Err(error) => match error.raw_os_error() {
                Some(libc::ENOENT | libc::EINVAL) => Ok(false),
                _ => Err(error),
            },
        }
    }

    /// `name` as a symlink to exactly `target`. A matching link is kept and nothing is written.
    /// Anything else is replaced atomically: the new link is made under a unique temp leaf and
    /// renamed over `name`, so the old entry is never removed first. A directory at `name`
    /// refuses the rename. The rename is not synced here: a caller that changed links in this
    /// directory issues one [`Self::sync`] after its last change, before any durable write.
    pub fn exact_symlink(&self, name: &CStr, target: &Path) -> io::Result<bool> {
        if self.symlink_matches(name, target)? {
            return Ok(false);
        }
        // The NUL checks run before any entry is created, so a refused target mutates nothing.
        let target = CString::new(target.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "link target contains NUL"))?;
        let leaf = temp_name(
            OsStr::from_bytes(name.to_bytes()),
            uuid::Uuid::new_v4().simple(),
        );
        let temp = CString::new(leaf.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "link name contains NUL"))?;
        // SAFETY: the target and temp leaf are NUL-terminated; the held parent anchors creation.
        if unsafe { libc::symlinkat(target.as_ptr(), self.0.as_raw_fd(), temp.as_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both names are leaves beneath the same held directory fd.
        if unsafe {
            libc::renameat(
                self.0.as_raw_fd(),
                temp.as_ptr(),
                self.0.as_raw_fd(),
                name.as_ptr(),
            )
        } == 0
        {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        // SAFETY: removes this directory's own temp entry, never a link target.
        if unsafe { libc::unlinkat(self.0.as_raw_fd(), temp.as_ptr(), 0) } != 0 {
            let cleanup = io::Error::last_os_error();
            if cleanup.kind() != io::ErrorKind::NotFound {
                return Err(io::Error::new(
                    error.kind(),
                    format!("{error}; removing the temporary link also failed: {cleanup}"),
                ));
            }
        }
        Err(error)
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
/// Dot-prefixed so crash residue hides from listings, with a `.tmp.` infix so one recognizer
/// ([`is_temp_artifact`]) names every writer's residue. Every writer creates its temp beside the
/// final name it renames over. The publishers here write regular files into the directory they
/// are handed. [`AnchoredDirectory::exact_symlink`] can also run in workspace content: it repoints
/// a link where the link is, such as a package's `node_modules/.bin` alias (developer-link
/// restoration) or an inherited link's directory (`inherited_links::apply`). A crash between
/// creating its temp symlink and renaming it leaves an inert, dot-hidden symlink in that
/// directory, so workspace content can hold residue as well as controller metadata. Creating the
/// symlink runs nothing. No sweeper or watcher exists, so this residue is never swept on its own:
/// it is reclaimed only when the tree holding it is removed whole, by `remove_owned_tree` or the
/// removal that owns that tree.
pub fn temp_name(final_name: &OsStr, discriminator: impl fmt::Display) -> OsString {
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
pub fn is_temp_artifact(file_name: &OsStr) -> bool {
    let Some(name) = file_name.to_str() else {
        return false;
    };
    name.starts_with('.') && name.contains(".tmp.")
}

/// The Linux mount identity of an already-held inode, not its backing device number.
/// A bind mount has a distinct identity even when both paths share the same filesystem.
#[cfg(target_os = "linux")]
pub fn mount_id(held: &File) -> io::Result<u64> {
    use rustix::fs::{AtFlags, StatxFlags, statx};
    let stat = statx(held, c"", AtFlags::EMPTY_PATH, StatxFlags::MNT_ID)?;
    if !StatxFlags::from_bits_retain(stat.stx_mask).contains(StatxFlags::MNT_ID) {
        return Err(io::Error::other(
            "cannot prove mount boundary: STATX_MNT_ID unavailable",
        ));
    }
    Ok(stat.stx_mnt_id)
}

/// Remove a directory tree a sandboxed process wrote, read-only directories included.
///
/// Tools seal what they write: Go's module cache extracts every directory 0555, and
/// content-addressed caches commonly do the same, so `remove_dir_all` gets `EACCES` unlinking a
/// child of such a directory. As `go clean -modcache` does, a removal refused that way restores the
/// owner's `rwx` bits on every directory in the tree this user owns, then removes it once more.
/// Only this user's own directories are touched: one another user owns keeps its mode, so the
/// second removal reports exactly what still refuses it. Symlinks are never followed.
pub fn remove_owned_tree(path: &Path) -> io::Result<()> {
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
pub fn grant_owner_directory_access(root: &Path) -> io::Result<()> {
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
pub fn sync_directory(path: &Path) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC);
    }
    full_sync(&options.open(path)?)
}

/// [`sync_directory`] for a directory the caller already holds by descriptor, so no path is
/// resolved again between the change and its flush.
pub fn sync_held_directory(directory: &File) -> io::Result<()> {
    full_sync(directory)
}

/// Atomically rename one directory-relative entry without replacing an existing destination.
#[cfg(target_os = "macos")]
pub fn rename_noreplace(
    directory: std::os::fd::RawFd,
    source: &std::ffi::CStr,
    destination_directory: std::os::fd::RawFd,
    destination: &std::ffi::CStr,
) -> io::Result<()> {
    // SAFETY: both names are NUL-terminated and remain live for the call; `directory` is an open
    // directory descriptor owned by the caller. RENAME_EXCL makes the destination check atomic.
    let result = unsafe {
        libc::renameatx_np(
            directory,
            source.as_ptr(),
            destination_directory,
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
pub fn rename_noreplace(
    directory: std::os::fd::RawFd,
    source: &std::ffi::CStr,
    destination_directory: std::os::fd::RawFd,
    destination: &std::ffi::CStr,
) -> io::Result<()> {
    // SAFETY: both names are NUL-terminated and remain live for the syscall; `directory` is an
    // open directory descriptor owned by the caller. libc supplies the kernel ABI flag value.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            source.as_ptr(),
            destination_directory,
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
pub fn rename_noreplace(
    _directory: std::os::fd::RawFd,
    _source: &std::ffi::CStr,
    _destination_directory: std::os::fd::RawFd,
    _destination: &std::ffi::CStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic create-new rename is unsupported",
    ))
}

/// Atomically exchange two directory entries, without following either entry's symlink.
/// Darwin uses RENAME_SWAP; Linux uses the renameat2 syscall directly so this also works
/// with libc versions predating its wrapper. A failure leaves both entries unchanged.
pub fn rename_exchange(
    directory: std::os::fd::RawFd,
    source: &CStr,
    destination: &CStr,
) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    // SAFETY: the NUL-terminated names remain live for the call. The caller owns the directory
    // descriptor, or passes AT_FDCWD. RENAME_SWAP exchanges entries in one kernel operation.
    let result = unsafe {
        libc::renameatx_np(
            directory,
            source.as_ptr(),
            directory,
            destination.as_ptr(),
            libc::RENAME_SWAP,
        )
    };
    #[cfg(target_os = "linux")]
    // SAFETY: the names and descriptor have the same lifetime contract as above. The syscall
    // uses the kernel ABI flags, avoiding a dependency on glibc's newer renameat2 wrapper.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            source.as_ptr(),
            directory,
            destination.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (directory, source, destination);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic directory-entry exchange is unsupported",
        ))
    }
}

/// Open the directory at `path` for directory-relative work, refusing a symlink or a
/// non-directory at its last component. Earlier components resolve normally, so a root below a
/// linked system prefix (macOS `/var` → `/private/var`) opens.
pub fn open_directory_nofollow(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}

/// Create the child directory `name` of `parent` with mode 0700, or open the one already there,
/// refusing a symlink or a non-directory at `name`. `true` when this call created it: the parent
/// then gained an entry its writer may have to make durable.
pub fn open_or_create_child_directory(parent: &File, name: &CStr) -> io::Result<(File, bool)> {
    validate_directory_leaf(name)?;
    // SAFETY: mkdirat resolves one leaf beneath the held parent fd; both outlive the call.
    let created = if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) } == 0 {
        true
    } else {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error);
        }
        false
    };
    // SAFETY: the held parent fd and NUL-terminated leaf outlive the call. NOFOLLOW and
    // DIRECTORY refuse a link or a non-directory planted at the name.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful openat returned a new owned directory descriptor.
    Ok((unsafe { File::from_raw_fd(fd) }, created))
}

/// Create the private file `name` beneath `directory`. `O_EXCL` and `O_NOFOLLOW` refuse any entry
/// already there, a link included; the mode is set to 0600 after the open because the open's mode
/// is masked by the umask. A file whose mode cannot be set is unlinked before the error returns.
pub fn create_private_file_at(directory: &File, name: &CStr) -> io::Result<File> {
    validate_directory_leaf(name)?;
    // SAFETY: the held directory fd and NUL-terminated leaf outlive the call.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful openat returned a new owned file descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    let cleanup = TemporaryAt::new(directory, name);
    file.set_permissions(<fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600))?;
    cleanup.disarm();
    Ok(file)
}

/// Unlinks a directory-relative temporary when dropped armed: publication either renames the
/// temporary to its sealed name and disarms this, or leaves nothing behind. The unlink is the
/// last act of a path that already failed, so its own failure has nowhere to go.
pub struct TemporaryAt<'a> {
    directory: &'a File,
    name: &'a CStr,
    armed: bool,
}

impl<'a> TemporaryAt<'a> {
    pub fn new(directory: &'a File, name: &'a CStr) -> Self {
        Self {
            directory,
            name,
            armed: true,
        }
    }

    pub fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryAt<'_> {
    fn drop(&mut self) {
        if self.armed {
            // SAFETY: removes this directory's own temporary entry, never a link target.
            unsafe { libc::unlinkat(self.directory.as_raw_fd(), self.name.as_ptr(), 0) };
        }
    }
}

/// How [`publish_private_file`] failed: an I/O step (with the path it was about), or the caller's
/// own write closure. Typed so callers keep their structured errors instead of flattening
/// serialization failures into `io::Error`.
pub enum PublishError<E> {
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
pub enum Durability {
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
    pub fn sync_file(self, file: &File) -> io::Result<()> {
        match self {
            Self::Process => Ok(()),
            Self::Device => {
                #[cfg(any(test, feature = "test-hooks"))]
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
    pub fn sync_new_entry(self, directory: &Path) -> io::Result<()> {
        match self {
            Self::Process | Self::Device => Ok(()),
            Self::PowerLoss => sync_directory(directory),
        }
    }

    /// [`Self::sync_new_entry`] through a directory descriptor the writer already holds.
    pub fn sync_new_entry_at(self, directory: &File) -> io::Result<()> {
        match self {
            Self::Process | Self::Device => Ok(()),
            Self::PowerLoss => full_sync(directory),
        }
    }
}

/// `F_FULLFSYNC` on macOS (`fsync(2)` elsewhere), the only full-device flush a [`Durability`]
/// issues.
fn full_sync(file: &File) -> io::Result<()> {
    #[cfg(any(test, feature = "test-hooks"))]
    count_sync(|syncs| syncs.full += 1);
    file.sync_all()
}

/// The syncs this thread issued through [`Durability`] since the last [`take_syncs`], so a test
/// can hold a path to the flush its state's tier allows.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Syncs {
    /// `fsync(2)`: [`Durability::Device`].
    pub fsync: usize,
    /// Full-device flushes: [`Durability::PowerLoss`] files and directories.
    pub full: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
thread_local! {
    static SYNCS: std::cell::Cell<Syncs> = const { std::cell::Cell::new(Syncs { fsync: 0, full: 0 }) };
}

#[cfg(any(test, feature = "test-hooks"))]
fn count_sync(count: impl FnOnce(&mut Syncs)) {
    SYNCS.with(|cell| {
        let mut syncs = cell.get();
        count(&mut syncs);
        cell.set(syncs);
    });
}

#[cfg(any(test, feature = "test-hooks"))]
pub fn take_syncs() -> Syncs {
    SYNCS.with(std::cell::Cell::take)
}

/// Atomically publish a private regular file at `path`: write into a uniquely named temp sibling,
/// fsync it, rename it over `path`, fsync the parent. A failure at any step removes the temp.
///
/// The temp is created with `create_new` — `O_EXCL` refuses a symlink final component, dangling
/// included, so no separate `O_NOFOLLOW` is needed — plus `O_CLOEXEC`. Permissions are `0600`
/// twice on purpose: the open mode is masked by the umask, so the explicit `set_permissions`
/// afterwards is what guarantees the private mode.
pub fn publish_private_file<E>(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<(), E>,
) -> Result<(), PublishError<E>> {
    publish_private_file_with(path, Durability::PowerLoss, write)
}

/// [`publish_private_file`] at `durability`: the rename is atomic at every level, so a reader
/// sees the old file or the new one, never a torn one; the level decides only which crash the
/// new one survives.
pub fn publish_private_file_with<E>(
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

    /// A test's private directory under the canonical `/tmp` parent, removed on drop —
    /// the same protocol as cowshed-core's test-support TempRoot, kept local because
    /// this crate cannot depend on core's test support.
    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new_in(parent: &Path, prefix: &str) -> Self {
            use std::os::unix::fs::DirBuilderExt as _;
            let path = parent.join(format!("{prefix}-{}", uuid::Uuid::new_v4().simple()));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .unwrap_or_else(|error| panic!("create test root {}: {error}", path.display()));
            Self(path)
        }
    }

    impl std::ops::Deref for TempRoot {
        type Target = Path;

        fn deref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            match std::fs::remove_dir_all(&self.0) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) if std::thread::panicking() => {
                    crate::eprintln!("remove test root {}: {error}", self.0.display());
                }
                Err(error) => panic!("remove test root {}: {error}", self.0.display()),
            }
        }
    }

    #[test]
    fn held_tree_removal_refuses_a_replaced_child_without_touching_either_tree() {
        let root = TempRoot::new_in(&canonical_parent(), "fsio-held-tree");
        let parent = AnchoredDirectory::create(&root.join("parent")).unwrap();
        let child = parent.child(c"child").unwrap();
        fs::write(root.join("parent/child/ours"), b"our tree").unwrap();
        let foreign = root.join("foreign");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("kept"), b"foreign tree").unwrap();
        fs::rename(root.join("parent/child"), root.join("retained")).unwrap();
        std::os::unix::fs::symlink(&foreign, root.join("parent/child")).unwrap();

        assert_eq!(
            parent.remove_tree(c"child", &child).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(fs::read(foreign.join("kept")).unwrap(), b"foreign tree");
        assert_eq!(fs::read(root.join("retained/ours")).unwrap(), b"our tree");
        assert_eq!(child.current_path().unwrap(), root.join("retained"));
        for _ in 0..2 {
            let mut seen = 0;
            child
                .visit_entries(|name| {
                    assert_eq!(name, c"ours");
                    seen += 1;
                    Ok(ControlFlow::Continue(()))
                })
                .unwrap();
            assert_eq!(seen, 1, "each descriptor listing starts at its own offset");
        }
    }

    fn canonical_parent() -> PathBuf {
        std::fs::canonicalize("/tmp").unwrap_or_else(|error| panic!("canonical /tmp: {error}"))
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn read_only_directory_capability_never_creates_or_follows_a_link() {
        let temporary =
            std::env::temp_dir().join(format!("fsio-read-anchor-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&temporary).unwrap();
        let root = fs::canonicalize(&temporary).unwrap();
        let directory = AnchoredDirectory::open(&root).unwrap();
        assert_eq!(
            directory.existing_child(c"missing").unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(!root.join("missing").exists());
        assert!(AnchoredDirectory::open(&root.join("absent/child")).is_err());
        assert!(!root.join("absent").exists());
        let target = root.join("target");
        fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink(&target, root.join("link")).unwrap();
        let metadata = directory.stat_child(c"link").unwrap().unwrap();
        assert_eq!(metadata.st_mode & libc::S_IFMT, libc::S_IFLNK);
        assert!(directory.existing_child(c"link").is_err());
        assert!(AnchoredDirectory::open(&root.join("link")).is_err());
        assert!(fs::read_dir(target).unwrap().next().is_none());
        fs::remove_dir_all(temporary).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn held_directory_sole_entry_checks_do_not_share_a_directory_offset() {
        let temporary =
            std::env::temp_dir().join(format!("fsio-only-entry-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&temporary).unwrap();
        let root = fs::canonicalize(&temporary).unwrap();
        let directory = AnchoredDirectory::open(&root).unwrap();
        assert!(!directory.contains_only(c".fseventsd").unwrap());
        fs::create_dir(root.join(".fseventsd")).unwrap();
        assert!(directory.contains_only(c".fseventsd").unwrap());
        assert!(
            directory.contains_only(c".fseventsd").unwrap(),
            "a second check must not see EOF"
        );
        fs::write(root.join(".other"), b"preserve hidden work").unwrap();
        assert!(!directory.contains_only(c".fseventsd").unwrap());
        assert_eq!(
            fs::read(root.join(".other")).unwrap(),
            b"preserve hidden work"
        );
        fs::remove_dir_all(temporary).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn held_parent_rename_never_overwrites_or_follows_a_replaced_parent_path() {
        let temporary =
            std::env::temp_dir().join(format!("fsio-held-rename-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&temporary).unwrap();
        let root = fs::canonicalize(&temporary).unwrap();
        let parent = root.join("parent");
        let outside = root.join("outside");
        fs::create_dir(&parent).unwrap();
        fs::create_dir(&outside).unwrap();
        for (name, bytes) in [
            ("from", b"source".as_slice()),
            ("to", b"destination".as_slice()),
        ] {
            fs::create_dir(parent.join(name)).unwrap();
            fs::write(parent.join(name).join("proof"), bytes).unwrap();
        }
        let directory = AnchoredDirectory::open(&parent).unwrap();
        assert_eq!(
            directory
                .rename_child_noreplace(c"from", c"to")
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(parent.join("from/proof")).unwrap(), b"source");
        assert_eq!(fs::read(parent.join("to/proof")).unwrap(), b"destination");
        let moved = root.join("moved");
        fs::rename(&parent, &moved).unwrap();
        std::os::unix::fs::symlink(&outside, &parent).unwrap();
        directory
            .rename_child_noreplace(c"from", c".root-held-test")
            .unwrap();
        assert_eq!(
            fs::read(moved.join(".root-held-test/proof")).unwrap(),
            b"source"
        );
        assert!(
            fs::read_dir(&outside).unwrap().next().is_none(),
            "the replacement was never followed"
        );
        fs::remove_dir_all(temporary).unwrap();
    }

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
    fn exchange_moves_entries_without_following_links_and_failure_preserves_both() {
        let root = std::env::temp_dir().join(format!("fsio-exchange-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(root.join("payload/src")).unwrap();
        fs::write(root.join("payload/src/proof"), b"payload").unwrap();
        let outside = root.join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("proof"), b"outside").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("record")).unwrap();
        let directory = open_directory_nofollow(&root).unwrap();

        rename_exchange(directory.as_raw_fd(), c"payload", c"record").unwrap();
        assert_eq!(fs::read_link(root.join("payload")).unwrap(), outside);
        assert_eq!(fs::read(root.join("record/src/proof")).unwrap(), b"payload");
        assert_eq!(fs::read(outside.join("proof")).unwrap(), b"outside");
        assert_eq!(
            rename_exchange(directory.as_raw_fd(), c"record", c"absent")
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(fs::read(root.join("record/src/proof")).unwrap(), b"payload");
        assert_eq!(fs::read_link(root.join("payload")).unwrap(), outside);
        assert!(!root.join("absent").exists());

        rename_exchange(directory.as_raw_fd(), c"record", c"payload").unwrap();
        assert_eq!(
            fs::read(root.join("payload/src/proof")).unwrap(),
            b"payload"
        );
        assert_eq!(fs::read_link(root.join("record")).unwrap(), outside);
        fs::remove_dir_all(root).unwrap();
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
        assert_eq!(fs::read(bin.join(".links")).unwrap(), b"\0\0node\0npm\0");

        directory
            .reconcile_links(c".links", &[(c"node", &node)])
            .unwrap();
        assert!(
            fs::symlink_metadata(bin.join("npm")).is_err(),
            "a stale link stays on PATH"
        );
        assert_eq!(fs::read_link(bin.join("node")).unwrap(), node);
        assert_eq!(fs::read(bin.join(".links")).unwrap(), b"\0\0node\0");
        assert_eq!(fs::read(bin.join("unowned")).unwrap(), b"someone else's");

        directory.reconcile_links::<&CStr>(c".links", &[]).unwrap();
        assert!(fs::symlink_metadata(bin.join("node")).is_err());
        assert_eq!(fs::read(bin.join(".links")).unwrap(), b"\0\0");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn clean_reconciliation_checks_links_without_writes_or_syncs() {
        use std::os::unix::fs::MetadataExt;
        let (root, bin, directory) = reconcile_fixture("clean");
        let target = root.join("node");
        let links = [(c"node", target.as_path())];
        directory.reconcile_links(c".links", &links).unwrap();
        let manifest_inode = fs::metadata(bin.join(".links")).unwrap().ino();
        let link_inode = fs::symlink_metadata(bin.join("node")).unwrap().ino();
        take_syncs();
        directory.reconcile_links(c".links", &links).unwrap();
        assert_eq!(take_syncs(), Syncs::default());
        assert_eq!(
            fs::metadata(bin.join(".links")).unwrap().ino(),
            manifest_inode
        );
        assert_eq!(
            fs::symlink_metadata(bin.join("node")).unwrap().ino(),
            link_inode
        );

        // The receipt never stands in for inspecting the current links.
        fs::remove_file(bin.join("node")).unwrap();
        std::os::unix::fs::symlink(root.join("wrong"), bin.join("node")).unwrap();
        directory.reconcile_links(c".links", &links).unwrap();
        assert_eq!(fs::read_link(bin.join("node")).unwrap(), target);
        assert_eq!(
            take_syncs().full,
            3,
            "pending, repaired links, clean receipt"
        );
        directory.reconcile_links::<&CStr>(c".links", &[]).unwrap();
        take_syncs();
        directory.reconcile_links::<&CStr>(c".links", &[]).unwrap();
        assert_eq!(
            take_syncs(),
            Syncs::default(),
            "an empty clean set is stable too"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pending_and_unmarked_reconciliations_sync_even_when_every_link_matches() {
        let (root, bin, directory) = reconcile_fixture("pending");
        let target = root.join("node");
        directory.exact_symlink(c"node", &target).unwrap();
        for receipt in [b"node\0".as_slice(), b"\0node\0"] {
            // Model an older writer, or death after the last link rename but before its barrier.
            directory.publish_file(c".links", receipt).unwrap();
            take_syncs();
            directory
                .reconcile_links(c".links", &[(c"node", &target)])
                .unwrap();
            assert!(take_syncs().full >= 2, "links sync before publishing clean");
            assert_eq!(fs::read(bin.join(".links")).unwrap(), b"\0\0node\0");
            directory
                .reconcile_links(c".links", &[(c"node", &target)])
                .unwrap();
            assert_eq!(take_syncs(), Syncs::default());
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_failed_repair_keeps_pending_ownership_until_the_retry_finishes() {
        let (root, bin, directory) = reconcile_fixture("interrupted");
        let target = root.join("program");
        directory
            .reconcile_links(c".links", &[(c"stale", &target)])
            .unwrap();
        fs::create_dir(bin.join("blocked")).unwrap();
        let links = [(c"first", target.as_path()), (c"blocked", target.as_path())];
        assert!(directory.reconcile_links(c".links", &links).is_err());
        assert_eq!(
            fs::read(bin.join(".links")).unwrap(),
            b"\0blocked\0first\0stale\0",
            "failed repairs retain the durable union, not a clean receipt"
        );
        assert_eq!(fs::read_link(bin.join("first")).unwrap(), target);
        assert!(fs::symlink_metadata(bin.join("stale")).is_err());
        fs::remove_dir(bin.join("blocked")).unwrap();
        take_syncs();
        directory.reconcile_links(c".links", &links).unwrap();
        assert_eq!(
            take_syncs().full,
            2,
            "resume pending, sync links, publish clean"
        );
        assert_eq!(
            fs::read(bin.join(".links")).unwrap(),
            b"\0\0blocked\0first\0"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_refused_pending_manifest_leaves_links_untouched() {
        let (root, bin, directory) = reconcile_fixture("pending-refusal");
        let old = root.join("old");
        directory.exact_symlink(c"node", &old).unwrap();
        fs::create_dir(bin.join(".links")).unwrap();
        assert!(
            directory
                .reconcile_links(c".links", &[(c"node", &root.join("new"))])
                .is_err()
        );
        assert_eq!(fs::read_link(bin.join("node")).unwrap(), old);
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

    #[test]
    fn a_held_parent_swapped_for_a_link_never_writes_through_the_foreign_directory() {
        let scratch = TempRoot::new_in(&canonical_parent(), "fsio-held-parent-swap");
        let parent = scratch.join("parent");
        let held = AnchoredDirectory::create(&parent).unwrap();
        fs::create_dir(parent.join("alias")).unwrap();
        let foreign = scratch.join("foreign");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("sentinel"), "foreign\n").unwrap();
        fs::rename(&parent, scratch.join("moved")).unwrap();
        std::os::unix::fs::symlink(&foreign, &parent).unwrap();
        held.restore_symlink(c"alias", Path::new("../target"), || {
            Ok((
                AnchoredDirectory::create(&scratch.join("discard"))?,
                CString::new("displaced").map_err(io::Error::other)?,
            ))
        })
        .unwrap();
        assert_eq!(fs::read(foreign.join("sentinel")).unwrap(), b"foreign\n");
        assert!(fs::symlink_metadata(foreign.join("alias")).is_err());
        assert_eq!(
            fs::read_link(scratch.join("moved/alias")).unwrap(),
            Path::new("../target")
        );
    }

    #[test]
    fn a_discard_leaf_already_occupied_refuses_the_replaced_directory_untouched() {
        let scratch = TempRoot::new_in(&canonical_parent(), "fsio-occupied-discard");
        let links = AnchoredDirectory::create(&scratch.join("links")).unwrap();
        let discard = AnchoredDirectory::create(&scratch.join("discard")).unwrap();
        fs::create_dir(scratch.join("links/alias")).unwrap();
        fs::write(scratch.join("links/alias/keep"), "real\n").unwrap();
        fs::create_dir(scratch.join("discard/occupied")).unwrap();
        fs::write(scratch.join("discard/occupied/keep"), "earlier\n").unwrap();
        let target = scratch.join("target");
        let error = links
            .restore_symlink(c"alias", &target, || {
                Ok((discard, CString::new("occupied").unwrap()))
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            fs::read(scratch.join("links/alias/keep")).unwrap(),
            b"real\n"
        );
        assert_eq!(
            fs::read(scratch.join("discard/occupied/keep")).unwrap(),
            b"earlier\n"
        );
    }

    #[test]
    fn exact_symlink_replaces_a_wrong_link_atomically_and_refuses_a_nul_target_untouched() {
        let scratch = TempRoot::new_in(&canonical_parent(), "fsio-exact-symlink");
        let held = AnchoredDirectory::create(&scratch.join("links")).unwrap();
        std::os::unix::fs::symlink(scratch.join("old"), scratch.join("links/alias")).unwrap();
        assert!(held.exact_symlink(c"alias", &scratch.join("new")).unwrap());
        assert_eq!(
            fs::read_link(scratch.join("links/alias")).unwrap(),
            scratch.join("new")
        );
        assert!(!held.exact_symlink(c"alias", &scratch.join("new")).unwrap());
        let nul = Path::new(OsStr::from_bytes(b"a\0b"));
        let error = held.exact_symlink(c"alias", nul).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            fs::read_link(scratch.join("links/alias")).unwrap(),
            scratch.join("new"),
            "the old link is intact after a refused target"
        );
        let leftovers = fs::read_dir(scratch.join("links")).unwrap().count();
        assert_eq!(leftovers, 1, "no temporary link is left behind");
    }

    #[test]
    fn exact_symlink_refuses_a_directory_at_the_name_without_removing_it() {
        let scratch = TempRoot::new_in(&canonical_parent(), "fsio-exact-symlink-dir");
        let held = AnchoredDirectory::create(&scratch.join("links")).unwrap();
        fs::create_dir(scratch.join("links/alias")).unwrap();
        fs::write(scratch.join("links/alias/keep"), "real\n").unwrap();
        assert!(held.exact_symlink(c"alias", &scratch.join("new")).is_err());
        assert_eq!(
            fs::read(scratch.join("links/alias/keep")).unwrap(),
            b"real\n"
        );
        assert_eq!(fs::read_dir(scratch.join("links")).unwrap().count(), 1);
    }

    /// The rename is atomic and carries no sync of its own: a batch of link changes costs the one
    /// barrier its caller issues, and a link that already matches costs none.
    #[test]
    fn exact_symlink_defers_its_directory_sync_to_the_caller() {
        let scratch = TempRoot::new_in(&canonical_parent(), "fsio-deferred-sync");
        let held = AnchoredDirectory::create(&scratch.join("links")).unwrap();
        take_syncs();
        assert!(held.exact_symlink(c"one", &scratch.join("a")).unwrap());
        assert!(held.exact_symlink(c"two", &scratch.join("b")).unwrap());
        assert!(!held.exact_symlink(c"one", &scratch.join("a")).unwrap());
        assert_eq!(
            take_syncs(),
            Syncs::default(),
            "a link change issues no sync of its own"
        );
        held.sync().unwrap();
        assert_eq!(
            take_syncs(),
            Syncs { fsync: 0, full: 1 },
            "the held directory is synced once"
        );
    }

    /// Three bootstrap directories are reconciled before every sandboxed spawn. Run explicitly
    /// on the host with `taskpolicy -b cargo test -p cowshed-os bootstrap_reconciliation_cost
    /// -- --ignored --nocapture --test-threads=1`; timings are observations, not assertions.
    #[test]
    #[ignore = "manual filesystem performance measurement"]
    fn bootstrap_reconciliation_cost() {
        for count in [1, 8, 32] {
            let scratch = TempRoot::new_in(&canonical_parent(), "fsio-bootstrap-cost");
            let directories: Vec<_> = ["bin", "manager-bin", "real-bin"]
                .map(|name| AnchoredDirectory::create(&scratch.join(name)).unwrap())
                .into_iter()
                .collect();
            let names: Vec<_> = (0..count)
                .map(|index| CString::new(format!("tool-{index}")).unwrap())
                .collect();
            let links: Vec<_> = names
                .iter()
                .map(|name| (name.as_c_str(), Path::new("/usr/bin/true")))
                .collect();
            let reconcile = || {
                for directory in &directories {
                    directory.reconcile_links(c".links", &links).unwrap();
                }
            };
            take_syncs();
            let started = std::time::Instant::now();
            reconcile();
            let cold = started.elapsed();
            let cold_syncs = take_syncs();
            let started = std::time::Instant::now();
            for _ in 0..24 {
                reconcile();
            }
            let warm = started.elapsed();
            let warm_syncs = take_syncs();
            println!(
                "bootstrap-cost links_per_directory={count} jobs=24 cold_us={} cold_full={} warm_us_per_job={:.3} warm_ms_per_1000={:.3} warm_full={}",
                cold.as_micros(),
                cold_syncs.full,
                warm.as_secs_f64() * 1e6 / 24.0,
                warm.as_secs_f64() * 1e6 / 24.0,
                warm_syncs.full,
            );
        }
    }
}
