//! `copyfile(3)` given paths opens its source and destination without `O_CLOEXEC` and holds
//! them for the whole call, so a child this process spawns meanwhile keeps them past its `exec`,
//! for its whole life, and holds the copied tree's volume (cowshed-os `fork_lock`). Measured
//! 2026-10-21: a sibling test's `tail -f` kept a copied workspace's `.git/logs/refs/heads` open
//! and land kept that workspace as in use. So every copy here names descriptors this process
//! opened, which `std` opens close-on-exec, through `fcopyfile(3)`. Only a clone names paths:
//! `COPYFILE_CLONE_FORCE` is one `clonefileat(2)` on the two paths and opens no descriptor.

use super::LeafMaterialization;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;

/// Data plus every attribute `copyfile(3)` knows how to carry.
const COPYFILE_ALL: libc::copyfile_flags_t = libc::COPYFILE_METADATA | libc::COPYFILE_DATA;

fn c_path(path: &Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt as _;
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains an interior NUL: {}", path.display()),
        )
    })
}

#[expect(
    clippy::disallowed_methods,
    reason = "COPYFILE_CLONE_FORCE is one clonefileat(2) on the two paths and opens no descriptor"
)]
fn clone(source: &Path, destination: &Path) -> io::Result<()> {
    let source = c_path(source)?;
    let destination = c_path(destination)?;
    // SAFETY: both C strings outlive the call, and a null state requests the
    // default one-shot copy rather than a caller-managed one.
    let status = unsafe {
        libc::copyfile(
            source.as_ptr(),
            destination.as_ptr(),
            std::ptr::null_mut(),
            COPYFILE_ALL | libc::COPYFILE_CLONE_FORCE | libc::COPYFILE_NOFOLLOW,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// The object at `path` itself, a final symlink included (`O_SYMLINK`; any other object opens
/// as a plain read-only open does), close-on-exec as `std` always opens.
fn open_object(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_SYMLINK)
        .open(path)
}

fn fcopyfile(source: &File, destination: &File, flags: libc::copyfile_flags_t) -> io::Result<()> {
    // SAFETY: both files keep their descriptors open for the whole call, and a null state
    // requests the default one-shot copy rather than a caller-managed one.
    let status = unsafe {
        libc::fcopyfile(
            source.as_raw_fd(),
            destination.as_raw_fd(),
            std::ptr::null_mut(),
            flags,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Ask APFS for a metadata-only clone first. Adoption usually crosses from the checkout's
/// volume into the staged image, where clonefile is unsupported; that is a per-entry fallback,
/// not a reason to abort the whole tree after earlier subtrees have completed.
pub fn copy_leaf(source: &Path, destination: &Path) -> io::Result<LeafMaterialization> {
    match clone(source, destination) {
        Ok(()) => Ok(LeafMaterialization::Cloned),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(code) if code == libc::EXDEV || code == libc::ENOTSUP
            ) =>
        {
            match std::fs::remove_file(destination) {
                Ok(()) => {}
                Err(remove) if remove.kind() == io::ErrorKind::NotFound => {}
                Err(remove) => return Err(remove),
            }
            copy_unshared(source, destination).map(|()| LeafMaterialization::Copied)
        }
        Err(error) => Err(error),
    }
}

fn copy_unshared(source: &Path, destination: &Path) -> io::Result<()> {
    let input = open_object(source)?;
    if input.metadata()?.file_type().is_symlink() {
        return copy_symlink_from(&input, source, destination);
    }
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)?;
    copy_regular(&input, &output)
}

/// Copy a regular file through already admitted descriptors, preserving sparse holes and metadata.
fn copy_regular(input: &File, output: &File) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = input.metadata()?;
    let sparse = metadata.blocks().saturating_mul(512) < metadata.len()
        && super::has_holes(input, metadata.len())?;
    let flags = if sparse {
        libc::COPYFILE_METADATA | libc::COPYFILE_DATA_SPARSE
    } else {
        COPYFILE_ALL
    };
    fcopyfile(input, output, flags)
        .map_err(|error| super::metadata_error(output, "copy data and metadata", None, error))
}

fn copy_symlink_from(input: &File, source: &Path, destination: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(std::fs::read_link(source)?, destination)?;
    fcopyfile(input, &open_object(destination)?, libc::COPYFILE_METADATA)
}

/// Metadata only: the directory already exists and its contents were written
/// by this copier, so nothing but the directory's own attributes moves.
pub fn directory_metadata(source: &Path, destination: &Path) -> io::Result<()> {
    fcopyfile(
        &open_object(source)?,
        &open_object(destination)?,
        libc::COPYFILE_METADATA,
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// The open descriptors of this process that are not close-on-exec and name a path under
    /// `root`: what a child spawned now would keep past its `exec`, for its whole life.
    fn inheritable_under(root: &Path) -> Vec<PathBuf> {
        let mut held = Vec::new();
        for descriptor in 0..256 {
            // SAFETY: F_GETFD reads one integer flag; a closed descriptor answers -1.
            let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
            if flags < 0 || flags & libc::FD_CLOEXEC != 0 {
                continue;
            }
            let mut path = [0_u8; libc::PATH_MAX as usize];
            // SAFETY: F_GETPATH writes at most PATH_MAX bytes, NUL-terminated, into `path`.
            if unsafe { libc::fcntl(descriptor, libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
                continue;
            }
            let path = std::ffi::CStr::from_bytes_until_nul(&path).expect("a NUL-terminated path");
            let path = Path::new(std::ffi::OsStr::from_bytes(path.to_bytes()));
            if path.starts_with(root) {
                held.push(path.to_owned());
            }
        }
        held
    }

    /// A child spawned while the copier writes inherits no descriptor of either tree.
    /// `copyfile(3)` given paths opens both without `O_CLOEXEC` and holds them for the whole
    /// call, so a sibling's spawn kept them for its life and held the copied workspace's volume.
    /// A watcher reads every descriptor's flag while the copies run; one sighting is the defect.
    #[test]
    fn a_copy_holds_no_descriptor_a_concurrent_spawn_inherits() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-copy-cloexec-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(root.join("source/directory")).expect("source tree");
        fs::create_dir_all(root.join("destination/directory")).expect("destination tree");
        fs::write(root.join("source/file"), b"payload").expect("source file");
        std::os::unix::fs::symlink("file", root.join("source/link")).expect("source symlink");
        let root = fs::canonicalize(&root).expect("the kernel's name for the root");
        let (source, destination) = (root.join("source"), root.join("destination"));
        let done = AtomicBool::new(false);
        let seen = std::thread::scope(|scope| {
            let watcher = scope.spawn(|| {
                let mut seen = Vec::new();
                while !done.load(Ordering::Acquire) {
                    seen.extend(inheritable_under(&root));
                }
                seen
            });
            for _ in 0..2_000 {
                super::directory_metadata(
                    &source.join("directory"),
                    &destination.join("directory"),
                )
                .expect("copy directory metadata");
                for leaf in ["file", "link"] {
                    super::copy_unshared(&source.join(leaf), &destination.join(leaf))
                        .expect("copy a leaf without a clone");
                    fs::remove_file(destination.join(leaf)).expect("remove the copied leaf");
                }
            }
            done.store(true, Ordering::Release);
            watcher.join().expect("the watcher")
        });
        fs::remove_dir_all(&root).expect("remove the fixture");
        assert!(
            seen.is_empty(),
            "{} sightings of an inheritable copy descriptor, first {:?}",
            seen.len(),
            seen.first()
        );
    }
}
