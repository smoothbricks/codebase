use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use super::LeafMaterialization;

/// Whether a refused `FICLONE` says only that these two files cannot share extents, so the
/// leaf is copied instead; any other failure is the copy's.
fn clone_fallback_error(error: &std::io::Error) -> bool {
    error.raw_os_error().is_some_and(|code| {
        code == libc::EXDEV
            || code == libc::EOPNOTSUPP
            || code == libc::ENOTTY
            || code == libc::EINVAL
    })
}

fn set_times(path: &Path, metadata: &fs::Metadata) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;
    let target = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "interior NUL in path"))?;
    let times = [
        libc::timespec {
            tv_sec: metadata.atime(),
            tv_nsec: metadata.atime_nsec(),
        },
        libc::timespec {
            tv_sec: metadata.mtime(),
            tv_nsec: metadata.mtime_nsec(),
        },
    ];
    // SAFETY: the path string and the two-element array both outlive the call.
    let status = unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            target.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn reflink(source: &Path, destination: &Path) -> io::Result<bool> {
    let mut source_options = OpenOptions::new();
    source_options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let source = source_options.open(source)?;
    let mut destination_options = OpenOptions::new();
    destination_options
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC);
    let destination_file = destination_options.open(destination)?;
    // SAFETY: both descriptors are live regular files for the duration of the ioctl; FICLONE does
    // not retain either descriptor and atomically shares the source extents with the destination.
    if unsafe {
        libc::ioctl(
            destination_file.as_raw_fd(),
            libc::FICLONE,
            source.as_raw_fd(),
        )
    } == 0
    {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    drop(destination_file);
    fs::remove_file(destination)?;
    if clone_fallback_error(&error) {
        Ok(false)
    } else {
        Err(error)
    }
}

pub fn copy_leaf(source: &Path, destination: &Path) -> io::Result<LeafMaterialization> {
    let metadata = fs::symlink_metadata(source)?;
    let materialization = if metadata.file_type().is_symlink() {
        std::os::unix::fs::symlink(fs::read_link(source)?, destination)?;
        LeafMaterialization::Copied
    } else if reflink(source, destination)? {
        LeafMaterialization::Cloned
    } else {
        fs::copy(source, destination)?;
        LeafMaterialization::Copied
    };
    if !metadata.file_type().is_symlink() {
        fs::set_permissions(destination, metadata.permissions())?;
    }
    set_times(destination, &metadata)?;
    Ok(materialization)
}

pub fn directory_metadata(source: &Path, destination: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.file_type().is_symlink()
        && !fs::symlink_metadata(destination)?.file_type().is_symlink()
    {
        fs::set_permissions(destination, metadata.permissions())?;
    }
    set_times(destination, &metadata)
}

/// Linux has no per-descriptor uncached mode short of `O_DIRECT`, which binds every buffer,
/// offset and length to the device's logical block alignment; these transfers keep no such
/// contract. The page cache carries them, and this changes nothing.
pub(crate) fn bypass_cache(_: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::clone_fallback_error;

    #[test]
    fn only_filesystem_clone_capability_errors_fall_back() {
        for code in [libc::EXDEV, libc::EOPNOTSUPP, libc::ENOTTY, libc::EINVAL] {
            assert!(clone_fallback_error(&std::io::Error::from_raw_os_error(
                code
            )));
        }
        for code in [libc::EIO, libc::ENOSPC, libc::EACCES] {
            assert!(!clone_fallback_error(&std::io::Error::from_raw_os_error(
                code
            )));
        }
    }

    #[test]
    fn directory_metadata_preserves_symlink_times_without_touching_its_target() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let root = std::env::temp_dir().join(format!(
            "cowshed-linux-symlink-meta-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let source = root.join("source");
        let destination = root.join("destination");
        std::fs::create_dir_all(&source).expect("source dir");
        std::fs::create_dir_all(&destination).expect("destination dir");

        let target = destination.join("target");
        std::fs::write(&target, b"keep").expect("target file");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))
            .expect("target mode");
        let target_before = std::fs::symlink_metadata(&target).expect("target meta");

        symlink("payload", source.join("dangling")).expect("source dangling");
        symlink("missing", destination.join("dangling")).expect("dest dangling");
        super::directory_metadata(&source.join("dangling"), &destination.join("dangling"))
            .expect("dangling symlink metadata must not fail ENOENT");
        assert_eq!(
            std::fs::symlink_metadata(source.join("dangling"))
                .expect("source dangling meta")
                .modified()
                .expect("source mtime"),
            std::fs::symlink_metadata(destination.join("dangling"))
                .expect("dest dangling meta")
                .modified()
                .expect("dest mtime"),
        );

        symlink("payload", source.join("live")).expect("source live");
        symlink("target", destination.join("live")).expect("dest live");
        super::directory_metadata(&source.join("live"), &destination.join("live"))
            .expect("live symlink metadata");
        let target_after = std::fs::symlink_metadata(&target).expect("target meta after");
        assert_eq!(
            target_after.permissions().mode() & 0o777,
            target_before.permissions().mode() & 0o777,
            "symlink metadata must not chmod the symlink target"
        );
        assert_eq!(
            target_after.modified().expect("after mtime"),
            target_before.modified().expect("before mtime"),
            "symlink metadata must not change the symlink target's mtime"
        );

        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}
