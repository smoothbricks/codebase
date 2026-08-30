use std::fs::{self, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use super::{LeafMaterialization, clone_fallback_error};

const FICLONE: libc::c_ulong = 0x4004_9409;

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
    if unsafe { libc::ioctl(destination_file.as_raw_fd(), FICLONE, source.as_raw_fd()) } == 0 {
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
    fs::set_permissions(destination, metadata.permissions())?;
    set_times(destination, &metadata)
}
