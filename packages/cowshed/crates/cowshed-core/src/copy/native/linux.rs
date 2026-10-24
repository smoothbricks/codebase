use std::fs::{self, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use super::{LeafMaterialization, clone_fallback_error};

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
    fs::set_permissions(destination, metadata.permissions())?;
    set_times(destination, &metadata)
}

/// Descriptor-only sparse copying for a newly created recovery artifact.
pub fn copy_regular(input: &fs::File, output: &fs::File) -> io::Result<()> {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
    use std::os::unix::fs::MetadataExt as _;
    let metadata = input.metadata()?;
    let length = metadata.len();
    output.set_len(length)?;
    let mut input = input;
    let mut output = output;
    let mut offset = 0_u64;
    let mut buffer = [0_u8; 65536];
    while offset < length {
        let start =
            i64::try_from(offset).map_err(|_| io::Error::other("file offset exceeds off_t"))?;
        // SAFETY: input remains open; the seek observes the next allocated logical extent.
        let data = unsafe { libc::lseek(input.as_raw_fd(), start, libc::SEEK_DATA) };
        if data < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ENXIO) {
                break;
            }
            return Err(error);
        }
        // SAFETY: the same live descriptor is queried for this extent's end.
        let hole = unsafe { libc::lseek(input.as_raw_fd(), data, libc::SEEK_HOLE) };
        if hole < 0 {
            return Err(io::Error::last_os_error());
        }
        let data = u64::try_from(data).map_err(|_| io::Error::other("negative data extent"))?;
        let end = u64::try_from(hole)
            .map_err(|_| io::Error::other("negative hole extent"))?
            .min(length);
        if data < offset || end <= data {
            return Err(io::Error::other("invalid sparse extent"));
        }
        input.seek(SeekFrom::Start(data))?;
        output.seek(SeekFrom::Start(data))?;
        offset = data;
        while offset < end {
            let count = usize::try_from((end - offset).min(65536))
                .map_err(|_| io::Error::other("extent length exceeds usize"))?;
            input.read_exact(&mut buffer[..count])?;
            output.write_all(&buffer[..count])?;
            offset +=
                u64::try_from(count).map_err(|_| io::Error::other("read length exceeds u64"))?;
        }
    }
    // Ownership changes may clear set-id bits and capabilities. Apply ownership first,
    // then mode, then the kernel ACL/xattrs (including security.capability), then times.
    // SAFETY: output owns the descriptor; uid/gid come from the held source's stat.
    if unsafe { libc::fchown(output.as_raw_fd(), metadata.uid(), metadata.gid()) } != 0 {
        return Err(super::metadata_error(
            output,
            "copy ownership",
            None,
            io::Error::last_os_error(),
        ));
    }
    output
        .set_permissions(metadata.permissions())
        .map_err(|error| super::metadata_error(output, "copy permissions", None, error))?;
    copy_xattrs(input, output)?;
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
    // SAFETY: output and the two initialized timestamps outlive futimens.
    if unsafe { libc::futimens(output.as_raw_fd(), times.as_ptr()) } != 0 {
        return Err(super::metadata_error(
            output,
            "copy timestamps",
            None,
            io::Error::last_os_error(),
        ));
    }
    Ok(())
}

pub(super) fn list_xattrs(file: &fs::File, buffer: &mut [u8]) -> io::Result<usize> {
    // SAFETY: file remains open; the writable buffer has exactly the supplied length.
    super::metadata_count(unsafe {
        libc::flistxattr(file.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len())
    })
}

pub(super) fn read_xattr(
    file: &fs::File,
    name: &std::ffi::CStr,
    buffer: &mut [u8],
) -> io::Result<usize> {
    // SAFETY: file, NUL-terminated name and writable buffer outlive the call.
    super::metadata_count(unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
        )
    })
}

fn copy_xattrs(source: &fs::File, destination: &fs::File) -> io::Result<()> {
    let source_names = super::xattr_names(source)?;
    let destination_names = super::xattr_names(destination)?;
    let source_names = super::ordered_xattr_names(&source_names)?;
    let destination_names = super::ordered_xattr_names(&destination_names)?;
    // New files can inherit ACLs or labels. Their exact source metadata, not the
    // creating directory's defaults, must survive the cross-filesystem transfer.
    for name in destination_names {
        if source_names.binary_search(&name).is_err() {
            // SAFETY: the live descriptor and NUL-terminated name outlive the call.
            if unsafe { libc::fremovexattr(destination.as_raw_fd(), name.as_ptr()) } != 0 {
                return Err(super::metadata_error(
                    destination,
                    "remove destination-only extended attribute",
                    Some(name),
                    io::Error::last_os_error(),
                ));
            }
        }
    }
    let mut value = Vec::new();
    for name in source_names {
        super::xattr_value(source, name, &mut value)?;
        // SAFETY: the held descriptor, name and initialized value outlive the call.
        // Flags zero creates or replaces the attribute; no namespace is silently omitted.
        if unsafe {
            libc::fsetxattr(
                destination.as_raw_fd(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        } != 0
        {
            return Err(super::metadata_error(
                destination,
                "copy extended attribute",
                Some(name),
                io::Error::last_os_error(),
            ));
        }
    }
    Ok(())
}
