#[cfg(not(target_os = "macos"))]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

use std::ffi::{CStr, CString};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd as _, RawFd};

// Darwin's sys/acl.h owns this opaque ABI; copy and its OS-level oracle share these declarations.
#[cfg(target_os = "macos")]
#[repr(C)]
pub(super) struct NativeAcl {
    _private: [u8; 0],
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    pub(super) fn acl_get_fd_np(fd: libc::c_int, kind: libc::c_int) -> *mut NativeAcl;
    pub(super) fn acl_free(object: *mut libc::c_void) -> libc::c_int;
}
#[cfg(not(target_os = "macos"))]
use linux::{list_xattrs, read_xattr};
#[cfg(target_os = "macos")]
use macos::{list_xattrs, read_xattr};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeafMaterialization {
    Cloned,
    Copied,
}

#[cfg(any(not(target_os = "macos"), test))]
pub(super) fn clone_fallback_error(error: &std::io::Error) -> bool {
    error.raw_os_error().is_some_and(|code| {
        code == libc::EXDEV
            || code == libc::EOPNOTSUPP
            || code == libc::ENOTTY
            || code == libc::EINVAL
    })
}

#[cfg(not(target_os = "macos"))]
pub use linux::{copy_leaf, copy_regular, directory_metadata};
#[cfg(target_os = "macos")]
pub use macos::{copy_leaf, copy_regular, copy_symlink, directory_metadata};

/// Observe a logical hole in a freshly opened regular file. Physical compression alone is not
/// a hole: SEEK_HOLE answers in the logical data stream. Restore offset zero before the caller
/// verifies bytes; an unsupported observation is an error, never guessed from st_blocks.
pub(super) fn has_holes(file: &std::fs::File, length: u64) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;
    if length == 0 {
        return Ok(false);
    }
    // SAFETY: the file owns its descriptor through both seeks; neither consumes it.
    let hole = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_HOLE) };
    if hole < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the same live descriptor is restored to the start before byte reads.
    if unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_SET) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let hole = u64::try_from(hole).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "negative logical hole offset",
        )
    })?;
    if hole > length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "logical hole offset exceeds the recorded file length",
        ));
    }
    Ok(hole < length)
}

/// Retain the native cause and descriptor context; the recovery owner adds admitted paths.
#[derive(Debug, thiserror::Error)]
#[error("{operation} on descriptor {descriptor}, attribute {attribute:?}: {source}")]
struct MetadataError {
    operation: &'static str,
    descriptor: RawFd,
    attribute: Option<CString>,
    #[source]
    source: io::Error,
}

fn metadata_error(
    file: &File,
    operation: &'static str,
    attribute: Option<&CStr>,
    source: io::Error,
) -> io::Error {
    io::Error::new(
        source.kind(),
        MetadataError {
            operation,
            descriptor: file.as_raw_fd(),
            attribute: attribute.map(CStr::to_owned),
            source,
        },
    )
}

fn metadata_count(count: libc::ssize_t) -> io::Result<usize> {
    if count < 0 {
        Err(io::Error::last_os_error())
    } else {
        usize::try_from(count).map_err(|_| io::Error::other("metadata length exceeds usize"))
    }
}

fn xattr_names(file: &File) -> io::Result<Vec<u8>> {
    let read = |buffer: &mut [u8]| {
        list_xattrs(file, buffer)
            .map_err(|error| metadata_error(file, "list extended attributes", None, error))
    };
    let length = read(&mut [])?;
    let mut names = vec![0; length];
    if read(&mut names)? != length {
        return Err(metadata_error(
            file,
            "list extended attributes",
            None,
            io::Error::new(
                io::ErrorKind::InvalidData,
                "attribute names changed while reading",
            ),
        ));
    }
    Ok(names)
}

fn ordered_xattr_names(mut bytes: &[u8]) -> io::Result<Vec<&CStr>> {
    let mut names = Vec::new();
    while !bytes.is_empty() {
        let name = CStr::from_bytes_until_nul(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty extended attribute name",
            ));
        }
        names.push(name);
        bytes = &bytes[name.to_bytes_with_nul().len()..];
    }
    names.sort_unstable();
    Ok(names)
}

fn xattr_value(file: &File, name: &CStr, buffer: &mut Vec<u8>) -> io::Result<()> {
    let read = |buffer: &mut [u8]| {
        read_xattr(file, name, buffer)
            .map_err(|error| metadata_error(file, "read extended attribute", Some(name), error))
    };
    let length = read(&mut [])?;
    buffer.resize(length, 0);
    if read(buffer)? != length {
        return Err(metadata_error(
            file,
            "read extended attribute",
            Some(name),
            io::Error::new(
                io::ErrorKind::InvalidData,
                "attribute value changed while reading",
            ),
        ));
    }
    Ok(())
}

/// Linux exposes POSIX ACLs in its kernel xattr namespace. Darwin keeps its extended ACL
/// separately; ordinary xattrs include resource forks, but not private compression storage.
/// Metadata changes during these reads are also fenced by the caller's ctime witnesses.
pub(super) fn verify_metadata(source: &File, destination: &File) -> io::Result<()> {
    let source_names = xattr_names(source)?;
    let destination_names = xattr_names(destination)?;
    let source_names = ordered_xattr_names(&source_names)?;
    let destination_names = ordered_xattr_names(&destination_names)?;
    if source_names != destination_names {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "extended attribute names differ: source descriptor {} {source_names:?}, destination descriptor {} {destination_names:?}",
                source.as_raw_fd(),
                destination.as_raw_fd(),
            ),
        ));
    }
    let mut source_value = Vec::new();
    let mut destination_value = Vec::new();
    for name in source_names {
        xattr_value(source, name, &mut source_value)?;
        xattr_value(destination, name, &mut destination_value)?;
        if source_value != destination_value {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "extended attribute {name:?} differs: source descriptor {}, destination descriptor {}",
                    source.as_raw_fd(),
                    destination.as_raw_fd(),
                ),
            ));
        }
    }
    #[cfg(target_os = "macos")]
    macos::verify_acl(source, destination)?;
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
}
