use std::ffi::CString;
use std::io;
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

fn copyfile(source: &Path, destination: &Path, flags: libc::copyfile_flags_t) -> io::Result<()> {
    let source = c_path(source)?;
    let destination = c_path(destination)?;
    // SAFETY: both C strings outlive the call, and a null state requests the
    // default one-shot copy rather than a caller-managed one.
    let status = unsafe {
        libc::copyfile(
            source.as_ptr(),
            destination.as_ptr(),
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
pub fn copy_leaf(source: &Path, destination: &Path) -> io::Result<()> {
    let clone = copyfile(
        source,
        destination,
        COPYFILE_ALL | libc::COPYFILE_CLONE_FORCE | libc::COPYFILE_NOFOLLOW,
    );
    match clone {
        Ok(()) => Ok(()),
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
            copyfile(source, destination, COPYFILE_ALL | libc::COPYFILE_NOFOLLOW)
        }
        Err(error) => Err(error),
    }
}

/// Metadata only: the directory already exists and its contents were written
/// by this copier, so nothing but the directory's own attributes moves.
pub fn directory_metadata(source: &Path, destination: &Path) -> io::Result<()> {
    copyfile(
        source,
        destination,
        libc::COPYFILE_METADATA | libc::COPYFILE_NOFOLLOW,
    )
}
