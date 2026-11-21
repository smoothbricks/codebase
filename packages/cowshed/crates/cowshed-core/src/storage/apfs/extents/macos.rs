//! macOS's view of an image file's physical layout: `F_LOG2PHYS_EXT` maps a file range to its
//! device run, `F_PUNCHHOLE` deallocates one, `F_NOCACHE` keeps a rewrite out of the page cache
//! and `statfs` reads the room a rewrite needs.

use std::fs::File;
use std::io;
use std::path::Path;

use super::{ApfsStorageError, PhysicalRun, io_error};

pub(super) use crate::copy::native::bypass_cache;

/// The length of the physically contiguous run from `offset` on, at most `length` bytes.
pub(super) fn run_bytes(file: &File, offset: u64, length: u64) -> io::Result<u64> {
    physical_run(file, offset, length).map(|run| run.bytes)
}

/// The physically contiguous run from `offset` on, at most `length` bytes.
pub(super) fn physical_run(file: &File, offset: u64, length: u64) -> io::Result<PhysicalRun> {
    use std::os::fd::AsRawFd;

    let mut query = libc::log2phys {
        l2p_flags: 0,
        l2p_contigbytes: libc::off_t::try_from(length).map_err(io::Error::other)?,
        l2p_devoffset: libc::off_t::try_from(offset).map_err(io::Error::other)?,
    };
    // SAFETY: `query` is a live, writable `log2phys` for the duration of the call, which is the
    // argument `F_LOG2PHYS_EXT` reads the file range from and writes the device run into.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_LOG2PHYS_EXT, &raw mut query) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let contiguous = query.l2p_contigbytes;
    let device = query.l2p_devoffset;
    let device = u64::try_from(device).map_err(|_| {
        io::Error::other(format!(
            "F_LOG2PHYS_EXT reported device offset {device} at offset {offset}"
        ))
    })?;
    match u64::try_from(contiguous) {
        Ok(0) | Err(_) => Err(io::Error::other(format!(
            "F_LOG2PHYS_EXT reported {contiguous} contiguous bytes at offset {offset}"
        ))),
        Ok(contiguous) => Ok(PhysicalRun {
            device,
            bytes: contiguous.min(length),
        }),
    }
}

/// Deallocate `[offset, offset + length)`, leaving a hole that reads as zeros.
pub(super) fn punch_hole(file: &File, offset: u64, length: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let mut hole = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: libc::off_t::try_from(offset).map_err(io::Error::other)?,
        fp_length: libc::off_t::try_from(length).map_err(io::Error::other)?,
    };
    // SAFETY: `hole` is a live `fpunchhole_t` for the duration of the call; `F_PUNCHHOLE` reads
    // the range from it and changes nothing but the file's allocation.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &raw mut hole) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The bytes an unprivileged writer may still allocate on the filesystem holding `directory`.
pub(super) fn available_bytes(directory: &Path) -> Result<u64, ApfsStorageError> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(directory.as_os_str().as_bytes()).map_err(|_| {
        ApfsStorageError::Host(format!("path contains NUL: {}", directory.display()))
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` is writable storage `statfs` fills on success.
    if unsafe { libc::statfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io_error(
            "read free space",
            directory,
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `statfs` returned 0, so it initialized every field.
    let stats = unsafe { stats.assume_init() };
    Ok(stats.f_bavail.saturating_mul(u64::from(stats.f_bsize)))
}
