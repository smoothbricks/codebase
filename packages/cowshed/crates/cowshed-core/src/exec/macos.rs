use std::io;
use std::os::unix::process::CommandExt;
use std::process::Command;

use super::{DESCRIPTOR_PREPARATION_ERRNO, SUPERVISOR_FD_CEILING, SpawnFailure};

pub(super) fn validate_fd_listing_size(bytes: libc::c_int, capacity: usize) -> io::Result<usize> {
    if bytes < 0 {
        return Err(io::Error::last_os_error());
    }
    let bytes = bytes as usize;
    if bytes > capacity || !bytes.is_multiple_of(std::mem::size_of::<libc::proc_fdinfo>()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "open descriptor listing exceeds the supervisor FD ceiling",
        ));
    }
    Ok(bytes / std::mem::size_of::<libc::proc_fdinfo>())
}

pub(super) fn mark_macos_non_stdio_close_on_exec(
    descriptors: &mut [std::mem::MaybeUninit<libc::proc_fdinfo>],
) -> io::Result<()> {
    let capacity = std::mem::size_of_val(descriptors);
    let required = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDLISTFDS,
            0,
            std::ptr::null_mut(),
            0,
        )
    };
    validate_fd_listing_size(required, capacity)?;
    let bytes = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDLISTFDS,
            0,
            descriptors.as_mut_ptr().cast(),
            capacity as libc::c_int,
        )
    };
    let count = validate_fd_listing_size(bytes, capacity)?;
    for descriptor in &descriptors[..count] {
        let descriptor = unsafe { descriptor.assume_init_ref() }.proc_fd;
        if descriptor > libc::STDERR_FILENO {
            super::mark_descriptor_close_on_exec(descriptor)?;
        }
    }
    Ok(())
}

pub(super) fn prepare_child_descriptors(command: &mut Command) -> Result<(), SpawnFailure> {
    let mut descriptors = Box::<[libc::proc_fdinfo]>::new_uninit_slice(SUPERVISOR_FD_CEILING);

    unsafe {
        command.pre_exec(move || {
            mark_macos_non_stdio_close_on_exec(&mut descriptors)
                .map_err(|_| io::Error::from_raw_os_error(DESCRIPTOR_PREPARATION_ERRNO))
        });
    }
    Ok(())
}
