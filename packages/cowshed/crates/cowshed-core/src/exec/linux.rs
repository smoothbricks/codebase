use std::io;
use std::os::unix::process::CommandExt;
use std::process::Command;

use super::{
    DESCRIPTOR_PREPARATION_ERRNO, SpawnFailure, WrapperStage, close_range_result_with,
    descriptor_limit, mark_descriptor_close_on_exec, mark_non_stdio_close_on_exec_with,
};

pub(super) fn mark_non_stdio_close_on_exec(limit: libc::rlim_t) -> io::Result<()> {
    mark_non_stdio_close_on_exec_with(
        limit,
        |first, last, flags| {
            let result = unsafe { libc::syscall(libc::SYS_close_range, first, last, flags) };
            close_range_result_with(result, io::Error::last_os_error)
        },
        mark_descriptor_close_on_exec,
    )
}

pub(super) fn prepare_child_descriptors(command: &mut Command) -> Result<(), SpawnFailure> {
    let descriptor_limit = descriptor_limit().map_err(|source| SpawnFailure {
        stage: WrapperStage::PrepareChildDescriptors,
        source,
    })?;

    unsafe {
        command.pre_exec(move || {
            mark_non_stdio_close_on_exec(descriptor_limit)
                .map_err(|_| io::Error::from_raw_os_error(DESCRIPTOR_PREPARATION_ERRNO))
        });
    }
    Ok(())
}
