use std::io;
use std::process::Command;

use super::{
    SpawnFailure, WrapperStage, close_range_result_with, descriptor_limit,
    install_cloexec_pre_exec, mark_descriptor_close_on_exec, mark_non_stdio_close_on_exec_with,
};

pub(super) fn mark_non_stdio_close_on_exec(limit: libc::rlim_t) -> io::Result<()> {
    mark_non_stdio_close_on_exec_with(
        limit,
        |first, last, flags| {
            // SAFETY: close_range takes integer bounds and flags only; invalid values return errno.
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

    install_cloexec_pre_exec(command, descriptor_limit, mark_non_stdio_close_on_exec);
    Ok(())
}
