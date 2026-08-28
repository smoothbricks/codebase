use std::io;
use std::os::unix::process::CommandExt;
use std::process::Command;

use super::{
    DESCRIPTOR_PREPARATION_ERRNO, SpawnFailure, WrapperStage, descriptor_limit,
    mark_descriptor_close_on_exec, mark_descriptor_range_close_on_exec_with,
};

pub(super) fn mark_non_stdio_close_on_exec(limit: libc::rlim_t) -> io::Result<()> {
    mark_descriptor_range_close_on_exec_with(limit, mark_descriptor_close_on_exec)
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
