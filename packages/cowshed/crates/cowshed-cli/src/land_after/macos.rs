//! How one after-land command runs to its log on Darwin: its members are its own process group,
//! which leaves nothing to retire, so `log` names nothing here.

use std::fs::File;
use std::path::Path;
use std::process::Command;

use super::{Executed, SILENCE, ran};

pub(super) fn run_to_log(
    role: &'static str,
    mut command: Command,
    _log: &Path,
    output: File,
) -> Executed {
    ran(cowshed_os::deadline::run_to_log(
        role,
        &mut command,
        cowshed_os::deadline::Members::OwnGroup,
        output,
        SILENCE,
    )
    .map_err(|error| error.to_string()))
}
