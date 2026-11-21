//! util-linux `flock(1)`: `flock <lock> <command>`, which never removes the lock file.

use std::path::Path;
use std::process::Command;

pub(crate) const NAME: &str = "flock";

pub(crate) fn holding(lock: &Path) -> Command {
    let mut command = Command::new(NAME);
    command.arg(lock);
    command
}
