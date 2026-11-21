//! BSD `lockf(1)`: `lockf -k <lock> <command>`, keeping the lock file when it ends.

use std::path::Path;
use std::process::Command;

pub(crate) const NAME: &str = "/usr/bin/lockf";

pub(crate) fn holding(lock: &Path) -> Command {
    let mut command = Command::new(NAME);
    command.arg("-k").arg(lock);
    command
}
