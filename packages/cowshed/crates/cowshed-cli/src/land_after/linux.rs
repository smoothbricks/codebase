//! How one after-land command runs to its log on Linux: its members are a cgroup v2 leaf of its
//! own inside this after-land job's cgroup, named for its log and retired once it ends, so nothing
//! an earlier command left behind is counted as this one's.

use std::fs::File;
use std::path::Path;
use std::process::Command;

use super::{Executed, SILENCE, ran};

pub(super) fn run_to_log(
    role: &'static str,
    mut command: Command,
    log: &Path,
    output: File,
) -> Executed {
    let own = match cowshed_os::process::own_cgroup() {
        Ok(own) => own,
        Err(error) => return ran(Err(error.to_string())),
    };
    let name = format!(
        "after-{}",
        log.file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned())
    );
    let leaf = match cowshed_core::runtime::job_cgroup::RunCgroup::inside(&own, &name) {
        Ok(leaf) => leaf,
        Err(error) => return ran(Err(error.to_string())),
    };
    let outcome = cowshed_os::deadline::run_to_log(
        role,
        &mut command,
        cowshed_os::deadline::Members::Leaf {
            cgroup: leaf.directory(),
            path: leaf.path(),
        },
        output,
        SILENCE,
    )
    .map_err(|error| error.to_string());
    Executed {
        ran: outcome,
        unretired: leaf
            .retire(cowshed_os::deadline::REAP)
            .err()
            .map(|error| error.to_string()),
    }
}
