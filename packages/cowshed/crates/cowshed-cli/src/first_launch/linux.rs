//! Linux assesses no executable at its first launch: nothing scans a new program and no process
//! is charged for one. So there is nothing to measure, here or in the gateway daemon, no probe
//! is minted or signed, and no responsible process to name.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cowshed_gateway_types::{FirstLaunch, FirstLaunchProbe};

/// Nothing measures a first launch in the calling process.
pub fn host_probe() -> Option<fn(&Path) -> io::Result<FirstLaunch>> {
    None
}

/// The gateway daemon serves no first-launch probe.
pub fn daemon_probe(_template: &Path) -> Option<Arc<dyn FirstLaunchProbe>> {
    None
}

/// No process answers for this one's launches.
pub fn responsible_program() -> Option<PathBuf> {
    None
}
