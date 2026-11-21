//! Linux records no unique id in a lease: its census asks every process (`procfs_census`), and a
//! lock's custody is read from the kernel's own lock state.

use std::io;

/// No unique id is recorded for `pid`.
pub(super) fn unique_id(_pid: u32) -> io::Result<Option<crate::api::held_lock::DecimalU64>> {
    Ok(None)
}
