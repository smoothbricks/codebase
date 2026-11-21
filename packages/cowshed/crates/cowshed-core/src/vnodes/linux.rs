//! Linux has no vnode table limit: its inode and dentry caches are bounded by memory and
//! reclaimed under memory pressure, so no reading exists to take.

use std::io;

use super::VnodeTable;

pub(super) fn read_table() -> io::Result<VnodeTable> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this kernel has no vnode table limit to read",
    ))
}
