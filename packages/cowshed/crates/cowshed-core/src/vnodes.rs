//! Reading the kernel vnode table: the sysctl shell behind the values in
//! cowshed_types::vnodes, read where a stalled or failed disk operation needs its
//! cause named.

use std::io;

use cowshed_types::vnodes::VnodeTable;

/// The table now. Unsupported where the kernel has no such table (Linux).
pub fn read_vnode_table() -> io::Result<VnodeTable> {
    read_table()
}

/// The table now, only when it is saturated: the evidence a failure report attaches. A host
/// whose table cannot be read attaches nothing rather than a guess.
pub fn vnode_saturation() -> Option<VnodeTable> {
    read_vnode_table().ok().filter(|table| table.saturated())
}

#[cfg(target_os = "macos")]
fn read_table() -> io::Result<VnodeTable> {
    Ok(VnodeTable {
        allocated: sysctl_integer(c"kern.num_vnodes")?,
        free: sysctl_integer(c"kern.free_vnodes")?,
        limit: sysctl_integer(c"kern.maxvnodes")?,
        memory: sysctl_integer(c"hw.memsize")?,
    })
}

#[cfg(not(target_os = "macos"))]
fn read_table() -> io::Result<VnodeTable> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this kernel has no vnode table limit to read",
    ))
}

/// One integer sysctl, whichever width the kernel publishes it at.
#[cfg(target_os = "macos")]
fn sysctl_integer(name: &std::ffi::CStr) -> io::Result<u64> {
    let mut value = [0_u8; 8];
    let mut size = value.len();
    // SAFETY: `name` is NUL-terminated; `value` is writable for `size` bytes and `size` is
    // updated in place to the width the kernel wrote. No new value is set.
    let status = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    match size {
        4 => Ok(u64::from(u32::from_ne_bytes([
            value[0], value[1], value[2], value[3],
        ]))),
        8 => Ok(u64::from_ne_bytes(value)),
        width => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("sysctl {name:?} answered {width} bytes, not an integer"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn the_live_table_is_readable() {
        let table = read_vnode_table()
            .expect("kern.num_vnodes, kern.free_vnodes, kern.maxvnodes, hw.memsize");
        assert!(
            table.allocated > 0 && table.limit > 0 && table.memory > 0,
            "{table:?}"
        );
    }
}
