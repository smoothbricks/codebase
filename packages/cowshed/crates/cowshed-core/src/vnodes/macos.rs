//! The vnode table as XNU publishes it: `kern.num_vnodes`, `kern.free_vnodes`, `kern.maxvnodes`
//! and `hw.memsize`, read by `sysctlbyname`.

use std::io;

use super::VnodeTable;

pub(super) fn read_table() -> io::Result<VnodeTable> {
    Ok(VnodeTable {
        allocated: sysctl_integer(c"kern.num_vnodes")?,
        free: sysctl_integer(c"kern.free_vnodes")?,
        limit: sysctl_integer(c"kern.maxvnodes")?,
        memory: sysctl_integer(c"hw.memsize")?,
    })
}

/// One integer sysctl, whichever width the kernel publishes it at.
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
    use super::super::VnodeTable;

    #[test]
    fn the_live_table_is_readable() {
        let table = VnodeTable::read()
            .expect("kern.num_vnodes, kern.free_vnodes, kern.maxvnodes, hw.memsize");
        assert!(
            table.allocated > 0 && table.limit > 0 && table.memory > 0,
            "{table:?}"
        );
    }
}
