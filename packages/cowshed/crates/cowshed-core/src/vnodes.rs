//! The kernel vnode table, read where a stalled or failed disk operation needs its cause named.
//!
//! macOS keeps one vnode for every file-system object recently touched and caches them up to
//! `kern.maxvnodes`: on a busy host `kern.num_vnodes` sits at the limit as a matter of course,
//! with most of them on the free list (`kern.free_vnodes`), ready to be recycled for the next
//! lookup. That full cache is healthy. The table is saturated only when the vnodes in use — the
//! allocated ones that are not free — reach the limit: then nothing can be recycled, the kernel
//! allocates past the limit, and every lookup, open, mount and unlink waits, or fails with
//! `ENFILE` ("Too many open files in system"). A `mount_apfs` that finishes in a second on a
//! healthy host was measured past a two-minute deadline on one whose table had overshot to 272631
//! of 263168. The caller sees a hung or failed disk child; the cause is a host limit only the
//! operator can raise, so cowshed reads the table where such a failure is reported and names the
//! limit instead of blaming the executable or the image.

use std::fmt;
use std::io;

/// The table's occupancy as the kernel reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VnodeTable {
    /// `kern.num_vnodes`: every vnode allocated, in use or cached free.
    pub allocated: u64,
    /// `kern.free_vnodes`: allocated vnodes on the free list, recyclable at once.
    pub free: u64,
    /// `kern.maxvnodes`.
    pub limit: u64,
}

impl VnodeTable {
    /// The table now. Unsupported where the kernel has no such table (Linux).
    pub fn read() -> io::Result<Self> {
        read_table()
    }

    /// The table now, only when it is saturated: the evidence a failure report attaches. A host
    /// whose table cannot be read attaches nothing rather than a guess.
    pub fn saturation() -> Option<Self> {
        Self::read().ok().filter(|table| table.saturated())
    }

    /// The vnodes held open rather than cached free.
    pub fn in_use(self) -> u64 {
        self.allocated.saturating_sub(self.free)
    }

    /// Every vnode the limit allows is in use, so each new one waits for a recycle that has
    /// nothing to take. A cache full of free vnodes is not this.
    pub fn saturated(self) -> bool {
        self.in_use() >= self.limit
    }

    /// The operator's remedy: twice the larger of the limit and the vnodes in use, which clears
    /// the overflow with the same headroom the limit had.
    pub fn remedy(self) -> String {
        format!(
            "raise the kernel vnode limit (sudo sysctl kern.maxvnodes={}), then retry",
            self.limit.max(self.in_use()).saturating_mul(2)
        )
    }
}

impl fmt::Display for VnodeTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the kernel vnode table is saturated ({} vnodes in use of kern.maxvnodes {}; \
             kern.num_vnodes {}, kern.free_vnodes {}), so every file-system call waits to \
             recycle a vnode",
            self.in_use(),
            self.limit,
            self.allocated,
            self.free
        )
    }
}

#[cfg(target_os = "macos")]
fn read_table() -> io::Result<VnodeTable> {
    Ok(VnodeTable {
        allocated: sysctl_integer(c"kern.num_vnodes")?,
        free: sysctl_integer(c"kern.free_vnodes")?,
        limit: sysctl_integer(c"kern.maxvnodes")?,
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

    fn table(allocated: u64, free: u64) -> VnodeTable {
        VnodeTable {
            allocated,
            free,
            limit: 263_168,
        }
    }

    #[test]
    fn a_full_cache_of_free_vnodes_is_not_saturation() {
        // The steady state of a busy host: the cache sits at (or just past) the limit, and
        // most of it is free to recycle.
        assert!(!table(263_229, 180_000).saturated());
        assert!(!table(263_168, 1).saturated());
    }

    #[test]
    fn saturation_is_every_allowed_vnode_in_use() {
        assert!(table(263_168, 0).saturated());
        // Overshooting the limit is what the kernel does when nothing is free to recycle.
        assert!(table(272_631, 0).saturated());
        assert!(table(272_631, 9_463).saturated());
        assert!(!table(272_631, 9_464).saturated());
    }

    #[test]
    fn the_remedy_clears_an_overflow_with_the_limit_headroom() {
        assert!(table(272_631, 0).remedy().contains("kern.maxvnodes=545262"));
        assert!(table(263_168, 0).remedy().contains("kern.maxvnodes=526336"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn the_live_table_is_readable() {
        let table = VnodeTable::read().expect("kern.num_vnodes, kern.free_vnodes, kern.maxvnodes");
        assert!(table.allocated > 0 && table.limit > 0, "{table:?}");
    }
}
