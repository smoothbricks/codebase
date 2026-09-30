//! The kernel vnode table, read where a stalled or failed disk operation needs its cause named.
//!
//! macOS keeps one vnode for every file-system object recently touched, up to `kern.maxvnodes`.
//! Past that limit every lookup, open, mount and unlink first has to recycle a vnode another
//! process holds; when too many are busy to recycle, the table overflows the limit and each
//! allocation waits, or fails with `ENFILE` ("Too many open files in system"). A `mount_apfs`
//! that finishes in a second on a healthy host was measured past a two-minute deadline on one
//! whose table stood at 272631 of 263168. The caller sees a hung or failed disk child; the
//! cause is a host limit only the operator can raise, so cowshed reads the table where such a
//! failure is reported and names the limit instead of blaming the executable or the image.

use std::fmt;
use std::io;

/// The table's occupancy as the kernel reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VnodeTable {
    /// `kern.num_vnodes`.
    pub in_use: u64,
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

    /// Every vnode the limit allows is in use, so each new one waits for a recycle.
    pub fn saturated(self) -> bool {
        self.in_use >= self.limit
    }

    /// The operator's remedy: twice the larger of the limit and the current occupancy, which
    /// clears the overflow with the same headroom the limit had.
    pub fn remedy(self) -> String {
        format!(
            "raise the kernel vnode limit (sudo sysctl kern.maxvnodes={}), then retry",
            self.limit.max(self.in_use).saturating_mul(2)
        )
    }
}

impl fmt::Display for VnodeTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the kernel vnode table is saturated (kern.num_vnodes {} of kern.maxvnodes {}), so every \
             file-system call waits to recycle a vnode",
            self.in_use, self.limit
        )
    }
}

#[cfg(target_os = "macos")]
fn read_table() -> io::Result<VnodeTable> {
    Ok(VnodeTable {
        in_use: sysctl_integer(c"kern.num_vnodes")?,
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

    #[test]
    fn saturation_starts_at_the_limit() {
        let below = VnodeTable {
            in_use: 263_167,
            limit: 263_168,
        };
        assert!(!below.saturated());
        for in_use in [263_168, 272_631] {
            assert!(
                VnodeTable {
                    in_use,
                    limit: 263_168
                }
                .saturated()
            );
        }
    }

    #[test]
    fn the_remedy_clears_an_overflow_with_the_limit_headroom() {
        let overflowing = VnodeTable {
            in_use: 272_631,
            limit: 263_168,
        };
        assert!(overflowing.remedy().contains("kern.maxvnodes=545262"));
        let at_limit = VnodeTable {
            in_use: 100,
            limit: 100,
        };
        assert!(at_limit.remedy().contains("kern.maxvnodes=200"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn the_live_table_is_readable_and_nonempty() {
        let table = VnodeTable::read().expect("kern.num_vnodes and kern.maxvnodes");
        assert!(table.in_use > 0 && table.limit > 0, "{table:?}");
    }
}
