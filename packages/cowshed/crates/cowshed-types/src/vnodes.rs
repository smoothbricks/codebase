//! The kernel vnode table's occupancy and what it costs the host, as values: read by a
//! shell where a stalled or failed disk operation needs its cause named, and judged
//! here against the limit this host's memory calls for (specs/cowshed/01a_apfs.md,
//! "The vnode table"). The table is saturated when the vnodes in use — the allocated
//! ones that are not free — reach the limit: then nothing can be recycled, the kernel
//! allocates past the limit, and every lookup, open, mount and unlink waits, or fails
//! with ENFILE. The limit is the operator's to raise, so cowshed names it and the
//! value to set instead of blaming the executable or the image.

use std::fmt;
/// The memory each vnode of the limit's floor stands for: one vnode per 32 KiB, so a full cache
/// of regular files, about 1.2 KB of kernel zones each (vnode, APFS inode, name-cache entry, UBC
/// info, pager and VM object), costs under 4% of memory.
pub const MEMORY_PER_VNODE: u64 = 32 * 1024;

/// The table's occupancy as the kernel reports it, and the memory its limit is judged against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VnodeTable {
    /// `kern.num_vnodes`: every vnode allocated, in use or cached free.
    pub allocated: u64,
    /// `kern.free_vnodes`: allocated vnodes on the free list, recyclable at once.
    pub free: u64,
    /// `kern.maxvnodes`.
    pub limit: u64,
    /// `hw.memsize`: the host's memory in bytes.
    pub memory: u64,
}

/// What the table costs the host now, worst first.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pressure {
    /// Every vnode the limit allows is in use: each new one waits for a recycle with nothing to
    /// take, and file-system calls stall.
    Saturated,
    /// The table is at a limit under the floor with free vnodes left: each new vnode reclaims a
    /// cached one and drops that file's cached pages while memory could still hold them.
    Full,
    /// The table has room, but its limit is under [`VnodeTable::floor`].
    BelowFloor,
    /// Room, or full at a limit at or over the floor: a full cache, which every busy host reaches.
    Sound,
}

impl VnodeTable {
    /// The vnodes held open rather than cached free.
    pub fn in_use(self) -> u64 {
        self.allocated.saturating_sub(self.free)
    }

    /// Every vnode the limit allows is in use, so each new one waits for a recycle that has
    /// nothing to take. A cache full of free vnodes is not this.
    pub fn saturated(self) -> bool {
        self.in_use() >= self.limit
    }

    /// The lowest limit this host runs with: one vnode per [`MEMORY_PER_VNODE`] bytes of memory.
    pub fn floor(self) -> u64 {
        self.memory / MEMORY_PER_VNODE
    }

    /// The worst state the table is in now.
    pub fn pressure(self) -> Pressure {
        if self.saturated() {
            Pressure::Saturated
        } else if self.limit >= self.floor() {
            Pressure::Sound
        } else if self.allocated >= self.limit {
            Pressure::Full
        } else {
            Pressure::BelowFloor
        }
    }

    /// The limit to set: a saturated table gets twice the larger of the limit and the vnodes in
    /// use, the headroom the limit had; no limit is under the floor.
    pub fn recommended_limit(self) -> u64 {
        let grown = if self.saturated() {
            self.limit.max(self.in_use()).saturating_mul(2)
        } else {
            self.limit
        };
        grown.max(self.floor())
    }

    /// The operator's remedy: the sysctl that raises the limit now, and the boot-time job that
    /// keeps it, since the kernel boots with its default.
    pub fn remedy(self) -> String {
        let limit = self.recommended_limit();
        format!(
            "raise the kernel vnode limit now (sudo sysctl -w kern.maxvnodes={limit}) and at every \
             boot (nix-darwin: a launchd.daemons job with RunAtLoad running \
             /usr/sbin/sysctl -w kern.maxvnodes={limit})"
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

#[cfg(test)]
mod tests {
    use super::*;
    fn table(allocated: u64, free: u64) -> VnodeTable {
        VnodeTable {
            allocated,
            free,
            limit: 263_168,
            memory: 16 << 30,
        }
    }

    /// The fleet host: 128 GiB.
    fn host(allocated: u64, free: u64, limit: u64) -> VnodeTable {
        VnodeTable {
            allocated,
            free,
            limit,
            memory: 128 << 30,
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
        assert!(table(263_168, 0).remedy().contains("launchd.daemons"));
    }

    #[test]
    fn the_floor_is_one_vnode_per_32_kib_of_memory() {
        assert_eq!(host(0, 0, 0).floor(), 4_194_304);
        assert_eq!(table(0, 0).floor(), 524_288);
    }

    #[test]
    fn pressure_names_the_worst_state_first() {
        let cases = [
            ((1_048_649, 0, 1_048_576), Pressure::Saturated),
            // Saturated at the floor still stalls: saturation is judged before the floor.
            ((4_194_304, 0, 4_194_304), Pressure::Saturated),
            // The fleet at a 1 Mi limit: full, mostly free.
            ((1_048_576, 899_560, 1_048_576), Pressure::Full),
            // The fleet full at the floor: a full cache, not a finding.
            ((4_194_304, 3_000_000, 4_194_304), Pressure::Sound),
            // The boot default with room left.
            ((200_000, 150_000, 263_168), Pressure::BelowFloor),
            // The fleet at the floor.
            ((2_036_323, 1_594_926, 4_194_304), Pressure::Sound),
        ];
        for ((allocated, free, limit), pressure) in cases {
            assert_eq!(
                host(allocated, free, limit).pressure(),
                pressure,
                "{allocated} allocated, {free} free, limit {limit}"
            );
        }
    }

    #[test]
    fn the_recommended_limit_is_never_under_the_floor() {
        // A saturated table doubles, even at the floor.
        assert_eq!(host(4_194_304, 0, 4_194_304).recommended_limit(), 8_388_608);
        // A full table under the floor is not doubled; the remedy is the floor.
        assert_eq!(
            host(1_048_576, 899_560, 1_048_576).recommended_limit(),
            4_194_304
        );
        assert_eq!(
            host(200_000, 150_000, 263_168).recommended_limit(),
            4_194_304
        );
        // A full table at the floor keeps its limit, as a sound one does.
        assert_eq!(
            host(4_194_304, 3_000_000, 4_194_304).recommended_limit(),
            4_194_304
        );
        assert_eq!(
            host(2_036_323, 1_594_926, 4_194_304).recommended_limit(),
            4_194_304
        );
    }

    #[test]
    fn the_edges_stay_total() {
        // No limit: every table is saturated, and the remedy is the floor.
        assert_eq!(host(0, 0, 0).pressure(), Pressure::Saturated);
        assert_eq!(host(0, 0, 0).recommended_limit(), 4_194_304);
        // No memory reading of size: no floor, so only the table's own occupancy counts.
        let unmeasured = VnodeTable {
            memory: 0,
            ..host(200_000, 150_000, 263_168)
        };
        assert_eq!(unmeasured.pressure(), Pressure::Sound);
        assert_eq!(unmeasured.recommended_limit(), 263_168);
        // Doubling saturates instead of wrapping.
        assert_eq!(host(u64::MAX, 0, u64::MAX).recommended_limit(), u64::MAX);
    }
}
