//! The host's disk-image device classes, observed (01a_apfs.md, "How the APFS host degrades" §2;
//! 08_testing.md, "Host limit").
//!
//! Every DiskImages2 attach (`diskutil image attach`) creates an `AppleDiskImageDevice`, its
//! `DIDeviceIOUserClient` and its request pool. Most leave the kernel with their detach; some stay
//! alive after they have left the registry: still counted among their class's instances, no
//! longer registered. Once that pool runs out, every DiskImages2 attach, `diskutil image info`
//! and cold mount fails with "error code 150" (`kIOReturnNoResources`). A `diskimagesiod` helper
//! stalled past the IOMedia match-notification ceiling keeps every device that terminates
//! afterwards (01a_apfs.md, "The IOMedia match-notification ceiling"); its death frees them, as
//! killing orphaned helpers frees their mappings ([`crate::disk_image_helpers`]).
//!
//! Cowshed attaches every image in the kernel (`hdiutil attach -kernel`), retained re-adoption
//! inputs included, and creates its blank images as plain sparse files, so it runs no DiskImages2
//! verb of its own and nothing here gates, records or latches anything. Another tool's attach, or
//! the user's own `cowshed disk-command diskutil -- image attach …` passthrough, can still leak a
//! device; `doctor` reports the leaked devices as a warning, and the real-APFS lane checks that
//! their count does not grow across a run.
//!
//! Both sides are read unprivileged and without attaching anything: the instance counts are the
//! registry root's `IOKitDiagnostics` class counts, which `ioclasscount` prints, and the
//! registered counts are the registry's own entries. A device instance with no registered entry
//! is leaked. The same subtraction for `AppleAPFSVolume` and `IOMedia` counts departed volumes
//! and media the kernel has not freed ([`Departed`]). They take no attach-pool slot: `doctor`
//! reports them as a finding of their own, and what holds them is attributed by registry ID, not
//! assumed (01a_apfs.md, "Spotlight media references").

use std::fmt;
use std::io;

use serde::{Deserialize, Serialize};

/// What frees leaked image devices.
pub const LEAKED_DEVICES_HINT: &str = "a diskimagesiod whose attach never returned keeps every \
     image device terminated after it: killing that one helper (sudo kill -9 <pid>) frees them; \
     reboot (sudo shutdown -r now) when no helper is stalled or the devices stay";

/// What attributes departed volumes and media to the process that holds them. It reads and
/// changes nothing and names no restart: the holder has differed between builds, classes and
/// boots, so anything done about a holder starts from its exact pid, re-read as still holding
/// the IDs (01a_apfs.md, "Spotlight media references", "Release and device custody").
pub const DEPARTED_ATTRIBUTION_HINT: &str = "attribute the departed volumes and media by exact \
     registry ID before acting; this changes nothing. As root, count per process the rights in \
     `lsmp -a` (`IOKIT-OBJECT AppleAPFSVolume(0x...)`, `IOKIT-OBJECT IOMedia(0x...)`) whose ID \
     is absent from `ioreg -r -c IOMedia -d 1 -w0` (`id 0x...`, AppleAPFSVolume entries \
     included). A count matching the finding names the holder's exact pid, to be re-read as \
     still holding those IDs before it is touched; IDs no process holds are held in the kernel";

/// One boot of the host. `kern.bootsessionuuid` is the key: `kern.boottime` moves within a boot
/// whenever the wall clock is stepped, and is kept only to say when the boot began.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Boot {
    pub session: String,
    pub booted_at: i64,
}

/// One class's live kernel instances against the registry entries that account for them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassCount {
    pub instances: u64,
    pub registered: u64,
}

impl ClassCount {
    /// Instances no registered entry accounts for.
    pub fn leaked(self) -> u64 {
        self.instances.saturating_sub(self.registered)
    }
}

impl fmt::Display for ClassCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} instances, {} registered, {} leaked",
            self.instances,
            self.registered,
            self.leaked()
        )
    }
}

/// The four classes at one instant. The instances and the registry are two reads, so each
/// count can be off by the disk-image commands in flight between them, at most the host's
/// storage phase cap (05_gateway.md, "Disk-lifecycle lease").
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KernelReading {
    pub image_devices: ClassCount,
    pub user_clients: ClassCount,
    pub apfs_volumes: ClassCount,
    pub media: ClassCount,
}

impl KernelReading {
    /// The departed volumes and media the kernel has not freed, from the first of either, or
    /// `None` when no `AppleAPFSVolume` or `IOMedia` instance outlives its registry entry.
    pub fn departed(&self) -> Option<Departed> {
        let departed = Departed {
            volumes: self.apfs_volumes.leaked(),
            media: self.media.leaked(),
        };
        (departed.volumes > 0 || departed.media > 0).then_some(departed)
    }

    /// Whether `doctor` should warn: an image device the kernel still holds that no registered
    /// entry accounts for.
    pub fn is_leaking(&self) -> bool {
        self.image_devices.leaked() > 0
    }

    /// What frees the leaked image devices, if any are leaked. Departed volumes and media take no
    /// attach-pool slot, so they name nothing here.
    pub fn remedy(&self) -> &'static str {
        if self.is_leaking() {
            LEAKED_DEVICES_HINT
        } else {
            "nothing to free"
        }
    }
}

impl fmt::Display for KernelReading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AppleDiskImageDevice {}; DIDeviceIOUserClient {}; AppleAPFSVolume {}; IOMedia {}",
            self.image_devices, self.user_clients, self.apfs_volumes, self.media,
        )
    }
}

/// Departed APFS volumes and media the kernel has not freed: instances no registered entry
/// accounts for. They take no attach-pool slot, so they are no part of the pool
/// (01a_apfs.md, "Spotlight media references"). The instances and the registry are two reads
/// ([`KernelReading`]), so a disk command in flight between them can account for a few.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Departed {
    pub volumes: u64,
    pub media: u64,
}

impl fmt::Display for Departed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} departed AppleAPFSVolume and {} departed IOMedia instances: live in the kernel, \
             registered nowhere. They take no attach-pool slot. Nothing here says what holds \
             them, and a disk command in flight between doctor's registry read and its instance \
             read can account for a few",
            self.volumes, self.media
        )
    }
}

/// The current boot.
#[cfg(target_os = "macos")]
pub fn boot() -> io::Result<Boot> {
    let mut buffer = [0u8; 64];
    let mut length = buffer.len();
    // SAFETY: the name is NUL-terminated, `buffer` and `length` describe writable storage of
    // exactly that size, and nothing is written to the kernel.
    let status = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            buffer.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    let session = std::ffi::CStr::from_bytes_until_nul(&buffer[..length.min(buffer.len())])
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "kern.bootsessionuuid is not a C string",
            )
        })?
        .to_str()
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "kern.bootsessionuuid is not UTF-8",
            )
        })?
        .to_owned();
    Ok(Boot {
        session,
        booted_at: booted_at()?,
    })
}

#[cfg(target_os = "macos")]
fn booted_at() -> io::Result<i64> {
    let mut mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];
    let mut value = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    let mut size = std::mem::size_of::<libc::timeval>();
    // SAFETY: `mib` names kern.boottime, whose value is one `timeval`, and `value` and `size`
    // describe writable storage of exactly that size; nothing is written to the kernel.
    let status = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&raw mut value).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value.tv_sec)
}

#[cfg(not(target_os = "macos"))]
pub fn boot() -> io::Result<Boot> {
    Err(unsupported())
}

/// The four classes now.
#[cfg(target_os = "macos")]
pub fn read_kernel() -> io::Result<KernelReading> {
    crate::disk_image_class_counts()
}

#[cfg(not(target_os = "macos"))]
pub fn read_kernel() -> io::Result<KernelReading> {
    Err(unsupported())
}

#[cfg(not(target_os = "macos"))]
fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "this kernel has no disk-image device classes",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(instances: u64, registered: u64) -> ClassCount {
        ClassCount {
            instances,
            registered,
        }
    }

    /// This host at its first 150 on 2026-10-10, with diskarbitrationd still holding the
    /// departed volumes.
    fn spent_host() -> KernelReading {
        KernelReading {
            image_devices: count(1090, 66),
            user_clients: count(1077, 66),
            apfs_volumes: count(1065, 81),
            media: count(1075, 80),
        }
    }

    /// This host just after its reboot: 178 images attached, nothing leaked.
    fn rebooted_host() -> KernelReading {
        KernelReading {
            image_devices: count(178, 178),
            user_clients: count(178, 178),
            apfs_volumes: count(193, 193),
            media: count(185, 185),
        }
    }

    fn leaking(leaked: u64) -> KernelReading {
        KernelReading {
            image_devices: count(10 + leaked, 10),
            ..rebooted_host()
        }
    }

    #[test]
    fn leaked_is_instances_no_registered_entry_accounts_for() {
        assert_eq!(spent_host().image_devices.leaked(), 1024);
        assert_eq!(
            spent_host().departed(),
            Some(Departed {
                volumes: 984,
                media: 995
            })
        );
        assert_eq!(count(3, 5).leaked(), 0);
        assert_eq!(rebooted_host().departed(), None);
    }

    /// The counts are named departed and no holder is: what holds them differs between builds,
    /// classes and boots, so the hint attributes by registry ID and kills or restarts nothing.
    #[test]
    fn the_departed_message_names_no_holder_and_its_hint_changes_nothing() {
        let message = Departed {
            volumes: 3,
            media: 0,
        }
        .to_string();
        assert!(
            message.starts_with("3 departed AppleAPFSVolume and 0 departed IOMedia instances: "),
            "{message}"
        );
        assert!(message.contains("take no attach-pool slot"), "{message}");
        for holder in ["mds", "Spotlight", "diskarbitrationd"] {
            assert!(!message.contains(holder), "{holder} in {message}");
        }
        assert!(DEPARTED_ATTRIBUTION_HINT.contains("`lsmp -a`"));
        assert!(DEPARTED_ATTRIBUTION_HINT.contains("registry ID"));
        assert!(DEPARTED_ATTRIBUTION_HINT.contains("exact pid"));
        for change in ["kill", "restart", "reboot", "diskarbitrationd"] {
            assert!(
                !DEPARTED_ATTRIBUTION_HINT.contains(change),
                "{change} in the hint"
            );
        }
    }

    /// Leaked image devices warn and name what frees them; a healthy host has nothing to free.
    #[test]
    fn a_leaked_device_warns_and_a_healthy_host_frees_nothing() {
        let healthy = rebooted_host();
        assert!(!healthy.is_leaking());
        assert_eq!(healthy.remedy(), "nothing to free");
        assert!(
            healthy
                .to_string()
                .contains("AppleDiskImageDevice 178 instances, 178 registered, 0 leaked"),
            "{healthy}"
        );
        assert!(leaking(1).is_leaking());
        assert_eq!(leaking(1).remedy(), LEAKED_DEVICES_HINT);
    }

    /// Departed volumes and media take no slot in the pool: with or without them the warning
    /// and the remedy read alike, and the first departed instance of either class is reported.
    #[test]
    fn departed_volumes_never_spend_the_pool_and_the_first_is_reported() {
        let departed = |reading: KernelReading| KernelReading {
            apfs_volumes: count(1065, 81),
            media: count(1075, 80),
            ..reading
        };
        for leaked in [0, 700, 1024] {
            let without = leaking(leaked);
            let with = departed(leaking(leaked));
            assert_eq!(with.is_leaking(), without.is_leaking(), "{leaked} leaked");
            assert_eq!(with.remedy(), without.remedy(), "{leaked} leaked");
        }
        let first_volume = KernelReading {
            apfs_volumes: count(194, 193),
            ..rebooted_host()
        };
        assert_eq!(
            first_volume.departed(),
            Some(Departed {
                volumes: 1,
                media: 0
            })
        );
        let first_media = KernelReading {
            media: count(186, 185),
            ..rebooted_host()
        };
        assert_eq!(
            first_media.departed(),
            Some(Departed {
                volumes: 0,
                media: 1
            })
        );
        for first in [first_volume, first_media] {
            assert!(!first.is_leaking());
            assert_eq!(first.remedy(), "nothing to free");
        }
        let freed = KernelReading {
            apfs_volumes: count(192, 193),
            media: count(184, 185),
            ..rebooted_host()
        };
        assert_eq!(freed.departed(), None);
        assert_eq!(rebooted_host().departed(), None);
    }
}
