//! `diskimagesiod` helpers that outlived their disk image (01_storage.md, "How the APFS host
//! degrades" §2).
//!
//! Every attached disk image has its own `diskimagesiod` (launchd job
//! `system/com.apple.diskimagesiod.<UUID>`), which maps its IO request pool into the kernel: 36
//! shared buffers of 2 MiB each, 72 MiB per helper. The helper opens the image's
//! `AppleDiskImageDevice` through a `DIDeviceIOUserClient`, and the kernel records who opened it
//! as the client's `IOUserClientCreator` (`pid N, diskimagesiod`). Detach does not always end the
//! helper: an orphan keeps running, and keeps its mapping, with no device behind it. Once enough
//! accumulate the kernel cannot map a new pool and every attach fails with error code 150
//! (`kIOReturnNoResources`). `launchctl kill` of an orphan's job is refused even to root;
//! `sudo kill -9 <pid>` frees its mapping, and the next attach succeeds with no reboot.
//!
//! An orphan is therefore a running helper whose pid created no attached device's user client.
//! Both sides are read unprivileged: the process table through libproc (`proc_listallpids`,
//! `proc_pidpath`), which names root's processes where `ps` in a sandboxed shell lists only the
//! caller's own, and the creators from the I/O Registry. A helper whose attach is still under
//! way has no device registered yet and looks the same, and killing it mid-attach is what leaks a
//! device until reboot (attach_budget.rs), so a pid is named only when two reads in a row find it
//! orphaned and no `diskutil` or `hdiutil` was running during either. cowshed never signals a
//! helper itself: only root can, and a helper serving an image cowshed did not attach is not
//! cowshed's to end.

use std::collections::BTreeSet;
use std::fmt;
use std::io;

/// The helper's executable.
pub const HELPER_EXECUTABLE: &str = "/usr/libexec/diskimagesiod";

#[cfg(target_os = "macos")]
/// The disk-image tools whose attach may be under way while its helper has no device yet.
const DISK_IMAGE_TOOLS: [&str; 2] = ["/usr/sbin/diskutil", "/usr/bin/hdiutil"];

/// The kernel mapping each helper holds: its IO request pool of 36 buffers of 2 MiB.
pub const MAPPED_MIB_PER_HELPER: u64 = 72;

/// The running helpers no attached disk image's user client names, by ascending pid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrphanedHelpers {
    pids: Vec<libc::pid_t>,
}

/// What a check of the helpers proves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HelperReading {
    /// Two reads in a row agree on these orphans, with no disk-image tool running in either.
    Settled(OrphanedHelpers),
    /// A `diskutil` or `hdiutil` ran during a read: a helper it started may have no device yet,
    /// so no pid is named. `suspected` helpers looked orphaned.
    Busy { suspected: usize },
}

impl HelperReading {
    /// Settle two reads, each the orphans it saw and whether a disk-image tool was running.
    pub fn from_reads(first: (OrphanedHelpers, bool), second: (OrphanedHelpers, bool)) -> Self {
        let ((first, first_busy), (second, second_busy)) = (first, second);
        let confirmed = first.confirmed(&second);
        if first_busy || second_busy {
            Self::Busy {
                suspected: confirmed.pids.len(),
            }
        } else {
            Self::Settled(confirmed)
        }
    }

    /// The orphans, when the reading settled.
    pub fn orphans(&self) -> Option<&OrphanedHelpers> {
        match self {
            Self::Settled(orphans) => Some(orphans),
            Self::Busy { .. } => None,
        }
    }
}

impl OrphanedHelpers {
    /// The orphans two reads in a row prove. Unsupported where the kernel has no disk image
    /// helpers (Linux).
    pub fn read() -> io::Result<HelperReading> {
        let first = read_orphans()?;
        let second = read_orphans()?;
        Ok(HelperReading::from_reads(first, second))
    }

    /// Every running helper whose pid created no live device's user client.
    pub fn among(helpers: &BTreeSet<libc::pid_t>, creators: &BTreeSet<libc::pid_t>) -> Self {
        Self {
            pids: helpers.difference(creators).copied().collect(),
        }
    }

    /// The helpers both this read and `later` found orphaned.
    pub fn confirmed(&self, later: &Self) -> Self {
        Self {
            pids: self
                .pids
                .iter()
                .filter(|pid| later.pids.contains(pid))
                .copied()
                .collect(),
        }
    }

    pub fn pids(&self) -> &[libc::pid_t] {
        &self.pids
    }

    pub fn is_empty(&self) -> bool {
        self.pids.is_empty()
    }

    /// The kernel mapping the orphans hold between them.
    pub fn mapped_mib(&self) -> u64 {
        MAPPED_MIB_PER_HELPER.saturating_mul(u64::try_from(self.pids.len()).unwrap_or(u64::MAX))
    }

    /// The root command that frees the orphans' mappings.
    pub fn remedy(&self) -> String {
        format!("sudo kill -9 {}", self.pid_list(" "))
    }

    fn pid_list(&self, separator: &str) -> String {
        self.pids
            .iter()
            .map(libc::pid_t::to_string)
            .collect::<Vec<_>>()
            .join(separator)
    }
}

impl fmt::Display for OrphanedHelpers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} diskimagesiod helper(s) outlived their disk image (pid {}): two reads in a row \
             found no attached AppleDiskImageDevice with a user client they created, while no \
             disk-image tool was running, yet each still holds the {MAPPED_MIB_PER_HELPER} MiB of \
             IO buffers it mapped into the kernel ({} MiB in all). Every attach maps \
             {MAPPED_MIB_PER_HELPER} MiB more for its own helper, and once the kernel has no room \
             left every disk image attach fails with error code 150 (kIOReturnNoResources) until \
             root kills the orphans: `launchctl kill` is refused, `kill -9` frees the mappings \
             without a reboot. Killing them does not free leaked image devices: those take a \
             reboot (disk-image-attach-budget)",
            self.pids.len(),
            self.pid_list(", "),
            self.mapped_mib()
        )
    }
}

/// The pid an `IOUserClientCreator` names: `pid N, <process name>`.
pub fn creator_pid(creator: &str) -> io::Result<libc::pid_t> {
    creator
        .strip_prefix("pid ")
        .and_then(|rest| rest.split_once(", "))
        .and_then(|(pid, _name)| pid.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("IOUserClientCreator {creator:?} is not `pid N, <process>`"),
            )
        })
}

/// One read: the orphans it saw, and whether a disk-image tool was running during it.
#[cfg(target_os = "macos")]
fn read_orphans() -> io::Result<(OrphanedHelpers, bool)> {
    // Helpers first: a helper must already run to be named, and its device must be missing at
    // the later registry read, so a helper that exits with its image is not caught midway.
    let scan = scan_processes()?;
    let creators = crate::disk_image_user_client_creators()?
        .iter()
        .map(|creator| creator_pid(creator))
        .collect::<io::Result<BTreeSet<_>>>()?;
    let busy = scan.disk_tools || scan_processes()?.disk_tools;
    Ok((OrphanedHelpers::among(&scan.helpers, &creators), busy))
}

#[cfg(not(target_os = "macos"))]
fn read_orphans() -> io::Result<(OrphanedHelpers, bool)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this kernel has no disk image helpers",
    ))
}

/// The running helpers, and whether any disk-image tool is running.
#[cfg(target_os = "macos")]
struct ProcessScan {
    helpers: BTreeSet<libc::pid_t>,
    disk_tools: bool,
}

/// Every running process whose executable is [`HELPER_EXECUTABLE`], and whether one of
/// [`DISK_IMAGE_TOOLS`] is running. A pid that exits between the listing and its path read
/// (`ESRCH`), or has no executable vnode (`ENOENT`), is neither; any other refusal would hide
/// helpers, so it fails the read.
#[cfg(target_os = "macos")]
fn scan_processes() -> io::Result<ProcessScan> {
    let mut scan = ProcessScan {
        helpers: BTreeSet::new(),
        disk_tools: false,
    };
    let mut path = [0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    for pid in all_pids()? {
        // SAFETY: `path` is writable for its whole length, which is the size passed.
        let length =
            unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), path.len() as u32) };
        if let Ok(length @ 1..) = usize::try_from(length) {
            let executable = &path[..length];
            if executable == HELPER_EXECUTABLE.as_bytes() {
                scan.helpers.insert(pid);
            } else if DISK_IMAGE_TOOLS
                .iter()
                .any(|tool| executable == tool.as_bytes())
            {
                scan.disk_tools = true;
            }
            continue;
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::ESRCH | libc::ENOENT) => {}
            _ => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("proc_pidpath({pid}): {error}"),
                ));
            }
        }
    }
    Ok(scan)
}

/// Every pid the kernel lists. The sizing call answers the process count with headroom; a
/// listing that fills the whole buffer may have been cut short by processes started since, so
/// it is asked again with the larger count.
#[cfg(target_os = "macos")]
fn all_pids() -> io::Result<Vec<libc::pid_t>> {
    const PID_BYTES: usize = std::mem::size_of::<libc::pid_t>();
    // SAFETY: a null buffer asks only for the number of pids to allot room for.
    let mut capacity = listed(unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) })?;
    loop {
        capacity += 64;
        let mut pids: Vec<libc::pid_t> = vec![0; capacity];
        let bytes = libc::c_int::try_from(capacity * PID_BYTES).map_err(|_| {
            io::Error::other(format!(
                "{capacity} pids overflow proc_listallpids's buffer size"
            ))
        })?;
        // SAFETY: `pids` is writable for `bytes` bytes, the size passed.
        let count = listed(unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), bytes) })?;
        if count < capacity {
            pids.truncate(count);
            return Ok(pids);
        }
        capacity = count;
    }
}

#[cfg(target_os = "macos")]
fn listed(count: libc::c_int) -> io::Result<usize> {
    usize::try_from(count).map_err(|_| {
        let error = io::Error::last_os_error();
        io::Error::new(error.kind(), format!("proc_listallpids: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pids(pids: &[libc::pid_t]) -> BTreeSet<libc::pid_t> {
        pids.iter().copied().collect()
    }

    /// The measured state: helpers that serve attached images are named by their device's user
    /// client; the ones that outlived their image are not, whatever their pid order.
    #[test]
    fn an_orphan_is_a_helper_no_device_user_client_names() {
        let helpers = pids(&[4353, 25101, 28634, 75251, 75814, 90001]);
        let creators = pids(&[25101, 28634, 75814]);
        let orphans = OrphanedHelpers::among(&helpers, &creators);
        assert_eq!(orphans.pids(), [4353, 75251, 90001]);
        assert_eq!(orphans.mapped_mib(), 216);
        assert_eq!(orphans.remedy(), "sudo kill -9 4353 75251 90001");
    }

    /// A client created by a process that is not a running helper (one that exited after the
    /// listing, or another opener) clears no helper and invents no orphan.
    #[test]
    fn a_creator_that_is_not_a_running_helper_changes_nothing() {
        let helpers = pids(&[100, 200]);
        let orphans = OrphanedHelpers::among(&helpers, &pids(&[200, 300]));
        assert_eq!(orphans.pids(), [100]);
        assert!(OrphanedHelpers::among(&helpers, &pids(&[100, 200])).is_empty());
        assert!(OrphanedHelpers::among(&pids(&[]), &pids(&[300])).is_empty());
    }

    #[test]
    fn the_finding_names_count_pids_mapping_and_why() {
        let orphans = OrphanedHelpers::among(&pids(&[11, 12]), &pids(&[]));
        let message = orphans.to_string();
        for needle in [
            "2 diskimagesiod helper(s)",
            "(pid 11, 12)",
            "two reads in a row",
            "72 MiB",
            "144 MiB in all",
            "error code 150",
            "launchctl kill",
            "does not free leaked image devices",
        ] {
            assert!(
                message.contains(needle),
                "{needle:?} missing from {message}"
            );
        }
    }

    /// A helper is named only when two reads agree it is orphaned and no disk-image tool ran
    /// during either: an attach still under way has a helper with no device registered yet.
    #[test]
    fn only_orphans_two_quiet_reads_agree_on_are_named() {
        let read = |orphans: &[libc::pid_t]| OrphanedHelpers::among(&pids(orphans), &pids(&[]));
        let settled =
            HelperReading::from_reads((read(&[1, 2, 3]), false), (read(&[2, 3, 4]), false));
        assert_eq!(
            settled.orphans().map(OrphanedHelpers::pids),
            Some(&[2, 3][..])
        );
        for busy in [(true, false), (false, true)] {
            let reading =
                HelperReading::from_reads((read(&[1, 2, 3]), busy.0), (read(&[2, 3, 4]), busy.1));
            assert_eq!(reading, HelperReading::Busy { suspected: 2 });
            assert!(reading.orphans().is_none());
        }
    }

    #[test]
    fn a_creator_names_its_pid_or_is_refused() {
        assert_eq!(creator_pid("pid 75814, diskimagesiod").unwrap(), 75814);
        for garbled in [
            "",
            "pid , diskimagesiod",
            "pid 75814",
            "75814, diskimagesiod",
        ] {
            let error = creator_pid(garbled).expect_err(garbled);
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    /// libproc names root's helpers to an unprivileged caller: a device whose user client
    /// exists both before and after the listing had its helper running throughout, so the
    /// listing names it whatever else attaches or detaches meanwhile.
    #[cfg(target_os = "macos")]
    #[test]
    fn the_live_helpers_and_creators_are_readable() {
        let creators = || {
            crate::disk_image_user_client_creators()
                .expect("read disk image user client creators")
                .iter()
                .map(|creator| creator_pid(creator).expect("creator pid"))
                .collect::<BTreeSet<_>>()
        };
        let before = creators();
        let helpers = scan_processes()
            .expect("list diskimagesiod helpers")
            .helpers;
        let after = creators();
        let throughout = before
            .intersection(&after)
            .copied()
            .collect::<BTreeSet<_>>();
        assert!(
            throughout.is_subset(&helpers),
            "user clients name helpers libproc does not list: {:?}",
            throughout.difference(&helpers).collect::<Vec<_>>()
        );
    }
}
