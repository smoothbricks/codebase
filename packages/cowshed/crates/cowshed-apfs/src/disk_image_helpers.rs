//! `diskimagesiod` helpers that outlived their disk image (01a_apfs.md, "How the APFS host
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
//!
//! A disk-image command still running past its deadline reads the host the other way round
//! ([`DiskImageHost`]): every disk-image tool running, oldest first, and every helper serving no
//! device, named as a fact rather than as an orphan to kill. Measured 2026-10-21: one attach whose
//! kernel device never got its BSD node held every later attach on the host for up to 41 minutes,
//! and once its clients were killed its helper stayed, serving no device; launchd keys a helper by
//! its image, so the next attach of that image reconnected to the wedged helper and hung again,
//! holding main's image lock and with it every verb on main. No waiter said which attach was the
//! first.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::time::Duration;
#[cfg(target_os = "macos")]
use std::time::SystemTime;

/// A disk-image tool running on the host: its pid, its command line, and how long it has run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunningDiskCommand {
    pub pid: libc::pid_t,
    pub command: String,
    pub running: Duration,
}

/// What a disk-image command still running past its deadline sees on the host: every disk-image
/// tool running, oldest first, and every helper serving no device, by ascending pid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskImageHost {
    pub commands: Vec<RunningDiskCommand>,
    pub idle_helpers: Vec<libc::pid_t>,
}

impl DiskImageHost {
    /// One read of the process table and the I/O Registry. Unsupported where the kernel has no
    /// disk image helpers (Linux).
    #[cfg(target_os = "macos")]
    pub fn read() -> io::Result<Self> {
        let scan = scan_processes()?;
        let creators = creator_pids(None)?;
        let now = SystemTime::now();
        let mut commands = Vec::with_capacity(scan.disk_tools.len());
        for pid in scan.disk_tools {
            // A tool that exited since the listing is no longer running: it is not named.
            if let Some(command) = running_command(pid, now)? {
                commands.push(command);
            }
        }
        Ok(Self::of(
            commands,
            OrphanedHelpers::among(&scan.helpers, &creators).pids,
        ))
    }

    #[cfg(not(target_os = "macos"))]
    pub fn read() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this kernel has no disk image helpers",
        ))
    }

    /// The reading of `commands` in any order and `idle_helpers` in ascending pid order.
    pub fn of(mut commands: Vec<RunningDiskCommand>, idle_helpers: Vec<libc::pid_t>) -> Self {
        commands.sort_by(|left, right| {
            right
                .running
                .cmp(&left.running)
                .then(left.pid.cmp(&right.pid))
        });
        Self {
            commands,
            idle_helpers,
        }
    }
}

impl fmt::Display for DiskImageHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.commands.first() {
            Some(oldest) => write!(
                f,
                "the oldest of {} disk-image command(s) running on this host is pid {}, running \
                 {}s: {}",
                self.commands.len(),
                oldest.pid,
                oldest.running.as_secs(),
                oldest.command
            )?,
            None => write!(f, "no disk-image command is running on this host")?,
        }
        if !self.idle_helpers.is_empty() {
            write!(
                f,
                "; diskimagesiod helper(s) serving no device: pid {}. A helper whose attach is \
                 under way serves none yet; one that still serves none at the next warning is \
                 wedged, and an attach of its image reconnects to it and waits with it. Only \
                 root's `kill -9` ends it, and killing one whose attach is under way can leak \
                 that device until reboot (01a_apfs.md, \"How the APFS host degrades\" §2)",
                self.idle_helpers
                    .iter()
                    .map(libc::pid_t::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

/// `pid`'s command line and how long it has run at `now`; `None` once it has exited.
#[cfg(target_os = "macos")]
fn running_command(pid: libc::pid_t, now: SystemTime) -> io::Result<Option<RunningDiskCommand>> {
    // SAFETY: `info` is a plain C struct the kernel fills; zeroed is a valid value of it.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(io::Error::other)?;
    // SAFETY: `info` is writable for `size` bytes, the size passed.
    let written =
        unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size) };
    if written != size {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(None),
            _ => Err(io::Error::new(
                error.kind(),
                format!("proc_pidinfo({pid}, PROC_PIDTBSDINFO): {error}"),
            )),
        };
    }
    let started = SystemTime::UNIX_EPOCH
        + Duration::from_secs(info.pbi_start_tvsec)
        + Duration::from_micros(info.pbi_start_tvusec);
    let command = match cowshed_os::process::process_arguments(pid) {
        Ok(arguments) => arguments
            .argv
            .iter()
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect::<Vec<_>>()
            .join(" "),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
        // Its arguments unreadable, it is still named, by what was in the way.
        Err(error) => format!("(command line unreadable: {error})"),
    };
    Ok(Some(RunningDiskCommand {
        pid,
        command,
        running: now.duration_since(started).unwrap_or_default(),
    }))
}

/// Image attaches running on the host, any user's: a `diskutil image attach` or an `hdiutil
/// attach` (media_cap.rs). A disk-image tool whose command line cannot be read is counted, since
/// an attach is what its silence must not hide; one that exits meanwhile is not.
#[cfg(target_os = "macos")]
pub fn running_attaches() -> io::Result<u64> {
    let mut attaching = 0;
    for pid in scan_processes()?.disk_tools {
        match cowshed_os::process::process_arguments(pid) {
            Ok(arguments) if !attaches(&arguments.argv) => {}
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {}
            Ok(_) | Err(_) => attaching += 1,
        }
    }
    Ok(attaching)
}

/// Whether `argv`, program first, attaches an image: `hdiutil attach …` or `diskutil image
/// attach …`.
#[cfg(any(target_os = "macos", test))]
fn attaches(argv: &[Vec<u8>]) -> bool {
    match argv {
        [_, verb, ..] if verb.as_slice() == b"attach" => true,
        [_, noun, verb, ..] => noun.as_slice() == b"image" && verb.as_slice() == b"attach",
        _ => false,
    }
}

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

    /// Read only helpers previously joined to the caller's exact images. The caller must
    /// finish those images' attach/release operations first; unrelated disk tools cannot
    /// turn this owned reading into an unavailable host-wide census.
    #[cfg(target_os = "macos")]
    pub fn read_owned(owned: &BTreeSet<libc::pid_t>) -> io::Result<Self> {
        let read = || -> io::Result<Self> {
            let scan = scan_processes()?;
            let creators = creator_pids(None)?;
            Ok(Self::among_owned(&scan.helpers, &creators, owned))
        };
        Ok(read()?.confirmed(&read()?))
    }

    /// Keep orphan attribution within the helpers the caller actually created.
    pub fn among_owned(
        helpers: &BTreeSet<libc::pid_t>,
        creators: &BTreeSet<libc::pid_t>,
        owned: &BTreeSet<libc::pid_t>,
    ) -> Self {
        Self {
            pids: helpers
                .intersection(owned)
                .filter(|pid| !creators.contains(pid))
                .copied()
                .collect(),
        }
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
             reboot (disk-image-attach-budget). The helpers run as the `_diskimagesiod` user, so \
             `ps` as this user does not list them; `launchctl print system | grep \
             diskimagesiod` names each running one with its pid, and `sudo ps -p <pid>` shows it",
            self.pids.len(),
            self.pid_list(", "),
            self.mapped_mib()
        )
    }
}

/// The helper PIDs the kernel joins to this exact backing image while it is attached.
/// Missing image/client evidence returns an empty set, never unrelated helpers.
#[cfg(target_os = "macos")]
pub fn image_helper_pids(image: &std::path::Path) -> io::Result<BTreeSet<libc::pid_t>> {
    creator_pids(Some(&std::path::absolute(image)?))
}

#[cfg(target_os = "macos")]
fn creator_pids(image: Option<&std::path::Path>) -> io::Result<BTreeSet<libc::pid_t>> {
    crate::disk_image_user_client_creators(image)?
        .iter()
        .map(|creator| creator_pid(creator))
        .collect()
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
    let creators = creator_pids(None)?;
    let busy = !scan.disk_tools.is_empty() || !scan_processes()?.disk_tools.is_empty();
    Ok((OrphanedHelpers::among(&scan.helpers, &creators), busy))
}

#[cfg(not(target_os = "macos"))]
fn read_orphans() -> io::Result<(OrphanedHelpers, bool)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this kernel has no disk image helpers",
    ))
}

/// The running helpers, and the disk-image tools running, by ascending pid.
#[cfg(target_os = "macos")]
struct ProcessScan {
    helpers: BTreeSet<libc::pid_t>,
    disk_tools: Vec<libc::pid_t>,
}

/// Every running process whose executable is [`HELPER_EXECUTABLE`], and every one whose
/// executable is one of [`DISK_IMAGE_TOOLS`]. A pid that exits between the listing and its path
/// read (`ESRCH`), or has no executable vnode (`ENOENT`), is neither; any other refusal would
/// hide helpers, so it fails the read.
#[cfg(target_os = "macos")]
fn scan_processes() -> io::Result<ProcessScan> {
    let mut scan = ProcessScan {
        helpers: BTreeSet::new(),
        disk_tools: Vec::new(),
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
                scan.disk_tools.push(pid);
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
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::ChildBackstop as _;

    fn pids(pids: &[libc::pid_t]) -> BTreeSet<libc::pid_t> {
        pids.iter().copied().collect()
    }

    /// An attach is `hdiutil attach` or `diskutil image attach`; a detach, an info read or a
    /// bare program is not.
    #[test]
    fn an_attach_is_told_from_every_other_disk_image_command() {
        let argv = |words: &[&str]| -> Vec<Vec<u8>> {
            words.iter().map(|word| word.as_bytes().to_vec()).collect()
        };
        for attach in [
            &["/usr/bin/hdiutil", "attach", "/x.dmg"][..],
            &[
                "/usr/sbin/diskutil",
                "image",
                "attach",
                "--noMount",
                "/x.dmg",
            ],
        ] {
            assert!(attaches(&argv(attach)), "{attach:?}");
        }
        for other in [
            &["/usr/bin/hdiutil", "detach", "/dev/disk9"][..],
            &["/usr/sbin/diskutil", "image", "info", "/x.dmg"],
            &["/usr/sbin/diskutil", "eject", "/dev/disk9"],
            &["/usr/sbin/diskutil"],
        ] {
            assert!(!attaches(&argv(other)), "{other:?}");
        }
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

    /// The measured wedge, as an overdue attach reads it: the oldest disk-image command names the
    /// attach every later one waited behind, however the listing ordered them, and the helpers
    /// serving no device are named as facts with the risk of ending them, never as a remedy.
    #[test]
    fn an_overdue_attach_names_the_oldest_disk_image_command_and_the_idle_helpers() {
        let command = |pid, seconds| RunningDiskCommand {
            pid,
            command: format!("/usr/sbin/diskutil image attach --noMount /store/{pid}.dmg"),
            running: Duration::from_secs(seconds),
        };
        let host = DiskImageHost::of(
            vec![
                command(38338, 2148),
                command(90338, 2400),
                command(29678, 951),
            ],
            vec![29680, 90396],
        );
        assert_eq!(
            host.commands
                .iter()
                .map(|command| command.pid)
                .collect::<Vec<_>>(),
            [90338, 38338, 29678]
        );
        let said = host.to_string();
        assert!(
            said.starts_with(
                "the oldest of 3 disk-image command(s) running on this host is pid 90338, \
                 running 2400s: /usr/sbin/diskutil image attach --noMount /store/90338.dmg; \
                 diskimagesiod helper(s) serving no device: pid 29680, 90396."
            ),
            "{said}"
        );
        assert!(!said.contains("sudo kill"), "{said}");
        assert_eq!(
            DiskImageHost::of(Vec::new(), Vec::new()).to_string(),
            "no disk-image command is running on this host"
        );
    }

    /// A real running process is read with its exact command line and an age measured from the
    /// kernel's start time; one that has exited is not named at all.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_running_process_is_read_with_its_command_line_and_age() {
        use cowshed_os::fork_lock::Spawn as _;
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn_locked()
            .expect("spawn a stand-in");
        let pid = libc::pid_t::try_from(child.id()).expect("a pid fits pid_t");
        let read = running_command(pid, SystemTime::now() + Duration::from_secs(5))
            .expect("read the stand-in")
            .expect("the stand-in runs");
        child.kill().expect("end the stand-in");
        child.wait_bounded().expect("reap the stand-in");
        assert_eq!(read.pid, pid);
        assert_eq!(read.command, "/bin/sleep 30");
        assert!(read.running >= Duration::from_secs(5), "{read:?}");
        assert_eq!(running_command(pid, SystemTime::now()).expect("read"), None);
    }

    #[test]
    fn an_unrelated_orphan_cannot_fail_an_owned_image_reading() {
        let helpers = pids(&[100, 200]);
        let creators = pids(&[100]);
        let owned = pids(&[100]);
        assert_eq!(OrphanedHelpers::among(&helpers, &creators).pids(), [200]);
        let first = OrphanedHelpers::among_owned(&helpers, &creators, &owned);
        let second = OrphanedHelpers::among_owned(&helpers, &creators, &owned);
        assert!(first.confirmed(&second).is_empty());
    }

    #[test]
    fn an_owned_orphan_remains_a_failure_after_two_reads() {
        let helpers = pids(&[100, 200]);
        let owned = pids(&[100]);
        assert!(OrphanedHelpers::among_owned(&helpers, &helpers, &owned).is_empty());
        let creators = pids(&[200]);
        let first = OrphanedHelpers::among_owned(&helpers, &creators, &owned);
        let second = OrphanedHelpers::among_owned(&helpers, &creators, &owned);
        assert_eq!(first.confirmed(&second).pids(), [100]);
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
    fn the_finding_names_count_pids_and_their_memory_mapping() {
        let orphans = OrphanedHelpers::among(&pids(&[11, 12]), &pids(&[]));
        let message = orphans.to_string();
        for needle in [
            "2 diskimagesiod helper(s)",
            "(pid 11, 12)",
            "72 MiB",
            "144 MiB in all",
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
}
