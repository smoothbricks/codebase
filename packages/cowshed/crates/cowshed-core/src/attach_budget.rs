//! The host's disk-image attach pool (01_storage.md, "How the APFS host degrades" §2;
//! 08_testing.md, "Host limit").
//!
//! Every `diskutil image attach` creates an `AppleDiskImageDevice`, its `DIDeviceIOUserClient`
//! and its request pool. A device whose attach or helper was killed mid-operation can stay alive
//! in the kernel after it leaves the registry: still counted among its class's instances, no
//! longer registered. Once the leaked devices fill the pool, every attach, `diskutil image info`
//! and cold mount fails with "error code 150" (`kIOReturnNoResources`), and only a reboot frees
//! them. The same error has a second cause, kernel mappings held by orphaned `diskimagesiod`
//! helpers ([`crate::disk_image_helpers`]), which root clears without a reboot.
//!
//! Both sides are read unprivileged and without attaching anything: the instance counts are the
//! registry root's `IOKitDiagnostics` class counts, which `ioclasscount` prints, and the
//! registered counts are the registry's own entries. A device instance with no registered entry
//! is leaked. The same subtraction for `AppleAPFSVolume` and `IOMedia` names references to
//! departed volumes, which `diskarbitrationd` was measured holding and which a restart of it
//! frees.
//!
//! The ceiling is an observation, not a documented limit: on 2026-10-10 the first 150 struck
//! with 1090 `AppleDiskImageDevice` instances and 66 registered, exactly 1024 leaked, and no
//! orphaned helper. That is the default; the leaked count at a later 150 that no orphaned helper
//! explains replaces it.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The framework's report when the attach pool is spent, as `diskutil image` prints it:
/// "Error: Disk image operation failed with error code 150".
const POOL_EXHAUSTED: &[u8] = b"error code 150";

/// Leaked image devices at which the pool ran out on the measuring host (module docs), used
/// until this host records a 150 of its own that no orphaned helper explains.
pub const OBSERVED_LEAK_CEILING: u64 = 1024;

/// Leaked volume or media references at which `doctor` names `diskarbitrationd`. One clean
/// attach→mount→detach cycle was measured leaving one `AppleAPFSVolume`, so a few are routine.
pub const VOLUME_LEAK_WARNING: u64 = 64;

/// What frees leaked image devices.
pub const REBOOT_HINT: &str =
    "reboot the host: only a reboot frees leaked disk-image devices (sudo shutdown -r now)";

/// What frees leaked volume and media references.
pub const RESTART_DISK_ARBITRATION_HINT: &str = "sudo killall diskarbitrationd: launchd \
     restarts it, and the departed volumes it held are freed";

/// What clears a 150 the leaked devices do not explain.
pub const ORPHANS_FIRST_HINT: &str = "kill the orphaned diskimagesiod helpers `cowshed doctor` \
     names (orphaned-disk-image-helpers), then `attach-budget --clear`; reboot if attaches still \
     fail with error code 150";

/// How the lane's checkout runs the operator's reset ([`clear`]): the bin is built from the
/// checkout, not installed.
pub const CLEAR_COMMAND: &str =
    "cargo --frozen run -q -p cowshed-core --bin attach-budget -- --clear";

/// The next step for a command the spent pool refused.
pub const EXHAUSTED_HINT: &str = "cowshed doctor: its disk-image-attach-budget finding says \
     whether a reboot or killing orphaned diskimagesiod helpers frees the attach pool";

/// Whether a disk-image command's stderr reports the spent attach pool: "error code 150" not
/// followed by another digit.
pub fn reports_pool_exhausted(stderr: &[u8]) -> bool {
    stderr.windows(POOL_EXHAUSTED.len() + 1).any(|window| {
        window.starts_with(POOL_EXHAUSTED) && !window[POOL_EXHAUSTED.len()].is_ascii_digit()
    }) || stderr.ends_with(POOL_EXHAUSTED)
}

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
    /// Leaked references to departed volumes and media, the larger of the two classes.
    pub fn leaked_volumes(&self) -> u64 {
        self.apfs_volumes.leaked().max(self.media.leaked())
    }
}

/// What the host looked like at a boot's first 150. Either read may have failed: the 150 is
/// recorded regardless.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Exhaustion {
    pub boot: Boot,
    pub image_devices: Option<ClassCount>,
    pub orphaned_helpers: Option<u64>,
}

impl Exhaustion {
    /// The leaked count this 150 proves is a ceiling: only when no orphaned helper's mapping
    /// could have caused it instead.
    fn ceiling(&self) -> Option<u64> {
        match (self.image_devices, self.orphaned_helpers) {
            (Some(devices), Some(0)) => Some(devices.leaked()),
            _ => None,
        }
    }

    /// Whether the leaked devices at this 150 alone account for it, against `ceiling`.
    fn explained_by(&self, ceiling: u64) -> bool {
        self.image_devices
            .is_some_and(|devices| devices.leaked() >= ceiling)
    }
}

/// Disk-image commands killed at their hard deadline during one boot: each probably left its
/// device leaked (01_storage.md, "No disk-image command is killed at its deadline").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbableLeaks {
    pub session: String,
    pub killed: u64,
}

/// What this user's cowshed has seen of the pool. The pool is host-wide; each user keeps its
/// own record of what its own commands met.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolRecord {
    /// The last 150, whatever caused it.
    pub last_exhaustion: Option<Exhaustion>,
    /// The last 150 whose leaked count is a ceiling.
    pub ceiling: Option<Exhaustion>,
    /// The boot whose pool is spent now, until an attach in it succeeds again.
    pub spent_session: Option<String>,
    pub probable_leaks: Option<ProbableLeaks>,
}

impl PoolRecord {
    /// The record after a disk-image command during `boot` failed with 150. A later 150 in a
    /// boot already spent keeps the first: that is where the pool ran out.
    pub fn exhausted(
        self,
        boot: &Boot,
        image_devices: Option<ClassCount>,
        orphaned_helpers: Option<u64>,
    ) -> Self {
        if self.spent_session.as_deref() == Some(boot.session.as_str()) {
            return self;
        }
        let exhaustion = Exhaustion {
            boot: boot.clone(),
            image_devices,
            orphaned_helpers,
        };
        Self {
            ceiling: if exhaustion.ceiling().is_some() {
                Some(exhaustion.clone())
            } else {
                self.ceiling
            },
            last_exhaustion: Some(exhaustion),
            spent_session: Some(boot.session.clone()),
            probable_leaks: self.probable_leaks,
        }
    }

    /// The record after an attach during `session` succeeded: whatever spent the pool was
    /// freed. The exhaustion stays as evidence and ceiling.
    pub fn attached(self, session: &str) -> Self {
        Self {
            spent_session: self.spent_session.filter(|spent| spent != session),
            ..self
        }
    }

    /// The record after the operator cleared it (`attach-budget --clear`): this boot's
    /// exhaustion ends, as after a successful attach, and the observed ceiling is forgotten,
    /// back to [`OBSERVED_LEAK_CEILING`]. A ceiling only ever moves down and survives reboots,
    /// so one 150 at a low leaked count would otherwise refuse the lane on every later boot,
    /// with no attach left to raise it. The last exhaustion stays as evidence.
    pub fn cleared(self, session: &str) -> Self {
        Self {
            ceiling: None,
            ..self.attached(session)
        }
    }

    /// The record after a disk-image command was killed at its hard deadline during `session`.
    pub fn killed(self, session: &str) -> Self {
        let killed = match &self.probable_leaks {
            Some(leaks) if leaks.session == session => leaks.killed + 1,
            _ => 1,
        };
        Self {
            probable_leaks: Some(ProbableLeaks {
                session: session.to_owned(),
                killed,
            }),
            ..self
        }
    }

    /// Whether this boot's pool is spent, and by which 150.
    pub fn spent(&self, session: &str) -> Option<&Exhaustion> {
        self.last_exhaustion
            .as_ref()
            .filter(|_| self.spent_session.as_deref() == Some(session))
    }

    /// The leaked image devices at which the pool runs out, as this record proves it.
    pub fn ceiling(&self) -> Ceiling {
        self.ceiling
            .as_ref()
            .and_then(|exhaustion| {
                exhaustion.ceiling().map(|leaked| Ceiling::Observed {
                    leaked,
                    booted_at: exhaustion.boot.booted_at,
                })
            })
            .unwrap_or(Ceiling::Default)
    }

    fn ceiling_leaked(&self) -> u64 {
        self.ceiling().leaked()
    }

    /// The pool during `session`, as `reading` and this record prove it.
    pub fn budget(&self, session: &str, reading: KernelReading) -> Budget {
        Budget {
            reading,
            ceiling: self.ceiling(),
            spent: self.spent(session).cloned(),
            probable_leaks: self
                .probable_leaks
                .as_ref()
                .filter(|leaks| leaks.session == session)
                .map_or(0, |leaks| leaks.killed),
        }
    }
}

/// The leaked image devices at which the pool runs out.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ceiling {
    /// No ceiling recorded on this host: [`OBSERVED_LEAK_CEILING`].
    Default,
    /// The leaked devices at this host's last 150 that no orphaned helper explains.
    Observed { leaked: u64, booted_at: i64 },
}

impl Ceiling {
    pub fn leaked(self) -> u64 {
        match self {
            Self::Default => OBSERVED_LEAK_CEILING,
            Self::Observed { leaked, .. } => leaked,
        }
    }
}

impl fmt::Display for Ceiling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => write!(
                f,
                "{OBSERVED_LEAK_CEILING} leaked (the default: where one host's pool ran out, an \
                 observation from one host and boot, not a documented limit)"
            ),
            Self::Observed { leaked, booted_at } => write!(
                f,
                "{leaked} leaked (observed here at error code 150, in the boot that began at \
                 {booted_at})"
            ),
        }
    }
}

/// The pool for the current boot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Budget {
    pub reading: KernelReading,
    pub ceiling: Ceiling,
    /// This boot's first 150, while nothing has attached since.
    pub spent: Option<Exhaustion>,
    /// Disk-image commands killed at their hard deadline this boot.
    pub probable_leaks: u64,
}

impl Budget {
    /// Leaked image devices the pool can still take.
    pub fn headroom(&self) -> u64 {
        self.ceiling
            .leaked()
            .saturating_sub(self.reading.image_devices.leaked())
    }

    /// Whether `need` more attaches fit, each counted as one device it could leak: refused once
    /// this boot's pool is spent, or when the headroom is below `need`. The real-APFS lane's gate.
    pub fn admits(&self, need: u64) -> Result<(), AttachPoolExhausted> {
        if self.spent.is_some() || self.headroom() < need {
            return Err(AttachPoolExhausted {
                need,
                headroom: Some(self.headroom()),
                detail: self.to_string(),
                remedy: self.refusal_remedy(need),
            });
        }
        Ok(())
    }

    /// What frees enough of the pool for `need` more attaches, which a refusal always names:
    /// the orphaned helpers when the leaked devices do not explain a spent boot, the recorded
    /// ceiling when it alone is below `need`, and otherwise a reboot, the one thing that frees
    /// leaked image devices. Leaked volumes and media add the `diskarbitrationd` restart first.
    fn refusal_remedy(&self, need: u64) -> String {
        let step = match self.ceiling {
            _ if self.spent.is_some() && !self.devices_full() => ORPHANS_FIRST_HINT.to_owned(),
            Ceiling::Observed { leaked, .. } if leaked < need => format!(
                "the ceiling recorded at this host's last error code 150, {leaked} leaked \
                 devices, is below the {need} attaches needed whatever leaks now; it survives \
                 reboots and only moves down, so if it no longer holds, `{CLEAR_COMMAND}` \
                 forgets it (back to {OBSERVED_LEAK_CEILING})"
            ),
            Ceiling::Observed { .. } | Ceiling::Default => REBOOT_HINT.to_owned(),
        };
        if self.reading.leaked_volumes() >= VOLUME_LEAK_WARNING {
            format!("{RESTART_DISK_ARBITRATION_HINT}; then {step}")
        } else {
            step
        }
    }

    /// Whether the leaked devices alone account for the spent pool.
    fn devices_full(&self) -> bool {
        self.headroom() == 0
    }

    /// Whether `doctor` should warn.
    pub fn is_low(&self) -> bool {
        self.spent.is_some()
            || self.headroom() <= self.ceiling.leaked() / 4
            || self.reading.leaked_volumes() >= VOLUME_LEAK_WARNING
            || self.probable_leaks > 0
    }

    /// What frees what leaked.
    pub fn remedy(&self) -> String {
        let mut steps = Vec::new();
        if self.reading.leaked_volumes() >= VOLUME_LEAK_WARNING {
            steps.push(RESTART_DISK_ARBITRATION_HINT);
        }
        if self.spent.is_some() && !self.devices_full() {
            steps.push(ORPHANS_FIRST_HINT);
        } else if self.spent.is_some() || self.headroom() <= self.ceiling.leaked() / 4 {
            steps.push(REBOOT_HINT);
        }
        if steps.is_empty() {
            return "nothing to free".to_owned();
        }
        steps.join("; then ")
    }
}

impl fmt::Display for Budget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reading = &self.reading;
        if let Some(spent) = &self.spent {
            let spent = Spent {
                exhaustion: spent,
                devices_full: self.devices_full(),
            };
            write!(f, "{spent}. ")?;
        }
        write!(
            f,
            "AppleDiskImageDevice {}; DIDeviceIOUserClient {}; AppleAPFSVolume {}; IOMedia {}. \
             Ceiling {}, headroom {} leaked image devices",
            reading.image_devices,
            reading.user_clients,
            reading.apfs_volumes,
            reading.media,
            self.ceiling,
            self.headroom()
        )?;
        if self.probable_leaks > 0 {
            write!(
                f,
                "; {} disk-image command(s) killed at their hard deadline this boot, each a \
                 probable leaked device",
                self.probable_leaks
            )?;
        }
        Ok(())
    }
}

/// A boot's spent pool, said the way `doctor` and the gates say it: "reboot required" only when
/// the leaked devices account for it.
struct Spent<'a> {
    exhaustion: &'a Exhaustion,
    devices_full: bool,
}

impl fmt::Display for Spent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("attach pool exhausted")?;
        if self.devices_full {
            f.write_str(", reboot required")?;
        }
        f.write_str(
            ": a disk-image command failed with error code 150 (kIOReturnNoResources) this boot",
        )?;
        if let Some(devices) = self.exhaustion.image_devices {
            write!(
                f,
                " with {} disk images attached and {} leaked image devices",
                devices.registered,
                devices.leaked()
            )?;
        }
        match self.exhaustion.orphaned_helpers {
            Some(0) => {}
            Some(orphans) => write!(f, " and {orphans} orphaned diskimagesiod helpers")?,
            None => f.write_str(" (orphaned helpers unread)")?,
        }
        f.write_str("; every attach, `diskutil image info` and cold mount fails until it is freed")
    }
}

/// The refusal to attach into a spent, or too nearly spent, pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachPoolExhausted {
    pub need: u64,
    /// `None` when the gate refused from the record alone, without reading the kernel.
    pub headroom: Option<u64>,
    pub detail: String,
    pub remedy: String,
}

impl fmt::Display for AttachPoolExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AttachPoolExhausted: {} attaches needed", self.need)?;
        if let Some(headroom) = self.headroom {
            write!(f, ", {headroom} of headroom")?;
        }
        write!(f, "; {}\nnext: {}", self.detail, self.remedy)
    }
}

impl std::error::Error for AttachPoolExhausted {}

/// `/private/var/tmp/cowshed-attach-pool-<euid>/`: this user's private directory, which
/// survives reboot so the ceiling does too. Checked like the image leases' directory: created
/// 0700, and refused unless it is a directory this user owns with that mode.
pub fn record_directory() -> io::Result<PathBuf> {
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    let uid = unsafe { libc::geteuid() };
    let directory = PathBuf::from(format!("/private/var/tmp/cowshed-attach-pool-{uid}"));
    crate::apfs::private_user_directory(&directory, "attach-pool record directory")?;
    Ok(directory)
}

/// This user's record.
pub fn record_path() -> io::Result<PathBuf> {
    Ok(record_directory()?.join("pool.json"))
}

/// The record at `path`; a missing one is empty.
pub fn load(path: &Path) -> io::Result<PoolRecord> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: {error}", path.display()),
            )
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(PoolRecord::default()),
        Err(error) => Err(error),
    }
}

/// The record at `path` as [`load`] reads it, except that a record that does not parse (a hand
/// edit, a newer schema) is read as empty and said aloud with the file it names: it proves
/// nothing about the pool, and must neither stop the kernel from being reported nor keep a 150
/// from being recorded. The flag says the stored bytes are broken, so the next write replaces
/// them.
fn load_or_empty(path: &Path) -> io::Result<(PoolRecord, bool)> {
    match load(path) {
        Ok(record) => Ok((record, false)),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            eprintln!(
                "cowshed: warning: the disk-image attach pool record does not parse ({error}); \
                 read as empty, and replaced at its next write"
            );
            Ok((PoolRecord::default(), true))
        }
        Err(error) => Err(error),
    }
}

/// Apply `change` to the record at `path` under its sibling lock, so concurrent test processes
/// cannot lose each other's updates, and answer the record now stored. An unchanged record is
/// not rewritten, unless its stored bytes do not parse.
pub fn update(
    path: &Path,
    change: impl FnOnce(PoolRecord) -> PoolRecord,
) -> io::Result<PoolRecord> {
    let _lock = crate::apfs::locked_private_file(&path.with_extension("lock"))?;
    let (before, broken) = load_or_empty(path)?;
    let after = change(before.clone());
    if after == before && !broken {
        return Ok(after);
    }
    crate::fsio::publish_private_file(path, |writer| serde_json::to_writer(writer, &after))
        .map_err(|error| match error {
            crate::fsio::PublishError::Io { path, source } => {
                io::Error::new(source.kind(), format!("{}: {source}", path.display()))
            }
            crate::fsio::PublishError::Write(error) => io::Error::other(error),
        })?;
    Ok(after)
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
    crate::apfs::disk_image_class_counts()
}

#[cfg(not(target_os = "macos"))]
pub fn read_kernel() -> io::Result<KernelReading> {
    Err(unsupported())
}

#[cfg(not(target_os = "macos"))]
fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "this kernel has no disk-image attach pool",
    )
}

/// The pool now. Nothing is attached and the record is not written; only the record's private
/// directory is created if it is missing. A record that does not parse reads as empty
/// ([`load_or_empty`]), so the kernel's counts are still reported.
pub fn observe() -> io::Result<Budget> {
    let boot = boot()?;
    let reading = read_kernel()?;
    let (record, _) = load_or_empty(&record_path()?)?;
    Ok(record.budget(&boot.session, reading))
}

/// Record that the host just answered a disk-image command with error code 150. The 150 is
/// recorded even when the kernel or the helpers cannot be read; only the ceiling needs them.
pub fn record_exhaustion() -> io::Result<PoolRecord> {
    let boot = boot()?;
    let devices = read_kernel().ok().map(|reading| reading.image_devices);
    let orphans = crate::disk_image_helpers::OrphanedHelpers::read()
        .ok()
        .and_then(|reading| reading.orphans().map(|orphans| orphans.pids().len()))
        .and_then(|orphans| u64::try_from(orphans).ok());
    update(&record_path()?, |record| {
        record.exhausted(&boot, devices, orphans)
    })
}

/// Record that an attach just succeeded, which ends this boot's exhaustion if one is recorded.
pub fn record_attach() -> io::Result<PoolRecord> {
    record_attach_at(&record_path()?, &boot()?.session)
}

pub fn record_attach_at(path: &Path, session: &str) -> io::Result<PoolRecord> {
    let (record, _) = load_or_empty(path)?;
    if record.spent(session).is_none() {
        return Ok(record);
    }
    update(path, |record| record.attached(session))
}

/// Record a disk-image command killed at its hard deadline: a probable leaked device.
pub fn record_killed() -> io::Result<PoolRecord> {
    record_killed_at(&record_path()?, &boot()?.session)
}

pub fn record_killed_at(path: &Path, session: &str) -> io::Result<PoolRecord> {
    update(path, |record| record.killed(session))
}

/// The operator's reset ([`PoolRecord::cleared`]): end this boot's exhaustion and forget the
/// observed ceiling. A record that does not parse is replaced.
pub fn clear() -> io::Result<PoolRecord> {
    clear_at(&record_path()?, &boot()?.session)
}

pub fn clear_at(path: &Path, session: &str) -> io::Result<PoolRecord> {
    update(path, |record| record.cleared(session))
}

/// Refuse `need` attaches the pool cannot take: the real-APFS lane's gate.
pub fn admit(need: u64) -> io::Result<Result<Budget, AttachPoolExhausted>> {
    let budget = observe()?;
    Ok(budget.admits(need).map(|()| budget))
}

/// Refuse a real-APFS test fixture once this boot's pool is spent (08_testing.md, "Host
/// limit"): the runner that met the first 150, in whichever test process, recorded it, and every
/// later fixture of the run stops here before it attaches. It reads only the boot and the
/// record. A record it cannot read proves nothing about the pool, so it is said aloud and the
/// fixture proceeds: only a recorded 150 refuses.
pub fn harness_gate() -> Result<(), AttachPoolExhausted> {
    match boot().and_then(|boot| Ok((record_path()?, boot))) {
        Ok((path, boot)) => harness_gate_at(&path, &boot.session),
        Err(error) if error.kind() == io::ErrorKind::Unsupported => Ok(()),
        Err(error) => {
            unreadable(&error);
            Ok(())
        }
    }
}

/// [`harness_gate`] against the record at `path` during boot `session`.
pub fn harness_gate_at(path: &Path, session: &str) -> Result<(), AttachPoolExhausted> {
    let record = match load_or_empty(path) {
        Ok((record, _)) => record,
        Err(error) => {
            unreadable(&error);
            return Ok(());
        }
    };
    let Some(spent) = record.spent(session) else {
        return Ok(());
    };
    let explained = spent.explained_by(record.ceiling_leaked());
    Err(AttachPoolExhausted {
        need: 1,
        headroom: None,
        detail: Spent {
            exhaustion: spent,
            devices_full: explained,
        }
        .to_string(),
        remedy: if explained {
            REBOOT_HINT
        } else {
            ORPHANS_FIRST_HINT
        }
        .to_owned(),
    })
}

fn unreadable(error: &io::Error) {
    eprintln!(
        "cowshed: warning: the disk-image attach pool record is unreadable ({error}); attaching \
         anyway"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boot(session: &str) -> Boot {
        Boot {
            session: session.to_owned(),
            booted_at: 1_791_515_025,
        }
    }

    const BOOT: &str = "C979D80A-3BC4-4F57-A9E2-1FE362B935C7";
    const NEXT_BOOT: &str = "0E3E6D1C-6C7B-4B53-8A1E-6C5C1E0D1F00";

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

    fn spent_record() -> PoolRecord {
        PoolRecord::default().exhausted(&boot(BOOT), Some(spent_host().image_devices), Some(0))
    }

    #[test]
    fn the_framework_report_of_a_spent_pool_is_recognized_and_nothing_else_is() {
        assert!(reports_pool_exhausted(
            b"Error: Disk image operation failed with error code 150\n"
        ));
        assert!(reports_pool_exhausted(b"failed with error code 150"));
        for other in [
            &b"Error: Disk image operation failed with error code 15\n"[..],
            b"Error: Disk image operation failed with error code 1500\n",
            b"attach failed",
        ] {
            assert!(
                !reports_pool_exhausted(other),
                "{:?}",
                String::from_utf8_lossy(other)
            );
        }
    }

    #[test]
    fn leaked_is_instances_no_registered_entry_accounts_for() {
        assert_eq!(spent_host().image_devices.leaked(), 1024);
        assert_eq!(spent_host().leaked_volumes(), 995);
        assert_eq!(count(3, 5).leaked(), 0);
        assert_eq!(rebooted_host().leaked_volumes(), 0);
    }

    #[test]
    fn with_no_ceiling_recorded_the_observed_1024_is_the_ceiling_and_a_healthy_host_frees_nothing()
    {
        let budget = PoolRecord::default().budget(NEXT_BOOT, rebooted_host());
        assert_eq!(budget.ceiling, Ceiling::Default);
        assert_eq!(budget.headroom(), 1024);
        assert!(!budget.is_low());
        assert_eq!(budget.remedy(), "nothing to free");
        assert!(budget.admits(1024).is_ok());
        assert!(budget.admits(1025).is_err());
        let report = budget.to_string();
        assert!(report.contains("not a documented limit"), "{report}");
        assert!(
            report.contains("AppleDiskImageDevice 178 instances, 178 registered, 0 leaked"),
            "{report}"
        );

        let full = PoolRecord::default().budget(NEXT_BOOT, leaking(1024));
        assert_eq!(full.headroom(), 0);
        assert!(full.is_low());
        assert_eq!(full.remedy(), REBOOT_HINT);
        assert!(full.admits(1).is_err());
        let quarter = PoolRecord::default().budget(NEXT_BOOT, leaking(1024 - 256));
        assert!(quarter.is_low());
        assert!(
            !PoolRecord::default()
                .budget(NEXT_BOOT, leaking(1024 - 257))
                .is_low()
        );
    }

    #[test]
    fn a_150_spends_the_boot_and_its_leaked_count_becomes_the_ceiling() {
        let record = spent_record();
        let exhaustion = Exhaustion {
            boot: boot(BOOT),
            image_devices: Some(count(1090, 66)),
            orphaned_helpers: Some(0),
        };
        assert_eq!(record.last_exhaustion, Some(exhaustion.clone()));
        assert_eq!(record.ceiling, Some(exhaustion.clone()));
        let budget = record.budget(BOOT, spent_host());
        assert_eq!(budget.spent, Some(exhaustion));
        assert!(budget.is_low());
        assert!(budget.admits(0).is_err());
        let report = budget.to_string();
        assert!(
            report.starts_with("attach pool exhausted, reboot required"),
            "{report}"
        );
        assert!(
            report.contains("with 66 disk images attached and 1024 leaked image devices"),
            "{report}"
        );
        assert_eq!(
            budget.remedy(),
            format!("{RESTART_DISK_ARBITRATION_HINT}; then {REBOOT_HINT}")
        );

        // A second 150 in the same boot keeps where the pool first ran out.
        assert_eq!(
            record
                .clone()
                .exhausted(&boot(BOOT), Some(count(2000, 10)), Some(0)),
            record
        );

        // After the reboot the recorded leaked count is the ceiling, and nothing is spent.
        let rebooted = record.budget(NEXT_BOOT, leaking(24));
        assert_eq!(
            rebooted.ceiling,
            Ceiling::Observed {
                leaked: 1024,
                booted_at: 1_791_515_025
            }
        );
        assert_eq!(rebooted.spent, None);
        assert_eq!(rebooted.headroom(), 1000);
        assert!(rebooted.admits(1000).is_ok());
        let refused = rebooted
            .admits(1001)
            .expect_err("one more than the headroom");
        let report = refused.to_string();
        assert!(
            report.starts_with("AttachPoolExhausted: 1001 attaches needed, 1000 of headroom"),
            "{report}"
        );
        // Leaked devices are what refuse, and only a reboot frees them.
        assert!(
            report.ends_with(&format!("\nnext: {REBOOT_HINT}")),
            "{report}"
        );

        // A later 150 at a lower leaked count lowers the ceiling.
        let lowered = record
            .exhausted(&boot(NEXT_BOOT), Some(count(310, 10)), Some(0))
            .budget(NEXT_BOOT, leaking(0));
        assert_eq!(lowered.ceiling.leaked(), 300);
    }

    #[test]
    fn a_150_the_orphans_could_explain_spends_the_boot_but_sets_no_ceiling() {
        for orphans in [Some(10), None] {
            let record = PoolRecord::default().exhausted(&boot(BOOT), Some(count(96, 66)), orphans);
            assert!(record.ceiling.is_none(), "{orphans:?}");
            let budget = record.budget(BOOT, leaking(30));
            assert_eq!(budget.ceiling, Ceiling::Default);
            assert!(budget.spent.is_some());
            assert_eq!(budget.remedy(), ORPHANS_FIRST_HINT);
            let report = budget.to_string();
            assert!(report.starts_with("attach pool exhausted: "), "{report}");
            assert!(!report.contains("reboot required"), "{report}");
        }
        // The kernel unread at the 150: the boot is spent all the same.
        let unread = PoolRecord::default().exhausted(&boot(BOOT), None, None);
        assert!(unread.spent(BOOT).is_some());
        assert!(unread.ceiling.is_none());
    }

    #[test]
    fn an_attach_that_succeeds_again_ends_the_boots_exhaustion_but_keeps_the_ceiling() {
        let spent = spent_record();
        let recovered = spent.clone().attached(BOOT);
        assert_eq!(recovered.spent_session, None);
        assert_eq!(recovered.ceiling, spent.ceiling);
        assert!(recovered.budget(BOOT, spent_host()).spent.is_none());
        // A success in another boot says nothing about this one.
        assert_eq!(spent.clone().attached(NEXT_BOOT), spent);
    }

    #[test]
    fn a_hard_deadline_kill_is_a_probable_leak_of_this_boot() {
        let record = PoolRecord::default().killed(BOOT).killed(BOOT);
        let budget = record.budget(BOOT, rebooted_host());
        assert_eq!(budget.probable_leaks, 2);
        assert!(budget.is_low());
        assert!(
            budget
                .to_string()
                .contains("2 disk-image command(s) killed")
        );
        assert_eq!(
            record
                .clone()
                .killed(NEXT_BOOT)
                .budget(NEXT_BOOT, rebooted_host())
                .probable_leaks,
            1
        );
        assert_eq!(record.budget(NEXT_BOOT, rebooted_host()).probable_leaks, 0);
    }

    #[test]
    fn the_remedy_names_what_frees_what_leaked() {
        let record = PoolRecord::default();
        let volumes = KernelReading {
            apfs_volumes: count(1065, 81),
            ..rebooted_host()
        };
        assert_eq!(
            record.budget(NEXT_BOOT, volumes).remedy(),
            RESTART_DISK_ARBITRATION_HINT
        );
        assert!(record.budget(NEXT_BOOT, volumes).is_low());
        let few = KernelReading {
            media: count(185 + VOLUME_LEAK_WARNING - 1, 185),
            ..rebooted_host()
        };
        assert!(!record.budget(NEXT_BOOT, few).is_low());
        assert_eq!(record.budget(NEXT_BOOT, few).remedy(), "nothing to free");
    }

    fn scratch(label: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "cowshed-attach-pool-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn the_record_round_trips_tolerates_new_fields_and_is_not_rewritten_unchanged() {
        let directory = scratch("record");
        let path = directory.join("pool.json");
        assert_eq!(load(&path).unwrap(), PoolRecord::default());
        let spent = update(&path, |_| spent_record()).unwrap();
        assert_eq!(load(&path).unwrap(), spent);
        let written = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(update(&path, |record| record).unwrap(), spent);
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            written
        );
        std::fs::write(&path, b"{\"spentSession\":null,\"later\":1}").unwrap();
        assert_eq!(load(&path).unwrap(), PoolRecord::default());
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn concurrent_updates_lose_nothing() {
        let directory = scratch("concurrent");
        let path = directory.join("pool.json");
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..25 {
                        record_killed_at(&path, BOOT).unwrap();
                    }
                });
            }
        });
        assert_eq!(
            load(&path)
                .unwrap()
                .budget(BOOT, rebooted_host())
                .probable_leaks,
            200
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_successful_attach_clears_the_recorded_boot() {
        let directory = scratch("attach");
        let path = directory.join("pool.json");
        update(&path, |_| spent_record()).unwrap();
        assert!(harness_gate_at(&path, BOOT).is_err());
        record_attach_at(&path, BOOT).unwrap();
        assert!(harness_gate_at(&path, BOOT).is_ok());
        assert!(load(&path).unwrap().ceiling.is_some());
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Every refusal names a step that can free the pool. Between a quarter of the ceiling and
    /// the need, the doctor remedy has nothing to free, yet the gate refuses: it says reboot.
    #[test]
    fn a_refusal_for_low_headroom_says_reboot_even_when_doctor_would_not() {
        let budget = PoolRecord::default().budget(NEXT_BOOT, leaking(700));
        assert_eq!(budget.headroom(), 324);
        assert_eq!(budget.remedy(), "nothing to free");
        let refused = budget.admits(400).expect_err("324 < 400");
        assert_eq!(refused.remedy, REBOOT_HINT);
        let volumes = KernelReading {
            apfs_volumes: count(1065, 81),
            ..leaking(700)
        };
        let refused = PoolRecord::default()
            .budget(NEXT_BOOT, volumes)
            .admits(400)
            .expect_err("324 < 400");
        assert_eq!(
            refused.remedy,
            format!("{RESTART_DISK_ARBITRATION_HINT}; then {REBOOT_HINT}")
        );
        // A spent boot the leaked devices do not explain sends the operator to the orphans.
        let orphaned = PoolRecord::default()
            .exhausted(&boot(BOOT), Some(count(96, 66)), Some(10))
            .budget(BOOT, leaking(30));
        assert_eq!(orphaned.admits(1).unwrap_err().remedy, ORPHANS_FIRST_HINT);
    }

    /// A ceiling below the need refuses whatever leaks now; the refusal names the reset, and the
    /// reset forgets the ceiling and the spent boot, keeping the last exhaustion as evidence.
    #[test]
    fn a_recorded_ceiling_below_the_need_names_the_reset_that_forgets_it() {
        let low = PoolRecord::default().exhausted(&boot(BOOT), Some(count(40, 10)), Some(0));
        let budget = low.budget(NEXT_BOOT, leaking(0));
        assert_eq!(budget.ceiling.leaked(), 30);
        let refused = budget.admits(273).expect_err("30 < 273");
        assert!(
            refused.remedy.contains(&format!("`{CLEAR_COMMAND}`")),
            "{refused}"
        );
        assert!(refused.remedy.contains("30 leaked devices"), "{refused}");
        // A ceiling above the need with too many leaked devices is the reboot's, not the reset's.
        let high = PoolRecord::default().exhausted(&boot(BOOT), Some(count(510, 10)), Some(0));
        let refused = high
            .budget(NEXT_BOOT, leaking(400))
            .admits(273)
            .unwrap_err();
        assert_eq!(refused.remedy, REBOOT_HINT);

        let cleared = low.clone().cleared(BOOT);
        assert_eq!(cleared.ceiling, None);
        assert_eq!(cleared.spent_session, None);
        assert_eq!(cleared.last_exhaustion, low.last_exhaustion);
        assert!(cleared.budget(NEXT_BOOT, leaking(0)).admits(273).is_ok());

        let directory = scratch("clear");
        let path = directory.join("pool.json");
        update(&path, |_| low.clone()).unwrap();
        assert!(harness_gate_at(&path, BOOT).is_err());
        clear_at(&path, BOOT).unwrap();
        assert!(harness_gate_at(&path, BOOT).is_ok());
        assert_eq!(load(&path).unwrap().ceiling(), Ceiling::Default);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A record that does not parse proves nothing: it reads as empty, the next 150 is still
    /// recorded over it (so the cascade guard holds), and the reset replaces it.
    #[test]
    fn a_record_that_does_not_parse_is_read_as_empty_and_replaced() {
        let directory = scratch("broken");
        let path = directory.join("pool.json");
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(load(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(load_or_empty(&path).unwrap(), (PoolRecord::default(), true));
        update(&path, |record| {
            record.exhausted(&boot(BOOT), Some(spent_host().image_devices), Some(0))
        })
        .unwrap();
        assert!(harness_gate_at(&path, BOOT).is_err());

        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(clear_at(&path, BOOT).unwrap(), PoolRecord::default());
        assert_eq!(
            load(&path).unwrap(),
            PoolRecord::default(),
            "rewritten, not left broken"
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// The cascade this gate exists for: 26 tests each attaching into a spent pool. Every test
    /// passes the gate, then attaches; the host answers 150 from the third attach on. Only that
    /// one attach is spent on the spent pool: the 23 tests after it are refused before attaching.
    #[test]
    fn the_harness_stops_the_run_at_the_first_150() {
        let directory = scratch("harness");
        let path = directory.join("pool.json");
        let mut attaches = 0;
        let mut refused = Vec::new();
        for test in 0..26 {
            if let Err(exhausted) = harness_gate_at(&path, BOOT) {
                refused.push(exhausted.to_string());
                continue;
            }
            attaches += 1;
            if test >= 2 {
                update(&path, |record| {
                    record.exhausted(&boot(BOOT), Some(spent_host().image_devices), Some(0))
                })
                .unwrap();
            }
        }
        assert_eq!(attaches, 3);
        assert_eq!(refused.len(), 23);
        assert!(
            refused[0].starts_with(
                "AttachPoolExhausted: 1 attaches needed; attach pool exhausted, reboot required"
            ),
            "{}",
            refused[0]
        );
        assert!(
            refused[0].ends_with(&format!("\nnext: {REBOOT_HINT}")),
            "{}",
            refused[0]
        );

        // After the reboot the fixtures run again; only the lane gate weighs the ceiling.
        assert!(harness_gate_at(&path, NEXT_BOOT).is_ok());
        assert!(
            load(&path)
                .unwrap()
                .budget(NEXT_BOOT, leaking(1024))
                .admits(1)
                .is_err()
        );

        // An unreadable record proves nothing: the fixture proceeds.
        std::fs::write(&path, b"not json").unwrap();
        assert!(harness_gate_at(&path, BOOT).is_ok());
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
