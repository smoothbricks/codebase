//! The host's disk-image attach pool (01a_apfs.md, "How the APFS host degrades" §2;
//! 08_testing.md, "Host limit").
//!
//! Every `diskutil image attach` creates an `AppleDiskImageDevice`, its `DIDeviceIOUserClient`
//! and its request pool. Most leave the kernel with their detach; some stay alive after they have
//! left the registry: still counted among their class's instances, no longer registered. The
//! pool runs out of the instances, live and leaked alike, together with the mappings orphaned
//! `diskimagesiod` helpers hold ([`crate::disk_image_helpers`]): then every attach, `diskutil
//! image info` and cold mount fails with "error code 150" (`kIOReturnNoResources`). Root frees
//! orphans without a reboot; only a reboot frees leaked devices.
//!
//! Both sides are read unprivileged and without attaching anything: the instance counts are the
//! registry root's `IOKitDiagnostics` class counts, which `ioclasscount` prints, and the
//! registered counts are the registry's own entries. A device instance with no registered entry
//! is leaked. The same subtraction for `AppleAPFSVolume` and `IOMedia` counts departed volumes
//! and media the kernel has not freed ([`Departed`]). They take no slot in the pool: `doctor`
//! reports them as a finding of their own, and what holds them is attributed by registry ID, not
//! assumed (01a_apfs.md, "Spotlight media references").
//!
//! Two gates refuse an image attach before it spawns, so none is ever spent into a pool that
//! cannot take it, and a failed attach, which leaks one more device, never happens on purpose:
//! a boot that has met error code 150 is spent until the operator clears it, and an attach whose
//! device would bring the instances to [`POOL_CEILING`] is refused by that name.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The framework's report when the attach pool is spent, as `diskutil image` prints it:
/// "Error: Disk image operation failed with error code 150".
const POOL_EXHAUSTED: &[u8] = b"error code 150";

/// A conservative admission bound, not a documented limit: the kernel's IO-client allocation at the
/// earliest error code 150 observed. In the boot of 2026-10-21 20:12 +04 the first 150 (06:01:48)
/// struck with 271 images registered, 963 `DIDeviceIOUserClient` and 972 `AppleDiskImageDevice`
/// instances allocated; each IO client maps 36 shared 2 MiB buffers, which only its `free()`
/// returns. Counting image devices, never fewer than their clients, against it errs low. An attach
/// is admitted only while the instances it and the storage phase's other members may add stay
/// below it.
pub const POOL_CEILING: u64 = 963;

/// The image devices one admitted attach reserves: itself and every other member of its storage
/// phase, each of which may pass this gate before any of them spawns (05_gateway.md,
/// "Disk-lifecycle lease"), so concurrent attaches cannot together cross the ceiling.
pub fn attach_reserve() -> u64 {
    u64::from(cowshed_gateway_types::DISK_PHASE_MEMBERS.get())
}

/// What frees leaked image devices.
pub const REBOOT_HINT: &str =
    "reboot the host: only a reboot frees leaked disk-image devices (sudo shutdown -r now)";

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

/// What clears a 150 the leaked devices do not explain.
pub const ORPHANS_FIRST_HINT: &str = "kill the orphaned diskimagesiod helpers `cowshed doctor` \
     names (orphaned-disk-image-helpers), then `cowshed doctor --clear-attach-budget`; reboot if attaches still \
     fail with error code 150";

/// How the lane's checkout runs the operator's reset ([`clear`]): the bin is built from the
/// command a hint prints is one that parses and runs as printed.
pub const CLEAR_COMMAND: &str = "cowshed doctor --clear-attach-budget";

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
    /// The departed volumes and media the kernel has not freed, from the first of either, or
    /// `None` when no `AppleAPFSVolume` or `IOMedia` instance outlives its registry entry.
    pub fn departed(&self) -> Option<Departed> {
        let departed = Departed {
            volumes: self.apfs_volumes.leaked(),
            media: self.media.leaked(),
        };
        (departed.volumes > 0 || departed.media > 0).then_some(departed)
    }
}

/// Departed APFS volumes and media the kernel has not freed: instances no registered entry
/// accounts for. They take no attach-pool slot, so they are no part of the pool's headroom
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
    /// Whether the image devices at this 150 alone account for it: the instances had reached
    /// [`POOL_CEILING`].
    fn devices_full(&self) -> bool {
        self.image_devices
            .is_some_and(|devices| devices.instances >= POOL_CEILING)
    }
}

/// Disk-image commands killed at a hard deadline during one boot: each probably left its device
/// leaked. Cowshed no longer kills one (01a_apfs.md, "No disk-image command is killed"); the
/// count is what an earlier build recorded this boot, still reported until the boot ends.
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
    /// The boot whose pool is spent, until the operator clears it. A later success does not
    /// clear it: an attach admitted before the first 150 may still finish after it, and the boot
    /// stays spent all the same.
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
        Self {
            last_exhaustion: Some(Exhaustion {
                boot: boot.clone(),
                image_devices,
                orphaned_helpers,
            }),
            spent_session: Some(boot.session.clone()),
            probable_leaks: self.probable_leaks,
        }
    }

    /// The record after the operator cleared it (`cowshed doctor --clear-attach-budget`), once
    /// they freed what spent the pool: this boot's exhaustion ends. The last exhaustion stays as
    /// evidence.
    pub fn cleared(self, session: &str) -> Self {
        Self {
            spent_session: self.spent_session.filter(|spent| spent != session),
            ..self
        }
    }

    /// The record after a disk-image command was killed during `session`, as an earlier build
    /// wrote it. Nothing in production kills one any more; tests build such a record.
    #[cfg(test)]
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

    /// The pool during `session`, as `reading` and this record prove it.
    pub fn budget(&self, session: &str, reading: KernelReading) -> Budget {
        Budget {
            reading,
            spent: self.spent(session).cloned(),
            probable_leaks: self
                .probable_leaks
                .as_ref()
                .filter(|leaks| leaks.session == session)
                .map_or(0, |leaks| leaks.killed),
        }
    }
}

/// The pool for the current boot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Budget {
    pub reading: KernelReading,
    /// This boot's first 150, until the operator clears it.
    pub spent: Option<Exhaustion>,
    /// Disk-image commands an earlier build killed at its hard deadline this boot.
    pub probable_leaks: u64,
}

impl Budget {
    /// Image devices the pool can still take below [`POOL_CEILING`], live and leaked alike.
    pub fn headroom(&self) -> u64 {
        POOL_CEILING
            .saturating_sub(1)
            .saturating_sub(self.reading.image_devices.instances)
    }

    /// Whether `need` more image devices fit: refused once this boot's pool is spent, or when
    /// the headroom is below `need`.
    pub fn admits(&self, need: u64) -> Result<(), AttachPoolExhausted> {
        let instances = self.reading.image_devices.instances;
        if self.spent.is_some() || instances.saturating_add(need) >= POOL_CEILING {
            return Err(AttachPoolExhausted {
                need,
                headroom: Some(self.headroom()),
                detail: self.to_string(),
                remedy: self.remedy_for_refusal(),
            });
        }
        Ok(())
    }

    /// What frees the pool, which a refusal always names: the orphaned helpers first when the
    /// image devices do not account for a spent boot, otherwise a reboot, the one thing that
    /// frees leaked image devices. Departed volumes and media take no slot, so they name nothing.
    fn remedy_for_refusal(&self) -> String {
        if self.spent.is_some() && !self.devices_account() {
            ORPHANS_FIRST_HINT.to_owned()
        } else {
            REBOOT_HINT.to_owned()
        }
    }

    /// Whether the image devices alone account for the pool's state: they fill it now, or they
    /// filled it at this boot's recorded 150.
    fn devices_account(&self) -> bool {
        self.headroom() == 0 || self.spent.as_ref().is_some_and(Exhaustion::devices_full)
    }

    /// Whether `doctor` should warn about the pool: spent, within a quarter of the ceiling, or
    /// with commands an earlier build killed this boot.
    pub fn is_low(&self) -> bool {
        self.spent.is_some() || self.headroom() <= POOL_CEILING / 4 || self.probable_leaks > 0
    }

    /// What frees what leaked.
    pub fn remedy(&self) -> String {
        if self.spent.is_some() || self.headroom() <= POOL_CEILING / 4 {
            self.remedy_for_refusal()
        } else {
            "nothing to free".to_owned()
        }
    }
}

impl fmt::Display for Budget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reading = &self.reading;
        if let Some(spent) = &self.spent {
            let spent = Spent {
                exhaustion: spent,
                devices_full: self.devices_account(),
            };
            write!(f, "{spent}. ")?;
        }
        write!(
            f,
            "AppleDiskImageDevice {}; DIDeviceIOUserClient {}; AppleAPFSVolume {}; IOMedia {}. \
             Ceiling {POOL_CEILING} image-device instances, live and leaked (the IO clients allocated \
             at the earliest error code 150 observed), headroom {}",
            reading.image_devices,
            reading.user_clients,
            reading.apfs_volumes,
            reading.media,
            self.headroom()
        )?;
        if self.probable_leaks > 0 {
            write!(
                f,
                "; {} disk-image command(s) killed this boot by an earlier cowshed's hard \
                 deadline, each a probable leaked device",
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
    crate::private_user_directory(&directory, "attach-pool record directory")?;
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
            cowshed_os::eprintln!(
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
    let _lock = crate::locked_private_file(&path.with_extension("lock"))?;
    let (before, broken) = load_or_empty(path)?;
    let after = change(before.clone());
    if after == before && !broken {
        return Ok(after);
    }
    cowshed_os::fsio::publish_private_file(path, |writer| serde_json::to_writer(writer, &after))
        .map_err(|error| match error {
            cowshed_os::fsio::PublishError::Io { path, source } => {
                io::Error::new(source.kind(), format!("{}: {source}", path.display()))
            }
            cowshed_os::fsio::PublishError::Write(error) => io::Error::other(error),
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

/// Record a disk-image command killed during `session`, as an earlier build did.
#[cfg(test)]
pub fn record_killed_at(path: &Path, session: &str) -> io::Result<PoolRecord> {
    update(path, |record| record.killed(session))
}

/// The operator's reset ([`PoolRecord::cleared`]), once they freed what spent the pool: end this
/// boot's exhaustion. A record that does not parse is replaced.
pub fn clear() -> io::Result<PoolRecord> {
    clear_at(&record_path()?, &boot()?.session)
}

pub fn clear_at(path: &Path, session: &str) -> io::Result<PoolRecord> {
    update(path, |record| record.cleared(session))
}

/// Refuse `need` image devices the pool cannot take now.
///
/// Unlike [`observe`], a record whose bytes do not parse is an error here: it may have held this
/// boot's 150, and reading it as empty would admit attaches into a spent pool. The operator's
/// clear ([`clear`]) replaces it.
pub fn admit(need: u64) -> io::Result<Result<Budget, AttachPoolExhausted>> {
    admit_at(&record_path()?, &boot()?.session, read_kernel()?, need)
}

/// [`admit`] against the record at `path` during boot `session` with the kernel as `reading`.
pub fn admit_at(
    path: &Path,
    session: &str,
    reading: KernelReading,
    need: u64,
) -> io::Result<Result<Budget, AttachPoolExhausted>> {
    let budget = load(path)?.budget(session, reading);
    Ok(budget.admits(need).map(|()| budget))
}

/// A DiskImages2 attach: `diskutil image attach`, the one verb that creates an
/// `AppleDiskImageDevice` and so draws on this pool. A DI1 `hdiutil attach` creates an IOHDIX
/// drive and takes no slot in it.
fn is_disk_images2_attach(request: &crate::CommandRequest) -> bool {
    request.program == Path::new(crate::DISKUTIL)
        && request.args.first().is_some_and(|verb| verb == "image")
        && request.args.get(1).is_some_and(|verb| verb == "attach")
}

/// Refuse a DiskImages2 attach before it spawns when this boot's pool is spent or the attach's
/// reserve ([`attach_reserve`]) would bring the image devices to [`POOL_CEILING`]; every other
/// command passes. A pool that cannot be read refuses too: nothing says the attach would not
/// fail with 150, and a failed attach leaks a device, while a refused one can run again. A
/// record that cannot be read, or whose bytes do not parse, refuses as well ([`admit`]). A kernel
/// with no attach pool has nothing to gate.
pub(crate) fn refuse_attach_into_pool(
    request: &crate::CommandRequest,
) -> Result<(), crate::CommandRunError> {
    if !is_disk_images2_attach(request) {
        return Ok(());
    }
    let failure = match admit(attach_reserve()) {
        Ok(Ok(_)) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::Unsupported => return Ok(()),
        Ok(Err(exhausted)) => crate::CommandRunFailure::AttachPool(exhausted),
        Err(error) => crate::CommandRunFailure::PoolUnread(error),
    };
    Err(crate::CommandRunError {
        request: request.clone(),
        failure,
    })
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

    /// The ceiling counts image-device instances, live and leaked alike: a healthy host has the
    /// headroom up to it, a quarter of it left warns, and none left refuses with a reboot.
    #[test]
    fn the_ceiling_counts_live_and_leaked_image_devices_alike() {
        let budget = PoolRecord::default().budget(NEXT_BOOT, rebooted_host());
        assert_eq!(budget.headroom(), POOL_CEILING - 1 - 178);
        assert!(!budget.is_low());
        assert_eq!(budget.remedy(), "nothing to free");
        assert!(budget.admits(budget.headroom()).is_ok());
        assert!(budget.admits(budget.headroom() + 1).is_err());
        let report = budget.to_string();
        assert!(
            report.contains("Ceiling 963 image-device instances"),
            "{report}"
        );
        assert!(
            report.contains("AppleDiskImageDevice 178 instances, 178 registered, 0 leaked"),
            "{report}"
        );

        // 256 attached and 786 leaked: where the boot of 2026-10-21 first met 150.
        let full = PoolRecord::default().budget(
            NEXT_BOOT,
            KernelReading {
                image_devices: count(POOL_CEILING, 256),
                ..rebooted_host()
            },
        );
        assert_eq!(full.headroom(), 0);
        assert!(full.is_low());
        assert_eq!(full.remedy(), REBOOT_HINT);
        assert_eq!(full.admits(1).unwrap_err().remedy, REBOOT_HINT);
        let quarter = |instances| {
            PoolRecord::default().budget(
                NEXT_BOOT,
                KernelReading {
                    image_devices: count(instances, 10),
                    ..rebooted_host()
                },
            )
        };
        assert!(quarter(POOL_CEILING - 1 - POOL_CEILING / 4).is_low());
        assert!(!quarter(POOL_CEILING - 2 - POOL_CEILING / 4).is_low());
    }

    /// A 150 spends the boot until the operator clears it. A later successful attach does not
    /// clear it: one admitted before the 150 may still finish after it.
    #[test]
    fn a_150_spends_the_boot_until_the_operator_clears_it() {
        let record = spent_record();
        let exhaustion = Exhaustion {
            boot: boot(BOOT),
            image_devices: Some(count(1090, 66)),
            orphaned_helpers: Some(0),
        };
        assert_eq!(record.last_exhaustion, Some(exhaustion.clone()));
        let budget = record.budget(BOOT, rebooted_host());
        assert_eq!(budget.spent, Some(exhaustion));
        assert!(budget.is_low());
        let refused = budget.admits(0).expect_err("a spent boot admits nothing");
        assert_eq!(refused.remedy, REBOOT_HINT);
        let report = budget.to_string();
        assert!(
            report.starts_with("attach pool exhausted, reboot required"),
            "{report}"
        );
        assert!(
            report.contains("with 66 disk images attached and 1024 leaked image devices"),
            "{report}"
        );

        // A second 150 in the same boot keeps where the pool first ran out.
        assert_eq!(
            record
                .clone()
                .exhausted(&boot(BOOT), Some(count(2000, 10)), Some(0)),
            record
        );
        // The next boot is not spent.
        assert_eq!(record.budget(NEXT_BOOT, rebooted_host()).spent, None);

        // Clearing ends this boot's exhaustion and keeps the evidence; another boot's clear
        // changes nothing.
        let cleared = record.clone().cleared(BOOT);
        assert_eq!(cleared.spent_session, None);
        assert_eq!(cleared.last_exhaustion, record.last_exhaustion);
        assert!(cleared.budget(BOOT, rebooted_host()).admits(1).is_ok());
        assert_eq!(record.clone().cleared(NEXT_BOOT), record);
    }

    #[test]
    fn a_150_the_devices_do_not_account_for_names_the_orphans_first() {
        for orphans in [Some(10), None] {
            let record = PoolRecord::default().exhausted(&boot(BOOT), Some(count(96, 66)), orphans);
            let budget = record.budget(BOOT, leaking(30));
            assert!(budget.spent.is_some());
            assert_eq!(budget.remedy(), ORPHANS_FIRST_HINT);
            assert_eq!(budget.admits(1).unwrap_err().remedy, ORPHANS_FIRST_HINT);
            let report = budget.to_string();
            assert!(report.starts_with("attach pool exhausted: "), "{report}");
            assert!(!report.contains("reboot required"), "{report}");
        }
        // The kernel unread at the 150: the boot is spent all the same.
        let unread = PoolRecord::default().exhausted(&boot(BOOT), None, None);
        assert!(unread.spent(BOOT).is_some());
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

    /// Departed volumes and media take no slot in the pool: with or without them the headroom,
    /// the warning, the remedy and the gate read alike, and the first departed instance of either
    /// class is reported all the same.
    #[test]
    fn departed_volumes_never_spend_the_pool_and_the_first_is_reported() {
        let record = PoolRecord::default();
        let departed = |reading: KernelReading| KernelReading {
            apfs_volumes: count(1065, 81),
            media: count(1075, 80),
            ..reading
        };
        for leaked in [0, 700, 1024] {
            let without = record.budget(NEXT_BOOT, leaking(leaked));
            let with = record.budget(NEXT_BOOT, departed(leaking(leaked)));
            assert_eq!(with.headroom(), without.headroom(), "{leaked} leaked");
            assert_eq!(with.is_low(), without.is_low(), "{leaked} leaked");
            assert_eq!(with.remedy(), without.remedy(), "{leaked} leaked");
            for need in [1, 400] {
                assert_eq!(
                    with.admits(need).map_err(|refused| refused.remedy),
                    without.admits(need).map_err(|refused| refused.remedy),
                    "{leaked} leaked, {need} needed"
                );
            }
        }
        let healthy = record.budget(NEXT_BOOT, departed(rebooted_host()));
        assert_eq!(healthy.remedy(), "nothing to free");
        assert!(!healthy.is_low());
        assert!(healthy.admits(healthy.headroom()).is_ok());

        // The first departed instance of either class is counted, and moves nothing in the pool.
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
            let budget = record.budget(NEXT_BOOT, first);
            assert!(!budget.is_low());
            assert_eq!(budget.remedy(), "nothing to free");
            assert_eq!(budget.headroom(), POOL_CEILING - 1 - 178);
        }
        // Fewer instances than registered entries is no departure.
        let freed = KernelReading {
            apfs_volumes: count(192, 193),
            media: count(184, 185),
            ..rebooted_host()
        };
        assert_eq!(freed.departed(), None);
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

    /// Every refusal names a step that can free the pool. Between a quarter of the ceiling and
    /// the need, the doctor remedy has nothing to free, yet the gate refuses: it says reboot.
    #[test]
    fn a_refusal_for_low_headroom_says_reboot_even_when_doctor_would_not() {
        let budget = PoolRecord::default().budget(NEXT_BOOT, leaking(700));
        assert_eq!(budget.headroom(), POOL_CEILING - 1 - 710);
        assert_eq!(budget.remedy(), "nothing to free");
        let refused = budget.admits(400).expect_err("331 < 400");
        assert_eq!(refused.remedy, REBOOT_HINT);
        let volumes = KernelReading {
            apfs_volumes: count(1065, 81),
            ..leaking(700)
        };
        let refused = PoolRecord::default()
            .budget(NEXT_BOOT, volumes)
            .admits(400)
            .expect_err("331 < 400");
        // Departed volumes add nothing to a refusal: the remedy is the reboot alone.
        assert_eq!(refused.remedy, REBOOT_HINT);
    }

    /// The reset ends the spent boot on disk and keeps the last exhaustion as evidence.
    #[test]
    fn the_reset_ends_the_spent_boot_and_keeps_the_evidence() {
        let directory = scratch("clear");
        let path = directory.join("pool.json");
        let spent = spent_record();
        update(&path, |_| spent.clone()).unwrap();
        assert!(load(&path).unwrap().spent(BOOT).is_some());
        clear_at(&path, BOOT).unwrap();
        let cleared = load(&path).unwrap();
        assert!(cleared.spent(BOOT).is_none());
        assert_eq!(cleared.last_exhaustion, spent.last_exhaustion);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Admission reads the record strictly: bytes that do not parse may have held this boot's
    /// 150, so they refuse until the operator's clear replaces them; a spent record refuses
    /// whatever the kernel shows; the clear admits again. Nothing but the clear ends a spent boot.
    #[test]
    fn admission_refuses_a_corrupt_or_spent_record_until_the_clear() {
        let directory = scratch("admit");
        let path = directory.join("pool.json");
        assert!(matches!(
            admit_at(&path, BOOT, rebooted_host(), 1),
            Ok(Ok(_))
        ));
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(
            admit_at(&path, BOOT, rebooted_host(), 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        clear_at(&path, BOOT).unwrap();
        assert!(matches!(
            admit_at(&path, BOOT, rebooted_host(), 1),
            Ok(Ok(_))
        ));
        update(&path, |_| spent_record()).unwrap();
        assert!(matches!(
            admit_at(&path, BOOT, rebooted_host(), 1),
            Ok(Err(_))
        ));
        assert!(matches!(
            admit_at(&path, NEXT_BOOT, rebooted_host(), 1),
            Ok(Ok(_))
        ));
        clear_at(&path, BOOT).unwrap();
        assert!(matches!(
            admit_at(&path, BOOT, rebooted_host(), 1),
            Ok(Ok(_))
        ));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A record that does not parse proves nothing: it reads as empty, the next 150 is still
    /// recorded over it (so the boot latch holds), and the reset replaces it.
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
        assert!(load(&path).unwrap().spent(BOOT).is_some());

        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(clear_at(&path, BOOT).unwrap(), PoolRecord::default());
        assert_eq!(
            load(&path).unwrap(),
            PoolRecord::default(),
            "rewritten, not left broken"
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// The cascade the latch exists for: 26 attaches into a pool that answers 150 from the
    /// third on, and an attach admitted before that 150 that succeeds after it. Only the one
    /// failed attach is spent into the spent pool: the 23 after it are refused before
    /// attaching, the late success notwithstanding, and the refusal says reboot.
    #[test]
    fn the_latch_stops_every_attach_after_the_first_150() {
        let mut record = PoolRecord::default();
        let mut attaches = 0;
        let mut refused = Vec::new();
        for attach in 0..26 {
            if let Err(exhausted) = record
                .budget(BOOT, rebooted_host())
                .admits(attach_reserve())
            {
                refused.push(exhausted.to_string());
                continue;
            }
            attaches += 1;
            if attach >= 2 {
                record = record.exhausted(&boot(BOOT), Some(spent_host().image_devices), Some(0));
            }
        }
        assert_eq!(attaches, 3);
        assert_eq!(refused.len(), 23);
        assert!(
            refused[0].contains("attach pool exhausted, reboot required"),
            "{}",
            refused[0]
        );
        assert!(
            refused[0].ends_with(&format!("\nnext: {REBOOT_HINT}")),
            "{}",
            refused[0]
        );
        // After the reboot attaches run again.
        assert!(
            record
                .budget(NEXT_BOOT, rebooted_host())
                .admits(attach_reserve())
                .is_ok()
        );
    }

    /// Only a DiskImages2 attach is gated: a detach, an info read or a DI1 attach never reads
    /// the pool.
    #[test]
    fn only_an_image_attach_is_gated_by_the_pool() {
        for request in [
            crate::CommandRequest::new(crate::DISKUTIL, ["image", "info", "/x.dmg"]),
            crate::CommandRequest::new(crate::HDIUTIL, ["detach", "/dev/disk9"]),
            crate::CommandRequest::new(crate::DISKUTIL, ["info", "/dev/disk9"]),
            crate::CommandRequest::new(crate::HDIUTIL, ["attach", "-kernel", "/x.dmg"]),
        ] {
            assert!(refuse_attach_into_pool(&request).is_ok(), "{request:?}");
        }
    }

    proptest::proptest! {
        /// The admission decision is exactly: not spent, and every reserved device stays below
        /// the ceiling. It is monotone (more instances or a larger need never turns a refusal
        /// into an admission), ignores departed volumes and media, and every refusal names a
        /// step that frees the pool.
        #[test]
        fn admission_is_not_spent_and_below_the_ceiling(
            instances in 0u64..2048,
            registered in 0u64..1100,
            need in 0u64..64,
            departed in 0u64..4096,
            spent in proptest::bool::ANY,
            orphans in proptest::option::of(0u64..16),
        ) {
            let reading = KernelReading {
                image_devices: count(instances, registered),
                apfs_volumes: count(departed + 10, 10),
                media: count(departed + 10, 10),
                ..rebooted_host()
            };
            let record = if spent {
                PoolRecord::default().exhausted(&boot(BOOT), Some(count(instances, registered)), orphans)
            } else {
                PoolRecord::default()
            };
            let budget = record.budget(BOOT, reading);
            let admitted = budget.admits(need).is_ok();
            proptest::prop_assert_eq!(admitted, !spent && instances + need < POOL_CEILING);
            if let Err(refused) = budget.admits(need) {
                proptest::prop_assert!(
                    refused.remedy == REBOOT_HINT || refused.remedy == ORPHANS_FIRST_HINT
                );
            }
            let more = record.budget(BOOT, KernelReading {
                image_devices: count(instances + 1, registered),
                ..reading
            });
            proptest::prop_assert!(admitted || more.admits(need).is_err());
            proptest::prop_assert!(admitted || budget.admits(need + 1).is_err());
            let without_departed = record.budget(BOOT, KernelReading {
                apfs_volumes: rebooted_host().apfs_volumes,
                media: rebooted_host().media,
                ..reading
            });
            proptest::prop_assert_eq!(
                budget.admits(need).map_err(|refused| refused.remedy),
                without_departed.admits(need).map_err(|refused| refused.remedy)
            );
            // The record survives a cleared boot as evidence, and a clear admits what fits.
            let cleared = record.clone().cleared(BOOT);
            proptest::prop_assert_eq!(&cleared.last_exhaustion, &record.last_exhaustion);
            proptest::prop_assert_eq!(
                cleared.budget(BOOT, reading).admits(need).is_ok(),
                instances + need < POOL_CEILING
            );
        }
    }
}
