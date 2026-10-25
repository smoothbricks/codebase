//! The `ram-lease` operation's decisions and its record (specs/cowshed/05_gateway.md, "RAM disks").
//!
//! A RAM disk the gateway attaches is a lease its connection holds. The lease ends once: `keep`
//! hands the disk to its owner, and every other end (`release`, a line that is no end, the
//! connection ending or breaking with its owner gone) detaches it. The decisions are the pure
//! functions below; `control.rs` reads the connection and runs what they choose.
//!
//! Before an attach spawns, the gateway records its intent, the size asked, in a 0700 directory
//! beside its control socket; once the attach answers and its disk's registry entry is read, the
//! lease's record, named by that entry, replaces the intent, all before the owner hears of the
//! disk. The record goes once the lease is kept or its disk has left the registry. A gateway that
//! stops while it holds a lease leaves the record, and one that stops mid-attach (aborted, killed,
//! or past its drain) leaves the intent. The next gateway's startup scan reads both before its
//! control socket serves: it detaches each recorded lease's disk, and for each intent, every disk
//! of that size this user attached that nothing mounts and no record names.

use std::collections::BTreeMap;
use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use cowshed_gateway_types::{
    RamAttachRefusal, RamDetached, RamDiskAttached, RamDiskDevice, RamDiskEntry, RamDiskSectors,
    RamDisks, RamLeaseEnd, RamLeaseEndLine,
};

/// How a held lease's connection ended, as the connection read it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Ending {
    /// Its owner wrote this end line.
    Said(RamLeaseEnd),
    /// Its owner wrote a line that is no end.
    Garbled,
    /// The connection ended or broke before any line: its owner is gone. A lease a previous
    /// gateway left recorded ended this way, with that gateway's connections.
    Gone,
}

impl Ending {
    /// What the one line after the attach answer says; `None` when no line came.
    pub(crate) fn read(line: Option<&[u8]>) -> Self {
        match line {
            None => Self::Gone,
            Some(line) => serde_json::from_slice::<RamLeaseEndLine>(line)
                .map_or(Self::Garbled, |line| Self::Said(line.end)),
        }
    }
}

/// What the gateway does with a lease once it ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Settlement {
    /// Its owner keeps the disk: forget the record, then answer `ok` ([`after_forget`]).
    Forget,
    /// Detach the disk, then reply.
    Detach(Reply),
}

/// Who hears a detached lease's outcome, and how.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Reply {
    /// Its owner asked for the detach: its `ramDetach`.
    Outcome,
    /// Its owner wrote no end: `invalid-request`, naming the outcome.
    Invalid,
    /// The gateway could not keep the lease (its record could not be written or forgotten):
    /// `rejected`, naming the outcome.
    Rejected,
    /// Nobody is left to answer.
    Nobody,
}

/// What a lease's ending makes the gateway do.
pub(crate) const fn settle(ending: Ending) -> Settlement {
    match ending {
        Ending::Said(RamLeaseEnd::Keep) => Settlement::Forget,
        Ending::Said(RamLeaseEnd::Release) => Settlement::Detach(Reply::Outcome),
        Ending::Garbled => Settlement::Detach(Reply::Invalid),
        Ending::Gone => Settlement::Detach(Reply::Nobody),
    }
}

/// After a keep's record was forgotten, or not: `None` answers `ok`. A record that stays would
/// have the next gateway's startup scan detach a disk its owner uses, so a keep that cannot forget
/// it detaches the disk and refuses the keep instead.
pub(crate) const fn after_forget(forgotten: bool) -> Option<Reply> {
    if forgotten {
        None
    } else {
        Some(Reply::Rejected)
    }
}

/// What becomes of a lease's record once its disk's detach was tried.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Fate {
    /// The entry is no longer registered: the lease is over.
    Remove,
    /// The entry is still registered, with or without its whole disk, or the registry could not be
    /// read: the record waits for the next gateway's startup scan, never for a timer.
    Keep,
}

/// A detach's outcome alone never settles a record (an `unknown` one may have detached; a
/// `terminal` failure or a `not-started` refusal did not): the entry's presence in the registry
/// right after it does. An entry id is never reused, so only its absence proves the disk gone.
pub(crate) fn fate(registered: &io::Result<bool>) -> Fate {
    match registered {
        Ok(false) => Fate::Remove,
        Ok(true) | Err(_) => Fate::Keep,
    }
}

/// The disks a previous gateway's unanswered attach may have left: of `unmounted`, the disks of an
/// intent's size this user attached that nothing mounts, those no lease record names (a recorded
/// one is detached as its lease).
pub(crate) fn strays(
    recorded: &[RamDiskEntry],
    unmounted: &[RamDiskAttached],
) -> Vec<RamDiskAttached> {
    unmounted
        .iter()
        .filter(|disk| !recorded.contains(&disk.registry_entry))
        .cloned()
        .collect()
}

/// The gateway's records of its RAM disk leases, in a 0700 directory beside its control socket: an
/// intent, `<pid>-<n>.intent` holding the sectors asked, from before an attach spawns until its
/// lease is recorded or proven never made; a lease, `<entry>` holding its whole disk, from before
/// its owner hears of it until it is kept or its disk has left the registry. Every file is written
/// whole under `.<name>.new` and renamed into place, so none is read half written.
#[derive(Clone, Debug)]
pub(crate) struct Records {
    dir: PathBuf,
    next: Arc<AtomicU64>,
}

/// An attach's recorded intent.
#[derive(Debug)]
pub(crate) struct Intent(PathBuf);

/// What a previous gateway left recorded, as the startup scan reads it.
#[derive(Debug, Default)]
pub(crate) struct Left {
    pub(crate) leases: Vec<RamDiskAttached>,
    pub(crate) intents: Vec<(PathBuf, RamDiskSectors)>,
    /// `.new` files a writer died before renaming: their intent, written first, still stands.
    scraps: Vec<PathBuf>,
    /// Files that name no lease or intent, each with why: said and left in place.
    unread: Vec<String>,
}

impl Left {
    pub(crate) fn is_empty(&self) -> bool {
        self.leases.is_empty() && self.intents.is_empty()
    }
}

impl Records {
    pub(crate) fn beside(control_socket: &Path) -> Self {
        Self {
            dir: control_socket.parent().map_or_else(
                || PathBuf::from("ram-leases"),
                |parent| parent.join("ram-leases"),
            ),
            next: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Write `body` whole as `name`, durably: under a scratch name first, then renamed into place.
    fn publish(&self, name: &str, body: &str) -> io::Result<PathBuf> {
        match std::fs::DirBuilder::new().mode(0o700).create(&self.dir) {
            Err(error) if error.kind() != io::ErrorKind::AlreadyExists => return Err(error),
            _ => {}
        }
        let scratch = self.dir.join(format!(".{name}.new"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&scratch)?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
        let path = self.dir.join(name);
        std::fs::rename(&scratch, &path)?;
        Ok(path)
    }

    /// Record the intent of an attach of `sectors`, before it spawns.
    pub(crate) fn intend(&self, sectors: RamDiskSectors) -> io::Result<Intent> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let name = format!("{}-{n}.intent", std::process::id());
        self.publish(&name, &sectors.get().to_string()).map(Intent)
    }

    /// Record `lease` before its owner hears of it.
    pub(crate) fn record(&self, lease: &RamDiskAttached) -> io::Result<()> {
        self.publish(&lease.registry_entry.to_string(), lease.device.as_str())
            .map(drop)
    }

    /// Remove `intent`: its lease is recorded, or no disk of its attach is left. One that cannot
    /// be removed is said; the next startup scan finds nothing of it to detach and removes it.
    pub(crate) fn settled(&self, intent: Intent) {
        if let Err(error) = std::fs::remove_file(&intent.0) {
            cowshed_os::eprintln!(
                "cowshed: ram-lease intent {} could not be removed ({error}); the next startup scan removes it",
                intent.0.display()
            );
        }
    }

    /// Forget the lease of `entry`; one already forgotten is.
    pub(crate) fn forget(&self, entry: RamDiskEntry) -> io::Result<()> {
        match std::fs::remove_file(self.dir.join(entry.to_string())) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    /// Everything recorded here, read by the startup scan before the control socket serves, so
    /// every file is a previous gateway's.
    pub(crate) fn left(&self) -> io::Result<Left> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Left::default()),
            Err(error) => return Err(error),
        };
        let mut left = Left::default();
        for entry in entries {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                left.unread
                    .push(format!("{}: its name is not UTF-8", path.display()));
                continue;
            };
            if name.starts_with('.') && name.ends_with(".new") {
                left.scraps.push(path);
                continue;
            }
            let body = match std::fs::read_to_string(&path) {
                Ok(body) => body,
                Err(error) => {
                    left.unread.push(format!("{}: {error}", path.display()));
                    continue;
                }
            };
            let read = if name.ends_with(".intent") {
                body.parse::<std::num::NonZeroU64>()
                    .map(|sectors| {
                        left.intents
                            .push((path.clone(), RamDiskSectors::new(sectors)))
                    })
                    .map_err(|error| format!("its sectors {body:?} do not parse ({error})"))
            } else {
                RamDiskEntry::parse(name)
                    .map_err(|error| error.to_string())
                    .and_then(|registry_entry| {
                        RamDiskDevice::parse(&body)
                            .map(|device| {
                                left.leases.push(RamDiskAttached {
                                    device,
                                    registry_entry,
                                })
                            })
                            .map_err(|error| error.to_string())
                    })
            };
            if let Err(why) = read {
                left.unread.push(format!("{}: {why}", path.display()));
            }
        }
        Ok(left)
    }
}

/// Why an attach answers no lease.
#[derive(Debug)]
pub(crate) enum Refused {
    /// Its intent could not be recorded, so nothing was attached.
    Unintended(io::Error),
    /// The attach was refused or failed. One that failed may still have made a disk; its intent
    /// stays for the next gateway's startup scan.
    Attach(RamAttachRefusal),
    /// The disk was attached but its lease could not be recorded: it is detached again, and the
    /// attach refused, so no disk leaves the gateway unrecorded.
    Unrecorded {
        error: io::Error,
        detached: RamDetached,
    },
}

/// Record the intent, attach, and record the lease before anyone hears of the disk. Blocking: run
/// on a thread no connection owns. Whatever stops this thread part way, its intent or its record
/// is on disk for the next gateway's startup scan.
pub(crate) fn attach(
    disks: &dyn RamDisks,
    records: &Records,
    sectors: RamDiskSectors,
) -> Result<RamDiskAttached, Refused> {
    let intent = records.intend(sectors).map_err(Refused::Unintended)?;
    match disks.attach(sectors) {
        Ok(lease) => match records.record(&lease) {
            Ok(()) => {
                records.settled(intent);
                Ok(lease)
            }
            Err(error) => {
                let why = format!("its record could not be written ({error})");
                let (detached, fate) = detach(disks, records, &lease, &why);
                if fate == Fate::Remove {
                    records.settled(intent);
                }
                Err(Refused::Unrecorded { error, detached })
            }
        },
        // Refused before any spawn: no disk.
        Err(refusal @ RamAttachRefusal::TooLarge { .. }) => {
            records.settled(intent);
            Err(Refused::Attach(refusal))
        }
        Err(refusal @ RamAttachRefusal::Failed(_)) => Err(Refused::Attach(refusal)),
    }
}

/// Detach `lease`'s disk, settle its record by the entry's presence right after ([`fate`]), and
/// say both on stderr with `why` the lease ended. Blocking: run on a thread no connection owns.
pub(crate) fn detach(
    disks: &dyn RamDisks,
    records: &Records,
    lease: &RamDiskAttached,
    why: &str,
) -> (RamDetached, Fate) {
    let detached = disks.detach(
        &lease.device,
        lease.registry_entry,
        cowshed_core::api::dto::MAX_INLINE_OUTPUT_BYTES,
    );
    let registered = disks.registered(lease.registry_entry);
    let fate = fate(&registered);
    let record = match (fate, &registered) {
        (Fate::Remove, _) => match records.forget(lease.registry_entry) {
            Ok(()) => "its record is removed".to_owned(),
            Err(error) => format!("its record could not be removed ({error})"),
        },
        (Fate::Keep, Ok(_)) => {
            "the entry is still registered; its record waits for the next gateway's startup scan"
                .to_owned()
        }
        (Fate::Keep, Err(error)) => format!(
            "the registry could not be read ({error}); its record waits for the next gateway's startup scan"
        ),
    };
    cowshed_os::eprintln!(
        "cowshed: ram-lease {} entry={} {why}: detach outcome={detached}; {record}",
        lease.device,
        lease.registry_entry
    );
    (detached, fate)
}

/// The startup scan, before the control socket serves: read what a previous gateway left, and the
/// disks of each intent's size that nothing mounts, so none of this gateway's own leases is among
/// them; then detach each recorded lease and each stray ([`strays`]) on one blocking thread,
/// whose task is answered for a caller that waits for it (the gateway does not). An intent goes
/// once every stray of its size has left the registry.
pub(crate) fn sweep(
    disks: Option<Arc<dyn RamDisks>>,
    records: &Records,
) -> Option<tokio::task::JoinHandle<()>> {
    let left = match records.left() {
        Ok(left) => left,
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: ram-lease startup scan could not read {}: {error}",
                records.dir.display()
            );
            return None;
        }
    };
    for why in &left.unread {
        cowshed_os::eprintln!("cowshed: ram-lease startup scan left a file in place: {why}");
    }
    for scrap in &left.scraps {
        if let Err(error) = std::fs::remove_file(scrap) {
            cowshed_os::eprintln!(
                "cowshed: ram-lease startup scan could not remove {}: {error}",
                scrap.display()
            );
        }
    }
    if left.is_empty() {
        return None;
    }
    let Some(disks) = disks else {
        cowshed_os::eprintln!(
            "cowshed: ram-lease startup scan: {} lease(s) and {} intent(s) in {} wait for a gateway with a disk runner",
            left.leases.len(),
            left.intents.len(),
            records.dir.display()
        );
        return None;
    };
    let recorded: Vec<RamDiskEntry> = left
        .leases
        .iter()
        .map(|lease| lease.registry_entry)
        .collect();
    let mut sizes: BTreeMap<u64, (RamDiskSectors, Vec<PathBuf>)> = BTreeMap::new();
    for (path, sectors) in left.intents {
        sizes
            .entry(sectors.get())
            .or_insert_with(|| (sectors, Vec::new()))
            .1
            .push(path);
    }
    let mut strayed = Vec::new();
    for (sectors, intents) in sizes.into_values() {
        match disks.unmounted(sectors) {
            Ok(unmounted) => strayed.push((intents, strays(&recorded, &unmounted))),
            Err(error) => cowshed_os::eprintln!(
                "cowshed: ram-lease startup scan could not list the RAM disks of {sectors} ({error}); their {} intent(s) wait for the next start",
                intents.len()
            ),
        }
    }
    let records = records.clone();
    Some(tokio::task::spawn_blocking(move || {
        for lease in &left.leases {
            detach(&*disks, &records, lease, "was left by a previous gateway");
        }
        for (intents, strays) in strayed {
            let mut settled = true;
            for stray in &strays {
                let why = "was attached for a lease a previous gateway never answered";
                settled &= detach(&*disks, &records, stray, why).1 == Fate::Remove;
            }
            if settled {
                intents
                    .into_iter()
                    .for_each(|intent| records.settled(Intent(intent)));
            }
        }
    }))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_is_asked_answered_and_ended_in_one_line_each() {
        let sectors = RamDiskSectors::new(std::num::NonZeroU64::new(16).unwrap());
        assert_eq!(
            serde_json::to_string(&cowshed_gateway_types::RamLeaseRequest::new(sectors)).unwrap(),
            r#"{"op":"ram-lease","sectors":16}"#
        );
        let attached = RamDiskAttached {
            device: RamDiskDevice::parse("/dev/disk9").unwrap(),
            registry_entry: RamDiskEntry::parse("0x9").unwrap(),
        };
        let wire = r#"{"device":"/dev/disk9","registryEntry":"0x9"}"#;
        assert_eq!(serde_json::to_string(&attached).unwrap(), wire);
        assert_eq!(
            serde_json::from_str::<RamDiskAttached>(wire).unwrap(),
            attached
        );
        for (end, line) in [
            (RamLeaseEnd::Keep, r#"{"end":"keep"}"#),
            (RamLeaseEnd::Release, r#"{"end":"release"}"#),
        ] {
            assert_eq!(
                serde_json::to_string(&RamLeaseEndLine { end }).unwrap(),
                line
            );
            assert_eq!(
                serde_json::from_str::<RamLeaseEndLine>(line).unwrap().end,
                end
            );
        }
    }
    #[test]
    fn a_lease_ends_kept_only_by_its_owners_keep_and_every_other_end_detaches() {
        let table = [
            (
                Some(&br#"{"end":"keep"}"#[..]),
                Ending::Said(RamLeaseEnd::Keep),
                Settlement::Forget,
            ),
            (
                Some(&b"{\"end\":\"keep\"}\n"[..]),
                Ending::Said(RamLeaseEnd::Keep),
                Settlement::Forget,
            ),
            (
                Some(&br#"{"end":"release"}"#[..]),
                Ending::Said(RamLeaseEnd::Release),
                Settlement::Detach(Reply::Outcome),
            ),
            (
                Some(&br#"{"end":"kept"}"#[..]),
                Ending::Garbled,
                Settlement::Detach(Reply::Invalid),
            ),
            (
                Some(&br#"{"end":"keep","force":true}"#[..]),
                Ending::Garbled,
                Settlement::Detach(Reply::Invalid),
            ),
            (
                Some(&br#"keep"#[..]),
                Ending::Garbled,
                Settlement::Detach(Reply::Invalid),
            ),
            (
                Some(&b""[..]),
                Ending::Garbled,
                Settlement::Detach(Reply::Invalid),
            ),
            (None, Ending::Gone, Settlement::Detach(Reply::Nobody)),
        ];
        for (line, ending, settlement) in table {
            assert_eq!(Ending::read(line), ending, "{line:?}");
            assert_eq!(settle(ending), settlement, "{ending:?}");
        }
    }

    #[test]
    fn a_lease_the_gateway_cannot_record_or_forget_is_detached_and_refused() {
        assert_eq!(after_forget(true), None);
        assert_eq!(after_forget(false), Some(Reply::Rejected));
    }

    #[test]
    fn a_record_goes_only_once_its_entry_has_left_the_registry() {
        assert_eq!(fate(&Ok(false)), Fate::Remove);
        assert_eq!(fate(&Ok(true)), Fate::Keep);
        assert_eq!(fate(&Err(io::Error::other("unreadable"))), Fate::Keep);
    }

    #[test]
    fn an_unanswered_attachs_strays_are_the_unmounted_disks_no_record_names() {
        let disk = |n: u64| RamDiskAttached {
            device: RamDiskDevice::parse(&format!("/dev/disk{n}")).unwrap(),
            registry_entry: RamDiskEntry::new(std::num::NonZeroU64::new(n).unwrap()),
        };
        let entry = |n: u64| disk(n).registry_entry;
        assert_eq!(strays(&[], &[]), vec![]);
        assert_eq!(strays(&[], &[disk(3)]), vec![disk(3)]);
        assert_eq!(strays(&[entry(3)], &[disk(3), disk(4)]), vec![disk(4)]);
        assert_eq!(strays(&[entry(3), entry(4)], &[disk(3), disk(4)]), vec![]);
        assert_eq!(strays(&[entry(9)], &[disk(3)]), vec![disk(3)]);
    }
}
