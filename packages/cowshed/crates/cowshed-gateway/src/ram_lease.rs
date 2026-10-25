//! The `ram-lease` operation's decisions and its record (specs/cowshed/05_gateway.md, "RAM disks").
//!
//! A RAM disk the gateway attaches is a lease its connection holds. The lease ends once: `keep`
//! hands the disk to its owner, and every other end (`release`, a line that is no end, the
//! connection ending or breaking with its owner gone) detaches it. The decisions are the pure
//! functions below; `control.rs` reads the connection and runs what they choose.
//!
//! The gateway records each lease before its owner hears of the disk, one file per lease beside its
//! control socket, and removes the file once the lease is kept or its disk has left the registry.
//! A gateway that stops while it holds a lease, its connection gone with it, leaves the file; the
//! next gateway's startup scan reads every file before its control socket serves and detaches each
//! disk, since no owner of a previous gateway's lease is left to end it.

use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    /// The entry no longer registers the disk: the lease is over.
    Remove,
    /// The disk is still registered, or the registry could not be read: the record waits for the
    /// next gateway's startup scan, never for a timer.
    Keep,
}

/// A detach's outcome alone never settles a record (an `unknown` one may have detached; a
/// `terminal` failure did not): the registry's answer right after it does.
pub(crate) fn fate(registered: &io::Result<bool>) -> Fate {
    match registered {
        Ok(false) => Fate::Remove,
        Ok(true) | Err(_) => Fate::Keep,
    }
}

/// The leases the gateway holds, one file each in a 0700 directory beside its control socket: named
/// by the disk's registry entry, holding its whole disk. A file exists from before its owner hears
/// of the disk until the lease is kept or the disk has left the registry.
#[derive(Clone, Debug)]
pub(crate) struct Records {
    dir: PathBuf,
}

impl Records {
    pub(crate) fn beside(control_socket: &Path) -> Self {
        Self {
            dir: control_socket.parent().map_or_else(
                || PathBuf::from("ram-leases"),
                |parent| parent.join("ram-leases"),
            ),
        }
    }

    fn path(&self, entry: RamDiskEntry) -> PathBuf {
        self.dir.join(entry.to_string())
    }

    /// Record `lease`, durably, before its owner hears of it.
    pub(crate) fn record(&self, lease: &RamDiskAttached) -> io::Result<()> {
        match std::fs::DirBuilder::new().mode(0o700).create(&self.dir) {
            Err(error) if error.kind() != io::ErrorKind::AlreadyExists => return Err(error),
            _ => {}
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.path(lease.registry_entry))?;
        file.write_all(lease.device.as_str().as_bytes())?;
        file.sync_all()
    }

    /// Forget the lease of `entry`; one already forgotten is.
    pub(crate) fn forget(&self, entry: RamDiskEntry) -> io::Result<()> {
        match std::fs::remove_file(self.path(entry)) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    /// Every lease recorded here: read by the startup scan before the control socket serves, so
    /// every one is a previous gateway's. A file that names no lease is said and left in place.
    pub(crate) fn left(&self) -> io::Result<Vec<Result<RamDiskAttached, String>>> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut left = Vec::new();
        for entry in entries {
            let path = entry?.path();
            let lease = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| RamDiskEntry::parse(name).ok())
                .ok_or_else(|| "its name is no registry entry".to_owned())
                .and_then(|registry_entry| {
                    let text = std::fs::read_to_string(&path).map_err(|error| error.to_string())?;
                    RamDiskDevice::parse(&text)
                        .map(|device| RamDiskAttached {
                            device,
                            registry_entry,
                        })
                        .map_err(|error| error.to_string())
                });
            left.push(lease.map_err(|why| format!("{}: {why}", path.display())));
        }
        Ok(left)
    }
}

/// Why an attach answers no lease.
#[derive(Debug)]
pub(crate) enum Refused {
    Attach(RamAttachRefusal),
    /// The disk was attached but could not be recorded: it is detached again, and the attach refused, so no disk leaves the gateway unrecorded.
    Unrecorded {
        error: io::Error,
        detached: RamDetached,
    },
}

/// Attach a disk and record it before anyone hears of it. Blocking: run on a thread no connection
/// owns, so a connection cut off mid-attach still records the disk it made.
pub(crate) fn attach(
    disks: &dyn RamDisks,
    records: &Records,
    sectors: RamDiskSectors,
) -> Result<RamDiskAttached, Refused> {
    let lease = disks.attach(sectors).map_err(Refused::Attach)?;
    match records.record(&lease) {
        Ok(()) => Ok(lease),
        Err(error) => {
            let why = format!("its record could not be written ({error})");
            Err(Refused::Unrecorded {
                detached: detach(disks, records, &lease, &why),
                error,
            })
        }
    }
}

/// Detach `lease`'s disk, settle its record by the registry right after ([`fate`]), and say both
/// on stderr with `why` the lease ended. Blocking: run on a thread no connection owns.
pub(crate) fn detach(
    disks: &dyn RamDisks,
    records: &Records,
    lease: &RamDiskAttached,
    why: &str,
) -> RamDetached {
    let detached = disks.detach(
        &lease.device,
        lease.registry_entry,
        cowshed_core::api::dto::MAX_INLINE_OUTPUT_BYTES,
    );
    let registered = disks.registers(&lease.device, lease.registry_entry);
    let record = match (fate(&registered), &registered) {
        (Fate::Remove, _) => match records.forget(lease.registry_entry) {
            Ok(()) => "its record is removed".to_owned(),
            Err(error) => format!("its record could not be removed ({error})"),
        },
        (Fate::Keep, Ok(_)) => {
            "the disk is still registered; its record waits for the next gateway's startup scan"
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
    detached
}

/// The startup scan: detach every lease a previous gateway left recorded. `left` is read before
/// the control socket serves; the detaches run on one blocking thread, whose task is answered for a
/// caller that waits for it (the gateway does not).
pub(crate) fn sweep(
    disks: Option<Arc<dyn RamDisks>>,
    records: &Records,
    left: io::Result<Vec<Result<RamDiskAttached, String>>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let left = match left {
        Ok(left) if left.is_empty() => return None,
        Ok(left) => left,
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: ram-lease startup scan could not read {}: {error}",
                records.dir.display()
            );
            return None;
        }
    };
    let Some(disks) = disks else {
        cowshed_os::eprintln!(
            "cowshed: ram-lease startup scan: {} lease(s) in {} wait for a gateway with a disk runner",
            left.len(),
            records.dir.display()
        );
        return None;
    };
    let records = records.clone();
    Some(tokio::task::spawn_blocking(move || {
        for lease in left {
            match lease {
                Ok(lease) => {
                    detach(&*disks, &records, &lease, "was left by a previous gateway");
                }
                Err(why) => cowshed_os::eprintln!(
                    "cowshed: ram-lease startup scan left a record in place: {why}"
                ),
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
    fn a_record_goes_only_once_the_registry_no_longer_lists_its_disk() {
        assert_eq!(fate(&Ok(false)), Fate::Remove);
        assert_eq!(fate(&Ok(true)), Fate::Keep);
        assert_eq!(fate(&Err(io::Error::other("unreadable"))), Fate::Keep);
    }
}
