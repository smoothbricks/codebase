//! The process holding a cowshed lifecycle lease, as the lease file records it (02_workspaces.md,
//! "Holders cowshed does not run").
//!
//! Two leases carry the record: a workspace's intent lease (`sessions/<ws>.intent.lock`, held by
//! the process running a create, fork, rename, adoption or retirement of it) and its land lease
//! (`sessions/<ws>.land.lock`, held by a land or an `rm`). The holder writes the record after it
//! takes the lease's `flock(2)` and empties it before it lets go, so a record outlives its hold only
//! when its process ended without unwinding, and that process's unique id then names nothing.
//!
//! A process-table census asks nothing of a process such a record names ([`running_lifecycle`]).
//! Such a process can sit in one kernel call for a minute while it holds one of its descriptors'
//! vnodes: a fork's read-write mount of a new clone pays the clone's extent-map copy
//! (16_build_volumes.md), and every per-descriptor query of that process waits as long. It is no
//! foreign holder: the operation's own leases and image locks already refuse the operations a
//! census guards.

use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

// The kernel's unique id of the running process `pid` where the kernel gives one: `None` once
// it has exited, and always on Linux, which records none.
use platform::unique_id;

/// The process a lease file names. Read from either lease, so a land lease's `holding` is not one
/// of its fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Process {
    pub pid: u32,
    /// The kernel's unique id of the process (`p_uniqueid`), which no later process given the same
    /// pid has, as its exact decimal string. Absent where the platform has none. A record an
    /// earlier build wrote with a number does not parse: it names nothing and proves nothing, and
    /// no reader takes a pid from it (02_workspaces.md, "Holders cowshed does not run").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<crate::api::held_lock::DecimalU64>,
    /// When it took the lease, as the deletion log writes times.
    pub since: String,
    /// Its argument vector joined by spaces, each argument decoded lossily: what a person reads,
    /// never an identity. The pid and unique id name the process.
    pub command: String,
}

impl Process {
    /// This process, holding a lease from now. Its arguments are read as the kernel holds them, so
    /// no argument that is not UTF-8 stops the record ([`display_command`]).
    pub fn this() -> io::Result<Self> {
        let pid = std::process::id();
        Ok(Self {
            pid,
            unique_id: unique_id(pid)?,
            since: crate::storage::deletion_log::rfc3339_utc(SystemTime::now()),
            command: display_command(std::env::args_os()),
        })
    }
}

/// `arguments` joined by spaces, each decoded lossily: the record's `command`, for a person to
/// read. Only this copy is lossy; nothing the process runs is read from it.
fn display_command(arguments: impl IntoIterator<Item = std::ffi::OsString>) -> String {
    let mut command = String::new();
    for (index, argument) in arguments.into_iter().enumerate() {
        if index > 0 {
            command.push(' ');
        }
        command.push_str(&argument.to_string_lossy());
    }
    command
}

/// Write `holder` into the lease `file` this process has just locked, replacing whatever an
/// earlier holder left: the record's bytes are truncated and rewritten on the locked inode itself,
/// which is never unlinked or replaced, so the lock and every waiter queued on it stay with it.
pub fn record(file: &File, holder: &impl Serialize) -> io::Result<()> {
    let bytes = serde_json::to_vec(holder).map_err(io::Error::other)?;
    file.set_len(0)?;
    file.write_all_at(&bytes, 0)
}

/// Empty the lease `file` at `path` before its holder lets go of it. One that cannot be emptied is
/// said: its record then names this process until it exits, and a census asks nothing of it.
pub fn clear(file: &File, path: &Path) {
    if let Err(error) = file.set_len(0) {
        cowshed_os::eprintln!(
            "cowshed: cannot clear the holder record of lease {}: {error}",
            path.display()
        );
    }
}

/// Every process other than this one that a lease of the store at `store` names, with the unique id
/// it has now: the life that wrote the record, which empties it before it lets go of the lease.
/// A record whose pid names no process, or one with another unique id, names nothing; so does
/// one without a unique id. The store's owners are its one owner enumeration's
/// ([`crate::storage::bootstrap::project_owner_directories`]: one listing of the store root and one
/// `lstat` per entry), never every store-root directory, so the controller's own namespaces
/// (`run/`, whose socket leases a long-lived host holds by the tens of thousands) and an unmarked
/// directory are never listed (01_storage.md, "Owners are marked, never inferred"). Then one
/// listing per owner and per project's `sessions` and one `read` per lease file, and no
/// process-table walk. The store is the census's own
/// ([`crate::census::CensusScope::store`]): the host's for the host store's census, a fixture's
/// for a fixture's, and no other store's leases are read. A store, project or lease this user
/// cannot read names nothing, and so does a store the owner enumeration refuses (a layout that
/// predates owner markers): the census then asks those processes, as it asks any other. This
/// process is always asked, lease or not (02_workspaces.md: genuine holders, including the
/// controller's own pid, refuse). Only Darwin's census consults it: Linux's asks every process,
/// and no lease there carries a unique id, so it would name none.
#[allow(dead_code)]
pub(crate) fn running_lifecycle(
    store: &Path,
    mut read: impl FnMut(&Path) -> io::Result<Vec<u8>>,
) -> BTreeSet<libc::pid_t> {
    let own = std::process::id();
    let mut running = BTreeSet::new();
    for owner in crate::storage::bootstrap::project_owner_directories(store).unwrap_or_default() {
        for project in directories(&owner) {
            for lease in leases(&project.join(crate::repository::SESSIONS_DIRECTORY)) {
                let Ok(bytes) = read(&lease) else {
                    continue;
                };
                let Ok(holder) = serde_json::from_slice::<Process>(&bytes) else {
                    continue;
                };
                let Ok(pid) = libc::pid_t::try_from(holder.pid) else {
                    continue;
                };
                if holder.pid != own
                    && holder.unique_id.is_some()
                    && unique_id(holder.pid).ok().flatten() == holder.unique_id
                {
                    running.insert(pid);
                }
            }
        }
    }
    running
}

/// The subdirectories of `directory`; none when it cannot be listed.
fn directories(directory: &Path) -> Vec<PathBuf> {
    fs::read_dir(directory).map_or_else(
        |_| Vec::new(),
        |entries| {
            entries
                .flatten()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .map(|entry| entry.path())
                .collect()
        },
    )
}

/// Whether `name` is a cowshed lifecycle lease file: a workspace's intent lease or its land lease.
/// A lease is a lock whatever its length: the record it holds describes its holder, or a holder
/// that ended without unwinding, and is no state of its own.
pub fn is_lease(name: &str) -> bool {
    name.ends_with(crate::storage::recovery::INTENT_LEASE_SUFFIX)
        || name.ends_with(crate::runtime::land_lease::LAND_LEASE_SUFFIX)
}

/// The intent and land lease files in a project's `sessions` directory ([`is_lease`]).
fn leases(sessions: &Path) -> Vec<PathBuf> {
    fs::read_dir(sessions).map_or_else(
        |_| Vec::new(),
        |entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(is_lease)
                })
                .collect()
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An argument that is not UTF-8 is recorded in the display command, decoded lossily, beside
    /// the arguments that are, rather than stopping the record.
    #[test]
    fn an_argument_that_is_not_utf8_is_shown_lossily_and_never_stops_the_record() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;
        let command = display_command([
            OsString::from("cowshed"),
            OsString::from_vec(vec![b'a', 0xff, b'b']),
            OsString::from("lock"),
        ]);
        assert_eq!(command, "cowshed a\u{fffd}b lock");
        assert_eq!(display_command(std::iter::empty()), "");
    }

    /// A record an earlier build wrote, its unique id a JSON number, does not parse: it names no
    /// process and proves nothing. One with the exact decimal string does.
    #[test]
    fn a_record_with_a_numeric_unique_id_names_nothing() {
        assert!(
            serde_json::from_str::<Process>(
                r#"{"pid":7,"uniqueId":42,"since":"2026-10-21T00:00:00Z","command":"cowshed fork"}"#
            )
            .is_err()
        );
        let current: Process = serde_json::from_str(
            r#"{"pid":7,"uniqueId":"42","since":"2026-10-21T00:00:00Z","command":"cowshed fork"}"#,
        )
        .expect("the decimal-string record");
        assert_eq!(
            current.unique_id,
            Some(crate::api::held_lock::DecimalU64::new(42))
        );
    }
}
