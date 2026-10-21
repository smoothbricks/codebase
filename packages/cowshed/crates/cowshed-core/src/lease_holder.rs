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
//! vnodes: a fork rewrites its new build image's first block to pay the clone's extent-map copy
//! (16_build_volumes.md), and every per-descriptor query of that process waits as long. It is no
//! foreign holder: the operation's own leases and image locks already refuse the operations a
//! census guards.

#[cfg(target_os = "macos")]
use std::collections::BTreeSet;
#[cfg(target_os = "macos")]
use std::fs;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
#[cfg(target_os = "macos")]
use std::path::PathBuf;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

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

/// The kernel's unique id of the running process `pid`; `None` once it has exited.
#[cfg(target_os = "macos")]
fn unique_id(pid: u32) -> io::Result<Option<crate::api::held_lock::DecimalU64>> {
    let pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    Ok(crate::runtime::job_groups::process_record(pid)?
        .map(|record| crate::api::held_lock::DecimalU64::new(record.unique_id)))
}

/// Linux records no unique id: its census asks every process (`procfs_census`).
#[cfg(not(target_os = "macos"))]
fn unique_id(_pid: u32) -> io::Result<Option<crate::api::held_lock::DecimalU64>> {
    Ok(None)
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

/// Every process other than this one that a lease of the host's store names, with the unique id
/// it has now: the life that wrote the record, which empties it before it lets go of the lease.
/// A record whose pid names no process, or one with another unique id, names nothing; so does
/// one without a unique id. One directory listing per project of the store and one read per lease
/// file, and no process-table walk. A store, project or lease this user cannot read names nothing:
/// the census then asks those processes, as it asks any other. This process is always asked,
/// lease or not (02_workspaces.md: genuine holders, including the controller's own pid, refuse).
#[cfg(target_os = "macos")]
pub(crate) fn running_lifecycle() -> BTreeSet<libc::pid_t> {
    let own = std::process::id();
    let mut running = BTreeSet::new();
    for store in stores() {
        for owner in directories(&store) {
            for project in directories(&owner) {
                for lease in leases(&project.join(crate::repository::SESSIONS_DIRECTORY)) {
                    let Ok(bytes) = fs::read(&lease) else {
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
    }
    running
}

/// The subdirectories of `directory`; none when it cannot be listed.
#[cfg(target_os = "macos")]
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
#[cfg(target_os = "macos")]
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

/// The stores whose leases a census reads: the host's. A fixture store elsewhere is not read, so a
/// census asks a fixture's lifecycle process like any other.
#[cfg(all(target_os = "macos", not(test)))]
fn stores() -> Vec<PathBuf> {
    vec![PathBuf::from(cowshed_gateway_types::STORE_ROOT)]
}

#[cfg(all(target_os = "macos", test))]
thread_local! {
    static STORES: std::cell::RefCell<Vec<PathBuf>> =
        std::cell::RefCell::new(vec![PathBuf::from(cowshed_gateway_types::STORE_ROOT)]);
}

/// In this crate's tests, the stores this thread's censuses read ([`with_stores`]).
#[cfg(all(target_os = "macos", test))]
fn stores() -> Vec<PathBuf> {
    STORES.with_borrow(Clone::clone)
}

/// Run `work` with this thread's censuses reading the leases of `stores` in place of the host's.
#[cfg(all(target_os = "macos", test))]
pub(crate) fn with_stores<T>(stores: Vec<PathBuf>, work: impl FnOnce() -> T) -> T {
    let host = STORES.replace(stores);
    let done = work();
    STORES.set(host);
    done
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

    /// A lease names this process with its unique id from the moment it is recorded, and records
    /// nothing once cleared.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_recorded_holder_names_this_life_and_a_cleared_lease_names_nothing() {
        let root = crate::temp_root::TempRoot::new("lease-holder");
        let path = root.join("topic.intent.lock");
        let file = File::create(&path).expect("lease file");
        record(&file, &Process::this().expect("this process")).expect("record");
        let written: Process =
            serde_json::from_slice(&fs::read(&path).expect("read")).expect("parse");
        assert_eq!(written.pid, std::process::id());
        assert_eq!(
            written.unique_id,
            unique_id(std::process::id()).expect("own record")
        );
        assert!(written.unique_id.is_some());
        clear(&file, &path);
        assert!(fs::read(&path).expect("read").is_empty());
    }
}
