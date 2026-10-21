//! A land's hold on its workspace, and an `rm`'s (02_workspaces.md "`cowshed land`", "A land
//! and an rm of one workspace exclude each other").
//!
//! A land moves the target first and then adopts the workspace's build volume and retires the
//! workspace; from the fast-forward on, the workspace's commits are contained in the target, so
//! an `rm` that judges containment passes. Measured 2026-10-21: an agent's watcher saw the landed
//! head on main and ran `cowshed rm` on the workspace while its land was still releasing the
//! target's previous build volume; the land then could not read its own workspace back and
//! reported `not-found`, and its wrapper skipped the push of a main the land had moved.
//!
//! So a land and an `rm` each hold `sessions/<workspace>.land.lock` exclusively for as long as
//! they run, a `flock(2)` on a descriptor the process owns, so the kernel drops it when that
//! process exits however it ends: a killed land never keeps a workspace from being removed. The
//! file records which operation holds it, so the one refused names the one that holds it.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{CowshedError, Result};

/// Suffix of the per-workspace land lease beside the project's session images.
pub const LAND_LEASE_SUFFIX: &str = ".land.lock";

/// The lease file of `workspace` in the project's session directory.
pub fn lease_path(sessions: &Path, workspace: &str) -> PathBuf {
    sessions.join(format!("{workspace}{LAND_LEASE_SUFFIX}"))
}

/// Which operation holds a workspace's lease.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Holding {
    Land,
    Remove,
}

impl Holding {
    const fn verb(self) -> &'static str {
        match self {
            Self::Land => "landed",
            Self::Remove => "removed",
        }
    }
}

/// What the lease file says about its holder.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Holder {
    pub holding: Holding,
    pub pid: u32,
    /// When it took the lease, as the deletion log writes times.
    pub since: String,
    pub command: String,
}

/// A held lease: dropping it, or the process ending, releases it. The file stays.
#[derive(Debug)]
pub struct LandLease {
    _file: File,
}

/// Take `workspace`'s lease for `holding`, or refuse naming whoever holds it.
pub fn take(sessions: &Path, workspace: &str, holding: Holding) -> Result<LandLease> {
    let path = lease_path(sessions, workspace);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| lease_unavailable(&path, &error))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            let mut recorded = Vec::new();
            let holder = file
                .read_to_end(&mut recorded)
                .ok()
                .and_then(|_| serde_json::from_slice::<Holder>(&recorded).ok());
            return Err(refused(workspace, holding, holder.as_ref()));
        }
        Err(TryLockError::Error(error)) => return Err(lease_unavailable(&path, &error)),
    }
    let holder = Holder {
        holding,
        pid: std::process::id(),
        since: crate::storage::deletion_log::rfc3339_utc(SystemTime::now()),
        command: std::env::args().collect::<Vec<_>>().join(" "),
    };
    let bytes = serde_json::to_vec(&holder).map_err(|error| {
        CowshedError::internal(format!("cannot encode the land lease holder: {error}"))
    })?;
    file.set_len(0)
        .and_then(|()| file.seek(SeekFrom::Start(0)))
        .and_then(|_| file.write_all(&bytes))
        .map_err(|error| lease_unavailable(&path, &error))?;
    Ok(LandLease { _file: file })
}

fn lease_unavailable(path: &Path, error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot take the land lease {}: {error}", path.display()),
        "cowshed doctor --json",
    )
}

fn refused(workspace: &str, wanted: Holding, holder: Option<&Holder>) -> CowshedError {
    let doing = match wanted {
        Holding::Land => "land",
        Holding::Remove => "remove",
    };
    match holder {
        Some(holder) => CowshedError::conflict(
            format!(
                "cannot {doing} workspace {workspace}: it is being {} by pid {} since {} (`{}`)",
                holder.holding.verb(),
                holder.pid,
                holder.since,
                holder.command
            ),
            match holder.holding {
                Holding::Land => {
                    "a land retires its workspace itself when it ends; wait for it, then remove \
                     the workspace if the land kept it"
                }
                Holding::Remove => "wait for the removal to end",
            },
        ),
        None => CowshedError::conflict(
            format!(
                "cannot {doing} workspace {workspace}: another land or removal of it holds its \
                 land lease, and its record cannot be read"
            ),
            "wait for that operation to end",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_root::TempRoot;
    use std::fs;

    /// A private root holding an empty session directory, the only place a lease lives.
    fn sessions() -> (TempRoot, PathBuf) {
        let root = TempRoot::new("land-lease");
        let sessions = root.join("sessions");
        fs::create_dir(&sessions).expect("create the sessions directory");
        (root, sessions)
    }

    #[test]
    fn a_second_taker_is_refused_naming_the_first() {
        let (_root, sessions) = sessions();
        let held =
            take(&sessions, "topic", Holding::Land).expect("the first taker holds the lease");

        let refusal =
            take(&sessions, "topic", Holding::Remove).expect_err("a second taker was admitted");
        let pid = std::process::id();
        assert!(
            refusal
                .message
                .contains(&format!("being landed by pid {pid} ")),
            "the refusal did not name the holder: {refusal}"
        );

        drop(held);
        take(&sessions, "topic", Holding::Remove)
            .expect("the lease is free once its holder drops it");
    }

    /// BSD `lockf(1)` takes the same `flock(2)`; Linux ships no such command.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_killed_holder_releases_the_lease() {
        use cowshed_os::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;
        use std::process::{Command, Stdio};
        use std::thread;
        use std::time::Duration;
        let (_root, sessions) = sessions();
        let lease = lease_path(&sessions, "topic");
        // `lockf -k` takes flock(2) on the lease and runs `sleep` as its child; the child inherits
        // the locked descriptor. Killing the whole process group ends both, as a killed land's tree.
        let mut holder = Command::new("/usr/bin/lockf")
            .arg("-k")
            .arg(&lease)
            .args(["/bin/sleep", "600"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn_locked()
            .expect("spawn lockf");

        // Until lockf locks, a taker can win; a win writes its own record, so empty the file again
        // and release, and retry. Once the child holds the lock, a taker is refused with no record.
        loop {
            if let Some(status) = holder.try_wait().expect("poll lockf") {
                panic!("lockf exited ({status}) before it held the lease");
            }
            match take(&sessions, "topic", Holding::Remove) {
                Ok(won) => {
                    fs::write(&lease, b"").expect("empty the record this taker wrote");
                    drop(won);
                    thread::sleep(Duration::from_millis(20));
                }
                Err(refusal) => {
                    assert!(
                        refusal.message.contains("its record cannot be read"),
                        "the held lease was refused without a record: {refusal}"
                    );
                    break;
                }
            }
        }

        let group = libc::pid_t::try_from(holder.id()).expect("a pid fits pid_t");
        // SAFETY: `holder` leads its own process group (`process_group(0)`), so its pid names the
        // group; SIGKILL to that group ends lockf and the sleep child that holds the descriptor.
        let killed = unsafe { libc::killpg(group, libc::SIGKILL) };
        assert_eq!(killed, 0, "kill the lockf process group");
        holder.wait().expect("reap lockf");

        // The lock drops when the sleep child's descriptor closes, a moment after the kill.
        loop {
            match take(&sessions, "topic", Holding::Remove) {
                Ok(released) => {
                    drop(released);
                    break;
                }
                Err(refusal) => {
                    assert!(
                        refusal.message.contains("its record cannot be read"),
                        "the killed holder's lease was refused with a record: {refusal}"
                    );
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }
}
