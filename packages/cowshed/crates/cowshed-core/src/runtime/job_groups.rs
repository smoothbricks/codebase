//! The process groups a served supervisor's running jobs lead, kept beside its socket so that
//! whoever finds the supervisor gone can end them (11_shell.md "Supervisor recovery").
//!
//! The ledger is a complete snapshot, replaced whole (write a temporary, rename) each time a
//! job's group starts or its job is sealed, so a reader never sees half a write. Each group
//! carries its leader's start time: a group whose leader is alive with another start time is a
//! reused pid, never signalled. A group whose leader is gone may still hold the job's other
//! processes, and no process can take a pid that is still a live group's id, so it is ended.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The ledger beside a supervisor socket.
pub fn ledger_path(socket: &Path) -> PathBuf {
    socket.with_extension("groups")
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Ledger {
    /// The supervisor process that wrote it.
    supervisor: u32,
    groups: Vec<Group>,
    /// Its groups were ended after the supervisor was lost; the jobs still await sealing.
    #[serde(default)]
    ended: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Group {
    job_id: u64,
    pgid: i32,
    /// The leader's start time in microseconds since the epoch, when it could be read.
    leader_start: Option<u64>,
}

/// Replace the ledger at `path` with these `(job id, process group)` pairs.
pub fn record(path: &Path, groups: &[(u64, u32)]) -> io::Result<()> {
    let ledger = Ledger {
        supervisor: std::process::id(),
        ended: false,
        groups: groups
            .iter()
            .filter_map(|&(job_id, pgid)| {
                let pgid = i32::try_from(pgid).ok()?;
                Some(Group {
                    job_id,
                    pgid,
                    leader_start: start_time(pgid),
                })
            })
            .collect(),
    };
    replace(path, &ledger)
}

fn replace(path: &Path, ledger: &Ledger) -> io::Result<()> {
    let bytes = serde_json::to_vec(ledger).map_err(io::Error::other)?;
    let temporary = path.with_extension(format!("groups.{}", std::process::id()));
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(&temporary, path)
}

/// Whose ledger may be acted on.
#[derive(Clone, Copy, Debug)]
pub enum Writer {
    /// Only the one this supervisor process wrote: a watcher of that process, which must not
    /// touch a ledger a newer supervisor of the workspace has written since.
    Process(u32),
    /// Whichever supervisor wrote it: the caller holds the workspace's socket, so every
    /// earlier supervisor is gone.
    Any,
}

/// End every group the ledger at `path` names — TERM, `grace`, then KILL — when `writer` wrote
/// it, and mark it ended; the jobs it names stay in it until the next supervisor of the
/// workspace seals them ([`take_lost`]). Returns the job ids whose groups were signalled.
pub fn end_recorded(path: &Path, writer: Writer, grace: Duration) -> io::Result<Vec<u64>> {
    let Some(mut ledger) = read(path)? else {
        return Ok(Vec::new());
    };
    if let Writer::Process(pid) = writer
        && pid != ledger.supervisor
    {
        return Ok(Vec::new());
    }
    if ledger.ended {
        return Ok(Vec::new());
    }
    let signalled = end_groups(&ledger.groups, grace);
    ledger.ended = true;
    replace(path, &ledger)?;
    Ok(signalled)
}

/// For the supervisor now holding the workspace's socket: end whatever its lost predecessor's
/// ledger still names, remove the ledger, and return every job it names — the jobs to seal as
/// lost. Jobs no ledger names (another cowshed build's) are never touched.
pub fn take_lost(path: &Path, grace: Duration) -> io::Result<Vec<u64>> {
    let Some(ledger) = read(path)? else {
        return Ok(Vec::new());
    };
    if !ledger.ended {
        end_groups(&ledger.groups, grace);
    }
    std::fs::remove_file(path)?;
    Ok(ledger.groups.iter().map(|group| group.job_id).collect())
}

fn read(path: &Path) -> io::Result<Option<Ledger>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// TERM the groups that are the jobs', wait `grace`, KILL them; the job ids signalled.
fn end_groups(groups: &[Group], grace: Duration) -> Vec<u64> {
    let ours: Vec<Group> = groups
        .iter()
        .copied()
        .filter(|group| match (group.leader_start, start_time(group.pgid)) {
            // The leader lives on under the recorded start: the job's own group.
            (Some(recorded), Some(now)) => recorded == now,
            // The pid now names another process: not ours.
            (None, Some(_)) => false,
            // The leader is gone; the group, if it still exists, is the job's.
            (_, None) => true,
        })
        .collect();
    let signalled: Vec<u64> = ours
        .iter()
        .filter(|group| signal_group(group.pgid, libc::SIGTERM))
        .map(|group| group.job_id)
        .collect();
    if !signalled.is_empty() {
        std::thread::sleep(grace);
        for group in &ours {
            signal_group(group.pgid, libc::SIGKILL);
        }
    }
    signalled
}

/// Whether `signal` reached a group that exists.
fn signal_group(pgid: i32, signal: i32) -> bool {
    // SAFETY: killpg takes plain integers and touches no memory of ours.
    unsafe { libc::killpg(pgid, signal) == 0 }
}

/// When `pid` started, in microseconds since the epoch, if it is alive.
#[cfg(target_os = "macos")]
fn start_time(pid: i32) -> Option<u64> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).ok()?;
    // SAFETY: `info` is writable storage of exactly `size` bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        return None;
    }
    // SAFETY: proc_pidinfo filled all `size` bytes.
    let info = unsafe { info.assume_init() };
    info.pbi_start_tvsec
        .checked_mul(1_000_000)?
        .checked_add(info.pbi_start_tvusec)
}

/// When `pid` started, in clock ticks since boot, if it is alive.
#[cfg(target_os = "linux")]
fn start_time(pid: i32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name is parenthesized and may hold spaces; fields resume after the last ')'.
    let rest = &stat[stat.rfind(')')? + 1..];
    rest.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use super::{Writer, end_recorded, ledger_path, record, take_lost};

    /// A job-shaped process tree: a group leader with a child, both sleeping.
    fn job_group() -> std::process::Child {
        Command::new("/bin/sh")
            .args(["-c", "sleep 300 & wait"])
            .stdin(Stdio::null())
            .process_group(0)
            .spawn()
            .expect("spawn a job group")
    }

    fn group_alive(pgid: i32) -> bool {
        // SAFETY: signal 0 only checks that the group exists.
        unsafe { libc::killpg(pgid, 0) == 0 }
    }

    fn scratch() -> std::path::PathBuf {
        let directory =
            std::env::temp_dir().join(format!("cowshed-groups-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&directory).unwrap();
        ledger_path(&directory.join("s.sock"))
    }

    #[test]
    fn a_lost_supervisor_s_groups_end_whole_and_its_ledger_goes() {
        let mut job = job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let ledger = scratch();
        record(&ledger, &[(7, job.id())]).unwrap();
        assert!(group_alive(pgid));

        let ended = end_recorded(&ledger, Writer::Any, Duration::from_millis(200)).unwrap();
        assert_eq!(ended, vec![7]);
        let _ = job.wait();
        std::thread::sleep(Duration::from_millis(100));
        assert!(!group_alive(pgid), "the leader's child died with it");
        assert!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO)
                .unwrap()
                .is_empty(),
            "an ended ledger ends nothing twice"
        );
        assert_eq!(
            take_lost(&ledger, Duration::ZERO).unwrap(),
            vec![7],
            "the next supervisor still learns which jobs to seal"
        );
        assert!(!ledger.exists());
    }

    #[test]
    fn a_watcher_of_one_supervisor_never_acts_on_a_newer_one_s_ledger() {
        let mut job = job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let ledger = scratch();
        record(&ledger, &[(3, job.id())]).unwrap();

        let other_supervisor = std::process::id() + 1;
        let ended =
            end_recorded(&ledger, Writer::Process(other_supervisor), Duration::ZERO).unwrap();
        assert!(ended.is_empty());
        assert!(
            group_alive(pgid),
            "a newer supervisor's running job is left alone"
        );
        assert!(ledger.exists());

        end_recorded(
            &ledger,
            Writer::Process(std::process::id()),
            Duration::from_millis(100),
        )
        .unwrap();
        let _ = job.wait();
    }
}
