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
    /// The leader's start time; `None` means it was already authoritatively absent.
    leader_start: Option<u64>,
}

/// Replace the ledger at `path` with these `(job id, process group)` pairs.
pub fn record(path: &Path, groups: &[(u64, u32)]) -> io::Result<()> {
    let ledger = Ledger {
        supervisor: std::process::id(),
        ended: false,
        groups: groups
            .iter()
            .map(|&(job_id, pgid)| {
                let pgid = i32::try_from(pgid)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
                if pgid <= 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "a job process group must be positive",
                    ));
                }
                Ok(Group {
                    job_id,
                    pgid,
                    leader_start: start_time(pgid)?,
                })
            })
            .collect::<io::Result<Vec<_>>>()?,
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
/// it, and mark it ended; the ledger stays until the next supervisor of the workspace takes it
/// ([`take_lost`]). Returns the job ids whose groups were signalled.
pub fn end_recorded(path: &Path, writer: Writer, grace: Duration) -> io::Result<Vec<u64>> {
    let Some(mut ledger) = read(path)? else {
        return Ok(Vec::new());
    };
    if let Writer::Process(pid) = writer
        && pid != ledger.supervisor
    {
        return Ok(Vec::new());
    }
    let signalled = end_groups(&ledger.groups, grace)?;
    ledger.ended = true;
    replace(path, &ledger)?;
    Ok(signalled)
}

/// For the supervisor now holding the workspace's socket: end whatever its lost predecessor's
/// ledger still names and remove the ledger. Which jobs to seal as lost is the job records' to
/// say, not the ledger's: power loss can take the ledger with the processes it names.
pub fn take_lost(path: &Path, grace: Duration) -> io::Result<()> {
    let Some(ledger) = read(path)? else {
        return Ok(());
    };
    // A prior signaling pass is not authority to discard a still-live group's evidence.
    end_groups(&ledger.groups, grace)?;
    std::fs::remove_file(path)
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

/// TERM the owned groups, wait `grace`, revalidate their ownership, then KILL them.
/// A failed inspection or signal leaves the caller's original ledger available for retry.
fn end_groups(groups: &[Group], grace: Duration) -> io::Result<Vec<u64>> {
    let mut signalled = Vec::with_capacity(groups.len());
    for group in groups {
        if owns_group(group)? && signal_group(group.pgid, libc::SIGTERM)? {
            signalled.push(group.job_id);
        }
    }
    if !signalled.is_empty() {
        std::thread::sleep(grace);
        for group in groups {
            if owns_group(group)? {
                signal_group(group.pgid, libc::SIGKILL)?;
            }
        }
    }
    Ok(signalled)
}

fn owns_group(group: &Group) -> io::Result<bool> {
    if group.pgid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a recorded job process group must be positive",
        ));
    }
    let now = start_time(group.pgid).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot verify process group {}: {error}", group.pgid),
        )
    })?;
    Ok(match (group.leader_start, now) {
        (Some(recorded), Some(now)) => recorded == now,
        (None, Some(_)) => false,
        // Leader absence alone says nothing about surviving descendants.
        (_, None) => return group_has_live_members(group.pgid),
    })
}

#[cfg(target_os = "macos")]
fn group_has_live_members(pgid: i32) -> io::Result<bool> {
    fn read_members(pgid: i32, members: &mut [i32]) -> io::Result<usize> {
        let bytes = i32::try_from(std::mem::size_of_val(members)).map_err(io::Error::other)?;
        // SAFETY: errno is thread-local; libproc writes at most `bytes` into this live slice.
        let count = unsafe {
            *libc::__error() = 0;
            libc::proc_listpgrppids(pgid, members.as_mut_ptr().cast(), bytes)
        };
        if count <= 0 {
            let error = io::Error::last_os_error();
            return if count == 0 && error.raw_os_error() == Some(0) {
                Ok(0)
            } else {
                Err(error)
            };
        }
        let count = usize::try_from(count).map_err(io::Error::other)?;
        if count > members.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "process group membership exceeded its buffer",
            ));
        }
        Ok(count)
    }

    fn contains_live(members: &[i32]) -> io::Result<bool> {
        for &pid in members {
            if pid <= 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "process group membership contains an invalid pid",
                ));
            }
            if start_time(pid)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Small groups need no allocation. A full buffer is never evidence of complete absence.
    let mut stack = [0; 32];
    let count = read_members(pgid, &mut stack)?;
    if contains_live(&stack[..count])? {
        return Ok(true);
    }
    if count < stack.len() {
        return Ok(false);
    }
    let mut members = vec![0; stack.len() * 2];
    loop {
        let count = read_members(pgid, &mut members)?;
        if contains_live(&members[..count])? {
            return Ok(true);
        }
        if count < members.len() {
            return Ok(false);
        }
        let length = members.len().checked_mul(2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process group membership overflowed",
            )
        })?;
        members.resize(length, 0);
    }
}

#[cfg(target_os = "linux")]
fn group_has_live_members(pgid: i32) -> io::Result<bool> {
    signal_group(pgid, 0)
}

/// Whether a signal reached a group, or ESRCH proved it absent. Other failures are errors.
fn signal_group(pgid: i32, signal: i32) -> io::Result<bool> {
    // SAFETY: killpg takes plain integers and touches no memory of ours.
    if unsafe { libc::killpg(pgid, signal) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(io::Error::new(
        error.kind(),
        format!("signaling process group {pgid} with signal {signal} failed: {error}"),
    ))
}

/// The leader's start time, or confirmed absence. An unreadable process is never absent.
#[cfg(target_os = "macos")]
fn start_time(pid: i32) -> io::Result<Option<u64>> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size =
        i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).map_err(io::Error::other)?;
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
        if written <= 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("proc_pidinfo returned {written} bytes, expected {size}"),
        ));
    }
    // SAFETY: proc_pidinfo filled all `size` bytes.
    let info = unsafe { info.assume_init() };
    info.pbi_start_tvsec
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(info.pbi_start_tvusec))
        .map(Some)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process start time overflowed"))
}

/// The leader's start time in ticks since boot, or confirmed absence on a mounted procfs.
#[cfg(target_os = "linux")]
fn start_time(pid: i32) -> io::Result<Option<u64>> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            std::fs::metadata("/proc/self/stat")?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    // The command name is parenthesized and may hold spaces; fields resume after the last ')'.
    let closing = stat.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command delimiter",
        )
    })?;
    let start = stat[closing + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "process stat has no start time")
        })?;
    start
        .parse()
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
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

    #[cfg(target_os = "macos")]
    struct OwnedGroup(std::process::Child);

    #[cfg(target_os = "macos")]
    impl Drop for OwnedGroup {
        fn drop(&mut self) {
            let pgid = i32::try_from(self.0.id()).expect("owned child pid");
            // SAFETY: this unreaped child is the group leader spawned by this test.
            unsafe { libc::killpg(pgid, libc::SIGKILL) };
            let _ = self.0.wait();
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn an_absent_leader_does_not_hide_its_live_group_descendant() {
        use std::io::BufRead as _;

        let child = Command::new("/bin/sh")
            .args(["-c", "(trap '' TERM; echo READY; exec sleep 300) & wait"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn()
            .unwrap();
        let mut group = OwnedGroup(child);
        let pgid = i32::try_from(group.0.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(group.0.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready.trim(), "READY");
        let ledger = scratch();
        record(&ledger, &[(9, group.0.id())]).unwrap();
        group.0.kill().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while super::start_time(pgid).unwrap().is_some() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(super::start_time(pgid).unwrap().is_none());
        assert!(super::group_has_live_members(pgid).unwrap());
        assert_eq!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO).unwrap(),
            vec![9]
        );
        drop(group);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while super::group_has_live_members(pgid).unwrap() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!super::group_has_live_members(pgid).unwrap());
        take_lost(&ledger, Duration::ZERO).unwrap();
        assert!(!ledger.exists());
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
        take_lost(&ledger, Duration::ZERO).unwrap();
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

    #[cfg(target_os = "macos")]
    #[test]
    fn permission_denials_preserve_the_owned_ledger_and_allow_retry() {
        const LEDGER_ENV: &str = "COWSHED_TEST_GROUP_LEDGER";
        if let Some(path) = std::env::var_os(LEDGER_ENV) {
            let path = std::path::PathBuf::from(path);
            let error = end_recorded(&path, Writer::Any, Duration::ZERO)
                .expect_err("denied group authority cannot mark the ledger ended");
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            let error = take_lost(&path, Duration::ZERO)
                .expect_err("denied group authority cannot remove the ledger");
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            return;
        }

        for (profile, previously_ended) in [
            ("(version 1)(allow default)(deny signal)", false),
            (
                "(version 1)(allow default)(deny process-info*)(deny signal)",
                false,
            ),
            ("(version 1)(allow default)(deny signal)", true),
        ] {
            let group = OwnedGroup(job_group());
            let pgid = i32::try_from(group.0.id()).unwrap();
            let ledger = scratch();
            record(&ledger, &[(7, group.0.id())]).unwrap();
            if previously_ended {
                let mut recorded = super::read(&ledger).unwrap().unwrap();
                recorded.ended = true;
                super::replace(&ledger, &recorded).unwrap();
            }
            let before = std::fs::read(&ledger).unwrap();
            let output = Command::new("/usr/bin/sandbox-exec")
                .args(["-p", profile, "--"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::job_groups::tests::permission_denials_preserve_the_owned_ledger_and_allow_retry",
                    "--nocapture",
                ])
                .env(LEDGER_ENV, &ledger)
                .output()
                .expect("real restricted cleanup process");
            let after = std::fs::read(&ledger).unwrap();
            let survived_refusal = group_alive(pgid);
            let retried = end_recorded(&ledger, Writer::Any, Duration::ZERO);
            let taken = take_lost(&ledger, Duration::ZERO);
            drop(group);

            assert!(
                output.status.success(),
                "restricted cleanup: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(after, before, "failed cleanup retains the original ledger");
            assert!(survived_refusal, "the refused group remained alive");
            assert_eq!(
                retried.unwrap(),
                vec![7],
                "host authority can retry cleanup"
            );
            taken.unwrap();
            assert!(!ledger.exists(), "successful retry consumes the ledger");
        }
    }
}
