//! Nx's state inside a build volume: who holds its task database open, its daemon, and the
//! run summary stock Nx writes after every run (16_build_volumes.md, "The adoption needs the
//! target's Nx database closed", Land steps 4, 6 and 7).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::Deserialize;

use super::BuildVolumeState;

/// Where Nx keeps its daemon's record inside `workspace-data` (pinned Nx 23.2.1,
/// `daemon/tmp-dir.js`). It names one checkout's process and socket, so it never travels.
pub const DAEMON_DIRECTORY: &str = "d";
const DAEMON_RECORD: &str = "server-process.json";
/// The run summary Nx's `StoreRunInformationLifeCycle` writes into its cache directory after
/// every `run`/`run-many` (pinned Nx 23.2.1, `tasks-runner/life-cycles/store-run-information-life-cycle.js`).
pub const RUN_SUMMARY: &str = "run.json";
/// How long a stopped daemon gets to exit after `SIGTERM`, as `nx daemon --stop` sends it.
const DAEMON_EXIT_GRACE: Duration = Duration::from_secs(10);

/// A process that holds a task database open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Holder {
    pub pid: i32,
    pub command: String,
}

impl std::fmt::Display for Holder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "pid {} ({})", self.pid, self.command)
    }
}

/// Every Nx task database in the volume rooted at `volume`: each `*.db` file directly in a
/// `workspace-data` directory the volume holds.
pub fn task_databases(volume: &Path, state: &BuildVolumeState) -> io::Result<Vec<PathBuf>> {
    let mut databases = Vec::new();
    for data in state.nx_workspace_data() {
        databases.extend(task_databases_in(&volume.join(data))?);
    }
    Ok(databases)
}

/// Delete every daemon record in the volume: a record names another checkout's process and
/// socket (rule "One Nx state per checkout").
pub fn discard_daemon_records(volume: &Path, state: &BuildVolumeState) -> io::Result<()> {
    for data in state.nx_workspace_data() {
        match fs::remove_dir_all(volume.join(data).join(DAEMON_DIRECTORY)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Why a volume's Nx state could not be closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Busy {
    /// Processes other than the daemon hold a task database open.
    Held {
        database: PathBuf,
        holders: Vec<Holder>,
    },
    /// The daemon did not exit within its hang guard after `SIGTERM`.
    DaemonStayed { daemon: Holder },
}

impl std::fmt::Display for Busy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Held { database, holders } => {
                write!(formatter, "{} is open in ", database.display())?;
                for (index, holder) in holders.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{holder}")?;
                }
                Ok(())
            }
            Self::DaemonStayed { daemon } => write!(
                formatter,
                "the Nx daemon {daemon} did not exit within {}s of SIGTERM",
                DAEMON_EXIT_GRACE.as_secs()
            ),
        }
    }
}

/// Close the Nx state of the volume rooted at `volume`: when no process but the checkout's live
/// daemon holds a task database open, stop that daemon as stock `nx daemon --stop` does
/// (`SIGTERM` to the pid its record names, `daemon/client/client.js` `stop`), wait for its exit,
/// and ask once more. Any holder then is [`Busy`]; nothing is stopped except a daemon already
/// asked to.
#[cfg(target_os = "macos")]
pub fn close(volume: &Path, state: &BuildVolumeState) -> io::Result<Result<(), Busy>> {
    for data in state.nx_workspace_data() {
        let data = volume.join(data);
        let daemon = live_daemon(&data);
        let databases = task_databases_in(&data)?;
        for database in &databases {
            let holders: Vec<Holder> = holders(database)?
                .into_iter()
                .filter(|holder| Some(holder.pid) != daemon)
                .collect();
            if !holders.is_empty() {
                return Ok(Err(Busy::Held {
                    database: database.clone(),
                    holders,
                }));
            }
        }
        if let Some(pid) = daemon
            && !stop_daemon(pid)?
        {
            return Ok(Err(Busy::DaemonStayed {
                daemon: Holder {
                    pid,
                    command: command_line(pid),
                },
            }));
        }
        for database in &databases {
            let holders = holders(database)?;
            if !holders.is_empty() {
                return Ok(Err(Busy::Held {
                    database: database.clone(),
                    holders,
                }));
            }
        }
    }
    Ok(Ok(()))
}

fn task_databases_in(data: &Path) -> io::Result<Vec<PathBuf>> {
    let mut databases = Vec::new();
    let entries = match fs::read_dir(data) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(databases),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "db") && path.is_file() {
            databases.push(path);
        }
    }
    databases.sort();
    Ok(databases)
}

/// The pid of the daemon `workspace-data`'s record names, when that daemon is live, from the one
/// read of the record that verified it: its process runs and its socket accepts a connection.
#[cfg(target_os = "macos")]
fn live_daemon(data: &Path) -> Option<i32> {
    crate::runtime::nx_daemon::live_pid(&data.join(DAEMON_DIRECTORY).join(DAEMON_RECORD))
}

/// `SIGTERM` `pid` and wait for its exit on the kernel's exit event: `true` once it has exited,
/// `false` when it outlived [`DAEMON_EXIT_GRACE`], which guards only against a hang. The
/// daemon's own termination handler removes its socket and record.
#[cfg(target_os = "macos")]
fn stop_daemon(pid: i32) -> io::Result<bool> {
    use std::os::fd::{FromRawFd, OwnedFd};
    // SAFETY: `kqueue` takes no arguments; a negative answer is an error.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `queue` is a fresh descriptor this function alone owns.
    let queue = unsafe { OwnedFd::from_raw_fd(queue) };
    let exit = libc::kevent {
        ident: pid as libc::uintptr_t,
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    use std::os::fd::AsRawFd;
    // Registered before the signal, so an exit between the two is never missed.
    // SAFETY: one valid change, no events requested back.
    if unsafe {
        libc::kevent(
            queue.as_raw_fd(),
            &exit,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    } < 0
    {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(true),
            _ => Err(error),
        };
    }
    // SAFETY: `pid` is positive (a live daemon's), so it names one process and never a group.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(true),
            _ => Err(error),
        };
    }
    let timeout = libc::timespec {
        tv_sec: DAEMON_EXIT_GRACE.as_secs() as libc::time_t,
        tv_nsec: 0,
    };
    let mut event = exit;
    loop {
        // SAFETY: room for one event; `timeout` outlives the call.
        let ready = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                std::ptr::null(),
                0,
                &mut event,
                1,
                &timeout,
            )
        };
        match ready {
            1 => return Ok(true),
            0 => return Ok(false),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

/// Every process that has `path` open: one open-file query (`proc_listpidspath`), the kernel's
/// own answer, never a scan of `lsof` output.
#[cfg(target_os = "macos")]
pub fn holders(path: &Path) -> io::Result<Vec<Holder>> {
    use std::os::unix::ffi::OsStrExt;
    const PROC_ALL_PIDS: u32 = 1;
    unsafe extern "C" {
        fn proc_listpidspath(
            kind: u32,
            typeinfo: u32,
            path: *const libc::c_char,
            pathflags: u32,
            buffer: *mut libc::c_void,
            buffersize: libc::c_int,
        ) -> libc::c_int;
    }
    let path_c = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let mut capacity = 256usize;
    loop {
        let mut pids = vec![0 as libc::pid_t; capacity];
        let bytes = libc::c_int::try_from(capacity * size_of::<libc::pid_t>())
            .map_err(|_| io::Error::other("process table too large"))?;
        // SAFETY: `path_c` is NUL-terminated and `pids` is writable for `bytes` bytes.
        let filled = unsafe {
            proc_listpidspath(
                PROC_ALL_PIDS,
                0,
                path_c.as_ptr(),
                0,
                pids.as_mut_ptr().cast(),
                bytes,
            )
        };
        if filled < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(filled).unwrap_or(0) / size_of::<libc::pid_t>();
        if count == capacity {
            capacity *= 4;
            continue;
        }
        return Ok(pids[..count]
            .iter()
            .copied()
            .filter(|&pid| pid > 0)
            .map(|pid| Holder {
                pid,
                command: command_line(pid),
            })
            .collect());
    }
}

/// `pid`'s argument vector joined by spaces (`KERN_PROCARGS2`), or its executable path, or a
/// placeholder naming why neither could be read.
#[cfg(target_os = "macos")]
fn command_line(pid: libc::pid_t) -> String {
    let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
    let mut argmax: libc::c_int = 0;
    let mut argmax_size = size_of::<libc::c_int>();
    // SAFETY: `mib` names KERN_ARGMAX, whose value is one c_int written into `argmax`.
    let ok = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&raw mut argmax).cast(),
            &mut argmax_size,
            std::ptr::null_mut(),
            0,
        )
    } == 0;
    if ok && let Ok(capacity) = usize::try_from(argmax) {
        let mut buffer = vec![0u8; capacity];
        let mut size: libc::size_t = capacity;
        let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
        // SAFETY: `buffer` is writable for `size` bytes; the kernel writes at most that many.
        let read = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                3,
                buffer.as_mut_ptr().cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        } == 0;
        if read && let Some(command) = parse_procargs(&buffer[..size]) {
            return command;
        }
    }
    let mut path = [0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: `path` is writable for its whole length.
    let length = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), path.len() as u32) };
    if length > 0 {
        return String::from_utf8_lossy(&path[..length as usize]).into_owned();
    }
    "command unreadable".to_owned()
}

/// `KERN_PROCARGS2`: a native-endian `argc`, the executable path, NUL padding, then `argc`
/// NUL-terminated arguments (then the environment, which is never read).
fn parse_procargs(buffer: &[u8]) -> Option<String> {
    let argc = i32::from_ne_bytes(buffer.get(..4)?.try_into().ok()?);
    let mut rest = &buffer[4..];
    let executable_end = rest.iter().position(|&byte| byte == 0)?;
    rest = &rest[executable_end..];
    let first = rest.iter().position(|&byte| byte != 0)?;
    rest = &rest[first..];
    let mut arguments = Vec::new();
    for _ in 0..argc {
        let end = rest.iter().position(|&byte| byte == 0)?;
        arguments.push(String::from_utf8_lossy(&rest[..end]).into_owned());
        rest = &rest[end + 1..];
    }
    (!arguments.is_empty()).then(|| arguments.join(" "))
}

/// The last Nx run recorded in a cache directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Run {
    /// `nx <arguments>`, as Nx records its own command line.
    pub command: String,
    pub tasks: Vec<RunTask>,
}

/// One task of a run, as Nx's run summary records it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunTask {
    pub task_id: String,
    pub project: String,
    pub target: String,
    pub hash: String,
    pub cache: CacheStatus,
    pub code: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheStatus {
    LocalHit,
    RemoteHit,
    Miss,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunSummaryWire {
    run: RunWire,
    tasks: Vec<RunTaskWire>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunWire {
    command: String,
    start_time: String,
    end_time: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunTaskWire {
    task_id: String,
    target: String,
    project_name: String,
    hash: String,
    cache_status: String,
    status: i32,
}

/// The last Nx run recorded in `cache` when that run began at or after `started` and ended by
/// `ended` (Nx's own `run.startTime`/`run.endTime`), or `None` when none did: the command ran
/// no Nx there, or a run outside the window wrote the summary last.
pub fn run_within(cache: &Path, started: SystemTime, ended: SystemTime) -> io::Result<Option<Run>> {
    let path = cache.join(RUN_SUMMARY);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, message);
    let summary: RunSummaryWire = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(format!("{}: {error}", path.display())))?;
    let time = |value: &str| {
        parse_utc_millis(value).ok_or_else(|| {
            invalid(format!(
                "{} records an unreadable time {value:?}",
                path.display()
            ))
        })
    };
    let (run_started, run_ended) = (time(&summary.run.start_time)?, time(&summary.run.end_time)?);
    // Nx stamps milliseconds; the window is widened to the millisecond it truncates to.
    let floor = |at: SystemTime| {
        at.duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or(0)
    };
    if run_ended < run_started || run_started < floor(started) || run_ended > floor(ended) + 1 {
        return Ok(None);
    }
    let tasks = summary
        .tasks
        .into_iter()
        .map(|task| {
            let cache = match task.cache_status.as_str() {
                "local-cache-hit" => CacheStatus::LocalHit,
                "remote-cache-hit" => CacheStatus::RemoteHit,
                "cache-miss" => CacheStatus::Miss,
                other => {
                    return Err(invalid(format!(
                        "{} records unknown cache status {other:?}",
                        path.display()
                    )));
                }
            };
            Ok(RunTask {
                task_id: task.task_id,
                project: task.project_name,
                target: task.target,
                hash: task.hash,
                cache,
                code: task.status,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(Some(Run {
        command: summary.run.command,
        tasks,
    }))
}

/// Milliseconds since the epoch of `YYYY-MM-DDTHH:MM:SS.mmmZ`, as JavaScript's
/// `Date.prototype.toISOString` writes it.
fn parse_utc_millis(value: &str) -> Option<u128> {
    let bytes = value.as_bytes();
    if bytes.len() != 24
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || bytes[23] != b'Z'
    {
        return None;
    }
    let number = |range: std::ops::Range<usize>| value.get(range)?.parse::<u64>().ok();
    let days = crate::storage::days_from_civil(number(0..4)?, number(5..7)?, number(8..10)?)?;
    let (hour, minute, second, millis) = (
        number(11..13)?,
        number(14..16)?,
        number(17..19)?,
        number(20..23)?,
    );
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    Some(u128::from(seconds) * 1_000 + u128::from(millis))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::BuildStatePath;
    use crate::fork_lock::Spawn as _;

    fn scratch(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-nx-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn state() -> BuildVolumeState {
        BuildVolumeState {
            paths: vec![
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
            ],
        }
    }

    fn summary(start: &str, end: &str) -> String {
        format!(
            r#"{{"run":{{"command":"nx run-many -t build","startTime":"{start}","endTime":"{end}","inner":false}},
               "tasks":[
                 {{"taskId":"a:build","target":"build","projectName":"a","hash":"1","startTime":"x","endTime":"y","params":"","cacheStatus":"local-cache-hit","status":0}},
                 {{"taskId":"b:test","target":"test","projectName":"b","hash":"2","startTime":"x","endTime":"y","params":"","cacheStatus":"cache-miss","status":0}}]}}"#
        )
    }

    fn at(millis: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
    }

    #[test]
    fn the_run_summary_counts_only_when_its_own_run_lies_within_the_check() {
        let root = scratch("run");
        // 2026-10-03T16:19:43.794Z .. 16:19:44.292Z
        let (start, end) = (1_791_044_383_794, 1_791_044_384_292);
        assert_eq!(run_within(&root, at(0), at(u64::MAX / 2)).unwrap(), None);
        fs::write(
            root.join(RUN_SUMMARY),
            summary("2026-10-03T16:19:43.794Z", "2026-10-03T16:19:44.292Z"),
        )
        .unwrap();
        let run = run_within(&root, at(start), at(end)).unwrap().unwrap();
        assert_eq!(run.command, "nx run-many -t build");
        assert_eq!(run.tasks.len(), 2);
        assert_eq!(run.tasks[0].cache, CacheStatus::LocalHit);
        assert_eq!(run.tasks[1].cache, CacheStatus::Miss);
        assert_eq!(run.tasks[1].task_id, "b:test");
        assert_eq!(
            run_within(&root, at(start + 1), at(end)).unwrap(),
            None,
            "a run that began before the check is another run"
        );
        assert_eq!(
            run_within(&root, at(start), at(end - 2)).unwrap(),
            None,
            "a run that ended after the check is another run"
        );
        fs::write(root.join(RUN_SUMMARY), summary("yesterday", "today")).unwrap();
        assert!(run_within(&root, at(start), at(end)).is_err());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn daemon_records_are_discarded_and_databases_found() {
        let root = scratch("records");
        fs::create_dir_all(root.join("nx/workspace-data/d")).unwrap();
        fs::write(root.join("nx/workspace-data/d/server-process.json"), b"{}").unwrap();
        fs::write(root.join("nx/workspace-data/ABC-v3.db"), b"").unwrap();
        fs::write(root.join("nx/workspace-data/ABC-v3.db-wal"), b"").unwrap();
        assert_eq!(
            task_databases(&root, &state()).unwrap(),
            [root.join("nx/workspace-data/ABC-v3.db")]
        );
        discard_daemon_records(&root, &state()).unwrap();
        assert!(!root.join("nx/workspace-data/d").exists());
        discard_daemon_records(&root, &state()).unwrap();
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn procargs_yield_the_argument_vector() {
        let mut buffer = 2i32.to_ne_bytes().to_vec();
        buffer.extend_from_slice(b"/usr/bin/node\0\0\0node\0nx.js\0HOME=/x\0");
        assert_eq!(parse_procargs(&buffer).as_deref(), Some("node nx.js"));
        assert_eq!(parse_procargs(&[1, 0]), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn an_open_database_names_its_holder_and_a_closed_one_none() {
        let root = scratch("holders");
        fs::create_dir_all(root.join("nx/workspace-data")).unwrap();
        let database = root.join("nx/workspace-data/T-v3.db");
        fs::write(&database, b"").unwrap();
        assert_eq!(holders(&database).unwrap(), []);
        assert_eq!(close(&root, &state()).unwrap(), Ok(()));
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 3<\"$0\"; read _")
            .arg(&database)
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = child.id() as i32;
        let started = std::time::Instant::now();
        let held = loop {
            let held = holders(&database).unwrap();
            if !held.is_empty() || started.elapsed() > Duration::from_secs(10) {
                break held;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            held.iter().map(|holder| holder.pid).collect::<Vec<_>>(),
            [pid]
        );
        assert!(held[0].command.contains("sh"), "{held:?}");
        match close(&root, &state()).unwrap() {
            Err(Busy::Held { holders, .. }) => assert_eq!(holders[0].pid, pid),
            other => panic!("expected the open database to refuse: {other:?}"),
        }
        drop(child.stdin.take());
        child.wait().unwrap();
        assert_eq!(close(&root, &state()).unwrap(), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    /// Stopping the daemon natively is exactly stock `nx daemon --stop` (`SIGTERM` to the
    /// recorded pid): a real Nx daemon exits on it, and `nx daemon --start` afterwards starts a
    /// new one cleanly. If stock Nx ever needs more than the signal, this fails.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_stock_nx_daemon_stopped_natively_restarts_cleanly() {
        let package = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../node_modules/nx")
            .canonicalize()
            .expect("the repository's own Nx is installed");
        let nx = package.join("dist/bin/nx.js");
        let root = scratch("daemon");
        let checkout = fs::canonicalize(&root).unwrap();
        fs::write(
            checkout.join("package.json"),
            r#"{"name":"fixture","private":true}"#,
        )
        .unwrap();
        fs::write(checkout.join("nx.json"), r#"{"useDaemonProcess":true}"#).unwrap();
        fs::create_dir_all(checkout.join("node_modules")).unwrap();
        std::os::unix::fs::symlink(&package, checkout.join("node_modules/nx")).unwrap();
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", ".nx/workspace-data").unwrap()],
        };
        let daemon = |verb: &str| {
            let mut command = std::process::Command::new("node");
            command
                .arg(&nx)
                .args(["daemon", verb])
                .current_dir(&checkout);
            for (key, _) in std::env::vars_os() {
                if key.to_string_lossy().starts_with("NX_") || key == "CI" {
                    command.env_remove(key);
                }
            }
            let output = command
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn_locked()
                .and_then(std::process::Child::wait_with_output)
                .expect("node runs");
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.status.success(), "nx daemon {verb}: {text}");
            text
        };
        let record = checkout.join(".nx/workspace-data/d/server-process.json");
        daemon("--start");
        let first = crate::runtime::nx_daemon::live_pid(&record).expect("a live daemon");
        let socket = PathBuf::from(
            serde_json::from_slice::<serde_json::Value>(&fs::read(&record).unwrap()).unwrap()
                ["socketPath"]
                .as_str()
                .unwrap(),
        );
        assert!(socket.exists());
        assert_eq!(close(&checkout, &state).unwrap(), Ok(()));
        // SAFETY: signal 0 only checks existence.
        assert_ne!(unsafe { libc::kill(first, 0) }, 0, "the daemon exited");
        // The daemon's own shutdown ran: it removed its record and its socket, as after
        // `nx daemon --stop`. A signal Nx did not handle would leave both behind.
        assert!(!record.exists(), "the daemon removed its record");
        assert!(!socket.exists(), "the daemon removed its socket");
        let restarted = daemon("--start");
        assert!(
            !restarted.to_lowercase().contains("stale") && !restarted.contains("EADDRINUSE"),
            "{restarted}"
        );
        let second = crate::runtime::nx_daemon::live_pid(&record).expect("a new live daemon");
        assert_ne!(first, second);
        assert_eq!(close(&checkout, &state).unwrap(), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }
}
