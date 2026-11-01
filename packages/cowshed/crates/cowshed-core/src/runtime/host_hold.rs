//! A held job's hold on mbx's pool (05_gateway.md "Held jobs"): the shell that carries out
//! [`super::job_admission`]'s `Reserve` and `Release`. It runs the installed mbx's `reserve` for
//! the job, tells the job's supervisor of the grant or why none came, names the job's place in
//! the pool's queue once the hold has waited a second, and ends the hold when it is dropped.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::OwnedFd;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use bytes::Bytes;
use cowshed_os::deadline;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _};
use tokio::sync::{mpsc, oneshot};

use super::job_admission::PoolPriority;
use super::supervisor::ProcessEvent;
use crate::api::dto::JobId;
use crate::capabilities::EnvAction;
use crate::storage::job_artifact::StreamKind;

/// The held command: it prints the count mbx granted it (`MBX_RESERVE_CPUS`, at most `--cpus`,
/// at least one), then ends once its stdin, the supervisor's end of the hold, closes.
const HELD: [&str; 3] = [
    "/bin/sh",
    "-c",
    "printf '%s\\n' \"$MBX_RESERVE_CPUS\"; read -r _; exit 0",
];
/// How long a hold waits in the pool before its job's stderr names its place there.
const QUEUE_NOTE_AFTER: Duration = Duration::from_secs(1);
/// How much of mbx's stderr a hold that ends keeps to say why.
const STDERR_KEPT: usize = 4096;

/// The pool a job's hold enters: the installed mbx, run with `HOME` and the canonical mbx
/// environment's `MBX_*` values alone, so the hold enters the pool the job's own work enters.
#[derive(Clone, Debug)]
pub struct Pool {
    mbx: PathBuf,
    environment: Vec<(OsString, OsString)>,
}

/// A job's hold on the pool. Dropping it ends the hold: a hold still waiting has its process
/// group killed, which gives back its place in the queue, and a granted one has its held
/// command's stdin closed, so the command ends and mbx releases.
pub struct Hold {
    // Dropped first: the watch learns the hold was released before its command can end.
    _withdraw: oneshot::Sender<()>,
    _release: OwnedFd,
}

impl Pool {
    /// The mbx installed beside cowshed under `home` (14_nix.md, "mbx"), with `home` and the
    /// `MBX_*` values a job's capability `environment` owns.
    pub fn installed(home: &Path, environment: &BTreeMap<&'static str, EnvAction>) -> Self {
        let mut values = vec![(OsString::from("HOME"), home.as_os_str().to_owned())];
        values.extend(
            environment
                .iter()
                .filter_map(|(name, action)| match action {
                    EnvAction::Own(value) if name.starts_with("MBX_") => {
                        Some((OsString::from(name), value.clone()))
                    }
                    _ => None,
                }),
        );
        Self {
            mbx: crate::capabilities::mbx::host_link(home),
            environment: values,
        }
    }

    pub fn mbx(&self) -> &Path {
        &self.mbx
    }

    fn command(&self) -> std::process::Command {
        let mut command = std::process::Command::new(&self.mbx);
        command
            .env_clear()
            .envs(self.environment.iter().map(|(name, value)| (name, value)));
        command
    }

    /// Start `job`'s hold of at most `cpus` at `priority`: `mbx reserve` in a process group of
    /// its own, its held command's stdin a pipe whose write end the returned hold alone keeps and
    /// its stdout the grant. The count granted, or why none was, reaches the supervisor on
    /// `events`.
    pub fn reserve(
        &self,
        job: JobId,
        cpus: NonZeroUsize,
        priority: PoolPriority,
        events: mpsc::Sender<ProcessEvent>,
    ) -> io::Result<Hold> {
        use cowshed_os::fork_lock::Spawn as _;

        let (held_input, release) = super::shell_job::pipe()?;
        let mut command = self.command();
        command
            .args([
                "reserve",
                "--cpus",
                &cpus.to_string(),
                "--priority",
                priority.as_str(),
                "--",
            ])
            .args(HELD)
            .stdin(Stdio::from(held_input))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let mut child = tokio::process::Command::from(command).spawn_locked()?;
        // A spawned mbx reports its pid and has the pipes it was given; should either be missing,
        // it is killed before the error returns, so no reservation outlives this call.
        let (Some(group), Some(grant)) = (
            child.id().and_then(|pid| libc::pid_t::try_from(pid).ok()),
            child.stdout.take(),
        ) else {
            let _ = child.start_kill();
            return Err(io::Error::other(
                "the spawned mbx reserve reports no pid or has no stdout",
            ));
        };
        let stderr = child.stderr.take().map(kept_stderr);
        let (withdraw, withdrawn) = oneshot::channel();
        tokio::spawn(watch(Watch {
            job,
            asked: cpus,
            child,
            group,
            grant,
            stderr,
            withdrawn,
            events,
            pool: self.clone(),
        }));
        Ok(Hold {
            _withdraw: withdraw,
            _release: release,
        })
    }
}

/// What one hold's watch owns.
struct Watch {
    job: JobId,
    /// The most the hold asked for.
    asked: NonZeroUsize,
    child: tokio::process::Child,
    group: libc::pid_t,
    /// The held command's stdout, whose first line is the count mbx granted.
    grant: tokio::process::ChildStdout,
    stderr: Option<tokio::sync::watch::Receiver<Vec<u8>>>,
    withdrawn: oneshot::Receiver<()>,
    events: mpsc::Sender<ProcessEvent>,
    pool: Pool,
}

/// Why a hold ended without a grant.
enum Ungranted {
    /// The held command's output ended with no count: mbx refused the hold, or could not run.
    Ended,
    /// mbx's answer is no grant of the cores asked; the hold, granted or not, ends with its group.
    Unreadable(String),
}

/// Read mbx's stderr to its end, keeping its last [`STDERR_KEPT`] bytes where the watch can read
/// them at any moment: a pipe nobody reads would stall mbx once full, and a lost hold's cause is
/// read while its held command may still hold the pipe open. The channel closes at end of file.
fn kept_stderr(mut stderr: tokio::process::ChildStderr) -> tokio::sync::watch::Receiver<Vec<u8>> {
    let (kept, tail) = tokio::sync::watch::channel(Vec::new());
    tokio::spawn(async move {
        let mut chunk = [0_u8; 1024];
        while let Ok(read) = stderr.read(&mut chunk).await
            && read > 0
        {
            kept.send_modify(|kept| {
                kept.extend_from_slice(&chunk[..read]);
                let excess = kept.len().saturating_sub(STDERR_KEPT);
                kept.drain(..excess);
            });
        }
    });
    tail
}

/// Follow one hold from its reserve to its end: the count granted or why none was, once the queue
/// note, and the hold's end, asked for by the supervisor or not.
async fn watch(watch: Watch) {
    let Watch {
        job,
        asked,
        mut child,
        group,
        grant,
        stderr,
        mut withdrawn,
        events,
        pool,
    } = watch;
    // Pinned once each, so neither the grant line read part-way nor the note's report read is
    // dropped by another arm's turn of the loop.
    let note = queue_note_after(&pool, group);
    tokio::pin!(note);
    let mut noted = false;
    let line = first_line(grant);
    tokio::pin!(line);
    let line = loop {
        tokio::select! {
            biased;
            _ = &mut withdrawn => {
                // Withdrawn while it waits: its place in the queue goes with its process group.
                signal_group(group, libc::SIGKILL);
                settle(&mut child, group).await;
                return;
            }
            line = &mut line => break line,
            note = &mut note, if !noted => {
                noted = true;
                let _ = events
                    .send(ProcessEvent::Output {
                        job_id: job,
                        stream: StreamKind::Stderr,
                        bytes: Bytes::from(note),
                    })
                    .await;
            }
        }
    };
    let cpus = match line {
        Ok(Some(line)) => granted(&line, asked).map_err(Ungranted::Unreadable),
        Ok(None) => Err(Ungranted::Ended),
        Err(error) => Err(Ungranted::Unreadable(format!(
            "its grant could not be read: {error}"
        ))),
    };
    let cpus = match cpus {
        Ok(cpus) => cpus,
        Err(ungranted) => {
            let why = match ungranted {
                Ungranted::Ended => String::new(),
                Ungranted::Unreadable(why) => {
                    signal_group(group, libc::SIGKILL);
                    format!("{why}; ")
                }
            };
            let ended = settle(&mut child, group).await;
            let error = format!(
                "{why}mbx reserve {ended}{}",
                last_line(stderr, Tail::Whole).await
            );
            let _ = events
                .send(ProcessEvent::HoldRefused { job_id: job, error })
                .await;
            return;
        }
    };
    let _ = events
        .send(ProcessEvent::HoldGranted { job_id: job, cpus })
        .await;
    tokio::select! {
        biased;
        _ = &mut withdrawn => {
            settle(&mut child, group).await;
        }
        ended = held(&mut child) => {
            let cause = match ended {
                Ok(status) => format!("mbx reserve ended {status}"),
                Err(error) => format!("mbx reserve could not be waited for: {error}"),
            };
            // The held command may still hold mbx's stderr open, and a group is never signalled once
            // its leader is reaped, so the cause carries the tail kept so far.
            let cause = format!("{cause}{}", last_line(stderr, Tail::SoFar).await);
            let _ = events.send(ProcessEvent::HoldLost { job_id: job, cause }).await;
        }
    }
}

/// A granted hold's end.
// WHY unbounded: a granted hold lasts its job's life; the supervisor's release, or the hold's own
// end, ends this wait.
#[allow(clippy::disallowed_methods)]
async fn held(child: &mut tokio::process::Child) -> io::Result<std::process::ExitStatus> {
    child.wait().await
}

/// The job's queue note, once the hold has waited [`QUEUE_NOTE_AFTER`] in the pool.
async fn queue_note_after(pool: &Pool, group: libc::pid_t) -> String {
    tokio::time::sleep(QUEUE_NOTE_AFTER).await;
    queue_note(pool, group).await
}

/// The held command's first line, the count mbx granted it, with its newline; `None` when its
/// output ended with nothing, so mbx ended before it granted the hold. A line its output ended in
/// the middle of is returned as it is: it is no grant, and the refusal names what mbx wrote.
async fn first_line(stdout: tokio::process::ChildStdout) -> io::Result<Option<String>> {
    let mut line = String::new();
    tokio::io::BufReader::new(stdout)
        .read_line(&mut line)
        .await?;
    Ok((!line.is_empty()).then_some(line))
}

/// The cores a granted hold holds: the count mbx granted, a whole line, at least one and at most
/// `asked`.
fn granted(line: &str, asked: NonZeroUsize) -> Result<NonZeroUsize, String> {
    line.strip_suffix('\n')
        .and_then(|count| count.parse::<NonZeroUsize>().ok())
        .filter(|cpus| *cpus <= asked)
        .ok_or_else(|| {
            format!(
                "mbx granted `{}`, which is no count of the {asked} cores asked",
                line.escape_debug()
            )
        })
}

/// Collect the hold's mbx once it ends, within the reap bound: its stdin closed or its group
/// killed, it ends at once. One still running past the bound has its group killed and its exit
/// collected later. The answer says how it ended.
async fn settle(child: &mut tokio::process::Child, group: libc::pid_t) -> String {
    let bound = deadline::start("end a host hold", deadline::REAP);
    match deadline::reap_async("mbx reserve", child, &bound, deadline::Progress::Nothing).await {
        Ok(status) => format!("ended {status}"),
        Err(error) => {
            signal_group(group, libc::SIGKILL);
            format!("did not end: {error}")
        }
    }
}

/// `signal` to the hold's process group: mbx, its reservation worker and the held command. A
/// group already gone needs nothing.
fn signal_group(group: libc::pid_t, signal: libc::c_int) {
    // SAFETY: killpg on the process group this hold made for its own mbx.
    unsafe {
        libc::killpg(group, signal);
    }
}

/// How much of mbx's stderr a cause waits for.
enum Tail {
    /// mbx and its held command have ended, or never started a command: the stderr ends soon, so
    /// the whole of it is waited for, within the reap bound.
    Whole,
    /// The held command may still hold the stderr open: what has arrived so far.
    SoFar,
}

/// `: <mbx's last stderr line>`, or nothing when it said nothing.
async fn last_line(stderr: Option<tokio::sync::watch::Receiver<Vec<u8>>>, tail: Tail) -> String {
    let Some(mut stderr) = stderr else {
        return String::new();
    };
    match tail {
        // The channel closes at end of file, which ends `changed` with an error.
        Tail::Whole => {
            let _ = tokio::time::timeout(deadline::REAP.within, async {
                while stderr.changed().await.is_ok() {}
            })
            .await;
        }
        // Best effort: mbx's last writes preceded its exit, and one yield lets a drain already
        // woken by them run, but nothing orders the drain before this read.
        Tail::SoFar => tokio::task::yield_now().await,
    }
    let kept = stderr.borrow().clone();
    String::from_utf8_lossy(&kept)
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(|line| format!(": {}", line.trim()))
        .unwrap_or_default()
}

/// The job's stderr line naming its hold's place in the pool's queue, read once from mbx's
/// activity report; a report that cannot be read is said instead.
async fn queue_note(pool: &Pool, group: libc::pid_t) -> String {
    let mut command = pool.command();
    command.args(["stats", "--scheduler", "--json", "--activity"]);
    let read = tokio::task::spawn_blocking(move || {
        let output = deadline::output_within(
            "mbx activity report",
            &mut command,
            &deadline::start("read the host pool's queue", deadline::CONTROL),
        )
        .map_err(|error| error.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "mbx stats ended {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim_end()
            ));
        }
        serde_json::from_slice::<Report>(&output.stdout).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())
    .and_then(|read| read);
    match read {
        Ok(report) => queue_line(&report.scheduler.activity, |pid| {
            // SAFETY: getpgid only reads the process table.
            libc::pid_t::try_from(pid).is_ok_and(|pid| unsafe { libc::getpgid(pid) } == group)
        })
        .unwrap_or_else(|| {
            "cowshed: host admission queued: the pool lists no waiter of this hold\n".to_owned()
        }),
        Err(error) => format!("cowshed: host admission queue unread: {error}\n"),
    }
}

/// mbx's version-2 activity report (14_nix.md, "mbx"): its leases and waiters.
#[derive(Debug, Deserialize)]
struct Report {
    scheduler: Activity,
}

#[derive(Debug, Deserialize)]
struct Activity {
    activity: Vec<Row>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Lease,
    Waiter,
}

#[derive(Debug, Deserialize)]
struct Row {
    kind: Kind,
    charged_cpu: u64,
    pid: Option<u32>,
    queue_position: Option<usize>,
    live: Option<bool>,
}

/// The line naming the waiter `ours` picks among `rows` by its pid: its place in the queue, how
/// many wait there, and what the leases hold; `None` when no waiter is ours.
fn queue_line(rows: &[Row], ours: impl Fn(u32) -> bool) -> Option<String> {
    let position = rows
        .iter()
        .find(|row| row.kind == Kind::Waiter && row.pid.is_some_and(&ours))?
        .queue_position?;
    let waiters = rows
        .iter()
        .filter(|row| row.kind == Kind::Waiter && row.live == Some(true))
        .count();
    let (leases, held) = rows
        .iter()
        .filter(|row| row.kind == Kind::Lease)
        .fold((0, 0), |(leases, held), row| {
            (leases + 1, held + row.charged_cpu)
        });
    Some(format!(
        "cowshed: host admission queued position={position} waiters={waiters} held_cpus={held} leases={leases}\n"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The report as the pinned mbx prints it: the line names the waiter whose pid is ours, the
    /// live waiters, and the leases' cores; a report without our waiter names nothing.
    #[test]
    fn the_queue_line_reads_mbx_activity_report() {
        let report: Report = serde_json::from_str(
            r#"{"version":2,"scheduler":{"activity":[
                {"kind":"lease","name":"mbx reserve","mode_key":"reservation","build_id":"unknown","pressure_domain":"host","charged_permits":4,"charged_cpu":4,"charged_memory_bytes":0,"charged_bytes":0,"age_secs":3,"pid":null,"ticket":null,"queue_position":null,"live":null},
                {"kind":"lease","name":"rustc","mode_key":"m","build_id":"b","pressure_domain":"host","charged_permits":1,"charged_cpu":1,"charged_memory_bytes":9,"charged_bytes":9,"age_secs":1,"pid":null,"ticket":null,"queue_position":null,"live":null},
                {"kind":"waiter","name":"mbx reserve","mode_key":"reservation","build_id":"unknown","pressure_domain":"host","charged_permits":4,"charged_cpu":4,"charged_memory_bytes":0,"charged_bytes":0,"age_secs":2,"pid":41,"ticket":7,"queue_position":1,"live":true},
                {"kind":"waiter","name":"mbx reserve","mode_key":"reservation","build_id":"unknown","pressure_domain":"host","charged_permits":2,"charged_cpu":2,"charged_memory_bytes":0,"charged_bytes":0,"age_secs":1,"pid":42,"ticket":8,"queue_position":2,"live":true}
            ]}}"#,
        )
        .expect("the pinned mbx's activity report");
        assert_eq!(
            queue_line(&report.scheduler.activity, |pid| pid == 42).as_deref(),
            Some("cowshed: host admission queued position=2 waiters=2 held_cpus=5 leases=2\n")
        );
        assert_eq!(
            queue_line(&report.scheduler.activity, |pid| pid == 43),
            None
        );
    }

    /// mbx grants as many of the asked cores as are free, at least one: the held command's line
    /// is that count, and anything else is no grant.
    #[test]
    fn a_grant_is_a_count_of_the_cores_asked() {
        let four = NonZeroUsize::new(4).expect("four");
        assert_eq!(granted("2\n", four).map(NonZeroUsize::get), Ok(2));
        assert_eq!(granted("4\n", four).map(NonZeroUsize::get), Ok(4));
        for line in ["0\n", "5\n", "\n", "-1\n", "two\n", " 2\n", "2"] {
            assert_eq!(
                granted(line, four),
                Err(format!(
                    "mbx granted `{}`, which is no count of the 4 cores asked",
                    line.escape_debug()
                )),
                "{line:?}"
            );
        }
    }
}
