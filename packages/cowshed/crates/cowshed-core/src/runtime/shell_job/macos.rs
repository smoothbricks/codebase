//! What a warm-shell job takes from Darwin: a pipe pair made close-on-exec under the fork lock,
//! and a command's start handed to its host behind the observation fence
//! (`process_fence`). The host forks the command held; this process attaches the job's observer
//! to it and places its class's nice, then releases it.

use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};

use super::{pipe_error, protocol_error};
use crate::error::{CowshedError, PriorityHostContext, Result};
use crate::runtime::job_groups::Birth;
use crate::runtime::process_fence::{HeldEnds, LeaderGate, leader_gate};
use crate::runtime::process_monitor::ProcessMonitor;
use crate::runtime::supervisor::CommandPlacement;

/// [`super::pipe`], running `between` after creating the pair and before marking it close-on-exec.
/// Darwin has no `pipe2`: the two steps run under the exclusive fork lock, so no spawn forks
/// between them ([`cowshed_os::fork_lock::create`]).
pub(super) fn pipe_with(between: impl FnOnce()) -> io::Result<(OwnedFd, OwnedFd)> {
    cowshed_os::fork_lock::create(|| {
        let mut ends = [0; 2];
        // SAFETY: `ends` has room for the two descriptors pipe(2) writes.
        if unsafe { libc::pipe(ends.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both descriptors were just created and are owned here alone.
        let (read, write) =
            unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
        between();
        for end in [&read, &write] {
            // SAFETY: plain fcntl on an owned descriptor.
            if unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok((read, write))
    })
}

/// A command's start as it is handed to its host: the fence the host's forked command is held
/// at, whose held ends travel with the request after the job's stdio.
pub(super) struct Handoff {
    gate: LeaderGate,
    held: HeldEnds,
}

impl Handoff {
    pub(super) fn prepare(_placement: &CommandPlacement) -> Result<Self> {
        let (gate, held) = leader_gate().map_err(pipe_error)?;
        Ok(Self { gate, held })
    }

    /// The descriptors the request carries: the job's stdio, then the fence's ready and go ends.
    pub(super) fn descriptors(&self, stdio: [RawFd; 3]) -> [RawFd; 5] {
        [
            stdio[0],
            stdio[1],
            stdio[2],
            self.held.ready.as_raw_fd(),
            self.held.go.as_raw_fd(),
        ]
    }

    /// The request is sent: attach `monitor` to the held command and place it at its nice, then
    /// release it. A refused placement fails the launch.
    pub(super) async fn sent(
        self,
        host_pid: i32,
        placement: &CommandPlacement,
        monitor: &ProcessMonitor,
    ) -> Result<Placed> {
        let Self { gate, held } = self;
        drop(held);
        let monitor = monitor.clone();
        let nice = placement.nice();
        let (observed, placed) = tokio::task::spawn_blocking(move || {
            let mut placed = None;
            let observed = gate.observe(|pid| {
                monitor.watch(pid).map_err(io::Error::other)?;
                let placement = crate::runtime::placement::macos::place_nice(pid, nice);
                let refused = placement
                    .as_ref()
                    .err()
                    .map(|failure| io::Error::other(format!("{failure:?}")));
                placed = Some(placement);
                refused.map_or(Ok(()), Err)
            });
            (observed, placed)
        })
        .await
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()));
        let placed = match placed {
            Some(Err(failure)) => {
                return Err(CowshedError::priority_in_host(
                    failure,
                    host_priority_context(host_pid),
                ));
            }
            Some(Ok(nice)) => Some(nice),
            None => None,
        };
        observed.map_err(pipe_error)?;
        Ok(Placed(placed))
    }
}

/// Read the warm host's nice, parent pid and this supervisor's own nice on a refused placement.
/// Never called on the success path; failed reads stay explicit, never guessed.
fn host_priority_context(host_pid: i32) -> PriorityHostContext {
    let host_nice = u32::try_from(host_pid)
        .map_err(|_| format!("invalid pid {host_pid}"))
        .and_then(|pid| {
            crate::runtime::placement::macos::nice_of(pid).map_err(|error| error.to_string())
        });
    let host_ppid = match crate::runtime::job_groups::process_record(host_pid) {
        Ok(Some(record)) => Ok(record.ppid),
        Ok(None) => Err("process exited".to_owned()),
        Err(error) => Err(error.to_string()),
    };
    PriorityHostContext {
        host_pid,
        host_nice,
        host_ppid,
        supervisor_nice: crate::runtime::placement::macos::own_nice(),
    }
}

/// The nice the command was observed at while the fence held it, before it ran anything; `None`
/// when the fence saw no command.
pub(super) struct Placed(Option<i32>);

impl Placed {
    /// The host says a command started: the fence must have held it.
    pub(super) fn fenced(&self) -> Result<()> {
        if self.0.is_none() {
            return Err(protocol_error(
                "reply",
                "a command started that its fence never held",
            ));
        }
        Ok(())
    }

    /// The started command's observed nice. Its observer watches it since the fence held it.
    pub(super) async fn started(self, _birth: &Birth, _monitor: &ProcessMonitor) -> Option<i32> {
        self.0
    }
}

/// Clone the bound program `source` to a new file at `destination` (`fclonefileat`, which reads
/// no descriptor offset); `false`, with nothing made, when the two are on different volumes or
/// theirs cannot clone.
pub(super) fn clone_bound(
    source: &std::fs::File,
    destination: &std::path::Path,
) -> io::Result<bool> {
    use std::os::unix::ffi::OsStrExt as _;
    let (Some(parent), Some(name)) = (destination.parent(), destination.file_name()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a staged program is a file in a directory",
        ));
    };
    let directory = std::fs::File::open(parent)?;
    let name = std::ffi::CString::new(name.as_bytes())?;
    // SAFETY: both descriptors stay open across the call, and the name is NUL-terminated.
    let cloned =
        unsafe { libc::fclonefileat(source.as_raw_fd(), directory.as_raw_fd(), name.as_ptr(), 0) };
    if cloned == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EXDEV | libc::ENOTSUP) => Ok(false),
        _ => Err(error),
    }
}
