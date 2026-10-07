//! A job's process tree as the macOS kernel reports it (07_api.md, "Process-tree observations").
//!
//! Each member is watched with kqueue `NOTE_FORK`, `NOTE_EXEC` and `NOTE_EXIT`/
//! `NOTE_EXITSTATUS`. `NOTE_FORK` neither names nor counts the children (xnu `filt_procevent`
//! ORs the event bits; `NOTE_TRACK` is refused with `ENOTSUP`), so each one is answered by
//! reading the member's children with `proc_listchildpids`: a child forked and reaped before
//! that read is never seen. A member that forks therefore makes the tree's coverage a gap
//! ([`ProcessCoverageGap::UncountedFork`]); the job's exact totals come from the leader's own and
//! reaped-children rusage, not from this tree.
//!
//! Identity is the kernel's unique id (`p_uniqueid`), which no other process is given while the
//! system runs. Every kernel read here names a pid alone, so each is fenced by that id:
//!
//! - A registration carries the life's unique id as its cookie (`EV_UDATA_SPECIFIC`, so two
//!   lives of one pid are two registrations), and every event names its life by that cookie,
//!   never by its pid: a late event of an older life lands on that life, not on the one now
//!   holding its pid. The life is read before the registration and again after it; only the
//!   same unique id both times proves the registration is that life's, and anything else is
//!   withdrawn unadopted.
//! - An image or a child list is read between two reads of its process's record. Read from a
//!   life that was no longer running by the second read, it is discarded as a stranger's, and
//!   the life's image is a gap ([`ProcessCoverageGap::UnreadImage`]).
//! - An image that could not be read is a gap, and a read that failed is also returned: an
//!   observation error is an operational value, the observer stays usable, and no errno is
//!   swallowed into a gap. `KERN_PROCARGS2` answers `EINVAL` alike for a pid it cannot find,
//!   for a process between two images, and for arguments it refuses this reader (xnu
//!   `sysctl_procargsx`); it answers `EIO` while the arguments' pages are unmapped (measured
//!   mid-exec on Darwin 25.6). A refusal is named by the kernel's own credential rule,
//!   `is_procargs_content_read_permitted`: a reader whose effective uid is neither 0 nor the
//!   process's is refused ([`ObserveError::ImageNotPermitted`], keeping the `EINVAL`). Every
//!   other failed read is [`ObserveError::Kernel`] with its call and errno, whether or not the
//!   life still ran; a child list in particular never fails because its parent exited.
//! - A child is adopted only when its parent's unique id is the member's.
//! - The kernel hands over a batch of events at once and keeps none of them. Every event of a
//!   batch, and every bit of each event, is folded even after another failed, so a failed image
//!   read never drops the exit coalesced beside it; the batch's failures are returned together.

use std::collections::HashSet;
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::process::ExitStatusExt;

use crate::api::dto::CommandArg;
use crate::api::process::ProcessCoverageGap;
use crate::error::CowshedError;
use crate::process::process_arguments;
use crate::runtime::job_groups::{ProcessRecord, process_record};
use crate::runtime::process_tree::{
    BirthToken, ProcessFoldError, ProcessIdentity, ProcessImage, ProcessObservation,
    ProcessTreeFold,
};
use crate::runtime::supervisor::{process_termination_from_wait, utc_at_second, utc_now};

const WATCHED: u32 = libc::NOTE_FORK | libc::NOTE_EXEC | libc::NOTE_EXIT | libc::NOTE_EXITSTATUS;

/// Why observing a job's processes failed.
#[derive(Debug, thiserror::Error)]
pub enum ObserveError {
    #[error("{call} for process {pid} failed: {source}")]
    Kernel {
        call: &'static str,
        pid: u32,
        source: io::Error,
    },
    /// The kernel refuses this observer the arguments of a process running as another user;
    /// `source` is the call's own `EINVAL`.
    #[error(
        "KERN_PROCARGS2 refuses process {pid}'s arguments: it runs as uid {uid}, this observer as uid {reader}: {source}"
    )]
    ImageNotPermitted {
        pid: u32,
        uid: u32,
        reader: u32,
        source: io::Error,
    },
    #[error("the kernel's process events contradict each other: {0}")]
    Contradiction(#[from] ProcessFoldError),
    #[error(transparent)]
    Cowshed(#[from] CowshedError),
    /// More than one event of one batch failed; every event of it was folded all the same.
    #[error("{first}; {} more in the same batch: {}", .rest.len(), Listed(.rest))]
    Several {
        first: Box<ObserveError>,
        rest: Vec<ObserveError>,
    },
}

struct Listed<'a>(&'a [ObserveError]);

impl fmt::Display for Listed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

/// The failures of one batch, all of whose events are folded regardless.
#[derive(Debug, Default)]
struct Failures(Vec<ObserveError>);

impl Failures {
    fn note(&mut self, result: Result<(), ObserveError>) {
        match result {
            Ok(()) => {}
            Err(ObserveError::Several { first, rest }) => {
                self.0.push(*first);
                self.0.extend(rest);
            }
            Err(error) => self.0.push(error),
        }
    }

    fn into_result(self) -> Result<(), ObserveError> {
        let mut failures = self.0.into_iter();
        let Some(first) = failures.next() else {
            return Ok(());
        };
        let rest: Vec<_> = failures.collect();
        if rest.is_empty() {
            return Err(first);
        }
        Err(ObserveError::Several {
            first: Box::new(first),
            rest,
        })
    }
}

/// When an image is read: after the kernel reported an exec, or as a child is adopted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Seen {
    Exec,
    Adopted,
}

/// The kernel's per-process reads, each naming a pid alone. The observer fences every one.
pub trait ProcessReads {
    /// The process `pid` names now, read in one call; `None` once it has exited.
    fn record(&self, pid: u32) -> io::Result<Option<ProcessRecord>>;
    /// The executable and byte-exact argv of the image `pid` runs now.
    fn image(&self, pid: u32) -> io::Result<ProcessImage>;
    /// The pids whose parent is `pid` now.
    fn children(&self, pid: u32) -> io::Result<Vec<u32>>;
    /// The effective uid the reads are made as.
    fn reader_uid(&self) -> u32;
}

/// The running kernel.
#[derive(Clone, Copy, Debug, Default)]
pub struct Darwin;

impl ProcessReads for Darwin {
    fn record(&self, pid: u32) -> io::Result<Option<ProcessRecord>> {
        process_record(libc::pid_t::try_from(pid).map_err(io::Error::other)?)
    }

    fn image(&self, pid: u32) -> io::Result<ProcessImage> {
        let arguments = process_arguments(libc::pid_t::try_from(pid).map_err(io::Error::other)?)?;
        Ok(ProcessImage {
            program: String::from_utf8_lossy(&arguments.executable).into_owned(),
            argv: arguments
                .argv
                .into_iter()
                .map(|argument| CommandArg::new(OsString::from_vec(argument)))
                .collect(),
        })
    }

    fn children(&self, pid: u32) -> io::Result<Vec<u32>> {
        let parent = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
        let mut capacity = 64_usize;
        loop {
            let mut pids: Vec<libc::pid_t> = vec![0; capacity];
            let bytes = libc::c_int::try_from(capacity * size_of::<libc::pid_t>())
                .map_err(io::Error::other)?;
            // SAFETY: `pids` is writable for `bytes` bytes.
            let count = unsafe { list_child_pids(parent, pids.as_mut_ptr().cast(), bytes) }?;
            if count < capacity {
                return Ok(pids[..count]
                    .iter()
                    .filter_map(|&child| u32::try_from(child).ok())
                    .filter(|&child| child > 0)
                    .collect());
            }
            capacity *= 2;
        }
    }

    fn reader_uid(&self) -> u32 {
        // SAFETY: geteuid takes nothing and cannot fail.
        unsafe { libc::geteuid() }
    }
}

/// `proc_listchildpids`, its failure told apart from an empty list. libproc's `proc_listpids`
/// answers the kernel's -1 with 0 and leaves errno set (libsyscall `libproc.c`), so a 0 is an
/// empty list only when errno stayed clear across the call.
///
/// # Safety
///
/// `buffer` is writable for `bytes` bytes.
unsafe fn list_child_pids(
    parent: libc::pid_t,
    buffer: *mut libc::c_void,
    bytes: libc::c_int,
) -> io::Result<usize> {
    // SAFETY: `__error` names this thread's errno.
    unsafe { *libc::__error() = 0 };
    // SAFETY: the caller's contract on `buffer`.
    let count = unsafe { libc::proc_listchildpids(parent, buffer, bytes) };
    let error = io::Error::last_os_error();
    match (usize::try_from(count), error.raw_os_error()) {
        (Ok(count), Some(0)) => Ok(count),
        (Ok(count), _) if count > 0 => Ok(count),
        (_, Some(0)) => Err(io::Error::other(format!(
            "proc_listchildpids returned {count} without an errno"
        ))),
        _ => Err(error),
    }
}

/// The process tree of one job, observed from before its root could fork or exec.
#[derive(Debug)]
pub struct KqueueObserver<P = Darwin> {
    queue: OwnedFd,
    reads: P,
    fold: ProcessTreeFold,
    /// Every life with a registration: its events are the only ones the observer folds.
    registered: HashSet<ProcessIdentity>,
}

impl<P> AsRawFd for KqueueObserver<P> {
    /// Readable when an event waits for [`KqueueObserver::drain`].
    fn as_raw_fd(&self) -> RawFd {
        self.queue.as_raw_fd()
    }
}

impl KqueueObserver {
    /// Watch `root` and everything it forks. The caller holds `root` before its exec, so the
    /// root has neither forked nor exec'd yet.
    pub fn watch(root: u32) -> Result<Self, ObserveError> {
        Self::watch_with(Darwin, root)
    }
}

impl<P: ProcessReads> KqueueObserver<P> {
    pub fn watch_with(reads: P, root: u32) -> Result<Self, ObserveError> {
        // SAFETY: kqueue takes nothing and returns a new descriptor.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(kernel("kqueue", root, io::Error::last_os_error()));
        }
        // SAFETY: a fresh descriptor this function alone owns.
        let queue = unsafe { OwnedFd::from_raw_fd(queue) };
        let mut observer = Self {
            queue,
            reads,
            fold: ProcessTreeFold::default(),
            registered: HashSet::new(),
        };
        let not_running = || {
            kernel(
                "proc_pidinfo",
                root,
                io::Error::new(io::ErrorKind::NotFound, "the held root is not running"),
            )
        };
        let before = observer.record(root)?.ok_or_else(not_running)?;
        let record = observer.attach(before)?.ok_or_else(not_running)?;
        let process = identity(&record);
        let image = observer.image(process)?.ok_or_else(not_running)?;
        observer.fold.apply(ProcessObservation::Root {
            process,
            ppid: record.ppid,
            at: utc_at_second(record.started_seconds)?,
            image,
        })?;
        Ok(observer)
    }

    pub fn fold(&self) -> &ProcessTreeFold {
        &self.fold
    }

    /// Fold every event already queued, without waiting. An `Err` names what could not be
    /// observed; every event was folded all the same, and the observer stays usable.
    pub fn drain(&mut self) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        self.drain_into(&mut failures);
        failures.into_result()
    }

    /// Wait for at least one event, then fold every event queued. An `Err` is what
    /// [`KqueueObserver::drain`] returns.
    pub fn wait(&mut self) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        if self.collect(None, &mut failures).is_some() {
            self.drain_into(&mut failures);
        }
        failures.into_result()
    }

    fn drain_into(&mut self, failures: &mut Failures) {
        let zero = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        while self
            .collect(Some(&zero), failures)
            .is_some_and(|ready| ready > 0)
        {}
    }

    /// Fold one batch of events: how many there were, or `None` when `kevent` itself failed.
    fn collect(
        &mut self,
        timeout: Option<&libc::timespec>,
        failures: &mut Failures,
    ) -> Option<usize> {
        // SAFETY: an all-zero kevent is a valid value of the plain C struct.
        let mut events: [libc::kevent; 64] = unsafe { std::mem::zeroed() };
        let capacity = libc::c_int::try_from(events.len()).expect("64 events");
        let ready = loop {
            // SAFETY: `events` is writable for `capacity` events; no changes are passed.
            let ready = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    events.as_mut_ptr(),
                    capacity,
                    timeout.map_or(std::ptr::null(), std::ptr::from_ref),
                )
            };
            match usize::try_from(ready) {
                Ok(ready) => break ready,
                Err(_) => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        failures.note(Err(kernel("kevent", 0, error)));
                        return None;
                    }
                }
            }
        };
        failures.note(self.fold_batch(&events[..ready]));
        Some(ready)
    }

    /// Fold every event of one batch the kernel handed over, each one even after another
    /// failed: the kernel keeps none of them. The batch's failures are returned together.
    fn fold_batch(&mut self, events: &[libc::kevent]) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        for event in events {
            failures.note(self.handle(event));
        }
        failures.into_result()
    }

    fn handle(&mut self, event: &libc::kevent) -> Result<(), ObserveError> {
        // `struct kevent` is packed on Darwin: copy each field out before using it.
        let (ident, flags, fflags, data, udata) = (
            event.ident,
            event.flags,
            event.fflags,
            event.data,
            event.udata,
        );
        let pid = u32::try_from(ident).map_err(|error| {
            kernel(
                "kevent",
                0,
                io::Error::new(io::ErrorKind::InvalidData, error),
            )
        })?;
        if flags & libc::EV_ERROR != 0 {
            let errno = i32::try_from(data).unwrap_or(libc::EINVAL);
            return Err(kernel("kevent", pid, io::Error::from_raw_os_error(errno)));
        }
        let cookie = u64::try_from(udata.addr()).map_err(|error| {
            kernel(
                "kevent",
                pid,
                io::Error::new(io::ErrorKind::InvalidData, error),
            )
        })?;
        let process = ProcessIdentity {
            pid,
            birth: BirthToken(cookie),
        };
        if !self.registered.contains(&process) {
            // A registration withdrawn unadopted: its queued events were never this tree's.
            return Ok(());
        }
        // The bits of one event are coalesced; a fork's children are read while the parent
        // still runs, before its exec or exit is folded. Each bit is folded even when an
        // earlier one failed.
        let mut failures = Failures::default();
        if fflags & libc::NOTE_FORK != 0 {
            failures.note(self.forked(process));
        }
        if fflags & libc::NOTE_EXEC != 0 {
            failures.note(self.exec(process));
        }
        if fflags & libc::NOTE_EXIT != 0 {
            failures.note(self.exited(process, data));
        }
        failures.into_result()
    }

    fn exec(&mut self, process: ProcessIdentity) -> Result<(), ObserveError> {
        self.fold_image(process, Seen::Exec)
    }

    /// Fold the image `process` runs now. After a `NOTE_EXEC` it is that exec's image; read
    /// at adoption, it is an exec only when it differs from the parent's image the child
    /// started with. An image that could not be read is the `UnreadImage` gap, and a read
    /// that failed is returned beside it with its call and errno.
    fn fold_image(&mut self, process: ProcessIdentity, seen: Seen) -> Result<(), ObserveError> {
        let unread = ProcessObservation::Lost(ProcessCoverageGap::UnreadImage { pid: process.pid });
        let mut failures = Failures::default();
        let observation = match self.image(process) {
            Ok(Some(image))
                if seen == Seen::Adopted && Some(&image) == self.fold.image(process) =>
            {
                None
            }
            Ok(Some(image)) => Some(ProcessObservation::Exec { process, image }),
            Ok(None) => Some(unread),
            Err(error) => {
                failures.note(Err(error));
                Some(unread)
            }
        };
        if let Some(observation) = observation {
            failures.note(self.fold.apply(observation).map_err(ObserveError::from));
        }
        failures.into_result()
    }

    fn exited(&mut self, process: ProcessIdentity, data: isize) -> Result<(), ObserveError> {
        // An exit ends the kernel's registration with it.
        self.registered.remove(&process);
        let status = i32::try_from(data).map_err(|_| {
            kernel(
                "NOTE_EXITSTATUS",
                process.pid,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("wait status {data} out of range"),
                ),
            )
        })?;
        let status = process_termination_from_wait(Ok(std::process::ExitStatus::from_raw(status)))?;
        self.fold.apply(ProcessObservation::Exited {
            process,
            status,
            at: utc_now()?,
        })?;
        Ok(())
    }

    /// `parent` forked an uncounted number of children: adopt every one it has now.
    fn forked(&mut self, parent: ProcessIdentity) -> Result<(), ObserveError> {
        self.fold.apply(ProcessObservation::Lost(
            ProcessCoverageGap::UncountedFork { pid: parent.pid },
        ))?;
        self.adopt_children(parent)
    }

    /// Adopt every child `parent` has now that the tree does not hold, each one even after
    /// another failed.
    fn adopt_children(&mut self, parent: ProcessIdentity) -> Result<(), ObserveError> {
        let Some(children) = self.children(parent)? else {
            // It exited; its children went to another parent. The fork gap covers them.
            return Ok(());
        };
        let mut failures = Failures::default();
        for child in children {
            failures.note(self.adopt(parent, child));
        }
        failures.into_result()
    }

    fn adopt(&mut self, parent: ProcessIdentity, child: u32) -> Result<(), ObserveError> {
        let Some(before) = self.record(child)? else {
            // Exited before it could be watched: the fork gap covers it.
            return Ok(());
        };
        let process = identity(&before);
        if self.fold.holds(process) || before.parent_unique_id != parent.birth.0 {
            // Already watched, or a stranger given the pid of a child reaped meanwhile.
            return Ok(());
        }
        let Some(record) = self.attach(before)? else {
            return Ok(());
        };
        self.fold.apply(ProcessObservation::Forked {
            process,
            parent,
            at: utc_at_second(record.started_seconds)?,
        })?;
        // It may have exec'd, and forked, before it was watched.
        let mut failures = Failures::default();
        failures.note(self.fold_image(process, Seen::Adopted));
        failures.note(self.adopt_children(process));
        failures.into_result()
    }

    /// Register the life `before` read for its events, then read its pid again. `Some` only
    /// when the same unique id still holds the pid: then the registration is that life's.
    fn attach(&mut self, before: ProcessRecord) -> Result<Option<ProcessRecord>, ObserveError> {
        let process = identity(&before);
        if !self.change(process, libc::EV_ADD | libc::EV_CLEAR)? {
            return Ok(None);
        }
        match self.record(process.pid)? {
            Some(after) if after.unique_id == before.unique_id => {
                self.registered.insert(process);
                Ok(Some(after))
            }
            // The pid named another life by the registration, or this one exited: whose
            // registration it is cannot be proven.
            Some(_) | None => {
                self.change(process, libc::EV_DELETE)?;
                Ok(None)
            }
        }
    }

    /// The record of `process`'s pid while that pid still names that life.
    fn current(&self, process: ProcessIdentity) -> Result<Option<ProcessRecord>, ObserveError> {
        Ok(self
            .record(process.pid)?
            .filter(|record| record.unique_id == process.birth.0))
    }

    /// The image `process` runs, read between two reads of its record. `None` when the image
    /// was not readable at that instant: the life ended, or was between two images.
    fn image(&self, process: ProcessIdentity) -> Result<Option<ProcessImage>, ObserveError> {
        let Some(before) = self.current(process)? else {
            return Ok(None);
        };
        let read = self.reads.image(process.pid);
        let after = self.current(process);
        let source = match read {
            // Read after the life ended, the image may be a stranger's.
            Ok(image) => return Ok(after?.map(|_| image)),
            Err(source) => source,
        };
        let (after, fence) = match after {
            Ok(after) => (after, None),
            Err(fence) => (None, Some(fence)),
        };
        // `EINVAL` is also the kernel's refusal; its own credential rule, applied to the
        // life's records around the call, says when it is one.
        let reader = self.reads.reader_uid();
        let refused = (source.raw_os_error() == Some(libc::EINVAL))
            .then(|| {
                std::iter::once(before)
                    .chain(after)
                    .find(|record| reader != 0 && reader != record.uid)
            })
            .flatten();
        let failed = match refused {
            Some(record) => ObserveError::ImageNotPermitted {
                pid: process.pid,
                uid: record.uid,
                reader,
                source,
            },
            None => kernel("KERN_PROCARGS2", process.pid, source),
        };
        Err(match fence {
            None => failed,
            Some(fence) => ObserveError::Several {
                first: Box::new(failed),
                rest: vec![fence],
            },
        })
    }

    /// The children `parent` has now, read between two reads of its record. `None` when the
    /// parent's life ended around the read, so the list may be a stranger's. A failed read is
    /// always an error: the kernel lists children by filtering every process by parent pid, so
    /// a parent's exit empties the list and never fails it.
    fn children(&self, parent: ProcessIdentity) -> Result<Option<Vec<u32>>, ObserveError> {
        if self.current(parent)?.is_none() {
            return Ok(None);
        }
        let children = self
            .reads
            .children(parent.pid)
            .map_err(|source| kernel("proc_listchildpids", parent.pid, source))?;
        Ok(self.current(parent)?.map(|_| children))
    }

    fn record(&self, pid: u32) -> Result<Option<ProcessRecord>, ObserveError> {
        self.reads
            .record(pid)
            .map_err(|source| kernel("proc_pidinfo", pid, source))
    }

    /// Apply one change to `process`'s registration, keyed by its unique id; `false` when the
    /// pid names no running process (`ESRCH`).
    fn change(&self, process: ProcessIdentity, flags: u16) -> Result<bool, ObserveError> {
        let cookie = usize::try_from(process.birth.0).map_err(|error| {
            kernel(
                "kevent(EVFILT_PROC)",
                process.pid,
                io::Error::new(io::ErrorKind::InvalidData, error),
            )
        })?;
        let change = libc::kevent {
            ident: usize::try_from(process.pid).map_err(|error| {
                kernel(
                    "kevent(EVFILT_PROC)",
                    process.pid,
                    io::Error::new(io::ErrorKind::InvalidData, error),
                )
            })?,
            filter: libc::EVFILT_PROC,
            flags: flags | libc::EV_UDATA_SPECIFIC,
            fflags: WATCHED,
            data: 0,
            udata: std::ptr::without_provenance_mut(cookie),
        };
        // SAFETY: one change read from live memory, no event list, on a live queue.
        let changed = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if changed == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        Err(kernel("kevent(EVFILT_PROC)", process.pid, error))
    }
}

fn kernel(call: &'static str, pid: u32, source: io::Error) -> ObserveError {
    ObserveError::Kernel { call, pid, source }
}

fn identity(record: &ProcessRecord) -> ProcessIdentity {
    ProcessIdentity {
        pid: record.pid,
        birth: BirthToken(record.unique_id),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};

    use super::*;
    use crate::api::dto::ExitStatus;
    use crate::api::process::{ProcessCoverage, ProcessExit};
    use crate::fork_lock::Spawn;

    /// `/bin/bash -c script` (not `/bin/sh`, a stub that execs the selected shell), held before
    /// its exec until it is watched. The pre-exec hook reports the child's pid, then waits for
    /// the observer; `extra` becomes the child's fd 3.
    fn watched(script: &str, extra: Option<OwnedFd>) -> (KqueueObserver, Child) {
        let (ready_read, ready_write) = std::io::pipe().unwrap();
        let (gate_read, mut gate_write) = std::io::pipe().unwrap();
        let mut command = Command::new("/bin/bash");
        command.args(["-c", script]).stdout(Stdio::piped());
        let ready = ready_write.as_raw_fd();
        let gate = gate_read.as_raw_fd();
        let extra = extra.as_ref().map(AsRawFd::as_raw_fd);
        // SAFETY: the hook calls only async-signal-safe functions.
        unsafe {
            command.pre_exec(move || {
                let pid = libc::getpid().to_ne_bytes();
                if libc::write(ready, pid.as_ptr().cast(), pid.len()) != 4 {
                    return Err(io::Error::last_os_error());
                }
                let mut byte = 0_u8;
                if libc::read(gate, (&raw mut byte).cast(), 1) != 1 {
                    return Err(io::Error::last_os_error());
                }
                // Last, since fd 3 may be `ready` or `gate`. `dup2` onto itself keeps the
                // close-on-exec flag every Rust pipe is opened with, so it is cleared instead.
                let inherited = match extra {
                    None => true,
                    Some(3) => libc::fcntl(3, libc::F_SETFD, 0) == 0,
                    Some(extra) => libc::dup2(extra, 3) == 3,
                };
                if !inherited {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        std::thread::scope(|scope| {
            let spawned = scope.spawn(|| command.spawn_locked().expect("spawn"));
            let mut pid = [0_u8; 4];
            std::io::Read::read_exact(&mut &ready_read, &mut pid).expect("child pid");
            let pid = u32::try_from(i32::from_ne_bytes(pid)).expect("pid");
            let observer = KqueueObserver::watch(pid).expect("watch the held root");
            gate_write.write_all(b"x").expect("release");
            let child = spawned.join().expect("spawner");
            assert_eq!(child.id(), pid);
            (observer, child)
        })
    }

    /// Every failure an `Err` carries, one by one.
    fn flatten(error: ObserveError, into: &mut Vec<ObserveError>) {
        match error {
            ObserveError::Several { first, rest } => {
                into.push(*first);
                into.extend(rest);
            }
            error => into.push(error),
        }
    }

    /// Fold until `pid` exits, keeping every failure the observer returned on the way: they
    /// are values the caller checks, never discarded.
    fn until_exited(observer: &mut KqueueObserver, pid: u32) -> Vec<ObserveError> {
        let mut failures = Vec::new();
        while observer.fold().live(pid).is_some() {
            if let Err(error) = observer.wait() {
                flatten(error, &mut failures);
            }
        }
        failures
    }

    /// A file holding `ready`, which a held cat prints only once it runs.
    fn marker(name: &str) -> String {
        let marker = std::env::temp_dir().join(format!("cowshed-{name}-{}", std::process::id()));
        std::fs::write(&marker, "ready\n").unwrap();
        marker.to_str().unwrap().to_owned()
    }

    fn argv(bytes: &[&str]) -> Vec<CommandArg> {
        bytes
            .iter()
            .map(|&argument| CommandArg::from(argument))
            .collect()
    }

    /// Two children held on a pipe while they are enumerated: each is retained with its pid,
    /// parent, exec'd image and exit, and the tree says forks were not counted.
    #[test]
    fn held_children_are_retained_with_their_parent_image_and_exit() {
        // Each child prints the marker only once it runs cat, then blocks reading fd 3.
        let marker = marker("held");
        let (hold_read, hold_write) = std::io::pipe().unwrap();
        let script = format!(
            "/bin/cat {marker} - <&3 & echo $!\n\
             /bin/cat {marker} - <&3 & echo $!\n\
             wait"
        );
        let (mut observer, mut child) = watched(&script, Some(hold_read.into()));
        let root = child.id();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let (mut expected, mut running) = (Vec::new(), 0);
        while expected.len() < 2 || running < 2 {
            let mut line = String::new();
            assert_ne!(
                stdout.read_line(&mut line).unwrap(),
                0,
                "the root's output ended"
            );
            match line.trim() {
                "ready" => running += 1,
                pid => expected.push(pid.parse::<u32>().expect("child pid")),
            }
        }
        // Both children run cat: the root's fork is queued, and the children read now are
        // past their exec.
        observer
            .drain()
            .expect("no read fails while both children run cat");
        for &pid in &expected {
            let process = observer.fold().live(pid).expect("adopted");
            assert_eq!(observer.fold().image(process).unwrap().program, "/bin/cat");
        }
        drop(hold_write);
        let failures = until_exited(&mut observer, root);
        assert!(failures.is_empty(), "{failures:?}");
        assert!(child.wait().unwrap().success());
        std::fs::remove_file(&marker).unwrap();

        let nodes: Vec<_> = observer.fold().nodes().collect();
        assert_eq!(nodes.len(), 3, "{nodes:?}");
        assert_eq!(nodes[0].identity.pid, root);
        assert_eq!(nodes[0].image.program, "/bin/bash");
        assert_eq!(nodes[0].image.argv, argv(&["/bin/bash", "-c", &script]));
        let exited = Some(&ProcessExit {
            status: ExitStatus::Exited { code: 0 },
            exited_at: nodes[0].exit.unwrap().exited_at.clone(),
        });
        assert_eq!(nodes[0].exit, exited);
        let mut children: Vec<u32> = nodes[1..].iter().map(|node| node.identity.pid).collect();
        children.sort_unstable();
        expected.sort_unstable();
        assert_eq!(children, expected);
        for node in &nodes[1..] {
            assert_eq!(node.parent, Some(nodes[0].identity));
            assert_eq!(node.ppid, root);
            assert_eq!(node.image.program, "/bin/cat");
            assert_eq!(node.image.argv, argv(&["/bin/cat", &marker, "-"]));
            assert_eq!(
                node.exit.map(|exit| &exit.status),
                Some(&ExitStatus::Exited { code: 0 })
            );
        }
        assert_eq!(
            observer.fold().coverage(),
            &ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UncountedFork { pid: root }
            }
        );
    }

    /// A process that never forks is the one tree macOS can prove complete. The root execs cat,
    /// which prints the marker only once it runs and then holds on fd 3, so its exec is folded
    /// while it runs.
    #[test]
    fn a_root_that_only_execs_is_complete() {
        let marker = marker("exec");
        let (hold_read, hold_write) = std::io::pipe().unwrap();
        let (mut observer, mut child) = watched(
            &format!("exec /bin/cat {marker} - <&3"),
            Some(hold_read.into()),
        );
        let root = child.id();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        assert_eq!(line, "ready\n");
        observer.drain().expect("the exec, read while cat runs");
        drop(hold_write);
        let failures = until_exited(&mut observer, root);
        assert!(failures.is_empty(), "{failures:?}");
        assert!(child.wait().unwrap().success());
        std::fs::remove_file(&marker).unwrap();
        let nodes: Vec<_> = observer.fold().nodes().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].image.program, "/bin/cat");
        assert_eq!(nodes[0].image.argv, argv(&["/bin/cat", &marker, "-"]));
        assert_eq!(observer.fold().coverage(), &ProcessCoverage::Complete);
    }

    /// A burst of children forked and reaped faster than they can be enumerated: every child
    /// the observer retained is the burst's, and the tree never claims to be complete. The
    /// number it missed is the measured gap. A child read mid-exec answers `EIO` or `EINVAL`:
    /// the observer returns each such failure, and the test checks every one.
    #[test]
    fn a_burst_reaped_before_enumeration_is_a_gap_never_complete() {
        let burst = 64;
        let script =
            format!("i=0; while [ $i -lt {burst} ]; do /usr/bin/true burst; i=$((i+1)); done");
        let (mut observer, mut child) = watched(&script, None);
        let root = child.id();
        let failures = until_exited(&mut observer, root);
        assert!(child.wait().unwrap().success());
        let nodes: Vec<_> = observer.fold().nodes().collect();
        let retained = nodes.len() - 1;
        for node in &nodes[1..] {
            assert_eq!(node.parent, Some(nodes[0].identity));
            assert!(
                node.image.program == "/usr/bin/true" || node.image.program == "/bin/bash",
                "{node:?}"
            );
        }
        for failure in &failures {
            let (call, pid, errno) = failed(failure);
            assert_eq!(call, "KERN_PROCARGS2", "{failure}");
            assert!(matches!(errno, Some(libc::EIO | libc::EINVAL)), "{failure}");
            assert!(
                nodes[1..].iter().any(|node| node.identity.pid == pid),
                "{failure}"
            );
        }
        assert!(retained <= burst);
        assert_eq!(
            observer.fold().coverage(),
            &ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UncountedFork { pid: root }
            }
        );
        println!(
            "kqueue burst: retained {retained} of {burst} children; {} image reads failed",
            failures.len()
        );
    }

    /// Kernel reads answered from a script, one queued answer per read, so a test sets the
    /// identity a pid names at each instant the observer asks. An unscripted read fails the test.
    #[derive(Default)]
    struct Scripted {
        records: std::cell::RefCell<
            std::collections::HashMap<
                u32,
                std::collections::VecDeque<io::Result<Option<ProcessRecord>>>,
            >,
        >,
        images: std::cell::RefCell<
            std::collections::HashMap<u32, std::collections::VecDeque<io::Result<ProcessImage>>>,
        >,
        children: std::cell::RefCell<
            std::collections::HashMap<u32, std::collections::VecDeque<io::Result<Vec<u32>>>>,
        >,
        /// The reader's effective uid; root unless a test sets it.
        reader: u32,
    }

    impl Scripted {
        fn record(self, pid: u32, answers: &[Option<ProcessRecord>]) -> Self {
            self.records
                .borrow_mut()
                .entry(pid)
                .or_default()
                .extend(answers.iter().copied().map(Ok));
            self
        }

        fn record_failure(self, pid: u32, errno: i32) -> Self {
            self.records
                .borrow_mut()
                .entry(pid)
                .or_default()
                .push_back(Err(io::Error::from_raw_os_error(errno)));
            self
        }

        fn image(self, pid: u32, answer: io::Result<ProcessImage>) -> Self {
            self.images
                .borrow_mut()
                .entry(pid)
                .or_default()
                .push_back(answer);
            self
        }

        fn children(self, pid: u32, answer: io::Result<Vec<u32>>) -> Self {
            self.children
                .borrow_mut()
                .entry(pid)
                .or_default()
                .push_back(answer);
            self
        }

        fn reading_as(self, reader: u32) -> Self {
            Self { reader, ..self }
        }
    }

    fn next<T>(
        script: &std::cell::RefCell<std::collections::HashMap<u32, std::collections::VecDeque<T>>>,
        what: &str,
        pid: u32,
    ) -> T {
        script
            .borrow_mut()
            .get_mut(&pid)
            .and_then(std::collections::VecDeque::pop_front)
            .unwrap_or_else(|| panic!("an unscripted {what} read of process {pid}"))
    }

    impl ProcessReads for Scripted {
        fn record(&self, pid: u32) -> io::Result<Option<ProcessRecord>> {
            next(&self.records, "record", pid)
        }

        fn image(&self, pid: u32) -> io::Result<ProcessImage> {
            next(&self.images, "image", pid)
        }

        fn children(&self, pid: u32) -> io::Result<Vec<u32>> {
            next(&self.children, "child list", pid)
        }

        fn reader_uid(&self) -> u32 {
            self.reader
        }
    }

    /// Every scripted process runs as this uid.
    const OWNER: u32 = 501;

    fn record(pid: u32, unique_id: u64, parent_unique_id: u64) -> ProcessRecord {
        ProcessRecord {
            pid,
            ppid: 1,
            unique_id,
            parent_unique_id,
            uid: OWNER,
            started_seconds: 1_700_000_000,
        }
    }

    fn life(pid: u32, unique_id: u64) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            birth: BirthToken(unique_id),
        }
    }

    fn shell_image(program: &str) -> ProcessImage {
        ProcessImage {
            program: program.to_owned(),
            argv: argv(&[program]),
        }
    }

    /// An observer over scripted reads and a real, empty kqueue, holding `fold` and the
    /// registrations named.
    fn scripted(
        reads: Scripted,
        fold: ProcessTreeFold,
        registered: &[ProcessIdentity],
    ) -> KqueueObserver<Scripted> {
        // SAFETY: kqueue takes nothing and returns a new descriptor.
        let queue = unsafe { libc::kqueue() };
        assert!(queue >= 0, "kqueue: {}", io::Error::last_os_error());
        KqueueObserver {
            // SAFETY: a fresh descriptor this observer owns.
            queue: unsafe { OwnedFd::from_raw_fd(queue) },
            reads,
            fold,
            registered: registered.iter().copied().collect(),
        }
    }

    fn event(process: ProcessIdentity, fflags: u32, data: isize) -> libc::kevent {
        libc::kevent {
            ident: usize::try_from(process.pid).unwrap(),
            filter: libc::EVFILT_PROC,
            flags: libc::EV_UDATA_SPECIFIC,
            fflags,
            data,
            udata: std::ptr::without_provenance_mut(usize::try_from(process.birth.0).unwrap()),
        }
    }

    /// A root, a first life of pid 900, and a second life of pid 900 forked before the first
    /// life's exit was folded.
    fn reused_pid_fold() -> (
        ProcessTreeFold,
        ProcessIdentity,
        ProcessIdentity,
        ProcessIdentity,
    ) {
        let (root, old, new) = (life(800, 1), life(900, 2), life(900, 3));
        let mut fold = ProcessTreeFold::default();
        let at = utc_at_second(1_700_000_000).unwrap();
        fold.apply(ProcessObservation::Root {
            process: root,
            ppid: 1,
            at: at.clone(),
            image: shell_image("/bin/bash"),
        })
        .unwrap();
        for process in [old, new] {
            fold.apply(ProcessObservation::Forked {
                process,
                parent: root,
                at: at.clone(),
            })
            .unwrap();
        }
        (fold, root, old, new)
    }

    /// The older life's late exit and exec land on that life by its cookie; the life now
    /// holding the pid keeps running its own image.
    #[test]
    fn an_older_life_s_queued_events_never_touch_the_life_now_holding_its_pid() {
        let (fold, root, old, new) = reused_pid_fold();
        // When the old life's exec is handled, the pid names the new life.
        let reads = Scripted::default().record(900, &[Some(record(900, 3, 1))]);
        let mut observer = scripted(reads, fold, &[root, old, new]);
        observer.handle(&event(old, libc::NOTE_EXEC, 0)).unwrap();
        observer.handle(&event(old, libc::NOTE_EXIT, 0)).unwrap();

        assert_eq!(observer.fold().live(900), Some(new));
        let nodes: Vec<_> = observer.fold().nodes().collect();
        let old_node = nodes.iter().find(|node| node.identity == old).unwrap();
        let new_node = nodes.iter().find(|node| node.identity == new).unwrap();
        assert_eq!(
            old_node.exit.map(|exit| &exit.status),
            Some(&ExitStatus::Exited { code: 0 })
        );
        assert_eq!(new_node.exit, None);
        assert_eq!(
            new_node.image,
            &shell_image("/bin/bash"),
            "no stranger image"
        );
        assert!(
            !observer.registered.contains(&old),
            "its exit ended its registration"
        );
    }

    /// An event of a registration the observer withdrew is not the tree's.
    #[test]
    fn an_event_of_a_withdrawn_registration_is_ignored() {
        let (fold, root, old, new) = reused_pid_fold();
        let mut observer = scripted(Scripted::default(), fold, &[root, old, new]);
        let stranger = life(900, 77);
        observer
            .handle(&event(stranger, libc::NOTE_EXIT | libc::NOTE_FORK, 0))
            .unwrap();
        assert_eq!(observer.fold().live(900), Some(new));
        assert_eq!(observer.fold().nodes().count(), 3);
    }

    /// A running process the test holds, so registrations against its pid succeed.
    fn held_process() -> (Child, u32) {
        let child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn_locked()
            .expect("spawn cat");
        let pid = child.id();
        (child, pid)
    }

    fn release(mut child: Child) {
        drop(child.stdin.take());
        child.wait().expect("reap");
    }

    /// The pid named another life after the registration than before it: nothing is adopted,
    /// and the registration is withdrawn.
    #[test]
    fn a_registration_whose_pid_changed_life_is_withdrawn_unadopted() {
        let (child, pid) = held_process();
        let before = record(pid, 40, 1);
        for after in [Some(record(pid, 41, 1)), None] {
            let reads = Scripted::default().record(pid, &[after]);
            let mut observer = scripted(reads, ProcessTreeFold::default(), &[]);
            assert_eq!(observer.attach(before).unwrap(), None);
            assert!(observer.registered.is_empty());
            // Deleting it again finds no registration: the observer already withdrew it.
            let error = observer
                .change(life(pid, 40), libc::EV_DELETE)
                .expect_err("no registration remains");
            assert!(
                matches!(&error, ObserveError::Kernel { source, .. } if source.raw_os_error() == Some(libc::ENOENT)),
                "{error}"
            );
        }
        // The same life before and after: adopted, and its registration stays.
        let reads = Scripted::default().record(pid, &[Some(before)]);
        let mut observer = scripted(reads, ProcessTreeFold::default(), &[]);
        assert_eq!(observer.attach(before).unwrap(), Some(before));
        assert!(observer.registered.contains(&life(pid, 40)));
        assert!(observer.change(life(pid, 40), libc::EV_DELETE).unwrap());
        release(child);
    }

    /// An image or child list read while the pid changed life is a stranger's and is never
    /// attributed; read while the life ran throughout, it is.
    #[test]
    fn an_image_or_child_list_read_across_a_change_of_life_is_discarded() {
        let process = life(910, 5);
        let ours = Some(record(910, 5, 1));
        let stranger = Some(record(910, 6, 1));
        let reads = Scripted::default()
            .record(
                910,
                &[ours, stranger, ours, None, ours, ours, ours, stranger],
            )
            .image(910, Ok(shell_image("/stranger")))
            .image(910, Ok(shell_image("/stranger")))
            .image(910, Ok(shell_image("/bin/cat")))
            .children(910, Ok(vec![911, 912]));
        let observer = scripted(reads, ProcessTreeFold::default(), &[]);
        assert_eq!(observer.image(process).unwrap(), None, "changed life");
        assert_eq!(observer.image(process).unwrap(), None, "exited");
        assert_eq!(
            observer.image(process).unwrap(),
            Some(shell_image("/bin/cat"))
        );
        assert_eq!(observer.children(process).unwrap(), None, "changed life");
    }

    fn failed(error: &ObserveError) -> (&'static str, u32, Option<i32>) {
        match error {
            ObserveError::Kernel { call, pid, source } => (*call, *pid, source.raw_os_error()),
            other => panic!("not a kernel error: {other}"),
        }
    }

    /// A failed read is the kernel's error with its call and errno, never swallowed into a gap
    /// or an empty answer, whether or not its life ran on: `EIO` and `EINVAL` included, which
    /// the kernel also answers between two images.
    #[test]
    fn a_failed_read_is_returned_with_call_and_errno_even_after_its_life_ended() {
        let process = life(920, 9);
        let ours = Some(record(920, 9, 1));
        for errno in [libc::ENOMEM, libc::EIO, libc::EINVAL] {
            for after in [ours, None, Some(record(920, 10, 1))] {
                let reads = Scripted::default()
                    .record(920, &[ours, after, ours])
                    .image(920, Err(io::Error::from_raw_os_error(errno)))
                    .children(920, Err(io::Error::from_raw_os_error(libc::EFAULT)));
                let observer = scripted(reads, ProcessTreeFold::default(), &[]);
                assert_eq!(
                    failed(&observer.image(process).unwrap_err()),
                    ("KERN_PROCARGS2", 920, Some(errno))
                );
                assert_eq!(
                    failed(&observer.children(process).unwrap_err()),
                    ("proc_listchildpids", 920, Some(libc::EFAULT))
                );
            }
        }
    }

    /// A record read after the image read that itself fails loses neither failure.
    #[test]
    fn a_failed_fence_after_a_failed_image_read_keeps_both() {
        let process = life(940, 13);
        let ours = Some(record(940, 13, 1));
        let reads = Scripted::default()
            .record(940, &[ours])
            .record_failure(940, libc::EPERM)
            .image(940, Err(io::Error::from_raw_os_error(libc::EIO)));
        let error = scripted(reads, ProcessTreeFold::default(), &[])
            .image(process)
            .unwrap_err();
        let ObserveError::Several { first, rest } = error else {
            panic!("{error}");
        };
        assert_eq!(
            failed(first.as_ref()),
            ("KERN_PROCARGS2", 940, Some(libc::EIO))
        );
        assert_eq!(
            rest.iter().map(failed).collect::<Vec<_>>(),
            [("proc_pidinfo", 940, Some(libc::EPERM))]
        );
        // A read that succeeded is unattributable without its fence: the fence's failure.
        let reads = Scripted::default()
            .record(940, &[ours])
            .record_failure(940, libc::EPERM)
            .image(940, Ok(shell_image("/bin/cat")));
        let error = scripted(reads, ProcessTreeFold::default(), &[])
            .image(process)
            .unwrap_err();
        assert_eq!(failed(&error), ("proc_pidinfo", 940, Some(libc::EPERM)));
    }

    /// `KERN_PROCARGS2`'s `EINVAL` is named a refusal where the kernel's credential rule refuses
    /// the reader, and keeps that `EINVAL`.
    #[test]
    fn an_einval_image_read_is_a_refusal_only_by_credentials() {
        let process = life(930, 11);
        let ours = Some(record(930, 11, 1));
        let einval = || Err(io::Error::from_raw_os_error(libc::EINVAL));
        let refused = |reads: Scripted| match scripted(reads, ProcessTreeFold::default(), &[])
            .image(process)
        {
            Err(ObserveError::ImageNotPermitted {
                pid,
                uid,
                reader,
                source,
            }) => (pid, uid, reader, source.raw_os_error()),
            other => panic!("{other:?}"),
        };
        let another_user = Scripted::default()
            .reading_as(OWNER + 1)
            .record(930, &[ours, ours])
            .image(930, einval());
        assert_eq!(
            refused(another_user),
            (930, OWNER, OWNER + 1, Some(libc::EINVAL))
        );
        // A set-id exec between the two reads: the life now runs as root.
        let set_id = Some(ProcessRecord {
            uid: 0,
            ..record(930, 11, 1)
        });
        let exec_set_id = Scripted::default()
            .reading_as(OWNER)
            .record(930, &[ours, set_id])
            .image(930, einval());
        assert_eq!(refused(exec_set_id), (930, 0, OWNER, Some(libc::EINVAL)));
        // Another user's `EIO` is no refusal: the kernel checked credentials before it.
        let unmapped = Scripted::default()
            .reading_as(OWNER + 1)
            .record(930, &[ours, ours])
            .image(930, Err(io::Error::from_raw_os_error(libc::EIO)));
        let error = scripted(unmapped, ProcessTreeFold::default(), &[])
            .image(process)
            .unwrap_err();
        assert_eq!(failed(&error), ("KERN_PROCARGS2", 930, Some(libc::EIO)));
    }

    /// Failed reads in a batch drop nothing: the exit coalesced with a failed exec, and every
    /// later event, are folded, and every failure of the batch is returned.
    #[test]
    fn a_failed_read_never_drops_the_rest_of_its_batch() {
        let (root, first, second) = (life(800, 1), life(900, 2), life(901, 3));
        let mut fold = ProcessTreeFold::default();
        let at = utc_at_second(1_700_000_000).unwrap();
        fold.apply(ProcessObservation::Root {
            process: root,
            ppid: 1,
            at: at.clone(),
            image: shell_image("/bin/bash"),
        })
        .unwrap();
        for process in [first, second] {
            fold.apply(ProcessObservation::Forked {
                process,
                parent: root,
                at: at.clone(),
            })
            .unwrap();
        }
        let enomem = || Err(io::Error::from_raw_os_error(libc::ENOMEM));
        let reads = Scripted::default()
            .record(900, &[Some(record(900, 2, 1)); 2])
            .image(900, enomem())
            .record(901, &[Some(record(901, 3, 1)); 2])
            .image(901, enomem());
        let mut observer = scripted(reads, fold, &[root, first, second]);
        let error = observer
            .fold_batch(&[
                event(first, libc::NOTE_EXEC | libc::NOTE_EXIT, 0),
                event(second, libc::NOTE_EXEC, 0),
                event(root, libc::NOTE_EXIT, 0),
            ])
            .expect_err("two reads failed");
        let ObserveError::Several { first: one, rest } = error else {
            panic!("{error}");
        };
        assert_eq!(
            failed(one.as_ref()),
            ("KERN_PROCARGS2", 900, Some(libc::ENOMEM))
        );
        assert_eq!(
            rest.iter().map(failed).collect::<Vec<_>>(),
            [("KERN_PROCARGS2", 901, Some(libc::ENOMEM))]
        );
        let exited = |process| {
            observer
                .fold()
                .nodes()
                .find(|node| node.identity == process)
                .is_some_and(|node| node.exit.is_some())
        };
        assert!(exited(first), "the exit beside the failed exec");
        assert!(exited(root), "the event after both failures");
        assert!(!exited(second));
        assert!(!observer.registered.contains(&first));
        assert!(!observer.registered.contains(&root));
        assert_eq!(
            observer.fold().coverage(),
            &ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnreadImage { pid: 900 }
            },
            "the failed read is a gap as well as an error"
        );
    }

    /// libproc answers a failed child list with 0 and errno: an error, never an empty list.
    /// An errno left over from before the call never makes an empty list a failure.
    #[test]
    fn a_failed_child_list_is_an_error_never_an_empty_list() {
        let (child, pid) = held_process();
        let me = libc::pid_t::try_from(std::process::id()).unwrap();
        // This process has a child, so the kernel copies a pid out to the unwritable address.
        // SAFETY: the kernel's copyout rejects the address; nothing here writes through it.
        let unwritable = unsafe { list_child_pids(me, std::ptr::without_provenance_mut(1), 256) };
        assert_eq!(unwritable.unwrap_err().raw_os_error(), Some(libc::EFAULT));
        let mut pids = [0_i32; 4];
        // SAFETY: `__error` names this thread's errno.
        unsafe { *libc::__error() = libc::EFAULT };
        // SAFETY: `pids` is writable for 16 bytes.
        let childless = unsafe {
            list_child_pids(
                libc::pid_t::try_from(pid).unwrap(),
                pids.as_mut_ptr().cast(),
                16,
            )
        };
        assert_eq!(childless.unwrap(), 0, "cat has no children");
        release(child);
    }
}
