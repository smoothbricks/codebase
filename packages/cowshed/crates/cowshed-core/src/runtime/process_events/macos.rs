//! macOS's process event source: kqueue `EVFILT_PROC`, which reports each registered life's
//! `NOTE_FORK`, `NOTE_EXEC` and `NOTE_EXIT`/`NOTE_EXITSTATUS` as events, and `proc_listchildpids`
//! for the children a fork left.
//!
//! A registration carries the life's unique id as its cookie (`EV_UDATA_SPECIFIC`, so two lives
//! of one pid are two registrations), and every event names its life by that cookie, never by its
//! pid. xnu grants `NOTE_EXITSTATUS` only for a child or a life this observer may SIGKILL
//! (`filt_procattach`) and refuses any other with `EACCES`; such a life is watched for
//! `NOTE_FORK`/`NOTE_EXEC`/`NOTE_EXIT` alone, which the kernel grants for every process.
//! Every transition arrives as an event, so nothing is left to a survey.

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use super::{ExitReport, ObservationPurpose, ObserveError, Report, kernel};
use crate::api::process::ProcessCoverageGap;
use crate::runtime::process_tree::{BirthToken, ProcessIdentity};

/// The call that reads a process's record.
pub(super) const RECORD: &str = "proc_pidinfo";
/// The call that reads the image a process runs.
pub(super) const IMAGE: &str = "KERN_PROCARGS2";
/// The errno the image read answers a reader it refuses, among its other causes.
pub(super) const IMAGE_REFUSAL: i32 = libc::EINVAL;

/// Whether a failed image read is what the kernel answers a reader it refuses.
pub(super) fn image_refused(error: &io::Error) -> bool {
    error.raw_os_error() == Some(IMAGE_REFUSAL)
}

/// The call that lists a process's children.
pub(super) const CHILDREN: &str = "proc_listchildpids";
/// The call that reads the job's group.
pub(super) const GROUP: &str = "proc_listpgrppids";
/// The call that signals one life.
pub(super) const SIGNAL: &str = "proc_signal_with_audittoken";
/// The call that registers a life's exit watch.
pub(super) const WATCH: &str = "kevent(EVFILT_PROC)";
/// What reports a life's wait status with its exit.
pub(super) const EXIT_STATUS: &str = "NOTE_EXITSTATUS";

const WATCHED: u32 = libc::NOTE_FORK | libc::NOTE_EXEC | libc::NOTE_EXIT | libc::NOTE_EXITSTATUS;
/// What the kernel grants for a life it refuses `NOTE_EXITSTATUS` with `EACCES`.
const WATCHED_WITHOUT_STATUS: u32 = libc::NOTE_FORK | libc::NOTE_EXEC | libc::NOTE_EXIT;

/// The exit watch the kernel granted one registered life.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExitWatch {
    /// `NOTE_EXIT` carries the life's wait status.
    Status,
    /// `NOTE_EXIT` alone: the exit is joined, its status is withheld from this observer.
    Withheld,
}

impl ExitWatch {
    /// kqueue's registration holds no descriptor of the life: its unique id is the fence.
    pub(super) fn held(&self) -> Option<BorrowedFd<'_>> {
        None
    }

    /// The watch of a registration a test scripts without a process behind it.
    #[cfg(test)]
    pub(super) fn scripted() -> Self {
        Self::Status
    }
}

/// What one registration of [`Source`] holds.
pub(super) type Watch = ExitWatch;

/// A tree observed through kqueue has no gap before an event names one: a root that never
/// forks is proven complete.
pub(super) fn standing_gap(_root: u32) -> Option<ProcessCoverageGap> {
    None
}

/// The pids whose parent is `pid` now (`proc_listchildpids`).
pub(super) fn children(pid: u32) -> io::Result<Vec<u32>> {
    let parent = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    let mut capacity = 64_usize;
    loop {
        let mut pids: Vec<libc::pid_t> = vec![0; capacity];
        let bytes =
            libc::c_int::try_from(capacity * size_of::<libc::pid_t>()).map_err(io::Error::other)?;
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

/// The kqueue every registered life reports to.
#[derive(Debug)]
pub(super) struct Source {
    queue: OwnedFd,
}

impl AsRawFd for Source {
    /// Readable when an event waits for [`Source::collect`].
    fn as_raw_fd(&self) -> RawFd {
        self.queue.as_raw_fd()
    }
}

impl Source {
    /// Allocate the queue before any workload can be forked.
    pub(super) fn open() -> Result<Self, ObserveError> {
        // SAFETY: kqueue takes nothing and returns a new descriptor.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(kernel("kqueue", 0, io::Error::last_os_error()));
        }
        // SAFETY: a fresh descriptor this function alone owns.
        let queue = unsafe { OwnedFd::from_raw_fd(queue) };
        Ok(Self { queue })
    }

    /// A duplicate queue descriptor wakes the sole writer; it is not an observation reader.
    pub(super) fn wake_handle(&self) -> io::Result<OwnedFd> {
        self.queue.try_clone()
    }

    /// kqueue reports every fork, exec and exit unasked: a survey finds nothing to add.
    pub(super) fn survey(
        &mut self,
        _purpose: ObservationPurpose,
        _registered: &HashMap<ProcessIdentity, Watch>,
        _reports: &mut Vec<Result<Report, ObserveError>>,
    ) {
    }

    /// Take one batch of events, waiting up to `within` for the first (`None`: until one comes):
    /// how many the kernel handed over, each registered life's appended to `reports`. The kernel
    /// keeps none of them. An `Err` is `kevent` itself failing.
    pub(super) fn collect(
        &mut self,
        within: Option<Duration>,
        registered: &HashMap<ProcessIdentity, Watch>,
        reports: &mut Vec<Result<Report, ObserveError>>,
    ) -> Result<usize, ObserveError> {
        let timeout = within.map(|within| libc::timespec {
            tv_sec: libc::time_t::try_from(within.as_secs()).unwrap_or(libc::time_t::MAX),
            tv_nsec: libc::c_long::from(within.subsec_nanos()),
        });
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
                    timeout
                        .as_ref()
                        .map_or(std::ptr::null(), std::ptr::from_ref),
                )
            };
            match usize::try_from(ready) {
                Ok(ready) => break ready,
                Err(_) => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(kernel("kevent", 0, error));
                    }
                }
            }
        };
        reports.extend(
            events[..ready]
                .iter()
                .filter_map(|event| decode(event, registered).transpose()),
        );
        Ok(ready)
    }

    /// Register `process` for its events, keyed by its unique id: the exit watch the kernel
    /// granted, or `None` when the pid names no running process (`ESRCH`). A life the kernel
    /// refuses `NOTE_EXITSTATUS` with `EACCES` is registered without it, which xnu grants for
    /// every process, so its exit is joined all the same.
    pub(super) fn register(
        &mut self,
        process: ProcessIdentity,
    ) -> Result<Option<ExitWatch>, ObserveError> {
        let add = libc::EV_ADD | libc::EV_CLEAR;
        let (watch, changed) = match self.change(process, add, WATCHED)? {
            Err(refused) if refused.raw_os_error() == Some(libc::EACCES) => (
                ExitWatch::Withheld,
                self.change(process, add, WATCHED_WITHOUT_STATUS)?,
            ),
            changed => (ExitWatch::Status, changed),
        };
        match changed {
            Ok(()) => Ok(Some(watch)),
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(None),
            Err(error) => Err(kernel("kevent(EVFILT_PROC)", process.pid, error)),
        }
    }

    /// Withdraw `process`'s registration; `false` when the pid names no running process
    /// (`ESRCH`). The registration is keyed by the life, so its watch names nothing more.
    pub(super) fn withdraw(
        &mut self,
        process: ProcessIdentity,
        _watch: &Watch,
    ) -> Result<bool, ObserveError> {
        match self.change(process, libc::EV_DELETE, WATCHED)? {
            Ok(()) => Ok(true),
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(false),
            Err(error) => Err(kernel("kevent(EVFILT_PROC)", process.pid, error)),
        }
    }

    /// Apply one change to `process`'s registration, keyed by its unique id. The outer `Err`
    /// is an identity the kernel cannot be handed; the inner one is the kernel's own answer.
    fn change(
        &self,
        process: ProcessIdentity,
        flags: u16,
        fflags: u32,
    ) -> Result<io::Result<()>, ObserveError> {
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
            fflags,
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
        Ok(if changed == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        })
    }
}

/// One kernel event as the report of the registered life it names. `None` for an event that is
/// no process event, or of a registration withdrawn unadopted.
fn decode(
    event: &libc::kevent,
    registered: &HashMap<ProcessIdentity, Watch>,
) -> Result<Option<Report>, ObserveError> {
    if event.filter != libc::EVFILT_PROC {
        return Ok(None);
    }
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
    let Some(&watch) = registered.get(&process) else {
        // A registration withdrawn unadopted: its queued events were never this tree's.
        return Ok(None);
    };
    // The bits of one event are coalesced. Its exit carries the wait status only under a watch
    // the kernel granted `NOTE_EXITSTATUS`.
    Ok(Some(Report {
        process,
        forked: fflags & libc::NOTE_FORK != 0,
        exec: fflags & libc::NOTE_EXEC != 0,
        exit: (fflags & libc::NOTE_EXIT != 0).then_some(match watch {
            ExitWatch::Status => ExitReport::Status(data),
            ExitWatch::Withheld => ExitReport::Withheld,
        }),
    }))
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::{BufRead, BufReader};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    use super::super::tests::{
        Scripted, TestFold, argv, failed, held_process, life, marker, record, release, scripted,
        until_exited, watched as watched_shell,
    };
    use super::super::{Darwin, KqueueObserver, identity};
    use super::*;
    use crate::api::dto::ExitStatus;
    use crate::api::process::{ProcessCoverage, ProcessExit};
    use crate::runtime::job_groups::process_record;
    use cowshed_os::fork_lock::Spawn;

    /// `/bin/bash -c script` (not `/bin/sh`, a stub that execs the selected shell), held before
    /// its exec until it is watched.
    fn watched(script: &str, extra: Option<OwnedFd>) -> (KqueueObserver, Child) {
        watched_shell("/bin/bash", script, extra)
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
            assert_eq!(
                &*observer.fold().image(process).unwrap().program,
                "/bin/cat"
            );
        }
        drop(hold_write);
        let failures = until_exited(&mut observer, root);
        assert!(failures.is_empty(), "{failures:?}");
        assert!(child.wait_bounded().unwrap().success());
        std::fs::remove_file(&marker).unwrap();

        let nodes: Vec<_> = observer.fold().nodes().collect();
        assert_eq!(nodes.len(), 3, "{nodes:?}");
        assert_eq!(nodes[0].identity.pid, root);
        assert_eq!(&*nodes[0].image.program, "/bin/bash");
        assert_eq!(
            &*nodes[0].image.argv,
            argv(&["/bin/bash", "-c", &script]).as_slice()
        );
        let exited = Some(&ProcessExit {
            status: Some(ExitStatus::Exited { code: 0 }),
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
            assert_eq!(&*node.image.program, "/bin/cat");
            assert_eq!(
                &*node.image.argv,
                argv(&["/bin/cat", &marker, "-"]).as_slice()
            );
            assert_eq!(
                node.exit.and_then(|exit| exit.status.as_ref()),
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
        assert!(child.wait_bounded().unwrap().success());
        std::fs::remove_file(&marker).unwrap();
        let nodes: Vec<_> = observer.fold().nodes().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(&*nodes[0].image.program, "/bin/cat");
        assert_eq!(
            &*nodes[0].image.argv,
            argv(&["/bin/cat", &marker, "-"]).as_slice()
        );
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
        assert!(child.wait_bounded().unwrap().success());
        let nodes: Vec<_> = observer.fold().nodes().collect();
        let retained = nodes.len() - 1;
        for node in &nodes[1..] {
            assert_eq!(node.parent, Some(nodes[0].identity));
            assert!(
                &*node.image.program == "/usr/bin/true" || &*node.image.program == "/bin/bash",
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

    /// The pid named another life after the registration than before it: nothing is adopted,
    /// and the registration is withdrawn.
    #[test]
    fn a_registration_whose_pid_changed_life_is_withdrawn_unadopted() {
        let (child, pid) = held_process();
        let before = record(pid, 40, 1);
        for after in [Some(record(pid, 41, 1)), None] {
            let reads = Scripted::default().record(pid, &[after]);
            let mut observer = scripted(reads, TestFold::default(), &[]);
            assert_eq!(observer.attach(before).unwrap(), None);
            assert!(observer.registered.is_empty());
            // Deleting it again finds no registration: the observer already withdrew it.
            let error = observer
                .source
                .withdraw(life(pid, 40), &ExitWatch::Status)
                .expect_err("no registration remains");
            assert!(
                matches!(&error, ObserveError::Kernel { source, .. } if source.raw_os_error() == Some(libc::ENOENT)),
                "{error}"
            );
        }
        // The same life before and after: adopted, and its registration stays.
        let reads = Scripted::default().record(pid, &[Some(before)]);
        let mut observer = scripted(reads, TestFold::default(), &[]);
        assert_eq!(observer.attach(before).unwrap(), Some(before));
        assert!(observer.registered.contains_key(&life(pid, 40)));
        assert!(
            observer
                .source
                .withdraw(life(pid, 40), &ExitWatch::Status)
                .unwrap()
        );
        release(child);
    }

    /// xnu `filt_procattach` refuses `NOTE_EXITSTATUS` for a process this observer may not
    /// SIGKILL -- launchd, pid 1, is one on every host -- and grants it for its own child. The
    /// one registration path answers each with the watch the kernel actually granted.
    #[test]
    fn registration_falls_back_to_exit_alone_where_the_kernel_withholds_the_status() {
        let mut observer = KqueueObserver::prepare_with(Darwin).expect("kqueue");
        let launchd = identity(
            &process_record(1)
                .expect("read launchd")
                .expect("launchd runs"),
        );
        assert_eq!(
            observer.source.register(launchd).expect("register launchd"),
            Some(ExitWatch::Withheld)
        );
        assert!(
            observer
                .source
                .withdraw(launchd, &ExitWatch::Withheld)
                .expect("withdraw launchd")
        );
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .spawn_locked()
            .expect("spawn cat");
        let pid = i32::try_from(child.id()).expect("pid");
        let own = identity(&process_record(pid).expect("read cat").expect("cat runs"));
        assert_eq!(
            observer.source.register(own).expect("register own child"),
            Some(ExitWatch::Status)
        );
        assert!(
            observer
                .source
                .withdraw(own, &ExitWatch::Status)
                .expect("withdraw own child")
        );
        drop(child.stdin.take());
        assert!(child.wait_bounded().expect("reap cat").success());
    }

    /// A member that runs a root command through `sudo -n` (bounded-exec's hang probe does) has a
    /// descendant the kernel refuses `NOTE_EXITSTATUS` for. It stays in custody under its
    /// withheld watch, its exit is joined, and only its status is absent. Needs this host's
    /// passwordless sudo grant for `/usr/sbin/lsof`; without it the test fails naming sudo's
    /// refusal. Every cow sandbox denies `/usr/bin/sudo` itself, so it runs in the host lane.
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_a_root_command_run_through_sudo_stays_in_custody_with_its_status_withheld() {
        let script = "/usr/bin/sudo -n /usr/sbin/lsof -nP -r 30 -p $$ & echo $!
wait $!";
        let (mut observer, mut child) = watched(script, None);
        let root = child.id();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).expect("sudo pid");
        let sudo: u32 = line.trim().parse().expect("sudo pid line");
        // lsof's first listing is the readiness event: sudo started it and it runs as root.
        line.clear();
        assert_ne!(
            stdout.read_line(&mut line).expect("lsof listing"),
            0,
            "sudo -n /usr/sbin/lsof ended before listing; this host must grant it without a password"
        );
        observer
            .drain_custody()
            .expect("custody registers a root descendant without a refusal");
        let sudo = observer.fold().live(sudo).expect("sudo adopted");
        assert_eq!(observer.registered.get(&sudo), Some(&ExitWatch::Status));
        let lsof = observer
            .fold()
            .nodes()
            .find(|node| node.parent == Some(sudo))
            .expect("lsof adopted under sudo")
            .identity;
        let record = process_record(i32::try_from(lsof.pid).unwrap())
            .unwrap()
            .expect("lsof runs");
        assert_eq!(record.uid, 0, "lsof runs as root");
        assert_eq!(observer.registered.get(&lsof), Some(&ExitWatch::Withheld));
        // Signalled itself, sudo relays SIGTERM to its root command and exits after it.
        // SAFETY: the null-free signal to this test's own grandchild, a same-user sudo.
        assert_eq!(
            unsafe { libc::kill(i32::try_from(sudo.pid).unwrap(), libc::SIGTERM) },
            0
        );
        while observer.fold().live(root).is_some() {
            observer
                .wait_custody(Duration::from_secs(3600))
                .expect("custody joins every exit");
        }
        child.wait_bounded().expect("reap the root");
        let exit = |process: ProcessIdentity| {
            observer
                .fold()
                .nodes()
                .find(|node| node.identity == process)
                .and_then(|node| node.exit.cloned())
                .expect("exit joined")
        };
        assert_eq!(exit(lsof).status, None, "the kernel withheld lsof's status");
        assert!(exit(sudo).status.is_some(), "sudo's own status is read");
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
