//! The writer's wake on macOS: a user event (`EVFILT_USER`) on the observer's own kqueue,
//! triggered through a duplicate of that queue. The writer's wait on the queue returns for it,
//! and the observer's drain passes over it: it is no process event.

use std::io;
use std::os::fd::{AsRawFd as _, OwnedFd};

use super::super::process_events::{KqueueObserver, ProcessReads};
use crate::error::{CowshedError, Result};

const CONTROL_EVENT: usize = usize::MAX;

/// What a caller wakes the writer by: a duplicate of the observer's queue.
pub(super) struct Wake(OwnedFd);

impl Wake {
    /// Register the control event on `observer`'s queue and keep a descriptor to trigger it by.
    pub(super) fn prepare<P: ProcessReads>(observer: &KqueueObserver<P>) -> Result<Self> {
        control(observer.as_raw_fd(), libc::EV_ADD | libc::EV_CLEAR, 0).map_err(|error| {
            CowshedError::internal(format!(
                "cannot prepare the process observer's control event: {error}"
            ))
        })?;
        let wake = observer.wake_handle().map_err(|error| {
            CowshedError::internal(format!(
                "cannot retain the process observer's wake descriptor: {error}"
            ))
        })?;
        Ok(Self(wake))
    }

    /// Wake the writer from its wait on the queue.
    pub(super) fn raise(&self) -> io::Result<()> {
        control(self.0.as_raw_fd(), 0, libc::NOTE_TRIGGER)
    }
}

fn control(queue: i32, flags: u16, fflags: u32) -> io::Result<()> {
    let change = libc::kevent {
        ident: CONTROL_EVENT,
        filter: libc::EVFILT_USER,
        flags,
        fflags,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    // SAFETY: one readable change, no returned events; the queue is owned by this caller.
    if unsafe { libc::kevent(queue, &change, 1, std::ptr::null_mut(), 0, std::ptr::null()) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;

    use super::super::*;
    use crate::api::dto::JobId;
    use crate::api::process::ProcessCoverage;

    /// Darwin's reads, except that the kernel refuses the read a flag names once it is set, as a
    /// loaded host answers: an image read with `EINVAL`, what `KERN_PROCARGS2` says of a process
    /// caught between two images, and a child list with `ESRCH`, a parent that vanished under the
    /// read.
    #[derive(Clone, Default)]
    struct KernelRefuses {
        images: Arc<std::sync::atomic::AtomicBool>,
        children: Arc<std::sync::atomic::AtomicBool>,
    }

    impl ProcessReads for KernelRefuses {
        fn record(
            &self,
            pid: u32,
        ) -> io::Result<Option<crate::runtime::job_groups::ProcessRecord>> {
            Darwin.record(pid)
        }

        fn image(&self, pid: u32) -> io::Result<crate::runtime::process_tree::ProcessImage> {
            if self.images.load(Ordering::SeqCst) {
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            } else {
                Darwin.image(pid)
            }
        }

        fn children(&self, pid: u32) -> io::Result<Vec<u32>> {
            if self.children.load(Ordering::SeqCst) {
                Err(io::Error::from_raw_os_error(libc::ESRCH))
            } else {
                Darwin.children(pid)
            }
        }

        fn reader_uid(&self) -> u32 {
            Darwin.reader_uid()
        }

        fn usage(
            &self,
            process: ProcessIdentity,
            held: Option<std::os::fd::BorrowedFd<'_>>,
            start: Option<crate::runtime::process_usage::StartStamp>,
            at: Instant,
        ) -> std::result::Result<
            crate::runtime::process_usage::Sampled,
            crate::runtime::process_usage::SampleError,
        > {
            Darwin.usage(process, held, start, at)
        }

        fn group(
            &self,
            leader: &Birth,
        ) -> io::Result<Vec<crate::runtime::job_groups::ProcessRecord>> {
            Darwin.group(leader)
        }

        fn signal(&self, process: ProcessIdentity, signal: libc::c_int) -> io::Result<bool> {
            Darwin.signal(process, signal)
        }
    }

    /// Run `script` as the observed job's only life, let the kernel refuse as `refuse` says once
    /// the life runs, release the script, and answer the first failure the event stream reports,
    /// the status read that follows it, and the life's pid. The script starts by reading a line
    /// and ends when its stdin does.
    async fn status_after_the_first_failure(
        script: &str,
        refuse: impl FnOnce(&KernelRefuses),
    ) -> (CowshedError, Result<JobProcessTree>, u32) {
        use cowshed_os::fork_lock::Spawn as _;
        use std::io::Write as _;

        let kernel = KernelRefuses::default();
        let monitor =
            ProcessMonitor::prepare_with(TreeOwner::Job(JobId::new(1).unwrap()), kernel.clone())
                .unwrap();
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", script])
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .expect("spawn the observed life");
        let pid = child.id();
        monitor.watch(pid).unwrap();
        let mut events = monitor.subscribe(Duration::from_millis(20)).await.unwrap();

        refuse(&kernel);
        let mut input = child.stdin.take().expect("the child's stdin is piped");
        input.write_all(b"go\n").unwrap();
        let failure = loop {
            match events.next().await {
                Some(Err(error)) => break error,
                Some(Ok(_)) => {}
                None => panic!("the events ended before the failed read was reported"),
            }
        };
        let status = monitor.tree();
        drop(input);
        assert!(child.wait_bounded().unwrap().success());
        (failure, status, pid)
    }

    /// An exec whose image the kernel will not read is a coverage gap of the tree, and the failed
    /// read is returned beside it to whoever follows the events (07_api.md, "Process-tree
    /// observations"). It is no reason for a running job's status read to fail: the observer
    /// folded everything it saw and stays usable.
    #[tokio::test]
    async fn an_unreadable_image_is_a_gap_a_running_jobs_status_read_still_returns() {
        let (failure, status, pid) =
            status_after_the_first_failure("read go; exec /bin/cat", |kernel| {
                kernel.images.store(true, Ordering::SeqCst);
            })
            .await;
        assert!(failure.to_string().contains("KERN_PROCARGS2"), "{failure}");

        let tree = status.expect(
            "a running job's status read returns its status when an image could not be read",
        );
        assert_eq!(
            tree.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnreadImage { pid }
            }
        );
    }

    /// A member's fork whose children the kernel will not list is the fork's gap, which the
    /// observer records before it reads the list; the failed read is returned beside it, and a
    /// running job's status read still returns the tree.
    #[tokio::test]
    async fn an_unlistable_child_list_is_a_gap_a_running_jobs_status_read_still_returns() {
        let (failure, status, pid) =
            status_after_the_first_failure("read go; /usr/bin/true; read rest; exit 0", |kernel| {
                kernel.children.store(true, Ordering::SeqCst);
            })
            .await;
        assert!(
            failure.to_string().contains("proc_listchildpids"),
            "{failure}"
        );

        let tree = status.expect(
            "a running job's status read returns its status when a child list could not be read",
        );
        assert_eq!(
            tree.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UncountedFork { pid }
            }
        );
    }
}
