//! The writer's wake on Linux: the eventfd in the observer's epoll set. A count written to it
//! makes the set readable, so the writer's wait returns; the observer's drain takes the count
//! and passes over it: it is no process event.

use std::io;
use std::os::fd::{AsRawFd as _, OwnedFd};

use super::super::process_events::{KqueueObserver, ProcessReads};
use crate::error::{CowshedError, Result};

/// What a caller wakes the writer by: a duplicate of the observer's wake eventfd.
pub(super) struct Wake(OwnedFd);

impl Wake {
    /// Keep a descriptor of `observer`'s wake eventfd, which its event source already waits on.
    pub(super) fn prepare<P: ProcessReads>(observer: &KqueueObserver<P>) -> Result<Self> {
        observer.wake_handle().map(Self).map_err(|error| {
            CowshedError::internal(format!(
                "cannot retain the process observer's wake descriptor: {error}"
            ))
        })
    }

    /// Wake the writer from its wait on the set.
    pub(super) fn raise(&self) -> io::Result<()> {
        let count = 1_u64.to_ne_bytes();
        loop {
            // SAFETY: `count` is readable for the eight bytes an eventfd write takes.
            if unsafe { libc::write(self.0.as_raw_fd(), count.as_ptr().cast(), count.len()) } >= 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => {}
                // The count is at its most: a wake the writer has not taken yet is waiting.
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::Write as _;
    use std::sync::atomic::AtomicBool;

    use super::super::*;
    use crate::api::dto::JobId;
    use crate::api::process::ProcessCoverage;

    /// The kernel's reads, except that an image read is refused once the flag is set, as `exe`
    /// is to a reader the kernel's ptrace read check refuses.
    #[derive(Clone, Default)]
    struct ImagesRefused(Arc<AtomicBool>);

    impl ProcessReads for ImagesRefused {
        fn record(
            &self,
            pid: u32,
        ) -> io::Result<Option<crate::runtime::job_groups::ProcessRecord>> {
            Darwin.record(pid)
        }

        fn image(&self, pid: u32) -> io::Result<crate::runtime::process_tree::ProcessImage> {
            if self.0.load(Ordering::SeqCst) {
                Err(io::Error::from_raw_os_error(libc::EACCES))
            } else {
                Darwin.image(pid)
            }
        }

        fn children(&self, pid: u32) -> io::Result<Vec<u32>> {
            Darwin.children(pid)
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

    /// An exec is found at the writer's tick, and one whose image the kernel will not read is a
    /// coverage gap of the tree, the failed read returned beside it to whoever follows the events
    /// (07_api.md, "Process-tree observations"). It is no reason for a running job's status read
    /// to fail: the tick folded everything it saw and the tree stays readable.
    #[tokio::test]
    async fn an_unreadable_image_is_a_gap_a_running_jobs_status_read_still_returns() {
        let kernel = ImagesRefused::default();
        let monitor =
            ProcessMonitor::prepare_with(TreeOwner::Job(JobId::new(1).unwrap()), kernel.clone())
                .unwrap();
        let mut child = std::process::Command::new("sh")
            .args(["-c", "read go; exec cat"])
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .expect("spawn the observed life");
        let pid = child.id();
        monitor.watch(pid).unwrap();
        let mut events = monitor.subscribe(Duration::from_millis(20)).await.unwrap();

        kernel.0.store(true, Ordering::SeqCst);
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
        assert!(failure.to_string().contains("/proc/<pid>/exe"), "{failure}");

        let tree = status.expect(
            "a running job's status read returns its status when an image could not be read",
        );
        // The first gap a tree has here is the one it has from its root's first moment.
        assert_eq!(
            tree.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UncountedFork { pid }
            }
        );
        let unread = tree
            .processes
            .iter()
            .find(|process| process.pid == pid)
            .expect("the observed life");
        assert!(unread.exit.is_none(), "{unread:?}");
    }
}
