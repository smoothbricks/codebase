//! Linux's read of a job's members into its census: each member's record under the pidfd the
//! membership read opened (`job_groups/linux.rs`), its image from `/proc/<pid>/exe` and
//! `/proc/<pid>/cmdline` (`cowshed_os::process::process_arguments`) and its own counters from
//! `/proc/<pid>/stat` and `/proc/<pid>/io` (`process_usage::sample`).
//!
//! Every read names the pid alone, and the member's pidfd fences each: the image counts only if
//! the pidfd still shows the process running afterwards, and the counters are read under it.
//!
//! The kernel gives a process's executable only to a reader that may inspect it
//! (`PTRACE_MODE_READ`): another user's process, or one that made itself undumpable, answers
//! `EACCES`. That is an image the census lacks, not a failure. A process that has released its
//! memory and not yet reported its exit has no executable left to name (`ENOENT`): its life is
//! ending around the read.

use std::io;
use std::time::Instant;

use super::{Census, ObserveError};
use crate::runtime::job_groups::Process;
use crate::runtime::process_tree::{BirthToken, ProcessIdentity, ProcessImage};
use crate::runtime::process_usage::{Sampled, UsageObservation, sample};

/// The kernel read an [`ObserveError::Image`] names.
pub(super) const IMAGE_READ: &str = "reading /proc/<pid>/exe and /proc/<pid>/cmdline";

/// The executable and byte-exact argv of the image `pid` runs now.
fn image(pid: libc::pid_t) -> io::Result<ProcessImage> {
    let arguments = cowshed_os::process::process_arguments(pid)?;
    Ok(ProcessImage::observed(
        String::from_utf8_lossy(&arguments.executable).into_owned(),
        arguments.argv,
    ))
}

/// Read each member of the job's group into `census` at `at`: its identity, the image it runs
/// and its own counters, each fenced to the life the membership read found. A member whose
/// pidfd shows it exited is no member; a read failure of a member still running is an error.
pub fn observe(
    census: &mut Census,
    members: &[Process],
    at: Instant,
) -> std::result::Result<(), ObserveError> {
    for member in members {
        let pid = member.pid().cast_unsigned();
        let read = |source| ObserveError::Read { pid, source };
        // Read under the pidfd the membership read opened, never the pid alone.
        let Some(record) = member.record().map_err(read)? else {
            continue;
        };
        let process = ProcessIdentity {
            pid,
            birth: BirthToken(record.unique_id),
        };
        // The image names the pid alone: it is this life's only if the member still runs after
        // the read.
        let image = image(member.pid());
        let running = member.running().map_err(read)?;
        match (image, running) {
            (Ok(image), true) => census.found(process, Some(image)),
            // The life ended around the read: the image may be a stranger's, or none.
            (Ok(_) | Err(_), false) => census.found(process, None),
            // Still running by its pidfd, with no executable left to name: it is exiting.
            (Err(source), true) if source.kind() == io::ErrorKind::NotFound => {
                census.found(process, None);
            }
            // The kernel's own credential rule withholds this image: one the census lacks, not
            // a failure.
            (Err(source), true) if source.kind() == io::ErrorKind::PermissionDenied => {
                census.unread_image(process);
            }
            // The census lacks this image whatever later reads find: the gap stays, beside the
            // failure this read returns.
            (Err(source), true) => {
                census.unread_image(process);
                return Err(ObserveError::Image { pid, source });
            }
        }
        // The counters name the pid alone too; the read fences itself by the member's pidfd,
        // which is the whole fence, so no earlier start is carried to it.
        if let Sampled::Read(reading) = sample(process, Some(member.pidfd()), None, at)? {
            census
                .usage()
                .apply(UsageObservation::Read { process, reading })?;
        }
    }
    census.close()?;
    Ok(())
}
