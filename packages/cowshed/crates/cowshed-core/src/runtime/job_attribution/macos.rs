//! macOS's read of a job's members into its census: each member's record through the pid
//! version the membership read found (`proc_pidinfo`), its image from `KERN_PROCARGS2` and its
//! own counters from `proc_pid_rusage`.

use super::*;
use crate::runtime::job_groups::Process;

/// The kernel read an [`ObserveError::Image`] names.
pub(super) const IMAGE_READ: &str = "KERN_PROCARGS2";

/// Read each member of the job's group into `census` at `at`: its identity, the image it runs
/// and its own counters, each fenced to the life the membership read found. A member whose pid
/// no longer names that life -- it exited, or the pid was given to another process -- is no
/// member; a read failure of a member still running is an error.
pub fn observe(
    census: &mut Census,
    members: &[Process],
    at: std::time::Instant,
) -> std::result::Result<(), ObserveError> {
    use crate::runtime::process_events::{Darwin, ProcessReads as _};
    use crate::runtime::process_tree::BirthToken;
    use crate::runtime::process_usage::sample;

    let reader = Darwin.reader_uid();
    for member in members {
        let pid = member.pid().cast_unsigned();
        let read = |source| ObserveError::Read { pid, source };
        // Read through the identity the membership read found, never the pid alone.
        let Some(before) = member.record().map_err(read)? else {
            continue;
        };
        let process = ProcessIdentity {
            pid,
            birth: BirthToken(before.unique_id),
        };
        // The image names the pid alone: it is this life's only if the member still holds the
        // pid after the read.
        let image = Darwin.image(pid);
        let after = member.record().map_err(read)?;
        match (image, after) {
            (Ok(image), Some(_)) => census.found(process, Some(image)),
            // The life ended around the read: the image may be a stranger's, or none.
            (Ok(_) | Err(_), None) => census.found(process, None),
            // `EINVAL` is also the kernel's refusal of another user's arguments, which its own
            // credential rule says: an image the census lacks, not a failure.
            (Err(source), Some(record))
                if source.raw_os_error() == Some(libc::EINVAL)
                    && reader != 0
                    && reader != record.uid =>
            {
                census.unread_image(process);
            }
            // The census lacks this image whatever later reads find: the gap stays, beside the
            // failure this read returns.
            (Err(source), Some(_)) => {
                census.unread_image(process);
                return Err(ObserveError::Image { pid, source });
            }
        }
        let start = census.usage().start(process);
        // The read is fenced by the life's start, so no held descriptor goes with it.
        if let crate::runtime::process_usage::Sampled::Read(reading) =
            sample(process, None, start, at)?
        {
            census
                .usage()
                .apply(UsageObservation::Read { process, reading })?;
        }
    }
    census.close()?;
    Ok(())
}
