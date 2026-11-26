//! macOS's read of a job's members into its census: each member's record through its immutable
//! life identity (`p_uniqueid`, surviving an exec of the same life), its image from
//! `KERN_PROCARGS2` fenced by its pid version across the read (so an exec mid-read leaves the
//! image unread), and its own counters from `proc_pid_rusage`.

use std::io;

use super::*;
use crate::runtime::job_groups::{Process, ProcessRecord};

/// The kernel read an [`ObserveError::Image`] names.
pub(super) const IMAGE_READ: &str = "KERN_PROCARGS2";

/// What one member's image read and its surrounding life checks prove for the census.
#[derive(Debug)]
enum ImageDecision {
    Found(Option<ProcessImage>),
    Unread,
    Failed(io::Error),
}

/// Decide what `image` proves for a member read at `before_version`, given `after` (the same
/// life's record and pid version read right after `image`, or `None` once the life ended) and
/// the observer's effective `reader` uid.
///
/// The image read names the pid alone, so `Ok(image)` names this life's current image only when
/// the life still ran at `before_version` afterwards. A changed pid version means the life
/// exec'd across the read (so the image may be stale, foreign or mid-exec: unread, never failed);
/// `None` means the life ended around the read. Under the same version, `EINVAL` for another
/// user's process is the kernel's credential refusal (unread), and any other error fails the
/// read while still marking the image unread.
fn decide_image(
    before_version: u32,
    image: io::Result<ProcessImage>,
    after: Option<(ProcessRecord, u32)>,
    reader: u32,
) -> ImageDecision {
    match (image, after) {
        // The life ended around the read: the image may be a stranger's, or none.
        (Ok(_) | Err(_), None) => ImageDecision::Found(None),
        // The same life exec'd across the read: the image may be stale, foreign or mid-exec.
        (Ok(_) | Err(_), Some((_, after_version))) if after_version != before_version => {
            ImageDecision::Unread
        }
        (Ok(image), Some(_)) => ImageDecision::Found(Some(image)),
        // `EINVAL` is also the kernel's refusal of another user's arguments, which its own
        // credential rule says: an image the census lacks, not a failure.
        (Err(source), Some((record, _)))
            if source.raw_os_error() == Some(libc::EINVAL)
                && reader != 0
                && reader != record.uid =>
        {
            ImageDecision::Unread
        }
        // The census lacks this image whatever later reads find: the gap stays, beside the
        // failure this read returns.
        (Err(source), Some(_)) => ImageDecision::Failed(source),
    }
}

/// Read each member of the job's group into `census` at `at`: its identity, the image it runs
/// and its own counters, each fenced to the life the membership read found. A member whose pid
/// no longer names that life -- it exited, or the pid was given to another process -- is no
/// member; an exec of the same life keeps its identity (`p_uniqueid`) and stays one life, while
/// an exec across the image read leaves that image unread; a read failure of a member still
/// running at the same version is an error.
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
        // Read through the life identity the membership read found, never the pid alone.
        let Some((before, before_version)) = member.life_record().map_err(read)? else {
            continue;
        };
        let process = ProcessIdentity {
            pid,
            birth: BirthToken(before.unique_id),
        };
        // The image names the pid alone: it is this life's current image only if this same life
        // still holds the pid at the same pid version (no exec) after the read.
        let image = Darwin.image(pid);
        let after = member.life_record().map_err(read)?;
        match decide_image(before_version, image, after, reader) {
            ImageDecision::Found(image) => census.found(process, image),
            ImageDecision::Unread => census.unread_image(process),
            ImageDecision::Failed(source) => {
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

#[cfg(test)]
mod tests {
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::os::unix::process::CommandExt as _;
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;

    use super::*;
    use crate::api::dto::CommandArg;
    use crate::runtime::job_groups::{Birth, job_members};

    /// A test's process group, killed and its leader reaped however the test ends.
    struct OwnedGroup(Child);

    impl Drop for OwnedGroup {
        fn drop(&mut self) {
            let pgid = i32::try_from(self.0.id()).expect("owned child pid");
            // SAFETY: this unreaped child is the group leader spawned by this test.
            unsafe { libc::killpg(pgid, libc::SIGKILL) };
            let _ = self.0.wait_bounded();
        }
    }

    /// A member that execs after `job_members` read its pre-exec pid version keeps its
    /// `p_uniqueid` life identity: observing through that pre-exec member list still finds the
    /// same life (never dropping it so `Census::close` marks it exited and a later fresh read
    /// refuses `AfterExit`).
    #[test]
    fn a_member_that_execs_after_the_membership_read_stays_one_live_life() {
        let child = Command::new("/bin/bash")
            .args([
                "-c",
                "echo READY; read _; exec /bin/bash -c 'echo EXECED; read _'",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a job group leader");
        let mut group = OwnedGroup(child);
        let pid = group.0.id();
        let mut stdout = BufReader::new(group.0.stdout.take().expect("leader stdout"));
        let mut line = String::new();
        stdout.read_line(&mut line).expect("leader ready line");
        assert_eq!(line.trim(), "READY");

        let birth = Birth::of(pid);
        let pre_exec = job_members(&birth).expect("pre-exec membership");
        assert_eq!(pre_exec.len(), 1, "only the leader runs in the group");
        assert_eq!(pre_exec[0].pid().cast_unsigned(), pid);

        let origin = Instant::now();
        let mut census = Census::led_by(pid, origin);
        observe(&mut census, &pre_exec, origin).expect("observe before exec");
        assert_eq!(census.lives.len(), 1, "one life before exec");
        assert!(!census.lives[0].exited, "live before exec");
        let pre_exec_identity = census.lives[0].identity;

        writeln!(group.0.stdin.as_mut().expect("leader stdin"), "go").expect("trigger exec");
        line.clear();
        stdout.read_line(&mut line).expect("post-exec ready line");
        assert_eq!(line.trim(), "EXECED");
        assert_eq!(
            pre_exec[0].record().expect("stale version check"),
            None,
            "exec changed the pid version, so Process::record rejects the pre-exec handle"
        );

        observe(&mut census, &pre_exec, origin + Duration::from_secs(1))
            .expect("first observe with pre-exec membership after exec");
        observe(&mut census, &pre_exec, origin + Duration::from_secs(2))
            .expect("second observe with pre-exec membership after exec");
        let post_exec = job_members(&birth).expect("post-exec membership");
        observe(&mut census, &post_exec, origin + Duration::from_secs(3))
            .expect("observe with fresh membership after exec");

        assert_eq!(census.lives.len(), 1, "exec kept one life in the census");
        assert_eq!(census.lives[0].identity, pre_exec_identity);
        assert!(!census.lives[0].exited, "the exec'd life stayed live");
        let image = census.lives[0].image.as_ref().expect("post-exec image");
        assert_eq!(&*image.program, "/bin/bash");
        assert!(
            image
                .argv
                .iter()
                .any(|arg| arg.as_os_str().to_string_lossy().contains("EXECED")),
            "census refreshed to the post-exec argv: {:?}",
            image.argv
        );
        let total = CpuTotals {
            user_us: CpuMicros::new(1).expect("exact"),
            sys_us: CpuMicros::new(0).expect("exact"),
        };
        let coverage = LeafCoverage::new(900).expect("share");
        let answer = census.attribute(total, coverage).expect("attribute");
        assert_eq!(
            answer.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UncountedFork { pid },
            },
            "no spurious UnreadFinalUsage or UnreadImage gap"
        );
    }

    /// Deciding what one image read proves from the surrounding life checks: the image is kept
    /// only when the same life still runs at the same pid version afterwards; a version change
    /// (an exec across the read) leaves the image unread whether the image read succeeded or
    /// errored, never keeping a stale image or failing the sample; absence after the read means
    /// the life ended; under the same version, `EINVAL` for another user's process is unread and
    /// any other error fails.
    #[test]
    fn an_exec_across_the_image_read_leaves_the_image_unread() {
        let record = |uid| ProcessRecord {
            pid: 42,
            ppid: 1,
            unique_id: 900,
            parent_unique_id: 1,
            uid,
            started_seconds: 10,
        };
        let image = |program: &str| ProcessImage {
            program: program.into(),
            argv: vec![CommandArg::new(program)].into(),
            truncated: false,
        };
        let errno = io::Error::from_raw_os_error;

        #[derive(Debug, Eq, PartialEq)]
        enum Expected {
            Found(Option<String>),
            Unread,
            Failed(Option<libc::c_int>),
        }

        type Case = (
            &'static str,
            u32,
            io::Result<ProcessImage>,
            Option<(ProcessRecord, u32)>,
            u32,
            Expected,
        );
        let cases: [Case; 8] = [
            (
                "same life and version keeps the image",
                7,
                Ok(image("/bin/bash")),
                Some((record(501), 7)),
                501,
                Expected::Found(Some("/bin/bash".into())),
            ),
            (
                "exec across a successful image read leaves the image unread",
                7,
                Ok(image("/bin/sh")),
                Some((record(501), 8)),
                501,
                Expected::Unread,
            ),
            (
                "exec across a failed image read (mid-exec EIO) leaves the image unread",
                7,
                Err(errno(libc::EIO)),
                Some((record(501), 8)),
                501,
                Expected::Unread,
            ),
            (
                "life ended across a successful image read records no image",
                7,
                Ok(image("/bin/bash")),
                None,
                501,
                Expected::Found(None),
            ),
            (
                "life ended across a failed image read records no image",
                7,
                Err(errno(libc::ESRCH)),
                None,
                501,
                Expected::Found(None),
            ),
            (
                "same version EINVAL for another user is credential refusal (unread)",
                7,
                Err(errno(libc::EINVAL)),
                Some((record(0), 7)),
                501,
                Expected::Unread,
            ),
            (
                "same version EINVAL as root reader fails the image read",
                7,
                Err(errno(libc::EINVAL)),
                Some((record(501), 7)),
                0,
                Expected::Failed(Some(libc::EINVAL)),
            ),
            (
                "same version unexpected error fails the image read",
                7,
                Err(errno(libc::EIO)),
                Some((record(501), 7)),
                501,
                Expected::Failed(Some(libc::EIO)),
            ),
        ];

        for (case, before_version, read, after, reader, expected) in cases {
            let actual = match decide_image(before_version, read, after, reader) {
                ImageDecision::Found(img) => {
                    Expected::Found(img.map(|img| img.program.to_string()))
                }
                ImageDecision::Unread => Expected::Unread,
                ImageDecision::Failed(source) => Expected::Failed(source.raw_os_error()),
            };
            assert_eq!(actual, expected, "{case}");
        }
    }
}
