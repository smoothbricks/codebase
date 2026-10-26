//! What a job cgroup's reap-class wait names when it gives up (specs/cowshed/05_gateway.md,
//! "Deadlines"; 07_api.md, "Complete job accounting and observation reconciliation"): the cgroup
//! and each member still alive in it, with its pid, its image and the state the kernel shows it
//! in. A member in an uninterruptible kernel wait, which SIGKILL ends only once that call returns,
//! is named as such. Platform-free, so its naming is tested on every host; the Linux job cgroup
//! fills it.

use std::fmt;
use std::path::PathBuf;

use cowshed_os::deadline::ChildPeer;

/// What a member is to the wait that names it.
pub const MEMBER_ROLE: &str = "job member";

/// One process a job cgroup still held when the wait for its exit gave up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveMember {
    pub peer: ChildPeer,
    /// Its executable as the kernel named it when its membership was read; `None` when unread.
    pub image: Option<String>,
}

impl LiveMember {
    /// The member `pid`, running `image`, in the state the kernel shows it in now.
    pub fn observed(pid: u32, image: Option<String>) -> Self {
        Self {
            peer: ChildPeer::observed(MEMBER_ROLE, pid),
            image,
        }
    }
}

impl fmt::Display for LiveMember {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.image {
            Some(image) => write!(formatter, "{}, image {image}", self.peer),
            None => write!(formatter, "{}, image unread", self.peer),
        }
    }
}

/// A job cgroup whose membership did not end within its reap bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveMembership {
    pub cgroup: PathBuf,
    /// The members alive when the wait gave up, or why they could not be read.
    pub members: Result<Vec<LiveMember>, String>,
}

impl fmt::Display for LiveMembership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cgroup = self.cgroup.display();
        let members = match &self.members {
            Err(why) => {
                return write!(
                    formatter,
                    "job cgroup {cgroup} whose members could not be read ({why})"
                );
            }
            Ok(members) if members.is_empty() => {
                return write!(
                    formatter,
                    "job cgroup {cgroup} with no live member left when it was read"
                );
            }
            Ok(members) => members,
        };
        let plural = if members.len() == 1 { "" } else { "s" };
        write!(
            formatter,
            "job cgroup {cgroup} with {} live member{plural}: ",
            members.len()
        )?;
        for (index, member) in members.iter().enumerate() {
            if index != 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{member}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use cowshed_os::deadline::{ChildPeer, Expired, LatencyClass, Progress};
    use cowshed_os::process::RunState;

    use super::{LiveMember, LiveMembership, MEMBER_ROLE};

    const JOB: &str = "/sys/fs/cgroup/svc/ws-1/job-7";

    fn member(pid: u32, state: RunState, image: Option<&str>) -> LiveMember {
        LiveMember {
            peer: ChildPeer {
                role: MEMBER_ROLE,
                pid,
                state,
            },
            image: image.map(str::to_owned),
        }
    }

    fn expired(members: Result<Vec<LiveMember>, String>) -> String {
        Expired {
            op: "job cgroup kill",
            peer: LiveMembership {
                cgroup: PathBuf::from(JOB),
                members,
            },
            class: LatencyClass::Reap,
            bound: Duration::from_secs(5),
            elapsed: Duration::from_millis(5_002),
            last: Progress::Signalled {
                signal: libc::SIGKILL,
            },
        }
        .to_string()
    }

    #[test]
    fn an_unreaped_job_names_each_live_member_and_its_kernel_state() {
        let message = expired(Ok(vec![
            member(41, RunState::Uninterruptible, Some("/usr/bin/fsck")),
            member(42, RunState::Sleeping, None),
        ]));
        assert_eq!(
            message,
            format!(
                "TIMEOUT: job cgroup kill on job cgroup {JOB} with 2 live members: \
                 job member pid 41 (in an uninterruptible kernel wait), image /usr/bin/fsck; \
                 job member pid 42 (sleeping), image unread: no answer within its reap \
                 bound of 5s; waited 5.002s, last progress: signalled 9"
            )
        );
    }

    #[test]
    fn an_unreaped_job_says_when_no_member_was_left_or_none_could_be_read() {
        assert!(
            expired(Ok(Vec::new())).contains(&format!(
                "on job cgroup {JOB} with no live member left when it was read:"
            )),
            "an expiry whose members exited before the census says so"
        );
        assert!(
            expired(Err("read cgroup.procs: permission denied".to_owned())).contains(&format!(
                "on job cgroup {JOB} whose members could not be read (read cgroup.procs: \
                 permission denied):"
            )),
            "a failed census is named, never an empty member list"
        );
    }
}
