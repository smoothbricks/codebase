//! A land's gate lives with its lander (05_gateway.md, "A job lives with its requester").
//! `cowshed land` names itself to its gate in [`VARIABLE`], and a foreground `cowshed exec` that
//! inherits the name ends when that process has.

use std::ffi::OsStr;
use std::fmt;
use std::io;

use cowshed_core::runtime::job_groups::start_time;
use cowshed_os::process::ExitWatch;
use tokio::sync::oneshot;

/// Where a land names itself to its gate: `<pid>:<start time>`.
pub(crate) const VARIABLE: &str = "COWSHED_LANDER";

/// One process, told from a later one given its pid by when it started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Lander {
    pid: i32,
    started: u64,
}

impl fmt::Display for Lander {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.pid, self.started)
    }
}

impl Lander {
    /// This process, as a land names itself to its gate.
    pub(crate) fn this() -> io::Result<Self> {
        let pid = i32::try_from(std::process::id()).map_err(io::Error::other)?;
        let started = start_time(pid)?
            .ok_or_else(|| io::Error::other("this process's start time reads as absent"))?;
        Ok(Self { pid, started })
    }

    /// The lander `value` names. Surrounding whitespace is not part of the name.
    pub(crate) fn named(value: &OsStr) -> Result<Self, String> {
        let refused = || format!("{VARIABLE} is {value:?}, not <pid>:<start time>");
        let (pid, started) = value
            .to_str()
            .and_then(|value| value.trim().split_once(':'))
            .ok_or_else(refused)?;
        let pid = pid.trim().parse().ok().filter(|pid| *pid > 0);
        Ok(Self {
            pid: pid.ok_or_else(refused)?,
            started: started.trim().parse().map_err(|_| refused())?,
        })
    }

    /// The lander named in this process's `COWSHED_LANDER`, if present and valid.
    pub(crate) fn inherited() -> Option<Self> {
        let named = std::env::var_os(VARIABLE)?;
        Self::named(&named).ok()
    }

    /// This lander as the typed [`cowshed_core::api::JobLander`] carried on a gate exec request.
    pub(crate) fn as_job_lander(self) -> cowshed_core::api::JobLander {
        cowshed_core::api::JobLander {
            pid: self.pid.unsigned_abs(),
            start: self.started,
        }
    }

    /// Watch this lander's exit from now. The watch is registered before the process holding the
    /// pid is identified, so a lander that ends in between is seen ended; a pid nothing holds,
    /// or that another process holds, is a lander that has ended already.
    pub(crate) fn watch(self) -> io::Result<Watch> {
        let exit = ExitWatch::of(self.pid.unsigned_abs())?;
        let (end, ended) = oneshot::channel();
        match exit {
            Some(exit) if start_time(self.pid)? == Some(self.started) => {
                // One thread parked in the kernel's exit notification until the lander ends; the
                // process ends by `exit`, which does not wait for it.
                tokio::task::spawn_blocking(move || {
                    let _ = end.send(exit.wait());
                });
            }
            Some(_) | None => {
                let _ = end.send(Ok(()));
            }
        }
        Ok(Watch {
            lander: self,
            ended,
        })
    }
}

/// A lander's exit, watched.
pub(crate) struct Watch {
    lander: Lander,
    ended: oneshot::Receiver<io::Result<()>>,
}

impl Watch {
    /// The watch on the lander this process's environment names, when it names one. A name that
    /// cannot be read or watched is said on stderr and ends nothing.
    pub(crate) fn inherited() -> Option<Self> {
        let named = std::env::var_os(VARIABLE)?;
        let watched = Lander::named(&named).and_then(|lander| {
            lander.watch().map_err(|error| {
                format!(
                    "cannot watch the exit of the land pid {}: {error}",
                    lander.pid
                )
            })
        });
        match watched {
            Ok(watch) => Some(watch),
            Err(why) => {
                cowshed_os::eprintln!("cowshed: {why}; this command will not end with its land");
                None
            }
        }
    }

    /// Resolves with the lander's pid once it has ended; never when `watch` is none, or when its
    /// wait fails, which is said on stderr once.
    pub(crate) async fn ended(watch: &mut Option<Self>) -> i32 {
        let Some(watching) = watch else {
            return std::future::pending().await;
        };
        let failed = match (&mut watching.ended).await {
            Ok(Ok(())) => return watching.lander.pid,
            Ok(Err(error)) => error.to_string(),
            Err(_) => "its watch ended unanswered".to_owned(),
        };
        cowshed_os::eprintln!(
            "cowshed: cannot wait for the exit of the land pid {}: {failed}; this command will not end with its land",
            watching.lander.pid
        );
        *watch = None;
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests {
    use super::{Lander, Watch};
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    /// Polls `watch` once: whether its lander has ended.
    async fn ended_now(watch: &mut Option<Watch>) -> bool {
        tokio::select! {
            biased;
            _ = Watch::ended(watch) => true,
            () = std::future::ready(()) => false,
        }
    }

    //#region smoo/cowshed!n/gate-ends-with-its-lander
    /// 05_gateway.md "A job lives with its requester": a land names itself as `<pid>:<start
    /// time>`, the watch of a lander that runs is pending, and it resolves when the lander ends,
    /// with no signal sent to anything. The stand-in lander is a shell held at its stdin.
    #[tokio::test]
    async fn a_watched_lander_s_end_is_seen_when_it_ends_and_not_before() {
        let mut stand_in = Command::new("/bin/sh")
            .args(["-c", "read line"])
            .stdin(Stdio::piped())
            .spawn()
            .expect("a stand-in lander");
        let pid = i32::try_from(stand_in.id()).expect("a pid");
        let started = super::start_time(pid)
            .expect("the stand-in's start time")
            .expect("the stand-in runs");
        let lander =
            Lander::named(format!(" {pid}:{started}\n").as_ref()).expect("a lander's name");
        assert_eq!(lander, Lander { pid, started });
        assert_eq!(lander.to_string(), format!("{pid}:{started}"));
        let mut watch = Some(lander.watch().expect("watch the stand-in"));
        assert!(
            !ended_now(&mut watch).await,
            "a lander that runs has not ended"
        );
        stand_in
            .stdin
            .take()
            .expect("the stand-in's stdin")
            .write_all(b"end\n")
            .expect("release the stand-in");
        assert_eq!(Watch::ended(&mut watch).await, pid);
        stand_in.wait_bounded().expect("reap the stand-in");
    }

    /// A lander that ended before the exec began, its pid free or another process's since, has
    /// ended: the exec does not start work for it.
    #[tokio::test]
    async fn a_lander_gone_before_the_watch_has_ended() {
        let mut stand_in = Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .expect("a stand-in lander");
        let pid = i32::try_from(stand_in.id()).expect("a pid");
        stand_in.wait_bounded().expect("reap the stand-in");
        let mut gone = Some(
            Lander { pid, started: 1 }
                .watch()
                .expect("watch a reaped pid"),
        );
        assert_eq!(Watch::ended(&mut gone).await, pid);
        // This process holds its pid, and did not start at the instant the name gives.
        let other = Lander {
            started: 1,
            ..Lander::this().expect("this process")
        };
        let mut recycled = Some(other.watch().expect("watch a pid another process holds"));
        assert_eq!(Watch::ended(&mut recycled).await, other.pid);
    }

    /// A name that is no `<pid>:<start time>` is refused with the name, never taken for a pid.
    #[test]
    fn a_malformed_name_is_refused_by_name() {
        for name in ["", "12", "x:1", "0:1", "-4:1", "12:soon"] {
            let refused = Lander::named(name.as_ref()).expect_err(name);
            assert!(refused.contains(&format!("{name:?}")), "{refused}");
        }
    }
    //#endregion smoo/cowshed!n/gate-ends-with-its-lander
}
