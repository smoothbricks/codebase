//! The decisions of stopping and reloading a LaunchAgent, and of whether an install may restart
//! the gateway now, pure over what the shell observed (05_gateway.md "Install order and
//! rollback", "An install refuses while a land runs"). `gateway_service` is the shell: it runs
//! `launchctl`, watches the old process's exit in the kernel, and reads the land-turn tickets.
//! Nothing here does I/O or reads a clock.

use std::fmt;
use std::time::Duration;

use cowshed_core::CowshedError;
use cowshed_core::api::dto::{LandTurnName, LandTurnQueue};
use cowshed_core::land_turn::Ticket;

/// A loaded agent as `launchctl print` reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoadedAgent {
    /// The process launchd runs for the agent; `None` between two runs (exited, or throttled
    /// before a `KeepAlive` respawn).
    pub pid: Option<u32>,
    /// How long launchd lets that process exit after a bootout's SIGTERM before it SIGKILLs it.
    /// It is the definition launchd *loaded*: a plist an install just rewrote changes it only at
    /// the bootstrap that follows the bootout.
    pub exit_timeout: Duration,
}

/// Why `launchctl print`'s answer for a loaded agent could not be read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrintError {
    /// No property line `exit timeout = <seconds>`: the bound launchd kills the process at is
    /// unknown, so no wait for its exit can be bounded by it.
    NoExitTimeout,
    /// A `pid` or `exit timeout` property whose value is not a number.
    Unreadable { line: String },
}

impl fmt::Display for PrintError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoExitTimeout => {
                formatter.write_str("launchctl print reports no `exit timeout` for the agent")
            }
            Self::Unreadable { line } => {
                write!(
                    formatter,
                    "launchctl print reports an unreadable `{}`",
                    line.trim()
                )
            }
        }
    }
}

/// Read a loaded agent from the stdout of `launchctl print gui/<uid>/<label>`. Only the service's
/// own properties count, the lines one tab deep: a nested block (`resource coalition = {`, …)
/// carries keys of its own two tabs deep.
pub fn parse_print(stdout: &str) -> Result<LoadedAgent, PrintError> {
    let mut pid = None;
    let mut exit_timeout = None;
    for line in stdout.lines() {
        let Some(property) = line.strip_prefix('\t') else {
            continue;
        };
        if property.starts_with('\t') {
            continue;
        }
        let Some((key, value)) = property.split_once(" = ") else {
            continue;
        };
        let unreadable = || PrintError::Unreadable {
            line: line.to_owned(),
        };
        match key {
            "pid" => pid = Some(value.parse::<u32>().map_err(|_| unreadable())?),
            "exit timeout" => {
                exit_timeout = Some(Duration::from_secs(
                    value.parse::<u64>().map_err(|_| unreadable())?,
                ));
            }
            _ => {}
        }
    }
    Ok(LoadedAgent {
        pid,
        exit_timeout: exit_timeout.ok_or(PrintError::NoExitTimeout)?,
    })
}

/// How long launchd's unload of a job may trail its own SIGKILL at the exit timeout: launchd
/// reaps the killed process and drops the job in one pass (measured 2026-10-22 on a test label:
/// unloaded 0.03 s after the kill).
pub const UNLOAD_GRACE: Duration = Duration::from_secs(1);

/// What still holds a bootout open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Remains {
    /// The agent's process has not exited, and holds every descriptor it had, its listeners
    /// included.
    Process { pid: u32 },
    /// The process is gone, and launchd still reports the job loaded.
    Job,
}

impl fmt::Display for Remains {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Process { pid } => write!(formatter, "its process {pid} has not exited"),
            Self::Job => formatter.write_str("its process exited and launchd still holds the job"),
        }
    }
}

/// Where a bootout stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Bootout {
    Done,
    Waiting,
    /// launchd's own deadline passed with `Remains` still there: its SIGKILL did not take (a
    /// process in an uninterruptible wait), or it never dropped the job.
    Overdue(Remains),
}

/// The latest a bootout of `agent` can finish: launchd SIGKILLs its process at the exit timeout,
/// and its unload trails that kill by at most [`UNLOAD_GRACE`].
pub fn bootout_deadline(agent: LoadedAgent) -> Duration {
    agent.exit_timeout.saturating_add(UNLOAD_GRACE)
}

/// Where a bootout of `agent` stands `elapsed` after it was asked, with `running` the pid of its
/// process while that has not exited (the kernel's word: an exit has closed every descriptor the
/// process held) and `job_loaded` whether launchd still reports the job. Done once both are gone,
/// at any time; overdue only past [`bootout_deadline`]. Never before it: the wait this replaces
/// gave up at 5 s, the very instant launchd's default exit timeout SIGKILLs a draining gateway,
/// and reported a bootout that finished 0.03 s later as stuck.
pub fn bootout_progress(
    agent: LoadedAgent,
    running: Option<u32>,
    job_loaded: bool,
    elapsed: Duration,
) -> Bootout {
    if running.is_none() && !job_loaded {
        return Bootout::Done;
    }
    if elapsed < bootout_deadline(agent) {
        return Bootout::Waiting;
    }
    Bootout::Overdue(match running {
        Some(pid) => Remains::Process { pid },
        None => Remains::Job,
    })
}

/// What launchd holds of an agent that a restore loads again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Held {
    /// launchd does not hold the agent: `kickstart` would answer "Could not find service".
    Absent,
    /// launchd holds the agent and runs it, as `launchctl print` read it: a kickstart of it waits
    /// for that process's exit up to its exit timeout.
    Loaded(LoadedAgent),
    /// A bootout of the agent went overdue and launchd still tears it down.
    TearingDown(Remains),
}

/// The one launchctl step that loads an agent again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reload {
    Bootstrap,
    Kickstart(LoadedAgent),
    /// Nothing can load it until launchd has let go of it: a kickstart of a job in teardown is
    /// launchctl 37, "operation already in progress", and a bootstrap of it collides.
    Blocked(Remains),
}

pub const fn reload(held: Held) -> Reload {
    match held {
        Held::Absent => Reload::Bootstrap,
        Held::Loaded(agent) => Reload::Kickstart(agent),
        Held::TearingDown(remains) => Reload::Blocked(remains),
    }
}

/// Work that a gateway restart would cut off now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Busy {
    /// A land holds its repository's turn: the restart would refuse its next step's connection.
    LandTurn { repo: LandTurnName, holder: Ticket },
}

impl fmt::Display for Busy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LandTurn { repo, holder } => write!(
                formatter,
                "the land of {} holds the {repo} land turn (ticket {}, pid {})",
                holder.workspace, holder.number, holder.pid
            ),
        }
    }
}

/// Everything a restart of the gateway would cut off now: the holder of each repository's land
/// turn (its first live ticket; a queued land behind it runs nothing yet, and its first step
/// waits for the install slot).
pub fn restart_blockers(turns: &[LandTurnQueue]) -> Vec<Busy> {
    turns
        .iter()
        .filter_map(|queue| {
            queue.tickets.first().map(|holder| Busy::LandTurn {
                repo: queue.repo.clone(),
                holder: holder.clone(),
            })
        })
        .collect()
}

/// The install's refusal while anything is busy, `None` when the restart may go ahead. An install
/// never waits for them: it says what runs, and the operator reruns it once that has ended.
pub fn refusal(busy: &[Busy]) -> Option<CowshedError> {
    if busy.is_empty() {
        return None;
    }
    Some(CowshedError::conflict(
        format!(
            "this install would restart the gateway under work it cuts off, so it installed and \
             restarted nothing: {}",
            busy.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "rerun it once they have ended; `cowshed gateway status` lists the land turns",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_gateway_types::{GATEWAY_DRAIN_WITHIN, GATEWAY_EXIT_TIMEOUT};
    use proptest::prelude::*;

    /// A `launchctl print` answer as launchd writes it: the service's properties one tab deep,
    /// and nested blocks whose own `pid`/`exit timeout` keys sit two tabs deep.
    fn print_text(agent: Option<LoadedAgent>, decoy_pid: u32, decoy_timeout: u64) -> String {
        let mut text = String::from("gui/501/dev.cowshed.test = {\n\tactive count = 1\n");
        text.push_str("\tstate = running\n\n\tresource coalition = {\n");
        text.push_str(&format!(
            "\t\tpid = {decoy_pid}\n\t\texit timeout = {decoy_timeout}\n\t\tstate = active\n\t}}\n\n"
        ));
        if let Some(agent) = agent {
            text.push_str(&format!(
                "\tminimum runtime = 10\n\texit timeout = {}\n\truns = 1\n",
                agent.exit_timeout.as_secs()
            ));
            if let Some(pid) = agent.pid {
                text.push_str(&format!("\tpid = {pid}\n"));
            }
        }
        text.push_str("\tlast exit code = (never exited)\n}\n");
        text
    }

    fn agent() -> impl Strategy<Value = LoadedAgent> {
        (proptest::option::of(1..u32::MAX), 0..86_400_u64).prop_map(|(pid, seconds)| LoadedAgent {
            pid,
            exit_timeout: Duration::from_secs(seconds),
        })
    }

    fn remains() -> impl Strategy<Value = Remains> {
        prop_oneof![
            (1..u32::MAX).prop_map(|pid| Remains::Process { pid }),
            Just(Remains::Job)
        ]
    }

    fn ticket() -> impl Strategy<Value = Ticket> {
        (any::<u64>(), 1..u32::MAX, "[a-z]{1,8}").prop_map(|(number, pid, workspace)| Ticket {
            number,
            pid,
            workspace,
            asked_unix_ms: 0,
            command: "land".to_owned(),
            bound: Vec::new(),
        })
    }

    fn queues() -> impl Strategy<Value = Vec<LandTurnQueue>> {
        proptest::collection::vec(
            ("[a-z]{1,8}", proptest::collection::vec(ticket(), 0..4)),
            0..4,
        )
        .prop_map(|queues| {
            queues
                .into_iter()
                .map(|(repo, tickets)| LandTurnQueue {
                    repo: LandTurnName::new(repo).expect("a generated queue name is valid"),
                    tickets,
                    abandoned: Vec::new(),
                })
                .collect()
        })
    }

    proptest! {
        /// The agent's own pid and exit timeout are read, never a nested block's.
        #[test]
        fn print_reads_the_service_properties_only(
            agent in agent(),
            decoy_pid in 1..u32::MAX,
            decoy_timeout in 0..86_400_u64,
        ) {
            prop_assert_eq!(parse_print(&print_text(Some(agent), decoy_pid, decoy_timeout)), Ok(agent));
            prop_assert_eq!(
                parse_print(&print_text(None, decoy_pid, decoy_timeout)),
                Err(PrintError::NoExitTimeout)
            );
        }

        /// A bootout is done exactly when the process has exited and launchd dropped the job, at
        /// any time; until then it waits up to launchd's own kill and the unload's grace, and is
        /// overdue only past both, naming what is left.
        #[test]
        fn a_bootout_is_overdue_only_past_launchd_s_own_kill(
            agent in agent(),
            running in proptest::option::of(1..u32::MAX),
            job_loaded in any::<bool>(),
            elapsed_ms in 0..100_000_000_u64,
        ) {
            let elapsed = Duration::from_millis(elapsed_ms);
            let progress = bootout_progress(agent, running, job_loaded, elapsed);
            if running.is_none() && !job_loaded {
                prop_assert_eq!(progress, Bootout::Done);
            } else if elapsed <= agent.exit_timeout {
                prop_assert_eq!(progress, Bootout::Waiting);
            } else if elapsed < bootout_deadline(agent) {
                prop_assert_eq!(progress, Bootout::Waiting);
            } else {
                prop_assert_eq!(progress, Bootout::Overdue(match running {
                    Some(pid) => Remains::Process { pid },
                    None => Remains::Job,
                }));
            }
            prop_assert!(bootout_deadline(agent) > agent.exit_timeout);
        }

        /// A restore bootstraps an agent launchd does not hold and kickstarts only one it holds and
        /// runs; one still in teardown is never kickstarted (launchctl 37) nor bootstrapped.
        #[test]
        fn a_restore_kickstarts_only_a_loaded_agent(remains in remains(), agent in agent()) {
            prop_assert_eq!(reload(Held::Absent), Reload::Bootstrap);
            prop_assert_eq!(reload(Held::Loaded(agent)), Reload::Kickstart(agent));
            prop_assert_eq!(reload(Held::TearingDown(remains)), Reload::Blocked(remains));
        }

        /// An install refuses exactly while a land holds a turn, naming each turn's holder once; a
        /// queued land is not named.
        #[test]
        fn an_install_refuses_exactly_while_a_land_runs(turns in queues()) {
            let busy = restart_blockers(&turns);
            let holders: Vec<Busy> = turns
                .iter()
                .filter_map(|queue| queue.tickets.first().map(|holder| Busy::LandTurn {
                    repo: queue.repo.clone(),
                    holder: holder.clone(),
                }))
                .collect();
            prop_assert_eq!(&busy, &holders);
            match refusal(&busy) {
                None => prop_assert!(holders.is_empty()),
                Some(error) => {
                    prop_assert_eq!(error.code, cowshed_core::ErrorCode::Conflict);
                    for blocker in &busy {
                        let named = blocker.to_string();
                        prop_assert!(error.message.contains(&named), "{} names {}", error.message, named);
                    }
                }
            }
        }
    }

    /// The drain finishes before launchd's kill, and the install's wait outlasts that kill: the
    /// equal 5 s bounds this replaces raced each other.
    #[test]
    fn the_drain_fits_launchd_s_kill_and_the_wait_outlasts_it() {
        let gateway = LoadedAgent {
            pid: Some(1),
            exit_timeout: GATEWAY_EXIT_TIMEOUT,
        };
        assert!(GATEWAY_DRAIN_WITHIN < GATEWAY_EXIT_TIMEOUT);
        assert!(GATEWAY_EXIT_TIMEOUT < bootout_deadline(gateway));
    }
}
