//! What a job's membership retirement does after a refusal (11_shell.md, "Shell activation and
//! process reuse"): the decisions the supervisor actor executes, apart from the I/O that feeds
//! them. A refusal keeps the job's claims and reaches every waiter, log followers included, as
//! the typed error; the job's own observer seeing its running lives end re-runs it.

use crate::error::CowshedError;

/// Why a membership retirement runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RetirementRun {
    /// The leader exited, or a cancellation or retirement asked for it.
    Requested,
    /// A refused retirement re-runs because the job's observer holds no running life any more.
    EmptyMembership,
}

/// A refused retirement: the typed error every waiter is answered with, and the run it ended.
#[derive(Clone, Debug)]
pub(super) struct Refusal {
    error: CowshedError,
    run: RetirementRun,
}

impl Refusal {
    pub(super) fn new(error: CowshedError, run: RetirementRun) -> Self {
        Self { error, run }
    }

    pub(super) fn error(&self) -> &CowshedError {
        &self.error
    }

    /// Whether the retirement re-runs, given how many lives the job's observer holds running now,
    /// or `None` for a job with no observer. A run refused while lives still ran -- survivors of
    /// SIGKILL still exiting under load, a member mid-exit -- re-runs once none runs: their exits
    /// are the event that concludes the job. A re-run refused with nothing running would refuse
    /// alike again, so it stands until a cancellation or retirement retries it.
    pub(super) fn reruns(&self, running: Option<usize>) -> bool {
        self.run == RetirementRun::Requested && running == Some(0)
    }
}

/// How a log read that has reached a stream's current end is answered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AtEnd {
    /// Answered now with what the stream holds, and whether it ended.
    Answer,
    /// Parked until the stream grows, ends or the job concludes.
    Wait,
    /// Answered with the job's retirement refusal: nothing ends the stream on its own any more.
    Refused,
}

/// The answer to a log read at a stream's current end: only a follower of a job that has not
/// concluded waits, and not once the job's retirement is refused.
pub(super) fn at_end(follow: bool, terminal: bool, refused: bool) -> AtEnd {
    match (follow && !terminal, refused) {
        (false, _) => AtEnd::Answer,
        (true, false) => AtEnd::Wait,
        (true, true) => AtEnd::Refused,
    }
}

#[cfg(test)]
mod tests {
    use super::{AtEnd, Refusal, RetirementRun, at_end};
    use crate::error::CowshedError;

    fn refusal(run: RetirementRun) -> Refusal {
        Refusal::new(
            CowshedError::conflict("job 7's processes [9] still run", "retry the retirement"),
            run,
        )
    }

    #[test]
    fn a_requested_refusal_reruns_once_its_observer_holds_no_running_life() {
        for (run, running, reruns) in [
            (RetirementRun::Requested, Some(0), true),
            (RetirementRun::Requested, Some(1), false),
            (RetirementRun::Requested, Some(3), false),
            (RetirementRun::Requested, None, false),
            (RetirementRun::EmptyMembership, Some(0), false),
            (RetirementRun::EmptyMembership, Some(2), false),
            (RetirementRun::EmptyMembership, None, false),
        ] {
            assert_eq!(
                refusal(run).reruns(running),
                reruns,
                "{run:?} with {running:?} running"
            );
        }
    }

    #[test]
    fn only_a_follower_of_a_job_still_concluding_waits_and_a_refusal_answers_it() {
        for (follow, terminal, refused, answer) in [
            (false, false, false, AtEnd::Answer),
            (false, false, true, AtEnd::Answer),
            (false, true, false, AtEnd::Answer),
            (true, true, false, AtEnd::Answer),
            (true, true, true, AtEnd::Answer),
            (true, false, false, AtEnd::Wait),
            (true, false, true, AtEnd::Refused),
        ] {
            assert_eq!(
                at_end(follow, terminal, refused),
                answer,
                "follow {follow}, terminal {terminal}, refused {refused}"
            );
        }
    }
}
