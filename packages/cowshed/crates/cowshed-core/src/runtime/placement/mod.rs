//! A job's class as the kernel schedules it (05_gateway.md "Host scheduling", "Classes"): one nice
//! per class, the same table on both platforms. Cowshed never decides whether runnable work runs;
//! the kernel weighs every runnable thread by its class's nice, and a sleeping thread weighs
//! nothing, whatever it holds.
//!
//! The platforms place the table differently, selected here and nowhere else:
//!
//! - macOS (`macos.rs`) has no group weight: the nice is placed on the job's held first process,
//!   below the supervisor's own, and read back before it runs anything of the job's.
//! - Linux (`linux.rs`) places it on the job's class cgroup as `cpu.weight.nice`: every job of
//!   a class shares one weight against the other classes, however many threads its jobs run.

use crate::api::dto::ExecClass;
use std::time::Duration;

#[cfg(target_os = "linux")]
pub(crate) mod linux;
#[cfg(target_os = "macos")]
pub mod macos;

/// The nice a job of `class` runs at: interactive work and an interactive resident 0, since a
/// person or model waits on them; a gate 5; background and a resident 10. On macOS it is an
/// offset below the supervisor's own nice; on Linux the nice of the job's class cgroup.
pub const fn class_nice(class: ExecClass) -> i32 {
    match class {
        ExecClass::Interactive | ExecClass::InteractiveResident => 0,
        ExecClass::Gate => 5,
        ExecClass::Background | ExecClass::Resident => 10,
    }
}

/// How long interactive work keeps its own nice: a person or model waits on a check this short;
/// past it the job runs at the gate's nice (05_gateway.md "Classes").
pub const INTERACTIVE_TO_GATE: Duration = Duration::from_secs(120);
/// When interactive work runs at background's nice: work this long is no edit's quick feedback.
pub const INTERACTIVE_TO_BACKGROUND: Duration = Duration::from_secs(240);
/// A step of interactive work's decay (05_gateway.md "Classes"): when after the job's start it
/// comes, and the class whose nice the job's group runs at from then. mbx decays the job's asks in
/// its pool from the start the job names (`MBX_SCHEDULER_JOB_STARTED_MS`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecayStep {
    ToGate,
    ToBackground,
}

impl DecayStep {
    /// Every step, in order.
    pub const ALL: [Self; 2] = [Self::ToGate, Self::ToBackground];

    /// How long after the job's start the step comes.
    pub const fn after(self) -> Duration {
        match self {
            Self::ToGate => INTERACTIVE_TO_GATE,
            Self::ToBackground => INTERACTIVE_TO_BACKGROUND,
        }
    }

    /// The class whose nice the job's group runs at from this step.
    pub const fn class(self) -> ExecClass {
        match self {
            Self::ToGate => ExecClass::Gate,
            Self::ToBackground => ExecClass::Background,
        }
    }
}

/// Whether a job of `class` decays: interactive work alone. An interactive resident serves many
/// short requests over a long life, and its age says nothing of any one request's.
pub const fn decays(class: ExecClass) -> bool {
    matches!(class, ExecClass::Interactive)
}
