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
