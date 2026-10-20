//! `cowshed doctor` names the probe it waits in (06_cli.md, "doctor"): every probe is a
//! `doctor <probe>` lifecycle step, so the step a call is in while an answer never comes is the
//! probe waiting for it, on stderr and to a caller that asked for its steps.

#[cfg(target_os = "macos")]
#[path = "doctor_progress/macos.rs"]
mod macos;
