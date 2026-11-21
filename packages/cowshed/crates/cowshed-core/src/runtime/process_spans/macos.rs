//! The signals XNU delivers beyond the ones every host shares.

use super::ExitSignal;

/// The highest signal number this host delivers.
#[cfg(test)]
pub(super) const HIGHEST_SIGNAL: i32 = 31;

/// The host's signal `signal` where only XNU has it, or `None`.
pub(super) fn host_signal(signal: i32) -> Option<ExitSignal> {
    match signal {
        libc::SIGEMT => Some(ExitSignal::Emt),
        libc::SIGINFO => Some(ExitSignal::Info),
        _ => None,
    }
}
