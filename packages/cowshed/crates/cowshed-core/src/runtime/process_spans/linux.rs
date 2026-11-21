//! The signals Linux delivers beyond the ones every host shares.

use super::ExitSignal;

/// The highest signal number this host delivers.
#[cfg(test)]
pub(super) const HIGHEST_SIGNAL: i32 = 64;

/// Linux's real-time signals `32..=64`, indexed by their offset from 32.
const REAL_TIME: [ExitSignal; 33] = [
    ExitSignal::Rt32,
    ExitSignal::Rt33,
    ExitSignal::Rt34,
    ExitSignal::Rt35,
    ExitSignal::Rt36,
    ExitSignal::Rt37,
    ExitSignal::Rt38,
    ExitSignal::Rt39,
    ExitSignal::Rt40,
    ExitSignal::Rt41,
    ExitSignal::Rt42,
    ExitSignal::Rt43,
    ExitSignal::Rt44,
    ExitSignal::Rt45,
    ExitSignal::Rt46,
    ExitSignal::Rt47,
    ExitSignal::Rt48,
    ExitSignal::Rt49,
    ExitSignal::Rt50,
    ExitSignal::Rt51,
    ExitSignal::Rt52,
    ExitSignal::Rt53,
    ExitSignal::Rt54,
    ExitSignal::Rt55,
    ExitSignal::Rt56,
    ExitSignal::Rt57,
    ExitSignal::Rt58,
    ExitSignal::Rt59,
    ExitSignal::Rt60,
    ExitSignal::Rt61,
    ExitSignal::Rt62,
    ExitSignal::Rt63,
    ExitSignal::Rt64,
];

/// The host's signal `signal` where only Linux has it, or `None`.
pub(super) fn host_signal(signal: i32) -> Option<ExitSignal> {
    match signal {
        libc::SIGSTKFLT => Some(ExitSignal::Stkflt),
        libc::SIGPWR => Some(ExitSignal::Pwr),
        real_time @ 32..=64 => {
            let offset = usize::try_from(real_time - 32).ok()?;
            REAL_TIME.get(offset).copied()
        }
        _ => None,
    }
}
