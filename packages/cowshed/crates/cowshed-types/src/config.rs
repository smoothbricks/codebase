//! Host configuration values: the `.cowshed.toml` schema and the stored bounds it
//! carries. Parsing is pure over the text it is given; shells read the file.

use std::time::Duration;
/// How long a shed must show no use of Nx before its supervisor stops the daemon it keeps: main's
/// `.cowshed.toml` `[nx] daemon_idle`, [`NxDaemonIdle::DEFAULT`] without it (04_sandbox.md, "The
/// Nx daemon stays on"). Never zero: a zero bound would stop a daemon at the probe after the one
/// that found it come up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NxDaemonIdle(Duration);

impl NxDaemonIdle {
    /// Long enough that a gate's runs, an edit and the next run share one warm daemon (a cold one
    /// recomputes the project graph and every plugin worker's state), short enough that a shed
    /// whose agent has finished costs its host five minutes of a daemon, not the rest of the day.
    pub const DEFAULT: Self = Self(Duration::from_secs(5 * 60));

    /// The bound `idle`; `None` for zero.
    pub fn new(idle: Duration) -> Option<Self> {
        (!idle.is_zero()).then_some(Self(idle))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}
