//! A loaded agent as `systemctl --user show` reports it: `MainPID`, the unit's main process (the
//! gateway host), and `TimeoutStopUSec`, how long after a stop's SIGTERM the manager SIGKILLs
//! it, of the definition the manager loaded.

use std::fmt;
use std::time::Duration;

use super::LoadedAgent;

/// Why `systemctl show`'s answer for a loaded unit could not be read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrintError {
    /// No `TimeoutStopUSec` property: the bound the manager kills the process at is unknown, so
    /// no wait for its exit can be bounded by it.
    NoExitTimeout,
    /// `TimeoutStopUSec=infinity`: the manager never kills the process, so its exit has no
    /// bound to wait within.
    NoBound,
    /// A `MainPID` or `TimeoutStopUSec` property whose value cannot be read.
    Unreadable { line: String },
}

impl fmt::Display for PrintError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoExitTimeout => {
                formatter.write_str("systemctl show reports no `TimeoutStopUSec` for the unit")
            }
            Self::NoBound => formatter.write_str(
                "systemctl show reports `TimeoutStopUSec=infinity`, so the manager never ends \
                 the unit's process and its stop has no bound",
            ),
            Self::Unreadable { line } => {
                write!(
                    formatter,
                    "systemctl show reports an unreadable `{}`",
                    line.trim()
                )
            }
        }
    }
}

/// Read a loaded unit from the stdout of `systemctl --user show <unit> --property=…`: one
/// `Name=value` line per property. `MainPID=0` is a unit with no process, between two runs or
/// stopped; a unit that states no `MainPID` has none either.
pub fn parse_print(stdout: &str) -> Result<LoadedAgent, PrintError> {
    let mut pid = None;
    let mut exit_timeout = None;
    for line in stdout.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let unreadable = || PrintError::Unreadable {
            line: line.to_owned(),
        };
        match key {
            "MainPID" => {
                pid = Some(value.parse::<u32>().map_err(|_| unreadable())?).filter(|pid| *pid != 0);
            }
            "TimeoutStopUSec" => {
                exit_timeout = Some(match timespan(value).ok_or_else(unreadable)? {
                    Timespan::Finite(timeout) => timeout,
                    Timespan::Infinity => return Err(PrintError::NoBound),
                });
            }
            _ => {}
        }
    }
    Ok(LoadedAgent {
        pid,
        exit_timeout: exit_timeout.ok_or(PrintError::NoExitTimeout)?,
    })
}

/// A time span as `show` prints one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Timespan {
    Finite(Duration),
    Infinity,
}

/// The units systemd's `format_timespan` writes, in microseconds (systemd.time(7): a month is
/// 30.44 days and a year 365.25).
const UNITS: [(&str, u64); 9] = [
    ("y", 31_557_600_000_000),
    ("month", 2_629_800_000_000),
    ("w", 604_800_000_000),
    ("d", 86_400_000_000),
    ("h", 3_600_000_000),
    ("min", 60_000_000),
    ("ms", 1_000),
    ("us", 1),
    ("s", 1_000_000),
];

/// Read a time span as `show` prints a `…USec` property: `infinity`, a bare `0`, or one or more
/// `<number><unit>` terms separated by spaces, the last one possibly with a decimal fraction
/// (`30s`, `1min 30s`, `250ms`, `1.500000s`, `2min 5.500000s`). `None` for anything else, and
/// for a span no `Duration` of microseconds holds.
fn timespan(text: &str) -> Option<Timespan> {
    match text {
        "infinity" => return Some(Timespan::Infinity),
        "0" => return Some(Timespan::Finite(Duration::ZERO)),
        _ => {}
    }
    let mut micros: u64 = 0;
    let mut terms = 0_usize;
    for term in text.split(' ') {
        micros = micros.checked_add(term_micros(term)?)?;
        terms += 1;
    }
    (terms > 0).then(|| Timespan::Finite(Duration::from_micros(micros)))
}

/// One `<whole>[.<fraction>]<unit>` term in microseconds, rounded down.
fn term_micros(term: &str) -> Option<u64> {
    let digits = term.len() - term.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    let (whole, rest) = term.split_at(digits);
    let (fraction, unit) = match rest.strip_prefix('.') {
        Some(rest) => {
            let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
            let (fraction, unit) = rest.split_at(digits);
            if fraction.is_empty() {
                return None;
            }
            (fraction, unit)
        }
        None => ("", rest),
    };
    let (_, unit_micros) = UNITS.iter().find(|(name, _)| *name == unit)?;
    let whole = whole.parse::<u64>().ok()?.checked_mul(*unit_micros)?;
    let fraction = match fraction {
        "" => 0,
        fraction => {
            let scale = 10_u128.checked_pow(u32::try_from(fraction.len()).ok()?)?;
            let part = u128::from(fraction.parse::<u64>().ok()?) * u128::from(*unit_micros) / scale;
            u64::try_from(part).ok()?
        }
    };
    whole.checked_add(fraction)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seconds(seconds: u64) -> Option<Timespan> {
        Some(Timespan::Finite(Duration::from_secs(seconds)))
    }

    /// Every span here but the last block is what systemd 260 printed for `TimeoutStopUSec` of
    /// a probe unit that stated the `TimeoutStopSec=` beside it.
    #[test]
    fn a_timespan_reads_as_show_prints_it() {
        for (printed, read) in [
            // TimeoutStopSec=30, 90, 3600, 86400, 1y
            ("30s", seconds(30)),
            ("1min 30s", seconds(90)),
            ("1h", seconds(3_600)),
            ("1d", seconds(86_400)),
            ("1y", seconds(31_557_600)),
            // TimeoutStopSec=0.25, 1500ms, 2min 5.5s, 100us
            ("250ms", Some(Timespan::Finite(Duration::from_millis(250)))),
            (
                "1.500000s",
                Some(Timespan::Finite(Duration::from_millis(1_500))),
            ),
            (
                "2min 5.500000s",
                Some(Timespan::Finite(Duration::from_millis(125_500))),
            ),
            ("100us", Some(Timespan::Finite(Duration::from_micros(100)))),
            // TimeoutStopSec=infinity
            ("infinity", Some(Timespan::Infinity)),
            // The other units `format_timespan` writes, and a zero span.
            ("1month 2w", seconds(2_629_800 + 1_209_600)),
            ("0", Some(Timespan::Finite(Duration::ZERO))),
            ("0s", seconds(0)),
            // Nothing `show` prints.
            ("", None),
            ("30", None),
            ("s", None),
            ("1.s", None),
            ("-1s", None),
            ("1 min", None),
            ("1min  30s", None),
            ("3fortnights", None),
            ("99999999999999y", None),
            ("18446744073709551615s", None),
        ] {
            assert_eq!(timespan(printed), read, "{printed:?}");
        }
    }

    fn agent(pid: Option<u32>, seconds: u64) -> Result<LoadedAgent, PrintError> {
        Ok(LoadedAgent {
            pid,
            exit_timeout: Duration::from_secs(seconds),
        })
    }

    fn unreadable(line: &str) -> Result<LoadedAgent, PrintError> {
        Err(PrintError::Unreadable {
            line: line.to_owned(),
        })
    }

    /// `systemctl --user show <unit> --property=LoadState,ActiveState,SubState,UnitFileState,
    /// MainPID,TimeoutStopUSec` as systemd 260 printed it for a probe unit in each state, and
    /// answers that cannot be read.
    #[test]
    fn a_loaded_unit_reads_its_main_process_and_stop_timeout() {
        for (state, show, read) in [
            (
                "running",
                "LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\nTimeoutStopUSec=3s\nMainPID=31807\n",
                agent(Some(31_807), 3),
            ),
            (
                "between two runs",
                "LoadState=loaded\nActiveState=activating\nSubState=auto-restart\nUnitFileState=enabled\nTimeoutStopUSec=3s\nMainPID=0\n",
                agent(None, 3),
            ),
            (
                "stopping",
                "LoadState=loaded\nActiveState=deactivating\nSubState=stop-sigterm\nUnitFileState=disabled\nTimeoutStopUSec=3s\nMainPID=31815\n",
                agent(Some(31_815), 3),
            ),
            (
                "enabled and stopped, the default stop timeout",
                "LoadState=loaded\nActiveState=failed\nSubState=failed\nUnitFileState=enabled\nTimeoutStopUSec=1min 30s\nMainPID=0\n",
                agent(None, 90),
            ),
            (
                "no main process stated",
                "LoadState=loaded\nActiveState=active\nTimeoutStopUSec=30s\n",
                agent(None, 30),
            ),
            (
                "a stop the manager never ends",
                "LoadState=loaded\nActiveState=inactive\nSubState=dead\nUnitFileState=disabled\nTimeoutStopUSec=infinity\nMainPID=0\n",
                Err(PrintError::NoBound),
            ),
            (
                "no stop timeout stated",
                "LoadState=loaded\nActiveState=active\nMainPID=7\n",
                Err(PrintError::NoExitTimeout),
            ),
            ("nothing", "", Err(PrintError::NoExitTimeout)),
            (
                "a main process that is no pid",
                "TimeoutStopUSec=3s\nMainPID=-1\n",
                unreadable("MainPID=-1"),
            ),
            (
                "a stop timeout that is no span",
                "MainPID=7\nTimeoutStopUSec=soon\n",
                unreadable("TimeoutStopUSec=soon"),
            ),
        ] {
            assert_eq!(parse_print(show), read, "{state}");
        }
    }

    /// The errors name the tool and the property, as the operator would look them up.
    #[test]
    fn a_print_error_names_what_show_reported() {
        for (error, said) in [
            (
                PrintError::NoExitTimeout,
                "systemctl show reports no `TimeoutStopUSec` for the unit",
            ),
            (
                PrintError::Unreadable {
                    line: "MainPID=x\n".to_owned(),
                },
                "systemctl show reports an unreadable `MainPID=x`",
            ),
        ] {
            assert_eq!(error.to_string(), said);
        }
        assert!(PrintError::NoBound.to_string().contains("infinity"));
    }
}
