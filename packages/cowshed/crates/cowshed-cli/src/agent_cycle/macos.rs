//! A loaded agent as `launchctl print` reports it.

use std::fmt;
use std::time::Duration;

use super::LoadedAgent;

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
