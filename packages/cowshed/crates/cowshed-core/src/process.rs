use std::ffi::OsStr;
use std::fmt;
use std::process::{ExitStatus, Output};

/// How a child process terminated, without collapsing signals into a synthetic exit code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStatus {
    Exit(i32),
    Signal(i32),
    Unknown,
}

impl ProcessStatus {
    pub fn succeeded(self) -> bool {
        self == Self::Exit(0)
    }
}

impl Default for ProcessStatus {
    fn default() -> Self {
        Self::Exit(0)
    }
}

impl From<ExitStatus> for ProcessStatus {
    fn from(status: ExitStatus) -> Self {
        if let Some(code) = status.code() {
            return Self::Exit(code);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                return Self::Signal(signal);
            }
        }
        Self::Unknown
    }
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exit(code) => write!(f, "exit status {code}"),
            Self::Signal(signal) => write!(f, "signal {signal}"),
            Self::Unknown => f.write_str("unknown termination status"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommandOutput {
    pub status: ProcessStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    pub fn success(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            status: ProcessStatus::Exit(0),
            stdout: stdout.into(),
            stderr: Vec::new(),
        }
    }

    pub fn failure(status: i32, stderr: impl Into<Vec<u8>>) -> Self {
        Self::failure_with_streams(ProcessStatus::Exit(status), Vec::new(), stderr)
    }

    pub fn failure_with_streams(
        status: ProcessStatus,
        stdout: impl Into<Vec<u8>>,
        stderr: impl Into<Vec<u8>>,
    ) -> Self {
        debug_assert!(!status.succeeded());
        Self {
            status,
            stdout: stdout.into(),
            stderr: stderr.into(),
        }
    }

    pub fn succeeded(&self) -> bool {
        self.status.succeeded()
    }
}

impl From<Output> for CommandOutput {
    fn from(output: Output) -> Self {
        Self {
            status: output.status.into(),
            stdout: output.stdout,
            stderr: output.stderr,
        }
    }
}

/// Write a subprocess failure without decoding arbitrary output bytes or allocating a joined
/// command string. Valid UTF-8 stays readable; invalid bytes are escaped individually, so the
/// diagnostic retains every byte instead of replacing it with U+FFFD.
pub(crate) fn fmt_command_failure<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    operation: &str,
    program: &OsStr,
    args: &[A],
    output: &CommandOutput,
) -> fmt::Result {
    write!(f, "{operation} failed: executable {program:?}, argv [")?;
    fmt_argv(f, args)?;
    write!(
        f,
        "], {}; stdout: {}; stderr: {}",
        output.status,
        DiagnosticBytes(&output.stdout),
        DiagnosticBytes(&output.stderr)
    )
}

pub(crate) fn fmt_command_spawn<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    program: &OsStr,
    args: &[A],
    source: &std::io::Error,
) -> fmt::Result {
    write!(f, "could not run executable {program:?}, argv [")?;
    fmt_argv(f, args)?;
    write!(f, "]: {source}")
}

fn fmt_argv<A: AsRef<OsStr>>(f: &mut fmt::Formatter<'_>, args: &[A]) -> fmt::Result {
    for (index, arg) in args.iter().enumerate() {
        if index != 0 {
            f.write_str(", ")?;
        }
        write!(f, "{:?}", arg.as_ref())?;
    }
    Ok(())
}

struct DiagnosticBytes<'a>(&'a [u8]);

impl fmt::Display for DiagnosticBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0.trim_ascii();
        if bytes.is_empty() {
            return f.write_str("<empty>");
        }

        let mut remaining = bytes;
        while !remaining.is_empty() {
            match std::str::from_utf8(remaining) {
                Ok(text) => {
                    f.write_str(text)?;
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if valid != 0 {
                        let prefix = std::str::from_utf8(&remaining[..valid])
                            .expect("Utf8Error valid prefix must decode");
                        f.write_str(prefix)?;
                    }
                    let invalid = error
                        .error_len()
                        .unwrap_or_else(|| remaining.len().saturating_sub(valid));
                    for byte in &remaining[valid..valid + invalid] {
                        write!(f, "\\x{byte:02x}")?;
                    }
                    remaining = &remaining[valid + invalid..];
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_bytes_trim_edges_and_preserve_invalid_bytes() {
        assert_eq!(DiagnosticBytes(b"  readable\n").to_string(), "readable");
        assert_eq!(
            DiagnosticBytes(b"\nleft\xffright\x80\n").to_string(),
            "left\\xffright\\x80"
        );
        assert_eq!(DiagnosticBytes(b" \t\n").to_string(), "<empty>");
    }

    #[test]
    fn command_failure_and_spawn_share_argv_rendering() {
        let args = ["--flag", "value with space"];
        struct Failure<'a>(&'a [&'a str]);
        impl fmt::Display for Failure<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_command_failure(
                    f,
                    "copy",
                    OsStr::new("diskutil"),
                    self.0,
                    &CommandOutput::failure(1, "denied"),
                )
            }
        }
        struct Spawn<'a>(&'a [&'a str]);
        impl fmt::Display for Spawn<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_command_spawn(
                    f,
                    OsStr::new("diskutil"),
                    self.0,
                    &std::io::Error::from_raw_os_error(2),
                )
            }
        }
        let argv = r#"argv ["--flag", "value with space"]"#;
        assert!(Failure(&args).to_string().contains(argv));
        assert!(Spawn(&args).to_string().contains(argv));
    }
}
