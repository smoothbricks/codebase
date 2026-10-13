//! `cowshed disk-command <diskutil|hdiutil> -- <args…>`: one disk tool run for a caller that is
//! not cowshed (06_cli.md, "Commands").
//!
//! There is no second runner here. The request goes to [`SystemCommandRunner`], the runner every
//! cowshed disk child goes through, so the caller gets what cowshed gets: the host disk lease,
//! the child's own session, the attach-pool bookkeeping, and a disk-image command that is
//! detached, file-backed and never killed (01_storage.md, "How the APFS host degrades" §2). This
//! module adds only the shape a script reads: the tool's bytes on stdout and stderr, and its exit
//! status as this process's.

use std::io::{self, Write};

use cowshed_apfs::{
    CommandOutput, CommandRequest, CommandRunError, CommandRunFailure, ProcessStatus,
    SystemCommandRunner,
};
use cowshed_core::{CowshedError, ErrorCode};

use crate::args::DiskCommandArgs;
use crate::output::Output;

/// The request handed to the runner: the named tool and its argv, untouched.
#[must_use]
pub fn request(args: &DiskCommandArgs) -> CommandRequest {
    CommandRequest::new(args.tool.program(), args.args.iter().cloned())
}

/// Run the tool and answer this process's exit code. Every failure of the runner itself is
/// reported on stderr and exits with the wrapper codes `exec` uses (100–106), so it can never be
/// mistaken for the tool's own status.
pub async fn dispatch<W, E>(args: &DiskCommandArgs, output: &mut Output<W, E>) -> i32
where
    W: Write + Send,
    E: Write + Send,
{
    let outcome = match run(args).await {
        Ok(ran) => {
            let (stdout, stderr) = output.writers_mut();
            deliver(&ran, stdout, stderr)
        }
        Err(error) => Err(error),
    };
    match outcome {
        Ok(code) => code,
        Err(error) => {
            let code = i32::from(error.exec_wrapper_exit_code());
            // The exit code is the report a caller that closed stderr still reads; a write that
            // fails here has nowhere else to go.
            let _ = output
                .error(&error.message)
                .and_then(|()| output.hint(&error.hint));
            code
        }
    }
}

/// The tool's outcome, waited for off the async runtime: a disk-image command can run for as
/// long as it takes. The tool reads this process's standard input, so a caller can pipe a
/// passphrase to it.
async fn run(args: &DiskCommandArgs) -> Result<CommandOutput, CowshedError> {
    let request = request(args);
    tokio::task::spawn_blocking(move || SystemCommandRunner.run_with_inherited_input(&request))
        .await
        .map_err(|error| {
            CowshedError::internal(format!("the disk-command runner thread failed: {error}"))
        })?
        .map_err(runner_error)
}

/// Why the runner produced no outcome, in the taxonomy a caller branches on.
fn runner_error(error: CommandRunError) -> CowshedError {
    let message = error.to_string();
    match error.failure {
        CommandRunFailure::Spawn(source) if source.kind() == io::ErrorKind::NotFound => {
            CowshedError::new(ErrorCode::EnvironmentMissing, message, "cowshed doctor")
        }
        CommandRunFailure::Deadline(_) => CowshedError::conflict(message, "cowshed doctor"),
        CommandRunFailure::Spawn(_) | CommandRunFailure::Wait(_) => CowshedError::internal(message),
    }
}

/// Write the tool's own bytes to `stdout` and `stderr`, and answer the exit code its status
/// maps to: its exit code, or 128 plus the signal that ended it, as a shell reports one.
///
/// The tool has already run to its end when this writes, so a reader that went away — a caller
/// that gave up and closed its pipes — undoes nothing: it is reported as a wrapper failure, and
/// stderr is still attempted when stdout is gone.
pub fn deliver(
    ran: &CommandOutput,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<i32, CowshedError> {
    let wrote_stdout = stdout.write_all(&ran.stdout).and_then(|()| stdout.flush());
    let wrote_stderr = stderr.write_all(&ran.stderr).and_then(|()| stderr.flush());
    if let Err(error) = wrote_stdout.and(wrote_stderr) {
        return Err(CowshedError::internal(format!(
            "the disk tool finished ({:?}), but its output could not be delivered: {error}",
            ran.status
        )));
    }
    match ran.status {
        ProcessStatus::Exit(code) => Ok(code),
        ProcessStatus::Signal(signal) => Ok(128 + signal),
        ProcessStatus::Unknown => Err(CowshedError::internal(
            "the disk tool ended, but the kernel reported neither its exit code nor its signal",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_apfs::DiskTool;
    use std::ffi::OsString;

    fn args(tool: DiskTool, argv: &[&str]) -> DiskCommandArgs {
        DiskCommandArgs {
            tool,
            args: argv.iter().map(OsString::from).collect(),
        }
    }

    #[test]
    fn the_request_is_the_named_tool_with_its_argv_untouched() {
        let request = request(&args(DiskTool::Hdiutil, &["info", "-plist"]));
        assert_eq!(request.program, std::path::Path::new("/usr/bin/hdiutil"));
        assert_eq!(
            request.args,
            [OsString::from("info"), OsString::from("-plist")]
        );
        let request = request_for_diskutil();
        assert_eq!(request.program, std::path::Path::new("/usr/sbin/diskutil"));
        assert_eq!(
            request.args,
            ["apfs", "unlockVolume", "--json", "-stdinpassphrase"].map(OsString::from)
        );
    }

    fn request_for_diskutil() -> CommandRequest {
        request(&args(
            DiskTool::Diskutil,
            &["apfs", "unlockVolume", "--json", "-stdinpassphrase"],
        ))
    }

    #[test]
    fn the_tools_bytes_and_status_pass_through_unchanged() {
        let ran = CommandOutput {
            status: ProcessStatus::Exit(35),
            stdout: b"\0<plist>\n".to_vec(),
            stderr: b"hdiutil: attach failed\n".to_vec(),
        };
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        assert_eq!(deliver(&ran, &mut stdout, &mut stderr).unwrap(), 35);
        assert_eq!(stdout, ran.stdout);
        assert_eq!(stderr, ran.stderr);

        let signaled = CommandOutput {
            status: ProcessStatus::Signal(libc::SIGTERM),
            ..CommandOutput::default()
        };
        assert_eq!(
            deliver(&signaled, &mut Vec::new(), &mut Vec::new()).unwrap(),
            128 + libc::SIGTERM
        );

        let unknown = CommandOutput {
            status: ProcessStatus::Unknown,
            stdout: b"out".to_vec(),
            stderr: Vec::new(),
        };
        let mut stdout = Vec::new();
        let error = deliver(&unknown, &mut stdout, &mut Vec::new()).unwrap_err();
        assert_eq!(stdout, b"out", "the bytes are delivered before the refusal");
        assert_eq!(error.exec_wrapper_exit_code(), 100);
    }

    /// A reader that closed its end.
    struct Closed;

    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        }
    }

    #[test]
    fn a_closed_reader_is_a_wrapper_failure_and_stderr_is_still_written() {
        let ran = CommandOutput {
            status: ProcessStatus::Exit(0),
            stdout: b"framework : 1\n".to_vec(),
            stderr: b"note\n".to_vec(),
        };
        let mut stderr = Vec::new();
        let error = deliver(&ran, &mut Closed, &mut stderr).unwrap_err();
        assert_eq!(stderr, b"note\n");
        assert_eq!(error.exec_wrapper_exit_code(), 100);
        assert!(error.message.contains("could not be delivered"), "{error}");
        assert!(deliver(&ran, &mut Vec::new(), &mut Closed).is_err());
    }

    #[test]
    fn runner_failures_map_to_wrapper_codes_never_a_tool_status() {
        let request = CommandRequest::new("/usr/bin/hdiutil", ["info"]);
        let missing = runner_error(CommandRunError {
            request: request.clone(),
            failure: CommandRunFailure::Spawn(io::Error::from(io::ErrorKind::NotFound)),
        });
        assert_eq!(missing.exec_wrapper_exit_code(), 104);
        let hung = runner_error(CommandRunError {
            request: request.clone(),
            failure: CommandRunFailure::Deadline(std::time::Duration::from_secs(120)),
        });
        assert_eq!(hung.exec_wrapper_exit_code(), 103);
        let lost = runner_error(CommandRunError {
            request,
            failure: CommandRunFailure::Wait(io::Error::from(io::ErrorKind::Interrupted)),
        });
        assert_eq!(lost.exec_wrapper_exit_code(), 100);
    }
}
