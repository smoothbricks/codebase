//! `cowshed disk-command <diskutil|hdiutil> -- <args…>`: one disk tool run for a caller that is
//! not cowshed (06_cli.md, "Commands").
//!
//! There is no second runner here. The request goes to [`SystemCommandRunner`], the runner every
//! cowshed disk child goes through, so the caller gets what cowshed gets: the host disk lease,
//! the child's own session, the attach-pool bookkeeping, and a disk-image command that is
//! detached, file-backed and never killed (01a_apfs.md, "How the APFS host degrades" §2). This
//! module adds only the shape a script reads: the tool's bytes on stdout and stderr, and its exit
//! status as this process's.

use std::io::{self, Write};

use cowshed_apfs::{
    CommandDeadline, CommandOutput, CommandRequest, CommandRunError, CommandRunFailure,
    ProcessStatus, SystemCommandRunner,
};
use cowshed_core::api::dto::{BinaryData, DiskCommandEvent, MAX_INLINE_OUTPUT_BYTES};
use cowshed_core::{CowshedError, ErrorCode};
use cowshed_os::namespace::HostNamespace;

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
    let outcome = match run(args, ToolInput::Inherited).await {
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

/// Where the tool's standard input comes from.
pub(crate) enum ToolInput {
    /// This process's own, so a caller of the verb can pipe a passphrase to it.
    Inherited,
    /// These bytes, closed after them: a host connection's request, whose own standard input is
    /// its protocol socket and never the tool's.
    Bytes(Vec<u8>),
}

/// The tool's outcome, waited for off the async runtime: a disk-image command can run for as
/// long as it takes.
pub(crate) async fn run(
    args: &DiskCommandArgs,
    input: ToolInput,
) -> Result<CommandOutput, CowshedError> {
    let request = request(args);
    let deadline = CommandDeadline::new(args.deadline);
    let registry_entry = args.registry_entry;
    let gateway_socket = args.gateway_socket.clone();
    // The verb is the host's own, so production's namespace names its runner.
    let runner = SystemCommandRunner::new(HostNamespace::Production);
    tokio::task::spawn_blocking(move || match (registry_entry, input) {
        // A bound detach reads no input, from either caller.
        (Some(entry), _) => runner.run_detach_of_entry(&request, entry, gateway_socket.as_deref()),
        (None, ToolInput::Inherited) => {
            runner.run_with_inherited_input(&request, deadline, gateway_socket.as_deref())
        }
        (None, ToolInput::Bytes(bytes)) => {
            runner.run_with_input(&request, deadline, bytes, gateway_socket.as_deref())
        }
    })
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
        CommandRunFailure::Deadline { .. } | CommandRunFailure::Unspawned(_) => {
            CowshedError::conflict(message, "cowshed doctor")
        }
        CommandRunFailure::VolumeRename => CowshedError::usage(
            message,
            "no disk tool renames a volume; create it under its final name (specs/cowshed/01a_apfs.md, \"Ownership, identity, and the volume label\")",
        ),
        CommandRunFailure::Release(refusal) => CowshedError::conflict(message, refusal.hint()),
        CommandRunFailure::DeviceChanged(_) => CowshedError::conflict(
            message,
            "read the I/O Registry again and detach the device its entry names now, if any",
        ),
        CommandRunFailure::Spawn(_)
        | CommandRunFailure::Wait(_)
        | CommandRunFailure::Kill { .. }
        | CommandRunFailure::Reap { .. }
        | CommandRunFailure::Output { .. }
        | CommandRunFailure::Input { .. } => CowshedError::internal(message),
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
    exit_code(ran.status)
}

/// The exit code a status maps to: its exit code, or 128 plus the signal that ended it.
fn exit_code(status: ProcessStatus) -> Result<i32, CowshedError> {
    match status {
        ProcessStatus::Exit(code) => Ok(code),
        ProcessStatus::Signal(signal) => Ok(128 + signal),
        ProcessStatus::Unknown => Err(CowshedError::internal(
            "the disk tool ended, but the kernel reported neither its exit code nor its signal",
        )),
    }
}

/// The tool's outcome as `host.diskCommand` delivers it, one event per demand: its whole stdout in
/// order, in chunks of at most one inline frame, then its whole stderr the same way, and last its
/// exit — or, when the kernel reported no status, the error `disk-command` reports after writing
/// the same bytes. It holds the run's own output: a stream that fits one frame moves its buffer
/// into its one event, and a larger one copies one bounded chunk per event.
pub(crate) struct OutputEvents {
    stdout: Stream,
    stderr: Stream,
    end: Option<Result<DiskCommandEvent, CowshedError>>,
}

/// One of the tool's streams and how much of it has been delivered.
struct Stream {
    bytes: Vec<u8>,
    delivered: usize,
}

impl Stream {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            delivered: 0,
        }
    }

    fn next_chunk(&mut self) -> Option<BinaryData> {
        let rest = self.bytes.len() - self.delivered;
        if rest == 0 {
            return None;
        }
        let data = if self.delivered == 0 && rest <= MAX_INLINE_OUTPUT_BYTES {
            BinaryData::new(std::mem::take(&mut self.bytes))
        } else {
            let length = rest.min(MAX_INLINE_OUTPUT_BYTES);
            let chunk = &self.bytes[self.delivered..self.delivered + length];
            self.delivered += length;
            BinaryData::new(chunk)
        };
        Some(data.expect("a chunk cut to the inline bound fits it"))
    }
}

impl OutputEvents {
    pub(crate) fn new(ran: CommandOutput) -> Self {
        Self {
            stdout: Stream::new(ran.stdout),
            stderr: Stream::new(ran.stderr),
            end: Some(exit_code(ran.status).map(|code| DiskCommandEvent::Exit { code })),
        }
    }

    /// The next event, made now; `None` once the exit (or the error) has been delivered.
    pub(crate) fn next_event(&mut self) -> Option<Result<DiskCommandEvent, CowshedError>> {
        if let Some(data) = self.stdout.next_chunk() {
            return Some(Ok(DiskCommandEvent::Stdout { data }));
        }
        if let Some(data) = self.stderr.next_chunk() {
            return Some(Ok(DiskCommandEvent::Stderr { data }));
        }
        self.end.take()
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
            deadline: cowshed_apfs::DISK_CHILD_DEADLINE,
            args: argv.iter().map(OsString::from).collect(),
            gateway_socket: None,
            registry_entry: None,
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

    /// `host.diskCommand` answers the same outcome as ordered events: stdout cut to the inline
    /// bound in order, then stderr, then the exit code `disk-command` exits with; a status the
    /// kernel did not report still delivers every byte before its error.
    #[test]
    fn the_host_events_are_the_whole_output_in_order_then_the_exit() {
        let stdout: Vec<u8> = (0..=MAX_INLINE_OUTPUT_BYTES)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        let ran = CommandOutput {
            status: ProcessStatus::Signal(libc::SIGTERM),
            stdout: stdout.clone(),
            stderr: b"err".to_vec(),
        };
        let mut events = OutputEvents::new(ran);
        let mut next = || events.next_event().map(Result::unwrap);
        assert_eq!(
            next(),
            Some(DiskCommandEvent::Stdout {
                data: BinaryData::new(&stdout[..MAX_INLINE_OUTPUT_BYTES]).unwrap()
            })
        );
        assert_eq!(
            next(),
            Some(DiskCommandEvent::Stdout {
                data: BinaryData::new(&stdout[MAX_INLINE_OUTPUT_BYTES..]).unwrap()
            })
        );
        assert_eq!(
            next(),
            Some(DiskCommandEvent::Stderr {
                data: BinaryData::new(&b"err"[..]).unwrap()
            })
        );
        assert_eq!(
            next(),
            Some(DiskCommandEvent::Exit {
                code: 128 + libc::SIGTERM
            })
        );
        assert_eq!(next(), None);

        // Output that fits one frame is moved into its event: the same buffer, never a copy.
        let single = b"out".to_vec();
        let buffer = single.as_ptr();
        let mut events = OutputEvents::new(CommandOutput {
            status: ProcessStatus::Unknown,
            stdout: single,
            stderr: Vec::new(),
        });
        let Some(Ok(DiskCommandEvent::Stdout { data })) = events.next_event() else {
            panic!("the single stdout frame comes first");
        };
        assert_eq!(data.as_bytes(), b"out");
        assert_eq!(data.as_bytes().as_ptr(), buffer);
        assert_eq!(
            events
                .next_event()
                .unwrap()
                .unwrap_err()
                .exec_wrapper_exit_code(),
            100
        );
        assert!(events.next_event().is_none());
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
            failure: CommandRunFailure::Deadline {
                deadline: std::time::Duration::from_secs(120),
                pid: 4242,
                ended: ProcessStatus::Signal(libc::SIGKILL),
            },
        });
        assert_eq!(hung.exec_wrapper_exit_code(), 103);
        assert!(hung.message.contains("reaped with signal"), "{hung}");
        let unspawned = runner_error(CommandRunError {
            request: request.clone(),
            failure: CommandRunFailure::Unspawned(std::time::Duration::from_millis(37)),
        });
        assert_eq!(unspawned.exec_wrapper_exit_code(), 103);
        assert!(
            unspawned.message.contains("no disk tool was started"),
            "{unspawned}"
        );
        let failed_kill = runner_error(CommandRunError {
            request: request.clone(),
            failure: CommandRunFailure::Kill {
                deadline: std::time::Duration::from_millis(37),
                pid: 123,
                source: io::Error::from(io::ErrorKind::PermissionDenied),
                ended: Ok(None),
            },
        });
        assert_eq!(failed_kill.exec_wrapper_exit_code(), 100);
        assert!(
            failed_kill.message.contains("still running as pid 123"),
            "{failed_kill}"
        );
        let failed_reap = runner_error(CommandRunError {
            request: request.clone(),
            failure: CommandRunFailure::Reap {
                deadline: std::time::Duration::from_millis(37),
                failure: Box::new(cowshed_os::deadline::ReapError::Wait {
                    op: "reap killed disk child",
                    role: "disk child",
                    pid: 123,
                    elapsed: std::time::Duration::from_millis(1),
                    source: io::Error::from(io::ErrorKind::Interrupted),
                }),
            },
        });
        assert_eq!(failed_reap.exec_wrapper_exit_code(), 100);
        assert!(
            failed_reap.message.contains("could not reap"),
            "{failed_reap}"
        );
        let lost = runner_error(CommandRunError {
            request,
            failure: CommandRunFailure::Wait(io::Error::from(io::ErrorKind::Interrupted)),
        });
        assert_eq!(lost.exec_wrapper_exit_code(), 100);
    }
}
