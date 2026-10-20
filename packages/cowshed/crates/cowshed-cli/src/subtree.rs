//! Direct native subtree commands: the wrapper itself is the authenticated socket peer.

use std::fs::File;
use std::io::{self, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::Command;

use cowshed_core::api::process::{
    JobSubtreeAdmission, JobSubtreeHandle, JobSubtreeRefusal, JobSubtreeRetirement,
};
use cowshed_core::runtime::supervisor_socket;
use cowshed_core::{CowshedError, Result};
use cowshed_os::fork_lock::{self, Fenced};
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::args::{RetireDescendantsArgs, SubtreeRootArgs};
use crate::output::Output;

const MAX_HANDLE_BYTES: u64 = 64 * 1024;
const HANDLE_HINT: &str = "pipe exactly one JobSubtreeHandle JSON value, at most 65536 bytes, to cowshed retire-descendants --grace-ms N";

pub(crate) async fn root(args: &SubtreeRootArgs) -> Result<i32> {
    // Claim the descriptor before opening the socket: a closed number must never accidentally
    // name a descriptor the admission request just allocated. Parsing has already finished.
    let channel = take_admission_fd(args.handle_fd)?;
    let admission = match std::env::var_os("COWSHED_SUPERVISOR_SOCKET") {
        Some(path) => supervisor_socket::subtree_handle(&PathBuf::from(path)).await?,
        None => JobSubtreeAdmission::Refused {
            reason: JobSubtreeRefusal::FoldUnavailable,
        },
    };
    publish_and_exec(args, channel, &admission)
}

fn take_admission_fd(fd: RawFd) -> Result<Fenced<File>> {
    fork_lock::create(|| {
        if fd < 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "descriptor must be at least 3",
            ));
        }
        // SAFETY: F_GETFD and F_GETFL inspect a descriptor without taking ownership or writing
        // through it. An invalid, read-only or runtime-owned CLOEXEC descriptor is left alone.
        let descriptor_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if descriptor_flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if descriptor_flags & libc::FD_CLOEXEC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "descriptor is close-on-exec, not an inherited admission channel",
            ));
        }
        // SAFETY: F_GETFL reads the descriptor's access mode and takes no pointer arguments.
        let status_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if status_flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if !matches!(
            status_flags & libc::O_ACCMODE,
            libc::O_WRONLY | libc::O_RDWR
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "descriptor is not writable",
            ));
        }
        // SAFETY: the CLI caller explicitly transferred this valid descriptor. Set CLOEXEC
        // under the fork lock before owning it, so another cowshed spawn cannot retain it.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, descriptor_flags | libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the descriptor was validated above and is exclusively transferred by the
        // invocation. No other Rust owner is constructed; Fenced closes it exactly once.
        Ok(Fenced::new(unsafe { File::from_raw_fd(fd) }))
    })
    .map_err(|error| {
        CowshedError::usage(
            format!("cannot take subtree-root --handle-fd {fd}: {error}"),
            "pass exclusive ownership of an inherited writable descriptor numbered at least 3",
        )
    })
}

fn publish_and_exec(
    args: &SubtreeRootArgs,
    channel: Fenced<File>,
    admission: &JobSubtreeAdmission,
) -> Result<i32> {
    let mut writer = &*channel;
    serde_json::to_writer(&mut writer, admission)
        .map_err(io::Error::other)
        .and_then(|()| writer.write_all(b"\n"))
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "could not write subtree admission to fd {}: {error}",
                    args.handle_fd
                ),
                "keep the admission reader connected until it receives the JSON record",
            )
        })?;
    // No buffering remains: the receiving caller observes EOF before the new program runs.
    drop(channel);
    // exec replaces this process, including a N-API host, without a fork, new session, group or
    // runtime shutdown. Both admitted and refused outcomes take this exact same path.
    let error = Command::new(&args.program).args(&args.args).exec();
    Err(CowshedError::environment_missing(
        format!(
            "could not exec subtree-root command {:?}: {error}",
            args.program
        ),
        "check the command path, executable permissions and arguments after --",
    ))
}

pub(crate) async fn retire<W: Write, E: Write>(
    args: &RetireDescendantsArgs,
    output: &mut Output<W, E>,
) -> Result<i32> {
    let handle = read_handle(tokio::io::stdin()).await?;
    let retirement = match std::env::var_os("COWSHED_SUPERVISOR_SOCKET") {
        Some(path) => {
            supervisor_socket::retire_descendants(&PathBuf::from(path), handle, args.grace).await?
        }
        None => JobSubtreeRetirement::Refused {
            reason: JobSubtreeRefusal::FoldUnavailable,
        },
    };
    output.bare_record(&retirement).map_err(|error| {
        CowshedError::environment_missing(
            format!("could not write subtree retirement: {error}"),
            "keep the retirement output reader connected",
        )
    })?;
    Ok(0)
}

async fn read_handle(input: impl AsyncRead + Unpin) -> Result<JobSubtreeHandle> {
    let mut bytes = Vec::new();
    input
        .take(MAX_HANDLE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("could not read subtree handle from stdin: {error}"),
                HANDLE_HINT,
            )
        })?;
    if u64::try_from(bytes.len()).expect("bounded input length fits u64") > MAX_HANDLE_BYTES {
        return Err(CowshedError::usage(
            "subtree handle exceeds 65536 bytes",
            HANDLE_HINT,
        ));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        CowshedError::usage(
            format!("stdin must contain exactly one JobSubtreeHandle JSON value: {error}"),
            HANDLE_HINT,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_core::api::JobId;
    use cowshed_os::fork_lock::Spawn as _;
    use std::ffi::OsString;
    use std::io::Read as _;
    use std::os::fd::AsRawFd as _;
    use std::os::unix::net::UnixStream;
    use std::process::Stdio;
    use std::time::Duration;

    fn handle() -> JobSubtreeHandle {
        JobSubtreeHandle {
            job_id: JobId::new(1).expect("job id"),
            index: 2,
            incarnation: "0123456789abcdef0123456789abcdef".into(),
        }
    }

    #[tokio::test]
    async fn handle_input_is_one_bounded_typed_json_value() {
        let valid = serde_json::to_vec(&handle()).expect("handle JSON");
        assert_eq!(read_handle(valid.as_slice()).await.unwrap(), handle());
        let mut at_limit = valid.clone();
        at_limit.resize(65536, b' ');
        assert_eq!(read_handle(at_limit.as_slice()).await.unwrap(), handle());
        at_limit.push(b' ');
        assert!(
            read_handle(at_limit.as_slice())
                .await
                .unwrap_err()
                .message
                .contains("exceeds")
        );
        for invalid in [b"".as_slice(), b"{}", b"null", b"[]", b"{not-json}"] {
            assert!(read_handle(invalid).await.is_err(), "{invalid:?}");
        }
        let mut two = valid.clone();
        two.extend_from_slice(&valid);
        assert!(read_handle(two.as_slice()).await.is_err());
        let admission =
            serde_json::to_vec(&JobSubtreeAdmission::Admitted { handle: handle() }).unwrap();
        assert!(read_handle(admission.as_slice()).await.is_err());
    }

    #[test]
    fn rejected_descriptor_is_not_closed() {
        let file = File::open(std::env::current_exe().unwrap()).unwrap();
        let fd = file.as_raw_fd();
        assert!(take_admission_fd(fd).is_err());
        // SAFETY: inspection only; the still-owned File must remain valid after refusal.
        assert!(unsafe { libc::fcntl(fd, libc::F_GETFD) } >= 0);
        for fd in [-1, 0, 1, 2, i32::MAX] {
            assert!(take_admission_fd(fd).is_err());
        }
    }

    /// Invoked only in an isolated test process by the test below: a real admitted response is
    /// published through the same production tail without needing a supervisor or a process fold.
    #[test]
    fn admitted_exec_helper() {
        if std::env::var_os("COWSHED_TEST_ADMITTED_EXEC").is_none() {
            return;
        }
        let args = SubtreeRootArgs {
            handle_fd: 3,
            program: OsString::from("/bin/sh"),
            args: ["-c", "printf 'subtree-pid=%s\\n' \"$$\"; exit 37"]
                .map(OsString::from)
                .into(),
        };
        let result = publish_and_exec(
            &args,
            take_admission_fd(3).unwrap(),
            &JobSubtreeAdmission::Admitted { handle: handle() },
        );
        panic!("exec returned: {result:?}");
    }

    #[test]
    fn admitted_wrapper_exec_preserves_pid_and_reports_handle() {
        let (mut reader, writer) = fork_lock::create(UnixStream::pair).unwrap();
        reader
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let writer_fd = writer.as_raw_fd();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "subtree::tests::admitted_exec_helper",
                "--nocapture",
            ])
            .env("COWSHED_TEST_ADMITTED_EXEC", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // SAFETY: after fork, only async-signal-safe fcntl/dup2 calls prepare the owned channel.
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(writer_fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn_locked().unwrap();
        let pid = child.id();
        drop(command);
        drop(writer);
        let mut record = Vec::new();
        if let Err(error) = reader.read_to_end(&mut record) {
            child.kill().expect("stop failed exec helper");
            child.wait().expect("reap failed exec helper");
            panic!("admission channel failed: {error}");
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(37), "{output:?}");
        assert!(
            output
                .stdout
                .ends_with(format!("subtree-pid={pid}\n").as_bytes()),
            "{output:?}"
        );
        assert_eq!(record.last(), Some(&b'\n'));
        assert_eq!(
            serde_json::from_slice::<JobSubtreeAdmission>(&record).unwrap(),
            JobSubtreeAdmission::Admitted { handle: handle() }
        );
    }
}
