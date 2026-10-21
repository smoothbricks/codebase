//! Native and library-hosted subtree wrappers, without a real supervisor or project.
#![cfg(unix)]

use std::ffi::OsString;
use std::io::{self, Read as _, Write as _};
use std::os::fd::AsRawFd as _;
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use cowshed_cli::args::{self, Command as CliCommand, ProjectDiscovery};
use cowshed_core::api::process::{JobSubtreeAdmission, JobSubtreeRefusal, JobSubtreeRetirement};
use cowshed_os::fork_lock::{self, Spawn as _};

fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set the CLI executable path"),
    )
}

fn wrapper(mut command: Command) -> (u32, Vec<u8>, Output) {
    let (mut reader, writer) = fork_lock::create(UnixStream::pair).unwrap();
    reader
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let writer_fd = writer.as_raw_fd();
    command
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: the child only duplicates the owned channel with async-signal-safe syscalls.
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
    let mut admission = Vec::new();
    if let Err(error) = reader.read_to_end(&mut admission) {
        child.kill().expect("stop wrapper after admission failure");
        child.wait().expect("reap wrapper after admission failure");
        panic!("admission read failed: {error}");
    }
    (pid, admission, child.wait_with_output().unwrap())
}

fn root_command(argv: &[OsString]) -> Command {
    let mut command = Command::new(cowshed());
    command
        .args(["subtree-root", "--handle-fd", "3", "--"])
        .args(argv)
        .env_remove("COWSHED_SUPERVISOR_SOCKET");
    command
}

fn assert_unavailable(admission: &[u8]) {
    assert_eq!(admission.last(), Some(&b'\n'));
    assert_eq!(
        serde_json::from_slice::<JobSubtreeAdmission>(admission).unwrap(),
        JobSubtreeAdmission::Refused {
            reason: JobSubtreeRefusal::FoldUnavailable
        }
    );
}

#[test]
fn refused_admission_execs_in_the_same_life_with_byte_exact_argv() {
    let opaque = OsString::from_vec(vec![b'x', 0x80, b'y']);
    let args = vec![
        OsString::from("/bin/sh"),
        OsString::from("-c"),
        OsString::from(
            "if [ -e /dev/fd/3 ]; then exit 38; fi; printf '%s\\n' \"$$\"; printf '%s\\000' \"$@\"; exit 37",
        ),
        OsString::from("subtree-test"),
        OsString::from(""),
        OsString::from("spaces and\nnewlines"),
        OsString::from("--help"),
        OsString::from("--json"),
        OsString::from("--project"),
        OsString::from("--"),
        opaque,
    ];
    let mut command = root_command(&args);
    command.env("LC_ALL", "C");
    let (pid, admission, output) = wrapper(command);
    assert_unavailable(&admission);
    assert_eq!(output.status.code(), Some(37), "{output:?}");
    let mut expected = format!("{pid}\n").into_bytes();
    for argument in &args[4..] {
        expected.extend_from_slice(argument.as_encoded_bytes());
        expected.push(0);
    }
    assert_eq!(output.stdout, expected);
    assert!(output.stderr.is_empty(), "{output:?}");
}

#[test]
fn transport_failure_does_not_exec_unleased() {
    let mut command = root_command(&["/bin/sh", "-c", "printf should-not-run"].map(OsString::from));
    // A directory is not a listening Unix socket, on either supported platform.
    command.env("COWSHED_SUPERVISOR_SOCKET", "/");
    let (_, admission, output) = wrapper(command);
    assert!(!output.status.success(), "{output:?}");
    assert!(admission.is_empty(), "{admission:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(
        !output.stderr.is_empty(),
        "operational failure must be loud"
    );
}

#[test]
fn exec_failure_is_nonzero_after_the_admission_record() {
    let (_, admission, output) = wrapper(root_command(&[OsString::from(
        "/cowshed-subtree-command-that-does-not-exist",
    )]));
    assert_unavailable(&admission);
    assert!(!output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("could not exec"),
        "{output:?}"
    );
}

#[test]
fn admission_write_failure_does_not_exec() {
    let (reader, writer) = fork_lock::create(UnixStream::pair).unwrap();
    drop(reader);
    let fd = writer.as_raw_fd();
    let mut command = root_command(&["/bin/sh", "-c", "printf should-not-run"].map(OsString::from));
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: the child only prepares the descriptor handed to the wrapper.
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let output = command.spawn_locked().unwrap().wait_with_output().unwrap();
    assert!(!output.status.success(), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("could not write subtree admission"),
        "{output:?}"
    );
}

#[test]
fn subtree_grammar_validates_every_argument_before_descriptor_ownership() {
    let (mut reader, mut writer) = fork_lock::create(UnixStream::pair).unwrap();
    let fd = writer.as_raw_fd().to_string();
    for argv in [
        vec!["subtree-root"],
        vec!["subtree-root", "--handle-fd", &fd, "--"],
        vec!["subtree-root", "--handle-fd", &fd, "/bin/true"],
        vec![
            "subtree-root",
            "--handle-fd",
            &fd,
            "--unknown",
            "--",
            "true",
        ],
        vec![
            "subtree-root",
            "--handle-fd",
            &fd,
            "--handle-fd",
            &fd,
            "--",
            "true",
        ],
        vec![
            "subtree-root",
            "--handle-fd",
            &fd,
            "--project",
            "/",
            "--",
            "true",
        ],
        vec!["subtree-root", "--handle-fd", &fd, "--", ""],
        vec!["subtree-root", "--handle-fd", &fd, "--", "nul\0command"],
        vec!["subtree-root", "--handle-fd", &fd, "--", "true", "nul\0arg"],
    ] {
        assert!(args::parse_args(argv.iter().copied()).is_err(), "{argv:?}");
    }
    for fd in ["", "0", "1", "2", "-1", "+3", "3.5", "no", "2147483648"] {
        assert!(
            args::parse_args(["subtree-root", "--handle-fd", fd, "--", "true"]).is_err(),
            "{fd}"
        );
    }
    writer.write_all(b"still-owned").unwrap();
    let mut bytes = [0; 11];
    reader.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"still-owned");
    let parsed =
        args::parse_args(["subtree-root", "--handle-fd", "3", "--", "true", "--help"]).unwrap();
    assert_eq!(
        parsed.command.project_discovery(),
        ProjectDiscovery::NotUsed
    );
    let CliCommand::SubtreeRoot(root) = parsed.command else {
        panic!("subtree root")
    };
    assert_eq!(root.args, [OsString::from("--help")]);
    for verb in ["subtree-root", "retire-descendants"] {
        let parsed = args::parse_args([verb, "--help"]).unwrap();
        assert!(matches!(parsed.command, CliCommand::Help(Some(_))));
        assert!(
            cowshed_cli::help::command_named(verb)
                .unwrap()
                .page()
                .contains("COWSHED_SUPERVISOR_SOCKET")
        );
    }
}

#[test]
fn retirement_grace_is_required_and_nonnegative() {
    for argv in [
        vec!["retire-descendants"],
        vec!["retire-descendants", "--grace-ms"],
        vec!["retire-descendants", "--grace-ms", "0", "extra"],
        vec!["retire-descendants", "--grace-ms", "0", "--grace-ms", "1"],
        vec!["retire-descendants", "--grace-ms", "0", "--project", "/"],
    ] {
        assert!(args::parse_args(argv.iter().copied()).is_err(), "{argv:?}");
    }
    for value in ["", "-1", "+1", "1.5", "no", "18446744073709551616"] {
        assert!(
            args::parse_args(["retire-descendants", "--grace-ms", value]).is_err(),
            "{value}"
        );
    }
    for value in [0_u64, 37, u64::MAX] {
        let parsed =
            args::parse_args(["retire-descendants", "--grace-ms", &value.to_string()]).unwrap();
        assert_eq!(
            parsed.command.project_discovery(),
            ProjectDiscovery::NotUsed
        );
        let CliCommand::RetireDescendants(retire) = parsed.command else {
            panic!("retirement")
        };
        assert_eq!(retire.grace, Duration::from_millis(value));
    }
}

fn retire(input: &[u8], socket: Option<&str>) -> Output {
    let mut command = Command::new(cowshed());
    command
        .args(["retire-descendants", "--grace-ms", "0"])
        .current_dir("/")
        .env_remove("COWSHED_SUPERVISOR_SOCKET")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(path) = socket {
        command.env("COWSHED_SUPERVISOR_SOCKET", path);
    }
    let mut child = command.spawn_locked().unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

const HANDLE: &[u8] = br#"{"jobId":1,"index":2,"incarnation":"0123456789abcdef0123456789abcdef"}"#;

#[test]
fn retirement_reports_typed_refusal_without_a_project_or_gateway() {
    let output = retire(HANDLE, None);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout.last(), Some(&b'\n'));
    assert_eq!(
        serde_json::from_slice::<JobSubtreeRetirement>(&output.stdout).unwrap(),
        JobSubtreeRetirement::Refused {
            reason: JobSubtreeRefusal::FoldUnavailable
        }
    );
    assert!(output.stderr.is_empty(), "{output:?}");
}

#[test]
fn retirement_transport_and_input_failures_are_loud_nonzero() {
    for output in [
        retire(HANDLE, Some("/")),
        retire(b"{}", None),
        retire(b"{} {}", None),
    ] {
        assert!(!output.status.success(), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(!output.stderr.is_empty(), "{output:?}");
    }
}

#[test]
fn hosted_entrypoint_helper() {
    if std::env::var_os("COWSHED_TEST_HOSTED_SUBTREE").is_none() {
        return;
    }
    // Prepared before the runtime exists, as the binary prepares its own argv: preparation owns
    // the invocation's inherited descriptors.
    let invocation = cowshed_cli::run::prepare(
        [
            "subtree-root",
            "--handle-fd",
            "3",
            "--",
            "/bin/sh",
            "-c",
            "if [ -e /dev/fd/3 ]; then exit 38; fi; printf 'hosted-pid=%s\\n' \"$$\"; exit 37",
        ]
        .map(OsString::from)
        .into(),
    )
    .unwrap_or_else(|code| panic!("the hosted subtree-root was refused with exit {code}"));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let code = runtime.block_on(cowshed_cli::run::run(invocation));
    panic!("hosted exec returned {code}");
}

#[test]
fn library_hosted_entrypoint_execs_instead_of_spawning() {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "hosted_entrypoint_helper", "--nocapture"])
        .env("COWSHED_TEST_HOSTED_SUBTREE", "1")
        .env_remove("COWSHED_SUPERVISOR_SOCKET");
    let (pid, admission, output) = wrapper(command);
    assert_unavailable(&admission);
    assert_eq!(output.status.code(), Some(37), "{output:?}");
    assert!(
        output
            .stdout
            .ends_with(format!("hosted-pid={pid}\n").as_bytes()),
        "{output:?}"
    );
}
