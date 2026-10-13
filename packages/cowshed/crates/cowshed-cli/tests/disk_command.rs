//! `cowshed disk-command`, run as the binary a script runs: the tool's own bytes and status
//! through the shared disk runner, usage refusals that never reach a tool, and output delivered
//! to a reader that already left reported as a wrapper failure, never a panic.
#![cfg(target_os = "macos")]

use std::os::fd::{FromRawFd, OwnedFd};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use cowshed_apfs::{CommandRequest, CommandRunner, ProcessStatus, SystemCommandRunner};
use cowshed_os::fork_lock::Run as _;

/// The `cowshed` binary under test, read when the test runs (see `output_contracts.rs`).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

fn disk_command(argv: &[&str]) -> std::process::Output {
    Command::new(cowshed())
        .arg("disk-command")
        .args(argv)
        .stdin(Stdio::null())
        .output_locked()
        .expect("run cowshed disk-command")
}

/// The facade a TypeScript, Python or shell caller types: `hdiutil info` answers through it.
#[test]
fn hdiutil_info_answers_through_the_facade() {
    let output = disk_command(&["hdiutil", "--", "info"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("framework"), "{stdout}");
    assert!(
        !stdout.contains("cowshed:"),
        "no facade text on stdout: {stdout}"
    );
}

/// A failing tool is the tool's failure: the facade's status and both streams are the bytes the
/// shared runner answers for the same argv in-process.
#[test]
fn a_failing_tool_passes_its_own_status_and_bytes_through() {
    let argv = ["info", "cowshed-disk-command-no-such-disk"];
    let expected = SystemCommandRunner
        .run(&CommandRequest::new("/usr/sbin/diskutil", argv))
        .expect("the runner answers diskutil");
    let ProcessStatus::Exit(code) = expected.status else {
        panic!("diskutil exits on its own: {expected:?}");
    };
    assert_ne!(code, 0, "{expected:?}");
    let output = disk_command(&["diskutil", "--", argv[0], argv[1]]);
    assert_eq!(output.status.code(), Some(code), "{output:?}");
    assert_eq!(output.stdout, expected.stdout);
    assert_eq!(output.stderr, expected.stderr);
}

#[test]
fn usage_refusals_never_reach_a_tool() {
    for (argv, said) in [
        (&["hdiutil", "info"][..], "requires `--`"),
        (&["hdiutil", "--"][..], "`--`"),
        (
            &["/usr/bin/hdiutil", "--", "info"][..],
            "diskutil or hdiutil",
        ),
        (&["newfs_apfs", "--", "-v", "x"][..], "diskutil or hdiutil"),
        (&[][..], "requires a tool"),
    ] {
        let output = disk_command(argv);
        assert_eq!(output.status.code(), Some(2), "{argv:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{argv:?}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(said), "{argv:?}: {stderr}");
    }
    let json = Command::new(cowshed())
        .args(["--json", "disk-command", "hdiutil", "--", "info"])
        .stdin(Stdio::null())
        .output_locked()
        .expect("run cowshed --json disk-command");
    assert_eq!(json.status.code(), Some(2), "{json:?}");
    assert!(
        String::from_utf8_lossy(&json.stdout).contains("\"ok\":false"),
        "{json:?}"
    );
}

/// A caller that gave up and closed its end of stdout before the tool finished: the tool runs to
/// its end, and the undeliverable output is the wrapper's internal failure (exit 100), said on
/// stderr — not a panic, and not the tool's status.
#[test]
fn output_to_a_reader_that_left_is_a_wrapper_failure() {
    let mut ends = [0; 2];
    // SAFETY: `pipe` writes two new descriptors into the two-element array.
    assert_eq!(unsafe { libc::pipe(ends.as_mut_ptr()) }, 0, "pipe");
    // SAFETY: both descriptors are fresh from `pipe` and owned only here.
    let (reader, writer) =
        unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
    drop(reader);
    let output = Command::new(cowshed())
        .args(["disk-command", "hdiutil", "--", "info"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(writer))
        .stderr(Stdio::piped())
        .output_locked()
        .expect("run cowshed disk-command");
    assert_eq!(output.status.code(), Some(100), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("could not be delivered"), "{stderr}");
}
