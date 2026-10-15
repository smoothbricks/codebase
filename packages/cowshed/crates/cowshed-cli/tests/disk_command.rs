//! `cowshed disk-command`, run as the binary a script runs: the tool's own bytes and status
//! through the shared disk runner, usage refusals that never reach a tool, and output delivered
//! to a reader that already left reported as a wrapper failure, never a panic.
#![cfg(target_os = "macos")]

use std::os::fd::{FromRawFd, OwnedFd};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use cowshed_apfs::{CommandRequest, CommandRunner, ProcessStatus, SystemCommandRunner};
use cowshed_os::fork_lock::{Run as _, Spawn as _};

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

/// A failing tool keeps its status and bytes. Stderr also carries the runner's lease progress;
/// those diagnostics precede the tool output, which is delivered verbatim after the run.
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
    assert!(output.stderr.ends_with(&expected.stderr), "{output:?}");
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

/// The facade is one more caller of the shared runner, so Disk Arbitration's rename is refused
/// through it as everywhere, before diskutil runs: a usage failure (exit 101) naming the command,
/// nothing on stdout. The target is a path that does not exist, so a facade that let the rename
/// through would only see diskutil fail on it.
#[test]
fn the_facade_refuses_disk_arbitration_renames() {
    let target = "/private/tmp/cowshed-disk-command-no-such-volume";
    for verb in ["renameVolume", "rename", "RENAMEVOLUME"] {
        let output = disk_command(&["diskutil", "--", verb, target, "renamed"]);
        assert_eq!(output.status.code(), Some(101), "{verb}: {output:?}");
        assert!(output.stdout.is_empty(), "{verb}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("refused") && stderr.contains(target),
            "{verb}: {stderr}"
        );
    }
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
        .spawn_locked()
        .expect("spawn cowshed with the closed stdout pipe")
        .wait_with_output()
        .expect("wait for cowshed disk-command");
    assert_eq!(output.status.code(), Some(100), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("could not be delivered"), "{stderr}");
}
