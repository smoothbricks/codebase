//! The gateway host (05_gateway.md "The gateway host"): it runs its child, passes it the signals
//! launchd stops a job with, and ends as the child ended.

use cowshed_cli::gateway_host::{EXIT_FAILED, VERB};
use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use cowshed_os::fork_lock::Spawn as _;

/// The host is the `cowshed` binary itself, run with the hidden verb launchd's plist gives it.
/// It is read when the test runs: `env!` would bake in the path of the checkout that compiled
/// this test, but the test runs from a cached nextest archive that is relocated into every
/// checkout, and nextest re-points `CARGO_BIN_EXE_cowshed` at the extracted binary only at runtime.
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

#[test]
fn the_host_exits_with_its_childs_exit_code() {
    let status = Command::new(cowshed())
        .args([VERB, "/bin/sh", "-c", "exit 7"])
        .status_bounded()
        .unwrap();
    assert_eq!(status.code(), Some(7));
    // `/bin/sh` is the one program path every supported host has: NixOS keeps no `/usr/bin/true`.
    let status = Command::new(cowshed())
        .args([VERB, "/bin/sh", "-c", "exit 0"])
        .status_bounded()
        .unwrap();
    assert_eq!(status.code(), Some(0));
}

#[test]
fn the_host_refuses_to_run_without_a_child_and_says_why() {
    let output = Command::new(cowshed()).arg(VERB).output_bounded().unwrap();
    assert_eq!(output.status.code(), Some(EXIT_FAILED));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("needs the program to run"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// launchd stops the agent with SIGTERM to the host. The child must receive it, and the
/// host must end only after the child, and the way the child did: here the child traps it
/// and exits 42, so the host's 42 proves both the forwarding and the order.
#[test]
fn a_signal_to_the_host_reaches_the_child_and_the_host_ends_as_the_child_did() {
    let mut host = Command::new(cowshed())
        .args([
            VERB,
            "/bin/sh",
            "-c",
            "trap 'exit 42' TERM; echo ready; while :; do sleep 0.05; done",
        ])
        .stdout(Stdio::piped())
        .spawn_locked()
        .unwrap();
    let mut ready = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(host.stdout.take().unwrap()),
        &mut ready,
    )
    .unwrap();
    assert_eq!(ready, "ready\n");
    let pid = libc::pid_t::try_from(host.id()).unwrap();
    // SAFETY: `pid` is this test's own unreaped child.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    assert_eq!(host.wait_bounded().unwrap().code(), Some(42));

    // A child killed by the forwarded signal leaves the host killed by it too.
    let mut host = Command::new(cowshed())
        .args([VERB, "/bin/sh", "-c", "echo ready; exec sleep 30"])
        .stdout(Stdio::piped())
        .spawn_locked()
        .unwrap();
    let mut ready = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(host.stdout.take().unwrap()),
        &mut ready,
    )
    .unwrap();
    let pid = libc::pid_t::try_from(host.id()).unwrap();
    // SAFETY: as above.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let status = host.wait_bounded().unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGTERM)
    );
}
