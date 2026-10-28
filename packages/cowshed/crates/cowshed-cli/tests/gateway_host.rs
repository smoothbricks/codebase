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

/// A host started in a process group of its own, with its child, which the host keeps in that
/// group. Dropped before the host was reaped (a failed assertion, the runner's bound, a cancelled
/// gate), it kills the whole group, so no child outlives the test: an orphaned child of this test
/// once ran for 79 hours. A reaped host's group is empty and its id may be reused, so it is left.
struct HostGroup {
    host: std::process::Child,
    reaped: bool,
}

impl HostGroup {
    /// The host running `/bin/sh -c script`, once the script has printed its `ready` line.
    fn ready(script: &str) -> Self {
        use std::os::unix::process::CommandExt as _;
        let mut host = Command::new(cowshed())
            .args([VERB, "/bin/sh", "-c", script])
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let stdout = host.stdout.take().unwrap();
        let group = Self {
            host,
            reaped: false,
        };
        let mut ready = String::new();
        std::io::BufRead::read_line(&mut std::io::BufReader::new(stdout), &mut ready).unwrap();
        assert_eq!(ready, "ready\n");
        group
    }

    fn signal(&self, signal: libc::c_int) {
        let pid = libc::pid_t::try_from(self.host.id()).unwrap();
        // SAFETY: `pid` is this test's own unreaped child.
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
    }

    fn wait(&mut self) -> std::process::ExitStatus {
        let status = self.host.wait_bounded().unwrap();
        self.reaped = true;
        status
    }
}

impl Drop for HostGroup {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        let group = libc::pid_t::try_from(self.host.id()).unwrap();
        // SAFETY: kill takes plain integers; the group is this test's own and its leader unreaped.
        unsafe { libc::kill(-group, libc::SIGKILL) };
        let _ = self.host.wait_bounded();
    }
}

/// launchd stops the agent with SIGTERM to the host. The child must receive it, and the
/// host must end only after the child, and the way the child did: here the child traps it
/// and exits 42, so the host's 42 proves both the forwarding and the order.
#[test]
fn a_signal_to_the_host_reaches_the_child_and_the_host_ends_as_the_child_did() {
    let mut host =
        HostGroup::ready("trap 'exit 42' TERM; echo ready; while :; do sleep 0.05; done");
    host.signal(libc::SIGTERM);
    assert_eq!(host.wait().code(), Some(42));

    // A child killed by the forwarded signal leaves the host killed by it too.
    let mut host = HostGroup::ready("echo ready; exec sleep 30");
    host.signal(libc::SIGTERM);
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&host.wait()),
        Some(libc::SIGTERM)
    );
}
