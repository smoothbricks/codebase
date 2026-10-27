//! `cowshed controller` as an embedding process meets it: the binary started with one end of a
//! socketpair as its standard input, and the controller protocol spoken on the other end.

use cowshed_os::test_backstop::{AsyncChildBackstop as _, CommandBackstop as _};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use cowshed_core::Cowshed;
use cowshed_core::metadata::WorkspaceRole;

/// The `cowshed` binary under test, read when the test runs. `env!` would bake in the path of
/// the checkout that compiled this test, but the test runs from a cached nextest archive
/// that is relocated into every checkout, and nextest re-points `CARGO_BIN_EXE_cowshed` at the
/// extracted binary only at runtime.
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A terminal or a pipe on standard input is refused as a usage error before anything else: the
/// project named here does not exist, and the refusal is still about standard input.
#[test]
fn a_standard_input_that_is_not_a_socket_is_refused_before_the_project_is_resolved() {
    let output = Command::new(cowshed())
        .args([
            "--json",
            "--project",
            "/nonexistent/cowshed-controller-probe",
            "controller",
        ])
        .stdin(Stdio::null())
        .output_bounded()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["ok"], false);
    assert_eq!(envelope["error"]["code"], "usage");
    let message = envelope["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("standard input, which is not a socket"),
        "{message}"
    );
    assert!(
        envelope["error"]["hint"]
            .as_str()
            .unwrap()
            .contains("socketpair"),
        "{envelope}"
    );
}

/// The whole contract against the real host: the embedder's end of the socketpair completes the
/// coordinator handshake with the child, opens the project the child serves and reads its main
/// workspace, and closing that end ends the child with exit 0 and nothing on stdout.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_an_embedding_process_reaches_its_project_through_the_controller_verb() {
    let root = checkout_root();
    let (ours, theirs) = UnixStream::pair().expect("socketpair");
    let child = tokio::process::Command::new(cowshed())
        .arg("--project")
        .arg(&root)
        .arg("controller")
        .stdin(Stdio::from(OwnedFd::from(theirs)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start cowshed controller");

    let (cowshed, token) = Cowshed::connect(OwnedFd::from(ours))
        .await
        .expect("coordinator handshake over the inherited socket");
    let project = cowshed.open(&root).await.expect("open the served project");
    let main = project.main().await.expect("the project's main workspace");
    assert_eq!(main.name().as_str(), "main");
    assert_eq!(main.info().role, WorkspaceRole::Main);
    drop(main);
    drop(project);
    drop(token);
    drop(cowshed);

    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed controller");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
}

/// The incident the SILENT rule was changed for, against the real host: a client whose call on a
/// `cowshed controller` fell SILENT keeps its connection, so the controller lives on. The child is
/// stopped (`SIGSTOP`) while a call waits on it, the call fails SILENT, the child is continued, and
/// the next call on the same connection is answered by the same, still running controller. A
/// client that closed its end on SILENT had made the controller exit 0 instead (06_cli.md).
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_controller_survives_a_client_whose_call_on_it_fell_silent() {
    let root = checkout_root();
    let (ours, theirs) = UnixStream::pair().expect("socketpair");
    let mut child = tokio::process::Command::new(cowshed())
        .arg("--project")
        .arg(&root)
        .arg("controller")
        .stdin(Stdio::from(OwnedFd::from(theirs)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start cowshed controller");
    let pid = libc::pid_t::try_from(child.id().expect("the controller runs")).expect("pid fits");

    let (cowshed, token) = Cowshed::connect(OwnedFd::from(ours))
        .await
        .expect("coordinator handshake over the inherited socket");
    let project = cowshed.open(&root).await.expect("open the served project");
    // SAFETY: `pid` is this test's own unreaped child.
    assert_eq!(
        unsafe { libc::kill(pid, libc::SIGSTOP) },
        0,
        "stop the controller"
    );
    let silent = project.main().await;
    // SAFETY: as above.
    assert_eq!(
        unsafe { libc::kill(pid, libc::SIGCONT) },
        0,
        "continue the controller"
    );
    let Err(silent) = silent else {
        panic!("a stopped controller's call fails");
    };
    assert!(silent.message.starts_with("SILENT: "), "{}", silent.message);
    let main = project
        .main()
        .await
        .expect("the next call on the same connection is answered");
    assert_eq!(main.name().as_str(), "main");
    assert!(
        child.try_wait().expect("poll the controller").is_none(),
        "the controller lives on after its client's call fell SILENT"
    );
    drop(main);
    drop(project);
    drop(token);
    drop(cowshed);
    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed controller");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The checkout this test runs in: main's, or a workspace's, either of which names the project.
/// The crate directory is read at run time, so a workspace that inherited main's compiled test
/// asks git about its own tree, not main's.
fn checkout_root() -> PathBuf {
    let crate_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR for the test process");
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(crate_dir)
        .output_bounded()
        .expect("git rev-parse");
    assert!(output.status.success(), "not inside a git checkout");
    PathBuf::from(String::from_utf8(output.stdout).expect("utf-8 path").trim())
}
