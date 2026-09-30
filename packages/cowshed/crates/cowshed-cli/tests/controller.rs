//! `cowshed controller` as an embedding process meets it: the binary started with one end of a
//! socketpair as its standard input, and the controller protocol spoken on the other end.

use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use cowshed_core::Cowshed;
use cowshed_core::metadata::WorkspaceRole;

/// A terminal or a pipe on standard input is refused as a usage error before anything else: the
/// project named here does not exist, and the refusal is still about standard input.
#[test]
fn a_standard_input_that_is_not_a_socket_is_refused_before_the_project_is_resolved() {
    let output = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .args([
            "--json",
            "--project",
            "/nonexistent/cowshed-controller-probe",
            "controller",
        ])
        .stdin(Stdio::null())
        .output()
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
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_cowshed"))
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

    let output = tokio::time::timeout(Duration::from_secs(60), child.wait_with_output())
        .await
        .expect("the controller ends once the embedder closes its end")
        .expect("wait for cowshed controller");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
}

/// The checkout this test runs in: main's, or a workspace's, either of which names the project.
fn checkout_root() -> PathBuf {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git rev-parse");
    assert!(output.status.success(), "not inside a git checkout");
    PathBuf::from(String::from_utf8(output.stdout).expect("utf-8 path").trim())
}
