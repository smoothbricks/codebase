//! `cowshed host` as an embedding process meets it: the binary started with one end of a
//! socketpair as its standard input, and the host operations called on the other end
//! (07_api.md, "Host operations").

use std::os::fd::OwnedFd;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use cowshed_core::Host;
use cowshed_core::api::call::{self, Arguments};
use cowshed_core::api::operations::HostBuildState;
use cowshed_core::build_volume::{BUILD_LINK, BuildVolumeState};
use cowshed_core::capabilities::BuildStatePath;

/// The `cowshed` binary under test, read when the test runs (see tests/controller.rs).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

#[path = "../../cowshed-core/tests/support/temp_parent.rs"]
mod temp_parent;
#[path = "../../cowshed-core/tests/support/temp_root.rs"]
mod temp_root;

/// A checkout with a recorded build volume, inside one private test root removed on every exit.
fn checkout() -> (temp_root::TempRoot, PathBuf) {
    let root = temp_root::TempRoot::new_in(&temp_parent::canonical_parent(), "cowshed-host");
    let checkout = root.join("checkout");
    let volume = root.join("volume");
    std::fs::create_dir_all(checkout.join(".cowshed")).unwrap();
    std::fs::create_dir(checkout.join(".git")).unwrap();
    std::fs::create_dir(&volume).unwrap();
    symlink(&volume, checkout.join(BUILD_LINK)).unwrap();
    BuildVolumeState {
        paths: vec![BuildStatePath::new(".nx/cache", "nx/cache").unwrap()],
        fingerprint: None,
    }
    .write(&volume)
    .unwrap();
    (root, checkout)
}

/// The host connection answers `host.buildState` with exactly what `cowshed build-state --json`
/// prints, writes nothing on stdout, and ends with exit 0 when the embedder closes its end.
#[tokio::test]
async fn a_host_connection_answers_what_the_verb_computes_and_ends_with_its_peer() {
    let (_fixture, checkout) = checkout();
    let verb = Command::new(cowshed())
        .args(["--json", "--project"])
        .arg(&checkout)
        .arg("build-state")
        .output()
        .unwrap();
    assert!(
        verb.status.success(),
        "{}",
        String::from_utf8_lossy(&verb.stderr)
    );
    let printed: serde_json::Value = serde_json::from_slice(&verb.stdout).unwrap();

    let (ours, theirs) = UnixStream::pair().unwrap();
    let child = tokio::process::Command::new(cowshed())
        .arg("host")
        .stdin(Stdio::from(OwnedFd::from(theirs)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let host = Host::connect(OwnedFd::from(ours))
        .await
        .expect("host handshake");
    let mut arguments = Arguments::new();
    arguments.insert("project".to_owned(), serde_json::json!(checkout));
    let answered = call::call::<HostBuildState, _>(&host, arguments)
        .await
        .expect("host.buildState");
    assert_eq!(serde_json::to_value(&answered).unwrap(), printed);

    drop(host);
    let output = child
        .wait_with_output()
        .await
        .expect("wait for cowshed host");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "the host connection never writes stdout"
    );
}

/// A project client never mistakes a host connection for a controller: `cowshed host`'s handshake
/// answer names no project, so `Cowshed::connect` refuses it, and the host process still ends with
/// its peer.
#[tokio::test]
async fn a_project_client_refuses_a_host_connection_handshake() {
    let (ours, theirs) = UnixStream::pair().unwrap();
    let child = tokio::process::Command::new(cowshed())
        .arg("host")
        .stdin(Stdio::from(OwnedFd::from(theirs)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let Err(refused) = cowshed_core::Cowshed::connect(OwnedFd::from(ours)).await else {
        panic!("a project client accepted a host connection");
    };
    assert!(
        refused.message.contains("handshake response is invalid"),
        "{refused:?}"
    );
    let output = child
        .wait_with_output()
        .await
        .expect("wait for cowshed host");
    assert!(
        output.stdout.is_empty(),
        "the host connection never writes stdout"
    );
}
