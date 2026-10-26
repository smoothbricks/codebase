//! `cowshed host` as an embedding process meets it: the binary started with one end of a
//! socketpair as its standard input, and the host operations called on the other end
//! (07_api.md, "Host operations").

use cowshed_os::test_backstop::{AsyncChildBackstop as _, ChildBackstop as _};
use std::os::fd::OwnedFd;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use cowshed_core::api::call::{self, Arguments};
use cowshed_core::api::operations::{
    HostBuildState, HostCredentialAdd, HostDiskCommand, HostFdHolders, HostLockHolders,
    HostRetireDescendants, HostTreeHolders, SccacheStatusRead,
};
use cowshed_core::build_volume::{BUILD_LINK, BuildVolumeState};
use cowshed_core::capabilities::BuildStatePath;
use cowshed_core::{Host, SccacheHost};

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
        .wait_with_output_bounded()
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
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed host");
    assert!(
        output.stdout.is_empty(),
        "the host connection never writes stdout"
    );
}

/// Starts `cowshed host` under no job supervisor and connects to it.
async fn host_without_supervisor() -> (Host, tokio::process::Child) {
    let (ours, theirs) = UnixStream::pair().unwrap();
    let child = tokio::process::Command::new(cowshed())
        .arg("host")
        .env_remove("COWSHED_SUPERVISOR_SOCKET")
        .stdin(Stdio::from(OwnedFd::from(theirs)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let host = Host::connect(OwnedFd::from(ours))
        .await
        .expect("host handshake");
    (host, child)
}

/// What a helper verb prints on stdout, each line one JSON value, run under no job supervisor.
fn printed(args: &[&std::ffi::OsStr], stdin: &[u8]) -> Vec<serde_json::Value> {
    use std::io::Write as _;
    let mut verb = Command::new(cowshed())
        .args(args)
        .env_remove("COWSHED_SUPERVISOR_SOCKET")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    verb.stdin.take().unwrap().write_all(stdin).unwrap();
    let output = verb.wait_with_output_bounded().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

/// `host.lockHolders`, `host.treeHolders` and `host.retireDescendants` answer what
/// `lock-holders`, `tree-holders` and `retire-descendants` print: the lock's observation, the
/// tree's holders (here a child whose working directory is in it) as one list, and, with no
/// supervisor socket, the typed `foldUnavailable` refusal.
#[tokio::test]
async fn a_host_connection_answers_the_holder_censuses_and_retirement_as_their_verbs_print_them() {
    let root =
        temp_root::TempRoot::new_in(&temp_parent::canonical_parent(), "cowshed-host-holders");
    let lock = root.join("lock");
    std::fs::write(&lock, b"").unwrap();
    let tree = root.join("tree");
    std::fs::create_dir(&tree).unwrap();
    // A holder whose working directory is in the tree, until its stdin closes.
    let mut holder = Command::new("cat")
        .current_dir(&tree)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let handle = serde_json::json!({
        "jobId": 7,
        "index": 2,
        "incarnation": "0123456789abcdef0123456789abcdef",
    });

    let lock_printed = printed(&["lock-holders".as_ref(), lock.as_os_str()], b"");
    let tree_printed = printed(&["tree-holders".as_ref(), tree.as_os_str()], b"");
    let retire_printed = printed(
        &[
            "retire-descendants".as_ref(),
            "--grace-ms".as_ref(),
            "0".as_ref(),
        ],
        handle.to_string().as_bytes(),
    );

    let (host, child) = host_without_supervisor().await;
    let mut arguments = Arguments::new();
    arguments.insert("path".to_owned(), serde_json::json!(lock));
    let observation = call::call::<HostLockHolders, _>(&host, arguments)
        .await
        .expect("host.lockHolders");
    assert_eq!(
        vec![serde_json::to_value(&observation).unwrap()],
        lock_printed
    );

    let mut arguments = Arguments::new();
    arguments.insert("path".to_owned(), serde_json::json!(tree));
    let holders = call::call::<HostTreeHolders, _>(&host, arguments)
        .await
        .expect("host.treeHolders");
    assert_eq!(
        serde_json::to_value(&holders).unwrap(),
        serde_json::json!(tree_printed)
    );
    assert!(
        holders
            .iter()
            .any(|held| held.pid == i32::try_from(holder.id()).unwrap()),
        "{holders:?}"
    );

    let mut arguments = Arguments::new();
    arguments.insert("handle".to_owned(), handle);
    arguments.insert("graceMs".to_owned(), serde_json::json!(0));
    let retirement = call::call::<HostRetireDescendants, _>(&host, arguments)
        .await
        .expect("host.retireDescendants");
    assert_eq!(
        vec![serde_json::to_value(&retirement).unwrap()],
        retire_printed
    );
    assert_eq!(
        serde_json::to_value(&retirement).unwrap(),
        serde_json::json!({ "kind": "refused", "reason": "foldUnavailable" })
    );

    drop(host);
    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed host");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(holder.stdin.take());
    assert!(holder.wait_bounded().unwrap().success());
}

/// `host.fdHolders` answers what `fd-holders` prints: here a child handed this test's pipe as its
/// stdout and a file as its stderr, named by both descriptors in one census, which on Darwin reads
/// every process's pipes, sockets and files. Other users' processes come and go between the two
/// censuses, so the holders compare whole and `uninspected` is only present in each.
#[tokio::test]
async fn a_host_connection_answers_fd_holders_as_the_verb_prints_them() {
    use std::os::fd::AsRawFd as _;

    let root = temp_root::TempRoot::new_in(&temp_parent::canonical_parent(), "cowshed-host-fds");
    let file = std::fs::File::create(root.join("held")).unwrap();
    let (_read, write) = std::io::pipe().unwrap();
    let mut options: Vec<std::ffi::OsString> = vec!["fd-holders".into()];
    let mut arguments = Arguments::new();
    let mut named = Vec::new();
    for descriptor in [write.as_raw_fd(), file.as_raw_fd()] {
        let (kind, option, value) = match cowshed_os::process::descriptor_name(descriptor).unwrap()
        {
            cowshed_os::process::DescriptorName::Device(address) => {
                ("device", "--device", format!("0x{address:x}"))
            }
            cowshed_os::process::DescriptorName::Inode(inode) => {
                ("inode", "--inode", inode.to_string())
            }
            other => panic!("a pipe or a file is named by its address or inode, not {other:?}"),
        };
        options.extend([option.into(), value.clone().into()]);
        arguments
            .entry(format!("{kind}s"))
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!(value));
        named.push(serde_json::json!({ "kind": kind, kind: value }));
    }
    let mut holder = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(write)
        .stderr(file)
        .spawn()
        .unwrap();
    let options: Vec<&std::ffi::OsStr> =
        options.iter().map(std::ffi::OsString::as_os_str).collect();
    let printed = printed(&options, b"");

    let (host, child) = host_without_supervisor().await;
    let answered = call::call::<HostFdHolders, _>(&host, arguments)
        .await
        .expect("host.fdHolders");
    let answered = serde_json::to_value(&answered).unwrap();
    assert_eq!(answered["holders"], printed[0]["holders"]);
    assert!(printed[0]["uninspected"].is_u64(), "{}", printed[0]);
    assert!(answered["uninspected"].is_u64(), "{answered}");
    let pid = i32::try_from(holder.id()).unwrap();
    let held = answered["holders"]
        .as_array()
        .unwrap()
        .iter()
        .find(|held| held["pid"] == pid)
        .unwrap_or_else(|| panic!("the child is named: {answered}"));
    assert_eq!(held["descriptors"], serde_json::json!([1, 2]), "{answered}");
    assert_eq!(held["matches"], serde_json::json!(named), "{answered}");

    drop(host);
    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed host");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(holder.stdin.take());
    assert!(holder.wait_bounded().unwrap().success());
}

#[cfg(target_os = "macos")]
mod real_apfs_host {
    use super::*;
    use cowshed_core::api::dto::DiskCommandEvent;
    /// `host.diskCommand` runs the verb's runner: `hdiutil info`, an image verb, given a 1 ms
    /// `deadlineMs` is neither refused nor killed, and answers its whole stdout then exit 0, as
    /// `disk-command hdiutil -- info` exits.
    #[tokio::test]
    async fn a_host_disk_command_is_the_verbs_run_its_output_then_its_exit() {
        let verb = Command::new(cowshed())
            .args([
                "disk-command",
                "hdiutil",
                "--deadline-ms",
                "1",
                "--",
                "info",
            ])
            .output()
            .unwrap();
        assert_eq!(
            verb.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&verb.stderr)
        );

        let (host, child) = host_without_supervisor().await;
        let mut arguments = Arguments::new();
        arguments.insert("tool".to_owned(), serde_json::json!("hdiutil"));
        arguments.insert("deadlineMs".to_owned(), serde_json::json!(1));
        arguments.insert(
            "argv".to_owned(),
            serde_json::json!([{ "encoding": "utf8", "data": "info" }]),
        );
        let mut events = call::call_stream::<HostDiskCommand, _>(&host, arguments)
            .await
            .expect("host.diskCommand");
        let mut stdout = Vec::new();
        let mut ended = None;
        while let Some(event) = events.next().await {
            assert!(ended.is_none(), "an event after exit");
            match event.expect("a disk-command event") {
                DiskCommandEvent::Stdout { data } => stdout.extend_from_slice(data.as_bytes()),
                DiskCommandEvent::Stderr { .. } => {}
                DiskCommandEvent::Exit { code } => ended = Some(code),
            }
        }
        assert_eq!(ended, Some(0));
        assert!(!stdout.is_empty(), "hdiutil info printed nothing");

        drop(events);
        drop(host);
        let output = child
            .wait_with_output_bounded()
            .await
            .expect("wait for cowshed host");
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// `host.credentialAdd` carries `--secret-stdin`'s bytes as its upload frame and nothing else: a
/// frame beside a secret named by environment is refused as usage before any project is opened,
/// and an upload secret reaches the verb's own project discovery, which refuses a directory that
/// is no checkout. Neither touches the platform credential store.
#[tokio::test]
async fn a_host_credential_upload_is_the_stdin_secret_and_nothing_else() {
    let root =
        temp_root::TempRoot::new_in(&temp_parent::canonical_parent(), "cowshed-host-credential");
    let (host, child) = host_without_supervisor().await;
    let request = |secret: serde_json::Value| {
        let mut arguments = Arguments::new();
        arguments.insert("project".to_owned(), serde_json::json!(root.join("absent")));
        arguments.insert(
            "origin".to_owned(),
            serde_json::json!("https://registry.example.test"),
        );
        arguments.insert("pathPrefixes".to_owned(), serde_json::json!(["/acme/"]));
        arguments.insert("secret".to_owned(), secret);
        arguments
    };
    let stray = call::call_upload::<HostCredentialAdd, _>(
        &host,
        request(serde_json::json!({ "kind": "env", "name": "REGISTRY_TOKEN" })),
        Some(bytes::Bytes::from_static(b"token")),
    )
    .await
    .unwrap_err();
    assert_eq!(stray.code, cowshed_core::ErrorCode::Usage, "{stray:?}");
    assert!(stray.message.contains("upload frame"), "{stray:?}");

    let uploaded = call::call_upload::<HostCredentialAdd, _>(
        &host,
        request(serde_json::json!({ "kind": "upload" })),
        Some(bytes::Bytes::from_static(b"token")),
    )
    .await
    .unwrap_err();
    assert_eq!(
        uploaded.code,
        cowshed_core::ErrorCode::EnvironmentMissing,
        "{uploaded:?}"
    );

    drop(host);
    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed host");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Starts `cowshed <verb>` on a socketpair and answers our end, as `openHost` and
/// `openSccacheHost` start theirs (the built binary here, not the launcher's choice).
fn serving(verb: &[&str]) -> (OwnedFd, tokio::process::Child) {
    let (ours, theirs) = UnixStream::pair().unwrap();
    let child = tokio::process::Command::new(cowshed())
        .args(verb)
        .stdin(Stdio::from(OwnedFd::from(theirs)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    (OwnedFd::from(ours), child)
}

/// The sccache role serves the CLI's status outcome: installed/running state on macOS,
/// or the same typed launchctl-unavailable failure on Linux. JSON failures are on stdout.
/// Each client refuses the other role's handshake before any call.
#[tokio::test]
async fn a_sccache_host_connection_serves_the_sccache_rows_and_no_other_role() {
    let verb = Command::new(cowshed())
        .args(["--json", "sccache", "status"])
        .output()
        .unwrap();

    let printed: serde_json::Value = serde_json::from_slice(&verb.stdout).unwrap();

    let (socket, child) = serving(&["sccache", "host"]);
    let sccache = SccacheHost::connect(socket)
        .await
        .expect("sccache host handshake");
    let status = call::call::<SccacheStatusRead, _>(&sccache, Arguments::new()).await;
    #[cfg(target_os = "macos")]
    {
        assert!(verb.status.success(), "{verb:?}");
        let answered = serde_json::to_value(status.expect("sccache.status")).unwrap();
        for field in ["installed", "running", "socket"] {
            assert_eq!(
                answered[field], printed["result"][field],
                "{field}: {answered} {printed}"
            );
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        assert_eq!(verb.status.code(), Some(1), "{verb:?}");
        let refused = status.expect_err("the LaunchAgent service is unavailable on this platform");
        assert_eq!(
            refused.code,
            cowshed_core::ErrorCode::Internal,
            "{refused:?}"
        );
        assert!(
            refused
                .message
                .contains("launchctl print: launchctl could not be started"),
            "{refused:?}"
        );
        assert_eq!(
            printed,
            serde_json::json!({ "ok": false, "error": refused })
        );
    }
    drop(sccache);
    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed sccache host");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "the connection never writes stdout"
    );

    let (socket, child) = serving(&["sccache", "host"]);
    let refused = Host::connect(socket)
        .await
        .expect_err("a host client refuses sccache host");
    assert!(refused.message.contains("Sccache"), "{refused:?}");
    child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed sccache host");

    let (socket, child) = serving(&["host"]);
    let refused = SccacheHost::connect(socket)
        .await
        .expect_err("a sccache host client refuses cowshed host");
    assert!(refused.message.contains("Host"), "{refused:?}");
    child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed host");
}

/// The host and CLI refuse the same forbidden disk operation before a native tool is needed.
/// Successful native output and exit streaming belongs to the real_apfs_host lane above.
#[tokio::test]
async fn a_host_disk_command_refuses_volume_renames_as_the_verb_does() {
    let root = temp_root::TempRoot::new_in(&temp_parent::canonical_parent(), "cowshed-host-disk");
    let target = root.join("absent-volume");
    let verb = Command::new(cowshed())
        .args(["disk-command", "diskutil", "--", "renameVolume"])
        .arg(&target)
        .arg("renamed")
        .output()
        .expect("run the disk-command verb");
    assert_eq!(verb.status.code(), Some(101), "{verb:?}");
    assert!(verb.stdout.is_empty(), "{verb:?}");

    let (host, child) = host_without_supervisor().await;
    let mut arguments = Arguments::new();
    arguments.insert("tool".to_owned(), serde_json::json!("diskutil"));
    arguments.insert(
        "argv".to_owned(),
        serde_json::json!([
            { "encoding": "utf8", "data": "renameVolume" },
            { "encoding": "utf8", "data": target },
            { "encoding": "utf8", "data": "renamed" }
        ]),
    );
    let mut events = call::call_stream::<HostDiskCommand, _>(&host, arguments)
        .await
        .expect("open host.diskCommand");
    let refused = events
        .next()
        .await
        .expect("the operation answers")
        .expect_err("volume renames are refused before running diskutil");
    assert_eq!(refused.code, cowshed_core::ErrorCode::Usage, "{refused:?}");
    assert!(refused.message.contains("refused"), "{refused:?}");
    assert!(
        refused.message.contains(target.to_str().unwrap()),
        "{refused:?}"
    );
    assert_eq!(
        String::from_utf8(verb.stderr).unwrap(),
        format!("cowshed: {}\nnext: {}\n", refused.message, refused.hint)
    );
    assert!(
        events.next().await.is_none(),
        "no event follows the refusal"
    );

    drop(events);
    drop(host);
    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for cowshed host");
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(
        output.stdout.is_empty(),
        "the connection never writes stdout"
    );
}
