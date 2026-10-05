use cowshed_cli::args::parse_args;
use cowshed_cli::output::Output;
use cowshed_cli::runtime::{ActorBridge, CliService, dispatch};
use cowshed_core::apfs::{CommandRunner, DetachIntent, DiskImageSource, SystemCommandRunner};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::RecoveryScope;
use cowshed_core::storage::apfs::ApfsSubstrateConfig;
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
use cowshed_core::{ErrorCode, Result};
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, Gateway, GatewayConfig, KeychainCredentialProvider,
    MirrorCacheConfig, SystemConnector,
};
use std::ffi::OsString;
use std::fs;
use std::net::{Ipv4Addr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

#[path = "../../../cowshed-core/tests/support/scratch_apfs.rs"]
mod scratch_apfs;

use scratch_apfs::ScratchRoot;

struct Fixture {
    _scratch: ScratchRoot,
    checkout: PathBuf,
    storage: ValidatedHostStorage,
    granted: PathBuf,
    gateway: Option<Gateway>,
}

impl Fixture {
    fn new() -> Self {
        // The supervisor socket includes this root and an incarnation hash. Keep the root short
        // enough for Darwin's Unix-socket path limit; ScratchRoot's pid/sequence keep it unique.
        let scratch = ScratchRoot::new("cli").expect("scratch APFS root");
        let checkout = scratch.path().join("checkout");
        let store = scratch.path().join("store");
        let caches = scratch.path().join("caches");
        let granted = scratch.path().join("granted");
        for path in [&checkout, &store, &caches, &granted] {
            fs::create_dir_all(path).expect("fixture directory");
        }
        git(&checkout, &["init", "-q", "-b", "main"]);
        fs::write(checkout.join("tracked"), b"tracked\n").expect("tracked file");
        git(&checkout, &["add", "tracked"]);
        git(&checkout, &["commit", "-q", "-m", "initial"]);
        let storage = ValidatedHostStorage::new(
            scratch.path().to_path_buf(),
            CanonicalRoots::at(store, caches),
        );
        Self {
            _scratch: scratch,
            checkout,
            storage,
            granted,
            gateway: None,
        }
    }

    async fn open(&self) -> ActorBridge {
        ActorBridge::open_for_adopt_at(
            &self.checkout,
            Some(RepoId::parse("fixture/dispatch").expect("repository identity")),
            self.storage.clone(),
        )
        .await
        .expect("open native project runtime on scratch APFS")
    }

    async fn reopen(&self) -> ActorBridge {
        ActorBridge::open_existing_at(
            &self.checkout,
            RecoveryScope::Workspaces([WorkspaceName::main()].into()),
            self.storage.clone(),
        )
        .await
        .expect("reopen persisted project runtime")
    }
    async fn start_gateway(&mut self) {
        let cache = self.storage.caches().join("mirror");
        let audit_root = self.storage.telemetry().join("gateway");
        fs::create_dir_all(&cache).expect("gateway mirror cache");
        fs::create_dir_all(&audit_root).expect("gateway audit directory");
        for directory in [&cache, &audit_root] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
                .expect("private gateway directory");
        }
        let config = GatewayConfig {
            control_socket: Some(self.storage.store().join("gateway.sock")),
            mirror_cache: MirrorCacheConfig::new(cache),
            ..GatewayConfig::default()
        };
        let connector =
            SystemConnector::new(config.timeouts.connect, config.timeouts.tls_handshake)
                .expect("production upstream connector");
        let audit = ArrowAuditSink::start(ArrowAuditConfig::new(audit_root).expect("audit config"))
            .expect("durable gateway audit");
        self.gateway = Some(
            Gateway::start(
                config,
                Arc::new(KeychainCredentialProvider::new()),
                Arc::new(connector),
                Arc::new(audit),
            )
            .await
            .expect("real gateway on scratch socket"),
        );
    }

    async fn stop_gateway(&mut self) {
        if let Some(gateway) = self.gateway.take() {
            gateway.drain().await.expect("drain real gateway");
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // During a failed assertion the fixture still owns its scratch root. Force-stop the real
        // gateway first, before ScratchRoot detaches images and removes its control socket.
        self.gateway.take();
    }
}

fn git(directory: &Path, args: &[&str]) {
    let verb = args.first().copied().unwrap_or("empty");
    eprintln!("dispatch fixture git: {verb} start");
    let started = std::time::Instant::now();
    let output = Command::new("git")
        .args(args)
        .current_dir(directory)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .output()
        .expect("git process");
    eprintln!(
        "dispatch fixture git: {verb} done elapsed={:?} status={}",
        started.elapsed(),
        output.status
    );
    assert!(
        output.status.success(),
        "git {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn run(
    service: &mut ActorBridge,
    args: impl IntoIterator<Item = impl Into<OsString>>,
) -> (Result<i32>, Vec<u8>, Vec<u8>) {
    let cli = parse_args(args).expect("valid CLI command");
    let mut output = Output::new(Vec::new(), Vec::new(), cli.global.quiet);
    let result = dispatch(service, cli, tokio::io::empty(), &mut output)
        .await
        .map(|exit| exit.code);
    let (stdout, stderr) = output.into_inner();
    (result, stdout, stderr)
}

async fn adopt(fixture: &Fixture, service: &mut ActorBridge) {
    let (result, stdout, stderr) = run(
        service,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--repo-id"),
            OsString::from("fixture/dispatch"),
            OsString::from("--capacity"),
            OsString::from("1g"),
        ],
    )
    .await;
    assert_eq!(result.expect("real APFS adoption"), 0);
    assert!(!stdout.is_empty(), "adoption reports its mounted checkout");
    assert!(
        !stderr.is_empty(),
        "adoption reports the gateway's actual availability"
    );
}

#[tokio::test]
async fn real_apfs_dispatch_grant_persists_across_runtime_restart() {
    let fixture = Fixture::new();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let path = fixture.granted.as_os_str().to_os_string();
    let (result, _, _) = run(
        &mut service,
        [
            OsString::from("grant"),
            OsString::from("main"),
            OsString::from("--read"),
            path.clone(),
            OsString::from("--write"),
            path,
        ],
    )
    .await;
    assert_eq!(result.expect("grant through real coordinator"), 0);
    let (result, _, stderr) =
        run(&mut service, ["grant", "main", "--deny-write", "generated"]).await;
    assert_eq!(result.expect("persist workspace deny-write"), 0);
    assert!(
        String::from_utf8(stderr)
            .expect("grant summary is UTF-8")
            .contains("1 denied writes"),
        "the summary must report the deny recorded by the real grant store"
    );
    service
        .shutdown()
        .await
        .expect("stop runtime before reopen");

    let mut service = fixture.reopen().await;
    let (result, stdout, _) = run(&mut service, ["grant", "main"]).await;
    assert_eq!(result.expect("list persisted grants"), 0);
    let listing = String::from_utf8(stdout).expect("grants are UTF-8");
    assert!(listing.contains(&fixture.granted.display().to_string()));
    let grants = service.grants("main").await.expect("durable grant store");
    assert!(grants.read.contains(&fixture.granted));
    assert!(grants.write.contains(&fixture.granted));
    assert!(grants.deny_write.contains(&PathBuf::from("generated")));
    service.shutdown().await.expect("shutdown runtime");
}

#[tokio::test]
async fn real_apfs_dispatch_denied_grant_does_not_change_persisted_grants() {
    let fixture = Fixture::new();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let before = service.grants("main").await.expect("baseline grants");
    let (result, stdout, _) = run(
        &mut service,
        [
            OsString::from("grant"),
            OsString::from("main"),
            OsString::from("--read"),
            fixture.storage.store().as_os_str().to_os_string(),
        ],
    )
    .await;
    let error = result.expect_err("store itself cannot be granted to a workspace");
    assert_eq!(error.code, ErrorCode::SandboxDenied);
    assert!(stdout.is_empty(), "a refusal has no machine answer");
    service.shutdown().await.expect("shutdown runtime");
    let mut service = fixture.reopen().await;
    assert_eq!(
        service.grants("main").await.expect("persisted grants"),
        before
    );
    service.shutdown().await.expect("shutdown reopened runtime");
}

#[tokio::test]
async fn real_apfs_dispatch_project_deny_write_is_persisted_not_just_echoed() {
    let fixture = Fixture::new();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let (result, _, _) = run(
        &mut service,
        [
            OsString::from("grant"),
            OsString::from("--project-wide"),
            OsString::from("--deny-write"),
            OsString::from("generated"),
        ],
    )
    .await;
    assert_eq!(result.expect("record deny-write in project grant store"), 0);
    service.shutdown().await.expect("shutdown runtime");
    let mut service = fixture.reopen().await;
    let project = service
        .project_grants()
        .await
        .expect("durable project policy");
    assert!(project.deny_write.contains(&PathBuf::from("generated")));
    service.shutdown().await.expect("shutdown reopened runtime");
}

#[tokio::test]
async fn real_apfs_dispatch_reallocates_a_raced_port_and_reconciles_gateway_grants() {
    let mut fixture = Fixture::new();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let original_port = service
        .grants("main")
        .await
        .expect("original durable block")
        .port_block
        .expect("allocated macOS block")
        .base();
    // Take the port after allocation and image publication, before gateway installation. This
    // cannot be solved by probing in the allocator: the listener must trigger a real bind refusal.
    let competing_listener =
        TcpListener::bind((Ipv4Addr::LOCALHOST, original_port)).expect("claim published port");
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("install main from real inventory");
    let reallocated_port = service
        .grants("main")
        .await
        .expect("reallocated durable block")
        .port_block
        .expect("replacement macOS block")
        .base();
    assert_ne!(reallocated_port, original_port);
    let gateway = fixture
        .gateway
        .as_ref()
        .expect("running real gateway")
        .handle();
    let before = gateway.status().await.expect("initial gateway status");
    assert_eq!(before.sessions.len(), 1);
    assert_eq!(
        before.sessions[0].endpoint,
        format!("127.0.0.1:{reallocated_port}")
    );

    let (result, _, _) = run(
        &mut service,
        ["grant", "main", "--egress", "registry.example.test"],
    )
    .await;
    assert_eq!(
        result.expect("persist egress grant through real service"),
        0
    );
    service
        .reconcile_gateway()
        .await
        .expect("install updated grant snapshot");
    let after = gateway.status().await.expect("updated gateway status");
    assert_eq!(after.sessions.len(), 1);
    assert!(
        after.sessions[0].revision > before.sessions[0].revision,
        "gateway must replace the old session with the persisted grant revision"
    );
    service.shutdown().await.expect("shutdown runtime");
    fixture.stop_gateway().await;
    drop(competing_listener);
}

#[tokio::test]
async fn real_apfs_checked_land_preserves_target_when_gateway_absent_then_fast_forwards() {
    let started = std::time::Instant::now();
    let phase = |name| {
        eprintln!(
            "checked-land fixture: {name} elapsed={:?}",
            started.elapsed()
        );
    };
    phase("fixture");
    let mut fixture = Fixture::new();
    phase("open");
    let mut service = fixture.open().await;
    phase("adopt");
    adopt(&fixture, &mut service).await;
    phase("start gateway");
    fixture.start_gateway().await;
    phase("reconcile gateway");
    service
        .reconcile_gateway()
        .await
        .expect("install main gateway session");
    phase("new topic");
    let (created, _, _) = run(&mut service, ["new", "topic"]).await;
    assert_eq!(created.expect("create real APFS topic"), 0);
    phase("commit topic");
    let topic = service.path("topic", false).await.expect("mounted topic");
    fs::write(topic.mount.join("feature.txt"), b"feature\n").expect("topic change");
    git(&topic.mount, &["add", "feature.txt"]);
    git(&topic.mount, &["commit", "-q", "-m", "feature"]);

    phase("stop gateway");
    fixture.stop_gateway().await;
    phase("refused land");
    let (refused, stdout, _) = run(
        &mut service,
        [
            "land",
            "topic",
            "--no-retire",
            "--check",
            "test -f feature.txt",
        ],
    )
    .await;
    assert_eq!(
        refused
            .expect_err("checked land requires a live gateway")
            .code,
        ErrorCode::EnvironmentMissing
    );
    assert!(stdout.is_empty(), "failed preflight has no machine answer");
    assert!(!fixture.checkout.join("feature.txt").exists());

    phase("restart gateway");
    fixture.start_gateway().await;
    phase("checked land");
    let (landed, _, stderr) = run(
        &mut service,
        [
            "land",
            "topic",
            "--no-retire",
            "--check",
            "test -f feature.txt",
        ],
    )
    .await;
    let code = landed.unwrap_or_else(|error| {
        panic!(
            "checked land through real service failed: {error}; stderr: {}",
            String::from_utf8_lossy(&stderr)
        )
    });
    assert_eq!(code, 0);
    assert_eq!(
        fs::read(fixture.checkout.join("feature.txt")).expect("fast-forwarded main worktree"),
        b"feature\n"
    );
    phase("shutdown runtime");
    service.shutdown().await.expect("shutdown runtime");
    phase("drain gateway");
    fixture.stop_gateway().await;
    phase("done");
}

#[tokio::test]
async fn real_apfs_plain_git_repository_adopts_clones_executes_and_lands_without_shell_hooks() {
    let mut fixture = Fixture::new();
    for convention in [
        ".envrc",
        ".envrc-local",
        "flake.nix",
        "devenv.nix",
        ".cowshed.toml",
    ] {
        assert!(!fixture.checkout.join(convention).exists());
    }
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("serve plain repository");
    let (created, _, stderr) = run(&mut service, ["new", "plain-topic"]).await;
    assert_eq!(
        created.unwrap_or_else(|error| panic!(
            "plain clone: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let topic = service
        .path("plain-topic", false)
        .await
        .expect("plain clone mount");
    let (executed, stdout, stderr) = run(
        &mut service,
        [
            "exec",
            "plain-topic",
            "--",
            "/bin/sh",
            "-c",
            "test ! -e .envrc && test ! -e .envrc-local && test ! -e .gitignore && test ! -e flake.nix && test ! -e devenv.nix && printf 'generic\\n' > delivered.txt && printf 'plain-sandbox\\n'",
        ],
    )
    .await;
    assert_eq!(
        executed.unwrap_or_else(|error| panic!(
            "plain exec: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    assert_eq!(stdout, b"plain-sandbox\n");
    git(&topic.mount, &["add", "delivered.txt"]);
    git(
        &topic.mount,
        &["commit", "-qm", "deliver generic repository change"],
    );
    let (landed, _, stderr) = run(
        &mut service,
        [
            "land",
            "plain-topic",
            "--no-retire",
            "--check",
            "test -f delivered.txt",
        ],
    )
    .await;
    assert_eq!(
        landed.unwrap_or_else(|error| panic!(
            "plain checked land: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    assert_eq!(
        fs::read(fixture.checkout.join("delivered.txt")).unwrap(),
        b"generic\n"
    );
    for directory in [&fixture.checkout, &topic.mount] {
        assert!(
            !directory.join(".envrc").exists(),
            "adopt/new/exec/land never creates a shell hook"
        );
        assert!(!directory.join(".envrc-local").exists());
        assert!(!directory.join(".gitignore").exists());
    }
    service
        .shutdown()
        .await
        .expect("stop plain repository runtime");
    fixture.stop_gateway().await;
}

/// The repository's own Nx and the node that runs it, as absolute paths resolved at run time:
/// `env!` would compile this checkout's path into the test binary.
fn repository_nx() -> (PathBuf, PathBuf) {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    let nx = PathBuf::from(manifest)
        .join("../../../../node_modules/nx")
        .canonicalize()
        .expect("the repository's own Nx is installed");
    let node = std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|directory| directory.join("node"))
                .find(|candidate| candidate.is_file())
        })
        .expect("node on the test's PATH")
        .canonicalize()
        .expect("node resolves");
    (nx, node)
}

/// An Nx project whose `a:build` writes `a/generated.txt` and whose `a:test` reads it (its
/// default inputs include it). `declared` says whether `a:build` declares that output.
fn write_nx_project(checkout: &Path, nx: &Path, node: &Path, declared: bool) {
    let node = node.display();
    let outputs = if declared {
        r#"["{projectRoot}/generated.txt"]"#
    } else {
        "[]"
    };
    fs::write(
        checkout.join("package.json"),
        r#"{"name":"fixture","private":true}"#,
    )
    .unwrap();
    fs::write(checkout.join("nx.json"), r#"{"useDaemonProcess":false}"#).unwrap();
    fs::write(checkout.join(".gitignore"), "node_modules\n.nx\n").unwrap();
    fs::create_dir_all(checkout.join("a")).unwrap();
    fs::write(checkout.join("a/src.txt"), b"src\n").unwrap();
    fs::write(
        checkout.join("a/project.json"),
        format!(
            r#"{{"name":"a","targets":{{
  "build":{{"executor":"nx:run-commands","cache":true,"inputs":["{{projectRoot}}/src.txt"],"outputs":{outputs},
    "options":{{"command":"{node} -e \"require('fs').writeFileSync('a/generated.txt','generated\\n')\""}}}},
  "test":{{"executor":"nx:run-commands","cache":true,"inputs":["default"],"dependsOn":["build"],"outputs":[],
    "options":{{"command":"{node} -e \"require('fs').readFileSync('a/generated.txt')\""}}}}}}}}"#
        ),
    )
    .unwrap();
    fs::create_dir_all(checkout.join("node_modules")).unwrap();
    std::os::unix::fs::symlink(nx, checkout.join("node_modules/nx")).unwrap();
    git(checkout, &["add", "-A"]);
    git(checkout, &["commit", "-q", "-m", "nx project"]);
}

/// Every `landAdoption` commitment sealed under `telemetry`.
fn land_adoptions(telemetry: &Path) -> Vec<cowshed_core::api::dto::LandAdoptionCommitment> {
    let mut found = Vec::new();
    let mut pending = vec![telemetry.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if path.is_dir() {
                pending.push(path);
            } else if name.starts_with("commitment-") && name.ends_with(".arrow") {
                let commitment = cowshed_core::storage::job_artifact::decode_controller_commitment(
                    &fs::read(&path).unwrap(),
                )
                .unwrap();
                if let cowshed_core::api::dto::ControllerCommitment::LandAdoption(adoption) =
                    commitment
                {
                    found.push(adoption);
                }
            }
        }
    }
    found
}

/// A land whose checks build `a` and then test it, from a topic forked off main: the build's
/// output reaches the test in the topic, where the build wrote it. In main, after adoption, the
/// build hits the cache and restores only what it declares. Answers the `landAdoption`
/// commitments the land sealed and the topic's landed head.
async fn land_nx_project(
    declared: bool,
) -> (Vec<cowshed_core::api::dto::LandAdoptionCommitment>, String) {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::new();
    write_nx_project(&fixture.checkout, &nx, &node, declared);
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let links = nx
        .ancestors()
        .find(|ancestor| ancestor.file_name().is_some_and(|name| name == "links"))
        .unwrap_or(&nx)
        .to_path_buf();
    let (granted, _, stderr) = run(
        &mut service,
        [
            OsString::from("grant"),
            OsString::from("--project-wide"),
            OsString::from("--read"),
            links.into_os_string(),
            OsString::from("--read"),
            OsString::from("/nix/store"),
        ],
    )
    .await;
    assert_eq!(
        granted.unwrap_or_else(|error| panic!(
            "grant Nx: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("serve the project");
    let (created, _, stderr) = run(&mut service, ["new", "topic"]).await;
    assert_eq!(
        created.unwrap_or_else(|error| panic!(
            "new topic: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let topic = service.path("topic", false).await.expect("mounted topic");
    fs::write(topic.mount.join("a/src.txt"), b"src, changed\n").unwrap();
    git(&topic.mount, &["commit", "-q", "-am", "change a"]);
    let landed_head = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&topic.mount)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    let check = |target: &str| {
        format!(
            "{} node_modules/nx/dist/bin/nx.js run-many -t {target}",
            node.display()
        )
    };
    let (landed, _, stderr) = run(
        &mut service,
        [
            "land".to_owned(),
            "topic".to_owned(),
            // The undeclared output is untracked work in the topic, which a retire refuses.
            "--no-retire".to_owned(),
            "--check".to_owned(),
            check("build"),
            "--check".to_owned(),
            check("test"),
        ],
    )
    .await;
    let stderr = String::from_utf8_lossy(&stderr).into_owned();
    assert_eq!(
        landed.unwrap_or_else(|error| panic!("land: {error}; {stderr}")),
        0,
        "{stderr}"
    );
    eprintln!("land report: {stderr}");
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
    (land_adoptions(fixture.storage.telemetry()), landed_head)
}

/// A task output Nx is not told about is a 2b miss (16_build_volumes.md, Land step 7), and each
/// miss is a durable `landAdoption` commitment naming the task (13_telemetry.md). Declaring the
/// output makes the miss, and the record, disappear.
#[tokio::test]
async fn real_apfs_an_undeclared_nx_output_lands_as_a_land_adoption_commitment() {
    let (adoptions, landed_head) = land_nx_project(false).await;
    assert_eq!(
        adoptions
            .iter()
            .map(|adoption| (adoption.task.as_str(), adoption.landed_head.as_str()))
            .collect::<Vec<_>>(),
        [("a:test", landed_head.as_str())],
        "{adoptions:?}"
    );
    assert_ne!(
        adoptions[0].landing_incarnation,
        adoptions[0].target_incarnation
    );
    assert!(!adoptions[0].task_hash.is_empty());

    let (adoptions, _) = land_nx_project(true).await;
    assert_eq!(adoptions, [], "a declared output restores in main and hits");
}
