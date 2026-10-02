use cowshed_cli::args::parse_args;
use cowshed_cli::output::Output;
use cowshed_cli::runtime::{ActorBridge, CliService, dispatch};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::RecoveryScope;
use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
use cowshed_core::{ErrorCode, Result};
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, Gateway, GatewayConfig, KeychainCredentialProvider,
    MirrorCacheConfig, SystemConnector,
};
use serde_json::{Value, json};
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
        fs::write(
            checkout.join(".gitignore"),
            b".envrc\n.envrc-local\n.devenv/\n",
        )
        .expect("workspace hooks and evaluated profile ignore");
        git(&checkout, &["add", "tracked", ".gitignore"]);
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

    fn install_checked_land_shell(&self) {
        // The checked command runs inside the workspace sandbox, which cannot inherit CI's
        // checkout PATH. Give this scratch repository the same real, store-backed profile that
        // devenv evaluated for the test runner; its direnv binary must be usable after cloning.
        let manifest = PathBuf::from(
            std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"),
        );
        let profile = manifest
            .join("../../../../tooling/direnv/.devenv/profile")
            .canonicalize()
            .expect("evaluated repository devenv profile");
        assert!(
            profile.starts_with("/nix/store"),
            "profile must be immutable"
        );
        assert!(
            profile.join("bin/direnv").is_file(),
            "real direnv is installed"
        );
        let state = self.checkout.join(".devenv");
        fs::create_dir_all(&state).expect("workspace devenv state");
        std::os::unix::fs::symlink(profile, state.join("profile"))
            .expect("workspace profile points to real Nix shell");
    }

    fn install_inherited_shell(&self) -> PathBuf {
        let manifest = PathBuf::from(
            std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"),
        );
        let source = manifest.join("../../../../tooling/direnv");
        let directory = self.checkout.join("tooling/direnv");
        fs::create_dir_all(&directory).expect("devenv source directory");
        fs::copy(
            source.join("inherited-devenv.ts"),
            directory.join("inherited-devenv.ts"),
        )
        .expect("candidate inherited shell implementation");
        fs::write(
            directory.join("devenv.nix"),
            "{ ... }: { env.PROBE = \"host-origin\"; }\n",
        )
        .expect("devenv fixture");
        fs::write(
            directory.join("devenv.yaml"),
            "inputs:\n  nixpkgs:\n    url: github:cachix/devenv-nixpkgs/rolling\n",
        )
        .expect("devenv inputs");
        let lock: Value =
            serde_json::from_slice(&fs::read(source.join("devenv.lock")).expect("repository lock"))
                .expect("valid repository lock");
        let nodes = &lock["nodes"];
        let isolated = json!({
            "nodes": {
                "devenv": nodes["devenv"],
                "nixpkgs": nodes["nixpkgs"],
                "nixpkgs-src": nodes["nixpkgs-src"],
                "root": { "inputs": { "devenv": "devenv", "nixpkgs": "nixpkgs" } }
            },
            "root": "root",
            "version": lock["version"]
        });
        fs::write(
            directory.join("devenv.lock"),
            serde_json::to_vec(&isolated).expect("isolated lock"),
        )
        .expect("write isolated lock");
        fs::write(
            self.checkout.join(".envrc"),
            "shell=$(bun \"$PWD/tooling/direnv/inherited-devenv.ts\" \"$PWD\" direnv-export) || exit\n\
             eval \"$shell\"\n\
             local_override=\"$PWD/.envrc-local\"\n\
             source_env_if_exists \"$local_override\"\n",
        )
        .expect("direnv shell hook");
        git(&self.checkout, &["add", "-f", ".envrc"]);
        git(&self.checkout, &["add", "tooling/direnv"]);
        git(&self.checkout, &["commit", "-q", "-m", "shell fixture"]);
        let home = self._scratch.path().join("host-home");
        let keys = home.join(".local/share/devenv/cachix_trusted_keys.json");
        fs::create_dir_all(keys.parent().expect("devenv home")).expect("devenv home directory");
        fs::write(
            keys,
            "{\"devenv\":\"devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=\"}\n",
        )
        .expect("public cache key");
        let result = Command::new("bun")
            .arg(directory.join("inherited-devenv.ts"))
            .arg(&self.checkout)
            .arg("direnv-export")
            .current_dir(&directory)
            .env("HOME", &home)
            .env("PRIVATE_CREDENTIAL", "origin-only-secret-sentinel")
            .env_remove("COWSHED_PORT_BASE")
            .output()
            .expect("evaluate origin shell");
        assert!(
            result.status.success(),
            "origin evaluation: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let artifact = directory.join(".devenv/inherited-shell.json");
        let metadata = fs::metadata(&artifact).expect("published private origin artifact");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert!(
            !fs::read(&artifact)
                .expect("private artifact")
                .windows(b"origin-only-secret-sentinel".len())
                .any(|part| part == b"origin-only-secret-sentinel"),
            "origin secret cannot enter private artifact"
        );
        artifact
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
    fixture.install_checked_land_shell();
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

// The real host-origin Nix evaluation plus two APFS sandbox entries exceed nextest's 30s
// per-test limit. Run this explicit physical smoke separately without changing that limit.
#[tokio::test]
#[ignore = "run explicitly: cargo test -p cowshed-cli --test command_dispatch real_apfs_first_shell_reuses_private_origin_artifact_without_exposing_origin -- --ignored --nocapture"]
async fn real_apfs_first_shell_reuses_private_origin_artifact_without_exposing_origin() {
    let mut fixture = Fixture::new();
    let artifact = fixture.install_inherited_shell();
    let origin = fixture.checkout.display().to_string();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("install main on the isolated gateway");
    let (created, _, stderr) = run(&mut service, ["new", "fresh-shell"]).await;
    assert_eq!(
        created.unwrap_or_else(|error| panic!(
            "real APFS clone: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let clone = service
        .path("fresh-shell", false)
        .await
        .expect("mounted COW clone");
    let inherited = clone
        .mount
        .join("tooling/direnv/.devenv/inherited-shell.json");
    assert_eq!(
        fs::metadata(&inherited)
            .expect("COW-inherited origin artifact")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let (result, stdout, stderr) = run(
        &mut service,
        [
            "exec",
            "fresh-shell",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s|%s|%s\\n' \"$PROBE\" \"$DEVENV_ROOT\" \"$DEVENV_STATE\"",
        ],
    )
    .await;
    assert_eq!(
        result.unwrap_or_else(|error| panic!(
            "first sandbox shell: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0,
        "sandbox stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
    let output = String::from_utf8(stdout).expect("first-shell stdout");
    let feedback = String::from_utf8(stderr).expect("first-shell stderr");
    let shell_root = clone.mount.join("tooling/direnv");
    assert!(
        output.contains(&format!(
            "host-origin|{}|{}",
            shell_root.display(),
            shell_root.join(".devenv/state").display()
        )),
        "first shell must use clone-local identity: {output}"
    );
    assert!(
        feedback.contains("reused this checkout"),
        "shell must reuse the origin artifact: {feedback}"
    );
    assert!(
        !output.contains(&origin) && !feedback.contains(&origin),
        "shell output must not expose origin path"
    );
    assert!(
        !output.contains("origin-only-secret-sentinel")
            && !feedback.contains("origin-only-secret-sentinel"),
        "shell output must not expose origin credentials"
    );

    let poisoned = fixture._scratch.path().join("poisoned-artifact.json");
    fs::write(&poisoned, fs::read(&artifact).expect("origin artifact")).expect("poison source");
    fs::remove_file(&inherited).expect("remove COW copy");
    std::os::unix::fs::symlink(&poisoned, &inherited).expect("poisoned artifact symlink");
    let (result, stdout, stderr) = run(
        &mut service,
        [
            "exec",
            "fresh-shell",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s|%s\\n' \"$PROBE\" \"$DEVENV_ROOT\"",
        ],
    )
    .await;
    assert_eq!(
        result.unwrap_or_else(|error| panic!(
            "poisoned artifact fallback: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0,
        "fallback stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
    let fallback = String::from_utf8(stdout).expect("fallback stdout");
    let feedback = String::from_utf8(stderr).expect("fallback stderr");
    assert!(fallback.contains(&format!("host-origin|{}", shell_root.display())));
    assert!(
        !feedback.contains("reused this checkout"),
        "symlink must not be trusted"
    );
    assert!(!fallback.contains(&origin) && !feedback.contains(&origin));
    assert!(
        !fallback.contains("origin-only-secret-sentinel")
            && !feedback.contains("origin-only-secret-sentinel")
    );
    service.shutdown().await.expect("shutdown scratch runtime");
    fixture.stop_gateway().await;
}
