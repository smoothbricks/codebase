use cowshed_cli::args::parse_args;
use cowshed_cli::output::Output;
use cowshed_cli::runtime::{ActorBridge, CliService, dispatch};
use cowshed_core::apfs::{
    ApfsBackend, CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend,
    SystemCommandRunner,
};
use cowshed_core::api::JsonEnvelope;
use cowshed_core::api::dto::{Adoption, AdoptionSkip, DatabaseHolder, LandReport};
use cowshed_core::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, link, nx,
};
use cowshed_core::metadata::{ImageCapacity, PortBlock, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::RecoveryScope;
use cowshed_core::storage::apfs::ApfsSubstrateConfig;
use cowshed_core::storage::apfs::native::{
    MacOsApfsExecutionHost, blank_template, blank_template_path,
};
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
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

#[path = "../../../cowshed-core/tests/support/blank_image.rs"]
mod blank_image;
#[path = "../../../cowshed-core/tests/support/scratch_apfs.rs"]
mod scratch_apfs;

use scratch_apfs::ScratchRoot;

/// The checkout's whole `.cowshed.toml`: the build-volume cap, nothing else.
const FIXTURE_COWSHED_TOML: &str = "[build]\ncapacity = \"1g\"\n";

struct Fixture {
    _scratch: ScratchRoot,
    checkout: PathBuf,
    storage: ValidatedHostStorage,
    granted: PathBuf,
    gateway: Option<Gateway>,
}

impl Fixture {
    fn new() -> Self {
        Self::with(Project::Plain)
    }

    /// A fixture whose checkout holds `project`, committed: an APFS clone of this run's checkout
    /// template for it ([`checkout_template`]).
    fn with(project: Project<'_>) -> Self {
        // The supervisor socket includes this root and an incarnation hash. Keep the root short
        // enough for Darwin's Unix-socket path limit; ScratchRoot's pid/sequence keep it unique.
        let scratch = ScratchRoot::new("cli").expect("scratch APFS root");
        let checkout = scratch.path().join("checkout");
        let store = scratch.path().join("store");
        let caches = scratch.path().join("caches");
        let granted = scratch.path().join("granted");
        for path in [&store, &caches, &granted] {
            fs::create_dir_all(path).expect("fixture directory");
        }
        // Main and build volumes are minted from the store's blank template: the run's, seeded
        // here, at the 1 GiB test cap both `adopt --capacity` and `.cowshed.toml` ask for.
        blank_image::blank_image(&blank_template_path(&store, blank_image::CAPACITY));
        clone_tree(&checkout_template(project), &checkout);
        let storage = ValidatedHostStorage::new(
            scratch.path().to_path_buf(),
            CanonicalRoots::at(store, caches),
        );
        if let Project::Build {
            rust: Some(rust), ..
        } = project
        {
            install_rust(storage.home(), rust);
        }
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

/// What a fixture's checkout holds, committed, when its test starts.
#[derive(Clone, Copy)]
enum Project<'a> {
    /// `tracked` and the build-volume cap in `.cowshed.toml`, in one commit.
    Plain,
    /// [`Project::Plain`] and then [`write_nx_project`]'s project, its output `declared` or not.
    Nx {
        nx: &'a Path,
        node: &'a Path,
        declared: bool,
    },
    /// [`Project::Plain`] and then the build-volume tests' tree ([`write_build_project`]).
    Build {
        nx: Option<(&'a Path, &'a Path)>,
        rust: Option<&'a Path>,
    },
}

impl Project<'_> {
    /// The template's name: everything that varies between checkouts of one run. The toolchain
    /// paths are the run's own and the same in every test.
    fn key(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Nx { declared: true, .. } => "nx-declared",
            Self::Nx {
                declared: false, ..
            } => "nx-undeclared",
            Self::Build {
                nx: Some(_),
                rust: Some(_),
            } => "build-nx-rust",
            Self::Build {
                nx: Some(_),
                rust: None,
            } => "build-nx",
            Self::Build {
                nx: None,
                rust: Some(_),
            } => "build-rust",
            Self::Build {
                nx: None,
                rust: None,
            } => "build",
        }
    }

    fn write(self, checkout: &Path) {
        fs::create_dir_all(checkout).expect("checkout directory");
        git(checkout, &["init", "-q", "-b", "main"]);
        fs::write(checkout.join("tracked"), b"tracked\n").expect("tracked file");
        // Integration images stay at the 1 GiB test cap (08_testing.md): main through `adopt
        // --capacity`, the build volume through the operator's own `.cowshed.toml` setting.
        fs::write(checkout.join(".cowshed.toml"), FIXTURE_COWSHED_TOML).expect("cowshed config");
        git(checkout, &["add", "tracked", ".cowshed.toml"]);
        git(checkout, &["commit", "-q", "-m", "initial"]);
        match self {
            Self::Plain => {}
            Self::Nx { nx, node, declared } => {
                write_nx_project(checkout, nx, node, declared);
                git(checkout, &["add", "-A"]);
                git(checkout, &["commit", "-q", "-m", "nx project"]);
            }
            Self::Build { nx, rust } => write_build_project(checkout, nx, rust),
        }
    }
}

/// This run's committed checkout holding `project`, written by whichever test asks first.
///
/// A fixture checkout is a handful of git, Cargo and file writes that every test repeated; on
/// the loaded gate each git process took 0.8 s against 75 ms alone. The tree holds no path of
/// the scratch root it is cloned under, so one per run serves every test. Like the blank image
/// template, it lives in the runner's template directory and is written aside and moved in
/// whole, so the next run's sweep reclaims it and a writer killed halfway leaves no template.
fn checkout_template(project: Project<'_>) -> PathBuf {
    // SAFETY: `getppid` has no preconditions and cannot fail.
    let run = unsafe { libc::getppid() };
    let directory = PathBuf::from(format!("{}{run}-templates", scratch_apfs::ROOT_PREFIX));
    fs::create_dir_all(&directory)
        .unwrap_or_else(|error| panic!("template directory {}: {error}", directory.display()));
    let key = project.key();
    let template = directory.join(format!("checkout-{key}"));
    let lock = directory.join(format!("checkout-{key}.lock"));
    let _writing = scratch_apfs::lock_exclusive(&lock)
        .unwrap_or_else(|error| panic!("template lock {}: {error}", lock.display()));
    if !template.exists() {
        let aside = directory.join(format!("checkout-{key}.{}", std::process::id()));
        project.write(&aside);
        fs::rename(&aside, &template).unwrap_or_else(|error| {
            panic!("publish checkout template {}: {error}", template.display())
        });
    }
    template
}

/// `source`'s whole tree cloned to `destination`, which must not exist: one `clonefile`, no copy.
fn clone_tree(source: &Path, destination: &Path) {
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        fn clonefile(src: *const libc::c_char, dst: *const libc::c_char, flags: u32)
        -> libc::c_int;
    }
    let path = |path: &Path| std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path");
    let (from, to) = (path(source), path(destination));
    // SAFETY: both arguments are NUL-terminated strings that outlive the call, which reads them
    // and touches no other memory.
    let cloned = unsafe { clonefile(from.as_ptr(), to.as_ptr(), 0) };
    assert_eq!(
        cloned,
        0,
        "clone {} to {}: {}",
        source.display(),
        destination.display(),
        std::io::Error::last_os_error()
    );
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

/// Every macOS port block of `fixture`'s store claimed for this process except `kept`, exactly
/// as a concurrent creator's reservation claims one; dropping it releases the claims.
struct PortClaims(Vec<PathBuf>);

impl PortClaims {
    fn all_but(fixture: &Fixture, kept: PortBlock) -> Self {
        let staging = fixture.storage.store().join(".staging");
        fs::create_dir_all(&staging).expect("reservation directory");
        let owner = std::process::id().to_string();
        Self(
            PortBlock::macos_candidates()
                .filter(|block| *block != kept)
                .map(|block| {
                    let marker = staging.join(format!("port-{}.reservation", block.base()));
                    std::os::unix::fs::symlink(&owner, &marker).expect("port reservation marker");
                    marker
                })
                .collect(),
        )
    }
}

impl Drop for PortClaims {
    fn drop(&mut self) {
        for marker in &self.0 {
            if let Err(error) = fs::remove_file(marker) {
                eprintln!("port reservation {}: {error}", marker.display());
            }
        }
    }
}

/// The highest macOS port block whose every port the host has free right now.
fn highest_free_port_block() -> PortBlock {
    let candidates: Vec<PortBlock> = PortBlock::macos_candidates().collect();
    candidates
        .into_iter()
        .rev()
        .find(|block| {
            (block.base()..block.base() + block.size())
                .map(|port| TcpListener::bind((Ipv4Addr::LOCALHOST, port)))
                .collect::<std::io::Result<Vec<_>>>()
                .is_ok()
        })
        .expect("a macOS port block is free on the host")
}

#[tokio::test]
async fn real_apfs_dispatch_reallocates_a_raced_port_and_reconciles_gateway_grants() {
    let mut fixture = Fixture::new();
    // Main's block must be one no parallel test's store is also handed. Every store allocates
    // from its lowest free block up and checks the host only when it allocates, so two tests can
    // hold the same low block, and the other's gateway took this test's port before this test
    // could ("claim published port: AddrInUse"). Adoption here is steered to the highest free
    // block, which no other store reaches, by claiming every other block in this store.
    let steered = highest_free_port_block();
    let claims = PortClaims::all_but(&fixture, steered);
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let original = service
        .grants("main")
        .await
        .expect("original durable block")
        .port_block
        .expect("allocated macOS block");
    assert_eq!(original, steered);
    let original_port = original.base();
    // Take the port after allocation and image publication, before gateway installation. This
    // cannot be solved by probing in the allocator: the listener must trigger a real bind refusal.
    let competing_listener =
        TcpListener::bind((Ipv4Addr::LOCALHOST, original_port)).expect("claim published port");
    // The reallocation needs a block to move to.
    drop(claims);
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
    for convention in [".envrc", ".envrc-local", "flake.nix", "devenv.nix"] {
        assert!(!fixture.checkout.join(convention).exists());
    }
    assert_eq!(
        fs::read_to_string(fixture.checkout.join(".cowshed.toml")).expect("fixture config"),
        FIXTURE_COWSHED_TOML,
        "the only cowshed configuration is the fixture's image cap"
    );
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

/// The package-manager link directory the repository's Nx is installed through: the sandbox
/// reads Nx there.
fn nx_links(nx: &Path) -> PathBuf {
    nx.ancestors()
        .find(|ancestor| ancestor.file_name().is_some_and(|name| name == "links"))
        .unwrap_or(nx)
        .to_path_buf()
}

/// Adopt the fixture's checkout as main, let every workspace read `reads` (the toolchains its
/// jobs run) and `/nix/store`, and serve it through a real gateway.
async fn serve_project(fixture: &mut Fixture, reads: &[PathBuf]) -> ActorBridge {
    let mut service = fixture.open().await;
    adopt(fixture, &mut service).await;
    let mut grant = vec![OsString::from("grant"), OsString::from("--project-wide")];
    for read in reads
        .iter()
        .map(PathBuf::as_path)
        .chain([Path::new("/nix/store")])
    {
        grant.extend([OsString::from("--read"), read.as_os_str().to_os_string()]);
    }
    succeed(&mut service, grant).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("serve the project");
    service
}

/// Dispatch one CLI command that must exit 0; answers its stdout and stderr.
async fn succeed(
    service: &mut ActorBridge,
    args: impl IntoIterator<Item = impl Into<OsString>>,
) -> (Vec<u8>, String) {
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let (result, stdout, stderr) = run(service, args.clone()).await;
    let stderr = String::from_utf8_lossy(&stderr).into_owned();
    match result {
        Ok(0) => (stdout, stderr),
        other => panic!(
            "cowshed {args:?}: {other:?}\nstdout: {}\nstderr: {stderr}",
            String::from_utf8_lossy(&stdout)
        ),
    }
}

/// An Nx project whose `a:build` writes `a/generated.txt` and whose `a:test` reads it (its
/// default inputs include it). `declared` says whether `a:build` declares that output. Written,
/// not committed: the caller commits once it has written all it adds.
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
    let mut fixture = Fixture::with(Project::Nx {
        nx: &nx,
        node: &node,
        declared,
    });
    let mut service = serve_project(&mut fixture, &[nx_links(&nx)]).await;
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
/// output makes the miss, and the record, disappear. The two projects are independent, each on
/// its own scratch store, so they land side by side.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_an_undeclared_nx_output_lands_as_a_land_adoption_commitment() {
    let undeclared = tokio::spawn(land_nx_project(false));
    let declared = tokio::spawn(land_nx_project(true));
    let (adoptions, landed_head) = undeclared.await.expect("undeclared-output land");
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

    let (adoptions, _) = declared.await.expect("declared-output land");
    assert_eq!(adoptions, [], "a declared output restores in main and hits");
}

/// How long any build-volume step may take before a test gives up on it: generous, because a
/// loaded host slows every real build and detach, and a slow pass is not a failure.
const PATIENCE: Duration = Duration::from_secs(300);

/// The Rust toolchain's `bin` directory the test itself builds with, resolved at run time like
/// [`repository_nx`].
fn rust_toolchain() -> PathBuf {
    let cargo = std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|directory| directory.join("cargo"))
                .find(|candidate| candidate.is_file())
        })
        .expect("cargo on the test's PATH")
        .canonicalize()
        .expect("cargo resolves");
    let bin = cargo.parent().expect("cargo has a directory").to_path_buf();
    assert!(
        bin.join("rustc").is_file(),
        "rustc beside {}",
        cargo.display()
    );
    bin
}

/// Install `rust` where a host keeps its own toolchain, `~/.cargo/bin` of the fixture's home,
/// so the Cargo capability finds it as it finds a real host's (15_capabilities.md, "Bootstrap
/// executables"): every job then runs plain `cargo`, and build-state discovery asks that same
/// Cargo for the target directory.
fn install_rust(home: &Path, rust: &Path) {
    let bin = home.join(".cargo/bin");
    fs::create_dir_all(&bin).unwrap();
    for program in ["cargo", "rustc", "rustdoc"] {
        std::os::unix::fs::symlink(rust.join(program), bin.join(program)).unwrap();
    }
}

/// A Cargo workspace of two path crates, `fixture-app` depending on `fixture-base`: libraries
/// only, so building them links nothing. Its lockfile comes from the same Cargo the sandbox
/// runs, so no build rewrites a tracked file.
fn write_cargo_workspace(checkout: &Path, rust: &Path) {
    fs::write(
        checkout.join("Cargo.toml"),
        "[workspace]\nmembers = [\"base\", \"app\"]\nresolver = \"3\"\n",
    )
    .unwrap();
    for (directory, manifest, source) in [
        (
            "base",
            "[package]\nname = \"fixture-base\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
            "pub fn base() -> u32 {\n    1\n}\n",
        ),
        (
            "app",
            "[package]\nname = \"fixture-app\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n\
             [dependencies]\nfixture-base = { path = \"../base\" }\n",
            "pub fn app() -> u32 {\n    fixture_base::base() + 1\n}\n",
        ),
    ] {
        fs::create_dir_all(checkout.join(directory).join("src")).unwrap();
        fs::write(checkout.join(directory).join("Cargo.toml"), manifest).unwrap();
        fs::write(checkout.join(directory).join("src/lib.rs"), source).unwrap();
    }
    let lockfile = Command::new(rust.join("cargo"))
        .args(["generate-lockfile", "--offline"])
        .current_dir(checkout)
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .expect("cargo generate-lockfile");
    assert!(
        lockfile.status.success(),
        "cargo generate-lockfile: {}",
        String::from_utf8_lossy(&lockfile.stderr)
    );
}

/// Main's tree for the build-volume tests, written into `checkout` and committed: the Nx project
/// with its outputs declared and/or the Cargo workspace, with every build output and the tests'
/// own control files ignored, so a retiring land finds the landing tree clean.
fn write_build_project(checkout: &Path, nx: Option<(&Path, &Path)>, rust: Option<&Path>) {
    if let Some((nx, node)) = nx {
        write_nx_project(checkout, nx, node, true);
    }
    if let Some(rust) = rust {
        write_cargo_workspace(checkout, rust);
    }
    fs::write(
        checkout.join(".gitignore"),
        "node_modules\n.nx\n/target\n/a/generated.txt\n/stop-ticking\n",
    )
    .unwrap();
    git(checkout, &["add", "-A"]);
    git(checkout, &["commit", "-q", "-m", "build volume fixture"]);
}

/// The Cargo build every checkout runs, spelled identically everywhere.
const CARGO_BUILD: &str = "cargo build --message-format=json";

/// Capability detection put `checkout`'s Cargo target directory on its build volume: `target`
/// is the fixed link through `.cowshed/build` (16_build_volumes.md, "One link per checkout").
fn assert_target_on_build_volume(checkout: &Path) {
    assert_eq!(
        fs::read_link(checkout.join("target"))
            .unwrap_or_else(|error| panic!("{}/target is not a link: {error}", checkout.display())),
        Path::new(".cowshed/build/target")
    );
}

fn nx_run_many(node: &Path) -> String {
    format!(
        "{} node_modules/nx/dist/bin/nx.js run-many -t build test",
        node.display()
    )
}

/// Each `compiler-artifact` Cargo reported on `stdout`: its package id and whether it was fresh.
fn cargo_artifacts(stdout: &[u8]) -> Vec<(String, bool)> {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|message| message["reason"] == "compiler-artifact")
        .map(|message| {
            (
                message["package_id"]
                    .as_str()
                    .expect("an artifact names its package")
                    .to_owned(),
                message["fresh"]
                    .as_bool()
                    .expect("an artifact says whether it is fresh"),
            )
        })
        .collect()
}

/// Run `script` with `/bin/sh` in `workspace`'s sandbox; it must exit 0. Answers its stdout.
async fn sh(service: &mut ActorBridge, workspace: &str, script: &str) -> Vec<u8> {
    succeed(service, ["exec", workspace, "--", "/bin/sh", "-c", script])
        .await
        .0
}

async fn new_workspace(service: &mut ActorBridge, name: &str) -> PathBuf {
    succeed(service, ["new", name]).await;
    service
        .path(name, false)
        .await
        .unwrap_or_else(|error| panic!("{name} is mounted: {error}"))
        .mount
}

/// `cowshed land --json <workspace>` with `checks`, retiring the workspace unless `retire` is
/// false; answers the report the CLI printed.
async fn land(
    service: &mut ActorBridge,
    workspace: &str,
    retire: bool,
    checks: &[&str],
) -> LandReport {
    let mut args = vec!["--json", "land", workspace];
    if !retire {
        args.push("--no-retire");
    }
    for check in checks {
        args.extend(["--check", check]);
    }
    let (stdout, stderr) = succeed(service, args).await;
    let envelope: JsonEnvelope<LandReport> =
        serde_json::from_slice(&stdout).unwrap_or_else(|error| {
            panic!(
                "land --json printed no report ({error}): {}\n{stderr}",
                String::from_utf8_lossy(&stdout)
            )
        });
    envelope
        .result()
        .cloned()
        .expect("a successful land report")
}

fn git_stdout(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(directory)
        .output()
        .expect("git process");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git prints UTF-8")
        .trim()
        .to_owned()
}

/// The fixture project's build volumes, where the runtime keeps them.
fn build_volumes(fixture: &Fixture) -> BuildVolumeLayout {
    let storage = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("fixture storage layout");
    BuildVolumeLayout::new(storage.project()).expect("fixture build volume layout")
}

/// The build volume `checkout`'s `.cowshed/build` names.
fn linked_volume(layout: &BuildVolumeLayout, checkout: &Path) -> BuildVolumeId {
    let mount = link::linked(checkout)
        .expect("read the build link")
        .unwrap_or_else(|| panic!("{} links no build volume", checkout.display()));
    layout.volume_at(&mount).unwrap_or_else(|| {
        panic!(
            "{} links {}, which is not under {}",
            checkout.display(),
            mount.display(),
            layout.mounts().display()
        )
    })
}

/// Every seed whose target is main.
fn seeds_of_main(layout: &BuildVolumeLayout) -> Vec<(BuildVolumeId, BuildVolumeRecord)> {
    layout
        .list()
        .expect("list build volumes")
        .into_iter()
        .filter_map(|id| {
            let record = layout.read_record_present(&id).expect("read a record")?;
            matches!(&record.role, BuildVolumeRole::Seed { target, .. } if *target == WorkspaceName::main())
                .then_some((id, record))
        })
        .collect()
}

async fn eventually(what: &str, mut ready: impl FnMut() -> bool) {
    let started = Instant::now();
    while !ready() {
        assert!(
            started.elapsed() < PATIENCE,
            "{what}: not within {PATIENCE:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Run `cowshed gc` until build volume `id`'s image is gone: collection never forces a detach,
/// so a holder that is still exiting defers it to the next pass.
async fn collected(service: &mut ActorBridge, layout: &BuildVolumeLayout, id: &BuildVolumeId) {
    let started = Instant::now();
    while layout.image(id).exists() {
        assert!(
            started.elapsed() < PATIENCE,
            "gc did not reclaim build volume {id} within {PATIENCE:?}"
        );
        let (_, stderr) = succeed(service, ["gc"]).await;
        eprintln!("gc: {stderr}");
        if layout.image(id).exists() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    assert!(
        !layout.record(id).exists(),
        "build volume {id}'s record outlives its image"
    );
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).map_or(0, |metadata| metadata.len())
}

/// A host process (never the daemon) holding a file open, killed when dropped.
struct Holder(std::process::Child);

impl Drop for Holder {
    fn drop(&mut self) {
        // A holder that already exited cannot be killed again; reaping it is all that is left.
        if let Err(error) = self.0.kill() {
            eprintln!("holder {}: kill: {error}", self.0.id());
        }
        if let Err(error) = self.0.wait() {
            eprintln!("holder {}: wait: {error}", self.0.id());
        }
    }
}

/// A fork clones its target's seed, so a fork of a warm main is warm: every Cargo unit is
/// `Fresh` and every Nx task a local cache hit (16_build_volumes.md, "Fork", rules "Clones
/// preserve mtimes", "One Cargo environment on every path" and "Hash inputs are the same in
/// every checkout"). The relocated host build starts with the agent harness's `CI=true` and
/// sources the checkout's environment; the sandbox build must reuse those same units. Main's
/// first seed was frozen empty at first touch; a land is what warms it.
#[tokio::test]
async fn real_apfs_a_fork_of_a_warm_target_is_all_fresh_and_all_hits() {
    let (nx, node) = repository_nx();
    let rust = rust_toolchain();
    let mut fixture = Fixture::with(Project::Build {
        nx: Some((&nx, &node)),
        rust: Some(&rust),
    });
    let mut service = serve_project(&mut fixture, &[nx_links(&nx)]).await;
    assert_target_on_build_volume(&fixture.checkout);
    let nx_check = nx_run_many(&node);

    let w1 = new_workspace(&mut service, "w1").await;
    fs::write(w1.join("a/src.txt"), b"src, changed\n").unwrap();
    git(&w1, &["commit", "-q", "-am", "change a"]);
    let cold = cargo_artifacts(&sh(&mut service, "w1", CARGO_BUILD).await);
    assert_eq!(cold.len(), 2, "{cold:?}");
    assert!(
        cold.iter().all(|(_, fresh)| !fresh),
        "w1 forks main's empty first seed, so it builds every unit: {cold:?}"
    );
    sh(&mut service, "w1", &nx_check).await;
    let report = land(&mut service, "w1", true, &[&nx_check, CARGO_BUILD]).await;
    assert!(report.build_volume.seeded, "{report:?}");
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");

    let w2 = new_workspace(&mut service, "w2").await;
    assert_ne!(
        w1, w2,
        "the warm build volume is cloned to another checkout path"
    );
    assert_target_on_build_volume(&w2);
    let host = Command::new("/bin/sh")
        .args(["-ec", &format!(". .cowshed/env; exec {CARGO_BUILD}")])
        .current_dir(&w2)
        .env_clear()
        .env("HOME", fixture.storage.home())
        .env(
            "PATH",
            std::env::join_paths([rust.as_path(), Path::new("/usr/bin"), Path::new("/bin")])
                .unwrap(),
        )
        .env("CI", "true")
        .output()
        .expect("host build of the relocated target");
    let host_warm = cargo_artifacts(&host.stdout);
    let started = Instant::now();
    let warm = cargo_artifacts(&sh(&mut service, "w2", CARGO_BUILD).await);
    eprintln!("w2 cargo build: {:?}", started.elapsed());
    let spawned = SystemTime::now();
    sh(&mut service, "w2", &nx_check).await;
    let exited = SystemTime::now();
    let nx_run = nx::attribute(&w2.join(".nx/cache"), &nx_check, spawned, exited);
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
    assert!(
        host.status.success(),
        "host Cargo: {}",
        String::from_utf8_lossy(&host.stderr)
    );
    assert_eq!(host_warm.len(), 2, "{host_warm:?}");
    assert!(
        host_warm.iter().all(|(_, fresh)| *fresh),
        "the relocated host build with CI=true stays all-Fresh: {host_warm:?}\nstderr: {}",
        String::from_utf8_lossy(&host.stderr)
    );
    assert_eq!(warm.len(), 2, "{warm:?}");
    assert!(
        warm.iter().all(|(_, fresh)| *fresh),
        "the sandbox reuses every unit of the relocated host build: {warm:?}"
    );
    match nx_run {
        nx::Attribution::Ours(run) => {
            assert_eq!(run.tasks.len(), 2, "{run:?}");
            assert!(
                run.tasks
                    .iter()
                    .all(|task| task.cache == nx::CacheStatus::LocalHit),
                "every Nx task of a fork of a warm main is a local cache hit: {run:?}"
            );
        }
        nx::Attribution::Unattributed(reason) => {
            panic!("w2's Nx run left no summary of its own: {reason:?}")
        }
    }
}

/// A land freezes main's new seed from the landing volume at the landed tree, moves main's link
/// onto that volume, releases main's previous volume, and re-runs the check in main: every task
/// hits (16_build_volumes.md, Land steps 5–7).
#[tokio::test]
async fn real_apfs_a_land_adopts_freezes_the_seed_releases_the_old_volume_and_reports_2b() {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let mut service = serve_project(&mut fixture, &[nx_links(&nx)]).await;
    let layout = build_volumes(&fixture);
    let previous = linked_volume(&layout, &fixture.checkout);

    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("a/src.txt"), b"src, changed\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "change a"]);
    let landing = linked_volume(&layout, &topic);
    assert_ne!(landing, previous);
    let landed_tree = git_stdout(&topic, &["rev-parse", "HEAD^{tree}"]);
    let report = land(&mut service, "topic", true, &[&nx_run_many(&node)]).await;

    assert!(report.retired, "{report:?}");
    assert!(report.build_volume.seeded, "{report:?}");
    match &report.build_volume.adoption {
        Adoption::Adopted { check, .. } => {
            assert_eq!(check.hits, 2, "a:build and a:test hit in main: {report:?}");
            assert_eq!(check.misses, [], "{report:?}");
            assert_eq!(check.failed, [], "{report:?}");
            assert_eq!(check.unattributed, [], "{report:?}");
        }
        Adoption::Skipped { reason } => panic!("the adoption was skipped: {reason:?}"),
    }
    assert_eq!(linked_volume(&layout, &fixture.checkout), landing);
    assert_eq!(
        layout.read_record(&landing).expect("adopted record").role,
        BuildVolumeRole::Linked {
            checkout: WorkspaceName::main()
        }
    );
    let seeds = seeds_of_main(&layout);
    let [(seed, record)] = seeds.as_slice() else {
        panic!("main keeps exactly its latest seed: {seeds:?}");
    };
    assert!(
        *seed != landing && *seed != previous,
        "{seed} is a new volume"
    );
    assert_eq!(
        record.tree.as_ref().map(|tree| tree.as_str()),
        Some(landed_tree.as_str()),
        "the seed is frozen at the landed tree"
    );
    collected(&mut service, &layout, &previous).await;
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// A process other than main's daemon holding main's Nx task database open makes the land skip
/// the swap and name that process (16_build_volumes.md, "The adoption needs the target's Nx
/// database closed"). The seed is still frozen; main keeps its own volume.
#[tokio::test]
async fn real_apfs_a_foreign_database_holder_skips_adoption_with_its_pid_and_argv() {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let mut service = serve_project(&mut fixture, &[nx_links(&nx)]).await;
    let layout = build_volumes(&fixture);
    let nx_check = nx_run_many(&node);

    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("a/src.txt"), b"src, changed\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "change a"]);
    // Main's own task database, written by a run in main.
    sh(&mut service, "main", &nx_check).await;
    let data = fixture.checkout.join(".nx/workspace-data");
    let databases: Vec<PathBuf> = fs::read_dir(&data)
        .expect("main's Nx workspace data")
        .map(|entry| entry.expect("workspace data entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "db"))
        .collect();
    let [database] = databases.as_slice() else {
        panic!("one task database in {}: {databases:?}", data.display());
    };
    let before = linked_volume(&layout, &fixture.checkout);

    let holder = Holder(
        Command::new("/usr/bin/tail")
            .arg("-f")
            .arg(database)
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn a database holder"),
    );
    let pid = i32::try_from(holder.0.id()).expect("a pid fits i32");
    eventually("tail holds the task database open", || {
        nx::holders(database)
            .expect("query the database's holders")
            .iter()
            .any(|holder| holder.pid == pid)
    })
    .await;
    let report = land(&mut service, "topic", true, &[&nx_check]).await;

    assert!(
        report.build_volume.seeded,
        "forks still start from the new seed: {report:?}"
    );
    match report.build_volume.adoption {
        Adoption::Skipped {
            reason:
                AdoptionSkip::TargetHeld {
                    database: held,
                    holders,
                },
        } => {
            assert_eq!(
                held.canonicalize().expect("the held database exists"),
                database.canonicalize().expect("main's database exists")
            );
            assert_eq!(
                holders,
                [DatabaseHolder {
                    pid,
                    command: format!("/usr/bin/tail -f {}", database.display()),
                }]
            );
        }
        adoption => panic!("a held database skips the adoption: {adoption:?}"),
    }
    assert_eq!(linked_volume(&layout, &fixture.checkout), before);
    drop(holder);
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// A job admitted before an adoption keeps the volume it was admitted with: its working
/// directory stays in main's previous volume, which therefore stays until the job exits. A job
/// admitted afterwards resolves main's link to the adopted volume (16_build_volumes.md, "Process
/// lifetime across a swap", "Garbage collection").
#[tokio::test]
async fn real_apfs_a_job_running_across_an_adoption_keeps_its_volume_and_later_jobs_get_the_new_one()
 {
    let rust = rust_toolchain();
    let mut fixture = Fixture::with(Project::Build {
        nx: None,
        rust: Some(&rust),
    });
    let mut service = serve_project(&mut fixture, &[]).await;
    assert_target_on_build_volume(&fixture.checkout);
    let layout = build_volumes(&fixture);
    let old = linked_volume(&layout, &fixture.checkout);
    let old_mount = layout.mount(&old);
    let stop = fixture.checkout.join("stop-ticking");
    let job = format!(
        "mkdir -p target/ticking && cd target/ticking && \
         while [ ! -e {} ]; do printf . >> ticks; sleep 0.1; done; printf done > exited",
        stop.display()
    );
    succeed(
        &mut service,
        ["exec", "main", "--background", "--", "/bin/sh", "-c", &job],
    )
    .await;
    let ticks = old_mount.join("target/ticking/ticks");
    eventually("the job ticks in main's volume", || file_len(&ticks) > 0).await;

    let workspace = new_workspace(&mut service, "w").await;
    fs::write(workspace.join("notes.txt"), b"notes\n").unwrap();
    git(&workspace, &["add", "notes.txt"]);
    git(&workspace, &["commit", "-q", "-m", "notes"]);
    let landing = linked_volume(&layout, &workspace);
    let report = land(&mut service, "w", true, &[CARGO_BUILD]).await;
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");
    assert_eq!(linked_volume(&layout, &fixture.checkout), landing);
    let new_mount = layout.mount(&landing);

    let at_swap = file_len(&ticks);
    eventually("the job keeps ticking in the previous volume", || {
        file_len(&ticks) > at_swap
    })
    .await;
    assert!(!new_mount.join("target/ticking").exists());
    sh(&mut service, "main", "touch target/after").await;
    assert!(new_mount.join("target/after").is_file());
    assert!(!old_mount.join("target/after").exists());
    assert_eq!(
        layout.read_record(&old).expect("previous record").role,
        BuildVolumeRole::Unlinked
    );
    succeed(&mut service, ["gc"]).await;
    assert!(
        layout.image(&old).exists(),
        "gc never forces away a volume a running job is in"
    );

    fs::write(&stop, b"").unwrap();
    let exited = old_mount.join("target/ticking/exited");
    eventually("the job exits", || exited.is_file()).await;
    collected(&mut service, &layout, &old).await;
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}
