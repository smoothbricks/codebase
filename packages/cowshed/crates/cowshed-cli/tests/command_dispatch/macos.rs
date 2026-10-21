use cowshed_apfs::attach_budget;
use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::store_lock::verify_no_symlinks;
use cowshed_apfs::template::{blank_template, blank_template_path};
use cowshed_apfs::{
    ApfsBackend, CommandRunner, DetachIntent, DiskImageSource, ImageCapture, MacOsApfsBackend,
    SystemCommandRunner,
};
use cowshed_cli::args::parse_args;
use cowshed_cli::output::Output;
use cowshed_cli::runtime::{ActorBridge, CliService, dispatch};
use cowshed_core::api::JsonEnvelope;
use cowshed_core::api::dto::{
    Adoption, AdoptionSkip, CarrySide, DatabaseHolder, GcReport, LandReport, LandSeed,
    LandedReport, RebaseBuildVolume, RebaseCarrySkip, RebaseReport,
};
use cowshed_core::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, link, nx,
};
use cowshed_core::metadata::{PortBlock, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::RecoveryScope;
use cowshed_core::storage::apfs::native::{
    KernelMountSource, MacOsApfsExecutionHost, SystemKernelMountSource,
};
use cowshed_core::storage::apfs::{ApfsSubstrateConfig, build_volume_label};
use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
use cowshed_core::{ErrorCode, Result};
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, CpuBudgetLimits, Gateway, GatewayConfig,
    KeychainCredentialProvider, MirrorCacheConfig, SystemConnector,
};
use cowshed_os::test_backstop::NO_PROGRESS;
use std::ffi::OsString;
use std::fs;
use std::net::{Ipv4Addr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

#[path = "../../../cowshed-apfs/tests/support/blank_image.rs"]
mod blank_image;
#[path = "../../../cowshed-core/tests/support/nx_reads.rs"]
mod nx_dependencies;
#[path = "../../../cowshed-apfs/tests/support/scratch_apfs.rs"]
mod scratch_apfs;
#[expect(
    clippy::duplicate_mod,
    reason = "scratch_directory and scratch_apfs each include the stateless scratch_processes \
              helpers, so either support file also stands alone in a test binary"
)]
#[path = "../../../cowshed-core/tests/support/scratch_directory.rs"]
mod scratch_directory;
use nx_dependencies::nx_read_paths;

use scratch_apfs::ScratchRoot;

enum FixtureRoot {
    Apfs(ScratchRoot),
    Directory(scratch_directory::ScratchRoot),
}

impl FixtureRoot {
    fn path(&self) -> &Path {
        match self {
            Self::Apfs(root) => root.path(),
            Self::Directory(root) => root.path(),
        }
    }
}

/// The checkout's whole `.cowshed.toml`: the build-volume cap, nothing else.
const FIXTURE_COWSHED_TOML: &str = "[build]\ncapacity = \"1g\"\n";

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

struct Fixture {
    _scratch: FixtureRoot,
    checkout: PathBuf,
    storage: ValidatedHostStorage,
    granted: PathBuf,
    gateway: Option<Gateway>,
    /// The tokens its gateway's CPU ledger serves: the host's by default, or a fixed small
    /// host's, so a scenario whose admission depends on the host's size runs as a small CI
    /// runner runs it.
    cpu_budget: CpuBudgetLimits,
}

impl Fixture {
    fn directory() -> Self {
        Self::with_directory(Project::Plain)
    }

    fn with_directory(project: Project<'_>) -> Self {
        Self::with_root(
            project,
            FixtureRoot::Directory(
                scratch_directory::ScratchRoot::new("cli").expect("scratch directory root"),
            ),
        )
    }

    /// A fixture whose checkout holds `project`, committed: an APFS clone of this run's checkout
    /// template for it ([`checkout_template`]).
    fn with(project: Project<'_>) -> Self {
        Self::with_root(
            project,
            FixtureRoot::Apfs(ScratchRoot::new("cli").expect("scratch APFS root")),
        )
    }

    fn with_root(project: Project<'_>, scratch: FixtureRoot) -> Self {
        let checkout = scratch.path().join("checkout");
        let store = scratch.path().join("store");
        let granted = scratch.path().join("granted");
        for path in [&store, &granted] {
            fs::create_dir_all(path).expect("fixture directory");
        }
        if matches!(&scratch, FixtureRoot::Apfs(_)) {
            // Only the native backend mints images from the user's blank template.
            blank_image::blank_image(&blank_template_path(&store, blank_image::CAPACITY));
        }
        clone_tree(&checkout_template(project), &checkout);
        let storage =
            ValidatedHostStorage::new(scratch.path().to_path_buf(), CanonicalRoots::at(store));
        Self {
            _scratch: scratch,
            checkout,
            storage,
            granted,
            gateway: None,
            cpu_budget: CpuBudgetLimits::default(),
        }
    }

    async fn open(&self) -> ActorBridge {
        let repo = Some(RepoId::parse("fixture/dispatch").expect("repository identity"));
        match &self._scratch {
            FixtureRoot::Apfs(_) => {
                ActorBridge::open_for_adopt_at(&self.checkout, repo, self.storage.clone()).await
            }
            FixtureRoot::Directory(_) => {
                ActorBridge::open_directory_for_adopt_at(&self.checkout, repo, self.storage.clone())
                    .await
            }
        }
        .expect("open project runtime on the fixture's storage")
    }

    async fn reopen(&self) -> ActorBridge {
        let scope = RecoveryScope::Workspaces([WorkspaceName::main()].into());
        match &self._scratch {
            FixtureRoot::Apfs(_) => {
                ActorBridge::open_existing_at(&self.checkout, scope, self.storage.clone()).await
            }
            FixtureRoot::Directory(_) => {
                ActorBridge::open_directory_existing_at(&self.checkout, scope, self.storage.clone())
                    .await
            }
        }
        .expect("reopen persisted project runtime")
    }
    async fn start_gateway(&mut self) {
        let cache = cowshed_core::host_dirs::gateway_mirror(self.storage.home());
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
            cpu_budget: self.cpu_budget,
            ..GatewayConfig::default()
        };
        let connector =
            SystemConnector::new(NO_PROGRESS, NO_PROGRESS).expect("production upstream connector");
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
        rust: Option<&'a RustToolchain>,
    },
    /// [`Project::Plain`] whose `.cowshed.toml` also declares [`DECLARED_STATE`] build state,
    /// with an ignored, untracked nested Git checkout already there ([`write_declared_project`]).
    Declared,
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
            Self::Declared => "declared",
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
            Self::Declared => write_declared_project(checkout),
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
    materialized_template(
        &checkout_template_directory(),
        &format!("checkout-{}", project.key()),
        |aside| project.write(aside),
    )
}

fn checkout_template_directory() -> PathBuf {
    // SAFETY: `getppid` has no preconditions and cannot fail.
    let run = unsafe { libc::getppid() };
    let directory = PathBuf::from(format!(
        "/private/tmp/{}{run}-templates",
        scratch_directory::ROOT_PREFIX
    ));
    fs::create_dir_all(&directory)
        .unwrap_or_else(|error| panic!("template directory {}: {error}", directory.display()));
    directory
}

/// Publish one immutable tree for this runner, under the same lock used by its readers.
fn materialized_template(directory: &Path, key: &str, write: impl FnOnce(&Path)) -> PathBuf {
    let template = directory.join(key);
    let lock = directory.join(format!("{key}.lock"));
    let _writing = scratch_apfs::lock_exclusive(&lock)
        .unwrap_or_else(|error| panic!("template lock {}: {error}", lock.display()));
    if !template.exists() {
        let aside = directory.join(format!("{key}.{}", std::process::id()));
        write(&aside);
        fs::rename(&aside, &template).unwrap_or_else(|error| {
            panic!("publish fixture template {}: {error}", template.display())
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

/// Dispatch one CLI command. Its start and end go to stderr beside the runtime's own spans, as
/// [`git`]'s do: an exec has no span of its own, so a slow job reads as the time between these.
async fn run(
    service: &mut ActorBridge,
    args: impl IntoIterator<Item = impl Into<OsString>>,
) -> (Result<i32>, Vec<u8>, Vec<u8>) {
    let cli = parse_args(args).expect("valid CLI command");
    let verb = format!("{:?}", cli.command);
    let verb = verb
        .split(['(', ' ', '{'])
        .next()
        .unwrap_or("command")
        .to_owned();
    eprintln!("dispatch fixture cowshed: {verb} start");
    let started = Instant::now();
    let mut output = Output::new(Vec::new(), Vec::new(), cli.global.quiet);
    let result = dispatch(service, cli, tokio::io::empty(), &mut output)
        .await
        .map(|exit| exit.code);
    eprintln!(
        "dispatch fixture cowshed: {verb} done elapsed={:?} ok={}",
        started.elapsed(),
        result.is_ok()
    );
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

/// Persist grants across restarts before installing the gateway, then race the published port
/// and reconcile a changed grant revision on the same adopted main.
#[tokio::test(flavor = "multi_thread")]
async fn directory_dispatch_grant_persists_across_runtime_restart() {
    let mut fixture = Fixture::directory();
    // Main's blocks must be ones no parallel test's store is also handed. Every store allocates
    // from its lowest free block up and checks the host only when it allocates, so two tests can
    // hold the same low block, and the other's gateway took this test's port before this test
    // could ("claim published port: AddrInUse"). The same holds for the block main moves to: a
    // low one is free on the host while this test's fixture gateway restarts, another store's
    // adopt takes it then, and the restore correctly refuses the occupied base and moves main
    // again (05_gateway.md, `AddrInUse`). Adoption is steered to the highest free block and the
    // move to the next one below it, which no other store reaches, by claiming every other block
    // in this store for the whole test.
    let [steered, moved_to] = highest_free_port_blocks(&fixture);
    let mut claims = PortClaims::all_but(&fixture, steered);
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
    let project = service
        .project_grants()
        .await
        .expect("durable project policy");
    assert!(project.deny_write.contains(&PathBuf::from("generated")));
    let original = service
        .grants("main")
        .await
        .expect("original durable block")
        .port_block
        .expect("allocated macOS block");
    assert_eq!(original, steered);
    assert!(fixture.storage.port_allocation().contains(original));
    assert!(
        !CanonicalRoots::global()
            .port_allocation()
            .contains(original)
    );
    let original_port = original.base();
    // Take the port after allocation and image publication, before gateway installation. This
    // cannot be solved by probing in the allocator: the listener must trigger a real bind refusal.
    let competing_listener =
        TcpListener::bind((Ipv4Addr::LOCALHOST, original_port)).expect("claim published port");
    // The reallocation's one block to move to; every other block stays claimed until the end.
    claims.release(moved_to);
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("install main from real inventory");
    let reallocated = service
        .grants("main")
        .await
        .expect("reallocated durable block")
        .port_block
        .expect("replacement macOS block");
    assert!(fixture.storage.port_allocation().contains(reallocated));
    assert!(
        !CanonicalRoots::global()
            .port_allocation()
            .contains(reallocated)
    );
    assert_eq!(
        reallocated, moved_to,
        "main moves to the one unclaimed block"
    );
    let reallocated_port = reallocated.base();
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
    fixture.stop_gateway().await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("restore sessions after fixture gateway restart");
    let (result, _, _) = run(&mut service, ["grant", "main"]).await;
    assert_eq!(result.expect("CLI remains live after gateway restart"), 0);
    let restarted = fixture
        .gateway
        .as_ref()
        .expect("restarted fixture gateway")
        .handle()
        .status()
        .await
        .expect("gateway responds after restart and CLI dispatch");
    assert_eq!(restarted.sessions.len(), 1);
    assert_eq!(restarted.sessions[0].endpoint, after.sessions[0].endpoint);
    assert_eq!(restarted.sessions[0].revision, after.sessions[0].revision);
    service.shutdown().await.expect("shutdown runtime");
    fixture.stop_gateway().await;
    drop(competing_listener);
    drop(claims);
}

/// Every macOS port block of `fixture`'s store claimed for this process except `kept`, exactly
/// as a concurrent creator's reservation claims one; dropping it releases the claims.
struct PortClaims(Vec<(PortBlock, PathBuf)>);

impl PortClaims {
    fn all_but(fixture: &Fixture, kept: PortBlock) -> Self {
        let staging = fixture.storage.store().join(".staging");
        fs::create_dir_all(&staging).expect("reservation directory");
        let owner = std::process::id().to_string();
        Self(
            PortBlock::macos_candidates(fixture.storage.port_allocation())
                .filter(|block| *block != kept)
                .map(|block| {
                    let marker = staging.join(format!("port-{}.reservation", block.base()));
                    std::os::unix::fs::symlink(&owner, &marker).expect("port reservation marker");
                    (block, marker)
                })
                .collect(),
        )
    }

    /// Release the claim on `block`, leaving every other one held.
    fn release(&mut self, block: PortBlock) {
        let index = self
            .0
            .iter()
            .position(|(claimed, _)| *claimed == block)
            .expect("the released block is claimed");
        let (_, marker) = self.0.swap_remove(index);
        fs::remove_file(&marker).expect("release a port reservation");
    }
}

impl Drop for PortClaims {
    fn drop(&mut self) {
        for (_, marker) in &self.0 {
            if let Err(error) = fs::remove_file(marker) {
                eprintln!("port reservation {}: {error}", marker.display());
            }
        }
    }
}

/// The `N` highest blocks in the fixture partition whose every port is free right now, highest
/// first.
fn highest_free_port_blocks<const N: usize>(fixture: &Fixture) -> [PortBlock; N] {
    let free: Vec<PortBlock> = PortBlock::macos_candidates(fixture.storage.port_allocation())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .filter(|block| {
            (block.base()..block.base() + block.size())
                .map(|port| TcpListener::bind((Ipv4Addr::LOCALHOST, port)))
                .collect::<std::io::Result<Vec<_>>>()
                .is_ok()
        })
        .take(N)
        .collect();
    free.try_into()
        .unwrap_or_else(|free: Vec<PortBlock>| panic!("{N} free macOS port blocks, not {free:?}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_adopt_initializes_mains_spotlight_marker() {
    let fixture = Fixture::with(Project::Plain);
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let main = service.path("main", false).await.expect("adopted main");
    let marker = fs::symlink_metadata(main.mount.join(cowshed_apfs::SPOTLIGHT_OPT_OUT))
        .expect("adopt writes main's Spotlight opt-out");
    assert!(marker.file_type().is_file());
    assert_eq!(marker.len(), 0);
}

/// Two workspaces recorded with one port block — what an allocator that missed an unfinished
/// intent's block handed out — are a `port-block-collision` doctor names with both, and
/// `doctor --repair` moves the newer one to a free block: its grants publish the new block and
/// its served `.cowshed/env` exports it.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_doctor_repair_moves_the_newer_of_two_workspaces_sharing_a_port_block() {
    let mut fixture = Fixture::with(Project::Plain);
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("install main gateway session");
    let topic = new_workspace(&mut service, "topic").await;
    let main_block = service
        .grants("main")
        .await
        .expect("main grants")
        .port_block
        .expect("main block");
    service
        .shutdown()
        .await
        .expect("stop runtime before planting");

    // Plant the duplicate the allocator once handed out: topic's grants name main's block.
    let layout = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("project layout");
    let image = layout
        .session_image(&WorkspaceName::session("topic").expect("topic"))
        .expect("topic image")
        .image()
        .to_owned();
    let mut metadata = cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&image)
        .expect("topic metadata");
    metadata.grants.port_block = Some(main_block);
    metadata
        .write_for_image(&image)
        .expect("plant the duplicate block");

    let findings = |stdout: &[u8], code: &str| -> Vec<String> {
        let doctor: serde_json::Value = serde_json::from_slice(stdout).expect("doctor --json");
        doctor["result"]["findings"]
            .as_array()
            .expect("doctor findings")
            .iter()
            .filter(|finding| finding["code"] == code)
            .map(|finding| finding["message"].as_str().unwrap_or_default().to_owned())
            .collect()
    };
    // Plain doctor opens as an inspection, the way the CLI opens it, and changes nothing.
    let mut inspection = ActorBridge::open_existing_at(
        &fixture.checkout,
        RecoveryScope::Inspect,
        fixture.storage.clone(),
    )
    .await
    .expect("open the project for inspection");
    // Doctor exits 5 on any error finding the scratch host has; only its report matters here.
    let (_, stdout, _) = run(&mut inspection, ["--json", "doctor"]).await;
    let collisions = findings(&stdout, "port-block-collision");
    assert!(
        matches!(collisions.as_slice(), [message]
            if message.contains("fixture/dispatch/topic") && message.contains("fixture/dispatch/main")),
        "doctor names both workspaces of the collision: {collisions:?}"
    );
    inspection.shutdown().await.expect("close the inspection");

    let mut service = fixture.reopen().await;
    let (_, stdout, _) = run(&mut service, ["--json", "doctor", "--repair"]).await;
    let repaired = findings(&stdout, "port-block-collision-repaired");
    assert_eq!(
        repaired.len(),
        1,
        "doctor --repair moves topic: {repaired:?}"
    );
    let moved = service
        .grants("topic")
        .await
        .expect("topic grants")
        .port_block
        .expect("topic block");
    assert!(
        !moved.overlaps(main_block),
        "{moved} still shares ports with {main_block}"
    );
    assert_eq!(moved.size(), main_block.size());
    assert_eq!(
        service
            .grants("main")
            .await
            .expect("main grants")
            .port_block,
        Some(main_block),
        "the older workspace keeps its block"
    );
    service
        .path("topic", false)
        .await
        .expect("topic is served from its repaired grants");
    let environment =
        fs::read_to_string(topic.join(".cowshed/env")).expect("topic's workspace environment");
    assert!(
        environment.contains(&format!("export COWSHED_PORT_BASE={}\n", moved.base())),
        "{environment}"
    );
    let (_, stdout, _) = run(&mut service, ["--json", "doctor"]).await;
    assert!(findings(&stdout, "port-block-collision").is_empty());
    service.shutdown().await.expect("shutdown runtime");
    fixture.stop_gateway().await;
}

/// A plain Git repository, adopted and served through a real gateway, with one workspace,
/// `topic`. Its exec runs with no shell hook. Push preserves the workspace branch as
/// `refs/cowshed/topic/heads/cowshed/topic` in main's repository and never moves main's
/// branch or working tree (02_workspaces.md, "`cowshed push`"). A checked land refuses while
/// the gateway is absent, then fast-forwards main once it is back (02_workspaces.md,
/// "`cowshed land`"). Adopt, new, exec and land never create a shell hook (02_workspaces.md,
/// "Workspace environment"; 15_capabilities.md, "Required evidence").
#[tokio::test(flavor = "multi_thread")]
async fn directory_plain_git_repository_adopts_clones_executes_and_lands_without_shell_hooks() {
    let started = std::time::Instant::now();
    let phase = |name| {
        eprintln!("plain-git fixture: {name} elapsed={:?}", started.elapsed());
    };
    phase("fixture");
    let mut fixture = Fixture::directory();
    for convention in [".envrc", ".envrc-local", "flake.nix", "devenv.nix"] {
        assert!(!fixture.checkout.join(convention).exists());
    }
    assert_eq!(
        fs::read_to_string(fixture.checkout.join(".cowshed.toml")).expect("fixture config"),
        FIXTURE_COWSHED_TOML,
        "the only cowshed configuration is the fixture's image cap"
    );
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
        .expect("serve plain repository");
    phase("fork topic");
    let (created, _, stderr) = run(&mut service, ["fork", "topic"]).await;
    assert_eq!(
        created.unwrap_or_else(|error| panic!(
            "plain clone: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let topic = service
        .path("topic", false)
        .await
        .expect("plain clone mount");
    phase("exec topic");
    let (executed, stdout, stderr) = run(
        &mut service,
        [
            "exec",
            "topic",
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

    phase("commit topic");
    assert_eq!(
        git_stdout(&topic.mount, &["remote"]),
        "main",
        "the current layout gives a workspace exactly one remote"
    );
    fs::write(topic.mount.join("feature.txt"), b"feature\n").expect("topic change");
    git(&topic.mount, &["add", "feature.txt"]);
    git(&topic.mount, &["commit", "-q", "-m", "feature"]);
    let head = git_stdout(&topic.mount, &["rev-parse", "HEAD"]);
    let main_before = git_stdout(&fixture.checkout, &["rev-parse", "refs/heads/main"]);

    phase("push topic");
    let (pushed, stdout, stderr) = run(&mut service, ["push", "topic"]).await;
    assert_eq!(
        pushed.unwrap_or_else(|error| panic!(
            "push through real service: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let preserved = "refs/cowshed/topic/heads/cowshed/topic";
    assert_eq!(
        String::from_utf8(stdout).expect("push answers UTF-8"),
        format!("{preserved}\t{head}\n")
    );
    assert_eq!(
        git_stdout(&fixture.checkout, &["rev-parse", preserved]),
        head
    );
    assert_eq!(
        git_stdout(
            &fixture.checkout,
            &["for-each-ref", "--format=%(refname)", "refs/cowshed/topic"]
        ),
        preserved,
        "the preservation ref is the only ref push leaves behind"
    );
    assert_eq!(
        git_stdout(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        main_before,
        "push never advances main's branch"
    );
    assert!(
        !fixture.checkout.join("feature.txt").exists(),
        "push never touches main's working tree"
    );

    // The destination expectation is a compare-and-swap: a preservation ref that already exists
    // refuses `missing` and stays exactly where it was.
    fs::write(topic.mount.join("feature.txt"), b"feature, revised\n").expect("topic revision");
    git(&topic.mount, &["commit", "-q", "-a", "-m", "revision"]);
    let (refused, stdout, _) = run(
        &mut service,
        ["push", "topic", "--expected-destination-head", "missing"],
    )
    .await;
    assert_eq!(
        refused
            .expect_err("an existing preservation ref is not missing")
            .code,
        ErrorCode::Conflict
    );
    assert!(stdout.is_empty(), "a refusal has no machine answer");
    assert_eq!(
        git_stdout(&fixture.checkout, &["rev-parse", preserved]),
        head
    );
    assert_eq!(
        git_stdout(
            &fixture.checkout,
            &["for-each-ref", "--format=%(refname)", "refs/cowshed/topic"]
        ),
        preserved,
        "a refused push leaves no staging ref behind"
    );

    // The checked land's payload is its own file, committed before the gateway stops.
    phase("commit checked-feature");
    fs::write(topic.mount.join("checked-feature.txt"), b"feature\n").expect("topic change");
    git(&topic.mount, &["add", "checked-feature.txt"]);
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
            "test -f checked-feature.txt",
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
    assert!(!fixture.checkout.join("checked-feature.txt").exists());

    phase("restart gateway");
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("restore project sessions after fixture gateway restart");
    phase("checked land");
    let (landed, _, stderr) = run(
        &mut service,
        [
            "land",
            "topic",
            "--no-retire",
            "--check",
            "test -f delivered.txt && test -f checked-feature.txt",
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
        fs::read(fixture.checkout.join("checked-feature.txt"))
            .expect("fast-forwarded main worktree"),
        b"feature\n"
    );
    assert_eq!(
        fs::read(fixture.checkout.join("delivered.txt")).unwrap(),
        b"generic\n"
    );
    for directory in [&fixture.checkout, &topic.mount] {
        assert!(
            !directory.join(".envrc").exists(),
            "adopt/fork/exec/land never creates a shell hook"
        );
        assert!(!directory.join(".envrc-local").exists());
        assert!(!directory.join(".gitignore").exists());
    }
    phase("shutdown runtime");
    service
        .shutdown()
        .await
        .expect("stop plain repository runtime");
    phase("drain gateway");
    fixture.stop_gateway().await;
    phase("done");
}

/// A `--git-worktree` workspace's checkout holds a `.git` pointer file, not a repository: its
/// excludes are main's, the common directory's. Its supervisor wrote cowshed's excludes into
/// the checkout's own `.git/info/exclude` as for a standalone clone, refused the pointer file
/// ('Git metadata path is not a real directory'), and so every verb that runs work in such a
/// workspace failed. The supervisor starts, writes the excludes into main's, and the
/// workspace's runtime state and the volume's own bookkeeping stay out of its status.
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_git_worktree_workspace_starts_its_supervisor_and_executes() {
    let mut fixture = Fixture::directory();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("serve the project");
    // Main's own supervisor, started by the adoption, already wrote cowshed's excludes into
    // main's file; take them out again, so only the worktree's supervisor can put them back.
    let main_exclude = fixture.checkout.join(".git/info/exclude");
    let stripped = fs::read_to_string(&main_exclude)
        .expect("main's exclude file")
        .lines()
        .filter(|line| ![".cowshed/", ".Trashes/", ".fseventsd/"].contains(line))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    fs::write(&main_exclude, stripped).expect("strip cowshed's excludes from main's");
    let (created, _, stderr) = run(&mut service, ["fork", "linked-topic", "--git-worktree"]).await;
    assert_eq!(
        created.unwrap_or_else(|error| panic!(
            "git-worktree clone: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let topic = service
        .path("linked-topic", false)
        .await
        .expect("git-worktree clone mount");
    assert!(
        fs::symlink_metadata(topic.mount.join(".git"))
            .expect("the checkout's .git")
            .is_file(),
        "a git-worktree checkout holds a pointer file"
    );
    let (executed, stdout, stderr) = run(
        &mut service,
        [
            "exec",
            "linked-topic",
            "--",
            "/bin/sh",
            "-c",
            "printf 'linked-sandbox\\n'",
        ],
    )
    .await;
    assert_eq!(
        executed.unwrap_or_else(|error| panic!(
            "git-worktree exec: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0,
        "{}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(stdout, b"linked-sandbox\n");
    let excludes = fs::read_to_string(fixture.checkout.join(".git/info/exclude"))
        .expect("main's exclude file");
    for pattern in [".cowshed/", ".Trashes/", ".fseventsd/"] {
        assert!(
            excludes.lines().any(|line| line == pattern),
            "main's excludes, which the worktree reads, name {pattern}: {excludes}"
        );
    }
    assert!(
        topic.mount.join(".cowshed").is_dir(),
        "the workspace holds cowshed's runtime state"
    );
    assert_eq!(
        git_stdout(&topic.mount, &["status", "--porcelain"]),
        "",
        "the worktree's runtime state and volume bookkeeping are excluded"
    );
    service.shutdown().await.expect("stop git-worktree runtime");
    fixture.stop_gateway().await;
}

/// `rm` stops the checkout's Nx daemon as `detach` does. A daemon detaches from the job that
/// started it, so stopping the supervisor leaves it running with its cwd on the checkout: the
/// unmount was refused for its whole grace, then forced, and the daemon outlived the removal.
#[tokio::test(flavor = "multi_thread")]
async fn directory_rm_stops_the_checkouts_nx_daemon_before_removal() {
    use std::os::unix::process::ExitStatusExt;

    let mut fixture = Fixture::directory();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("serve the project");
    let topic = new_workspace(&mut service, "daemon-topic").await;
    // A daemon as Nx's record names one: a live process whose socket accepts a connection, its
    // cwd on the checkout. The record is Nx's own state, never the user's work.
    let exclude = topic.join(".git/info/exclude");
    let mut excluded = fs::read(&exclude).expect("the workspace's own exclude file");
    excluded.extend_from_slice(b"/.nx/\n");
    fs::write(&exclude, excluded).expect("exclude Nx state");
    let socket = std::env::temp_dir().join(format!("cs-nxd-{}.sock", std::process::id()));
    let _ = fs::remove_file(&socket);
    let mut daemon = Command::new("/usr/bin/nc")
        .arg("-lkU")
        .arg(&socket)
        .current_dir(&topic)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("a listening stand-in daemon");
    eventually(|| socket.exists()).await;
    let record = topic.join(".nx/workspace-data/d/server-process.json");
    fs::create_dir_all(record.parent().expect("record directory")).expect("record directory");
    fs::write(
        &record,
        serde_json::json!({
            "processId": daemon.id(),
            "socketPath": socket,
            "nxVersion": "23.2.1",
        })
        .to_string(),
    )
    .expect("daemon record");

    // Forked seconds ago: forced past the age floor alone; nothing else here needs the force.
    let (removed, _, stderr) = run(&mut service, ["rm", "daemon-topic", "--force"]).await;
    assert_eq!(
        removed.unwrap_or_else(|error| panic!("rm: {error}; {}", String::from_utf8_lossy(&stderr))),
        0,
        "{}",
        String::from_utf8_lossy(&stderr)
    );
    let mut exited = None;
    eventually(|| {
        exited = daemon.try_wait().expect("poll the stand-in daemon");
        exited.is_some()
    })
    .await;
    assert_eq!(
        exited.and_then(|status| status.signal()),
        Some(libc::SIGTERM),
        "stopped as `nx daemon --stop` stops it; {}",
        String::from_utf8_lossy(&stderr)
    );
    let _ = fs::remove_file(&socket);
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// Processes a host shell started, which cowshed does not run: each `sleep` is backgrounded by an
/// `sh` that exits at once, so launchd adopts it rather than this test process (which serves
/// the fixture's supervisors). Killed when dropped.
struct HostProcesses(Vec<i32>);

impl HostProcesses {
    /// A `sleep` working in `cwd`, with `open` open as descriptor 3 when given.
    fn start(&mut self, cwd: &Path, open: Option<&Path>) -> i32 {
        let script = match open {
            Some(_) => "exec 3<\"$1\"; /bin/sleep 600 </dev/null >/dev/null 2>&1 & echo $!",
            None => "/bin/sleep 600 </dev/null >/dev/null 2>&1 & echo $!",
        };
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(script).arg("sh");
        if let Some(file) = open {
            command.arg(file);
        }
        let output = command
            .current_dir(cwd)
            .output()
            .expect("start a host process");
        assert!(output.status.success(), "{output:?}");
        let pid = String::from_utf8(output.stdout)
            .expect("a pid")
            .trim()
            .parse()
            .expect("a pid");
        self.0.push(pid);
        pid
    }
}

impl Drop for HostProcesses {
    fn drop(&mut self) {
        for pid in self.0.drain(..) {
            // SAFETY: kill takes plain integers; a pid that already exited answers ESRCH.
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
}

/// A host shell's `direnv exec <ws> nx …` runs on the mount with no supervisor and no job: a
/// process working in the workspace and one with a file of it open, both started outside
/// cowshed. `detach`, `rm` and `rm --force` refuse before anything stops, naming each with what
/// it holds and where, and offer no override. Once they are gone the workspace detaches.
#[tokio::test(flavor = "multi_thread")]
async fn directory_detach_and_rm_refuse_a_host_process_holding_the_mount() {
    let mut fixture = Fixture::directory();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("serve the project");
    let topic = new_workspace(&mut service, "held-topic")
        .await
        .canonicalize()
        .expect("the mount resolves");
    let head = topic.join(".git/HEAD");
    let mut host = HostProcesses(Vec::new());
    let working = host.start(&topic, None);
    let reading = host.start(&std::env::temp_dir(), Some(&head));
    let evidence = [
        (working, format!("working directory {}", topic.display())),
        (reading, format!("open file {}", head.display())),
    ];
    for args in [
        &["detach", "held-topic"][..],
        &["rm", "held-topic"],
        &["rm", "--force", "held-topic"],
    ] {
        let (refused, _, stderr) = run(&mut service, args.iter().copied()).await;
        let refusal = refused.expect_err(&format!(
            "{args:?} beneath a host process; {}",
            String::from_utf8_lossy(&stderr)
        ));
        assert_eq!(refusal.code, ErrorCode::Conflict, "{args:?}: {refusal}");
        for (pid, holds) in &evidence {
            assert!(
                refusal.message.contains(&format!("pid {pid} ("))
                    && refusal.message.contains(holds),
                "{args:?} does not name pid {pid} with {holds:?}: {}",
                refusal.message
            );
        }
        assert!(
            !refusal.hint.contains("--force"),
            "no override: {}",
            refusal.hint
        );
        assert!(head.is_file(), "{args:?} left the workspace mounted");
    }
    drop(host);
    let (detached, _, stderr) = run(&mut service, ["detach", "held-topic"]).await;
    let mut detached = detached.map(|_| ());
    for _ in 0..100 {
        if detached.is_ok() {
            break;
        }
        // SIGKILL is asynchronous: the killed processes leave the process table shortly.
        tokio::time::sleep(Duration::from_millis(50)).await;
        detached = run(&mut service, ["detach", "held-topic"])
            .await
            .0
            .map(|_| ());
    }
    detached.unwrap_or_else(|error| {
        panic!(
            "detach once nothing holds it: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )
    });
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// The checkout's ordinary Bun-installed patched Nx and its actual Node runtime.
fn repository_nx() -> (PathBuf, PathBuf) {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    let nx = PathBuf::from(manifest)
        .join("../../../../node_modules/nx")
        .canonicalize()
        .expect("install the pinned Bun-patched Nx before the sandbox consumer");
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

/// The fixture checkout's own Bun-installed Nx, canonical: the program each fixture runs.
fn fixture_nx(checkout: &Path) -> PathBuf {
    checkout
        .join("node_modules/nx")
        .canonicalize()
        .expect("the fixture's Bun-installed Nx")
}

/// A genuine sibling checkout of the suite's ordinary Bun-installed patched Nx product.
/// Its private dependency namespace stays beside that product; no repository package is borrowed.
fn developer_nx(fixture: &Fixture) -> (PathBuf, PathBuf) {
    let checkout = fixture._scratch.path().join("developer-checkout");
    fs::create_dir_all(checkout.join("packages")).unwrap();
    let nx = checkout.join("packages/nx");
    clone_tree(&fixture_nx(&fixture.checkout), &nx);
    let product = ["package.json", "dist/bin/nx.js"];
    let before = product.map(|file| fs::read(nx.join(file)).expect("maintained developer product"));
    fs::copy(
        repository_root().join("bunfig.toml"),
        nx.join("bunfig.toml"),
    )
    .expect("the existing Bun install configuration");
    let configuration =
        fs::read(nx.join("bunfig.toml")).expect("actual developer Bun configuration");
    eprintln!(
        "developer Nx bunfig.toml sha256={}",
        cowshed_core::api::dto::Sha256Digest::compute(&configuration)
    );
    let bun = host_bun();
    let context = Command::new(&bun)
        .args(["--print", "process.cwd()"])
        .env("PWD", &nx)
        .current_dir(&nx)
        .output()
        .expect("observe the installer package root");
    assert!(
        context.status.success(),
        "{}",
        String::from_utf8_lossy(&context.stderr)
    );
    assert_eq!(
        PathBuf::from(String::from_utf8(context.stdout).unwrap().trim()),
        nx,
        "the observed Bun root is the standalone developer package"
    );
    eprintln!("developer Nx installer root: {}", nx.display());
    // Production mode is frozen and never writes a missing lock. The ordinary producer must
    // generate this package's exact lock first, just as the covered-install fixture does.
    let lock_started = Instant::now();
    let locked = Command::new(&bun)
        .args([
            "install",
            "--lockfile-only",
            "--ignore-scripts",
            "--offline",
        ])
        .env("PWD", &nx)
        .current_dir(&nx)
        .output()
        .expect("produce the actual developer package lock");
    eprintln!(
        "developer Nx lock producer: {:?}; stdout: {}; stderr: {}",
        lock_started.elapsed(),
        String::from_utf8_lossy(&locked.stdout),
        String::from_utf8_lossy(&locked.stderr)
    );
    assert!(
        locked.status.success(),
        "the ordinary developer package lock producer failed"
    );
    assert!(
        nx.join("bun.lock").is_file(),
        "the normal producer wrote this package's own lock"
    );
    let began = Instant::now();
    let install = Command::new(&bun)
        .args(["install", "--production", "--ignore-scripts", "--offline"])
        .env("PWD", &nx)
        .current_dir(&nx)
        .output()
        .expect("install the developer package's exact production closure");
    eprintln!(
        "developer Nx production install: {:?}; stdout: {}; stderr: {}",
        began.elapsed(),
        String::from_utf8_lossy(&install.stdout),
        String::from_utf8_lossy(&install.stderr)
    );
    assert!(
        install.status.success(),
        "the real developer package install failed"
    );
    // Nx's unchanged CLI resolves its global namespace four levels above dist/bin.
    // Produce that genuine workspace binding with Bun, never a hand-made self alias.
    let package_lock = fs::read(nx.join("bun.lock")).expect("the package's own production lock");
    fs::write(
        checkout.join("package.json"),
        serde_json::to_vec(&serde_json::json!({
            "name": "fixture-developer-nx", "private": true,
            "workspaces": ["packages/nx"], "dependencies": { "nx": "workspace:*" }
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(checkout.join("bunfig.toml"), &configuration).unwrap();
    for args in [
        [
            "install",
            "--lockfile-only",
            "--ignore-scripts",
            "--offline",
        ],
        ["install", "--production", "--ignore-scripts", "--offline"],
    ] {
        let began = Instant::now();
        let result = Command::new(&bun)
            .args(args)
            .env("PWD", &checkout)
            .current_dir(&checkout)
            .output()
            .expect("produce the developer checkout's ordinary workspace namespace");
        eprintln!(
            "developer Nx workspace producer {args:?}: {:?}; stdout: {}; stderr: {}",
            began.elapsed(),
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            result.status.success(),
            "the actual workspace namespace producer failed"
        );
        assert!(
            checkout.join("bun.lock").is_file(),
            "the root producer wrote its own lock"
        );
    }
    assert_eq!(
        fs::read(nx.join("bun.lock")).unwrap(),
        package_lock,
        "the root producer preserves the package's own production lock"
    );
    assert_eq!(checkout.join("node_modules/nx").canonicalize().unwrap(), nx);
    eprintln!(
        "developer Nx workspace bun.lock sha256={}",
        cowshed_core::api::dto::Sha256Digest::compute(
            &fs::read(checkout.join("bun.lock")).unwrap()
        )
    );
    for (file, bytes) in product.into_iter().zip(before) {
        let after = fs::read(nx.join(file)).expect("the installed developer product");
        assert_eq!(
            after, bytes,
            "ordinary dependency installation must not change the maintained Nx {file}"
        );
        eprintln!(
            "developer Nx {file} sha256={}",
            cowshed_core::api::dto::Sha256Digest::compute(&after)
        );
    }
    let lock =
        fs::read(nx.join("bun.lock")).expect("ordinary Bun wrote the developer closure lock");
    eprintln!(
        "developer Nx bun.lock sha256={}",
        cowshed_core::api::dto::Sha256Digest::compute(&lock)
    );
    assert!(
        fs::metadata(nx.join("dist/bin/nx.js"))
            .expect("the real developer Nx program")
            .permissions()
            .mode()
            & 0o111
            != 0,
        "the maintained developer program remains executable"
    );
    (checkout, nx)
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

/// The repository root: the checkout the test binary was built from.
fn repository_root() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    PathBuf::from(manifest)
        .join("../../../..")
        .canonicalize()
        .expect("the repository root")
}

fn host_bun() -> PathBuf {
    let observed = Command::new("bun")
        .args(["--print", "process.execPath"])
        .output()
        .expect("observe the actual pinned Bun executable");
    assert!(
        observed.status.success(),
        "{}",
        String::from_utf8_lossy(&observed.stderr)
    );
    let executable = PathBuf::from(
        String::from_utf8(observed.stdout)
            .expect("Bun executable path is UTF-8")
            .trim(),
    )
    .canonicalize()
    .expect("the observed pinned Bun executable resolves");
    eprintln!("fixture Bun executable: {}", executable.display());
    executable
}

/// The installed dependency closure of `nx`, walked through the repository's own Bun store:
/// each package's dependencies resolve to the sibling Bun linked beside it in the same store
/// entry, so the walk yields the exact instances the repository runs, platform binaries for
/// this host included and other platforms' absent, as Bun's linker left them.
fn nx_install_closure(nx: &Path) -> std::collections::BTreeMap<String, PathBuf> {
    let mut members = std::collections::BTreeMap::new();
    let mut queue = vec![nx.to_path_buf()];
    while let Some(directory) = queue.pop() {
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(directory.join("package.json")).expect("an installed package manifest"),
        )
        .expect("an installed package manifest parses");
        let name = manifest["name"]
            .as_str()
            .expect("an installed package names itself")
            .to_owned();
        if members.insert(name.clone(), directory.clone()).is_some() {
            continue;
        }
        // The store's own neighbourhood for this package: <store>/node_modules/<name>, so
        // dependencies are that node_modules directory's other entries, canonicalized
        // through Bun's links to the store entries that hold them.
        let neighbourhood = if name.starts_with('@') {
            directory
                .parent()
                .and_then(Path::parent)
                .expect("a scoped package sits in its store neighbourhood")
        } else {
            directory
                .parent()
                .expect("a package sits in its store neighbourhood")
        };
        for field in ["dependencies", "optionalDependencies", "peerDependencies"] {
            let Some(dependencies) = manifest[field].as_object() else {
                continue;
            };
            for dependency in dependencies.keys() {
                let sibling = neighbourhood.join(dependency);
                if sibling.exists() {
                    queue.push(
                        sibling
                            .canonicalize()
                            .expect("an installed dependency resolves through Bun's link"),
                    );
                }
            }
        }
    }
    members
}

/// Clone the repository's Bun-installed Nx namespace into `checkout`, offline.
///
/// This is a clone, not an install, and the difference is deliberate. The fixture used to
/// resolve Nx from the registry with `bun install --offline`, which reads Bun's local
/// cache — warm on a developer host because the repository's own install ran there, cold
/// on a runner whose checkout was cloned with its `node_modules` already seeded, where it
/// fails with `no cached manifest for "nx"`. No offline warmup exists (the repository
/// root's `bun install --offline --frozen-lockfile` fails cold too: Bun does not populate
/// its cache from a seeded `node_modules`), and seeding a cache from the fixture would
/// add a host cache path, which 08_testing.md ("Fixtures") forbids. A workspace-member
/// install does work offline, but leaves `node_modules/nx` canonicalizing into a plain
/// `packages/` tree, which the shared read-grant traversal (`nx_reads.rs`) does not
/// accept: a consumer's Nx must sit in its checkout's `.bun` namespace, with eligible
/// dependencies shared through the global store. Bun cannot be made to produce that
/// namespace from local packages either — it copies `file:` members as leaves and strips
/// their nested `node_modules`, so their dependencies never link (measured: one missing
/// edge, `figures` → `escape-string-regexp`, in every such variant). So the namespace Bun
/// already produced for the repository is cloned whole: every `.bun` store entry Nx's
/// closure reaches, the `node_modules/nx` link and the `.bin` entries, in the exact shape
/// the repository's install left them (`developer_nx` clones the installed product the
/// same way). What the fixture stops proving is that Bun can install Nx from a registry —
/// the repository's own install in CI setup and the nx-plugin suites that execute the
/// installed Nx carry that proof; what it keeps proving is everything downstream of an
/// installed Nx, with byte-identical product files.
fn clone_repository_nx_install(checkout: &Path, nx: &Path) {
    clone_nx_namespace(
        &checkout_template_directory(),
        &checkout.join("node_modules"),
        |installed| write_repository_nx_install(installed, nx),
    );
}

/// Every checkout shape clones one published install; only its first writer crosses volumes.
fn clone_nx_namespace(templates: &Path, destination: &Path, write: impl FnOnce(&Path)) {
    let installed = materialized_template(templates, "nx-install", write);
    clone_tree(&installed, destination);
}

#[test]
fn directory_nx_install_is_materialized_once_across_checkout_templates() {
    let scratch = scratch_directory::ScratchRoot::new("nx-install-template").expect("scratch root");
    let templates = scratch.path().join("templates");
    fs::create_dir(&templates).unwrap();
    let source = scratch.path().join("installed");
    fs::create_dir_all(source.join("nx")).unwrap();
    fs::write(source.join("nx/product"), b"immutable installed bytes").unwrap();
    std::os::unix::fs::symlink("nx/product", source.join("entry")).unwrap();
    let copies = std::cell::Cell::new(0);
    let write = |destination: &Path| {
        copies.set(copies.get() + 1);
        assert_eq!(
            copies.get(),
            1,
            "a second checkout template must not reread or recopy the installed namespace"
        );
        clone_tree(&source, destination);
    };
    let first = scratch.path().join("nx-declared");
    clone_nx_namespace(&templates, &first, write);
    // The source is no longer available. The second checkout must clone the already-published
    // immutable namespace, not rediscover or copy the repository's installation again.
    fs::remove_dir_all(&source).unwrap();
    let second = scratch.path().join("build-nx");
    clone_nx_namespace(&templates, &second, write);
    assert_eq!(copies.get(), 1);
    for checkout in [first, second] {
        assert_eq!(
            fs::read(checkout.join("entry")).unwrap(),
            b"immutable installed bytes"
        );
        assert_eq!(
            fs::read_link(checkout.join("entry")).unwrap(),
            Path::new("nx/product")
        );
    }
}

fn write_repository_nx_install(installed: &Path, nx: &Path) {
    let bun_root = nx
        .ancestors()
        .find(|ancestor| ancestor.file_name() == Some(std::ffi::OsStr::new(".bun")))
        .expect("the repository's Nx sits in its Bun install namespace");
    let bun_destination = installed.join(".bun");
    fs::create_dir_all(&bun_destination).unwrap();
    // The store entries the closure reaches, by entry directory name: cloning an entry
    // clones the package and the dependency links Bun left beside it in one move. The set
    // is closed along the entries' own relative links, so a cloned entry never points at
    // a sibling that was not cloned with it. Packages Bun shares through its global store
    // are already installed on the host and stay there — the read-grant traversal expects
    // them in the store, not in the checkout.
    let mut entries = std::collections::BTreeMap::new();
    for directory in nx_install_closure(nx).values() {
        let Some(entry) = directory
            .ancestors()
            .find(|ancestor| ancestor.parent() == Some(bun_root))
        else {
            continue;
        };
        entries.insert(
            entry
                .file_name()
                .expect("a store entry is named")
                .to_string_lossy()
                .into_owned(),
            entry.to_path_buf(),
        );
    }
    let mut pending: Vec<PathBuf> = entries.values().cloned().collect();
    while let Some(entry) = pending.pop() {
        let mut stack = vec![entry];
        while let Some(directory) = stack.pop() {
            // A store entry another process is pruning mid-walk simply contributes no
            // links; a genuinely missing dependency still fails the assertions below.
            let Ok(items) = fs::read_dir(&directory) else {
                continue;
            };
            for item in items {
                let Ok(item) = item else { continue };
                let path = item.path();
                let Ok(file_type) = item.file_type() else {
                    continue;
                };
                if file_type.is_symlink() {
                    let Ok(target) = fs::read_link(&path) else {
                        continue;
                    };
                    if target.is_absolute() {
                        continue;
                    }
                    // Resolve the link lexically against its own directory, then take the
                    // store entry that holds the resolved path, if it is one of Bun's.
                    let mut resolved = path
                        .parent()
                        .expect("a Bun store entry link has a directory")
                        .to_path_buf();
                    for component in target.components() {
                        match component {
                            std::path::Component::ParentDir => {
                                resolved.pop();
                            }
                            std::path::Component::Normal(part) => resolved.push(part),
                            _ => {}
                        }
                    }
                    let Some(linked) = resolved
                        .ancestors()
                        .find(|ancestor| ancestor.parent() == Some(bun_root))
                    else {
                        continue;
                    };
                    let name = linked
                        .file_name()
                        .expect("a store entry is named")
                        .to_string_lossy()
                        .into_owned();
                    if let std::collections::btree_map::Entry::Vacant(vacant) = entries.entry(name)
                    {
                        vacant.insert(linked.to_path_buf());
                        pending.push(linked.to_path_buf());
                    }
                } else if file_type.is_dir() {
                    stack.push(path);
                }
            }
        }
    }
    for (name, entry) in &entries {
        // A link Bun left for a package this host never installed names an entry that
        // does not exist; there is nothing to clone for it, and the traversal below
        // asserts on whatever a consumer actually reaches.
        if entry.exists() {
            clone_or_copy_tree(entry, &bun_destination.join(name));
        }
    }
    // The links Bun's install leaves at the namespace root, recreated relative so they
    // survive the cloning every fixture performs: `node_modules/nx` and its `.bin` entries.
    let nx_entry = nx
        .ancestors()
        .find(|ancestor| ancestor.parent() == Some(bun_root))
        .expect("the repository's Nx sits in a Bun store entry");
    std::os::unix::fs::symlink(
        Path::new(".bun")
            .join(nx_entry.file_name().expect("the Nx store entry is named"))
            .join("node_modules/nx"),
        installed.join("nx"),
    )
    .expect("the namespace's Nx link");
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(nx.join("package.json")).expect("the repository's Nx manifest"),
    )
    .expect("the repository's Nx manifest parses");
    let bins: Vec<(String, String)> = match &manifest["bin"] {
        serde_json::Value::String(path) => vec![("nx".to_owned(), path.clone())],
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(name, path)| {
                (
                    name.clone(),
                    path.as_str().expect("a bin entry is a path").to_owned(),
                )
            })
            .collect(),
        _ => Vec::new(),
    };
    fs::create_dir_all(installed.join(".bin")).unwrap();
    for (name, path) in bins {
        std::os::unix::fs::symlink(
            Path::new("../nx").join(path),
            installed.join(".bin").join(&name),
        )
        .unwrap_or_else(|error| panic!("the namespace's .bin/{name} link: {error}"));
    }
}

/// Clone `source`'s tree to `destination`, copying when the two live on different volumes
/// (the repository's store and the fixture's scratch need not share one, where `clonefile`
/// cannot reach). Links are recreated as links, so the installed namespace keeps the exact
/// shape Bun produced.
fn clone_or_copy_tree(source: &Path, destination: &Path) {
    // An entry Bun itself keeps as a link into its global store stays that link: cloning
    // its target into the checkout would strand the relative links inside it, which are
    // written against the store's layout, and the traversal expects shared packages there.
    if fs::symlink_metadata(source)
        .expect("a closure member entry")
        .file_type()
        .is_symlink()
    {
        let target = fs::read_link(source).expect("a closure member link");
        std::os::unix::fs::symlink(&target, destination).expect("recreate a member link");
        return;
    }
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        fn clonefile(src: *const libc::c_char, dst: *const libc::c_char, flags: u32)
        -> libc::c_int;
    }
    let path = |path: &Path| std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path");
    let (from, to) = (path(source), path(destination));
    // SAFETY: both arguments are NUL-terminated strings that outlive the call.
    if unsafe { clonefile(from.as_ptr(), to.as_ptr(), 0) } == 0 {
        return;
    }
    fn copy(source: &Path, destination: &Path) {
        let metadata = fs::symlink_metadata(source).expect("a closure member entry");
        if metadata.file_type().is_symlink() {
            let target = fs::read_link(source).expect("a closure member link");
            std::os::unix::fs::symlink(&target, destination).expect("recreate a member link");
        } else if metadata.is_dir() {
            fs::create_dir(destination).expect("a closure member directory");
            for entry in fs::read_dir(source).expect("read a closure member directory") {
                let entry = entry.expect("a closure member entry");
                copy(&entry.path(), &destination.join(entry.file_name()));
            }
        } else {
            fs::copy(source, destination).expect("copy a closure member file");
        }
    }
    copy(source, destination);
}

/// An Nx project whose `a:build` writes `a/generated.txt` and whose `a:test` reads it (its
/// default inputs include it). `declared` says whether `a:build` declares that output. Written,
/// not committed: the caller commits once it has written all it adds.
fn write_nx_project(checkout: &Path, nx: &Path, node: &Path, declared: bool) {
    // A command a workspace runs by name, as the rebase carry's task-hash probe runs `node`,
    // resolves through its shim directory, which travels with every clone (03_caches.md).
    fs::create_dir_all(checkout.join(".cowshed/bin")).unwrap();
    std::os::unix::fs::symlink(node, checkout.join(".cowshed/bin/node")).unwrap();
    let node = node.display();
    let outputs = if declared {
        r#"["{projectRoot}/generated.txt"]"#
    } else {
        "[]"
    };
    let root = repository_root();
    fs::copy(root.join("bunfig.toml"), checkout.join("bunfig.toml"))
        .expect("the repository's bunfig.toml");
    fs::write(
        checkout.join("package.json"),
        r#"{"name":"fixture","private":true}"#,
    )
    .unwrap();
    // The genuine fixture uses the normal daemon: checks must share the keeper's warm graph,
    // not race its detached startup while bypassing it (16_build_volumes.md, daemon stays on).
    fs::write(
        checkout.join("nx.json"),
        r#"{"cacheDirectory":".nx/cache"}"#,
    )
    .unwrap();
    fs::write(checkout.join(".gitignore"), "node_modules\n.nx\n.cowshed\n").unwrap();
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
    let started = std::time::Instant::now();
    clone_repository_nx_install(checkout, nx);
    eprintln!(
        "cloned the repository's Bun-installed Nx namespace in {}: {:?}",
        checkout.display(),
        started.elapsed()
    );
    for file in ["package.json", "dist/bin/nx.js"] {
        assert_eq!(
            fs::read(checkout.join("node_modules/nx").join(file))
                .expect("the fixture's installed Nx"),
            fs::read(nx.join(file)).expect("the repository's installed Nx"),
            "the fixture's Nx {file} must match the repository's patched product",
        );
    }
}

/// A cloned Nx project must retain the real package-manager executable entry the keeper runs.
fn assert_nx_executable(checkout: &Path) {
    let entry = checkout.join("node_modules/.bin/nx");
    let target = fs::read_link(&entry).expect("Nx executable link survives image cloning");
    assert!(
        target.is_relative(),
        "Bun's Nx entry {} must be relative to survive image cloning",
        target.display()
    );
    let installed = checkout
        .join("node_modules/nx/dist/bin/nx.js")
        .canonicalize()
        .expect("the installed real Nx program resolves");
    assert!(
        installed.starts_with(checkout.canonicalize().expect("the checkout resolves")),
        "the installed Nx program lives inside the checkout"
    );
    assert_eq!(
        entry
            .canonicalize()
            .expect("the keeper's Nx executable resolves"),
        installed,
    );
    assert_ne!(
        fs::metadata(&entry)
            .expect("the keeper's Nx executable exists")
            .permissions()
            .mode()
            & 0o111,
        0,
        "the real Nx program is executable, not a diagnostic stand-in",
    );
}

/// Every controller commitment sealed under `telemetry`.
fn controller_commitments(telemetry: &Path) -> Vec<cowshed_core::api::dto::ControllerCommitment> {
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
                found.push(commitment);
            }
        }
    }
    found
}

/// The fixture's check running `targets` across its projects with the repository's Nx and Node.
fn nx_run_many(node: &Path, targets: &str) -> String {
    format!(
        "{} node_modules/nx/dist/bin/nx.js run-many -t {targets}",
        node.display()
    )
}

fn land_adoptions(telemetry: &Path) -> Vec<cowshed_core::api::dto::LandAdoptionCommitment> {
    controller_commitments(telemetry)
        .into_iter()
        .filter_map(|value| match value {
            cowshed_core::api::dto::ControllerCommitment::LandAdoption(value) => Some(value),
            _ => None,
        })
        .collect()
}

fn assert_land_outcome(telemetry: &Path, report: &LandedReport) {
    let build = &report.build_volume;
    let outcomes = controller_commitments(telemetry)
        .into_iter()
        .filter_map(|value| match value {
            cowshed_core::api::dto::ControllerCommitment::LandOutcome(value) => Some(value),
            _ => None,
        })
        .filter(|outcome| outcome.landed_head == report.landed_head)
        .collect::<Vec<_>>();
    let [outcome] = outcomes.as_slice() else {
        panic!(
            "one durable outcome for landed head {}: {outcomes:?}",
            report.landed_head
        );
    };
    assert_eq!(
        outcome.seeded,
        matches!(build.seed, cowshed_core::api::dto::LandSeed::Frozen)
    );
    assert_eq!(
        outcome.skip.as_ref(),
        match &build.adoption {
            Adoption::Adopted { .. } => None,
            Adoption::Skipped { reason } => Some(reason),
        }
    );
}

/// A land whose checks build `a` and then test it, from a topic forked off main: the build's
/// output reaches the test in the topic, where the build wrote it. In main, after adoption, the
/// build hits the cache and restores only what it declares. Answers the adopted volume's step-7
/// check, the `landAdoption` commitments the land sealed and the topic's landed head.
async fn land_nx_project(
    declared: bool,
) -> (
    cowshed_core::api::dto::AdoptionCheck,
    Vec<cowshed_core::api::dto::LandAdoptionCommitment>,
    String,
) {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Nx {
        nx: &nx,
        node: &node,
        declared,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let (created, _, stderr) = run(&mut service, ["fork", "topic"]).await;
    assert_eq!(
        created.unwrap_or_else(|error| panic!(
            "new topic: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let topic = service.path("topic", false).await.expect("mounted topic");
    assert_nx_executable(&topic.mount);
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
    let build = nx_run_many(&node, "build");
    let test = nx_run_many(&node, "test");
    // The undeclared output is untracked work in the topic, which a retire refuses.
    let report = land(&mut service, "topic", false, &[&build, &test]).await;
    eprintln!("land report: {report:?}");
    assert_eq!(report.build_volume.seed, LandSeed::Frozen, "{report:?}");
    let cowshed_core::api::dto::Adoption::Adopted { check: rerun, .. } =
        &report.build_volume.adoption
    else {
        panic!("the land adopted no build volume: {report:?}");
    };
    let rerun = rerun.clone();
    assert_eq!(
        fs::read(fixture.checkout.join("a/generated.txt")).ok(),
        declared.then(|| b"generated\n".to_vec()),
        "only declared Nx outputs are materialized in main"
    );
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
    (
        rerun,
        land_adoptions(fixture.storage.telemetry()),
        landed_head,
    )
}

/// A task output Nx is not told about is a 2b miss (16_build_volumes.md, Land step 7), and each
/// miss is a durable `landAdoption` commitment naming the task (13_telemetry.md). Declaring the
/// output makes the miss, and the record, disappear. The two projects are independent, each on
/// its own scratch store, so they land side by side.
// Admitted to the 60 s lifecycle class below its 3× rule and kept paired, because split halves
// cost more; its samples are load-dependent (08_testing.md, Integration tests).
#[tokio::test(flavor = "multi_thread")]
async fn directory_an_undeclared_nx_output_lands_as_a_land_adoption_commitment_substrate_lifecycle()
{
    let undeclared = tokio::spawn(land_nx_project(false));
    let declared = tokio::spawn(land_nx_project(true));
    let (rerun, adoptions, landed_head) = undeclared.await.expect("undeclared-output land");
    assert_eq!(rerun.hits, 2, "{rerun:?}");
    assert_eq!(
        rerun
            .misses
            .iter()
            .map(|miss| miss.task.as_str())
            .collect::<Vec<_>>(),
        ["a:test"],
        "only the undeclared output's task misses: {rerun:?}"
    );
    assert!(
        rerun.misses[0].reason.contains("differs from checked hash"),
        "{rerun:?}"
    );
    assert!(
        !rerun.misses[0].hash.is_empty(),
        "the finding retains the checked hash"
    );
    assert_eq!(rerun.unattributed, [], "{rerun:?}");
    assert_eq!(
        rerun.failed,
        [],
        "a miss is reported without executing a:test: {rerun:?}"
    );
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

    let (rerun, adoptions, _) = declared.await.expect("declared-output land");
    assert_eq!(
        rerun,
        cowshed_core::api::dto::AdoptionCheck {
            hits: 3,
            ..Default::default()
        },
        "a declared output restores in main and every task hits"
    );
    assert_eq!(adoptions, [], "a hit leaves no landAdoption commitment");
}

/// Checks run once before delivery. A non-Nx shell and an uncacheable Nx executor both leave
/// witnesses only in the landing workspace; the cache-only adoption still restores a:build.
#[tokio::test(flavor = "multi_thread")]
async fn directory_adoption_never_reexecutes_shell_checks_or_uncacheable_tasks_substrate_lifecycle()
{
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Nx {
        nx: &nx,
        node: &node,
        declared: true,
    });
    let project_path = fixture.checkout.join("a/project.json");
    let mut project: serde_json::Value =
        serde_json::from_slice(&fs::read(&project_path).unwrap()).unwrap();
    project["targets"]["test"]["cache"] = serde_json::json!(false);
    project["targets"]["test"]["options"]["command"] = serde_json::json!(format!(
        "{} -e \"require('fs').appendFileSync('.cowshed/task-runs','once\\n')\"",
        node.display()
    ));
    fs::write(&project_path, serde_json::to_vec(&project).unwrap()).unwrap();
    git(&fixture.checkout, &["add", "a/project.json"]);
    git(
        &fixture.checkout,
        &["commit", "-q", "-m", "make validation uncacheable"],
    );
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("a/src.txt"), b"landed input\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "change input"]);
    let shell = "printf 'once\\n' >> .cowshed/shell-runs";
    let test = format!("{} node_modules/nx/dist/bin/nx.js test a", node.display());
    let report = land(&mut service, "topic", false, &[shell, &test]).await;
    let Adoption::Adopted { check, .. } = &report.build_volume.adoption else {
        panic!("the build volume was not adopted: {report:?}");
    };
    assert_eq!(
        fs::read(topic.join(".cowshed/shell-runs")).unwrap(),
        b"once\n"
    );
    assert_eq!(
        fs::read(topic.join(".cowshed/task-runs")).unwrap(),
        b"once\n"
    );
    assert!(
        !fixture.checkout.join(".cowshed/shell-runs").exists(),
        "the shell check never runs in main"
    );
    assert!(
        !fixture.checkout.join(".cowshed/task-runs").exists(),
        "the uncacheable executor never runs in main"
    );
    assert_eq!(
        fs::read(fixture.checkout.join("a/generated.txt")).unwrap(),
        b"generated\n"
    );
    assert_eq!(
        check.hits, 1,
        "only the dependency's cached output is restored: {check:?}"
    );
    assert_eq!(
        check
            .misses
            .iter()
            .map(|miss| miss.task.as_str())
            .collect::<Vec<_>>(),
        ["a:test"]
    );
    assert!(
        check.misses[0].reason.contains("not cacheable"),
        "{check:?}"
    );
    assert!(
        !check.misses[0].hash.is_empty(),
        "uncacheable findings retain the checked hash"
    );
    assert_eq!(check.failed, [], "{check:?}");
    assert_eq!(
        check.unattributed.len(),
        1,
        "the shell has no Nx summary: {check:?}"
    );
    assert_eq!(check.unattributed[0].check, shell);
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// A real-Nx test that fails mid-way leaves no Nx daemon under its root. A daemon started in a
/// fixture's checkout -- by a shed's keeper, by a check, by hand -- detaches from whatever started
/// it, so the failed test's unwinding never reached it, and it kept running in the deleted tree.
/// The fixture's directory root ends every process working in its tree before removal, without
/// acquiring any disk-image capability.
#[test]
fn directory_a_failed_nx_test_leaves_no_daemon_under_its_root_substrate_lifecycle() {
    let (nx, node) = repository_nx();
    let started = std::cell::RefCell::new(None);
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let fixture = Fixture::with_directory(Project::Nx {
            nx: &nx,
            node: &node,
            declared: true,
        });
        let mut start = Command::new(&node);
        start
            .args(["node_modules/nx/dist/bin/nx.js", "daemon", "--start"])
            .current_dir(&fixture.checkout);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("NX_") || key == "CI" {
                start.env_remove(key);
            }
        }
        let output = start.output().expect("node runs");
        assert!(
            output.status.success(),
            "nx daemon --start: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let record = fixture
            .checkout
            .join(".nx/workspace-data/d/server-process.json");
        let record: serde_json::Value =
            serde_json::from_slice(&fs::read(&record).expect("the daemon's record"))
                .expect("the record is JSON");
        *started.borrow_mut() = Some((
            fixture._scratch.path().to_path_buf(),
            record["processId"].clone(),
        ));
        panic!("the test fails while its fixture's Nx daemon runs");
    }));
    assert!(failed.is_err(), "the test failed");
    let (root, daemon) = started
        .into_inner()
        .expect("the daemon started before the test failed");
    let left = scratch_directory::processes::processes_in(|cwd| cwd.starts_with(&root))
        .expect("list the processes working under the root");
    assert_eq!(
        left,
        [],
        "daemon {daemon} or another process outlived the test"
    );
    assert!(!root.exists(), "{} was removed", root.display());
}

/// The real Rust toolchain that drives this test, each program followed to its real file.
struct RustToolchain {
    cargo: PathBuf,
    rustc: PathBuf,
    rustdoc: PathBuf,
}

impl RustToolchain {
    /// `cargo` is canonical `CARGO`, which Cargo and nextest set for every test to the real
    /// driver running it, so no installed `cargo` link, an earlier install's included, can stand
    /// in (05_gateway.md, "Cargo reads its holder's jobserver"). `rustc` and `rustdoc` resolve by
    /// Cargo's own rule: `RUSTC`/`RUSTDOC` if set, else the first executable on an absolute
    /// `PATH` entry, canonicalized. Nothing installed answers to `rustc` or `rustdoc`. Canonical
    /// `CARGO` need not sit beside them: a Nix toolchain's `bin/cargo` is a wrapper that execs
    /// the Cargo of another store path.
    fn of_this_test() -> Self {
        let cargo = fs::canonicalize(
            std::env::var_os("CARGO").expect("Cargo publishes its real compiler driver"),
        )
        .expect("real Cargo driver");
        let toolchain = Self {
            cargo,
            rustc: cargo_tool("RUSTC", "rustc"),
            rustdoc: cargo_tool("RUSTDOC", "rustdoc"),
        };
        eprintln!(
            "fixture Rust toolchain: cargo={}, rustc={}, rustdoc={}",
            toolchain.cargo.display(),
            toolchain.rustc.display(),
            toolchain.rustdoc.display()
        );
        toolchain
    }

    /// Each program under the name a checkout's shim directory gives it.
    fn programs(&self) -> [(&'static str, &Path); 3] {
        [
            ("cargo", &self.cargo),
            ("rustc", &self.rustc),
            ("rustdoc", &self.rustdoc),
        ]
    }
}

/// The real file of the tool Cargo runs as `program`: `variable` if set, else the first
/// executable `program` on an absolute `PATH` entry.
fn cargo_tool(variable: &str, program: &str) -> PathBuf {
    let named = std::env::var_os(variable).map(PathBuf::from).or_else(|| {
        std::env::split_paths(&std::env::var_os("PATH").expect("the test's PATH"))
            .filter(|directory| directory.is_absolute())
            .map(|directory| directory.join(program))
            .find(|candidate| {
                candidate.metadata().is_ok_and(|metadata| {
                    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                })
            })
    });
    let named = named.unwrap_or_else(|| panic!("{variable} or an executable {program} on PATH"));
    fs::canonicalize(&named)
        .unwrap_or_else(|error| panic!("{program} {}: {error}", named.display()))
}

/// The fixture repository declares its own toolchain, as a real one does through its shell:
/// cowshed links no Rust program (15_capabilities.md, "Bootstrap executables"). Like the Nx
/// fixture's `node`, the programs resolve through the checkout's shim directory, which travels
/// with every clone (03_caches.md), so every job runs plain `cargo` and build-state discovery
/// asks that same Cargo for the target directory.
fn declare_rust(checkout: &Path, rust: &RustToolchain) {
    let bin = checkout.join(".cowshed/bin");
    fs::create_dir_all(&bin).unwrap();
    for (name, program) in rust.programs() {
        std::os::unix::fs::symlink(program, bin.join(name)).unwrap();
    }
}

/// A Cargo workspace of two path crates, `fixture-app` depending on `fixture-base`: libraries
/// only, so building them links nothing. Its lockfile comes from the same Cargo the sandbox
/// runs, so no build rewrites a tracked file.
fn write_cargo_workspace(checkout: &Path, rust: &RustToolchain) {
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
    let lockfile = Command::new(&rust.cargo)
        .args(["generate-lockfile", "--offline"])
        .current_dir(checkout)
        .env("RUSTC", &rust.rustc)
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
fn write_build_project(checkout: &Path, nx: Option<(&Path, &Path)>, rust: Option<&RustToolchain>) {
    if let Some((nx, node)) = nx {
        write_nx_project(checkout, nx, node, true);
    }
    if let Some(rust) = rust {
        declare_rust(checkout, rust);
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

/// The moment `cowshed fork` returns, `fork`'s build state resolves into a volume of its own and
/// never into the one `target` links: a process the fork starts at once must not open the
/// target's task database, or the target's next land cannot adopt.
fn assert_on_own_build_volume(fork: &Path, target: &Path) {
    let resolve = |path: PathBuf| {
        path.canonicalize()
            .unwrap_or_else(|error| panic!("{} resolves: {error}", path.display()))
    };
    let own = resolve(fork.join(".cowshed/build"));
    assert_ne!(
        own,
        resolve(target.join(".cowshed/build")),
        "{}",
        fork.display()
    );
    assert!(
        resolve(fork.join("target")).starts_with(&own),
        "{}/target resolves outside its own volume {}",
        fork.display(),
        own.display()
    );
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
    succeed(service, ["fork", name]).await;
    let mount = service
        .path(name, false)
        .await
        .unwrap_or_else(|error| panic!("{name} is mounted: {error}"))
        .mount;
    if mount.join("nx.json").is_file() {
        assert_nx_executable(&mount);
    }
    mount
}

/// `cowshed land --json <workspace>` with `checks`, retiring the workspace unless `retire` is
/// false; answers the report the CLI printed.
async fn land(
    service: &mut ActorBridge,
    workspace: &str,
    retire: bool,
    checks: &[&str],
) -> LandedReport {
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
    match envelope.result().expect("a successful land report") {
        LandReport::Landed(report) => report.clone(),
        LandReport::Delivered(delivery) => {
            panic!("ordinary land only preserved its head: {delivery:?}")
        }
    }
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

/// Review-gated delivery validates and preserves the head, but is not a land, even with
/// retirement left at its default. The control then takes the ordinary landing path.
#[tokio::test(flavor = "multi_thread")]
async fn directory_push_only_preserves_the_validated_head_without_landing_or_retiring_substrate_lifecycle()
 {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &reads).await;
    let layout = build_volumes(&fixture);
    let main = fixture.checkout.clone();
    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("a/src.txt"), b"reviewed source\\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "prepare reviewed source"]);
    let source_head = git_stdout(&topic, &["rev-parse", "HEAD"]);
    let source_branch = git_stdout(&topic, &["symbolic-ref", "--short", "HEAD"]);
    let preservation_ref = format!("refs/cowshed/topic/heads/{source_branch}");
    let main_head = git_stdout(&main, &["rev-parse", "HEAD"]);
    assert_ne!(source_head, main_head);
    let main_index = fs::read(main.join(".git/index")).unwrap();
    let main_source = fs::read(main.join("a/src.txt")).unwrap();
    let main_build = fs::read_link(main.join(".cowshed/build")).unwrap();
    let topic_build = fs::read_link(topic.join(".cowshed/build")).unwrap();
    let seeds = serde_json::to_value(seeds_of_main(&layout)).unwrap();
    let volumes = layout.list().unwrap();
    let land_commitments = || {
        controller_commitments(fixture.storage.telemetry())
            .into_iter()
            .filter(|value| {
                matches!(
                    value,
                    cowshed_core::api::dto::ControllerCommitment::LandOutcome(_)
                        | cowshed_core::api::dto::ControllerCommitment::LandAdoption(_)
                )
            })
            .map(|value| serde_json::to_value(value).unwrap())
            .collect::<Vec<_>>()
    };
    let commitments = land_commitments();
    let (stdout, _) = succeed(
        &mut service,
        [
            "--json",
            "land",
            "topic",
            "--push-only",
            "--check",
            "/usr/bin/touch .git/push-only-validated",
        ],
    )
    .await;
    let envelope: JsonEnvelope<LandReport> = serde_json::from_slice(&stdout).unwrap();
    let LandReport::Delivered(delivery) = envelope.result().expect("successful delivery") else {
        panic!("preservation-only returned a fabricated land: {envelope:?}");
    };
    assert!(
        topic.join(".git/push-only-validated").exists(),
        "step 2 validated the workspace"
    );
    assert_eq!(delivery.source_head.as_str(), source_head);
    assert_eq!(delivery.destination_ref, preservation_ref);
    assert_eq!(
        git_stdout(&main, &["rev-parse", &preservation_ref]),
        source_head
    );
    assert_eq!(git_stdout(&main, &["rev-parse", "HEAD"]), main_head);
    assert_eq!(fs::read(main.join(".git/index")).unwrap(), main_index);
    assert_eq!(fs::read(main.join("a/src.txt")).unwrap(), main_source);
    assert_eq!(
        fs::read_link(main.join(".cowshed/build")).unwrap(),
        main_build
    );
    assert_eq!(
        fs::read_link(topic.join(".cowshed/build")).unwrap(),
        topic_build
    );
    assert_eq!(serde_json::to_value(seeds_of_main(&layout)).unwrap(), seeds);
    assert_eq!(
        layout.list().unwrap(),
        volumes,
        "no build collection or adoption"
    );
    assert_eq!(
        land_commitments(),
        commitments,
        "no fabricated land commitments"
    );
    assert!(
        topic.exists(),
        "the default retire=true must not retire a delivery"
    );
    let (text, diagnostics) = succeed(&mut service, ["land", "topic", "--push-only"]).await;
    assert_eq!(
        text,
        format!("{preservation_ref}\t{source_head}\n").into_bytes()
    );
    assert!(
        !diagnostics.contains("build volume"),
        "delivery has no adoption report: {diagnostics}"
    );
    let landed = land(&mut service, "topic", true, &[]).await;
    assert_eq!(landed.landed_head.as_str(), source_head);
    assert_eq!(git_stdout(&main, &["rev-parse", "HEAD"]), source_head);
    assert!(landed.retired, "ordinary land still retires by default");
    // Logical retirement returns at step4; step5 reclaims the mountpoint in the background.
    eventually(|| !topic.exists()).await;
    assert!(!topic.exists());
    assert_land_outcome(fixture.storage.telemetry(), &landed);
    service
        .shutdown()
        .await
        .expect("stop the real directory project");
}

/// A land fences the target's head under the caller's ref lock (02_workspaces.md "`cowshed
/// land`" step 3). A writer holds the lock from before the land starts and moves main only once
/// the land's check has run, so the land read main's old head before it: the land, waiting for
/// the lock meanwhile, reads the new head once it holds it and refuses with the target moved
/// rather than fast-forwarding. Rebased, the next land finds the lock free and lands, and the
/// lock is free again once it returns.
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_land_fences_the_target_head_under_the_caller_ref_lock_substrate_lifecycle() {
    let mut fixture = Fixture::directory();
    let mut service = serve_project(&mut fixture, &[]).await;
    let main = fixture.checkout.clone();
    let ref_lock = main.parent().expect("the fixture root").join("ref.lock");
    let ref_lock_arg = ref_lock.to_str().expect("a UTF-8 fixture path").to_owned();
    let open_lock = || {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&ref_lock)
            .expect("open the ref lock")
    };
    let flock = |file: &fs::File, operation: libc::c_int| {
        use std::os::unix::io::AsRawFd as _;
        // SAFETY: `file` owns the descriptor for the whole call.
        unsafe { libc::flock(file.as_raw_fd(), operation) == 0 }
    };
    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("topic.txt"), b"topic\n").unwrap();
    git(&topic, &["add", "topic.txt"]);
    git(&topic, &["commit", "-q", "-m", "topic"]);
    let main_head = git_stdout(&main, &["rev-parse", "HEAD"]);
    let checked = topic.join(".git/ref-fence-checked");

    let writer = open_lock();
    assert!(
        flock(&writer, libc::LOCK_EX),
        "the writer took the ref lock"
    );
    let land = run(
        &mut service,
        [
            "--json",
            "land",
            "topic",
            "--ref-lock",
            &ref_lock_arg,
            "--expected-target-head",
            &main_head,
            "--check",
            "/usr/bin/touch .git/ref-fence-checked",
        ],
    );
    let doc_commit = async {
        eventually(|| checked.exists()).await;
        fs::write(main.join("doc.md"), b"doc\n").unwrap();
        git(&main, &["add", "doc.md"]);
        git(&main, &["commit", "-q", "-m", "doc"]);
        drop(writer);
    };
    let ((result, _, stderr), ()) = tokio::join!(land, doc_commit);
    let error = result.expect_err("a land whose target moved under the ref lock landed");
    assert!(
        matches!(
            error.fence_source(),
            Some(cowshed_core::error::FenceRefusal::TargetMoved { observed: Some(_) })
        ),
        "the land did not refuse with the target moved: {error:?}\n{}",
        String::from_utf8_lossy(&stderr)
    );
    let doc_head = git_stdout(&main, &["rev-parse", "HEAD"]);
    assert_eq!(
        git_stdout(&main, &["rev-parse", "HEAD~1"]),
        main_head,
        "main holds the writer's commit and nothing the land delivered"
    );
    assert!(topic.exists(), "a refused land retired its workspace");

    succeed(&mut service, ["rebase", "topic"]).await;
    let rebased = git_stdout(&topic, &["rev-parse", "HEAD"]);
    succeed(
        &mut service,
        [
            "--json",
            "land",
            "topic",
            "--ref-lock",
            &ref_lock_arg,
            "--expected-target-head",
            &doc_head,
        ],
    )
    .await;
    assert_eq!(git_stdout(&main, &["rev-parse", "HEAD"]), rebased);
    assert!(
        flock(&open_lock(), libc::LOCK_EX | libc::LOCK_NB),
        "a land kept the ref lock after it returned"
    );
    service
        .shutdown()
        .await
        .expect("stop the real directory project");
}

async fn eventually(mut ready: impl FnMut() -> bool) {
    while !ready() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait for the last job hold on build volume `id` to drop. Jobs hold the volume by a shared
/// lock on its hold file ([`BuildVolumeLayout::hold`]), so an exclusive lock on it is granted at
/// the very event: the last job's terminal event releasing its hold, or the kernel releasing a
/// dead holder's. The lock is let go at once, leaving the volume for the gc after it.
async fn job_holds_released(layout: &BuildVolumeLayout, id: &BuildVolumeId) {
    let hold = layout.hold_path(id);
    let released =
        tokio::task::spawn_blocking(move || fs::File::open(&hold).and_then(|file| file.lock()));
    released
        .await
        .expect("the hold wait's thread")
        .expect("lock build volume's hold file");
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
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_fork_of_a_warm_target_is_all_fresh_and_all_hits_substrate_lifecycle() {
    let (nx, node) = repository_nx();
    let rust = RustToolchain::of_this_test();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: Some(&rust),
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    assert_target_on_build_volume(&fixture.checkout);
    let nx_check = nx_run_many(&node, "build test");

    let w1 = new_workspace(&mut service, "w1").await;
    fs::write(w1.join("a/src.txt"), b"src, changed\n").unwrap();
    git(&w1, &["commit", "-q", "-am", "change a"]);
    let cold = cargo_artifacts(&sh(&mut service, "w1", CARGO_BUILD).await);
    assert_eq!(cold.len(), 2, "{cold:?}");
    assert!(
        cold.iter().all(|(_, fresh)| !fresh),
        "w1 forks main's empty first seed, so it builds every unit: {cold:?}"
    );
    let report = land(&mut service, "w1", true, &[&nx_check, CARGO_BUILD]).await;
    assert_eq!(report.build_volume.seed, LandSeed::Frozen, "{report:?}");
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");

    let w2 = new_workspace(&mut service, "w2").await;
    assert_ne!(
        w1, w2,
        "the warm build volume is cloned to another checkout path"
    );
    assert_target_on_build_volume(&w2);
    assert_on_own_build_volume(&w2, &fixture.checkout);
    let host = Command::new("/bin/sh")
        .args(["-ec", &format!(". .cowshed/env; exec {CARGO_BUILD}")])
        .current_dir(&w2)
        .env_clear()
        .env("HOME", fixture.storage.home())
        .env(
            "PATH",
            // The checkout's declared toolchain, as its own jobs run it ([`declare_rust`]).
            std::env::join_paths([
                w2.join(".cowshed/bin").as_path(),
                Path::new("/usr/bin"),
                Path::new("/bin"),
            ])
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
    eprintln!("physical warm proof: stopping runtime after both real builds and Nx run");
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
    eprintln!("physical warm proof: runtime and gateway stopped; checking captured outcomes");
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
    eprintln!(
        "physical warm proof: semantic assertions passed; releasing owned runtime and scratch images"
    );
    drop(fixture);
    eprintln!("physical warm proof: owned scratch teardown complete");
}

/// The checkout path [`Project::Declared`] declares as build state.
const DECLARED_STATE: &str = "upstream";

/// Main's tree for the declared-build-state test: `.cowshed.toml` declares [`DECLARED_STATE`]
/// as build state, in a multi-line array the way an author lays out a growing list, and
/// `.gitignore` ignores it, committed; then, untracked, what an author's reconstruct script
/// leaves there before cowshed ever saw it: a nested Git checkout of an upstream project and its
/// build tree.
fn write_declared_project(checkout: &Path) {
    fs::write(
        checkout.join(".cowshed.toml"),
        format!("{FIXTURE_COWSHED_TOML}state = [\n  # rebuilt by the reconstruct script\n  \"{DECLARED_STATE}\",\n]\n"),
    )
    .unwrap();
    fs::write(checkout.join(".gitignore"), format!("/{DECLARED_STATE}/\n")).unwrap();
    git(checkout, &["add", ".cowshed.toml", ".gitignore"]);
    git(checkout, &["commit", "-q", "-m", "declare build state"]);
    let upstream = checkout.join(DECLARED_STATE);
    fs::create_dir_all(upstream.join("build")).unwrap();
    git(&upstream, &["init", "-q", "-b", "main"]);
    fs::write(upstream.join("build/build.ninja"), b"before cowshed\n").unwrap();
}

/// Run `nx_check` in `workspace` and answer the cache status of each task of that run.
async fn nx_statuses(
    service: &mut ActorBridge,
    workspace: &str,
    checkout: &Path,
    nx_check: &str,
) -> Vec<(String, nx::CacheStatus)> {
    let spawned = SystemTime::now();
    sh(service, workspace, nx_check).await;
    let exited = SystemTime::now();
    match nx::attribute(&checkout.join(".nx/cache"), nx_check, spawned, exited) {
        nx::Attribution::Ours(run) => run
            .tasks
            .into_iter()
            .map(|task| (task.task_id, task.cache))
            .collect(),
        nx::Attribution::Unattributed(reason) => {
            panic!("{workspace}'s Nx run left no summary of its own: {reason:?}")
        }
    }
}

/// The retiring land first proves ownership, frozen-seed identity and tree, immediate reclamation
/// of the old volume, its new label and the exact linked mounts (16_build_volumes.md, steps 5–7).
/// What main runs after a land reaches main's seed (16_build_volumes.md, "Targets and seeds"):
/// main changes `a` and runs it on the adopted volume, so its seed, frozen from the landing
/// volume at the same tree, is behind; doctor says so, and the next `fork` reseeds main
/// from its volume before it clones the seed and hits main's run. No command does it by hand.
/// A keeper-held workspace takes the separate busy-skip path:
/// [`directory_a_fork_of_a_keeper_held_workspace_keeps_its_existing_seed_substrate_lifecycle`].
#[tokio::test(flavor = "multi_thread")]
async fn directory_what_main_runs_after_a_land_reaches_its_seed_and_a_fork_hits_it_substrate_lifecycle()
 {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let layout = build_volumes(&fixture);
    let previous = linked_volume(&layout, &fixture.checkout);
    let nx_check = nx_run_many(&node, "build test");
    let main = fixture.checkout.clone();

    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("a/src.txt"), b"src, landed\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "land a"]);
    let landing = linked_volume(&layout, &topic);
    assert_ne!(landing, previous);
    let landed_tree = git_stdout(&topic, &["rev-parse", "HEAD^{tree}"]);
    let report = land(&mut service, "topic", true, &[&nx_check]).await;
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");
    assert!(report.retired, "{report:?}");
    assert_eq!(report.build_volume.seed, LandSeed::Frozen, "{report:?}");
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
    assert!(
        !layout.image(&previous).exists(),
        "land immediately reclaims the previous build image"
    );
    assert!(
        !layout.record(&previous).exists(),
        "land immediately reclaims its record"
    );
    assert_eq!(
        fs::canonicalize(layout.mount(&linked_volume(&layout, &fixture.checkout))).unwrap(),
        fs::canonicalize(layout.image(&linked_volume(&layout, &fixture.checkout))).unwrap(),
        "main resolves to the directory payload of the volume it adopted"
    );

    assert_eq!(
        fs::read(main.join("a/generated.txt"))
            .expect("the landed Nx output is available without running a check"),
        b"generated\n"
    );
    let warm = new_workspace(&mut service, "warm").await;
    let restored = nx_statuses(&mut service, "warm", &warm, &nx_check).await;
    assert_eq!(restored.len(), 2, "{restored:?}");
    assert!(
        restored
            .iter()
            .all(|(_, status)| *status == nx::CacheStatus::LocalHit),
        "a fork immediately consumes the restored outputs and adopted cache: {restored:?}"
    );

    let [(landed_seed, _)] = seeds_of_main(&layout).try_into().expect("one seed of main");

    // Main's own work on the adopted volume, after the seed was frozen: a miss in main.
    fs::write(main.join("a/src.txt"), b"src, changed in main\n").unwrap();
    git(&main, &["commit", "-q", "-am", "change a in main"]);
    let ran = nx_statuses(&mut service, "main", &main, &nx_check).await;
    assert!(
        ran.iter()
            .all(|(_, status)| *status == nx::CacheStatus::Miss),
        "main runs a:build and a:test itself: {ran:?}"
    );

    // Doctor exits 5 on any error finding the scratch host has; only its report matters here.
    let (_, stdout, _) = run(&mut service, ["--json", "doctor"]).await;
    let doctor: serde_json::Value = serde_json::from_slice(&stdout).expect("doctor --json");
    let behind = doctor["result"]["findings"]
        .as_array()
        .expect("doctor findings")
        .iter()
        .filter(|finding| finding["code"] == "seed-age")
        .map(|finding| finding["message"].as_str().unwrap_or_default().to_owned())
        .collect::<Vec<_>>();
    assert!(
        behind.iter().any(|message| {
            message.starts_with("main's build volume ")
                && message.contains(&format!("its seed {landed_seed}, "))
        }),
        "doctor reports main's seed behind its volume: {behind:?}"
    );

    let w1 = new_workspace(&mut service, "w1").await;
    let [(seed, record)] = seeds_of_main(&layout).try_into().expect("one seed of main");
    assert_ne!(seed, landed_seed, "the fork reseeded main first");
    assert_eq!(
        record.tree.as_ref().map(|tree| tree.as_str()),
        Some(landed_tree.as_str()),
        "the seed records the tree its volume was adopted at"
    );
    let hits = nx_statuses(&mut service, "w1", &w1, &nx_check).await;
    assert_eq!(hits.len(), 2, "{hits:?}");
    assert!(
        hits.iter()
            .all(|(_, status)| *status == nx::CacheStatus::LocalHit),
        "a fork hits what main ran after the land: {hits:?}"
    );
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// A foreground keeper holds `w1`'s volume, so `cowshed fork` cannot refresh its seed.
/// It keeps the existing seed without waiting or bypassing the hold; `w2` therefore runs
/// both tasks again. The no-keeper main case above retains the warm-fork `LocalHit` oracle.
/// Before either fork, doctor diagnoses main's displaced workspace-data link and an exec restores
/// it, so the forks never inherit a real directory in place of the build-volume link.
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_fork_of_a_keeper_held_workspace_keeps_its_existing_seed_substrate_lifecycle() {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let layout = build_volumes(&fixture);
    assert_eq!(build_link_findings(&mut service).await, [""; 0]);

    let data = fixture.checkout.join(".nx/workspace-data");
    let link = fs::read_link(&data).expect("main's workspace-data link");
    fs::remove_file(&data).unwrap();
    fs::create_dir(&data).unwrap();
    fs::write(data.join("project-graph.json"), b"{}").unwrap();
    let displaced = build_link_findings(&mut service).await;

    sh(&mut service, "main", "true").await;
    let relinked = fs::read_link(&data);
    let after = build_link_findings(&mut service).await;
    assert!(
        matches!(
            displaced.as_slice(),
            [message] if message.starts_with(
                "main's .nx/workspace-data is a real directory where its link onto the build volume belongs"
            )
        ),
        "{displaced:?}"
    );
    assert_eq!(relinked.expect("the exec's refresh relinked it"), link);
    assert_eq!(after, [""; 0]);

    let nx_check = nx_run_many(&node, "build test");

    let w1 = new_workspace(&mut service, "w1").await;
    fs::write(w1.join("a/src.txt"), b"src, changed in w1\n").unwrap();
    git(&w1, &["commit", "-q", "-am", "change a in w1"]);
    let ran = nx_statuses(&mut service, "w1", &w1, &nx_check).await;
    assert!(
        ran.iter()
            .all(|(_, status)| *status == nx::CacheStatus::Miss),
        "w1 runs a:build and a:test itself: {ran:?}"
    );

    let source_daemon = recorded_daemon(&w1).expect("w1's keeper left its daemon recorded");
    assert!(
        cowshed_os::process::running(source_daemon).expect("observe w1's daemon"),
        "the source keeper's daemon runs before the fork"
    );
    let source_volume = linked_volume(&layout, &w1);
    assert!(
        layout
            .held(&source_volume)
            .expect("observe the source hold"),
        "the source keeper holds its build volume before the fork"
    );
    // The keeper's ordinary lifetime hold makes capture skip, retaining the older seed.
    // There is no synchronous release wait and no permission to bypass this hold.
    succeed(&mut service, ["fork", "w1", "w2"]).await;
    let w2 = service
        .path("w2", false)
        .await
        .expect("w2 is mounted")
        .mount;
    let hits = nx_statuses(&mut service, "w2", &w2, &nx_check).await;
    // Each shed's keeper keeps an Nx daemon with its plugin workers. The runtime's shutdown
    // retires the supervisors it serves, and a supervisor retires only once its keeper's daemon
    // stopped (04_sandbox.md): shutdown returns after that stop saw the daemon exit, and the
    // daemon's plugin workers follow it out.
    let kept = recorded_daemon(&w2).expect("w2's run left its kept daemon recorded");
    assert!(
        cowshed_os::process::running(kept).expect("read the daemon's state"),
        "w2's kept daemon {kept} runs before the shutdown"
    );
    let workers: Vec<libc::pid_t> = [&w1, &w2]
        .into_iter()
        .filter_map(|checkout| recorded_daemon(checkout))
        .flat_map(children_of)
        .collect();
    service.shutdown().await.expect("stop the runtime");
    for checkout in [&w1, &w2] {
        if let Some(daemon) = recorded_daemon(checkout) {
            assert!(
                !cowshed_os::process::running(daemon).expect("read the daemon's state"),
                "{}'s kept Nx daemon {daemon} outlives the runtime",
                checkout.display()
            );
        }
    }
    await_exits(&workers);
    fixture.stop_gateway().await;
    assert_eq!(hits.len(), 2, "{hits:?}");
    assert!(
        hits.iter()
            .all(|(_, status)| *status == nx::CacheStatus::Miss),
        "a keeper-held fork keeps the older seed and runs both tasks again: {hits:?}"
    );
}

/// The pid the Nx daemon record in `checkout` names, if there is a record.
fn recorded_daemon(checkout: &Path) -> Option<libc::pid_t> {
    let record = checkout.join(".nx/workspace-data/d/server-process.json");
    let bytes = match fs::read(&record) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("{}: {error}", record.display()),
    };
    let record: serde_json::Value =
        serde_json::from_slice(&bytes).expect("the daemon record is JSON");
    Some(
        record["processId"]
            .as_i64()
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .expect("the record names the daemon's pid"),
    )
}

/// The pids whose parent is `parent`: an Nx daemon's plugin workers.
fn children_of(parent: libc::pid_t) -> Vec<libc::pid_t> {
    let listed = Command::new("/bin/ps")
        .args(["-Ao", "pid=,ppid="])
        .output()
        .expect("list processes");
    String::from_utf8_lossy(&listed.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace().map(str::parse::<libc::pid_t>);
            match (fields.next(), fields.next()) {
                (Some(Ok(pid)), Some(Ok(ppid))) if ppid == parent => Some(pid),
                _ => None,
            }
        })
        .collect()
}

/// Wait on the kernel's exit event of each of `pids`, however long that takes; one already gone
/// has nothing to wait for. A process that stays makes the test outlive its slow bound.
fn await_exits(pids: &[libc::pid_t]) {
    // SAFETY: kqueue takes nothing and returns a new descriptor or -1.
    let queue = unsafe { libc::kqueue() };
    assert!(queue >= 0, "kqueue: {}", std::io::Error::last_os_error());
    // SAFETY: a fresh descriptor this function alone owns.
    let queue = unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(queue) };
    let raw = std::os::fd::AsRawFd::as_raw_fd(&queue);
    let mut waiting = 0;
    for &pid in pids {
        let exit = libc::kevent {
            ident: pid as libc::uintptr_t,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: one valid change, no events requested back.
        if unsafe { libc::kevent(raw, &exit, 1, std::ptr::null_mut(), 0, std::ptr::null()) } == 0 {
            waiting += 1;
        } else {
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.raw_os_error(),
                Some(libc::ESRCH),
                "watch {pid}: {error}"
            );
        }
    }
    while waiting > 0 {
        // SAFETY: an all-zero kevent is a valid buffer for the kernel to fill.
        let mut event: libc::kevent = unsafe { std::mem::zeroed() };
        // SAFETY: room for exactly one event; no changes; no timeout.
        let got =
            unsafe { libc::kevent(raw, std::ptr::null(), 0, &mut event, 1, std::ptr::null()) };
        match got {
            1 => waiting -= 1,
            _ => {
                let error = std::io::Error::last_os_error();
                assert_eq!(
                    error.raw_os_error(),
                    Some(libc::EINTR),
                    "await exits: {error}"
                );
            }
        }
    }
}

/// Doctor's `build-link` findings: one message each.
async fn build_link_findings(service: &mut ActorBridge) -> Vec<String> {
    // Doctor exits 5 on any error finding the scratch host has; only its report matters here.
    let (_, stdout, _) = run(service, ["--json", "doctor"]).await;
    let doctor: serde_json::Value = serde_json::from_slice(&stdout).expect("doctor --json");
    doctor["result"]["findings"]
        .as_array()
        .expect("doctor findings")
        .iter()
        .filter(|finding| finding["code"] == "build-link")
        .map(|finding| finding["message"].as_str().unwrap_or_default().to_owned())
        .collect()
}

/// A land carries main's Nx cache entries into the volume main adopts (16_build_volumes.md,
/// "Carry"): `a` lands and warms `a:build` and `a:test` in main; `b`, forked before that land
/// and changing nothing `a` hashes, lands next without running them. `b` catches up with plain
/// git, since `cowshed rebase` would carry `a`'s entries into its volume first. Main then adopts
/// `b`'s volume, which never held `a`'s entries, and a fork of main still hits both.
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_land_carries_mains_nx_entries_into_the_volume_main_adopts_substrate_lifecycle()
{
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let nx_check = nx_run_many(&node, "build test");

    let b = new_workspace(&mut service, "b").await;
    fs::write(b.join("unrelated.txt"), b"nothing a hashes\n").unwrap();
    git(&b, &["add", "unrelated.txt"]);
    git(&b, &["commit", "-q", "-m", "add an unrelated file"]);

    let a = new_workspace(&mut service, "a").await;
    fs::write(a.join("a/src.txt"), b"src, landed by a\n").unwrap();
    git(&a, &["commit", "-q", "-am", "change a"]);
    let report = land(&mut service, "a", true, &[&nx_check]).await;
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");

    git(
        &b,
        &["fetch", "-q", &fixture.checkout.to_string_lossy(), "HEAD"],
    );
    git(&b, &["rebase", "-q", "FETCH_HEAD"]);
    let report = land(&mut service, "b", true, &[]).await;
    match &report.build_volume.adoption {
        Adoption::Adopted { carried, .. } => {
            assert_eq!(carried.stopped, None, "{report:?}");
            assert!(
                carried.entries >= 2,
                "a:build and a:test, which main held and b's volume did not: {report:?}"
            );
        }
        Adoption::Skipped { reason } => panic!("the adoption was skipped: {reason:?}"),
    }

    let fork = new_workspace(&mut service, "fork").await;
    let hits = nx_statuses(&mut service, "fork", &fork, &nx_check).await;
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
    assert_eq!(hits.len(), 2, "{hits:?}");
    assert!(
        hits.iter()
            .all(|(_, status)| *status == nx::CacheStatus::LocalHit),
        "a fork of main hits what a's land ran, though b's land adopted a volume without it: {hits:?}"
    );
}

/// A rebase carries main's Nx cache entries into the rebased workspace's volume
/// (16_build_volumes.md, "Rebase carry"), and only those the rebased tree looks up: `b`, forked
/// before main runs the check at the original tree and before `a` lands and warms `a:build` and
/// `a:test` at its own, rebases after that land. Main then indexes both trees' entries; `b` takes
/// the landed tree's two and none of the original's, and its run of the landed check hits both
/// without re-running either. A rebase while a process holds `b`'s task database carries nothing
/// and names that process.
// Admitted to the 60 s lifecycle class below its 3× rule: its one low-load sample (34.983 s) ran
// with the daemon off, and its daemon-on samples are load-dependent (08_testing.md, Integration
// tests).
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_rebase_carries_mains_nx_entries_into_the_rebased_workspace_substrate_lifecycle()
 {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let nx_check = nx_run_many(&node, "build test");

    let b = new_workspace(&mut service, "b").await;
    fs::write(b.join("unrelated.txt"), b"nothing a hashes\n").unwrap();
    git(&b, &["add", "unrelated.txt"]);
    git(&b, &["commit", "-q", "-m", "add an unrelated file"]);
    // Entries no tree after the land looks up: what a rebase that carried main's whole cache
    // copied, entry for entry.
    sh(&mut service, "main", &nx_check).await;

    let a = new_workspace(&mut service, "a").await;
    fs::write(a.join("a/src.txt"), b"src, landed by a\n").unwrap();
    git(&a, &["commit", "-q", "-am", "change a"]);
    let report = land(&mut service, "a", true, &[&nx_check]).await;
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");

    let rebased = rebase(&mut service, "b").await;
    assert_eq!(rebased.oid.as_str(), git_stdout(&b, &["rev-parse", "HEAD"]));
    match &rebased.build_volume {
        RebaseBuildVolume::Carried { carried, deferred } => {
            assert_eq!(carried.stopped, None, "{rebased:?}");
            assert_eq!(
                carried.entries, 2,
                "a:build and a:test at the landed tree, and neither entry of the original: {rebased:?}"
            );
            assert_eq!(
                *deferred, 0,
                "neither task hashes another's outputs: {rebased:?}"
            );
        }
        RebaseBuildVolume::Skipped { reason } => panic!("the carry was skipped: {reason}"),
    }
    let hits = nx_statuses(&mut service, "b", &b, &nx_check).await;

    let data = b.join(".nx/workspace-data");
    let databases: Vec<PathBuf> = fs::read_dir(&data)
        .expect("b's Nx workspace data")
        .map(|entry| entry.expect("workspace data entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "db"))
        .collect();
    let [database] = databases.as_slice() else {
        panic!("one task database in {}: {databases:?}", data.display());
    };
    let holder = Holder(
        Command::new("/usr/bin/tail")
            .arg("-f")
            .arg(database)
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn a database holder"),
    );
    let pid = i32::try_from(holder.0.id()).expect("a pid fits i32");
    eventually(|| {
        nx::holders(database)
            .expect("query the database's holders")
            .iter()
            .any(|holder| holder.pid == pid)
    })
    .await;
    let held = rebase(&mut service, "b").await;
    drop(holder);
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;

    assert_eq!(hits.len(), 2, "{hits:?}");
    assert!(
        hits.iter()
            .all(|(_, status)| *status == nx::CacheStatus::LocalHit),
        "b runs the landed check without re-running what a's land ran: {hits:?}"
    );
    match held.build_volume {
        RebaseBuildVolume::Skipped {
            reason:
                RebaseCarrySkip::Held {
                    side: CarrySide::Workspace,
                    database: named,
                    holders,
                },
        } => {
            assert_eq!(
                named.canonicalize().expect("the held database exists"),
                database.canonicalize().expect("b's database exists")
            );
            assert!(
                holders.iter().any(|holder| holder.pid == pid),
                "tail is named among {holders:?}"
            );
        }
        build_volume => panic!("a held database skips the carry: {build_volume:?}"),
    }
}

/// A task that hashes its dependency's outputs is carried by task (16_build_volumes.md, "Rebase
/// carry"): stock Nx hashes `a:test`, whose inputs name `dependentTasksOutputFiles`, only once
/// `a:build` has written or restored `a/generated.txt`, and `b`'s copy of that file is stale. `b`
/// runs the check at the original tree, so its `a/generated.txt` says what the original
/// `a/src.txt` built; `a` changes `a/src.txt` and lands; `b` rebases. A hash of `a:test` from the
/// file on disk would be a key no run looks up, so the carry takes main's latest `a:test` entry by
/// task instead, and `b`'s run of the landed check hits both tasks.
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_rebase_carries_a_task_that_hashes_its_dependencys_stale_outputs_substrate_lifecycle()
 {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let node_path = node.display().to_string();
    let project = serde_json::json!({
        "name": "a",
        "targets": {
            "build": {
                "executor": "nx:run-commands",
                "cache": true,
                "inputs": ["{projectRoot}/src.txt"],
                "outputs": ["{projectRoot}/generated.txt"],
                "options": {
                    "command": format!(
                        "{node_path} -e \"const fs = require('fs'); fs.writeFileSync('a/generated.txt', 'built from ' + fs.readFileSync('a/src.txt'))\""
                    ),
                },
            },
            "test": {
                "executor": "nx:run-commands",
                "cache": true,
                "inputs": ["default", {"dependentTasksOutputFiles": "**/*.txt"}],
                "dependsOn": ["build"],
                "outputs": [],
                "options": {
                    "command": format!(
                        "{node_path} -e \"require('fs').readFileSync('a/generated.txt')\""
                    ),
                },
            },
        },
    });
    fs::write(
        fixture.checkout.join("a/project.json"),
        serde_json::to_vec_pretty(&project).unwrap(),
    )
    .unwrap();
    git(
        &fixture.checkout,
        &["commit", "-q", "-am", "a:test hashes a:build's outputs"],
    );
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let nx_check = nx_run_many(&node, "build test");

    let b = new_workspace(&mut service, "b").await;
    fs::write(b.join("unrelated.txt"), b"nothing a hashes\n").unwrap();
    git(&b, &["add", "unrelated.txt"]);
    git(&b, &["commit", "-q", "-m", "add an unrelated file"]);
    sh(&mut service, "b", &nx_check).await;

    let a = new_workspace(&mut service, "a").await;
    fs::write(a.join("a/src.txt"), b"src, landed by a\n").unwrap();
    git(&a, &["commit", "-q", "-am", "change a"]);
    let report = land(&mut service, "a", true, &[&nx_check]).await;
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");

    let rebased = rebase(&mut service, "b").await;
    let stale = fs::read(b.join("a/generated.txt")).expect("b's own build output");
    let hits = nx_statuses(&mut service, "b", &b, &nx_check).await;
    let built = fs::read(b.join("a/generated.txt")).expect("the restored build output");
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;

    assert_eq!(
        stale, b"built from src\n",
        "b's output on disk is the original tree's"
    );
    assert_eq!(built, b"built from src, landed by a\n");
    match &rebased.build_volume {
        RebaseBuildVolume::Carried { carried, deferred } => {
            assert_eq!(carried.stopped, None, "{rebased:?}");
            assert_eq!(
                (carried.entries, *deferred),
                (2, 1),
                "a:build by its hash, a:test by task: {rebased:?}"
            );
        }
        RebaseBuildVolume::Skipped { reason } => panic!("the carry was skipped: {reason}"),
    }
    assert_eq!(hits.len(), 2, "{hits:?}");
    assert!(
        hits.iter()
            .all(|(_, status)| *status == nx::CacheStatus::LocalHit),
        "b hits a:test though its own a/generated.txt was stale when it rebased: {hits:?}"
    );
}

/// `cowshed rebase --json <workspace>`; answers the report the CLI printed.
async fn rebase(service: &mut ActorBridge, workspace: &str) -> RebaseReport {
    let (stdout, stderr) = succeed(service, ["--json", "rebase", workspace]).await;
    let envelope: JsonEnvelope<RebaseReport> =
        serde_json::from_slice(&stdout).unwrap_or_else(|error| {
            panic!(
                "rebase --json printed no report ({error}): {}\n{stderr}",
                String::from_utf8_lossy(&stdout)
            )
        });
    envelope
        .result()
        .cloned()
        .expect("a successful rebase report")
}

/// Stock Nx's open lock fences the look-to-rename window: an opener skips the swap and is
/// named, while the target keeps its volume and the landing tree still freezes its seed.
/// Once the opener exits, a new commit on the same topic exercises a foreign database holder:
/// the retiring land names that holder and freezes a new seed without swapping main's volume
/// (16_build_volumes.md, "The adoption needs the target's Nx database closed").
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_foreign_database_opener_skips_adoption_with_its_pid_and_argv_substrate_lifecycle()
 {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let layout = build_volumes(&fixture);
    let nx_check = nx_run_many(&node, "build test");
    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("a/src.txt"), b"src, changed\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "change a"]);
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
    let mut opening = Holder(
        Command::new("/usr/bin/lockf")
            .arg("-k")
            .arg(database.with_extension("lock"))
            .args(["/bin/sh", "-c", "echo held; read _"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn an Nx open-lock holder"),
    );
    {
        use std::io::BufRead as _;
        let mut held = String::new();
        std::io::BufReader::new(opening.0.stdout.as_mut().expect("piped stdout"))
            .read_line(&mut held)
            .expect("read lockf's command");
        assert_eq!(held, "held\n", "lockf holds the open lock");
    }
    let lockf = i32::try_from(opening.0.id()).expect("a pid fits i32");
    let report = land(&mut service, "topic", false, &[&nx_check]).await;
    assert_eq!(
        report.build_volume.seed,
        LandSeed::Frozen,
        "a skipped swap still freezes its seed: {report:?}"
    );
    assert_land_outcome(fixture.storage.telemetry(), &report);
    match report.build_volume.adoption {
        Adoption::Skipped {
            reason:
                AdoptionSkip::TargetOpening {
                    database: opened,
                    holders,
                },
        } => {
            assert_eq!(
                opened.canonicalize().expect("the opened database exists"),
                database.canonicalize().expect("main's database exists")
            );
            assert!(
                holders.iter().any(|holder| {
                    holder.pid == lockf && holder.command.starts_with("/usr/bin/lockf")
                }),
                "lockf is named among {holders:?}"
            );
        }
        adoption => {
            panic!("an Nx process opening main's database skips the adoption: {adoption:?}")
        }
    }
    assert_eq!(linked_volume(&layout, &fixture.checkout), before);
    drop(opening.0.stdin.take());
    opening.0.wait().expect("lockf exits with its command");
    drop(opening);

    // The first land retained topic and left main's database in place. Change the landing tree
    // again before holding that database, so this is another checked land, not a no-op replay.
    fs::write(
        topic.join("a/src.txt"),
        b"src, changed with a database holder\n",
    )
    .unwrap();
    git(
        &topic,
        &["commit", "-q", "-am", "change a after the opener exits"],
    );
    let holder = Holder(
        Command::new("/usr/bin/tail")
            .arg("-f")
            .arg(database)
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn a database holder"),
    );
    let pid = i32::try_from(holder.0.id()).expect("a pid fits i32");
    eventually(|| {
        nx::holders(database)
            .expect("query the database's holders")
            .iter()
            .any(|holder| holder.pid == pid)
    })
    .await;
    let report = land(&mut service, "topic", true, &[&nx_check]).await;
    assert_eq!(
        report.build_volume.seed,
        LandSeed::Frozen,
        "forks still start from the new seed: {report:?}"
    );
    assert_land_outcome(fixture.storage.telemetry(), &report);
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

/// An open cache outside the named Nx/Cargo writer exclusion does not delay capture or adoption.
/// The land replaces main's previous seed while the reader remains alive, and a subsequent fork
/// inherits the cache bytes without requiring that reader to close (16_build_volumes.md, Land
/// step 5.6). The real-APFS runtime proof owns block-atomic image and SQLite consistency coverage.
#[tokio::test(flavor = "multi_thread")]
async fn directory_an_open_cache_reader_allows_seed_capture_and_adoption_substrate_lifecycle() {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let layout = build_volumes(&fixture);
    let nx_check = nx_run_many(&node, "build test");
    let topic = new_workspace(&mut service, "topic").await;
    fs::write(topic.join("a/src.txt"), b"src, changed\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "change a"]);
    let landed_tree = git_stdout(&topic, &["rev-parse", "HEAD^{tree}"]);
    // The fork reseeded main first: the land must replace that seed while the reader stays open.
    let [(previous_seed, _)] = seeds_of_main(&layout)
        .try_into()
        .expect("main has one seed");
    let landing = linked_volume(&layout, &topic);
    let index = link::linked(&topic)
        .expect("read the build link")
        .expect("topic links its build volume")
        .join("codegraph.db");
    fs::write(&index, b"an index nothing in the land knows\n").unwrap();
    let holder = Holder(
        Command::new("/usr/bin/tail")
            .arg("-f")
            .arg(&index)
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn an index holder"),
    );
    let pid = i32::try_from(holder.0.id()).expect("a pid fits i32");
    eventually(|| {
        nx::holders(&index)
            .expect("query the index's holders")
            .iter()
            .any(|holder| holder.pid == pid)
    })
    .await;

    let report = land(&mut service, "topic", true, &[&nx_check]).await;
    assert_eq!(
        report.build_volume.seed,
        LandSeed::Frozen,
        "an exempt cache reader must not refuse seed capture: {report:?}"
    );
    match &report.build_volume.adoption {
        Adoption::Adopted { check, .. } => {
            assert_eq!(check.hits, 2, "a:build and a:test hit in main: {report:?}");
            assert_eq!(check.misses, [], "{report:?}");
            assert_eq!(check.failed, [], "{report:?}");
        }
        Adoption::Skipped { reason } => {
            panic!("an exempt cache reader skipped adoption: {reason:?}")
        }
    }
    assert_eq!(linked_volume(&layout, &fixture.checkout), landing);
    let [(seed, record)] = seeds_of_main(&layout)
        .try_into()
        .expect("main keeps exactly one seed");
    assert_ne!(
        seed, previous_seed,
        "the land replaces the seed while the exempt reader remains alive"
    );
    assert_eq!(
        record.tree.as_ref().map(|tree| tree.as_str()),
        Some(landed_tree.as_str()),
        "the captured seed records the tree main adopted"
    );
    assert!(
        fs::read(&index).is_ok_and(|bytes| bytes == b"an index nothing in the land knows\n"),
        "main's adopted volume carries the index the holder kept open"
    );

    // A fork captures or reuses main's current seed without waiting for the same reader.
    let after = new_workspace(&mut service, "after").await;
    let captured = link::linked(&after)
        .expect("read the fork's build link")
        .expect("the fork links its captured build volume");
    assert_eq!(
        fs::read(captured.join("codegraph.db")).unwrap().as_slice(),
        b"an index nothing in the land knows\n",
        "the fork inherits the actual cache bytes while the original reader remains open"
    );
    let [(_, record)] = seeds_of_main(&layout).try_into().expect("one seed of main");
    assert_eq!(
        record.tree.as_ref().map(|tree| tree.as_str()),
        Some(landed_tree.as_str()),
        "the seed records the tree main adopted"
    );
    drop(holder);
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// A job admitted before an adoption keeps the volume it was admitted with: its working
/// directory stays in main's previous volume, which therefore stays until the job exits. A job
/// admitted afterwards resolves main's link to the adopted volume (16_build_volumes.md, "Process
/// lifetime across a swap", "Garbage collection").
///
/// The job that runs across the adoption is a loop that sleeps between ticks: a resident, which
/// holds no CPU token for its lifetime (05_gateway.md, "A resident holds no token"). The ledger
/// has three tokens, as the macOS arm64 CI runner has, so its background room is one token
/// ("Room for a gate"). Run as an ordinary background job, the loop held that token, so the
/// land's `--check` job queued behind it at `check-wait` and the land never reached its adoption:
/// it hung on that runner until the idle bound, twice, and hung the same way here with this
/// ledger at three tokens, while at four it passed. The jobs draw on that fixture ledger, never on
/// a pool this test's runner holds ([`holds_no_cpu_pool`]), so every run sees the runner's room.
#[tokio::test(flavor = "multi_thread")]
async fn directory_a_job_running_across_an_adoption_keeps_its_volume_and_later_jobs_get_the_new_one()
 {
    if !holds_no_cpu_pool(
        "macos::directory_a_job_running_across_an_adoption_keeps_its_volume_and_later_jobs_get_the_new_one",
    )
    .await
    {
        return;
    }
    let rust = RustToolchain::of_this_test();
    let mut fixture = Fixture::with_directory(Project::Build {
        nx: None,
        rust: Some(&rust),
    });
    fixture.cpu_budget = CpuBudgetLimits {
        tokens: std::num::NonZeroUsize::new(3).expect("three tokens"),
    };
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
        [
            "exec",
            "main",
            "--background",
            "--class",
            "resident",
            "--",
            "/bin/sh",
            "-c",
            &job,
        ],
    )
    .await;
    let ticks = old_mount.join("target/ticking/ticks");
    eventually(|| file_len(&ticks) > 0).await;

    let workspace = new_workspace(&mut service, "w").await;
    fs::write(workspace.join("notes.txt"), b"notes\n").unwrap();
    git(&workspace, &["add", "notes.txt"]);
    git(&workspace, &["commit", "-q", "-m", "notes"]);
    let landing = linked_volume(&layout, &workspace);
    let report = land(&mut service, "w", true, &[CARGO_BUILD]).await;
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");
    assert_eq!(linked_volume(&layout, &fixture.checkout), landing);
    let new_mount = layout.mount(&landing);

    // `fork w` reseeded main from the volume the job writes, so the adopted volume may hold a
    // copy of the ticks as they were then; the job itself never writes it.
    let copied = new_mount.join("target/ticking/ticks");
    let copied_at_swap = file_len(&copied);
    let at_swap = file_len(&ticks);
    eventually(|| file_len(&ticks) > at_swap).await;
    assert_eq!(file_len(&copied), copied_at_swap);
    sh(&mut service, "main", "touch target/after").await;
    assert!(new_mount.join("target/after").is_file());
    assert!(!old_mount.join("target/after").exists());
    assert_eq!(
        layout.read_record(&old).expect("previous record").role,
        BuildVolumeRole::Unlinked
    );
    let (stdout, stderr) = succeed(&mut service, ["--json", "gc", "--dry-run"]).await;
    let preview: JsonEnvelope<GcReport> =
        serde_json::from_slice(&stdout).expect("dry-run GC report");
    let preview = preview.result().expect("successful dry-run");
    assert!(
        preview.deferred.iter().any(|deferred| {
            deferred.path == layout.image(&old)
                && deferred.diagnostic.contains("job admitted on it")
        }),
        "dry-run explains the job hold without claiming the volume: {preview:?}\n{stderr}"
    );
    assert!(
        !preview
            .candidates
            .iter()
            .any(|candidate| candidate.path == layout.image(&old)),
        "a held build volume is not a reclaimable dry-run candidate: {preview:?}"
    );
    assert!(
        layout.image(&old).exists(),
        "dry-run never deletes the held image"
    );
    let (_, stderr) = succeed(&mut service, ["gc"]).await;
    assert!(
        stderr.contains(old.as_str()) && stderr.contains("job admitted on it"),
        "gc names the old volume and explains its job hold: {stderr}"
    );
    assert!(
        layout.image(&old).exists(),
        "gc never forces away a volume a running job is in"
    );

    fs::write(&stop, b"").unwrap();
    job_holds_released(&layout, &old).await;
    assert!(
        old_mount.join("target/ticking/exited").is_file(),
        "the job ran to its end in the previous volume"
    );
    let (_, stderr) = succeed(&mut service, ["gc"]).await;
    assert!(
        !layout.image(&old).exists() && !layout.record(&old).exists(),
        "the first gc once the job's hold is gone reclaims the previous volume and its record: \
         {stderr}"
    );
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// Every mounted build volume has a live checkout link, and every supplied link is mounted.
/// Query the kernel, not sidecars: an unlinked attachment is precisely the regression.
fn assert_linked_build_mounts(layout: &BuildVolumeLayout, checkouts: &[&Path]) {
    let root = layout.mounts().canonicalize().expect("build mount root");
    let expected: std::collections::BTreeSet<_> = checkouts
        .iter()
        .map(|checkout| {
            layout
                .mount(&linked_volume(layout, checkout))
                .canonicalize()
                .expect("linked build mount")
        })
        .collect();
    let mounted: std::collections::BTreeSet<_> = SystemKernelMountSource
        .mounts()
        .expect("kernel mount inventory")
        .into_iter()
        .map(|mount| mount.mount_point)
        .filter(|mount| mount.starts_with(&root))
        .collect();
    assert_eq!(
        mounted, expected,
        "no unlinked build volume may stay mounted"
    );
}

async fn assert_build_label(layout: &BuildVolumeLayout, checkout: &Path, name: &str) {
    let mount = layout.mount(&linked_volume(layout, checkout));
    let expected = build_volume_label(
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
        &WorkspaceName::new(name).expect("checkout name"),
    );
    eventually(|| {
        cowshed_apfs::volume_name(&mount).expect("read build volume name") == expected.as_str()
    })
    .await;
}

/// One declared project, one walk. Main's first touch links the declared path onto its build
/// volume, and the author's reconstruct script writes through that link (16_build_volumes.md,
/// "Declared build state"). A fork of main gets that state warm through the same fixed link,
/// and adopt/new/fork name every attached build volume (06_cli.md, "Build-volume names and
/// reclamation"). Land renames the adopted volume even without a check, and rm/land reclaim
/// unlinked volumes before returning, without a gc (16_build_volumes.md, "Garbage collection").
/// The same sibling retirement holds a host process in that build volume and proves its
/// forced release names the exact pid and command; no second project is adopted for that proof.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_build_volume_names_follow_checkout_ownership_and_unlinked_mounts_are_reclaimed()
{
    const CHILD: &str = "COWSHED_TEST_BUILD_VOLUME_HOLDER_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // Native release diagnostics use the process's stderr, not dispatch's Output writer.
        // A separate test process captures them without redirecting descriptors shared by tests.
        let output = tokio::process::Command::new(
            std::env::current_exe().expect("test executable"),
        )
        .args([
            "--exact",
            "macos::real_apfs_build_volume_names_follow_checkout_ownership_and_unlinked_mounts_are_reclaimed",
            "--nocapture",
                        // Keep the actual PID record line-framed even with inherited RUST_TEST_THREADS=1.
            "--quiet",
        ])
        .env(CHILD, "1")
        .output()
        .await
        .expect("run the APFS holder scenario in a child test process");
        let stdout = String::from_utf8(output.stdout).expect("child test stdout");
        let stderr = String::from_utf8(output.stderr).expect("child test stderr");
        assert!(
            output.status.success(),
            "child scenario failed\n{stdout}\n{stderr}"
        );
        let pid = stdout
            .lines()
            .find_map(|line| line.strip_prefix("build-volume-holder-pid="))
            .and_then(|pid| pid.parse::<i32>().ok())
            .expect("the child names its real holder pid");
        assert!(
            stderr.contains(&format!("pid {pid} (/bin/cat)")),
            "forced release names the holder's pid and command: {stderr}"
        );
        assert!(
            stderr.contains("forced"),
            "forced release is explicit: {stderr}"
        );
        return;
    }
    let mut fixture = Fixture::with(Project::Declared);
    assert!(fixture.checkout.join(DECLARED_STATE).join(".git").is_dir());
    let mut service = serve_project(&mut fixture, &[]).await;
    let link = Path::new(".cowshed/build/declared").join(DECLARED_STATE);
    assert_eq!(
        fs::read_link(fixture.checkout.join(DECLARED_STATE)).expect("main's declared link"),
        link
    );
    assert!(
        !fixture
            .checkout
            .join(DECLARED_STATE)
            .join("build/build.ninja")
            .exists(),
        "the first touch discards the old checkout and never copies it"
    );
    let layout = build_volumes(&fixture);
    let previous = linked_volume(&layout, &fixture.checkout);
    assert_build_label(&layout, &fixture.checkout, "main").await;
    assert_linked_build_mounts(&layout, &[&fixture.checkout]);

    // The author's reconstruct script, run once more: from now on it lives on main's volume.
    let warm = fixture.checkout.join(DECLARED_STATE).join("build");
    fs::create_dir_all(&warm).unwrap();
    fs::write(warm.join("build.ninja"), b"warm\n").unwrap();
    assert_eq!(
        git_stdout(
            &fixture.checkout,
            &["status", "--porcelain", "--untracked-files=all"]
        ),
        "",
        "the link is excluded, never source"
    );

    let topic = new_workspace(&mut service, "topic").await;
    assert_ne!(linked_volume(&layout, &topic), previous);
    assert_eq!(
        fs::read_link(topic.join(DECLARED_STATE)).expect("the fork's declared link"),
        link
    );
    assert_eq!(
        fs::read(topic.join(DECLARED_STATE).join("build/build.ninja"))
            .expect("the fork's declared state"),
        b"warm\n",
        "a fork of main clones main's declared state warm"
    );
    assert_build_label(&layout, &topic, "topic").await;
    assert_linked_build_mounts(&layout, &[&fixture.checkout, &topic]);

    succeed(&mut service, ["fork", "topic", "sibling"]).await;
    let sibling = service
        .path("sibling", false)
        .await
        .expect("sibling is mounted")
        .mount;
    let sibling_volume = linked_volume(&layout, &sibling);
    assert_build_label(&layout, &sibling, "sibling").await;
    assert_linked_build_mounts(&layout, &[&fixture.checkout, &topic, &sibling]);
    let mount = layout.mount(&sibling_volume);
    let holder = Holder(
        Command::new("/bin/cat")
            .current_dir(&mount)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("a host process with cwd in the build volume"),
    );
    let pid = i32::try_from(holder.0.id()).expect("a pid fits i32");
    println!("build-volume-holder-pid={pid}");
    eventually(|| {
        nx::volume_holders(&mount)
            .expect("query build volume holders")
            .iter()
            .any(|holder| holder.pid == pid)
    })
    .await;
    // Forked seconds ago: forced past the age floor alone. The holder is a host process, not a
    // job, so the force changes nothing the release does to it.
    succeed(&mut service, ["rm", "sibling", "--force"]).await;
    assert!(
        !layout.image(&sibling_volume).exists(),
        "rm reclaims the unlinked image before returning"
    );
    assert!(
        !layout.record(&sibling_volume).exists(),
        "rm reclaims the unlinked record"
    );
    assert_linked_build_mounts(&layout, &[&fixture.checkout, &topic]);
    drop(holder);

    fs::write(topic.join("tracked"), b"landed change\n").expect("change tracked source");
    git(&topic, &["commit", "-q", "-am", "change tracked source"]);
    let landing = linked_volume(&layout, &topic);
    let report = land(&mut service, "topic", false, &[]).await;
    assert!(!report.retired, "{report:?}");
    assert!(report.build_volume.adoption.is_adopted(), "{report:?}");
    assert_eq!(linked_volume(&layout, &fixture.checkout), landing);
    assert!(
        !layout.image(&previous).exists(),
        "land --no-retire immediately reclaims main's previous image"
    );
    assert!(
        !layout.record(&previous).exists(),
        "land reclaims the previous record"
    );
    assert_build_label(&layout, &fixture.checkout, "main").await;
    assert_build_label(&layout, &topic, "topic").await;
    assert_linked_build_mounts(&layout, &[&fixture.checkout, &topic]);

    succeed(&mut service, ["rm", "topic", "--force"]).await;
    assert!(
        layout.image(&landing).exists(),
        "rm preserves main's adopted volume"
    );
    assert_build_label(&layout, &fixture.checkout, "main").await;
    assert_linked_build_mounts(&layout, &[&fixture.checkout]);
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// Re-adoption (02_workspaces.md, "Re-adoption: format transition and space reclamation"):
/// RAW replacement, interrupted RAW recovery and interrupted legacy recovery are independent real walks
/// over fresh projects.
/// - a default adopt over a main in a format this runtime only replaces refuses before it
///   records anything, and leaves that main's bytes alone;
/// - `adopt --re-adopt` replaces an adopted main with a fresh RAW image under a new incarnation,
///   keeping the checkout's tracked and untracked bytes, its Git HEAD and status, its build
///   volume's identity, owner record and warm contents, and the original `.pre-cowshed` tree;
/// - the replaced main and build images stay at their retained paths, in their own format.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_raw_re_adoption_preserves_build_identity_and_replays_published_build_loss() {
    prove_re_adoption(ReAdoptionProof::RawReplacement).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_re_adopt_resumes_an_interrupted_replacement_without_a_second_one() {
    prove_re_adoption(ReAdoptionProof::RawInterruptedRecovery).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_re_adopt_resumes_legacy_inputs_without_a_second_replacement() {
    prove_re_adoption(ReAdoptionProof::LegacyRecovery).await;
}

enum ReAdoptionProof {
    RawReplacement,
    RawInterruptedRecovery,
    LegacyRecovery,
}

async fn prove_re_adoption(proof: ReAdoptionProof) {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    struct RestoreStderr {
        saved: Option<OwnedFd>,
        report_path: PathBuf,
    }
    impl RestoreStderr {
        fn restore(&mut self) -> std::io::Result<()> {
            if let Some(saved) = self.saved.take() {
                // SAFETY: saved owns the descriptor until duplication completes.
                if unsafe { libc::dup2(saved.as_raw_fd(), libc::STDERR_FILENO) } < 0 {
                    self.saved = Some(saved);
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        }
    }
    impl Drop for RestoreStderr {
        fn drop(&mut self) {
            if self.saved.is_some() {
                if let Err(error) = self.restore() {
                    eprintln!("replacement proof could not restore stderr: {error}");
                    return;
                }
                match fs::read_to_string(&self.report_path) {
                    Ok(report) => eprint!("{report}"),
                    Err(error) => eprintln!(
                        "replacement proof could not read captured stderr {}: {error}",
                        self.report_path.display()
                    ),
                }
            }
        }
    }
    let mut fixture = Fixture::with(Project::Declared);
    let mut service = fixture.open().await;
    let storage = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("fixture storage layout");
    let project_root = storage.project().project_root.clone();
    let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", fixture.checkout.display()));
    let adopt_args = [
        OsString::from("adopt"),
        fixture.checkout.as_os_str().to_os_string(),
        OsString::from("--repo-id"),
        OsString::from("fixture/dispatch"),
        OsString::from("--capacity"),
        OsString::from("1g"),
    ];

    // A main adopted before RAW is invisible to this runtime's inventory: a default adopt must
    // refuse rather than journal over it and mint a second main.
    let legacy = project_root.join("main.asif");
    fs::create_dir_all(&project_root).expect("project directory");
    fs::write(&legacy, b"legacy main").expect("plant a legacy main");
    let (refused, _, _) = run(&mut service, adopt_args.clone()).await;
    let error = refused.expect_err("a default adopt over a legacy main refuses");
    assert_eq!(error.code, ErrorCode::Conflict, "{error:?}");
    assert!(error.hint.contains("--re-adopt"), "{error:?}");
    assert_eq!(fs::read(&legacy).expect("legacy main"), b"legacy main");
    assert!(!project_root.join("lifecycle-intents.json").exists());
    assert!(!project_root.join("main.dmg").exists());
    assert!(!pre_cowshed.exists(), "the checkout was not touched");
    fs::remove_file(&legacy).expect("remove the planted legacy main");

    // An ordinary RAW main, with a linked build volume holding warm state.
    let (result, _, _) = run(&mut service, adopt_args).await;
    assert_eq!(result.expect("RAW adoption"), 0);
    let layout = build_volumes(&fixture);
    let main_volume = linked_volume(&layout, &fixture.checkout);
    let mut record = layout
        .read_record(&main_volume)
        .expect("main's build record");
    let warm = fixture.checkout.join(DECLARED_STATE).join("build");
    fs::create_dir_all(&warm).unwrap();
    fs::write(warm.join("build.ninja"), b"warm\n").unwrap();
    fs::write(
        fixture.checkout.join("tracked"),
        b"changed before re-adoption\n",
    )
    .unwrap();
    git(
        &fixture.checkout,
        &["commit", "-q", "-am", "change tracked"],
    );
    fs::write(fixture.checkout.join("notes.txt"), b"untracked notes\n").unwrap();
    let head = git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]);
    let pre_cowshed_head = fs::read(pre_cowshed.join(".git/HEAD")).expect("retained tree");
    let main_image = project_root.join("main.dmg");
    record.tree = Some(
        cowshed_core::api::GitOid::new(git_stdout(
            &fixture.checkout,
            &["rev-parse", "HEAD^{tree}"],
        ))
        .expect("a tree oid"),
    );
    layout
        .write_record(&main_volume, &record)
        .expect("record the warm build's tree before replacement");
    if let ReAdoptionProof::RawReplacement = proof {
        fs::write(
            layout.mount(&main_volume).join("published-cache"),
            b"cached payload\n",
        )
        .expect("warm the build before replacement and the published-loss phase");
        let status = git_stdout(
            &fixture.checkout,
            &["status", "--porcelain", "--untracked-files=all"],
        );
        let old = cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&main_image)
            .expect("main's sidecar")
            .workspace_incarnation;

        // Core retention guidance writes to the real stderr descriptor, not the CLI Output buffer.
        // Observe that actual channel; the isolated nextest process has only this lifecycle walk.
        let report_path = fixture._scratch.path().join("re-adopt-report.stderr");
        let report_file = fs::File::create(&report_path).expect("capture core replacement report");
        // SAFETY: fcntl duplicates the live standard-error descriptor and owns the result.
        let saved = unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_DUPFD_CLOEXEC, 3) };
        assert!(
            saved >= 0,
            "save stderr: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: the successful duplication returned an owned, non-stdio descriptor.
        let mut restore = RestoreStderr {
            saved: Some(unsafe { OwnedFd::from_raw_fd(saved) }),
            report_path,
        };
        // SAFETY: report_file stays open until stderr is restored.
        assert!(
            unsafe { libc::dup2(report_file.as_raw_fd(), libc::STDERR_FILENO) } >= 0,
            "capture stderr: {}",
            std::io::Error::last_os_error()
        );
        let (replacement, stdout, stderr) = run(
            &mut service,
            [
                OsString::from("adopt"),
                fixture.checkout.as_os_str().to_os_string(),
                OsString::from("--re-adopt"),
            ],
        )
        .await;
        restore.restore().expect("restore stderr after replacement");
        drop(report_file);
        let core_report =
            fs::read_to_string(&restore.report_path).expect("read actual replacement guidance");
        // Restore the real channel before asserting: neither an operational refusal nor
        // a panic may disappear into a private capture deleted by fixture teardown.
        eprint!("{core_report}");
        let stderr = String::from_utf8_lossy(&stderr);
        assert!(
            matches!(replacement, Ok(0)),
            "cowshed adopt --re-adopt: {replacement:?}\nstdout: {}\nstderr: {stderr}",
            String::from_utf8_lossy(&stdout)
        );

        let new = cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&main_image)
            .expect("the replacement's sidecar")
            .workspace_incarnation;
        assert_ne!(new, old, "the replacement is a new incarnation");
        assert_eq!(
            fs::read(fixture.checkout.join("tracked")).unwrap(),
            b"changed before re-adoption\n"
        );
        assert_eq!(
            fs::read(fixture.checkout.join("notes.txt")).unwrap(),
            b"untracked notes\n"
        );
        assert_eq!(git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]), head);
        // Read-only, as the product's own probes are: a status that refreshed the published
        // index would change main's `.git` after publication, and the published-loss phase below
        // replays the re-adoption's remount verification against the recorded before-state.
        assert_eq!(
            git_stdout(
                &fixture.checkout,
                &[
                    "--no-optional-locks",
                    "status",
                    "--porcelain",
                    "--untracked-files=all"
                ]
            ),
            status
        );
        assert_eq!(
            linked_volume(&layout, &fixture.checkout),
            main_volume,
            "the build volume keeps its identity"
        );
        assert_eq!(
            layout
                .read_record(&main_volume)
                .expect("the replacement's build record"),
            record,
            "the owner record is carried verbatim"
        );
        assert_eq!(
            fs::read(warm.join("build.ninja")).expect("warm build state"),
            b"warm\n",
            "the build volume's contents are copied, never substituted"
        );
        assert_eq!(
            fs::read(pre_cowshed.join(".git/HEAD")).expect("retained tree"),
            pre_cowshed_head,
            "the original .pre-cowshed tree is never touched"
        );
        let retained = project_root.join("retained");
        for kept in [
            retained.join(format!("main-{}.dmg", old.as_str())),
            retained.join(format!("main-{}.dmg.grants.json", old.as_str())),
            retained.join(format!("build-{}.dmg", main_volume.as_str())),
            retained.join(format!("build-{}.dmg.json", main_volume.as_str())),
        ] {
            assert!(kept.exists(), "{} is retained: {stderr}", kept.display());
            assert!(
                core_report.contains(&kept.display().to_string()),
                "the actual stderr report names {}: {core_report}",
                kept.display()
            );
        }

        service.shutdown().await.expect("stop the runtime");
        prove_published_build_loss(&mut fixture).await;
        return;
    }
    service.shutdown().await.expect("stop the runtime");

    // Start either independent interrupted-recovery walk from this first warm RAW adoption.
    // Only the legacy walk converts detached fixture inputs; the RAW walk keeps their format.
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(fixture.storage.store(), &fixture.checkout),
    )
    .expect("fixture controller");
    if let ReAdoptionProof::LegacyRecovery = proof {
        let build_image = layout.image(&main_volume);
        for image in [&main_image, &build_image] {
            host.detach_existing_image(image, DetachIntent::WhenIdle)
                .expect("seal only this fixture input");
            if image == &main_image {
                // This own underlay blocks publication after the warm build copy, giving the
                // prepublication crash boundary without a mock or a second volume setup.
                fs::write(
                    fixture.checkout.join("publication-blocker"),
                    b"operator data\n",
                )
                .expect("hold the fixture's publication boundary");
            }
            let legacy_image = image.with_extension("asif");
            let convert = cowshed_apfs::CommandRequest::new(
                "/usr/sbin/diskutil",
                [
                    OsString::from("image"),
                    OsString::from("create"),
                    OsString::from("from"),
                    OsString::from("--format"),
                    OsString::from("ASIF"),
                    image.as_os_str().to_os_string(),
                    legacy_image.as_os_str().to_os_string(),
                ],
            );
            let converted = SystemCommandRunner
                .run(&convert)
                .expect("native ASIF fixture conversion");
            assert!(
                converted.succeeded(),
                "ASIF fixture conversion: {converted:?}"
            );
            fs::rename(image, image.with_extension("fixture-raw"))
                .expect("retain original RAW fixture");
            let suffixes: &[&str] = if image == &main_image {
                &[".grants.json", ".ca.key"]
            } else {
                &[".json", ".hold"]
            };
            for suffix in suffixes {
                let from = PathBuf::from(format!("{}{suffix}", image.display()));
                let to = PathBuf::from(format!("{}{suffix}", legacy_image.display()));
                if from.exists() {
                    fs::rename(from, to).expect("carry the fixture's actual sidecar or hold");
                }
            }
            let mount = if image == &main_image {
                fixture.checkout.clone()
            } else {
                layout.mount(&main_volume)
            };
            let attach = cowshed_apfs::CommandRequest::new(
                "/usr/sbin/diskutil",
                [
                    OsString::from("image"),
                    OsString::from("attach"),
                    OsString::from("--nobrowse"),
                    OsString::from("--mountPoint"),
                    mount.as_os_str().to_os_string(),
                    legacy_image.as_os_str().to_os_string(),
                ],
            );
            let attached = SystemCommandRunner
                .run(&attach)
                .expect("attach actual legacy fixture");
            assert!(attached.succeeded(), "legacy fixture attach: {attached:?}");
        }
        assert_eq!(linked_volume(&layout, &fixture.checkout), main_volume);
        let legacy_record: BuildVolumeRecord =
            cowshed_core::metadata::read_json(&build_image.with_extension("asif.json"))
                .expect("the original ASIF build owner record");
        assert_eq!(legacy_record, record);
        // Setup's native project list must not admit this legacy main to rebuild-only refresh.
        // This is the exact production inventory used by NativeSetupHost::build_state_projects.
        let setup_projects = cowshed_core::NativeGatewayInventory::new(fixture.storage.clone())
            .adopted_projects()
            .await
            .expect("setup inventory preserves legacy inputs");
        assert!(
            setup_projects
                .iter()
                .all(|project| { project.repo_id != RepoId::parse("fixture/dispatch").unwrap() })
        );
        assert_eq!(fs::read(warm.join("build.ninja")).unwrap(), b"warm\n");
        assert_eq!(
            cowshed_core::metadata::read_json::<BuildVolumeRecord>(
                &build_image.with_extension("asif.json")
            )
            .unwrap(),
            record
        );
    } else {
        use cowshed_core::storage::apfs::native::{KernelMountSource, SystemKernelMountSource};
        let main_device = SystemKernelMountSource
            .mounts()
            .expect("own fixture mounts")
            .into_iter()
            .find(|mount| mount.mount_point == fixture.checkout)
            .expect("main is mounted at its checkout")
            .source_device;
        let unmount = cowshed_apfs::CommandRequest::new(
            "/sbin/umount",
            [fixture.checkout.as_os_str().to_os_string()],
        );
        let result = SystemCommandRunner.run(&unmount).unwrap();
        assert!(result.succeeded(), "own main unmount: {result:?}");
        fs::write(
            fixture.checkout.join("publication-blocker"),
            b"operator data\n",
        )
        .expect("hold the fixture's publication boundary");
        let remount = cowshed_apfs::CommandRequest::new(
            "/sbin/mount_apfs",
            [
                OsString::from("-o"),
                OsString::from("nobrowse,owners,noatime"),
                OsString::from(&main_device),
                fixture.checkout.as_os_str().to_os_string(),
            ],
        );
        let result = SystemCommandRunner.run(&remount).unwrap();
        assert!(result.succeeded(), "own main remount: {result:?}");
        assert_eq!(git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]), head);
        assert_eq!(fs::read(warm.join("build.ninja")).unwrap(), b"warm\n");
    }
    let mut recovery_service = fixture.open().await;
    let (refused, _, _) = run(
        &mut recovery_service,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;
    let refused = refused.expect_err("the own underlay refuses publication after build copy");
    assert!(
        refused
            .message
            .contains("the checkout a replacement mounts on"),
        "{refused:?}"
    );
    recovery_service
        .shutdown()
        .await
        .expect("stop the interrupted re-adoption runtime");
    let journal_path = project_root.join("lifecycle-intents.json");
    let interrupted =
        cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path).unwrap();
    let interrupted = interrupted
        .get(&WorkspaceName::main())
        .expect("recorded replacement");
    assert!(interrupted.completion.is_none());
    let cowshed_core::storage::recovery::LifecycleIntent::ReAdopt { source, .. } =
        &interrupted.operation
    else {
        panic!("the same re-adoption is recorded")
    };
    let replacement = source.replacement.clone();
    let cowshed_core::storage::recovery::ReAdoptMain::Retained(source_image) = &source.main else {
        panic!("a healthy re-adoption retains main's image")
    };
    let replacement_build = source.build.as_ref().expect("recorded warm build");
    assert!(
        replacement_build.replacement_image.exists(),
        "the build-copy-complete boundary"
    );
    assert!(
        !main_image.exists(),
        "the replacement main is not published yet"
    );
    assert!(
        source_image.retained.exists(),
        "the original source remains retained"
    );
    assert!(
        replacement_build.image.retained.exists(),
        "the original build remains retained"
    );
    assert!(
        !fixture.checkout.join(".git").exists(),
        "there is no Git checkout to discover"
    );
    assert_eq!(
        fs::read(fixture.checkout.join("publication-blocker")).unwrap(),
        b"operator data\n"
    );
    fs::remove_file(fixture.checkout.join("publication-blocker"))
        .expect("the operator releases only this fixture's owned publication blocker");

    // Both normal CLI routes use the same exact strict unfinished-journal authority.
    let inventory = cowshed_core::NativeGatewayInventory::new(fixture.storage.clone());
    assert_eq!(
        inventory
            .forming_re_adoption_root(&fixture.checkout)
            .await
            .unwrap(),
        Some(fixture.checkout.clone())
    );
    let unrelated = fixture._scratch.path().join("unrelated-empty");
    fs::create_dir(&unrelated).unwrap();
    assert_eq!(
        inventory
            .forming_re_adoption_root(&unrelated)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        cowshed_core::forming_re_adoption_checkout(
            &storage,
            &RepoId::parse("other/dispatch").unwrap(),
            &fixture.checkout,
        )
        .unwrap(),
        None,
        "the same checkout never admits a different repository"
    );
    let resolved_mount = cowshed_cli::mount_main::resolve_main_mount(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").unwrap(),
    )
    .expect("the documented mount route resolves a prepublication replacement");
    assert!(resolved_mount.resuming_re_adoption);
    assert_eq!(resolved_mount.checkout_path, fixture.checkout);
    let retained_before: std::collections::BTreeSet<_> =
        fs::read_dir(project_root.join("retained"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
    // The operator can restore the retained source and build read-only at their canonical
    // paths while the same intent is pending. Reproduce that real recovery boundary here.
    use cowshed_core::storage::apfs::native::{KernelMountSource, SystemKernelMountSource};
    let mounts = SystemKernelMountSource
        .mounts()
        .expect("own retained fixture mounts");
    for (from, to) in [
        (&source_image.source_mount, fixture.checkout.clone()),
        (
            &replacement_build.image.source_mount,
            layout.mount(&main_volume),
        ),
    ] {
        let device = &mounts
            .iter()
            .find(|mount| mount.mount_point == *from)
            .expect("the recorded retained fixture input is mounted")
            .source_device;
        let unmount = cowshed_apfs::CommandRequest::new(
            "/usr/sbin/diskutil",
            [OsString::from("unmount"), OsString::from(device)],
        );
        let result = SystemCommandRunner.run(&unmount).unwrap();
        assert!(
            result.succeeded(),
            "normal own retained-input unmount: {result:?}"
        );
        let remount = cowshed_apfs::CommandRequest::new(
            "/usr/sbin/diskutil",
            [
                OsString::from("mount"),
                OsString::from("readOnly"),
                OsString::from("nobrowse"),
                OsString::from("-mountOptions"),
                OsString::from("owners"),
                OsString::from("-mountPoint"),
                to.as_os_str().to_os_string(),
                OsString::from(device),
            ],
        );
        let result = SystemCommandRunner.run(&remount).unwrap();
        assert!(
            result.succeeded(),
            "normal own readonly remount: {result:?}"
        );
        let refused = fs::write(to.join("readonly-remount-probe"), b"must refuse")
            .expect_err("the restored retained input must be read-only");
        assert_eq!(refused.raw_os_error(), Some(libc::EROFS));
    }
    assert_eq!(
        cowshed_core::git::GitRepository::discover(
            fixture.storage.roots().shared_store(),
            &fixture.checkout
        )
        .await
        .unwrap()
        .root(),
        fixture.checkout,
        "Git is now present, but the pending replacement still owns this retry"
    );
    assert_eq!(
        inventory
            .forming_re_adoption_root(&fixture.checkout)
            .await
            .unwrap(),
        Some(fixture.checkout.clone())
    );
    let backend = cowshed_cli::mount_main::NativeMainMountBackend::new(fixture.storage.clone());
    let mut mount_output = Output::new(Vec::new(), Vec::new(), false);
    cowshed_cli::mount_main::dispatch_mount_main(
        &backend,
        &RepoId::parse("fixture/dispatch").unwrap(),
        true,
        &mut mount_output,
    )
    .await
    .expect("the actual native mount command resumes the prepublication replacement");
    recovery_service = fixture.open().await;
    // The retry returns the existing coordinator's main info, never another adopt/reclaim.
    let recovered_info = recovery_service
        .workspace_at(fixture.checkout.clone())
        .await
        .expect("the existing coordinator returns the recovered main");
    assert_eq!(recovered_info.workspace, WorkspaceName::main());
    assert_eq!(recovered_info.mount, fixture.checkout);
    let (mount_stdout, _) = mount_output.into_inner();
    let mount_response: serde_json::Value = serde_json::from_slice(&mount_stdout).unwrap();
    assert_eq!(
        mount_response["result"]["mount"],
        fixture.checkout.to_str().unwrap()
    );
    let recovered_metadata =
        cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&main_image).unwrap();
    assert_eq!(recovered_metadata.workspace_incarnation, replacement);
    assert_eq!(
        inventory
            .forming_re_adoption_root(&fixture.checkout)
            .await
            .unwrap(),
        None
    );
    let resumed =
        cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path).unwrap();
    let resumed = resumed.get(&WorkspaceName::main()).unwrap();
    assert!(resumed.completion.is_some());
    assert_eq!(
        resumed.operation, interrupted.operation,
        "no second replacement was issued"
    );
    let retained_after: std::collections::BTreeSet<_> = fs::read_dir(project_root.join("retained"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(
        retained_after, retained_before,
        "recovery adds no second retained generation"
    );
    assert_eq!(git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]), head);
    assert_eq!(
        fs::read(fixture.checkout.join("notes.txt")).unwrap(),
        b"untracked notes\n"
    );
    assert_eq!(fs::read(warm.join("build.ninja")).unwrap(), b"warm\n");
    assert_eq!(linked_volume(&layout, &fixture.checkout), main_volume);
    assert_eq!(layout.read_record(&main_volume).unwrap(), record);
    assert_eq!(
        fs::read(pre_cowshed.join(".git/HEAD")).unwrap(),
        pre_cowshed_head
    );
    // Published-new-main / empty-checkout kill window: reuse the actual recorded replacement
    // intent, stop the fixture at an empty real mountpoint, then reopen through ordinary startup.
    let journal_path = project_root.join("lifecycle-intents.json");
    let journal =
        cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path).unwrap();
    let operation = journal
        .get(&WorkspaceName::main())
        .unwrap()
        .operation
        .clone();
    recovery_service
        .shutdown()
        .await
        .expect("stop the recovered re-adoption runtime");
    host.detach_existing_image(&main_image, DetachIntent::WhenIdle)
        .expect("produce the actual post-publication empty mountpoint");
    cowshed_core::storage::recovery::LifecycleIntentJournal::update(&journal_path, |journal| {
        journal.begin(operation);
        journal.mark_mutating(&WorkspaceName::main())
    })
    .unwrap();
    assert!(
        !fixture.checkout.join(".git").exists(),
        "the crash window has no Git checkout mounted"
    );
    let recovered = fixture.open().await;
    assert_eq!(git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]), head);
    assert_eq!(fs::read(warm.join("build.ninja")).unwrap(), b"warm\n");
    assert_eq!(linked_volume(&layout, &fixture.checkout), main_volume);
    assert_eq!(layout.read_record(&main_volume).unwrap(), record);
    let recovered_journal =
        cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path).unwrap();
    assert!(
        recovered_journal
            .get(&WorkspaceName::main())
            .unwrap()
            .completion
            .is_some()
    );
    recovered
        .shutdown()
        .await
        .expect("stop recovered fixture runtime");
}

/// `path`'s host allocation is below its apparent length: a sparse image, not a materialised one.
fn assert_host_sparse(path: &Path) {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = fs::metadata(path).expect("an image's metadata");
    assert!(
        metadata.blocks() * 512 < metadata.len(),
        "{} is materialised on the host: {} of {} bytes allocated",
        path.display(),
        metadata.blocks() * 512,
        metadata.len()
    );
}

/// The image lease excludes only cowshed's in-flight disk operation, never an unrelated open
/// descriptor of the very same process. Prove that guard for healthy and missing-main inputs.
async fn assert_re_adopt_refuses_own_build_file(
    fixture: &Fixture,
    service: &mut ActorBridge,
    mount: &Path,
) {
    let held = fs::File::open(mount).expect("hold the fixture's own build mount");
    let (result, _, _) = run(
        service,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;
    let error = result.expect_err("an actual same-PID holder refuses re-adoption");
    assert_eq!(error.code, ErrorCode::Conflict, "{error}");
    assert!(
        error.message.contains(&mount.display().to_string())
            && error
                .message
                .contains(&format!("pid {} (", std::process::id())),
        "the refusal names the held build mount and this process: {error}"
    );
    drop(held);
}

/// One adopted fixture exercises the three missing-main transitions (02_workspaces.md,
/// "Missing main image"): publish an empty build after loss, keep a surviving build under a
/// new id after the next loss, then replay a refused copy after the user commits new work.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_missing_main_recovers_empty_and_surviving_builds_and_replays_live_work() {
    const REMOTE: &str = "https://github.com/fixture/dispatch.git";
    let mut fixture = Fixture::with(Project::Declared);
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let storage = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("fixture storage layout");
    let project_root = storage.project().project_root.clone();
    let journal_path = project_root.join("lifecycle-intents.json");
    let main_image = project_root.join("main.dmg");
    let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", fixture.checkout.display()));
    let layout = build_volumes(&fixture);
    let main_volume = linked_volume(&layout, &fixture.checkout);
    let cache_marker = layout.mount(&main_volume).join("discarded-cache");
    fs::write(&cache_marker, b"old cached payload\n").expect("warm the original build cache");

    assert_re_adopt_refuses_own_build_file(&fixture, &mut service, &layout.mount(&main_volume))
        .await;

    // The completed healthy re-adoption is the journal authority for main's build volume.
    succeed(
        &mut service,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;
    let healthy = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the lifecycle journal after the healthy re-adoption");
    let cowshed_core::storage::recovery::LifecycleIntent::ReAdopt {
        source: healthy_source,
        ..
    } = &healthy
        .get(&WorkspaceName::main())
        .expect("main's completed re-adoption is journaled")
        .operation
    else {
        panic!("main's journal entry is a re-adoption")
    };
    let record = layout
        .read_record(&main_volume)
        .expect("main's build record before its loss");
    let pre_cowshed_head =
        fs::read(pre_cowshed.join(".git/HEAD")).expect("the original tree is retained");

    // A commit makes the plain checkout's HEAD exist only in that checkout.
    fs::write(
        fixture.checkout.join("tracked"),
        b"changed before recovery\n",
    )
    .expect("change tracked");
    git(
        &fixture.checkout,
        &["commit", "-q", "-am", "change tracked"],
    );
    service
        .shutdown()
        .await
        .expect("stop the runtime before main is lost");
    let lost_incarnation =
        cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&main_image)
            .expect("main's Active sidecar before the loss")
            .workspace_incarnation;
    let scratch = fixture._scratch.path();
    let plain_source = scratch.join("plain-source");
    git(
        scratch,
        &[
            "clone",
            "-q",
            fixture.checkout.to_str().expect("a UTF-8 checkout path"),
            plain_source.to_str().expect("a UTF-8 source path"),
        ],
    );

    // Lose main's image with its sidecar and key, and the build volume's image, record and
    // retained contents. Only the journal survives as authority.
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(fixture.storage.store(), &fixture.checkout),
    )
    .expect("fixture controller");
    host.detach_existing_image(&main_image, DetachIntent::WhenIdle)
        .expect("detach main without force");
    let build_image = layout.image(&main_volume);
    host.detach_existing_image(&build_image, DetachIntent::WhenIdle)
        .expect("detach the build volume without force");
    let retained_build = healthy_source
        .build
        .as_ref()
        .expect("the old build is recorded")
        .image
        .retained
        .clone();
    // The retained build goes with its record companion, as the re-adoption's removal guidance
    // names both: the later phases re-adopt this main again under the same build identity, and
    // a stray retained record would stand beside the next retention's original.
    for lost in [
        main_image.clone(),
        cowshed_core::metadata::sidecar_path(&main_image),
        PathBuf::from(format!("{}.ca.key", main_image.display())),
        build_image.clone(),
        layout.record(&main_volume),
        PathBuf::from(format!("{}.json", retained_build.display())),
        retained_build,
    ] {
        fs::remove_file(&lost).unwrap_or_else(|error| panic!("lose {}: {error}", lost.display()));
    }

    // A plain checkout of main's repository at main's path, with uncommitted work.
    fs::remove_dir(&fixture.checkout).expect("the detached mountpoint is an empty directory");
    git(
        scratch,
        &[
            "clone",
            "-q",
            plain_source.to_str().expect("a UTF-8 source path"),
            fixture.checkout.to_str().expect("a UTF-8 checkout path"),
        ],
    );
    git(&fixture.checkout, &["remote", "set-url", "origin", REMOTE]);
    fs::write(fixture.checkout.join("tracked"), b"unstaged plain work\n")
        .expect("dirty the plain tracked file");
    fs::write(
        fixture.checkout.join("notes.txt"),
        b"untracked plain work\n",
    )
    .expect("create plain untracked work");
    let plain_head = git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]);

    let mut recovered = fixture.open().await;
    succeed(
        &mut recovered,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;

    let replacement =
        cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&main_image)
            .expect("the replacement's Active sidecar")
            .workspace_incarnation;
    assert_ne!(
        replacement, lost_incarnation,
        "a new incarnation is published"
    );
    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the lifecycle journal after recovery");
    let entry = journal
        .get(&WorkspaceName::main())
        .expect("main's recovery is journaled");
    assert!(entry.completion.is_some(), "the recovery completed");
    let cowshed_core::storage::recovery::LifecycleIntent::ReAdopt {
        source: recovered_source,
        ..
    } = &entry.operation
    else {
        panic!("main's recovery is a re-adoption")
    };
    assert_eq!(recovered_source.replacement, replacement);
    let cowshed_core::storage::recovery::ReAdoptMain::Missing(missing) = &recovered_source.main
    else {
        panic!("the journal records a missing main")
    };
    assert_eq!(
        git_stdout(&missing.preserved, &["rev-parse", "HEAD"]),
        plain_head,
        "the plain checkout is preserved beside main"
    );
    assert_eq!(
        linked_volume(&layout, &fixture.checkout),
        main_volume,
        "main links its recorded build volume id"
    );
    assert_eq!(
        layout
            .read_record(&main_volume)
            .expect("the published build record"),
        BuildVolumeRecord {
            tree: None,
            ..record.clone()
        },
        "the empty build keeps its id, Linked main owner and createdAt, with no built-at tree"
    );
    assert_host_sparse(&main_image);
    assert_host_sparse(&build_image);
    assert!(
        SystemKernelMountSource
            .mounts()
            .expect("observe the build mount")
            .iter()
            .any(|mount| mount.mount_point == layout.mount(&main_volume)),
        "the empty build volume is mounted where main's link points"
    );
    assert!(!cache_marker.exists(), "the lost cache is never recovered");
    assert_eq!(
        git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]),
        plain_head,
        "main holds the plain checkout's HEAD"
    );
    assert_eq!(
        fs::read(fixture.checkout.join("tracked")).expect("tracked work"),
        b"unstaged plain work\n"
    );
    assert_eq!(
        fs::read(fixture.checkout.join("notes.txt")).expect("untracked work"),
        b"untracked plain work\n"
    );
    assert_eq!(
        git_stdout(&fixture.checkout, &["remote", "get-url", "origin"]),
        REMOTE
    );
    assert_eq!(
        fs::read(pre_cowshed.join(".git/HEAD")).expect("the original tree is retained"),
        pre_cowshed_head,
        "recovery never touches .pre-cowshed"
    );
    recovered
        .shutdown()
        .await
        .expect("stop the recovered runtime");
    prove_missing_main_keeps_surviving_build(&mut fixture).await;
    prove_missing_main_replays_live_work(&mut fixture).await;
    fixture.stop_gateway().await;
}

/// Main's image, sidecar and CA key lost after a completed healthy re-adoption, with a plain Git
/// checkout of main's repository cloned at main's path in their place, its origin naming the
/// project. The build volumes are left as the caller arranged them. Answers the plain HEAD.
fn lose_main_to_a_plain_checkout(fixture: &Fixture, main_image: &Path, phase: &str) -> String {
    const REMOTE: &str = "https://github.com/fixture/dispatch.git";
    let scratch = fixture._scratch.path();
    let plain_source = scratch.join(phase);
    git(
        scratch,
        &[
            "clone",
            "-q",
            fixture.checkout.to_str().expect("a UTF-8 checkout path"),
            plain_source.to_str().expect("a UTF-8 source path"),
        ],
    );
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(fixture.storage.store(), &fixture.checkout),
    )
    .expect("fixture controller");
    host.detach_existing_image(main_image, DetachIntent::WhenIdle)
        .expect("detach main without force");
    for lost in [
        main_image.to_owned(),
        cowshed_core::metadata::sidecar_path(main_image),
        PathBuf::from(format!("{}.ca.key", main_image.display())),
    ] {
        fs::remove_file(&lost).unwrap_or_else(|error| panic!("lose {}: {error}", lost.display()));
    }
    fs::remove_dir(&fixture.checkout).expect("the detached mountpoint is an empty directory");
    git(
        scratch,
        &[
            "clone",
            "-q",
            plain_source.to_str().expect("a UTF-8 source path"),
            fixture.checkout.to_str().expect("a UTF-8 checkout path"),
        ],
    );
    git(&fixture.checkout, &["remote", "set-url", "origin", REMOTE]);
    git_stdout(&fixture.checkout, &["rev-parse", "HEAD"])
}

/// 02_workspaces.md, "Missing main image": main's build volume is the project's one record owned
/// by `Linked main`, whatever its id. A land gives main the landing workspace's volume and
/// releases the one main's completed re-adoption recorded, so after main's image is lost the
/// recorded id names nothing while main's real volume survives. Re-adoption keeps that surviving
/// volume, warm and with its owner record, links main to it and seeds it; it never mints an empty
/// volume under the dead id.
async fn prove_missing_main_keeps_surviving_build(fixture: &mut Fixture) {
    let mut service = fixture.open().await;
    let storage = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("fixture storage layout");
    let main_image = storage.project().project_root.join("main.dmg");
    let layout = build_volumes(fixture);
    succeed(
        &mut service,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;
    service
        .shutdown()
        .await
        .expect("stop the runtime before main is lost");

    // A land moved main onto another volume: main's warm volume now has an id the completed
    // re-adoption never recorded, and the recorded id names nothing.
    let recorded = linked_volume(&layout, &fixture.checkout);
    fs::write(
        layout.mount(&recorded).join("warm-cache"),
        b"main's warm build state\n",
    )
    .expect("warm main's build volume");
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(fixture.storage.store(), &fixture.checkout),
    )
    .expect("fixture controller");
    host.detach_existing_image(&layout.image(&recorded), DetachIntent::WhenIdle)
        .expect("detach main's build volume without force");
    let surviving = BuildVolumeId::mint();
    for (from, to) in [
        (layout.image(&recorded), layout.image(&surviving)),
        (layout.record(&recorded), layout.record(&surviving)),
        (layout.hold_path(&recorded), layout.hold_path(&surviving)),
    ] {
        if from.exists() {
            fs::rename(&from, &to).expect("move main's volume to its landed id");
        }
    }
    let owner = layout
        .read_record(&surviving)
        .expect("main's surviving build record");
    assert!(
        matches!(&owner.role, BuildVolumeRole::Linked { checkout } if checkout.is_main()),
        "the surviving volume is main's: {owner:?}"
    );
    let plain_head = lose_main_to_a_plain_checkout(fixture, &main_image, "surviving-source");

    let mut recovered = fixture.open().await;
    succeed(
        &mut recovered,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;

    assert_eq!(
        git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]),
        plain_head
    );
    assert_eq!(
        linked_volume(&layout, &fixture.checkout),
        surviving,
        "main links its surviving build volume, not the recorded id"
    );
    assert_eq!(
        layout
            .read_record(&surviving)
            .expect("the kept build record"),
        owner,
        "the kept volume keeps its owner record, built-at tree included"
    );
    assert_eq!(
        fs::read(layout.mount(&surviving).join("warm-cache")).expect("the kept warm state"),
        b"main's warm build state\n"
    );
    assert!(
        !layout.image(&recorded).exists() && !layout.record(&recorded).exists(),
        "nothing is minted under the dead recorded id"
    );
    let replacement =
        cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&main_image)
            .expect("the replacement's Active sidecar")
            .workspace_incarnation;
    let (_, seed) = layout
        .seed_of(&WorkspaceName::main(), &replacement)
        .expect("main's seed lookup")
        .expect("the kept volume is seeded for the replacement");
    assert_eq!(seed.tree, owner.tree, "the seed carries the kept tree");
    recovered
        .shutdown()
        .await
        .expect("stop the recovered runtime");
}

/// 02_workspaces.md, "Missing main image": until the handoff swaps it aside, a missing main's copy
/// source is the user's live checkout. An attempt that refuses after it is journaled and before
/// anything is published (here, a file the copy cannot read) leaves the user free to commit; the
/// replay records the checkout's Git state again before it copies, so it publishes the new HEAD
/// instead of refusing against the state the first attempt recorded, on every run.
async fn prove_missing_main_replays_live_work(fixture: &mut Fixture) {
    let mut service = fixture.open().await;
    let storage = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("fixture storage layout");
    let project_root = storage.project().project_root.clone();
    let journal_path = project_root.join("lifecycle-intents.json");
    let main_image = project_root.join("main.dmg");
    succeed(
        &mut service,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;
    service
        .shutdown()
        .await
        .expect("stop the runtime before main is lost");
    let layout = build_volumes(fixture);
    let build_mount = layout.mount(&linked_volume(&layout, &fixture.checkout));
    lose_main_to_a_plain_checkout(fixture, &main_image, "replay-source");

    // An ignored file the copy cannot read: the attempt is journaled, then refuses at the copy.
    let excludes = fixture.checkout.join(".git/info/exclude");
    let mut excluded = fs::read(&excludes).unwrap_or_default();
    excluded.extend_from_slice(b"\nunreadable.bin\n");
    fs::write(&excludes, excluded).expect("ignore the unreadable file");
    let unreadable = fixture.checkout.join("unreadable.bin");
    fs::write(&unreadable, b"no one may read this\n").expect("write the unreadable file");
    fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000))
        .expect("make the file unreadable");
    let mut refused = fixture.open().await;
    assert_re_adopt_refuses_own_build_file(fixture, &mut refused, &build_mount).await;
    let (result, _, _) = run(
        &mut refused,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;
    assert!(
        !matches!(result, Ok(0)),
        "the unreadable file refuses the copy: {result:?}"
    );
    refused.shutdown().await.expect("stop the refused runtime");
    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the refused journal");
    let entry = journal
        .get(&WorkspaceName::main())
        .expect("main's re-adoption is recorded");
    assert!(
        entry.completion.is_none(),
        "the refused re-adoption was journaled and left unfinished"
    );
    assert!(!main_image.exists(), "nothing was published");

    // The user repairs the file and goes on working: a commit moves HEAD and the refs.
    fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o644))
        .expect("make the file readable again");
    fs::write(
        fixture.checkout.join("tracked"),
        b"committed after the refusal\n",
    )
    .expect("change tracked");
    git(
        &fixture.checkout,
        &["commit", "-q", "-am", "work after the refusal"],
    );
    let new_head = git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]);

    // Opening the project replays the unfinished re-adoption, as `adopt --re-adopt` does.
    let resumed = fixture.open().await;
    assert_eq!(
        git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]),
        new_head,
        "main holds the commit made after the refused attempt"
    );
    assert_eq!(
        fs::read(fixture.checkout.join("tracked")).expect("tracked work"),
        b"committed after the refusal\n"
    );
    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the resumed journal");
    assert!(
        journal
            .get(&WorkspaceName::main())
            .expect("main's re-adoption is recorded")
            .completion
            .is_some(),
        "the replay completed"
    );
    resumed.shutdown().await.expect("stop the resumed runtime");
}

/// 02_workspaces.md, "Re-adoption" step 3: an interrupted re-adoption whose retained build image
/// is gone resumes from its retained main, publishes the recorded build volume empty under its
/// id, and discards the staged build copy nothing can verify any more.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_a_resumed_re_adopt_whose_retained_build_is_gone_publishes_it_empty() {
    let mut fixture = Fixture::with(Project::Declared);
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let storage = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("fixture storage layout");
    let project_root = storage.project().project_root.clone();
    let journal_path = project_root.join("lifecycle-intents.json");
    let main_image = project_root.join("main.dmg");
    let layout = build_volumes(&fixture);
    let main_volume = linked_volume(&layout, &fixture.checkout);
    let record = layout
        .read_record(&main_volume)
        .expect("main's build record");
    let cache_marker = layout.mount(&main_volume).join("lost-cache");
    fs::write(&cache_marker, b"cached payload\n").expect("warm the build cache");
    fs::write(fixture.checkout.join("tracked"), b"uncommitted work\n").expect("dirty main");
    let head = git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]);
    let status = git_stdout(&fixture.checkout, &["status", "--porcelain"]);
    service.shutdown().await.expect("stop the runtime");

    // An underlay file at the checkout blocks publication after the build copy: the
    // prepublication crash boundary, with main's own image attached back over it.
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(fixture.storage.store(), &fixture.checkout),
    )
    .expect("fixture controller");
    host.detach_existing_image(&main_image, DetachIntent::WhenIdle)
        .expect("detach main without force");
    fs::write(
        fixture.checkout.join("publication-blocker"),
        b"operator data\n",
    )
    .expect("hold the publication boundary");
    let attach = cowshed_apfs::CommandRequest::new(
        "/usr/sbin/diskutil",
        [
            OsString::from("image"),
            OsString::from("attach"),
            OsString::from("--nobrowse"),
            OsString::from("--mountOptions"),
            OsString::from("owners"),
            OsString::from("--mountPoint"),
            fixture.checkout.as_os_str().to_os_string(),
            main_image.as_os_str().to_os_string(),
        ],
    );
    let attached = SystemCommandRunner.run(&attach).expect("reattach main");
    assert!(attached.succeeded(), "reattach main: {attached:?}");
    let mut interrupted = fixture.open().await;
    let (refused, _, _) = run(
        &mut interrupted,
        [
            OsString::from("adopt"),
            fixture.checkout.as_os_str().to_os_string(),
            OsString::from("--re-adopt"),
        ],
    )
    .await;
    refused.expect_err("the underlay refuses publication after the build copy");
    interrupted
        .shutdown()
        .await
        .expect("stop the interrupted runtime");
    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the interrupted journal");
    let entry = journal
        .get(&WorkspaceName::main())
        .expect("main's re-adoption is recorded");
    assert!(entry.completion.is_none(), "the re-adoption is unfinished");
    let cowshed_core::storage::recovery::LifecycleIntent::ReAdopt { source, .. } = &entry.operation
    else {
        panic!("main's journal entry is a re-adoption")
    };
    let source = source.clone();
    let build = source.build.clone().expect("the recorded build volume");
    assert!(
        build.replacement_image.exists(),
        "the build-copy-complete boundary"
    );

    // The retained build image and the old seeds' images are lost; their records survive.
    host.detach_existing_image(&build.image.retained, DetachIntent::WhenIdle)
        .expect("detach the retained build without force");
    fs::remove_file(&build.image.retained).expect("lose the retained build image");
    for seed in &source.old_seeds {
        if seed.retained.exists() {
            fs::remove_file(&seed.retained).expect("lose an old seed image");
        }
    }
    fs::remove_file(fixture.checkout.join("publication-blocker"))
        .expect("release the publication boundary");

    // Opening the project replays the unfinished re-adoption, as `adopt --re-adopt` does.
    let resumed = fixture.open().await;

    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the resumed journal");
    let entry = journal
        .get(&WorkspaceName::main())
        .expect("main's re-adoption is recorded");
    assert!(
        entry.completion.is_some(),
        "the resumed re-adoption completed"
    );
    assert_eq!(
        cowshed_core::metadata::DetachedWorkspaceMetadata::read_for_image(&main_image)
            .expect("the replacement's sidecar")
            .workspace_incarnation,
        source.replacement,
        "the recorded replacement is published"
    );
    assert_eq!(git_stdout(&fixture.checkout, &["rev-parse", "HEAD"]), head);
    assert_eq!(
        git_stdout(&fixture.checkout, &["status", "--porcelain"]),
        status
    );
    assert_eq!(
        linked_volume(&layout, &fixture.checkout),
        main_volume,
        "main links its recorded build volume id"
    );
    assert_eq!(
        layout
            .read_record(&main_volume)
            .expect("the published build record"),
        BuildVolumeRecord {
            tree: None,
            ..record.clone()
        },
        "the empty build keeps its id, Linked main owner and createdAt, with no built-at tree"
    );
    assert!(
        SystemKernelMountSource
            .mounts()
            .expect("observe the build mount")
            .iter()
            .any(|mount| mount.mount_point == layout.mount(&main_volume)),
        "the empty build volume is mounted where main's link points"
    );
    assert!(!cache_marker.exists(), "the lost cache is never recovered");
    assert!(
        !build.replacement_image.exists(),
        "the unverifiable staged build copy is discarded"
    );
    let cowshed_core::storage::recovery::ReAdoptMain::Retained(image) = &source.main else {
        panic!("the re-adoption retains main's image")
    };
    assert!(image.retained.exists(), "main's retained image is kept");
    // The empty volume sits at the RAW original's own build image path, so its hold file is the
    // one the re-adoption's source build claim already locks. Its seed is captured under that
    // claim; a second exclusive claim on the same hold would refuse the seed, and the replay with
    // it (16_build_volumes.md, "Targets and seeds").
    let (seed, seed_record) = layout
        .seed_of(&WorkspaceName::main(), &source.replacement)
        .expect("main's seed lookup")
        .expect("the replacement has a seed of its empty build");
    assert_eq!(
        seed_record.tree, None,
        "a seed of an empty build has no tree"
    );
    assert!(
        layout.image(&seed).exists(),
        "the seed of the empty build is captured"
    );
    resumed.shutdown().await.expect("stop the resumed runtime");
    fixture.stop_gateway().await;
}

/// 02_workspaces.md, "Re-adoption" step 3: a re-adoption that published main's build replacement
/// under its owner record, then stopped before the replacement's seed and its conclusion, resumes
/// even when its retained build image is lost after that publication. Nothing reads the retained
/// image past publication: the published warm replacement and its owner record are kept, and the
/// replacement's seed is captured from it under the source build claim.
async fn prove_published_build_loss(fixture: &mut Fixture) {
    let storage = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("fixture storage layout");
    let journal_path = storage
        .project()
        .project_root
        .join("lifecycle-intents.json");
    let layout = build_volumes(fixture);
    let main_volume = linked_volume(&layout, &fixture.checkout);
    let cache_marker = layout.mount(&main_volume).join("published-cache");
    let main = WorkspaceName::main();
    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the completed journal");
    let cowshed_core::storage::recovery::LifecycleIntent::ReAdopt { source, .. } = &journal
        .get(&main)
        .expect("main's re-adoption is recorded")
        .operation
    else {
        panic!("main's journal entry is a re-adoption")
    };
    let source = source.clone();
    let build = source.build.clone().expect("the recorded build volume");
    let owner = layout
        .read_record(&main_volume)
        .expect("the published build record");
    assert_eq!(
        owner, build.owner_record,
        "published under its owner record"
    );
    assert!(
        owner.tree.is_some(),
        "the owner record names a built-at tree"
    );

    // The crash boundary: the build replacement is published with its owner record, and neither
    // the replacement's seed nor the conclusion happened.
    let (seed, _) = layout
        .seed_of(&main, &source.replacement)
        .expect("main's seed lookup")
        .expect("the completed re-adoption froze a seed");
    for path in [
        layout.image(&seed),
        layout.record(&seed),
        layout.hold_path(&seed),
    ] {
        if path.exists() {
            fs::remove_file(&path)
                .unwrap_or_else(|error| panic!("unwind {}: {error}", path.display()));
        }
    }
    cowshed_core::storage::recovery::LifecycleIntentJournal::update(&journal_path, |state| {
        let operation = state
            .get(&main)
            .expect("main's re-adoption is recorded")
            .operation
            .clone();
        state.clear(&main);
        state.begin(operation);
        state.mark_mutating(&main)
    })
    .expect("reopen main's re-adoption at the crash boundary");

    // After publication, the retained build image is lost.
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(fixture.storage.store(), &fixture.checkout),
    )
    .expect("fixture controller");
    host.detach_existing_image(&build.image.retained, DetachIntent::WhenIdle)
        .expect("detach the retained build without force");
    fs::remove_file(&build.image.retained).expect("lose the retained build image");

    // Opening the project replays the unfinished re-adoption, as `adopt --re-adopt` does.
    let resumed = fixture.open().await;

    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(&journal_path)
        .expect("the resumed journal");
    assert!(
        journal
            .get(&main)
            .expect("main's re-adoption is recorded")
            .completion
            .is_some(),
        "the resumed re-adoption completed"
    );
    assert_eq!(
        layout
            .read_record(&main_volume)
            .expect("the build record after the replay"),
        owner,
        "the published replacement keeps its owner record, built-at tree included"
    );
    assert_eq!(
        fs::read(&cache_marker).expect("the published warm cache"),
        b"cached payload\n",
        "the published replacement is kept, not replaced by an empty volume"
    );
    assert!(
        layout
            .seed_of(&main, &source.replacement)
            .expect("main's seed lookup")
            .is_some(),
        "the replacement's seed is captured under the source build claim"
    );
    resumed.shutdown().await.expect("stop the resumed runtime");
}

/// A rename whose source is edited after the destination's canonical clone is published. The
/// observer waits for the canonical sidecar to turn `Active` (the publication the rename's
/// preservation guard starts after), then writes the source. The rename must refuse with the
/// `rm <destination> --force` cancel, keep both images, and keep the edits it raced.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_rename_keeps_edits_written_after_clone_publication() {
    let mut fixture = Fixture::with(Project::Plain);
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service
        .reconcile_gateway()
        .await
        .expect("install main gateway session");
    let (created, _, stderr) = run(&mut service, ["fork", "a"]).await;
    assert_eq!(
        created.unwrap_or_else(|error| panic!(
            "create a: {error}; {}",
            String::from_utf8_lossy(&stderr)
        )),
        0
    );
    let a_mount = service.path("a", false).await.expect("mounted a").mount;
    fs::write(a_mount.join("tracked.txt"), b"one\n").expect("tracked file");
    git(&a_mount, &["add", "tracked.txt"]);
    git(&a_mount, &["commit", "-qm", "tracked"]);
    fs::write(a_mount.join("tracked.txt"), b"two\n").expect("tracked edit");
    fs::write(a_mount.join("u"), b"u1\n").expect("untracked file");

    let layout = cowshed_core::storage::StorageLayout::new(
        fixture.storage.store(),
        &RepoId::parse("fixture/dispatch").expect("repository identity"),
    )
    .expect("project layout");
    let name_a = WorkspaceName::new("a").expect("workspace name");
    let name_b = WorkspaceName::new("b").expect("workspace name");
    let image_a = layout.canonical_image(&name_a).expect("source image path");
    let image_b = layout
        .canonical_image(&name_b)
        .expect("destination image path");
    let sidecar_b = image_b.sidecar().to_path_buf();
    let image_b_path = image_b.image().to_path_buf();
    let image_a_path = image_a.image().to_path_buf();

    let observer = tokio::task::spawn_blocking({
        let tracked = a_mount.join("tracked.txt");
        let untracked = a_mount.join("u");
        move || -> Instant {
            loop {
                match cowshed_core::metadata::read_json::<
                    cowshed_core::metadata::DetachedWorkspaceMetadata,
                >(&sidecar_b)
                {
                    Ok(metadata)
                        if metadata.publication_state
                            == cowshed_core::metadata::PublicationState::Active =>
                    {
                        break;
                    }
                    Ok(_) => std::thread::sleep(Duration::from_millis(1)),
                    Err(cowshed_core::metadata::MetadataError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::NotFound =>
                    {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("read the destination sidecar: {error}"),
                }
            }
            fs::write(&tracked, b"five\n").expect("edit tracked after publication");
            fs::write(&untracked, b"u2\n").expect("edit untracked after publication");
            Instant::now()
        }
    });

    let (result, _, _) = run(&mut service, ["mv", "a", "b"]).await;
    let finished = Instant::now();
    let written = observer.await.expect("publication observer");
    assert!(
        written <= finished,
        "the source edits must race the live rename, not follow it"
    );
    let error = result.expect_err("a rename whose source changed after clone must refuse");
    assert_eq!(error.code.as_str(), "conflict", "{}", error.message);
    assert!(error.hint.contains("rm b --force"), "{}", error.hint);

    assert_eq!(
        fs::read(a_mount.join("tracked.txt")).expect("source tracked"),
        b"five\n"
    );
    assert_eq!(
        fs::read(a_mount.join("u")).expect("source untracked"),
        b"u2\n"
    );
    assert!(
        image_a_path.exists(),
        "the source image survives the refusal"
    );
    assert!(
        image_b_path.exists(),
        "the destination image survives the refusal"
    );

    let b_mount = layout
        .workspace_mount(&name_b)
        .expect("destination mount path");
    assert_eq!(
        fs::read(b_mount.join("tracked.txt")).expect("destination tracked"),
        b"two\n",
        "the destination keeps the bytes captured at its clone, not the post-publication edit"
    );
    assert_eq!(
        fs::read(b_mount.join("u")).expect("destination untracked"),
        b"u1\n",
        "the destination keeps the untracked bytes captured at its clone"
    );

    let journal = cowshed_core::storage::recovery::LifecycleIntentJournal::load(
        &layout
            .project()
            .project_root
            .join(cowshed_core::storage::recovery::LIFECYCLE_INTENTS_FILE),
    )
    .expect("lifecycle intent journal");
    let record = journal
        .get(&name_b)
        .expect("the unfinished rename stays journaled under its destination");
    assert!(
        record.completion.is_none(),
        "the rename must stay unfinished"
    );
    assert!(
        matches!(
            &record.operation,
            cowshed_core::storage::recovery::LifecycleIntent::Fork {
                source,
                destination,
                rename: true,
            } if *source == name_a && *destination == name_b
        ),
        "the journaled operation must be the rename of a into b"
    );
    assert_eq!(
        record.phase,
        cowshed_core::storage::recovery::LifecycleIntentPhase::Mutating
    );

    service.shutdown().await.expect("stop the dispatch service");
    fixture.stop_gateway().await;
}

fn write_emit_project(checkout: &Path, node: &Path, guarded: bool) {
    fs::create_dir_all(checkout.join("b")).unwrap();
    fs::write(checkout.join("b/src.txt"), b"src\n").unwrap();
    // The producer's outputs are generated, never source: ignored as `a/generated.txt` is.
    fs::write(checkout.join("b/.gitignore"), "/gen.txt\n/uncached.txt\n").unwrap();
    let node = node.display();
    let guard = if guarded {
        "require('fs').readFileSync('node_modules/runtime-ok');"
    } else {
        ""
    };
    fs::write(
        checkout.join("b/project.json"),
        format!(
            r#"{{"name":"b","targets":{{
  "emit":{{"executor":"nx:run-commands","cache":true,"inputs":["{{projectRoot}}/src.txt"],"outputs":["{{projectRoot}}/gen.txt"],
    "options":{{"command":"{node} -e \"{guard}require('fs').writeFileSync('b/gen.txt','emit\\n')\""}}}},
  "uncached":{{"executor":"nx:run-commands","cache":false,"inputs":["{{projectRoot}}/src.txt"],"outputs":["{{projectRoot}}/uncached.txt"],
    "options":{{"command":"{node} -e \"require('fs').writeFileSync('b/uncached.txt','uncached\\n')\""}}}},
  "empty":{{"executor":"nx:run-commands","cache":true,"inputs":["{{projectRoot}}/src.txt"],"outputs":[],
    "options":{{"command":"{node} -e \"0\""}}}}}}}}"#
        ),
    )
    .unwrap();
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LandDaemonRecord {
    process_id: i32,
}

/// Land 4 (16_build_volumes.md): a landing daemon the job ledger does not prove the job's own is
/// never signalled. A daemon started detached from the job, as stock Nx does, skips adoption as
/// `LandingDaemonStayed` and is still running after the land.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_land_leaves_a_stock_detached_landing_daemon_running_and_skips_adoption() {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let topic = new_workspace(&mut service, "outputs").await;
    fs::write(topic.join("a/src.txt"), "changed\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "change output input"]);
    sh(&mut service, "outputs", &nx_run_many(&node, "build")).await;

    // A real foreign stock daemon owns a standalone root that retirement must not remove.
    // Its produced record nominates it in the landing checkout; it grants no ledger ownership.
    let foreign = fixture._scratch.path().join("foreign-daemon-checkout");
    clone_tree(
        &checkout_template(Project::Build {
            nx: Some((&nx, &node)),
            rust: None,
        }),
        &foreign,
    );
    let state = fixture._scratch.path().join("foreign-daemon-state");
    let data = state.join("workspace-data");
    let socket = cowshed_core::sandbox::workspace_runtime_link(&foreign).join("nx");
    fs::create_dir_all(&socket).unwrap();
    let mut command = Command::new(&node);
    command
        .args(["node_modules/nx/dist/bin/nx.js", "daemon", "--start"])
        .current_dir(&foreign);
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("NX_") || key == "CI" {
            command.env_remove(key);
        }
    }
    command
        .env("NX_USE_LOCAL", "true")
        .env("NX_WORKSPACE_ROOT_PATH", &foreign)
        .env("NX_CACHE_DIRECTORY", state.join("cache"))
        .env("NX_WORKSPACE_DATA_DIRECTORY", &data)
        .env("NX_SOCKET_DIR", &socket);
    let started = command.output().expect("start the standalone stock daemon");
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    let record = data.join("d/server-process.json");
    let bytes = fs::read(&record).expect("the standalone daemon's produced record");
    let daemon_pid = serde_json::from_slice::<LandDaemonRecord>(&bytes)
        .unwrap()
        .process_id;
    let process = cowshed_core::runtime::job_groups::Process::observe(daemon_pid)
        .unwrap()
        .expect("the actual foreign identity");
    let exit = process
        .watch_exit()
        .expect("watch the foreign identity before land");
    fs::write(
        topic.join(".nx/workspace-data/d/server-process.json"),
        &bytes,
    )
    .unwrap();

    let report = land(&mut service, "outputs", true, &[]).await;
    match &report.build_volume.adoption {
        Adoption::Skipped {
            reason: AdoptionSkip::LandingDaemonStayed { daemon },
        } => assert_eq!(daemon.pid, daemon_pid, "{report:?}"),
        other => panic!("an unproven landing daemon skips adoption: {other:?}"),
    }
    assert_ne!(report.build_volume.seed, LandSeed::Frozen, "{report:?}");
    assert!(
        !exit.within(Duration::ZERO).unwrap(),
        "the actual foreign identity survived retirement"
    );
    assert_eq!(
        fs::read(&record).unwrap(),
        bytes,
        "retirement leaves the foreign owner record unchanged"
    );
    let socket_path: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    std::os::unix::net::UnixStream::connect(socket_path["socketPath"].as_str().unwrap())
        .expect("the real foreign daemon socket survives");

    // Cleanup belongs to the daemon's standalone owner, only after the survival proof.
    let mut stop = Command::new(&node);
    stop.args(["node_modules/nx/dist/bin/nx.js", "daemon", "--stop"])
        .current_dir(&foreign);
    for (key, value) in command.get_envs() {
        if let Some(value) = value {
            stop.env(key, value);
        } else {
            stop.env_remove(key);
        }
    }
    let stopped = stop
        .output()
        .expect("stop the standalone stock daemon through its own Nx owner");
    assert!(
        stopped.status.success(),
        "{}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    exit.wait().expect("owned foreign cleanup completes");
    fs::remove_dir_all(socket.parent().expect("owned short socket root"))
        .expect("remove the standalone daemon socket namespace");
    service.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// A default land cannot turn Land 4's unproven nominee into signal authority at retirement.
/// A live socket makes the record-nominated close capable of signalling the foreign process.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_default_land_never_signals_a_foreign_nominee_during_retirement() {
    let (nx, node) = repository_nx();
    let mut fixture = Fixture::with(Project::Build {
        nx: Some((&nx, &node)),
        rust: None,
    });
    write_emit_project(&fixture.checkout, &node, false);
    git(&fixture.checkout, &["add", "b"]);
    git(
        &fixture.checkout,
        &["commit", "-q", "-m", "add emit producer"],
    );
    let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
    let mut service = serve_project(&mut fixture, &nx_reads).await;
    let topic = new_workspace(&mut service, "outputs").await;
    fs::write(topic.join("a/src.txt"), "changed\n").unwrap();
    git(&topic, &["commit", "-q", "-am", "change output input"]);
    sh(&mut service, "outputs", &nx_run_many(&node, "build")).await;
    // A process no job started, under sandbox-exec with signals to other pids denied. The ledger
    // names no group for it, so the forged record is an unproven nominee.
    let mut foreign = Command::new("/usr/bin/sandbox-exec")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .args([
            "-p",
            "(version 1)(allow default)(deny signal (target others))",
            "/bin/sh",
            "-c",
            "echo READY; read _",
        ])
        .spawn()
        .expect("sandboxed foreign sleeper");
    use std::io::BufRead as _;
    let mut ready = String::new();
    std::io::BufReader::new(foreign.stdout.take().expect("foreign stdout"))
        .read_line(&mut ready)
        .expect("settled sandboxed foreign nominee");
    assert_eq!(ready.trim(), "READY");
    let foreign_pid = i32::try_from(foreign.id()).expect("pid");
    let socket = fixture._scratch.path().join("foreign.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket).expect("live foreign socket");
    let record = topic.join(".nx/workspace-data/d/server-process.json");
    fs::create_dir_all(record.parent().expect("record directory")).unwrap();
    fs::write(
        &record,
        serde_json::json!({
            "processId": foreign_pid,
            "socketPath": socket,
        })
        .to_string(),
    )
    .unwrap();

    let report = land(&mut service, "outputs", true, &[]).await;
    assert!(
        report.retired,
        "the default land must exercise retirement: {report:?}"
    );
    match &report.build_volume.adoption {
        Adoption::Skipped {
            reason: AdoptionSkip::LandingDaemonStayed { daemon },
        } => assert_eq!(daemon.pid, foreign_pid, "{report:?}"),
        other => panic!("a forged nominee skips adoption: {other:?}"),
    }
    assert!(
        foreign
            .try_wait()
            .expect("observe the foreign process")
            .is_none(),
        "the foreign process survived the complete default land, including retirement"
    );
    std::os::unix::net::UnixStream::connect(&socket).expect("the foreign listener remains live");
    foreign.kill().expect("stop the test's own foreign sleeper");
    foreign.wait().expect("reap the sleeper");
    drop(listener);
    drop(service);
    fixture.stop_gateway().await;
}

/// An unreadable or malformed landing daemon record cannot authorize retirement or adoption.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_land_skips_an_unverifiable_daemon_record_before_quiesce() {
    let (nx, node) = repository_nx();
    for unreadable in [false, true] {
        let mut fixture = Fixture::with(Project::Build {
            nx: Some((&nx, &node)),
            rust: None,
        });
        write_emit_project(&fixture.checkout, &node, false);
        git(&fixture.checkout, &["add", "b"]);
        git(
            &fixture.checkout,
            &["commit", "-q", "-m", "add emit producer"],
        );
        let nx_reads = nx_read_paths(&fixture_nx(&fixture.checkout));
        let mut service = serve_project(&mut fixture, &nx_reads).await;
        let topic = new_workspace(&mut service, "outputs").await;
        fs::write(topic.join("a/src.txt"), "changed\n").unwrap();
        git(&topic, &["commit", "-q", "-am", "change output input"]);
        sh(&mut service, "outputs", &nx_run_many(&node, "build")).await;
        let record = topic.join(".nx/workspace-data/d/server-process.json");
        fs::create_dir_all(record.parent().unwrap()).unwrap();
        if record.exists() {
            fs::remove_file(&record).unwrap();
        }
        if unreadable {
            fs::create_dir(&record).unwrap();
        } else {
            fs::write(&record, b"{not a daemon record").unwrap();
        }
        let report = land(&mut service, "outputs", true, &[]).await;
        match &report.build_volume.adoption {
            Adoption::Skipped {
                reason:
                    AdoptionSkip::LandingDaemonUnverifiable {
                        record: skipped_record,
                        reason,
                    },
            } => {
                assert_eq!(skipped_record, &record);
                assert!(!reason.is_empty());
            }
            other => panic!("an unverifiable daemon record skips adoption: {other:?}"),
        }
        assert_ne!(report.build_volume.seed, LandSeed::Frozen, "{report:?}");
        drop(service);
        fixture.stop_gateway().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_managed_install_restores_developer_links_before_nx_and_cleans_absolute_bypass() {
    cowshed_core::runtime::shell_host::register(
        cowshed_core::runtime::shell_host::ShellHostProgram::with_arguments(
            cowshed(),
            vec![cowshed_core::runtime::shell_host::SHELL_HOST_ARGUMENT.into()],
        ),
    );
    let (repository, node) = repository_nx();
    let mut fixture = Fixture::with(Project::Build {
        nx: Some((&repository, &node)),
        rust: None,
    });
    let (developer, nx) = developer_nx(&fixture);
    let registry = fixture.checkout.join("fixture-registry-nx");
    fs::create_dir_all(registry.join("dist/bin")).unwrap();
    fs::write(
        registry.join("package.json"),
        r#"{"name":"nx","version":"23.2.1","bin":{"nx":"dist/bin/nx.js"}}"#,
    )
    .unwrap();
    fs::write(
        registry.join("dist/bin/nx.js"),
        "#!/usr/bin/env node\nthrow new Error('registry replacement was not restored');\n",
    )
    .unwrap();
    fs::set_permissions(
        registry.join("dist/bin/nx.js"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    fs::write(fixture.checkout.join("package.json"), serde_json::to_vec(&serde_json::json!({
        "name": "fixture", "private": true, "dependencies": { "nx": "file:./fixture-registry-nx" }
    })).unwrap()).unwrap();
    let bun = host_bun();
    let installed = Command::new(&bun)
        .args(["install", "--ignore-scripts"])
        .current_dir(&fixture.checkout)
        .output()
        .expect("generate the actual local-package lockfile");
    assert!(
        installed.status.success(),
        "{}",
        String::from_utf8_lossy(&installed.stderr)
    );
    for entry in ["node_modules/nx", "node_modules/.bin/nx"] {
        let path = fixture.checkout.join(entry);
        let metadata = fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("installed fixture entry {}: {error}", path.display()));
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(&path).unwrap_or_else(|error| {
                panic!("remove installed directory {}: {error}", path.display())
            });
        } else {
            fs::remove_file(&path).unwrap_or_else(|error| {
                panic!("remove installed entry {}: {error}", path.display())
            });
        }
    }
    std::os::unix::fs::symlink(&nx, fixture.checkout.join("node_modules/nx")).unwrap();
    std::os::unix::fs::symlink(
        "../nx/dist/bin/nx.js",
        fixture.checkout.join("node_modules/.bin/nx"),
    )
    .unwrap();
    git(
        &fixture.checkout,
        &["add", "package.json", "bun.lock", "fixture-registry-nx"],
    );
    git(
        &fixture.checkout,
        &["commit", "-q", "-m", "record local installer fixture"],
    );
    let reads = nx_dependencies::installed_nx_read_paths(&nx, Some(&developer));
    // The file provider travels inside each checkout and needs no sibling read grant.
    let mut service = serve_project(&mut fixture, &reads).await;
    succeed(&mut service, ["fork", "links"]).await;
    let topic = service
        .path("links", false)
        .await
        .expect("mounted links workspace")
        .mount;
    assert_eq!(
        fixture_nx(&topic),
        nx,
        "the fork preserves the genuine sibling product"
    );
    let tslib_manifest = nx.join("node_modules/tslib/package.json");
    assert!(
        tslib_manifest.is_file(),
        "the ordinary installed sibling tslib payload exists"
    );
    let native_manifest = nx.join("node_modules/@nx/nx-darwin-arm64/package.json");
    let native_identity: serde_json::Value = serde_json::from_slice(
        &fs::read(&native_manifest)
            .expect("ordinary Bun installed the pinned native platform package"),
    )
    .unwrap();
    assert_eq!(native_identity["name"], "@nx/nx-darwin-arm64");
    assert_eq!(native_identity["version"], "23.2.1");
    let closure_probe = format!(
        "node -e 'const assert=require(\"node:assert/strict\"); const root=require(\"node:path\").resolve({},\"../../../../\"); assert.equal(root,{}); assert.equal(require(\"node:module\").createRequire(root+\"/package.json\").resolve(\"nx/bin/nx.js\"),{}); require(\"fs\").readFileSync({}); require(\"fs\").readFileSync({}); require({})'",
        serde_json::to_string(&nx.join("dist/bin").to_string_lossy()).unwrap(),
        serde_json::to_string(&developer.to_string_lossy()).unwrap(),
        serde_json::to_string(&nx.join("dist/bin/nx.js").to_string_lossy()).unwrap(),
        serde_json::to_string(&tslib_manifest.to_string_lossy()).unwrap(),
        serde_json::to_string(&native_manifest.to_string_lossy()).unwrap(),
        serde_json::to_string(&nx.join("dist/src/native").to_string_lossy()).unwrap()
    );
    sh(&mut service, "links", &closure_probe).await;
    let (stdout, stderr) = succeed(
        &mut service,
        [
            "exec", "links", "--", "/bin/sh", "-c",
            &format!("bun install --ignore-scripts && {closure_probe} && node_modules/.bin/nx show projects --json"),
        ],
    )
    .await;
    assert!(
        String::from_utf8_lossy(&stdout).contains("\"a\""),
        "real Nx ran: {}",
        String::from_utf8_lossy(&stdout)
    );
    assert!(
        stderr.contains("restored developer link node_modules/nx"),
        "the installer actually displaced the link: {stderr}"
    );
    assert_eq!(topic.join("node_modules/nx").canonicalize().unwrap(), nx);
    let guard = format!(
        "if(require('fs').realpathSync('node_modules/nx') === {}) throw new Error('bypass did not replace the link'); console.log('bypass replaced');",
        serde_json::to_string(&nx.to_string_lossy()).unwrap(),
    ).replace('\'', "'\\''");
    let bypass = format!(
        "{} install --ignore-scripts && node -e '{guard}' && node_modules/.bin/nx show projects --json",
        bun.display(),
    );
    let (exit, stdout, _) = run(
        &mut service,
        ["exec", "links", "--", "/bin/sh", "-c", &bypass],
    )
    .await;
    assert_ne!(
        exit.unwrap_or(-1),
        0,
        "absolute bypass is not chained-command coverage"
    );
    assert!(String::from_utf8_lossy(&stdout).contains("bypass replaced"));
    assert_eq!(
        topic.join("node_modules/nx").canonicalize().unwrap(),
        nx,
        "terminal cleanup restored the absolute bypass"
    );
    // A tool that a covered install ran may not replace the wrapper or the real binary it execs.
    let links_before = (
        fs::read_link(topic.join("node_modules/nx")).unwrap(),
        fs::read_link(topic.join("node_modules/.bin/nx")).unwrap(),
    );
    for script in [
        "rm .cowshed/tools/manager-bin/bun",
        ": > .cowshed/tools/real-bin/bun",
    ] {
        let (exit, _, stderr) = run(
            &mut service,
            ["exec", "links", "--", "/bin/sh", "-c", script],
        )
        .await;
        let stderr = String::from_utf8_lossy(&stderr).into_owned();
        assert_ne!(
            exit.expect("the admitted child returned an exit code"),
            0,
            "{script} was refused: {stderr}"
        );
        assert!(
            stderr.contains("Operation not permitted"),
            "{script} names the kernel refusal: {stderr}"
        );
    }
    assert_eq!(
        (
            fs::read_link(topic.join("node_modules/nx")).unwrap(),
            fs::read_link(topic.join("node_modules/.bin/nx")).unwrap(),
        ),
        links_before,
        "a denied tool write leaves the developer links byte-identical"
    );
    sh(&mut service, "links", "bun --version").await;
    let link_grants = service.grants("links").await.expect("baseline grants").read;
    sh(
        &mut service,
        "links",
        "mkdir -p .cache/relinked-product && printf '%s' '{\"name\":\"relinked-product\",\"version\":\"1.0.0\"}' > .cache/relinked-product/package.json && rm node_modules/nx && ln -s ../.cache/relinked-product node_modules/nx",
    )
    .await;
    assert_eq!(
        fs::read_link(topic.join("node_modules/nx")).unwrap(),
        PathBuf::from("../.cache/relinked-product"),
        "the job's own relink survives terminal cleanup",
    );
    assert_eq!(
        service
            .grants("links")
            .await
            .expect("persisted grants")
            .read,
        link_grants
    );
    let foreign = fixture.storage.home().join(".ssh/foreign");
    fs::create_dir_all(&foreign).unwrap();
    fs::write(foreign.join("secret"), "must stay denied\n").unwrap();
    std::os::unix::fs::symlink(&foreign, topic.join("node_modules/foreign")).unwrap();
    let denied = format!("/bin/cat {}", foreign.join("secret").display());
    let (exit, _, stderr) = run(
        &mut service,
        ["exec", "links", "--", "/bin/sh", "-c", &denied],
    )
    .await;
    let code = exit.expect("the admitted child returned an exit code");
    assert_ne!(
        code,
        0,
        "the foreign read was refused: {}",
        String::from_utf8_lossy(&stderr)
    );
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(
        stderr.contains("cat:"),
        "cat reported the refusal: {stderr}"
    );
    assert!(
        stderr.contains("Operation not permitted"),
        "kernel denied the later foreign target: {stderr}"
    );
    sh(&mut service, "links", "true").await;
    drop(service);
    fixture.stop_gateway().await;
}

/// A covered `bun install` that a real root postinstall displaces the developer links of and then
/// stays alive in, killed through the public job API before it finishes: terminal cleanup must
/// restore the links it removed, because the wrapper recorded the install, and leave no marker.
#[tokio::test(flavor = "multi_thread")]
async fn real_apfs_killed_covered_install_restores_links_its_postinstall_removed() {
    cowshed_core::runtime::shell_host::register(
        cowshed_core::runtime::shell_host::ShellHostProgram::with_arguments(
            cowshed(),
            vec![cowshed_core::runtime::shell_host::SHELL_HOST_ARGUMENT.into()],
        ),
    );
    let (repository, node) = repository_nx();
    let mut fixture = Fixture::with(Project::Build {
        nx: Some((&repository, &node)),
        rust: None,
    });
    let (developer, nx) = developer_nx(&fixture);
    // Real Bun runs the root's postinstall for `bun install` without --ignore-scripts. The script
    // removes the developer links, records its TMPDIR in `ready`, then stays alive.
    fs::create_dir_all(fixture.checkout.join("scripts")).unwrap();
    fs::create_dir_all(fixture.checkout.join("fixture-dependency")).unwrap();
    fs::write(
        fixture.checkout.join("fixture-dependency/package.json"),
        r#"{"name":"fixture-dependency","version":"1.0.0"}"#,
    )
    .unwrap();
    fs::write(
        fixture.checkout.join("scripts/post.js"),
        "const fs = require('fs');\nfs.rmSync('node_modules/nx', { force: true });\nfs.rmSync('node_modules/.bin/nx', { force: true });\nfs.writeFileSync('ready', process.env.TMPDIR);\nsetInterval(() => {}, 1000);\n",
    )
    .unwrap();
    fs::write(
        fixture.checkout.join("package.json"),
        serde_json::to_vec(&serde_json::json!({
            "name": "fixture",
            "private": true,
            "dependencies": { "fixture-dependency": "file:./fixture-dependency" },
            "scripts": { "postinstall": format!("{} scripts/post.js", node.display()) }
        }))
        .unwrap(),
    )
    .unwrap();
    // Bun's own lockfile, written without running any lifecycle script, so `bun.lock` is real and
    // Bun detection (capabilities/bun.rs) activates on it. Resolved as the managed-install test does.
    let bun = host_bun();
    let lockfile = Command::new(&bun)
        .args(["install", "--ignore-scripts", "--lockfile-only"])
        .current_dir(&fixture.checkout)
        .output()
        .expect("generate the actual Bun lockfile");
    assert!(
        lockfile.status.success(),
        "{}",
        String::from_utf8_lossy(&lockfile.stderr)
    );
    assert!(
        fixture.checkout.join("bun.lock").is_file(),
        "Bun wrote bun.lock"
    );
    // This witness displaces a developer choice, not a normal package-manager store alias.
    fs::remove_file(fixture.checkout.join("node_modules/nx")).unwrap();
    fs::remove_file(fixture.checkout.join("node_modules/.bin/nx")).unwrap();
    std::os::unix::fs::symlink(&nx, fixture.checkout.join("node_modules/nx")).unwrap();
    std::os::unix::fs::symlink(
        "../nx/dist/bin/nx.js",
        fixture.checkout.join("node_modules/.bin/nx"),
    )
    .unwrap();
    git(
        &fixture.checkout,
        &[
            "add",
            "package.json",
            "scripts",
            "fixture-dependency",
            "bun.lock",
        ],
    );
    git(
        &fixture.checkout,
        &["commit", "-q", "-m", "fixture postinstall"],
    );
    let reads = nx_dependencies::installed_nx_read_paths(&nx, Some(&developer));
    let mut service = serve_project(&mut fixture, &reads).await;
    succeed(&mut service, ["fork", "links"]).await;
    let topic = service
        .path("links", false)
        .await
        .expect("mounted links workspace")
        .mount;
    assert_eq!(
        fixture_nx(&topic),
        nx,
        "the fork preserves the genuine sibling product"
    );
    let nx_link = fs::read_link(topic.join("node_modules/nx")).unwrap();
    let bin_link = fs::read_link(topic.join("node_modules/.bin/nx")).unwrap();
    service
        .shutdown()
        .await
        .expect("stop the CLI bridge so the job's runtime is the only controller");

    let runtime = cowshed_core::runtime::ProjectRuntime::open_existing_at(
        &fixture.checkout,
        cowshed_core::runtime::RecoveryScope::Workspaces(std::collections::BTreeSet::from([
            WorkspaceName::new("links").expect("workspace name"),
        ])),
        fixture.storage.clone(),
        cowshed_cli::gateway_service::runtime_control,
    )
    .await
    .expect("reopen the project runtime");
    let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let connection = tokio::spawn(cowshed_core::api::server::serve_controller_connection(
        server.into(),
        cowshed_core::api::server::ConnectionAuthority::Coordinator {
            repo_id: runtime.descriptor().repo_id.clone(),
        },
        runtime.router(),
    ));
    let (cowshed, token) = cowshed_core::Cowshed::connect(client.into())
        .await
        .expect("controller handshake");
    let project = cowshed
        .open(&fixture.checkout)
        .await
        .expect("open the project");
    let coordinator = cowshed.coordinator(&project, token).expect("coordinator");
    let worker = coordinator.worker("links").await.expect("links worker");
    let job = worker
        .exec(cowshed_core::api::ExecRequest {
            command: cowshed_core::api::dto::ExecCommand::Argv(vec![
                cowshed_core::api::CommandArg::from("/bin/sh"),
                cowshed_core::api::CommandArg::from("-c"),
                cowshed_core::api::CommandArg::from("bun install"),
            ]),
            cwd: None,
            mode: cowshed_core::api::RunSandboxMode::ReadWrite,
            env: std::collections::HashMap::new(),
            trace: None,
            stdin: cowshed_core::api::StdinSource::Empty,
            stdout_copy: None,
            stderr_copy: None,
            admission_key: None,
            class: cowshed_core::api::ExecClass::Background,
            want: std::num::NonZeroUsize::MIN,
            scope: cowshed_core::api::ExecScope::Workspace,
        })
        .await
        .expect("start bun install as a covered job");

    let ready = topic.join("ready");
    // The wait ends at the sentinel or when the job ends, never at a clock. A job that ends
    // before writing `ready` is reported with its actual state and stderr.
    while !fs::read_to_string(&ready).is_ok_and(|text| !text.is_empty()) {
        let status = job.status().await.expect("the covered job's status");
        if !matches!(status.state, cowshed_core::api::dto::JobState::Running) {
            let mut logs = job
                .logs(cowshed_core::api::JobStream::Stderr, 0, false)
                .await
                .expect("read ended install stderr");
            let mut stderr = Vec::new();
            while let Some(chunk) = logs.next().await {
                stderr.extend_from_slice(&chunk.expect("stderr chunk"));
            }
            panic!(
                "the covered install ended before its postinstall was ready: state {:?}, exit {:?}, stderr: {}",
                status.state,
                status.exit,
                String::from_utf8_lossy(&stderr)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        fs::symlink_metadata(topic.join("node_modules/nx")).is_err(),
        "the real postinstall displaced the developer link"
    );
    let tmp = PathBuf::from(fs::read_to_string(&ready).unwrap());
    job.kill().await.expect("kill the covered install");
    job.wait().await.expect("the killed job's terminal record");

    assert_eq!(
        fs::read_link(topic.join("node_modules/nx")).unwrap(),
        nx_link,
        "terminal cleanup restored the link the observed install removed"
    );
    assert_eq!(
        fs::read_link(topic.join("node_modules/.bin/nx")).unwrap(),
        bin_link,
        "terminal cleanup restored the executable alias"
    );
    let leftovers: Vec<PathBuf> = fs::read_dir(tmp.join("install-observed"))
        .expect("the observation directory")
        .map(|entry| entry.unwrap().path())
        .collect();
    assert!(
        leftovers.is_empty(),
        "terminal cleanup removed the install marker: {leftovers:?}"
    );
    drop(worker);
    drop(coordinator);
    connection.abort();
    runtime.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// The fixture gateway's ledger, read whole.
async fn cpu_budget(socket: &Path) -> cowshed_gateway_types::CpuBudgetStatus {
    cowshed_gateway::GatewayControlClient::new(socket.to_path_buf())
        .expect("an absolute control socket")
        .cpu_budget()
        .await
        .expect("the gateway answers its ledger")
}

/// The checkout [`LoadedHost`] holds the host for.
const LOADING_CHECKOUT: &str = "/loaded-host/another-checkout";

/// Another checkout's background work holding all of background's room in the fixture gateway's
/// real ledger (05_gateway.md, "Room for a gate"): a background job of this project then waits
/// for a returned token, as on a host other work has loaded. Its tokens return when it drops.
struct LoadedHost {
    _lease: tokio::io::BufReader<tokio::net::UnixStream>,
}

impl LoadedHost {
    async fn hold(socket: &Path) -> Self {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};
        let total = cpu_budget(socket).await.total;
        let mut stream = tokio::net::UnixStream::connect(socket)
            .await
            .expect("connect the gateway's control socket");
        let mut request = serde_json::to_string(&cowshed_gateway_types::CpuTokensRequest::new(
            std::num::NonZeroUsize::new(total).expect("a host has a token"),
            LOADING_CHECKOUT,
            "another checkout's background build",
        ))
        .expect("encode the request");
        request.push('\n');
        stream
            .write_all(request.as_bytes())
            .await
            .expect("ask for the whole host");
        let mut lease = tokio::io::BufReader::new(stream);
        loop {
            let mut line = String::new();
            lease
                .read_line(&mut line)
                .await
                .expect("the gateway answers");
            let answer: serde_json::Value =
                serde_json::from_str(&line).expect("a JSON answer line");
            assert_eq!(answer["ok"], true, "{answer}");
            if answer["lease"] == "granted" {
                return Self { _lease: lease };
            }
        }
    }
}

/// The one request the gateway holds waiting for a checkout other than the loading one, once the
/// supervisor's ask for it reached the ledger. The ask leaves the supervisor off its actor path
/// after the exec answered, so the ledger is read until it shows; a request that never arrives is
/// a hang the leg's no-progress backstop decides.
async fn queued_request(socket: &Path) -> cowshed_gateway_types::CpuWaiter {
    loop {
        let ledger = cpu_budget(socket).await;
        let waiting: Vec<_> = ledger
            .background
            .into_iter()
            .filter(|stake| stake.checkout != LOADING_CHECKOUT)
            .flat_map(|stake| stake.waiters)
            .collect();
        if let Ok([waiter]) = <[_; 1]>::try_from(waiting) {
            return waiter;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Every variable a holder hands its CPU grant to the process it starts by (05_gateway.md,
/// "Nested runners use a parent-held sub-ledger", "Cargo reads its holder's jobserver").
const HOLDER_CPU_BUDGET: [&str; 5] = [
    "BOUNDED_EXEC_CPU_POOL",
    "BOUNDED_EXEC_CPU_TOKENS",
    "CARGO_MAKEFLAGS",
    "MAKEFLAGS",
    "MFLAGS",
];

/// Marks the child test process [`holds_no_cpu_pool`] starts.
const NO_CPU_POOL_CHILD: &str = "COWSHED_TEST_NO_CPU_POOL_CHILD";

/// Whether this process is a requester that holds no CPU pool, so `test`'s body runs here. The
/// controller runs in the requester's process, and a job draws on the requester's live pool, never
/// on the host ledger (05_gateway.md, "Work a holder starts draws on its grant"); a test process a
/// gate's runner started holds that runner's pool. A test whose jobs only its fixture gateway's
/// ledger may budget therefore runs `test` again in a child test process whose environment names
/// none of [`HOLDER_CPU_BUDGET`] (08_testing.md: fixtures clear every inherited sizing variable),
/// answers `false` once that child passed, and its body runs only in the child. A child, not
/// `remove_var`, since the tests of one binary may share its process and its environment.
async fn holds_no_cpu_pool(test: &str) -> bool {
    if std::env::var_os(NO_CPU_POOL_CHILD).is_some() {
        return true;
    }
    let mut child = tokio::process::Command::new(std::env::current_exe().expect("test executable"));
    child
        .args(["--exact", test, "--nocapture"])
        .env(NO_CPU_POOL_CHILD, "1");
    for name in HOLDER_CPU_BUDGET {
        child.env_remove(name);
    }
    let status = child
        .status()
        .await
        .expect("run the scenario in a child test process");
    assert!(
        status.success(),
        "{test}, run by a requester that holds no CPU pool: {status}"
    );
    false
}

/// An exec on a loaded host answers at its admission, not at its CPU grant (05_gateway.md, "The
/// task server owns the live grant and nice"; 11_shell.md, "Server-owned CPU admission and nice
/// placement"). Measured before: `cowshed exec --background` returned at the grant, 25 s later on
/// a loaded host, so a waiting job had no id to read or cancel. Here the gateway's real ledger is
/// held whole by another checkout: the CLI's `--background` exec prints its job `queued` while the
/// gateway holds its request, and it runs once the load returns its tokens; a keyed API exec
/// answers with its id while queued, reads queued, and its kill withdraws the request without a
/// command. Both execs ask that ledger only because their requester holds no pool of its own
/// ([`holds_no_cpu_pool`]).
#[tokio::test]
async fn directory_an_exec_on_a_loaded_host_answers_at_admission_and_cancels_while_queued() {
    if !holds_no_cpu_pool(
        "macos::directory_an_exec_on_a_loaded_host_answers_at_admission_and_cancels_while_queued",
    )
    .await
    {
        return;
    }
    let mut fixture = Fixture::directory();
    let mut service = serve_project(&mut fixture, &[]).await;
    let socket = fixture.storage.store().join("gateway.sock");

    let load = LoadedHost::hold(&socket).await;
    let ran = fixture.checkout.join("ran");
    let (stdout, stderr) = succeed(
        &mut service,
        [
            "--json",
            "exec",
            "main",
            "--background",
            "--",
            "/bin/sh",
            "-c",
            "printf ran > ran",
        ],
    )
    .await;
    let answer: serde_json::Value =
        serde_json::from_slice(&stdout).expect("one JSON envelope on stdout");
    let info = &answer["result"];
    assert_eq!(
        (
            info["state"].as_str(),
            info["class"].as_str(),
            &info["pid"],
            &info["cpuTokens"]
        ),
        (
            Some("queued"),
            Some("background"),
            &serde_json::Value::Null,
            &serde_json::Value::Null
        ),
        "the backgrounded exec answered while its job waits for the host: {answer}\n{stderr}"
    );
    let waiting = queued_request(&socket).await;
    assert_eq!(waiting.want, 1, "{waiting:?}");
    assert!(!ran.exists(), "nothing ran while the host is loaded");
    drop(load);
    eventually(|| fs::read(&ran).is_ok_and(|bytes| bytes == b"ran")).await;
    service
        .shutdown()
        .await
        .expect("stop the CLI bridge so the API's runtime is the only controller");

    let runtime = cowshed_core::runtime::ProjectRuntime::open_directory_existing_at(
        &fixture.checkout,
        RecoveryScope::Workspaces([WorkspaceName::main()].into()),
        fixture.storage.clone(),
        cowshed_cli::gateway_service::runtime_control,
    )
    .await
    .expect("reopen the project runtime");
    let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let connection = tokio::spawn(cowshed_core::api::server::serve_controller_connection(
        server.into(),
        cowshed_core::api::server::ConnectionAuthority::Coordinator {
            repo_id: runtime.descriptor().repo_id.clone(),
        },
        runtime.router(),
    ));
    let (cowshed, token) = cowshed_core::Cowshed::connect(client.into())
        .await
        .expect("controller handshake");
    let project = cowshed
        .open(&fixture.checkout)
        .await
        .expect("open the project");
    let coordinator = cowshed.coordinator(&project, token).expect("coordinator");
    let worker = coordinator.worker("main").await.expect("main's worker");

    let load = LoadedHost::hold(&socket).await;
    let never = fixture.checkout.join("never");
    let job = worker
        .exec(cowshed_core::api::ExecRequest {
            command: cowshed_core::api::dto::ExecCommand::Argv(vec![
                cowshed_core::api::CommandArg::from("/bin/sh"),
                cowshed_core::api::CommandArg::from("-c"),
                cowshed_core::api::CommandArg::from("printf ran > never"),
            ]),
            cwd: None,
            mode: cowshed_core::api::RunSandboxMode::ReadWrite,
            env: std::collections::HashMap::new(),
            trace: None,
            stdin: cowshed_core::api::StdinSource::Empty,
            stdout_copy: None,
            stderr_copy: None,
            admission_key: Some(
                cowshed_core::api::dto::AdmissionKey::new("loaded-host-op").expect("a key"),
            ),
            class: cowshed_core::api::ExecClass::Background,
            want: std::num::NonZeroUsize::MIN,
            scope: cowshed_core::api::ExecScope::Workspace,
        })
        .await
        .expect("a keyed exec answers at admission while the host is loaded");
    let queued = job.status().await.expect("the queued job's status");
    assert_eq!(
        (
            queued.state,
            queued.class,
            queued.pid,
            queued.cpu_tokens,
            queued.nice
        ),
        (
            cowshed_core::api::dto::JobState::Queued,
            cowshed_core::api::ExecClass::Background,
            None,
            None,
            None
        ),
        "the answer's job is queued: its class is set and it holds no grant"
    );
    queued_request(&socket).await;
    job.kill().await.expect("kill the queued job");
    let ended = job
        .wait()
        .await
        .expect("the cancelled job's terminal record");
    assert_eq!(
        (ended.state, ended.pid, ended.exit, ended.cpu_tokens),
        (cowshed_core::api::dto::JobState::Killed, None, None, None),
        "killed while queued: it never ran"
    );
    let ledger = cpu_budget(&socket).await;
    assert!(
        ledger
            .background
            .iter()
            .filter(|stake| stake.checkout != LOADING_CHECKOUT)
            .all(|stake| stake.waiters.is_empty() && stake.held == 0),
        "the kill withdrew the request from the gateway's queue: {ledger:?}"
    );
    drop(load);
    assert!(!never.exists(), "the cancelled job's command never ran");

    drop(job);
    drop(worker);
    drop(coordinator);
    connection.abort();
    runtime.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

#[path = "workspace_isolation.rs"]
mod workspace_isolation;
