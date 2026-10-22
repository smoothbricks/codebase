use std::fs;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use arrow_ipc::reader::StreamReader;
use cowshed_core::api::ControllerCommitment;
use cowshed_core::api::dto::{AdoptOptions, ForkOptions, RemoveOptions, RevisionTarget};
use cowshed_core::api::operations::OperationRequest;
use cowshed_core::api::server::ConnectionAuthority;
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, PortBlock, PublicationState, WorkspaceIncarnation, WorkspaceName,
};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope, supervisor_socket};
use cowshed_core::storage::StorageLayout;
use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
use cowshed_core::storage::job_artifact::{
    controller_commitment_schema, decode_controller_commitments,
};
use cowshed_core::storage::recovery::{
    LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentCompletion, LifecycleIntentJournal,
    LifecycleIntentPhase, LifecycleIntentRecord,
};
use cowshed_core::{CowshedError, Result};
use cowshed_os::fork_lock::{Run as _, Spawn as _};
use serde_json::{Value, json};

#[path = "../support/refusing_gateway.rs"]
mod refusing_gateway;

#[path = "../support/scratch_directory.rs"]
mod scratch_directory;

use scratch_directory::ScratchRoot;

const REFUSAL: &str = "no macOS workspace port block remains";

/// `rm` answers only after physical source release. Replaying the exact mutating intent
/// after that commit must acknowledge absence without constructing retirement trash.
#[tokio::test]
async fn clean_landed_source_is_physically_deleted_before_reply_and_crash_replay() {
    let fixture = Fixture::new("clean-source-release");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "landed", ForkOptions::default())
        .await
        .expect("fork");
    let image = fixture
        .layout()
        .session_image(&name("landed"))
        .unwrap()
        .image()
        .to_owned();
    fixture
        .remove(&runtime, "landed")
        .await
        .expect("release source");
    assert!(
        !image.exists(),
        "source image is physically absent before shutdown"
    );
    assert!(!cowshed_core::metadata::sidecar_path(&image).exists());
    let trash = fixture.layout().project().sessions.join(".trash");
    assert!(
        !trash.exists(),
        "clean release never prepares a trash rename"
    );
    runtime.shutdown().await.expect("stop runtime");
    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Retire {
            workspace: name("landed"),
            options: RemoveOptions {
                force: true,
                ..RemoveOptions::default()
            },
            origin: None,
        });
        journal.mark_mutating(&name("landed"))
    })
    .expect("simulate death after unlink before acknowledgement");
    let restarted = fixture
        .open_removing("landed")
        .await
        .expect("recover removal");
    fixture
        .remove(&restarted, "landed")
        .await
        .expect("idempotent reply");
    assert!(matches!(
        fixture.intent("landed").unwrap().completion,
        Some(LifecycleIntentCompletion::Retire(_))
    ));
    assert!(!image.exists());
    assert!(!trash.exists());
    restarted.shutdown().await.expect("stop restarted runtime");
}

/// Neither a hidden name nor gitignore proves rebuildability. Only-copy source bytes keep
/// their complete recovery image across shutdown, startup, plain gc and bundle-only gc.
#[tokio::test]
async fn ignored_only_copy_source_content_keeps_recovery_trash() {
    let fixture = Fixture::new("only-copy-source");
    let runtime = fixture.adopt().await;
    let minted = fixture
        .mint(&runtime, "notes", ForkOptions::default())
        .await
        .expect("fork");
    let mount = PathBuf::from(minted["info"]["mount"].as_str().unwrap());
    let image = fixture
        .layout()
        .session_image(&name("notes"))
        .unwrap()
        .image()
        .to_owned();
    let metadata = DetachedWorkspaceMetadata::read_for_image(&image).unwrap();
    fs::write(mount.join(".private-notes"), b"only hidden copy\n").unwrap();
    fs::write(mount.join("ignored-notes"), b"only ignored copy\n").unwrap();
    let exclude = mount.join(".git/info/exclude");
    use std::io::Write as _;
    fs::OpenOptions::new()
        .append(true)
        .open(exclude)
        .unwrap()
        .write_all(b"\n/ignored-notes\n")
        .unwrap();
    fixture
        .remove(&runtime, "notes")
        .await
        .expect("retire to recovery");
    let recovery = fixture.layout().project().sessions.join(format!(
        ".trash/notes-{}.dmg",
        metadata.workspace_incarnation,
    ));
    assert!(!image.exists());
    assert_eq!(
        fs::read(recovery.join(".private-notes")).unwrap(),
        b"only hidden copy\n"
    );
    assert_eq!(
        fs::read(recovery.join("ignored-notes")).unwrap(),
        b"only ignored copy\n"
    );
    runtime.shutdown().await.expect("stop runtime");
    let restarted = fixture.gc().await;
    for abandoned in [false, true] {
        fixture
            .call(
                &restarted,
                "coordinator.gc",
                json!({
                    "repoId": fixture.repo,
                    "options": { "dryRun": false, "abandoned": abandoned },
                }),
            )
            .await
            .expect("collection preserves source recovery");
        assert_eq!(
            fs::read(recovery.join(".private-notes")).unwrap(),
            b"only hidden copy\n"
        );
        assert_eq!(
            fs::read(recovery.join("ignored-notes")).unwrap(),
            b"only ignored copy\n"
        );
        assert!(cowshed_core::metadata::sidecar_path(&recovery).exists());
    }
    restarted.shutdown().await.expect("stop restarted runtime");
    fs::remove_file(cowshed_core::metadata::sidecar_path(&recovery)).unwrap();
    let missing_record = fixture.gc().await;
    fixture
        .call(
            &missing_record,
            "coordinator.gc",
            json!({
                "repoId": fixture.repo,
                "options": { "dryRun": false, "abandoned": true },
            }),
        )
        .await
        .expect("missing grants do not authorize byte deletion");
    assert_eq!(
        fs::read(recovery.join(".private-notes")).unwrap(),
        b"only hidden copy\n"
    );
    assert_eq!(
        fs::read(recovery.join("ignored-notes")).unwrap(),
        b"only ignored copy\n"
    );
    missing_record
        .shutdown()
        .await
        .expect("stop missing-record runtime");
}

/// One adopted project on its own real directory store.
struct Fixture {
    /// Owned so its drop ends this test's workers and removes its tree.
    _scratch: ScratchRoot,
    checkout: PathBuf,
    storage: ValidatedHostStorage,
    repo: RepoId,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let scratch = ScratchRoot::new(label).expect("scratch directory root");
        let checkout = scratch.path().join("checkout");
        let store = scratch.path().join("store");
        for path in [&checkout, &store] {
            fs::create_dir_all(path).expect("fixture directory");
        }
        git(&checkout, &["init", "-q", "-b", "main"]);
        fs::write(checkout.join("tracked"), b"tracked\n").expect("tracked file");
        fs::write(
            checkout.join(".gitignore"),
            b".envrc\n.envrc-local\n.devenv/\n",
        )
        .expect("workspace hooks ignore");
        git(&checkout, &["add", "tracked", ".gitignore"]);
        git(&checkout, &["commit", "-q", "-m", "initial"]);
        let storage = ValidatedHostStorage::new(
            scratch.path().to_path_buf(),
            Vec::new(),
            CanonicalRoots::at(store),
        );
        Self {
            _scratch: scratch,
            checkout,
            storage,
            repo: RepoId::parse("fixture/intents").expect("repository identity"),
        }
    }

    async fn adopt(&self) -> ProjectRuntime {
        let runtime = ProjectRuntime::open_directory_for_adopt_at(
            &self.checkout,
            Some(self.repo.clone()),
            self.storage.clone(),
            refusing_gateway::connect,
        )
        .await
        .expect("open project runtime on the directory store");
        self.call(
            &runtime,
            "coordinator.adopt",
            json!({
                "repoId": self.repo,
                "options": AdoptOptions {
                    path: Some(self.checkout.clone()),
                    repo_id: Some(self.repo.clone()),
                    capacity: Some("1g".to_owned()),
                    quarantine: false,
                    re_adopt: false,
                },
            }),
        )
        .await
        .expect("directory-backed adoption");
        runtime
    }

    /// What `cowshed gc` opens: store-wide recovery, which finishes every unfinished intent as
    /// residue.
    async fn gc(&self) -> ProjectRuntime {
        ProjectRuntime::open_directory_existing_at(
            &self.checkout,
            RecoveryScope::Store,
            self.storage.clone(),
            refusing_gateway::connect,
        )
        .await
        .expect("store-scope recovery opens the project")
    }

    /// What `cowshed fork <workspace>` and other verbs naming it open: their own intents and
    /// main's are replayed, and a failed replay fails the opening.
    async fn open_naming(&self, workspace: &str) -> Result<ProjectRuntime> {
        ProjectRuntime::open_directory_existing_at(
            &self.checkout,
            RecoveryScope::Workspaces([name(workspace)].into()),
            self.storage.clone(),
            refusing_gateway::connect,
        )
        .await
    }

    /// What `cowshed __workspace-supervisor <checkout> <workspace>` opens before it serves
    /// `workspace`.
    async fn open_as_supervisor(&self, workspace: &str) -> Result<ProjectRuntime> {
        ProjectRuntime::open_directory_existing_at(
            &self.checkout,
            RecoveryScope::Supervisor(name(workspace)),
            self.storage.clone(),
            refusing_gateway::connect,
        )
        .await
    }

    /// What `cowshed rm <workspace>` opens.
    async fn open_removing(&self, workspace: &str) -> Result<ProjectRuntime> {
        ProjectRuntime::open_directory_existing_at(
            &self.checkout,
            RecoveryScope::Removal(name(workspace)),
            self.storage.clone(),
            refusing_gateway::connect,
        )
        .await
    }

    /// Remove `workspace`, forced past the age floor alone: every workspace these tests remove
    /// is seconds old, younger than an explicit `rm` removes unforced (02_workspaces.md, `rm`
    /// step 1), and nothing in one is dirty or running for the force to override.
    async fn remove(&self, runtime: &ProjectRuntime, workspace: &str) -> Result<Value> {
        self.call(
            runtime,
            "coordinator.destroy",
            json!({
                "repoId": self.repo,
                "workspace": workspace,
                "options": RemoveOptions { force: true, ..RemoveOptions::default() },
            }),
        )
        .await
    }

    async fn call(&self, runtime: &ProjectRuntime, method: &str, params: Value) -> Result<Value> {
        let response = runtime
            .router()
            .route(
                ConnectionAuthority::Coordinator {
                    repo_id: self.repo.clone(),
                },
                OperationRequest::decode(method, &params)?,
                None,
                None,
            )
            .await?;
        Ok(response.into_answer()?.0)
    }

    async fn mint(
        &self,
        runtime: &ProjectRuntime,
        workspace: &str,
        options: ForkOptions,
    ) -> Result<Value> {
        self.call(
            runtime,
            "coordinator.fork",
            json!({ "repoId": self.repo, "workspace": workspace, "options": options }),
        )
        .await
    }

    async fn fork(
        &self,
        runtime: &ProjectRuntime,
        source: &str,
        destination: &str,
    ) -> Result<Value> {
        self.call(
            runtime,
            "coordinator.fork",
            json!({ "repoId": self.repo, "source": source, "workspace": destination, "options": ForkOptions::default() }),
        )
        .await
    }

    async fn listed(&self, runtime: &ProjectRuntime) -> Vec<String> {
        let listed = self
            .call(runtime, "project.list", json!({ "repoId": self.repo }))
            .await
            .expect("list workspaces");
        let mut names = listed
            .as_array()
            .expect("workspace array")
            .iter()
            .map(|workspace| {
                workspace["info"]["workspace"]
                    .as_str()
                    .expect("workspace name")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn layout(&self) -> StorageLayout {
        StorageLayout::new(self.storage.store(), &self.repo).expect("project layout")
    }

    fn journal_path(&self) -> PathBuf {
        self.layout()
            .project()
            .project_root
            .join(LIFECYCLE_INTENTS_FILE)
    }

    fn intent(&self, workspace: &str) -> Option<LifecycleIntentRecord> {
        LifecycleIntentJournal::load(&self.journal_path())
            .expect("lifecycle intent journal")
            .get(&name(workspace))
            .cloned()
    }

    /// Rewrites one record as a binary from before the `Mutating` mark wrote it: with no phase,
    /// which reads as `Prepared`.
    fn strip_phase(&self, workspace: &str) {
        let path = self.journal_path();
        let mut journal: Value =
            serde_json::from_slice(&fs::read(&path).expect("journal bytes")).expect("journal JSON");
        journal["entries"][workspace]
            .as_object_mut()
            .expect("journal record")
            .remove("phase")
            .expect("the record was marked mutating");
        fs::write(&path, serde_json::to_vec(&journal).expect("journal JSON"))
            .expect("rewrite journal");
    }

    /// Claims every macOS port block for this live process, exactly as a concurrent creator's
    /// reservation does, so the next allocation refuses for want of a block.
    fn claim_every_port_block(&self) -> PortClaims {
        let staging = self.storage.store().join(".staging");
        fs::create_dir_all(&staging).expect("reservation directory");
        let owner = std::process::id().to_string();
        PortClaims(
            PortBlock::macos_candidates(self.storage.port_allocation())
                .map(|block| {
                    let marker = staging.join(format!("port-{}.reservation", block.base()));
                    std::os::unix::fs::symlink(&owner, &marker).expect("port reservation marker");
                    marker
                })
                .collect(),
        )
    }

    fn slot_tenant(&self, slot: u32) -> Option<WorkspaceName> {
        self.layout()
            .slot_bindings()
            .expect("slot bindings")
            .tenant(cowshed_core::metadata::SlotId::new(slot).expect("slot"))
            .cloned()
    }

    /// A stale `cowshed/<workspace>` branch in main. A git-worktree mint's initializer refuses
    /// it after the `PendingFence` image exists, and keeps the clone pending: its initializer
    /// registers in main's repository, so the clone stays for the retry to resume.
    fn plant_workspace_branch(&self, workspace: &str) {
        git(&self.checkout, &["branch", &format!("cowshed/{workspace}")]);
    }

    /// A committed link in main leaving the tree for a path that does not exist: a clone's
    /// initializer refuses it after the `PendingFence` image exists, and so does every resume,
    /// which judges the clone's own copy of the link.
    fn commit_escaping_link(&self) {
        std::os::unix::fs::symlink("../nowhere", self.checkout.join("escaping"))
            .expect("escaping link");
        git(&self.checkout, &["add", "escaping"]);
        git(&self.checkout, &["commit", "-q", "-m", "escaping link"]);
    }

    fn remove_escaping_link(&self) {
        git(&self.checkout, &["rm", "-q", "escaping"]);
        git(
            &self.checkout,
            &["commit", "-q", "-m", "drop escaping link"],
        );
    }

    /// Journals `workspace`'s unfinished create as a binary without the start-revision check
    /// left it: past its mutation fence, starting from a revision main does not hold, so every
    /// replay of it fails again.
    fn journal_unreplayable_create(&self, workspace: &str) {
        LifecycleIntentJournal::update(&self.journal_path(), |journal| {
            journal.begin(LifecycleIntent::Create {
                workspace: name(workspace),
                options: cowshed_core::storage::recovery::MintOptions {
                    revision: missing_revision().revision,
                    ..Default::default()
                },
            });
            journal.mark_mutating(&name(workspace))
        })
        .expect("journal the unreplayable create");
    }

    /// Every build volume image the project holds, by file name.
    fn build_images(&self) -> Vec<std::ffi::OsString> {
        let directory = self.layout().project().project_root.join("build");
        let mut images = match fs::read_dir(&directory) {
            Ok(entries) => entries
                .map(|entry| entry.expect("build image entry").file_name())
                .collect::<Vec<_>>(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("list {}: {error}", directory.display()),
        };
        images.sort();
        images
    }

    /// Stands at `workspace`'s supervisor socket until a lifecycle verb asks who serves it, which
    /// the verb does only once the workspace's image is published. At that moment a gateway
    /// reconcile in another process may install the workspace's session on the published block,
    /// so the block must hold no kernel claim of the verb's: it answers the block's ports any
    /// socket of this process -- the runtime's -- is still bound to. Then it lets go of the
    /// socket, so the verb serves its own supervisor there.
    ///
    /// It never binds the block to find out: the host's port space is shared with every other
    /// store (each concurrent test has its own) and program, whose allocator takes the lowest
    /// free block the moment this one is released, so a refused bind says nothing of the verb.
    fn gateway_at_hello(&self, workspace: &str) -> GatewayAtHello {
        let workspace = name(workspace);
        let socket = supervisor_socket::socket_path(self.storage.store(), &self.repo, &workspace);
        fs::create_dir_all(socket.parent().expect("socket directory")).expect("run directory");
        let listener = tokio::net::UnixListener::bind(&socket).expect("stand at the socket");
        let image = self
            .layout()
            .canonical_image(&workspace)
            .expect("image path")
            .image()
            .to_path_buf();
        GatewayAtHello(tokio::spawn(async move {
            let (hello, _) = listener.accept().await.expect("the verb's hello");
            let published = DetachedWorkspaceMetadata::read_for_image(&image).expect("sidecar");
            assert_eq!(published.publication_state, PublicationState::Active);
            let block = published.grants.port_block.expect("a port block");
            let held = own_tcp_ports()
                .into_iter()
                .filter(|port| block.ports().expect("block ports").contains(port))
                .collect();
            drop(listener);
            drop(hello);
            held
        }))
    }
}

/// Every local TCP port a socket of this process is bound to, from the descriptors it has open.
fn own_tcp_ports() -> Vec<u16> {
    let mut ports = Vec::new();
    for entry in fs::read_dir("/dev/fd").expect("list this process's descriptors") {
        let entry = entry.expect("descriptor entry");
        let fd: libc::c_int = entry
            .file_name()
            .to_str()
            .and_then(|fd| fd.parse().ok())
            .expect("a descriptor number");
        // SAFETY: an all-zero `sockaddr_storage` is a valid value of the plain C struct.
        let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_storage>() };
        let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::sockaddr_storage>())
            .expect("sockaddr_storage size");
        // SAFETY: `address` is writable storage of `length` bytes.
        let named = unsafe {
            libc::getsockname(fd, (&raw mut address).cast::<libc::sockaddr>(), &mut length)
        };
        if named != 0 {
            let error = std::io::Error::last_os_error();
            match error.raw_os_error() {
                // Not a socket; or the listing's own descriptor, closed since it was listed.
                Some(libc::ENOTSOCK | libc::EBADF) => continue,
                _ => panic!("getsockname({fd}): {error}"),
            }
        }
        let port = match libc::c_int::from(address.ss_family) {
            libc::AF_INET => {
                // SAFETY: the kernel wrote a `sockaddr_in` for an AF_INET socket.
                let address = unsafe { *(&raw const address).cast::<libc::sockaddr_in>() };
                address.sin_port
            }
            libc::AF_INET6 => {
                // SAFETY: the kernel wrote a `sockaddr_in6` for an AF_INET6 socket.
                let address = unsafe { *(&raw const address).cast::<libc::sockaddr_in6>() };
                address.sin6_port
            }
            _ => continue,
        };
        ports.push(u16::from_be(port));
    }
    ports
}

/// Reservation markers this test holds on behalf of an imaginary concurrent creator.
struct PortClaims(Vec<PathBuf>);

impl PortClaims {
    fn release(self) {
        for marker in &self.0 {
            fs::remove_file(marker).expect("release port reservation marker");
        }
    }
}

/// The ports of the published block this process still held at the verb's hello.
struct GatewayAtHello(tokio::task::JoinHandle<Vec<u16>>);

impl GatewayAtHello {
    async fn claimed(self) -> Vec<u16> {
        self.0.await.expect("the gateway stand-in")
    }
}

fn name(value: &str) -> WorkspaceName {
    WorkspaceName::new(value).expect("workspace name")
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
        .output_locked()
        .expect("git process");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn refused(error: CowshedError) {
    assert!(
        error.message.contains(REFUSAL),
        "expected the port refusal, got {}: {}",
        error.code.as_str(),
        error.message
    );
}

fn completed(record: Option<LifecycleIntentRecord>) -> bool {
    matches!(
        record.and_then(|record| record.completion),
        Some(LifecycleIntentCompletion::Workspace(_))
    )
}

/// A short commit id main does not hold, spelled as `cowshed fork --ref` passes it.
const MISSING_REVISION: &str = "2f9cfc74";

fn missing_revision() -> ForkOptions {
    ForkOptions {
        revision: Some(RevisionTarget::parse_cli(MISSING_REVISION).expect("revision spelling")),
        clean: true,
        ..ForkOptions::default()
    }
}

/// A git-worktree mint: the shape whose refused clone stays pending, because its initializer
/// reaches main's repository. A standalone mint's refused clone is reclaimed instead.
fn worktree() -> ForkOptions {
    ForkOptions {
        git_worktree: true,
        ..ForkOptions::default()
    }
}
#[tokio::test]
async fn cold_named_open_reads_selected_authority_and_preserves_full_diagnostics() {
    use cowshed_core::metadata::sidecar_path;
    let fixture = Fixture::new("cold-named-scope");
    let runtime = fixture.adopt().await;
    for workspace in ["selected", "broken"] {
        fixture
            .mint(&runtime, workspace, ForkOptions::default())
            .await
            .expect("publish fixture workspace");
    }
    runtime.shutdown().await.expect("stop initial controller");
    let layout = fixture.layout();
    let broken = layout
        .canonical_image(&name("broken"))
        .expect("broken image")
        .image()
        .to_owned();
    let broken_sidecar = sidecar_path(&broken);
    let original = fs::read(&broken_sidecar).expect("preserve broken fixture metadata");
    let selected = layout
        .canonical_image(&name("selected"))
        .expect("selected image")
        .image()
        .to_owned();
    let selected_sidecar = sidecar_path(&selected);
    let selected_original =
        fs::read(&selected_sidecar).expect("preserve selected fixture metadata");

    for corrupt in [true, false] {
        if corrupt {
            fs::write(&broken_sidecar, b"{ broken sibling").unwrap();
        } else {
            fs::remove_file(&broken_sidecar).unwrap();
        }
        let named = fixture
            .open_naming("selected")
            .await
            .expect("cold named open ignores a broken neighbour");
        let view = fixture
            .call(
                &named,
                "project.workspace",
                json!({"repoId": fixture.repo, "workspace": "selected"}),
            )
            .await
            .expect("selected authoritative snapshot");
        assert_eq!(view["info"]["workspace"], "selected");
        if corrupt {
            assert!(
                fixture
                    .call(&named, "project.list", json!({"repoId": fixture.repo}))
                    .await
                    .is_err(),
                "full inventory still reports malformed sibling authority"
            );
        }
        named.shutdown().await.expect("stop selected controller");
        let removing = fixture
            .open_removing("selected")
            .await
            .expect("cold removal open has the same selected read scope");
        removing.shutdown().await.expect("stop removal controller");

        // A session-origin invocation is host-validated even when the command has not named a
        // job. That one extra observation must not make the bad sibling a recovery dependency.
        let mount = layout
            .workspace_mount(&name("selected"))
            .expect("selected mount");
        let origin = ProjectRuntime::open_directory_existing_at(
            &mount,
            RecoveryScope::Workspaces(Default::default()),
            fixture.storage.clone(),
            refusing_gateway::connect,
        )
        .await
        .expect("valid selected invocation origin is included in cold observations");
        origin.shutdown().await.expect("stop origin controller");

        if corrupt {
            for scope in [RecoveryScope::Store, RecoveryScope::Inspect] {
                match ProjectRuntime::open_directory_existing_at(
                    &fixture.checkout,
                    scope,
                    fixture.storage.clone(),
                    refusing_gateway::connect,
                )
                .await
                {
                    Err(_) => {}
                    Ok(unexpected) => {
                        unexpected
                            .shutdown()
                            .await
                            .expect("stop unexpected full open");
                        panic!("full Store/Inspect must retain malformed sibling diagnostics");
                    }
                }
            }
        }
        fs::write(&broken_sidecar, &original).expect("restore sibling fixture metadata");
    }
    fs::write(&selected_sidecar, b"{ broken selected").unwrap();
    match fixture.open_naming("selected").await {
        Err(_) => {}
        Ok(unexpected) => {
            unexpected
                .shutdown()
                .await
                .expect("stop unexpected selected open");
            panic!("selected metadata corruption must still refuse");
        }
    }
    fs::write(&selected_sidecar, selected_original).expect("restore selected fixture metadata");
}

/// Names the image lock this test binary holds when it runs as [`image_lock_holder`].
const HELD_LOCK_ENV: &str = "COWSHED_TEST_HELD_IMAGE_LOCK";
/// Names the store root that lock is in.
const HELD_STORE_ENV: &str = "COWSHED_TEST_HELD_IMAGE_STORE";

/// Another process holding one image lock as a lifecycle operation of its own holds it (a fork
/// holds its source's for the whole of its clone): this test binary run as
/// [`image_lock_holder`]. It holds the lock until [`Self::release`] closes its stdin.
struct LockHolder {
    child: std::process::Child,
    said: BufReader<std::process::ChildStderr>,
}

impl LockHolder {
    /// Start the holder beneath `scratch`, so the fixture's cleanup ends it, and return once it
    /// says on stderr that it holds `lock`. Its stdout is libtest's, which frames the test's own
    /// output: run on one thread, as nextest runs it, `held` would follow `test … ... ` on one line.
    fn hold(scratch: &Path, store: &Path, lock: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("this test binary"))
            .args(["--exact", "macos::image_lock_holder", "--nocapture"])
            .env(HELD_LOCK_ENV, lock)
            .env(HELD_STORE_ENV, store)
            .current_dir(scratch)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn_locked()
            .expect("start the lock holder");
        let mut said = BufReader::new(child.stderr.take().expect("the holder's stderr"));
        let mut transcript = String::new();
        loop {
            let start = transcript.len();
            let read = said
                .read_line(&mut transcript)
                .expect("the holder's stderr");
            assert_ne!(
                read,
                0,
                "the lock holder ended before it held {}; it said: {transcript}",
                lock.display()
            );
            if transcript[start..].trim_end() == "held" {
                break;
            }
        }
        Self { child, said }
    }

    /// Close the holder's stdin so it lets go of the lock, and require that it held the lock
    /// until then and exited cleanly.
    fn release(mut self) {
        drop(self.child.stdin.take());
        std::io::copy(&mut self.said, &mut std::io::sink()).expect("the holder's last output");
        let status = self.child.wait().expect("wait for the lock holder");
        assert!(status.success(), "the lock holder failed: {status}");
    }
}

/// Not a test of its own: the other process of [`LockHolder`]. Run as this binary with
/// [`HELD_LOCK_ENV`] set, it takes that image lock as a lifecycle operation takes it, says
/// `held` on stderr, and holds it until its stdin closes. Run any other way it does nothing.
#[test]
fn image_lock_holder() {
    let Some(lock) = std::env::var_os(HELD_LOCK_ENV) else {
        return;
    };
    let store = std::env::var_os(HELD_STORE_ENV).expect("the held lock's store");
    let _held = cowshed_apfs::store_lock::acquire_image_locks(
        Path::new(&store),
        &[PathBuf::from(lock)],
        cowshed_apfs::store_lock::LockMode::Wait,
    )
    .expect("take the image lock")
    .expect("a waited-for lock is taken");
    // On stderr, which libtest neither captures nor frames (see [`LockHolder::hold`]).
    std::io::stderr()
        .write_all(b"held\n")
        .expect("say the lock is held");
    std::io::copy(&mut std::io::stdin(), &mut std::io::sink()).expect("wait for the release");
}

/// Blocks until `path` exists, woken by the writes to its directory rather than by a clock.
fn wait_for_entry(path: &Path) {
    let directory = fs::File::open(path.parent().expect("the entry's directory"))
        .expect("open the entry's directory");
    // SAFETY: `kqueue` takes no arguments; its answer is checked before use.
    let queue = unsafe { libc::kqueue() };
    assert!(queue >= 0, "kqueue: {}", std::io::Error::last_os_error());
    // SAFETY: `queue` is a descriptor this function was just handed and alone owns.
    let queue = unsafe { OwnedFd::from_raw_fd(queue) };
    let watch = libc::kevent {
        ident: usize::try_from(directory.as_raw_fd()).expect("a descriptor"),
        filter: libc::EVFILT_VNODE,
        flags: libc::EV_ADD | libc::EV_CLEAR,
        fflags: libc::NOTE_WRITE,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    // SAFETY: one valid change in, no event out, no timeout.
    let registered = unsafe {
        libc::kevent(
            queue.as_raw_fd(),
            &raw const watch,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    };
    assert_eq!(
        registered,
        0,
        "watch {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
    while !path.exists() {
        let mut event = std::mem::MaybeUninit::<libc::kevent>::uninit();
        // SAFETY: no change in, room for one event out, no timeout.
        let woken = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                std::ptr::null(),
                0,
                event.as_mut_ptr(),
                1,
                std::ptr::null(),
            )
        };
        if woken < 0 {
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::Interrupted,
                "wait for {}: {error}",
                path.display()
            );
        }
    }
}

/// A workspace supervisor opens and serves while other processes hold main's image lock and its
/// own workspace's, as forks cloning them hold each for minutes on a large build image. Its
/// opening used to wait for those locks, and its manager killed it at the start bound as a
/// supervisor that "did not serve" (measured 2026-10-22: a new supervisor spent every sample of
/// over two minutes in `flock` on `main.dmg.lock`, under `acquire_image_locks` in the project
/// opening). Its opening finishes nothing and runs no store-wide pass, which would refuse the
/// corrupt sibling below; it leaves an unrecorded earlier generation of its workspace in the trash,
/// and so does an ordinary opening once the holders let go: a recovery image without its record
/// is retained, never startup garbage (02_workspaces.md, `rm` step 5). Its mount, already settled,
/// is served as it stands under the held lock.
#[tokio::test]
async fn a_supervisor_serves_while_other_processes_hold_its_image_locks() {
    use cowshed_core::metadata::sidecar_path;
    let fixture = Fixture::new("sup-locks");
    let runtime = fixture.adopt().await;
    for workspace in ["served", "broken"] {
        fixture
            .mint(&runtime, workspace, ForkOptions::default())
            .await
            .expect("mint a fixture workspace");
    }
    runtime
        .shutdown()
        .await
        .expect("stop the controller that minted them");
    let layout = fixture.layout();
    let broken = layout
        .canonical_image(&name("broken"))
        .expect("the sibling's image")
        .image()
        .to_owned();
    fs::write(sidecar_path(&broken), b"{ broken sibling").expect("corrupt the sibling's sidecar");
    // An earlier generation of `served` whose retirement record is already gone: a recovery image
    // no opening deletes, since missing authority never proves its bytes disposable.
    let residue = layout
        .project()
        .sessions
        .join(".trash")
        .join("served-0123456789abcdef0123456789abcdef.dmg");
    fs::create_dir_all(&residue).expect("plant an earlier generation of served in the trash");
    fs::write(residue.join("left"), b"an earlier generation").expect("its bytes");
    let scratch = fixture.checkout.parent().expect("the scratch root");
    let served = name("served");
    let holders = [WorkspaceName::main(), served.clone()].map(|workspace| {
        let lock = layout
            .canonical_image(&workspace)
            .expect("a selected image")
            .lock()
            .to_owned();
        LockHolder::hold(scratch, fixture.storage.store(), &lock)
    });

    let supervisor = fixture
        .open_as_supervisor("served")
        .await
        .expect("the supervisor's opening waits on no lock another operation holds");
    let socket = supervisor_socket::socket_path(fixture.storage.store(), &fixture.repo, &served);
    fs::create_dir_all(socket.parent().expect("the socket directory")).expect("run directory");
    let published = tokio::task::spawn_blocking({
        let socket = socket.clone();
        move || wait_for_entry(&socket)
    });
    {
        let serving = supervisor.serve_supervisor(&served);
        tokio::pin!(serving);
        tokio::select! {
            ended = &mut serving => panic!("the supervisor ended before it served: {ended:?}"),
            published = published => published.expect("watch the socket directory"),
        }
        let hello = supervisor_socket::hello(&socket)
            .await
            .expect("the supervisor answers");
        assert_eq!(hello.pid, std::process::id());
        assert_eq!(hello.authority.workspace, served);
        supervisor_socket::drain(&socket)
            .await
            .expect("retire the supervisor");
        serving.await.expect("the supervisor retires cleanly");
    }
    supervisor
        .shutdown()
        .await
        .expect("stop the supervisor's runtime");
    assert_eq!(
        fs::read(residue.join("left")).expect("the residue the supervisor left"),
        b"an earlier generation"
    );
    for holder in holders {
        holder.release();
    }
    let ordinary = fixture
        .open_naming("served")
        .await
        .expect("a verb naming served opens once the holders let go");
    assert_eq!(
        fs::read(residue.join("left")).expect("the recovery image the ordinary opening retains"),
        b"an earlier generation"
    );
    ordinary
        .shutdown()
        .await
        .expect("stop the ordinary opening");
}

#[tokio::test]
async fn a_create_interrupted_after_its_pending_clone_exists_still_resumes() {
    // One adopted project proves both journal spellings (08_testing.md, "Host limit"): the
    // current binary's `Mutating` create, and an older binary's `Prepared` one — binaries
    // before the `Mutating` mark journaled every create `Prepared`, and its pending clone,
    // not its phase, is what recovery decides on.
    let fixture = Fixture::new("mutating-create");
    let runtime = fixture.adopt().await;
    for workspace in ["resumed", "legacy"] {
        fixture.plant_workspace_branch(workspace);
        fixture
            .mint(&runtime, workspace, worktree())
            .await
            .expect_err("the clone's initializer refuses main's stale workspace branch");
        // The cause is main's, not the clone's: once it is gone, the resume can finish.
        git(
            &fixture.checkout,
            &["branch", "-D", &format!("cowshed/{workspace}")],
        );
    }
    let record = fixture
        .intent("resumed")
        .expect("the intent stays journaled");
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);
    assert_eq!(record.completion, None);
    fixture.strip_phase("legacy");
    assert_eq!(
        fixture.intent("legacy").expect("legacy intent").phase,
        LifecycleIntentPhase::Prepared
    );
    assert_eq!(fixture.listed(&runtime).await, ["main"]);
    runtime
        .shutdown()
        .await
        .expect("stop the interrupted runtime");

    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["legacy", "main", "resumed"]);
    assert!(completed(fixture.intent("resumed")));
    assert!(completed(fixture.intent("legacy")));
    gc.shutdown().await.expect("stop gc");
}

#[tokio::test]
async fn a_slot_bound_create_that_fails_after_binding_is_kept_and_resumed() {
    let fixture = Fixture::new("slot-create");
    let runtime = fixture.adopt().await;
    let claims = fixture.claim_every_port_block();
    let slotted = ForkOptions {
        slot: Some(3),
        ..ForkOptions::default()
    };

    refused(
        fixture
            .mint(&runtime, "slotted", slotted)
            .await
            .expect_err("every port block is claimed"),
    );
    let record = fixture
        .intent("slotted")
        .expect("the bound slot keeps the intent");
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);
    assert_eq!(fixture.slot_tenant(3), Some(name("slotted")));

    // A bind refusal mutates nothing: the slot is another workspace's.
    fixture
        .mint(
            &runtime,
            "intruder",
            ForkOptions {
                slot: Some(3),
                ..ForkOptions::default()
            },
        )
        .await
        .expect_err("slot 3 belongs to slotted");
    assert_eq!(fixture.intent("intruder"), None);
    runtime.shutdown().await.expect("stop the refusing runtime");

    claims.release();
    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["main", "slotted"]);
    assert!(completed(fixture.intent("slotted")));
    assert_eq!(fixture.intent("intruder"), None);
    gc.shutdown().await.expect("stop gc");
}

/// The residue an unchecked `--ref` left: a create past its mutation fence whose pending
/// clone exists, and whose every replay fails. Here the git-worktree clone stopped on a link
/// it cannot restore, and its record carries the missing revision as the incident's did. `rm`
/// retires the clone instead of replaying it, and the name is free again. The same unreplayable
/// create with no clone behind it — stopped after its mutation mark, before the clone's
/// first write — is the second workspace here: `rm` has only the intent to retire, and
/// retires it. One adopted project proves both (08_testing.md, "Host limit").
#[tokio::test]
async fn rm_retires_a_pending_clone_whose_create_can_never_be_replayed() {
    let fixture = Fixture::new("unreplayable-clone");
    let runtime = fixture.adopt().await;
    fixture.commit_escaping_link();
    fixture
        .mint(&runtime, "wedged", worktree())
        .await
        .expect_err("the clone's initializer refuses the escaping link");
    assert!(
        fixture
            .layout()
            .session_image(&name("wedged"))
            .expect("pending image path")
            .image()
            .exists(),
        "the git-worktree clone stays pending"
    );
    runtime
        .shutdown()
        .await
        .expect("stop the interrupted runtime");
    fixture.journal_unreplayable_create("wedged");
    fixture.journal_unreplayable_create("stranded");

    let rm = fixture
        .open_removing("wedged")
        .await
        .expect("`rm wedged` opens without replaying the create");
    fixture
        .remove(&rm, "wedged")
        .await
        .expect("rm retires the pending clone");
    assert_eq!(fixture.listed(&rm).await, ["main"]);
    rm.shutdown().await.expect("stop the removing runtime");
    fixture.remove_escaping_link();

    let next = fixture
        .open_naming("wedged")
        .await
        .expect("`new wedged` opens: the retired create is not replayed");
    fixture
        .mint(&next, "wedged", ForkOptions::default())
        .await
        .expect("the name is free again");
    assert_eq!(fixture.listed(&next).await, ["main", "wedged"]);
    next.shutdown().await.expect("stop the creating runtime");
}

/// The same unreplayable create with no clone behind it — stopped after its mutation mark,
/// before the clone's first write. `rm` has only the intent to retire, and retires it.
#[tokio::test]
async fn rm_retires_a_clone_intent_that_left_no_clone_and_can_never_be_replayed() {
    let fixture = Fixture::new("unreplayable-intent");
    let runtime = fixture.adopt().await;
    runtime.shutdown().await.expect("stop the adopting runtime");
    fixture.journal_unreplayable_create("stranded");
    let rm = fixture
        .open_removing("stranded")
        .await
        .expect("`rm stranded` opens without replaying the create");
    fixture
        .remove(&rm, "stranded")
        .await
        .expect("rm retires the bare intent");
    assert!(matches!(
        fixture
            .intent("stranded")
            .and_then(|record| record.completion),
        Some(LifecycleIntentCompletion::Retire(_))
    ));
    rm.shutdown().await.expect("stop the removing runtime");

    let next = fixture
        .open_naming("wedged")
        .await
        .expect("`new wedged` opens: the retired create is not replayed");
    fixture
        .mint(&next, "wedged", ForkOptions::default())
        .await
        .expect("the name is free again");
    assert_eq!(fixture.listed(&next).await, ["main", "wedged"]);
    next.shutdown().await.expect("stop the creating runtime");
}

/// A fork whose initializer refuses leaves nothing behind, and the next attempt clones the
/// source as it is then. The incident: a stale worktree registration in main refused a fork,
/// and every retry resumed the refused clone's own copy of it, refusing again long after main
/// had dropped it, until the clone was removed by hand. Here a committed link leaving the tree
/// refuses the first attempt; once main drops it, the retry must succeed.
#[tokio::test]
async fn a_refused_fork_leaves_nothing_and_its_retry_clones_the_repaired_source() {
    let fixture = Fixture::new("refused-fork");
    let runtime = fixture.adopt().await;
    let volumes = fixture.build_images();
    fixture.commit_escaping_link();
    fixture
        .mint(&runtime, "retried", ForkOptions::default())
        .await
        .expect_err("the clone's initializer refuses the escaping link");
    assert!(
        !fixture
            .layout()
            .session_image(&name("retried"))
            .expect("refused image path")
            .image()
            .exists(),
        "the refused clone is reclaimed"
    );
    assert_eq!(
        fixture.intent("retried"),
        None,
        "the refusal withdraws its intent"
    );
    assert_eq!(
        fixture.build_images(),
        volumes,
        "the refusal collects the build volume it forked"
    );
    assert_eq!(fixture.listed(&runtime).await, ["main"]);

    fixture.remove_escaping_link();
    fixture
        .mint(&runtime, "retried", ForkOptions::default())
        .await
        .expect("the retry clones main as it is now");
    assert_eq!(fixture.listed(&runtime).await, ["main", "retried"]);
    assert!(completed(fixture.intent("retried")));
    runtime.shutdown().await.expect("stop the creating runtime");
}

/// The incident's trigger: main's repository still registered a worktree whose directory was
/// gone, and that registration's hook links named files inside it. A standalone fork drops the
/// registrations it inherits, so it never judges those links, and the fork succeeds.
#[tokio::test]
async fn a_stale_worktree_registration_in_main_does_not_refuse_a_standalone_fork() {
    let fixture = Fixture::new("stale-registration");
    let runtime = fixture.adopt().await;
    let hooks = fixture.checkout.join(".git/worktrees/gone/hooks");
    fs::create_dir_all(&hooks).expect("stale registration hooks");
    fs::write(
        hooks.parent().expect("registration").join("gitdir"),
        b"/nowhere/gone/.git\n",
    )
    .expect("stale registration gitdir");
    std::os::unix::fs::symlink(
        "../../../../../gone/tooling/git-hooks/commit-msg.sh",
        hooks.join("commit-msg"),
    )
    .expect("stale registration hook link");
    fixture
        .mint(&runtime, "forked", ForkOptions::default())
        .await
        .expect("main's stale registration is not the fork's");
    assert_eq!(fixture.listed(&runtime).await, ["forked", "main"]);
    runtime.shutdown().await.expect("stop the creating runtime");
}

/// Named removal inspects the pending fork itself, even after its clone source is gone.
#[tokio::test]
async fn rm_retires_a_pending_fork_after_its_source_is_removed() {
    let fixture = Fixture::new("rm-gone-source");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "source", ForkOptions::default())
        .await
        .expect("mint source");
    // A git-worktree fork's branch lives in main, so a stale one there refuses the fork's
    // initializer and keeps its clone pending for the retry.
    fixture.plant_workspace_branch("wedged");
    fixture
        .call(
            &runtime,
            "coordinator.fork",
            json!({ "repoId": fixture.repo, "source": "source", "workspace": "wedged", "options": worktree() }),
        )
        .await
        .expect_err("main's stale destination branch interrupts the fork");
    let image = fixture
        .layout()
        .session_image(&name("wedged"))
        .expect("pending image")
        .image()
        .to_owned();
    let pending = DetachedWorkspaceMetadata::read_for_image(&image).expect("pending metadata");
    assert_eq!(pending.publication_state, PublicationState::PendingFence);
    let origin = fixture.intent("wedged").expect("clone intent").operation;
    fixture
        .remove(&runtime, "source")
        .await
        .expect("retire the clean, landed source");
    assert!(
        !fixture
            .layout()
            .session_image(&name("source"))
            .expect("source image")
            .image()
            .exists()
    );
    runtime.shutdown().await.expect("stop the creating runtime");

    let rm = fixture
        .open_removing("wedged")
        .await
        .expect("removal never replays the missing source");
    assert_eq!(fixture.listed(&rm).await, ["main"]);
    fixture
        .remove(&rm, "wedged")
        .await
        .expect("retire the pending fork without its source");
    assert!(
        !image.exists(),
        "the clean pending image is physically released"
    );
    let retired = fixture.intent("wedged").expect("retirement record");
    assert!(matches!(
        retired.completion,
        Some(LifecycleIntentCompletion::Retire(_))
    ));
    assert!(
        matches!(retired.operation, LifecycleIntent::Retire { origin: Some(ref retained), .. } if **retained == origin)
    );
    assert_eq!(
        fixture.listed(&rm).await,
        ["main"],
        "the fork is never activated"
    );
    rm.shutdown().await.expect("stop the removing runtime");
    // Clean pending clones use the same direct physical release, never recovery trash.
    let trash = fixture.layout().project().sessions.join(".trash");
    assert!(!trash.exists());

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("source"),
            destination: name("empty"),
            rename: false,
        });
        journal.mark_mutating(&name("empty"))
    })
    .expect("journal a fork stopped before its first image write");
    let rm = fixture
        .open_removing("empty")
        .await
        .expect("open removal of an image-free fork");
    fixture
        .remove(&rm, "empty")
        .await
        .expect("retire the image-free intent without its source");
    assert!(matches!(
        fixture.intent("empty").and_then(|record| record.completion),
        Some(LifecycleIntentCompletion::Retire(_))
    ));
    let empty = fixture
        .intent("empty")
        .expect("image-free retirement record");
    assert!(
        matches!(empty.operation, LifecycleIntent::Retire { origin: Some(ref retained), .. }
        if matches!(retained.as_ref(), LifecycleIntent::Fork { source, destination, rename: false }
            if source == &name("source") && destination == &name("empty")))
    );
    rm.shutdown()
        .await
        .expect("stop the image-free removing runtime");
}

/// Losing a finished fork's source never authorizes losing its unlanded commits, and abandoning
/// them keeps them: `--abandon` writes the head into main's repository under
/// `refs/cowshed/<ws>/abandoned/<head>/` before the image retires, and the deletion log names the
/// head and that ref. A bundle whose refs are gone — what an earlier build left — gets them back
/// from `gc --abandoned` before the sweep deletes it, journaled the same way.
#[tokio::test]
async fn rm_keeps_a_finished_forks_unlanded_commits_after_its_source_is_removed() {
    let fixture = Fixture::new("rm-unlanded-fork");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "source", ForkOptions::default())
        .await
        .expect("mint source");
    let fork = fixture
        .fork(&runtime, "source", "finished")
        .await
        .expect("finish fork");
    let mount = PathBuf::from(fork["info"]["mount"].as_str().expect("fork mount"));
    fs::write(mount.join("unlanded"), b"unique work\n").expect("unlanded file");
    git(&mount, &["add", "unlanded"]);
    git(&mount, &["commit", "-q", "-m", "unlanded work"]);
    let original_head = head(&mount);
    fixture
        .remove(&runtime, "source")
        .await
        .expect("retire the landed source");
    runtime.shutdown().await.expect("stop the creating runtime");

    let rm = fixture
        .open_removing("finished")
        .await
        .expect("open finished-fork removal");
    for force in [false, true] {
        let refusal = fixture
            .call(
                &rm,
                "coordinator.destroy",
                json!({
                    "repoId": fixture.repo, "workspace": "finished",
                    "options": RemoveOptions { force, ..RemoveOptions::default() },
                }),
            )
            .await
            .expect_err("unlanded commits require explicit abandonment");
        assert_eq!(refusal.code, cowshed_core::ErrorCode::Conflict);
        assert!(
            refusal
                .message
                .contains("carries 1 commit changing 1 file whose change main"),
            "{refusal:?}"
        );
        assert_eq!(head(&mount), original_head);
        assert!(
            fixture
                .layout()
                .session_image(&name("finished"))
                .expect("finished image")
                .image()
                .exists()
        );
    }

    let abandoned = fixture
        .call(
            &rm,
            "coordinator.destroy",
            json!({
                "repoId": fixture.repo, "workspace": "finished",
                "options": RemoveOptions { abandon: true, force: true, ..RemoveOptions::default() },
            }),
        )
        .await
        .expect("--abandon retires the unlanded fork");
    let recovery = fixture
        .layout()
        .project()
        .sessions
        .join(format!(".trash/finished-{}.dmg", incarnation_of(&fork),));
    assert_eq!(
        head(&recovery),
        original_head,
        "unlanded history remains in recovery"
    );
    rm.shutdown().await.expect("stop the abandoning runtime");
    let reference = format!("refs/cowshed/finished/abandoned/{original_head}/HEAD");
    let kept_head = json!({ "name": "HEAD", "tip": original_head, "reference": reference });
    assert!(
        abandoned["abandoned"]["preserved"]["refs"]
            .as_array()
            .is_some_and(|kept| kept.contains(&kept_head)),
        "{abandoned}"
    );
    assert_eq!(
        resolved(&fixture.checkout, &reference),
        original_head,
        "main's repository holds the abandoned commit"
    );
    let retired = deletion_log(&fixture, "retire-workspace", "finished");
    assert_eq!(retired["git"]["head"], json!(original_head), "{retired}");
    assert_kept_by_ref(&retired, &original_head, &fixture.checkout, &reference);

    // An earlier build abandoned into the bundle alone: without the ref, the bundle in the trash
    // is the only copy, and `gc --abandoned` keeps its tips before it deletes it.
    git(&fixture.checkout, &["update-ref", "-d", &reference]);
    let bundle = PathBuf::from(
        abandoned["abandoned"]["bundle"]
            .as_str()
            .expect("bundle path"),
    );
    let gc = fixture.gc().await;
    fixture
        .call(
            &gc,
            "coordinator.gc",
            json!({ "repoId": fixture.repo, "options": { "dryRun": false, "abandoned": true } }),
        )
        .await
        .expect("gc --abandoned");
    gc.shutdown().await.expect("stop gc");
    assert!(!bundle.exists(), "the sweep deleted {}", bundle.display());
    assert_eq!(
        head(&recovery),
        original_head,
        "bundle authorization does not discard its image"
    );
    assert_eq!(
        resolved(&fixture.checkout, &reference),
        original_head,
        "the sweep kept the bundle's tip in main's repository first"
    );
    let kept = deletion_log(&fixture, "keep-abandoned-tips", "finished");
    assert_eq!(kept["artifact"], json!(bundle), "{kept}");
    assert_eq!(kept["git"]["head"], json!(original_head), "{kept}");
    assert_kept_by_ref(&kept, &original_head, &fixture.checkout, &reference);

    // A bundle is not permission to delete the complete recovery image. Terminal store
    // removal refuses before main moves while that evidence remains.
    let purge = fixture.gc().await;
    let refusal = fixture
        .call(
            &purge,
            "coordinator.removeProject",
            json!({ "repoId": fixture.repo, "options": { "force": false, "abandon": true } }),
        )
        .await
        .expect_err("recovery image refuses terminal project removal");
    assert_eq!(refusal.code, cowshed_core::ErrorCode::Conflict);
    assert_eq!(head(&recovery), original_head);
    purge.shutdown().await.expect("stop the purging runtime");
    assert_eq!(
        resolved(&fixture.checkout, &reference),
        original_head,
        "the still-bound main checkout keeps the abandoned commit"
    );
}

/// `entry`'s Git tips name `HEAD` at `head`, kept by `reference` in the repository at
/// `repository`.
fn assert_kept_by_ref(entry: &Value, head: &str, repository: &Path, reference: &str) {
    let tip = entry["git"]["tips"]
        .as_array()
        .and_then(|tips| tips.iter().find(|tip| tip["name"] == "HEAD"))
        .unwrap_or_else(|| panic!("no HEAD tip: {entry}"));
    assert_eq!(tip["tip"], json!(head), "{entry}");
    assert_eq!(tip["kept"]["by"], json!("ref"), "{entry}");
    assert_eq!(tip["kept"]["reference"], json!(reference), "{entry}");
    let kept_in = PathBuf::from(tip["kept"]["repository"].as_str().expect("repository"));
    assert_eq!(
        kept_in.canonicalize().expect("kept repository"),
        repository.canonicalize().expect("main checkout"),
        "{entry}"
    );
}

/// A clean workspace at main reads as landed, so an `rm` sweep can take one its owner forked a
/// minute ago and has not written in yet. An explicit `rm` refuses a workspace that young, naming
/// its age and the floor, and refuses a dirty one first; `--force` removes it, and the deletion
/// log says which process forced it and past which guards (02_workspaces.md, `rm` step 1).
#[tokio::test]
async fn rm_refuses_a_young_or_dirty_workspace_unforced_and_journals_who_forced_it() {
    let fixture = Fixture::new("rm-young");
    let runtime = fixture.adopt().await;
    let minted = fixture
        .mint(&runtime, "fresh", ForkOptions::default())
        .await
        .expect("mint fresh");
    let mount = PathBuf::from(minted["info"]["mount"].as_str().expect("fresh mount"));
    let unforced = || {
        fixture.call(
            &runtime,
            "coordinator.destroy",
            json!({ "repoId": fixture.repo, "workspace": "fresh", "options": RemoveOptions::default() }),
        )
    };
    let young = unforced()
        .await
        .expect_err("a workspace minted seconds ago is not removed unforced");
    assert_eq!(young.code, cowshed_core::ErrorCode::Conflict, "{young:?}");
    assert!(
        young.message.contains("workspace fresh was created")
            && young
                .message
                .contains("younger than 10 minutes is not removed"),
        "{young:?}"
    );
    assert!(!young.hint.contains("--force"), "{young:?}");

    fs::write(mount.join("draft"), b"first edit\n").expect("an uncommitted edit");
    let dirty = unforced()
        .await
        .expect_err("uncommitted work is not removed unforced");
    assert!(
        dirty.message.contains("has uncommitted Git work"),
        "{dirty:?}"
    );
    assert!(mount.join("draft").exists(), "a refusal keeps the edit");

    fixture
        .call(
            &runtime,
            "coordinator.destroy",
            json!({
                "repoId": fixture.repo, "workspace": "fresh",
                "options": RemoveOptions { force: true, ..RemoveOptions::default() },
            }),
        )
        .await
        .expect("--force removes it");
    let image = fixture
        .layout()
        .session_image(&name("fresh"))
        .unwrap()
        .image()
        .to_owned();
    assert!(
        !image.exists(),
        "forced dirty retirement leaves the live namespace"
    );
    let recovery = fixture
        .layout()
        .project()
        .sessions
        .join(format!(".trash/fresh-{}.dmg", incarnation_of(&minted),));
    assert_eq!(fs::read(recovery.join("draft")).unwrap(), b"first edit\n");
    runtime.shutdown().await.expect("stop the runtime");
    let retired = deletion_log(&fixture, "retire-workspace", "fresh");
    let by = &retired["retiredBy"];
    assert_eq!(by["caller"]["pid"], json!(std::process::id()), "{retired}");
    assert_eq!(
        by["caller"]["argv"],
        json!(
            std::env::args_os()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        ),
        "{retired}"
    );
    let forced = by["forced"].as_array().expect("a forced removal says so");
    assert_eq!(forced[0], json!({ "guard": "dirty" }), "{retired}");
    assert_eq!(forced[1]["guard"], "young", "{retired}");
    assert_eq!(
        forced[1]["createdAt"], minted["info"]["createdAt"],
        "{retired}"
    );
    assert!(
        forced[1]["ageSeconds"]
            .as_u64()
            .is_some_and(|age| age < 600),
        "{retired}"
    );
}

/// The newest `op` line of the project's deletion log for `workspace`.
fn deletion_log(fixture: &Fixture, op: &str, workspace: &str) -> Value {
    let log = fixture
        .layout()
        .project()
        .project_root
        .join("deletion-log.jsonl");
    let text = fs::read_to_string(&log).expect("deletion log");
    text.lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|entry| entry["op"] == op && entry["workspace"] == workspace)
        .unwrap_or_else(|| panic!("no {op} line for {workspace} in {}:\n{text}", log.display()))
}

/// The commit `revision` names in `directory`.
fn resolved(directory: &Path, revision: &str) -> String {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", revision])
        .current_dir(directory)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_locked()
        .expect("git process");
    assert!(
        output.status.success(),
        "git rev-parse {revision} in {}: {}",
        directory.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("revision is UTF-8")
        .trim()
        .to_owned()
}

/// Main's repository keeps no ref for work it already has (01_storage.md, "Retention"). Every gc
/// retires an abandonment main holds by content — here a pick whose conflict main resolved to the
/// workspace's own text, which neither ancestry nor patch identity sees — and journals it; it keeps
/// one main lacks, which doctor names with its count. Its owner lets that one go only by naming it
/// with a one-line reason, which the deletion log records.
#[tokio::test]
async fn gc_retires_the_abandoned_refs_main_holds_and_supersedes_the_rest_only_with_a_reason() {
    let fixture = Fixture::new("abandoned-refs");
    let runtime = fixture.adopt().await;
    let listed = fixture
        .call(&runtime, "project.list", json!({ "repoId": fixture.repo }))
        .await
        .expect("list workspaces");
    let main = PathBuf::from(
        listed
            .as_array()
            .and_then(|workspaces| {
                workspaces
                    .iter()
                    .find(|workspace| workspace["info"]["role"] == "main")
            })
            .and_then(|workspace| workspace["info"]["mount"].as_str())
            .unwrap_or_else(|| panic!("main's mount: {listed}")),
    );
    let mut heads = Vec::new();
    for (workspace, file, text) in [
        ("picked", "tracked", "picked\n"),
        ("spike", "spike", "spike\n"),
    ] {
        let minted = fixture
            .mint(&runtime, workspace, ForkOptions::default())
            .await
            .expect("mint workspace");
        let mount = PathBuf::from(minted["info"]["mount"].as_str().expect("workspace mount"));
        fs::write(mount.join(file), text).expect("workspace change");
        git(&mount, &["add", file]);
        git(&mount, &["commit", "-q", "-m", workspace]);
        heads.push(head(&mount));
    }
    runtime.shutdown().await.expect("stop the creating runtime");
    let (picked, spike) = (heads[0].clone(), heads[1].clone());
    for workspace in ["picked", "spike"] {
        let rm = fixture
            .open_removing(workspace)
            .await
            .expect("open removal");
        fixture
            .call(
                &rm,
                "coordinator.destroy",
                json!({
                    "repoId": fixture.repo, "workspace": workspace,
                    "options": RemoveOptions { abandon: true, force: true, ..RemoveOptions::default() },
                }),
            )
            .await
            .expect("--abandon keeps the unlanded tips in main");
        rm.shutdown().await.expect("stop the abandoning runtime");
    }
    let picked_ref = format!("refs/cowshed/picked/abandoned/{picked}/HEAD");
    let spike_ref = format!("refs/cowshed/spike/abandoned/{spike}/HEAD");
    // Main changes the line itself, then takes the workspace's text resolving the conflict: the
    // content lands under a commit whose diff no patch identity matches.
    fs::write(main.join("tracked"), b"main\n").expect("main's own change");
    git(&main, &["commit", "-q", "-am", "main's own change"]);
    fs::write(main.join("tracked"), b"picked\n").expect("resolved pick");
    git(
        &main,
        &["commit", "-q", "-am", "pick picked, conflict resolved"],
    );
    let main_head = head(&main);
    let landed = json!({ "kind": "landed", "targetBranch": "main", "targetHead": main_head });

    let gc = fixture.gc().await;
    let sweep = async |options: Value| {
        fixture
            .call(
                &gc,
                "coordinator.gc",
                json!({ "repoId": fixture.repo, "options": options }),
            )
            .await
    };
    let preview = sweep(json!({ "dryRun": true }))
        .await
        .expect("gc --dry-run");
    assert_eq!(
        preview["abandonedRefs"]["retired"][0]["workspace"], "picked",
        "{preview}"
    );
    assert_eq!(
        preview["abandonedRefs"]["retired"][0]["by"], landed,
        "{preview}"
    );
    assert_eq!(
        resolved(&main, &picked_ref),
        picked,
        "a dry run deletes nothing"
    );

    let swept = sweep(json!({})).await.expect("gc");
    let report = &swept["abandonedRefs"];
    assert_eq!(
        report["retired"].as_array().map(Vec::len),
        Some(1),
        "{swept}"
    );
    assert_eq!(report["retired"][0]["by"], landed, "{swept}");
    assert!(!has_ref(&main, &picked_ref), "gc retired {picked_ref}");
    let retired = deletion_log(&fixture, "retire-abandoned-ref", "picked");
    assert_eq!(retired["git"]["head"], json!(picked), "{retired}");
    assert_eq!(
        retired["git"]["tips"][0]["kept"],
        json!({ "by": "target", "branch": "main", "head": main_head }),
        "{retired}"
    );
    assert_eq!(report["kept"].as_array().map(Vec::len), Some(1), "{swept}");
    assert_eq!(report["kept"][0]["workspace"], "spike", "{swept}");
    assert_eq!(
        report["kept"][0]["tips"][0]["commits"]["unlanded"], 1,
        "{swept}"
    );
    assert_eq!(
        resolved(&main, &spike_ref),
        spike,
        "gc keeps work main lacks"
    );

    let doctor = fixture
        .call(&gc, "coordinator.doctor", json!({ "repoId": fixture.repo }))
        .await
        .expect("doctor");
    let named = doctor["findings"]
        .as_array()
        .expect("findings")
        .iter()
        .filter(|finding| finding["code"] == "abandoned-refs")
        .collect::<Vec<_>>();
    assert_eq!(named.len(), 1, "{doctor}");
    let message = named[0]["message"].as_str().expect("message");
    assert!(
        message.contains(&format!("refs/cowshed/spike/abandoned/{spike}/"))
            && message.contains("carries 1 commit main does not hold"),
        "{doctor}"
    );
    assert!(
        named[0]["hint"]
            .as_str()
            .is_some_and(|hint| hint.contains(&format!("cowshed gc --supersede spike/{spike}"))),
        "{doctor}"
    );

    for reason in ["", "  \t", "two\nlines"] {
        let refusal = sweep(json!({
            "supersede": { "workspace": "spike", "head": spike, "reason": reason }
        }))
        .await
        .expect_err("a supersession needs a one-line reason");
        assert!(refusal.message.contains("superseded reason"), "{refusal:?}");
    }
    let refusal = sweep(json!({
        "supersede": { "workspace": "picked", "head": picked, "reason": "landed already" }
    }))
    .await
    .expect_err("gc already retired picked's refs");
    assert_eq!(
        refusal.code,
        cowshed_core::ErrorCode::Conflict,
        "{refusal:?}"
    );
    assert!(
        refusal.message.contains(&format!(
            "keeps no refs under refs/cowshed/picked/abandoned/{picked}/"
        )),
        "{refusal:?}"
    );
    assert_eq!(
        resolved(&main, &spike_ref),
        spike,
        "a refused supersession deletes nothing"
    );

    let reason = "the spike is replaced by the gate rewrite";
    let superseded = sweep(json!({
        "supersede": { "workspace": "spike", "head": spike, "reason": reason }
    }))
    .await
    .expect("gc --supersede");
    assert_eq!(
        superseded["abandonedRefs"]["retired"][0]["by"],
        json!({ "kind": "superseded", "reason": reason }),
        "{superseded}"
    );
    assert!(!has_ref(&main, &spike_ref), "the owner retired {spike_ref}");
    let retired = deletion_log(&fixture, "retire-abandoned-ref", "spike");
    assert_eq!(
        retired["git"]["tips"][0]["kept"],
        json!({ "by": "superseded", "reason": reason }),
        "{retired}"
    );
    gc.shutdown().await.expect("stop gc");
}

/// Whether `reference` names a commit in the repository at `directory`.
fn has_ref(directory: &Path, reference: &str) -> bool {
    Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", reference])
        .current_dir(directory)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_locked()
        .expect("git process")
        .status
        .success()
}

/// The kernel claim on a new workspace's port block lasts until its image publication is
/// complete and no longer (05_gateway.md): from then on the image owns the block, and another
/// process's gateway reconcile installing the workspace's session must find no claim of the
/// verb's on it, or it moves the workspace to another block at a new grant revision.
/// Before publication, current port refusals and an older binary's artifact-free Prepared
/// intent each pass through their own store-scope recovery. A missing revision then leaves no
/// intent for a named reopening, whose successful retry also proves the create's port handoff.
#[tokio::test]
async fn a_created_workspaces_port_block_is_free_for_its_gateway_session_once_published() {
    let fixture = Fixture::new("published-create");
    let main_gateway = fixture.gateway_at_hello("main");
    let runtime = fixture.adopt().await;
    assert_eq!(main_gateway.claimed().await, [0_u16; 0]);

    let claims = fixture.claim_every_port_block();
    refused(
        fixture
            .mint(&runtime, "refused", ForkOptions::default())
            .await
            .expect_err("every port block is claimed"),
    );
    assert_eq!(
        fixture.intent("refused"),
        None,
        "a refusal before the first mutation leaves no intent"
    );
    refused(
        fixture
            .fork(&runtime, "main", "refused-fork")
            .await
            .expect_err("every port block is claimed"),
    );
    assert_eq!(fixture.intent("refused-fork"), None);
    runtime.shutdown().await.expect("stop the refusing runtime");

    // With blocks free again, a replayed intent would now succeed: gc must find none.
    claims.release();
    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["main"]);
    assert_eq!(fixture.intent("refused"), None);
    assert_eq!(fixture.intent("refused-fork"), None);
    gc.shutdown().await.expect("stop gc");

    // An older binary left Prepared without an artifact; a separate opening discards it.
    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Create {
            workspace: name("refused"),
            options: cowshed_core::storage::recovery::MintOptions::default(),
        });
        Ok(())
    })
    .expect("journal the refused create as an older binary did");
    let runtime = fixture.gc().await;
    assert_eq!(fixture.listed(&runtime).await, ["main"]);
    assert_eq!(fixture.intent("refused"), None);

    let error = fixture
        .mint(&runtime, "wedged", missing_revision())
        .await
        .expect_err("main holds no such commit");
    assert!(
        error.message.contains(MISSING_REVISION),
        "the refusal names the revision, got {}: {}",
        error.code.as_str(),
        error.message
    );
    assert_eq!(
        fixture.intent("wedged"),
        None,
        "a revision refused before the clone journals nothing"
    );
    assert_eq!(fixture.listed(&runtime).await, ["main"]);
    runtime.shutdown().await.expect("stop the refusing runtime");

    let next = fixture
        .open_naming("wedged")
        .await
        .expect("`new wedged` opens: nothing of the refused create is replayed");
    let gateway = fixture.gateway_at_hello("wedged");
    fixture
        .mint(&next, "wedged", ForkOptions::default())
        .await
        .expect("the name is free for a create that can succeed");
    assert_eq!(fixture.listed(&next).await, ["main", "wedged"]);
    assert!(completed(fixture.intent("wedged")));
    assert_eq!(gateway.claimed().await, [0_u16; 0]);

    let gateway = fixture.gateway_at_hello("forked");
    fixture.fork(&next, "main", "forked").await.expect("fork");
    assert_eq!(gateway.claimed().await, [0_u16; 0]);
    next.shutdown().await.expect("stop the creating runtime");
}

/// A `Conflict` refusal, whether the opening or the verb it runs refuses.
fn refused_by_conflict<T>(result: Result<T>) {
    match result {
        Ok(_) => panic!("expected a conflict refusal, got success"),
        Err(error) => assert_eq!(error.code.as_str(), "conflict", "{}", error.message),
    }
}

/// The commit `HEAD` names in `directory`.
fn head(directory: &Path) -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(directory)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_locked()
        .expect("git process");
    assert!(output.status.success(), "git rev-parse HEAD");
    String::from_utf8(output.stdout)
        .expect("HEAD is UTF-8")
        .trim()
        .to_owned()
}

/// A rename whose clone was captured while its source is still live: the source must not be
/// retired, and neither name is served, until the intent completes.
#[tokio::test]
async fn a_pending_rename_refuses_both_names_while_its_source_is_still_live() {
    let fixture = Fixture::new("rename-source-live");
    let runtime = fixture.adopt().await;
    let minted = fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let a_mount = PathBuf::from(minted["info"]["mount"].as_str().expect("a mount"));
    fixture
        .fork(&runtime, "a", "b")
        .await
        .expect("fork a to b at H1");
    fs::write(a_mount.join("tracked"), b"H2\n").expect("H2 change in a");
    git(&a_mount, &["add", "tracked"]);
    git(&a_mount, &["commit", "-q", "-m", "H2"]);
    let h2 = head(&a_mount);
    runtime.shutdown().await.expect("stop the runtime");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        journal.mark_mutating(&name("b"))
    })
    .expect("journal the rename after its clone is captured");

    refused_by_conflict(fixture.open_naming("a").await);
    refused_by_conflict(fixture.open_naming("b").await);
    assert_eq!(head(&a_mount), h2, "the source keeps its H2 branch state");
    assert!(
        !completed(fixture.intent("b")),
        "the rename stays unfinished, so neither name is served"
    );
}

/// A rename journaled `Prepared` with nothing published: its source's opening discards it and
/// creates nothing under the destination name.
#[tokio::test]
async fn a_prepared_rename_is_discarded_by_its_source_and_creates_nothing() {
    let fixture = Fixture::new("rename-prepared");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    runtime.shutdown().await.expect("stop the runtime");
    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        Ok(())
    })
    .expect("journal the prepared rename");

    let opened = fixture
        .open_naming("a")
        .await
        .expect("`mv a b`'s source opens without a refusal");
    assert_eq!(fixture.listed(&opened).await, ["a", "main"]);
    assert_eq!(
        fixture.intent("b"),
        None,
        "the prepared rename is discarded"
    );
    opened.shutdown().await.expect("stop the opened runtime");
}

/// Journals a rename `a -> b` for `b`, `Prepared` or, with `mutating`, past its clone's mark.
fn journal_rename(fixture: &Fixture, mutating: bool) {
    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        if mutating {
            journal.mark_mutating(&name("b"))
        } else {
            Ok(())
        }
    })
    .expect("journal the rename");
}

/// A prepared rename that never changed anything is discarded by removing its source, which
/// then retires like any other workspace, and no destination is ever created.
#[tokio::test]
async fn removes_source_after_discarding_unpublished_prepared_rename() {
    let fixture = Fixture::new("ren-rm");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    runtime.shutdown().await.expect("stop the runtime");
    journal_rename(&fixture, false);

    let rm = fixture
        .open_removing("a")
        .await
        .expect("rm a opens and discards the prepared rename");
    fixture
        .remove(&rm, "a")
        .await
        .expect("rm a removes the source");
    assert_eq!(
        fixture.intent("b"),
        None,
        "the prepared rename is discarded"
    );
    assert_eq!(fixture.listed(&rm).await, ["main"]);
    let layout = fixture.layout();
    assert!(
        !layout
            .canonical_image(&name("a"))
            .expect("source image path")
            .image()
            .exists()
    );
    assert!(
        !layout
            .canonical_image(&name("b"))
            .expect("destination image path")
            .image()
            .exists()
    );
    rm.shutdown().await.expect("stop the removing runtime");
}

/// A rename past its clone's mark is owned by its claim: removing its source is refused by name,
/// keeps the source image and the unfinished intent, and creates no destination.
#[tokio::test]
async fn removing_source_of_mutating_rename_refuses_and_keeps_claim() {
    let fixture = Fixture::new("ren-mut");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    runtime.shutdown().await.expect("stop the runtime");
    journal_rename(&fixture, true);

    let refusal = match fixture.open_removing("a").await {
        Err(error) => error,
        Ok(rm) => {
            let error = fixture
                .remove(&rm, "a")
                .await
                .expect_err("rm a must refuse the mutating rename's source");
            rm.shutdown().await.expect("stop the removing runtime");
            error
        }
    };
    assert_eq!(refusal.code.as_str(), "conflict", "{}", refusal.message);
    assert!(
        refusal.hint.contains("rm b --force"),
        "the refusal names the cancellation, got {}",
        refusal.hint
    );
    let record = fixture.intent("b").expect("the rename stays journaled");
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);
    assert_eq!(record.completion, None);
    let layout = fixture.layout();
    assert!(
        layout
            .canonical_image(&name("a"))
            .expect("source image path")
            .image()
            .exists()
    );
    assert!(
        !layout
            .canonical_image(&name("b"))
            .expect("destination image path")
            .image()
            .exists()
    );
}

fn incarnation_of(view: &Value) -> WorkspaceIncarnation {
    serde_json::from_value(view["info"]["workspaceIncarnation"].clone())
        .expect("workspace incarnation")
}

/// A historical create journaled over a published workspace completes as a workspace
/// introduction, never as a fork: the replay appends one introduced commitment for the
/// destination and no fork commitment for it.
#[tokio::test]
async fn a_historical_create_completion_records_introduction_not_fork() {
    let fixture = Fixture::new("hist-create");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let forked = fixture
        .fork(&runtime, "main", "b")
        .await
        .expect("fork b published");
    let destination = incarnation_of(&forked);
    runtime.shutdown().await.expect("stop the runtime");
    let forks_before = fork_commitments(&fixture, &destination);
    let introduced_before = introduced_commitments(&fixture, &destination);

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Create {
            workspace: name("b"),
            options: cowshed_core::storage::recovery::MintOptions::default(),
        });
        journal.mark_mutating(&name("b"))
    })
    .expect("journal the historical create over the published b");

    let reopened = fixture
        .open_naming("b")
        .await
        .expect("the historical create completes");
    assert!(completed(fixture.intent("b")));
    assert_eq!(
        introduced_commitments(&fixture, &destination),
        introduced_before + 1,
        "the completion records one workspace introduction"
    );
    assert_eq!(
        fork_commitments(&fixture, &destination),
        forks_before,
        "a historical create appends no fork commitment for b"
    );
    reopened
        .shutdown()
        .await
        .expect("stop the completing runtime");
}

/// Fork commitments recorded for `destination` across every sealed telemetry segment.
fn fork_commitments(fixture: &Fixture, destination: &WorkspaceIncarnation) -> usize {
    commitments_counted(
        fixture,
        |commitment| matches!(commitment, ControllerCommitment::Fork(fork) if fork.destination_incarnation == *destination),
    )
}

/// Workspace-introduced commitments recorded for `incarnation`.
fn introduced_commitments(fixture: &Fixture, incarnation: &WorkspaceIncarnation) -> usize {
    commitments_counted(
        fixture,
        |commitment| matches!(commitment, ControllerCommitment::WorkspaceIntroduced(introduced) if introduced.workspace_incarnation == *incarnation),
    )
}

/// Every sealed commitment telemetry segment's commitments that `counts` accepts, read the way
/// the audit sink's own tests read them.
fn commitments_counted(fixture: &Fixture, counts: impl Fn(&ControllerCommitment) -> bool) -> usize {
    let Ok(dates) = fs::read_dir(fixture.storage.telemetry()) else {
        return 0;
    };
    let mut count = 0;
    for date in dates {
        let date = date.expect("telemetry date entry");
        if !date.file_type().expect("file type").is_dir() {
            continue;
        }
        for child in fs::read_dir(date.path()).expect("telemetry date") {
            let child = child.expect("telemetry segment entry");
            let segment = child.file_name();
            let bytes = segment.as_encoded_bytes();
            if !(bytes.starts_with(b"commitment-") && bytes.ends_with(b".arrow")) {
                continue;
            }
            let file = fs::File::open(child.path()).expect("sealed segment");
            let reader = StreamReader::try_new(file, None).expect("arrow stream");
            for batch in reader {
                let batch = batch.expect("commitment batch");
                assert_eq!(batch.schema(), controller_commitment_schema());
                count += decode_controller_commitments(&batch)
                    .expect("decode commitments")
                    .iter()
                    .filter(|commitment| counts(commitment))
                    .count();
            }
        }
    }
    count
}

/// The unfinished rename's source still exists: its replay must append one fork commitment for
/// the destination incarnation, then complete the intent and retire the source.
#[tokio::test]
async fn a_replayed_rename_with_a_live_source_appends_its_fork_commitment() {
    let fixture = Fixture::new("r-audit");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let forked = capture_rename(&fixture, &runtime).await;
    let destination = incarnation_of(&forked);
    let before = fork_commitments(&fixture, &destination);
    runtime.shutdown().await.expect("stop the runtime");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        journal.mark_mutating(&name("b"))
    })
    .expect("journal the mutating rename");

    let reopened = fixture.open_naming("b").await.expect("the rename replays");
    assert!(completed(fixture.intent("b")));
    assert_eq!(
        fork_commitments(&fixture, &destination),
        before + 1,
        "replay appends one fork commitment for the destination incarnation"
    );
    assert_eq!(fixture.listed(&reopened).await, ["b", "main"]);
    reopened
        .shutdown()
        .await
        .expect("stop the replaying runtime");
}

/// The source was already retired before the rename's replay: the replay still records the
/// fork commitment for the destination it finishes.
#[tokio::test]
async fn a_replayed_rename_with_a_retired_source_appends_its_fork_commitment() {
    let fixture = Fixture::new("r-gone");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let forked = capture_rename(&fixture, &runtime).await;
    let destination = incarnation_of(&forked);
    let before = fork_commitments(&fixture, &destination);
    fixture
        .remove(&runtime, "a")
        .await
        .expect("retire the source before the rename is journaled");
    runtime.shutdown().await.expect("stop the runtime");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        journal.mark_mutating(&name("b"))
    })
    .expect("journal the mutating rename");

    let reopened = fixture
        .open_naming("b")
        .await
        .expect("the rename replays with its source gone");
    assert!(completed(fixture.intent("b")));
    assert_eq!(
        fork_commitments(&fixture, &destination),
        before + 1,
        "the source-retired replay appends one fork commitment for the destination"
    );
    reopened
        .shutdown()
        .await
        .expect("stop the replaying runtime");
}

/// An active fork whose source is gone, journaled as a mutating create: the create replay
/// finishes it from its own destination marker, without requiring the source.
#[tokio::test]
async fn an_active_fork_with_a_gone_source_completes_its_create_intent() {
    let fixture = Fixture::new("r-line");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    fixture
        .fork(&runtime, "a", "b")
        .await
        .expect("fork a to b at H1");
    fixture
        .remove(&runtime, "a")
        .await
        .expect("destroy the clean, landed source");
    runtime.shutdown().await.expect("stop the runtime");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Create {
            workspace: name("b"),
            options: cowshed_core::storage::recovery::MintOptions::fork(
                Some(name("a")),
                ForkOptions::default(),
            ),
        });
        journal.mark_mutating(&name("b"))
    })
    .expect("journal the mutating fork as a create");

    let reopened = fixture
        .open_naming("b")
        .await
        .expect("the active destination completes with its source gone");
    assert!(completed(fixture.intent("b")));
    assert_eq!(fixture.listed(&reopened).await, ["b", "main"]);
    reopened
        .shutdown()
        .await
        .expect("stop the replaying runtime");
}

/// A detached source has no mounted image to clone, so a fork of it refuses with the attach
/// command that brings it back, and journals nothing for the destination.
#[tokio::test]
async fn a_detached_fork_source_is_refused_with_its_attach_command() {
    let fixture = Fixture::new("r-det");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    fixture
        .call(
            &runtime,
            "coordinator.detach",
            json!({ "repoId": fixture.repo, "workspace": "a" }),
        )
        .await
        .expect("detach a");

    let error = fixture
        .fork(&runtime, "a", "b")
        .await
        .expect_err("a detached source must refuse the fork");
    assert_eq!(error.code.as_str(), "conflict", "{}", error.message);
    assert!(error.hint.contains("cowshed attach a"), "{}", error.hint);
    assert_eq!(fixture.intent("b"), None);
    assert!(
        !fixture
            .layout()
            .canonical_image(&name("b"))
            .expect("destination image path")
            .image()
            .exists(),
        "a refused fork leaves no destination image"
    );
    runtime.shutdown().await.expect("stop the runtime");
}

/// Git bytes of `args` run in `directory`, asserted successful. The raw output, not a parsed one.
fn git_bytes(directory: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_locked()
        .expect("git process");
    assert!(output.status.success(), "git {args:?}");
    output.stdout
}

/// A dirty source renamed on real APFS keeps its branch, staged index, working bytes, and remotes;
/// both linked and standalone sources carry the same state, and the linked registration moves
/// from `a` to `b` with no leftover administration under the old name.
#[tokio::test]
async fn a_dirty_native_rename_preserves_branch_index_and_warm_state() {
    for (label, linked) in [("r-mv-u", false), ("r-mv-l", true)] {
        let fixture = Fixture::new(label);
        let runtime = fixture.adopt().await;
        let minted = fixture
            .mint(
                &runtime,
                "a",
                ForkOptions {
                    git_worktree: linked,
                    ..ForkOptions::default()
                },
            )
            .await
            .expect("mint a");
        let a_mount = PathBuf::from(minted["info"]["mount"].as_str().expect("a mount"));

        fs::write(a_mount.join("stage.txt"), b"base\n").expect("tracked file");
        git(&a_mount, &["add", "stage.txt"]);
        git(&a_mount, &["commit", "-q", "-m", "base"]);
        fs::write(a_mount.join("stage.txt"), b"staged\n").expect("staged edit");
        git(&a_mount, &["add", "stage.txt"]);
        fs::write(a_mount.join("stage.txt"), b"staged\nunstaged\n").expect("unstaged edit");
        fs::write(a_mount.join("loose.txt"), b"untracked\n").expect("untracked file");
        fs::write(a_mount.join(".gitignore"), b"/warm/\n").expect("ignore rule");
        fs::create_dir_all(a_mount.join("warm")).expect("warm directory");
        fs::write(a_mount.join("warm/file"), [0_u8, 1, 2, 255]).expect("warm bytes");

        let branch = git_bytes(&a_mount, &["branch", "--show-current"]);
        let head = git_bytes(&a_mount, &["rev-parse", "HEAD"]);
        let index_path = PathBuf::from(
            String::from_utf8(git_bytes(
                &a_mount,
                &["rev-parse", "--path-format=absolute", "--git-path", "index"],
            ))
            .expect("index path is UTF-8")
            .trim()
            .to_owned(),
        );
        let index_before = fs::read(&index_path).expect("source index bytes");
        let stage_before = git_bytes(&a_mount, &["ls-files", "--stage"]);
        let remotes_before = git_bytes(&a_mount, &["remote", "-v"]);
        let status_before = git_bytes(
            &a_mount,
            &[
                "--no-optional-locks",
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
            ],
        );
        let files_before = [
            fs::read(a_mount.join("stage.txt")).expect("stage bytes"),
            fs::read(a_mount.join("loose.txt")).expect("untracked bytes"),
            fs::read(a_mount.join("warm/file")).expect("warm bytes"),
        ];

        let renamed = fixture
            .call(
                &runtime,
                "coordinator.rename",
                json!({ "repoId": fixture.repo, "source": "a", "destination": "b" }),
            )
            .await
            .expect("rename a to b");
        let b_mount = PathBuf::from(renamed["info"]["mount"].as_str().expect("b mount"));

        assert_eq!(
            git_bytes(&b_mount, &["branch", "--show-current"]),
            branch,
            "{label}"
        );
        assert_ne!(
            git_bytes(&b_mount, &["branch", "--show-current"]),
            b"cowshed/b\n",
            "{label}"
        );
        assert_eq!(git_bytes(&b_mount, &["rev-parse", "HEAD"]), head, "{label}");
        assert_eq!(
            git_bytes(&b_mount, &["ls-files", "--stage"]),
            stage_before,
            "{label}"
        );
        if linked {
            let index_after_path = PathBuf::from(
                String::from_utf8(git_bytes(
                    &b_mount,
                    &["rev-parse", "--path-format=absolute", "--git-path", "index"],
                ))
                .expect("index path is UTF-8")
                .trim()
                .to_owned(),
            );
            assert_eq!(
                fs::read(&index_after_path).expect("index bytes"),
                index_before,
                "{label}"
            );
        }
        assert_eq!(
            git_bytes(
                &b_mount,
                &[
                    "--no-optional-locks",
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--untracked-files=all"
                ],
            ),
            status_before,
            "{label}"
        );
        assert_eq!(
            [
                fs::read(b_mount.join("stage.txt")).expect("stage bytes"),
                fs::read(b_mount.join("loose.txt")).expect("untracked bytes"),
                fs::read(b_mount.join("warm/file")).expect("warm bytes"),
            ],
            files_before,
            "{label}"
        );
        assert_eq!(
            git_bytes(&b_mount, &["remote", "-v"]),
            remotes_before,
            "{label}"
        );
        assert_eq!(fixture.listed(&runtime).await, ["b", "main"], "{label}");
        assert!(completed(fixture.intent("b")), "{label}");
        if linked {
            let main_admin = fixture.checkout.join(".git/worktrees");
            assert!(
                !main_admin.join("a").exists(),
                "{label}: old administration remains"
            );
            assert!(
                main_admin.join("b").exists(),
                "{label}: destination administration missing"
            );
        }
        runtime.shutdown().await.expect("stop the runtime");
    }
}

/// `git status --porcelain` in `directory`: the codes and paths, not the bytes.
fn porcelain(directory: &Path) -> String {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(directory)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_locked()
        .expect("git process");
    assert!(output.status.success(), "git status --porcelain");
    String::from_utf8(output.stdout).expect("status is UTF-8")
}

/// Captures the rename's clone of `a` as `b`, then puts `b` on `cowshed/a` at the tip it was
/// cloned from. This is the raw rename's original-branch shape: the source repository is left
/// alone, and nothing is forced. A public fork prunes the source's branches at mint, so the
/// capture recreates the carried branch rather than switching to it.
async fn capture_rename(fixture: &Fixture, runtime: &ProjectRuntime) -> Value {
    let forked = fixture
        .fork(runtime, "a", "b")
        .await
        .expect("fork a to b at the captured state");
    let b_mount = PathBuf::from(forked["info"]["mount"].as_str().expect("b mount"));
    git(&b_mount, &["switch", "-c", "cowshed/a"]);
    forked
}

/// A prepared rename whose destination is already published must resume: nothing is discarded,
/// and the source is retired by the completing replay.
#[tokio::test]
async fn a_prepared_rename_with_a_published_destination_resumes_without_discard() {
    let fixture = Fixture::new("b-pub");
    let runtime = fixture.adopt().await;
    fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let _b_mount = capture_rename(&fixture, &runtime).await;
    runtime.shutdown().await.expect("stop the runtime");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        Ok(())
    })
    .expect("journal the prepared rename over a published destination");

    let opened = fixture
        .open_naming("a")
        .await
        .expect("the prepared rename resumes");
    assert!(completed(fixture.intent("b")), "the rename completes");
    assert_eq!(fixture.listed(&opened).await, ["b", "main"]);
    opened.shutdown().await.expect("stop the resuming runtime");
}

/// A captured rename whose source is then edited must refuse, and keep the source's later bytes:
/// tracked and untracked content changed after capture, with the same HEAD and the same status.
#[tokio::test]
async fn a_captured_rename_keeps_later_dirty_and_untracked_bytes() {
    let fixture = Fixture::new("b-dirty");
    let runtime = fixture.adopt().await;
    let minted = fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let a_mount = PathBuf::from(minted["info"]["mount"].as_str().expect("a mount"));
    fs::write(a_mount.join("tracked"), b"two\n").expect("dirty tracked file");
    fs::write(a_mount.join("u1"), b"u1\n").expect("untracked file");
    let captured = capture_rename(&fixture, &runtime).await;
    let captured_status = porcelain(&a_mount);
    let captured_head = head(&a_mount);
    runtime.shutdown().await.expect("stop the runtime");
    let before = fork_commitments(&fixture, &incarnation_of(&captured));

    fs::write(a_mount.join("tracked"), b"five\n").expect("rewrite tracked after capture");
    fs::write(a_mount.join("u1"), b"u2\n").expect("rewrite untracked after capture");
    assert_eq!(head(&a_mount), captured_head);
    assert_eq!(porcelain(&a_mount), captured_status, "same codes and paths");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        journal.mark_mutating(&name("b"))
    })
    .expect("journal the mutating rename");

    refused_by_conflict(fixture.open_naming("a").await);
    refused_by_conflict(fixture.open_naming("a").await);
    assert_eq!(
        fork_commitments(&fixture, &incarnation_of(&captured)),
        before,
        "refused opens append no fork commitment for the captured rename"
    );
    assert_eq!(fs::read(a_mount.join("tracked")).unwrap(), b"five\n");
    assert_eq!(fs::read(a_mount.join("u1")).unwrap(), b"u2\n");
    let record = fixture.intent("b").expect("the rename stays journaled");
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);
    assert_eq!(record.completion, None);
}

/// A captured rename whose source gains only a new untracked path, with an unchanged HEAD, must
/// refuse and keep that path's bytes and the intent.
#[tokio::test]
async fn a_captured_rename_keeps_new_untracked_path_without_head_change() {
    let fixture = Fixture::new("b-new");
    let runtime = fixture.adopt().await;
    let minted = fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let a_mount = PathBuf::from(minted["info"]["mount"].as_str().expect("a mount"));
    let _b_mount = capture_rename(&fixture, &runtime).await;
    runtime.shutdown().await.expect("stop the runtime");

    fs::write(a_mount.join("new.txt"), b"fresh\n").expect("new untracked path in a");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        journal.mark_mutating(&name("b"))
    })
    .expect("journal the mutating rename");

    refused_by_conflict(fixture.open_naming("a").await);
    assert_eq!(fs::read(a_mount.join("new.txt")).unwrap(), b"fresh\n");
    assert!(
        fixture
            .intent("b")
            .is_some_and(|record| record.completion.is_none())
    );
}

/// A refused resume of an existing destination must leave the rename `Mutating`: a refusal that
/// resets the phase lets the next open discard a rename whose clone is already published.
#[tokio::test]
async fn a_refused_existing_destination_resume_keeps_mutating_phase() {
    let fixture = Fixture::new("b-phase");
    let runtime = fixture.adopt().await;
    let minted = fixture
        .mint(&runtime, "a", ForkOptions::default())
        .await
        .expect("mint a");
    let a_mount = PathBuf::from(minted["info"]["mount"].as_str().expect("a mount"));
    let _b_mount = capture_rename(&fixture, &runtime).await;
    fs::write(a_mount.join("tracked"), b"H2\n").expect("H2 change in a");
    git(&a_mount, &["add", "tracked"]);
    git(&a_mount, &["commit", "-q", "-m", "H2"]);
    runtime.shutdown().await.expect("stop the runtime");

    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Fork {
            source: name("a"),
            destination: name("b"),
            rename: true,
        });
        Ok(())
    })
    .expect("journal the prepared rename over a published destination");

    refused_by_conflict(fixture.open_naming("a").await);
    refused_by_conflict(fixture.open_naming("b").await);
    let record = fixture.intent("b").expect("the rename stays journaled");
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);
    assert_eq!(record.completion, None);
}
