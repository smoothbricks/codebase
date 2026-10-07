use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_core::apfs::{
    ApfsBackend, CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend,
    SystemCommandRunner,
};
use cowshed_core::api::dto::{AdoptOptions, CreateOptions, RemoveOptions, RevisionTarget};
use cowshed_core::api::operations::OperationRequest;
use cowshed_core::api::server::ConnectionAuthority;
use cowshed_core::fork_lock::Run as _;
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, ImageCapacity, PortBlock, PublicationState, WorkspaceName,
};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope, supervisor_socket};
use cowshed_core::storage::StorageLayout;
use cowshed_core::storage::apfs::ApfsSubstrateConfig;
use cowshed_core::storage::apfs::native::{
    MacOsApfsExecutionHost, blank_template, blank_template_path,
};
use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
use cowshed_core::storage::recovery::{
    LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentCompletion, LifecycleIntentJournal,
    LifecycleIntentPhase, LifecycleIntentRecord,
};
use cowshed_core::{CowshedError, Result};
use serde_json::{Value, json};

#[path = "../support/blank_image.rs"]
mod blank_image;
#[path = "../support/scratch_apfs.rs"]
mod scratch_apfs;

use scratch_apfs::ScratchRoot;

const REFUSAL: &str = "no macOS workspace port block remains";

/// One adopted project on its own scratch APFS store.
struct Fixture {
    /// Owned so its drop detaches this test's images and removes its tree.
    _scratch: ScratchRoot,
    checkout: PathBuf,
    storage: ValidatedHostStorage,
    repo: RepoId,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let scratch = ScratchRoot::new(label).expect("scratch APFS root");
        let checkout = scratch.path().join("checkout");
        let store = scratch.path().join("store");
        for path in [&checkout, &store] {
            fs::create_dir_all(path).expect("fixture directory");
        }
        // Adoption mints main from the store's blank template: the run's, seeded here.
        blank_image::blank_image(&blank_template_path(&store, blank_image::CAPACITY));
        git(&checkout, &["init", "-q", "-b", "main"]);
        fs::write(checkout.join("tracked"), b"tracked\n").expect("tracked file");
        fs::write(
            checkout.join(".gitignore"),
            b".envrc\n.envrc-local\n.devenv/\n",
        )
        .expect("workspace hooks ignore");
        git(&checkout, &["add", "tracked", ".gitignore"]);
        git(&checkout, &["commit", "-q", "-m", "initial"]);
        let storage =
            ValidatedHostStorage::new(scratch.path().to_path_buf(), CanonicalRoots::at(store));
        Self {
            _scratch: scratch,
            checkout,
            storage,
            repo: RepoId::parse("fixture/intents").expect("repository identity"),
        }
    }

    async fn adopt(&self) -> ProjectRuntime {
        let runtime = ProjectRuntime::open_for_adopt_at(
            &self.checkout,
            Some(self.repo.clone()),
            self.storage.clone(),
        )
        .await
        .expect("open native project runtime on scratch APFS");
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
                },
            }),
        )
        .await
        .expect("real APFS adoption");
        runtime
    }

    /// What `cowshed gc` opens: store-wide recovery, which finishes every unfinished intent as
    /// residue.
    async fn gc(&self) -> ProjectRuntime {
        ProjectRuntime::open_existing_at(&self.checkout, RecoveryScope::Store, self.storage.clone())
            .await
            .expect("store-scope recovery opens the project")
    }

    /// What `cowshed new <workspace>` and other verbs naming it open: their own intents and
    /// main's are replayed, and a failed replay fails the opening.
    async fn open_naming(&self, workspace: &str) -> Result<ProjectRuntime> {
        ProjectRuntime::open_existing_at(
            &self.checkout,
            RecoveryScope::Workspaces([name(workspace)].into()),
            self.storage.clone(),
        )
        .await
    }

    /// What `cowshed rm <workspace>` opens.
    async fn open_removing(&self, workspace: &str) -> Result<ProjectRuntime> {
        ProjectRuntime::open_existing_at(
            &self.checkout,
            RecoveryScope::Removal(name(workspace)),
            self.storage.clone(),
        )
        .await
    }

    async fn remove(&self, runtime: &ProjectRuntime, workspace: &str) -> Result<Value> {
        self.call(
            runtime,
            "coordinator.destroy",
            json!({
                "repoId": self.repo,
                "workspace": workspace,
                "options": RemoveOptions::default(),
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
        Ok(response.into_parts().0)
    }

    async fn create(
        &self,
        runtime: &ProjectRuntime,
        workspace: &str,
        options: CreateOptions,
    ) -> Result<Value> {
        self.call(
            runtime,
            "coordinator.create",
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
            json!({ "repoId": self.repo, "source": source, "destination": destination }),
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
            PortBlock::macos_candidates()
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

    /// A stale `cowshed/<workspace>` branch in main: a fresh clone inherits it and its
    /// initializer refuses, after the `PendingFence` image exists.
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
                options: missing_revision(),
            });
            journal.mark_mutating(&name(workspace))
        })
        .expect("journal the unreplayable create");
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

/// A short commit id main does not hold, spelled as `cowshed new --ref` passes it.
const MISSING_REVISION: &str = "2f9cfc74";

fn missing_revision() -> CreateOptions {
    CreateOptions {
        revision: Some(RevisionTarget::parse_cli(MISSING_REVISION).expect("revision spelling")),
        ..CreateOptions::default()
    }
}

#[tokio::test]
async fn a_create_refused_for_want_of_a_port_block_leaves_nothing_for_gc_to_create() {
    let fixture = Fixture::new("refused-create");
    let runtime = fixture.adopt().await;
    let claims = fixture.claim_every_port_block();

    refused(
        fixture
            .create(&runtime, "refused", CreateOptions::default())
            .await
            .expect_err("every port block is claimed"),
    );
    assert_eq!(
        fixture.intent("refused"),
        None,
        "a refusal before the first mutation leaves no intent"
    );
    runtime.shutdown().await.expect("stop the refusing runtime");

    // With blocks free again, a replayed intent would now succeed: gc must find none.
    claims.release();
    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["main"]);
    assert_eq!(fixture.intent("refused"), None);
    gc.shutdown().await.expect("stop gc");
}

#[tokio::test]
async fn a_fork_refused_for_want_of_a_port_block_leaves_nothing_for_gc_to_create() {
    let fixture = Fixture::new("refused-fork");
    let runtime = fixture.adopt().await;
    let claims = fixture.claim_every_port_block();

    refused(
        fixture
            .fork(&runtime, "main", "refused")
            .await
            .expect_err("every port block is claimed"),
    );
    assert_eq!(fixture.intent("refused"), None);
    runtime.shutdown().await.expect("stop the refusing runtime");

    claims.release();
    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["main"]);
    assert_eq!(fixture.intent("refused"), None);
    gc.shutdown().await.expect("stop gc");
}

/// The residue the incident left: an older binary's create refused before anything existed,
/// journaled `Prepared` with no artifact. Store-scope recovery discards it instead of creating.
#[tokio::test]
async fn gc_discards_an_older_binarys_refused_create_instead_of_creating_it() {
    let fixture = Fixture::new("legacy-refusal");
    let runtime = fixture.adopt().await;
    runtime.shutdown().await.expect("stop the adopting runtime");
    LifecycleIntentJournal::update(&fixture.journal_path(), |journal| {
        journal.begin(LifecycleIntent::Create {
            workspace: name("refused"),
            options: CreateOptions::default(),
        });
        Ok(())
    })
    .expect("journal the refused create as an older binary did");

    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["main"]);
    assert_eq!(fixture.intent("refused"), None);
    gc.shutdown().await.expect("stop gc");
}

#[tokio::test]
async fn a_create_interrupted_after_its_pending_clone_exists_still_resumes() {
    let fixture = Fixture::new("mutating-create");
    let runtime = fixture.adopt().await;
    fixture.plant_workspace_branch("resumed");

    fixture
        .create(&runtime, "resumed", CreateOptions::default())
        .await
        .expect_err("the clone's initializer refuses the inherited workspace branch");
    let record = fixture
        .intent("resumed")
        .expect("the intent stays journaled");
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);
    assert_eq!(record.completion, None);
    assert_eq!(fixture.listed(&runtime).await, ["main"]);
    runtime
        .shutdown()
        .await
        .expect("stop the interrupted runtime");

    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["main", "resumed"]);
    assert!(completed(fixture.intent("resumed")));
    gc.shutdown().await.expect("stop gc");
}

/// Binaries before the `Mutating` mark journaled every create `Prepared`. Its pending clone,
/// not its phase, is what recovery decides on.
#[tokio::test]
async fn an_older_binarys_prepared_create_with_a_pending_clone_still_resumes() {
    let fixture = Fixture::new("legacy-create");
    let runtime = fixture.adopt().await;
    fixture.plant_workspace_branch("resumed");

    fixture
        .create(&runtime, "resumed", CreateOptions::default())
        .await
        .expect_err("the clone's initializer refuses the inherited workspace branch");
    runtime
        .shutdown()
        .await
        .expect("stop the interrupted runtime");
    fixture.strip_phase("resumed");
    assert_eq!(
        fixture.intent("resumed").expect("legacy intent").phase,
        LifecycleIntentPhase::Prepared
    );

    let gc = fixture.gc().await;
    assert_eq!(fixture.listed(&gc).await, ["main", "resumed"]);
    assert!(completed(fixture.intent("resumed")));
    gc.shutdown().await.expect("stop gc");
}

#[tokio::test]
async fn a_slot_bound_create_that_fails_after_binding_is_kept_and_resumed() {
    let fixture = Fixture::new("slot-create");
    let runtime = fixture.adopt().await;
    let claims = fixture.claim_every_port_block();
    let slotted = CreateOptions {
        slot: Some(3),
        ..CreateOptions::default()
    };

    refused(
        fixture
            .create(&runtime, "slotted", slotted)
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
        .create(
            &runtime,
            "intruder",
            CreateOptions {
                slot: Some(3),
                ..CreateOptions::default()
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

/// `cowshed new wedged --ref <commit main lacks>` is refused before it journals anything, so
/// the very next `new wedged` opens without replaying it and creates the workspace.
#[tokio::test]
async fn a_create_from_a_revision_the_source_lacks_leaves_the_name_free() {
    let fixture = Fixture::new("missing-revision");
    let runtime = fixture.adopt().await;

    let error = fixture
        .create(&runtime, "wedged", missing_revision())
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
    fixture
        .create(&next, "wedged", CreateOptions::default())
        .await
        .expect("the name is free for a create that can succeed");
    assert_eq!(fixture.listed(&next).await, ["main", "wedged"]);
    assert!(completed(fixture.intent("wedged")));
    next.shutdown().await.expect("stop the creating runtime");
}

/// The residue an unchecked `--ref` left: a create past its mutation fence whose pending
/// clone exists, and whose every replay fails. Here the clone stopped on a link it cannot
/// restore, and its record carries the missing revision as the incident's did. `rm` retires
/// the clone instead of replaying it, and the name is free again.
#[tokio::test]
async fn rm_retires_a_pending_clone_whose_create_can_never_be_replayed() {
    let fixture = Fixture::new("unreplayable-clone");
    let runtime = fixture.adopt().await;
    fixture.commit_escaping_link();
    fixture
        .create(&runtime, "wedged", CreateOptions::default())
        .await
        .expect_err("the clone's initializer refuses the escaping link");
    runtime
        .shutdown()
        .await
        .expect("stop the interrupted runtime");
    fixture.journal_unreplayable_create("wedged");

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
        .create(&next, "wedged", CreateOptions::default())
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
    fixture.journal_unreplayable_create("wedged");

    let rm = fixture
        .open_removing("wedged")
        .await
        .expect("`rm wedged` opens without replaying the create");
    fixture
        .remove(&rm, "wedged")
        .await
        .expect("rm retires the bare intent");
    assert!(matches!(
        fixture
            .intent("wedged")
            .and_then(|record| record.completion),
        Some(LifecycleIntentCompletion::Retire(_))
    ));
    rm.shutdown().await.expect("stop the removing runtime");

    let next = fixture
        .open_naming("wedged")
        .await
        .expect("`new wedged` opens: the retired create is not replayed");
    fixture
        .create(&next, "wedged", CreateOptions::default())
        .await
        .expect("the name is free again");
    assert_eq!(fixture.listed(&next).await, ["main", "wedged"]);
    next.shutdown().await.expect("stop the creating runtime");
}

/// The kernel claim on a new workspace's port block lasts until its image publication is
/// complete and no longer (05_gateway.md): from then on the image owns the block, and another
/// process's gateway reconcile installing the workspace's session must find no claim of the
/// verb's on it, or it moves the workspace to another block at a new grant revision.
#[tokio::test]
async fn a_created_workspaces_port_block_is_free_for_its_gateway_session_once_published() {
    let fixture = Fixture::new("published-create");
    let runtime = fixture.adopt().await;
    let gateway = fixture.gateway_at_hello("published");

    fixture
        .create(&runtime, "published", CreateOptions::default())
        .await
        .expect("create");
    assert_eq!(gateway.claimed().await, [0_u16; 0]);
    runtime.shutdown().await.expect("stop the runtime");
}

#[tokio::test]
async fn a_forked_workspaces_port_block_is_free_for_its_gateway_session_once_published() {
    let fixture = Fixture::new("published-fork");
    let runtime = fixture.adopt().await;
    let gateway = fixture.gateway_at_hello("published");

    fixture
        .fork(&runtime, "main", "published")
        .await
        .expect("fork");
    assert_eq!(gateway.claimed().await, [0_u16; 0]);
    runtime.shutdown().await.expect("stop the runtime");
}

#[tokio::test]
async fn an_adopted_mains_port_block_is_free_for_its_gateway_session_once_published() {
    let fixture = Fixture::new("published-adopt");
    let gateway = fixture.gateway_at_hello("main");

    let runtime = fixture.adopt().await;
    assert_eq!(gateway.claimed().await, [0_u16; 0]);
    runtime.shutdown().await.expect("stop the runtime");
}
