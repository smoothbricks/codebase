use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_core::apfs::{CommandRunner, DetachIntent, DiskImageSource, SystemCommandRunner};
use cowshed_core::api::dto::{AdoptOptions, CreateOptions};
use cowshed_core::api::server::ConnectionAuthority;
use cowshed_core::metadata::{PortBlock, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::storage::StorageLayout;
use cowshed_core::storage::apfs::ApfsSubstrateConfig;
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
use cowshed_core::storage::recovery::{
    LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentCompletion, LifecycleIntentJournal,
    LifecycleIntentPhase, LifecycleIntentRecord,
};
use cowshed_core::{CowshedError, Result};
use serde_json::{Value, json};

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
        let caches = scratch.path().join("caches");
        for path in [&checkout, &store, &caches] {
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
            CanonicalRoots::at(store, caches),
        );
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

    async fn call(&self, runtime: &ProjectRuntime, method: &str, params: Value) -> Result<Value> {
        let response = runtime
            .router()
            .route(
                ConnectionAuthority::Coordinator {
                    repo_id: self.repo.clone(),
                },
                method.to_owned(),
                params,
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
        .output()
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
