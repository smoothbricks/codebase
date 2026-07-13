use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::ThreadId;

use async_trait::async_trait;
use cowshed_core::metadata::{
    GrantSet, ImageFormat, PortBlock, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::CheckpointLabel;
use cowshed_core::storage::lifecycle::*;
use proptest::prelude::*;

fn repo() -> RepoId {
    RepoId::parse("acme/project").unwrap()
}
fn make_incarnation(digit: char) -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(digit.to_string().repeat(32)).unwrap()
}
fn identity() -> OperationIdentity {
    OperationIdentity {
        project_root: PathBuf::from("/project"),
        base_commit: "0123456789abcdef".to_owned(),
        created_at: "2026-07-13T00:00:00Z".to_owned(),
        created_trace: "lifecycle-contract".to_owned(),
        grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
            .expect("grants"),
    }
}
fn workspace(name: &str, revision: u64, topology: u64) -> LifecycleWorkspace {
    LifecycleWorkspace::new(
        repo(),
        WorkspaceName::new(name).unwrap(),
        make_incarnation('a'),
        Revision::new(revision),
        Revision::new(topology),
        if name == "main" {
            WorkspaceRole::Main
        } else {
            WorkspaceRole::Workspace
        },
        ImageFormat::Asif,
    )
    .unwrap()
}
fn observed(expected: &ExpectedState) -> ObservedState {
    match expected {
        ExpectedState::Exists {
            repo,
            name,
            incarnation,
            revision,
            topology_revision,
            retired,
        } => ObservedState::Exists {
            repo: repo.clone(),
            name: name.clone(),
            incarnation: incarnation.clone(),
            revision: *revision,
            topology_revision: *topology_revision,
            retired: *retired,
        },
        ExpectedState::Absent {
            repo,
            name,
            topology_revision,
        } => ObservedState::Absent {
            repo: repo.clone(),
            name: name.clone(),
            topology_revision: *topology_revision,
        },
        ExpectedState::Checkpoint {
            repo,
            workspace,
            label,
            revision,
        } => ObservedState::Checkpoint {
            repo: repo.clone(),
            workspace: workspace.clone(),
            label: label.clone(),
            revision: *revision,
        },
    }
}

#[test]
fn planning_is_deterministic_capability_free_and_has_no_effects() {
    let planner = PurePlanner;
    let source = workspace("main", 7, 11);
    let destination = Destination {
        repo: repo(),
        name: WorkspaceName::session("topic").unwrap(),
        topology_revision: Revision::new(11),
        identity: identity(),
    };
    let first = planner.plan_create(&source, destination.clone()).unwrap();
    let second = planner.plan_create(&source, destination).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.expected().len(), 2);
    let representation = format!("{first:?}");
    assert!(!representation.contains("/Users/") && !representation.contains("/tmp/"));
    assert!(!representation.contains("hdiutil"));
    assert!(!representation.contains("zfs"));
}

struct SpyBackend {
    actual: Vec<ObservedState>,
    acquired: AtomicUsize,
    reread: AtomicUsize,
    command_effects: AtomicUsize,
    filesystem_effects: AtomicUsize,
}
struct SpyGuard {
    reread_complete: bool,
}

#[async_trait]
impl LifecycleBackend for SpyBackend {
    type Guard = SpyGuard;
    type Output = ();
    type Error = &'static str;

    async fn acquire(&self, _: &Operation) -> Result<Self::Guard, Self::Error> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        Ok(SpyGuard {
            reread_complete: false,
        })
    }
    async fn read_authoritative(
        &self,
        guard: &mut Self::Guard,
        _: &[ExpectedState],
    ) -> Result<Vec<ObservedState>, Self::Error> {
        assert_eq!(self.acquired.load(Ordering::SeqCst), 1);
        self.reread.fetch_add(1, Ordering::SeqCst);
        guard.reread_complete = true;
        Ok(self.actual.clone())
    }
    async fn apply(
        &self,
        guard: &mut Self::Guard,
        _: &Operation,
    ) -> Result<Self::Output, Self::Error> {
        assert!(guard.reread_complete);
        self.command_effects.fetch_add(1, Ordering::SeqCst);
        self.filesystem_effects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn backend(actual: Vec<ObservedState>) -> SpyBackend {
    SpyBackend {
        actual,
        acquired: AtomicUsize::new(0),
        reread: AtomicUsize::new(0),
        command_effects: AtomicUsize::new(0),
        filesystem_effects: AtomicUsize::new(0),
    }
}

struct ContractSubstrate {
    workspace: LifecycleWorkspace,
}

impl LifecyclePlanner for ContractSubstrate {
    fn plan_adopt(&self, request: AdoptRequest) -> Result<AdoptPlan, PlanError> {
        PurePlanner.plan_adopt(request)
    }

    fn plan_create(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<CreatePlan, PlanError> {
        PurePlanner.plan_create(from, destination)
    }

    fn plan_fork(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<ForkPlan, PlanError> {
        PurePlanner.plan_fork(from, destination)
    }

    fn plan_checkpoint(
        &self,
        workspace: &LifecycleWorkspace,
        label: CheckpointLabel,
        pin: Pin,
    ) -> Result<CheckpointPlan, PlanError> {
        PurePlanner.plan_checkpoint(workspace, label, pin)
    }

    fn plan_restore(
        &self,
        workspace: &LifecycleWorkspace,
        checkpoint: &CheckpointRef,
        mode: RestoreMode,
        identity: OperationIdentity,
    ) -> Result<RestorePlan, PlanError> {
        PurePlanner.plan_restore(workspace, checkpoint, mode, identity)
    }

    fn plan_retire(&self, workspace: &LifecycleWorkspace) -> Result<RetirePlan, PlanError> {
        PurePlanner.plan_retire(workspace)
    }
}

#[async_trait]
impl Substrate for ContractSubstrate {
    type Error = &'static str;

    async fn execute_adopt(&self, _: AdoptPlan) -> Result<LifecycleReceipt, Self::Error> {
        Err("not exercised")
    }

    async fn execute_create(&self, _: CreatePlan) -> Result<LifecycleReceipt, Self::Error> {
        Err("not exercised")
    }

    async fn execute_checkpoint(&self, _: CheckpointPlan) -> Result<CheckpointRef, Self::Error> {
        Err("not exercised")
    }

    async fn execute_restore(&self, _: RestorePlan) -> Result<RestoreReceipt, Self::Error> {
        Err("not exercised")
    }

    async fn execute_fork(&self, _: ForkPlan) -> Result<LifecycleReceipt, Self::Error> {
        Err("not exercised")
    }

    async fn execute_retire(&self, plan: RetirePlan) -> Result<RetiredRef, Self::Error> {
        match plan.operation() {
            Operation::Retire { workspace, .. } if workspace == self.workspace.name() => {
                Ok(RetiredRef::new(
                    self.workspace.clone(),
                    Revision::new(self.workspace.revision().get() + 1),
                ))
            }
            _ => Err("wrong retirement plan"),
        }
    }

    async fn reclaim(&self, retired: RetiredRef) -> Result<(), Self::Error> {
        if retired.workspace() == &self.workspace
            && retired.resulting_revision().get() == self.workspace.revision().get() + 1
        {
            Ok(())
        } else {
            Err("wrong retired identity")
        }
    }

    async fn list(&self, repo: &RepoId) -> Result<Vec<DerivedWorkspace>, Self::Error> {
        if repo == self.workspace.repo() {
            Ok(vec![DerivedWorkspace {
                workspace: self.workspace.clone(),
                mount_state: MountState::Mounted { mount_id: 77 },
                checkpoints: Vec::new(),
            }])
        } else {
            Err("wrong repository")
        }
    }

    async fn mount_state(&self, workspace: &LifecycleWorkspace) -> Result<MountState, Self::Error> {
        if workspace == &self.workspace {
            Ok(MountState::Mounted { mount_id: 77 })
        } else {
            Err("wrong workspace")
        }
    }

    async fn ensure_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        _: MountIntent,
    ) -> Result<PathBuf, Self::Error> {
        if workspace == &self.workspace {
            Ok(PathBuf::from("/canonical").join(workspace.name().as_str()))
        } else {
            Err("wrong workspace")
        }
    }

    async fn unmount(&self, workspace: &LifecycleWorkspace) -> Result<(), Self::Error> {
        if workspace == &self.workspace {
            Ok(())
        } else {
            Err("wrong workspace")
        }
    }

    async fn caches_root(&self) -> Result<PathBuf, Self::Error> {
        Ok(PathBuf::from("/canonical/caches"))
    }

    async fn stats(&self, workspace: &LifecycleWorkspace) -> Result<SubstrateStats, Self::Error> {
        if workspace == &self.workspace {
            Ok(SubstrateStats {
                logical_bytes: workspace.revision().get(),
                allocated_bytes: workspace.topology_revision().get(),
                checkpoint_count: 2,
            })
        } else {
            Err("wrong workspace")
        }
    }

    async fn gc(&self) -> Result<StorageGcReport, Self::Error> {
        Ok(StorageGcReport::default())
    }
}

#[tokio::test]
async fn finalized_substrate_surface_returns_direct_values_and_canonical_paths() {
    let ws = workspace("topic", 3, 5);
    let substrate = ContractSubstrate {
        workspace: ws.clone(),
    };

    assert_eq!(
        substrate.list(&repo()).await.unwrap()[0].workspace,
        ws.clone()
    );
    assert_eq!(
        substrate.mount_state(&ws).await.unwrap(),
        MountState::Mounted { mount_id: 77 }
    );
    assert_eq!(
        substrate
            .ensure_mounted(&ws, MountIntent { browse: true })
            .await
            .unwrap(),
        PathBuf::from("/canonical/topic")
    );
    assert_eq!(
        substrate.caches_root().await.unwrap(),
        PathBuf::from("/canonical/caches")
    );
    assert_eq!(
        substrate.stats(&ws).await.unwrap(),
        SubstrateStats {
            logical_bytes: 3,
            allocated_bytes: 5,
            checkpoint_count: 2,
        }
    );

    let retired = substrate
        .execute_retire(substrate.plan_retire(&ws).unwrap())
        .await
        .unwrap();
    assert_eq!(retired.workspace(), &ws);
    assert_eq!(retired.resulting_revision(), Revision::new(4));
    assert_eq!(substrate.reclaim(retired).await, Ok(()));
}

#[test]
fn value_accessors_and_every_fact_field_are_observable() {
    assert_eq!(Revision::new(42).get(), 42);
    let ws = workspace("topic", 3, 5);
    let checkpoint = CheckpointRef::new(
        ws.clone(),
        CheckpointLabel::new("safe").unwrap(),
        Revision::new(8),
        true,
    );
    assert!(checkpoint.pinned());
    assert!(
        !CheckpointRef::new(
            ws.clone(),
            CheckpointLabel::new("automatic").unwrap(),
            Revision::new(9),
            false,
        )
        .pinned()
    );

    let destination = Destination {
        repo: repo(),
        name: WorkspaceName::session("destination").unwrap(),
        topology_revision: Revision::new(5),
        identity: identity(),
    };
    let create = PurePlanner.plan_create(&ws, destination).unwrap();
    let create_actual: Vec<_> = create.expected().iter().map(observed).collect();
    assert_eq!(revalidate(create.expected(), &create_actual), Ok(()));

    let restore = PurePlanner
        .plan_restore(&ws, &checkpoint, RestoreMode::Replace, identity())
        .unwrap();
    let restore_actual: Vec<_> = restore.expected().iter().map(observed).collect();
    assert_eq!(revalidate(restore.expected(), &restore_actual), Ok(()));

    for actual in [
        ObservedState::Checkpoint {
            repo: RepoId::parse("other/project").unwrap(),
            workspace: ws.name().clone(),
            label: checkpoint.label().clone(),
            revision: checkpoint.revision(),
        },
        ObservedState::Checkpoint {
            repo: repo(),
            workspace: WorkspaceName::session("other").unwrap(),
            label: checkpoint.label().clone(),
            revision: checkpoint.revision(),
        },
        ObservedState::Checkpoint {
            repo: repo(),
            workspace: ws.name().clone(),
            label: CheckpointLabel::new("other").unwrap(),
            revision: checkpoint.revision(),
        },
    ] {
        assert!(revalidate(&restore.expected()[1..], &[actual]).is_err());
    }

    let absent = &create.expected()[1..];
    for actual in [
        ObservedState::Absent {
            repo: RepoId::parse("other/project").unwrap(),
            name: WorkspaceName::session("destination").unwrap(),
            topology_revision: Revision::new(5),
        },
        ObservedState::Absent {
            repo: repo(),
            name: WorkspaceName::session("other").unwrap(),
            topology_revision: Revision::new(5),
        },
        ObservedState::Absent {
            repo: repo(),
            name: WorkspaceName::session("destination").unwrap(),
            topology_revision: Revision::new(6),
        },
    ] {
        assert!(revalidate(absent, &[actual]).is_err());
    }
}

#[test]
fn restore_rejects_each_checkpoint_identity_mismatch() {
    let ws = workspace("topic", 3, 5);
    let variants = [
        LifecycleWorkspace::new(
            RepoId::parse("other/project").unwrap(),
            ws.name().clone(),
            ws.incarnation().clone(),
            ws.revision(),
            ws.topology_revision(),
            ws.role(),
            ws.format(),
        )
        .unwrap(),
        workspace("other", 3, 5),
        LifecycleWorkspace::new(
            repo(),
            ws.name().clone(),
            make_incarnation('b'),
            ws.revision(),
            ws.topology_revision(),
            ws.role(),
            ws.format(),
        )
        .unwrap(),
    ];
    for checkpoint_workspace in variants {
        let checkpoint = CheckpointRef::new(
            checkpoint_workspace,
            CheckpointLabel::new("safe").unwrap(),
            Revision::new(8),
            false,
        );
        assert_eq!(
            PurePlanner.plan_restore(&ws, &checkpoint, RestoreMode::Replace, identity()),
            Err(PlanError::CheckpointMismatch)
        );
    }
}

#[tokio::test]
async fn execute_acquires_rereads_revalidates_then_mutates() {
    let plan = PurePlanner.plan_retire(&workspace("topic", 3, 5)).unwrap();
    let spy = backend(plan.expected().iter().map(observed).collect());
    execute_checked(&spy, &plan).await.unwrap();
    assert_eq!(spy.acquired.load(Ordering::SeqCst), 1);
    assert_eq!(spy.reread.load(Ordering::SeqCst), 1);
    assert_eq!(spy.command_effects.load(Ordering::SeqCst), 1);
    assert_eq!(spy.filesystem_effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn every_stale_precondition_conflicts_with_zero_effects() {
    let ws = workspace("topic", 3, 5);
    let checkpoint = CheckpointRef::new(
        ws.clone(),
        CheckpointLabel::new("safe").unwrap(),
        Revision::new(8),
        false,
    );
    let plan = PurePlanner
        .plan_restore(&ws, &checkpoint, RestoreMode::Replace, identity())
        .unwrap();
    let canonical: Vec<_> = plan.expected().iter().map(observed).collect();
    let mut stale_matrix = Vec::new();

    let mut stale = canonical.clone();
    if let ObservedState::Exists { incarnation, .. } = &mut stale[0] {
        *incarnation = make_incarnation('b');
    }
    stale_matrix.push(stale);
    let mut stale = canonical.clone();
    if let ObservedState::Exists { revision, .. } = &mut stale[0] {
        *revision = Revision::new(4);
    }
    stale_matrix.push(stale);
    let mut stale = canonical.clone();
    if let ObservedState::Exists {
        topology_revision, ..
    } = &mut stale[0]
    {
        *topology_revision = Revision::new(6);
    }
    stale_matrix.push(stale);
    let mut stale = canonical.clone();
    if let ObservedState::Exists { retired, .. } = &mut stale[0] {
        *retired = true;
    }
    stale_matrix.push(stale);
    let mut stale = canonical;
    if let ObservedState::Checkpoint { revision, .. } = &mut stale[1] {
        *revision = Revision::new(9);
    }
    stale_matrix.push(stale);

    for actual in stale_matrix {
        let spy = backend(actual);
        assert!(matches!(
            execute_checked(&spy, &plan).await,
            Err(ExecuteError::Conflict(Conflict::Stale { .. }))
        ));
        assert_eq!(spy.command_effects.load(Ordering::SeqCst), 0);
        assert_eq!(spy.filesystem_effects.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_work_is_dispatched_off_the_async_worker() {
    let runtime_worker: ThreadId = std::thread::current().id();
    let blocking_worker = dispatch_blocking(|| std::thread::current().id())
        .await
        .unwrap();
    assert_ne!(runtime_worker, blocking_worker);
}

#[test]
fn duplicate_canonical_volume_names_are_rejected_before_mount_joining() {
    let storage = || {
        vec![
            StorageFact {
                workspace: workspace("alpha", 0, 0),
                volume_name: "shared-volume".to_owned(),
            },
            StorageFact {
                workspace: workspace("beta", 0, 0),
                volume_name: "shared-volume".to_owned(),
            },
        ]
    };
    let mount_tables = [
        vec![],
        vec![KernelMountFact {
            mount_id: 41,
            volume_name: "shared-volume".to_owned(),
        }],
        vec![KernelMountFact {
            mount_id: 42,
            volume_name: "unrelated-volume".to_owned(),
        }],
    ];

    for mounts in mount_tables {
        assert_eq!(
            derive_workspaces(storage(), mounts, Vec::new()),
            Err(DerivationError::DuplicateVolumeName(
                "shared-volume".to_owned()
            ))
        );
    }
}

proptest! {
    #[test]
    fn generated_plans_are_deterministic(revision in any::<u64>(), topology in any::<u64>(), suffix in 0u16..1000) {
        let source = workspace("main", revision, topology);
        let destination = Destination { repo: repo(), name: WorkspaceName::session(format!("topic-{suffix}")).unwrap(), topology_revision: Revision::new(topology), identity: identity() };
        prop_assert_eq!(PurePlanner.plan_fork(&source, destination.clone()).unwrap(), PurePlanner.plan_fork(&source, destination).unwrap());
    }

    #[test]
    fn derived_enumeration_uses_only_storage_and_kernel_facts(mounted in proptest::collection::btree_set(0u8..20, 0..20), count in 0u8..20) {
        let storage: Vec<_> = (0..count).map(|index| StorageFact { workspace: workspace(&format!("ws-{index}"), 0, 0), volume_name: format!("volume-{index}") }).collect();
        let mounts: Vec<_> = mounted.iter().map(|index| KernelMountFact { mount_id: u64::from(*index) + 100, volume_name: format!("volume-{index}") }).collect();
        let derived = derive_workspaces(storage, mounts, Vec::new()).unwrap();
        prop_assert_eq!(derived.len(), usize::from(count));
        for item in derived {
            let index: u8 = item.workspace.name().as_str().strip_prefix("ws-").unwrap().parse().unwrap();
            let expected = if mounted.contains(&index) { MountState::Mounted { mount_id: u64::from(index) + 100 } } else { MountState::Detached };
            prop_assert_eq!(item.mount_state, expected);
        }
    }

    #[test]
    fn generated_ambiguous_volume_names_are_rejected(
        left in 0u16..1000,
        right in 0u16..1000,
        volume in any::<u64>(),
        mount_id in any::<u64>(),
    ) {
        prop_assume!(left != right);
        let volume_name = format!("volume-{volume}");
        let storage = vec![
            StorageFact {
                workspace: workspace(&format!("left-{left}"), 0, 0),
                volume_name: volume_name.clone(),
            },
            StorageFact {
                workspace: workspace(&format!("right-{right}"), 0, 0),
                volume_name: volume_name.clone(),
            },
        ];
        let mounts = vec![KernelMountFact {
            mount_id,
            volume_name: volume_name.clone(),
        }];

        prop_assert_eq!(
            derive_workspaces(storage, mounts, Vec::new()),
            Err(DerivationError::DuplicateVolumeName(volume_name))
        );
    }
}
