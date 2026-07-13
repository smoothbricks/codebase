use std::collections::BTreeSet;

use thiserror::Error;

use crate::metadata::WorkspaceIncarnation;

/// Objects in these namespaces are controller implementation details and never canonical listings.
pub const CHECKPOINT_NAMESPACE: &str = ".checkpoints";
pub const STAGING_NAMESPACE: &str = ".staging";
pub const TRASH_NAMESPACE: &str = ".trash";

/// The lifecycle mutations that share the publication and recovery protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionKind {
    Adopt,
    Create,
    Restore,
    Retire,
}

/// A durable transaction record. Each value is an immutable crash-recovery boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionPhase {
    Prepare,
    Prepared,
    Validated,
    IncarnationMinted,
    TokenMinted,
    StagedAuthoritySynced,
    CanonicalSwapped,
    CanonicalValidated,
    DetachedMetadataReplaced,
    Published,
    Admitted,
    CleanupPending,
    Complete,
    RolledBack,
}

impl TransactionPhase {
    /// Publication is the one-way incarnation fence. A recovery pass may never cross it backward.
    pub const fn recovery_disposition(self) -> RecoveryDisposition {
        match self {
            Self::Prepare
            | Self::Prepared
            | Self::Validated
            | Self::IncarnationMinted
            | Self::TokenMinted
            | Self::StagedAuthoritySynced
            | Self::CanonicalSwapped
            | Self::CanonicalValidated
            | Self::DetachedMetadataReplaced => RecoveryDisposition::RollBack,
            Self::Published | Self::Admitted | Self::CleanupPending => {
                RecoveryDisposition::RollForward
            }
            Self::Complete | Self::RolledBack => RecoveryDisposition::Settled,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryDisposition {
    RollBack,
    RollForward,
    Settled,
}

/// A gateway credential bound to exactly one workspace incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authority {
    incarnation: WorkspaceIncarnation,
    token: String,
}

impl Authority {
    fn mint(incarnation: WorkspaceIncarnation, transaction_id: &str) -> Self {
        Self {
            token: format!("gateway-{transaction_id}-{}", incarnation.as_str()),
            incarnation,
        }
    }

    pub fn incarnation(&self) -> &WorkspaceIncarnation {
        &self.incarnation
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ObjectNamespace {
    Canonical,
    Checkpoint,
    Staging,
    Trash,
}

/// A substrate object tagged with its visibility instead of inferring policy from a filename.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StoredObject {
    name: String,
    namespace: ObjectNamespace,
}

impl StoredObject {
    pub fn canonical(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: ObjectNamespace::Canonical,
        }
    }

    pub fn checkpoint(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: ObjectNamespace::Checkpoint,
        }
    }

    pub fn staging(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: ObjectNamespace::Staging,
        }
    }

    pub fn trash(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: ObjectNamespace::Trash,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn namespace(&self) -> ObjectNamespace {
        self.namespace
    }
}

/// Enumerate only canonical state. In-flight, retained, and cleanup objects are invisible.
pub fn enumerate_published(objects: impl IntoIterator<Item = StoredObject>) -> Vec<String> {
    objects
        .into_iter()
        .filter_map(|object| {
            (object.namespace == ObjectNamespace::Canonical).then_some(object.name)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Capability-free durable inputs for a lifecycle transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionSpec {
    kind: TransactionKind,
    logical_name: String,
    transaction_id: String,
    next_incarnation: Option<WorkspaceIncarnation>,
}

impl TransactionSpec {
    pub fn new(
        kind: TransactionKind,
        logical_name: impl Into<String>,
        transaction_id: impl Into<String>,
        next_incarnation: Option<WorkspaceIncarnation>,
    ) -> Result<Self, RecoveryError> {
        let logical_name = logical_name.into();
        let transaction_id = transaction_id.into();
        if logical_name.is_empty() {
            return Err(RecoveryError::EmptyLogicalName);
        }
        if transaction_id.is_empty() {
            return Err(RecoveryError::EmptyTransactionId);
        }
        match kind {
            TransactionKind::Adopt | TransactionKind::Create | TransactionKind::Restore => {
                if next_incarnation.is_none() {
                    return Err(RecoveryError::MissingNextIncarnation(kind));
                }
            }
            TransactionKind::Retire => {
                if next_incarnation.is_some() {
                    return Err(RecoveryError::UnexpectedNextIncarnation(kind));
                }
            }
        }
        Ok(Self {
            kind,
            logical_name,
            transaction_id,
            next_incarnation,
        })
    }

    pub const fn kind(&self) -> TransactionKind {
        self.kind
    }

    pub fn logical_name(&self) -> &str {
        &self.logical_name
    }

    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    pub fn next_incarnation(&self) -> Option<&WorkspaceIncarnation> {
        self.next_incarnation.as_ref()
    }

    pub fn staging_object(&self) -> Option<StoredObject> {
        self.next_incarnation.as_ref().map(|_| {
            StoredObject::staging(format!(
                "{STAGING_NAMESPACE}/{}-{}",
                self.logical_name, self.transaction_id
            ))
        })
    }

    /// Only a retire operation creates reclaimable trash.
    pub fn cleanup_object(&self) -> Option<StoredObject> {
        (self.kind == TransactionKind::Retire).then(|| {
            StoredObject::trash(format!(
                "{TRASH_NAMESPACE}/{}-{}",
                self.logical_name, self.transaction_id
            ))
        })
    }

    pub fn restore_checkpoint_object(&self) -> Option<StoredObject> {
        (self.kind == TransactionKind::Restore).then(|| {
            StoredObject::checkpoint(format!(
                "{CHECKPOINT_NAMESPACE}/{}-pre-restore-{}",
                self.logical_name, self.transaction_id
            ))
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointMetadata {
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormatMetadata {
    pub version: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopologyMetadata {
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionMetadata {
    pub retain_until_revision: u64,
}

/// Facts read from the authoritative lifecycle store while lifecycle exclusion is held.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativeObservations {
    pub incarnation: Option<WorkspaceIncarnation>,
    pub grant_revision: u64,
    pub checkpoint: Option<CheckpointMetadata>,
    pub format: FormatMetadata,
    pub topology: TopologyMetadata,
    pub retired: bool,
    pub retention: RetentionMetadata,
}

/// A capability-free plan checked by the lifecycle planner before storage execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedLifecyclePlan {
    spec: TransactionSpec,
    expected: AuthoritativeObservations,
}

impl CheckedLifecyclePlan {
    pub fn new(
        spec: TransactionSpec,
        expected: AuthoritativeObservations,
    ) -> Result<Self, RecoveryError> {
        if matches!(
            spec.kind,
            TransactionKind::Restore | TransactionKind::Retire
        ) && expected.incarnation.is_none()
        {
            return Err(RecoveryError::MissingCurrentIncarnation(spec.kind));
        }
        if expected.retired {
            return Err(RecoveryError::AlreadyRetired(spec.kind));
        }
        if spec.kind == TransactionKind::Restore && expected.checkpoint.is_none() {
            return Err(RecoveryError::MissingRestoreCheckpoint);
        }
        if let (Some(current), Some(next)) = (
            expected.incarnation.as_ref(),
            spec.next_incarnation.as_ref(),
        ) {
            if current == next {
                return Err(RecoveryError::ReusedIncarnation);
            }
        }
        Ok(Self { spec, expected })
    }

    pub fn spec(&self) -> &TransactionSpec {
        &self.spec
    }

    pub fn expected(&self) -> &AuthoritativeObservations {
        &self.expected
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StaleDimension {
    Incarnation,
    GrantRevision,
    Checkpoint,
    Format,
    Topology,
    Retirement,
}

/// A lifecycle conflict detected before the first storage effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryConflict {
    stale: Vec<StaleDimension>,
}

impl RecoveryConflict {
    pub fn stale_dimensions(&self) -> &[StaleDimension] {
        &self.stale
    }

    pub const fn effect_count(&self) -> usize {
        0
    }

    pub const fn phase(&self) -> Option<TransactionPhase> {
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeginOutcome {
    Started(RecoveryModel),
    Conflict(RecoveryConflict),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionOutcome {
    Interrupted(RecoveryModel),
    Conflict(RecoveryConflict),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Generation {
    Old,
    New,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataGeneration {
    Absent,
    Old,
    New,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedCheckpoint {
    object: StoredObject,
    displaced_incarnation: WorkspaceIncarnation,
    source_checkpoint: Option<CheckpointMetadata>,
    format: FormatMetadata,
    retention: RetentionMetadata,
}

impl RetainedCheckpoint {
    pub fn object(&self) -> &StoredObject {
        &self.object
    }

    pub fn displaced_incarnation(&self) -> &WorkspaceIncarnation {
        &self.displaced_incarnation
    }

    pub const fn source_checkpoint(&self) -> Option<CheckpointMetadata> {
        self.source_checkpoint
    }

    pub const fn format(&self) -> FormatMetadata {
        self.format
    }

    pub const fn retention(&self) -> RetentionMetadata {
        self.retention
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolEvent {
    PreparedAtHiddenName,
    StagedValidated,
    IncarnationMinted,
    TokenMinted,
    StagedAuthorityFlushedAndVerified,
    CanonicalSwap,
    PreRestoreCheckpointRetained,
    CanonicalValidated,
    AtomicMetadataReplace,
    MetadataParentFsync,
    AuthorityCutover {
        from: Option<Generation>,
        to: Option<Generation>,
    },
    NewAdmission,
    CleanupDeferred,
    RollbackBeforePublication,
    CleanupReclaimed,
}

/// Reusable crash fixture that stops immediately after a selected durable phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Failpoint {
    after: TransactionPhase,
}

impl Failpoint {
    pub const fn after(phase: TransactionPhase) -> Self {
        Self { after: phase }
    }

    pub const fn phase(self) -> TransactionPhase {
        self.after
    }

    pub fn interrupt(
        self,
        plan: CheckedLifecyclePlan,
        observed: AuthoritativeObservations,
    ) -> Result<ExecutionOutcome, RecoveryError> {
        let kind = plan.spec.kind;
        let mut model = match RecoveryModel::begin(plan, observed) {
            BeginOutcome::Started(model) => model,
            BeginOutcome::Conflict(conflict) => return Ok(ExecutionOutcome::Conflict(conflict)),
        };
        loop {
            if model.phase == self.after {
                return Ok(ExecutionOutcome::Interrupted(model));
            }
            if matches!(
                model.phase,
                TransactionPhase::Complete | TransactionPhase::RolledBack
            ) {
                return Err(RecoveryError::UnreachableFailpoint {
                    kind,
                    phase: self.after,
                });
            }
            model = model.advance()?;
        }
    }
}

/// Pure fake substrate used to execute and recover the shared protocol at every failpoint.
///
/// Methods consume `self`, so a completed phase record cannot be mutated behind a planner's back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryModel {
    plan: CheckedLifecyclePlan,
    authoritative: AuthoritativeObservations,
    phase: TransactionPhase,
    canonical: MetadataGeneration,
    metadata: MetadataGeneration,
    synced_metadata: MetadataGeneration,
    accepted_authority: Option<Generation>,
    admitted_authority: Option<Generation>,
    minted_authority: Option<Authority>,
    staging_present: bool,
    cleanup_present: bool,
    retained_checkpoint: Option<RetainedCheckpoint>,
    events: Vec<ProtocolEvent>,
}

impl RecoveryModel {
    /// Revalidate every authoritative lifecycle dimension before creating a Prepare record.
    pub fn begin(
        plan: CheckedLifecyclePlan,
        authoritative: AuthoritativeObservations,
    ) -> BeginOutcome {
        let stale = stale_dimensions(plan.expected(), &authoritative);
        if !stale.is_empty() {
            return BeginOutcome::Conflict(RecoveryConflict { stale });
        }
        let old = if authoritative.incarnation.is_some() {
            MetadataGeneration::Old
        } else {
            MetadataGeneration::Absent
        };
        let old_authority = authoritative.incarnation.as_ref().map(|_| Generation::Old);
        BeginOutcome::Started(Self {
            plan,
            authoritative,
            phase: TransactionPhase::Prepare,
            canonical: old,
            metadata: old,
            synced_metadata: old,
            accepted_authority: old_authority,
            admitted_authority: None,
            minted_authority: None,
            staging_present: false,
            cleanup_present: false,
            retained_checkpoint: None,
            events: Vec::new(),
        })
    }

    pub fn spec(&self) -> &TransactionSpec {
        self.plan.spec()
    }

    pub fn plan(&self) -> &CheckedLifecyclePlan {
        &self.plan
    }

    pub const fn phase(&self) -> TransactionPhase {
        self.phase
    }

    pub const fn canonical(&self) -> MetadataGeneration {
        self.canonical
    }

    pub const fn metadata(&self) -> MetadataGeneration {
        self.metadata
    }

    pub const fn synced_metadata(&self) -> MetadataGeneration {
        self.synced_metadata
    }

    pub const fn accepted_authority(&self) -> Option<Generation> {
        self.accepted_authority
    }

    pub const fn admitted_authority(&self) -> Option<Generation> {
        self.admitted_authority
    }

    pub fn minted_authority(&self) -> Option<&Authority> {
        self.minted_authority.as_ref()
    }

    pub const fn staging_present(&self) -> bool {
        self.staging_present
    }

    pub const fn cleanup_present(&self) -> bool {
        self.cleanup_present
    }

    pub fn retained_checkpoint(&self) -> Option<&RetainedCheckpoint> {
        self.retained_checkpoint.as_ref()
    }

    pub fn events(&self) -> &[ProtocolEvent] {
        &self.events
    }

    /// Execute exactly one durable phase while the backend holds its sole lifecycle exclusion.
    pub fn advance(mut self) -> Result<Self, RecoveryError> {
        self.phase = match self.phase {
            TransactionPhase::Prepare => {
                self.staging_present = self.spec().next_incarnation.is_some();
                self.events.push(ProtocolEvent::PreparedAtHiddenName);
                TransactionPhase::Prepared
            }
            TransactionPhase::Prepared => {
                self.events.push(ProtocolEvent::StagedValidated);
                TransactionPhase::Validated
            }
            TransactionPhase::Validated if self.spec().kind == TransactionKind::Retire => {
                self.swap_canonical();
                TransactionPhase::CanonicalSwapped
            }
            TransactionPhase::Validated => {
                self.events.push(ProtocolEvent::IncarnationMinted);
                TransactionPhase::IncarnationMinted
            }
            TransactionPhase::IncarnationMinted => {
                let authority = Authority::mint(
                    self.spec()
                        .next_incarnation
                        .clone()
                        .expect("replacement transactions have a next incarnation"),
                    &self.spec().transaction_id,
                );
                self.minted_authority = Some(authority);
                self.events.push(ProtocolEvent::TokenMinted);
                TransactionPhase::TokenMinted
            }
            TransactionPhase::TokenMinted => {
                self.events
                    .push(ProtocolEvent::StagedAuthorityFlushedAndVerified);
                TransactionPhase::StagedAuthoritySynced
            }
            TransactionPhase::StagedAuthoritySynced => {
                self.swap_canonical();
                TransactionPhase::CanonicalSwapped
            }
            TransactionPhase::CanonicalSwapped => {
                self.events.push(ProtocolEvent::CanonicalValidated);
                TransactionPhase::CanonicalValidated
            }
            TransactionPhase::CanonicalValidated => {
                self.metadata = self.target_generation();
                self.events.push(ProtocolEvent::AtomicMetadataReplace);
                TransactionPhase::DetachedMetadataReplaced
            }
            TransactionPhase::DetachedMetadataReplaced => {
                self.synced_metadata = self.metadata;
                self.events.push(ProtocolEvent::MetadataParentFsync);
                let from = self
                    .authoritative
                    .incarnation
                    .as_ref()
                    .map(|_| Generation::Old);
                let to = self
                    .spec()
                    .next_incarnation
                    .as_ref()
                    .map(|_| Generation::New);
                self.accepted_authority = to;
                self.events
                    .push(ProtocolEvent::AuthorityCutover { from, to });
                TransactionPhase::Published
            }
            TransactionPhase::Published if self.spec().next_incarnation.is_some() => {
                self.admitted_authority = Some(Generation::New);
                self.events.push(ProtocolEvent::NewAdmission);
                TransactionPhase::Admitted
            }
            TransactionPhase::Published | TransactionPhase::Admitted if self.cleanup_present => {
                self.events.push(ProtocolEvent::CleanupDeferred);
                TransactionPhase::CleanupPending
            }
            TransactionPhase::Published | TransactionPhase::Admitted => TransactionPhase::Complete,
            TransactionPhase::CleanupPending => {
                self.reclaim_cleanup();
                TransactionPhase::Complete
            }
            TransactionPhase::Complete | TransactionPhase::RolledBack => {
                return Err(RecoveryError::TransactionSettled(self.phase));
            }
        };
        Ok(self)
    }

    /// Recover authoritative state according only to the durable publication fence.
    pub fn recover(mut self) -> Self {
        match self.phase.recovery_disposition() {
            RecoveryDisposition::RollBack => {
                let old = if self.authoritative.incarnation.is_some() {
                    MetadataGeneration::Old
                } else {
                    MetadataGeneration::Absent
                };
                self.canonical = old;
                self.metadata = old;
                self.synced_metadata = old;
                self.accepted_authority = self
                    .authoritative
                    .incarnation
                    .as_ref()
                    .map(|_| Generation::Old);
                self.admitted_authority = self.accepted_authority;
                self.minted_authority = None;
                self.staging_present = false;
                self.cleanup_present = false;
                self.retained_checkpoint = None;
                self.events.push(ProtocolEvent::RollbackBeforePublication);
                self.phase = TransactionPhase::RolledBack;
            }
            RecoveryDisposition::RollForward => {
                let target = self.target_generation();
                self.canonical = target;
                self.metadata = target;
                self.synced_metadata = target;
                self.accepted_authority = self
                    .spec()
                    .next_incarnation
                    .as_ref()
                    .map(|_| Generation::New);
                self.admitted_authority = self.accepted_authority;
                self.staging_present = false;
                self.phase = if self.cleanup_present {
                    TransactionPhase::CleanupPending
                } else {
                    TransactionPhase::Complete
                };
            }
            RecoveryDisposition::Settled => {}
        }
        self
    }

    /// GC only reclaims retire debris; it never reclaims retained restore checkpoints.
    pub fn gc_pass(mut self) -> (Self, GcPass) {
        if self.phase != TransactionPhase::CleanupPending {
            return (self, GcPass::default());
        }
        let examined = usize::from(self.cleanup_present);
        let reclaimed = examined;
        self.reclaim_cleanup();
        self.phase = TransactionPhase::Complete;
        (
            self,
            GcPass {
                examined,
                reclaimed,
            },
        )
    }

    fn target_generation(&self) -> MetadataGeneration {
        if self.spec().next_incarnation.is_some() {
            MetadataGeneration::New
        } else {
            MetadataGeneration::Absent
        }
    }

    fn swap_canonical(&mut self) {
        self.canonical = self.target_generation();
        self.staging_present = false;
        self.cleanup_present = self.spec().kind == TransactionKind::Retire;
        if self.spec().kind == TransactionKind::Restore {
            self.retained_checkpoint = Some(RetainedCheckpoint {
                object: self
                    .spec()
                    .restore_checkpoint_object()
                    .expect("restore transactions retain an undo checkpoint"),
                displaced_incarnation: self
                    .authoritative
                    .incarnation
                    .clone()
                    .expect("checked restore plans have a current incarnation"),
                source_checkpoint: self.authoritative.checkpoint,
                format: self.authoritative.format,
                retention: self.authoritative.retention,
            });
            self.events
                .push(ProtocolEvent::PreRestoreCheckpointRetained);
        }
        self.events.push(ProtocolEvent::CanonicalSwap);
    }

    fn reclaim_cleanup(&mut self) {
        if self.cleanup_present {
            self.cleanup_present = false;
            self.events.push(ProtocolEvent::CleanupReclaimed);
        }
    }
}

fn stale_dimensions(
    expected: &AuthoritativeObservations,
    authoritative: &AuthoritativeObservations,
) -> Vec<StaleDimension> {
    let mut stale = Vec::new();
    if expected.incarnation != authoritative.incarnation {
        stale.push(StaleDimension::Incarnation);
    }
    if expected.grant_revision != authoritative.grant_revision {
        stale.push(StaleDimension::GrantRevision);
    }
    if expected.checkpoint != authoritative.checkpoint {
        stale.push(StaleDimension::Checkpoint);
    }
    if expected.format != authoritative.format {
        stale.push(StaleDimension::Format);
    }
    if expected.topology != authoritative.topology {
        stale.push(StaleDimension::Topology);
    }
    if expected.retired != authoritative.retired {
        stale.push(StaleDimension::Retirement);
    }
    stale
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcPass {
    pub examined: usize,
    pub reclaimed: usize,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RecoveryError {
    #[error("logical workspace name must not be empty")]
    EmptyLogicalName,
    #[error("transaction id must not be empty")]
    EmptyTransactionId,
    #[error("{0:?} requires a next incarnation")]
    MissingNextIncarnation(TransactionKind),
    #[error("{0:?} does not accept a next incarnation")]
    UnexpectedNextIncarnation(TransactionKind),
    #[error("{0:?} requires a current incarnation")]
    MissingCurrentIncarnation(TransactionKind),
    #[error("{0:?} cannot start from an already retired workspace")]
    AlreadyRetired(TransactionKind),
    #[error("Restore requires a concrete checkpoint identity")]
    MissingRestoreCheckpoint,
    #[error("replacement must mint a fresh workspace incarnation")]
    ReusedIncarnation,
    #[error("{phase:?} is not reachable for {kind:?}")]
    UnreachableFailpoint {
        kind: TransactionKind,
        phase: TransactionPhase,
    },
    #[error("transaction is already settled at {0:?}")]
    TransactionSettled(TransactionPhase),
}
