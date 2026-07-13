use std::collections::BTreeSet;

use thiserror::Error;

use crate::metadata::WorkspaceIncarnation;

/// Objects in these namespaces are controller implementation details and never canonical listings.
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
    pub fn new(
        incarnation: WorkspaceIncarnation,
        token: impl Into<String>,
    ) -> Result<Self, RecoveryError> {
        let token = token.into();
        if token.is_empty() {
            return Err(RecoveryError::EmptyToken);
        }
        Ok(Self { incarnation, token })
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

/// Enumerate only canonical state. In-flight and cleanup objects are invisible by construction.
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

/// Immutable inputs retained by every recoverable phase record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionSpec {
    kind: TransactionKind,
    logical_name: String,
    transaction_id: String,
    old_authority: Option<Authority>,
    new_authority: Option<Authority>,
}

impl TransactionSpec {
    pub fn new(
        kind: TransactionKind,
        logical_name: impl Into<String>,
        transaction_id: impl Into<String>,
        old_authority: Option<Authority>,
        new_authority: Option<Authority>,
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
            TransactionKind::Adopt | TransactionKind::Create => {
                if old_authority.is_some() {
                    return Err(RecoveryError::UnexpectedOldAuthority(kind));
                }
                if new_authority.is_none() {
                    return Err(RecoveryError::MissingNewAuthority(kind));
                }
            }
            TransactionKind::Restore => {
                if old_authority.is_none() {
                    return Err(RecoveryError::MissingOldAuthority(kind));
                }
                if new_authority.is_none() {
                    return Err(RecoveryError::MissingNewAuthority(kind));
                }
            }
            TransactionKind::Retire => {
                if old_authority.is_none() {
                    return Err(RecoveryError::MissingOldAuthority(kind));
                }
                if new_authority.is_some() {
                    return Err(RecoveryError::UnexpectedNewAuthority(kind));
                }
            }
        }
        if let (Some(old), Some(new)) = (&old_authority, &new_authority) {
            if old.incarnation == new.incarnation {
                return Err(RecoveryError::ReusedIncarnation);
            }
            if old.token == new.token {
                return Err(RecoveryError::ReusedToken);
            }
        }
        Ok(Self {
            kind,
            logical_name,
            transaction_id,
            old_authority,
            new_authority,
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

    pub fn old_authority(&self) -> Option<&Authority> {
        self.old_authority.as_ref()
    }

    pub fn new_authority(&self) -> Option<&Authority> {
        self.new_authority.as_ref()
    }

    pub fn staging_object(&self) -> Option<StoredObject> {
        self.new_authority.as_ref().map(|_| {
            StoredObject::staging(format!(
                "{}/{}-{}",
                STAGING_NAMESPACE, self.logical_name, self.transaction_id
            ))
        })
    }

    pub fn cleanup_object(&self) -> Option<StoredObject> {
        self.old_authority.as_ref().map(|_| {
            StoredObject::trash(format!(
                "{}/{}-{}",
                TRASH_NAMESPACE, self.logical_name, self.transaction_id
            ))
        })
    }
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
pub enum ProtocolEvent {
    PreparedAtHiddenName,
    StagedValidated,
    IncarnationMinted,
    TokenMinted,
    StagedAuthorityFlushedAndVerified,
    CanonicalSwap,
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

    pub fn interrupt(self, spec: TransactionSpec) -> Result<RecoveryModel, RecoveryError> {
        let kind = spec.kind;
        let mut model = RecoveryModel::begin(spec);
        loop {
            if model.phase == self.after {
                return Ok(model);
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
    spec: TransactionSpec,
    phase: TransactionPhase,
    canonical: MetadataGeneration,
    metadata: MetadataGeneration,
    synced_metadata: MetadataGeneration,
    accepted_authority: Option<Generation>,
    admitted_authority: Option<Generation>,
    staging_present: bool,
    cleanup_present: bool,
    events: Vec<ProtocolEvent>,
}

impl RecoveryModel {
    pub fn begin(spec: TransactionSpec) -> Self {
        let old = if spec.old_authority.is_some() {
            MetadataGeneration::Old
        } else {
            MetadataGeneration::Absent
        };
        let old_authority = spec.old_authority.as_ref().map(|_| Generation::Old);
        Self {
            spec,
            phase: TransactionPhase::Prepare,
            canonical: old,
            metadata: old,
            synced_metadata: old,
            accepted_authority: old_authority,
            admitted_authority: None,
            staging_present: false,
            cleanup_present: false,
            events: Vec::new(),
        }
    }

    pub fn spec(&self) -> &TransactionSpec {
        &self.spec
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

    pub const fn staging_present(&self) -> bool {
        self.staging_present
    }

    pub const fn cleanup_present(&self) -> bool {
        self.cleanup_present
    }

    pub fn events(&self) -> &[ProtocolEvent] {
        &self.events
    }

    /// Execute exactly one durable phase while the backend holds its sole lifecycle exclusion.
    pub fn advance(mut self) -> Result<Self, RecoveryError> {
        self.phase = match self.phase {
            TransactionPhase::Prepare => {
                self.staging_present = self.spec.new_authority.is_some();
                self.events.push(ProtocolEvent::PreparedAtHiddenName);
                TransactionPhase::Prepared
            }
            TransactionPhase::Prepared => {
                self.events.push(ProtocolEvent::StagedValidated);
                TransactionPhase::Validated
            }
            TransactionPhase::Validated if self.spec.kind == TransactionKind::Retire => {
                self.swap_canonical();
                TransactionPhase::CanonicalSwapped
            }
            TransactionPhase::Validated => {
                self.events.push(ProtocolEvent::IncarnationMinted);
                TransactionPhase::IncarnationMinted
            }
            TransactionPhase::IncarnationMinted => {
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
                let from = self.spec.old_authority.as_ref().map(|_| Generation::Old);
                let to = self.spec.new_authority.as_ref().map(|_| Generation::New);
                self.accepted_authority = to;
                self.events
                    .push(ProtocolEvent::AuthorityCutover { from, to });
                TransactionPhase::Published
            }
            TransactionPhase::Published if self.spec.new_authority.is_some() => {
                self.admitted_authority = Some(Generation::New);
                self.events.push(ProtocolEvent::NewAdmission);
                TransactionPhase::Admitted
            }
            TransactionPhase::Published | TransactionPhase::Admitted => {
                self.events.push(ProtocolEvent::CleanupDeferred);
                TransactionPhase::CleanupPending
            }
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
                let old = if self.spec.old_authority.is_some() {
                    MetadataGeneration::Old
                } else {
                    MetadataGeneration::Absent
                };
                self.canonical = old;
                self.metadata = old;
                self.synced_metadata = old;
                self.accepted_authority = self.spec.old_authority.as_ref().map(|_| Generation::Old);
                self.admitted_authority = self.accepted_authority;
                self.staging_present = false;
                self.cleanup_present = false;
                self.events.push(ProtocolEvent::RollbackBeforePublication);
                self.phase = TransactionPhase::RolledBack;
            }
            RecoveryDisposition::RollForward => {
                let target = self.target_generation();
                self.canonical = target;
                self.metadata = target;
                self.synced_metadata = target;
                self.accepted_authority = self.spec.new_authority.as_ref().map(|_| Generation::New);
                self.admitted_authority = self.accepted_authority;
                self.staging_present = false;
                if self.cleanup_present {
                    self.phase = TransactionPhase::CleanupPending;
                } else {
                    self.phase = TransactionPhase::Complete;
                }
            }
            RecoveryDisposition::Settled => {}
        }
        self
    }

    /// GC only reclaims debris already classified by recovery; it never chooses rollback or roll-forward.
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
        if self.spec.new_authority.is_some() {
            MetadataGeneration::New
        } else {
            MetadataGeneration::Absent
        }
    }

    fn swap_canonical(&mut self) {
        self.canonical = self.target_generation();
        self.staging_present = false;
        self.cleanup_present = self.spec.old_authority.is_some();
        self.events.push(ProtocolEvent::CanonicalSwap);
    }

    fn reclaim_cleanup(&mut self) {
        if self.cleanup_present {
            self.cleanup_present = false;
            self.events.push(ProtocolEvent::CleanupReclaimed);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcPass {
    pub examined: usize,
    pub reclaimed: usize,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RecoveryError {
    #[error("gateway token must not be empty")]
    EmptyToken,
    #[error("logical workspace name must not be empty")]
    EmptyLogicalName,
    #[error("transaction id must not be empty")]
    EmptyTransactionId,
    #[error("{0:?} requires old authority")]
    MissingOldAuthority(TransactionKind),
    #[error("{0:?} does not accept old authority")]
    UnexpectedOldAuthority(TransactionKind),
    #[error("{0:?} requires new authority")]
    MissingNewAuthority(TransactionKind),
    #[error("{0:?} does not accept new authority")]
    UnexpectedNewAuthority(TransactionKind),
    #[error("replacement must mint a fresh workspace incarnation")]
    ReusedIncarnation,
    #[error("replacement must mint a fresh gateway token")]
    ReusedToken,
    #[error("{phase:?} is not reachable for {kind:?}")]
    UnreachableFailpoint {
        kind: TransactionKind,
        phase: TransactionPhase,
    },
    #[error("transaction is already settled at {0:?}")]
    TransactionSettled(TransactionPhase),
}
