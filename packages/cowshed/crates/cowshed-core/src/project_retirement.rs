//! Explicit-abandon retirement of a whole project registration.
//!
//! `cowshed forget <owner/repo> --abandon` is the one authority this module obeys: the operator
//! names a project and gives up serving it. What that buys is a move, never a deletion: the
//! project's whole store tree — images with the Git inside them, sidecars, grants, CA keys,
//! resource evidence and the registration itself — is renamed unchanged into reserved host
//! storage. A missing checkout, marker, PID or prefix, and any age, authorize nothing.
//!
//! The record sits outside the tree it fences, at `.retirements/<owner>/<repo>.json` under the
//! component-safe layout encoding, so it survives the move and a later run finds it without the
//! checkout ([`outstanding_retirements`]). Beside it:
//!
//! - `<repo>.lock` serializes every read-modify-write of the record, apart from the project
//!   lease, so authorization can fence new opens while actors still hold the project;
//! - `<repo>.lease` is the project lease. Every project actor holds it shared for its lifetime
//!   ([`acquire_project_reader`]); the collector takes it exclusive without waiting
//!   ([`acquire_collector`]), which succeeds only once every actor that opened before the fence has
//!   released it. No PID is inferred: the kernel lease is the whole proof. The lease is the
//!   inode the path names: whoever locks it checks afterwards, by device and inode and without
//!   following a link, that the path still names what it locked, and reopens it when not. That is
//!   what lets an unbinding remove the lease ([`ProjectReadLease::remove_unbound`]): only once its
//!   own shared hold upgrades to exclusive without waiting, so no one else holds it, and only
//!   while no record stands beside it. An opener that reached the removed inode finds its path
//!   gone or naming a new one and takes that instead, so every holder still meets on one inode;
//! - `<repo>.run` admits one execution of the retirement at a time, from before the drain
//!   through the archive commit, and only for the exact record it was given
//!   ([`acquire_retirement_run`]).
//!
//! A reader takes its shared lease *before* it reads the record, and authorization publishes the
//! record before anything else happens. So once the record is published every later reader is
//! refused, and every earlier one still holds the lease the collector's exclusive attempt meets.
//!
//! Phases advance one step at a time: `Fenced` (record durable, new opens refused) → `Drained`
//! (the collector lease proved no actor remains) → `Detached` (the orchestrator detached the exact
//! backing images, unforced) → `Archived` (the tree was renamed whole to
//! `.retired-projects/<operation-id>`, its final record copied to
//! `.retired-projects/<operation-id>.json`). Only [`RetirementIntent::archive_project`] records
//! `Archived`, and only an unarchived record blocks an open.

use std::ffi::{CStr, CString, OsStr};
use std::fmt;
use std::fs::{File, TryLockError};
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::{Path, PathBuf};

use cowshed_os::fork_lock::Fenced;
use cowshed_os::fsio::{self, Durability, TemporaryAt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::dto::Sha256Digest;
use crate::repository::{
    REPOSITORY_BINDING_FILE, RepoId, RepositoryBinding, encode_component, encode_layout_owner,
    is_lexically_canonical,
};
use crate::storage::bootstrap::{RETIRED_PROJECTS_NAMESPACE, RETIREMENTS_NAMESPACE};
use crate::{CowshedError, Result};

pub mod native;

const RECORD_VERSION: u32 = 1;
const RECORD_SUFFIX: &str = ".json";
const SERIALIZATION_SUFFIX: &str = ".lock";
const LEASE_SUFFIX: &str = ".lease";
const RUN_SUFFIX: &str = ".run";
/// How many times one taking of the project lease reopens it after finding it removed under
/// the open. Each reopen means an unbinding removed the inode this open reached, which happens
/// at most once per unbinding, so a run of them is the store being tampered with, not a race.
const LEASE_REOPENS: usize = 16;
/// Records and registrations are a few hundred bytes; a file this large is neither.
const MAX_CONTROLLER_FILE: u64 = 1 << 20;
const LOCK_FILE_MODE: libc::c_uint = 0o600;
const STORAGE_HINT: &str = "check controller storage permissions and retry";
const RECORD_HINT: &str =
    "inspect the record; only `cowshed forget <owner/repo> --abandon` writes one";

/// What `cowshed forget <owner/repo> --abandon` reports: the project it retired, where its whole
/// tree now stands, and how many backing images that tree carries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectRetirementReport {
    pub repo_id: RepoId,
    pub archive: PathBuf,
    pub images: usize,
}

/// The authority a retirement record carries. One variant: an explicit operator abandonment is
/// the only thing that may retire a project, and a record naming anything else does not decode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RetirementAuthorization {
    ExplicitAbandon,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RetirementPhase {
    /// The record is durable and the project admits no new open.
    Fenced,
    /// The collector's exclusive lease proved no project actor remains.
    Drained,
    /// The project's exact backing images are detached.
    Detached,
    /// The whole tree stands in the archive. Terminal; blocks nothing.
    Archived,
}

impl RetirementPhase {
    const fn successor(self) -> Option<Self> {
        match self {
            Self::Fenced => Some(Self::Drained),
            Self::Drained => Some(Self::Detached),
            Self::Detached => Some(Self::Archived),
            Self::Archived => None,
        }
    }

    /// Whether a record in this phase refuses new opens of its project.
    pub const fn blocks_open(self) -> bool {
        !matches!(self, Self::Archived)
    }
}

impl fmt::Display for RetirementPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Fenced => "fenced",
            Self::Drained => "drained",
            Self::Detached => "detached",
            Self::Archived => "archived",
        })
    }
}

/// One explicit retirement of one project, as durably recorded.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetirementIntent {
    version: u32,
    pub operation_id: Uuid,
    pub repo_id: RepoId,
    pub authorization: RetirementAuthorization,
    pub phase: RetirementPhase,
    /// SHA-256 of the exact `repository.json` bytes the operator abandoned, valid or not.
    pub registration_sha256: Sha256Digest,
}

/// A project actor's shared hold on its project lease. Held for the actor's lifetime; while any
/// is held, no collector can take the project.
#[derive(Debug)]
pub struct ProjectReadLease {
    repo_id: RepoId,
    lease: Fenced<File>,
}

/// What an unbinding's [`ProjectReadLease::remove_unbound`] did with the project lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnboundLease {
    /// It is gone, and with it the owner directory it left empty.
    Removed,
    /// Another holder shares it, so it stays; the caller keeps its shared hold.
    Held { lease: PathBuf },
    /// A retirement record stands beside it, which it fences, so it stays; the caller keeps its
    /// shared hold.
    Recorded { lease: PathBuf },
}

impl ProjectReadLease {
    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Remove the project lease once the project's unbinding removed its binding, so an unbound
    /// identity leaves nothing under `.retirements`. Only this holder may: its shared hold
    /// upgrades to exclusive without waiting, or another holder shares it and it stays
    /// ([`UnboundLease::Held`]). With the lease exclusive, a record beside it keeps it too
    /// ([`UnboundLease::Recorded`]); otherwise the lease and an owner directory it leaves empty
    /// are removed. Whenever it stays, the hold goes back to shared. Once removed, this hold is on
    /// an unlinked inode and fences nothing: an opener that reached that inode sees its path no
    /// longer names it and opens the one there now. A record published after the check is fenced
    /// all the same: the collector takes the new inode, which no one holding the old one ever
    /// kept, because this hold excluded them all.
    pub fn remove_unbound(&self, store: &Path) -> Result<UnboundLease> {
        let store = Store::open(store)?;
        let names = Names::of(&self.repo_id);
        let lease = store.retirement_path(&names, &names.lease);
        match self.lease.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                self.keep_shared(&lease)?;
                return Ok(UnboundLease::Held { lease });
            }
            Err(TryLockError::Error(error)) => {
                return Err(io_failure("upgrade project lease", &lease, error));
            }
        }
        let removed = (|| {
            let owner = store
                .existing_retirement_owner(&names)?
                .ok_or_else(|| replaced(&lease))?;
            if !names_file(&owner, &names.lease, &self.lease)
                .map_err(|error| io_failure("inspect project lease", &lease, error))?
            {
                return Err(replaced(&lease));
            }
            if store.load_at(&owner, &names, &self.repo_id)?.is_some() {
                return Ok(UnboundLease::Recorded {
                    lease: lease.clone(),
                });
            }
            unlink_at(&owner, &names.lease, 0)
                .map_err(|error| io_failure("remove project lease", &lease, error))?;
            let Some(retirements) = store.retirements()? else {
                return Ok(UnboundLease::Removed);
            };
            let owner_path = store
                .path
                .join(RETIREMENTS_NAMESPACE)
                .join(component(&names.owner));
            match unlink_at(&retirements, &names.owner, libc::AT_REMOVEDIR) {
                Ok(()) => Ok(UnboundLease::Removed),
                // Another identity of the owner keeps it, or a run removed it already.
                Err(error)
                    if error.raw_os_error() == Some(libc::ENOTEMPTY)
                        || error.raw_os_error() == Some(libc::EEXIST)
                        || error.kind() == io::ErrorKind::NotFound =>
                {
                    Ok(UnboundLease::Removed)
                }
                Err(error) => Err(io_failure("remove", &owner_path, error)),
            }
        })();
        if !matches!(removed, Ok(UnboundLease::Removed)) {
            self.keep_shared(&lease)?;
        }
        removed
    }

    /// Hold the lease shared again after an upgrade that did not remove it. A refused
    /// non-blocking upgrade keeps the shared hold on Darwin; Linux drops it first, so it is
    /// taken again rather than assumed.
    fn keep_shared(&self, lease: &Path) -> Result<()> {
        match self.lease.try_lock_shared() {
            Ok(()) => Ok(()),
            Err(TryLockError::WouldBlock) => Err(CowshedError::integrity(
                format!(
                    "this process lost its shared hold on project lease {} to an exclusive holder",
                    lease.display()
                ),
                forget_hint(&self.repo_id),
            )),
            Err(TryLockError::Error(error)) => {
                Err(io_failure("take project lease shared", lease, error))
            }
        }
    }
}

/// One execution of one retirement, held from before the drain through the archive commit: two
/// runs of the same operation never overlap, and a run holding a stale intent never acts on what
/// the record has become since.
#[derive(Debug)]
pub struct RetirementRunLease {
    repo_id: RepoId,
    operation_id: Uuid,
    _lease: Fenced<File>,
}

impl RetirementRunLease {
    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub fn operation_id(&self) -> Uuid {
        self.operation_id
    }
}

/// The exclusive project lease one pending retirement holds once no actor remains.
#[derive(Debug)]
pub struct CollectorLease {
    repo_id: RepoId,
    operation_id: Uuid,
    _lease: Fenced<File>,
}

impl CollectorLease {
    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub fn operation_id(&self) -> Uuid {
        self.operation_id
    }
}

impl RetirementIntent {
    /// Record the operator's explicit abandonment of `repo`, or resume the one already recorded.
    ///
    /// A new record needs the project's registration in place: its bytes are what the record
    /// pins. A valid registration naming another identity refuses; a malformed one is preserved
    /// as the bytes it is. A pending record resumes only while its evidence — the registration in
    /// place, or in the archive after a rename that outran its phase write — still carries the
    /// recorded digest. An archived record is the completed result while no registration stands
    /// at the project path again; a project adopted there since is a new retirement.
    pub fn authorize(store: &Path, repo: &RepoId) -> Result<Self> {
        let store = Store::open(store)?;
        let names = Names::of(repo);
        let owner = store.retirement_owner(&names)?;
        let _serialized = store.serialize(&owner, &names)?;
        match store.load_at(&owner, &names, repo)? {
            Some(pending) if pending.phase.blocks_open() => {
                pending.locate_evidence(&store, &names)?;
                return Ok(pending);
            }
            Some(archived) => {
                let reregistered = match store.project(&names)? {
                    Some((_, project)) => {
                        registration_digest(&project, repo, &store.project_path(&names))?.is_some()
                    }
                    None => false,
                };
                if !reregistered {
                    return Ok(archived);
                }
                // The superseded record survives as the archive's own copy.
                archived.preserve_final_record(&store)?;
            }
            None => {}
        }
        let project_path = store.project_path(&names);
        let registration = match store.project(&names)? {
            Some((_, project)) => registration_digest(&project, repo, &project_path)?,
            None => None,
        };
        let Some(registration_sha256) = registration else {
            return Err(CowshedError::not_found(
                format!(
                    "no project registration {} to retire",
                    project_path.join(REPOSITORY_BINDING_FILE).display()
                ),
                "name a project this store registers",
            ));
        };
        let intent = Self {
            version: RECORD_VERSION,
            operation_id: Uuid::new_v4(),
            repo_id: repo.clone(),
            authorization: RetirementAuthorization::ExplicitAbandon,
            phase: RetirementPhase::Fenced,
            registration_sha256,
        };
        store.publish_record(&owner, &names, &intent)?;
        Ok(intent)
    }

    /// The record for `repo`, if one was ever written. Creates nothing.
    pub fn load(store: &Path, repo: &RepoId) -> Result<Option<Self>> {
        let store = Store::open(store)?;
        let names = Names::of(repo);
        match store.existing_retirement_owner(&names)? {
            Some(owner) => store.load_at(&owner, &names, repo),
            None => Ok(None),
        }
    }

    /// Durably record this intent's phase over its own record: the same operation, staying put
    /// or advancing exactly one phase, and never to `Archived`, which only
    /// [`Self::archive_project`] may record.
    pub fn persist(&self, store: &Path) -> Result<()> {
        if self.phase == RetirementPhase::Archived {
            return Err(CowshedError::internal(format!(
                "retirement {} of {} may record archived only by archiving its project",
                self.operation_id, self.repo_id
            )));
        }
        let store = Store::open(store)?;
        let names = Names::of(&self.repo_id);
        let owner = store.recorded_owner(&names)?;
        let _serialized = store.serialize(&owner, &names)?;
        let current = store.recorded(&owner, &names, &self.repo_id)?;
        self.require_same_operation(&current, &store, &names)?;
        if self.phase == current.phase {
            return Ok(());
        }
        if current.phase.successor() != Some(self.phase) {
            return Err(CowshedError::conflict(
                format!(
                    "retirement {} of {} cannot move from {} to {}",
                    self.operation_id, self.repo_id, current.phase, self.phase
                ),
                forget_hint(&self.repo_id),
            ));
        }
        store.publish_record(&owner, &names, self)
    }

    /// Where the project tree stands while it is registered.
    pub fn project_path(&self, store: &Path) -> PathBuf {
        let names = Names::of(&self.repo_id);
        store
            .join(component(&names.owner))
            .join(component(&names.repo))
    }

    /// Where this operation preserves the whole project tree.
    pub fn archive_path(&self, store: &Path) -> PathBuf {
        store
            .join(RETIRED_PROJECTS_NAMESPACE)
            .join(self.operation_id.hyphenated().to_string())
    }

    /// Refuse unless the abandoned registration still carries the recorded digest where this
    /// phase leaves it: in place, or, after a rename that outran its phase write, in the archive;
    /// an archived retirement only in its archive. Reads only — check it before every disk effect,
    /// since an actor admitted before the fence may rewrite the registration while it drains.
    pub fn verify_registration(&self, store: &Path) -> Result<()> {
        let store = Store::open(store)?;
        let names = Names::of(&self.repo_id);
        if self.phase.blocks_open() {
            return self.locate_evidence(&store, &names).map(|_| ());
        }
        let archive_path = self.archive_path(store.path);
        let archive = match store.retired()? {
            Some(retired) => existing_directory(&retired, &self.operation_name())
                .map_err(|error| io_failure("open archive", &archive_path, error))?,
            None => None,
        };
        let digest = match archive {
            Some(archive) => registration_digest(&archive, &self.repo_id, &archive_path)?,
            None => None,
        };
        if digest == Some(self.registration_sha256) {
            Ok(())
        } else {
            Err(evidence_mismatch(self, &archive_path))
        }
    }

    /// Move the detached project tree whole into this operation's archive and record `Archived`.
    ///
    /// Runs only from `Detached`, under the collector lease of this same operation. The rename
    /// refuses an existing archive and follows no link; the registration digest is checked on the
    /// tree before it moves and on the archive after. A rename that completed before a crash kept
    /// its phase write is recognised by the archive carrying the recorded digest while the
    /// project path is empty, and is finished, not repeated. An already archived record answers
    /// at once.
    pub fn archive_project(&mut self, store: &Path, collector: &CollectorLease) -> Result<()> {
        if collector.repo_id != self.repo_id || collector.operation_id != self.operation_id {
            return Err(CowshedError::internal(format!(
                "collector lease of {} retirement {} cannot archive {} retirement {}",
                collector.repo_id, collector.operation_id, self.repo_id, self.operation_id
            )));
        }
        let store = Store::open(store)?;
        let names = Names::of(&self.repo_id);
        let owner = store.recorded_owner(&names)?;
        let _serialized = store.serialize(&owner, &names)?;
        let current = store.recorded(&owner, &names, &self.repo_id)?;
        self.require_same_operation(&current, &store, &names)?;
        match current.phase {
            RetirementPhase::Archived => {
                current.preserve_final_record(&store)?;
                *self = current;
                return Ok(());
            }
            RetirementPhase::Detached => {}
            RetirementPhase::Fenced | RetirementPhase::Drained => {
                return Err(CowshedError::conflict(
                    format!(
                        "retirement {} of {} is {}; only a detached project is archived",
                        current.operation_id, current.repo_id, current.phase
                    ),
                    forget_hint(&current.repo_id),
                ));
            }
        }
        let retired = store.retired_created()?;
        let operation = current.operation_name();
        if let Evidence::InPlace { parent } = current.locate_evidence(&store, &names)? {
            let archive = current.archive_path(store.path);
            rename_noreplace_between(&parent, &names.repo, &retired, &operation)
                .map_err(|error| io_failure("archive project tree to", &archive, error))?;
            Durability::PowerLoss
                .sync_new_entry_at(&retired)
                .and_then(|()| Durability::PowerLoss.sync_new_entry_at(&parent))
                .map_err(|error| io_failure("make durable the archive", &archive, error))?;
            let moved = existing_directory(&retired, &operation)
                .map_err(|error| io_failure("open archive", &archive, error))?;
            let digest = match moved {
                Some(moved) => registration_digest(&moved, &current.repo_id, &archive)?,
                None => None,
            };
            if digest != Some(current.registration_sha256) {
                return Err(evidence_mismatch(&current, &archive));
            }
        }
        let mut archived = current;
        archived.phase = RetirementPhase::Archived;
        archived.preserve_final_record(&store)?;
        store.publish_record(&owner, &names, &archived)?;
        *self = archived;
        Ok(())
    }

    fn operation_name(&self) -> CString {
        CString::new(self.operation_id.hyphenated().to_string())
            .expect("a hyphenated UUID carries no NUL")
    }

    fn require_same_operation(
        &self,
        current: &Self,
        store: &Store<'_>,
        names: &Names,
    ) -> Result<()> {
        if current.operation_id == self.operation_id
            && current.registration_sha256 == self.registration_sha256
            && current.authorization == self.authorization
        {
            return Ok(());
        }
        Err(CowshedError::conflict(
            format!(
                "retirement record {} names operation {}, not {}",
                store.record_path(names).display(),
                current.operation_id,
                self.operation_id
            ),
            forget_hint(&self.repo_id),
        ))
    }

    /// Where the abandoned registration stands now, refusing any place it does not carry the
    /// recorded digest: no path or checksum fallback.
    fn locate_evidence(&self, store: &Store<'_>, names: &Names) -> Result<Evidence> {
        let project_path = store.project_path(names);
        let archive_path = self.archive_path(store.path);
        let project = store.project(names)?;
        let archive = match store.retired()? {
            Some(retired) => existing_directory(&retired, &self.operation_name())
                .map_err(|error| io_failure("open archive", &archive_path, error))?,
            None => None,
        };
        match (project, archive) {
            (Some((parent, project)), None) => {
                if registration_digest(&project, &self.repo_id, &project_path)?
                    != Some(self.registration_sha256)
                {
                    return Err(evidence_mismatch(self, &project_path));
                }
                Ok(Evidence::InPlace { parent })
            }
            (None, Some(archive)) => {
                if registration_digest(&archive, &self.repo_id, &archive_path)?
                    != Some(self.registration_sha256)
                {
                    return Err(evidence_mismatch(self, &archive_path));
                }
                Ok(Evidence::Archived)
            }
            (Some(_), Some(_)) => Err(CowshedError::integrity(
                format!(
                    "retirement {} of {} finds both {} and its archive {}",
                    self.operation_id,
                    self.repo_id,
                    project_path.display(),
                    archive_path.display()
                ),
                "inspect both trees; cowshed moves neither while both exist",
            )),
            (None, None) => Err(CowshedError::integrity(
                format!(
                    "retirement {} of {} finds neither {} nor its archive {}",
                    self.operation_id,
                    self.repo_id,
                    project_path.display(),
                    archive_path.display()
                ),
                "inspect the store; the abandoned project tree is not where the record left it",
            )),
        }
    }

    /// Copy this archived record beside its archive, so the archive explains itself after a
    /// later retirement of a re-adopted project supersedes the per-project record.
    fn preserve_final_record(&self, store: &Store<'_>) -> Result<()> {
        debug_assert_eq!(self.phase, RetirementPhase::Archived);
        let retired = store.retired_created()?;
        let name = CString::new(format!("{}{RECORD_SUFFIX}", self.operation_id.hyphenated()))
            .expect("a hyphenated UUID carries no NUL");
        let path = store
            .path
            .join(RETIRED_PROJECTS_NAMESPACE)
            .join(component(&name));
        publish_at(&retired, &name, &encode(self)?)
            .map_err(|error| io_failure("preserve retirement record", &path, error))
    }
}

/// Take `repo`'s project lease shared, then refuse when an unarchived retirement fences it.
///
/// Never waits: an exclusive holder is a collector retiring the project, or an unbinding for
/// the instant it removes the lease.
pub fn acquire_project_reader(store: &Path, repo: &RepoId) -> Result<ProjectReadLease> {
    let store = Store::open(store)?;
    let names = Names::of(repo);
    let Some((owner, lease)) = store.lock_lease(
        &names,
        LeaseMode::Shared,
        || store.retirement_owner(&names),
        open_lock,
    )?
    else {
        return Err(CowshedError::conflict(
            format!(
                "project {repo} is being collected by its explicit retirement, or its unbinding \
                 is removing its lease"
            ),
            forget_hint(repo),
        ));
    };
    if let Some(intent) = store.load_at(&owner, &names, repo)?
        && intent.phase.blocks_open()
    {
        return Err(pending_retirement(&intent));
    }
    Ok(ProjectReadLease {
        repo_id: repo.clone(),
        lease,
    })
}

/// Take `repo`'s project lease exclusive, without waiting, for its pending retirement.
///
/// Refused without an unarchived explicit record — nothing else authorizes collection — and
/// while any project actor still holds the lease shared.
pub fn acquire_collector(store: &Path, repo: &RepoId) -> Result<CollectorLease> {
    let store = Store::open(store)?;
    let names = Names::of(repo);
    let unauthorized = || {
        CowshedError::conflict(
            format!("no pending explicit retirement authorizes collecting {repo}"),
            forget_hint(repo),
        )
    };
    let Some(owner) = store.existing_retirement_owner(&names)? else {
        return Err(unauthorized());
    };
    let Some(intent) = store.load_at(&owner, &names, repo)? else {
        return Err(unauthorized());
    };
    if !intent.phase.blocks_open() {
        return Err(unauthorized());
    }
    let owner_path = store
        .path
        .join(RETIREMENTS_NAMESPACE)
        .join(component(&names.owner));
    let Some((owner, lease)) = store.lock_lease(
        &names,
        LeaseMode::Exclusive,
        || {
            owner
                .try_clone()
                .map_err(|error| io_failure("open", &owner_path, error))
        },
        open_lock,
    )?
    else {
        return Err(CowshedError::conflict(
            format!(
                "project {repo} still has active controllers holding its lease; retirement {} \
                 fences new opens and waits for them to drain",
                intent.operation_id
            ),
            forget_hint(repo),
        ));
    };
    // The record may have moved on between the read above and the lease.
    let current = store.recorded(&owner, &names, repo)?;
    if current.operation_id != intent.operation_id || !current.phase.blocks_open() {
        return Err(unauthorized());
    }
    Ok(CollectorLease {
        repo_id: repo.clone(),
        operation_id: current.operation_id,
        _lease: lease,
    })
}

/// Take the run lease of `intent`'s retirement without waiting, then require the record to be
/// exactly `intent`: the same operation, registration digest, authorization and phase. A busy
/// lease is another run of this retirement; any difference is a stale intent, reloaded rather
/// than acted on. An archived record is admitted like any other, so a retry can report it.
pub fn acquire_retirement_run(
    store: &Path,
    intent: &RetirementIntent,
) -> Result<RetirementRunLease> {
    let store = Store::open(store)?;
    let names = Names::of(&intent.repo_id);
    let owner = store.recorded_owner(&names)?;
    let path = store.retirement_path(&names, &names.run);
    let lease = open_lock(&owner, &names.run)
        .map_err(|error| io_failure("open retirement run lease", &path, error))?;
    match lease.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(CowshedError::conflict(
                format!(
                    "another run of retirement {} of {} is executing",
                    intent.operation_id, intent.repo_id
                ),
                forget_hint(&intent.repo_id),
            ));
        }
        Err(TryLockError::Error(error)) => {
            return Err(io_failure("take retirement run lease", &path, error));
        }
    }
    let current = store.recorded(&owner, &names, &intent.repo_id)?;
    if current != *intent {
        return Err(CowshedError::conflict(
            format!(
                "retirement record {} holds operation {} ({}), not this run's operation {} ({})",
                store.record_path(&names).display(),
                current.operation_id,
                current.phase,
                intent.operation_id,
                intent.phase
            ),
            forget_hint(&intent.repo_id),
        ));
    }
    Ok(RetirementRunLease {
        repo_id: intent.repo_id.clone(),
        operation_id: intent.operation_id,
        _lease: lease,
    })
}

/// Whether an unarchived explicit retirement refuses new opens of `repo`.
pub fn retirement_blocks_open(store: &Path, repo: &RepoId) -> Result<bool> {
    Ok(RetirementIntent::load(store, repo)?.is_some_and(|intent| intent.phase.blocks_open()))
}

/// Admit an open of `repo` only while no unarchived explicit retirement fences it; the refusal is
/// the same conflict a project reader gets.
pub fn require_project_active(store: &Path, repo: &RepoId) -> Result<()> {
    match RetirementIntent::load(store, repo)? {
        Some(intent) if intent.phase.blocks_open() => Err(pending_retirement(&intent)),
        _ => Ok(()),
    }
}

/// Every unarchived retirement recorded in `store`, by repository: found from the records alone,
/// whatever became of the projects' checkouts. A record whose identity does not encode to the
/// name it is filed under refuses, like any other damaged record.
pub fn outstanding_retirements(store: &Path) -> Result<Vec<RetirementIntent>> {
    let store = Store::open(store)?;
    let Some(retirements) = store.retirements()? else {
        return Ok(Vec::new());
    };
    let retirements_path = store.path.join(RETIREMENTS_NAMESPACE);
    let mut outstanding = Vec::new();
    for owner_name in directory_entries(&retirements)
        .map_err(|error| io_failure("list", &retirements_path, error))?
    {
        let owner_path = retirements_path.join(component(&owner_name));
        let Some(owner) = existing_directory(&retirements, &owner_name)
            .map_err(|error| io_failure("open", &owner_path, error))?
        else {
            continue;
        };
        for record_name in
            directory_entries(&owner).map_err(|error| io_failure("list", &owner_path, error))?
        {
            if !record_name.to_bytes().ends_with(RECORD_SUFFIX.as_bytes()) {
                continue;
            }
            let path = owner_path.join(component(&record_name));
            let Some(bytes) = read_regular(&owner, &record_name)
                .map_err(|error| io_failure("read retirement record", &path, error))?
            else {
                continue;
            };
            let intent = decode(&bytes, &path)?;
            let names = Names::of(&intent.repo_id);
            if names.owner != owner_name || names.record != record_name {
                return Err(CowshedError::integrity(
                    format!(
                        "retirement record {} names {}, which is filed elsewhere",
                        path.display(),
                        intent.repo_id
                    ),
                    RECORD_HINT,
                ));
            }
            if intent.phase.blocks_open() {
                outstanding.push(intent);
            }
        }
    }
    outstanding.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
    Ok(outstanding)
}

fn forget_hint(repo: &RepoId) -> String {
    format!("cowshed forget {repo} --abandon")
}

/// The project lease this holder locked is no longer the one at its path, which only a removal
/// it did not make could do.
fn replaced(lease: &Path) -> CowshedError {
    CowshedError::integrity(
        format!(
            "project lease {} no longer names the inode this process holds",
            lease.display()
        ),
        "reopen the project so its actors hold the lease at that path",
    )
}

fn pending_retirement(intent: &RetirementIntent) -> CowshedError {
    CowshedError::conflict(
        format!(
            "project {} is fenced by explicit retirement {} ({}); it admits no new open",
            intent.repo_id, intent.operation_id, intent.phase
        ),
        forget_hint(&intent.repo_id),
    )
}

fn evidence_mismatch(intent: &RetirementIntent, at: &Path) -> CowshedError {
    CowshedError::integrity(
        format!(
            "retirement {} of {} abandoned registration sha256 {}, which {} no longer carries",
            intent.operation_id,
            intent.repo_id,
            intent.registration_sha256,
            at.join(REPOSITORY_BINDING_FILE).display()
        ),
        "inspect the registration; cowshed moves only the tree the operator abandoned",
    )
}

/// Classify a failed storage step: a link or non-directory in a controller path, or bytes that are
/// not a controller file, are integrity faults; a cross-filesystem tree cannot move whole;
/// anything else is the storage's.
fn io_failure(action: &str, path: &Path, error: io::Error) -> CowshedError {
    match error.raw_os_error() {
        Some(libc::ELOOP | libc::ENOTDIR) => CowshedError::integrity(
            format!(
                "cannot {action} {}: a symbolic link or non-directory stands in the controller path \
                 ({error})",
                path.display()
            ),
            "inspect that path; cowshed never follows a link out of its store",
        ),
        Some(libc::EXDEV) => CowshedError::environment_missing(
            format!(
                "cannot {action} {}: the project tree is another filesystem, so it cannot move whole \
                 ({error}); it is left in place",
                path.display()
            ),
            "preserve the project tree by hand; cowshed never copies and deletes an abandoned tree",
        ),
        _ if error.kind() == io::ErrorKind::InvalidData => CowshedError::integrity(
            format!("cannot {action} {}: {error}", path.display()),
            "inspect that path; it is not the controller file cowshed wrote there",
        ),
        _ => CowshedError::storage_failure(
            format!("cannot {action} {}: {error}", path.display()),
            &error,
            STORAGE_HINT,
        ),
    }
}

fn encode(intent: &RetirementIntent) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(intent).map_err(|error| {
        CowshedError::internal(format!("cannot encode retirement record: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn decode(bytes: &[u8], path: &Path) -> Result<RetirementIntent> {
    let intent: RetirementIntent = serde_json::from_slice(bytes).map_err(|error| {
        CowshedError::integrity(
            format!(
                "retirement record {} is not an explicit-abandon record: {error}",
                path.display()
            ),
            RECORD_HINT,
        )
    })?;
    if intent.version != RECORD_VERSION {
        return Err(CowshedError::integrity(
            format!(
                "retirement record {} has unsupported version {}",
                path.display(),
                intent.version
            ),
            RECORD_HINT,
        ));
    }
    Ok(intent)
}

/// The SHA-256 of the registration in the project tree `directory`, or `None` without one. A
/// registration that parses must name `repo` as its primary identity; one that does not parse is
/// the corrupt registration this module exists to retire, and only its bytes are pinned.
fn registration_digest(
    directory: &File,
    repo: &RepoId,
    tree: &Path,
) -> Result<Option<Sha256Digest>> {
    let path = tree.join(REPOSITORY_BINDING_FILE);
    let name = CString::new(REPOSITORY_BINDING_FILE).expect("a constant name carries no NUL");
    let Some(bytes) = read_regular(directory, &name)
        .map_err(|error| io_failure("read registration", &path, error))?
    else {
        return Ok(None);
    };
    if let Ok(binding) = serde_json::from_slice::<RepositoryBinding>(&bytes) {
        let primary = binding.primary().map_err(|error| {
            CowshedError::integrity(
                format!("registration {} is invalid: {error}", path.display()),
                "inspect the registration",
            )
        })?;
        if primary.repo_id != *repo {
            return Err(CowshedError::integrity(
                format!(
                    "registration {} names {}, not {repo}",
                    path.display(),
                    primary.repo_id
                ),
                "name the project by the identity its registration records",
            ));
        }
    }
    Ok(Some(Sha256Digest::compute(&bytes)))
}

/// The leaf names one repository's retirement files and project tree go by, each one already
/// component-encoded.
struct Names {
    owner: CString,
    repo: CString,
    record: CString,
    serialization: CString,
    lease: CString,
    run: CString,
}

impl Names {
    fn of(repo: &RepoId) -> Self {
        let owner = encode_layout_owner(repo.owner()).expect("a parsed RepoId owner is non-empty");
        let project = encode_component(repo.repo()).expect("a parsed RepoId repo is non-empty");
        // Encoded components are ASCII from `[a-z0-9._%-]`, never NUL.
        let leaf =
            |suffix: &str| CString::new(format!("{project}{suffix}")).expect("encoded, no NUL");
        Self {
            record: leaf(RECORD_SUFFIX),
            serialization: leaf(SERIALIZATION_SUFFIX),
            lease: leaf(LEASE_SUFFIX),
            run: leaf(RUN_SUFFIX),
            owner: CString::new(owner).expect("encoded, no NUL"),
            repo: CString::new(project).expect("encoded, no NUL"),
        }
    }
}

fn component(name: &CStr) -> &Path {
    Path::new(OsStr::from_bytes(name.to_bytes()))
}

enum Evidence {
    /// The tree is still at the project path; `parent` holds the owner directory it leaves.
    InPlace { parent: File },
    /// The tree already stands in the archive.
    Archived,
}

/// The store root held by descriptor: every path below is reached by `openat` without following
/// a link, never re-resolved from its path.
struct Store<'a> {
    path: &'a Path,
    root: File,
}

impl<'a> Store<'a> {
    fn open(path: &'a Path) -> Result<Self> {
        if !is_lexically_canonical(path) {
            return Err(CowshedError::usage(
                format!(
                    "store root {} is not absolute and normalized",
                    path.display()
                ),
                "pass the validated host store",
            ));
        }
        let root = fsio::open_directory_nofollow(path)
            .map_err(|error| io_failure("open store", path, error))?;
        Ok(Self { path, root })
    }

    fn retirement_path(&self, names: &Names, leaf: &CStr) -> PathBuf {
        self.path
            .join(RETIREMENTS_NAMESPACE)
            .join(component(&names.owner))
            .join(component(leaf))
    }

    fn record_path(&self, names: &Names) -> PathBuf {
        self.retirement_path(names, &names.record)
    }

    fn project_path(&self, names: &Names) -> PathBuf {
        self.path
            .join(component(&names.owner))
            .join(component(&names.repo))
    }

    fn retirements(&self) -> Result<Option<File>> {
        existing_directory(&self.root, &namespace(RETIREMENTS_NAMESPACE))
            .map_err(|error| io_failure("open", &self.path.join(RETIREMENTS_NAMESPACE), error))
    }

    fn retirement_owner(&self, names: &Names) -> Result<File> {
        let path = self
            .path
            .join(RETIREMENTS_NAMESPACE)
            .join(component(&names.owner));
        created_directory(&self.root, &namespace(RETIREMENTS_NAMESPACE))
            .and_then(|retirements| created_directory(&retirements, &names.owner))
            .map_err(|error| io_failure("create", &path, error))
    }

    fn existing_retirement_owner(&self, names: &Names) -> Result<Option<File>> {
        let Some(retirements) = self.retirements()? else {
            return Ok(None);
        };
        let path = self
            .path
            .join(RETIREMENTS_NAMESPACE)
            .join(component(&names.owner));
        existing_directory(&retirements, &names.owner)
            .map_err(|error| io_failure("open", &path, error))
    }

    /// The owner directory of a record that must already exist.
    fn recorded_owner(&self, names: &Names) -> Result<File> {
        self.existing_retirement_owner(names)?
            .ok_or_else(|| self.record_missing(names))
    }

    fn record_missing(&self, names: &Names) -> CowshedError {
        CowshedError::integrity(
            format!(
                "retirement record {} is missing",
                self.record_path(names).display()
            ),
            RECORD_HINT,
        )
    }

    fn retired(&self) -> Result<Option<File>> {
        existing_directory(&self.root, &namespace(RETIRED_PROJECTS_NAMESPACE))
            .map_err(|error| io_failure("open", &self.path.join(RETIRED_PROJECTS_NAMESPACE), error))
    }

    fn retired_created(&self) -> Result<File> {
        created_directory(&self.root, &namespace(RETIRED_PROJECTS_NAMESPACE)).map_err(|error| {
            io_failure("create", &self.path.join(RETIRED_PROJECTS_NAMESPACE), error)
        })
    }

    /// The project's owner directory and tree, when both exist.
    fn project(&self, names: &Names) -> Result<Option<(File, File)>> {
        let owner_path = self.path.join(component(&names.owner));
        let Some(parent) = existing_directory(&self.root, &names.owner)
            .map_err(|error| io_failure("open", &owner_path, error))?
        else {
            return Ok(None);
        };
        let project = existing_directory(&parent, &names.repo)
            .map_err(|error| io_failure("open", &self.project_path(names), error))?;
        Ok(project.map(|project| (parent, project)))
    }

    fn serialize(&self, owner: &File, names: &Names) -> Result<Fenced<File>> {
        let path = self.retirement_path(names, &names.serialization);
        let lock = open_lock(owner, &names.serialization)
            .map_err(|error| io_failure("open retirement lock", &path, error))?;
        loop {
            match lock.lock() {
                Ok(()) => return Ok(lock),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(io_failure("take retirement lock", &path, error)),
            }
        }
    }

    /// Lock `names`' project lease in `mode` without waiting; `None` while another holder
    /// excludes it. The lease is the inode its path names, and an unbinding may remove it after
    /// this opened it ([`ProjectReadLease::remove_unbound`]): after the attempt, locked or
    /// refused, the path must still name the inode it was made on, compared by device and inode
    /// without following a link, or the lease is reopened from `owner` (which reopens an owner
    /// directory the unbinding removed). `open` is [`open_lock`] outside tests.
    fn lock_lease(
        &self,
        names: &Names,
        mode: LeaseMode,
        mut owner: impl FnMut() -> Result<File>,
        mut open: impl FnMut(&File, &CStr) -> io::Result<Fenced<File>>,
    ) -> Result<Option<(File, Fenced<File>)>> {
        let path = self.retirement_path(names, &names.lease);
        for _ in 0..LEASE_REOPENS {
            let directory = owner()?;
            let lease = match open(&directory, &names.lease) {
                Ok(lease) => lease,
                // An unbinding removed the owner directory after `owner` opened it.
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(io_failure("open project lease", &path, error)),
            };
            let locked = match mode {
                LeaseMode::Shared => lease.try_lock_shared(),
                LeaseMode::Exclusive => lease.try_lock(),
            };
            let busy = match locked {
                Ok(()) => false,
                Err(TryLockError::WouldBlock) => true,
                Err(TryLockError::Error(error)) => {
                    return Err(io_failure("take project lease", &path, error));
                }
            };
            if names_file(&directory, &names.lease, &lease)
                .map_err(|error| io_failure("inspect project lease", &path, error))?
            {
                return Ok(if busy { None } else { Some((directory, lease)) });
            }
        }
        Err(CowshedError::conflict(
            format!(
                "project lease {} was removed under this open {LEASE_REOPENS} times in a row",
                path.display()
            ),
            "retry once the project's unbindings have finished",
        ))
    }

    fn load_at(
        &self,
        owner: &File,
        names: &Names,
        repo: &RepoId,
    ) -> Result<Option<RetirementIntent>> {
        let path = self.record_path(names);
        let Some(bytes) = read_regular(owner, &names.record)
            .map_err(|error| io_failure("read retirement record", &path, error))?
        else {
            return Ok(None);
        };
        let intent = decode(&bytes, &path)?;
        if intent.repo_id != *repo {
            return Err(CowshedError::integrity(
                format!(
                    "retirement record {} names {}, not {repo}",
                    path.display(),
                    intent.repo_id
                ),
                RECORD_HINT,
            ));
        }
        Ok(Some(intent))
    }

    fn recorded(&self, owner: &File, names: &Names, repo: &RepoId) -> Result<RetirementIntent> {
        self.load_at(owner, names, repo)?
            .ok_or_else(|| self.record_missing(names))
    }

    fn publish_record(&self, owner: &File, names: &Names, intent: &RetirementIntent) -> Result<()> {
        publish_at(owner, &names.record, &encode(intent)?).map_err(|error| {
            io_failure("publish retirement record", &self.record_path(names), error)
        })
    }
}

fn namespace(name: &str) -> CString {
    CString::new(name).expect("a constant namespace carries no NUL")
}

/// The existing directory `name` beneath `parent`; `None` when absent. A link or non-directory
/// there is an error, never followed.
fn existing_directory(parent: &File, name: &CStr) -> io::Result<Option<File>> {
    // SAFETY: the held parent fd and NUL-terminated leaf outlive the call. NOFOLLOW and DIRECTORY
    // refuse a link or a non-directory at the leaf.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: a successful openat returned a new owned directory descriptor.
    Ok(Some(unsafe { File::from_raw_fd(fd) }))
}

/// The directory `name` beneath `parent`, created 0700 and made durable when it was absent.
fn created_directory(parent: &File, name: &CStr) -> io::Result<File> {
    let (directory, created) = fsio::open_or_create_child_directory(parent, name)?;
    if created {
        Durability::PowerLoss.sync_new_entry_at(parent)?;
    }
    Ok(directory)
}

/// The bytes of the regular file `name` beneath `directory`, or `None` when absent. A link, a
/// non-regular file or an oversized one is an error, never read through.
fn read_regular(directory: &File, name: &CStr) -> io::Result<Option<Vec<u8>>> {
    // SAFETY: the held directory fd and NUL-terminated leaf outlive the call. NONBLOCK keeps a
    // planted FIFO from stalling the open; it is refused as not regular below.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: a successful openat returned a new owned file descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata()?;
    let oversized = || io::Error::new(io::ErrorKind::InvalidData, "file exceeds 1 MiB");
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a regular file",
        ));
    }
    if metadata.len() > MAX_CONTROLLER_FILE {
        return Err(oversized());
    }
    let mut bytes = Vec::new();
    (&file)
        .take(MAX_CONTROLLER_FILE + 1)
        .read_to_end(&mut bytes)?;
    if !u64::try_from(bytes.len()).is_ok_and(|length| length <= MAX_CONTROLLER_FILE) {
        return Err(oversized());
    }
    Ok(Some(bytes))
}

/// Atomically publish `bytes` as the private file `name` beneath `directory`, surviving power
/// loss: an exclusive 0600 temp sibling, synced, renamed over `name`, the directory synced.
fn publish_at(directory: &File, name: &CStr, bytes: &[u8]) -> io::Result<()> {
    let temp = CString::new(
        fsio::temp_name(OsStr::from_bytes(name.to_bytes()), Uuid::new_v4().simple()).into_vec(),
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains NUL"))?;
    let mut file = fsio::create_private_file_at(directory, &temp)?;
    let cleanup = TemporaryAt::new(directory, &temp);
    file.write_all(bytes)?;
    Durability::PowerLoss.sync_file(&file)?;
    // SAFETY: both leaves are NUL-terminated and resolve beneath the same held directory fd.
    if unsafe {
        libc::renameat(
            directory.as_raw_fd(),
            temp.as_ptr(),
            directory.as_raw_fd(),
            name.as_ptr(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    cleanup.disarm();
    Durability::PowerLoss.sync_new_entry_at(directory)
}

/// How a project lease is taken.
#[derive(Clone, Copy, Debug)]
enum LeaseMode {
    Shared,
    Exclusive,
}

/// Whether `name` beneath `directory` is `file` itself: the same device and inode, the leaf
/// not followed. An absent `name` is not.
fn names_file(directory: &File, name: &CStr, file: &File) -> io::Result<bool> {
    let mut at = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the held directory fd and NUL-terminated leaf outlive the call, which fills `at`
    // on success. NOFOLLOW stats a link at the leaf itself.
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            at.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(error)
        };
    }
    let mut held = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the held fd outlives the call, which fills `held` on success.
    if unsafe { libc::fstat(file.as_raw_fd(), held.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both calls succeeded, so both buffers are initialized.
    let (at, held) = unsafe { (at.assume_init(), held.assume_init()) };
    Ok(at.st_dev == held.st_dev && at.st_ino == held.st_ino)
}

/// Remove the leaf `name` beneath `directory` (`AT_REMOVEDIR` for an empty directory), never
/// following it.
fn unlink_at(directory: &File, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    // SAFETY: the held directory fd and NUL-terminated leaf outlive the call.
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), flags) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Open the lock file `name` beneath `directory`, creating it 0600 when absent, fenced from the
/// open on (`fork_lock`). Never one `O_CREAT` open: racing creators on macOS get `ENOENT` from it
/// (`store_lock::open_or_create_lock_at`).
fn open_lock(directory: &File, name: &CStr) -> io::Result<Fenced<File>> {
    let open = |flags: libc::c_int| {
        // SAFETY: the held directory fd and NUL-terminated leaf outlive the call. NOFOLLOW
        // refuses a link at the leaf; the mode applies only to O_CREAT.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC | flags,
                LOCK_FILE_MODE,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: a successful openat returned a new owned file descriptor.
            Ok(Fenced::new(unsafe { File::from_raw_fd(fd) }))
        }
    };
    let lock = open(0)
        .or_else(|error| match error.kind() {
            io::ErrorKind::NotFound => open(libc::O_CREAT | libc::O_EXCL),
            _ => Err(error),
        })
        .or_else(|error| match error.kind() {
            io::ErrorKind::AlreadyExists => open(0),
            _ => Err(error),
        })?;
    if !lock.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "lock is not a regular file",
        ));
    }
    Ok(lock)
}

/// Rename `from_name` beneath `from` to `to_name` beneath `to`, refusing an existing destination
/// atomically and following neither leaf.
fn rename_noreplace_between(
    from: &File,
    from_name: &CStr,
    to: &File,
    to_name: &CStr,
) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    // SAFETY: both directory fds are held and both leaves are NUL-terminated for the call.
    // RENAME_EXCL makes the destination check part of the rename.
    let result = unsafe {
        libc::renameatx_np(
            from.as_raw_fd(),
            from_name.as_ptr(),
            to.as_raw_fd(),
            to_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(target_os = "linux")]
    // SAFETY: as above; the raw syscall avoids depending on glibc's renameat2 wrapper.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            from.as_raw_fd(),
            from_name.as_ptr(),
            to.as_raw_fd(),
            to_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (from, from_name, to, to_name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic create-new rename is unsupported",
        ))
    }
}

/// The names in `directory`, read through its descriptor, skipping `.`, `..` and dot-prefixed
/// temp residue. A fresh `openat(".")` gives the listing its own offset.
fn directory_entries(directory: &File) -> io::Result<Vec<CString>> {
    // SAFETY: "." is a constant leaf beneath the live held directory.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c".".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is a new directory descriptor; on success fdopendir owns it.
    let stream = unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: fdopendir failed, so the descriptor still belongs to this call.
        drop(unsafe { File::from_raw_fd(fd) });
        return Err(error);
    }
    let mut names = Vec::new();
    let result = loop {
        clear_errno();
        // SAFETY: the stream is held until closed below.
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            break match error.raw_os_error() {
                Some(0) => Ok(()),
                _ => Err(error),
            };
        }
        // SAFETY: readdir returns a NUL-terminated name, live until the next call.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if !name.to_bytes().starts_with(b".") {
            names.push(name.to_owned());
        }
    };
    // SAFETY: this call owns the stream; closedir also closes its descriptor.
    if unsafe { libc::closedir(stream) } != 0 {
        let close = io::Error::last_os_error();
        result?;
        return Err(close);
    }
    result.map(|()| names)
}

/// Zero this thread's errno, which `readdir` leaves untouched at the end of a stream.
fn clear_errno() {
    #[cfg(target_os = "macos")]
    // SAFETY: errno is thread-local and always writable.
    unsafe {
        *libc::__error() = 0;
    }
    #[cfg(target_os = "linux")]
    // SAFETY: errno is thread-local and always writable.
    unsafe {
        *libc::__errno_location() = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;
    use crate::temp_root::TempRoot;
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;

    fn repo(value: &str) -> RepoId {
        RepoId::parse(value).expect("valid repository identity")
    }

    fn registration(repo: &str) -> Vec<u8> {
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "identities": [
                { "repoId": repo, "remoteName": null, "remoteUrl": null, "primary": true }
            ]
        }))
        .expect("registration")
    }

    fn store(root: &TempRoot) -> PathBuf {
        let store = root.join("store");
        fs::create_dir(&store).expect("store");
        store
    }

    /// A project tree carrying everything an archive must keep: the registration, images with Git
    /// inside them, grants, a CA key, checkpoints, the trash, a job's TMPDIR and a link.
    fn plant_project(store: &Path, id: &str, registration: &[u8]) -> PathBuf {
        let tree = store.join(id);
        fs::create_dir_all(tree.join("sessions/.trash")).expect("trash");
        fs::create_dir_all(tree.join("checkpoints/main")).expect("checkpoints");
        fs::create_dir_all(tree.join("tmp/raven/go")).expect("tmp");
        fs::write(tree.join(REPOSITORY_BINDING_FILE), registration).expect("registration");
        fs::write(tree.join("main.dmg"), b"apfs image; .git/objects inside").expect("main");
        fs::write(tree.join("main.dmg.grants.json"), b"{\"revision\":3}").expect("grants");
        fs::write(tree.join("main.dmg.ca.key"), b"private key").expect("key");
        fs::set_permissions(
            tree.join("main.dmg.ca.key"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("key mode");
        fs::write(tree.join("main.dmg.lock"), b"").expect("lock");
        fs::write(tree.join("sessions/raven.dmg"), b"session image").expect("session");
        fs::write(
            tree.join("sessions/.trash/old.bundle"),
            b"abandoned commits",
        )
        .expect("bundle");
        fs::write(tree.join("checkpoints/main/label.dmg"), b"checkpoint").expect("checkpoint");
        fs::write(tree.join("tmp/raven/go/module"), b"go module").expect("tmp file");
        fs::set_permissions(
            tree.join("tmp/raven/go/module"),
            fs::Permissions::from_mode(0o444),
        )
        .expect("read-only file");
        std::os::unix::fs::symlink("../main.dmg", tree.join("sessions/alias")).expect("link");
        tree
    }

    #[derive(Debug, Eq, PartialEq)]
    enum Node {
        Directory(u32),
        File(Vec<u8>, u32),
        Link(PathBuf),
    }

    /// Every entry beneath `root` by relative path: type, mode, bytes or link text. Follows no
    /// link.
    fn snapshot(root: &Path) -> BTreeMap<PathBuf, Node> {
        let mut nodes = BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(path) = pending.pop() {
            let metadata = fs::symlink_metadata(&path).expect("entry");
            let relative = path.strip_prefix(root).expect("beneath root").to_path_buf();
            let mode = metadata.permissions().mode() & 0o7777;
            let node = if metadata.file_type().is_symlink() {
                Node::Link(fs::read_link(&path).expect("link"))
            } else if metadata.is_dir() {
                for entry in fs::read_dir(&path).expect("directory") {
                    pending.push(entry.expect("entry").path());
                }
                Node::Directory(mode)
            } else {
                Node::File(fs::read(&path).expect("file"), mode)
            };
            nodes.insert(relative, node);
        }
        nodes
    }

    /// Drive one retirement through every phase the way the orchestrator does.
    fn retire(store: &Path, repo: &RepoId) -> RetirementIntent {
        let mut intent = RetirementIntent::authorize(store, repo).expect("authorize");
        let collector = acquire_collector(store, repo).expect("collector");
        intent.phase = RetirementPhase::Drained;
        intent.persist(store).expect("drained");
        intent.phase = RetirementPhase::Detached;
        intent.persist(store).expect("detached");
        intent
            .archive_project(store, &collector)
            .expect("archive project");
        intent
    }

    fn record_path(store: &Path, id: &str) -> PathBuf {
        store
            .join(RETIREMENTS_NAMESPACE)
            .join(format!("{id}{RECORD_SUFFIX}"))
    }

    #[test]
    fn neither_an_absent_record_nor_an_absent_registration_authorizes_anything() {
        let root = TempRoot::new("retire-absent");
        let store = store(&root);
        let widget = repo("acme/widget");
        // The registration is the marker discovery keys on; its absence is no authority.
        let tree = plant_project(&store, "acme/widget", &registration("acme/widget"));
        fs::remove_file(tree.join(REPOSITORY_BINDING_FILE)).expect("drop registration");
        let before = snapshot(&tree);

        assert!(!retirement_blocks_open(&store, &widget).expect("no record"));
        assert_eq!(RetirementIntent::load(&store, &widget).expect("load"), None);
        assert_eq!(
            acquire_collector(&store, &widget)
                .expect_err("no record, no collector")
                .code,
            ErrorCode::Conflict
        );
        assert_eq!(
            RetirementIntent::authorize(&store, &widget)
                .expect_err("nothing registered to abandon")
                .code,
            ErrorCode::NotFound
        );
        assert_eq!(RetirementIntent::load(&store, &widget).expect("load"), None);
        assert!(
            outstanding_retirements(&store)
                .expect("discover")
                .is_empty()
        );
        assert_eq!(snapshot(&tree), before);
        assert!(!store.join(RETIRED_PROJECTS_NAMESPACE).exists());
    }

    #[test]
    fn a_record_without_explicit_abandon_is_refused_not_obeyed() {
        let root = TempRoot::new("retire-unauthorized");
        let store = store(&root);
        let widget = repo("acme/widget");
        let bytes = registration("acme/widget");
        let tree = plant_project(&store, "acme/widget", &bytes);
        let before = snapshot(&tree);
        fs::create_dir_all(store.join(RETIREMENTS_NAMESPACE).join("acme")).expect("owner");
        let mut record = serde_json::json!({
            "version": 1,
            "operationId": Uuid::new_v4(),
            "repoId": "acme/widget",
            "authorization": "explicitAbandon",
            "phase": "detached",
            "registrationSha256": Sha256Digest::compute(&bytes),
        });
        let forged = [
            serde_json::json!("ageExpired"),
            serde_json::json!("pidVanished"),
            serde_json::Value::Null,
        ];
        for authorization in forged.into_iter().map(Some).chain([None]) {
            match authorization {
                Some(value) => record["authorization"] = value,
                None => {
                    record
                        .as_object_mut()
                        .expect("object")
                        .remove("authorization");
                }
            }
            fs::write(
                record_path(&store, "acme/widget"),
                serde_json::to_vec(&record).expect("record"),
            )
            .expect("plant record");
            for refusal in [
                RetirementIntent::load(&store, &widget).map(|_| ()),
                retirement_blocks_open(&store, &widget).map(|_| ()),
                acquire_collector(&store, &widget).map(|_| ()),
                acquire_project_reader(&store, &widget).map(|_| ()),
                RetirementIntent::authorize(&store, &widget).map(|_| ()),
            ] {
                assert_eq!(
                    refusal.expect_err("forged record").code,
                    ErrorCode::Integrity
                );
            }
            assert!(outstanding_retirements(&store).is_err());
            assert_eq!(snapshot(&tree), before);
        }
    }

    #[test]
    fn a_live_shared_lease_blocks_the_collector_until_it_is_released() {
        let root = TempRoot::new("retire-lease");
        let store = store(&root);
        let widget = repo("acme/widget");
        plant_project(&store, "acme/widget", &registration("acme/widget"));
        let reader = acquire_project_reader(&store, &widget).expect("actor opens");
        assert_eq!(reader.repo_id(), &widget);

        let intent = RetirementIntent::authorize(&store, &widget).expect("authorize");
        assert_eq!(intent.phase, RetirementPhase::Fenced);
        assert_eq!(
            intent.authorization,
            RetirementAuthorization::ExplicitAbandon
        );
        let refusal = acquire_collector(&store, &widget).expect_err("actor still holds");
        assert_eq!(refusal.code, ErrorCode::Conflict);
        assert!(
            refusal.message.contains("active controllers"),
            "{refusal:?}"
        );

        drop(reader);
        let collector = acquire_collector(&store, &widget).expect("drained");
        assert_eq!(collector.operation_id(), intent.operation_id);
        let refusal = acquire_project_reader(&store, &widget).expect_err("collecting");
        assert_eq!(refusal.code, ErrorCode::Conflict);
    }

    fn lease_path(store: &Path, id: &str) -> PathBuf {
        store
            .join(RETIREMENTS_NAMESPACE)
            .join(format!("{id}{LEASE_SUFFIX}"))
    }

    /// Whether some holder other than the probe keeps `lease` from being taken exclusive.
    fn held(lease: &Path) -> bool {
        let probe = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(lease)
            .expect("probe the lease");
        match probe.try_lock() {
            Ok(()) => false,
            Err(TryLockError::WouldBlock) => true,
            Err(TryLockError::Error(error)) => panic!("probe the lease: {error}"),
        }
    }

    #[test]
    fn an_unbinding_alone_on_its_lease_removes_it_and_its_emptied_owner_directory() {
        let root = TempRoot::new("unbound-lease");
        let store = store(&root);
        let widget = repo("acme/widget");
        let owner = store.join(RETIREMENTS_NAMESPACE).join("acme");
        let lease = lease_path(&store, "acme/widget");
        let unbinding = acquire_project_reader(&store, &widget).expect("actor opens");
        assert!(lease.is_file());

        assert_eq!(
            unbinding.remove_unbound(&store).expect("remove"),
            UnboundLease::Removed
        );
        assert!(!lease.exists(), "the lease outlives the unbinding");
        assert!(!owner.exists(), "an emptied owner directory remains");

        // A later actor of the identity makes the lease afresh; another identity of the owner
        // keeps the owner directory.
        let sibling = acquire_project_reader(&store, &repo("acme/gadget")).expect("sibling");
        let again = acquire_project_reader(&store, &widget).expect("readopted actor opens");
        assert!(lease.is_file());
        assert_eq!(
            again.remove_unbound(&store).expect("remove"),
            UnboundLease::Removed
        );
        assert!(!lease.exists());
        assert!(owner.is_dir(), "another identity's owner directory went");
        assert!(held(&lease_path(&store, "acme/gadget")));
        drop(sibling);
    }

    #[test]
    fn an_unbinding_refused_its_upgrade_leaves_the_lease_and_keeps_its_hold() {
        let root = TempRoot::new("unbound-lease-held");
        let store = store(&root);
        let widget = repo("acme/widget");
        plant_project(&store, "acme/widget", &registration("acme/widget"));
        let lease = lease_path(&store, "acme/widget");
        let unbinding = acquire_project_reader(&store, &widget).expect("actor opens");
        let other = acquire_project_reader(&store, &widget).expect("another actor opens");

        assert_eq!(
            unbinding.remove_unbound(&store).expect("remove"),
            UnboundLease::Held {
                lease: lease.clone()
            }
        );
        assert!(lease.is_file(), "a lease another actor holds was removed");

        // The unbinding still holds it shared: a collector waits for it too.
        drop(other);
        RetirementIntent::authorize(&store, &widget).expect("authorize");
        let refusal =
            acquire_collector(&store, &widget).expect_err("the unbinding still holds the lease");
        assert_eq!(refusal.code, ErrorCode::Conflict);
        drop(unbinding);
        acquire_collector(&store, &widget).expect("drained");
    }

    #[test]
    fn an_unbinding_keeps_the_lease_a_retirement_record_fences() {
        let root = TempRoot::new("unbound-lease-recorded");
        let store = store(&root);
        let widget = repo("acme/widget");
        plant_project(&store, "acme/widget", &registration("acme/widget"));
        retire(&store, &widget);
        let lease = lease_path(&store, "acme/widget");
        let unbinding =
            acquire_project_reader(&store, &widget).expect("an archived record blocks no open");

        assert_eq!(
            unbinding.remove_unbound(&store).expect("remove"),
            UnboundLease::Recorded {
                lease: lease.clone()
            }
        );
        assert!(lease.is_file(), "the lease beside a record was removed");
        assert!(record_path(&store, "acme/widget").is_file());
        assert!(held(&lease), "the unbinding gave up its shared hold");
        drop(unbinding);
        assert!(!held(&lease));
    }

    /// The unbinding holds the lease exclusive, a reader has already opened that inode, and the
    /// unbinding removes it before the reader locks: the reader must hold the lease at the path,
    /// which is the one a collector meets, never the removed inode. Whether the unbinding still
    /// holds the removed inode when the reader locks it (refused) or has let go (locked), and
    /// whether another reader has made the lease anew at the path by then.
    #[test]
    fn an_opener_that_reached_a_removed_lease_takes_the_one_at_its_path() {
        use std::os::unix::fs::MetadataExt as _;

        for (still_held, recreated) in [(true, false), (true, true), (false, false), (false, true)]
        {
            let root = TempRoot::new("removed-lease-race");
            let store_path = store(&root);
            let widget = repo("acme/widget");
            let lease_at = lease_path(&store_path, "acme/widget");
            let store = Store::open(&store_path).expect("store");
            let names = Names::of(&widget);
            let owner = store.retirement_owner(&names).expect("owner");
            let removing = open_lock(&owner, &names.lease).expect("unbinding's lease");
            removing.try_lock().expect("unbinding upgrades");
            let mut reached = Some(open_lock(&owner, &names.lease).expect("reader opens"));
            let removed = reached
                .as_ref()
                .expect("reached")
                .metadata()
                .expect("inode");
            fs::remove_file(&lease_at).expect("the unbinding removes the lease");
            let first = recreated
                .then(|| acquire_project_reader(&store_path, &widget).expect("a new reader"));
            if !still_held {
                drop(removing);
            }

            let mut opens = 0;
            let (_, lease) = store
                .lock_lease(
                    &names,
                    LeaseMode::Shared,
                    || store.retirement_owner(&names),
                    |directory, name| {
                        opens += 1;
                        reached
                            .take()
                            .map_or_else(|| open_lock(directory, name), Ok)
                    },
                )
                .expect("lock")
                .expect("the lease at the path is free");
            let case = format!("still held {still_held}, recreated {recreated}");
            assert_eq!(opens, 2, "{case}: the removed inode was not reopened");
            let at_path = fs::symlink_metadata(&lease_at).expect("lease at its path");
            let locked = lease.metadata().expect("locked inode");
            assert_eq!(
                (locked.dev(), locked.ino()),
                (at_path.dev(), at_path.ino()),
                "{case}"
            );
            assert_ne!(locked.ino(), removed.ino(), "{case}");
            assert!(
                held(&lease_at),
                "{case}: a collector would not meet this reader"
            );
            drop(first);
        }
    }

    #[test]
    fn a_pending_record_refuses_every_new_reader_and_is_discovered_without_a_checkout() {
        let root = TempRoot::new("retire-fence");
        let store = store(&root);
        let widget = repo("acme/widget");
        let gadget = repo("acme/gadget");
        plant_project(&store, "acme/widget", &registration("acme/widget"));
        plant_project(&store, "acme/gadget", &registration("acme/gadget"));

        let intent = RetirementIntent::authorize(&store, &widget).expect("authorize");
        assert!(retirement_blocks_open(&store, &widget).expect("fenced"));
        for refusal in [
            acquire_project_reader(&store, &widget).map(|_| ()),
            require_project_active(&store, &widget),
        ] {
            let refusal = refusal.expect_err("fenced");
            assert_eq!(refusal.code, ErrorCode::Conflict);
            assert_eq!(refusal.hint, "cowshed forget acme/widget --abandon");
        }
        require_project_active(&store, &gadget).expect("other project open");
        acquire_project_reader(&store, &gadget).expect("other project reader");
        assert_eq!(
            outstanding_retirements(&store).expect("discover"),
            vec![intent.clone()]
        );
        assert_eq!(
            RetirementIntent::authorize(&store, &widget).expect("retry resumes"),
            intent
        );
    }

    #[test]
    fn persist_advances_one_phase_of_its_own_operation_and_never_records_archived() {
        let root = TempRoot::new("retire-persist");
        let store = store(&root);
        let widget = repo("acme/widget");
        let tree = plant_project(&store, "acme/widget", &registration("acme/widget"));
        let intent = RetirementIntent::authorize(&store, &widget).expect("authorize");

        let mut skipping = intent.clone();
        skipping.phase = RetirementPhase::Detached;
        assert_eq!(
            skipping.persist(&store).expect_err("skip").code,
            ErrorCode::Conflict
        );
        let mut archived = intent.clone();
        archived.phase = RetirementPhase::Archived;
        assert_eq!(
            archived.persist(&store).expect_err("archived").code,
            ErrorCode::Internal
        );
        let mut foreign = intent.clone();
        foreign.operation_id = Uuid::new_v4();
        foreign.phase = RetirementPhase::Drained;
        assert_eq!(
            foreign.persist(&store).expect_err("foreign").code,
            ErrorCode::Conflict
        );

        let before = snapshot(&tree);
        let collector = acquire_collector(&store, &widget).expect("collector");
        let mut early = intent.clone();
        assert_eq!(
            early
                .archive_project(&store, &collector)
                .expect_err("not detached")
                .code,
            ErrorCode::Conflict
        );
        assert_eq!(snapshot(&tree), before);
        assert_eq!(
            RetirementIntent::load(&store, &widget).expect("load"),
            Some(intent)
        );
    }

    #[test]
    fn a_registration_naming_another_identity_refuses() {
        let root = TempRoot::new("retire-foreign");
        let store = store(&root);
        let widget = repo("acme/widget");
        let tree = plant_project(&store, "acme/widget", &registration("other/thing"));
        let before = snapshot(&tree);
        assert_eq!(
            RetirementIntent::authorize(&store, &widget)
                .expect_err("foreign identity")
                .code,
            ErrorCode::Integrity
        );
        assert_eq!(RetirementIntent::load(&store, &widget).expect("load"), None);
        assert_eq!(snapshot(&tree), before);
    }

    #[test]
    fn a_registration_changed_after_authorization_refuses_to_move() {
        let root = TempRoot::new("retire-digest");
        let store = store(&root);
        let widget = repo("acme/widget");
        let tree = plant_project(&store, "acme/widget", &registration("acme/widget"));
        let mut intent = RetirementIntent::authorize(&store, &widget).expect("authorize");
        let collector = acquire_collector(&store, &widget).expect("collector");
        intent.phase = RetirementPhase::Drained;
        intent.persist(&store).expect("drained");
        intent.phase = RetirementPhase::Detached;
        intent.persist(&store).expect("detached");
        // Same identity, other bytes: not the registration the operator abandoned.
        let mut changed = registration("acme/widget");
        changed.push(b'\n');
        intent
            .verify_registration(&store)
            .expect("unchanged before");
        fs::write(tree.join(REPOSITORY_BINDING_FILE), &changed).expect("change");
        assert_eq!(
            intent
                .verify_registration(&store)
                .expect_err("changed during drain")
                .code,
            ErrorCode::Integrity
        );
        let before = snapshot(&tree);

        assert_eq!(
            intent
                .archive_project(&store, &collector)
                .expect_err("digest")
                .code,
            ErrorCode::Integrity
        );
        drop(collector);
        assert_eq!(
            RetirementIntent::authorize(&store, &widget)
                .expect_err("resume checks the digest")
                .code,
            ErrorCode::Integrity
        );
        assert_eq!(snapshot(&tree), before);
        assert!(!intent.archive_path(&store).exists());
        assert_eq!(intent.phase, RetirementPhase::Detached);
    }

    #[test]
    fn symlinked_project_or_retirement_paths_refuse_without_touching_their_targets() {
        let root = TempRoot::new("retire-symlink");
        let store = store(&root);
        let widget = repo("acme/widget");
        let elsewhere = root.join("elsewhere");
        fs::create_dir(&elsewhere).expect("elsewhere");
        let outside = plant_project(&elsewhere, "acme/widget", &registration("acme/widget"));
        let before = snapshot(&outside);
        fs::create_dir(store.join("acme")).expect("owner");
        std::os::unix::fs::symlink(&outside, store.join("acme/widget")).expect("plant link");

        assert_eq!(
            RetirementIntent::authorize(&store, &widget)
                .expect_err("linked project")
                .code,
            ErrorCode::Integrity
        );
        assert_eq!(RetirementIntent::load(&store, &widget).expect("load"), None);
        assert_eq!(snapshot(&outside), before);

        // The first half left a real `.retirements`; the plant replaces it.
        fs::remove_dir_all(store.join(RETIREMENTS_NAMESPACE)).expect("clear retirements");
        let decoy = root.join("decoy");
        fs::create_dir(&decoy).expect("decoy");
        std::os::unix::fs::symlink(&decoy, store.join(RETIREMENTS_NAMESPACE)).expect("link");
        assert_eq!(
            acquire_project_reader(&store, &widget)
                .expect_err("linked retirements")
                .code,
            ErrorCode::Integrity
        );
        assert!(RetirementIntent::load(&store, &widget).is_err());
        assert!(fs::read_dir(&decoy).expect("decoy").next().is_none());
    }

    #[test]
    fn archiving_preserves_the_whole_tree_exactly_and_leaves_other_projects_alone() {
        let root = TempRoot::new("retire-archive");
        let store = store(&root);
        let widget = repo("acme/widget");
        let gadget = repo("acme/gadget");
        let tree = plant_project(&store, "acme/widget", &registration("acme/widget"));
        let other = plant_project(&store, "acme/gadget", &registration("acme/gadget"));
        let before = snapshot(&tree);
        let other_before = snapshot(&other);
        // A live actor of another project does not hold this one.
        let other_reader = acquire_project_reader(&store, &gadget).expect("other actor");

        let intent = retire(&store, &widget);
        assert_eq!(intent.phase, RetirementPhase::Archived);
        let archive = intent.archive_path(&store);
        assert_eq!(
            archive,
            store
                .join(RETIRED_PROJECTS_NAMESPACE)
                .join(intent.operation_id.hyphenated().to_string())
        );
        assert_eq!(intent.project_path(&store), tree);
        assert!(!tree.exists());
        assert_eq!(snapshot(&archive), before);
        intent
            .verify_registration(&store)
            .expect("archive carries the registration");
        let copy = fs::read(
            store
                .join(RETIRED_PROJECTS_NAMESPACE)
                .join(format!("{}.json", intent.operation_id.hyphenated())),
        )
        .expect("final record copy");
        assert_eq!(decode(&copy, &archive).expect("decode"), intent);
        assert_eq!(
            RetirementIntent::load(&store, &widget).expect("load"),
            Some(intent.clone())
        );
        assert!(!retirement_blocks_open(&store, &widget).expect("archived"));
        assert!(
            outstanding_retirements(&store)
                .expect("discover")
                .is_empty()
        );
        assert_eq!(
            RetirementIntent::authorize(&store, &widget).expect("completed result"),
            intent
        );

        drop(other_reader);
        assert_eq!(snapshot(&other), other_before);
        assert!(!retirement_blocks_open(&store, &gadget).expect("other"));
    }

    #[test]
    fn a_malformed_registration_is_preserved_as_its_raw_bytes() {
        let root = TempRoot::new("retire-malformed");
        let store = store(&root);
        let widget = repo("acme/widget");
        let raw = b"{\"version\": 1, \"identities\": [ torn".as_slice();
        let tree = plant_project(&store, "acme/widget", raw);
        let before = snapshot(&tree);
        let intent = retire(&store, &widget);
        assert_eq!(intent.registration_sha256, Sha256Digest::compute(raw));
        assert_eq!(snapshot(&intent.archive_path(&store)), before);
    }

    #[test]
    fn a_retry_after_the_rename_landed_finishes_without_moving_anything_again() {
        let root = TempRoot::new("retire-crash");
        let store = store(&root);
        let widget = repo("acme/widget");
        let tree = plant_project(&store, "acme/widget", &registration("acme/widget"));
        let before = snapshot(&tree);
        let mut intent = RetirementIntent::authorize(&store, &widget).expect("authorize");
        let collector = acquire_collector(&store, &widget).expect("collector");
        intent.phase = RetirementPhase::Drained;
        intent.persist(&store).expect("drained");
        intent.phase = RetirementPhase::Detached;
        intent.persist(&store).expect("detached");
        // The crash: the whole-tree rename landed, the phase write did not.
        fs::create_dir(store.join(RETIRED_PROJECTS_NAMESPACE)).expect("retired");
        fs::rename(&tree, intent.archive_path(&store)).expect("rename landed");
        drop(collector);

        assert!(retirement_blocks_open(&store, &widget).expect("still pending"));
        let mut resumed = RetirementIntent::authorize(&store, &widget).expect("retry resumes");
        assert_eq!(resumed, intent);
        let collector = acquire_collector(&store, &widget).expect("collector again");
        resumed
            .archive_project(&store, &collector)
            .expect("finish archive");
        assert_eq!(resumed.phase, RetirementPhase::Archived);
        assert_eq!(snapshot(&resumed.archive_path(&store)), before);
        assert!(!tree.exists());
        assert!(!retirement_blocks_open(&store, &widget).expect("archived"));
    }

    #[test]
    fn one_run_executes_a_retirement_and_a_stale_intent_is_refused() {
        let root = TempRoot::new("retire-run");
        let store = store(&root);
        let widget = repo("acme/widget");
        plant_project(&store, "acme/widget", &registration("acme/widget"));
        let intent = RetirementIntent::authorize(&store, &widget).expect("authorize");

        let run = acquire_retirement_run(&store, &intent).expect("run");
        assert_eq!(run.operation_id(), intent.operation_id);
        assert_eq!(run.repo_id(), &widget);
        assert_eq!(
            acquire_retirement_run(&store, &intent)
                .expect_err("second run")
                .code,
            ErrorCode::Conflict
        );
        let mut advanced = intent.clone();
        advanced.phase = RetirementPhase::Drained;
        advanced.persist(&store).expect("drained");
        drop(run);

        assert_eq!(
            acquire_retirement_run(&store, &intent)
                .expect_err("stale intent")
                .code,
            ErrorCode::Conflict
        );
        acquire_retirement_run(&store, &advanced).expect("current intent");

        let unrecorded = repo("acme/gadget");
        let mut foreign = intent.clone();
        foreign.repo_id = unrecorded;
        assert_eq!(
            acquire_retirement_run(&store, &foreign)
                .expect_err("no record")
                .code,
            ErrorCode::Integrity
        );
    }

    #[test]
    fn a_project_adopted_again_after_archival_is_a_new_retirement() {
        let root = TempRoot::new("retire-readopt");
        let store = store(&root);
        let widget = repo("acme/widget");
        plant_project(&store, "acme/widget", &registration("acme/widget"));
        let first = retire(&store, &widget);
        let first_archive = snapshot(&first.archive_path(&store));

        plant_project(&store, "acme/widget", &registration("acme/widget"));
        let second = RetirementIntent::authorize(&store, &widget).expect("new retirement");
        assert_ne!(second.operation_id, first.operation_id);
        assert_eq!(second.phase, RetirementPhase::Fenced);
        assert_eq!(snapshot(&first.archive_path(&store)), first_archive);
        assert!(
            store
                .join(RETIRED_PROJECTS_NAMESPACE)
                .join(format!("{}.json", first.operation_id.hyphenated()))
                .is_file()
        );
    }
}
