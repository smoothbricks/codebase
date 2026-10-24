//! Source-held authority for recovery exports. Destination documents report custody; they
//! cannot authorize it. The existing lifecycle journal owns every mutable transition.
use super::*;
use crate::storage::recovery::{LIFECYCLE_INTENTS_FILE, LifecycleIntentJournal};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryExport {
    pub(super) admission: Manifest,
    pub(super) progress: Manifest,
    pub(super) directory: PathBuf,
    manifest_identity: Option<CopyFileIdentity>,
    proof_identity: Option<CopyFileIdentity>,
    retirement_directory: Option<RetirementDirectory>,
    retirements: Vec<Retirement>,
    complete: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetirementDirectory {
    name: String,
    identity: DirectoryIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "camelCase", deny_unknown_fields)]
enum Retirement {
    Original,
    Staging,
    Staged { identity: CopyFileIdentity },
    Retired,
}

impl RecoveryExport {
    pub(super) fn new(admission: Manifest, directory: PathBuf) -> Self {
        Self {
            retirements: vec![Retirement::Original; admission.files.len()],
            progress: admission.clone(),
            admission,
            directory,
            manifest_identity: None,
            proof_identity: None,
            retirement_directory: None,
            complete: false,
        }
    }

    pub(crate) fn validate(&self, source: &Path) -> Result<()> {
        let invalid = || {
            transfer_failure(
                "validate source-held authority",
                source,
                &self.directory,
                "recovery journal record has inconsistent admission, progress or retirement custody",
            )
        };
        let Some(admitted) = &self.admission.copy else {
            return Err(invalid());
        };
        let Some(progress) = &self.progress.copy else {
            return Err(invalid());
        };
        let mut identity = self.progress.clone();
        identity.copy = self.admission.copy.clone();
        if !source.is_absolute()
            || !self.directory.is_absolute()
            || self.admission.source != source
            || !self.admission.describes(
                &self.admission.repo_id,
                &self.admission.workspace,
                &self.admission.incarnation,
                source,
            )
            || !self.progress.describes(
                &self.admission.repo_id,
                &self.admission.workspace,
                &self.admission.incarnation,
                source,
            )
            || identity != self.admission
            || admitted.source_directory != progress.source_directory
            || admitted.destination_directory != progress.destination_directory
            || admitted
                .artifacts
                .iter()
                .zip(&progress.artifacts)
                .any(|(before, now)| {
                    !matches!(before.state, CopyState::Pending) || before.source != now.source
                })
            || self.retirements.len() != self.admission.files.len()
            || (self
                .retirements
                .iter()
                .any(|state| !matches!(state, Retirement::Original))
                && (self.retirement_directory.is_none()
                    || self.proof_identity.is_none()
                    || !self.published()))
            || (self.proof_identity.is_some()
                && (!self.published() || self.manifest_identity.is_none()))
            || (self.manifest_identity.is_none()
                && progress
                    .artifacts
                    .iter()
                    .any(|artifact| !matches!(artifact.state, CopyState::Pending)))
            || (self.complete
                && (!self
                    .retirements
                    .iter()
                    .all(|state| matches!(state, Retirement::Retired))
                    || self.proof_identity.is_none()))
        {
            return Err(invalid());
        }
        if let Some(retirement) = &self.retirement_directory {
            let prefix = format!(".{}.recovery-retire-", file_name(source)?);
            if !retirement
                .name
                .strip_prefix(&prefix)
                .and_then(|id| id.strip_suffix(".partial"))
                .is_some_and(|id| id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    fn published(&self) -> bool {
        self.progress.copy.as_ref().is_some_and(|copy| {
            copy.artifacts
                .iter()
                .all(|artifact| matches!(artifact.state, CopyState::Published { .. }))
        })
    }
}

pub(super) fn journal_path(project_root: &Path) -> PathBuf {
    project_root.join(LIFECYCLE_INTENTS_FILE)
}

pub(super) fn load_export(path: &Path, source: &Path) -> Result<Option<RecoveryExport>> {
    Ok(LifecycleIntentJournal::load(path)?
        .recovery_export(source)
        .cloned())
}

pub(super) fn save_export(
    path: &Path,
    previous: Option<&RecoveryExport>,
    next: &RecoveryExport,
) -> Result<()> {
    LifecycleIntentJournal::update(path, |journal| {
        journal.record_recovery_export(&next.admission.source, previous, next)
    })
    .map(|_| ())
}

fn require_document(
    directory: &fs::File,
    path: &Path,
    expected: &Manifest,
    identity: Option<&CopyFileIdentity>,
) -> Result<CopyFileIdentity> {
    let file =
        open_regular_at(directory, &held_name(&file_name(path)?, path)?).map_err(|error| {
            transfer_failure(
                "open immutable recovery document",
                &expected.source,
                path,
                error,
            )
        })?;
    let before = CopyFileIdentity::read(&file).map_err(|error| {
        transfer_failure("identify recovery document", &expected.source, path, error)
    })?;
    if let Some(identity) = identity {
        require_identity(&file, identity, &expected.source, path, "recovery document")?;
    }
    let actual: Manifest = serde_json::from_reader(&file).map_err(|error| {
        transfer_failure(
            "read immutable recovery document",
            &expected.source,
            path,
            error,
        )
    })?;
    if actual != *expected {
        return Err(transfer_failure(
            "admit recovery document",
            &expected.source,
            path,
            "destination document differs from the source-held record",
        ));
    }
    require_identity(&file, &before, &expected.source, path, "recovery document")?;
    Ok(before)
}

fn ensure_document(
    directory: &fs::File,
    path: &Path,
    expected: &Manifest,
    identity: Option<&CopyFileIdentity>,
) -> Result<CopyFileIdentity> {
    let name = held_name(&file_name(path)?, path)?;
    if !present_in(directory, &name, path)? {
        if identity.is_some() {
            return Err(transfer_failure(
                "admit recovery document",
                &expected.source,
                path,
                "previously admitted immutable document is missing",
            ));
        }
        save_manifest(directory, path, expected)?;
    }
    if identity.is_none() {
        open_regular_at(directory, &name)
            .and_then(|file| file.sync_all())
            .map_err(|error| {
                transfer_failure("sync admitted document", &expected.source, path, error)
            })?;
    }
    let identity = require_document(directory, path, expected, identity)?;
    cowshed_os::fsio::sync_held_directory(directory)
        .map_err(|error| io_failure("sync immutable document", path, &error))?;
    Ok(identity)
}

pub(super) fn ensure_admission(
    record: &mut RecoveryExport,
    directory: &fs::File,
    journal: &Path,
) -> Result<()> {
    let expected = &record
        .admission
        .copy
        .as_ref()
        .ok_or_else(|| CowshedError::internal("missing admitted copy directory"))?
        .destination_directory;
    let actual = DirectoryIdentity::read(directory)
        .map_err(|error| io_failure("identify admitted directory", &record.directory, &error))?;
    if actual != *expected {
        return Err(transfer_failure(
            "admit destination directory",
            &record.admission.source,
            &record.directory,
            format!("expected {expected:?}, observed {actual:?}"),
        ));
    }
    let identity = ensure_document(
        directory,
        &record.directory.join(MANIFEST_FILE),
        &record.admission,
        record.manifest_identity.as_ref(),
    )?;
    if record.manifest_identity.is_none() {
        let previous = record.clone();
        record.manifest_identity = Some(identity);
        save_export(journal, Some(&previous), record)?;
    }
    Ok(())
}

pub(super) fn ensure_copy_proof(
    record: &mut RecoveryExport,
    directory: &fs::File,
    journal: &Path,
) -> Result<()> {
    if !record.published() {
        return Err(CowshedError::internal(
            "copy proof before complete publication",
        ));
    }
    let identity = ensure_document(
        directory,
        &record.directory.join(COPY_PROOF_FILE),
        &record.progress,
        record.proof_identity.as_ref(),
    )?;
    if record.proof_identity.is_none() {
        let previous = record.clone();
        record.proof_identity = Some(identity);
        save_export(journal, Some(&previous), record)?;
    }
    Ok(())
}

fn open_retirement(record: &RecoveryExport, trash: &Path) -> Result<Option<fs::File>> {
    let Some(retirement) = &record.retirement_directory else {
        return Ok(None);
    };
    let path = trash.join(&retirement.name);
    match fs::symlink_metadata(&path) {
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                && record
                    .retirements
                    .iter()
                    .all(|state| matches!(state, Retirement::Retired)) =>
        {
            return Ok(None);
        }
        Err(error) => {
            return Err(io_failure(
                "inspect private retirement directory",
                &path,
                &error,
            ));
        }
        Ok(_) => {}
    }
    let held = open_directory(&path)?;
    let actual = DirectoryIdentity::read(&held)
        .map_err(|error| io_failure("identify retirement directory", &path, &error))?;
    if actual != retirement.identity {
        return Err(transfer_failure(
            "admit private retirement directory",
            &record.admission.source,
            &path,
            format!("expected {:?}, observed {actual:?}", retirement.identity),
        ));
    }
    Ok(Some(held))
}

pub(super) fn admit_export(
    record: &RecoveryExport,
    trash: &fs::File,
    destination: &fs::File,
    trash_path: &Path,
) -> Result<()> {
    let manifest_path = record.directory.join(MANIFEST_FILE);
    require_document(
        destination,
        &manifest_path,
        &record.admission,
        record.manifest_identity.as_ref(),
    )?;
    let proof_path = record.directory.join(COPY_PROOF_FILE);
    if record.proof_identity.is_some()
        || present_in(
            destination,
            &held_name(COPY_PROOF_FILE, &proof_path)?,
            &proof_path,
        )?
    {
        if !record.published() {
            return Err(transfer_failure(
                "admit copy proof",
                &record.admission.source,
                &proof_path,
                "destination claims completion before the source journal published every artifact",
            ));
        }
        require_document(
            destination,
            &proof_path,
            &record.progress,
            record.proof_identity.as_ref(),
        )?;
    }
    super::admit_transfer(
        &record.progress,
        trash,
        destination,
        trash_path,
        &record.directory,
        &manifest_path,
    )?;
    let retired = open_retirement(record, trash_path)?;
    let copy = record
        .progress
        .copy
        .as_ref()
        .ok_or_else(|| CowshedError::internal("missing copy authority"))?;
    for ((name, artifact), state) in record
        .progress
        .files
        .iter()
        .zip(&copy.artifacts)
        .zip(&record.retirements)
    {
        let from = trash_path.join(name);
        let into = record.directory.join(name);
        let entry = held_name(name, &manifest_path)?;
        let original_exists = present_in(trash, &entry, &from)?;
        let staged_exists = match &retired {
            Some(held) => present_in(held, &entry, &from)?,
            None => false,
        };
        match state {
            Retirement::Original => {
                let source = open_regular_at(trash, &entry).map_err(|error| {
                    transfer_failure("admit original source", &from, &into, error)
                })?;
                require_identity(&source, &artifact.source, &from, &into, "source")?;
                if staged_exists {
                    return Err(transfer_failure(
                        "admit original source",
                        &from,
                        &into,
                        "unexpected retirement entry",
                    ));
                }
            }
            Retirement::Staging => {
                if original_exists == staged_exists {
                    return Err(transfer_failure(
                        "admit staging source",
                        &from,
                        &into,
                        "expected exactly one original or retirement entry",
                    ));
                }
                if staged_exists {
                    let held = retired
                        .as_ref()
                        .ok_or_else(|| CowshedError::internal("missing retirement directory"))?;
                    let staged = open_regular_at(held, &entry).map_err(|error| {
                        transfer_failure("admit staged source", &from, &into, error)
                    })?;
                    let actual = CopyFileIdentity::read(&staged).map_err(|error| {
                        transfer_failure("identify staged source", &from, &into, error)
                    })?;
                    if !artifact.source.after_rename(&actual) {
                        return Err(transfer_failure(
                            "admit staged source",
                            &from,
                            &into,
                            format!("expected {:?}, observed {actual:?}", artifact.source),
                        ));
                    }
                }
            }
            Retirement::Staged { identity } => {
                if original_exists {
                    return Err(transfer_failure(
                        "admit staged source",
                        &from,
                        &into,
                        "original name was occupied after staging",
                    ));
                }
                if let Some(held) = &retired
                    && staged_exists
                {
                    let staged = open_regular_at(held, &entry).map_err(|error| {
                        transfer_failure("open staged source", &from, &into, error)
                    })?;
                    require_identity(&staged, identity, &from, &into, "retirement source")?;
                }
            }
            Retirement::Retired => {
                if original_exists || staged_exists {
                    return Err(transfer_failure(
                        "admit retired source",
                        &from,
                        &into,
                        "a retired source name is occupied",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn prepare_retirement(
    record: &mut RecoveryExport,
    trash: &fs::File,
    trash_path: &Path,
    journal: &Path,
) -> Result<fs::File> {
    if let Some(held) = open_retirement(record, trash_path)? {
        return Ok(held);
    }
    if record.retirement_directory.is_some() {
        return Err(CowshedError::internal(
            "completed retirement directory requested again",
        ));
    }
    let name = format!(
        ".{}.recovery-retire-{}.partial",
        file_name(&record.admission.source)?,
        uuid::Uuid::new_v4().simple()
    );
    let entry = held_name(&name, journal)?;
    let (held, created) =
        cowshed_os::fsio::open_or_create_child_directory(trash, &entry).map_err(|error| {
            io_failure(
                "create private retirement directory",
                &trash_path.join(&name),
                &error,
            )
        })?;
    if !created {
        return Err(occupied(&trash_path.join(&name)));
    }
    cowshed_os::fsio::sync_held_directory(trash)
        .map_err(|error| io_failure("sync retirement directory", trash_path, &error))?;
    let identity = DirectoryIdentity::read(&held).map_err(|error| {
        io_failure(
            "identify retirement directory",
            &trash_path.join(&name),
            &error,
        )
    })?;
    let previous = record.clone();
    record.retirement_directory = Some(RetirementDirectory { name, identity });
    save_export(journal, Some(&previous), record)?;
    Ok(held)
}

fn mark_retirement(
    record: &mut RecoveryExport,
    index: usize,
    state: Retirement,
    journal: &Path,
) -> Result<()> {
    let previous = record.clone();
    record.retirements[index] = state;
    save_export(journal, Some(&previous), record)
}

pub(super) fn retire_sources(
    record: &mut RecoveryExport,
    trash: &fs::File,
    destination: &fs::File,
    trash_path: &Path,
    project_root: &Path,
    observe: &mut impl FnMut(CopyCheckpoint, &Path) -> Result<()>,
) -> Result<()> {
    let journal = journal_path(project_root);
    if !record.published() || record.proof_identity.is_none() {
        return Err(CowshedError::internal(
            "retirement without complete durable copy proof",
        ));
    }
    if record.complete {
        return Ok(());
    }
    if record
        .retirements
        .iter()
        .any(|state| !matches!(state, Retirement::Retired))
    {
        let held = prepare_retirement(record, trash, trash_path, &journal)?;
        for index in 0..record.progress.files.len() {
            if matches!(record.retirements[index], Retirement::Retired) {
                continue;
            }
            let name = record.progress.files[index].clone();
            let from = trash_path.join(&name);
            let into = record.directory.join(&name);
            let entry = held_name(&name, &journal)?;
            let artifact = record
                .progress
                .copy
                .as_ref()
                .ok_or_else(|| CowshedError::internal("missing copy proof"))?
                .artifacts[index]
                .clone();
            let CopyState::Published { target, .. } = &artifact.state else {
                return Err(CowshedError::internal("unpublished retirement"));
            };
            let copied = open_regular_at(destination, &entry).map_err(|error| {
                transfer_failure("open destination before retirement", &from, &into, error)
            })?;
            require_identity(&copied, target, &from, &into, "destination")?;
            require_document(
                destination,
                &record.directory.join(MANIFEST_FILE),
                &record.admission,
                record.manifest_identity.as_ref(),
            )?;
            require_document(
                destination,
                &record.directory.join(COPY_PROOF_FILE),
                &record.progress,
                record.proof_identity.as_ref(),
            )?;
            if matches!(record.retirements[index], Retirement::Original) {
                mark_retirement(record, index, Retirement::Staging, &journal)?;
            }
            if matches!(record.retirements[index], Retirement::Staging) {
                if !present_in(&held, &entry, &from)? {
                    let original = open_regular_at(trash, &entry).map_err(|error| {
                        transfer_failure("open source to stage", &from, &into, error)
                    })?;
                    require_identity(&original, &artifact.source, &from, &into, "source")?;
                    observe(CopyCheckpoint::BeforeSourceRetire, &from)?;
                    cowshed_os::fsio::rename_noreplace(
                        trash.as_raw_fd(),
                        &entry,
                        held.as_raw_fd(),
                        &entry,
                    )
                    .map_err(|error| {
                        transfer_failure("stage source retirement", &from, &into, error)
                    })?;
                    cowshed_os::fsio::sync_held_directory(&held).map_err(|error| {
                        transfer_failure("sync staged source", &from, &into, error)
                    })?;
                    cowshed_os::fsio::sync_held_directory(trash).map_err(|error| {
                        transfer_failure("sync source staging", &from, &into, error)
                    })?;
                }
                let staged = open_regular_at(&held, &entry)
                    .map_err(|error| transfer_failure("open staged source", &from, &into, error))?;
                let actual = CopyFileIdentity::read(&staged).map_err(|error| {
                    transfer_failure("identify staged source", &from, &into, error)
                })?;
                if !artifact.source.after_rename(&actual) {
                    let restored = cowshed_os::fsio::rename_noreplace(
                        held.as_raw_fd(),
                        &entry,
                        trash.as_raw_fd(),
                        &entry,
                    );
                    cowshed_os::fsio::sync_held_directory(&held).map_err(|error| {
                        transfer_failure("sync preserved foreign entry", &from, &into, error)
                    })?;
                    cowshed_os::fsio::sync_held_directory(trash).map_err(|error| {
                        transfer_failure("sync preserved source name", &from, &into, error)
                    })?;
                    return Err(transfer_failure(
                        "validate moved source",
                        &from,
                        &into,
                        format!(
                            "expected {:?}, observed {actual:?}; foreign entry restore: {restored:?}; private custody {:?}",
                            artifact.source, record.retirement_directory
                        ),
                    ));
                }
                mark_retirement(
                    record,
                    index,
                    Retirement::Staged { identity: actual },
                    &journal,
                )?;
                observe(CopyCheckpoint::RetirementStaged, &from)?;
            }
            if present_in(trash, &entry, &from)? {
                return Err(transfer_failure(
                    "retire staged source",
                    &from,
                    &into,
                    "original source name was occupied after staging",
                ));
            }
            if present_in(&held, &entry, &from)? {
                let Retirement::Staged { identity } = &record.retirements[index] else {
                    return Err(CowshedError::internal("unstaged source unlink"));
                };
                let staged = open_regular_at(&held, &entry).map_err(|error| {
                    transfer_failure("open owned staged source", &from, &into, error)
                })?;
                require_identity(&staged, identity, &from, &into, "owned staged source")?;
                require_identity(&copied, target, &from, &into, "destination")?;
                // SAFETY: only the validated entry inside the operation-owned private directory is removed.
                if unsafe { libc::unlinkat(held.as_raw_fd(), entry.as_ptr(), 0) } != 0 {
                    return Err(transfer_failure(
                        "retire owned staged source",
                        &from,
                        &into,
                        io::Error::last_os_error(),
                    ));
                }
                cowshed_os::fsio::sync_held_directory(&held).map_err(|error| {
                    transfer_failure("sync source retirement", &from, &into, error)
                })?;
                deletion_log::log_recovery(
                    project_root,
                    record.progress.workspace.as_str(),
                    &record.progress.source,
                    &from,
                    artifact_kind(&record.progress.source, &from),
                    &into,
                );
            }
            mark_retirement(record, index, Retirement::Retired, &journal)?;
            observe(CopyCheckpoint::Retired, &from)?;
        }
    }
    admit_export(record, trash, destination, trash_path)?;
    if let Some(held) = open_retirement(record, trash_path)? {
        let retirement = record
            .retirement_directory
            .as_ref()
            .ok_or_else(|| CowshedError::internal("missing retirement directory identity"))?;
        let name = held_name(&retirement.name, &journal)?;
        let actual = DirectoryIdentity::read(&held).map_err(|error| {
            io_failure("identify empty retirement directory", trash_path, &error)
        })?;
        if actual != retirement.identity {
            return Err(occupied(&trash_path.join(&retirement.name)));
        }
        // SAFETY: AT_REMOVEDIR removes only this empty private directory, never file contents.
        if unsafe { libc::unlinkat(trash.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
            return Err(io_failure(
                "remove empty retirement directory",
                &trash_path.join(&retirement.name),
                &io::Error::last_os_error(),
            ));
        }
        cowshed_os::fsio::sync_held_directory(trash)
            .map_err(|error| io_failure("sync retirement directory removal", trash_path, &error))?;
    }
    let previous = record.clone();
    record.complete = true;
    save_export(&journal, Some(&previous), record)
}
