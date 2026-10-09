//! `cowshed adopt <checkout> --re-adopt` (02_workspaces.md, "Re-adoption: format transition and
//! space reclamation"): replace an adopted main through the same staged copy, verification,
//! publication fence and checkout handoff as adopt, from its retained image, keeping its build
//! volume's identity.
//!
//! Every input the transaction needs is recorded in the `ReAdopt` lifecycle intent before its
//! first mutation, in the same journal update that marks it mutating, so a replay never reads the
//! checkout (which is an empty mountpoint for most of the transaction) and never re-derives a
//! path: it converges from the recorded paths and what is on disk and in the kernel.

use std::path::{Path, PathBuf};

use super::{NativeProjectRuntimeHost, close_checkout_nx, main_name, native_integrity_error};
use super::{WorkspaceSnapshot, native_staged_error, native_storage_error};
use crate::api::dto::AdoptOptions;
use crate::build_volume::BuildVolumeId;
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::{IMAGE_EXTENSION, WorkspaceName};
use crate::storage::recovery::{
    LifecycleIntent, RETAINED_NAMESPACE, ReAdoptBuild, ReAdoptImage, ReAdoptRetained,
    ReAdoptSource, STAGING_NAMESPACE,
};

impl NativeProjectRuntimeHost {
    /// Re-adopt main: gate, record, then run the replacement (step 1 of the spec, then 2-5).
    pub(super) async fn re_adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceSnapshot> {
        let main = main_name();
        // A re-adoption already under way resumes from its record, whatever this call asked. A
        // completed one is not resumed: a further `--re-adopt` is a new explicit reclaim, and its
        // retained paths carry the new incarnation, so it never replaces the earlier evidence.
        if let Some(record) = self.lifecycle_intents.get(&main)
            && record.completion.is_none()
            && let LifecycleIntent::ReAdopt { source, .. } = &record.operation
        {
            let source = source.as_ref().clone();
            return self.resume_re_adopt(source).await;
        }
        self.validate_binding().await?;
        let checkout = self.descriptor.git_root.to_path_buf();
        let replaced = self.replaced_main(&checkout).await?;
        // Step 1: the caller is outside the checkout, the source has no holders, and nothing is
        // ended to make it so.
        let cwd = std::env::current_dir().map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot read the working directory: {error}"),
                "retry from an existing directory",
            )
        })?;
        if cwd.starts_with(&checkout) {
            return Err(CowshedError::usage(
                format!(
                    "re-adoption replaces the volume at {}, which this shell's working directory is inside",
                    checkout.display()
                ),
                format!("cd out of {} and retry", checkout.display()),
            ));
        }
        // Admission closes only if main runs no job; a running job refuses, and nothing ends it.
        // A legacy main's supervisor, if any, is of another build and cannot prove itself idle
        // here: nothing of it is stopped, and the holder gate below refuses, naming its
        // processes, while anything still works in its volume.
        // The retired supervisor's socket lease is held until the re-adoption is recorded, so no
        // replacement supervisor starts on the source while it is measured.
        let supervisor = if replaced.legacy {
            None
        } else {
            self.ensure_supervisor(&main)
                .await?
                .quiesce_if_idle()
                .await
                .map_err(|error| match error.code {
                    ErrorCode::Conflict => CowshedError::conflict(
                        "main cannot be re-adopted while it runs jobs",
                        "let main's jobs finish, then retry",
                    ),
                    _ => error,
                })?;
            let lease = self.stop_supervisor(&main).await?;
            if let Err(busy) = close_checkout_nx(&main, &checkout).await? {
                return Err(CowshedError::conflict(
                    format!("main cannot be re-adopted while its Nx state is in use: {busy}"),
                    "stop those processes, then retry",
                ));
            }
            Some(lease)
        };
        let volumes = self.build_volumes()?;
        let build = volumes.layout.linked(&checkout)?;
        let mut held = vec![checkout.clone()];
        if let Some(id) = &build {
            held.push(volumes.layout.mount(id));
        }
        for mount in &held {
            let holders = crate::build_volume::nx::volume_holders(mount).map_err(|error| {
                CowshedError::environment_missing(
                    format!("cannot list what holds {}: {error}", mount.display()),
                    "cowshed doctor --json",
                )
            })?;
            if !holders.is_empty() {
                let named = holders
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(CowshedError::conflict(
                    format!(
                        "{} is in use by {named}; no running job or user process was stopped and \
                         nothing was forced",
                        mount.display()
                    ),
                    "close those processes, then retry",
                ));
            }
        }
        let source = self
            .record_re_adopt_source(&replaced, &checkout, build.as_ref())
            .await?;
        let intent = LifecycleIntent::ReAdopt {
            options,
            source: Box::new(source.clone()),
        };
        // Recorded and marked mutating in one journal update: the record exists before the
        // first effect, and there is never a prepared-but-unrecorded re-adoption to discard.
        if !self.claim_intent_lease(&main)? {
            return Err(super::another_process_is_running(&main));
        }
        // Recovery can locate this exact project after the source and its sidecar leave the
        // canonical names, before a PendingFence replacement exists or Git is mounted.
        let recorded_layout = self.layout.clone();
        let recorded_checkout = checkout.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            recorded_layout.record_checkout_root(&recorded_checkout)
        })
        .await
        .map_err(|error| CowshedError::internal(error.to_string()))?
        .map_err(native_integrity_error)?;
        self.update_lifecycle_intents(move |journal| {
            journal.begin(intent);
            journal.mark_mutating(&main_name())
        })
        .await?;
        // Recorded: the replacement retires main's supervisor again under its own lease, as a
        // replay does, so the gate's lease ends here.
        drop(supervisor);
        self.resume_re_adopt(source).await
    }

    /// Everything step 1 records: exact paths, kernel capacities, the Git state and the build
    /// volume's identity and owner record, all while the source is quiet.
    async fn record_re_adopt_source(
        &self,
        replaced: &ReplacedMain,
        checkout: &Path,
        build: Option<&BuildVolumeId>,
    ) -> Result<ReAdoptSource> {
        let old = replaced.metadata.workspace_incarnation.clone();
        let replacement = {
            use crate::storage::apfs::IncarnationSource as _;
            crate::storage::apfs::UuidIncarnationSource
                .mint()
                .map_err(native_storage_error)?
        };
        let project = self.layout.project().project_root.clone();
        let retained = project.join(RETAINED_NAMESPACE);
        let staging = self.layout.project().mount_root.join(STAGING_NAMESPACE);
        let images = self.build_volume_layout()?;
        let main_image = replaced.image.clone();
        let host = self.substrate.shared_host();
        let capacity_of = {
            let host = host.clone();
            move |image: PathBuf| -> Result<u64> {
                host.attached_capacity_bytes(&image)
                    .map_err(native_storage_error)
            }
        };
        let main_capacity = crate::storage::lifecycle::dispatch_blocking({
            let capacity_of = capacity_of.clone();
            let image = main_image.clone();
            move || capacity_of(image)
        })
        .await
        .map_err(|error| CowshedError::internal(error.to_string()))??;
        // The build images as they are on disk, in whichever format each was written: a main
        // adopted before RAW has its build volume and seeds in the same format as itself.
        let inputs = build_inputs(images.images())?;
        let build = match build {
            Some(id) => {
                let Some((_, image)) = inputs.iter().find(|(input, _)| input == id) else {
                    return Err(CowshedError::integrity(
                        format!(
                            "main's build link names volume {} with no image in {}",
                            id.as_str(),
                            images.images().display()
                        ),
                        "cowshed doctor --json",
                    ));
                };
                let image = image.clone();
                let capacity_bytes = crate::storage::lifecycle::dispatch_blocking({
                    let capacity_of = capacity_of.clone();
                    let image = image.clone();
                    move || capacity_of(image)
                })
                .await
                .map_err(|error| CowshedError::internal(error.to_string()))??;
                Some(ReAdoptBuild {
                    id: id.clone(),
                    owner_record: read_build_record(&image)?,
                    replacement_image: project
                        .join(STAGING_NAMESPACE)
                        .join(format!("readopt-{}-build", replacement.as_str()))
                        .with_extension(IMAGE_EXTENSION),
                    replacement_mount: staging
                        .join(format!("readopt-{}-build", replacement.as_str())),
                    image: ReAdoptImage {
                        retained: retained_beside(
                            &retained,
                            &format!("build-{}", id.as_str()),
                            &image,
                        ),
                        original: image,
                        source_mount: staging.join(format!("readopt-{}-build", old.as_str())),
                        capacity_bytes,
                    },
                })
            }
            None => None,
        };
        let mut old_seeds = Vec::new();
        for (id, original) in inputs {
            // An image with no record yet is an interrupted creation, never a seed.
            if read_build_record_present(&original)?
                .is_some_and(|record| record.is_seed_of(&WorkspaceName::main(), &old))
            {
                old_seeds.push(ReAdoptRetained {
                    retained: retained_beside(
                        &retained,
                        &format!("seed-{}", id.as_str()),
                        &original,
                    ),
                    original,
                });
            }
        }
        let git_before = {
            let checkout = checkout.to_owned();
            crate::storage::lifecycle::dispatch_blocking(move || {
                crate::git_preservation::capture(&checkout, &checkout)
            })
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))??
        };
        // A source whose own repository fails fsck has nothing a verified copy could preserve:
        // refuse before anything is recorded or detached.
        if !git_before.fsck_succeeded() {
            return Err(CowshedError::integrity(
                format!(
                    "git fsck fails in {}; re-adoption copies a repository only as it verifies",
                    checkout.display()
                ),
                "repair the repository, then retry",
            ));
        }
        Ok(ReAdoptSource {
            checkout: checkout.to_owned(),
            image: ReAdoptImage {
                retained: retained_beside(
                    &retained,
                    &format!("main-{}", old.as_str()),
                    &main_image,
                ),
                original: main_image,
                source_mount: staging.join(format!("readopt-{}-main", old.as_str())),
                capacity_bytes: main_capacity,
            },
            metadata: replaced.metadata.clone(),
            replacement,
            git_before,
            build,
            old_seeds,
        })
    }

    /// Steps 2-5 from the record, converging from whatever a crash left.
    pub(super) async fn resume_re_adopt(
        &mut self,
        source: ReAdoptSource,
    ) -> Result<WorkspaceSnapshot> {
        let source = std::sync::Arc::new(source);
        let main = main_name();
        let lock = self
            .layout
            .canonical_image(&main)
            .map_err(native_integrity_error)?
            .lock()
            .to_owned();
        let layout = self.build_volume_layout()?;
        // This is the build volume's job-admission resource claim, not a metadata mutex. It holds
        // the recorded old image's exact hold from here until the build publication below, across
        // `replace_main` and `finish_adoption`. A legacy job holds `<old>.asif.hold`, not the new
        // RAW-derived hold path.
        let build_claim = if let Some(build) = &source.build {
            let held_layout = layout.clone();
            let id = build.id.clone();
            let original = build.image.original.clone();
            let observed_hold = crate::metadata::append_suffix(&original, ".hold");
            self.substrate
                .dispatch_with_image_lock(lock.clone(), move || {
                    held_layout.claim_re_adopt_source(&id, &original)
                })
                .await
                .map_err(native_storage_error)?
                .map_err(|error| {
                    CowshedError::storage_failure(
                        format!(
                            "cannot claim source build hold {}: {error}",
                            observed_hold.display()
                        ),
                        &error,
                        "repair the recorded build hold and retry re-adoption",
                    )
                })?
                .ok_or_else(|| {
                    CowshedError::conflict(
                        format!(
                            "source build hold {} is still owned by a job",
                            observed_hold.display()
                        ),
                        "let that job finish, then retry re-adoption",
                    )
                })
                .map(Some)?
        } else {
            None
        };
        // Published already: only the build publication, verification and conclusion remain.
        let published = match self.current(&main).await {
            Ok(current) if current.derived.workspace.incarnation() == &source.replacement => {
                Some(current)
            }
            Ok(_) => None,
            Err(error) if error.code == ErrorCode::NotFound => None,
            Err(error) => return Err(error),
        };
        let workspace = match published {
            Some(current) => {
                self.substrate
                    .finish_adoption(
                        &current.derived.workspace,
                        &crate::storage::lifecycle::AdoptHandoff::Replacement {
                            copy_source: source.image.source_mount.clone(),
                            incarnation: source.replacement.clone(),
                        },
                    )
                    .await
                    .map_err(native_storage_error)?;
                current.derived.workspace
            }
            None => {
                self.replace_main(std::sync::Arc::clone(&source), lock.clone())
                    .await?
            }
        };
        // The replacement's build volume takes main's build identity, under main's lock. The
        // build claim moves into that publication and is released when its closure returns,
        // still under main's image lock: until then it keeps every new job hold on the recorded
        // old build volume out, and the lock is released only after the claim is gone. No build
        // volume means no claim and nothing to publish, so the publication is skipped.
        if let Some(build_claim) = build_claim {
            let source = source.clone();
            self.substrate
                .dispatch_on_host_with_image_lock(lock.clone(), move |host| {
                    host.publish_re_adopt_build(&layout, &source, &build_claim)
                })
                .await
                .map_err(native_storage_error)?;
        }
        // Step 5: the mounted replacement holds exactly the recorded Git state.
        // A published replay may begin after an earlier run released its staging mounts.
        // Reattach only the intent's retained inputs read-only before comparing the copy.
        {
            let retained = std::sync::Arc::clone(&source);
            self.substrate
                .dispatch_on_host_with_image_lock(lock.clone(), move |host| {
                    host.mount_re_adopt_sources(&retained)
                })
                .await
                .map_err(native_storage_error)?;
        }
        {
            let source = std::sync::Arc::clone(&source);
            crate::storage::lifecycle::dispatch_blocking(move || {
                crate::git_preservation::verify_copy(
                    &source.checkout,
                    &source.checkout,
                    &source.git_before,
                    &source.image.source_mount,
                )
            })
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))??;
        }
        {
            let source = source.clone();
            self.substrate
                .dispatch_on_host_with_image_lock(lock, move |host| {
                    host.release_re_adopt_sources(&source)
                })
                .await
                .map_err(native_storage_error)?;
        }
        let snapshot = self
            .conclude_adoption(&workspace, super::super::build_volumes::FirstMint::Nothing)
            .await?;
        let retained = retained_paths(&source);
        eprintln!(
            "cowshed: re-adopted main as {}; this re-adoption kept what it replaced at:",
            source.replacement.as_str()
        );
        for path in &retained {
            eprintln!("  {}", path.display());
        }
        eprintln!(
            "cowshed: once you have checked the replacement, removing exactly those paths returns \
             their space: rm -- {}",
            retained
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );
        Ok(snapshot)
    }

    /// Steps 2-4: retain the replaced inputs under main's lock, copy the build volume, then run
    /// the staged adopt from the retained main's read-only mount.
    async fn replace_main(
        &mut self,
        source: std::sync::Arc<ReAdoptSource>,
        lock: PathBuf,
    ) -> Result<crate::storage::lifecycle::LifecycleWorkspace> {
        use crate::storage::lifecycle::LifecyclePlanner;
        let main = main_name();
        // A RAW main's own supervisor is retired again here, as on a replay; a legacy main's is
        // never stopped by this build, and the retention's unforced unmount refuses, naming its
        // processes, while anything of it still works in the volume.
        let legacy = source
            .image
            .original
            .extension()
            .is_none_or(|extension| extension != IMAGE_EXTENSION);
        let _supervisor = if legacy {
            None
        } else {
            Some(self.stop_supervisor(&main).await?)
        };
        {
            let source = source.clone();
            self.substrate
                .dispatch_on_host_with_image_lock(lock.clone(), move |host| {
                    host.retain_re_adopt_source(&source)?;
                    host.mount_re_adopt_sources(&source)?;
                    host.copy_re_adopt_build(&source)
                })
                .await
                .map_err(native_storage_error)?;
        }
        let info = &source.metadata.info_snapshot;
        let identity = crate::storage::lifecycle::OperationIdentity {
            project_root: source.checkout.clone(),
            base_commit: source.git_before.head().as_str().to_owned(),
            created_at: info.created_at.clone(),
            branch: info.branch.clone(),
            forked_from: None,
            created_trace: source.replacement.as_str().to_owned(),
            grants: source.metadata.grants.clone(),
            git_worktree: false,
        };
        let plan = self
            .substrate
            .plan_adopt(crate::storage::lifecycle::AdoptRequest {
                repo: self.descriptor.repo_id.clone(),
                capacity: crate::metadata::ImageCapacity::from_bytes(source.image.capacity_bytes),
                topology_revision: crate::storage::lifecycle::Revision::new(0),
                source_checkout: source.checkout.clone(),
                handoff: crate::storage::lifecycle::AdoptHandoff::Replacement {
                    copy_source: source.image.source_mount.clone(),
                    incarnation: source.replacement.clone(),
                },
                identity,
            })
            .map_err(native_integrity_error)?;
        let evidence = std::sync::Arc::clone(&source);
        let host = self.substrate.shared_host();
        let replacement = self
            .layout
            .canonical_image(&main)
            .map_err(native_integrity_error)?
            .image()
            .to_owned();
        // Per-file hole preservation is proved by the copy verifier inside the staged copy.
        let (receipt, ()) = self
            .substrate
            .execute_adopt_staged(
                plan,
                crate::storage::apfs::Alongside::none(),
                // Step 4, before anything is published: the staged copy holds exactly the
                // recorded Git state, and its image holds its entries' blocks, not its capacity.
                move |stage| async move {
                    crate::storage::lifecycle::dispatch_blocking(move || {
                        crate::git_preservation::verify_copy(
                            &stage.mount_point,
                            &evidence.checkout,
                            &evidence.git_before,
                            &evidence.image.source_mount,
                        )?;
                        let (new, old) = host
                            .require_sparse_replacement(
                                &replacement,
                                &evidence.image.retained,
                                evidence.image.capacity_bytes,
                            )
                            .map_err(native_storage_error)?;
                        eprintln!(
                            "cowshed: re-adoption replacement allocates {new} bytes; the retained \
                             image {old}"
                        );
                        Ok::<(), CowshedError>(())
                    })
                    .await
                    .map_err(|error| CowshedError::internal(error.to_string()))?
                },
            )
            .await
            .map_err(native_staged_error)?;
        Ok(receipt.workspace)
    }

    /// The main a re-adoption replaces: the RAW main the store enumerates, or else a main left in
    /// a format this runtime no longer enumerates, read from its own sidecar only here, for its
    /// explicit replacement (02_workspaces.md, "Re-adoption": existing ASIF images are retained
    /// inputs to this transaction). Either must be mounted at the checkout.
    async fn replaced_main(&self, checkout: &Path) -> Result<ReplacedMain> {
        let replaced = match self.current(&main_name()).await {
            Ok(current) => ReplacedMain {
                mounted: matches!(
                    current.derived.mount_state,
                    crate::storage::lifecycle::MountState::Mounted { .. }
                ),
                image: current.image,
                metadata: current.metadata,
                legacy: false,
            },
            Err(error) if error.code == ErrorCode::NotFound => {
                let Some(image) = self.legacy_main_image()? else {
                    return Err(error);
                };
                // Explicit transition input only: strict deserialization still validates the
                // sidecar schema, grants and role. Ordinary RAW path validation stays intact.
                let metadata: crate::metadata::DetachedWorkspaceMetadata =
                    crate::metadata::read_json(&crate::metadata::sidecar_path(&image))
                        .map_err(native_integrity_error)?;
                if metadata.repo_id != self.descriptor.repo_id
                    || metadata.workspace != main_name()
                    || metadata.info_snapshot.project_root != checkout
                    || metadata.publication_state != crate::metadata::PublicationState::Active
                {
                    return Err(CowshedError::integrity(
                        format!(
                            "legacy main image {} does not record this project's main at {}",
                            image.display(),
                            checkout.display()
                        ),
                        "cowshed doctor --json",
                    ));
                }
                ReplacedMain {
                    mounted: super::is_mount_point(checkout).map_err(|error| {
                        CowshedError::environment_missing(
                            format!("cannot inspect {}: {error}", checkout.display()),
                            "cowshed doctor --json",
                        )
                    })?,
                    image,
                    metadata,
                    legacy: true,
                }
            }
            Err(error) => return Err(error),
        };
        if !replaced.mounted {
            return Err(CowshedError::conflict(
                "main must be mounted at its checkout to be re-adopted",
                "mount main at its checkout, then retry",
            ));
        }
        Ok(replaced)
    }

    /// Main's image in a format this runtime does not enumerate, if the project directory holds
    /// one: an adoption from before RAW, which only `cowshed adopt --re-adopt` replaces.
    pub(super) fn legacy_main_image(&self) -> Result<Option<PathBuf>> {
        let project = &self.layout.project().project_root;
        for extension in LEGACY_IMAGE_EXTENSIONS {
            let image = project.join("main").with_extension(extension);
            match std::fs::symlink_metadata(&image) {
                Ok(_) => return Ok(Some(image)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(CowshedError::environment_missing(
                        format!("cannot inspect {}: {error}", image.display()),
                        "check controller storage permissions and retry",
                    ));
                }
            }
        }
        Ok(None)
    }
}

/// Image formats an adoption from before RAW left, read only as re-adoption inputs.
const LEGACY_IMAGE_EXTENSIONS: [&str; 2] = ["asif", "sparseimage"];

/// The main a re-adoption replaces, as [`NativeProjectRuntimeHost::replaced_main`] found it.
struct ReplacedMain {
    image: PathBuf,
    metadata: crate::metadata::DetachedWorkspaceMetadata,
    mounted: bool,
    /// Read from a format this runtime does not enumerate.
    legacy: bool,
}

/// Every path this re-adoption retained, companions included, and nothing else: what the user
/// may remove once the replacement is checked.
fn retained_paths(source: &ReAdoptSource) -> Vec<PathBuf> {
    use crate::metadata::append_suffix;
    let main = &source.image.retained;
    let mut paths = vec![
        main.clone(),
        append_suffix(main, crate::metadata::GRANTS_SIDECAR_SUFFIX),
        append_suffix(main, ".ca.key"),
    ];
    let built = source
        .build
        .iter()
        .map(|build| &build.image.retained)
        .chain(source.old_seeds.iter().map(|seed| &seed.retained));
    for image in built {
        paths.push(image.clone());
        paths.push(append_suffix(image, ".json"));
    }
    paths
}

/// Every build volume image in `images`, RAW or written before RAW, with the identity its file
/// name spells. Read only here, as re-adoption's inputs; nothing else enumerates a legacy one.
fn build_inputs(images: &Path) -> Result<Vec<(BuildVolumeId, PathBuf)>> {
    let unreadable = |error: std::io::Error| {
        CowshedError::environment_missing(
            format!(
                "cannot list build volume images in {}: {error}",
                images.display()
            ),
            "check controller storage permissions and retry",
        )
    };
    let entries = match std::fs::read_dir(images) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(unreadable(error)),
    };
    let mut inputs = Vec::new();
    for entry in entries {
        let path = entry.map_err(unreadable)?.path();
        let known = path.extension().is_some_and(|extension| {
            extension == IMAGE_EXTENSION
                || LEGACY_IMAGE_EXTENSIONS
                    .iter()
                    .any(|legacy| extension == *legacy)
        });
        let id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(BuildVolumeId::parse);
        if let (true, Some(id)) = (known, id) {
            inputs.push((id, path));
        }
    }
    inputs.sort();
    Ok(inputs)
}

/// The record beside build volume image `image` (`<image>.json`), or `None` if it has none.
fn read_build_record_present(
    image: &Path,
) -> Result<Option<crate::build_volume::BuildVolumeRecord>> {
    let record = crate::metadata::append_suffix(image, ".json");
    match crate::metadata::read_json::<crate::build_volume::BuildVolumeRecord>(&record) {
        Ok(read) if read.version == crate::build_volume::RECORD_VERSION => Ok(Some(read)),
        Ok(read) => Err(CowshedError::integrity(
            format!(
                "build volume record {} has version {}, not {}",
                record.display(),
                read.version,
                crate::build_volume::RECORD_VERSION
            ),
            "cowshed doctor --json",
        )),
        Err(crate::metadata::MetadataError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(native_integrity_error(error)),
    }
}

/// The record beside build volume image `image`, which must have one: its owner record is
/// carried verbatim to the replacement, and nothing stands in for a missing one.
fn read_build_record(image: &Path) -> Result<crate::build_volume::BuildVolumeRecord> {
    read_build_record_present(image)?.ok_or_else(|| {
        CowshedError::integrity(
            format!("build volume image {} has no record", image.display()),
            "cowshed doctor --json",
        )
    })
}

/// `<retained>/<stem>.<the extension of original>`: a retained input keeps its format.
fn retained_beside(retained: &Path, stem: &str, original: &Path) -> PathBuf {
    let path = retained.join(stem);
    match original.extension() {
        Some(extension) => path.with_extension(extension),
        None => path,
    }
}
