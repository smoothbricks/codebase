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

use super::{StorageProjectRuntimeHost, close_checkout_nx, main_name, native_integrity_error};
use super::{WorkspaceSnapshot, native_staged_error, native_storage_error};
use crate::runtime::storage_host::RuntimeStorageHost;
use crate::api::dto::AdoptOptions;
use crate::build_volume::BuildVolumeId;
use crate::error::{CowshedError, ErrorCode, Result};
use cowshed_apfs::image::IMAGE_EXTENSION;
use crate::metadata::WorkspaceName;
use crate::storage::apfs::COMPANION_SUFFIX;
use crate::storage::lifecycle::AdoptHandoff;
use crate::storage::recovery::{
    LifecycleIntent, LifecycleIntentCompletion, MissingMainBuild, RETAINED_NAMESPACE, ReAdoptBuild,
    ReAdoptEmptyBuild, ReAdoptImage, ReAdoptKeptBuild, ReAdoptMain, ReAdoptMissingMain,
    ReAdoptRetained, ReAdoptSource, STAGING_NAMESPACE,
};

impl<H: RuntimeStorageHost> StorageProjectRuntimeHost<H> {
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
        // Step 1: the caller is outside the checkout, the source has no holders, and nothing is
        // ended to make it so.
        require_outside(&checkout)?;
        let Some(replaced) = self.replaced_main(&checkout).await? else {
            // Main's image is missing: its plain checkout is the copy input.
            let source = self.record_missing_main(&options, &checkout).await?;
            return self.begin_re_adopt(options, source, None).await;
        };
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
            refuse_held(mount, &holders)?;
        }
        let source = self
            .record_re_adopt_source(&replaced, &checkout, build.as_ref())
            .await?;
        self.begin_re_adopt(options, source, supervisor).await
    }

    /// Record `source` and mark it mutating in one journal update, then run it. `supervisor` is
    /// the retired supervisor's socket lease, held until the re-adoption is recorded so no
    /// replacement supervisor starts on the source while it is measured.
    async fn begin_re_adopt(
        &mut self,
        options: AdoptOptions,
        source: ReAdoptSource,
        supervisor: Option<super::super::supervisor_socket::BoundSocket>,
    ) -> Result<WorkspaceSnapshot> {
        let main = main_name();
        let checkout = source.checkout.clone();
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
        crate::storage::lifecycle::dispatch_blocking(move || {
            recorded_layout.record_checkout_root(&checkout)
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

    /// Everything a missing main's re-adoption records before its first mutation
    /// (02_workspaces.md, "Missing main image"). The plain checkout at main's path is the copy
    /// input; main's completed re-adoption journal is the only authority left for main's
    /// metadata and its build volume, whose image is gone with main's.
    async fn record_missing_main(
        &self,
        options: &AdoptOptions,
        checkout: &Path,
    ) -> Result<ReAdoptSource> {
        let main = main_name();
        let inspect = |path: &Path, error: std::io::Error| {
            CowshedError::environment_missing(
                format!("cannot inspect {}: {error}", path.display()),
                "cowshed doctor --json",
            )
        };
        if super::is_mount_point(checkout).map_err(|error| inspect(checkout, error))? {
            return Err(CowshedError::conflict(
                format!(
                    "main's image is missing, but a volume is mounted at {}",
                    checkout.display()
                ),
                "unmount it, leaving main's plain Git checkout at that path, then retry",
            ));
        }
        let git = crate::git::GitRepository::discover(checkout).await?;
        let root = std::fs::canonicalize(git.root()).map_err(|error| inspect(git.root(), error))?;
        let named = std::fs::canonicalize(checkout).map_err(|error| inspect(checkout, error))?;
        require_checkout_of(
            &root,
            &named,
            &git.remotes().await?,
            &self.descriptor.repo_id,
        )?;
        let canonical = self
            .layout
            .canonical_image(&main)
            .map_err(native_integrity_error)?
            .image()
            .to_owned();
        for path in [
            canonical.clone(),
            crate::metadata::sidecar_path(&canonical),
            crate::metadata::append_suffix(&canonical, COMPANION_SUFFIX),
        ] {
            if present(&path)? {
                return Err(CowshedError::integrity(
                    format!("main's image is missing, but {} remains", path.display()),
                    "cowshed doctor --json",
                ));
            }
        }
        let authority = self.lifecycle_intents.get(&main).and_then(|record| {
            match (&record.operation, &record.completion) {
                (
                    LifecycleIntent::ReAdopt { source, .. },
                    Some(LifecycleIntentCompletion::Workspace(published)),
                ) if *published == source.replacement && source.checkout == checkout => {
                    Some(source.as_ref())
                }
                _ => None,
            }
        });
        let Some(completed) = authority else {
            return Err(CowshedError::integrity(
                format!(
                    "main's image is missing, and no completed re-adoption of main at {} records \
                     its metadata and build volume",
                    checkout.display()
                ),
                "cowshed doctor --json",
            ));
        };
        completed.validate()?;
        // The lost main is the incarnation that completed re-adoption published.
        let mut metadata = completed.metadata.clone();
        metadata.workspace_incarnation = completed.replacement.clone();
        let layout = self.build_volume_layout()?;
        // Main's build volume is the project's one build record owned by `Linked main`, whatever
        // its id: an image of its own, it outlives main's image, and every land gives main a new
        // one. Only when none survives is one published empty under the recorded id.
        let build = match linked_main_volumes(&layout)?.as_slice() {
            [] => match published_build(completed) {
                None => None,
                Some((id, record, capacity_bytes)) => {
                    for path in [layout.image(&id), layout.record(&id)] {
                        if present(&path)? {
                            return Err(CowshedError::conflict(
                                format!(
                                    "main's build volume {} is not missing: {} remains; a missing \
                                     main's re-adoption publishes only a missing build volume",
                                    id.as_str(),
                                    path.display()
                                ),
                                "cowshed doctor --json",
                            ));
                        }
                    }
                    Some(MissingMainBuild::Empty(ReAdoptEmptyBuild {
                        id,
                        record: crate::build_volume::BuildVolumeRecord {
                            tree: None,
                            ..record
                        },
                        capacity_bytes,
                    }))
                }
            },
            [(kept, record)] => {
                // A volume census of a directory that is no mount point would name every
                // process on the volume holding it, so only a mounted volume is censused.
                let mount = layout.mount(kept);
                if super::is_mount_point(&mount).map_err(|error| inspect(&mount, error))? {
                    let holders =
                        crate::build_volume::nx::volume_holders(&mount).map_err(|error| {
                            CowshedError::environment_missing(
                                format!("cannot list what holds {}: {error}", mount.display()),
                                "cowshed doctor --json",
                            )
                        })?;
                    refuse_held(&mount, &holders)?;
                }
                Some(MissingMainBuild::Kept(ReAdoptKeptBuild {
                    kept: kept.clone(),
                    record: record.clone(),
                }))
            }
            several => {
                return Err(CowshedError::integrity(
                    format!(
                        "main's image is missing, and build volumes {} each record `Linked main`; \
                         main has one build volume",
                        several
                            .iter()
                            .map(|(id, _)| id.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    "cowshed doctor --json",
                ));
            }
        };
        if let Some(build) = &build
            && let Some(linked) = layout.linked(checkout)?
            && &linked != build.id()
        {
            return Err(CowshedError::conflict(
                format!(
                    "{} links build volume {}, not main's {}",
                    checkout.display(),
                    linked.as_str(),
                    build.id().as_str()
                ),
                "remove that build link, then retry",
            ));
        }
        let capacity_bytes = match options.capacity.as_deref() {
            Some(requested) => super::parse_capacity(requested)?.bytes(),
            None => main_capacity(completed),
        };
        let replacement = {
            use crate::storage::apfs::IncarnationSource as _;
            crate::storage::apfs::UuidIncarnationSource
                .mint()
                .map_err(native_storage_error)?
        };
        let preserved = crate::storage::recovery::preserved_checkout(checkout, &replacement)
            .ok_or_else(|| {
                CowshedError::usage(
                    format!("checkout {} has no name", checkout.display()),
                    "re-adopt main at its named checkout",
                )
            })?;
        let retained = self.layout.project().project_root.join(RETAINED_NAMESPACE);
        let old_seeds = old_seeds(
            build_inputs(layout.images())?,
            &metadata.workspace_incarnation,
            &retained,
        )?;
        // Step 1: the plain checkout is the copy source, so nothing may hold it while its state
        // is recorded and copied; nothing is ended to make it so.
        refuse_tree_held(checkout)?;
        // Main's volume root holds cowshed's runtime state and macOS's bookkeeping, which main's
        // excludes hide. The checkout takes them now, so its Git state verifies against its copy.
        git.ensure_cowshed_excludes().await?;
        // The plain checkout links main's build volume before its Git state is recorded, so its
        // copy carries that link and main's first touch keeps the id. A kept volume is main's
        // already; an empty one is published under the recorded id once main is.
        if let Some(build) = &build {
            let (linked, mount) = (checkout.to_owned(), layout.mount(build.id()));
            crate::storage::lifecycle::dispatch_blocking(move || {
                crate::build_volume::link::point(&linked, &mount)
            })
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))??;
        }
        Ok(ReAdoptSource {
            checkout: checkout.to_owned(),
            main: ReAdoptMain::Missing(ReAdoptMissingMain {
                canonical,
                preserved,
                capacity_bytes,
                build,
            }),
            metadata,
            replacement,
            git_before: capture_git(checkout).await?,
            build: None,
            old_seeds,
        })
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
        let old_seeds = old_seeds(inputs, &old, &retained)?;
        let git_before = capture_git(checkout).await?;
        Ok(ReAdoptSource {
            checkout: checkout.to_owned(),
            main: ReAdoptMain::Retained(ReAdoptImage {
                retained: retained_beside(
                    &retained,
                    &format!("main-{}", old.as_str()),
                    &main_image,
                ),
                original: main_image,
                source_mount: staging.join(format!("readopt-{}-main", old.as_str())),
                capacity_bytes: main_capacity,
            }),
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
        // Until the handoff swaps it aside, a missing main's copy source is the user's live
        // checkout. A commit or a fetch made after an earlier attempt refused must not wedge
        // every replay against the Git state that attempt recorded, so an unpublished replay
        // proves the checkout unheld and records its state again before copying it.
        let rerecord = published.is_none()
            && match &source.main {
                ReAdoptMain::Missing(missing) => !present(&missing.preserved)?,
                ReAdoptMain::Retained(_) => false,
            };
        let source = if rerecord {
            std::sync::Arc::new(self.rerecord_missing_main_git(&source).await?)
        } else {
            source
        };
        let workspace = match published {
            Some(current) => {
                self.substrate
                    .finish_adoption(&current.derived.workspace, &handoff(&source))
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
        // old build volume out, and the lock is released only after the claim is gone. A missing
        // main records no source build and so holds no claim; its recorded build volume is
        // published empty.
        {
            let source = source.clone();
            self.substrate
                .dispatch_on_host_with_image_lock(lock.clone(), move |host| {
                    host.publish_re_adopt_build(&layout, &source, build_claim.as_ref())
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
                    published_source(&source),
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
        // Read before the conclusion, so a retained path that cannot be inspected refuses while
        // the intent is still open and a replay reports it again.
        let retained = retained_paths(&source)?;
        let snapshot = self
            .conclude_adoption(&workspace, super::super::build_volumes::FirstMint::Nothing)
            .await?;
        eprintln!(
            "cowshed: re-adopted main as {}; this re-adoption kept what it replaced at:",
            source.replacement.as_str()
        );
        for path in &retained {
            eprintln!("  {}", path.display());
        }
        eprintln!(
            "cowshed: once you have checked the replacement, removing exactly those paths returns \
             their space: rm -R -- {}",
            retained
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );
        Ok(snapshot)
    }

    /// The recorded missing-main re-adoption `source`, its plain checkout first proved unheld and
    /// its Git state captured again (02_workspaces.md, "Missing main image"). An unchanged state
    /// writes nothing; a changed one replaces only the recorded `git_before`, under the journal's
    /// lock and only while the record is still exactly `source`.
    async fn rerecord_missing_main_git(&mut self, source: &ReAdoptSource) -> Result<ReAdoptSource> {
        refuse_tree_held(&source.checkout)?;
        let git_before = capture_git(&source.checkout).await?;
        if git_before == source.git_before {
            return Ok(source.clone());
        }
        let expected = source.clone();
        self.update_lifecycle_intents(move |journal| {
            journal.rerecord_missing_main_git(&expected, git_before)
        })
        .await
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
        // processes, while anything of it still works in the volume. A missing main has none.
        let _supervisor = match &source.main {
            ReAdoptMain::Retained(image)
                if image
                    .original
                    .extension()
                    .is_some_and(|extension| extension == IMAGE_EXTENSION) =>
            {
                Some(self.stop_supervisor(&main).await?)
            }
            ReAdoptMain::Retained(_) | ReAdoptMain::Missing(_) => None,
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
                capacity: cowshed_apfs::image::ImageCapacity::from_bytes(main_capacity(&source)),
                topology_revision: crate::storage::lifecycle::Revision::new(0),
                source_checkout: source.checkout.clone(),
                handoff: handoff(&source),
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
                            staged_source(&evidence),
                        )?;
                        if let ReAdoptMain::Retained(image) = &evidence.main {
                            let (new, old) = host
                                .require_sparse_replacement(
                                    &replacement,
                                    &image.retained,
                                    image.capacity_bytes,
                                )
                                .map_err(native_storage_error)?;
                            eprintln!(
                                "cowshed: re-adoption replacement allocates {new} bytes; the \
                                 retained image {old}"
                            );
                        }
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
    /// inputs to this transaction). Either must be mounted at the checkout. `None` when main has no image at all.
    async fn replaced_main(&self, checkout: &Path) -> Result<Option<ReplacedMain>> {
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
            Err(missing) if missing.code == ErrorCode::NotFound => {
                let Some(image) = self.legacy_main_image()? else {
                    return Ok(None);
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
        Ok(Some(replaced))
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

/// The main a re-adoption replaces, as [`StorageProjectRuntimeHost::replaced_main`] found it.
struct ReplacedMain {
    image: PathBuf,
    metadata: crate::metadata::DetachedWorkspaceMetadata,
    mounted: bool,
    /// Read from a format this runtime does not enumerate.
    legacy: bool,
}

/// Every path this re-adoption retained, companions included, and nothing else: what the user
/// may remove once the replacement is checked. A lost build input retained only its record, or
/// nothing, so only what is there is named; a path that cannot be inspected is an error, never
/// silently left out of the report.
fn retained_paths(source: &ReAdoptSource) -> Result<Vec<PathBuf>> {
    use crate::metadata::append_suffix;
    let mut paths = match &source.main {
        ReAdoptMain::Retained(image) => vec![
            image.retained.clone(),
            append_suffix(&image.retained, crate::metadata::GRANTS_SIDECAR_SUFFIX),
            append_suffix(&image.retained, COMPANION_SUFFIX),
        ],
        ReAdoptMain::Missing(missing) => vec![missing.preserved.clone()],
    };
    let built = source
        .build
        .iter()
        .map(|build| &build.image.retained)
        .chain(source.old_seeds.iter().map(|seed| &seed.retained));
    for image in built {
        paths.push(image.clone());
        paths.push(append_suffix(image, crate::build_volume::RECORD_SUFFIX));
    }
    let mut present = Vec::with_capacity(paths.len());
    for path in paths {
        match path.symlink_metadata() {
            Ok(_) => present.push(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CowshedError::environment_missing(
                    format!("cannot inspect retained {}: {error}", path.display()),
                    "cowshed doctor --json",
                ));
            }
        }
    }
    Ok(present)
}

/// Refuse outright when this shell's working directory is inside `checkout`, which the
/// re-adoption replaces (02_workspaces.md, "Re-adoption" step 1).
fn require_outside(checkout: &Path) -> Result<()> {
    let cwd = std::env::current_dir().map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot read the working directory: {error}"),
            "retry from an existing directory",
        )
    })?;
    if cwd.starts_with(checkout) {
        return Err(CowshedError::usage(
            format!(
                "re-adoption replaces the volume at {}, which this shell's working directory is inside",
                checkout.display()
            ),
            format!("cd out of {} and retry", checkout.display()),
        ));
    }
    Ok(())
}

/// Refuse a plain checkout unless `root`, its repository's root, is `checkout` itself and one
/// of its remotes names `repo`: a missing main is re-adopted only from a checkout of its own
/// repository (02_workspaces.md, "Missing main image"). Both paths arrive canonical.
/// Refuse when anything holds `path`, naming each holder by PID and command (02_workspaces.md,
/// "Re-adoption" step 1). Nothing is stopped, killed or forced.
fn refuse_held(path: &Path, holders: &[cowshed_os::process::Holder]) -> Result<()> {
    if holders.is_empty() {
        return Ok(());
    }
    let named = holders
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    Err(CowshedError::conflict(
        format!(
            "{} is in use by {named}; no running job or user process was stopped and nothing was \
             forced",
            path.display()
        ),
        "close those processes, then retry",
    ))
}

/// Refuse when anything holds a file, a working directory or an executable inside the plain
/// checkout tree at `checkout`, a missing main's copy source.
fn refuse_tree_held(checkout: &Path) -> Result<()> {
    let holders = crate::build_volume::nx::tree_holders(checkout).map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot list what holds {}: {error}", checkout.display()),
            "cowshed doctor --json",
        )
    })?;
    refuse_held(checkout, &holders)
}

/// Every published build volume whose record names `Linked main`, with that record: main's
/// build volume, whatever its id (02_workspaces.md, "Missing main image").
fn linked_main_volumes(
    layout: &crate::build_volume::BuildVolumeLayout,
) -> Result<Vec<(BuildVolumeId, crate::build_volume::BuildVolumeRecord)>> {
    let mut found = Vec::new();
    for id in layout.list()? {
        if let Some(record) = layout.read_record_present(&id)?
            && matches!(
                &record.role,
                crate::build_volume::BuildVolumeRole::Linked { checkout } if checkout.is_main()
            )
        {
            found.push((id, record));
        }
    }
    Ok(found)
}

fn require_checkout_of(
    root: &Path,
    checkout: &Path,
    remotes: &[crate::git::RemoteUrl],
    repo: &crate::repository::RepoId,
) -> Result<()> {
    if root != checkout {
        return Err(CowshedError::conflict(
            format!(
                "{} is not the root of a Git checkout; its repository root is {}",
                checkout.display(),
                root.display()
            ),
            "restore main's plain Git checkout at that path, then retry",
        ));
    }
    let names_repo = remotes.iter().any(|remote| {
        matches!(
            remote.url.to_str().map(crate::repository::normalize_remote_url),
            Some(Ok(derived)) if derived == *repo
        )
    });
    if !names_repo {
        return Err(CowshedError::conflict(
            format!(
                "no remote of {} names {repo}; a missing main is re-adopted only from a checkout \
                 of its own repository",
                checkout.display()
            ),
            "re-adopt from a checkout of that repository",
        ));
    }
    Ok(())
}

/// Whether anything is at `path`, read without following a link.
fn present(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(CowshedError::environment_missing(
            format!("cannot inspect {}: {error}", path.display()),
            "check controller storage permissions and retry",
        )),
    }
}

/// The Git state of `checkout` while it is quiet. A source whose own repository fails fsck has
/// nothing a verified copy could preserve: refuse before anything is recorded or detached.
async fn capture_git(checkout: &Path) -> Result<crate::git_preservation::GitPreservationSnapshot> {
    let captured = {
        let checkout = checkout.to_owned();
        crate::storage::lifecycle::dispatch_blocking(move || {
            crate::git_preservation::capture(&checkout, &checkout)
        })
        .await
        .map_err(|error| CowshedError::internal(error.to_string()))??
    };
    if !captured.fsck_succeeded() {
        return Err(CowshedError::integrity(
            format!(
                "git fsck fails in {}; re-adoption copies a repository only as it verifies",
                checkout.display()
            ),
            "repair the repository, then retry",
        ));
    }
    Ok(captured)
}

/// Every image in `inputs` recorded as a seed of main's `old` incarnation, with the retained
/// path it moves to. An image with no record yet is an interrupted creation, never a seed.
fn old_seeds(
    inputs: Vec<(BuildVolumeId, PathBuf)>,
    old: &crate::metadata::WorkspaceIncarnation,
    retained: &Path,
) -> Result<Vec<ReAdoptRetained>> {
    let mut seeds = Vec::new();
    for (id, original) in inputs {
        if read_build_record_present(&original)?
            .is_some_and(|record| record.is_seed_of(&WorkspaceName::main(), old))
        {
            seeds.push(ReAdoptRetained {
                retained: retained_beside(retained, &format!("seed-{}", id.as_str()), &original),
                original,
            });
        }
    }
    Ok(seeds)
}

/// Main's image capacity as `source` records it.
fn main_capacity(source: &ReAdoptSource) -> u64 {
    match &source.main {
        ReAdoptMain::Retained(image) => image.capacity_bytes,
        ReAdoptMain::Missing(missing) => missing.capacity_bytes,
    }
}

/// Main's build volume as `source` publishes it: its id, owner record and capacity.
fn published_build(
    source: &ReAdoptSource,
) -> Option<(BuildVolumeId, crate::build_volume::BuildVolumeRecord, u64)> {
    match &source.main {
        ReAdoptMain::Retained(_) => source.build.as_ref().map(|build| {
            (
                build.id.clone(),
                build.owner_record.clone(),
                build.image.capacity_bytes,
            )
        }),
        ReAdoptMain::Missing(missing) => missing.build.as_ref().map(|build| match build {
            MissingMainBuild::Empty(build) => {
                (build.id.clone(), build.record.clone(), build.capacity_bytes)
            }
            // A kept volume's capacity was never recorded: it was not minted here. Should it be
            // lost too, its replacement takes a fresh build volume's default capacity.
            MissingMainBuild::Kept(build) => (
                build.kept.clone(),
                build.record.clone(),
                crate::build_volume::DEFAULT_BUILD_VOLUME_CAPACITY.bytes(),
            ),
        }),
    }
}

/// The handoff a re-adoption publishes through: a retained main's copy comes from its staging
/// mount onto the emptied checkout; a missing main's plain checkout moves to its preserved
/// sibling.
fn handoff(source: &ReAdoptSource) -> AdoptHandoff {
    match &source.main {
        ReAdoptMain::Retained(image) => AdoptHandoff::Replacement {
            copy_source: image.source_mount.clone(),
            incarnation: source.replacement.clone(),
        },
        ReAdoptMain::Missing(missing) => AdoptHandoff::PlainReplacement {
            preserved: missing.preserved.clone(),
            incarnation: source.replacement.clone(),
        },
    }
}

/// The tree the staged copy is read from and verified against before publication.
fn staged_source(source: &ReAdoptSource) -> &Path {
    match &source.main {
        ReAdoptMain::Retained(image) => &image.source_mount,
        ReAdoptMain::Missing(_) => &source.checkout,
    }
}

/// The tree the published replacement is verified against after its handoff.
fn published_source(source: &ReAdoptSource) -> &Path {
    match &source.main {
        ReAdoptMain::Retained(image) => &image.source_mount,
        ReAdoptMain::Missing(missing) => &missing.preserved,
    }
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
    let record = crate::metadata::append_suffix(image, crate::build_volume::RECORD_SUFFIX);
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::require_checkout_of;
    use crate::error::ErrorCode;
    use crate::git::RemoteUrl;
    use crate::repository::RepoId;

    fn remote(name: &str, url: &str) -> RemoteUrl {
        RemoteUrl {
            name: name.to_owned(),
            url: PathBuf::from(url),
        }
    }

    /// 02_workspaces.md, "Missing main image": a missing main is re-adopted only from the root
    /// of a checkout one of whose remotes names the project's repository.
    #[test]
    fn a_missing_main_is_re_adopted_only_from_a_checkout_of_its_own_repository() {
        let repo = RepoId::parse("acme.sc/widget").expect("repository identity");
        let checkout = Path::new("/work/widget");
        let own = [
            remote("github", "https://github.com/former/widget.git"),
            remote("origin", "ssh://git@forge.example:2223/acme.sc/widget.git"),
        ];
        require_checkout_of(checkout, checkout, &own, &repo)
            .expect("a remote names the repository");

        let foreign = [remote("origin", "https://github.com/other/widget.git")];
        let refused = require_checkout_of(checkout, checkout, &foreign, &repo)
            .expect_err("no remote names the repository");
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert!(
            refused.message.contains("acme.sc/widget"),
            "{}",
            refused.message
        );

        let unremoted = require_checkout_of(checkout, checkout, &[], &repo)
            .expect_err("a checkout without remotes names nothing");
        assert_eq!(unremoted.code, ErrorCode::Conflict);

        let nested = require_checkout_of(Path::new("/work"), checkout, &own, &repo)
            .expect_err("a directory inside another repository is not a checkout root");
        assert_eq!(nested.code, ErrorCode::Conflict);
    }
}
