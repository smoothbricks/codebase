use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::api::dto::{AdoptOptions, ForkOptions, RemoveOptions, RemoveReport, RevisionTarget};
use crate::error::{CowshedError, Result as CowshedResult};
use crate::metadata::{MetadataError, WorkspaceIncarnation, WorkspaceName, read_json, write_json};
use crate::repository::RepoId;
use cowshed_os::fork_lock::Fenced;
/// Objects in these namespaces are controller implementation details and never canonical listings.
pub const STAGING_NAMESPACE: &str = ".staging";
pub const TRASH_NAMESPACE: &str = ".trash";
/// Extension of the recovery bundle a removal verifies and writes into the trash before it
/// abandons unlanded work: `sessions/.trash/<ws>-<tip>.bundle`. It is the only copy of those
/// commits, so only a sweep explicitly authorized for them reclaims it.
pub const ABANDON_BUNDLE_EXTENSION: &str = "bundle";

/// What a project's trash keeps of `rm --abandon`: its abandon bundles, the only copy of the
/// commits each removal gave up, which only an explicit `gc --abandoned` deletes. The surfaces
/// that never otherwise show the trash — `ls --all`, plain `gc`, `doctor` — name them with this.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeptAbandonBundles {
    pub trash: PathBuf,
    pub count: usize,
    /// Allocated bytes, as `gc --dry-run --abandoned` counts them.
    pub bytes: u64,
}

impl KeptAbandonBundles {
    pub const HINT: &'static str =
        "cowshed gc --dry-run --abandoned lists them; cowshed gc --abandoned deletes them";

    /// The bundles an `--abandoned` gc preview names; `None` when it names none.
    pub fn named_by(
        candidates: &[crate::storage::lifecycle::StorageGcCandidate],
    ) -> CowshedResult<Option<Self>> {
        let mut kept: Option<Self> = None;
        for candidate in candidates.iter().filter(|candidate| {
            candidate.reason() == crate::storage::lifecycle::StorageGcReason::AbandonedBundle
        }) {
            let summary = kept.get_or_insert_with(|| Self {
                trash: candidate
                    .path()
                    .parent()
                    .unwrap_or(candidate.path())
                    .to_owned(),
                count: 0,
                bytes: 0,
            });
            summary
                .add(candidate.bytes())
                .ok_or_else(|| CowshedError::internal("abandon bundle byte accounting overflow"))?;
        }
        Ok(kept)
    }

    /// The bundles directly in `trash`, by the same rule a sweep uses: a regular file with the
    /// bundle extension. `None` when there are none, including when `trash` does not exist.
    pub fn in_trash(trash: &Path) -> io::Result<Option<Self>> {
        use std::os::unix::fs::MetadataExt as _;

        let entries = match std::fs::read_dir(trash) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut kept: Option<Self> = None;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path
                .extension()
                .is_none_or(|extension| extension != ABANDON_BUNDLE_EXTENSION)
            {
                continue;
            }
            let metadata = entry.metadata()?;
            if !metadata.file_type().is_file() {
                continue;
            }
            let summary = kept.get_or_insert_with(|| Self {
                trash: trash.to_owned(),
                count: 0,
                bytes: 0,
            });
            metadata
                .blocks()
                .checked_mul(512)
                .and_then(|bytes| summary.add(bytes))
                .ok_or_else(|| io::Error::other("abandon bundle byte accounting overflow"))?;
        }
        Ok(kept)
    }

    /// `None` when the byte total would overflow.
    fn add(&mut self, bytes: u64) -> Option<()> {
        self.count += 1;
        self.bytes = self.bytes.checked_add(bytes)?;
        Some(())
    }
}

impl std::fmt::Display for KeptAbandonBundles {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} keeps {} abandon {} totalling {} bytes, the only copy of the commits \
             `rm --abandon` gave up, which no plain gc deletes",
            self.trash.display(),
            self.count,
            if self.count == 1 { "bundle" } else { "bundles" },
            self.bytes
        )
    }
}

/// Same-parent, hidden mountpoint names retaining root-owned system debris; never workspaces.
pub const ROOT_HELD_MOUNT_PREFIX: &str = ".root-held-";
pub const LIFECYCLE_INTENTS_FILE: &str = "lifecycle-intents.json";
const LIFECYCLE_INTENT_VERSION: u32 = 1;
/// Suffix of the per-workspace lease file beside a project's session images.
pub const INTENT_LEASE_SUFFIX: &str = ".intent.lock";

/// A filesystem input retained by explicit re-adoption, never a second runtime image format.
/// Exact paths and kernel IOMedia capacities are captured before the first detach.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoptImage {
    pub original: PathBuf,
    pub retained: PathBuf,
    pub source_mount: PathBuf,
    pub capacity_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoptBuild {
    pub id: crate::build_volume::BuildVolumeId,
    pub image: ReAdoptImage,
    pub owner_record: crate::build_volume::BuildVolumeRecord,
    /// Where the replacement volume is minted and copied, outside the build images collection
    /// reads, until it is verified, detached and renamed to the build image path:
    /// `<project>/.staging/readopt-<replacement>-build.dmg`.
    pub replacement_image: PathBuf,
    /// Where the replacement volume is mounted while it is copied:
    /// `<mount root>/.staging/readopt-<replacement>-build`, beside the source mounts.
    pub replacement_mount: PathBuf,
}

/// An old-incarnation seed re-adoption moved, bytes and record unchanged, out of the build
/// images collection reads, so it stays as retained evidence (02_workspaces.md, "Re-adoption"
/// step 3) until the user removes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoptRetained {
    pub original: PathBuf,
    pub retained: PathBuf,
}

/// Persisted replacement authority. A real absent build link is `None`, not an empty stand-in:
/// the `build` key is required, an explicit `null` is the only way to say there is none.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoptSource {
    pub checkout: PathBuf,
    /// Main's copy input. The key stays `image`, which every journal written before a missing
    /// main could be re-adopted holds as a retained image.
    #[serde(rename = "image")]
    pub main: ReAdoptMain,
    pub metadata: crate::metadata::DetachedWorkspaceMetadata,
    /// The incarnation the replacement is published as, chosen before the first mutation: the
    /// only one the intent may complete with.
    pub replacement: WorkspaceIncarnation,
    pub(crate) git_before: crate::git_preservation::GitPreservationSnapshot,
    // `deserialize_with` turns off serde's missing-`Option`-is-`None` rule for this field.
    #[serde(deserialize_with = "Option::deserialize")]
    pub build: Option<ReAdoptBuild>,
    pub old_seeds: Vec<ReAdoptRetained>,
}

/// What a re-adoption copies main from, recorded before the first mutation so a replay never
/// guesses its input (02_workspaces.md, "Re-adoption" and "Missing main image").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReAdoptMain {
    /// Main's image, retained and copied from its read-only staging mount.
    Retained(ReAdoptImage),
    /// Main's image is missing: the plain Git checkout at the checkout path is copied.
    Missing(ReAdoptMissingMain),
}

impl ReAdoptMain {
    /// The retained main image; `None` for a missing main, which retains none.
    pub fn retained(&self) -> Option<&ReAdoptImage> {
        match self {
            Self::Retained(image) => Some(image),
            Self::Missing(_) => None,
        }
    }
}

/// A re-adoption of a main whose image is missing (02_workspaces.md, "Missing main image").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoptMissingMain {
    /// `<project>/main.dmg`, absent when recorded: the replacement is published there.
    pub canonical: PathBuf,
    /// [`preserved_checkout`]: where the plain checkout moves when the replacement takes its path.
    pub preserved: PathBuf,
    pub capacity_bytes: u64,
    /// Main's build volume: the surviving one kept, or one published empty under the id main's
    /// completed re-adoption recorded. `None` when neither exists.
    #[serde(deserialize_with = "Option::deserialize")]
    pub build: Option<MissingMainBuild>,
}

/// Main's build volume in a missing main's re-adoption (02_workspaces.md, "Missing main image").
/// A build volume is an image of its own, so main's can outlive main's image; and every land
/// gives main the landing workspace's volume, so the id main's completed re-adoption recorded may
/// be long gone. Main's volume is therefore the project's one build record owned by
/// `Linked main`, whatever its id; only when none survives is one published empty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MissingMainBuild {
    /// No build record names `Linked main`: an empty RAW volume is published under the id the
    /// completed re-adoption recorded. Journals from before a surviving volume could be kept hold
    /// only this shape.
    Empty(ReAdoptEmptyBuild),
    /// The project's one build record owned by `Linked main` survived: the volume is kept as it
    /// is, and main's build link names it.
    Kept(ReAdoptKeptBuild),
}

impl MissingMainBuild {
    /// The build volume main's build link names.
    pub fn id(&self) -> &crate::build_volume::BuildVolumeId {
        match self {
            Self::Empty(build) => &build.id,
            Self::Kept(build) => &build.kept,
        }
    }
}

/// Main's surviving build volume, kept as it is: `record` is its `Linked main` owner record as
/// read when the re-adoption was recorded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoptKeptBuild {
    pub kept: crate::build_volume::BuildVolumeId,
    pub record: crate::build_volume::BuildVolumeRecord,
}

/// An empty RAW build volume published under a recorded identity: `record` is the recorded
/// `Linked main` owner record with its built-at tree cleared.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoptEmptyBuild {
    pub id: crate::build_volume::BuildVolumeId,
    pub record: crate::build_volume::BuildVolumeRecord,
    pub capacity_bytes: u64,
}

/// `<checkout>.readopt-<replacement>`: the sibling a plain checkout moves to when a missing
/// main's replacement takes its path. Never `.pre-cowshed`, which a first adoption keeps.
pub fn preserved_checkout(checkout: &Path, replacement: &WorkspaceIncarnation) -> Option<PathBuf> {
    let mut name = checkout.file_name()?.to_os_string();
    name.push(".readopt-");
    name.push(replacement.as_str());
    Some(checkout.with_file_name(name))
}

impl ReAdoptSource {
    /// Refuse a replacement record whose paths could reach outside its own project, or whose
    /// names do not say whose they are, before anything acts on it (02_workspaces.md,
    /// "Re-adoption" step 1: exact source backing path, incarnation and retained paths).
    pub(crate) fn validate(&self) -> CowshedResult<()> {
        let refuse = |reason: String| {
            Err(CowshedError::integrity(
                format!("re-adoption record is invalid: {reason}"),
                "repair the lifecycle intent journal, then reopen cowshed",
            ))
        };
        let old = self.metadata.workspace_incarnation.as_str();
        let main_image = match &self.main {
            ReAdoptMain::Retained(image) => &image.original,
            ReAdoptMain::Missing(missing) => &missing.canonical,
        };
        let Some(project) = main_image.parent().filter(|root| root.is_absolute()) else {
            return refuse(format!(
                "main image {} is not an absolute path",
                main_image.display()
            ));
        };
        if !self.checkout.is_absolute() || self.checkout != self.metadata.info_snapshot.project_root
        {
            return refuse(format!(
                "checkout {} is not main's absolute recorded checkout {}",
                self.checkout.display(),
                self.metadata.info_snapshot.project_root.display()
            ));
        }
        if self.metadata.workspace != WorkspaceName::main() {
            return refuse(format!(
                "source workspace {} is not main",
                self.metadata.workspace
            ));
        }
        if self.replacement == self.metadata.workspace_incarnation {
            return refuse(format!("replacement reuses the incarnation {old}"));
        }
        let retained = project.join(RETAINED_NAMESPACE);
        let images = project.join(crate::build_volume::IMAGES_DIRECTORY);
        match &self.main {
            ReAdoptMain::Retained(image) => {
                check_image(
                    image,
                    project,
                    "main",
                    &retained,
                    &format!("main-{old}"),
                    &format!("readopt-{old}-main"),
                )
                .or_else(refuse)?;
                if let Some(build) = &self.build {
                    let id = build.id.as_str();
                    check_image(
                        &build.image,
                        &images,
                        id,
                        &retained,
                        &format!("build-{id}"),
                        &format!("readopt-{old}-build"),
                    )
                    .or_else(refuse)?;
                    let new = self.replacement.as_str();
                    let staged_image = project
                        .join(STAGING_NAMESPACE)
                        .join(format!("readopt-{new}-build"))
                        .with_extension(cowshed_apfs::image::IMAGE_EXTENSION);
                    if build.replacement_image != staged_image {
                        return refuse(format!(
                            "replacement build image {} is not {}",
                            build.replacement_image.display(),
                            staged_image.display()
                        ));
                    }
                    let mounts = image.source_mount.parent();
                    if build.image.source_mount.parent() != mounts
                        || build.replacement_mount.parent() != mounts
                        || build
                            .replacement_mount
                            .file_name()
                            .is_none_or(|name| *name != *format!("readopt-{new}-build"))
                    {
                        return refuse(format!(
                            "build mounts {} and {} are not readopt mounts beside {}",
                            build.image.source_mount.display(),
                            build.replacement_mount.display(),
                            image.source_mount.display()
                        ));
                    }
                }
            }
            ReAdoptMain::Missing(missing) => {
                let canonical = project
                    .join("main")
                    .with_extension(cowshed_apfs::image::IMAGE_EXTENSION);
                if missing.canonical != canonical {
                    return refuse(format!(
                        "missing main image {} is not {}",
                        missing.canonical.display(),
                        canonical.display()
                    ));
                }
                if preserved_checkout(&self.checkout, &self.replacement).as_ref()
                    != Some(&missing.preserved)
                {
                    return refuse(format!(
                        "preserved checkout {} is not the checkout's readopt sibling",
                        missing.preserved.display()
                    ));
                }
                if missing.capacity_bytes == 0 {
                    return refuse("missing main's replacement capacity is zero".to_owned());
                }
                if self.build.is_some() {
                    return refuse(
                        "a missing main's re-adoption copies no build volume".to_owned(),
                    );
                }
                let linked_main = |record: &crate::build_volume::BuildVolumeRecord| {
                    matches!(
                        &record.role,
                        crate::build_volume::BuildVolumeRole::Linked { checkout }
                            if checkout.is_main()
                    )
                };
                match &missing.build {
                    Some(MissingMainBuild::Empty(build))
                        if !linked_main(&build.record)
                            || build.record.tree.is_some()
                            || build.capacity_bytes == 0 =>
                    {
                        return refuse(format!(
                            "empty build volume {} is not main's linked volume with no built-at \
                             tree and a capacity",
                            build.id.as_str()
                        ));
                    }
                    Some(MissingMainBuild::Kept(build)) if !linked_main(&build.record) => {
                        return refuse(format!(
                            "kept build volume {} is not main's linked volume",
                            build.kept.as_str()
                        ));
                    }
                    None | Some(MissingMainBuild::Empty(_) | MissingMainBuild::Kept(_)) => {}
                }
            }
        }
        for seed in &self.old_seeds {
            let id = seed
                .original
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(crate::build_volume::BuildVolumeId::parse);
            let valid = id.is_some_and(|id| {
                retained_at(
                    &seed.original,
                    &seed.retained,
                    &images,
                    id.as_str(),
                    &retained,
                    &format!("seed-{}", id.as_str()),
                )
            });
            if !valid {
                return refuse(format!(
                    "old seed {} retained at {} is not a build volume image of {} retained in {}",
                    seed.original.display(),
                    seed.retained.display(),
                    images.display(),
                    retained.display()
                ));
            }
        }
        Ok(())
    }

    /// Every staging mountpoint this re-adoption uses, exactly as recorded: the read-only
    /// source mounts of a retained main and its build volume, and the build replacement's copy mount.
    /// Staging collection leaves these alone while the intent forms.
    pub fn staging_mounts(&self) -> impl Iterator<Item = &Path> {
        self.main
            .retained()
            .map(|image| image.source_mount.as_path())
            .into_iter()
            .chain(self.build.iter().flat_map(|build| {
                [
                    build.image.source_mount.as_path(),
                    build.replacement_mount.as_path(),
                ]
            }))
    }

    /// Every staged image this re-adoption mints outside the canonical names: the build
    /// replacement until it is renamed to its build image path.
    pub fn staging_images(&self) -> impl Iterator<Item = &Path> {
        self.build
            .iter()
            .map(|build| build.replacement_image.as_path())
    }
}

/// The images a re-adoption record retains: main's (a missing main retains none), its build
/// volume's, and every old seed's, each by its original or its retained path.
impl cowshed_apfs::RetainedInputs for ReAdoptSource {
    fn names(&self, path: &Path) -> bool {
        let named = |original: &Path, retained: &Path| path == original || path == retained;
        self.main
            .retained()
            .is_some_and(|image| named(&image.original, &image.retained))
            || self
                .build
                .as_ref()
                .is_some_and(|build| named(&build.image.original, &build.image.retained))
            || self
                .old_seeds
                .iter()
                .any(|seed| named(&seed.original, &seed.retained))
    }
}

#[cfg(any(target_os = "macos", test))]
impl ReAdoptSource {
    /// `path` as the retained-input token the disk-image layer's read-only attachment takes: one
    /// of exactly the images this validated record names.
    pub(crate) fn retained_input<'a>(
        &self,
        path: &'a Path,
    ) -> CowshedResult<cowshed_apfs::RetainedInput<'a>> {
        self.validate()?;
        cowshed_apfs::RetainedInput::named_by(self, path).ok_or_else(|| {
            CowshedError::integrity(
                format!(
                    "{} is not an input named by this re-adoption record",
                    path.display()
                ),
                "keep the source images and repair the replacement record",
            )
        })
    }
}

/// Where re-adoption retains the images it replaced, under the project directory.
pub const RETAINED_NAMESPACE: &str = "retained";

/// `original` is `<directory>/<stem>.<extension>` and `retained` is
/// `<retained_directory>/<retained_stem>.<the same extension>`: a retained input keeps the
/// format it was written in, never renamed as if it were RAW.
fn retained_at(
    original: &Path,
    retained: &Path,
    directory: &Path,
    stem: &str,
    retained_directory: &Path,
    retained_stem: &str,
) -> bool {
    let Some(extension) = original.extension().filter(|extension| {
        [cowshed_apfs::image::IMAGE_EXTENSION, "asif", "sparseimage"]
            .iter()
            .any(|known| **extension == **known)
    }) else {
        return false;
    };
    original.parent() == Some(directory)
        && original.file_stem().is_some_and(|name| name == stem)
        && retained
            == retained_directory
                .join(retained_stem)
                .with_extension(extension)
}

/// `image` is retained as [`retained_at`] says, read from a mount named `mount_name` in a
/// staging namespace: a name no BuildVolumeId parses as.
fn check_image(
    image: &ReAdoptImage,
    directory: &Path,
    stem: &str,
    retained_directory: &Path,
    retained_stem: &str,
    mount_name: &str,
) -> std::result::Result<(), String> {
    if !retained_at(
        &image.original,
        &image.retained,
        directory,
        stem,
        retained_directory,
        retained_stem,
    ) {
        return Err(format!(
            "image {} retained at {} is not {stem} in {} retained as {retained_stem} in {}",
            image.original.display(),
            image.retained.display(),
            directory.display(),
            retained_directory.display()
        ));
    }
    let mount_ok = image.source_mount.is_absolute()
        && image
            .source_mount
            .file_name()
            .is_some_and(|name| name == mount_name)
        && image
            .source_mount
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == STAGING_NAMESPACE);
    if !mount_ok {
        return Err(format!(
            "source mount {} is not {STAGING_NAMESPACE}/{mount_name}",
            image.source_mount.display()
        ));
    }
    if image.capacity_bytes == 0 {
        return Err(format!(
            "image {} records no capacity",
            image.original.display()
        ));
    }
    Ok(())
}

/// Frozen create-intent storage schema, not a public creation API. Missing `clean` preserves
/// older binaries' main-only discard rule; new fork intents record their explicit choice.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct MintOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<RevisionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_workspace: Option<WorkspaceName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clean: Option<bool>,
    pub browse: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<u32>,
    pub register: bool,
    pub git_worktree: bool,
}

impl MintOptions {
    pub fn fork(source: Option<WorkspaceName>, options: ForkOptions) -> Self {
        Self {
            revision: options.revision,
            from_workspace: source,
            clean: Some(options.clean),
            browse: options.browse,
            slot: options.slot,
            register: options.register,
            git_worktree: options.git_worktree,
        }
    }
}

/// Durable user intent written before a lifecycle verb's first mutation.
///
/// Deliberately not [`super::lifecycle::Operation`], and not substitutable for it. An intent
/// records the caller's pre-plan API options so a crashed verb can be *replanned* against
/// whatever the store looks like on the next open; an `Operation` is the already-resolved,
/// capability-free plan that `revalidate` checks against reread facts immediately before
/// mutating. Feeding an intent where a plan is expected would execute without the expected-fact
/// set that makes execution refusable; feeding a plan where an intent is expected would persist
/// a decision made against facts that no longer hold. The overlap in their verb names is the
/// only thing the two share, and the on-disk `kind` tags here are schema, not vocabulary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LifecycleIntent {
    Adopt {
        options: AdoptOptions,
    },
    ReAdopt {
        options: AdoptOptions,
        source: Box<ReAdoptSource>,
    },
    Create {
        workspace: WorkspaceName,
        options: MintOptions,
    },
    Fork {
        source: WorkspaceName,
        destination: WorkspaceName,
        /// True only for the clone/registration/retire rename transaction. Older fork tags
        /// omit it and retain their raw-clone recovery behavior.
        #[serde(default, skip_serializing_if = "not_rename")]
        rename: bool,
    },
    Retire {
        workspace: WorkspaceName,
        options: RemoveOptions,
        /// Original create/fork intent when retiring an unpublished clone. Its source is
        /// needed to verify the pending image after a crash; ordinary retirements omit it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<Box<LifecycleIntent>>,
    },
}

fn not_rename(rename: &bool) -> bool {
    !*rename
}

impl LifecycleIntent {
    pub fn target(&self) -> &WorkspaceName {
        match self {
            Self::Adopt { .. } | Self::ReAdopt { .. } => main_name(),
            Self::Create { workspace, .. } | Self::Retire { workspace, .. } => workspace,
            Self::Fork { destination, .. } => destination,
        }
    }

    /// The verb that recorded the intent, for operator-facing reports.
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Adopt { .. } | Self::ReAdopt { .. } => "adopt",
            Self::Create { .. } => "fork",
            Self::Fork { rename: true, .. } => "rename",
            Self::Fork { rename: false, .. } => "fork",
            Self::Retire { .. } => "retire",
        }
    }

    /// The detached metadata this intent carries for the workspace it rebuilds. A re-adoption's
    /// main keeps its grants, port block included, here while its image sits retained or staged
    /// outside the canonical inventory (04_sandbox.md, `portBlock`).
    pub fn carried_metadata(&self) -> Option<&crate::metadata::DetachedWorkspaceMetadata> {
        match self {
            Self::ReAdopt { source, .. } => Some(&source.metadata),
            Self::Adopt { .. } | Self::Create { .. } | Self::Fork { .. } | Self::Retire { .. } => {
                None
            }
        }
    }
}

/// The result needed to make an idempotent re-issue indistinguishable from the first call.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "result",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum LifecycleIntentCompletion {
    Workspace(WorkspaceIncarnation),
    Retire(RemoveReport),
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LifecycleIntentPhase {
    #[default]
    Prepared,
    Mutating,
}

impl LifecycleIntentPhase {
    fn is_prepared(&self) -> bool {
        *self == Self::Prepared
    }
}

/// One publication-fence artifact. The fence writes three things in order — the grants
/// sidecar (which carries `PendingFence`), the CA companion, the image payload — and a
/// crash can land between any two. Recording each completion separately lets a later reader
/// tell "the controller died mid-fence" from "something deleted a finished artifact".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FenceStep {
    Sidecar,
    Companion,
    Image,
}

/// Per-artifact completion flags for a fenced create/fork publication.
///
/// `None` on the record (the serialized field absent) means unknown: journals written
/// before this evidence existed load unchanged and are treated exactly as before — an open
/// fence with no step evidence is a crash window, never an external deletion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FenceSteps {
    #[serde(default)]
    pub sidecar: bool,
    #[serde(default)]
    pub companion: bool,
    #[serde(default)]
    pub image: bool,
}

impl FenceSteps {
    /// True once every fence artifact is durably written.
    pub fn is_complete(self) -> bool {
        self.sidecar && self.companion && self.image
    }

    fn mark(&mut self, step: FenceStep) {
        match step {
            FenceStep::Sidecar => self.sidecar = true,
            FenceStep::Companion => self.companion = true,
            FenceStep::Image => self.image = true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LifecycleIntentRecord {
    pub operation: LifecycleIntent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<LifecycleIntentCompletion>,
    /// `Prepared` means no durable mutation has begun. Recovery may discard a prepared
    /// retirement; only `Mutating` retirement records carry enough evidence to resume deletion.
    /// A create or fork turns `Mutating` just after binding its slot or just before the staged
    /// clone writes its `PendingFence` sidecar, so a prepared one that left neither and
    /// published nothing was refused, not interrupted, and recovery discards it rather than
    /// creating a workspace its caller was told failed. Older binaries never marked create or
    /// fork, so recovery decides a prepared clone from those durable facts, not from the phase.
    #[serde(default, skip_serializing_if = "LifecycleIntentPhase::is_prepared")]
    pub phase: LifecycleIntentPhase,
    /// Fence sub-step completions for create/fork publications. Absent (`None`) on older
    /// journals and on intents with no fence (adopt, retire); never backfilled, so absence
    /// always means unknown, never "no steps done".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence_steps: Option<FenceSteps>,
}

impl LifecycleIntentRecord {
    pub fn pending(operation: LifecycleIntent) -> Self {
        Self {
            operation,
            completion: None,
            phase: LifecycleIntentPhase::Prepared,
            fence_steps: None,
        }
    }
}

/// One bounded record per logical workspace. A later lifecycle supersedes the prior record for the
/// same name, so retry evidence cannot grow without bound.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LifecycleIntentJournal {
    version: u32,
    entries: BTreeMap<WorkspaceName, LifecycleIntentRecord>,
}

impl Default for LifecycleIntentJournal {
    fn default() -> Self {
        Self {
            version: LIFECYCLE_INTENT_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

impl LifecycleIntentJournal {
    pub fn load(path: &Path) -> CowshedResult<Self> {
        let journal = match read_json(path) {
            Ok(journal) => journal,
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Self::default()
            }
            Err(error) => {
                return Err(CowshedError::integrity(
                    format!(
                        "cannot read lifecycle intent journal {}: {error}",
                        path.display()
                    ),
                    "repair the lifecycle intent journal, then reopen cowshed",
                ));
            }
        };
        journal.validate().map_err(|error| {
            let project = path.parent().unwrap_or(path);
            CowshedError::new(
                error.code,
                format!(
                    "invalid lifecycle intent journal for project {} in {}: {}",
                    project.display(),
                    path.display(),
                    error.message
                ),
                error.hint,
            )
        })?;
        Ok(journal)
    }

    /// Apply one change to the journal on disk and return what the journal became.
    ///
    /// Every cowshed process that opens a project writes this one file, so a change is never
    /// applied to a copy read earlier: that copy predates whatever other processes recorded since,
    /// and writing it back erased their intents — a `new` killed after such an erasure left an
    /// unpublished clone that nothing could finish or retire. The file is re-read and rewritten
    /// under an exclusive lock on its `.lock` sibling, held for exactly that read and write.
    pub fn update<T>(
        path: &Path,
        change: impl FnOnce(&mut Self) -> CowshedResult<T>,
    ) -> CowshedResult<(Self, T)> {
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = open_lock_file(Path::new(&lock_path))?;
        lock.lock().map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot lock lifecycle intent journal {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            )
        })?;
        let mut journal = Self::load(path)?;
        let value = change(&mut journal)?;
        journal.persist(path)?;
        Ok((journal, value))
    }

    fn persist(&self, path: &Path) -> CowshedResult<()> {
        self.validate()?;
        write_json(path, self).map_err(|error| {
            CowshedError::storage_failure(
                format!(
                    "cannot persist lifecycle intent journal {}: {error}",
                    path.display()
                ),
                &error,
                "repair cowshed storage and retry the lifecycle operation",
            )
        })
    }

    pub fn get(&self, workspace: &WorkspaceName) -> Option<&LifecycleIntentRecord> {
        self.entries.get(workspace)
    }

    pub fn records(&self) -> impl Iterator<Item = (&WorkspaceName, &LifecycleIntentRecord)> {
        self.entries.iter()
    }

    /// The workspaces an unfinished create, fork or adopt is bringing into existence, from its
    /// mutation fence until it completes. Everything such a verb makes before publication — its
    /// staged clone, the build volume and seed it forks into that clone, the first build volume
    /// an adopt mints beside main's image — is named only by the verb itself until then, so no
    /// other process may take it for garbage. A verb still `Prepared` has made nothing yet.
    pub fn forming(&self) -> impl Iterator<Item = &WorkspaceName> {
        self.entries
            .iter()
            .filter(|(_, record)| {
                record.completion.is_none()
                    && record.phase == LifecycleIntentPhase::Mutating
                    && matches!(
                        record.operation,
                        LifecycleIntent::Adopt { .. }
                            | LifecycleIntent::ReAdopt { .. }
                            | LifecycleIntent::Create { .. }
                            | LifecycleIntent::Fork { .. }
                    )
            })
            .map(|(workspace, _)| workspace)
    }

    /// The validated source records of the re-adoptions [`Self::forming`] names: what staging
    /// collection reads for the exact mounts and staged images
    /// ([`ReAdoptSource::staging_mounts`], [`ReAdoptSource::staging_images`]) it leaves alone.
    pub fn forming_re_adoptions(&self) -> impl Iterator<Item = &ReAdoptSource> {
        self.entries
            .values()
            .filter_map(|record| match &record.operation {
                LifecycleIntent::ReAdopt { source, .. }
                    if record.completion.is_none()
                        && record.phase == LifecycleIntentPhase::Mutating =>
                {
                    Some(source.as_ref())
                }
                _ => None,
            })
    }

    /// Records `operation` as the workspace's pending intent and returns the record it
    /// supersedes, which a verb refused before its first mutation puts back with
    /// [`Self::discard_prepared_clone_intent`].
    pub fn begin(&mut self, operation: LifecycleIntent) -> Option<LifecycleIntentRecord> {
        self.entries.insert(
            operation.target().clone(),
            LifecycleIntentRecord::pending(operation),
        )
    }
    pub fn mark_mutating(&mut self, workspace: &WorkspaceName) -> CowshedResult<()> {
        let record = self.entries.get_mut(workspace).ok_or_else(|| {
            CowshedError::integrity(
                format!("lifecycle intent for {workspace} disappeared before mutation"),
                "reopen cowshed to reconcile lifecycle state",
            )
        })?;
        if record.completion.is_some() {
            return Err(CowshedError::integrity(
                format!("completed lifecycle intent for {workspace} cannot begin mutation"),
                "reopen cowshed to reconcile lifecycle state",
            ));
        }
        record.phase = LifecycleIntentPhase::Mutating;
        self.validate()
    }
    /// Records one completed publication-fence sub-step for a create/fork intent.
    ///
    /// Monotonic within a record: steps are only ever set, never cleared, so a crash between
    /// two marks leaves the earlier ones durable. Marking a completed intent is refused —
    /// completion already implies a finished fence, and rewriting it would destroy the
    /// crash-window evidence the steps exist to preserve.
    pub fn mark_fence_step(
        &mut self,
        workspace: &WorkspaceName,
        step: FenceStep,
    ) -> CowshedResult<()> {
        let record = self.entries.get_mut(workspace).ok_or_else(|| {
            CowshedError::integrity(
                format!("lifecycle intent for {workspace} disappeared before fencing"),
                "reopen cowshed to reconcile lifecycle state",
            )
        })?;
        if record.completion.is_some() {
            return Err(CowshedError::integrity(
                format!("completed lifecycle intent for {workspace} cannot record fence steps"),
                "reopen cowshed to reconcile lifecycle state",
            ));
        }
        record.fence_steps.get_or_insert_default().mark(step);
        self.validate()
    }

    /// Forgets one retirement which was proven not to have crossed its mutation fence.
    ///
    /// The caller must first observe that the target workspace still exists. Older journals have
    /// no phase field and deserialize as `Prepared`; host state, not journal age, decides whether
    /// those records are safe to discard.
    pub fn discard_prepared_retirement(&mut self, workspace: &WorkspaceName) -> bool {
        let discard = self.entries.get(workspace).is_some_and(|record| {
            record.completion.is_none()
                && record.phase == LifecycleIntentPhase::Prepared
                && matches!(record.operation, LifecycleIntent::Retire { .. })
        });
        if discard {
            self.entries.remove(workspace);
        }
        discard
    }

    /// Withdraws a create or fork intent that never crossed its mutation fence, putting back
    /// `superseded` (the record [`Self::begin`] replaced) or, with none, forgetting the name.
    ///
    /// A refusal before the first durable mutation — no port block left, say — is not an
    /// operation anybody may finish later: replaying it would create a workspace whose caller
    /// was told it failed. The caller must first establish that nothing durable exists:
    /// either it is the verb that just refused before marking the intent `Mutating`, or it is
    /// recovery and found neither the destination, nor a `PendingFence` image, nor a slot
    /// binding of that name.
    pub fn discard_prepared_clone_intent(
        &mut self,
        workspace: &WorkspaceName,
        superseded: Option<LifecycleIntentRecord>,
    ) -> bool {
        let discard = self.entries.get(workspace).is_some_and(|record| {
            record.completion.is_none()
                && record.phase == LifecycleIntentPhase::Prepared
                && matches!(
                    record.operation,
                    LifecycleIntent::Create { .. } | LifecycleIntent::Fork { .. }
                )
        });
        if discard {
            match superseded {
                Some(record) => self.entries.insert(workspace.clone(), record),
                None => self.entries.remove(workspace),
            };
        }
        discard
    }

    /// A refused unpublished-clone retirement must leave its create/fork recovery
    /// authority intact. Only a prepared record can be returned to that state.
    pub fn restore_prepared_clone_intent(&mut self, workspace: &WorkspaceName) -> bool {
        let Some(record) = self.entries.get_mut(workspace) else {
            return false;
        };
        if record.completion.is_some() || record.phase != LifecycleIntentPhase::Prepared {
            return false;
        }
        let LifecycleIntent::Retire {
            origin: Some(origin),
            ..
        } = &record.operation
        else {
            return false;
        };
        record.operation = (**origin).clone();
        true
    }

    pub fn complete(
        &mut self,
        workspace: &WorkspaceName,
        completion: LifecycleIntentCompletion,
    ) -> CowshedResult<()> {
        let record = self.entries.get_mut(workspace).ok_or_else(|| {
            CowshedError::integrity(
                format!("lifecycle intent for {workspace} disappeared before completion"),
                "reopen cowshed to reconcile lifecycle state",
            )
        })?;
        record.completion = Some(completion);
        self.validate()
    }

    /// Replace the Git state an unfinished missing-main re-adoption recorded with `git_before`,
    /// captured again from its plain checkout before a replay copies it (02_workspaces.md,
    /// "Missing main image"). While nothing is published the checkout is the user's live tree,
    /// so a commit or a fetch made after an earlier attempt refused must not wedge every replay
    /// against the state that attempt recorded. The journaled record must still be exactly
    /// `expected`, unfinished and a missing main's; otherwise the journal moved underneath the
    /// capture and nothing is written. Only `git_before` changes.
    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn rerecord_missing_main_git(
        &mut self,
        expected: &ReAdoptSource,
        git_before: crate::git_preservation::GitPreservationSnapshot,
    ) -> CowshedResult<ReAdoptSource> {
        let moved = || {
            CowshedError::integrity(
                "main's re-adoption record moved before its checkout's Git state was recorded again",
                "reopen cowshed to reconcile lifecycle state, then retry the re-adoption",
            )
        };
        let record = self.entries.get_mut(main_name()).ok_or_else(moved)?;
        if record.completion.is_some() {
            return Err(moved());
        }
        let LifecycleIntent::ReAdopt { source, .. } = &mut record.operation else {
            return Err(moved());
        };
        if source.as_ref() != expected || !matches!(source.main, ReAdoptMain::Missing(_)) {
            return Err(moved());
        }
        source.git_before = git_before;
        Ok(source.as_ref().clone())
    }

    pub fn clear(&mut self, workspace: &WorkspaceName) {
        self.entries.remove(workspace);
    }

    fn validate(&self) -> CowshedResult<()> {
        if self.version != LIFECYCLE_INTENT_VERSION {
            return Err(CowshedError::integrity(
                format!(
                    "unsupported lifecycle intent journal version {}",
                    self.version
                ),
                "upgrade cowshed or repair the lifecycle intent journal",
            ));
        }
        for (workspace, record) in &self.entries {
            if record.operation.target() != workspace {
                return Err(CowshedError::integrity(
                    format!(
                        "lifecycle intent record {workspace} disagrees with target {}",
                        record.operation.target()
                    ),
                    "repair the lifecycle intent journal, then reopen cowshed",
                ));
            }
            if let LifecycleIntent::Retire {
                origin: Some(origin),
                ..
            } = &record.operation
                && (origin.target() != workspace
                    || !matches!(
                        origin.as_ref(),
                        LifecycleIntent::Create { .. } | LifecycleIntent::Fork { .. }
                    ))
            {
                return Err(CowshedError::integrity(
                    format!("pending retirement for {workspace} has no matching clone intent"),
                    "repair the lifecycle intent journal, then reopen cowshed",
                ));
            }
            if let LifecycleIntent::ReAdopt { source, .. } = &record.operation {
                source.validate()?;
                // A replacement concludes as exactly the incarnation recorded before its first
                // mutation; any other, the replaced one above all, would report a main that is not
                // the replacement.
                if let Some(LifecycleIntentCompletion::Workspace(incarnation)) = &record.completion
                    && incarnation != &source.replacement
                {
                    return Err(CowshedError::integrity(
                        format!(
                            "re-adoption of {workspace} completes with incarnation {}, not its \
                             recorded replacement {}",
                            incarnation.as_str(),
                            source.replacement.as_str()
                        ),
                        "repair the lifecycle intent journal, then reopen cowshed",
                    ));
                }
            }
            let valid_completion = matches!(
                (&record.operation, &record.completion),
                (_, None)
                    | (
                        LifecycleIntent::Adopt { .. }
                            | LifecycleIntent::ReAdopt { .. }
                            | LifecycleIntent::Create { .. }
                            | LifecycleIntent::Fork { .. },
                        Some(LifecycleIntentCompletion::Workspace(_))
                    )
                    | (
                        LifecycleIntent::Retire { .. },
                        Some(LifecycleIntentCompletion::Retire(_))
                    )
            );
            if !valid_completion {
                return Err(CowshedError::integrity(
                    format!("lifecycle intent record {workspace} has an incompatible completion"),
                    "repair the lifecycle intent journal, then reopen cowshed",
                ));
            }
        }
        Ok(())
    }
}

/// Proof that this process is executing the lifecycle operation recorded for one workspace.
///
/// An unfinished intent is either a crash's residue, which the next process to open the project
/// finishes, or an operation another process is running right now, which nobody else may touch:
/// replaying it runs it a second time beside the first — a second clone, a second bundle writer
/// on the same trash path. The journal cannot tell the two apart; an exclusive lock can. The
/// executing process holds one per workspace for as long as it works on it, and the kernel
/// releases it when that process ends, however it ends. Dropping it releases it at once: it is
/// fenced, so no child still being spawned holds it (`fork_lock`).
#[derive(Debug)]
pub struct IntentLease {
    _file: Fenced<File>,
}

impl IntentLease {
    /// Take the lease on `workspace`'s intent, whose lock file lives in `directory`; `None`
    /// while another process holds it.
    pub fn try_claim(directory: &Path, workspace: &WorkspaceName) -> CowshedResult<Option<Self>> {
        std::fs::create_dir_all(directory).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot create lifecycle lease directory {}: {error}",
                    directory.display()
                ),
                "check controller storage permissions and retry",
            )
        })?;
        let path = directory.join(format!("{workspace}{INTENT_LEASE_SUFFIX}"));
        let file = open_lock_file(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(CowshedError::environment_missing(
                format!("cannot lock lifecycle lease {}: {error}", path.display()),
                "check controller storage permissions and retry",
            )),
        }
    }
}

/// Open (creating) a lock file without following a symlink planted at its name, fenced.
fn open_lock_file(path: &Path) -> CowshedResult<Fenced<File>> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map(Fenced::new)
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot open lifecycle lock {}: {error}", path.display()),
                "check controller storage permissions and retry",
            )
        })
}

/// Store-root intent for the one operation whose project directory changes name.
///
/// It lives at the store root rather than in `lifecycle-intents.json` because that journal moves
/// with the project directory this operation renames, and because the disagreement it describes —
/// a project directory and the identity in its binding naming different things — is store-wide
/// evidence that any reader of the store may need.
///
/// There is at most one at a time: an identity change holds the project actor exclusively, and a
/// second one cannot start while a first is unfinished because the first thing every open does is
/// finish it.
pub const REPO_IDENTITY_INTENT_FILE: &str = ".cowshed-repo-id-intent.json";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryIdentityIntent {
    pub old_repo_id: RepoId,
    pub new_repo_id: RepoId,
    pub old_project_root: PathBuf,
    pub new_project_root: PathBuf,
    pub old_mount_root: PathBuf,
    pub new_mount_root: PathBuf,
    /// `Prepared` means nothing durable has been touched and the record may be discarded.
    /// `Mutating` is the mutation fence: it is written only once every volume is unmounted and the
    /// next act is a directory rename, so recovery from it is always forward.
    #[serde(default)]
    pub phase: LifecycleIntentPhase,
}

impl RepositoryIdentityIntent {
    /// Refuse anything that is not a well-formed identity change of this store.
    ///
    /// A malformed or truncated record is refused rather than skipped: it is evidence that a
    /// transaction was interrupted, and silently ignoring it would leave the store half-renamed
    /// with nothing left to say so.
    ///
    /// Both project roots must be inside the store, and the two mount roots must be siblings under
    /// one mount root — the workspace mount root is configurable and deliberately lives outside the
    /// store, so containment there is expressed as "the same place, a different name" rather than a
    /// prefix. Either way a record naming somewhere else can never make recovery rename a directory
    /// cowshed does not own.
    pub fn validate(&self, store_root: &Path) -> CowshedResult<()> {
        let paths = [
            ("old project", &self.old_project_root, true),
            ("new project", &self.new_project_root, true),
            ("old mount", &self.old_mount_root, false),
            ("new mount", &self.new_mount_root, false),
        ];
        for (name, path, inside_store) in paths {
            if !path.is_absolute() {
                return Err(malformed_identity_intent(format!(
                    "repository identity intent has a relative {name} path {}",
                    path.display()
                )));
            }
            if inside_store && !path.starts_with(store_root) {
                return Err(malformed_identity_intent(format!(
                    "repository identity intent {name} path {} escapes store {}",
                    path.display(),
                    store_root.display()
                )));
            }
        }
        // `<mount-root>/<owner>/<repo>` on both sides, so the shared grandparent is the fixed
        // per-user mount root itself.
        if self.old_mount_root.parent().and_then(Path::parent)
            != self.new_mount_root.parent().and_then(Path::parent)
        {
            return Err(malformed_identity_intent(format!(
                "repository identity intent mount paths {} and {} are not under one mount root",
                self.old_mount_root.display(),
                self.new_mount_root.display()
            )));
        }
        if self.old_repo_id == self.new_repo_id
            || self.old_project_root == self.new_project_root
            || self.old_mount_root == self.new_mount_root
        {
            return Err(malformed_identity_intent(
                "repository identity intent does not describe a change".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn path(store_root: &Path) -> PathBuf {
        store_root.join(REPO_IDENTITY_INTENT_FILE)
    }

    pub fn persist(&self, store_root: &Path) -> CowshedResult<()> {
        self.validate(store_root)?;
        let path = Self::path(store_root);
        write_json(&path, self).map_err(|error| {
            CowshedError::storage_failure(
                format!(
                    "cannot persist repository identity intent {}: {error}",
                    path.display()
                ),
                &error,
                "repair cowshed storage and retry `cowshed mv main --repo-id`",
            )
        })
    }

    pub fn load(store_root: &Path) -> CowshedResult<Option<Self>> {
        let path = Self::path(store_root);
        match read_json::<Self>(&path) {
            Ok(intent) => {
                intent.validate(store_root)?;
                Ok(Some(intent))
            }
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(error) => Err(malformed_identity_intent(format!(
                "cannot read repository identity intent {}: {error}",
                path.display()
            ))),
        }
    }

    pub fn clear(store_root: &Path) -> CowshedResult<()> {
        let path = Self::path(store_root);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CowshedError::storage_failure(
                format!(
                    "cannot clear repository identity intent {}: {error}",
                    path.display()
                ),
                &error,
                "repair cowshed storage and retry `cowshed mv main --repo-id`",
            )),
        }
    }
}

fn malformed_identity_intent(message: String) -> CowshedError {
    CowshedError::integrity(
        message,
        "inspect the identity intent with `cowshed doctor` before removing it — a project may be \
         half-renamed",
    )
}

/// The refusal a retained `Prepared` retirement produces when its target state cannot be read.
///
/// macOS-only because its sole caller is the APFS lifecycle recovery path, which is itself
/// `cfg(target_os = "macos")`. Without the gate a Linux `cargo check` of the library alone sees
/// no caller and `-D dead-code` fails the build, while a local `--all-targets` run hides it
/// behind the test below.
#[cfg(target_os = "macos")]
pub(crate) fn prepared_retirement_unreadable(
    workspace: &WorkspaceName,
    expected_mount: &Path,
    cause: CowshedError,
) -> CowshedError {
    CowshedError::new(
        cause.code,
        format!(
            "cannot establish retirement state for workspace {workspace} at {}: {}",
            expected_mount.display(),
            cause.message
        ),
        format!(
            "retirement record for {workspace} was kept deliberately; make {} and its workspace \
             image readable, then reopen cowshed",
            expected_mount.display()
        ),
    )
}

#[cfg(test)]
pub(crate) mod re_adopt_tests;

#[cfg(all(test, target_os = "macos"))]
mod prepared_retirement_diagnostic_tests {
    use std::path::Path;

    use crate::error::{CowshedError, ErrorCode};
    use crate::metadata::WorkspaceName;

    use super::prepared_retirement_unreadable;

    #[test]
    fn unreadable_state_names_the_target_and_preserves_the_recovery_record() {
        let workspace = WorkspaceName::new("unreadable").expect("workspace");
        let error = prepared_retirement_unreadable(
            &workspace,
            Path::new("/tmp/cowshed/example/unreadable"),
            CowshedError::environment_missing(
                "workspace metadata could not be read",
                "repair the metadata",
            ),
        );

        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert!(error.message.contains("workspace unreadable"));
        assert!(error.message.contains("/tmp/cowshed/example/unreadable"));
        assert!(error.message.contains("metadata could not be read"));
        assert!(
            error
                .hint
                .contains("record for unreadable was kept deliberately")
        );
        assert!(error.hint.contains("make /tmp/cowshed/example/unreadable"));
    }
}

static MAIN_NAME: LazyLock<WorkspaceName> = LazyLock::new(WorkspaceName::main);

fn main_name() -> &'static WorkspaceName {
    &MAIN_NAME
}
