//! The one declared inventory of what cowshed writes under a project's terminal store trees
//! (`.staging/`, `sessions/`, `tmp/` and `build/`, beside the project root's own locks;
//! 01_storage.md, "Directory layout"), and what the project's unbinding does with each
//! (02_workspaces.md, "Checkout layouts"). Cowshed's derived state is discarded: cowshed
//! regenerates all of it. User work, the only copy of something, is refused by name until the
//! user discards it. A path the inventory does not name is not cowshed's, and is refused by name
//! too.

use std::ffi::OsStr;
use std::path::{Component, Path};

use cowshed_apfs::image::IMAGE_EXTENSION;

use crate::build_volume::{COMPACTING_DIRECTORY, HOLD_SUFFIX, IMAGES_DIRECTORY, VolumeFile};
use crate::metadata::{GRANTS_SIDECAR_SUFFIX, WorkspaceIncarnation};
use crate::repository::{EXEC_TEMP_DIRECTORY, SESSIONS_DIRECTORY};
use crate::storage::apfs::release::DELETING_DIRECTORY;
use crate::storage::recovery::{
    ABANDON_BUNDLE_EXTENSION, INTENT_LEASE_SUFFIX, PATCH_IDS_SUFFIX, STAGING_NAMESPACE,
    TRASH_NAMESPACE,
};

/// Where builds before the background verbs traced their runs kept the land's collection log
/// (16_build_volumes.md, Land 6.5). Nothing writes it any more; a store that ran such a land
/// still holds it.
const EARLIER_COLLECTING_DIRECTORY: &str = ".collecting";
/// The background verbs' logs and the compaction's failure record (`failed`, staged as
/// `failed.<id>`) of those earlier builds, in their directories.
const EARLIER_LOG: &str = "log";
const EARLIER_FAILURE: &str = "failed";

/// What the unbinding does with an artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Class {
    /// Cowshed's own regenerable state: the unbinding discards it.
    Derived(Disposal),
    /// The only copy of the work it names: the unbinding refuses it, saying why, until the user
    /// discards it (`unadopt --abandon`).
    UserWork(&'static str),
}

/// How the unbinding discards a derived artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Disposal {
    /// Removed: a file, or a directory's whole tree.
    Remove,
    /// An image, renamed into the store's `.deleting` for the gateway's cleanup: the unbinding
    /// frees nothing itself (05_gateway.md, "Deleting released images").
    Release,
    /// A build volume's image or record. The unbinding releases every volume before it plans
    /// (16_build_volumes.md, "Garbage collection"), so one still there is held by a job or its
    /// release failed, and refuses until it is released.
    VolumeRelease,
}

macro_rules! store_artifacts {
    ($($(#[doc = $doc:literal])* $variant:ident,)*) => {
        /// One kind of entry cowshed writes under a project's terminal store trees.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) enum StoreArtifact {
            $($(#[doc = $doc])* $variant,)*
        }

        impl StoreArtifact {
            /// Every kind, in declaration order.
            #[cfg(test)]
            pub(crate) const ALL: &[Self] = &[$(Self::$variant,)*];
        }
    };
}

store_artifacts! {
    /// `<name>.lock` at the project root: main's image lock and the journal's lock.
    ProjectLock,
    /// `sessions/<ws>.dmg`: a session workspace's image. The unbinding comes after every
    /// session's own removal, which retires each one whose work main holds.
    SessionImage,
    /// `sessions/<ws>.dmg.grants.json`: its grants and detached metadata.
    SessionGrants,
    /// `sessions/<ws>.dmg.ca.key`: its workspace CA key.
    SessionCaKey,
    /// `sessions/<ws>.dmg.lock`: its lifecycle lock, holding its last taker's record.
    ImageLock,
    /// `sessions/<ws>.intent.lock`: the lease of the process executing its lifecycle intent.
    IntentLease,
    /// `sessions/<ws>.land.lock`: the lease a land or an rm of it holds.
    LandLease,
    /// `sessions/.trash/<ws>-<tip>.bundle`: the abandoned work an `rm --abandon` kept.
    AbandonBundle,
    /// `sessions/.trash/<bundle>.patch-ids`: the patch identities computed from it. Derived: beside its
    /// bundle the bundle's refusal keeps it, and one whose bundle is gone holds nothing.
    AbandonBundlePatchIds,
    /// `sessions/.trash/.<name>.<id>.partial`: a bundle or its patch identities being written.
    StagedBundle,
    /// `sessions/.trash/.bundle-verify-<id>/`: the bare repository a bundle is verified in.
    BundleVerification,
    /// `sessions/.trash/<ws>-<inc>.dmg`: a retired workspace's image.
    RetiredImage,
    /// `sessions/.trash/<ws>-<inc>.dmg.grants.json`: its grants.
    RetiredGrants,
    /// `sessions/.trash/<ws>-<inc>.dmg.ca.key`: its CA key.
    RetiredCaKey,
    /// `sessions/.deleting/<name>.dmg`: a released image waiting for the gateway's cleanup.
    ReleasedSessionImage,
    /// `tmp/<ws>/`: a workspace's TMPDIR.
    ExecTemp,
    /// `build/<id>.dmg`: a build volume.
    BuildVolumeImage,
    /// `build/<id>.dmg.json`: its record.
    BuildVolumeRecord,
    /// `build/<id>.dmg.hold`: the lock its jobs hold.
    BuildVolumeHold,
    /// `build/.deleting/<id>.dmg`: a released build volume waiting for the gateway's cleanup.
    ReleasedBuildVolume,
    /// `build/.compacting/lock`: the seed compaction's one-at-a-time lock.
    CompactionLock,
    /// `build/.compacting/<name>.dmg`: the clone a seed compaction copies, or its copy.
    CompactionImage,
    /// `build/.collecting/log`, `build/.compacting/log`, `build/.compacting/failed` and its
    /// staged `failed.<id>`: what earlier builds' background verbs wrote.
    EarlierBackgroundRecord,
    /// `.staging/<ws>-<inc>.dmg`, `recover-<ws>-<inc>.dmg` or `readopt-<inc>-build.dmg`: an
    /// image an unfinished lifecycle transaction staged.
    StagedImage,
    /// `.staging/<stem>.dmg.grants.json`: its grants.
    StagedGrants,
    /// `.staging/<stem>.dmg.ca.key`: its CA key.
    StagedCaKey,
    /// `.<name>.tmp.<id>` beside the file it publishes (`fsio::temp_name`): a write a crash cut.
    PublicationTemp,
}

impl StoreArtifact {
    /// What the unbinding does with this artifact.
    pub(crate) fn class(self) -> Class {
        use Disposal::{Release, Remove, VolumeRelease};
        match self {
            Self::SessionImage => Class::UserWork(
                "a session workspace whose removal kept it: it holds work main does not",
            ),
            Self::AbandonBundle => {
                Class::UserWork("the abandoned work an `rm --abandon` kept, its only copy")
            }
            Self::BuildVolumeImage | Self::BuildVolumeRecord => Class::Derived(VolumeRelease),
            Self::RetiredImage
            | Self::ReleasedSessionImage
            | Self::ReleasedBuildVolume
            | Self::CompactionImage
            | Self::StagedImage => Class::Derived(Release),
            Self::ProjectLock
            | Self::AbandonBundlePatchIds
            | Self::SessionGrants
            | Self::SessionCaKey
            | Self::ImageLock
            | Self::IntentLease
            | Self::LandLease
            | Self::StagedBundle
            | Self::BundleVerification
            | Self::RetiredGrants
            | Self::RetiredCaKey
            | Self::ExecTemp
            | Self::BuildVolumeHold
            | Self::CompactionLock
            | Self::EarlierBackgroundRecord
            | Self::StagedGrants
            | Self::StagedCaKey
            | Self::PublicationTemp => Class::Derived(Remove),
        }
    }

    /// The artifact at `relative`, a path under the project root, that is a directory when
    /// `directory` holds. `None` for anything else: a directory the inventory names nothing for
    /// holds entries that are each judged, and a file is not cowshed's.
    pub(crate) fn of(relative: &Path, directory: bool) -> Option<Self> {
        let mut parts = [""; 3];
        let mut depth = 0;
        for component in relative.components() {
            let Component::Normal(part) = component else {
                return None;
            };
            *parts.get_mut(depth)? = part.to_str()?;
            depth += 1;
        }
        match (&parts[..depth], directory) {
            ([name], false) => name.ends_with(".lock").then_some(Self::ProjectLock),
            ([SESSIONS_DIRECTORY, name], false) => session_file(name),
            ([SESSIONS_DIRECTORY, TRASH_NAMESPACE, name], false) => trash_file(name),
            ([SESSIONS_DIRECTORY, TRASH_NAMESPACE, name], true) => name
                .starts_with(crate::git::BUNDLE_VERIFICATION_PREFIX)
                .then_some(Self::BundleVerification),
            ([SESSIONS_DIRECTORY, DELETING_DIRECTORY, name], _) => {
                crate::storage::apfs::is_deleting_image_name(name)
                    .then_some(Self::ReleasedSessionImage)
            }
            ([EXEC_TEMP_DIRECTORY, _], true) => Some(Self::ExecTemp),
            ([IMAGES_DIRECTORY, name], false) => build_file(name),
            ([IMAGES_DIRECTORY, DELETING_DIRECTORY, name], _) => {
                crate::build_volume::is_image_name(name).then_some(Self::ReleasedBuildVolume)
            }
            ([IMAGES_DIRECTORY, COMPACTING_DIRECTORY, name], false) => compacting_file(name),
            ([IMAGES_DIRECTORY, EARLIER_COLLECTING_DIRECTORY, EARLIER_LOG], false) => {
                Some(Self::EarlierBackgroundRecord)
            }
            ([STAGING_NAMESPACE, name], false) => staging_file(name),
            _ => None,
        }
    }
}

/// The stem a re-adoption stages main's build volume under in `.staging`: its image is
/// `readopt-<inc>-build.dmg` and its mountpoint `readopt-<inc>-build`.
pub(crate) fn readopt_build_stem(replacement: &WorkspaceIncarnation) -> String {
    format!("{READOPT_PREFIX}{}{READOPT_SUFFIX}", replacement.as_str())
}

const READOPT_PREFIX: &str = "readopt-";
const READOPT_SUFFIX: &str = "-build";

fn is_publication_temp(name: &str) -> bool {
    cowshed_os::fsio::is_temp_artifact(OsStr::new(name))
}

/// `name` without its `.dmg`, when it has a non-empty stem.
fn image_stem(name: &str) -> Option<&str> {
    name.strip_suffix(IMAGE_EXTENSION)?
        .strip_suffix('.')
        .filter(|stem| !stem.is_empty())
}

/// The image name an image's companion `name` (`<image>.grants.json`, `<image>.ca.key`) belongs
/// to, and which companion it is.
fn companion(name: &str) -> Option<(&str, Companion)> {
    name.strip_suffix(GRANTS_SIDECAR_SUFFIX)
        .map(|image| (image, Companion::Grants))
        .or_else(|| {
            name.strip_suffix(crate::storage::apfs::COMPANION_SUFFIX)
                .map(|image| (image, Companion::CaKey))
        })
}

#[derive(Clone, Copy)]
enum Companion {
    Grants,
    CaKey,
}

fn session_file(name: &str) -> Option<StoreArtifact> {
    let is_session_image = |image: &str| {
        crate::storage::image_workspace(Path::new(image))
            .is_some_and(|workspace| !workspace.is_main())
    };
    if crate::storage::is_image_lock(name) {
        Some(StoreArtifact::ImageLock)
    } else if name.ends_with(INTENT_LEASE_SUFFIX) {
        Some(StoreArtifact::IntentLease)
    } else if name.ends_with(crate::runtime::land_lease::LAND_LEASE_SUFFIX) {
        Some(StoreArtifact::LandLease)
    } else if is_publication_temp(name) {
        Some(StoreArtifact::PublicationTemp)
    } else if let Some((image, which)) = companion(name) {
        is_session_image(image).then_some(match which {
            Companion::Grants => StoreArtifact::SessionGrants,
            Companion::CaKey => StoreArtifact::SessionCaKey,
        })
    } else {
        is_session_image(name).then_some(StoreArtifact::SessionImage)
    }
}

fn trash_file(name: &str) -> Option<StoreArtifact> {
    let is_retired_image = |image: &str| {
        image_stem(image)
            .and_then(crate::storage::apfs::split_retired_stem)
            .is_some()
    };
    let is_bundle = |bundle: &str| {
        bundle
            .strip_suffix(ABANDON_BUNDLE_EXTENSION)
            .is_some_and(|stem| stem.ends_with('.'))
    };
    if name.starts_with('.') && name.ends_with(crate::git::STAGED_BUNDLE_SUFFIX) {
        Some(StoreArtifact::StagedBundle)
    } else if let Some(bundle) = name.strip_suffix(PATCH_IDS_SUFFIX) {
        is_bundle(bundle).then_some(StoreArtifact::AbandonBundlePatchIds)
    } else if is_bundle(name) {
        Some(StoreArtifact::AbandonBundle)
    } else if let Some((image, which)) = companion(name) {
        is_retired_image(image).then_some(match which {
            Companion::Grants => StoreArtifact::RetiredGrants,
            Companion::CaKey => StoreArtifact::RetiredCaKey,
        })
    } else {
        is_retired_image(name).then_some(StoreArtifact::RetiredImage)
    }
}

fn build_file(name: &str) -> Option<StoreArtifact> {
    if let Some(file) = VolumeFile::of(OsStr::new(name)) {
        return Some(match file {
            VolumeFile::Image(_) => StoreArtifact::BuildVolumeImage,
            VolumeFile::Record(_) => StoreArtifact::BuildVolumeRecord,
        });
    }
    if name
        .strip_suffix(HOLD_SUFFIX)
        .is_some_and(crate::build_volume::is_image_name)
    {
        Some(StoreArtifact::BuildVolumeHold)
    } else {
        is_publication_temp(name).then_some(StoreArtifact::PublicationTemp)
    }
}

fn compacting_file(name: &str) -> Option<StoreArtifact> {
    if name == crate::build_volume::COMPACTION_LOCK {
        Some(StoreArtifact::CompactionLock)
    } else if name == EARLIER_LOG
        || name == EARLIER_FAILURE
        || name
            .strip_prefix(EARLIER_FAILURE)
            .is_some_and(|staged| staged.starts_with('.'))
    {
        Some(StoreArtifact::EarlierBackgroundRecord)
    } else {
        image_stem(name).map(|_| StoreArtifact::CompactionImage)
    }
}

fn staging_file(name: &str) -> Option<StoreArtifact> {
    let is_staged_image = |image: &str| {
        image_stem(image).is_some_and(|stem| {
            crate::storage::apfs::native::staged_stem_workspace(stem).is_some()
                || stem
                    .strip_prefix(READOPT_PREFIX)
                    .and_then(|rest| rest.strip_suffix(READOPT_SUFFIX))
                    .is_some_and(|incarnation| WorkspaceIncarnation::new(incarnation).is_ok())
        })
    };
    if is_publication_temp(name) {
        Some(StoreArtifact::PublicationTemp)
    } else if let Some((image, which)) = companion(name) {
        is_staged_image(image).then_some(match which {
            Companion::Grants => StoreArtifact::StagedGrants,
            Companion::CaKey => StoreArtifact::StagedCaKey,
        })
    } else {
        is_staged_image(name).then_some(StoreArtifact::StagedImage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn work_and_its_orphan_patch_ids_have_distinct_classes(
            workspace in "work-[a-z]{1,16}",
            tip in "[a-f0-9]{40}",
        ) {
            let image = format!("sessions/{workspace}.dmg");
            let bundle = format!("sessions/.trash/{workspace}-{tip}.bundle");
            let patch_ids = format!("{bundle}.patch-ids");
            for path in [&image, &bundle] {
                let artifact = StoreArtifact::of(Path::new(path), false).expect("user work");
                prop_assert!(matches!(artifact.class(), Class::UserWork(_)));
            }
            let artifact = StoreArtifact::of(Path::new(&patch_ids), false).expect("derived sidecar");
            prop_assert_eq!(artifact, StoreArtifact::AbandonBundlePatchIds);
            prop_assert_eq!(artifact.class(), Class::Derived(Disposal::Remove));
        }

        #[test]
        fn unknown_files_are_not_inventory_in_any_terminal_namespace(name in "[a-z]{1,24}") {
            for namespace in ["", "sessions", "sessions/.trash", "sessions/.deleting", "build", "build/.compacting", "build/.collecting", "build/.deleting", ".staging", "tmp"] {
                let path = Path::new(namespace).join(format!("user-{name}.txt"));
                prop_assert_eq!(StoreArtifact::of(&path, false), None, "{}", path.display());
            }
        }
    }
}
