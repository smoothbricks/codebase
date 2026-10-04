//! Retiring the caches volume: steps 1–3 of 03_caches.md, "Retiring the caches volume".
//!
//! Earlier releases kept layer-3 caches on a dedicated volume and linked the host's tool paths
//! into it. Every directory on that volume now has a home in the host HOME: a detector's tool
//! cache at the tool's own default, the gateway's mirrors in cowshed's cache directory, sccache's
//! store at sccache's default, and a repository-placed cache at the `[caches] home` path an
//! adopted project's main declares. Retirement moves each one there.
//!
//! The work is split the usual way: [`observe`] reads the volume and the host paths, [`plan`] is a
//! pure function from that observation to the moves, merges, drops and leftovers, and [`execute`]
//! is the thin imperative shell that performs a plan. Nothing is deleted that was not moved or
//! proven a duplicate: a content-addressed cache's entry the host already holds under the same key
//! names the same bytes, and Nix's client state, which is not content-addressed, keeps the host's
//! copy. Every step is idempotent and resumable: a copy goes to a recognizable staging name beside
//! its destination and is published with one rename, so a crash leaves either the source intact or
//! a complete destination, and the next run cleans the staging name and carries on.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use crate::capabilities::cache::SharedToolHome;
use crate::capabilities::{bun, cargo, go, gradle, nix, npm, pnpm, sccache, uv, zig};
use crate::fork_lock::Run as _;
use crate::storage::bootstrap::VOLUME_MARKER_FILE;

/// The suffix of a copy that is not yet published: `.<name>.cowshed-retiring` beside `<name>`.
pub const STAGING_SUFFIX: &str = ".cowshed-retiring";

/// What the volume's own filesystem keeps at its root: cowshed's marker and macOS's per-volume
/// bookkeeping. None of it is a cache, and none of it outlives the volume.
const BOOKKEEPING: [&str; 7] = [
    VOLUME_MARKER_FILE,
    ".fseventsd",
    ".Spotlight-V100",
    ".Trashes",
    ".TemporaryItems",
    ".DocumentRevisions-V100",
    ".DS_Store",
];

/// How a volume directory reconciles with a copy the host already holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Merge {
    /// The same key names the same bytes: entries the host lacks move in, entries it holds are
    /// dropped from the volume as duplicates.
    ContentAddressed,
    /// Not content-addressed (Nix's client state): the host's copy wins whole.
    HostWins,
}

/// The process that writes a cache while it runs, which must not run while it moves.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Owner {
    /// Cargo's own package-cache locks keep every cargo process out for the move.
    Cargo,
    /// cowshed-gateway writes its mirrors; setup stops it around the move.
    Gateway,
    /// The sccache daemon writes its store; setup stops it around the move.
    Sccache,
}

/// One volume directory and the host directory it moves to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Placement {
    /// What it is, in words: `cargo registry`, `gateway mirror`.
    pub label: String,
    pub volume: PathBuf,
    pub host: PathBuf,
    pub merge: Merge,
    pub owner: Option<Owner>,
}

enum Destination {
    Tool(&'static SharedToolHome, Option<&'static str>),
    Sccache,
    CowshedCache(fn(&Path) -> PathBuf),
}

struct Retired {
    volume: &'static str,
    label: &'static str,
    destination: Destination,
    merge: Merge,
    owner: Option<Owner>,
}

/// Every directory the volume held for a detector, the gateway or sccache, where it lived there.
fn retired() -> [Retired; 16] {
    use Destination::{CowshedCache, Sccache, Tool};
    use Merge::{ContentAddressed, HostWins};
    let tool = |volume, label, home, child, owner| Retired {
        volume,
        label,
        destination: Tool(home, child),
        merge: ContentAddressed,
        owner,
    };
    [
        tool(
            "cargo/registry",
            "cargo registry",
            &cargo::HOME,
            Some("registry"),
            Some(Owner::Cargo),
        ),
        tool(
            "cargo/git",
            "cargo git",
            &cargo::HOME,
            Some("git"),
            Some(Owner::Cargo),
        ),
        tool(
            "bun/install/cache",
            "bun install cache",
            &bun::BUN_HOME,
            None,
            None,
        ),
        tool("npm", "npm cache", &npm::NPM_HOME, None, None),
        tool("pnpm/store", "pnpm store", &pnpm::PNPM_HOME, None, None),
        tool("uv", "uv cache", &uv::UV_HOME, None, None),
        tool("zig", "zig global cache", &zig::ZIG_HOME, None, None),
        tool(
            "gradle/caches",
            "gradle caches",
            &gradle::GRADLE_HOME,
            Some("caches"),
            None,
        ),
        tool("go/mod", "go module cache", &go::MODULE_HOME, None, None),
        tool("go/build", "go build cache", &go::BUILD_HOME, None, None),
        Retired {
            merge: HostWins,
            ..tool(
                "nix/cache",
                "nix fetcher cache",
                &nix::CACHE_HOME,
                None,
                None,
            )
        },
        Retired {
            merge: HostWins,
            ..tool("nix/state", "nix state", &nix::STATE_HOME, None, None)
        },
        Retired {
            volume: "sccache",
            label: "sccache store",
            destination: Sccache,
            merge: ContentAddressed,
            owner: Some(Owner::Sccache),
        },
        Retired {
            volume: "mirror",
            label: "gateway mirror",
            destination: CowshedCache(crate::host_dirs::gateway_mirror),
            merge: ContentAddressed,
            owner: Some(Owner::Gateway),
        },
        Retired {
            volume: "repo-mirrors",
            label: "repository mirrors",
            destination: CowshedCache(crate::host_dirs::repo_mirrors),
            merge: ContentAddressed,
            owner: Some(Owner::Gateway),
        },
        // Where `repo mirror` cloned before its mirrors had a name of their own.
        Retired {
            volume: "mirrors",
            label: "repository mirrors",
            destination: CowshedCache(crate::host_dirs::repo_mirrors),
            merge: ContentAddressed,
            owner: None,
        },
    ]
}

/// Where every directory the volume may hold belongs on this host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetirementLayout {
    home: PathBuf,
    volume: PathBuf,
    cargo_home: PathBuf,
    /// Keyed by the volume-relative path.
    placements: BTreeMap<PathBuf, Placement>,
    /// The absolute host paths adopted projects' mains declare in `[caches] home`.
    declared: BTreeSet<PathBuf>,
}

impl RetirementLayout {
    /// `declared` holds the `[caches] home` entries, HOME-relative, of every adopted project's main.
    pub fn new(home: &Path, volume: &Path, declared: &[PathBuf]) -> Self {
        let placements = retired()
            .into_iter()
            .map(|retired| {
                let host = match retired.destination {
                    Destination::Tool(tool, Some(child)) => tool.host_path(home).join(child),
                    Destination::Tool(tool, None) => tool.host_path(home),
                    Destination::Sccache => sccache::cache_directory(home),
                    Destination::CowshedCache(path) => path(home),
                };
                (
                    PathBuf::from(retired.volume),
                    Placement {
                        label: retired.label.to_owned(),
                        volume: volume.join(retired.volume),
                        host,
                        merge: retired.merge,
                        owner: retired.owner,
                    },
                )
            })
            .collect();
        Self {
            volume: volume.to_path_buf(),
            home: home.to_path_buf(),
            cargo_home: cargo::HOME.host_path(home),
            placements,
            declared: declared
                .iter()
                .map(|relative| home.join(relative))
                .collect(),
        }
    }

    pub fn volume(&self) -> &Path {
        &self.volume
    }

    /// A volume path some placement lies beneath: its children are decided one by one.
    fn is_interior(&self, relative: &Path) -> bool {
        self.placements
            .keys()
            .any(|placement| placement != relative && placement.starts_with(relative))
    }

    /// Every host path a plan may write to, for [`observe`].
    fn destinations(&self) -> impl Iterator<Item = &Path> {
        self.placements
            .values()
            .map(|placement| placement.host.as_path())
            .chain(self.declared.iter().map(PathBuf::as_path))
    }

    /// The declared host paths whose final component is `name`.
    fn declared_named(&self, name: &OsStr) -> Vec<PathBuf> {
        self.declared
            .iter()
            .filter(|path| path.file_name() == Some(name))
            .cloned()
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryKind {
    Directory { empty: bool },
    File,
    Symlink,
}

/// One volume entry the planner decides: a root entry, or a child of a directory some placement
/// lies beneath.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeEntry {
    /// Relative to the volume root.
    pub path: PathBuf,
    pub kind: EntryKind,
    /// Apparent bytes of every file beneath it; zero for a directory the planner descends into.
    pub bytes: u64,
}

/// What stands at a destination host path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostState {
    Absent,
    /// A link that resolves to the volume directory it replaces.
    LinkIntoVolume,
    Directory,
    /// A link into the nix store: a home-manager, NixOS or nix-darwin module owns it.
    ModuleLink(PathBuf),
    /// A link to anything else.
    OtherLink(PathBuf),
    NotDirectory,
}

/// The volume and host as [`observe`] found them: the planner's whole input.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Observation {
    pub entries: Vec<VolumeEntry>,
    pub hosts: BTreeMap<PathBuf, HostState>,
    /// Staging names a crashed run left beside a destination.
    pub staging: Vec<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Step {
    /// Remove an unpublished copy a crashed run left behind.
    CleanStaging(PathBuf),
    /// Copy the volume directory to the host path through a staging name, publish it with one
    /// rename (replacing the host's link into the volume when `replace_link`), then remove it
    /// from the volume.
    Move {
        placement: Placement,
        replace_link: bool,
        bytes: u64,
    },
    /// Both hold a copy: reconcile by [`Placement::merge`].
    Merge { placement: Placement, bytes: u64 },
    /// Remove an empty volume directory.
    RemoveEmpty(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeftoverReason {
    /// No detector names it and no adopted project's main declares a `[caches] home` path with
    /// its name.
    Undeclared,
    /// More than one declared `[caches] home` path has its name.
    Ambiguous(Vec<PathBuf>),
    /// Its destination holds something setup does not replace.
    Conflict(String),
}

/// Something retirement leaves on the volume, named with its size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Leftover {
    pub path: PathBuf,
    pub bytes: u64,
    pub reason: LeftoverReason,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Plan {
    pub steps: Vec<Step>,
    pub leftovers: Vec<Leftover>,
}

impl Plan {
    /// The writers that must not run while this plan executes.
    pub fn owners(&self) -> BTreeSet<Owner> {
        self.steps
            .iter()
            .filter_map(|step| match step {
                Step::Move { placement, .. } | Step::Merge { placement, .. } => placement.owner,
                Step::CleanStaging(_) | Step::RemoveEmpty(_) => None,
            })
            .collect()
    }
}

/// `.<name>.cowshed-retiring` beside `path`.
pub fn staging_path(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?;
    let mut staging = OsString::from(".");
    staging.push(name);
    staging.push(STAGING_SUFFIX);
    Some(path.with_file_name(staging))
}

fn is_bookkeeping(relative: &Path) -> bool {
    let mut components = relative.components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(name)), None) if BOOKKEEPING.iter().any(|entry| name == *entry)
    )
}

/// Decide every volume entry. Pure: the observation is the whole input.
pub fn plan(layout: &RetirementLayout, observation: &Observation) -> Plan {
    let mut plan = Plan {
        steps: observation
            .staging
            .iter()
            .cloned()
            .map(Step::CleanStaging)
            .collect(),
        leftovers: Vec::new(),
    };
    let mut emptied = Vec::new();
    for entry in &observation.entries {
        if is_bookkeeping(&entry.path) {
            continue;
        }
        let absolute = layout.volume.join(&entry.path);
        if let Some(placement) = layout.placements.get(&entry.path) {
            place(
                &mut plan,
                &mut emptied,
                layout,
                observation,
                placement.clone(),
                entry,
            );
        } else if layout.is_interior(&entry.path)
            && matches!(entry.kind, EntryKind::Directory { .. })
        {
            emptied.push(absolute);
        } else if let EntryKind::Directory { empty: true } = entry.kind {
            emptied.push(absolute);
        } else if entry.path.components().count() == 1
            && let EntryKind::Directory { empty: false } = entry.kind
        {
            let name = entry.path.as_os_str();
            match layout.declared_named(name).as_slice() {
                [] => plan.leftovers.push(Leftover {
                    path: absolute,
                    bytes: entry.bytes,
                    reason: LeftoverReason::Undeclared,
                }),
                [host] => {
                    let placement = Placement {
                        label: format!("{} (repository cache)", name.to_string_lossy()),
                        volume: absolute,
                        host: host.clone(),
                        merge: Merge::ContentAddressed,
                        owner: None,
                    };
                    place(
                        &mut plan,
                        &mut emptied,
                        layout,
                        observation,
                        placement,
                        entry,
                    );
                }
                candidates => plan.leftovers.push(Leftover {
                    path: absolute,
                    bytes: entry.bytes,
                    reason: LeftoverReason::Ambiguous(candidates.to_vec()),
                }),
            }
        } else {
            plan.leftovers.push(Leftover {
                path: absolute,
                bytes: entry.bytes,
                reason: LeftoverReason::Undeclared,
            });
        }
    }
    // A directory still holding a leftover stays, named through the leftover. The rest go deepest
    // first, so an interior directory is removed only after its children.
    emptied.retain(|directory| {
        !plan
            .leftovers
            .iter()
            .any(|leftover| leftover.path != *directory && leftover.path.starts_with(directory))
    });
    emptied.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    plan.steps
        .extend(emptied.into_iter().map(Step::RemoveEmpty));
    plan
}

fn place(
    plan: &mut Plan,
    emptied: &mut Vec<PathBuf>,
    layout: &RetirementLayout,
    observation: &Observation,
    placement: Placement,
    entry: &VolumeEntry,
) {
    let EntryKind::Directory { empty } = entry.kind else {
        plan.leftovers.push(Leftover {
            path: placement.volume,
            bytes: entry.bytes,
            reason: LeftoverReason::Conflict("it is not a directory".to_owned()),
        });
        return;
    };
    let host = observation
        .hosts
        .get(&placement.host)
        .cloned()
        .unwrap_or(HostState::Absent);
    let bytes = entry.bytes;
    let conflict = |reason: String| Leftover {
        path: placement.volume.clone(),
        bytes,
        reason: LeftoverReason::Conflict(reason),
    };
    match host {
        HostState::LinkIntoVolume => plan.steps.push(Step::Move {
            placement,
            replace_link: true,
            bytes,
        }),
        HostState::Absent | HostState::Directory if empty => emptied.push(placement.volume),
        HostState::Absent => plan.steps.push(Step::Move {
            placement,
            replace_link: false,
            bytes,
        }),
        HostState::Directory => plan.steps.push(Step::Merge { placement, bytes }),
        HostState::ModuleLink(target) => {
            let option = placement
                .host
                .strip_prefix(&layout.home)
                .unwrap_or(&placement.host)
                .display()
                .to_string();
            plan.leftovers.push(conflict(format!(
                "a home-manager, NixOS or nix-darwin module links {} to {}; remove the module option that declares it (home-manager: home.file.\"{option}\"), then rerun cowshed setup",
                placement.host.display(),
                target.display(),
            )));
        }
        HostState::OtherLink(target) => plan.leftovers.push(conflict(format!(
            "{} links to {}; remove that link, then rerun cowshed setup",
            placement.host.display(),
            target.display()
        ))),
        HostState::NotDirectory => plan.leftovers.push(conflict(format!(
            "{} is not a directory",
            placement.host.display()
        ))),
    }
}

/// Read the volume entries the planner decides and the state of every destination.
pub fn observe(layout: &RetirementLayout) -> io::Result<Observation> {
    let mut observation = Observation::default();
    let mut pending = vec![PathBuf::new()];
    while let Some(directory) = pending.pop() {
        let mut names = Vec::new();
        for entry in fs::read_dir(layout.volume.join(&directory))? {
            names.push(entry?.file_name());
        }
        names.sort();
        for name in names {
            let relative = directory.join(&name);
            if is_bookkeeping(&relative) {
                continue;
            }
            let absolute = layout.volume.join(&relative);
            let metadata = fs::symlink_metadata(&absolute)?;
            let interior = metadata.is_dir() && layout.is_interior(&relative);
            let kind = if metadata.file_type().is_symlink() {
                EntryKind::Symlink
            } else if metadata.is_dir() {
                EntryKind::Directory {
                    empty: fs::read_dir(&absolute)?.next().is_none(),
                }
            } else {
                EntryKind::File
            };
            let bytes = if interior { 0 } else { tree_bytes(&absolute)? };
            if interior {
                pending.push(relative.clone());
            }
            observation.entries.push(VolumeEntry {
                path: relative,
                kind,
                bytes,
            });
        }
    }
    observation
        .entries
        .sort_by(|left, right| left.path.cmp(&right.path));
    for host in layout.destinations() {
        observation
            .hosts
            .insert(host.to_path_buf(), host_state(layout, host)?);
        if let Some(staging) = staging_path(host)
            && fs::symlink_metadata(&staging).is_ok()
        {
            observation.staging.push(staging);
        }
    }
    Ok(observation)
}

fn host_state(layout: &RetirementLayout, host: &Path) -> io::Result<HostState> {
    let metadata = match fs::symlink_metadata(host) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HostState::Absent),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_symlink() {
        return Ok(if metadata.is_dir() {
            HostState::Directory
        } else {
            HostState::NotDirectory
        });
    }
    let target = fs::read_link(host)?;
    if target.starts_with("/nix/store") {
        return Ok(HostState::ModuleLink(target));
    }
    let resolved = fs::canonicalize(host).ok();
    let into_volume = resolved.is_some_and(|resolved| {
        fs::canonicalize(&layout.volume).is_ok_and(|volume| resolved.starts_with(volume))
    });
    Ok(if into_volume {
        HostState::LinkIntoVolume
    } else {
        HostState::OtherLink(target)
    })
}

/// Apparent bytes of every file at or beneath `path`, never following a link.
pub fn tree_bytes(path: &Path) -> io::Result<u64> {
    let mut bytes = 0;
    for entry in walkdir::WalkDir::new(path).follow_links(false) {
        let entry = entry.map_err(io::Error::other)?;
        let metadata = entry.metadata().map_err(io::Error::other)?;
        if !metadata.is_dir() {
            bytes += metadata.len();
        }
    }
    Ok(bytes)
}

/// Whether the volume holds nothing but its marker and its filesystem's own bookkeeping: the one
/// state in which `cowshed setup --retire-caches-volume` deletes it.
pub fn holds_only_marker(volume: &Path) -> io::Result<bool> {
    if fs::symlink_metadata(volume.join(VOLUME_MARKER_FILE)).is_err() {
        return Ok(false);
    }
    for entry in fs::read_dir(volume)? {
        if !is_bookkeeping(Path::new(&entry?.file_name())) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// What one cache's move did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheOutcome {
    pub placement: Placement,
    pub moved_bytes: u64,
    pub dropped_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetirementReport {
    pub caches: Vec<CacheOutcome>,
    pub removed_empty: Vec<PathBuf>,
    /// What the volume still holds after the run, from a fresh observation.
    pub leftovers: Vec<Leftover>,
}

#[derive(Debug)]
pub enum RetirementError {
    /// A cargo process holds cargo's package-cache lock, so cargo's caches cannot move now.
    CargoLockHeld {
        cargo_home: PathBuf,
    },
    /// A step failed; everything before it is in `report`.
    Step {
        step: String,
        error: io::Error,
        report: RetirementReport,
    },
    Observe(io::Error),
}

impl std::fmt::Display for RetirementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CargoLockHeld { cargo_home } => write!(
                formatter,
                "a cargo process holds {}'s package-cache lock, so cargo's caches cannot move now",
                cargo_home.display()
            ),
            Self::Step { step, error, .. } => write!(formatter, "{step} failed: {error}"),
            Self::Observe(error) => write!(formatter, "cannot inspect the caches volume: {error}"),
        }
    }
}

impl std::error::Error for RetirementError {}

/// Perform `plan`, then observe the volume again for what it still holds.
///
/// Cargo's caches move under cargo's own package-cache locks; a lock a cargo process holds right
/// now refuses the whole run before anything moves.
pub fn execute(
    layout: &RetirementLayout,
    plan: &Plan,
) -> Result<RetirementReport, RetirementError> {
    let _cargo_lock = if plan.owners().contains(&Owner::Cargo) {
        Some(lock_cargo(&layout.cargo_home)?)
    } else {
        None
    };
    let mut report = RetirementReport::default();
    for step in &plan.steps {
        let result = match step {
            Step::CleanStaging(path) => remove_tree(path),
            Step::RemoveEmpty(path) => match fs::remove_dir(path) {
                Ok(()) => {
                    report.removed_empty.push(path.clone());
                    Ok(())
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            },
            Step::Move {
                placement,
                replace_link,
                bytes,
            } => move_directory(&placement.volume, &placement.host, *replace_link).map(|()| {
                report.caches.push(CacheOutcome {
                    placement: placement.clone(),
                    moved_bytes: *bytes,
                    dropped_bytes: 0,
                });
            }),
            Step::Merge { placement, bytes } => {
                let mut totals = Totals::default();
                let merged = match placement.merge {
                    Merge::ContentAddressed => {
                        merge_tree(&placement.volume, &placement.host, &mut totals)
                            .and_then(|()| fs::remove_dir(&placement.volume))
                    }
                    Merge::HostWins => {
                        totals.dropped = *bytes;
                        remove_tree(&placement.volume)
                    }
                };
                merged.map(|()| {
                    report.caches.push(CacheOutcome {
                        placement: placement.clone(),
                        moved_bytes: totals.moved,
                        dropped_bytes: totals.dropped,
                    });
                })
            }
        };
        if let Err(error) = result {
            return Err(RetirementError::Step {
                step: describe(step),
                error,
                report,
            });
        }
    }
    let after = observe(layout).map_err(RetirementError::Observe)?;
    let replanned = plan_after(layout, &after);
    report.leftovers = replanned;
    Ok(report)
}

/// What a fresh plan leaves; a step it still proposes did not converge and is named as such.
fn plan_after(layout: &RetirementLayout, observation: &Observation) -> Vec<Leftover> {
    let replanned = plan(layout, observation);
    let mut leftovers = replanned.leftovers;
    for step in replanned.steps {
        match step {
            Step::Move {
                placement, bytes, ..
            }
            | Step::Merge { placement, bytes } => leftovers.push(Leftover {
                path: placement.volume,
                bytes,
                reason: LeftoverReason::Conflict(format!(
                    "it did not move to {}; rerun cowshed setup",
                    placement.host.display()
                )),
            }),
            Step::CleanStaging(_) | Step::RemoveEmpty(_) => {}
        }
    }
    leftovers
}

fn describe(step: &Step) -> String {
    match step {
        Step::CleanStaging(path) => format!("removing the unpublished copy {}", path.display()),
        Step::RemoveEmpty(path) => format!("removing the empty {}", path.display()),
        Step::Move { placement, .. } => format!(
            "moving {} to {}",
            placement.volume.display(),
            placement.host.display()
        ),
        Step::Merge { placement, .. } => format!(
            "merging {} into {}",
            placement.volume.display(),
            placement.host.display()
        ),
    }
}

fn lock_cargo(cargo_home: &Path) -> Result<cargo::CacheLock, RetirementError> {
    let failed = |error: io::Error| RetirementError::Step {
        step: format!(
            "taking cargo's package-cache lock in {}",
            cargo_home.display()
        ),
        error,
        report: RetirementReport::default(),
    };
    fs::create_dir_all(cargo_home).map_err(failed)?;
    match cargo::try_lock_caches(cargo_home) {
        Ok(Some(lock)) => Ok(lock),
        Ok(None) => Err(RetirementError::CargoLockHeld {
            cargo_home: cargo_home.to_path_buf(),
        }),
        Err(error) => Err(failed(error)),
    }
}

#[derive(Default)]
struct Totals {
    moved: u64,
    dropped: u64,
}

/// Move every entry the host lacks into it and drop every entry it already holds, recursing
/// where both hold a directory.
fn merge_tree(from: &Path, to: &Path, totals: &mut Totals) -> io::Result<()> {
    let mut names = Vec::new();
    for entry in fs::read_dir(from)? {
        names.push(entry?.file_name());
    }
    names.sort();
    for name in names {
        let source = from.join(&name);
        let destination = to.join(&name);
        if let Some(staging) = staging_path(&destination) {
            remove_tree(&staging)?;
        }
        let source_metadata = fs::symlink_metadata(&source)?;
        match fs::symlink_metadata(&destination) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let bytes = tree_bytes(&source)?;
                move_directory(&source, &destination, false)?;
                totals.moved += bytes;
            }
            Err(error) => return Err(error),
            Ok(destination_metadata)
                if source_metadata.is_dir()
                    && destination_metadata.is_dir()
                    && !source_metadata.file_type().is_symlink()
                    && !destination_metadata.file_type().is_symlink() =>
            {
                merge_tree(&source, &destination, totals)?;
                fs::remove_dir(&source)?;
            }
            Ok(_) => {
                totals.dropped += tree_bytes(&source)?;
                remove_tree(&source)?;
            }
        }
    }
    Ok(())
}

/// Copy `from` to `to` through its staging name and publish it with one rename, replacing the
/// host's link at `to` when `replace_link`, then remove `from`.
///
/// The link stays in place until the copy is whole, so a crash before the publication leaves the
/// host exactly as it was and the source untouched.
fn move_directory(from: &Path, to: &Path, replace_link: bool) -> io::Result<()> {
    let staging = staging_path(to)
        .ok_or_else(|| io::Error::other(format!("{} has no leaf name", to.display())))?;
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_tree(&staging)?;
    if let Err(error) = copy_tree(from, &staging) {
        let _ = remove_tree(&staging);
        return Err(error);
    }
    if replace_link {
        fs::remove_file(to)?;
    }
    fs::rename(&staging, to)?;
    remove_tree(from)
}

fn remove_tree(path: &Path) -> io::Result<()> {
    let result = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(error) => Err(error),
    };
    match result {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// The copy that preserves a cache: symlinks stay links, modes and times survive, and so do hard
/// links. Cargo's and nix's caches are content and metadata both, and cargo's git checkouts share
/// inodes with its databases; a copy that followed links, reset times or split hard links would
/// not be the same cache. macOS `cp -a` splits hard links and `ditto` keeps them; GNU `cp -a`
/// keeps them too.
#[cfg(target_os = "macos")]
const COPY_TREE: (&str, &[&str]) = ("/usr/bin/ditto", &[]);
#[cfg(not(target_os = "macos"))]
const COPY_TREE: (&str, &[&str]) = ("/bin/cp", &["-a"]);

/// Copy the tree at `from` to the new path `to` with [`COPY_TREE`].
fn copy_tree(from: &Path, to: &Path) -> io::Result<()> {
    let (program, options) = COPY_TREE;
    let output = Command::new(program)
        .args(options)
        .arg(from)
        .arg(to)
        .output_locked()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{program} {} {} exited with {}: {}",
            from.display(),
            to.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
