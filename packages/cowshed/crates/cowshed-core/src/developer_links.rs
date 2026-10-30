//! Developer package bindings carried by a mint and kept across package-manager installs.
//! The snapshot grants nothing: only the mint may add its captured canonical sibling targets
//! to controller-owned read grants. Job and package-manager snapshots restore existing entries,
//! except a link the managed declaration made and no longer names: that link is the declaration's,
//! so no snapshot keeps it and an install leaves the version it installs.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::{Arc, LazyLock};

use crate::capabilities::bun::BUN_HOME;
use crate::capabilities::cache::SharedToolHome;
use crate::capabilities::pnpm::PNPM_HOME;
use crate::input_memo::InputMemo;
use crate::runtime::shell_host::SHELL_HOST_DIRECTORY;
use crate::runtime::shell_watch::FileState;
use crate::{CowshedError, Result};
use cowshed_os::fork_lock::Run as _;
use cowshed_os::fsio::AnchoredDirectory;

/// The mode's private `tools/manager-bin`: the package-manager wrapper links, the only route that
/// leads PATH after shell activation (15_capabilities.md, "Bootstrap executables").
pub const MANAGER_BIN_ENV: &str = "COWSHED_MANAGER_BIN";
pub const INSTALL_OBSERVED_ENV: &str = "COWSHED_INSTALL_OBSERVED";
/// What an activated shell's declared package manager reports for `--version`, as
/// `<version> <dev>:<ino>` of the program that reported it ([`report`]).
pub const REPORTED_ENV: &str = "COWSHED_PACKAGE_MANAGER_REPORTED";

/// The managed declaration's receipts, relative to the checkout: one row per alias
/// `smoo.developerLinks` linked, with the exact link text it wrote (`DECLARED_RECORD` in the
/// managed `tooling/direnv/developer-links.ts`; cli README, "Declared Links").
const DECLARED_RECORD: &str = "node_modules/.smoo-declared-links";

/// The shared package-store roots a checkout's installers write to. A link into one is the
/// installer's own entry, never a developer binding. Classification only: a root grants no read.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct PackageStores {
    bun: Option<StoreRoot>,
    pnpm: Option<StoreRoot>,
}

/// One package-store root as its tool home names it, and its canonical form when the root exists.
///
/// The raw path matches a link spelled the way the home names the store. The canonical form
/// matches a link that spells the physical directory, which is how a symlinked home's store is
/// reached. Both come from the configured root alone, never from a link found in the tree being
/// captured.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StoreRoot {
    raw: PathBuf,
    canonical: Option<PathBuf>,
}

impl StoreRoot {
    /// Resolve `raw` for classification. A root that is absent, or whose ancestors this user cannot
    /// search, keeps its raw form alone: no ancestor or default is guessed in its place. Any other
    /// failure is an error, because the root cannot be classified against.
    fn resolve(raw: PathBuf) -> Result<Self> {
        let canonical = match fs::canonicalize(&raw) {
            Ok(canonical) => Some(canonical),
            Err(error) if unprobeable(&error) => None,
            Err(error) => {
                return Err(CowshedError::integrity(
                    format!(
                        "cannot resolve package store root {}: {error}",
                        raw.display()
                    ),
                    "repair the tool home variable or its directory permissions, then retry",
                ));
            }
        };
        Ok(Self { raw, canonical })
    }

    /// Whether `path` lies inside this root, by whole path components, in either spelling.
    fn contains(&self, path: &Path) -> bool {
        path.starts_with(&self.raw)
            || self
                .canonical
                .as_deref()
                .is_some_and(|canonical| path.starts_with(canonical))
    }
}

impl PackageStores {
    /// The stores of a controller home: each shared tool home's own host path under it.
    pub fn from_home(home: &Path) -> Result<Self> {
        Self::from_roots(
            Some(BUN_HOME.host_path(home)),
            Some(PNPM_HOME.host_path(home)),
        )
    }

    /// The stores a wrapper names in its environment: each shared tool home's existing variable.
    /// An unset variable leaves its store inactive.
    pub fn from_environment() -> Result<Self> {
        Self::from_roots(environment_root(&BUN_HOME), environment_root(&PNPM_HOME))
    }

    /// The stores named by explicit roots. A present root must be absolute: an empty or relative
    /// root would match everything, so it is refused rather than read. Each present root is then
    /// resolved to its canonical form where it exists, so a link spelled either way is recognized.
    pub fn from_roots(bun: Option<PathBuf>, pnpm: Option<PathBuf>) -> Result<Self> {
        for root in bun.iter().chain(pnpm.iter()) {
            if !root.is_absolute() {
                return Err(CowshedError::integrity(
                    format!(
                        "package store root {} is not an absolute path",
                        root.display()
                    ),
                    "repair the tool home variable, or unset it",
                ));
            }
        }
        Ok(Self {
            bun: bun.map(StoreRoot::resolve).transpose()?,
            pnpm: pnpm.map(StoreRoot::resolve).transpose()?,
        })
    }

    /// Whether `path` lies inside one of the roots, by whole path components.
    fn contains(&self, path: &Path) -> bool {
        [self.bun.as_ref(), self.pnpm.as_ref()]
            .into_iter()
            .flatten()
            .any(|root| root.contains(path))
    }
}

fn environment_root(home: &SharedToolHome) -> Option<PathBuf> {
    home.variable.and_then(std::env::var_os).map(PathBuf::from)
}

#[derive(Debug, Default)]
pub struct DeveloperLinks {
    links: Vec<DeveloperLink>,
    /// Links `smoo.developerLinks` made and no longer names: never restored, only said.
    released: Vec<ReleasedLink>,
    stores: PackageStores,
    /// The directories holding every captured and released link, each with the identity it had
    /// when the snapshot was taken, when all of them were settled then ([`Self::holders`]).
    holders: Option<Box<[(PathBuf, FileState)]>>,
}

#[derive(Debug)]
struct ReleasedLink {
    at: PathBuf,
    target: PathBuf,
}

#[derive(Debug)]
struct DeveloperLink {
    at: PathBuf,
    target: PathBuf,
    binding: Binding,
}

#[derive(Debug)]
enum Binding {
    Owned(PathBuf),
    Sibling(PathBuf),
    Executable(PathBuf),
    Unclassified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreMode {
    /// Immediate restoration after a covered install: every captured link is restored exactly.
    CoveredInstall,
    /// Terminal cleanup with no observed install: a missing link is reported and stays absent.
    JobTerminal,
    /// Terminal cleanup after the wrapper recorded an install: a missing link is restored.
    JobTerminalObservedInstall,
}

/// Mint capture is strict: an unreadable target fails. Job capture keeps it as an unclassified link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureMode {
    Mint,
    Job,
}

enum Current<'a> {
    ParentAbsent,
    Absent,
    Payload,
    Link(&'a [u8]),
}

#[derive(Debug, Eq, PartialEq)]
enum Plan {
    Keep,
    Restore,
    Finding(String),
}

impl DeveloperLinks {
    /// Job-time snapshot: an unreadable target is kept as an unclassified, restorable link.
    pub fn capture_job(root: &Path, stores: PackageStores) -> Result<Self> {
        Self::capture(root, CaptureMode::Job, stores)
    }

    /// The supervisor's job-time snapshot ([`Self::capture_job`]), kept in its process while every
    /// directory and path its capture read keeps its identity ([`InputMemo::read_recorded`]): an
    /// admission whose package trees are as the last capture found them takes that snapshot
    /// instead of listing every tree again (15_capabilities.md, "Bootstrap executables"). A
    /// checkout whose tracked listing is not its own `.git` index is captured afresh every time.
    pub fn capture_kept(root: &Path, stores: PackageStores) -> Result<Arc<Self>> {
        static KEPT: LazyLock<InputMemo<(PathBuf, PackageStores), Arc<DeveloperLinks>>> =
            LazyLock::new(InputMemo::new);
        if crate::capabilities::kept_listing_inputs(root).is_none() {
            return Self::capture_job(root, stores).map(Arc::new);
        }
        KEPT.read_recorded(&(root.to_owned(), stores.clone()), || {
            let mut reads = Reads::recording();
            let snapshot = Self::capture_reading(root, CaptureMode::Job, stores, &mut reads)?;
            Ok((Arc::new(snapshot), reads.into_paths()))
        })
    }

    pub fn capture(root: &Path, mode: CaptureMode, stores: PackageStores) -> Result<Self> {
        Self::capture_reading(root, mode, stores, &mut Reads::ignoring())
    }

    fn capture_reading(
        root: &Path,
        mode: CaptureMode,
        stores: PackageStores,
        reads: &mut Reads,
    ) -> Result<Self> {
        let asked = crate::input_memo::now_ns();
        let root = fs::canonicalize(root).map_err(|error| link_error(root, error))?;
        reads.resolution(&root, &root.join(crate::build_volume::BUILD_LINK));
        let volume = build_volume(&root)?;
        reads.path(&root.join(DECLARED_RECORD));
        reads.path(&root.join("package.json"));
        let released = released_links(&root)?;
        reads.packages(&root)?;
        let mut snapshot = Self {
            links: Vec::new(),
            released: Vec::new(),
            stores,
            holders: None,
        };
        crate::capabilities::installed_javascript_trees(&root, |tree| {
            let directory = root.join(tree);
            reads.within(&root, &directory);
            crate::storage::verify_no_symlinks(&root, &directory)
                .map_err(|error| link_error(&directory, error))?;
            for entry in fs::read_dir(&directory).map_err(|error| link_error(&directory, error))? {
                let entry = entry.map_err(|error| link_error(&directory, error))?;
                if entry.file_name().as_encoded_bytes().starts_with(b".") {
                    continue;
                }
                let kind = entry
                    .file_type()
                    .map_err(|error| link_error(&entry.path(), error))?;
                if entry.file_name().as_encoded_bytes().starts_with(b"@") && kind.is_dir() {
                    let scope = entry.path();
                    reads.path(&scope);
                    for package in
                        fs::read_dir(&scope).map_err(|error| link_error(&scope, error))?
                    {
                        let package = package.map_err(|error| link_error(&scope, error))?;
                        snapshot.capture_entry(
                            &root,
                            volume.as_deref(),
                            &released,
                            package,
                            mode,
                            reads,
                        )?;
                    }
                } else if kind.is_symlink() {
                    snapshot.capture_entry(
                        &root,
                        volume.as_deref(),
                        &released,
                        entry,
                        mode,
                        reads,
                    )?;
                }
            }
            snapshot.capture_executables(&root, tree, mode, reads)?;
            Ok(())
        })?;
        snapshot
            .links
            .sort_unstable_by(|left, right| left.at.cmp(&right.at));
        snapshot.holders = asked.and_then(|asked| snapshot.holders(&root, asked));
        Ok(snapshot)
    }

    /// The directories holding every captured and released link, and every directory above them
    /// up to `root`, each with its identity now, links not followed, when each was last changed
    /// [`crate::input_memo::SETTLED`] before `asked`. A link is removed, replaced or retargeted
    /// only by changing the directory that holds it, and that directory is moved or replaced by a
    /// link only by changing the one above it, so a job that leaves each of these as it is changed
    /// no link and left no path a restore would refuse.
    fn holders(&self, root: &Path, asked: i128) -> Option<Box<[(PathBuf, FileState)]>> {
        let mut directories = BTreeSet::new();
        for at in self
            .links
            .iter()
            .map(|link| link.at.as_path())
            .chain(self.released.iter().map(|link| link.at.as_path()))
        {
            for directory in root.join(at).ancestors().skip(1) {
                if !directories.insert(directory.to_path_buf()) || directory == root {
                    break;
                }
            }
        }
        let holders: Box<[(PathBuf, FileState)]> = directories
            .into_iter()
            .map(|directory| {
                let identity = FileState::of_entry(&directory);
                (directory, identity)
            })
            .collect();
        let identities: Vec<FileState> = holders.iter().map(|(_, identity)| *identity).collect();
        crate::input_memo::keeps(&identities, asked).then_some(holders)
    }

    /// `volume` is the physical build volume `root` reaches through its build link: the checkout's
    /// own build state, declared state included, lives there (16_build_volumes.md), so a product
    /// a contained link reaches through it is owned like one beneath `root`. `released` holds the
    /// receipts of links the declaration no longer names; a link still spelled as its receipt
    /// records is the declaration's own and is released rather than captured.
    fn capture_entry(
        &mut self,
        root: &Path,
        volume: Option<&Path>,
        released: &BTreeMap<PathBuf, PathBuf>,
        entry: fs::DirEntry,
        mode: CaptureMode,
        reads: &mut Reads,
    ) -> Result<()> {
        let at = entry.path();
        if !entry
            .file_type()
            .map_err(|error| link_error(&at, error))?
            .is_symlink()
        {
            return Ok(());
        }
        let target = fs::read_link(&at).map_err(|error| link_error(&at, error))?;
        let relative = at.strip_prefix(root).expect("the package tree is owned");
        if released.get(relative) == Some(&target) {
            self.released.push(ReleasedLink {
                at: relative.to_owned(),
                target,
            });
            return Ok(());
        }
        let parent = relative.parent().expect("a package link has a parent");
        let resolved = crate::inherited_links::resolve_in_source(root, parent, &target);
        // Bun and pnpm's own package-store entries are installer state, not developer choices.
        if registry_store(root, &self.stores, &resolved) {
            return Ok(());
        }
        reads.resolution(root, &at);
        let canonical = match fs::canonicalize(&at) {
            Ok(target) => {
                reads.path(&target);
                target
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) if unreadable(&error, mode) => {
                return self.push_unclassified(relative, target);
            }
            Err(error) => return Err(link_error(&at, error)),
        };
        // A developer alias into a physical package store is the store's entry, not a binding.
        if self.stores.contains(&canonical) {
            return Ok(());
        }
        let metadata = match fs::metadata(&canonical) {
            Ok(metadata) => metadata,
            Err(error) if unreadable(&error, mode) => {
                return self.push_unclassified(relative, target);
            }
            Err(error) => return Err(link_error(&at, error)),
        };
        if !metadata.is_dir() {
            return Ok(());
        }
        let owned = canonical.starts_with(root)
            || volume.is_some_and(|volume| canonical.starts_with(volume));
        if resolved.starts_with(root) && !owned {
            return Err(CowshedError::integrity(
                format!(
                    "owned developer link {} escapes its checkout to {}",
                    at.display(),
                    canonical.display()
                ),
                "repair the owned package product in the source checkout",
            ));
        }
        self.links.push(DeveloperLink {
            at: relative.to_owned(),
            target,
            binding: if owned {
                Binding::Owned(canonical)
            } else {
                Binding::Sibling(canonical)
            },
        });
        Ok(())
    }

    fn push_unclassified(&mut self, relative: &Path, target: PathBuf) -> Result<()> {
        self.links.push(DeveloperLink {
            at: relative.to_owned(),
            target,
            binding: Binding::Unclassified,
        });
        Ok(())
    }

    fn capture_executables(
        &mut self,
        root: &Path,
        tree: &Path,
        mode: CaptureMode,
        reads: &mut Reads,
    ) -> Result<()> {
        let directory = root.join(tree).join(".bin");
        reads.within(root, &directory);
        crate::storage::verify_no_symlinks(root, &directory)
            .map_err(|error| link_error(&directory, error))?;
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(link_error(&directory, error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| link_error(&directory, error))?;
            let at = entry.path();
            if !entry
                .file_type()
                .map_err(|error| link_error(&at, error))?
                .is_symlink()
            {
                continue;
            }
            reads.resolution(root, &at);
            let canonical = match fs::canonicalize(&at) {
                Ok(canonical) => {
                    reads.path(&canonical);
                    canonical
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) if unreadable(&error, mode) => {
                    self.links.push(DeveloperLink {
                        at: at
                            .strip_prefix(root)
                            .expect("the bin tree is owned")
                            .to_owned(),
                        target: fs::read_link(&at).map_err(|error| link_error(&at, error))?,
                        binding: Binding::Unclassified,
                    });
                    continue;
                }
                Err(error) => return Err(link_error(&at, error)),
            };
            let owner = self.links.iter().find_map(|link| match &link.binding {
                Binding::Owned(product) | Binding::Sibling(product)
                    if canonical.starts_with(product) =>
                {
                    Some(link.at.clone())
                }
                Binding::Owned(_)
                | Binding::Sibling(_)
                | Binding::Executable(_)
                | Binding::Unclassified => None,
            });
            if let Some(owner) = owner {
                self.links.push(DeveloperLink {
                    at: at
                        .strip_prefix(root)
                        .expect("the bin tree is owned")
                        .to_owned(),
                    target: fs::read_link(&at).map_err(|error| link_error(&at, error))?,
                    binding: Binding::Executable(owner),
                });
            }
        }
        Ok(())
    }

    pub fn sibling_targets(&self) -> impl Iterator<Item = &Path> {
        self.links.iter().filter_map(|link| match &link.binding {
            Binding::Sibling(target) => Some(target.as_path()),
            Binding::Owned(_) | Binding::Executable(_) | Binding::Unclassified => None,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.links.is_empty()
    }

    pub fn restore(&self, root: &Path, mode: RestoreMode) -> Result<Vec<String>> {
        // Every captured and released link sits in a directory whose entries are as the capture
        // found them: none was removed, replaced or retargeted, so there is nothing to say or
        // restore.
        if self.holders.as_ref().is_some_and(|holders| {
            holders
                .iter()
                .all(|(directory, identity)| identity.same_as(FileState::of_entry(directory)))
        }) {
            if !self.is_empty() {
                crate::build_volume::discard::reap(root);
            }
            return Ok(Vec::new());
        }
        self.say_released(root)?;
        if self.is_empty() {
            return Ok(Vec::new());
        }
        // Phase one plans every link before any mutation, so alias and owner decisions read the
        // installer's pre-restore state.
        let decisions = plan_links(root, &self.stores, &self.links, mode)?;
        let mut findings = Vec::new();
        let mut restores = Vec::new();
        for (link, decision) in self.links.iter().zip(decisions) {
            match decision {
                Plan::Keep => {}
                Plan::Finding(message) => findings.push(message),
                Plan::Restore => restores.push(link),
            }
        }
        restore_links(root, restores, mode)?;
        crate::build_volume::discard::reap(root);
        Ok(findings)
    }

    /// Names each released link the job or install displaced. Restoration never touches a
    /// released alias, so this reads the state the installer left.
    fn say_released(&self, root: &Path) -> Result<()> {
        for link in &self.released {
            let at = root.join(&link.at);
            let current = current_target(&at).map_err(|error| link_error(&at, error))?;
            if current.as_deref() != Some(link.target.as_os_str().as_bytes()) {
                cowshed_os::eprintln!(
                    "cowshed: developer link {} -> {} is not restored: smoo.developerLinks no longer declares its package",
                    link.at.display(),
                    link.target.display()
                );
            }
        }
        Ok(())
    }
}

fn link_error(path: &Path, error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("cannot keep developer link at {}: {error}", path.display()),
        "repair the checkout's package links and directory permissions",
    )
}

/// The physical directory `root`'s build link names, or `None` for a checkout never linked or
/// whose volume is not mounted here. A link into an absent volume resolves nowhere, so capture
/// skips it as it skips any link to nothing.
fn build_volume(root: &Path) -> Result<Option<PathBuf>> {
    let link = root.join(crate::build_volume::BUILD_LINK);
    match fs::canonicalize(&link) {
        Ok(volume) => Ok(Some(volume)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(link_error(&link, error)),
    }
}

/// What a capture to be kept read: every path whose identity decides the snapshot
/// ([`DeveloperLinks::capture_kept`]). A capture that keeps nothing records nothing.
struct Reads(Option<BTreeSet<PathBuf>>);

impl Reads {
    fn ignoring() -> Self {
        Self(None)
    }

    fn recording() -> Self {
        Self(Some(BTreeSet::new()))
    }

    fn path(&mut self, path: &Path) {
        if let Some(paths) = &mut self.0 {
            paths.insert(path.to_owned());
        }
    }

    /// `path` and every directory above it up to `root`: the entries that decide what it names.
    fn within(&mut self, root: &Path, path: &Path) {
        if self.0.is_none() {
            return;
        }
        for directory in path.ancestors() {
            self.path(directory);
            if directory == root {
                return;
            }
        }
    }

    /// What decides where `path` resolves: beneath `root`, every directory above each path the
    /// resolution passes through; elsewhere, each such path and the directory holding it. A
    /// replaced link changes the directory that holds it, and a resolution that comes to name
    /// another directory changes the identity at its end.
    fn resolution(&mut self, root: &Path, path: &Path) {
        if self.0.is_none() {
            return;
        }
        let mut pending = path.to_owned();
        for _ in 0..MAX_RESOLUTION_LINKS {
            if pending.starts_with(root) {
                self.within(root, &pending);
            } else {
                self.path(&pending);
                if let Some(parent) = pending.parent() {
                    self.path(parent);
                }
            }
            match first_link(root, &pending) {
                Some(next) => pending = next,
                None => return,
            }
        }
    }

    /// The checkout's tracked listing and every tracked package's directory: where an install
    /// makes or removes a package tree.
    fn packages(&mut self, root: &Path) -> Result<()> {
        if self.0.is_none() {
            return Ok(());
        }
        if let Some(listing) = crate::capabilities::kept_listing_inputs(root) {
            for input in &listing {
                self.path(input);
            }
        }
        for directory in crate::capabilities::tracked_package_directories(root)? {
            self.path(&directory);
        }
        Ok(())
    }

    fn into_paths(self) -> Vec<PathBuf> {
        self.0
            .map(|paths| paths.into_iter().collect())
            .unwrap_or_default()
    }
}

/// The links one recorded resolution follows; past it, the capture's own resolution names the
/// cycle.
const MAX_RESOLUTION_LINKS: usize = 40;

/// `path` with its first link replaced by that link's text, or `None` when no component is a
/// link, or one cannot be read (the capture's own resolution reports it). Beneath `root` only the
/// components below it are examined.
fn first_link(root: &Path, path: &Path) -> Option<PathBuf> {
    let (mut prefix, rest) = match path.strip_prefix(root) {
        Ok(rest) => (root.to_path_buf(), rest),
        Err(_) => (PathBuf::new(), path),
    };
    let mut components = rest.components();
    while let Some(component) = components.next() {
        prefix.push(component);
        if fs::symlink_metadata(&prefix).ok()?.file_type().is_symlink() {
            let target = fs::read_link(&prefix).ok()?;
            let mut next = prefix.parent().map(Path::to_path_buf).unwrap_or_default();
            next.push(target);
            let rest = components.as_path();
            if !rest.as_os_str().is_empty() {
                next.push(rest);
            }
            return Some(next);
        }
    }
    None
}

/// One row of `DECLARED_RECORD`; its `declaration` field is the managed script's own.
#[derive(serde::Deserialize)]
struct Receipt {
    path: String,
    target: PathBuf,
}

/// The receipts of links `smoo.developerLinks` made and no longer names, by alias, each with the
/// link text the declaration wrote: the same rule `keepDeveloperLinks` applies before an install
/// (cli README, "Declared Links"). A checkout without receipts reads no manifest. Malformed
/// ownership is an error, never no ownership, as in the managed script's `readDeclaredRecord`.
fn released_links(root: &Path) -> Result<BTreeMap<PathBuf, PathBuf>> {
    let record = root.join(DECLARED_RECORD);
    let bytes = match fs::read(&record) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(link_error(&record, error)),
    };
    let receipts: Vec<Receipt> =
        serde_json::from_slice(&bytes).map_err(|error| link_error(&record, error))?;
    if receipts.is_empty() {
        return Ok(BTreeMap::new());
    }
    let manifest = root.join("package.json");
    let manifest_bytes = fs::read(&manifest).map_err(|error| link_error(&manifest, error))?;
    let manifest_value: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).map_err(|error| link_error(&manifest, error))?;
    let declared = match manifest_value
        .get("smoo")
        .and_then(|smoo| smoo.get("developerLinks"))
    {
        None => None,
        Some(serde_json::Value::Object(declared)) => Some(declared),
        Some(_) => {
            return Err(link_error(
                &manifest,
                "smoo.developerLinks must map package names to checkout directories",
            ));
        }
    };
    let mut released = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for Receipt { path, target } in receipts {
        if !seen.insert(path.clone()) {
            return Err(link_error(&record, format!("duplicate receipt for {path}")));
        }
        // The package an alias stands for in its tree: `name` or `@scope/name`.
        let package = path
            .rsplit_once("node_modules/")
            .map_or(path.as_str(), |(_, package)| package);
        if declared.is_none_or(|declared| !declared.contains_key(package)) {
            released.insert(PathBuf::from(path), target);
        }
    }
    Ok(released)
}

/// The installer's own entries: a link into the checkout's own `.bun`/`.pnpm`, or into a shared
/// package store the tool homes name. A registry replacement is one of these, never a binding.
fn registry_store(root: &Path, stores: &PackageStores, resolved: &Path) -> bool {
    (resolved.starts_with(root)
        && resolved
            .components()
            .any(|part| matches!(part.as_os_str().to_str(), Some(".bun" | ".pnpm"))))
        || stores.contains(resolved)
}

/// Job capture keeps a permission-denied target restorable instead of refusing admission.
fn unreadable(error: &io::Error, mode: CaptureMode) -> bool {
    mode == CaptureMode::Job && error.kind() == io::ErrorKind::PermissionDenied
}

/// Phase one reads every captured leaf before any mutation. Capture already sorts the paths,
/// so adjacent siblings share a parent capability rather than reopening its whole chain for
/// every leaf. The capability is dropped at the next parent; nothing is cached across jobs.
fn plan_links(
    root: &Path,
    stores: &PackageStores,
    links: &[DeveloperLink],
    mode: RestoreMode,
) -> Result<Vec<Plan>> {
    let mut decisions = Vec::with_capacity(links.len());
    let mut buffer = [0u8; 4096];
    for group in links.chunk_by(|left, right| left.at.parent() == right.at.parent()) {
        let mut at = root.join(group[0].at.parent().expect("a package link has a parent"));
        crate::storage::verify_no_symlinks(root, &at).map_err(|error| link_error(&at, error))?;
        let directory = match AnchoredDirectory::open(&at) {
            Ok(directory) => Some(directory),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(link_error(&at, error)),
        };
        for link in group {
            at.push(link.at.file_name().expect("a package link has a name"));
            let current = match &directory {
                Some(directory) => current_leaf(directory, &at, &mut buffer)?,
                None => Current::ParentAbsent,
            };
            decisions.push(plan(root, stores, links, link, &at, current, mode)?);
            at.pop();
        }
    }
    Ok(decisions)
}

/// Decide from the observed leaf while all restoration is still pending.
fn plan(
    root: &Path,
    stores: &PackageStores,
    links: &[DeveloperLink],
    link: &DeveloperLink,
    at: &Path,
    current: Current<'_>,
    mode: RestoreMode,
) -> Result<Plan> {
    let target = link.target.as_os_str().as_bytes();
    Ok(match (mode, current) {
        (_, Current::Link(current)) if current == target => Plan::Keep,
        (RestoreMode::CoveredInstall, _) => Plan::Restore,
        (RestoreMode::JobTerminal, Current::ParentAbsent) => Plan::Finding(format!(
            "developer-link-ambiguous: {} is absent after a job with no observed install; it stays absent",
            at.display()
        )),
        (RestoreMode::JobTerminal, Current::Absent) => Plan::Finding(format!(
            "developer-link-ambiguous: {} was removed by a job with no observed install; it stays absent",
            at.display()
        )),
        (RestoreMode::JobTerminal | RestoreMode::JobTerminalObservedInstall, Current::Payload) => {
            Plan::Restore
        }
        (RestoreMode::JobTerminalObservedInstall, Current::ParentAbsent | Current::Absent) => {
            Plan::Restore
        }
        (
            RestoreMode::JobTerminal | RestoreMode::JobTerminalObservedInstall,
            Current::Link(current),
        ) => {
            if preserved(root, stores, links, link, current)? {
                Plan::Keep
            } else {
                Plan::Restore
            }
        }
    })
}

fn current_leaf<'a>(
    directory: &AnchoredDirectory,
    at: &Path,
    buffer: &'a mut [u8],
) -> Result<Current<'a>> {
    let name = leaf_name(at)?;
    match directory.read_link(&name, buffer) {
        Ok(target) => Ok(Current::Link(target)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Current::Absent),
        Err(error) if error.raw_os_error() == Some(libc::EINVAL) => Ok(Current::Payload),
        Err(error) => Err(link_error(at, error)),
    }
}

fn leaf_name(at: &Path) -> Result<CString> {
    CString::new(
        at.file_name()
            .expect("a package link has a name")
            .as_bytes(),
    )
    .map_err(|error| link_error(at, error))
}

/// A different link stays only when it is a live developer binding. Registry replacements,
/// installer payloads and unclassified links are restored. An alias stays only when its own
/// owner was relinked to a live directory, so the explicit relink and its alias survive together.
fn preserved(
    root: &Path,
    stores: &PackageStores,
    links: &[DeveloperLink],
    link: &DeveloperLink,
    current: &[u8],
) -> Result<bool> {
    match &link.binding {
        Binding::Owned(_) | Binding::Sibling(_) => {
            let at = root.join(&link.at);
            live_binding(root, stores, &link.at, current).map_err(|error| link_error(&at, error))
        }
        Binding::Unclassified => Ok(false),
        Binding::Executable(owner) => {
            let parent = link.at.parent().expect("a package link has a parent");
            let resolved = crate::inherited_links::resolve_in_source(
                root,
                parent,
                Path::new(OsStr::from_bytes(current)),
            );
            if registry_store(root, stores, &resolved) {
                return Ok(false);
            }
            let alias_at = root.join(&link.at);
            let canonical = match fs::canonicalize(&alias_at) {
                Ok(canonical) => canonical,
                Err(error) if unprobeable(&error) => return Ok(false),
                Err(error) => return Err(link_error(&alias_at, error)),
            };
            if registry_store(root, stores, &canonical) {
                return Ok(false);
            }
            match fs::metadata(&canonical) {
                Ok(metadata) if metadata.is_file() => {}
                Ok(_) => return Ok(false),
                Err(error) if unprobeable(&error) => return Ok(false),
                Err(error) => return Err(link_error(&alias_at, error)),
            }
            let Some(owner_link) = links.iter().find(|candidate| candidate.at == *owner) else {
                return Ok(false);
            };
            let owner_at = root.join(owner);
            let Some(owner_current) =
                current_target(&owner_at).map_err(|error| link_error(&owner_at, error))?
            else {
                return Ok(false);
            };
            if owner_current.as_slice() == owner_link.target.as_os_str().as_bytes() {
                return Ok(false);
            }
            live_binding(root, stores, owner, &owner_current)
                .map_err(|error| link_error(&owner_at, error))
        }
    }
}

/// The bytes of the link at `path`, or `None` when nothing is there or an installer payload sits there.
fn current_target(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match fs::read_link(path) {
        Ok(target) => Ok(Some(target.as_os_str().as_bytes().to_vec())),
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::EINVAL) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn live_binding(
    root: &Path,
    stores: &PackageStores,
    relative: &Path,
    target: &[u8],
) -> io::Result<bool> {
    let parent = relative.parent().expect("a package link has a parent");
    let resolved = crate::inherited_links::resolve_in_source(
        root,
        parent,
        Path::new(OsStr::from_bytes(target)),
    );
    if registry_store(root, stores, &resolved) {
        return Ok(false);
    }
    let canonical = match fs::canonicalize(root.join(relative)) {
        Ok(canonical) => canonical,
        Err(error) if unprobeable(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    if stores.contains(&canonical) {
        return Ok(false);
    }
    match fs::metadata(&canonical) {
        Ok(metadata) => Ok(metadata.is_dir()),
        Err(error) if unprobeable(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// A target this restoration cannot probe is not a live developer binding: a sandboxed job's
/// denied target, or one that is gone. The exact snapshot restores it without reading it.
fn unprobeable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
    )
}

/// Restores the planned links one held parent at a time. The parent is opened once, every link
/// planned under it is restored through that handle, and it is synced once if any entry changed,
/// before the next parent is opened, so one parent handle is held at a time.
fn restore_links(root: &Path, mut restores: Vec<&DeveloperLink>, mode: RestoreMode) -> Result<()> {
    restores.sort_by(|left, right| left.at.parent().cmp(&right.at.parent()));
    for group in restores.chunk_by(|left, right| left.at.parent() == right.at.parent()) {
        let parent = root.join(group[0].at.parent().expect("a package link has a parent"));
        let directory = match mode {
            RestoreMode::CoveredInstall | RestoreMode::JobTerminalObservedInstall => {
                AnchoredDirectory::create(&parent)
            }
            RestoreMode::JobTerminal => AnchoredDirectory::open(&parent),
        }
        .map_err(|error| link_error(&parent, error))?;
        let mut touched = false;
        for link in group {
            let at = root.join(&link.at);
            let name = leaf_name(&at)?;
            let restored = directory
                .restore_symlink(&name, &link.target, || {
                    let discard = AnchoredDirectory::create(
                        &root.join(crate::build_volume::discard::DISCARD_DIRECTORY),
                    )?;
                    Ok((discard, discard_leaf(&link.at)?))
                })
                .map_err(|error| link_error(&at, error))?;
            if restored {
                cowshed_os::eprintln!(
                    "cowshed: restored developer link {} -> {}",
                    link.at.display(),
                    link.target.display()
                );
                touched = true;
            }
        }
        if touched {
            directory
                .sync()
                .map_err(|error| link_error(&parent, error))?;
        }
    }
    Ok(())
}

/// Names a replaced payload in the discard lifecycle with the same rule as `discard::move_aside`.
fn discard_leaf(relative: &Path) -> io::Result<CString> {
    CString::new(crate::build_volume::discard::leaf_name(relative).as_bytes())
        .map_err(io::Error::other)
}

/// Only these detected command-name entrypoints use the managed package-manager route.
pub fn is_package_manager(name: &OsStr) -> bool {
    matches!(name.to_str(), Some("bun" | "npm" | "pnpm"))
}

fn installs(manager: &OsStr, arguments: &[OsString]) -> bool {
    let Some(verb) = arguments.first().and_then(|arg| arg.to_str()) else {
        return false;
    };
    match manager.to_str() {
        Some("bun") => matches!(verb, "install" | "i" | "add" | "remove" | "update"),
        Some("npm") => matches!(
            verb,
            "install" | "i" | "ci" | "add" | "uninstall" | "remove" | "update"
        ),
        Some("pnpm") => matches!(verb, "install" | "i" | "add" | "remove" | "update"),
        _ => false,
    }
}

/// `packageManager` in the root manifest, `<manager>@<version>`; an integrity suffix after `+` is
/// not part of the version.
#[derive(Debug, PartialEq, Eq)]
struct Declaration {
    manifest: PathBuf,
    manager: String,
    version: String,
}

impl Declaration {
    /// No manifest, or one without `packageManager`, declares nothing; an unreadable or
    /// malformed one is an error, never an absent declaration.
    fn read(root: &Path) -> io::Result<Option<Self>> {
        #[derive(serde::Deserialize)]
        struct Manifest {
            #[serde(rename = "packageManager")]
            package_manager: Option<String>,
        }
        let manifest = root.join("package.json");
        let bytes = match fs::read(&manifest) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("read {}: {error}", manifest.display()),
                ));
            }
        };
        let Manifest { package_manager } = serde_json::from_slice(&bytes).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parse {}: {error}", manifest.display()),
            )
        })?;
        let Some(declared) = package_manager else {
            return Ok(None);
        };
        let Some((manager, version)) = declared
            .split_once('@')
            .map(|(manager, version)| {
                (
                    manager,
                    version
                        .split_once('+')
                        .map_or(version, |(version, _)| version),
                )
            })
            .filter(|(manager, version)| !manager.is_empty() && !version.is_empty())
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} declares packageManager {declared:?}, not <name>@<version>",
                    manifest.display()
                ),
            ));
        };
        Ok(Some(Self {
            manager: manager.to_owned(),
            version: version.to_owned(),
            manifest,
        }))
    }
}

/// The executable a managed command name runs, with the file identity a version report names.
#[derive(Debug, PartialEq, Eq)]
struct Managed {
    path: PathBuf,
    identity: (u64, u64),
}

fn identity(metadata: &fs::Metadata) -> (u64, u64) {
    (metadata.dev(), metadata.ino())
}

/// The running wrapper or exec host: the staged executable every `tools/manager-bin` link
/// names, `<workspace>/.cowshed/shell-host/<file>`, which the supervisor writes and no job may.
/// Its identity and its workspace are read from the executable itself, never from the
/// environment, which any child may replace (a test spawning `bun` with only PATH and HOME).
#[derive(Debug, PartialEq, Eq)]
struct Staged {
    identity: (u64, u64),
    workspace: PathBuf,
}

impl Staged {
    fn running() -> io::Result<Self> {
        Self::at(&std::env::current_exe()?)
    }

    fn at(executable: &Path) -> io::Result<Self> {
        let staged = fs::canonicalize(executable).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "resolve the running executable {}: {error}",
                    executable.display()
                ),
            )
        })?;
        let metadata = fs::metadata(&staged).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("inspect {}: {error}", staged.display()),
            )
        })?;
        let workspace = staged
            .parent()
            .filter(|directory| directory.ends_with(SHELL_HOST_DIRECTORY))
            .and_then(|directory| directory.parent()?.parent())
            .ok_or_else(|| {
                io::Error::other(format!(
                    "{} is not an exec host staged in a workspace's {SHELL_HOST_DIRECTORY}",
                    staged.display()
                ))
            })?;
        Ok(Self {
            identity: identity(&metadata),
            workspace: workspace.to_owned(),
        })
    }
}

/// What `manager` runs: the first executable of that name on `path` that is not the `wrapper`
/// file, which after activation is the shell's own, the one the host route runs; else the program
/// the capability recorded in `real-bin` beside the first PATH directory holding the wrapper. The
/// wrapper is recognized by file identity, so no spelling of its route on PATH can make it run
/// itself, and its route is the one PATH reached it through.
fn resolve(manager: &OsStr, path: Option<&OsStr>, wrapper: (u64, u64)) -> io::Result<Managed> {
    let mut route = None;
    for directory in path.into_iter().flat_map(std::env::split_paths) {
        let candidate = directory.join(manager);
        let Ok(metadata) = fs::metadata(&candidate) else {
            continue;
        };
        if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
            continue;
        }
        if identity(&metadata) != wrapper {
            return Ok(Managed {
                identity: identity(&metadata),
                path: candidate,
            });
        }
        route.get_or_insert(directory);
    }
    let route = route.ok_or_else(|| {
        io::Error::other(format!(
            "PATH names no {} past the package-manager wrapper, nor the wrapper's own directory beside its recorded program",
            manager.display()
        ))
    })?;
    let recorded = route
        .parent()
        .ok_or_else(|| {
            io::Error::other(format!(
                "the package-manager wrapper directory {} has no parent",
                route.display()
            ))
        })?
        .join("real-bin")
        .join(manager);
    let metadata = fs::metadata(&recorded).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("recorded package manager {}: {error}", recorded.display()),
        )
    })?;
    Ok(Managed {
        identity: identity(&metadata),
        path: recorded,
    })
}

/// The version `program` prints for `--version`.
fn reported_version(program: &Path) -> io::Result<String> {
    // WHY unbounded: a package manager printing its version waits on no other process.
    #[allow(clippy::disallowed_methods)]
    let output = Command::new(program)
        .arg("--version")
        .output_locked()
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("ask {} for its version: {error}", program.display()),
            )
        })?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{} --version ended {}: {}",
            program.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout)
        .map(|version| version.trim().to_owned())
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} --version is not UTF-8: {error}", program.display()),
            )
        })
}

/// Refuse `program` when the workspace declares `manager` at a version the program does not
/// report. `witness` ([`REPORTED_ENV`]) answers for the one program file it names.
fn verify(
    manager: &OsStr,
    program: &Managed,
    root: &Path,
    witness: Option<&OsStr>,
) -> io::Result<()> {
    let Some(declared) = Declaration::read(root)? else {
        return Ok(());
    };
    if OsStr::new(&declared.manager) != manager {
        return Ok(());
    }
    let witnessed = witness
        .and_then(OsStr::to_str)
        .and_then(|witness| witness.split_once(' '))
        .filter(|(_, file)| {
            file.split_once(':')
                .and_then(|(dev, ino)| Some((dev.parse().ok()?, ino.parse().ok()?)))
                == Some(program.identity)
        })
        .map(|(version, _)| version.to_owned());
    let reported = match witnessed {
        Some(version) => version,
        None => reported_version(&program.path)?,
    };
    if reported == declared.version {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "{} declares packageManager {}@{}, but {} reports {reported:?}; put the declared {} first on PATH or change the declaration",
        declared.manifest.display(),
        declared.manager,
        declared.version,
        program.path.display(),
        declared.manager,
    )))
}

/// [`report`] for the exec host `host`.
fn witness(path: Option<&OsStr>, host: &Staged) -> io::Result<Option<OsString>> {
    let Some(declared) = Declaration::read(&host.workspace)? else {
        return Ok(None);
    };
    let manager = OsStr::new(&declared.manager);
    if !is_package_manager(manager) {
        return Ok(None);
    }
    let program = resolve(manager, path, host.identity)?;
    let version = reported_version(&program.path)?;
    Ok(Some(
        format!("{version} {}:{}", program.identity.0, program.identity.1).into(),
    ))
}

/// The [`REPORTED_ENV`] value an activated exec host exports once for all its commands: what the
/// declared manager's program on that shell's `path` reports. `None` when the workspace
/// declares no managed package manager.
pub fn report(path: Option<&OsStr>) -> io::Result<Option<OsString>> {
    let host = Staged::running()?;
    witness(path, &host)
}

/// Multicall dispatch through the existing protected bootstrap-program link, before a runtime starts.
/// The program run is the one PATH names past this wrapper ([`resolve`]), checked against the
/// workspace's declaration ([`verify`]) before anything runs. The wrapper and its workspace are
/// the staged executable's ([`Staged`]), so a child that replaced its environment still runs it.
pub fn dispatch() -> io::Result<Option<ExitStatus>> {
    let mut args = std::env::args_os();
    let invoked = args.next().unwrap_or_default();
    let Some(manager) = Path::new(&invoked)
        .file_name()
        .filter(|name| is_package_manager(name))
    else {
        return Ok(None);
    };
    let wrapper = Staged::running()?;
    let root = wrapper.workspace.as_path();
    let program = resolve(
        manager,
        std::env::var_os("PATH").as_deref(),
        wrapper.identity,
    )?;
    verify(
        manager,
        &program,
        root,
        std::env::var_os(REPORTED_ENV).as_deref(),
    )?;
    let arguments: Vec<OsString> = args.collect();
    if !installs(manager, &arguments) {
        return Err(Command::new(program.path).args(arguments).exec());
    }
    let snapshot = DeveloperLinks::capture_job(
        root,
        PackageStores::from_environment().map_err(io::Error::other)?,
    )
    .map_err(io::Error::other)?;
    if let Some(path) = std::env::var_os(INSTALL_OBSERVED_ENV) {
        fs::write(path, b"observed\n")?;
    }
    // WHY unbounded: the install is this wrapper's whole work, run in place of the package manager
    // the job called; the job's own bounds end it.
    #[allow(clippy::disallowed_methods)]
    let status = Command::new(program.path).args(arguments).status_locked();
    let restored = snapshot
        .restore(root, RestoreMode::CoveredInstall)
        .map_err(io::Error::other)
        .map(|findings| {
            for finding in &findings {
                cowshed_os::eprintln!("cowshed: {finding}");
            }
        });
    match (status, restored) {
        (Ok(status), Ok(())) => Ok(Some(status)),
        (Err(spawn), Ok(())) => Err(spawn),
        (Ok(status), Err(restore)) => Err(io::Error::other(format!(
            "package manager ended {status}, but developer-link restoration failed: {restore}"
        ))),
        (Err(spawn), Err(restore)) => Err(io::Error::other(format!(
            "package manager failed: {spawn}; developer-link restoration failed: {restore}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::bun::BUN_HOME as BUN;
    use crate::capabilities::pnpm::PNPM_HOME as PNPM;
    use cowshed_os::fsio::{Syncs, take_syncs};
    use cowshed_os::test_backstop::CommandBackstop as _;
    use std::os::unix::fs::symlink;

    /// The stores of the fixture home beside a checkout. No fixture link names a store root, so
    /// the existing fixtures classify exactly as they did before stores were explicit.
    fn fixture_stores(checkout: &Path) -> PackageStores {
        PackageStores::from_home(
            &checkout
                .parent()
                .expect("a checkout has a parent")
                .join("home"),
        )
        .expect("an absolute fixture home")
    }

    /// An executable fixture script running `body` for any arguments.
    fn script(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        fs::create_dir_all(path.parent().expect("a script has a parent")).unwrap();
        fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// The managed program at `path`, as [`resolve`] would name it.
    fn managed(path: PathBuf) -> Managed {
        Managed {
            identity: identity(&fs::metadata(&path).unwrap()),
            path,
        }
    }

    /// The wrapper runs what PATH names past it, as the host route does: after activation the
    /// shell's program, never the bootstrap one, and never itself under another spelling of its
    /// route. Only a PATH naming none falls back to the program recorded beside the directory PATH
    /// reached the wrapper through; a PATH naming neither is refused, naming the manager.
    #[test]
    fn the_wrapper_runs_the_program_path_names_past_it_else_the_recorded_one() {
        let scratch = crate::temp_root::TempRoot::new("managed-resolve");
        let root = scratch.join("checkout");
        let tools = root.join(".cowshed/tools");
        let route = tools.join("manager-bin");
        let staged = root.join(SHELL_HOST_DIRECTORY).join("host");
        script(&staged, "exit 99");
        fs::create_dir_all(&route).unwrap();
        symlink(&staged, route.join("bun")).unwrap();
        script(&scratch.join("bootstrap/bun"), "echo 1.4.2");
        fs::create_dir_all(tools.join("real-bin")).unwrap();
        symlink(scratch.join("bootstrap/bun"), tools.join("real-bin/bun")).unwrap();
        script(&scratch.join("shell/bun"), "echo 1.4.3");
        symlink(&route, scratch.join("alias")).unwrap();
        let wrapper = Staged::at(&route.join("bun")).unwrap();
        assert_eq!(
            wrapper,
            Staged {
                identity: identity(&fs::metadata(&staged).unwrap()),
                workspace: root.clone(),
            }
        );
        let bun = OsStr::new("bun");
        let activated =
            std::env::join_paths([route.clone(), scratch.join("alias"), scratch.join("shell")])
                .unwrap();
        assert_eq!(
            resolve(bun, Some(&activated), wrapper.identity).unwrap(),
            managed(scratch.join("shell/bun"))
        );
        let bootstrap = std::env::join_paths([route.clone(), scratch.join("alias")]).unwrap();
        assert_eq!(
            resolve(bun, Some(&bootstrap), wrapper.identity).unwrap(),
            managed(tools.join("real-bin/bun"))
        );
        let refused = resolve(bun, None, wrapper.identity)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("PATH names no bun"), "{refused}");
        assert_eq!(witness(Some(&activated), &wrapper).unwrap(), None);
        fs::write(
            root.join("package.json"),
            r#"{"packageManager":"bun@1.4.3"}"#,
        )
        .unwrap();
        let shell = managed(scratch.join("shell/bun"));
        assert_eq!(
            witness(Some(&activated), &wrapper).unwrap(),
            Some(OsString::from(format!(
                "1.4.3 {}:{}",
                shell.identity.0, shell.identity.1
            )))
        );
        let unstaged = Staged::at(&scratch.join("shell/bun"))
            .unwrap_err()
            .to_string();
        assert!(
            unstaged.contains("is not an exec host staged in a workspace's .cowshed/shell-host"),
            "{unstaged}"
        );
    }

    /// A program reporting another version than the workspace declares is refused, naming the
    /// declaration and the program with what it reports; another manager's declaration, or none,
    /// is not this wrapper's to check.
    #[test]
    fn a_program_reporting_another_version_than_the_declaration_is_refused_naming_both() {
        let scratch = crate::temp_root::TempRoot::new("managed-verify");
        let root = scratch.join("checkout");
        fs::create_dir_all(&root).unwrap();
        script(&scratch.join("declared/bun"), "echo 1.4.3");
        script(&scratch.join("other/bun"), "echo 1.4.2");
        let (declared, other) = (
            managed(scratch.join("declared/bun")),
            managed(scratch.join("other/bun")),
        );
        let bun = OsStr::new("bun");
        verify(bun, &other, &root, None).unwrap();
        fs::write(
            root.join("package.json"),
            r#"{"packageManager":"bun@1.4.3+sha224.0123"}"#,
        )
        .unwrap();
        verify(bun, &declared, &root, None).unwrap();
        let refused = verify(bun, &other, &root, None).unwrap_err().to_string();
        for named in [
            root.join("package.json").display().to_string(),
            "bun@1.4.3,".to_owned(),
            format!("{} reports \"1.4.2\"", scratch.join("other/bun").display()),
        ] {
            assert!(refused.contains(&named), "{named} in {refused}");
        }
        verify(OsStr::new("npm"), &other, &root, None).unwrap();
    }

    /// An activation's report answers for the one program file it names, without asking that
    /// program again; any other program is asked.
    #[test]
    fn an_activation_report_answers_for_its_own_program_file_only() {
        let scratch = crate::temp_root::TempRoot::new("managed-witness");
        let root = scratch.join("checkout");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("package.json"),
            r#"{"packageManager":"bun@1.4.3"}"#,
        )
        .unwrap();
        script(&scratch.join("silent/bun"), "echo probed >&2; exit 1");
        script(&scratch.join("elsewhere/bun"), "echo 1.4.3");
        let (silent, elsewhere) = (
            managed(scratch.join("silent/bun")),
            managed(scratch.join("elsewhere/bun")),
        );
        let witness = |program: &Managed, version: &str| {
            OsString::from(format!(
                "{version} {}:{}",
                program.identity.0, program.identity.1
            ))
        };
        let bun = OsStr::new("bun");
        verify(bun, &silent, &root, Some(&witness(&silent, "1.4.3"))).unwrap();
        let refused = verify(bun, &silent, &root, Some(&witness(&silent, "1.4.2")))
            .unwrap_err()
            .to_string();
        assert!(refused.contains("reports \"1.4.2\""), "{refused}");
        let asked = verify(bun, &silent, &root, Some(&witness(&elsewhere, "1.4.3")))
            .unwrap_err()
            .to_string();
        assert!(
            asked.contains("--version ended") && asked.contains("probed"),
            "{asked}"
        );
    }

    /// A declaration that is not `<name>@<version>`, or a manifest that does not parse, is refused
    /// rather than read as no declaration.
    #[test]
    fn a_malformed_package_manager_declaration_is_refused_not_ignored() {
        let scratch = crate::temp_root::TempRoot::new("managed-declaration");
        let root = scratch.join("checkout");
        fs::create_dir_all(&root).unwrap();
        assert_eq!(Declaration::read(&root).unwrap(), None);
        fs::write(root.join("package.json"), r#"{"name":"fixture"}"#).unwrap();
        assert_eq!(Declaration::read(&root).unwrap(), None);
        for manifest in [
            r#"{"packageManager":"bun"}"#,
            r#"{"packageManager":"@1.4.3"}"#,
            r#"{"packageManager":"bun@"}"#,
            r#"{"packageManager":7}"#,
            "{",
        ] {
            fs::write(root.join("package.json"), manifest).unwrap();
            let refused = Declaration::read(&root).unwrap_err();
            assert_eq!(
                refused.kind(),
                io::ErrorKind::InvalidData,
                "{manifest}: {refused}"
            );
        }
    }

    #[test]
    fn developer_links_are_restored_without_borrowing_dependency_store_links() {
        let scratch = crate::temp_root::TempRoot::new("developer-links");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::create_dir_all(root.join("node_modules/.bun/registry/node_modules/pkg")).unwrap();
        fs::create_dir_all(root.join(".cache/owned")).unwrap();
        let sibling = scratch.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        fs::create_dir_all(sibling.join("bin")).unwrap();
        fs::write(sibling.join("bin/dev"), "#!/bin/sh\n").unwrap();
        fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        symlink("../.cache/owned", root.join("node_modules/owned")).unwrap();
        symlink(
            ".bun/registry/node_modules/pkg",
            root.join("node_modules/pkg"),
        )
        .unwrap();
        symlink("../@scope/dev/bin/dev", root.join("node_modules/.bin/dev")).unwrap();
        let links =
            DeveloperLinks::capture(&root, CaptureMode::Mint, fixture_stores(&root)).unwrap();
        assert_eq!(links.links.len(), 3);
        assert_eq!(links.sibling_targets().collect::<Vec<_>>(), [&sibling]);
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        fs::create_dir(root.join("node_modules/@scope/dev")).unwrap();
        fs::write(
            root.join("node_modules/@scope/dev/registry-payload"),
            "installed\n",
        )
        .unwrap();
        fs::remove_file(root.join("node_modules/owned")).unwrap();
        fs::remove_file(root.join("node_modules/.bin/dev")).unwrap();
        symlink("../pkg/registry-bin", root.join("node_modules/.bin/dev")).unwrap();
        links.restore(&root, RestoreMode::CoveredInstall).unwrap();
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
            sibling
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/owned")).unwrap(),
            Path::new("../.cache/owned")
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/pkg")).unwrap(),
            Path::new(".bun/registry/node_modules/pkg")
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/.bin/dev")).unwrap(),
            Path::new("../@scope/dev/bin/dev")
        );
    }

    /// A linked-package snapshot lives in declared build state: `.cache/linked-snapshots` is a link
    /// through the build link into the checkout's own volume. A package alias bound to it is the
    /// checkout's own product, and a contained link into any other physical tree still escapes.
    #[test]
    fn owned_links_into_the_checkouts_own_build_volume_are_owned() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-build-volume");
        let root = scratch.join("checkout");
        let volume = scratch.join("volume");
        let snapshots = "declared/.cache/linked-snapshots";
        fs::create_dir_all(volume.join(snapshots).join("id/packages/lmao")).unwrap();
        fs::create_dir_all(root.join(".cowshed")).unwrap();
        fs::create_dir_all(root.join(".cache")).unwrap();
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        for args in [&["init", "--quiet"][..], &["add", "package.json"]] {
            assert!(
                Command::new("git")
                    .args(args)
                    .current_dir(&root)
                    .status_bounded()
                    .unwrap()
                    .success()
            );
        }
        symlink(&volume, root.join(crate::build_volume::BUILD_LINK)).unwrap();
        symlink(
            "../.cowshed/build/declared/.cache/linked-snapshots",
            root.join(".cache/linked-snapshots"),
        )
        .unwrap();
        let alias = root.join("node_modules/@scope/lmao");
        symlink("../../.cache/linked-snapshots/id/packages/lmao", &alias).unwrap();
        for mode in [CaptureMode::Mint, CaptureMode::Job] {
            let links = DeveloperLinks::capture(&root, mode, fixture_stores(&root)).unwrap();
            assert_eq!(links.links.len(), 1);
            assert!(matches!(links.links[0].binding, Binding::Owned(_)));
            assert_eq!(links.sibling_targets().count(), 0);
        }

        let elsewhere = scratch.join("elsewhere");
        fs::create_dir_all(elsewhere.join("id/packages/lmao")).unwrap();
        fs::remove_file(root.join(".cache/linked-snapshots")).unwrap();
        symlink(&elsewhere, root.join(".cache/linked-snapshots")).unwrap();
        let error = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap_err();
        assert!(
            error.to_string().contains("escapes its checkout"),
            "{error}"
        );
    }

    #[test]
    fn job_terminal_keeps_live_relinks_and_reports_removed_links() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-terminal");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let original = scratch.join("original");
        let other = scratch.join("other");
        let gone = scratch.join("gone");
        for directory in [&original, &other, &gone] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::create_dir_all(original.join("bin")).unwrap();
        fs::create_dir_all(other.join("bin")).unwrap();
        fs::write(original.join("bin/dev"), "#!/bin/sh\necho original\n").unwrap();
        fs::write(other.join("bin/dev"), "#!/bin/sh\necho other\n").unwrap();
        symlink(&original, root.join("node_modules/@scope/dev")).unwrap();
        symlink(&gone, root.join("node_modules/@scope/removed")).unwrap();
        symlink("../@scope/dev/bin/dev", root.join("node_modules/.bin/dev")).unwrap();
        let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        assert_eq!(links.links.len(), 3);
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        symlink(&other, root.join("node_modules/@scope/dev")).unwrap();
        fs::remove_file(root.join("node_modules/.bin/dev")).unwrap();
        symlink(other.join("bin/dev"), root.join("node_modules/.bin/dev")).unwrap();
        fs::remove_file(root.join("node_modules/@scope/removed")).unwrap();
        let findings = links.restore(&root, RestoreMode::JobTerminal).unwrap();
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
            other
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/.bin/dev")).unwrap(),
            other.join("bin/dev")
        );
        assert_eq!(
            fs::symlink_metadata(root.join("node_modules/@scope/removed"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(findings.len(), 1);
        assert!(findings[0].starts_with("developer-link-ambiguous: "));
        assert!(findings[0].contains("removed"));
    }

    #[test]
    fn parent_directory_replaced_by_symlink_is_refused_without_writes() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-parent-link");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let sibling = scratch.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        fs::remove_dir_all(root.join("node_modules/@scope")).unwrap();
        let foreign = scratch.join("foreign");
        fs::create_dir_all(&foreign).unwrap();
        fs::write(foreign.join("sentinel"), "keep\n").unwrap();
        symlink(&foreign, root.join("node_modules/@scope")).unwrap();
        assert!(links.restore(&root, RestoreMode::CoveredInstall).is_err());
        assert_eq!(fs::read_dir(&foreign).unwrap().count(), 1);
        assert_eq!(
            fs::read_to_string(foreign.join("sentinel")).unwrap(),
            "keep\n"
        );
    }

    #[test]
    fn job_terminal_restores_replacements_and_preserves_only_relinked_aliases() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-classes");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
        fs::create_dir_all(root.join("node_modules/.bun/registry/pkg/bin")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let payload_src = scratch.join("payload-src");
        let registry_src = scratch.join("registry-src");
        let owner = scratch.join("owner");
        let relinked_a = scratch.join("relinked-a");
        let relinked_b = scratch.join("relinked-b");
        for directory in [
            &payload_src,
            &registry_src,
            &owner,
            &relinked_a,
            &relinked_b,
        ] {
            fs::create_dir_all(directory.join("bin")).unwrap();
            fs::write(directory.join("bin/cli"), "#!/bin/sh\n").unwrap();
            fs::write(directory.join("bin/cli2"), "#!/bin/sh\n").unwrap();
        }
        symlink(&payload_src, root.join("node_modules/@scope/payload")).unwrap();
        symlink(&registry_src, root.join("node_modules/@scope/registry")).unwrap();
        symlink(&owner, root.join("node_modules/@scope/owner")).unwrap();
        symlink(&relinked_a, root.join("node_modules/@scope/relinked")).unwrap();
        symlink(
            "../@scope/owner/bin/cli",
            root.join("node_modules/.bin/owner-cli"),
        )
        .unwrap();
        symlink(
            "../@scope/relinked/bin/cli",
            root.join("node_modules/.bin/relinked-cli"),
        )
        .unwrap();
        let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        assert_eq!(links.links.len(), 6);
        fs::remove_file(root.join("node_modules/@scope/payload")).unwrap();
        fs::create_dir_all(root.join("node_modules/@scope/payload")).unwrap();
        fs::write(
            root.join("node_modules/@scope/payload/registry-payload"),
            "installed\n",
        )
        .unwrap();
        fs::remove_file(root.join("node_modules/@scope/registry")).unwrap();
        symlink(
            "../.bun/registry/pkg",
            root.join("node_modules/@scope/registry"),
        )
        .unwrap();
        fs::remove_file(root.join("node_modules/.bin/owner-cli")).unwrap();
        symlink(
            "../.bun/registry/pkg/bin/cli",
            root.join("node_modules/.bin/owner-cli"),
        )
        .unwrap();
        fs::remove_file(root.join("node_modules/@scope/relinked")).unwrap();
        symlink(&relinked_b, root.join("node_modules/@scope/relinked")).unwrap();
        fs::remove_file(root.join("node_modules/.bin/relinked-cli")).unwrap();
        symlink(
            relinked_b.join("bin/cli2"),
            root.join("node_modules/.bin/relinked-cli"),
        )
        .unwrap();
        let findings = links.restore(&root, RestoreMode::JobTerminal).unwrap();
        assert!(findings.is_empty());
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/payload")).unwrap(),
            payload_src
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/registry")).unwrap(),
            registry_src
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/.bin/owner-cli")).unwrap(),
            Path::new("../@scope/owner/bin/cli")
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/relinked")).unwrap(),
            relinked_b
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/.bin/relinked-cli")).unwrap(),
            relinked_b.join("bin/cli2")
        );
    }

    #[test]
    fn unreadable_targets_are_unclassified_only_for_jobs_and_restorable_without_grants() {
        assert!(unreadable(
            &io::Error::from_raw_os_error(libc::EPERM),
            CaptureMode::Job
        ));
        assert!(unreadable(
            &io::Error::from_raw_os_error(libc::EACCES),
            CaptureMode::Job
        ));
        assert!(!unreadable(
            &io::Error::from_raw_os_error(libc::EPERM),
            CaptureMode::Mint
        ));
        assert!(!unreadable(
            &io::Error::from(io::ErrorKind::NotFound),
            CaptureMode::Job
        ));
        let scratch = crate::temp_root::TempRoot::new("developer-links-unclassified");
        let root = scratch.join("checkout");
        fs::create_dir_all(&root).unwrap();
        let target = scratch.join("opaque");
        let links = DeveloperLinks {
            links: vec![DeveloperLink {
                at: PathBuf::from("node_modules/@scope/opaque"),
                target: target.clone(),
                binding: Binding::Unclassified,
            }],
            released: Vec::new(),
            stores: PackageStores::default(),
            holders: None,
        };
        assert_eq!(links.sibling_targets().count(), 0);
        links.restore(&root, RestoreMode::CoveredInstall).unwrap();
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/opaque")).unwrap(),
            target
        );
    }

    #[test]
    fn observed_install_restores_missing_links_and_unobserved_terminal_keeps_them_absent() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-observed");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let sibling = scratch.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
        let findings = links.restore(&root, RestoreMode::JobTerminal).unwrap();
        assert_eq!(findings.len(), 1);
        assert_eq!(
            fs::symlink_metadata(root.join("node_modules/@scope/dev"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        let findings = links
            .restore(&root, RestoreMode::JobTerminalObservedInstall)
            .unwrap();
        assert!(findings.is_empty());
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
            sibling
        );
        fs::remove_dir_all(root.join("node_modules/@scope")).unwrap();
        let findings = links
            .restore(&root, RestoreMode::JobTerminalObservedInstall)
            .unwrap();
        assert!(findings.is_empty());
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
            sibling
        );
    }

    /// A checkout whose root manifest declares `links` as `smoo.developerLinks`, with Bun's store
    /// entry for `@scope/dev` and its alias linked to that entry, as a fresh install leaves it.
    fn declaring_checkout(scratch: &Path, links: &str) -> PathBuf {
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join(STORE_ENTRY)).unwrap();
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        declare(&root, links);
        for args in [&["init", "--quiet"][..], &["add", "package.json"]] {
            assert!(
                Command::new("git")
                    .args(args)
                    .current_dir(&root)
                    .status_bounded()
                    .unwrap()
                    .success()
            );
        }
        relink(STORE_LINK, &root.join("node_modules/@scope/dev"));
        root
    }

    const STORE_ENTRY: &str = "node_modules/.bun/@scope+dev@1.0.0/node_modules/@scope/dev";
    /// The alias text Bun writes for `@scope/dev`: its store entry, from `node_modules/@scope`.
    const STORE_LINK: &str = "../.bun/@scope+dev@1.0.0/node_modules/@scope/dev";

    fn declare(root: &Path, links: &str) {
        fs::write(
            root.join("package.json"),
            format!("{{\"smoo\":{{\"developerLinks\":{links}}}}}\n"),
        )
        .unwrap();
    }

    fn relink(target: impl AsRef<Path>, at: &Path) {
        match fs::remove_file(at) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => panic!("{error}"),
        }
        symlink(target, at).unwrap();
    }

    /// What `developer-links.ts` records for each alias it linked (`writeDeclaredRecord`).
    fn record(root: &Path, receipts: &[(&str, &str, &Path)]) {
        let rows: Vec<_> = receipts
            .iter()
            .map(|(path, declaration, target)| {
                serde_json::json!({ "path": path, "declaration": declaration, "target": target })
            })
            .collect();
        fs::write(
            root.join(DECLARED_RECORD),
            format!("{}\n", serde_json::Value::Array(rows)),
        )
        .unwrap();
    }

    /// Declare a link, install, stop declaring it, install again: the second install leaves the
    /// lockfile's version, and neither the wrapper's snapshot nor the job's terminal cleanup puts
    /// the dropped link back. Each install is what lands on disk under a covered `bun install`:
    /// the wrapper's snapshot, Bun re-pointing the alias at its store entry, then the managed
    /// postinstall linking each declared alias and recording its receipt.
    #[test]
    fn a_link_the_declaration_no_longer_names_is_not_restored_by_the_next_install() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-undeclared");
        let sibling = scratch.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        let root = declaring_checkout(&scratch, r#"{"@scope/dev":"../sibling"}"#);
        let alias = root.join("node_modules/@scope/dev");
        let install = |postinstall: &dyn Fn()| {
            let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
            relink(STORE_LINK, &alias);
            postinstall();
            links.restore(&root, RestoreMode::CoveredInstall).unwrap();
        };
        let link_declared = || {
            relink(&sibling, &alias);
            record(
                &root,
                &[("node_modules/@scope/dev", "../sibling", &sibling)],
            );
        };

        install(&link_declared);
        assert_eq!(fs::read_link(&alias).unwrap(), sibling);

        declare(&root, "{}");
        let job = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        install(&|| {});
        assert_eq!(fs::read_link(&alias).unwrap(), Path::new(STORE_LINK));
        let findings = job
            .restore(&root, RestoreMode::JobTerminalObservedInstall)
            .unwrap();
        assert!(findings.is_empty());
        assert_eq!(fs::read_link(&alias).unwrap(), Path::new(STORE_LINK));
    }

    /// The converse, under an install whose postinstall did not run (`--ignore-scripts`): the
    /// declared link comes back from the snapshot, and so do the developer's own links, one with
    /// no receipt and one retargeted away from its receipt's text, declared or not.
    #[test]
    fn declared_and_developer_links_survive_an_install() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-declared");
        let [sibling, manual, retargeted] = ["sibling", "manual", "retargeted"].map(|name| {
            let directory = scratch.join(name);
            fs::create_dir_all(&directory).unwrap();
            directory
        });
        let root = declaring_checkout(&scratch, r#"{"@scope/dev":"../sibling"}"#);
        let alias = root.join("node_modules/@scope/dev");
        relink(&sibling, &alias);
        relink(&manual, &root.join("node_modules/manual"));
        relink(&retargeted, &root.join("node_modules/retargeted"));
        record(
            &root,
            &[
                ("node_modules/@scope/dev", "../sibling", &sibling),
                ("node_modules/retargeted", "../old", &scratch.join("old")),
            ],
        );
        let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        assert_eq!(links.links.len(), 3);
        relink(STORE_LINK, &alias);
        fs::remove_file(root.join("node_modules/manual")).unwrap();
        fs::remove_file(root.join("node_modules/retargeted")).unwrap();
        links.restore(&root, RestoreMode::CoveredInstall).unwrap();
        assert_eq!(fs::read_link(&alias).unwrap(), sibling);
        assert_eq!(
            fs::read_link(root.join("node_modules/manual")).unwrap(),
            manual
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/retargeted")).unwrap(),
            retargeted
        );
    }

    /// Restores are grouped by held parent: a parent with a changed entry is synced once, and a
    /// parent whose links already name their targets is not synced at all.
    #[test]
    fn restore_syncs_each_changed_parent_once() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-one-sync");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        let target = scratch.join("target");
        let names = ["node_modules/a", "node_modules/b", "node_modules/@scope/c"];
        let links = DeveloperLinks {
            links: names
                .into_iter()
                .map(|at| DeveloperLink {
                    at: PathBuf::from(at),
                    target: target.clone(),
                    binding: Binding::Unclassified,
                })
                .collect(),
            released: Vec::new(),
            stores: PackageStores::default(),
            holders: None,
        };
        take_syncs();
        links.restore(&root, RestoreMode::CoveredInstall).unwrap();
        assert_eq!(
            take_syncs(),
            Syncs { fsync: 0, full: 2 },
            "one sync for node_modules and one for node_modules/@scope"
        );
        for name in names {
            assert_eq!(fs::read_link(root.join(name)).unwrap(), target);
        }
        links.restore(&root, RestoreMode::CoveredInstall).unwrap();
        assert_eq!(
            take_syncs(),
            Syncs::default(),
            "links that already name their targets are kept without a sync"
        );
    }

    #[test]
    fn package_store_roots_are_registry_and_developer_siblings_outside_them_are_bindings() {
        let scratch_root = crate::temp_root::TempRoot::new("developer-links-stores");
        let scratch = fs::canonicalize(&*scratch_root).unwrap();
        let home = scratch.join("home");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let bun_store = BUN.host_path(&home).join("pkg");
        let pnpm_store = PNPM.host_path(&home).join("pkg");
        let sibling = home.join("Dev/pnpm/store/pkg");
        let registration = home.join(".bun/install/global/pkg");
        let alias = home.join("alias-into-store");
        for directory in [&bun_store, &pnpm_store, &sibling, &registration] {
            fs::create_dir_all(directory).unwrap();
        }
        symlink(&bun_store, &alias).unwrap();
        symlink(&bun_store, root.join("node_modules/@scope/bun-cache")).unwrap();
        symlink(&pnpm_store, root.join("node_modules/@scope/pnpm-cache")).unwrap();
        symlink(&alias, root.join("node_modules/@scope/aliased")).unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        symlink(&registration, root.join("node_modules/@scope/registered")).unwrap();
        let stores = PackageStores::from_home(&home).unwrap();
        let links = DeveloperLinks::capture(&root, CaptureMode::Mint, stores).unwrap();
        assert_eq!(
            links.links.len(),
            2,
            "only the two real developer bindings are captured"
        );
        assert_eq!(
            links.sibling_targets().collect::<Vec<_>>(),
            [&sibling, &registration]
        );
    }

    /// A home reached through a symlink names its stores by the raw path, and a link that spells
    /// the physical store is still that store's entry. A developer sibling outside both spellings
    /// stays a binding.
    #[test]
    fn a_symlinked_home_recognizes_its_store_by_the_physical_path() {
        let scratch_root = crate::temp_root::TempRoot::new("developer-links-home-alias");
        let scratch = fs::canonicalize(&*scratch_root).unwrap();
        let physical_home = scratch.join("physical-home");
        let home = scratch.join("home");
        let root = scratch.join("checkout");
        fs::create_dir_all(&physical_home).unwrap();
        symlink(&physical_home, &home).unwrap();
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let store = BUN.host_path(&physical_home).join("pkg");
        let sibling = physical_home.join("Dev/pnpm/store/pkg");
        for directory in [&store, &sibling] {
            fs::create_dir_all(directory).unwrap();
        }
        symlink(&store, root.join("node_modules/@scope/store-entry")).unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        let stores = PackageStores::from_home(&home).unwrap();
        let links = DeveloperLinks::capture(&root, CaptureMode::Mint, stores).unwrap();
        assert_eq!(
            links.links.len(),
            1,
            "the store's own entry is not captured"
        );
        assert_eq!(links.sibling_targets().collect::<Vec<_>>(), [&sibling]);
    }

    #[test]
    fn package_store_roots_must_be_absolute_and_never_match_everything() {
        assert!(PackageStores::from_roots(Some(PathBuf::new()), None).is_err());
        assert!(PackageStores::from_roots(None, Some(PathBuf::from("relative/store"))).is_err());
        let stores = PackageStores::from_roots(Some(PathBuf::from("/store/bun")), None).unwrap();
        assert!(stores.contains(Path::new("/store/bun/pkg")));
        assert!(
            !stores.contains(Path::new("/store/bunny/pkg")),
            "whole components only"
        );
        assert!(!stores.contains(Path::new("/elsewhere/pkg")));
        assert!(
            !PackageStores::default().contains(Path::new("/anything")),
            "no stores match nothing"
        );
        assert!(DeveloperLinks::default().is_empty());
    }

    #[test]
    fn manager_coverage_is_exact_and_non_install_commands_are_not_snapshotted() {
        let args = |verb: &str| vec![OsString::from(verb)];
        assert!(installs(OsStr::new("bun"), &args("install")));
        assert!(installs(OsStr::new("npm"), &args("ci")));
        assert!(installs(OsStr::new("pnpm"), &args("add")));
        for verb in ["run", "test", "link", "--version"] {
            assert!(!installs(OsStr::new("bun"), &args(verb)));
        }
    }

    /// This process's cumulative Unix and Mach syscall counts, read from `PROC_PIDTASKINFO`.
    #[cfg(target_os = "macos")]
    fn own_syscalls() -> io::Result<(i32, i32)> {
        let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::zeroed();
        let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_taskinfo>())
            .map_err(io::Error::other)?;
        // SAFETY: `info` is writable storage of exactly `size` bytes.
        let written = unsafe {
            libc::proc_pidinfo(
                libc::getpid(),
                libc::PROC_PIDTASKINFO,
                0,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        if written != size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("proc_pidinfo returned {written} bytes, expected {size}"),
            ));
        }
        // SAFETY: proc_pidinfo filled all `size` bytes.
        let info = unsafe { info.assume_init() };
        Ok((info.pti_syscalls_unix, info.pti_syscalls_mach))
    }

    /// A cumulative counter's growth over one interval; a counter that went backwards is an error.
    fn counter_delta(before: i32, after: i32) -> io::Result<u64> {
        let before = u64::try_from(before).map_err(io::Error::other)?;
        let after = u64::try_from(after).map_err(io::Error::other)?;
        after
            .checked_sub(before)
            .ok_or_else(|| io::Error::other("a syscall counter went backwards"))
    }

    #[test]
    fn counter_delta_refuses_a_counter_that_went_backwards() {
        assert_eq!(counter_delta(3, 10).unwrap(), 7);
        assert_eq!(counter_delta(4, 4).unwrap(), 0);
        assert!(counter_delta(10, 3).is_err());
        assert!(counter_delta(-1, 3).is_err());
    }

    /// Measures what admission's `DeveloperLinks::capture_job` costs against the real repository
    /// root: this process's own wall time and syscalls only, so child `git` processes are excluded.
    /// It is a component measurement, not a full admission or CLI latency number.
    #[cfg(target_os = "macos")]
    #[test]
    fn sb_admission_snapshot_wall_and_syscalls() {
        let manifest = PathBuf::from(
            std::env::var_os("CARGO_MANIFEST_DIR")
                .expect("cargo sets CARGO_MANIFEST_DIR for tests"),
        );
        // cowshed-core lives at <root>/packages/cowshed/crates/cowshed-core.
        let real_root = manifest
            .ancestors()
            .nth(4)
            .expect("the manifest sits below the repository root");
        let real_root = fs::canonicalize(real_root).expect("repository root");
        let (unix_before, mach_before) = own_syscalls().expect("counters before");
        let started = std::time::Instant::now();
        let home = PathBuf::from(std::env::var_os("HOME").expect("the host has a home"));
        let stores = PackageStores::from_home(&home).expect("an absolute host home");
        let links = DeveloperLinks::capture_job(&real_root, stores).expect("admission snapshot");
        let wall = started.elapsed();
        let (unix_after, mach_after) = own_syscalls().expect("counters after");
        let unix_syscalls = counter_delta(unix_before, unix_after).expect("unix delta");
        let mach_syscalls = counter_delta(mach_before, mach_after).expect("mach delta");
        // A snapshot that reads any directory must have made syscalls; zero would be a blind read.
        assert!(
            unix_syscalls > 0,
            "the snapshot made no Unix syscalls, so the counters did not observe it"
        );
        println!(
            "{}",
            serde_json::json!({
                "scope": "SB additional admission DeveloperLinks snapshot; process counters exclude child git",
                "wallMicros": u64::try_from(wall.as_micros()).expect("wall micros fit u64"),
                "unixSyscalls": unix_syscalls,
                "machSyscalls": mach_syscalls,
                "capturedLinks": links.links.len(),
            })
        );
    }

    /// The sandbox proof that a job snapshot keeps an unreadable target unclassified: a real
    /// `sandbox-exec` denial makes the target's canonicalize fail with the kernel's own error, not
    /// a substituted one. The parent sets up the fixture and runs this same test again inside the
    /// profile; the child asserts the job-side behaviour and writes proof beside the checkout. The
    /// `--exact` child never spawns another process.
    #[cfg(target_os = "macos")]
    #[test]
    fn job_snapshot_keeps_an_unreadable_target_unclassified_without_grants() {
        const FIXTURE_ENV: &str = "COWSHED_UNREADABLE_LINK_FIXTURE_ROOT";
        const PROOF: &str = "unreadable-link-proof";
        if let Some(root) = std::env::var_os(FIXTURE_ENV) {
            let root = PathBuf::from(root);
            let links = DeveloperLinks::capture_job(&root, fixture_stores(&root))
                .expect("a job snapshot of an unreadable link is admitted");
            assert_eq!(links.links.len(), 1, "exactly the foreign link");
            assert!(matches!(links.links[0].binding, Binding::Unclassified));
            assert_eq!(
                links.sibling_targets().count(),
                0,
                "unclassified grants nothing"
            );
            let target = links.links[0].target.clone();
            fs::remove_file(root.join("node_modules/foreign")).unwrap();
            links
                .restore(&root, RestoreMode::CoveredInstall)
                .expect("the covered restore recreates the link without reading its target");
            assert_eq!(
                fs::read_link(root.join("node_modules/foreign")).unwrap(),
                target
            );
            assert!(
                DeveloperLinks::capture(&root, CaptureMode::Mint, fixture_stores(&root)).is_err(),
                "the mint refuses the same unreadable target"
            );
            fs::write(
                root.parent().unwrap().join(PROOF),
                "unclassified-restored\n",
            )
            .unwrap();
            return;
        }
        let scratch = crate::temp_root::TempRoot::new("developer-links-unreadable-link");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let foreign = scratch.join("foreign");
        fs::create_dir_all(&foreign).unwrap();
        symlink(&foreign, root.join("node_modules/foreign")).unwrap();
        let denied = fs::canonicalize(&foreign).unwrap();
        let profile = format!(
            "(version 1)(allow default)(deny file-read* (subpath {}))",
            serde_json::to_string(denied.to_str().unwrap()).unwrap()
        );
        let status = Command::new("/usr/bin/sandbox-exec")
            .arg("-p")
            .arg(profile)
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "developer_links::tests::job_snapshot_keeps_an_unreadable_target_unclassified_without_grants",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(FIXTURE_ENV, &root)
            .status_bounded()
            .unwrap();
        assert!(status.success(), "sandboxed child failed: {status}");
        assert_eq!(
            fs::read_to_string(scratch.join(PROOF)).unwrap(),
            "unclassified-restored\n",
            "the child reached its assertions"
        );
    }

    #[test]
    fn an_alias_to_a_missing_path_is_restored_even_when_its_owner_was_relinked() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-alias-missing");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let original = scratch.join("original");
        let other = scratch.join("other");
        for directory in [&original, &other] {
            fs::create_dir_all(directory.join("bin")).unwrap();
            fs::write(directory.join("bin/cli"), "#!/bin/sh\n").unwrap();
        }
        symlink(&original, root.join("node_modules/@scope/owner")).unwrap();
        symlink(
            "../@scope/owner/bin/cli",
            root.join("node_modules/.bin/owner-cli"),
        )
        .unwrap();
        let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        assert_eq!(links.links.len(), 2);
        fs::remove_file(root.join("node_modules/@scope/owner")).unwrap();
        symlink(&other, root.join("node_modules/@scope/owner")).unwrap();
        fs::remove_file(root.join("node_modules/.bin/owner-cli")).unwrap();
        symlink(
            "../@scope/owner/bin/missing",
            root.join("node_modules/.bin/owner-cli"),
        )
        .unwrap();
        let findings = links.restore(&root, RestoreMode::JobTerminal).unwrap();
        assert!(findings.is_empty());
        assert_eq!(
            fs::read_link(root.join("node_modules/@scope/owner")).unwrap(),
            other
        );
        assert_eq!(
            fs::read_link(root.join("node_modules/.bin/owner-cli")).unwrap(),
            Path::new("../@scope/owner/bin/cli")
        );
    }

    /// Under a real sandbox, a job that replaces a registry payload and relinks a package to a
    /// target the sandbox denies must have both repaired by terminal cleanup. The child captures
    /// while the targets are still readable, then the job makes its changes, then the denial
    /// applies to the restore: the denied relink is restored to its captured bytes without
    /// reading its target. `/usr/bin/sandbox-exec` is the real seatbelt, not a mocked errno.
    #[cfg(target_os = "macos")]
    #[test]
    fn job_terminal_repairs_a_registry_replacement_and_a_relink_to_a_denied_target() {
        const FIXTURE_ENV: &str = "COWSHED_DENIED_RELINK_FIXTURE_ROOT";
        const PROOF: &str = "denied-relink-proof";
        if let Some(root) = std::env::var_os(FIXTURE_ENV) {
            let root = PathBuf::from(root);
            let links = DeveloperLinks::capture_job(&root, fixture_stores(&root))
                .expect("the job snapshot");
            let allowed = root.parent().unwrap().join("allowed");
            let denied_other = root.parent().unwrap().join("denied/other");
            let sibling = root.parent().unwrap().join("sibling");
            let original_cli = PathBuf::from("../foreign/bin/cli");
            // The job's own changes: a registry payload replaces the sibling link, the foreign
            // package is relinked into the denied subtree, and its .bin alias is retargeted to the
            // denied subtree's cli, so both denied links differ from their captured bytes.
            fs::remove_file(root.join("node_modules/@scope/dev")).unwrap();
            fs::create_dir_all(root.join("node_modules/@scope/dev")).unwrap();
            fs::write(
                root.join("node_modules/@scope/dev/registry-payload"),
                "installed\n",
            )
            .unwrap();
            fs::remove_file(root.join("node_modules/foreign")).unwrap();
            symlink(&denied_other, root.join("node_modules/foreign")).unwrap();
            fs::remove_file(root.join("node_modules/.bin/pkg")).unwrap();
            symlink(
                denied_other.join("bin/cli"),
                root.join("node_modules/.bin/pkg"),
            )
            .unwrap();
            let findings = links
                .restore(&root, RestoreMode::JobTerminal)
                .expect("terminal cleanup repairs all three without reading the denied targets");
            assert!(findings.is_empty());
            assert_eq!(
                fs::read_link(root.join("node_modules/@scope/dev")).unwrap(),
                sibling
            );
            assert_eq!(
                fs::read_link(root.join("node_modules/foreign")).unwrap(),
                allowed
            );
            assert_eq!(
                fs::read_link(root.join("node_modules/.bin/pkg")).unwrap(),
                original_cli,
                "the denied alias is restored to its captured bytes"
            );
            fs::write(
                root.parent().unwrap().join(PROOF),
                "denied-relink-repaired\n",
            )
            .unwrap();
            return;
        }
        let scratch = crate::temp_root::TempRoot::new("developer-links-denied-relink");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_bounded()
                .unwrap()
                .success()
        );
        let sibling = scratch.join("sibling");
        let allowed = scratch.join("allowed");
        let denied = scratch.join("denied");
        for directory in [
            &sibling,
            &allowed,
            &denied.join("other"),
            &denied.join("original"),
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::create_dir_all(allowed.join("bin")).unwrap();
        fs::create_dir_all(denied.join("other/bin")).unwrap();
        fs::write(allowed.join("bin/cli"), "#!/bin/sh\n").unwrap();
        fs::write(denied.join("other/bin/cli"), "#!/bin/sh\n").unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        symlink(&allowed, root.join("node_modules/foreign")).unwrap();
        symlink("../foreign/bin/cli", root.join("node_modules/.bin/pkg")).unwrap();
        let denied_canonical = fs::canonicalize(&denied).unwrap();
        let profile = format!(
            "(version 1)(allow default)(deny file-read* (subpath {}))",
            serde_json::to_string(denied_canonical.to_str().unwrap()).unwrap()
        );
        let status = Command::new("/usr/bin/sandbox-exec")
            .arg("-p")
            .arg(profile)
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "developer_links::tests::job_terminal_repairs_a_registry_replacement_and_a_relink_to_a_denied_target",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(FIXTURE_ENV, &root)
            .status_bounded()
            .unwrap();
        assert!(status.success(), "sandboxed child failed: {status}");
        assert_eq!(
            fs::read_to_string(scratch.join(PROOF)).unwrap(),
            "denied-relink-repaired\n",
            "the child reached its assertions"
        );
    }

    /// The previous per-link traversal, retained only as a differential and timing oracle.
    fn plan_per_link(
        root: &Path,
        snapshot: &DeveloperLinks,
        mode: RestoreMode,
    ) -> Result<Vec<Plan>> {
        snapshot
            .links
            .iter()
            .map(|link| {
                let at = root.join(&link.at);
                let parent = at.parent().expect("a package link has a parent");
                crate::storage::verify_no_symlinks(root, parent)
                    .map_err(|error| link_error(parent, error))?;
                let mut buffer = [0u8; 4096];
                let current = match AnchoredDirectory::open(parent) {
                    Ok(directory) => current_leaf(&directory, &at, &mut buffer)?,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Current::ParentAbsent,
                    Err(error) => return Err(link_error(parent, error)),
                };
                plan(
                    root,
                    &snapshot.stores,
                    &snapshot.links,
                    link,
                    &at,
                    current,
                    mode,
                )
            })
            .collect()
    }

    fn planning_fixture(root: &Path, count: usize, separate_parents: bool) -> DeveloperLinks {
        let mut links = Vec::with_capacity(count);
        for index in 0..count {
            let parent = if separate_parents {
                format!("node_modules/@scope-{index:04}")
            } else {
                "node_modules/@scope".to_owned()
            };
            let at = PathBuf::from(parent).join(format!("link-{index:04}"));
            fs::create_dir_all(root.join(at.parent().unwrap())).unwrap();
            let target = PathBuf::from("../../product");
            symlink(&target, root.join(&at)).unwrap();
            links.push(DeveloperLink {
                at,
                target,
                binding: Binding::Unclassified,
            });
        }
        DeveloperLinks {
            links,
            ..DeveloperLinks::default()
        }
    }

    #[test]
    fn grouped_planning_matches_per_link_reads_in_every_restore_mode() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-planning");
        let root = fs::canonicalize(&*scratch).unwrap();
        let mut snapshot = planning_fixture(&root, 5, false);
        // Unchanged, missing, replaced file, replaced directory, and a retargeted link.
        for link in &snapshot.links[1..] {
            fs::remove_file(root.join(&link.at)).unwrap();
        }
        fs::write(root.join(&snapshot.links[2].at), "replacement").unwrap();
        fs::create_dir(root.join(&snapshot.links[3].at)).unwrap();
        symlink("../../other", root.join(&snapshot.links[4].at)).unwrap();
        snapshot.links.push(DeveloperLink {
            at: PathBuf::from("node_modules/absent-parent/link"),
            target: PathBuf::from("../../product"),
            binding: Binding::Unclassified,
        });
        snapshot
            .links
            .sort_unstable_by(|left, right| left.at.cmp(&right.at));
        for mode in [
            RestoreMode::CoveredInstall,
            RestoreMode::JobTerminal,
            RestoreMode::JobTerminalObservedInstall,
        ] {
            let expected = plan_per_link(&root, &snapshot, mode).unwrap();
            let actual = plan_links(&root, &snapshot.stores, &snapshot.links, mode).unwrap();
            assert_eq!(actual, expected, "{mode:?}");
            assert_eq!(
                actual
                    .iter()
                    .filter(|plan| matches!(plan, Plan::Keep))
                    .count(),
                1
            );
            let findings = actual
                .iter()
                .filter(|plan| matches!(plan, Plan::Finding(_)))
                .count();
            assert_eq!(
                findings,
                if mode == RestoreMode::JobTerminal {
                    2
                } else {
                    0
                }
            );
        }
        // No parent or leaf observation survives a call.
        fs::remove_file(root.join(&snapshot.links[0].at)).unwrap();
        let actual = plan_links(
            &root,
            &snapshot.stores,
            &snapshot.links,
            RestoreMode::JobTerminal,
        )
        .unwrap();
        assert_eq!(
            actual,
            plan_per_link(&root, &snapshot, RestoreMode::JobTerminal).unwrap()
        );
        assert!(matches!(actual[0], Plan::Finding(_)));
    }

    #[test]
    fn grouped_planning_refuses_substituted_parents_before_any_restore() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-planning-refusal");
        let root = fs::canonicalize(&*scratch).unwrap();
        let snapshot = planning_fixture(&root, 2, true);
        let first = root.join(&snapshot.links[0].at);
        fs::remove_file(&first).unwrap();
        fs::write(&first, "installer payload").unwrap();
        let parent = root.join(snapshot.links[1].at.parent().unwrap());
        let moved = root.join("moved");
        fs::rename(&parent, &moved).unwrap();
        symlink(&moved, &parent).unwrap();
        let expected = plan_per_link(&root, &snapshot, RestoreMode::CoveredInstall).unwrap_err();
        let actual = snapshot
            .restore(&root, RestoreMode::CoveredInstall)
            .unwrap_err();
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.message, expected.message);
        assert_eq!(fs::read_to_string(&first).unwrap(), "installer payload");
        // A non-directory parent also refuses; it is not reported as a missing link.
        fs::remove_file(&parent).unwrap();
        fs::write(&parent, "not a directory").unwrap();
        assert!(
            plan_links(
                &root,
                &snapshot.stores,
                &snapshot.links,
                RestoreMode::JobTerminal
            )
            .is_err()
        );
    }

    #[cfg(target_os = "macos")]
    fn measure_planning(label: &str, root: &Path, snapshot: &DeveloperLinks) {
        use std::time::{Duration, Instant};
        let run = |grouped| {
            if grouped {
                plan_links(
                    root,
                    &snapshot.stores,
                    &snapshot.links,
                    RestoreMode::JobTerminal,
                )
                .unwrap()
            } else {
                plan_per_link(root, snapshot, RestoreMode::JobTerminal).unwrap()
            }
        };
        for _ in 0..4 {
            assert_eq!(run(false), run(true));
        }
        let mut samples = [Vec::with_capacity(1_000), Vec::with_capacity(1_000)];
        for job in 0..1_000 {
            let mut results = [Vec::new(), Vec::new()];
            for arm in [job % 2, 1 - job % 2] {
                let started = Instant::now();
                results[arm] = std::hint::black_box(run(arm == 1));
                samples[arm].push(started.elapsed());
            }
            assert_eq!(results[0], results[1]);
        }
        let totals = samples
            .each_ref()
            .map(|sample| sample.iter().sum::<Duration>().as_secs_f64() * 1_000.0);
        let syscalls = [false, true].map(|grouped| {
            let before = own_syscalls().unwrap();
            std::hint::black_box(run(grouped));
            let after = own_syscalls().unwrap();
            counter_delta(before.0, after.0).unwrap()
        });
        for sample in &mut samples {
            sample.sort_unstable();
        }
        println!(
            "{}",
            serde_json::json!({
                "scope": label,
                "jobsPerArm": 1000,
                "links": snapshot.links.len(),
                "parents": snapshot.links.iter().map(|link| link.at.parent().unwrap()).collect::<BTreeSet<_>>().len(),
                "beforePerJobMicros": totals[0],
                "afterPerJobMicros": totals[1],
                "before1000JobsMillis": totals[0],
                "after1000JobsMillis": totals[1],
                "medianBeforeMicros": samples[0][500].as_secs_f64() * 1_000_000.0,
                "medianAfterMicros": samples[1][500].as_secs_f64() * 1_000_000.0,
                "unixSyscallsBefore": syscalls[0],
                "unixSyscallsAfter": syscalls[1],
            })
        );
    }

    /// Read-only component timing; every arm plans the same snapshot, never restores a link.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "opt-in real-checkout component benchmark"]
    fn terminal_planning_benchmark() {
        let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
        let root = fs::canonicalize(manifest.ancestors().nth(4).unwrap()).unwrap();
        let home = PathBuf::from(std::env::var_os("HOME").unwrap());
        let mut capture = Vec::with_capacity(9);
        let mut snapshot = DeveloperLinks::default();
        for _ in 0..9 {
            let started = std::time::Instant::now();
            snapshot = DeveloperLinks::capture_job(&root, PackageStores::from_home(&home).unwrap())
                .unwrap();
            capture.push(started.elapsed());
        }
        capture.sort_unstable();
        println!(
            "{}",
            serde_json::json!({
                "scope": "unchanged-admission-capture",
                "samples": capture.len(),
                "medianMicros": capture[4].as_secs_f64() * 1_000_000.0,
                "links": snapshot.links.len(),
            })
        );
        measure_planning("real-checkout", &root, &snapshot);
        for count in [0, 1, 2, 8, 64] {
            let scratch = crate::temp_root::TempRoot::new("developer-links-planning-bench");
            let root = fs::canonicalize(&*scratch).unwrap();
            let snapshot = planning_fixture(&root, count, false);
            measure_planning("siblings", &root, &snapshot);
        }
        let scratch = crate::temp_root::TempRoot::new("developer-links-planning-distinct");
        let root = fs::canonicalize(&*scratch).unwrap();
        let snapshot = planning_fixture(&root, 64, true);
        measure_planning("distinct-parents", &root, &snapshot);
    }

    /// A checkout with a tracked root package, one scoped developer link to `original` and its
    /// executable alias, every directory settled behind the clock.
    fn settled_checkout(scratch: &Path) -> (PathBuf, PathBuf) {
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        for args in [&["init", "--quiet"][..], &["add", "package.json"]] {
            assert!(
                Command::new("git")
                    .args(args)
                    .current_dir(&root)
                    .status_bounded()
                    .unwrap()
                    .success()
            );
        }
        let original = scratch.join("original");
        fs::create_dir_all(original.join("bin")).unwrap();
        fs::write(original.join("bin/dev"), "#!/bin/sh\necho original\n").unwrap();
        symlink(&original, root.join("node_modules/@scope/dev")).unwrap();
        symlink("../@scope/dev/bin/dev", root.join("node_modules/.bin/dev")).unwrap();
        for settled in [
            root.clone(),
            root.join(".git"),
            root.join(".git/index"),
            root.join(".git/config"),
            root.join("package.json"),
            root.join("node_modules"),
            root.join("node_modules/@scope"),
            root.join("node_modules/.bin"),
            original.clone(),
            original.join("bin"),
            original.join("bin/dev"),
            scratch.to_path_buf(),
        ] {
            crate::input_memo::settle(&settled);
        }
        (fs::canonicalize(root).unwrap(), original)
    }

    /// A snapshot taken over settled directories restores nothing while they stay as they were,
    /// and still restores a link a job replaced with an installed payload: replacing it changed
    /// the directory that holds it.
    #[test]
    fn a_settled_snapshot_restores_exactly_what_a_job_changed() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-settled");
        let (root, original) = settled_checkout(&scratch);
        let links = DeveloperLinks::capture_job(&root, fixture_stores(&root)).unwrap();
        assert_eq!(links.links.len(), 2);
        assert!(
            links.holders.is_some(),
            "settled holders let a restore see nothing changed"
        );
        assert!(
            links
                .restore(&root, RestoreMode::JobTerminal)
                .unwrap()
                .is_empty()
        );
        let dev = root.join("node_modules/@scope/dev");
        fs::remove_file(&dev).unwrap();
        fs::create_dir(&dev).unwrap();
        fs::write(dev.join("registry-payload"), "installed\n").unwrap();
        assert!(
            links
                .restore(&root, RestoreMode::JobTerminal)
                .unwrap()
                .is_empty()
        );
        assert_eq!(fs::read_link(&dev).unwrap(), original);
    }

    /// The supervisor's snapshot is kept while what its capture read keeps its identity, and a
    /// developer link added under a package tree is in the next one.
    #[test]
    fn a_kept_snapshot_takes_a_new_link_into_the_next_admission() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-kept");
        let (root, _) = settled_checkout(&scratch);
        let first = DeveloperLinks::capture_kept(&root, fixture_stores(&root)).unwrap();
        let again = DeveloperLinks::capture_kept(&root, fixture_stores(&root)).unwrap();
        assert!(Arc::ptr_eq(&first, &again), "the kept snapshot");
        let added = scratch.join("added");
        fs::create_dir_all(&added).unwrap();
        symlink(&added, root.join("node_modules/@scope/added")).unwrap();
        let next = DeveloperLinks::capture_kept(&root, fixture_stores(&root)).unwrap();
        assert!(!Arc::ptr_eq(&first, &next));
        assert_eq!(next.links.len(), 3);
    }
}
