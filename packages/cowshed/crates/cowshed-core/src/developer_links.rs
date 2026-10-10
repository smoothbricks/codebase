//! Developer package bindings carried by a mint and kept across package-manager installs.
//! The snapshot grants nothing: only the mint may add its captured canonical sibling targets
//! to controller-owned read grants. Job and package-manager snapshots restore existing entries.

use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use crate::fork_lock::Run as _;
use crate::fsio::AnchoredDirectory;
use crate::{CowshedError, Result};

pub const ROOT_ENV: &str = "COWSHED_PACKAGE_MANAGER_ROOT";
pub const TOOL_BIN_ENV: &str = "COWSHED_TOOL_BIN";
pub const INSTALL_OBSERVED_ENV: &str = "COWSHED_INSTALL_OBSERVED";

#[derive(Debug, Default)]
pub struct DeveloperLinks {
    links: Vec<DeveloperLink>,
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

enum Plan {
    Keep,
    Restore,
    Finding(String),
}

impl DeveloperLinks {
    /// Job-time snapshot: an unreadable target is kept as an unclassified, restorable link.
    pub fn capture_job(root: &Path) -> Result<Self> {
        Self::capture(root, CaptureMode::Job)
    }

    pub fn capture(root: &Path, mode: CaptureMode) -> Result<Self> {
        let root = fs::canonicalize(root).map_err(|error| link_error(root, error))?;
        let mut snapshot = Self::default();
        crate::capabilities::installed_javascript_trees(&root, |tree| {
            let directory = root.join(tree);
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
                    for package in
                        fs::read_dir(&scope).map_err(|error| link_error(&scope, error))?
                    {
                        let package = package.map_err(|error| link_error(&scope, error))?;
                        snapshot.capture_entry(&root, package, mode)?;
                    }
                } else if kind.is_symlink() {
                    snapshot.capture_entry(&root, entry, mode)?;
                }
            }
            snapshot.capture_executables(&root, tree, mode)?;
            Ok(())
        })?;
        snapshot
            .links
            .sort_unstable_by(|left, right| left.at.cmp(&right.at));
        Ok(snapshot)
    }

    fn capture_entry(&mut self, root: &Path, entry: fs::DirEntry, mode: CaptureMode) -> Result<()> {
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
        let parent = relative.parent().expect("a package link has a parent");
        let resolved = crate::inherited_links::resolve_in_source(root, parent, &target);
        // Bun and pnpm's own package-store entries are installer state, not developer choices.
        if registry_store(root, &resolved) {
            return Ok(());
        }
        let canonical = match fs::canonicalize(&at) {
            Ok(target) => target,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) if unreadable(&error, mode) => {
                return self.push_unclassified(relative, target);
            }
            Err(error) => return Err(link_error(&at, error)),
        };
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
        if resolved.starts_with(root) && !canonical.starts_with(root) {
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
            binding: if canonical.starts_with(root) {
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

    fn capture_executables(&mut self, root: &Path, tree: &Path, mode: CaptureMode) -> Result<()> {
        let directory = root.join(tree).join(".bin");
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
            let canonical = match fs::canonicalize(&at) {
                Ok(canonical) => canonical,
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
        if self.is_empty() {
            return Ok(Vec::new());
        }
        let mut decisions = Vec::with_capacity(self.links.len());
        for link in &self.links {
            decisions.push(plan(root, &self.links, link, mode)?);
        }
        let mut findings = Vec::new();
        for (link, decision) in self.links.iter().zip(decisions) {
            match decision {
                Plan::Keep => {}
                Plan::Finding(message) => findings.push(message),
                Plan::Restore => restore_link(root, link, mode)?,
            }
        }
        crate::build_volume::discard::reap(root);
        Ok(findings)
    }
}

fn link_error(path: &Path, error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("cannot keep developer link at {}: {error}", path.display()),
        "repair the checkout's package links and directory permissions",
    )
}

/// Registry replacements are links into the checkout's own `.bun`/`.pnpm` or a known global cache.
fn registry_store(root: &Path, resolved: &Path) -> bool {
    (resolved.starts_with(root)
        && resolved
            .components()
            .any(|part| matches!(part.as_os_str().to_str(), Some(".bun" | ".pnpm"))))
        || global_store(resolved)
}

/// Bun's install cache and pnpm's store are the installer's global registries. Bun's global
/// registrations and other global directories are developer choices, not registry.
fn global_store(resolved: &Path) -> bool {
    let mut earlier = None;
    let mut previous = None;
    for name in resolved
        .components()
        .filter_map(|part| part.as_os_str().to_str())
    {
        if (earlier == Some(".bun") && previous == Some("install") && name == "cache")
            || (previous == Some("pnpm") && name == "store")
        {
            return true;
        }
        earlier = previous;
        previous = Some(name);
    }
    false
}

/// Job capture keeps a permission-denied target restorable instead of refusing admission.
fn unreadable(error: &io::Error, mode: CaptureMode) -> bool {
    mode == CaptureMode::Job && error.kind() == io::ErrorKind::PermissionDenied
}

/// Phase one reads every captured leaf before any mutation, so executable preservation
/// is decided from the installer's pre-restore state rather than restoration order.
fn plan(
    root: &Path,
    links: &[DeveloperLink],
    link: &DeveloperLink,
    mode: RestoreMode,
) -> Result<Plan> {
    let at = root.join(&link.at);
    let parent = at.parent().expect("a package link has a parent");
    crate::storage::verify_no_symlinks(root, parent).map_err(|error| link_error(parent, error))?;
    let mut buffer = [0u8; 4096];
    let current = current_leaf(parent, &at, &mut buffer)?;
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
            if preserved(root, links, link, current)? {
                Plan::Keep
            } else {
                Plan::Restore
            }
        }
    })
}

fn current_leaf<'a>(parent: &Path, at: &Path, buffer: &'a mut [u8]) -> Result<Current<'a>> {
    let name = leaf_name(at)?;
    let directory = match AnchoredDirectory::open(parent) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Current::ParentAbsent),
        Err(error) => return Err(link_error(parent, error)),
    };
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
    links: &[DeveloperLink],
    link: &DeveloperLink,
    current: &[u8],
) -> Result<bool> {
    match &link.binding {
        Binding::Owned(_) | Binding::Sibling(_) => {
            let at = root.join(&link.at);
            live_binding(root, &link.at, current).map_err(|error| link_error(&at, error))
        }
        Binding::Unclassified => Ok(false),
        Binding::Executable(owner) => {
            let parent = link.at.parent().expect("a package link has a parent");
            let resolved = crate::inherited_links::resolve_in_source(
                root,
                parent,
                Path::new(OsStr::from_bytes(current)),
            );
            if registry_store(root, &resolved) {
                return Ok(false);
            }
            let alias_at = root.join(&link.at);
            let canonical = match fs::canonicalize(&alias_at) {
                Ok(canonical) => canonical,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(link_error(&alias_at, error)),
            };
            if registry_store(root, &canonical)
                || !fs::metadata(&canonical)
                    .map_err(|error| link_error(&alias_at, error))?
                    .is_file()
            {
                return Ok(false);
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
            live_binding(root, owner, &owner_current).map_err(|error| link_error(&owner_at, error))
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

fn live_binding(root: &Path, relative: &Path, target: &[u8]) -> io::Result<bool> {
    let parent = relative.parent().expect("a package link has a parent");
    let resolved = crate::inherited_links::resolve_in_source(
        root,
        parent,
        Path::new(OsStr::from_bytes(target)),
    );
    if registry_store(root, &resolved) {
        return Ok(false);
    }
    let canonical = match fs::canonicalize(root.join(relative)) {
        Ok(canonical) => canonical,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(fs::metadata(&canonical)?.is_dir())
}

fn restore_link(root: &Path, link: &DeveloperLink, mode: RestoreMode) -> Result<()> {
    let at = root.join(&link.at);
    let parent = at.parent().expect("a package link has a parent");
    let directory = match mode {
        RestoreMode::CoveredInstall | RestoreMode::JobTerminalObservedInstall => {
            AnchoredDirectory::create(parent)
        }
        RestoreMode::JobTerminal => AnchoredDirectory::open(parent),
    }
    .map_err(|error| link_error(parent, error))?;
    let name = leaf_name(&at)?;
    let changed = directory
        .restore_symlink(&name, &link.target, || {
            let discard = AnchoredDirectory::create(
                &root.join(crate::build_volume::discard::DISCARD_DIRECTORY),
            )?;
            Ok((discard, discard_leaf(&link.at)?))
        })
        .map_err(|error| link_error(&at, error))?;
    if changed {
        eprintln!(
            "cowshed: restored developer link {} -> {}",
            link.at.display(),
            link.target.display()
        );
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

/// Multicall dispatch through the existing protected bootstrap-program link, before a runtime starts.
/// The real executable is the capability's recorded target, never searched again on PATH.
pub fn dispatch() -> io::Result<Option<ExitStatus>> {
    let mut args = std::env::args_os();
    let invoked = args.next().unwrap_or_default();
    let Some(manager) = Path::new(&invoked)
        .file_name()
        .filter(|name| is_package_manager(name))
    else {
        return Ok(None);
    };
    let tool = std::env::var_os(TOOL_BIN_ENV)
        .ok_or_else(|| io::Error::other("managed package manager has no tool route"))?;
    let mut real = PathBuf::from(tool);
    if !real.pop() {
        return Err(io::Error::other("the bootstrap bin has no parent"));
    }
    real.push("real-bin");
    real.push(manager);
    let arguments: Vec<OsString> = args.collect();
    if !installs(manager, &arguments) {
        return Err(Command::new(real).args(arguments).exec());
    }
    let root = std::env::var_os(ROOT_ENV)
        .ok_or_else(|| io::Error::other("managed package manager has no workspace root"))?;
    let root = Path::new(&root);
    let snapshot = DeveloperLinks::capture_job(root).map_err(io::Error::other)?;
    if let Some(path) = std::env::var_os(INSTALL_OBSERVED_ENV) {
        fs::write(path, b"observed\n")?;
    }
    let status = Command::new(real).args(arguments).status_locked();
    let restored = snapshot
        .restore(root, RestoreMode::CoveredInstall)
        .map_err(io::Error::other)
        .map(|findings| {
            for finding in &findings {
                eprintln!("cowshed: {finding}");
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
    use crate::fork_lock::Run;
    use std::os::unix::fs::symlink;

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
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
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
        let links = DeveloperLinks::capture(&root, CaptureMode::Mint).unwrap();
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
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
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
        let links = DeveloperLinks::capture_job(&root).unwrap();
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
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
                .unwrap()
                .success()
        );
        let sibling = scratch.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        let links = DeveloperLinks::capture_job(&root).unwrap();
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
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
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
        let links = DeveloperLinks::capture_job(&root).unwrap();
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
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
                .unwrap()
                .success()
        );
        let sibling = scratch.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        symlink(&sibling, root.join("node_modules/@scope/dev")).unwrap();
        let links = DeveloperLinks::capture_job(&root).unwrap();
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

    #[test]
    fn global_caches_are_registry_and_global_registrations_stay_developer_siblings() {
        let scratch = crate::temp_root::TempRoot::new("developer-links-global");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules/@scope")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
                .unwrap()
                .success()
        );
        let cache = scratch.join(".bun/install/cache/pkg");
        let registration = scratch.join(".bun/install/global/pkg");
        fs::create_dir_all(&cache).unwrap();
        fs::create_dir_all(&registration).unwrap();
        symlink(&cache, root.join("node_modules/@scope/cached")).unwrap();
        symlink(&registration, root.join("node_modules/@scope/registered")).unwrap();
        let links = DeveloperLinks::capture(&root, CaptureMode::Mint).unwrap();
        assert_eq!(links.links.len(), 1);
        assert_eq!(links.sibling_targets().collect::<Vec<_>>(), [&registration]);
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
        let links = DeveloperLinks::capture_job(&real_root).expect("admission snapshot");
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

    /// The sandbox proof for B3: a real `sandbox-exec` denial makes the target's canonicalize fail
    /// with the kernel's own error, not a substituted one. The parent sets up the fixture and runs
    /// this same test again inside the profile; the child asserts the job-side behaviour and writes
    /// proof beside the checkout. The `--exact` child never spawns another process.
    #[cfg(target_os = "macos")]
    #[test]
    fn job_snapshot_keeps_an_unreadable_target_unclassified_without_grants() {
        const FIXTURE_ENV: &str = "COWSHED_B3_FIXTURE_ROOT";
        const PROOF: &str = "b3-proof";
        if let Some(root) = std::env::var_os(FIXTURE_ENV) {
            let root = PathBuf::from(root);
            let links = DeveloperLinks::capture_job(&root)
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
                DeveloperLinks::capture(&root, CaptureMode::Mint).is_err(),
                "the mint refuses the same unreadable target"
            );
            fs::write(
                root.parent().unwrap().join(PROOF),
                "unclassified-restored\n",
            )
            .unwrap();
            return;
        }
        let scratch = crate::temp_root::TempRoot::new("developer-links-b3");
        let root = scratch.join("checkout");
        fs::create_dir_all(root.join("node_modules")).unwrap();
        fs::write(root.join("package.json"), "{}\n").unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&root)
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
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
            .status_locked()
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
                .status_locked()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["add", "package.json"])
                .current_dir(&root)
                .status_locked()
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
        let links = DeveloperLinks::capture_job(&root).unwrap();
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
}
