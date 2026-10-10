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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreMode {
    CoveredInstall,
    JobTerminal,
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
    pub fn capture(root: &Path) -> Result<Self> {
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
                        snapshot.capture_entry(&root, package)?;
                    }
                } else if kind.is_symlink() {
                    snapshot.capture_entry(&root, entry)?;
                }
            }
            snapshot.capture_executables(&root, tree)?;
            Ok(())
        })?;
        snapshot
            .links
            .sort_unstable_by(|left, right| left.at.cmp(&right.at));
        Ok(snapshot)
    }

    fn capture_entry(&mut self, root: &Path, entry: fs::DirEntry) -> Result<()> {
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
            Err(error) => return Err(link_error(&at, error)),
        };
        if !fs::metadata(&canonical)
            .map_err(|error| link_error(&at, error))?
            .is_dir()
        {
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

    fn capture_executables(&mut self, root: &Path, tree: &Path) -> Result<()> {
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
                Err(error) => return Err(link_error(&at, error)),
            };
            let owner = self.links.iter().find_map(|link| match &link.binding {
                Binding::Owned(product) | Binding::Sibling(product)
                    if canonical.starts_with(product) =>
                {
                    Some(link.at.clone())
                }
                Binding::Owned(_) | Binding::Sibling(_) | Binding::Executable(_) => None,
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
            Binding::Owned(_) | Binding::Executable(_) => None,
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
            decisions.push(plan(root, link, mode)?);
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

fn registry_store(root: &Path, resolved: &Path) -> bool {
    resolved.starts_with(root)
        && resolved
            .components()
            .any(|part| matches!(part.as_os_str().to_str(), Some(".bun" | ".pnpm")))
}

/// Phase one reads every captured leaf before any mutation, so executable preservation
/// is decided from the installer's pre-restore state rather than restoration order.
fn plan(root: &Path, link: &DeveloperLink, mode: RestoreMode) -> Result<Plan> {
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
        (RestoreMode::JobTerminal, Current::Payload) => Plan::Restore,
        (RestoreMode::JobTerminal, Current::Link(current)) => {
            if preserved(root, link, current)? {
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

/// A different link is a live developer binding only when it still resolves outside the
/// installer's registry and its canonical target is a directory. Executable aliases defer to their owner.
fn preserved(root: &Path, link: &DeveloperLink, current: &[u8]) -> Result<bool> {
    match &link.binding {
        Binding::Executable(owner) => {
            let at = root.join(owner);
            let owner_target = match fs::read_link(&at) {
                Ok(target) => target,
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound
                        || error.raw_os_error() == Some(libc::EINVAL) =>
                {
                    return Ok(false);
                }
                Err(error) => return Err(link_error(&at, error)),
            };
            live_binding(root, owner, owner_target.as_os_str().as_bytes())
                .map_err(|error| link_error(&at, error))
        }
        Binding::Owned(_) | Binding::Sibling(_) => {
            let at = root.join(&link.at);
            live_binding(root, &link.at, current).map_err(|error| link_error(&at, error))
        }
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
        RestoreMode::CoveredInstall => AnchoredDirectory::create(parent),
        RestoreMode::JobTerminal => AnchoredDirectory::open(parent),
    }
    .map_err(|error| link_error(parent, error))?;
    let name = leaf_name(&at)?;
    let changed = directory
        .restore_symlink(&name, &link.target, || {
            let discard = AnchoredDirectory::create(&root.join(".cowshed").join("discard"))?;
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
    let mut name = relative.to_string_lossy().replace('/', "%");
    name.push_str(&format!(
        "-{}-{}",
        std::process::id(),
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    CString::new(name).map_err(io::Error::other)
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
    let root = std::env::var_os(ROOT_ENV)
        .ok_or_else(|| io::Error::other("managed package manager has no workspace root"))?;
    let root = Path::new(&root);
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
    let snapshot = DeveloperLinks::capture(root).map_err(io::Error::other)?;
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
        let links = DeveloperLinks::capture(&root).unwrap();
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
        let links = DeveloperLinks::capture(&root).unwrap();
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
        let links = DeveloperLinks::capture(&root).unwrap();
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
    fn manager_coverage_is_exact_and_non_install_commands_are_not_snapshotted() {
        let args = |verb: &str| vec![OsString::from(verb)];
        assert!(installs(OsStr::new("bun"), &args("install")));
        assert!(installs(OsStr::new("npm"), &args("ci")));
        assert!(installs(OsStr::new("pnpm"), &args("add")));
        for verb in ["run", "test", "link", "--version"] {
            assert!(!installs(OsStr::new("bun"), &args(verb)));
        }
    }
}
