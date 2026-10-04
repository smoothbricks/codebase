//! The host's own read-at-build caches, relocated onto the caches volume (03_caches.md,
//! "Host-level relocation").
//!
//! Every sandbox of a project that uses a tool shares that tool's caches through the caches
//! volume; relocating the host's copies makes the host one more sharer instead of a separate
//! island. For a shared tool home it is more than disk: a sandbox uses the host's literal tool path
//! only once every one of the tool's caches is the shared one, and that literal path is what
//! keeps a clone's copied `target/` fresh and its `node_modules` links resolving.
//!
//! The caches are the detectors' own descriptors ([`crate::capabilities::Detector`]). Relocating
//! one is host preparation only: it never enables a capability in any repository.

use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;

use crate::capabilities::DETECTORS;
use crate::capabilities::cache::HostCache;
use crate::fork_lock::Run as _;

/// Every host cache setup relocates: each detector's tool homes' links, once each.
pub fn host_caches(home: &Path, caches: &Path) -> impl Iterator<Item = HostCache> {
    let mut links: Vec<HostCache> = Vec::new();
    for tool in DETECTORS
        .into_iter()
        .flat_map(|detector| detector.host_cache_homes)
    {
        for link in tool.links(home, caches) {
            if !links.contains(&link) {
                links.push(link);
            }
        }
    }
    links.into_iter()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostCacheState {
    /// The host path already resolves to the shared directory.
    Shared,
    /// Nothing at the host path yet; relocation links it.
    Absent,
    /// A real host directory, and a shared one that is missing or empty; relocation moves it.
    Movable,
    /// Relocating would overwrite or orphan something; the reason names what and what to do.
    Conflict(String),
}

pub fn inspect(cache: &HostCache) -> HostCacheState {
    let metadata = match fs::symlink_metadata(&cache.host) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return HostCacheState::Absent,
        Err(error) => return HostCacheState::Conflict(format!("cannot inspect it: {error}")),
    };
    if metadata.file_type().is_symlink() {
        if cache.is_shared() {
            return HostCacheState::Shared;
        }
        return HostCacheState::Conflict(match fs::read_link(&cache.host) {
            // A declarative module owns a link into the store; cowshed never rewrites it.
            Ok(target) if target.starts_with("/nix/store") => format!(
                "a declarative module links it to {}; point the module at {}",
                target.display(),
                cache.shared.display()
            ),
            Ok(target) => format!(
                "it links to {}; remove the link to let setup relink it",
                target.display()
            ),
            Err(error) => format!("cannot read the link: {error}"),
        });
    }
    if !metadata.is_dir() {
        return HostCacheState::Conflict("it is not a directory".to_owned());
    }
    match fs::read_dir(&cache.shared).map(|mut entries| entries.next().is_none()) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => HostCacheState::Movable,
        Ok(true) => HostCacheState::Movable,
        Ok(false) => HostCacheState::Conflict(format!(
            "{} already holds a cache too; keep one of the two, delete the other, and rerun",
            cache.shared.display()
        )),
        Err(error) => HostCacheState::Conflict(format!(
            "cannot inspect {}: {error}",
            cache.shared.display()
        )),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Relocation {
    AlreadyShared,
    Linked,
    Moved,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCacheRelocation {
    pub cache: HostCache,
    /// `Err` carries the reason the host path was left exactly as it was.
    pub outcome: Result<Relocation, String>,
}

/// Make the host path a link to the shared directory, moving an existing cache there first.
pub fn relocate(cache: HostCache) -> HostCacheRelocation {
    let outcome = match inspect(&cache) {
        HostCacheState::Shared => Ok(Relocation::AlreadyShared),
        HostCacheState::Absent => link(&cache)
            .map(|()| Relocation::Linked)
            .map_err(|error| format!("cannot link it: {error}")),
        HostCacheState::Movable => move_directory(&cache.host, &cache.shared)
            .and_then(|()| link(&cache))
            .map(|()| Relocation::Moved)
            .map_err(|error| format!("cannot move it: {error}")),
        HostCacheState::Conflict(reason) => Err(reason),
    };
    HostCacheRelocation { cache, outcome }
}

fn link(cache: &HostCache) -> io::Result<()> {
    fs::create_dir_all(&cache.shared)?;
    if let Some(parent) = cache.host.parent() {
        fs::create_dir_all(parent)?;
    }
    std::os::unix::fs::symlink(&cache.shared, &cache.host)
}

/// Move a directory onto the caches volume.
///
/// The caches volume is its own filesystem, so the move is normally a copy: into a staging
/// directory beside the destination, published with one rename, and only then is the original
/// removed. A run that dies part way leaves the original untouched and either nothing or a whole
/// copy at the destination, never half of one there.
fn move_directory(from: &Path, to: &Path) -> io::Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    // An empty destination stands in the way of the rename and of the copy's publication alike.
    match fs::remove_dir(to) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
        _ => {}
    }
    match fs::rename(from, to) {
        Ok(()) => return Ok(()),
        Err(error) if error.raw_os_error() != Some(libc::EXDEV) => return Err(error),
        Err(_) => {}
    }
    let leaf = to
        .file_name()
        .ok_or_else(|| io::Error::other("the shared directory has no leaf name"))?;
    let mut staging_name = std::ffi::OsString::from(".");
    staging_name.push(leaf);
    staging_name.push(".cowshed-relocating");
    let staging = to.with_file_name(staging_name);
    match fs::remove_dir_all(&staging) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
        _ => {}
    }
    if let Err(error) = copy_tree(from, &staging) {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    fs::rename(&staging, to)?;
    fs::remove_dir_all(from)
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

/// Copy the tree at `from` to a new directory `to` with [`COPY_TREE`].
fn copy_tree(from: &Path, to: &Path) -> io::Result<()> {
    let (program, options) = COPY_TREE;
    let status = Command::new(program)
        .args(options)
        .arg(from)
        .arg(to)
        .status_locked()?;
    if !status.success() {
        return Err(io::Error::other(format!("{program} exited with {status}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn scratch(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("cowshed-host-caches-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn cache(root: &Path) -> HostCache {
        HostCache {
            host: root.join("home/.cargo/registry"),
            shared: root.join("caches/cargo/registry"),
        }
    }

    /// One setup run relocates every detector's tool-home caches, after which each of those
    /// tool homes is shared; a populated cache arrives on the caches volume intact.
    #[test]
    fn relocating_every_host_cache_shares_every_tool_home() {
        let root = scratch("every");
        let home = root.join("home");
        let caches = root.join("caches");
        let package = home.join(".bun/install/cache/links/widget@1.0.0");
        fs::create_dir_all(&package).unwrap();
        fs::write(package.join("package.json"), b"{}").unwrap();

        for cache in host_caches(&home, &caches) {
            let relocation = relocate(cache);
            assert!(relocation.outcome.is_ok(), "{relocation:?}");
        }
        for detector in DETECTORS {
            for tool in detector.host_cache_homes {
                assert!(
                    tool.links(&home, &caches).iter().all(HostCache::is_shared),
                    "~/{} is not shared after setup",
                    tool.home
                );
            }
        }
        assert_eq!(
            fs::read(caches.join("bun/install/cache/links/widget@1.0.0/package.json")).unwrap(),
            b"{}"
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// The relocation copy is the same cache, hard links included: Cargo's git checkouts share
    /// inodes with its databases, and a copy that splits them grows the cache (measured on a
    /// cargo git cache: 8.5G before a link-splitting copy, 21G after).
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_the_relocation_copy_keeps_hard_links() {
        use std::os::unix::fs::MetadataExt;

        let root = scratch("hard-links");
        let from = root.join("cache");
        fs::create_dir_all(from.join("db")).unwrap();
        fs::create_dir_all(from.join("checkouts")).unwrap();
        fs::write(from.join("db/object"), b"bytes").unwrap();
        fs::hard_link(from.join("db/object"), from.join("checkouts/object")).unwrap();

        let to = root.join("copy");
        copy_tree(&from, &to).unwrap();
        let original = fs::metadata(to.join("db/object")).unwrap();
        let alias = fs::metadata(to.join("checkouts/object")).unwrap();
        assert_eq!(
            (original.ino(), original.nlink()),
            (alias.ino(), 2),
            "the copy split one file into two"
        );
        assert_eq!(fs::read(to.join("checkouts/object")).unwrap(), b"bytes");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_real_cache_moves_with_its_links_and_the_host_path_links_back() {
        let root = scratch("move");
        let cache = cache(&root);
        fs::create_dir_all(cache.host.join("src/crate-1.0.0")).unwrap();
        fs::write(cache.host.join("src/crate-1.0.0/lib.rs"), b"fn f() {}").unwrap();
        std::os::unix::fs::symlink("crate-1.0.0", cache.host.join("src/alias")).unwrap();
        // An empty shared directory is what a fresh caches volume has; it does not block the move.
        fs::create_dir_all(&cache.shared).unwrap();

        let relocation = relocate(cache.clone());
        assert_eq!(relocation.outcome, Ok(Relocation::Moved));
        assert_eq!(fs::read_link(&cache.host).unwrap(), cache.shared);
        assert_eq!(
            fs::read(cache.shared.join("src/crate-1.0.0/lib.rs")).unwrap(),
            b"fn f() {}"
        );
        assert_eq!(
            fs::read_link(cache.shared.join("src/alias")).unwrap(),
            Path::new("crate-1.0.0")
        );
        // A second run finds the relocation done and changes nothing.
        assert_eq!(
            relocate(cache.clone()).outcome,
            Ok(Relocation::AlreadyShared)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_absent_host_cache_is_linked_to_a_created_shared_directory() {
        let root = scratch("absent");
        let cache = cache(&root);
        assert_eq!(relocate(cache.clone()).outcome, Ok(Relocation::Linked));
        assert!(cache.shared.is_dir());
        assert_eq!(fs::read_link(&cache.host).unwrap(), cache.shared);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn two_populated_caches_are_a_conflict_that_touches_neither() {
        let root = scratch("conflict");
        let cache = cache(&root);
        fs::create_dir_all(cache.host.join("index")).unwrap();
        fs::create_dir_all(cache.shared.join("cache")).unwrap();

        assert!(matches!(inspect(&cache), HostCacheState::Conflict(_)));
        assert!(relocate(cache.clone()).outcome.is_err());
        assert!(cache.host.join("index").is_dir());
        assert!(fs::read_link(&cache.host).is_err());
        assert!(cache.shared.join("cache").is_dir());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_link_elsewhere_is_a_conflict_and_is_never_rewritten() {
        let root = scratch("elsewhere");
        let cache = cache(&root);
        fs::create_dir_all(cache.host.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(root.join("elsewhere"), &cache.host).unwrap();

        assert!(relocate(cache.clone()).outcome.is_err());
        assert_eq!(fs::read_link(&cache.host).unwrap(), root.join("elsewhere"));
        fs::remove_dir_all(root).unwrap();
    }
}
