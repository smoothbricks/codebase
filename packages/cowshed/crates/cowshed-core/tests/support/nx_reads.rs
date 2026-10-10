//! Read grants for host-controller probes that run the checkout's pinned Bun-installed Nx.

use std::fs;
use std::path::{Path, PathBuf};

#[derive(serde::Deserialize)]
struct InstalledPackageIdentity {
    name: String,
    version: String,
}

/// Read only the installed package-private namespaces reachable from Nx, not a hoist or home.
/// Bun keeps patched packages in this checkout's `.bun` tree and shares eligible dependencies
/// through its global store; neither layout requires a generated release closure.
pub(crate) fn nx_read_paths(nx: &Path) -> Vec<PathBuf> {
    // The namespace is the `.bun` directory above the checkout's own canonical Nx.
    let local = nx
        .ancestors()
        .find(|ancestor| ancestor.file_name() == Some(std::ffi::OsStr::new(".bun")))
        .expect("Nx must sit in its checkout's Bun install namespace")
        .to_path_buf();
    let store = std::env::var_os("BUN_INSTALL_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").expect("host HOME")).join(".bun/install/cache")
        })
        .canonicalize()
        .expect("the host's Bun install cache");
    let installed = |path: &Path| {
        (path.starts_with(&local) && path != local) || (path.starts_with(&store) && path != store)
    };
    assert!(
        installed(nx),
        "Nx must be the checkout's pinned Bun install"
    );
    let mut reads = std::collections::BTreeSet::from([nx.to_path_buf()]);
    let mut namespaces = std::collections::BTreeSet::new();
    let mut binaries = Vec::new();
    let mut pending = vec![nx.parent().expect("Nx package namespace").to_path_buf()];
    while let Some(namespace) = pending.pop() {
        assert!(
            installed(&namespace),
            "dependency namespace leaves the Bun install"
        );
        if !namespaces.insert(namespace.clone()) {
            continue;
        }
        let mut groups = vec![namespace];
        while let Some(group) = groups.pop() {
            for entry in fs::read_dir(&group).expect("installed dependency namespace") {
                let entry = entry.expect("dependency namespace entry");
                let target = entry
                    .path()
                    .canonicalize()
                    .expect("installed namespace entry resolves");
                if group.file_name() == Some(std::ffi::OsStr::new(".bin")) {
                    binaries.push(target);
                    continue;
                }
                if reads.iter().any(|package| target.starts_with(package)) {
                    continue;
                }
                let name = entry.file_name();
                if (name.to_string_lossy().starts_with('@') || name == ".bin")
                    && entry.file_type().expect("namespace entry kind").is_dir()
                {
                    groups.push(entry.path());
                    continue;
                }
                assert!(
                    installed(&target),
                    "dependency {} leaves the Bun install: {}",
                    entry.path().display(),
                    target.display()
                );
                let identity: InstalledPackageIdentity = serde_json::from_slice(
                    &fs::read(target.join("package.json")).expect("installed package manifest"),
                )
                .expect("installed logical package identity");
                assert!(
                    entry.path().ends_with(Path::new(&identity.name))
                        && !identity.version.is_empty(),
                    "dependency binding does not match its installed package identity"
                );
                reads.insert(target.clone());
                let mut dependency_namespace = target;
                for _ in Path::new(&identity.name).components() {
                    assert!(dependency_namespace.pop());
                }
                assert_eq!(
                    dependency_namespace.file_name(),
                    Some(std::ffi::OsStr::new("node_modules"))
                );
                pending.push(dependency_namespace);
            }
        }
    }
    assert!(
        binaries
            .iter()
            .all(|binary| reads.iter().any(|package| binary.starts_with(package))),
        "an installed binary binding leaves the package closure"
    );
    reads.extend(namespaces);
    // Local packages already belong to the workspace's read authority. External grants must
    // never intersect that protected workspace; only shared cache payloads need added reads.
    reads
        .into_iter()
        .filter(|path| !path.starts_with(&local))
        .collect()
}
