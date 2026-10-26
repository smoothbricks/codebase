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
    installed_nx_read_paths(nx, None)
}

/// One installed-package closure traversal; a genuine sibling also needs its private namespaces.
pub(crate) fn installed_nx_read_paths(nx: &Path, owned_checkout: Option<&Path>) -> Vec<PathBuf> {
    // The caller names the exact owned developer checkout, never a shared ancestor. A consumer's
    // registry Nx sits in its enclosing .bun namespace. Both use this one bounded traversal.
    let local = owned_checkout.map(Path::to_path_buf).unwrap_or_else(|| {
        nx.ancestors()
            .find(|ancestor| ancestor.file_name() == Some(std::ffi::OsStr::new(".bun")))
            .expect("consumer Nx must sit in its checkout's Bun install namespace")
            .to_path_buf()
    });
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
    let mut bindings = std::collections::BTreeSet::new();
    let mut binaries = Vec::new();
    let mut pending = if owned_checkout.is_some() {
        vec![nx.join("node_modules"), local.join("node_modules")]
    } else {
        vec![nx.parent().expect("Nx package namespace").to_path_buf()]
    };
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
                let name = entry.file_name();
                if name.as_encoded_bytes().starts_with(b".") && name != ".bin" {
                    continue; // Bun installer bookkeeping is not a logical package binding.
                }
                let target = entry
                    .path()
                    .canonicalize()
                    .expect("installed namespace entry resolves");
                if group.file_name() == Some(std::ffi::OsStr::new(".bin")) {
                    binaries.push(target);
                    continue;
                }
                if (name.to_string_lossy().starts_with('@') || name == ".bin")
                    && entry.file_type().expect("namespace entry kind").is_dir()
                {
                    groups.push(entry.path());
                    continue;
                }
                // Authority coverage is not traversal coverage: a local scoped namespace can
                // still bind a native package in the shared store. Deduplicate exact products.
                if reads.contains(&target) {
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
                // Keep the reachable lexical binding too: a private .bun alias is part of
                // resolving the shared package, even though its physical bytes lie in the store.
                if entry
                    .file_type()
                    .expect("namespace entry kind")
                    .is_symlink()
                {
                    let text = fs::read_link(entry.path()).expect("dependency link text");
                    let binding = cowshed_core::inherited_links::resolve_in_source(
                        &group,
                        Path::new(""),
                        &text,
                    );
                    if binding.starts_with(&local) {
                        bindings.insert(binding.parent().expect("dependency namespace").to_owned());
                    }
                }
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
    reads.extend(bindings);
    // A genuine sibling is a workspace root, and Node reads its manifest when it resolves from that
    // root (`trySelf`, the nearest parent package.json). Node 24.21.0 and 26.11.1 refuse an
    // unreadable one with ERR_INVALID_PACKAGE_CONFIG; 26.7.0 skipped it, which hid the missing read.
    if let Some(checkout) = owned_checkout {
        reads.insert(checkout.join("package.json"));
    }
    // Local packages already belong to the workspace's read authority. External grants must
    // never intersect that protected workspace; only shared cache payloads need added reads.
    reads
        .into_iter()
        .filter(|path| owned_checkout.is_some() || !path.starts_with(&local))
        .collect()
}
