//! Host tool caches a capability shares with every checkout on the host.
//!
//! A shared tool home is reached through ONE literal path: the host's own default, which host
//! setup links into the caches volume. Cargo and Bun record where their cache lives, so no other
//! spelling of the same bytes shares it. Cargo fingerprints a registry or git dependency by the
//! absolute path of its source under `$CARGO_HOME`: a `$CARGO_HOME` at any other path — a
//! sandbox's private HOME, even one whose `registry` links to the same bytes — dirties every
//! dependency a clone's copied `target/` holds. Bun's isolated linker writes
//! `node_modules/.bun/<package>` as absolute symlinks into its install cache, so main's
//! `node_modules` and every clone's resolve only if each names the same cache path. A sandboxed
//! child of a project that uses the tool is therefore pointed at the host path once host setup
//! has relocated the tool's caches; until then it keeps the tool's private default under the
//! sandbox HOME, and `cowshed doctor` says why.

use std::path::{Path, PathBuf};

/// One host cache path and the directory on the caches volume it belongs in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCache {
    pub host: PathBuf,
    pub shared: PathBuf,
}

impl HostCache {
    /// Whether the host path already resolves to the shared directory.
    pub fn is_shared(&self) -> bool {
        matches!(
            (
                std::fs::canonicalize(&self.host),
                std::fs::canonicalize(&self.shared),
            ),
            (Ok(host), Ok(shared)) if host == shared
        )
    }
}

/// A tool cache every checkout on the host reaches through the host's own default path.
#[derive(Debug, Eq, PartialEq)]
pub struct SharedToolHome {
    /// The variable that points a sandboxed child at the host path, for a tool that reads one; a
    /// cache reached only through the host link (Nix's client state) names none.
    pub variable: Option<&'static str>,
    /// The tool's own default directory under HOME: the host uses it unconfigured.
    pub home: &'static str,
    pub layout: SharedLayout,
    /// Checkouts hold symlinks into the cache (Bun's isolated linker), so while the host path is
    /// still a private directory it stays readable to every sandbox: a cloned `node_modules`
    /// would otherwise resolve to EPERM inside the sandbox while the same tree works on the host.
    pub linked_from_checkouts: bool,
}

/// Where a [`SharedToolHome`]'s bytes live on the caches volume.
#[derive(Debug, Eq, PartialEq)]
pub enum SharedLayout {
    /// The host path itself links to this directory under the caches root.
    Whole(&'static str),
    /// The host path stays a host directory holding configuration, credentials or binaries that
    /// never leave it. Only the `(child, directory under the caches root)` links inside it are
    /// shared, and `state_files` are the only files the tool writes at its root beside them.
    Split {
        links: &'static [(&'static str, &'static str)],
        state_files: &'static [&'static str],
    },
}

impl SharedToolHome {
    /// `<home>/<self.home>`: the literal path the host and every sandbox use.
    pub fn host_path(&self, home: &Path) -> PathBuf {
        home.join(self.home)
    }

    /// Each host link with the shared directory it must resolve to.
    pub fn links(&self, home: &Path, caches: &Path) -> Vec<HostCache> {
        let host = self.host_path(home);
        match &self.layout {
            SharedLayout::Whole(shared) => vec![HostCache {
                host,
                shared: caches.join(shared),
            }],
            SharedLayout::Split { links, .. } => links
                .iter()
                .map(|(child, shared)| HostCache {
                    host: host.join(child),
                    shared: caches.join(shared),
                })
                .collect(),
        }
    }
}
