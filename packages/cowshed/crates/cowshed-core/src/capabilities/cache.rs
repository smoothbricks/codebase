//! Host tool caches a capability shares with every checkout on the host.
//!
//! A shared tool home is reached through ONE literal path: the tool's own default in the host
//! HOME, the directory the host's own tools already use unconfigured. Cargo and Bun record where
//! their cache lives, so no other spelling of the same bytes shares it. Cargo fingerprints a
//! registry or git dependency by the absolute path of its source under `$CARGO_HOME`: a
//! `$CARGO_HOME` at any other path — a sandbox's private HOME, even one whose `registry` links to
//! the same bytes — dirties every dependency a clone's copied `target/` holds. Bun's isolated
//! linker writes `node_modules/.bun/<package>` as absolute symlinks into its install cache, so
//! main's `node_modules` and every clone's resolve only if each names the same cache path. A
//! sandboxed child of a project that uses the tool is therefore pointed at the host path, and the
//! HOME read deny is carved back for exactly the tool's cache directories (03_caches.md).

use std::path::{Path, PathBuf};

/// A tool cache every checkout on the host reaches through the host's own default path.
#[derive(Debug, Eq, PartialEq)]
pub struct SharedToolHome {
    /// The variable that points a sandboxed child at the host path, for a tool that reads one; a
    /// cache reached only through a private link (Nix's client state, Gradle's caches) names none.
    pub variable: Option<&'static str>,
    /// The tool's own default directory under HOME: the host uses it unconfigured.
    pub home: &'static str,
    pub layout: SharedLayout,
}

/// Which parts of a [`SharedToolHome`] a sandbox may write.
#[derive(Debug, Eq, PartialEq)]
pub enum SharedLayout {
    /// The tool's directory is itself the cache, shared read-write whole.
    Whole,
    /// The tool's directory stays host-owned, holding configuration, credentials or binaries that
    /// never become writable. Only `caches` beneath it are shared read-write, and `state_files`
    /// are the only files the tool writes at its root beside them.
    Split {
        caches: &'static [&'static str],
        state_files: &'static [&'static str],
    },
}

impl SharedToolHome {
    /// `<home>/<self.home>`: the literal path the host and every sandbox use.
    pub fn host_path(&self, home: &Path) -> PathBuf {
        home.join(self.home)
    }

    /// Every cache directory the tool writes, at its host path.
    pub fn cache_directories(&self, home: &Path) -> Vec<PathBuf> {
        let host = self.host_path(home);
        match &self.layout {
            SharedLayout::Whole => vec![host],
            SharedLayout::Split { caches, .. } => {
                caches.iter().map(|cache| host.join(cache)).collect()
            }
        }
    }
}
