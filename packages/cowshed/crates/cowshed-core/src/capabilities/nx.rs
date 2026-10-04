//! Nx (`nx.json`): the daemon a sandboxed client finds, and the state it indexes, are the
//! sandbox's own.
//!
//! Nx ignores `XDG_RUNTIME_DIR` and otherwise falls back to a world-shared directory or a private
//! HOME path longer than Unix sockets permit, so its socket directory is a real `nx` leaf below
//! the short runtime alias (its `O_NOFOLLOW` admission refuses a symlinked leaf). The host
//! prepares that leaf and the private state directories before a child runs.
//!
//! A client connects only to the socket named by `d/server-process.json` in Nx's workspace-data
//! directory, and a client that cannot reach that socket starts a daemon of its own, which
//! overwrites the record and so retires the daemon it replaced. Left in the checkout, the record
//! is shared with every host shell there: a sandboxed client cannot reach a host daemon's socket,
//! replaces it, and host clients then send their whole environment to a daemon inside the
//! sandbox. The workspace-data directory is therefore the sandbox's, in its private environment,
//! and so is the cache: once either is configured Nx keeps its task database in the
//! workspace-data directory, and that database indexes exactly one cache directory. Whether the
//! daemon runs stays Nx's own decision, so a caller's `NX_DAEMON` never reaches the child.
//!
//! A workspace is cloned from an image, so a daemon's rendezvous record arrives byte-identical
//! and names the daemon still serving the source tree. Both the host shell's record and the
//! sandbox's are discarded at mint; the warm project graph and hashes beside them are kept.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use super::{
    CapabilityContribution, CapabilityId, DetectionContext, DetectionScope, Detector, EnvAction,
};
use crate::{CowshedError, Result};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Nx,
    all: &["nx.json"],
    any: &[],
    scope: DetectionScope::Project,
    host_cache_homes: &[],
    contribute,
};

/// The daemon's rendezvous directory inside a workspace-data directory.
const DAEMON_RECORD: &str = "workspace-data/d";
/// Nx's default workspace-data parent, relative to its project root.
const HOST_STATE: &str = ".nx";

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let state = context.environment_root.join("cache/nx");
    let mut contribution = CapabilityContribution {
        env: [
            ("NX_SOCKET_DIR", own(context.runtime_dir.join("nx"))),
            (
                "NX_WORKSPACE_DATA_DIRECTORY",
                own(state.join("workspace-data")),
            ),
            ("NX_CACHE_DIRECTORY", own(state.join("cache"))),
            ("NX_WORKSPACE_ROOT_PATH", own(context.project_root.into())),
            ("NX_DAEMON", EnvAction::Unset),
        ]
        .into(),
        ..CapabilityContribution::default()
    };
    contribution.daemon_isolation.directories.extend([
        state.join("workspace-data"),
        state.join("cache"),
        context.environment_root.join("run/nx"),
    ]);
    let project = workspace_relative(context.workspace_root, context.project_root)?;
    let discard = &mut contribution.daemon_isolation.discard_at_mint;
    discard.push(project.join(HOST_STATE).join(DAEMON_RECORD));
    // The sandbox's record travels in the image only when the private environment does: a
    // read-only job's lives in its exec temp dir and is never cloned.
    if let Ok(private) = context
        .environment_root
        .strip_prefix(context.workspace_root)
    {
        discard.push(private.join("cache/nx").join(DAEMON_RECORD));
    }
    Ok(contribution)
}

fn own(path: PathBuf) -> EnvAction {
    EnvAction::Own(OsString::from(path))
}

fn workspace_relative(workspace: &Path, project: &Path) -> Result<PathBuf> {
    project
        .strip_prefix(workspace)
        .map(Path::to_path_buf)
        .map_err(|_| {
            CowshedError::integrity(
                format!(
                    "Nx project {} is outside workspace {}",
                    project.display(),
                    workspace.display()
                ),
                "use a workspace-relative capability directory",
            )
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::capabilities::test_support::{Fixture, assert_switch};
    use crate::capabilities::{CapabilityOverride, DaemonIsolation, detect};

    #[test]
    fn nx_json_switches_nx_on_and_off() {
        assert_switch(&DETECTOR, &["nx.json"]);
    }

    #[test]
    fn nx_owns_its_daemon_rendezvous_and_state_inside_the_sandbox() {
        let fixture = Fixture::new();
        fixture.files(&["nx.json"]);
        let contribution = DETECTOR
            .detect(&fixture.context())
            .expect("detection")
            .expect("nx detected");
        let state = fixture.environment.join("cache/nx");
        assert_eq!(
            contribution,
            CapabilityContribution {
                env: BTreeMap::from([
                    ("NX_CACHE_DIRECTORY", own(state.join("cache"))),
                    ("NX_DAEMON", EnvAction::Unset),
                    ("NX_SOCKET_DIR", own(fixture.runtime.join("nx"))),
                    (
                        "NX_WORKSPACE_DATA_DIRECTORY",
                        own(state.join("workspace-data"))
                    ),
                    ("NX_WORKSPACE_ROOT_PATH", own(fixture.root.clone())),
                ]),
                daemon_isolation: DaemonIsolation {
                    directories: vec![
                        state.join("workspace-data"),
                        state.join("cache"),
                        fixture.environment.join("run/nx"),
                    ],
                    discard_at_mint: vec![
                        PathBuf::from(".nx/workspace-data/d"),
                        PathBuf::from(".cowshed/cache/nx/workspace-data/d"),
                    ],
                },
                ..CapabilityContribution::default()
            }
        );
    }

    /// An override directory roots Nx in that project: its workspace root and its host-shell
    /// record move there, while the sandbox's private state stays the workspace's. An override
    /// never enables a project without `nx.json`.
    #[test]
    fn an_override_directory_roots_nx_in_that_project_and_never_enables_it() {
        let fixture = Fixture::new();
        fixture.files(&["apps/web/nx.json", "apps/api/package.json"]);
        let overridden = |directory: &str| {
            BTreeMap::from([(
                CapabilityId::Nx,
                CapabilityOverride {
                    disabled: false,
                    directory: Some(PathBuf::from(directory)),
                },
            )])
        };

        let web = detect(&fixture.context(), &overridden("apps/web")).expect("detection");
        assert!(web.active.contains(&CapabilityId::Nx));
        assert_eq!(
            web.contribution.env.get("NX_WORKSPACE_ROOT_PATH"),
            Some(&own(fixture.root.join("apps/web")))
        );
        assert_eq!(
            web.contribution.daemon_isolation.discard_at_mint,
            vec![
                PathBuf::from(".cowshed/cache/nx/workspace-data/d"),
                PathBuf::from("apps/web/.nx/workspace-data/d"),
            ]
        );

        let api = detect(&fixture.context(), &overridden("apps/api")).expect("detection");
        assert!(!api.active.contains(&CapabilityId::Nx));
        assert!(!api.contribution.env.contains_key("NX_SOCKET_DIR"));

        let root = detect(&fixture.context(), &BTreeMap::new()).expect("detection");
        assert!(
            !root.active.contains(&CapabilityId::Nx),
            "nx.json below the root is not the root's convention"
        );
    }
}
