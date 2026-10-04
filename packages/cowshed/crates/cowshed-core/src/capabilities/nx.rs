//! Nx (`nx.json`): one task cache, one task database and one daemon per checkout, shared by every
//! boundary that runs Nx there (04_sandbox.md, "One Nx state per checkout").
//!
//! Nx 23 keeps its task database in the workspace-data directory, and that database indexes
//! exactly one cache directory: a run restores only the artifacts its own database has rows for.
//! The daemon's rendezvous record, `d/server-process.json`, lives in the same workspace-data
//! directory, and setting any of `NX_WORKSPACE_DATA_DIRECTORY`, `NX_CACHE_DIRECTORY` or
//! `NX_PROJECT_GRAPH_CACHE_DIRECTORY` moves the record, the database and the cache together.
//! A sandbox that names its own directories therefore owns a second cache: a warm step that fills
//! one leaves a gate reading the other cold, and every clone inherits both half-warm. So a
//! read-write job names the checkout's own `.nx` — the directories a host shell's Nx uses — and
//! every boundary of the checkout shares one record, one daemon, one database and one cache.
//!
//! Sharing the record works only when every boundary can reach the daemon's socket, so the socket
//! directory is a real `nx` leaf below the short runtime alias, inside the checkout's tree, which
//! the sandbox may bind and connect (04_sandbox.md) and a host shell can reach. Nx ignores
//! `XDG_RUNTIME_DIR` and otherwise falls back to a world-shared directory or a private HOME path
//! longer than Unix sockets permit; its `O_NOFOLLOW` admission refuses a symlinked leaf, so the
//! host prepares the leaf before a child runs. Whether the daemon runs stays Nx's own decision, so
//! a caller's `NX_DAEMON` never reaches the child.
//!
//! A read-only job cannot write the checkout, and Nx writes its database on every run, so its
//! state lives in the job's exec temp dir: it is not a gate and its results warm nothing.
//!
//! A workspace is cloned from an image, so a daemon's rendezvous record arrives byte-identical and
//! names the daemon still serving the source tree. It is discarded at mint; the warm cache,
//! database and project graph beside it are kept.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use super::{
    CapabilityContribution, CapabilityId, DetectedCapabilities, DetectionContext, DetectionScope,
    Detector, EnvAction,
};
use crate::{CowshedError, Result};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Nx,
    all: &["nx.json"],
    any: &[],
    scope: DetectionScope::Project,
    host_cache_homes: &[],
    reached_from: None,
    contribute,
};

/// Nx's default state directory, relative to its project root.
const STATE: &str = ".nx";
/// The daemon's rendezvous directory inside Nx's state directory.
const DAEMON_RECORD: &str = "workspace-data/d";
/// The daemon's rendezvous record inside [`DAEMON_RECORD`], as Nx's daemon server writes it.
const DAEMON_RECORD_FILE: &str = "server-process.json";
/// The project root every Nx process of a job takes as its workspace root.
const WORKSPACE_ROOT_ENV: &str = "NX_WORKSPACE_ROOT_PATH";

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    // A read-write job's private environment lives in the image; a read-only job's lives in its
    // exec temp dir, outside the checkout it may not write.
    let read_write = context.environment_root.starts_with(context.workspace_root);
    let state = if read_write {
        context.project_root.join(STATE)
    } else {
        context.environment_root.join("cache/nx")
    };
    let mut contribution = CapabilityContribution {
        env: [
            ("NX_SOCKET_DIR", own(context.runtime_dir.join("nx"))),
            (
                "NX_WORKSPACE_DATA_DIRECTORY",
                own(state.join("workspace-data")),
            ),
            ("NX_CACHE_DIRECTORY", own(state.join("cache"))),
            (WORKSPACE_ROOT_ENV, own(context.project_root.into())),
            ("NX_DAEMON", EnvAction::Unset),
        ]
        .into(),
        ..CapabilityContribution::default()
    };
    if !read_write {
        contribution
            .daemon_isolation
            .directories
            .extend([state.join("workspace-data"), state.join("cache")]);
    }
    contribution
        .daemon_isolation
        .directories
        .push(context.runtime_dir.join("nx"));
    let project = workspace_relative(context.workspace_root, context.project_root)?;
    contribution
        .daemon_isolation
        .discard_at_mint
        .push(project.join(STATE).join(DAEMON_RECORD));
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

/// The Nx project root `capabilities` give their jobs, when Nx is active in them: the directory
/// each job's `NX_WORKSPACE_ROOT_PATH` names, whatever `.cowshed.toml` overrides placed it at.
pub(crate) fn project_root(capabilities: &DetectedCapabilities) -> Option<&Path> {
    if !capabilities.active.contains(&CapabilityId::Nx) {
        return None;
    }
    match capabilities.contribution.env.get(WORKSPACE_ROOT_ENV) {
        Some(EnvAction::Own(root)) => Some(Path::new(root)),
        Some(EnvAction::Default(_) | EnvAction::Append(_) | EnvAction::Unset) | None => None,
    }
}

/// The daemon rendezvous record of the Nx project at `project_root`, where a read-write job's Nx
/// writes it: the checkout's own `.nx`, which every boundary of the checkout shares.
pub(crate) fn daemon_record(project_root: &Path) -> PathBuf {
    project_root
        .join(STATE)
        .join(DAEMON_RECORD)
        .join(DAEMON_RECORD_FILE)
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

    /// One Nx state per checkout (04_sandbox.md): a read-write job's Nx state is the
    /// checkout's own `.nx`, the directories every other boundary of the checkout uses, never a
    /// directory of the sandbox's own.
    #[test]
    fn a_read_write_job_shares_the_checkouts_one_nx_state() {
        let fixture = Fixture::new();
        fixture.files(&["nx.json"]);
        let contribution = DETECTOR
            .detect(&fixture.context())
            .expect("detection")
            .expect("nx detected");
        let state = fixture.root.join(".nx");
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
                    directories: vec![fixture.runtime.join("nx")],
                    discard_at_mint: vec![PathBuf::from(".nx/workspace-data/d")],
                },
                ..CapabilityContribution::default()
            }
        );
        for name in ["NX_CACHE_DIRECTORY", "NX_WORKSPACE_DATA_DIRECTORY"] {
            let Some(EnvAction::Own(value)) = contribution.env.get(name) else {
                panic!("{name} is not owned");
            };
            assert!(
                !Path::new(value).starts_with(&fixture.environment),
                "{name} names the sandbox's private environment: {}",
                Path::new(value).display()
            );
        }
    }

    /// A read-only job may not write the checkout, so its Nx state is its exec temp dir's.
    #[test]
    fn a_read_only_job_keeps_nx_state_in_its_exec_temp_dir() {
        let fixture = Fixture::new();
        fixture.files(&["nx.json"]);
        let exec_temp =
            std::env::temp_dir().join(format!("cs-exec-{}", uuid::Uuid::new_v4().simple()));
        let mut context = fixture.context();
        context.environment_root = &exec_temp;
        let contribution = DETECTOR
            .detect(&context)
            .expect("detection")
            .expect("nx detected");
        let state = exec_temp.join("cache/nx");
        assert_eq!(
            contribution.env.get("NX_WORKSPACE_DATA_DIRECTORY"),
            Some(&own(state.join("workspace-data")))
        );
        assert_eq!(
            contribution.env.get("NX_CACHE_DIRECTORY"),
            Some(&own(state.join("cache")))
        );
        assert_eq!(
            contribution.daemon_isolation.directories,
            vec![
                state.join("workspace-data"),
                state.join("cache"),
                fixture.runtime.join("nx"),
            ]
        );
    }

    /// An override directory roots Nx in that project: its workspace root, its `.nx` and its
    /// inherited record move there. An override never enables a project without `nx.json`.
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
            web.contribution.env.get("NX_CACHE_DIRECTORY"),
            Some(&own(fixture.root.join("apps/web/.nx/cache")))
        );
        assert_eq!(
            web.contribution.daemon_isolation.discard_at_mint,
            vec![PathBuf::from("apps/web/.nx/workspace-data/d")]
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
