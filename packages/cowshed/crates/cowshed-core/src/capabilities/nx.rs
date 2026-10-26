//! Nx (`nx.json`): one task cache, one task database and one daemon per checkout, shared by every
//! boundary that runs Nx there (04_sandbox.md, "One Nx state per checkout").
//!
//! Nx 23 keeps its task database in the workspace-data directory, and that database indexes
//! exactly one cache directory: a run restores only the artifacts its own database has rows for.
//! The daemon's rendezvous record, `d/server-process.json`, lives in the same workspace-data
//! directory, and setting any of `NX_WORKSPACE_DATA_DIRECTORY`, `NX_CACHE_DIRECTORY` or
//! `NX_PROJECT_GRAPH_CACHE_DIRECTORY` moves the record, the database and the cache together.
//! A sandbox that names its own directories therefore owns a second cache: a build that fills
//! one leaves a gate reading the other cold, and every clone inherits both half-warm. So
//! every job names the checkout's own `.nx` — the directories a host shell's Nx uses — and
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
//! A read-only job keeps the source tree read-only, but may write its checkout's build volume.
//! It shares the same cache, task database and daemon as every other client of the checkout.
//!
//! A workspace is cloned from an image, so a daemon's rendezvous record arrives byte-identical and
//! names the daemon still serving the source tree. It is discarded at mint; the warm cache,
//! database and project graph beside it are kept.
//!
//! Every job's Nx answers from the checkout's installed Nx (`NX_USE_LOCAL=true`): without it the
//! daemon installs `nx@latest` from the registry to answer Nx Console's and the AI agents' status,
//! and a client without the variable changes the daemon's environment, which recomputes its graph.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use super::{
    CapabilityContribution, DetectedCapabilities, DetectionContext, DetectionScope, Detector,
    EnvAction,
};
use crate::{CowshedError, Result};
use cowshed_types::capabilities::{BuildStatePath, CapabilityId};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Nx,
    marker_kind: super::MarkerKind::File,
    all: &["nx.json"],
    any: &[],
    scope: DetectionScope::Project,
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
    let state = context.project_root.join(STATE);
    let project = workspace_relative(context.workspace_root, context.project_root)?;
    // Keep Nx's existing ten-percent policy, but derive the bound from the configured
    // build volume instead of whichever filesystem the client happens to discover.
    let cache_bytes = super::workspace_config(context.workspace_root)?
        .build_capacity()
        .bytes()
        / 10;
    let mut contribution = CapabilityContribution {
        env: [
            ("NX_SOCKET_DIR", own(context.runtime_dir.join("nx"))),
            (
                "NX_WORKSPACE_DATA_DIRECTORY",
                own(state.join("workspace-data")),
            ),
            ("NX_CACHE_DIRECTORY", own(state.join("cache"))),
            (
                "NX_MAX_CACHE_SIZE",
                EnvAction::Default(cache_bytes.to_string().into()),
            ),
            (WORKSPACE_ROOT_ENV, own(context.project_root.into())),
            ("NX_DAEMON", EnvAction::Unset),
            ("NX_PROJECT_GRAPH_CACHE_DIRECTORY", EnvAction::Unset),
            ("NX_USE_LOCAL", EnvAction::Own(OsString::from("true"))),
            // A job's Nx client never starts a daemon: a daemon it started would be shared
            // through the rendezvous record and end with the job (04_sandbox.md).
            ("NX_DAEMON_START", EnvAction::Own(OsString::from("never"))),
        ]
        .into(),
        build_state: ["cache", "workspace-data"]
            .into_iter()
            .map(|name| {
                BuildStatePath::from_paths(
                    &project.join(STATE).join(name),
                    &project.join("nx").join(name),
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?,
        ..CapabilityContribution::default()
    };
    contribution
        .daemon_isolation
        .directories
        .push(context.runtime_dir.join("nx"));
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

/// The `workspace-data` directory of the Nx project at `project_root`, which holds its task
/// databases and its daemon's rendezvous directory: the checkout's own, which every boundary of
/// the checkout shares.
pub(crate) fn workspace_data(project_root: &Path) -> PathBuf {
    project_root.join(STATE).join("workspace-data")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::capabilities::test_support::{Fixture, assert_switch};
    use crate::capabilities::{DaemonIsolation, detect};
    use cowshed_types::capabilities::CapabilityOverride;

    #[test]
    fn nx_json_switches_nx_on_and_off() {
        assert_switch(&DETECTOR, &["nx.json"]);
    }
    #[test]
    fn cache_bound_uses_configured_build_capacity_and_preserves_explicit_overrides() {
        let fixture = Fixture::new();
        fixture.files(&["nx.json"]);
        std::fs::write(
            fixture.root.join(".cowshed.toml"),
            "[build]\ncapacity = \"250g\"\n",
        )
        .unwrap();
        let contribution = DETECTOR.detect(&fixture.context()).unwrap().unwrap();
        assert_eq!(
            contribution.env.get("NX_MAX_CACHE_SIZE"),
            Some(&EnvAction::Default("26843545600".into())),
        );
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
                    ("NX_DAEMON_START", EnvAction::Own(OsString::from("never"))),
                    ("NX_PROJECT_GRAPH_CACHE_DIRECTORY", EnvAction::Unset),
                    ("NX_USE_LOCAL", EnvAction::Own(OsString::from("true"))),
                    (
                        "NX_MAX_CACHE_SIZE",
                        EnvAction::Default("10737418240".into())
                    ),
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
                build_state: vec![
                    cowshed_types::capabilities::BuildStatePath::new(".nx/cache", "nx/cache")
                        .unwrap(),
                    cowshed_types::capabilities::BuildStatePath::new(
                        ".nx/workspace-data",
                        "nx/workspace-data"
                    )
                    .unwrap(),
                ],
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

    /// A read-only job shares the checkout's state and daemon without gaining source writes.
    #[test]
    fn a_read_only_job_shares_the_checkouts_one_nx_state_and_daemon() {
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
        let state = fixture.root.join(".nx");
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
            vec![fixture.runtime.join("nx")]
        );
        assert_eq!(contribution.env.get("NX_DAEMON"), Some(&EnvAction::Unset));
        assert_eq!(
            contribution.env.get("NX_PROJECT_GRAPH_CACHE_DIRECTORY"),
            Some(&EnvAction::Unset)
        );
        let read_write = DETECTOR.detect(&fixture.context()).unwrap().unwrap();
        assert_eq!(contribution, read_write);
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
