//! The trusted project policy: `/private/cowshed/store/<owner>/<repo>/policy.json`.
//!
//! Controller-owned, mode 0600, on the store volume every sandbox is denied. It holds what the
//! operator decided for the whole project rather than for one workspace: checkpoint quotas, and
//! the project's standing grants — the read paths and egress hosts every workspace of the project
//! starts with. A workspace's own grants (`<image>.grants.json`) add on top; they never subtract.
//!
//! A missing file is the empty policy. A file that does not parse as exactly this shape fails
//! closed: an unknown field is refused rather than ignored, because a policy the controller cannot
//! read completely is not one it may partially apply.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::api::dto::CheckpointQuota;
use crate::metadata::{EgressRule, GrantSet, MetadataError, WorkspaceName, read_json, write_json};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectPolicy {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoint_quotas: BTreeMap<WorkspaceName, CheckpointQuota>,
    #[serde(default)]
    pub grants: ProjectGrants,
}

/// Grants every workspace of the project holds from its first supervisor launch.
///
/// Only reads and egress: a standing write grant would hand every workspace, forks included, a
/// shared writable tree outside its image, which is exactly the per-workspace decision a write
/// grant exists to force.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectGrants {
    /// Advanced by every change, never reset. A workspace's effective revision is its own plus
    /// this one, so a project change reaches the gateway's strictly increasing session revision
    /// and relaunches every supervisor exactly as a workspace grant change does.
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub read: Vec<PathBuf>,
    #[serde(default)]
    pub egress: Vec<EgressRule>,
}

impl ProjectPolicy {
    /// The policy at `path`; the empty policy when the operator has written none.
    pub fn read(path: &Path) -> Result<Self, MetadataError> {
        match read_json(path) {
            Ok(policy) => Ok(policy),
            Err(MetadataError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(Self::default())
            }
            Err(error) => Err(error),
        }
    }

    pub fn write(&self, path: &Path) -> Result<(), MetadataError> {
        write_json(path, self)
    }
}

/// The grant snapshot a workspace runs under: its own grants plus the project's standing ones.
///
/// Reads are the sorted union. Egress is the workspace's rules plus every project rule for a host
/// the workspace does not name itself: a workspace rule for the same host is the narrower,
/// deliberate decision (its ports, mode, impersonation) and is kept as written. The revision is
/// the sum of both revisions — each only ever grows, so the sum grows whenever either does.
pub fn effective_grants(
    workspace: &GrantSet,
    project: &ProjectGrants,
) -> Result<GrantSet, EffectiveRevisionOverflow> {
    let mut effective = workspace.clone();
    effective.revision = workspace
        .revision
        .checked_add(project.revision)
        .ok_or(EffectiveRevisionOverflow)?;
    effective.read.extend(project.read.iter().cloned());
    effective.read.sort();
    effective.read.dedup();
    for rule in &project.egress {
        if !workspace.egress.iter().any(|own| own.host == rule.host) {
            effective.egress.push(rule.clone());
        }
    }
    Ok(effective)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("workspace and project grant revisions overflow their sum")]
pub struct EffectiveRevisionOverflow;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::EgressMode;

    fn rule(host: &str, ports: &[u16]) -> EgressRule {
        EgressRule {
            host: host.to_owned(),
            ports: ports.to_vec(),
            mode: EgressMode::Intercept,
            impersonate: None,
        }
    }

    #[test]
    fn a_workspace_holds_the_project_grants_on_top_of_its_own() {
        let workspace = GrantSet {
            revision: 7,
            read: vec![PathBuf::from("/opt/b")],
            egress: vec![rule("git.example.test", &[443])],
            ..GrantSet::default()
        };
        let project = ProjectGrants {
            revision: 3,
            read: vec![PathBuf::from("/opt/a"), PathBuf::from("/opt/b")],
            egress: vec![
                rule("git.example.test", &[]),
                rule("registry.example.test", &[]),
            ],
        };

        let effective = effective_grants(&workspace, &project).expect("revisions fit");

        assert_eq!(effective.revision, 10);
        assert_eq!(
            effective.read,
            [PathBuf::from("/opt/a"), PathBuf::from("/opt/b")]
        );
        // The workspace's own rule for a host the project also names is kept as written.
        assert_eq!(
            effective.egress,
            [
                rule("git.example.test", &[443]),
                rule("registry.example.test", &[])
            ]
        );
        assert!(effective.write.is_empty());
    }

    #[test]
    fn a_change_to_either_side_advances_the_effective_revision() {
        let workspace = GrantSet {
            revision: 4,
            ..GrantSet::default()
        };
        let project = ProjectGrants {
            revision: 2,
            ..ProjectGrants::default()
        };
        let before = effective_grants(&workspace, &project).unwrap().revision;
        let project_advanced = ProjectGrants {
            revision: 3,
            ..project.clone()
        };
        let workspace_advanced = GrantSet {
            revision: 5,
            ..workspace.clone()
        };
        assert!(
            effective_grants(&workspace, &project_advanced)
                .unwrap()
                .revision
                > before
        );
        assert!(
            effective_grants(&workspace_advanced, &project)
                .unwrap()
                .revision
                > before
        );
        assert_eq!(
            effective_grants(
                &GrantSet {
                    revision: u64::MAX,
                    ..GrantSet::default()
                },
                &project
            ),
            Err(EffectiveRevisionOverflow)
        );
    }

    #[test]
    fn policy_is_one_typed_document_and_refuses_what_it_does_not_know() {
        let root = std::env::temp_dir().join(format!("project-policy-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("policy.json");

        assert_eq!(
            ProjectPolicy::read(&path).unwrap(),
            ProjectPolicy::default()
        );

        let mut policy = ProjectPolicy::default();
        policy.checkpoint_quotas.insert(
            WorkspaceName::new("raven").unwrap(),
            CheckpointQuota {
                max_count: 2,
                max_bytes: 1024,
            },
        );
        policy.grants = ProjectGrants {
            revision: 1,
            read: vec![PathBuf::from("/opt/shared")],
            egress: vec![rule("registry.example.test", &[])],
        };
        policy.write(&path).unwrap();
        assert_eq!(ProjectPolicy::read(&path).unwrap(), policy);
        let written: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            written,
            serde_json::json!({
                "checkpointQuotas": { "raven": { "maxCount": 2, "maxBytes": 1024 } },
                "grants": {
                    "revision": 1,
                    "read": ["/opt/shared"],
                    "egress": [{ "host": "registry.example.test" }]
                }
            })
        );

        // The flat workspace → quota map this file used to be is not silently read as policy.
        std::fs::write(
            &path,
            br#"{ "raven": { "maxCount": 2, "maxBytes": 1024 } }"#,
        )
        .unwrap();
        assert!(ProjectPolicy::read(&path).is_err());
        std::fs::write(&path, br#"{ "grants": { "write": ["/opt/shared"] } }"#).unwrap();
        assert!(ProjectPolicy::read(&path).is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }
}
