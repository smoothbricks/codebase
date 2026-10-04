//! Go (`go.mod` or `go.work`): the module and build caches on the shared caches volume.
//!
//! Both caches are content-addressed, so every workspace on the host shares one of each, named
//! directly through `GOMODCACHE` and `GOCACHE`; nothing on the host is relocated for them. A host
//! without a caches volume leaves both to Go's defaults under the private sandbox HOME. Go's
//! proxy, checksum database and toolchain policy are the project's, so they are left to Go's own
//! defaults and the project's environment.

use std::ffi::OsString;
use std::path::PathBuf;

use super::{
    CacheMount, CapabilityContribution, CapabilityId, DetectionContext, DetectionScope, Detector,
    EnvAction,
};
use crate::Result;

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Go,
    all: &[],
    any: &["go.mod", "go.work"],
    scope: DetectionScope::Project,
    host_cache_homes: &[],
    reached_from: None,
    contribute,
};

/// `(variable, directory under the caches root)` for each shared Go cache.
const CACHES: [(&str, &str); 2] = [("GOMODCACHE", "go/mod"), ("GOCACHE", "go/build")];

/// The official installer's location; package managers' are in the host program directories.
const OFFICIAL_INSTALL: &str = "/usr/local/go/bin";

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    let shared = context.caches_root.is_dir();
    for (variable, directory) in CACHES {
        let source = context.caches_root.join(directory);
        if shared {
            contribution
                .env
                .insert(variable, EnvAction::Own(OsString::from(&source)));
            contribution.cache_mounts.push(CacheMount {
                source,
                private_target: None,
            });
        } else {
            contribution.env.insert(variable, EnvAction::Unset);
        }
    }
    let mut directories = vec![PathBuf::from(OFFICIAL_INSTALL)];
    directories.extend(super::host_program_directories(context));
    super::add_bootstrap(&mut contribution, context, "go", &directories)?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use super::*;
    use crate::capabilities::test_support::{Fixture, assert_switch};

    #[test]
    fn go_mod_switches_go_on_and_off() {
        assert_switch(&DETECTOR, &["go.mod"]);
    }

    #[test]
    fn go_work_switches_go_on_and_off() {
        assert_switch(&DETECTOR, &["go.work"]);
    }

    /// With a caches volume both caches are shared, prepared and granted by their mounts, and no
    /// Go policy or `GOENV` file is contributed.
    #[test]
    fn go_names_the_shared_caches_directly() {
        let fixture = Fixture::new();
        fixture.files(&["go.mod"]);
        let contribution = DETECTOR
            .detect(&fixture.context())
            .expect("detection")
            .expect("go detected");
        assert_eq!(
            contribution.env,
            BTreeMap::from([
                (
                    "GOCACHE",
                    EnvAction::Own(fixture.caches.join("go/build").into())
                ),
                (
                    "GOMODCACHE",
                    EnvAction::Own(fixture.caches.join("go/mod").into())
                ),
            ])
        );
        assert_eq!(
            contribution.cache_mounts,
            vec![
                CacheMount {
                    source: fixture.caches.join("go/mod"),
                    private_target: None,
                },
                CacheMount {
                    source: fixture.caches.join("go/build"),
                    private_target: None,
                },
            ]
        );
        // Only a host go installation, if the machine has one, is granted (read-only).
        assert!(
            contribution
                .grants
                .iter()
                .all(|grant| grant.access == crate::capabilities::GrantAccess::Read)
        );
    }

    /// A host with no caches volume keeps Go's private defaults: both variables are the
    /// sandbox's to unset, and nothing outside the sandbox is mounted.
    #[test]
    fn without_a_caches_volume_go_keeps_its_private_defaults() {
        let fixture = Fixture::new();
        fixture.files(&["go.work"]);
        fs::remove_dir(&fixture.caches).expect("no caches volume");
        let contribution = DETECTOR
            .detect(&fixture.context())
            .expect("detection")
            .expect("go detected");
        assert_eq!(
            contribution.env,
            BTreeMap::from([
                ("GOCACHE", EnvAction::Unset),
                ("GOMODCACHE", EnvAction::Unset)
            ])
        );
        assert!(contribution.cache_mounts.is_empty());
        // Beneath the host home only the search's literal program probes; nothing in the
        // workspace.
        for grant in &contribution.grants {
            if grant.path.starts_with(&fixture.home) {
                assert_eq!(
                    (grant.scope, grant.access),
                    (
                        crate::capabilities::GrantScope::Literal,
                        crate::capabilities::GrantAccess::Read
                    ),
                    "{grant:?}"
                );
            } else {
                assert!(!grant.path.starts_with(&fixture.root), "{grant:?}");
            }
        }
    }
}
