//! Go (`go.mod`/`go.work` at the selected root, or a tracked nested `go.mod`): the host's own
//! module and build caches.
//!
//! Both caches are content-addressed, so every workspace on the host shares the host's own pair,
//! named directly through `GOMODCACHE` and `GOCACHE` at Go's defaults: `<home>/go/pkg/mod` and the
//! user cache directory's `go-build`. The host's own `go` uses the same two directories without
//! configuration. `GOPATH`, and with it `go install`'s binaries, stays at Go's default under the
//! private sandbox HOME. Go's proxy, checksum database and toolchain policy are the project's, so
//! they are left to Go's own defaults and the project's environment.

use std::path::PathBuf;

use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityId, DetectionContext, DetectionScope, Detector,
    add_shared_tool_home,
};
use crate::Result;

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Go,
    marker_kind: super::MarkerKind::File,
    all: &[],
    any: &["go.mod", "go.work"],
    scope: DetectionScope::Project,
    reached_from: Some(super::ReachedConvention::TrackedManifest("go.mod")),
    contribute,
};

/// Go's default module cache, `$GOPATH/pkg/mod` with the default `GOPATH` of `~/go`.
pub static MODULE_HOME: SharedToolHome = SharedToolHome {
    variable: Some("GOMODCACHE"),
    home: "go/pkg/mod",
    layout: SharedLayout::Whole,
};

#[cfg(target_os = "macos")]
const BUILD_HOME_PATH: &str = "Library/Caches/go-build";
#[cfg(not(target_os = "macos"))]
const BUILD_HOME_PATH: &str = ".cache/go-build";
/// Go's default build cache, `go-build` in the user cache directory.
pub static BUILD_HOME: SharedToolHome = SharedToolHome {
    variable: Some("GOCACHE"),
    home: BUILD_HOME_PATH,
    layout: SharedLayout::Whole,
};

/// The official installer's location; package managers' are in the host program directories.
const OFFICIAL_INSTALL: &str = "/usr/local/go/bin";

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    add_shared_tool_home(&mut contribution, context.home, &MODULE_HOME);
    add_shared_tool_home(&mut contribution, context.home, &BUILD_HOME);
    let mut directories = vec![PathBuf::from(OFFICIAL_INSTALL)];
    directories.extend(super::host_program_directories(context));
    super::add_bootstrap(&mut contribution, context, "go", &directories)?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::capabilities::test_support::{Fixture, assert_switch};
    use crate::capabilities::{EnvAction, GrantAccess, GrantScope, SharedCache};

    #[test]
    fn go_mod_switches_go_on_and_off() {
        assert_switch(&DETECTOR, &["go.mod"]);
    }

    #[test]
    fn go_work_switches_go_on_and_off() {
        assert_switch(&DETECTOR, &["go.work"]);
    }

    /// Both caches are the host's own defaults, shared read-write where they are; no Go policy or
    /// `GOENV` file is contributed, and beneath HOME only the search's literal program probes are
    /// granted besides them.
    #[test]
    fn go_names_the_host_caches_directly() {
        let fixture = Fixture::new();
        fixture.files(&["go.mod"]);
        let contribution = DETECTOR
            .detect(&fixture.context())
            .expect("detection")
            .expect("go detected");
        let module = fixture.home.join("go/pkg/mod");
        let build = fixture.home.join(BUILD_HOME_PATH);
        assert_eq!(
            contribution.env,
            BTreeMap::from([
                ("GOCACHE", EnvAction::Own(build.clone().into())),
                ("GOMODCACHE", EnvAction::Own(module.clone().into())),
            ])
        );
        assert_eq!(
            contribution.shared_caches,
            vec![
                SharedCache {
                    path: module,
                    private_link: None,
                    access: crate::capabilities::GrantAccess::ReadWrite,
                },
                SharedCache {
                    path: build,
                    private_link: None,
                    access: crate::capabilities::GrantAccess::ReadWrite,
                },
            ]
        );
        for grant in &contribution.grants {
            assert_eq!(grant.access, GrantAccess::Read, "{grant:?}");
            if grant.path.starts_with(&fixture.home) {
                assert_eq!(grant.scope, GrantScope::Literal, "{grant:?}");
            }
        }
    }
}
