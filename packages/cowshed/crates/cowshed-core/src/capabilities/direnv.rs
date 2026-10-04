use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, EnvAction, GrantAccess,
    SharedCache, ShellActivation, add_bootstrap, host_program_directories,
};
use crate::Result;

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Direnv,
    marker_kind: super::MarkerKind::File,
    scope: super::DetectionScope::CommandAncestors,
    all: &[".envrc"],
    any: &[],
    reached_from: None,
    contribute,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution {
        shell: Some(ShellActivation {
            directory: context.project_root.to_owned(),
            script: r#"directory=$1; shift; direnv allow "$directory/.envrc" && exec direnv exec "$directory" "$@""#,
            label: "cowshed-direnv",
        }),
        ..CapabilityContribution::default()
    };
    contribution.env.insert(
        "DIRENV_CONFIG",
        EnvAction::Own(
            context
                .environment_root
                .join("config/direnv")
                .into_os_string(),
        ),
    );
    // `source_url` keeps what it fetched in `$XDG_CACHE_HOME/direnv/cas`, named by its integrity
    // hash. The host's shell already fetched what an `.envrc` sources, so a sandbox reads the
    // host's store instead of needing egress to fetch it again. Read-only: direnv trusts an entry
    // it finds without rehashing it, and a host shell sources it, so no sandbox may plant one.
    contribution.shared_caches.push(SharedCache {
        path: context.home.join(".cache/direnv/cas"),
        private_link: Some(context.environment_root.join("cache/direnv/cas")),
        access: GrantAccess::Read,
    });
    add_bootstrap(
        &mut contribution,
        context,
        "direnv",
        &host_program_directories(context),
    )?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_an_envrc_enables_direnv() {
        super::super::test_support::assert_switch(&DETECTOR, &[".envrc"]);
    }

    /// An `.envrc`'s `source_url` reads the host's integrity-addressed store through the private
    /// `XDG_CACHE_HOME`, and never writes it.
    #[test]
    fn source_url_reads_the_host_store_read_only() {
        let fixture = super::super::test_support::Fixture::new();
        fixture.files(&[".envrc"]);
        let contribution = DETECTOR
            .detect(&fixture.context())
            .unwrap()
            .expect("direnv detected");
        assert_eq!(
            contribution.shared_caches,
            vec![SharedCache {
                path: fixture.home.join(".cache/direnv/cas"),
                private_link: Some(fixture.environment.join("cache/direnv/cas")),
                access: GrantAccess::Read,
            }]
        );
    }
}
