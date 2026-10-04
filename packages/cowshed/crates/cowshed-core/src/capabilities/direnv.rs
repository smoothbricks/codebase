use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, EnvAction, ShellActivation,
    add_bootstrap, host_program_directories,
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
}
