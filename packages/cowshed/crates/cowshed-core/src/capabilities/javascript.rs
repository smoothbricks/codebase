use super::cache::SharedToolHome;
use super::{
    CapabilityContribution, DetectionContext, EnvAction, add_bootstrap, host_program_directories,
    shared_tool_contribution,
};
use crate::Result;

pub(super) fn contribute(
    context: &DetectionContext<'_>,
    home: &'static SharedToolHome,
) -> Result<CapabilityContribution> {
    let mut contribution = shared_tool_contribution(context, home)?;
    contribution
        .env
        .insert("NODE_USE_ENV_PROXY", EnvAction::Own("1".into()));
    if let Some(bundle) = context.trust_bundle {
        contribution.env.insert(
            "NODE_EXTRA_CA_CERTS",
            EnvAction::Default(bundle.as_os_str().to_owned()),
        );
    }
    add_bootstrap(
        &mut contribution,
        context,
        "node",
        &host_program_directories(context),
    )?;
    Ok(contribution)
}
