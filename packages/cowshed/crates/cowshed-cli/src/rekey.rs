//! `cowshed rekey <ws>` — rebuild one keyless workspace's CA identity.
//!
//! The verb resolves its project exactly like `attach` (cwd or `--project`,
//! no new resolution scheme). The rotation is the controller's
//! `coordinator.rekey` operation, the one N-API callers reach too; this
//! module owns the verb's service call: rotate through
//! [`CliService::rekey`], then reconcile the gateway so sessions re-derive
//! from the new CA. Presentation (bare
//! line vs JSON, guidance, hints) follows the other verbs in the dispatch
//! arm. Rotation invalidates in-flight job certificates.

use cowshed_core::api::RekeyResult;

use crate::runtime::CliService;

/// Rotate `workspace` and reconcile the gateway so sessions re-derive.
pub async fn rekey_report<S>(service: &mut S, workspace: &str) -> cowshed_core::Result<RekeyResult>
where
    S: CliService,
{
    let result = service.rekey(workspace).await?;
    service.reconcile_gateway().await?;
    Ok(result)
}
