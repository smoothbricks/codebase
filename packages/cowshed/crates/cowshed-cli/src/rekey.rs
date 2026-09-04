//! `cowshed rekey <ws>` — rebuild one keyless workspace's CA identity.
//!
//! The verb resolves its project exactly like `attach` (cwd or `--project`,
//! no new resolution scheme). Store-side work lives in
//! [`cowshed_core::storage::apfs::rekey`]; this module owns the verb's
//! service call: rotate through [`CliService::rekey`], reconcile the
//! gateway afterwards so sessions re-derive from the new CA, and map the
//! report onto the frozen [`RekeyResult`] envelope. Presentation (bare
//! line vs JSON, guidance, hints) follows the other verbs in the dispatch
//! arm. Rotation invalidates in-flight job certificates.

use cowshed_core::api::RekeyResult;

use crate::runtime::CliService;

/// Rotate `workspace` and reconcile the gateway so sessions re-derive.
pub async fn rekey_report<S>(service: &mut S, workspace: &str) -> cowshed_core::Result<RekeyResult>
where
    S: CliService,
{
    let report = service.rekey(workspace).await?;
    service.reconcile_gateway().await?;
    Ok(RekeyResult {
        workspace: report.workspace.to_string(),
        workspace_incarnation: report.incarnation.as_str().to_owned(),
        revision: report.revision,
        tombstone_removed: report
            .tombstone_removed
            .map(|path| path.display().to_string()),
    })
}
