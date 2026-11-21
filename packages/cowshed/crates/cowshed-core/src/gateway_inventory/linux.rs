//! Linux's reading of one project's native store: the directory host's own records. A mounted
//! payload's mount fact is the mount record at its image path, proven by the marker in the tree
//! the record names, so no kernel mount table is joined and nothing in it can be ambiguous.

use super::{GatewayInventoryError, ProjectInventoryFacts, recorded_facts};
use crate::repository::RepoId;
use crate::runtime::native_host;
use crate::storage::StorageLayout;
use crate::storage::apfs::ApfsSubstrateConfig;

/// The storage and mount facts of `repo`, whose checkout `config` names.
pub(super) fn project_facts(
    config: &ApfsSubstrateConfig,
    layout: &StorageLayout,
    repo: &RepoId,
) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
    recorded_facts(&native_host(config.clone())?, config, layout, repo)
}
