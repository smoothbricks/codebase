//! `cowshed __compact-seed <project-root>`: main's seed compaction as a process of its own
//! (16_build_volumes.md, "Seed compaction").
//!
//! Not a user verb and not in the command map: a land that captured main's seed past the extent
//! bound starts it, in a session of its own, with stderr appended to the project's compaction
//! log. Run by hand, it is also how an operator clears a failure that holds this build's lands
//! back: it does not read the failure record, and its success removes it. It says what it did on
//! stderr and exits.

use std::ffi::OsString;
use std::path::PathBuf;

use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::{CowshedError, Result};

/// Compact main's seed of the project `arguments` name; the process's exit code.
pub async fn run(arguments: &[OsString]) -> i32 {
    match compact(arguments).await {
        Ok(report) => {
            cowshed_os::eprintln!("cowshed: seed compaction: {report}");
            0
        }
        Err(error) => {
            cowshed_os::eprintln!("cowshed: seed compaction failed: {}", error.message);
            cowshed_os::eprintln!("hint: {}", error.hint);
            i32::from(error.code.exit_code())
        }
    }
}

async fn compact(arguments: &[OsString]) -> Result<cowshed_core::api::dto::SeedCompactionReport> {
    let [project] = arguments else {
        return Err(CowshedError::usage(
            format!(
                "{} takes a project root",
                cowshed_core::runtime::SEED_COMPACTION_VERB
            ),
            "run `cowshed __compact-seed <project-root>`; a land into main starts it on its own",
        ));
    };
    let runtime = ProjectRuntime::open_existing(
        PathBuf::from(project),
        RecoveryScope::Workspaces(std::collections::BTreeSet::new()),
        crate::gateway_service::runtime_control,
    )
    .await?;
    let compacted = runtime.compact_seed().await;
    let shutdown = runtime.shutdown().await;
    let report = compacted?;
    shutdown.map(|()| report)
}
