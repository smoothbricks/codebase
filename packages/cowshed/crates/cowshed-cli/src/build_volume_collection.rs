//! `cowshed __collect-build-volumes <project-root>`: the build-volume collection a land owes, as
//! a process of its own (16_build_volumes.md, Land 6.5).
//!
//! Not a user verb and not in the command map: a land starts it once main has moved, adopted and
//! retired, in a session of its own, with stderr appended to `build/.collecting/log`, so the
//! land answers without waiting for any release's unmount or detach. Each release it makes, or
//! defers, is a line of that log; it ends with what it reclaimed. Run by hand it collects what
//! no checkout links, as the next land would.

use std::ffi::OsString;
use std::path::PathBuf;

use cowshed_core::api::dto::GcReport;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::{CowshedError, Result};

/// Collect the build volumes of the project `arguments` name; the process's exit code.
pub async fn run(arguments: &[OsString]) -> i32 {
    match collect(arguments).await {
        Ok(report) => {
            cowshed_os::eprintln!(
                "cowshed: build-volume collection: examined {}, reclaimed {} ({} bytes), {} stay",
                report.examined,
                report.reclaimed,
                report.released_bytes,
                report.deferred.len()
            );
            0
        }
        Err(error) => {
            cowshed_os::eprintln!("cowshed: build-volume collection failed: {}", error.message);
            cowshed_os::eprintln!("hint: {}", error.hint);
            i32::from(error.code.exit_code())
        }
    }
}

async fn collect(arguments: &[OsString]) -> Result<GcReport> {
    let [project] = arguments else {
        return Err(CowshedError::usage(
            format!(
                "{} takes a project root",
                cowshed_core::runtime::BUILD_VOLUME_COLLECTION_VERB
            ),
            "run `cowshed __collect-build-volumes <project-root>`; a land starts it on its own",
        ));
    };
    let runtime = ProjectRuntime::open_existing(
        PathBuf::from(project),
        RecoveryScope::Workspaces(std::collections::BTreeSet::new()),
        crate::gateway_service::runtime_control,
    )
    .await?;
    let collected = runtime.collect_build_volumes().await;
    let shutdown = runtime.shutdown().await;
    let report = collected?;
    shutdown.map(|()| report)
}
