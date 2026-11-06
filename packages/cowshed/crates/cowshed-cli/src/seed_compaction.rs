//! `cowshed __compact-seed <project-root>`: main's seed compaction as a process of its own
//! (16_build_volumes.md, "Seed compaction").
//!
//! Not a user verb and not in the command map: a fork or land whose main-seed work was slow starts
//! it, in a session of its own with null stdio. Its account is its span (13_telemetry.md,
//! "Background verb spans"): an `info` row for each wait on a land and each catch-up, and a
//! `span-ok` row with its report or a `span-err` row with its error. Run by hand it does the
//! same.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use cowshed_core::runtime::ProjectRuntime;
use cowshed_core::runtime::verb_spans::{COMPACTION_SPAN, VerbNotes, VerbSpan};
use cowshed_core::{CowshedError, Result};

/// Compact main's seed of the project `arguments` name; the process's exit code.
pub async fn run(arguments: &[OsString]) -> i32 {
    let project = match arguments {
        [project] => Some(Path::new(project)),
        _ => None,
    };
    let span = VerbSpan::host(COMPACTION_SPAN, project);
    let (code, outcome) = match compact(arguments, span.notes()).await {
        Ok(report) => (0, Ok(report.to_string())),
        Err(error) => (
            i32::from(error.code.exit_code()),
            Err(format!("{}; hint: {}", error.message, error.hint)),
        ),
    };
    let root = span.root().to_path_buf();
    // A seal that fails never changes the exit code (13_telemetry.md, "Background verb spans").
    if let Err(error) = span.end(outcome.as_deref().map_err(String::as_str)) {
        cowshed_os::eprintln!(
            "cowshed: the seed compaction's span could not be sealed under {}: {error}",
            root.display()
        );
    }
    code
}

async fn compact(
    arguments: &[OsString],
    notes: VerbNotes,
) -> Result<cowshed_core::api::dto::SeedCompactionReport> {
    let [project] = arguments else {
        return Err(CowshedError::usage(
            format!(
                "{} takes a project root",
                cowshed_core::runtime::SEED_COMPACTION_VERB
            ),
            "run `cowshed __compact-seed <project-root>`; a land into main starts it on its own",
        ));
    };
    let runtime = ProjectRuntime::open_existing_for_background_verb(
        PathBuf::from(project),
        crate::gateway_service::runtime_control,
        notes,
    )
    .await?;
    let compacted = runtime.compact_seed().await;
    let shutdown = runtime.shutdown().await;
    let report = compacted?;
    shutdown.map(|()| report)
}
