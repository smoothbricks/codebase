//! `cowshed __collect-build-volumes <project-root>`: the build-volume collection a land owes, as
//! a process of its own (16_build_volumes.md, Land 6.5).
//!
//! Not a user verb and not in the command map: a land starts it once main has moved, adopted and
//! retired, in a session of its own with null stdio, so the land answers without waiting for any
//! release's unmount or detach. Its account is its span (13_telemetry.md, "Background verb
//! spans"): an `info` row for each volume it defers, with the reason, and a `span-ok` row saying
//! what it examined and reclaimed, or a `span-err` row with its error. Run by hand it collects
//! what no checkout links, as the next land would.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use cowshed_core::api::dto::GcReport;
use cowshed_core::runtime::ProjectRuntime;
use cowshed_core::runtime::verb_spans::{COLLECTION_SPAN, VerbNotes, VerbSpan};
use cowshed_core::{CowshedError, Result};

/// Collect the build volumes of the project `arguments` name; the process's exit code.
pub async fn run(arguments: &[OsString]) -> i32 {
    let project = match arguments {
        [project] => Some(Path::new(project)),
        _ => None,
    };
    traced(arguments, VerbSpan::host(COLLECTION_SPAN, project)).await
}

/// The collection under `span`, sealed once it ends. A seal that fails is said on stderr and
/// never changes the exit code (13_telemetry.md, "Background verb spans").
async fn traced(arguments: &[OsString], span: VerbSpan) -> i32 {
    let notes = span.notes();
    let (code, outcome) = match collect(arguments, notes.clone()).await {
        Ok(report) => {
            for deferred in &report.deferred {
                notes.say(format!(
                    "build volume {} stays: {}",
                    deferred.path.display(),
                    deferred.diagnostic
                ));
            }
            (
                0,
                Ok(format!(
                    "examined {}, reclaimed {} ({} bytes), {} stay",
                    report.examined,
                    report.reclaimed,
                    report.released_bytes,
                    report.deferred.len()
                )),
            )
        }
        Err(error) => (
            i32::from(error.code.exit_code()),
            Err(format!("{}; hint: {}", error.message, error.hint)),
        ),
    };
    let root = span.root().to_path_buf();
    if let Err(error) = span.end(outcome.as_deref().map_err(String::as_str)) {
        cowshed_os::eprintln!(
            "cowshed: the build-volume collection's span could not be sealed under {}: {error}",
            root.display()
        );
    }
    code
}

async fn collect(arguments: &[OsString], notes: VerbNotes) -> Result<GcReport> {
    let [project] = arguments else {
        return Err(CowshedError::usage(
            format!(
                "{} takes a project root",
                cowshed_core::runtime::BUILD_VOLUME_COLLECTION_VERB
            ),
            "run `cowshed __collect-build-volumes <project-root>`; a land starts it on its own",
        ));
    };
    let runtime = ProjectRuntime::open_existing_for_background_verb(
        PathBuf::from(project),
        crate::gateway_service::runtime_control,
        notes,
    )
    .await?;
    let collected = runtime.collect_build_volumes().await;
    let shutdown = runtime.shutdown().await;
    let report = collected?;
    shutdown.map(|()| report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayAccessor as _;
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{UInt8Type, UInt32Type};

    /// A collection that fails leaves its span: one segment whose `span-err` row carries the
    /// error, here the usage error of a run that names no project.
    #[tokio::test]
    async fn a_failed_collection_seals_an_err_span_carrying_its_error() {
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "csverb");
        let code = traced(
            &[],
            VerbSpan::begin(root.to_path_buf(), None, COLLECTION_SPAN, None),
        )
        .await;
        assert_ne!(code, 0);

        let sealed = crate::sealed_segments::read_sealed(&root).expect("telemetry root");
        assert!(
            sealed.skipped.is_empty(),
            "torn segments: {:?}",
            sealed.skipped
        );
        let [segment] = &sealed.segments[..] else {
            panic!("one segment: {:?}", sealed.segments);
        };
        let batch = segment.batches.first().expect("one batch");
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("{name}"))
        };
        let entry = column("entry_type").as_dictionary::<UInt8Type>();
        let entry = entry
            .downcast_dict::<arrow_array::StringArray>()
            .expect("names");
        let message = column("message").as_dictionary::<UInt32Type>();
        let message = message
            .downcast_dict::<arrow_array::StringArray>()
            .expect("messages");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            (entry.value(0), message.value(0)),
            ("span-start", COLLECTION_SPAN)
        );
        assert_eq!(entry.value(1), "span-err");
        assert!(
            message
                .value(1)
                .starts_with("__collect-build-volumes takes a project root; hint: "),
            "{}",
            message.value(1)
        );
    }
}
