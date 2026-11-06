//! A background verb's run as one lmao span (13_telemetry.md, "Background verb spans";
//! 16_build_volumes.md, "Seed compaction" and Land 6.5).
//!
//! `cowshed __collect-build-volumes` and `cowshed __compact-seed` run in a session of their own
//! with null stdio, so what they say is their span, never a file: a `span-start` row named for the
//! verb when the run begins, an `info` row for each thing the run says as it goes
//! ([`VerbNotes`]), and a `span-ok` row with its summary or a `span-err` row with its error. The
//! span is span 1 of a fresh lmao thread in the trace the verb adopted from `TRACEPARENT`
//! ([`AdoptedTrace`]), sealed once the run ends as its own segment `verb-<uuid>.arrow` under
//! `<telemetry root>/<yyyy-mm-dd>/`, the UTC date the run began, by the same
//! [`crate::storage::trace_segment`] owner the wait, job and image spans use. A run that dies
//! before it ends seals nothing, as a crash loses a supervisor's unsealed rows.

use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use thiserror::Error;
use uuid::Uuid;

use super::wait_spans::AdoptedTrace;
use crate::storage::trace_columns::trace_columns;
use crate::storage::trace_columns::{Optional, Required, U32, U64, Utf8};
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError,
    seal_segment, trace_batch,
};

/// The name the start row of `cowshed __collect-build-volumes`'s span carries.
pub const COLLECTION_SPAN: &str = "cowshed.collect-build-volumes";
/// The name the start row of `cowshed __compact-seed`'s span carries.
pub const COMPACTION_SPAN: &str = "cowshed.compact-seed";
/// A verb's span is the only span of its own lmao thread.
const VERB_SPAN_ID: u32 = 1;

trace_columns! {
    /// Every row of a background verb's span: which project it ran for, and in which process.
    pub struct VerbColumns, fields: verb_fields, arrays: verb_column_arrays;
    /// The project root the verb was run for; null when its arguments named none.
    verb_project: Optional<Utf8>,
    process_id: Required<U32>,
    /// The adopted `TRACEPARENT`'s span id; null in a minted trace.
    w3c_parent_span_id: Optional<U64>,
}

/// One thing a run said, when it said it.
#[derive(Debug)]
struct Note {
    at: SystemTime,
    message: String,
}

/// Where the code a background verb runs says what it does: each line an `info` row on the
/// verb's span. A runtime opened for any other verb has no span to say it on, and says it on
/// stderr.
#[derive(Clone, Debug, Default)]
pub struct VerbNotes(Option<mpsc::Sender<Note>>);

impl VerbNotes {
    /// Say `message`: an `info` row on the verb's span, stamped now.
    pub fn say(&self, message: String) {
        match &self.0 {
            // The receiver is the span's; once it is sealed the run has ended and says no more.
            Some(notes) => {
                let _ = notes.send(Note {
                    at: SystemTime::now(),
                    message,
                });
            }
            None => cowshed_os::eprintln!("cowshed: {message}"),
        }
    }
}

/// Why a verb's span was not sealed. The verb's outcome never depends on it.
#[derive(Debug, Error)]
pub enum VerbSpanError {
    #[error("the run's span would start before the Unix epoch")]
    BeforeEpoch,
    #[error("the run's span would end beyond i64 nanoseconds")]
    BeyondNanoseconds,
    #[error(transparent)]
    Segment(#[from] TraceSegmentError),
}

/// One background verb's run, open from [`Self::begin`] until [`Self::end`] seals it.
#[derive(Debug)]
pub struct VerbSpan {
    root: PathBuf,
    trace: AdoptedTrace,
    name: &'static str,
    project: Option<String>,
    began: SystemTime,
    notes: mpsc::Sender<Note>,
    said: mpsc::Receiver<Note>,
}

impl VerbSpan {
    /// Begin `name`'s run for `project` now, to be sealed under the telemetry `root` in the trace
    /// `traceparent` names ([`AdoptedTrace::of`]).
    pub fn begin(
        root: PathBuf,
        traceparent: Option<&str>,
        name: &'static str,
        project: Option<&Path>,
    ) -> Self {
        let (notes, said) = mpsc::channel();
        Self {
            root,
            trace: AdoptedTrace::of(traceparent),
            name,
            project: project.map(|project| project.display().to_string()),
            began: SystemTime::now(),
            notes,
            said,
        }
    }

    /// The host's: its telemetry root, and the trace the verb's starter exported in
    /// `TRACEPARENT`, which a land's background verb inherits from the land.
    pub fn host(name: &'static str, project: Option<&Path>) -> Self {
        Self::begin(
            crate::storage::bootstrap::CanonicalRoots::global()
                .telemetry()
                .to_path_buf(),
            std::env::var("TRACEPARENT").ok().as_deref(),
            name,
            project,
        )
    }

    /// The telemetry root the span is sealed under.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where the run's code says what it does, as `info` rows on this span.
    pub fn notes(&self) -> VerbNotes {
        VerbNotes(Some(self.notes.clone()))
    }

    /// Seal the run: its start, everything it said in the order it said it, and its end,
    /// `Ok(summary)` as `span-ok` or `Err(error)` as `span-err`, stamped now. Answers the sealed
    /// segment's path.
    pub fn end(
        self,
        outcome: std::result::Result<&str, &str>,
    ) -> std::result::Result<PathBuf, VerbSpanError> {
        let ended = SystemTime::now();
        let began = self
            .began
            .duration_since(UNIX_EPOCH)
            .map_err(|_| VerbSpanError::BeforeEpoch)?;
        let segment = Uuid::new_v4();
        let batch = self.batch(outcome, ended, segment.as_u64_pair().0)?;
        Ok(seal_segment(
            &self.root,
            TelemetryDate::from_unix_seconds(began.as_secs()),
            &format!("verb-{}", segment.simple()),
            &batch,
        )?)
    }

    fn batch(
        &self,
        outcome: std::result::Result<&str, &str>,
        ended: SystemTime,
        thread_id: u64,
    ) -> std::result::Result<RecordBatch, VerbSpanError> {
        let said: Vec<Note> = self.said.try_iter().collect();
        let (end, summary) = match outcome {
            Ok(summary) => (EntryType::SpanOk, summary),
            Err(error) => (EntryType::SpanErr, error),
        };
        let rows = said.len() + 2;
        let span = SpanAddress {
            thread_id,
            span_id: VERB_SPAN_ID,
        };
        let mut system = SystemColumns::with_capacity(rows);
        for (at, entry_type, message) in
            std::iter::once((self.began, EntryType::SpanStart, self.name))
                .chain(
                    said.iter()
                        .map(|note| (note.at, EntryType::Info, note.message.as_str())),
                )
                .chain(std::iter::once((ended, end, summary)))
        {
            system.push(SystemRow {
                timestamp_ns: nanoseconds(at)?,
                trace_id: self.trace.trace_id.as_str(),
                span,
                parent: None,
                entry_type,
                message: Some(message),
            })?;
        }
        let columns = VerbColumns {
            verb_project: self.project.clone(),
            process_id: std::process::id(),
            w3c_parent_span_id: self.trace.parent_column(),
        };
        Ok(trace_batch(
            system,
            verb_column_arrays(std::iter::repeat_n(&columns, rows))
                .map_err(TraceSegmentError::from)?,
        )?)
    }
}

fn nanoseconds(at: SystemTime) -> std::result::Result<i64, VerbSpanError> {
    let since = at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| VerbSpanError::BeforeEpoch)?;
    i64::try_from(since.as_nanos()).map_err(|_| VerbSpanError::BeyondNanoseconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{UInt8Type, UInt32Type, UInt64Type};
    use arrow_array::{Array as _, ArrayAccessor as _};

    /// A run seals one segment holding its start, what it said and its end: a failed run's
    /// `span-err` row carries its error, a finished run's `span-ok` row its summary, all in the
    /// trace and under the span the verb adopted from `TRACEPARENT`.
    #[test]
    fn a_run_seals_its_start_what_it_said_and_its_error_or_summary() {
        for (outcome, end) in [
            (Err("the copy failed: Input/output error"), "span-err"),
            (Ok("compacted seed a into b"), "span-ok"),
        ] {
            let root = crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "cs-verb-span");
            let run = VerbSpan::begin(
                root.to_path_buf(),
                Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
                COMPACTION_SPAN,
                Some(Path::new("/Users/fixture/project")),
            );
            let notes = run.notes();
            notes.say("waiting: lands hold turns".to_owned());
            notes.say("copying again after waiting 15 s".to_owned());
            let sealed = run.end(outcome).expect("sealed");
            let name = sealed
                .file_name()
                .and_then(|name| name.to_str())
                .expect("name");
            assert!(
                name.starts_with("verb-") && name.ends_with(".arrow"),
                "{name}"
            );

            let mut reader = arrow_ipc::reader::StreamReader::try_new(
                std::fs::File::open(&sealed).expect("open segment"),
                None,
            )
            .expect("an Arrow stream");
            let batch = reader.next().expect("one batch").expect("a readable batch");
            assert!(reader.next().is_none(), "one batch per segment");
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
            let rows: Vec<_> = (0..batch.num_rows())
                .map(|row| (entry.value(row), message.value(row)))
                .collect();
            assert_eq!(
                rows,
                [
                    ("span-start", COMPACTION_SPAN),
                    ("info", "waiting: lands hold turns"),
                    ("info", "copying again after waiting 15 s"),
                    (end, outcome.unwrap_or_else(|error| error)),
                ]
            );
            let trace = column("trace_id").as_dictionary::<UInt32Type>();
            let trace = trace
                .downcast_dict::<arrow_array::StringArray>()
                .expect("traces");
            let threads = column("thread_id").as_primitive::<UInt64Type>();
            let spans = column("span_id").as_primitive::<UInt32Type>();
            let parent = column("w3c_parent_span_id").as_primitive::<UInt64Type>();
            let project = column("verb_project").as_string::<i32>();
            let pid = column("process_id").as_primitive::<UInt32Type>();
            for row in 0..batch.num_rows() {
                assert_eq!(trace.value(row), "4bf92f3577b34da6a3ce929d0e0e4736");
                assert_eq!(threads.value(row), threads.value(0));
                assert_eq!(spans.value(row), VERB_SPAN_ID);
                assert_eq!(parent.value(row), 0x00f0_67aa_0ba9_02b7);
                assert_eq!(project.value(row), "/Users/fixture/project");
                assert_eq!(pid.value(row), std::process::id());
            }
            assert_eq!(column("parent_span_id").null_count(), batch.num_rows());
        }
    }
}
