//! Native image observations, batched into the existing Arrow telemetry partition.
//! Counts describe file allocation, never bytes written. I/O is the observing process's window.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use uuid::Uuid;

use crate::runtime::job_spans::PROCESS_SEAL_INTERVAL;
use crate::storage::apfs::extents;
use crate::storage::trace_columns::{I64, Optional, Required, U32, U64, Utf8, trace_columns};
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError,
    seal_segment, trace_batch,
};

// The observed 435,459-extent / 281 GB image spent 60.8 s in first write. These are
// early-warning budgets, not a linear latency model: 100k is 23% of that count and
// 10k growth is 2.3%. Allocation warns at 128 GiB (49%) or 8 GiB growth (3.1%).
const EXTENTS_WARN: u64 = 100_000;
const EXTENTS_DELTA_WARN: i64 = 10_000;
const ALLOCATED_WARN: u64 = 128 << 30;
const ALLOCATED_DELTA_WARN: i64 = 8 << 30;
const IO_SCOPE: &str =
    "process-window, includes concurrent threads, excludes children and later async writes";
const QUEUE_CAPACITY: usize = 4096;
/// Retained Rust payload capacity; allocator and channel bookkeeping are additional.
const QUEUE_BYTES: usize = 16 << 20;

/// The same image across a rename, or one endpoint of a clone. A clone observes both
/// source->source and destination->destination; creation is absent (zero) -> present.
#[derive(Clone, Copy)]
pub(super) struct Image<'a> {
    pub role: &'static str,
    pub before: &'a Path,
    pub after: &'a Path,
}

/// What a snapshot reads of an image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    /// The extent map and the allocation: a capture, a first write, a recovery retirement.
    Walked,
    /// The allocation alone (`st_blocks`), for the cause its null extent columns carry: a
    /// release, whose commit never opens the image (05_gateway.md, "Deleting released images"),
    /// or a fork's capture under its source's image lock. A released build image's map runs to
    /// 880,000 extents, and walking it cost 2.5-10.6 s per image inside `rm` (measured
    /// 2026-10-25), all of it before the verb could answer.
    Allocation(&'static str),
}

/// The cause a release's null extent columns carry.
const UNWALKED: &str = "a release does not walk its image's extent map";

/// The cause the null extent columns of a fork's capture carry.
const UNWALKED_UNDER_SOURCE_LOCK: &str =
    "a capture under its source's image lock does not walk an extent map";
#[derive(Debug)]
struct Snapshot {
    extents: Option<u64>,
    allocated: Option<u64>,
    state: &'static str,
    error: Option<String>,
}

impl Snapshot {
    fn read(path: &Path, layout: Layout) -> Self {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Self {
                extents: Some(0),
                allocated: Some(0),
                state: "absent",
                error: None,
            },
            Err(error) => Self {
                extents: None,
                allocated: None,
                state: "unavailable",
                error: Some(error.to_string()),
            },
            Ok(metadata) if !metadata.is_file() => Self {
                extents: None,
                allocated: None,
                state: "unavailable",
                error: Some("image is not a regular file".into()),
            },
            Ok(_) => {
                let allocation = extents::allocated_bytes(path);
                let (extents, unwalked) = match layout {
                    Layout::Walked => match extents::count_extents(path) {
                        Ok(count) => (Some(count.get()), None),
                        Err(error) => (None, Some(error.to_string())),
                    },
                    Layout::Allocation(cause) => (None, Some(cause.to_owned())),
                };
                let error = match (unwalked, &allocation) {
                    (None, Ok(_)) => None,
                    (Some(error), Ok(_)) => Some(error),
                    (None, Err(error)) => Some(error.to_string()),
                    (Some(count), Err(allocation)) => Some(format!("{count}; {allocation}")),
                };
                Self {
                    extents,
                    allocated: allocation.ok(),
                    state: "present",
                    error,
                }
            }
        }
    }
}

fn delta(before: Option<u64>, after: Option<u64>) -> Option<i64> {
    i64::try_from(i128::from(after?) - i128::from(before?)).ok()
}

fn warning(snapshot: &Snapshot, before: Option<&Snapshot>) -> Option<String> {
    let mut reasons = Vec::new();
    if snapshot.extents.is_some_and(|value| value >= EXTENTS_WARN) {
        reasons.push("extent_absolute");
    }
    if before
        .and_then(|before| delta(before.extents, snapshot.extents))
        .is_some_and(|value| value >= EXTENTS_DELTA_WARN)
    {
        reasons.push("extent_delta");
    }
    if snapshot
        .allocated
        .is_some_and(|value| value >= ALLOCATED_WARN)
    {
        reasons.push("allocated_absolute");
    }
    if before
        .and_then(|before| delta(before.allocated, snapshot.allocated))
        .is_some_and(|value| value >= ALLOCATED_DELTA_WARN)
    {
        reasons.push("allocated_delta");
    }
    (!reasons.is_empty()).then(|| reasons.join(","))
}

trace_columns! {
    #[derive(Default)]
    struct ImageColumns, fields: image_fields, arrays: image_arrays;
    operation: Required<Utf8>,
    image_role: Required<Utf8>,
    image_path_before: Required<Utf8>,
    image_path_after: Required<Utf8>,
    image_state_before: Required<Utf8>,
    image_state_after: Optional<Utf8>,
    extents_before: Optional<U64>,
    extents_after: Optional<U64>,
    extents_delta: Optional<I64>,
    allocated_bytes_before: Optional<U64>,
    allocated_bytes_after: Optional<U64>,
    allocated_bytes_delta: Optional<I64>,
    elapsed_us: Optional<U64>,
    measurement_us: Optional<U64>,
    process_id: Required<U32>,
    process_io_write_bytes_before: Optional<U64>,
    process_io_write_bytes_after: Optional<U64>,
    process_io_write_bytes_delta: Optional<I64>,
    process_io_scope: Required<Utf8>,
    process_io_error: Optional<Utf8>,
    measurement_error_before: Optional<Utf8>,
    measurement_error_after: Optional<Utf8>,
    error: Optional<Utf8>,
    warning_reason: Optional<Utf8>,
    extents_warn: Required<U64>,
    extents_delta_warn: Required<I64>,
    allocated_bytes_warn: Required<U64>,
    allocated_bytes_delta_warn: Required<I64>,
}

struct Row {
    at: SystemTime,
    span: u32,
    kind: EntryType,
    columns: ImageColumns,
}

struct Observation {
    id: Uuid,
    rows: Vec<Row>,
}

impl Observation {
    /// Count owned row/vector storage and every string's reserved capacity, without
    /// serializing or copying the observation merely to price its admission.
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.rows.capacity() * std::mem::size_of::<Row>()
            + self
                .rows
                .iter()
                .map(|row| {
                    let columns = &row.columns;
                    [
                        Some(&columns.operation),
                        Some(&columns.image_role),
                        Some(&columns.image_path_before),
                        Some(&columns.image_path_after),
                        Some(&columns.image_state_before),
                        Some(&columns.process_io_scope),
                        columns.image_state_after.as_ref(),
                        columns.process_io_error.as_ref(),
                        columns.measurement_error_before.as_ref(),
                        columns.measurement_error_after.as_ref(),
                        columns.error.as_ref(),
                        columns.warning_reason.as_ref(),
                    ]
                    .into_iter()
                    .flatten()
                    .map(String::capacity)
                    .sum::<usize>()
                })
                .sum::<usize>()
    }
}

fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).expect("an operation fits in u64 microseconds")
}

fn own_io() -> Result<u64, String> {
    #[cfg(target_os = "macos")]
    {
        crate::runtime::process_usage::own_process_write_bytes().map_err(|error| error.to_string())
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err("APFS process-window I/O requires macOS proc_pid_rusage".into())
    }
}

/// One lazily started writer per native host, not one seal per observation. Last-owner
/// drop closes the channel and joins the final seal, including for short-lived CLI hosts.
pub(super) struct ImageTelemetry {
    root: PathBuf,
    writer: Mutex<Option<Writer>>,
    failed: Arc<AtomicU64>,
}

struct Writer {
    sender: mpsc::SyncSender<Observation>,
    thread: thread::JoinHandle<()>,
    /// Reservations include rows already received by the writer. Draining the channel
    /// must not turn a bounded queue into an unbounded interval buffer.
    pending: Arc<AtomicUsize>,
    pending_bytes: Arc<AtomicUsize>,
}

impl ImageTelemetry {
    pub fn new(store: &Path) -> Self {
        Self {
            root: store.join("telemetry"),
            writer: Mutex::new(None),
            failed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn measure<T, E: fmt::Display>(
        &self,
        operation: &'static str,
        images: &[Image<'_>],
        work: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        self.measure_with(operation, images, Layout::Walked, work)
    }

    /// `work`, the release of `image`, as a `retire` span that reads the image's allocation and
    /// walks no extent map (13_telemetry.md, "Native image observations").
    pub fn measure_release<T, E: fmt::Display>(
        &self,
        image: &Path,
        work: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let images = [Image {
            role: "image",
            before: image,
            after: image,
        }];
        self.measure_with("retire", &images, Layout::Allocation(UNWALKED), work)
    }

    /// `work`, a fork's capture under its source's image lock, as an `operation` span that
    /// reads each image's allocation and walks no extent map, so the walk never lengthens the
    /// lock (13_telemetry.md, "Native image observations").
    pub fn measure_under_source_lock<T, E: fmt::Display>(
        &self,
        operation: &'static str,
        images: &[Image<'_>],
        work: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        self.measure_with(
            operation,
            images,
            Layout::Allocation(UNWALKED_UNDER_SOURCE_LOCK),
            work,
        )
    }

    fn measure_with<T, E: fmt::Display>(
        &self,
        operation: &'static str,
        images: &[Image<'_>],
        layout: Layout,
        work: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let measuring = Instant::now();
        let before: Vec<_> = images.iter().map(|image| {
            let snapshot = Snapshot::read(image.before, layout);
            cowshed_os::eprintln!("cowshed: image {operation} before image={} role={} extents={:?} allocated_bytes={:?} state={} measurement_error={:?}",
                image.before.display(), image.role, snapshot.extents, snapshot.allocated, snapshot.state, snapshot.error);
            if let Some(reason) = warning(&snapshot, None) {
                cowshed_os::eprintln!("cowshed: warning image {operation} before image={} reason={reason} extents_warn={EXTENTS_WARN} extents_delta_warn={EXTENTS_DELTA_WARN} allocated_bytes_warn={ALLOCATED_WARN} allocated_bytes_delta_warn={ALLOCATED_DELTA_WARN}", image.before.display());
            }
            snapshot
        }).collect();
        let before_us = micros(measuring.elapsed());
        let io_before = own_io();
        let start = SystemTime::now();
        let started = Instant::now();
        let result = work();
        let elapsed_us = micros(started.elapsed());
        let end = SystemTime::now();
        let io_after = own_io();
        let measuring = Instant::now();
        let after: Vec<_> = images
            .iter()
            .map(|image| {
                // A refused rename still owns its old name. Never report it as freed space.
                let path =
                    if result.is_err() && image.before != image.after && !image.after.exists() {
                        image.before
                    } else {
                        image.after
                    };
                Snapshot::read(path, layout)
            })
            .collect();
        let measurement_us = before_us + micros(measuring.elapsed());
        let io_error = match (&io_before, &io_after) {
            (Ok(_), Ok(_)) => None,
            (Err(error), Ok(_)) => Some(format!("before: {error}")),
            (Ok(_), Err(error)) => Some(format!("after: {error}")),
            (Err(before), Err(after)) => Some(format!("before: {before}; after: {after}")),
        };
        let io_before = io_before.ok();
        let io_after = io_after.ok();
        let mut rows = Vec::with_capacity(images.len() * 4);
        for (index, ((image, before), after)) in images.iter().zip(before).zip(after).enumerate() {
            let mut columns = ImageColumns {
                operation: operation.into(),
                image_role: image.role.into(),
                image_path_before: image.before.display().to_string(),
                image_path_after: image.after.display().to_string(),
                image_state_before: before.state.into(),
                extents_before: before.extents,
                allocated_bytes_before: before.allocated,
                process_id: std::process::id(),
                process_io_write_bytes_before: io_before,
                process_io_scope: IO_SCOPE.into(),
                measurement_error_before: before.error.clone(),
                extents_warn: EXTENTS_WARN,
                extents_delta_warn: EXTENTS_DELTA_WARN,
                allocated_bytes_warn: ALLOCATED_WARN,
                allocated_bytes_delta_warn: ALLOCATED_DELTA_WARN,
                ..ImageColumns::default()
            };
            let span = u32::try_from(index + 1).expect("an operation has at most two images");
            rows.push(Row {
                at: start,
                span,
                kind: EntryType::SpanStart,
                columns: columns.clone(),
            });
            if let Some(reason) = warning(&before, None) {
                let mut warned = columns.clone();
                warned.warning_reason = Some(reason);
                rows.push(Row {
                    at: start,
                    span,
                    kind: EntryType::Warn,
                    columns: warned,
                });
            }
            columns.image_state_after = Some(after.state.into());
            columns.extents_after = after.extents;
            columns.extents_delta = delta(before.extents, after.extents);
            columns.allocated_bytes_after = after.allocated;
            columns.allocated_bytes_delta = delta(before.allocated, after.allocated);
            columns.elapsed_us = Some(elapsed_us);
            columns.measurement_us = Some(measurement_us);
            columns.process_io_write_bytes_after = io_after;
            columns.process_io_write_bytes_delta = delta(io_before, io_after);
            columns.process_io_error = io_error.clone();
            columns.warning_reason = warning(&after, Some(&before));
            columns.measurement_error_after = after.error;
            columns.error = result.as_ref().err().map(ToString::to_string);
            cowshed_os::eprintln!(
                "cowshed: image {operation} after image={} role={} extents_before={:?} extents_after={:?} extents_delta={:?} allocated_bytes_before={:?} allocated_bytes_after={:?} allocated_bytes_delta={:?} elapsed_us={elapsed_us} measurement_us={measurement_us} process_io_write_bytes_before={io_before:?} process_io_write_bytes_after={io_after:?} process_io_write_bytes_delta={:?} process_io_scope=\"{IO_SCOPE}\" process_io_error={io_error:?} measurement_error={:?} status={} error={:?}",
                image.after.display(),
                image.role,
                columns.extents_before,
                columns.extents_after,
                columns.extents_delta,
                columns.allocated_bytes_before,
                columns.allocated_bytes_after,
                columns.allocated_bytes_delta,
                columns.process_io_write_bytes_delta,
                columns.measurement_error_after,
                if result.is_ok() { "ok" } else { "err" },
                columns.error
            );
            if let Some(reason) = &columns.warning_reason {
                cowshed_os::eprintln!(
                    "cowshed: warning image {operation} after image={} reason={reason} extents_warn={EXTENTS_WARN} extents_delta_warn={EXTENTS_DELTA_WARN} allocated_bytes_warn={ALLOCATED_WARN} allocated_bytes_delta_warn={ALLOCATED_DELTA_WARN}",
                    image.after.display()
                );
                rows.push(Row {
                    at: end,
                    span,
                    kind: EntryType::Warn,
                    columns: columns.clone(),
                });
            }
            rows.push(Row {
                at: end,
                span,
                kind: if result.is_ok() {
                    EntryType::SpanOk
                } else {
                    EntryType::SpanErr
                },
                columns,
            });
        }
        self.publish(Observation {
            id: Uuid::new_v4(),
            rows,
        });
        result
    }

    fn publish(&self, observation: Observation) {
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if writer.is_none() {
            let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
            let root = self.root.clone();
            let failed = Arc::clone(&self.failed);
            let pending = Arc::new(AtomicUsize::new(0));
            let writing = Arc::clone(&pending);
            let pending_bytes = Arc::new(AtomicUsize::new(0));
            let writing_bytes = Arc::clone(&pending_bytes);
            match thread::Builder::new()
                .name("cowshed-image-telemetry".into())
                .spawn(move || {
                    write_observations(&root, receiver, &failed, &writing, &writing_bytes)
                }) {
                Ok(thread) => {
                    *writer = Some(Writer {
                        sender,
                        thread,
                        pending,
                        pending_bytes,
                    })
                }
                Err(error) => {
                    failure(
                        &self.failed,
                        1,
                        &self.root,
                        format_args!("writer could not start: {error}"),
                    );
                    return;
                }
            }
        }
        let writer = writer.as_ref().expect("writer started");
        if writer
            .pending
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
                (pending < QUEUE_CAPACITY).then_some(pending + 1)
            })
            .is_err()
        {
            failure(
                &self.failed,
                1,
                &self.root,
                format_args!(
                    "observation refused: interval operation queue is full (limit={QUEUE_CAPACITY})"
                ),
            );
            return;
        }
        let bytes = observation.retained_bytes();
        if let Err(pending_bytes) =
            writer
                .pending_bytes
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
                    pending
                        .checked_add(bytes)
                        .filter(|total| *total <= QUEUE_BYTES)
                })
        {
            writer.pending.fetch_sub(1, Ordering::Relaxed);
            failure(
                &self.failed,
                1,
                &self.root,
                format_args!(
                    "observation refused: interval payload byte budget is full (pending={pending_bytes}, incoming={bytes}, limit={QUEUE_BYTES})"
                ),
            );
            return;
        }
        if let Err(error) = writer.sender.try_send(observation) {
            writer.pending.fetch_sub(1, Ordering::Relaxed);
            writer.pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
            failure(
                &self.failed,
                1,
                &self.root,
                format_args!("observation refused: {error}"),
            );
        }
    }
}

impl Drop for ImageTelemetry {
    fn drop(&mut self) {
        if let Some(Writer { sender, thread, .. }) = self
            .writer
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            drop(sender);
            if thread.join().is_err() {
                failure(
                    &self.failed,
                    1,
                    &self.root,
                    format_args!("writer panicked; queued observations may be lost"),
                );
            }
        }
    }
}

fn batch(observations: &[Observation]) -> Result<RecordBatch, TraceSegmentError> {
    let mut system = SystemColumns::with_capacity(
        observations
            .iter()
            .map(|observation| observation.rows.len())
            .sum(),
    );
    for observation in observations {
        let trace = observation.id.simple().to_string();
        for row in &observation.rows {
            let timestamp_ns = i64::try_from(
                row.at
                    .duration_since(UNIX_EPOCH)
                    .expect("clock after epoch")
                    .as_nanos(),
            )
            .expect("timestamp fits i64");
            let message = format!("cowshed.image.{}", row.columns.operation);
            system.push(SystemRow {
                timestamp_ns,
                trace_id: &trace,
                span: SpanAddress {
                    thread_id: observation.id.as_u64_pair().0,
                    span_id: row.span,
                },
                parent: None,
                entry_type: row.kind,
                message: Some(&message),
            })?;
        }
    }
    trace_batch(
        system,
        image_arrays(
            observations
                .iter()
                .flat_map(|observation| observation.rows.iter().map(|row| &row.columns)),
        )?,
    )
}

fn seal(root: &Path, observations: &mut Vec<Observation>, failed: &AtomicU64) {
    let at = observations[0].rows[0]
        .at
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch");
    let result = batch(observations).and_then(|batch| {
        seal_segment(
            root,
            TelemetryDate::from_unix_seconds(at.as_secs()),
            &format!("image-{}", Uuid::new_v4().simple()),
            &batch,
        )
    });
    if let Err(error) = result {
        failure(
            failed,
            u64::try_from(observations.len()).expect("bounded batch"),
            root,
            format_args!("seal failed: {error}"),
        );
    }
    observations.clear();
}

/// Gather until the clock expires or the host closes, never until a row count.
/// Admission reserves at most QUEUE_CAPACITY observations across the channel and
/// this buffer; bursts refuse rather than raising the full-drive flush rate.
fn gather(
    first: Observation,
    mut receive: impl FnMut(Duration) -> Result<Observation, mpsc::RecvTimeoutError>,
) -> (Vec<Observation>, bool) {
    let deadline = Instant::now() + PROCESS_SEAL_INTERVAL;
    let mut observations = Vec::with_capacity(QUEUE_CAPACITY);
    observations.push(first);
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return (observations, false);
        };
        match receive(remaining) {
            Ok(observation) => observations.push(observation),
            Err(mpsc::RecvTimeoutError::Timeout) => return (observations, false),
            Err(mpsc::RecvTimeoutError::Disconnected) => return (observations, true),
        }
    }
}

fn write_observations(
    root: &Path,
    receiver: mpsc::Receiver<Observation>,
    failed: &AtomicU64,
    pending: &AtomicUsize,
    pending_bytes: &AtomicUsize,
) {
    while let Ok(first) = receiver.recv() {
        let (mut observations, closed) =
            gather(first, |remaining| receiver.recv_timeout(remaining));
        let count = observations.len();
        let bytes: usize = observations.iter().map(Observation::retained_bytes).sum();
        seal(root, &mut observations, failed);
        pending.fetch_sub(count, Ordering::Relaxed);
        pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
        if closed {
            return;
        }
    }
}

fn failure(failed: &AtomicU64, count: u64, root: &Path, reason: fmt::Arguments<'_>) {
    let total = failed.fetch_add(count, Ordering::Relaxed) + count;
    cowshed_os::eprintln!(
        "cowshed: warning image telemetry failed={total} at {}: {reason}",
        root.display()
    );
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use arrow_array::types::UInt8Type;
    use arrow_array::{Array, DictionaryArray, Int64Array, StringArray, UInt64Array};
    use arrow_ipc::reader::StreamReader;
    use cowshed_os::test_backstop::CommandBackstop as _;
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::FileExt;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("image-telemetry-{}", Uuid::new_v4()));
            fs::create_dir_all(root.join("telemetry")).unwrap();
            Self(root)
        }

        fn batches(&self) -> Vec<RecordBatch> {
            fs::read_dir(self.0.join("telemetry"))
                .unwrap()
                .flat_map(|partition| fs::read_dir(partition.unwrap().path()).unwrap())
                .flat_map(|entry| {
                    StreamReader::try_new(File::open(entry.unwrap().path()).unwrap(), None)
                        .unwrap()
                        .map(Result::unwrap)
                        .collect::<Vec<_>>()
                })
                .collect()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn kind(batch: &RecordBatch, row: usize) -> &str {
        let array = batch
            .column_by_name("entry_type")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .unwrap();
        array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(usize::from(array.keys().value(row)))
    }

    fn text<'a>(batch: &'a RecordBatch, name: &str, row: usize) -> Option<&'a str> {
        let array = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (!array.is_null(row)).then(|| array.value(row))
    }

    fn signed(batch: &RecordBatch, name: &str, row: usize) -> i64 {
        let array = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(!array.is_null(row), "{name}");
        array.value(row)
    }

    fn unsigned(batch: &RecordBatch, name: &str, row: usize) -> u64 {
        let array = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(!array.is_null(row), "{name}");
        array.value(row)
    }

    #[test]
    fn image_telemetry_large_native_extent_delta_emits_warning() {
        let fixture = Fixture::new();
        let image = fixture.0.join("image.dmg");
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&image)
            .unwrap();
        let block = vec![0x5a; 1 << 20];
        for _ in 0..200 {
            file.write_all(&block).unwrap();
        }
        file.sync_all().unwrap();
        let holder = fixture.0.join("holder.dmg");
        assert!(
            std::process::Command::new("/bin/cp")
                .arg("-c")
                .arg(&image)
                .arg(&holder)
                .status_bounded()
                .unwrap()
                .success()
        );
        let before = extents::count_extents(&image).unwrap().get();
        let telemetry = ImageTelemetry::new(&fixture.0);
        telemetry
            .measure(
                "clone",
                &[Image {
                    role: "image",
                    before: &image,
                    after: &image,
                }],
                || {
                    // Same CoW fragmentation witness as extents.rs: separated 16 KiB pages
                    // rewritten while a clone retains their neighbours. No image attach.
                    for offset in (0..200_u64 << 20).step_by(32 << 10) {
                        file.write_all_at(&block[..16 << 10], offset)?;
                    }
                    file.sync_all()
                },
            )
            .unwrap();
        let after = extents::count_extents(&image).unwrap().get();
        assert!(
            after - before >= u64::try_from(EXTENTS_DELTA_WARN).unwrap(),
            "{before} -> {after}"
        );
        drop(telemetry); // The last operation is sealed even before the ten-second timer.
        let batches = fixture.batches();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let warned = (0..batch.num_rows())
            .find(|&row| kind(batch, row) == "warn")
            .unwrap();
        assert_eq!(text(batch, "warning_reason", warned), Some("extent_delta"));
        assert_eq!(unsigned(batch, "extents_before", warned), before);
        assert_eq!(unsigned(batch, "extents_after", warned), after);
        assert_eq!(
            signed(batch, "extents_delta", warned),
            i64::try_from(after - before).unwrap()
        );
        assert_eq!(text(batch, "process_io_scope", warned), Some(IO_SCOPE));
        assert!(
            batch
                .column_by_name("process_io_write_bytes_delta")
                .unwrap()
                .is_valid(warned)
        );
    }

    #[test]
    fn image_telemetry_small_delta_no_warning_and_rename_is_not_reclamation() {
        let fixture = Fixture::new();
        let image = fixture.0.join("image.dmg");
        fs::write(&image, [0x5a; 8192]).unwrap();
        let trash = fixture.0.join("retired.dmg");
        let telemetry = ImageTelemetry::new(&fixture.0);
        telemetry
            .measure(
                "clone",
                &[Image {
                    role: "image",
                    before: &image,
                    after: &image,
                }],
                || {
                    OpenOptions::new()
                        .write(true)
                        .open(&image)?
                        .write_all_at(&[0x5a; 4096], 0)
                },
            )
            .unwrap();
        telemetry
            .measure(
                "retire",
                &[Image {
                    role: "image",
                    before: &image,
                    after: &trash,
                }],
                || fs::rename(&image, &trash),
            )
            .unwrap();
        drop(telemetry);
        let batches = fixture.batches();
        assert_eq!(batches.len(), 1, "both operations share one seal");
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 4, "two span pairs and no warning");
        for row in 0..batch.num_rows() {
            assert_ne!(kind(batch, row), "warn");
            if kind(batch, row) == "span-ok" {
                assert_eq!(signed(batch, "allocated_bytes_delta", row), 0);
                assert_eq!(text(batch, "image_state_after", row), Some("present"));
                assert!(batch.column_by_name("elapsed_us").unwrap().is_valid(row));
            }
        }
        assert_ne!(
            unsigned(batch, "thread_id", 0),
            unsigned(batch, "thread_id", 2)
        );
    }

    /// A release reads the image's allocation and never walks its extent map: its commit never
    /// opens the image, and a released build image's map took 2.5-10.6 s to walk inside `rm`.
    /// The extent columns are null with their cause; the allocation is measured.
    #[test]
    fn image_telemetry_release_reads_allocation_and_walks_no_extents() {
        let fixture = Fixture::new();
        let image = fixture.0.join("image.dmg");
        fs::write(&image, [0x5a; 8192]).unwrap();
        let entry = fixture.0.join("released.dmg");
        let telemetry = ImageTelemetry::new(&fixture.0);
        telemetry
            .measure_release(&image, || fs::rename(&image, &entry))
            .unwrap();
        drop(telemetry);
        let batches = fixture.batches();
        let batch = &batches[0];
        let ended = (0..batch.num_rows())
            .find(|&row| kind(batch, row) == "span-ok")
            .unwrap();
        assert!(
            batch
                .column_by_name("extents_before")
                .unwrap()
                .is_null(ended)
        );
        assert_eq!(
            text(batch, "measurement_error_before", ended),
            Some(UNWALKED)
        );
        assert!(unsigned(batch, "allocated_bytes_before", ended) > 0);
        assert_eq!(text(batch, "image_state_after", ended), Some("absent"));
    }

    #[test]
    fn image_telemetry_absolute_and_delta_thresholds_are_independent() {
        let snapshot = |extents, allocated| Snapshot {
            extents: Some(extents),
            allocated: Some(allocated),
            state: "present",
            error: None,
        };
        let high = snapshot(EXTENTS_WARN, 0);
        assert_eq!(
            warning(&high, Some(&high)).as_deref(),
            Some("extent_absolute")
        );
        assert_eq!(warning(&snapshot(EXTENTS_WARN - 1, 0), None), None);
        let low = snapshot(1, 0);
        assert_eq!(
            warning(&snapshot(10_001, 0), Some(&low)).as_deref(),
            Some("extent_delta")
        );
        assert_eq!(warning(&snapshot(10_000, 0), Some(&low)), None);
        assert_eq!(
            warning(&snapshot(1, ALLOCATED_WARN), None).as_deref(),
            Some("allocated_absolute")
        );
        assert_eq!(
            warning(&snapshot(1, 8 << 30), Some(&low)).as_deref(),
            Some("allocated_delta")
        );
        assert_eq!(warning(&low, Some(&high)), None, "shrinking is not growth");
    }

    #[test]
    fn image_telemetry_operation_failure_is_preserved_and_sealed() {
        let fixture = Fixture::new();
        let image = fixture.0.join("image.dmg");
        fs::write(&image, [1; 32]).unwrap();
        let telemetry = ImageTelemetry::new(&fixture.0);
        let result = telemetry.measure(
            "clone",
            &[Image {
                role: "image",
                before: &image,
                after: &image,
            }],
            || Err::<(), _>("first block is short"),
        );
        assert_eq!(result, Err("first block is short"));
        drop(telemetry);
        let batches = fixture.batches();
        assert_eq!(kind(&batches[0], 1), "span-err");
        assert_eq!(text(&batches[0], "error", 1), Some("first block is short"));
    }

    #[test]
    fn image_telemetry_full_queue_refuses_and_counts_without_waiting() {
        let fixture = Fixture::new();
        let (sender, receiver) = mpsc::sync_channel(1);
        let telemetry = ImageTelemetry {
            root: fixture.0.join("telemetry"),
            writer: Mutex::new(Some(Writer {
                sender,
                thread: thread::spawn(|| {}),
                pending: Arc::new(AtomicUsize::new(0)),
                pending_bytes: Arc::new(AtomicUsize::new(0)),
            })),
            failed: Arc::new(AtomicU64::new(0)),
        };
        telemetry.publish(Observation {
            id: Uuid::new_v4(),
            rows: Vec::new(),
        });
        telemetry.publish(Observation {
            id: Uuid::new_v4(),
            rows: Vec::new(),
        });
        assert_eq!(telemetry.failed.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.try_iter().count(), 1);
    }

    #[test]
    fn image_telemetry_full_interval_waits_for_time_not_operation_count() {
        let observation = || Observation {
            id: Uuid::new_v4(),
            rows: Vec::new(),
        };
        let mut incoming = (1..QUEUE_CAPACITY).map(|_| observation());
        let mut interval_elapsed = false;
        let (observations, closed) = gather(observation(), |_| match incoming.next() {
            Some(observation) => Ok(observation),
            None => {
                interval_elapsed = true;
                Err(mpsc::RecvTimeoutError::Timeout)
            }
        });
        assert!(
            interval_elapsed,
            "a full queue cannot seal before the interval"
        );
        assert!(!closed);
        assert_eq!(observations.len(), QUEUE_CAPACITY);

        let final_id = Uuid::new_v4();
        let (observations, closed) = gather(
            Observation {
                id: final_id,
                rows: Vec::new(),
            },
            |_| Err(mpsc::RecvTimeoutError::Disconnected),
        );
        assert!(
            closed,
            "host drop flushes without waiting for another interval"
        );
        assert_eq!(observations[0].id, final_id);
    }

    #[test]
    fn image_telemetry_receiving_does_not_free_interval_queue_capacity() {
        let fixture = Fixture::new();
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let telemetry = ImageTelemetry {
            root: fixture.0.join("telemetry"),
            writer: Mutex::new(Some(Writer {
                sender,
                thread: thread::spawn(|| {}),
                pending: Arc::clone(&pending),
                pending_bytes: Arc::new(AtomicUsize::new(0)),
            })),
            failed: Arc::new(AtomicU64::new(0)),
        };
        for _ in 0..QUEUE_CAPACITY {
            telemetry.publish(Observation {
                id: Uuid::new_v4(),
                rows: Vec::new(),
            });
        }
        let gathered: Vec<_> = receiver.try_iter().collect();
        assert_eq!(gathered.len(), QUEUE_CAPACITY);
        telemetry.publish(Observation {
            id: Uuid::new_v4(),
            rows: Vec::new(),
        });
        assert_eq!(telemetry.failed.load(Ordering::Relaxed), 1);
        assert_eq!(pending.load(Ordering::Relaxed), QUEUE_CAPACITY);
        assert!(
            receiver.try_recv().is_err(),
            "a drained channel cannot admit an unbounded batch"
        );
    }

    #[test]
    fn image_telemetry_byte_budget_refuses_before_operation_capacity() {
        let fixture = Fixture::new();
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let pending_bytes = Arc::new(AtomicUsize::new(0));
        let telemetry = ImageTelemetry {
            root: fixture.0.join("telemetry"),
            writer: Mutex::new(Some(Writer {
                sender,
                thread: thread::spawn(|| {}),
                pending: Arc::clone(&pending),
                pending_bytes: Arc::clone(&pending_bytes),
            })),
            failed: Arc::new(AtomicU64::new(0)),
        };
        let observation = || Observation {
            id: Uuid::new_v4(),
            rows: vec![Row {
                at: SystemTime::now(),
                span: 1,
                kind: EntryType::SpanErr,
                columns: ImageColumns {
                    error: Some("x".repeat(QUEUE_BYTES / 2)),
                    ..ImageColumns::default()
                },
            }],
        };
        let first = observation();
        let bytes = first.retained_bytes();
        assert!(bytes > QUEUE_BYTES / 2 && bytes < QUEUE_BYTES);
        telemetry.publish(first);
        telemetry.publish(observation());
        assert_eq!(telemetry.failed.load(Ordering::Relaxed), 1);
        assert_eq!(pending.load(Ordering::Relaxed), 1);
        assert_eq!(pending_bytes.load(Ordering::Relaxed), bytes);
        assert_eq!(receiver.try_iter().count(), 1);
    }

    #[test]
    fn image_telemetry_seal_failure_is_counted_not_an_operation_failure() {
        let fixture = Fixture::new();
        let image = fixture.0.join("image.dmg");
        fs::write(&image, [0; 4096]).unwrap();
        fs::remove_dir(fixture.0.join("telemetry")).unwrap();
        let telemetry = ImageTelemetry::new(&fixture.0);
        let failed = Arc::clone(&telemetry.failed);
        telemetry
            .measure(
                "clone",
                &[Image {
                    role: "image",
                    before: &image,
                    after: &image,
                }],
                || Ok::<_, std::io::Error>(()),
            )
            .unwrap();
        drop(telemetry);
        assert_eq!(failed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn image_telemetry_native_seed_clone_and_release_are_all_wired() {
        use crate::build_volume::{
            BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, Registrant,
            Registration,
        };
        use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
        use crate::repository::{ProjectPaths, RepoId};
        use crate::storage::apfs::{ApfsSubstrateConfig, native::MacOsApfsExecutionHost};
        use cowshed_os::namespace::{HostNamespace, TestNamespace};

        let fixture = Fixture::new();
        let project = ProjectPaths::with_mount_root(
            &fixture.0,
            fixture.0.join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let layout = BuildVolumeLayout::new(&project).unwrap();
        fs::create_dir_all(layout.images()).unwrap();
        let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
        let host = MacOsApfsExecutionHost::new(
            cowshed_apfs::SystemCommandRunner::new(namespace),
            ApfsSubstrateConfig::new(namespace, &fixture.0, fixture.0.join("checkout")),
        )
        .unwrap();
        let source = BuildVolumeId::mint();
        let seed = BuildVolumeId::mint();
        let clone = BuildVolumeId::mint();
        fs::write(layout.image(&source), [0x5a; 8192]).unwrap();
        layout
            .write_record(
                &source,
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Linked {
                        checkout: WorkspaceName::main(),
                    },
                ),
            )
            .unwrap();
        let seed_record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target: WorkspaceName::main(),
                incarnation: WorkspaceIncarnation::new("a".repeat(32)).unwrap(),
            },
        );
        host.clone_build_volume(
            &layout,
            &source,
            &seed,
            &Registration::new(seed_record, Registrant::Intent),
        )
        .unwrap();
        let clone_record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Linked {
                checkout: WorkspaceName::new("topic").unwrap(),
            },
        );
        host.clone_build_volume(
            &layout,
            &seed,
            &clone,
            &Registration::new(clone_record, Registrant::Intent),
        )
        .unwrap();
        host.release_build_volume(&layout, &clone).unwrap();
        host.release_build_volume(&layout, &seed).unwrap();
        assert!(!layout.image(&clone).exists() && !layout.image(&seed).exists());
        assert_eq!(
            fs::read_dir(layout.deleting()).unwrap().count(),
            2,
            "each release commits its image to build/.deleting and answers"
        );
        drop(host);
        let batches = fixture.batches();
        let mut counts = std::collections::BTreeMap::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                assert_ne!(kind(batch, row), "warn");
                if kind(batch, row) != "span-ok" {
                    continue;
                }
                let operation = text(batch, "operation", row).unwrap();
                *counts.entry(operation).or_insert(0) += 1;
                if operation == "retire" {
                    // A release frees the whole image before it answers.
                    assert_eq!(text(batch, "image_state_after", row), Some("absent"));
                    assert_eq!(unsigned(batch, "allocated_bytes_after", row), 0);
                    assert_eq!(signed(batch, "allocated_bytes_delta", row), -8192);
                } else {
                    assert!(unsigned(batch, "extents_after", row) > 0);
                    assert_eq!(unsigned(batch, "allocated_bytes_after", row), 8192);
                }
                if text(batch, "image_role", row) == Some("destination") {
                    assert_eq!(text(batch, "image_state_before", row), Some("absent"));
                    assert_eq!(unsigned(batch, "extents_before", row), 0);
                }
            }
        }
        assert_eq!(
            counts,
            std::collections::BTreeMap::from([("seed_capture", 2), ("clone", 2), ("retire", 2),])
        );
    }
}
