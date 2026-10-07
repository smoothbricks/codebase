//! lmao's Arrow trace schema, written without lmao's crates, and the sealed segment that
//! publishes one batch of it (13_telemetry.md, "Dependency honesty").
//!
//! `lmao-arrow::trace_schema` cannot be imported alone: that crate also links its sibling
//! `lmao-core` runtime. This module keeps the system-column prefix and the entry-type dictionary
//! byte-for-byte aligned with it until the schema is extracted into a dependency-free crate. The
//! gateway's request spans and the supervisor's job spans are both written here; neither keeps a
//! copy.
//!
//! A segment is one Arrow IPC stream holding one record batch, at
//! `<telemetry root>/<yyyy-mm-dd>/<stem>.arrow`: written to a create-new 0600 temporary,
//! synced, renamed without replace to its sealed name, and the partition directory synced. A
//! segment that exists is complete; a crash leaves at most a temporary in the shared temp grammar.

use std::ffi::{CString, OsStr};
use std::fmt;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, DictionaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt32Array, UInt64Array,
    builder::StringDictionaryBuilder,
    types::{UInt8Type, UInt32Type},
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use thiserror::Error;
use uuid::Uuid;

use crate::fsio::{
    Durability, TemporaryAt, create_private_file_at, open_directory_nofollow,
    open_or_create_child_directory, rename_noreplace, temp_name,
};

/// lmao's entry types, keyed as its `entry_type` dictionary keys them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EntryType {
    SpanStart,
    SpanOk,
    SpanErr,
    SpanException,
    SpanRetry,
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    FfAccess,
    FfUsage,
    PeriodStart,
    OpInvocations,
    OpErrors,
    OpExceptions,
    OpDurationTotal,
    OpDurationOk,
    OpDurationErr,
    OpDurationMin,
    OpDurationMax,
    BufferWrites,
    BufferSpans,
    BufferCapacity,
}

impl EntryType {
    /// The dictionary every segment carries: key 0 is the empty name no row uses.
    pub const NAMES: [&'static str; 25] = [
        "",
        "span-start",
        "span-ok",
        "span-err",
        "span-exception",
        "span-retry",
        "trace",
        "debug",
        "info",
        "warn",
        "error",
        "ff-access",
        "ff-usage",
        "period-start",
        "op-invocations",
        "op-errors",
        "op-exceptions",
        "op-duration-total",
        "op-duration-ok",
        "op-duration-err",
        "op-duration-min",
        "op-duration-max",
        "buffer-writes",
        "buffer-spans",
        "buffer-capacity",
    ];

    /// This entry type's key in [`Self::NAMES`].
    pub const fn key(self) -> u8 {
        match self {
            Self::SpanStart => 1,
            Self::SpanOk => 2,
            Self::SpanErr => 3,
            Self::SpanException => 4,
            Self::SpanRetry => 5,
            Self::Trace => 6,
            Self::Debug => 7,
            Self::Info => 8,
            Self::Warn => 9,
            Self::Error => 10,
            Self::FfAccess => 11,
            Self::FfUsage => 12,
            Self::PeriodStart => 13,
            Self::OpInvocations => 14,
            Self::OpErrors => 15,
            Self::OpExceptions => 16,
            Self::OpDurationTotal => 17,
            Self::OpDurationOk => 18,
            Self::OpDurationErr => 19,
            Self::OpDurationMin => 20,
            Self::OpDurationMax => 21,
            Self::BufferWrites => 22,
            Self::BufferSpans => 23,
            Self::BufferCapacity => 24,
        }
    }

    pub fn name(self) -> &'static str {
        Self::NAMES[usize::from(self.key())]
    }
}

/// A span's identity in lmao's columns: the thread that wrote it and its number in that thread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpanAddress {
    pub thread_id: u64,
    pub span_id: u32,
}

/// One row's system columns. `line` and the package columns are not carried: cowshed's rows have
/// no source location, so every row writes them as zero and null.
#[derive(Clone, Copy, Debug)]
pub struct SystemRow<'a> {
    pub timestamp_ns: i64,
    pub trace_id: &'a str,
    pub span: SpanAddress,
    pub parent: Option<SpanAddress>,
    pub entry_type: EntryType,
    pub message: Option<&'a str>,
}

/// The system columns of a batch, accumulated row by row.
pub struct SystemColumns {
    timestamp: Vec<i64>,
    trace_id: StringDictionaryBuilder<UInt32Type>,
    thread_id: Vec<u64>,
    span_id: Vec<u32>,
    parent_thread_id: Vec<Option<u64>>,
    parent_span_id: Vec<Option<u32>>,
    entry_type: Vec<u8>,
    message: StringDictionaryBuilder<UInt32Type>,
}

impl SystemColumns {
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            timestamp: Vec::with_capacity(rows),
            trace_id: StringDictionaryBuilder::new(),
            thread_id: Vec::with_capacity(rows),
            span_id: Vec::with_capacity(rows),
            parent_thread_id: Vec::with_capacity(rows),
            parent_span_id: Vec::with_capacity(rows),
            entry_type: Vec::with_capacity(rows),
            message: StringDictionaryBuilder::new(),
        }
    }

    /// Append one row. Refused only when a dictionary outgrows its `u32` keys.
    pub fn push(&mut self, row: SystemRow<'_>) -> Result<(), TraceSegmentError> {
        self.trace_id.append(row.trace_id)?;
        match row.message {
            Some(message) => {
                self.message.append(message)?;
            }
            None => self.message.append_null(),
        }
        self.timestamp.push(row.timestamp_ns);
        self.thread_id.push(row.span.thread_id);
        self.span_id.push(row.span.span_id);
        self.parent_thread_id
            .push(row.parent.map(|parent| parent.thread_id));
        self.parent_span_id
            .push(row.parent.map(|parent| parent.span_id));
        self.entry_type.push(row.entry_type.key());
        Ok(())
    }
}

/// A `Utf8` dictionary type keyed by `key`, the encoding of lmao's interned string columns.
pub fn dict_type(key: DataType) -> DataType {
    DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8))
}

/// One record batch: lmao's system columns, then the writer's own `custom` columns, each as long
/// as `system`.
pub fn trace_batch(
    system: SystemColumns,
    custom: Vec<(Field, ArrayRef)>,
) -> Result<RecordBatch, TraceSegmentError> {
    let SystemColumns {
        timestamp,
        mut trace_id,
        thread_id,
        span_id,
        parent_thread_id,
        parent_span_id,
        entry_type,
        mut message,
    } = system;
    let row_count = timestamp.len();
    let entry_type = DictionaryArray::<UInt8Type>::try_new(
        UInt8Array::from(entry_type),
        Arc::new(StringArray::from_iter_values(EntryType::NAMES)) as ArrayRef,
    )?;
    let mut fields = vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("trace_id", dict_type(DataType::UInt32), false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt32, false),
        Field::new("parent_thread_id", DataType::UInt64, true),
        Field::new("parent_span_id", DataType::UInt32, true),
        Field::new("entry_type", dict_type(DataType::UInt8), false),
        Field::new("message", dict_type(DataType::UInt32), true),
        Field::new("package_name", dict_type(DataType::UInt32), true),
        Field::new("package_file", dict_type(DataType::UInt32), true),
        Field::new("git_sha", dict_type(DataType::UInt32), true),
        Field::new("line", DataType::UInt32, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(timestamp)),
        Arc::new(trace_id.finish()),
        Arc::new(UInt64Array::from(thread_id)),
        Arc::new(UInt32Array::from(span_id)),
        Arc::new(UInt64Array::from(parent_thread_id)),
        Arc::new(UInt32Array::from(parent_span_id)),
        Arc::new(entry_type),
        Arc::new(message.finish()),
        Arc::new(null_dictionary(row_count)),
        Arc::new(null_dictionary(row_count)),
        Arc::new(null_dictionary(row_count)),
        Arc::new(UInt32Array::from(vec![0; row_count])),
    ];
    fields.reserve(custom.len());
    columns.reserve(custom.len());
    for (field, column) in custom {
        fields.push(field);
        columns.push(column);
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn null_dictionary(rows: usize) -> DictionaryArray<UInt32Type> {
    let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
    builder.append_nulls(rows);
    builder.finish()
}

/// A UTC calendar date: the name of one telemetry partition directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TelemetryDate {
    year: u64,
    month: u64,
    day: u64,
}

impl TelemetryDate {
    /// `None` unless the triple is a proleptic-Gregorian date on or after 1970-01-01.
    pub fn new(year: u16, month: u8, day: u8) -> Option<Self> {
        let (year, month, day) = (u64::from(year), u64::from(month), u64::from(day));
        super::days_from_civil(year, month, day).map(|_| Self { year, month, day })
    }

    /// The UTC date `seconds` after the epoch. Total over every `u64`.
    pub fn from_unix_seconds(seconds: u64) -> Self {
        let (year, month, day) = super::civil_from_days(seconds / 86_400);
        Self { year, month, day }
    }
}

impl fmt::Display for TelemetryDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.year, self.month, self.day
        )
    }
}

/// The step of [`seal_segment`] an I/O failure stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealStep {
    CreatingPartition,
    SyncingDirectory,
    CreatingSegment,
    SyncingSegment,
    PublishingSegment,
}

impl fmt::Display for SealStep {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CreatingPartition => "creating telemetry partition",
            Self::SyncingDirectory => "syncing telemetry directory",
            Self::CreatingSegment => "creating trace segment",
            Self::SyncingSegment => "syncing trace segment",
            Self::PublishingSegment => "publishing trace segment",
        })
    }
}

#[derive(Debug, Error)]
pub enum TraceSegmentError {
    #[error("{step}: {source}")]
    Io {
        step: SealStep,
        #[source]
        source: io::Error,
    },
    #[error("encoding Arrow trace batch: {0}")]
    Arrow(#[from] ArrowError),
    #[error("trace segment stem {0:?} is not one plain file name")]
    InvalidStem(String),
}

fn at(step: SealStep) -> impl FnOnce(io::Error) -> TraceSegmentError {
    move |source| TraceSegmentError::Io { step, source }
}

/// Seal `batch` as `<root>/<date>/<stem>.arrow`; the sealed path on success. The partition is
/// created 0700 when absent and the root synced for its new entry; a link at the root's last
/// component, at the partition or at either segment name is refused, never followed.
pub fn seal_segment(
    root: &Path,
    date: TelemetryDate,
    stem: &str,
    batch: &RecordBatch,
) -> Result<PathBuf, TraceSegmentError> {
    if stem.is_empty() || stem.starts_with('.') || stem.contains('/') {
        return Err(TraceSegmentError::InvalidStem(stem.to_owned()));
    }
    let invalid_stem = || TraceSegmentError::InvalidStem(stem.to_owned());
    let sealed_name = format!("{stem}.arrow");
    let sealed = CString::new(sealed_name.as_str()).map_err(|_| invalid_stem())?;
    let temporary = CString::new(
        temp_name(OsStr::new(&sealed_name), Uuid::new_v4().simple()).into_encoded_bytes(),
    )
    .map_err(|_| invalid_stem())?;
    let partition_name = date.to_string();
    let partition =
        CString::new(partition_name.as_str()).expect("a formatted date contains no NUL");

    let root_directory = open_directory_nofollow(root).map_err(at(SealStep::CreatingPartition))?;
    let (directory, created) = open_or_create_child_directory(&root_directory, &partition)
        .map_err(at(SealStep::CreatingPartition))?;
    if created {
        Durability::PowerLoss
            .sync_new_entry_at(&root_directory)
            .map_err(at(SealStep::SyncingDirectory))?;
    }

    let mut file =
        create_private_file_at(&directory, &temporary).map_err(at(SealStep::CreatingSegment))?;
    let cleanup = TemporaryAt::new(&directory, &temporary);
    {
        let mut writer = StreamWriter::try_new(&mut file, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Durability::PowerLoss
        .sync_file(&file)
        .map_err(at(SealStep::SyncingSegment))?;
    drop(file);
    rename_noreplace(directory.as_raw_fd(), &temporary, &sealed)
        .map_err(at(SealStep::PublishingSegment))?;
    cleanup.disarm();
    Durability::PowerLoss
        .sync_new_entry_at(&directory)
        .map_err(at(SealStep::SyncingDirectory))?;
    Ok(root.join(partition_name).join(sealed_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_type_keys_index_the_dictionary_by_name() {
        let all = [
            EntryType::SpanStart,
            EntryType::SpanOk,
            EntryType::SpanErr,
            EntryType::SpanException,
            EntryType::SpanRetry,
            EntryType::Trace,
            EntryType::Debug,
            EntryType::Info,
            EntryType::Warn,
            EntryType::Error,
            EntryType::FfAccess,
            EntryType::FfUsage,
            EntryType::PeriodStart,
            EntryType::OpInvocations,
            EntryType::OpErrors,
            EntryType::OpExceptions,
            EntryType::OpDurationTotal,
            EntryType::OpDurationOk,
            EntryType::OpDurationErr,
            EntryType::OpDurationMin,
            EntryType::OpDurationMax,
            EntryType::BufferWrites,
            EntryType::BufferSpans,
            EntryType::BufferCapacity,
        ];
        assert_eq!(all.len() + 1, EntryType::NAMES.len());
        for (index, entry) in all.into_iter().enumerate() {
            assert_eq!(usize::from(entry.key()), index + 1);
        }
        assert_eq!(EntryType::SpanErr.name(), "span-err");
        assert_eq!(EntryType::BufferCapacity.name(), "buffer-capacity");
    }

    #[test]
    fn telemetry_dates_name_utc_partitions() {
        assert_eq!(
            TelemetryDate::from_unix_seconds(0).to_string(),
            "1970-01-01"
        );
        assert_eq!(
            TelemetryDate::from_unix_seconds(1_700_000_000).to_string(),
            "2023-11-14"
        );
        assert_eq!(
            TelemetryDate::new(2026, 10, 7).map(|date| date.to_string()),
            Some("2026-10-07".to_owned())
        );
        assert_eq!(TelemetryDate::new(2026, 2, 29), None);
    }

    #[test]
    fn a_sealed_segment_refuses_a_stem_that_is_not_one_file_name() {
        let root = std::env::temp_dir();
        let batch = trace_batch(SystemColumns::with_capacity(0), Vec::new()).unwrap();
        for stem in ["", ".hidden", "a/b"] {
            assert!(matches!(
                seal_segment(&root, TelemetryDate::from_unix_seconds(0), stem, &batch),
                Err(TraceSegmentError::InvalidStem(_))
            ));
        }
    }
}
