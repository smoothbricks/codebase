//! Process-tree spans (13_telemetry.md, "Process-tree spans").
//!
//! One declaration, [`process_custom_fields`], owns the fourteen custom columns. The writer
//! and the reader both use it; neither keeps a second list. Parentage is the observer's
//! retained parent identity, never a numeric pid join. A coverage gap is not a process column:
//! [`JobProcessEvent::Gap`] seals no row.

use std::collections::HashMap;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::{UInt8Type, UInt32Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray, UInt8Array,
    UInt32Array, UInt64Array, builder::StringDictionaryBuilder,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field};
use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::{CommandArg, ExitStatus};
use crate::api::process::{
    BlockerChange, JobProcessDelta, JobProcessEvent, JobProcessSample, ProcessBlockedOn,
    ProcessUsage,
};
use crate::runtime::job_spans::JOB_SPAN_ID;
use crate::runtime::process_tree::ProcessIdentity;
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError, dict_type,
    seal_segment, trace_batch,
};

/// The span name on a process span's start and terminal rows.
pub const PROCESS_RUN: &str = "process.run";
/// Busy→idle, a standard trace row. Not a fifteenth column.
pub const PROCESS_IDLE: &str = "process.idle";
/// Idle→busy, a standard trace row.
pub const PROCESS_BUSY: &str = "process.busy";
/// One unchanged live process, once a minute.
pub const PROCESS_HEARTBEAT: &str = "process.heartbeat";
/// A blocker was observed. Null path and holder replace any previous lock detail.
pub const PROCESS_SET: &str = "process.set";
/// No blocker is observed. Null blocker columns are a clear, not "unchanged".
pub const PROCESS_CLEAR: &str = "process.clear";
/// A usage change that is not a busy/idle flip.
pub const PROCESS_CHANGE: &str = "process.change";

/// `blocked_on` vocabulary. Key 0 is unused, so a present key is never the empty name.
pub const BLOCKED_ON_NAMES: [&str; 8] = [
    "", "none", "lock", "socket", "pipe", "child", "stdin", "disk",
];

/// `exit_signal` vocabulary. Key 0 is unused.
pub const EXIT_SIGNAL_NAMES: [&str; 16] = [
    "", "SIGHUP", "SIGINT", "SIGQUIT", "SIGILL", "SIGTRAP", "SIGABRT", "SIGBUS", "SIGFPE",
    "SIGKILL", "SIGUSR1", "SIGSEGV", "SIGUSR2", "SIGPIPE", "SIGALRM", "SIGTERM",
];

const PROCESS_COLUMN_COUNT: usize = 14;

/// The fourteen custom columns every process span and row share. Adding a name here is the
/// only way the writer grows the schema, and [`read_process_segment`] refuses any other suffix.
pub fn process_custom_fields() -> Vec<Field> {
    vec![
        Field::new("proc_pid", DataType::UInt32, true),
        Field::new("proc_program", dict_type(DataType::UInt32), true),
        Field::new("proc_argv", DataType::Utf8, true),
        Field::new("cpu_user_us", DataType::UInt64, true),
        Field::new("cpu_sys_us", DataType::UInt64, true),
        Field::new("rss_bytes", DataType::UInt64, true),
        Field::new("rss_peak_bytes", DataType::UInt64, true),
        Field::new("io_read_bytes", DataType::UInt64, true),
        Field::new("io_write_bytes", DataType::UInt64, true),
        Field::new("exit_code", DataType::Int32, true),
        Field::new("exit_signal", dict_type(DataType::UInt8), true),
        Field::new("blocked_on", dict_type(DataType::UInt8), true),
        Field::new("blocked_path", dict_type(DataType::UInt32), true),
        Field::new("blocked_holder_pid", DataType::UInt32, true),
    ]
}

/// One observed event plus the identity the observer retained for it. A gap has neither
/// identity: it names no life. `parent` is the retained parent, consulted only when the life
/// is born; `None` hangs that span from the job span.
pub struct RetainedProcessEvent<'a> {
    pub event: &'a JobProcessEvent,
    pub identity: Option<ProcessIdentity>,
    pub parent: Option<ProcessIdentity>,
}

/// Why a process span could not be projected or sealed.
#[derive(Debug, Error)]
pub enum ProcessSpanError {
    #[error("process event {index} names no retained identity")]
    MissingIdentity { index: u32 },
    #[error("process {index} was born twice")]
    OpenedTwice { index: u32 },
    #[error("process {index} was not born")]
    NotOpened { index: u32 },
    #[error("process {index} was already closed")]
    AfterExit { index: u32 },
    #[error("process {index} has no span id below the job span")]
    IndexOverflow { index: u32 },
    #[error("process event names identity {pid} but span {index} was opened for {opened}")]
    IdentityMismatch { index: u32, pid: u32, opened: u32 },
    #[error("terminating signal {signal} has no declared name")]
    UnknownSignal { signal: i32 },
    #[error("process span time is before the Unix epoch")]
    BeforeEpoch,
    #[error("process span time exceeds i64 nanoseconds")]
    BeyondNanoseconds,
    #[error("process segment is not one row of the fourteen-column schema")]
    Schema,
    #[error(transparent)]
    Segment(#[from] TraceSegmentError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OpenSpan {
    span_id: u32,
    identity: ProcessIdentity,
    program: String,
    busy: bool,
    usage: Option<ProcessUsage>,
    closed: bool,
}

/// Projects observed process events onto one job's trace thread.
#[derive(Debug)]
pub struct ProcessSpanProjection {
    trace_id: String,
    thread: u64,
    by_identity: HashMap<ProcessIdentity, u32>,
    open: Vec<OpenSpan>,
}

impl ProcessSpanProjection {
    /// A projection for the job whose spans live on `thread` ([`job_span_thread`]).
    pub fn new(trace_id: impl Into<String>, thread: u64) -> Self {
        Self {
            trace_id: trace_id.into(),
            thread,
            by_identity: HashMap::new(),
            open: Vec::new(),
        }
    }

    /// The row `observed` seals, or `None` when the event changes no process column. A coverage
    /// gap is `None`: it does not grow this schema.
    pub fn project(
        &mut self,
        observed: RetainedProcessEvent<'_>,
        at: SystemTime,
    ) -> Result<Option<ProcessSpanRow>, ProcessSpanError> {
        match observed.event {
            JobProcessEvent::Gap { .. } => Ok(None),
            JobProcessEvent::Born { index, process } => self
                .born(*index, observed.identity, observed.parent, process, at)
                .map(Some),
            JobProcessEvent::Exec { index, process } => {
                let span = self.live(*index, observed.identity)?;
                let changed = span.program != process.program;
                span.program = process.program.clone();
                let span_id = span.span_id;
                if !changed {
                    return Ok(None);
                }
                let program = process.program.clone();
                Ok(Some(self.row(
                    span_id,
                    EntryType::Trace,
                    PROCESS_CHANGE,
                    at,
                    Sparse {
                        proc_program: Some(program),
                        ..Sparse::default()
                    },
                )))
            }
            JobProcessEvent::Changed(JobProcessDelta::Usage { index, usage }) => {
                let span = self.live(*index, observed.identity)?;
                let previous = span.usage;
                let flipped = span.busy != usage.busy;
                span.busy = usage.busy;
                span.usage = Some(*usage);
                let span_id = span.span_id;
                let sparse = usage_columns(previous, *usage, false);
                if !flipped && sparse.is_empty() {
                    return Ok(None);
                }
                let message = if !flipped {
                    PROCESS_CHANGE
                } else if usage.busy {
                    PROCESS_BUSY
                } else {
                    PROCESS_IDLE
                };
                Ok(Some(self.row(
                    span_id,
                    EntryType::Trace,
                    message,
                    at,
                    sparse,
                )))
            }
            JobProcessEvent::Changed(JobProcessDelta::Blocker { index, blocked_on }) => {
                let span_id = self.live(*index, observed.identity)?.span_id;
                Ok(Some(self.row(
                    span_id,
                    EntryType::Trace,
                    match blocked_on {
                        BlockerChange::Set(_) => PROCESS_SET,
                        BlockerChange::Clear => PROCESS_CLEAR,
                    },
                    at,
                    blocker_columns(blocked_on),
                )))
            }
            JobProcessEvent::Heartbeat { index, .. } => {
                let span_id = self.live(*index, observed.identity)?.span_id;
                Ok(Some(self.row(
                    span_id,
                    EntryType::PeriodStart,
                    PROCESS_HEARTBEAT,
                    at,
                    Sparse::default(),
                )))
            }
            JobProcessEvent::Exited { index, process } => self
                .exited(*index, observed.identity, process, at)
                .map(Some),
        }
    }

    fn born(
        &mut self,
        index: u32,
        identity: Option<ProcessIdentity>,
        parent: Option<ProcessIdentity>,
        process: &JobProcessSample,
        at: SystemTime,
    ) -> Result<ProcessSpanRow, ProcessSpanError> {
        let identity = identity.ok_or(ProcessSpanError::MissingIdentity { index })?;
        if self.open.len() != usize::try_from(index).unwrap_or(usize::MAX) {
            return Err(if index < self.open.len() as u32 {
                ProcessSpanError::OpenedTwice { index }
            } else {
                ProcessSpanError::NotOpened { index }
            });
        }
        let span_id = index
            .checked_add(JOB_SPAN_ID + 1)
            .ok_or(ProcessSpanError::IndexOverflow { index })?;
        let parent_span = parent
            .and_then(|parent| self.by_identity.get(&parent).copied())
            .unwrap_or(JOB_SPAN_ID);
        self.by_identity.insert(identity, span_id);
        self.open.push(OpenSpan {
            span_id,
            identity,
            program: process.program.clone(),
            busy: process.usage.is_some_and(|usage| usage.busy),
            usage: process.usage,
            closed: false,
        });
        let mut columns = Sparse {
            proc_pid: Some(process.pid),
            proc_program: Some(process.program.clone()),
            proc_argv: Some(argv_display(&process.argv)),
            ..Sparse::default()
        };
        if let Some(usage) = process.usage {
            columns.merge(usage_columns(None, usage, true));
        }
        Ok(self
            .row(span_id, EntryType::SpanStart, PROCESS_RUN, at, columns)
            .with_parent(parent_span))
    }

    fn exited(
        &mut self,
        index: u32,
        identity: Option<ProcessIdentity>,
        process: &JobProcessSample,
        at: SystemTime,
    ) -> Result<ProcessSpanRow, ProcessSpanError> {
        let span = self.live(index, identity)?;
        span.closed = true;
        let span_id = span.span_id;
        let mut columns = match process.usage {
            Some(usage) => {
                span.usage = Some(usage);
                span.busy = usage.busy;
                usage_columns(None, usage, true)
            }
            None => Sparse::default(),
        };
        match process.exit.as_ref().map(|exit| &exit.status) {
            Some(ExitStatus::Exited { code }) => columns.exit_code = Some(*code),
            Some(ExitStatus::Signaled { signal, .. }) => {
                columns.exit_signal = Some(signal_key(*signal)?);
            }
            None => {}
        }
        let entry = match process.exit.as_ref().map(|exit| &exit.status) {
            Some(ExitStatus::Exited { code: 0 }) => EntryType::SpanOk,
            _ => EntryType::SpanErr,
        };
        Ok(self.row(span_id, entry, PROCESS_RUN, at, columns))
    }

    fn live(
        &mut self,
        index: u32,
        identity: Option<ProcessIdentity>,
    ) -> Result<&mut OpenSpan, ProcessSpanError> {
        let identity = identity.ok_or(ProcessSpanError::MissingIdentity { index })?;
        let span = self
            .open
            .get_mut(index as usize)
            .ok_or(ProcessSpanError::NotOpened { index })?;
        if span.identity != identity {
            return Err(ProcessSpanError::IdentityMismatch {
                index,
                pid: identity.pid,
                opened: span.identity.pid,
            });
        }
        if span.closed {
            return Err(ProcessSpanError::AfterExit { index });
        }
        Ok(span)
    }

    fn row(
        &self,
        span_id: u32,
        entry_type: EntryType,
        message: &'static str,
        at: SystemTime,
        columns: Sparse,
    ) -> ProcessSpanRow {
        ProcessSpanRow {
            at,
            trace_id: self.trace_id.clone(),
            span: SpanAddress {
                thread_id: self.thread,
                span_id,
            },
            parent: SpanAddress {
                thread_id: self.thread,
                span_id: JOB_SPAN_ID,
            },
            entry_type,
            message,
            columns,
        }
    }
}

/// One sealed process-span row. `parent` is the job span unless [`ProcessSpanRow::with_parent`]
/// pointed it at the retained parent life.
pub struct ProcessSpanRow {
    at: SystemTime,
    trace_id: String,
    span: SpanAddress,
    parent: SpanAddress,
    entry_type: EntryType,
    message: &'static str,
    columns: Sparse,
}

impl ProcessSpanRow {
    fn with_parent(mut self, parent_span_id: u32) -> Self {
        self.parent.span_id = parent_span_id;
        self
    }

    pub fn span_id(&self) -> u32 {
        self.span.span_id
    }

    pub fn parent_span_id(&self) -> u32 {
        self.parent.span_id
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Sparse {
    proc_pid: Option<u32>,
    proc_program: Option<String>,
    proc_argv: Option<String>,
    cpu_user_us: Option<u64>,
    cpu_sys_us: Option<u64>,
    rss_bytes: Option<u64>,
    rss_peak_bytes: Option<u64>,
    io_read_bytes: Option<u64>,
    io_write_bytes: Option<u64>,
    exit_code: Option<i32>,
    exit_signal: Option<u8>,
    blocked_on: Option<u8>,
    blocked_path: Option<String>,
    blocked_holder_pid: Option<u32>,
}

impl Sparse {
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    fn merge(&mut self, other: Self) {
        if other.proc_pid.is_some() {
            self.proc_pid = other.proc_pid;
        }
        if other.proc_program.is_some() {
            self.proc_program = other.proc_program;
        }
        if other.proc_argv.is_some() {
            self.proc_argv = other.proc_argv;
        }
        if other.cpu_user_us.is_some() {
            self.cpu_user_us = other.cpu_user_us;
        }
        if other.cpu_sys_us.is_some() {
            self.cpu_sys_us = other.cpu_sys_us;
        }
        if other.rss_bytes.is_some() {
            self.rss_bytes = other.rss_bytes;
        }
        if other.rss_peak_bytes.is_some() {
            self.rss_peak_bytes = other.rss_peak_bytes;
        }
        if other.io_read_bytes.is_some() {
            self.io_read_bytes = other.io_read_bytes;
        }
        if other.io_write_bytes.is_some() {
            self.io_write_bytes = other.io_write_bytes;
        }
        if other.exit_code.is_some() {
            self.exit_code = other.exit_code;
        }
        if other.exit_signal.is_some() {
            self.exit_signal = other.exit_signal;
        }
        if other.blocked_on.is_some() {
            self.blocked_on = other.blocked_on;
        }
        if other.blocked_path.is_some() {
            self.blocked_path = other.blocked_path;
        }
        if other.blocked_holder_pid.is_some() {
            self.blocked_holder_pid = other.blocked_holder_pid;
        }
    }
}

fn usage_columns(previous: Option<ProcessUsage>, next: ProcessUsage, force: bool) -> Sparse {
    let changed =
        |current: u64, prior: Option<u64>| (force || prior != Some(current)).then_some(current);
    let prior_user = previous.map(|usage| usage.cpu_user_us.get());
    let prior_sys = previous.map(|usage| usage.cpu_sys_us.get());
    let prior_rss = previous.map(|usage| usage.rss_bytes.get());
    let prior_peak = previous.map(|usage| usage.rss_peak_bytes.get());
    let (io_read, io_write) = match next.io {
        crate::api::process::ProcessStorageIo::Read {
            read_bytes,
            write_bytes,
        } => {
            let prior = previous.and_then(|usage| match usage.io {
                crate::api::process::ProcessStorageIo::Read {
                    read_bytes,
                    write_bytes,
                } => Some((read_bytes.get(), write_bytes.get())),
                crate::api::process::ProcessStorageIo::Unavailable { .. } => None,
            });
            (
                changed(read_bytes.get(), prior.map(|pair| pair.0)),
                changed(write_bytes.get(), prior.map(|pair| pair.1)),
            )
        }
        crate::api::process::ProcessStorageIo::Unavailable { .. } => (None, None),
    };
    Sparse {
        cpu_user_us: changed(next.cpu_user_us.get(), prior_user),
        cpu_sys_us: changed(next.cpu_sys_us.get(), prior_sys),
        rss_bytes: changed(next.rss_bytes.get(), prior_rss),
        rss_peak_bytes: changed(next.rss_peak_bytes.get(), prior_peak),
        io_read_bytes: io_read,
        io_write_bytes: io_write,
        ..Sparse::default()
    }
}

fn blocker_columns(change: &BlockerChange) -> Sparse {
    match change {
        BlockerChange::Clear => Sparse::default(),
        BlockerChange::Set(blocked) => Sparse {
            blocked_on: Some(blocked_key(blocked)),
            blocked_path: match blocked {
                ProcessBlockedOn::Lock { path, .. } => Some(path.clone()),
                _ => None,
            },
            blocked_holder_pid: match blocked {
                ProcessBlockedOn::Lock {
                    holder: Some(holder),
                    ..
                } => Some(holder.pid),
                _ => None,
            },
            ..Sparse::default()
        },
    }
}

fn blocked_key(blocked: &ProcessBlockedOn) -> u8 {
    match blocked {
        ProcessBlockedOn::None => 1,
        ProcessBlockedOn::Lock { .. } => 2,
        ProcessBlockedOn::Socket => 3,
        ProcessBlockedOn::Pipe => 4,
        ProcessBlockedOn::Child => 5,
        ProcessBlockedOn::Stdin => 6,
        ProcessBlockedOn::Disk => 7,
    }
}

fn signal_key(signal: i32) -> Result<u8, ProcessSpanError> {
    let name = match signal {
        libc::SIGHUP => "SIGHUP",
        libc::SIGINT => "SIGINT",
        libc::SIGQUIT => "SIGQUIT",
        libc::SIGILL => "SIGILL",
        libc::SIGTRAP => "SIGTRAP",
        libc::SIGABRT => "SIGABRT",
        libc::SIGBUS => "SIGBUS",
        libc::SIGFPE => "SIGFPE",
        libc::SIGKILL => "SIGKILL",
        libc::SIGUSR1 => "SIGUSR1",
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGUSR2 => "SIGUSR2",
        libc::SIGPIPE => "SIGPIPE",
        libc::SIGALRM => "SIGALRM",
        libc::SIGTERM => "SIGTERM",
        _ => return Err(ProcessSpanError::UnknownSignal { signal }),
    };
    let key = EXIT_SIGNAL_NAMES
        .iter()
        .position(|candidate| *candidate == name)
        .expect("every named signal is in the vocabulary");
    u8::try_from(key).map_err(|_| ProcessSpanError::UnknownSignal { signal })
}

/// Deterministic argv display. UTF-8 is single-quoted; any other byte sequence is `$'\xHH…'`.
/// Neither form is JSON, and neither replaces a byte with U+FFFD.
pub fn argv_display(argv: &[CommandArg]) -> String {
    let mut out = String::new();
    for (index, argument) in argv.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        push_argument(&mut out, argument.as_os_str().as_bytes());
    }
    out
}

fn push_argument(out: &mut String, bytes: &[u8]) {
    if let Ok(text) = std::str::from_utf8(bytes) {
        out.push('\'');
        for character in text.chars() {
            if character == '\'' {
                out.push_str("'\\''");
            } else {
                out.push(character);
            }
        }
        out.push('\'');
        return;
    }
    out.push_str("$'");
    for byte in bytes {
        match *byte {
            b'\\' => out.push_str("\\\\"),
            b'\'' => out.push_str("\\'"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(*byte as char),
            other => {
                out.push_str("\\x");
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.push(HEX[(other >> 4) as usize] as char);
                out.push(HEX[(other & 0x0f) as usize] as char);
            }
        }
    }
    out.push('\'');
}

/// Seal one process row as `process-<order>-<writer>.arrow` under `root`, through the shared
/// segment writer. The batch's custom suffix is [`process_custom_fields`].
pub fn seal_process_row(
    root: &Path,
    writer: Uuid,
    order: u64,
    row: &ProcessSpanRow,
) -> Result<PathBuf, ProcessSpanError> {
    let since_epoch = row
        .at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProcessSpanError::BeforeEpoch)?;
    let timestamp_ns =
        i64::try_from(since_epoch.as_nanos()).map_err(|_| ProcessSpanError::BeyondNanoseconds)?;
    let batch = process_batch(row, timestamp_ns)?;
    Ok(seal_segment(
        root,
        TelemetryDate::from_unix_seconds(since_epoch.as_secs()),
        &format!("process-{order:020}-{writer}"),
        &batch,
    )?)
}

fn process_batch(
    row: &ProcessSpanRow,
    timestamp_ns: i64,
) -> Result<RecordBatch, TraceSegmentError> {
    let mut system = SystemColumns::with_capacity(1);
    system.push(SystemRow {
        timestamp_ns,
        trace_id: &row.trace_id,
        span: row.span,
        parent: Some(row.parent),
        entry_type: row.entry_type,
        message: Some(row.message),
    })?;
    let columns = &row.columns;
    let arrays = vec![
        uint32(columns.proc_pid).1,
        category(columns.proc_program.as_deref())?.1,
        text(columns.proc_argv.as_deref()).1,
        uint64(columns.cpu_user_us).1,
        uint64(columns.cpu_sys_us).1,
        uint64(columns.rss_bytes).1,
        uint64(columns.rss_peak_bytes).1,
        uint64(columns.io_read_bytes).1,
        uint64(columns.io_write_bytes).1,
        int32(columns.exit_code).1,
        enumeration(columns.exit_signal, &EXIT_SIGNAL_NAMES)?.1,
        enumeration(columns.blocked_on, &BLOCKED_ON_NAMES)?.1,
        category(columns.blocked_path.as_deref())?.1,
        uint32(columns.blocked_holder_pid).1,
    ];
    let fields = process_custom_fields();
    debug_assert_eq!(fields.len(), PROCESS_COLUMN_COUNT);
    debug_assert_eq!(arrays.len(), fields.len());
    trace_batch(system, fields.into_iter().zip(arrays).collect())
}

fn uint32(value: Option<u32>) -> (Field, ArrayRef) {
    (
        Field::new("ignored", DataType::UInt32, true),
        Arc::new(UInt32Array::from(vec![value])),
    )
}

fn uint64(value: Option<u64>) -> (Field, ArrayRef) {
    (
        Field::new("ignored", DataType::UInt64, true),
        Arc::new(UInt64Array::from(vec![value])),
    )
}

fn int32(value: Option<i32>) -> (Field, ArrayRef) {
    (
        Field::new("ignored", DataType::Int32, true),
        Arc::new(Int32Array::from(vec![value])),
    )
}

fn text(value: Option<&str>) -> (Field, ArrayRef) {
    (
        Field::new("ignored", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![value])),
    )
}

fn category(value: Option<&str>) -> Result<(Field, ArrayRef), TraceSegmentError> {
    let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
    if let Some(value) = value {
        builder.append(value)?;
    } else {
        builder.append_null();
    }
    Ok((
        Field::new("ignored", dict_type(DataType::UInt32), true),
        Arc::new(builder.finish()),
    ))
}

fn enumeration(key: Option<u8>, names: &[&str]) -> Result<(Field, ArrayRef), TraceSegmentError> {
    let values = Arc::new(StringArray::from_iter_values(names.iter().copied())) as ArrayRef;
    let array = DictionaryArray::<UInt8Type>::try_new(UInt8Array::from(vec![key]), values)?;
    Ok((
        Field::new("ignored", dict_type(DataType::UInt8), true),
        Arc::new(array),
    ))
}

/// One row read back from a sealed process segment, using the same column declaration the
/// writer used.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedProcessRow {
    pub span_id: u32,
    pub parent_span_id: u32,
    pub entry_type: String,
    pub message: String,
    pub proc_pid: Option<u32>,
    pub proc_program: Option<String>,
    pub proc_argv: Option<String>,
    pub cpu_user_us: Option<u64>,
    pub cpu_sys_us: Option<u64>,
    pub rss_bytes: Option<u64>,
    pub rss_peak_bytes: Option<u64>,
    pub io_read_bytes: Option<u64>,
    pub io_write_bytes: Option<u64>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<String>,
    pub blocked_on: Option<String>,
    pub blocked_path: Option<String>,
    pub blocked_holder_pid: Option<u32>,
}

/// Read the one row of a segment sealed by [`seal_process_row`]. A fifteenth custom column, a
/// renamed column, or a second row is [`ProcessSpanError::Schema`].
pub fn read_process_segment(path: &Path) -> Result<DecodedProcessRow, ProcessSpanError> {
    let file = std::fs::File::open(path).map_err(|source| {
        ProcessSpanError::Segment(TraceSegmentError::Io {
            step: crate::storage::trace_segment::SealStep::CreatingSegment,
            source,
        })
    })?;
    let mut reader = StreamReader::try_new(file, None)
        .map_err(|error| ProcessSpanError::Segment(TraceSegmentError::from(error)))?;
    let batch = reader
        .next()
        .ok_or(ProcessSpanError::Schema)?
        .map_err(|error| ProcessSpanError::Segment(TraceSegmentError::from(error)))?;
    if reader.next().is_some() || batch.num_rows() != 1 {
        return Err(ProcessSpanError::Schema);
    }
    let expected = process_custom_fields();
    let schema = batch.schema();
    let actual = schema.fields();
    let custom = actual
        .get(actual.len().saturating_sub(expected.len())..)
        .unwrap_or_default();
    if custom.len() != expected.len()
        || custom.iter().zip(&expected).any(|(found, declared)| {
            found.name() != declared.name() || found.data_type() != declared.data_type()
        })
    {
        return Err(ProcessSpanError::Schema);
    }
    let start = actual.len() - expected.len();
    let text = |name: &str| optional_text(&batch, start, name);
    let dict = |name: &str| optional_dict(&batch, start, name);
    let u32_col = |name: &str| optional_u32(&batch, start, name);
    let u64_col = |name: &str| optional_u64(&batch, start, name);
    Ok(DecodedProcessRow {
        span_id: batch
            .column(3)
            .as_primitive::<arrow_array::types::UInt32Type>()
            .value(0),
        parent_span_id: batch
            .column(5)
            .as_primitive::<arrow_array::types::UInt32Type>()
            .value(0),
        entry_type: dict_value(batch.column(6), 0).ok_or(ProcessSpanError::Schema)?,
        message: dict_value(batch.column(7), 0).ok_or(ProcessSpanError::Schema)?,
        proc_pid: u32_col("proc_pid")?,
        proc_program: dict("proc_program")?,
        proc_argv: text("proc_argv")?,
        cpu_user_us: u64_col("cpu_user_us")?,
        cpu_sys_us: u64_col("cpu_sys_us")?,
        rss_bytes: u64_col("rss_bytes")?,
        rss_peak_bytes: u64_col("rss_peak_bytes")?,
        io_read_bytes: u64_col("io_read_bytes")?,
        io_write_bytes: u64_col("io_write_bytes")?,
        exit_code: optional_i32(&batch, start, "exit_code")?,
        exit_signal: dict("exit_signal")?,
        blocked_on: dict("blocked_on")?,
        blocked_path: dict("blocked_path")?,
        blocked_holder_pid: u32_col("blocked_holder_pid")?,
    })
}

fn optional_text(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<String>, ProcessSpanError> {
    let column = column(batch, start, name)?;
    if column.is_null(0) {
        return Ok(None);
    }
    Ok(Some(column.as_string::<i32>().value(0).to_owned()))
}

fn optional_dict(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<String>, ProcessSpanError> {
    let column = column(batch, start, name)?;
    Ok(dict_value(column, 0))
}

fn dict_value(column: &ArrayRef, row: usize) -> Option<String> {
    if column.is_null(row) {
        return None;
    }
    if let Some(dict) = column.as_dictionary_opt::<UInt32Type>() {
        let values = dict.values().as_string::<i32>();
        return Some(values.value(dict.keys().value(row) as usize).to_owned());
    }
    if let Some(dict) = column.as_dictionary_opt::<UInt8Type>() {
        let values = dict.values().as_string::<i32>();
        return Some(values.value(dict.keys().value(row) as usize).to_owned());
    }
    None
}

fn optional_u32(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<u32>, ProcessSpanError> {
    let column = column(batch, start, name)?;
    if column.is_null(0) {
        return Ok(None);
    }
    Ok(Some(
        column
            .as_primitive::<arrow_array::types::UInt32Type>()
            .value(0),
    ))
}

fn optional_u64(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<u64>, ProcessSpanError> {
    let column = column(batch, start, name)?;
    if column.is_null(0) {
        return Ok(None);
    }
    Ok(Some(
        column
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0),
    ))
}

fn optional_i32(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<i32>, ProcessSpanError> {
    let column = column(batch, start, name)?;
    if column.is_null(0) {
        return Ok(None);
    }
    Ok(Some(
        column
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(0),
    ))
}

fn column<'a>(
    batch: &'a RecordBatch,
    start: usize,
    name: &str,
) -> Result<&'a ArrayRef, ProcessSpanError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| ProcessSpanError::Schema)?;
    if index < start {
        return Err(ProcessSpanError::Schema);
    }
    Ok(batch.column(index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::dto::UtcTimestamp;
    use crate::api::process::{LockHolder, ProcessExit, ProcessIoUnavailable, ProcessStorageIo};
    use crate::api::resources::{CpuMicros, ResidentBytes, StorageIoBytes};
    use crate::runtime::process_tree::BirthToken;
    use std::time::Duration;

    fn at(second: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(second)
    }

    fn identity(pid: u32, birth: u64) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            birth: BirthToken(birth),
        }
    }

    fn usage(user: u64, busy: bool, rss: u64) -> ProcessUsage {
        ProcessUsage {
            cpu_user_us: CpuMicros::new(user).unwrap(),
            cpu_sys_us: CpuMicros::new(0).unwrap(),
            busy,
            rss_bytes: ResidentBytes::new(rss).unwrap(),
            rss_peak_bytes: ResidentBytes::new(rss).unwrap(),
            io: ProcessStorageIo::Read {
                read_bytes: StorageIoBytes::new(0).unwrap(),
                write_bytes: StorageIoBytes::new(0).unwrap(),
            },
        }
    }

    fn sample(pid: u32, ppid: u32, program: &str, argv: &[&str]) -> JobProcessSample {
        JobProcessSample {
            pid,
            ppid,
            program: program.to_owned(),
            argv: argv
                .iter()
                .map(|argument| CommandArg::from(*argument))
                .collect(),
            born_at: UtcTimestamp::new("2026-10-12T00:00:00Z").unwrap(),
            usage: None,
            blocked_on: None,
            exit: None,
        }
    }

    fn project_born(
        projection: &mut ProcessSpanProjection,
        index: u32,
        who: ProcessIdentity,
        parent: Option<ProcessIdentity>,
        process: &JobProcessSample,
    ) -> ProcessSpanRow {
        projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Born {
                        index,
                        process: process.clone(),
                    },
                    identity: Some(who),
                    parent,
                },
                at(1),
            )
            .unwrap()
            .unwrap()
    }

    #[test]
    fn the_declaration_is_exactly_fourteen_columns() {
        let fields = process_custom_fields();
        let names: Vec<_> = fields.iter().map(|field| field.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "proc_pid",
                "proc_program",
                "proc_argv",
                "cpu_user_us",
                "cpu_sys_us",
                "rss_bytes",
                "rss_peak_bytes",
                "io_read_bytes",
                "io_write_bytes",
                "exit_code",
                "exit_signal",
                "blocked_on",
                "blocked_path",
                "blocked_holder_pid",
            ]
        );
        assert!(fields.iter().all(|field| field.is_nullable()));
        assert!(!names.contains(&"cpu_busy"));
    }

    #[test]
    fn argv_display_is_deterministic_and_keeps_non_utf8() {
        let raw = CommandArg::new(std::ffi::OsString::from_vec(vec![0xff, b'a']));
        let display = argv_display(&[CommandArg::from("it's"), raw]);
        assert_eq!(display, "'it'\\''s' $'\\xffa'");
        assert!(!display.contains('\u{FFFD}'));
        assert!(!display.starts_with('{'));
        assert_eq!(
            display,
            argv_display(&[
                CommandArg::from("it's"),
                CommandArg::new(std::ffi::OsString::from_vec(vec![0xff, b'a']))
            ])
        );
    }

    #[test]
    fn parentage_follows_retained_identity_not_pid() {
        let root = identity(10, 1);
        let child = identity(11, 2);
        let mut projection = ProcessSpanProjection::new("trace", 99);
        let root_row = project_born(
            &mut projection,
            0,
            root,
            None,
            &sample(10, 1, "/bin/bash", &["/bin/bash", "-c", "cat"]),
        );
        assert_eq!(root_row.parent_span_id(), JOB_SPAN_ID);
        // The sample's ppid is the root's pid, but the retained parent is a different identity
        // that was never opened. That must not invent a parent span from the pid.
        let child_row = project_born(
            &mut projection,
            1,
            child,
            Some(identity(10, 9)),
            &sample(11, 10, "/bin/cat", &["/bin/cat"]),
        );
        assert_eq!(child_row.parent_span_id(), JOB_SPAN_ID);
        let real = project_born(
            &mut projection,
            2,
            identity(12, 3),
            Some(root),
            &sample(12, 99, "/bin/cat", &["/bin/cat", "fifo"]),
        );
        assert_eq!(real.parent_span_id(), root_row.span_id());
        assert_ne!(real.parent_span_id(), child_row.span_id());
    }

    #[test]
    fn a_busy_to_idle_flip_is_one_idle_row_without_a_busy_column() {
        let who = identity(7, 1);
        let mut projection = ProcessSpanProjection::new("trace", 3);
        let mut process = sample(7, 1, "/bin/cat", &["/bin/cat"]);
        process.usage = Some(usage(1_000, true, 4096));
        let start = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Born {
                        index: 0,
                        process: process.clone(),
                    },
                    identity: Some(who),
                    parent: None,
                },
                at(1),
            )
            .unwrap()
            .unwrap();
        assert_eq!(start.message(), PROCESS_RUN);
        let idle = usage(1_000, false, 4096);
        let row = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Changed(JobProcessDelta::Usage {
                        index: 0,
                        usage: idle,
                    }),
                    identity: Some(who),
                    parent: None,
                },
                at(2),
            )
            .unwrap()
            .unwrap();
        assert_eq!(row.message(), PROCESS_IDLE);
        assert!(
            row.columns.cpu_user_us.is_none(),
            "no numeric column changed"
        );
        assert!(row.columns.is_empty());
        let again = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Changed(JobProcessDelta::Usage {
                        index: 0,
                        usage: idle,
                    }),
                    identity: Some(who),
                    parent: None,
                },
                at(3),
            )
            .unwrap();
        assert!(again.is_none(), "an unchanged sample is not a row");
        let root = std::env::temp_dir().join(format!("cowshed-span-idle-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let path = seal_process_row(&root, Uuid::nil(), 1, &row).unwrap();
        let decoded = read_process_segment(&path).unwrap();
        assert_eq!(decoded.message, PROCESS_IDLE);
        assert_eq!(decoded.entry_type, "trace");
        assert!(decoded.cpu_user_us.is_none());
        assert_eq!(decoded.span_id, start.span_id());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn clearing_a_lock_leaves_no_stale_path_or_holder() {
        let who = identity(4, 1);
        let mut projection = ProcessSpanProjection::new("trace", 5);
        project_born(
            &mut projection,
            0,
            who,
            None,
            &sample(4, 1, "/bin/cat", &["/bin/cat"]),
        );
        let set = ProcessBlockedOn::Lock {
            path: "/tmp/held".to_owned(),
            holder: Some(LockHolder { pid: 9, job: None }),
        };
        let set_row = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Changed(JobProcessDelta::Blocker {
                        index: 0,
                        blocked_on: BlockerChange::Set(set),
                    }),
                    identity: Some(who),
                    parent: None,
                },
                at(2),
            )
            .unwrap()
            .unwrap();
        let clear = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Changed(JobProcessDelta::Blocker {
                        index: 0,
                        blocked_on: BlockerChange::Clear,
                    }),
                    identity: Some(who),
                    parent: None,
                },
                at(3),
            )
            .unwrap()
            .unwrap();
        assert_eq!(clear.message(), PROCESS_CLEAR);
        assert!(clear.columns.blocked_path.is_none());
        assert!(clear.columns.blocked_holder_pid.is_none());
        assert!(clear.columns.blocked_on.is_none());
        let socket = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Changed(JobProcessDelta::Blocker {
                        index: 0,
                        blocked_on: BlockerChange::Set(ProcessBlockedOn::Socket),
                    }),
                    identity: Some(who),
                    parent: None,
                },
                at(4),
            )
            .unwrap()
            .unwrap();
        assert_eq!(socket.columns.blocked_on, Some(3));
        assert!(socket.columns.blocked_path.is_none());
        assert!(socket.columns.blocked_holder_pid.is_none());
        let root = std::env::temp_dir().join(format!("cowshed-span-clear-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let writer = Uuid::nil();
        let set_path = seal_process_row(&root, writer, 1, &set_row).unwrap();
        let clear_path = seal_process_row(&root, writer, 2, &clear).unwrap();
        let socket_path = seal_process_row(&root, writer, 3, &socket).unwrap();
        let mut kind = None;
        let mut path = None;
        let mut holder = None;
        for segment in [set_path, clear_path, socket_path] {
            let row = read_process_segment(&segment).unwrap();
            match row.message.as_str() {
                PROCESS_SET => {
                    kind = row.blocked_on;
                    path = row.blocked_path;
                    holder = row.blocked_holder_pid;
                }
                PROCESS_CLEAR => {
                    kind = None;
                    path = None;
                    holder = None;
                    assert!(row.blocked_path.is_none());
                    assert!(row.blocked_holder_pid.is_none());
                }
                _ => {}
            }
        }
        assert_eq!(kind.as_deref(), Some("socket"));
        assert_eq!(path, None);
        assert_eq!(holder, None);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_gap_and_an_unavailable_io_source_add_no_column() {
        let who = identity(3, 1);
        let mut projection = ProcessSpanProjection::new("trace", 8);
        let mut process = sample(3, 1, "/bin/cat", &["/bin/cat"]);
        process.usage = Some(ProcessUsage {
            io: ProcessStorageIo::Unavailable {
                reason: ProcessIoUnavailable::NotAccounted,
            },
            ..usage(10, false, 1)
        });
        let start = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Born {
                        index: 0,
                        process: process.clone(),
                    },
                    identity: Some(who),
                    parent: None,
                },
                at(1),
            )
            .unwrap()
            .unwrap();
        assert!(start.columns.io_read_bytes.is_none());
        assert!(start.columns.io_write_bytes.is_none());
        let gap = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Gap {
                        reason: crate::api::process::ProcessCoverageGap::EventsLost,
                    },
                    identity: None,
                    parent: None,
                },
                at(2),
            )
            .unwrap();
        assert!(gap.is_none());
        let root = std::env::temp_dir().join(format!("cowshed-span-schema-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let path = seal_process_row(&root, Uuid::nil(), 1, &start).unwrap();
        let decoded = read_process_segment(&path).unwrap();
        assert_eq!(decoded.proc_argv.as_deref(), Some("'/bin/cat'"));
        assert!(decoded.io_read_bytes.is_none());
        let good = process_batch(&start, 1).unwrap();
        let mut columns = good.columns().to_vec();
        columns.push(Arc::new(arrow_array::BooleanArray::from(vec![Some(true)])));
        let mut fields: Vec<Field> = good
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.push(Field::new("cpu_busy", DataType::Boolean, true));
        let bad =
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
        let refused = seal_segment(
            &root,
            TelemetryDate::from_unix_seconds(1),
            "process-extra-column",
            &bad,
        )
        .unwrap();
        assert!(matches!(
            read_process_segment(&refused),
            Err(ProcessSpanError::Schema)
        ));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn heartbeats_do_not_restate_argv_and_the_terminal_row_is_final_usage() {
        let who = identity(5, 1);
        let mut projection = ProcessSpanProjection::new("trace", 4);
        let start = project_born(
            &mut projection,
            0,
            who,
            None,
            &sample(5, 1, "/bin/sleep", &["/bin/sleep", "1"]),
        );
        let heartbeat = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Heartbeat {
                        index: 0,
                        process: sample(5, 1, "/bin/sleep", &["/bin/sleep", "1"]),
                    },
                    identity: Some(who),
                    parent: None,
                },
                at(61),
            )
            .unwrap()
            .unwrap();
        assert_eq!(heartbeat.message(), PROCESS_HEARTBEAT);
        assert!(heartbeat.columns.proc_argv.is_none());
        assert!(heartbeat.columns.is_empty());
        let mut ended = sample(5, 1, "/bin/sleep", &["/bin/sleep", "1"]);
        ended.usage = Some(usage(50, false, 0));
        ended.exit = Some(ProcessExit {
            status: ExitStatus::Exited { code: 0 },
            exited_at: UtcTimestamp::new("2026-10-12T00:01:00Z").unwrap(),
        });
        let terminal = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Exited {
                        index: 0,
                        process: ended,
                    },
                    identity: Some(who),
                    parent: None,
                },
                at(62),
            )
            .unwrap()
            .unwrap();
        assert_eq!(terminal.message(), PROCESS_RUN);
        assert_eq!(terminal.columns.exit_code, Some(0));
        assert_eq!(terminal.columns.cpu_user_us, Some(50));
        assert!(terminal.columns.proc_argv.is_none());
        let root = std::env::temp_dir().join(format!("cowshed-span-final-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let writer = Uuid::nil();
        let paths = [
            seal_process_row(&root, writer, 1, &start).unwrap(),
            seal_process_row(&root, writer, 2, &heartbeat).unwrap(),
            seal_process_row(&root, writer, 3, &terminal).unwrap(),
        ];
        let rows: Vec<_> = paths
            .iter()
            .map(|path| read_process_segment(path).unwrap())
            .collect();
        assert_eq!(rows.iter().filter(|row| row.proc_argv.is_some()).count(), 1);
        assert_eq!(
            rows.iter()
                .filter(|row| row.entry_type == "span-ok")
                .count(),
            1
        );
        assert_eq!(rows[2].cpu_user_us, Some(50));
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A real job, held on two FIFOs, seals three parented spans through the production sink.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_real_fifo_job_seals_three_parented_spans() {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};

        use crate::fork_lock::Spawn;
        use crate::runtime::process_events::KqueueObserver;
        use crate::runtime::process_tree::LifeReadings;

        struct Unread;
        impl LifeReadings for Unread {
            fn usage(&self, _: ProcessIdentity) -> Option<ProcessUsage> {
                None
            }
            fn blocked_on(
                &self,
                _: ProcessIdentity,
            ) -> Option<crate::api::process::ProcessBlockedOn> {
                None
            }
        }

        let root = std::env::temp_dir().join(format!("cowshed-span-fifo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let first = root.join("a");
        let second = root.join("b");
        for path in [&first, &second] {
            let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        }
        let script = format!(
            "cat '{}' &\ncat '{}' &\nwait\n",
            first.display(),
            second.display()
        );
        let (ready_read, ready_write) = std::io::pipe().unwrap();
        let (gate_read, mut gate_write) = std::io::pipe().unwrap();
        let mut command = Command::new("/bin/bash");
        command
            .args(["-c", &script])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let ready = ready_write.as_raw_fd();
        let gate = gate_read.as_raw_fd();
        unsafe {
            command.pre_exec(move || {
                let pid = libc::getpid().to_ne_bytes();
                if libc::write(ready, pid.as_ptr().cast(), pid.len()) != 4 {
                    return Err(std::io::Error::last_os_error());
                }
                let mut byte = 0_u8;
                if libc::read(gate, (&raw mut byte).cast(), 1) != 1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let (mut observer, mut child) = std::thread::scope(|scope| {
            let spawned = scope.spawn(|| command.spawn_locked().expect("spawn"));
            let mut pid = [0_u8; 4];
            std::io::Read::read_exact(&mut &ready_read, &mut pid).expect("child pid");
            let pid = u32::try_from(i32::from_ne_bytes(pid)).unwrap();
            let observer =
                KqueueObserver::watch(crate::api::dto::JobId::new(1).unwrap(), pid).expect("watch");
            gate_write.write_all(b"x").unwrap();
            (observer, spawned.join().unwrap())
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while observer.fold().nodes().count() < 3 && std::time::Instant::now() < deadline {
            let _ = observer.wait();
        }
        let births: Vec<_> = observer
            .fold()
            .nodes()
            .enumerate()
            .map(|(index, node)| {
                (
                    index as u32,
                    node.identity,
                    node.parent,
                    JobProcessSample {
                        pid: node.identity.pid,
                        ppid: node.ppid,
                        program: node.image.program.clone(),
                        argv: node.image.argv.clone(),
                        born_at: node.born_at.clone(),
                        usage: None,
                        blocked_on: None,
                        exit: None,
                    },
                )
            })
            .collect();
        assert_eq!(
            births.len(),
            3,
            "FIFO-held children must be retained, not inferred"
        );
        let mut projection = ProcessSpanProjection::new("4bf92f3577b34da6a3ce929d0e0e4736", 7);
        let mut sealed = Vec::new();
        let telemetry = root.join("telemetry");
        std::fs::create_dir(&telemetry).unwrap();
        let writer = Uuid::nil();
        let mut order = 1_u64;
        for (index, identity, parent, process) in &births {
            let row = projection
                .project(
                    RetainedProcessEvent {
                        event: &JobProcessEvent::Born {
                            index: *index,
                            process: process.clone(),
                        },
                        identity: Some(*identity),
                        parent: *parent,
                    },
                    at(order),
                )
                .unwrap()
                .unwrap();
            sealed.push(seal_process_row(&telemetry, writer, order, &row).unwrap());
            order += 1;
        }
        for path in [&first, &second] {
            let mut file = std::fs::File::create(path).unwrap();
            file.write_all(b"x").unwrap();
        }
        let exit_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while observer.fold().nodes().any(|node| node.exit.is_none())
            && std::time::Instant::now() < exit_deadline
        {
            let _ = observer.wait();
        }
        let _ = child.wait();
        for (index, identity, parent, _) in &births {
            let process = observer.fold().sample(*index as usize, &Unread);
            if process.exit.is_none() {
                continue;
            }
            let row = projection
                .project(
                    RetainedProcessEvent {
                        event: &JobProcessEvent::Exited {
                            index: *index,
                            process,
                        },
                        identity: Some(*identity),
                        parent: *parent,
                    },
                    at(order),
                )
                .unwrap()
                .unwrap();
            sealed.push(seal_process_row(&telemetry, writer, order, &row).unwrap());
            order += 1;
        }
        let rows: Vec<_> = sealed
            .iter()
            .map(|path| read_process_segment(path).unwrap())
            .collect();
        let starts: Vec<_> = rows
            .iter()
            .filter(|row| row.entry_type == "span-start")
            .collect();
        assert_eq!(starts.len(), 3);
        assert_eq!(
            starts.iter().filter(|row| row.proc_argv.is_some()).count(),
            3
        );
        let root_span = starts[0].span_id;
        assert_eq!(starts[0].parent_span_id, JOB_SPAN_ID);
        assert_eq!(starts[1].parent_span_id, root_span);
        assert_eq!(starts[2].parent_span_id, root_span);
        assert!(
            rows.iter()
                .filter(|row| row.entry_type == "span-ok" || row.entry_type == "span-err")
                .count()
                >= 1
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
