//! A job's processes as lmao `process.run` spans (13_telemetry.md, "Process-tree spans").
//!
//! [`ProcessSpanProjection`] projects each [`JobProcessEvent`] a job's process fold emits into at
//! most one [`ProcessSpanRow`]. The life at index `i` in birth order is span `i + 2` of the job's
//! lmao thread, beside the job span (span 1). A root hangs from the job span; a forked life hangs
//! from the span of the life the observer retained as its parent, fenced by birth identity and
//! never looked up again by pid. Every row of a span carries that parent address.
//!
//! The fourteen custom columns are declared once, in [`ProcessColumns`]: [`process_custom_fields`]
//! is their Arrow schema and [`process_column_arrays`] the only encoder. [`seal_process_batch`]
//! seals one fold batch as one segment, each row keeping its own timestamp and address, and
//! [`read_process_segment`] decodes a segment back, row by row.
//!
//! A row's kind ([`ProcessRowKind`]) is its lmao entry type and a fixed message, never a column.
//! `span-start` and `span-ok`/`span-err` are `process.run`; every other row is an `info` row:
//! `process.exec`, `process.busy`, `process.idle`, `process.usage`, `process.heartbeat`,
//! `process.blocked` and `process.blocker-cleared`. A row sets only the columns that changed; a
//! null column is unchanged. A span starts idle, and only its busy/idle rows change that.

use std::collections::HashMap;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Int32Type, TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_ipc::reader::StreamReader;
use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::{CommandArg, ExitStatus};
use crate::api::process::{
    BlockerChange, JobProcessDelta, JobProcessEvent, JobProcessSample, ProcessBlockedOn,
    ProcessStorageIo, ProcessUsage,
};
use crate::runtime::job_spans::{JOB_SPAN_ID, SpanOutcome};
use crate::runtime::process_tree::ProcessIdentity;
use crate::storage::trace_columns::{
    Category, Enum, I32, Optional, Text, TraceEnum, U32, U64, trace_columns, trace_enum,
};
use crate::storage::trace_segment::{
    EntryType, SealStep, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError,
    seal_segment, system_fields, trace_batch,
};

/// The name of a process span's start and end rows.
pub const PROCESS_RUN: &str = "process.run";

trace_enum! {
    /// What a process waits on: [`ProcessBlockedOn`] without its path and holder, which the
    /// blocker columns carry beside it.
    pub enum BlockedOn {
        None = "none",
        Lock = "lock",
        Socket = "socket",
        Pipe = "pipe",
        Child = "child",
        Stdin = "stdin",
        Disk = "disk",
    }
}

trace_enum! {
    /// Every signal a wait status can name, on any host: the named signals of macOS and Linux,
    /// then Linux's real-time signals by their kernel number. One dictionary everywhere, so a
    /// query names a signal alike whichever kernel numbered it.
    pub enum ExitSignal {
        Hup = "SIGHUP",
        Int = "SIGINT",
        Quit = "SIGQUIT",
        Ill = "SIGILL",
        Trap = "SIGTRAP",
        Abrt = "SIGABRT",
        Emt = "SIGEMT",
        Fpe = "SIGFPE",
        Kill = "SIGKILL",
        Bus = "SIGBUS",
        Segv = "SIGSEGV",
        Sys = "SIGSYS",
        Pipe = "SIGPIPE",
        Alrm = "SIGALRM",
        Term = "SIGTERM",
        Urg = "SIGURG",
        Stop = "SIGSTOP",
        Tstp = "SIGTSTP",
        Cont = "SIGCONT",
        Chld = "SIGCHLD",
        Ttin = "SIGTTIN",
        Ttou = "SIGTTOU",
        Io = "SIGIO",
        Xcpu = "SIGXCPU",
        Xfsz = "SIGXFSZ",
        Vtalrm = "SIGVTALRM",
        Prof = "SIGPROF",
        Winch = "SIGWINCH",
        Info = "SIGINFO",
        Usr1 = "SIGUSR1",
        Usr2 = "SIGUSR2",
        Stkflt = "SIGSTKFLT",
        Pwr = "SIGPWR",
        Rt32 = "SIG32",
        Rt33 = "SIG33",
        Rt34 = "SIG34",
        Rt35 = "SIG35",
        Rt36 = "SIG36",
        Rt37 = "SIG37",
        Rt38 = "SIG38",
        Rt39 = "SIG39",
        Rt40 = "SIG40",
        Rt41 = "SIG41",
        Rt42 = "SIG42",
        Rt43 = "SIG43",
        Rt44 = "SIG44",
        Rt45 = "SIG45",
        Rt46 = "SIG46",
        Rt47 = "SIG47",
        Rt48 = "SIG48",
        Rt49 = "SIG49",
        Rt50 = "SIG50",
        Rt51 = "SIG51",
        Rt52 = "SIG52",
        Rt53 = "SIG53",
        Rt54 = "SIG54",
        Rt55 = "SIG55",
        Rt56 = "SIG56",
        Rt57 = "SIG57",
        Rt58 = "SIG58",
        Rt59 = "SIG59",
        Rt60 = "SIG60",
        Rt61 = "SIG61",
        Rt62 = "SIG62",
        Rt63 = "SIG63",
        Rt64 = "SIG64",
    }
}

impl ExitSignal {
    /// Linux's real-time signals `32..=64`, indexed by their offset from 32.
    #[cfg(target_os = "linux")]
    const REAL_TIME: [Self; 33] = [
        Self::Rt32,
        Self::Rt33,
        Self::Rt34,
        Self::Rt35,
        Self::Rt36,
        Self::Rt37,
        Self::Rt38,
        Self::Rt39,
        Self::Rt40,
        Self::Rt41,
        Self::Rt42,
        Self::Rt43,
        Self::Rt44,
        Self::Rt45,
        Self::Rt46,
        Self::Rt47,
        Self::Rt48,
        Self::Rt49,
        Self::Rt50,
        Self::Rt51,
        Self::Rt52,
        Self::Rt53,
        Self::Rt54,
        Self::Rt55,
        Self::Rt56,
        Self::Rt57,
        Self::Rt58,
        Self::Rt59,
        Self::Rt60,
        Self::Rt61,
        Self::Rt62,
        Self::Rt63,
        Self::Rt64,
    ];

    /// The host's signal `signal`, or `None` for a number this host never delivers.
    pub fn from_raw(signal: i32) -> Option<Self> {
        Some(match signal {
            libc::SIGHUP => Self::Hup,
            libc::SIGINT => Self::Int,
            libc::SIGQUIT => Self::Quit,
            libc::SIGILL => Self::Ill,
            libc::SIGTRAP => Self::Trap,
            libc::SIGABRT => Self::Abrt,
            libc::SIGFPE => Self::Fpe,
            libc::SIGKILL => Self::Kill,
            libc::SIGBUS => Self::Bus,
            libc::SIGSEGV => Self::Segv,
            libc::SIGSYS => Self::Sys,
            libc::SIGPIPE => Self::Pipe,
            libc::SIGALRM => Self::Alrm,
            libc::SIGTERM => Self::Term,
            libc::SIGURG => Self::Urg,
            libc::SIGSTOP => Self::Stop,
            libc::SIGTSTP => Self::Tstp,
            libc::SIGCONT => Self::Cont,
            libc::SIGCHLD => Self::Chld,
            libc::SIGTTIN => Self::Ttin,
            libc::SIGTTOU => Self::Ttou,
            libc::SIGIO => Self::Io,
            libc::SIGXCPU => Self::Xcpu,
            libc::SIGXFSZ => Self::Xfsz,
            libc::SIGVTALRM => Self::Vtalrm,
            libc::SIGPROF => Self::Prof,
            libc::SIGWINCH => Self::Winch,
            libc::SIGUSR1 => Self::Usr1,
            libc::SIGUSR2 => Self::Usr2,
            #[cfg(target_os = "macos")]
            libc::SIGEMT => Self::Emt,
            #[cfg(target_os = "macos")]
            libc::SIGINFO => Self::Info,
            #[cfg(target_os = "linux")]
            libc::SIGSTKFLT => Self::Stkflt,
            #[cfg(target_os = "linux")]
            libc::SIGPWR => Self::Pwr,
            #[cfg(target_os = "linux")]
            real_time @ 32..=64 => {
                let offset = usize::try_from(real_time - 32).ok()?;
                *Self::REAL_TIME.get(offset)?
            }
            _ => return None,
        })
    }
}

trace_columns! {
    /// The fourteen custom columns every process span and row shares. A null cell is unchanged on
    /// every row, except the blocker columns of a `process.blocked` row, which state their values
    /// whole.
    #[derive(Default)]
    pub struct ProcessColumns, fields: process_custom_fields, arrays: process_column_arrays;
    /// Observed process PID, fenced by its retained birth identity.
    proc_pid: Optional<U32>,
    /// The program of the last observed exec.
    proc_program: Optional<Category>,
    /// The deterministic argv display ([`argv_display`]), on the span's start row only.
    proc_argv: Optional<Text>,
    /// Cumulative own-process user CPU microseconds.
    cpu_user_us: Optional<U64>,
    /// Cumulative own-process system CPU microseconds.
    cpu_sys_us: Optional<U64>,
    /// Current resident bytes.
    rss_bytes: Optional<U64>,
    /// Peak observed resident bytes.
    rss_peak_bytes: Optional<U64>,
    /// Process storage I/O read bytes; null while the source gives none.
    io_read_bytes: Optional<U64>,
    /// Process storage I/O write bytes; null while the source gives none.
    io_write_bytes: Optional<U64>,
    /// The ordinary exit code, on the end row of a process that exited.
    exit_code: Optional<I32>,
    /// The terminating signal, on the end row of a process a signal ended.
    exit_signal: Optional<Enum<ExitSignal>>,
    /// The observed blocker; null where none was observed.
    blocked_on: Optional<Enum<BlockedOn>>,
    /// The observed blocker's path, when it has one.
    blocked_path: Optional<Category>,
    /// The evidence-backed lock holder's PID, when known.
    blocked_holder_pid: Optional<U32>,
}

/// argv as one deterministic line, never parsed back: the typed process event keeps the exact
/// bytes. Arguments are separated by one space. An argument of only `[A-Za-z0-9_@%+=:,./-]` is
/// written bare; any other, the empty one included, in `$'...'` quotes with `\\` and `\'`
/// escaped, each byte of a control character or of an invalid UTF-8 sequence as `\xHH`, and
/// every other character as written. No byte is lost and none is replaced.
pub fn argv_display(argv: &[CommandArg]) -> String {
    let mut display = String::new();
    for (position, argument) in argv.iter().enumerate() {
        if position > 0 {
            display.push(' ');
        }
        let bytes = argument.as_os_str().as_bytes();
        let bare = !bytes.is_empty()
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(byte));
        if bare {
            display.extend(bytes.iter().map(|&byte| char::from(byte)));
            continue;
        }
        display.push_str("$'");
        for chunk in bytes.utf8_chunks() {
            for character in chunk.valid().chars() {
                match character {
                    '\\' => display.push_str("\\\\"),
                    '\'' => display.push_str("\\'"),
                    control if control.is_control() => {
                        let mut encoded = [0; 4];
                        for byte in control.encode_utf8(&mut encoded).bytes() {
                            push_hex(&mut display, byte);
                        }
                    }
                    other => display.push(other),
                }
            }
            for &byte in chunk.invalid() {
                push_hex(&mut display, byte);
            }
        }
        display.push('\'');
    }
    display
}

fn push_hex(display: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    display.push_str("\\x");
    display.push(char::from(HEX[usize::from(byte >> 4)]));
    display.push(char::from(HEX[usize::from(byte & 0xf)]));
}

/// What a row records, as its lmao entry type and message. Every row but a span's start and end
/// is `info`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessRowKind {
    Born,
    Exec,
    Busy,
    Idle,
    Usage,
    Heartbeat,
    Blocked,
    BlockerCleared,
    Exited(SpanOutcome),
}

impl ProcessRowKind {
    pub fn entry_type(self) -> EntryType {
        match self {
            Self::Born => EntryType::SpanStart,
            Self::Exited(SpanOutcome::Ok) => EntryType::SpanOk,
            Self::Exited(SpanOutcome::Err) => EntryType::SpanErr,
            Self::Exec
            | Self::Busy
            | Self::Idle
            | Self::Usage
            | Self::Heartbeat
            | Self::Blocked
            | Self::BlockerCleared => EntryType::Info,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Born | Self::Exited(_) => PROCESS_RUN,
            Self::Exec => "process.exec",
            Self::Busy => "process.busy",
            Self::Idle => "process.idle",
            Self::Usage => "process.usage",
            Self::Heartbeat => "process.heartbeat",
            Self::Blocked => "process.blocked",
            Self::BlockerCleared => "process.blocker-cleared",
        }
    }
}

/// One observed event plus the identity the observer retained for it. A gap has neither
/// identity: it names no life. `parent` is the retained parent, consulted only when the life
/// is born; `None` hangs that span from the job span.
pub struct RetainedProcessEvent<'a> {
    pub event: &'a JobProcessEvent,
    pub identity: Option<ProcessIdentity>,
    pub parent: Option<ProcessIdentity>,
}

/// Why a process span could not be projected, sealed or read. Each is a value for the caller to
/// count; none is a panic.
#[derive(Debug, Error)]
pub enum ProcessSpanError {
    #[error("process event {index} names no retained identity")]
    MissingIdentity { index: u32 },
    #[error("process {index} was born twice")]
    OpenedTwice { index: u32 },
    #[error("process {index} was not born")]
    NotOpened { index: u32 },
    #[error("process {index} names retained parent {pid} that was not born")]
    MissingParent { index: u32, pid: u32 },
    #[error("process {index} was already closed")]
    AfterExit { index: u32 },
    #[error("process {index} has no span id below the job span")]
    IndexOverflow { index: u32 },
    #[error("process event names identity {pid} but span {index} was opened for {opened}")]
    IdentityMismatch { index: u32, pid: u32, opened: u32 },
    #[error("process {index} exited without a status")]
    ExitWithoutStatus { index: u32 },
    #[error("terminating signal {signal} has no declared name")]
    UnknownSignal { signal: i32 },
    #[error("process span time is before the Unix epoch")]
    BeforeEpoch,
    #[error("process span time exceeds i64 nanoseconds")]
    BeyondNanoseconds,
    #[error("a process segment seals at least one row")]
    NoRows,
    #[error("process segment is not rows of the fourteen-column schema")]
    Schema,
    #[error(transparent)]
    Segment(#[from] TraceSegmentError),
}

/// One row of one process span, in the job's lmao thread.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessSpanRow {
    at: SystemTime,
    trace_id: String,
    span: SpanAddress,
    parent: SpanAddress,
    kind: ProcessRowKind,
    columns: ProcessColumns,
}

impl ProcessSpanRow {
    pub fn span_id(&self) -> u32 {
        self.span.span_id
    }

    pub fn parent_span_id(&self) -> u32 {
        self.parent.span_id
    }

    pub fn kind(&self) -> ProcessRowKind {
        self.kind
    }

    pub fn message(&self) -> &'static str {
        self.kind.message()
    }

    pub fn columns(&self) -> &ProcessColumns {
        &self.columns
    }
}

/// The two span addresses a life's rows share, by its index in the job's span numbering.
#[derive(Clone, Copy, Debug)]
struct SpanIds {
    span_id: u32,
    parent_span_id: u32,
}

#[derive(Clone, Debug)]
struct OpenSpan {
    ids: SpanIds,
    identity: ProcessIdentity,
    program: String,
    busy: bool,
    /// The newest usage this span has observed, with an unavailable storage pair replaced by the
    /// last one it read: the counters a later row is compared against.
    usage: Option<ProcessUsage>,
    closed: bool,
}

impl OpenSpan {
    /// Record `usage` as the span's newest reading: the counters that moved since its last row,
    /// and the busy/idle flip it made, if any.
    fn observe(&mut self, usage: &ProcessUsage) -> (ProcessColumns, Option<bool>) {
        let mut columns = ProcessColumns::default();
        write_usage(&mut columns, self.usage.as_ref(), usage, false);
        let flip = (self.busy != usage.busy).then_some(usage.busy);
        self.busy = usage.busy;
        let io = match (usage.io, self.usage) {
            (ProcessStorageIo::Unavailable { .. }, Some(previous)) => previous.io,
            _ => usage.io,
        };
        self.usage = Some(ProcessUsage { io, ..*usage });
        (columns, flip)
    }
}

/// Projects one job's observed process events onto its trace thread.
#[derive(Debug)]
pub struct ProcessSpanProjection {
    trace_id: String,
    thread: u64,
    by_identity: HashMap<ProcessIdentity, u32>,
    open: Vec<OpenSpan>,
}

impl ProcessSpanProjection {
    /// A projection for the job whose spans live on `thread`.
    pub fn new(trace_id: impl Into<String>, thread: u64) -> Self {
        Self {
            trace_id: trace_id.into(),
            thread,
            by_identity: HashMap::new(),
            open: Vec::new(),
        }
    }

    /// The row `observed` seals at `at`, or `None` when the event seals no row: a coverage gap,
    /// or a usage sample that moved no counter and flipped no busy state. A refused event changes
    /// no span.
    pub fn project(
        &mut self,
        observed: RetainedProcessEvent<'_>,
        at: SystemTime,
    ) -> Result<Option<ProcessSpanRow>, ProcessSpanError> {
        let identity = observed.identity;
        match observed.event {
            JobProcessEvent::Gap { .. } => Ok(None),
            JobProcessEvent::Born { index, process } => self
                .born(*index, identity, observed.parent, process, at)
                .map(Some),
            JobProcessEvent::Exec { index, process } => {
                let span = self.live(*index, identity)?;
                let ids = span.ids;
                let columns = if span.program == process.program {
                    ProcessColumns::default()
                } else {
                    ProcessColumns {
                        proc_program: Some(process.program.clone()),
                        ..ProcessColumns::default()
                    }
                };
                span.program.clone_from(&process.program);
                Ok(Some(self.row(ids, ProcessRowKind::Exec, at, columns)))
            }
            JobProcessEvent::Changed(JobProcessDelta::Usage { index, usage }) => {
                let span = self.live(*index, identity)?;
                let ids = span.ids;
                let (columns, flip) = span.observe(usage);
                if flip.is_none() && columns == ProcessColumns::default() {
                    return Ok(None);
                }
                Ok(Some(self.row(
                    ids,
                    sampled_kind(flip, ProcessRowKind::Usage),
                    at,
                    columns,
                )))
            }
            JobProcessEvent::Changed(JobProcessDelta::Blocker { index, blocked_on }) => {
                let ids = self.live(*index, identity)?.ids;
                let (kind, columns) = match blocked_on {
                    BlockerChange::Set(blocked) => {
                        let mut columns = ProcessColumns::default();
                        write_blocker(&mut columns, blocked);
                        (ProcessRowKind::Blocked, columns)
                    }
                    BlockerChange::Clear => {
                        (ProcessRowKind::BlockerCleared, ProcessColumns::default())
                    }
                };
                Ok(Some(self.row(ids, kind, at, columns)))
            }
            JobProcessEvent::Heartbeat { index, process } => {
                let span = self.live(*index, identity)?;
                let ids = span.ids;
                let (columns, flip) = match &process.usage {
                    Some(usage) => span.observe(usage),
                    None => (ProcessColumns::default(), None),
                };
                Ok(Some(self.row(
                    ids,
                    sampled_kind(flip, ProcessRowKind::Heartbeat),
                    at,
                    columns,
                )))
            }
            JobProcessEvent::Exited { index, process } => {
                self.exited(*index, identity, process, at).map(Some)
            }
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
        let opened = usize::try_from(index).map_err(|_| ProcessSpanError::NotOpened { index })?;
        if self.open.len() != opened {
            return Err(if opened < self.open.len() {
                ProcessSpanError::OpenedTwice { index }
            } else {
                ProcessSpanError::NotOpened { index }
            });
        }
        let span_id = index
            .checked_add(JOB_SPAN_ID + 1)
            .ok_or(ProcessSpanError::IndexOverflow { index })?;
        let parent_span_id =
            match parent {
                None => JOB_SPAN_ID,
                Some(parent) => self.by_identity.get(&parent).copied().ok_or(
                    ProcessSpanError::MissingParent {
                        index,
                        pid: parent.pid,
                    },
                )?,
            };
        let ids = SpanIds {
            span_id,
            parent_span_id,
        };
        let mut columns = ProcessColumns::default();
        if let Some(usage) = &process.usage {
            write_usage(&mut columns, None, usage, true);
        }
        if let Some(blocked) = &process.blocked_on {
            write_blocker(&mut columns, blocked);
        }
        columns.proc_pid = Some(process.pid);
        columns.proc_program = Some(process.program.clone());
        columns.proc_argv = Some(argv_display(&process.argv));
        self.by_identity.insert(identity, span_id);
        // A span starts idle. A busy birth record is flipped by the first row that observes it.
        self.open.push(OpenSpan {
            ids,
            identity,
            program: process.program.clone(),
            busy: false,
            usage: process.usage,
            closed: false,
        });
        Ok(self.row(ids, ProcessRowKind::Born, at, columns))
    }

    fn exited(
        &mut self,
        index: u32,
        identity: Option<ProcessIdentity>,
        process: &JobProcessSample,
        at: SystemTime,
    ) -> Result<ProcessSpanRow, ProcessSpanError> {
        let span = self.live(index, identity)?;
        let ids = span.ids;
        let exit = process
            .exit
            .as_ref()
            .ok_or(ProcessSpanError::ExitWithoutStatus { index })?;
        let mut columns = ProcessColumns::default();
        // A withheld status proves no clean exit: the span ends span-err, both exit columns null.
        let outcome = match exit.status {
            None => SpanOutcome::Err,
            Some(ExitStatus::Exited { code }) => {
                columns.exit_code = Some(code);
                if code == 0 {
                    SpanOutcome::Ok
                } else {
                    SpanOutcome::Err
                }
            }
            Some(ExitStatus::Signaled { signal, .. }) => {
                columns.exit_signal = Some(
                    ExitSignal::from_raw(signal)
                        .ok_or(ProcessSpanError::UnknownSignal { signal })?,
                );
                SpanOutcome::Err
            }
        };
        if let Some(usage) = &process.usage {
            write_usage(&mut columns, None, usage, true);
            span.usage = Some(*usage);
        }
        span.closed = true;
        Ok(self.row(ids, ProcessRowKind::Exited(outcome), at, columns))
    }

    fn live(
        &mut self,
        index: u32,
        identity: Option<ProcessIdentity>,
    ) -> Result<&mut OpenSpan, ProcessSpanError> {
        let identity = identity.ok_or(ProcessSpanError::MissingIdentity { index })?;
        let position = usize::try_from(index).map_err(|_| ProcessSpanError::NotOpened { index })?;
        let span = self
            .open
            .get_mut(position)
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
        ids: SpanIds,
        kind: ProcessRowKind,
        at: SystemTime,
        columns: ProcessColumns,
    ) -> ProcessSpanRow {
        ProcessSpanRow {
            at,
            trace_id: self.trace_id.clone(),
            span: SpanAddress {
                thread_id: self.thread,
                span_id: ids.span_id,
            },
            parent: SpanAddress {
                thread_id: self.thread,
                span_id: ids.parent_span_id,
            },
            kind,
            columns,
        }
    }
}

/// A busy/idle flip is its own row kind; any other sample keeps `steady`.
fn sampled_kind(flip: Option<bool>, steady: ProcessRowKind) -> ProcessRowKind {
    match flip {
        None => steady,
        Some(true) => ProcessRowKind::Busy,
        Some(false) => ProcessRowKind::Idle,
    }
}

/// Write into `columns` the counters of `next` that differ from `previous` (every counter when
/// `force`). Storage I/O is written only while the source reads it: an unavailable pair is null,
/// never zero.
fn write_usage(
    columns: &mut ProcessColumns,
    previous: Option<&ProcessUsage>,
    next: &ProcessUsage,
    force: bool,
) {
    fn moved(now: u64, before: Option<u64>, force: bool) -> Option<u64> {
        (force || before != Some(now)).then_some(now)
    }
    let before_io = previous.and_then(|usage| io_pair(&usage.io));
    if let Some((read, write)) = io_pair(&next.io) {
        columns.io_read_bytes = moved(read, before_io.map(|pair| pair.0), force);
        columns.io_write_bytes = moved(write, before_io.map(|pair| pair.1), force);
    }
    columns.cpu_user_us = moved(
        next.cpu_user_us.get(),
        previous.map(|usage| usage.cpu_user_us.get()),
        force,
    );
    columns.cpu_sys_us = moved(
        next.cpu_sys_us.get(),
        previous.map(|usage| usage.cpu_sys_us.get()),
        force,
    );
    columns.rss_bytes = moved(
        next.rss_bytes.get(),
        previous.map(|usage| usage.rss_bytes.get()),
        force,
    );
    columns.rss_peak_bytes = moved(
        next.rss_peak_bytes.get(),
        previous.map(|usage| usage.rss_peak_bytes.get()),
        force,
    );
}

fn io_pair(io: &ProcessStorageIo) -> Option<(u64, u64)> {
    match io {
        ProcessStorageIo::Read {
            read_bytes,
            write_bytes,
        } => Some((read_bytes.get(), write_bytes.get())),
        ProcessStorageIo::Unavailable { .. } => None,
    }
}

/// Write `blocked` whole into the three blocker columns: a blocker with no path or holder leaves
/// those columns null, so no earlier lock's detail survives it.
fn write_blocker(columns: &mut ProcessColumns, blocked: &ProcessBlockedOn) {
    let (kind, path, holder) = match blocked {
        ProcessBlockedOn::None => (BlockedOn::None, None, None),
        ProcessBlockedOn::Lock { path, holder } => (
            BlockedOn::Lock,
            Some(path.clone()),
            holder.as_ref().map(|holder| holder.pid),
        ),
        ProcessBlockedOn::Socket => (BlockedOn::Socket, None, None),
        ProcessBlockedOn::Pipe => (BlockedOn::Pipe, None, None),
        ProcessBlockedOn::Child => (BlockedOn::Child, None, None),
        ProcessBlockedOn::Stdin => (BlockedOn::Stdin, None, None),
        ProcessBlockedOn::Disk => (BlockedOn::Disk, None, None),
    };
    columns.blocked_on = Some(kind);
    columns.blocked_path = path;
    columns.blocked_holder_pid = holder;
}

/// The UTC nanoseconds of `at`, or why it has none.
fn timestamp_ns(at: SystemTime) -> Result<i64, ProcessSpanError> {
    let since_epoch = at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProcessSpanError::BeforeEpoch)?;
    i64::try_from(since_epoch.as_nanos()).map_err(|_| ProcessSpanError::BeyondNanoseconds)
}

/// Seal one fold batch's rows as one `process-<order>-<writer>.arrow` segment under `root`,
/// through the shared segment writer. Each row keeps its own timestamp and span address; the
/// segment's telemetry partition is the UTC date of its first row. An empty batch seals nothing:
/// [`ProcessSpanError::NoRows`].
pub fn seal_process_batch(
    root: &Path,
    writer: Uuid,
    order: u64,
    rows: &[ProcessSpanRow],
) -> Result<PathBuf, ProcessSpanError> {
    let first = rows.first().ok_or(ProcessSpanError::NoRows)?;
    let seconds = first
        .at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProcessSpanError::BeforeEpoch)?
        .as_secs();
    let batch = process_batch(rows)?;
    Ok(seal_segment(
        root,
        TelemetryDate::from_unix_seconds(seconds),
        &format!("process-{order:020}-{writer}"),
        &batch,
    )?)
}

fn process_batch(rows: &[ProcessSpanRow]) -> Result<RecordBatch, ProcessSpanError> {
    let mut system = SystemColumns::with_capacity(rows.len());
    for row in rows {
        system.push(SystemRow {
            timestamp_ns: timestamp_ns(row.at)?,
            trace_id: &row.trace_id,
            span: row.span,
            parent: Some(row.parent),
            entry_type: row.kind.entry_type(),
            message: Some(row.kind.message()),
        })?;
    }
    let custom = process_column_arrays(rows.iter().map(|row| &row.columns))
        .map_err(TraceSegmentError::from)?;
    Ok(trace_batch(system, custom)?)
}

/// One row read back from a sealed process segment, in the order it was sealed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedProcessRow {
    pub timestamp_ns: i64,
    pub span_id: u32,
    pub parent_span_id: u32,
    pub entry_type: String,
    pub message: String,
    pub columns: ProcessColumns,
}

/// Read every row of a segment sealed by [`seal_process_batch`]. A fifteenth custom column, a
/// renamed column, a dictionary that is not the declared vocabulary, or a segment with no rows is
/// [`ProcessSpanError::Schema`] or [`ProcessSpanError::NoRows`].
pub fn read_process_segment(path: &Path) -> Result<Vec<DecodedProcessRow>, ProcessSpanError> {
    let file = std::fs::File::open(path).map_err(|source| {
        ProcessSpanError::Segment(TraceSegmentError::Io {
            step: SealStep::CreatingSegment,
            source,
        })
    })?;
    let reader = StreamReader::try_new(file, None)
        .map_err(|error| ProcessSpanError::Segment(TraceSegmentError::from(error)))?;
    let mut declared = system_fields();
    let start = declared.len();
    declared.extend(process_custom_fields());
    let mut rows = Vec::new();
    for batch in reader {
        let batch =
            batch.map_err(|error| ProcessSpanError::Segment(TraceSegmentError::from(error)))?;
        let schema = batch.schema();
        let found = schema.fields();
        if found.len() != declared.len()
            || found.iter().zip(&declared).any(|(actual, expected)| {
                actual.name() != expected.name()
                    || actual.data_type() != expected.data_type()
                    || actual.is_nullable() != expected.is_nullable()
            })
        {
            return Err(ProcessSpanError::Schema);
        }
        for row in 0..batch.num_rows() {
            rows.push(decode_row(&batch, start, row)?);
        }
    }
    if rows.is_empty() {
        return Err(ProcessSpanError::NoRows);
    }
    Ok(rows)
}

fn decode_row(
    batch: &RecordBatch,
    start: usize,
    row: usize,
) -> Result<DecodedProcessRow, ProcessSpanError> {
    Ok(DecodedProcessRow {
        timestamp_ns: optional_primitive::<TimestampNanosecondType>(batch, 0, row, "timestamp")?
            .ok_or(ProcessSpanError::Schema)?,
        span_id: optional_primitive::<UInt32Type>(batch, 0, row, "span_id")?
            .ok_or(ProcessSpanError::Schema)?,
        parent_span_id: optional_primitive::<UInt32Type>(batch, 0, row, "parent_span_id")?
            .ok_or(ProcessSpanError::Schema)?,
        entry_type: dict_value(column(batch, 0, "entry_type")?, row)?
            .ok_or(ProcessSpanError::Schema)?,
        message: dict_value(column(batch, 0, "message")?, row)?.ok_or(ProcessSpanError::Schema)?,
        columns: decode_columns(batch, start, row)?,
    })
}

fn decode_columns(
    batch: &RecordBatch,
    start: usize,
    row: usize,
) -> Result<ProcessColumns, ProcessSpanError> {
    Ok(ProcessColumns {
        proc_pid: optional_primitive::<UInt32Type>(batch, start, row, "proc_pid")?,
        proc_program: optional_dict(batch, start, row, "proc_program")?,
        proc_argv: optional_dict(batch, start, row, "proc_argv")?,
        cpu_user_us: optional_primitive::<UInt64Type>(batch, start, row, "cpu_user_us")?,
        cpu_sys_us: optional_primitive::<UInt64Type>(batch, start, row, "cpu_sys_us")?,
        rss_bytes: optional_primitive::<UInt64Type>(batch, start, row, "rss_bytes")?,
        rss_peak_bytes: optional_primitive::<UInt64Type>(batch, start, row, "rss_peak_bytes")?,
        io_read_bytes: optional_primitive::<UInt64Type>(batch, start, row, "io_read_bytes")?,
        io_write_bytes: optional_primitive::<UInt64Type>(batch, start, row, "io_write_bytes")?,
        exit_code: optional_primitive::<Int32Type>(batch, start, row, "exit_code")?,
        exit_signal: optional_enum(batch, start, row, "exit_signal", ExitSignal::ALL)?,
        blocked_on: optional_enum(batch, start, row, "blocked_on", BlockedOn::ALL)?,
        blocked_path: optional_dict(batch, start, row, "blocked_path")?,
        blocked_holder_pid: optional_primitive::<UInt32Type>(
            batch,
            start,
            row,
            "blocked_holder_pid",
        )?,
    })
}

/// The custom column `name`, which must lie past the system columns.
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

fn optional_primitive<T: ArrowPrimitiveType>(
    batch: &RecordBatch,
    start: usize,
    row: usize,
    name: &str,
) -> Result<Option<T::Native>, ProcessSpanError> {
    let column = column(batch, start, name)?;
    if column.is_null(row) {
        return Ok(None);
    }
    Ok(Some(column.as_primitive::<T>().value(row)))
}

fn optional_dict(
    batch: &RecordBatch,
    start: usize,
    row: usize,
    name: &str,
) -> Result<Option<String>, ProcessSpanError> {
    dict_value(column(batch, start, name)?, row)
}

/// An `S.enum` column: its dictionary must be exactly `E`'s declared names, and the key names one
/// of `all`, which the declaration lists in the same order.
fn optional_enum<E: TraceEnum>(
    batch: &RecordBatch,
    start: usize,
    row: usize,
    name: &str,
    all: &[E],
) -> Result<Option<E>, ProcessSpanError> {
    let column = column(batch, start, name)?;
    let dict = column
        .as_dictionary_opt::<UInt16Type>()
        .ok_or(ProcessSpanError::Schema)?;
    let values = dict.values().as_string::<i32>();
    if values.len() != E::NAMES.len()
        || E::NAMES
            .iter()
            .enumerate()
            .any(|(key, expected)| values.value(key) != *expected)
    {
        return Err(ProcessSpanError::Schema);
    }
    if column.is_null(row) {
        return Ok(None);
    }
    let key = usize::from(dict.keys().value(row));
    all.get(key)
        .copied()
        .map(Some)
        .ok_or(ProcessSpanError::Schema)
}

/// The name a dictionary cell holds: `None` for a null cell, an error for a key the dictionary
/// does not hold or a column that is not a dictionary.
fn dict_value(column: &ArrayRef, row: usize) -> Result<Option<String>, ProcessSpanError> {
    if column.is_null(row) {
        return Ok(None);
    }
    if let Some(dict) = column.as_dictionary_opt::<UInt32Type>() {
        let key = usize::try_from(dict.keys().value(row)).map_err(|_| ProcessSpanError::Schema)?;
        return dictionary_entry(dict.values(), key).map(Some);
    }
    if let Some(dict) = column.as_dictionary_opt::<UInt8Type>() {
        return dictionary_entry(dict.values(), usize::from(dict.keys().value(row))).map(Some);
    }
    Err(ProcessSpanError::Schema)
}

fn dictionary_entry(values: &ArrayRef, key: usize) -> Result<String, ProcessSpanError> {
    let values = values.as_string::<i32>();
    if key >= values.len() {
        return Err(ProcessSpanError::Schema);
    }
    Ok(values.value(key).to_owned())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{DictionaryArray, RecordBatch, StringArray, UInt16Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::api::dto::UtcTimestamp;
    use crate::api::process::{LockHolder, ProcessCoverageGap, ProcessExit, ProcessIoUnavailable};
    use crate::api::resources::{CpuMicros, ResidentBytes, StorageIoBytes};
    use crate::runtime::process_tree::BirthToken;
    use crate::storage::trace_segment::dict_type;

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
            // A busy usage was judged so over a window that closed: a whole core for a second.
            cpu_window: busy.then(|| crate::api::process::ProcessCpuWindow {
                opened_us: crate::api::resources::WallMicros::new(0).unwrap(),
                closed_us: crate::api::resources::WallMicros::new(1_000_000).unwrap(),
                cpu_permille: crate::api::resources::OneCoreCpuPermille::new(1_000).unwrap(),
            }),
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
            truncated: false,
            born_at: UtcTimestamp::new("2026-10-12T00:00:00Z").unwrap(),
            usage: None,
            blocked_on: None,
            exit: None,
        }
    }

    /// Project one event of the life `who` at `second`.
    fn project(
        projection: &mut ProcessSpanProjection,
        event: &JobProcessEvent,
        who: ProcessIdentity,
        parent: Option<ProcessIdentity>,
        second: u64,
    ) -> Option<ProcessSpanRow> {
        projection
            .project(
                RetainedProcessEvent {
                    event,
                    identity: Some(who),
                    parent,
                },
                at(second),
            )
            .unwrap()
    }

    fn born(
        projection: &mut ProcessSpanProjection,
        index: u32,
        who: ProcessIdentity,
        parent: Option<ProcessIdentity>,
        process: &JobProcessSample,
    ) -> ProcessSpanRow {
        project(
            projection,
            &JobProcessEvent::Born {
                index,
                process: process.clone(),
            },
            who,
            parent,
            1,
        )
        .unwrap()
    }

    fn argv(arguments: &[&[u8]]) -> Vec<CommandArg> {
        arguments
            .iter()
            .map(|argument| CommandArg::new(OsString::from_vec(argument.to_vec())))
            .collect()
    }

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("cowshed-span-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        root
    }

    #[test]
    fn the_declaration_is_exactly_fourteen_columns() {
        let fields = process_custom_fields();
        let names: Vec<&str> = fields.iter().map(|field| field.name().as_str()).collect();
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
        let data_type = |name: &str| {
            fields
                .iter()
                .find(|field| field.name() == name)
                .expect("a declared column")
                .data_type()
                .clone()
        };
        assert_eq!(data_type("proc_pid"), DataType::UInt32);
        assert_eq!(data_type("proc_program"), dict_type(DataType::UInt32));
        assert_eq!(data_type("proc_argv"), dict_type(DataType::UInt32));
        assert_eq!(data_type("cpu_user_us"), DataType::UInt64);
        assert_eq!(data_type("exit_code"), DataType::Int32);
        assert_eq!(data_type("exit_signal"), dict_type(DataType::UInt16));
        assert_eq!(data_type("blocked_on"), dict_type(DataType::UInt16));
        assert_eq!(data_type("blocked_path"), dict_type(DataType::UInt32));
        assert_eq!(data_type("blocked_holder_pid"), DataType::UInt32);
    }

    #[test]
    fn argv_display_keeps_every_byte_and_quotes_deterministically() {
        assert_eq!(
            argv_display(&argv(&[b"cc", b"-o", b"out/a.o", b"x=1,y@2:%+"])),
            "cc -o out/a.o x=1,y@2:%+"
        );
        assert_eq!(
            argv_display(&argv(&[
                b"",
                b"two words",
                b"it's",
                b"back\\slash",
                b"tab\tnew\nline\x7f",
                "caf\u{e9} \u{85}".as_bytes(),
                b"\xff\xfe-not-utf8",
            ])),
            "$'' $'two words' $'it\\'s' $'back\\\\slash' $'tab\\x09new\\x0aline\\x7f' \
             $'caf\u{e9} \\xc2\\x85' $'\\xff\\xfe-not-utf8'"
        );
        assert_eq!(argv_display(&[]), "");
    }

    #[test]
    fn every_signal_this_host_delivers_has_one_declared_name() {
        let highest = if cfg!(target_os = "linux") { 64 } else { 31 };
        let mut names = (1..=highest)
            .map(|signal| {
                ExitSignal::from_raw(signal)
                    .unwrap_or_else(|| panic!("signal {signal} is named"))
                    .name()
            })
            .collect::<Vec<_>>();
        assert_eq!(ExitSignal::from_raw(libc::SIGTERM), Some(ExitSignal::Term));
        assert_eq!(ExitSignal::from_raw(libc::SIGKILL), Some(ExitSignal::Kill));
        assert_eq!(ExitSignal::from_raw(0), None);
        assert_eq!(ExitSignal::from_raw(highest + 1), None);
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            usize::try_from(highest).unwrap(),
            "no two signals share a name"
        );
        assert_eq!(
            ExitSignal::NAMES.len(),
            ExitSignal::ALL.len(),
            "one name per declared signal"
        );
    }

    #[test]
    fn a_blocker_set_states_its_three_columns_whole() {
        let mut columns = ProcessColumns {
            blocked_path: Some("/tmp/stale".to_owned()),
            blocked_holder_pid: Some(9),
            ..ProcessColumns::default()
        };
        write_blocker(&mut columns, &ProcessBlockedOn::Pipe);
        assert_eq!(
            (
                columns.blocked_on,
                columns.blocked_path,
                columns.blocked_holder_pid
            ),
            (Some(BlockedOn::Pipe), None, None)
        );
    }

    #[test]
    fn parentage_follows_retained_identity_not_pid() {
        let root = identity(10, 1);
        let child = identity(11, 2);
        let mut projection = ProcessSpanProjection::new("trace", 99);
        let root_row = born(
            &mut projection,
            0,
            root,
            None,
            &sample(10, 1, "/bin/bash", &["/bin/bash", "-c", "cat"]),
        );
        assert_eq!(root_row.parent_span_id(), JOB_SPAN_ID);
        // A sample ppid is not a parent edge. A retained parent that was never born refuses the
        // birth, rather than hanging the span from the job span.
        let missing = projection.project(
            RetainedProcessEvent {
                event: &JobProcessEvent::Born {
                    index: 1,
                    process: sample(11, 10, "/bin/cat", &["/bin/cat"]),
                },
                identity: Some(child),
                parent: Some(identity(10, 9)),
            },
            at(1),
        );
        assert!(matches!(
            missing,
            Err(ProcessSpanError::MissingParent { index: 1, pid: 10 })
        ));
        let real = born(
            &mut projection,
            1,
            identity(12, 3),
            Some(root),
            &sample(12, 99, "/bin/cat", &["/bin/cat", "fifo"]),
        );
        assert_eq!(real.parent_span_id(), root_row.span_id());
        // Every row of the span hangs from its retained parent. An exec of a new program states it;
        // an exec of the same program is a row of its own with no column set.
        let exec = project(
            &mut projection,
            &JobProcessEvent::Exec {
                index: 1,
                process: sample(12, 99, "/usr/bin/cat", &["cat", "fifo"]),
            },
            identity(12, 3),
            Some(root),
            2,
        )
        .unwrap();
        assert_eq!(exec.message(), "process.exec");
        assert_eq!(exec.columns().proc_program.as_deref(), Some("/usr/bin/cat"));
        assert_eq!(exec.span_id(), real.span_id());
        assert_eq!(exec.parent_span_id(), root_row.span_id());
        let same = project(
            &mut projection,
            &JobProcessEvent::Exec {
                index: 1,
                process: sample(12, 99, "/usr/bin/cat", &["cat", "fifo"]),
            },
            identity(12, 3),
            Some(root),
            3,
        )
        .unwrap();
        assert_eq!(same.kind(), ProcessRowKind::Exec);
        assert_eq!(same.columns(), &ProcessColumns::default());
    }

    #[test]
    fn a_span_starts_idle_so_a_busy_observation_is_its_first_busy_row() {
        let who = identity(8, 1);
        let mut projection = ProcessSpanProjection::new("trace", 3);
        let mut process = sample(8, 1, "/bin/cat", &["/bin/cat"]);
        process.usage = Some(usage(1_000, true, 4096));
        born(&mut projection, 0, who, None, &process);
        // Busy with no counter moved: the flip is the row, and every column is null.
        let busy = project(
            &mut projection,
            &JobProcessEvent::Heartbeat {
                index: 0,
                process: process.clone(),
            },
            who,
            None,
            61,
        )
        .unwrap();
        assert_eq!(busy.kind(), ProcessRowKind::Busy);
        assert_eq!(busy.columns(), &ProcessColumns::default());
        let idle = project(
            &mut projection,
            &JobProcessEvent::Changed(JobProcessDelta::Usage {
                index: 0,
                usage: usage(1_000, false, 4096),
            }),
            who,
            None,
            62,
        )
        .unwrap();
        assert_eq!(idle.kind(), ProcessRowKind::Idle);
        assert_eq!(idle.message(), "process.idle");
        assert_eq!(idle.columns(), &ProcessColumns::default());
        let rss_step = project(
            &mut projection,
            &JobProcessEvent::Changed(JobProcessDelta::Usage {
                index: 0,
                usage: usage(1_000, false, 8192),
            }),
            who,
            None,
            63,
        )
        .unwrap();
        assert_eq!(rss_step.message(), "process.usage");
        assert_eq!(rss_step.columns().rss_bytes, Some(8192));
        assert_eq!(rss_step.columns().cpu_user_us, None);
        let unchanged = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Changed(JobProcessDelta::Usage {
                        index: 0,
                        usage: usage(1_000, false, 8192),
                    }),
                    identity: Some(who),
                    parent: None,
                },
                at(64),
            )
            .unwrap();
        assert!(unchanged.is_none(), "an unchanged sample is not a row");
    }

    #[test]
    fn the_blocker_rows_seal_as_one_segment_with_each_row_whole() {
        let who = identity(4, 1);
        let mut projection = ProcessSpanProjection::new("trace", 5);
        let start = born(
            &mut projection,
            0,
            who,
            None,
            &sample(4, 1, "/bin/cat", &["/bin/cat"]),
        );
        let set = project(
            &mut projection,
            &JobProcessEvent::Changed(JobProcessDelta::Blocker {
                index: 0,
                blocked_on: BlockerChange::Set(ProcessBlockedOn::Lock {
                    path: "/tmp/held".to_owned(),
                    holder: Some(LockHolder {
                        pid: 9,
                        job: None,
                        program: None,
                    }),
                }),
            }),
            who,
            None,
            2,
        )
        .unwrap();
        let clear = project(
            &mut projection,
            &JobProcessEvent::Changed(JobProcessDelta::Blocker {
                index: 0,
                blocked_on: BlockerChange::Clear,
            }),
            who,
            None,
            3,
        )
        .unwrap();
        let socket = project(
            &mut projection,
            &JobProcessEvent::Changed(JobProcessDelta::Blocker {
                index: 0,
                blocked_on: BlockerChange::Set(ProcessBlockedOn::Socket),
            }),
            who,
            None,
            4,
        )
        .unwrap();
        assert_eq!(set.kind(), ProcessRowKind::Blocked);
        assert_eq!(set.columns().blocked_holder_pid, Some(9));
        assert_eq!(clear.kind(), ProcessRowKind::BlockerCleared);
        assert_eq!(clear.columns(), &ProcessColumns::default());
        assert_eq!(socket.columns().blocked_on, Some(BlockedOn::Socket));
        assert_eq!(socket.columns().blocked_path, None);
        assert_eq!(socket.columns().blocked_holder_pid, None);

        let root = temp_root("blocker");
        let path = seal_process_batch(&root, Uuid::nil(), 1, &[start, set, clear, socket]).unwrap();
        let decoded = read_process_segment(&path).unwrap();
        assert_eq!(decoded.len(), 4);
        assert!(decoded.iter().all(|row| row.parent_span_id == JOB_SPAN_ID));
        assert_eq!(decoded[1].entry_type, "info");
        assert_eq!(decoded[1].message, "process.blocked");
        assert_eq!(
            decoded[1].columns.blocked_path.as_deref(),
            Some("/tmp/held")
        );
        assert_eq!(decoded[2].message, "process.blocker-cleared");
        assert_eq!(decoded[2].columns.blocked_on, None);
        assert_eq!(decoded[3].columns.blocked_on, Some(BlockedOn::Socket));
        assert_eq!(decoded[3].timestamp_ns, 4_000_000_000);
        assert!(matches!(
            seal_process_batch(&root, Uuid::nil(), 2, &[]),
            Err(ProcessSpanError::NoRows)
        ));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_gap_adds_no_row_and_an_unavailable_io_source_states_no_counter() {
        let who = identity(3, 1);
        let mut projection = ProcessSpanProjection::new("trace", 8);
        let mut process = sample(3, 1, "/bin/cat", &["/bin/cat"]);
        process.usage = Some(ProcessUsage {
            io: ProcessStorageIo::Unavailable {
                reason: ProcessIoUnavailable::NotAccounted,
            },
            ..usage(10, false, 1)
        });
        let start = born(&mut projection, 0, who, None, &process);
        assert!(start.columns().io_read_bytes.is_none());
        assert!(start.columns().io_write_bytes.is_none());
        let gap = projection
            .project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Gap {
                        reason: ProcessCoverageGap::EventsLost,
                    },
                    identity: None,
                    parent: None,
                },
                at(2),
            )
            .unwrap();
        assert!(gap.is_none());
    }

    #[test]
    fn heartbeats_restate_moved_counters_and_the_terminal_row_is_final_usage() {
        let who = identity(5, 1);
        let mut projection = ProcessSpanProjection::new("trace", 4);
        let mut process = sample(5, 1, "/bin/sleep", &["/bin/sleep", "1"]);
        process.usage = Some(usage(10, false, 1));
        let start = born(&mut projection, 0, who, None, &process);

        let mut sampled = sample(5, 1, "/bin/sleep", &["/bin/sleep", "1"]);
        sampled.usage = Some(usage(40, false, 1));
        let heartbeat = project(
            &mut projection,
            &JobProcessEvent::Heartbeat {
                index: 0,
                process: sampled,
            },
            who,
            None,
            61,
        )
        .unwrap();
        assert_eq!(heartbeat.message(), "process.heartbeat");
        assert_eq!(heartbeat.columns().cpu_user_us, Some(40));
        assert_eq!(heartbeat.columns().rss_bytes, None);
        assert_eq!(heartbeat.columns().proc_argv, None);

        let mut ended = sample(5, 1, "/bin/sleep", &["/bin/sleep", "1"]);
        ended.usage = Some(usage(50, false, 0));
        ended.exit = Some(ProcessExit {
            status: Some(ExitStatus::Exited { code: 0 }),
            exited_at: UtcTimestamp::new("2026-10-12T00:01:00Z").unwrap(),
        });
        let terminal = project(
            &mut projection,
            &JobProcessEvent::Exited {
                index: 0,
                process: ended,
            },
            who,
            None,
            62,
        )
        .unwrap();
        assert_eq!(terminal.kind(), ProcessRowKind::Exited(SpanOutcome::Ok));
        assert_eq!(terminal.columns().exit_code, Some(0));
        assert_eq!(terminal.columns().cpu_user_us, Some(50));
        assert_eq!(terminal.columns().rss_bytes, Some(0));
        assert_eq!(terminal.columns().proc_argv, None);

        let root = temp_root("heartbeat");
        let path =
            seal_process_batch(&root, Uuid::nil(), 1, &[start, heartbeat, terminal]).unwrap();
        let rows = read_process_segment(&path).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].entry_type, "span-start");
        assert_eq!(rows[0].timestamp_ns, 1_000_000_000);
        assert_eq!(rows[1].entry_type, "info");
        assert_eq!(rows[1].timestamp_ns, 61_000_000_000);
        assert_eq!(rows[2].entry_type, "span-ok");
        assert_eq!(rows[2].columns.exit_code, Some(0));
        assert_eq!(rows[2].columns.cpu_user_us, Some(50));
        assert_eq!(
            rows.iter()
                .filter(|row| row.columns.proc_argv.is_some())
                .count(),
            1
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_refused_event_changes_no_span() {
        let who = identity(6, 1);
        let mut projection = ProcessSpanProjection::new("trace", 4);
        born(
            &mut projection,
            0,
            who,
            None,
            &sample(6, 1, "/bin/cat", &["/bin/cat"]),
        );
        let mut unnamed = sample(6, 1, "/bin/cat", &["/bin/cat"]);
        unnamed.exit = Some(ProcessExit {
            status: Some(ExitStatus::Signaled {
                signal: 999,
                core_dumped: false,
            }),
            exited_at: UtcTimestamp::new("2026-10-12T00:00:01Z").unwrap(),
        });
        let refused = projection.project(
            RetainedProcessEvent {
                event: &JobProcessEvent::Exited {
                    index: 0,
                    process: unnamed,
                },
                identity: Some(who),
                parent: None,
            },
            at(2),
        );
        assert!(matches!(
            refused,
            Err(ProcessSpanError::UnknownSignal { signal: 999 })
        ));
        let mut without_status = sample(6, 1, "/bin/cat", &["/bin/cat"]);
        without_status.exit = None;
        let refused = projection.project(
            RetainedProcessEvent {
                event: &JobProcessEvent::Exited {
                    index: 0,
                    process: without_status,
                },
                identity: Some(who),
                parent: None,
            },
            at(2),
        );
        assert!(matches!(
            refused,
            Err(ProcessSpanError::ExitWithoutStatus { index: 0 })
        ));
        // Neither refusal closed the span: its next sample still projects.
        let beat = project(
            &mut projection,
            &JobProcessEvent::Heartbeat {
                index: 0,
                process: sample(6, 1, "/bin/cat", &["/bin/cat"]),
            },
            who,
            None,
            3,
        )
        .unwrap();
        assert_eq!(beat.kind(), ProcessRowKind::Heartbeat);
    }

    #[test]
    fn a_missing_identity_is_named_and_changes_nothing() {
        let mut projection = ProcessSpanProjection::new("trace", 4);
        let refused = projection.project(
            RetainedProcessEvent {
                event: &JobProcessEvent::Born {
                    index: 7,
                    process: sample(7, 1, "/bin/cat", &["/bin/cat"]),
                },
                identity: None,
                parent: None,
            },
            at(1),
        );
        let error = refused.expect_err("a birth needs its identity");
        assert_eq!(
            error.to_string(),
            "process event 7 names no retained identity"
        );
    }

    #[test]
    fn a_forged_enum_dictionary_is_not_the_declared_vocabulary() {
        let segment = |names: Vec<&str>, key: u16| -> RecordBatch {
            let dict = DictionaryArray::<UInt16Type>::try_new(
                UInt16Array::from(vec![Some(key)]),
                Arc::new(StringArray::from(names)) as ArrayRef,
            )
            .unwrap();
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "blocked_on",
                    dict_type(DataType::UInt16),
                    true,
                )])),
                vec![Arc::new(dict) as ArrayRef],
            )
            .unwrap()
        };
        let forged = segment(vec!["none", "NOT_A_STATE"], 1);
        assert!(matches!(
            optional_enum(&forged, 0, 0, "blocked_on", BlockedOn::ALL),
            Err(ProcessSpanError::Schema)
        ));
        let declared = segment(BlockedOn::NAMES.to_vec(), 1);
        assert_eq!(
            optional_enum(&declared, 0, 0, "blocked_on", BlockedOn::ALL).unwrap(),
            Some(BlockedOn::Lock)
        );
    }

    /// A real job, held on two FIFOs, seals its three process spans through the production sink.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_real_fifo_job_seals_three_parented_spans() {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};

        use crate::runtime::process_events::KqueueObserver;
        use crate::runtime::process_tree::LifeReadings;
        use cowshed_os::fork_lock::Spawn;

        struct Unread;
        impl LifeReadings for Unread {
            fn usage(&self, _: ProcessIdentity) -> Option<ProcessUsage> {
                None
            }
            fn blocked_on(&self, _: ProcessIdentity) -> Option<ProcessBlockedOn> {
                None
            }
        }

        let root = temp_root("fifo");
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
            let observer = KqueueObserver::watch(pid).expect("watch");
            gate_write.write_all(b"x").unwrap();
            (observer, spawned.join().unwrap())
        });
        while observer.fold().nodes().count() < 3 {
            let _ = observer.wait();
            if let Some(status) = child.try_wait().unwrap() {
                panic!("the job exited before its three processes were observed: {status}");
            }
        }
        let births: Vec<_> = observer
            .fold()
            .nodes()
            .enumerate()
            .map(|(index, node)| {
                let index = u32::try_from(index).expect("a process index");
                (
                    index,
                    node.identity,
                    node.parent,
                    JobProcessSample {
                        pid: node.identity.pid,
                        ppid: node.ppid,
                        program: node.image.program.clone(),
                        argv: node.image.argv.clone(),
                        truncated: node.image.truncated,
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
        let telemetry = root.join("telemetry");
        std::fs::create_dir(&telemetry).unwrap();
        let writer = Uuid::nil();
        let mut born_rows = Vec::new();
        for (index, identity, parent, process) in &births {
            born_rows.push(
                project(
                    &mut projection,
                    &JobProcessEvent::Born {
                        index: *index,
                        process: process.clone(),
                    },
                    *identity,
                    *parent,
                    1,
                )
                .expect("a birth projects"),
            );
        }
        let mut sealed = vec![seal_process_batch(&telemetry, writer, 1, &born_rows).unwrap()];
        for path in [&first, &second] {
            let mut file = std::fs::File::create(path).unwrap();
            file.write_all(b"x").unwrap();
        }
        while observer.fold().nodes().any(|node| node.exit.is_none()) {
            let _ = observer.wait();
        }
        let _ = child.wait();
        let mut exit_rows = Vec::new();
        for (index, identity, parent, _) in &births {
            let row_index = usize::try_from(*index).expect("a process index");
            let process = observer.fold().sample(row_index, &Unread);
            if process.exit.is_none() {
                continue;
            }
            exit_rows.push(
                project(
                    &mut projection,
                    &JobProcessEvent::Exited {
                        index: *index,
                        process,
                    },
                    *identity,
                    *parent,
                    10,
                )
                .expect("an exit projects"),
            );
        }
        if !exit_rows.is_empty() {
            sealed.push(seal_process_batch(&telemetry, writer, 2, &exit_rows).unwrap());
        }
        let rows: Vec<DecodedProcessRow> = sealed
            .iter()
            .flat_map(|path| read_process_segment(path).unwrap())
            .collect();
        let starts: Vec<&DecodedProcessRow> = rows
            .iter()
            .filter(|row| row.entry_type == "span-start")
            .collect();
        assert_eq!(starts.len(), 3);
        assert_eq!(
            starts
                .iter()
                .filter(|row| row.columns.proc_argv.is_some())
                .count(),
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
