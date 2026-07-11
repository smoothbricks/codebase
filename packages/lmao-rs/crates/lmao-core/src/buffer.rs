//! Pre-allocated SoA span buffers with the fixed row layout, per
//! `specs/lmao/01b_columnar_buffer_architecture.md` and `01b5_spanbuffer_memory_layout.md`.
//!
//! Fixed row layout (load-bearing, everything downstream assumes it):
//! - Row 0 = `span-start`; `tag.*` OVERWRITES row 0 (Datadog/OTel set_tag semantics).
//! - Row 1 = pre-initialized to `span-exception` at creation (exception/panic safety);
//!   overwritten by `end_ok()` → `span-ok` or `end_err()` → `span-err`.
//! - Rows 2+ = log entries, append-only; `write_index` starts at 2.
//! - `duration = timestamp[1] - timestamp[0]` is therefore always valid.
//!
//! System columns (`timestamp`, `entry_type`) are eagerly allocated; schema attribute
//! columns are lazy (unused columns cost zero bytes, `01b1`). Strings are NOT interned
//! on the hot path (`01a`): category/text columns hold owned refs, dictionary building
//! is deferred to the Arrow flush pass in `lmao-arrow`.
//!
//! HARD CONSTRAINT (AxE `01-deterministic-scheduler.md` §5): zero heap allocations per
//! event after warmup. All growth happens via overflow chaining (allocate a NEW buffer,
//! never realloc in place) so writes are wait-free with respect to readers.

use crate::clock::{Clock, TraceAnchor};
use crate::columns::{SharedStr, StrColumn};
use crate::entry_type::EntryType;
use crate::identity::SpanIdentity;
use std::sync::Arc;

/// Row index reserved for span completion.
pub const COMPLETION_ROW: usize = 1;
/// First appendable log row.
pub const FIRST_LOG_ROW: usize = 2;

/// One span's columnar buffer. SoA: parallel `timestamps`/`entry_types` arrays plus
/// lazily-created attribute columns (todo: generated per-schema by `lmao-macros`).
#[derive(Debug)]
pub struct SpanBuffer {
    pub identity: Arc<SpanIdentity>,
    capacity: usize,
    write_index: usize,
    timestamps: Vec<i64>,
    entry_types: Vec<u8>,
    /// Eager system column (`01b1`): callsite line per row (0 = unknown).
    line_numbers: Vec<u32>,
    /// `message` column (`01f`): OVERLOADED per entry type — row 0 span name,
    /// rows 2+ log format-string TEMPLATES (never interpolated text). Lazy.
    messages: StrColumn,
    /// Callsite of the `span!` invocation (file is 'static via `file!()`).
    callsite: Option<(&'static str, u32)>,
    /// Overflow chain: same identity, appended when this buffer fills (`01b2`).
    overflow: Option<Box<SpanBuffer>>,
    /// Child spans, walked depth-first pre-order at Arrow conversion (`01k`).
    children: Vec<SpanBuffer>,
}

impl SpanBuffer {
    /// Create a buffer with row 0 = span-start (stamped now) and row 1 pre-armed as
    /// span-exception. `capacity` must be a power of two in `[8, 1024]` (`01b2`).
    pub fn start(
        identity: Arc<SpanIdentity>,
        capacity: usize,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> Self {
        debug_assert!(capacity.is_power_of_two() && (8..=1024).contains(&capacity));
        let mut timestamps = vec![0i64; capacity];
        let mut entry_types = vec![0u8; capacity];
        let line_numbers = vec![0u32; capacity];
        let now = anchor.timestamp(clock);
        timestamps[0] = now;
        entry_types[0] = EntryType::SpanStart.as_u8();
        // Exception safety: if the span is never completed, row 1 is already valid.
        timestamps[COMPLETION_ROW] = now;
        entry_types[COMPLETION_ROW] = EntryType::SpanException.as_u8();
        Self {
            identity,
            capacity,
            write_index: FIRST_LOG_ROW,
            timestamps,
            entry_types,
            line_numbers,
            messages: StrColumn::new(),
            callsite: None,
            overflow: None,
            children: Vec::new(),
        }
    }

    /// Span name — the row-0 `message` slot (`01f`: message is overloaded).
    pub fn set_name(&mut self, name: impl Into<SharedStr>) {
        let cap = self.capacity;
        self.messages.set(0, cap, name);
    }

    pub fn name(&self) -> Option<&str> {
        self.messages.get(0)
    }

    /// Record the `span!` callsite (`file!()`, `line!()`).
    pub fn set_callsite(&mut self, file: &'static str, line: u32) {
        self.callsite = Some((file, line));
        self.line_numbers[0] = line;
    }

    pub fn callsite(&self) -> Option<(&'static str, u32)> {
        self.callsite
    }

    /// Attach a finished/running child span (walked depth-first pre-order at
    /// Arrow conversion, `01k`).
    pub fn add_child(&mut self, child: SpanBuffer) {
        debug_assert!(child.identity.is_child_of(&self.identity));
        self.children.push(child);
    }

    /// Overwrite row 1 with `span-ok` (last-write-wins completion).
    pub fn end_ok(&mut self, anchor: &TraceAnchor, clock: &dyn Clock) {
        self.complete(EntryType::SpanOk, anchor, clock);
    }

    /// Overwrite row 1 with `span-err`.
    pub fn end_err(&mut self, anchor: &TraceAnchor, clock: &dyn Clock) {
        self.complete(EntryType::SpanErr, anchor, clock);
    }

    fn complete(&mut self, et: EntryType, anchor: &TraceAnchor, clock: &dyn Clock) {
        debug_assert!(et.is_completion());
        self.timestamps[COMPLETION_ROW] = anchor.timestamp(clock);
        self.entry_types[COMPLETION_ROW] = et.as_u8();
    }

    /// Append a log/metric entry; returns the row index written (relative to the
    /// buffer it landed in). When full, chains an overflow buffer sharing this
    /// buffer's identity (`01b2`) — the overflow's rows are all appendable (no
    /// span-start/completion rows), so its `write_index` starts at 0.
    pub fn append(
        &mut self,
        entry_type: EntryType,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> usize {
        let target = self.append_target();
        if target.write_index == target.capacity {
            let mut next = Box::new(SpanBuffer {
                identity: target.identity.clone(),
                capacity: target.capacity,
                write_index: 0,
                timestamps: vec![0i64; target.capacity],
                entry_types: vec![0u8; target.capacity],
                line_numbers: vec![0u32; target.capacity],
                messages: StrColumn::new(),
                callsite: None,
                overflow: None,
                children: Vec::new(),
            });
            let row = next.write_row(entry_type, anchor, clock);
            target.overflow = Some(next);
            return row;
        }
        target.write_row(entry_type, anchor, clock)
    }

    /// Append a log entry with its format-string TEMPLATE (`01f`: store the
    /// template, never interpolated text — values go in typed attribute columns)
    /// and callsite line. Returns the row index in the buffer it landed in.
    pub fn append_msg(
        &mut self,
        entry_type: EntryType,
        template: impl Into<SharedStr>,
        line: u32,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> usize {
        let row = self.append(entry_type, anchor, clock);
        let target = self.append_target_ref();
        let cap = target.capacity;
        target.messages.set(row, cap, template);
        target.line_numbers[row] = line;
        row
    }

    /// Last buffer in the overflow chain, immutable positioning helper for
    /// writers that already appended.
    fn append_target_ref(&mut self) -> &mut SpanBuffer {
        self.append_target()
    }

    pub fn message_at(&self, row: usize) -> Option<&str> {
        self.messages.get(row)
    }

    pub fn line_at(&self, row: usize) -> u32 {
        self.line_numbers.get(row).copied().unwrap_or(0)
    }

    /// Last buffer in the overflow chain (where appends go).
    fn append_target(&mut self) -> &mut SpanBuffer {
        let mut target = self;
        while target.overflow.is_some() {
            target = target.overflow.as_deref_mut().unwrap();
        }
        target
    }

    #[inline]
    fn write_row(&mut self, entry_type: EntryType, anchor: &TraceAnchor, clock: &dyn Clock) -> usize {
        let row = self.write_index;
        self.timestamps[row] = anchor.timestamp(clock);
        self.entry_types[row] = entry_type.as_u8();
        self.write_index = row + 1;
        row
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn write_index(&self) -> usize {
        self.write_index
    }

    #[inline]
    pub fn entry_type_at(&self, row: usize) -> Option<EntryType> {
        EntryType::from_u8(*self.entry_types.get(row)?)
    }

    #[inline]
    pub fn timestamp_at(&self, row: usize) -> Option<i64> {
        self.timestamps.get(row).copied()
    }

    /// Always-valid span duration in nanos (row 1 minus row 0).
    #[inline]
    pub fn duration_nanos(&self) -> i64 {
        self.timestamps[COMPLETION_ROW] - self.timestamps[0]
    }

    pub fn overflow(&self) -> Option<&SpanBuffer> {
        self.overflow.as_deref()
    }

    pub fn children(&self) -> &[SpanBuffer] {
        &self.children
    }
}
