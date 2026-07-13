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
use crate::packed_header::{
    StaticVocabularyNotAllowed, VocabularyId, entry_type_from_header, pack_dynamic, pack_static,
    vocabulary_id_from_header,
};
use std::sync::Arc;

/// Row index reserved for span completion.
pub const COMPLETION_ROW: usize = 1;
/// First appendable log row.
pub const FIRST_LOG_ROW: usize = 2;

/// One span's columnar buffer. SoA: parallel timestamp/packed-header arrays plus
/// lazily-created attribute columns (todo: generated per-schema by `lmao-macros`).
#[derive(Debug)]
pub struct SpanBuffer {
    pub identity: Arc<SpanIdentity>,
    capacity: usize,
    write_index: usize,
    timestamps: Vec<i64>,
    headers: Vec<u32>,
    /// Eager system column (`01b1`): callsite line per row (0 = unknown).
    line_numbers: Vec<u32>,
    /// Dynamic span names and log messages only. Static vocabulary paths leave
    /// this column untouched so it remains lazy and unallocated.
    messages: StrColumn,
    /// Callsite of the `span!` invocation (file is 'static via `file!()`).
    callsite: Option<(&'static str, u32)>,
    /// Overflow chain: same identity, appended when this buffer fills (`01b2`).
    overflow: Option<Box<SpanBuffer>>,
    /// Child spans, walked depth-first pre-order at Arrow conversion (`01k`).
    children: Vec<SpanBuffer>,
}

impl SpanBuffer {
    /// Start a span whose name is carried dynamically in the message column.
    pub fn start_dynamic(
        identity: Arc<SpanIdentity>,
        capacity: usize,
        name: SharedStr,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> Self {
        let mut buffer = Self::start_with_header(
            identity,
            capacity,
            pack_dynamic(EntryType::SpanStart),
            anchor,
            clock,
        );
        buffer.messages.set(0, capacity, name);
        buffer
    }

    /// Start a span whose name is represented by a manifest-global vocabulary ID.
    pub fn start_static(
        identity: Arc<SpanIdentity>,
        capacity: usize,
        span_name_id: VocabularyId,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> Self {
        let header = pack_static(EntryType::SpanStart, span_name_id)
            .expect("SpanStart must support a static vocabulary ID");
        Self::start_with_header(identity, capacity, header, anchor, clock)
    }

    fn start_with_header(
        identity: Arc<SpanIdentity>,
        capacity: usize,
        span_start_header: u32,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> Self {
        debug_assert!(capacity.is_power_of_two() && (8..=1024).contains(&capacity));
        let mut timestamps = vec![0i64; capacity];
        let mut headers = vec![0u32; capacity];
        let line_numbers = vec![0u32; capacity];
        let now = anchor.timestamp(clock);
        timestamps[0] = now;
        headers[0] = span_start_header;
        // Exception safety: if the span is never completed, row 1 is already valid.
        timestamps[COMPLETION_ROW] = now;
        headers[COMPLETION_ROW] = pack_dynamic(EntryType::SpanException);
        Self {
            identity,
            capacity,
            write_index: FIRST_LOG_ROW,
            timestamps,
            headers,
            line_numbers,
            messages: StrColumn::new(),
            callsite: None,
            overflow: None,
            children: Vec::new(),
        }
    }

    /// Dynamic span name stored at row 0, or `None` for a static span start.
    pub fn dynamic_name(&self) -> Option<&str> {
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

    fn complete(&mut self, entry_type: EntryType, anchor: &TraceAnchor, clock: &dyn Clock) {
        debug_assert!(entry_type.is_completion());
        self.timestamps[COMPLETION_ROW] = anchor.timestamp(clock);
        self.headers[COMPLETION_ROW] = pack_dynamic(entry_type);
    }

    /// Append a dynamic row, optionally storing its message. `None` leaves the
    /// lazy message column untouched.
    pub fn append_dynamic(
        &mut self,
        entry_type: EntryType,
        message: Option<SharedStr>,
        line: u32,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> usize {
        let row = self.append_header(pack_dynamic(entry_type), anchor, clock);
        let target = self.append_target();
        if let Some(message) = message {
            target.messages.set(row, target.capacity, message);
        }
        target.line_numbers[row] = line;
        row
    }

    /// Append a static log-template row. Validation is completed before any
    /// timestamp, index, overflow, line, or message state is mutated.
    pub fn append_static(
        &mut self,
        entry_type: EntryType,
        template_id: VocabularyId,
        line: u32,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> Result<usize, StaticVocabularyNotAllowed> {
        let header = pack_static(entry_type, template_id)?;
        if entry_type == EntryType::SpanStart {
            return Err(StaticVocabularyNotAllowed(entry_type));
        }
        let row = self.append_header(header, anchor, clock);
        self.append_target().line_numbers[row] = line;
        Ok(row)
    }

    fn append_header(&mut self, header: u32, anchor: &TraceAnchor, clock: &dyn Clock) -> usize {
        let target = self.append_target();
        if target.write_index == target.capacity {
            let mut next = Box::new(SpanBuffer {
                identity: target.identity.clone(),
                capacity: target.capacity,
                write_index: 0,
                timestamps: vec![0i64; target.capacity],
                headers: vec![0u32; target.capacity],
                line_numbers: vec![0u32; target.capacity],
                messages: StrColumn::new(),
                callsite: None,
                overflow: None,
                children: Vec::new(),
            });
            let row = next.write_row(header, anchor, clock);
            target.overflow = Some(next);
            return row;
        }
        target.write_row(header, anchor, clock)
    }

    /// Dynamic message for this physical buffer row. Static rows return `None`.
    pub fn dynamic_message_at(&self, row: usize) -> Option<&str> {
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
    fn write_row(&mut self, header: u32, anchor: &TraceAnchor, clock: &dyn Clock) -> usize {
        let row = self.write_index;
        self.timestamps[row] = anchor.timestamp(clock);
        self.headers[row] = header;
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
    pub fn packed_header_at(&self, row: usize) -> Option<u32> {
        self.headers.get(row).copied()
    }

    #[inline]
    pub fn vocabulary_id_at(&self, row: usize) -> Option<VocabularyId> {
        vocabulary_id_from_header(self.packed_header_at(row)?)
    }

    #[inline]
    pub fn entry_type_at(&self, row: usize) -> Option<EntryType> {
        entry_type_from_header(self.packed_header_at(row)?)
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
