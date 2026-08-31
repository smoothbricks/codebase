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
//! HARD CONSTRAINT (deterministic scheduler specification §5): zero heap allocations per
//! event after warmup. All growth happens via overflow chaining (allocate a NEW buffer,
//! never realloc in place) so writes are wait-free with respect to readers.

use crate::arena::{ScopeText, StringArena, TextInput};
use crate::clock::{Clock, TraceAnchor};
use crate::columns::{SharedStr, StrColumn};
use crate::entry_type::EntryType;
use crate::identity::SpanIdentity;
use crate::packed_header::{
    StaticVocabularyNotAllowed, VocabularyId, entry_type_from_header, pack_dynamic, pack_static,
    vocabulary_id_from_header,
};
use crate::scope::{ScopeEntry, SpanScope};
use crate::tuning::{MAX_CAPACITY, MIN_CAPACITY};
use std::sync::Arc;

/// Row index reserved for span completion.
pub const COMPLETION_ROW: usize = 1;
/// First appendable log row.
pub const FIRST_LOG_ROW: usize = 2;

/// Source identity has four deliberately distinct forms:
///
/// - a versioned module names its package and file and carries the source file's
///   40-hex last-touch commit;
/// - `js-hash:<64 hex>` names unversioned authored JavaScript by content and has
///   no commit;
/// - `<unversioned>` marks authored source with neither commit nor content hash;
/// - `<internal>` with no commit and line zero is reserved for test-only internal
///   machinery with no authored callsite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceMetadata {
    pub package_name: &'static str,
    pub package_file: &'static str,
    pub git_sha: Option<&'static str>,
    pub line: u32,
}

impl SourceMetadata {
    /// Explicit source marker for internal/test machinery that has no authored callsite.
    #[doc(hidden)]
    pub const UNATTRIBUTED: Self = Self {
        package_name: "<internal>",
        package_file: "<internal>",
        git_sha: None,
        line: 0,
    };
}

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
    /// Backing bytes for every dynamic string this buffer's cells name.
    ///
    /// Per buffer, not per chain: `append_dynamic` interns into the buffer it
    /// appends to and `dynamic_message_at` resolves against the buffer it reads
    /// from, so a cell and its bytes never live in different objects. Lazily
    /// allocated, so a span whose names are all static vocabulary owns none of
    /// it.
    arena: StringArena,
    /// Source attribution captured by `span!`.
    source: Option<SourceMetadata>,
    /// Overflow chain: same identity, appended when this buffer fills (`01b2`).
    overflow: Option<Box<SpanBuffer>>,
    /// Child spans, walked depth-first pre-order at Arrow conversion (`01k`).
    children: Vec<SpanBuffer>,
    /// Inherited scope attributes (`01i`). `None` is the empty scope, so the
    /// untouched case owns no heap at all — cheaper than the TypeScript
    /// `EMPTY_SCOPE` singleton it corresponds to, and canonical: exactly one
    /// representation per logical scope, since a merge that empties the scope
    /// yields `None`.
    ///
    /// [`SpanScope`] is itself a shared handle, so a child span inherits by refcount
    /// bump rather than copy, and the value the child captured cannot be perturbed by
    /// a later `set_scope` here — the snapshot semantics `01i` requires for async
    /// safety.
    scope: Option<SpanScope>,
}

impl SpanBuffer {
    /// Start a span whose name is carried dynamically in the message column.
    pub fn start_dynamic(
        identity: Arc<SpanIdentity>,
        capacity: usize,
        name: TextInput<'_>,
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
        let cell = buffer.intern_text(name);
        buffer.messages.set(0, capacity, cell);
        buffer
    }

    /// Turn caller text into the cell this buffer stores.
    ///
    /// Schema-generated attribute writers use the same arena as system
    /// messages, so every handle remains paired with its owning row store. The
    /// `expect` is an invariant, not an operational failure: the arena is
    /// bounded only by the `u32` offset space its handles address, and a single
    /// span reaching four gigabytes of distinct strings would exhaust the
    /// allocator first.
    #[inline]
    pub fn intern_text(&mut self, text: TextInput<'_>) -> SharedStr {
        match text {
            TextInput::Static(value) => SharedStr::Static(value),
            TextInput::Dynamic(value) => SharedStr::Arena(
                self.arena
                    .intern_str(value)
                    .expect("span string arena exceeded the u32 offset space"),
            ),
        }
    }

    /// Resolve a scope snapshot into a cell owned by this span.
    ///
    /// Owned scope text may have crossed from a parent buffer, so it is
    /// interned here instead of retaining a handle into the parent's arena.
    pub fn intern_scope_text(&mut self, text: &ScopeText) -> SharedStr {
        match text {
            std::borrow::Cow::Borrowed(value) => SharedStr::Static(value),
            std::borrow::Cow::Owned(value) => self.intern_text(TextInput::Dynamic(value)),
        }
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
        debug_assert!(
            capacity.is_power_of_two() && (MIN_CAPACITY..=MAX_CAPACITY).contains(&capacity)
        );
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
            arena: StringArena::new(StringArena::OFFSET_SPACE),
            source: None,
            overflow: None,
            children: Vec::new(),
            scope: None,
        }
    }

    /// Dynamic span name stored at row 0, or `None` for a static span start.
    pub fn dynamic_name(&self) -> Option<&str> {
        self.messages.get(0, &self.arena)
    }

    /// Backing bytes for this buffer's dynamic strings, for the flush pass.
    #[inline]
    pub fn arena(&self) -> &StringArena {
        &self.arena
    }

    /// Record the `span!` source attribution.
    pub fn set_source(&mut self, source: SourceMetadata) {
        self.line_numbers[0] = source.line;
        self.source = Some(source);
    }

    pub fn source(&self) -> Option<SourceMetadata> {
        self.source
    }

    /// This span's inherited scope attributes (`01i`), `None` when never set.
    pub fn scope(&self) -> Option<&SpanScope> {
        self.scope.as_ref()
    }

    /// The shared scope handle, for handing this span's snapshot to a child or to the
    /// flush pass. Cloning it is a refcount bump, never a copy.
    pub fn scope_handle(&self) -> Option<SpanScope> {
        self.scope.clone()
    }

    /// Adopt `scope` wholesale, sharing the caller's value. Used at child-span
    /// creation to take the parent's snapshot; `01i`'s zero-cost inheritance is
    /// exactly this refcount bump.
    pub fn inherit_scope(&mut self, scope: Option<SpanScope>) {
        self.assign_scope(scope);
    }

    /// Merge `update` into this span's scope, per `01i`: `Some` sets, `None` clears,
    /// an unnamed field is untouched. The previous scope value is not modified, so
    /// any child already holding it keeps its snapshot.
    ///
    /// Cold path by construction — `01i` places every scope operation off the hot
    /// path, and this one allocates the new immutable value.
    pub fn set_scope(&mut self, update: &[ScopeEntry]) {
        if update.is_empty() {
            return;
        }
        let merged = SpanScope::merge(self.scope.as_ref(), update);
        self.assign_scope(merged);
    }

    /// One span's overflow chain is ONE span, so every buffer in it must answer the
    /// same scope. Assigning down the chain keeps that true after a `set_scope` that
    /// follows an overflow, rather than leaving already-created continuation buffers
    /// holding a stale snapshot — the failure mode the TypeScript eager prefill has.
    fn assign_scope(&mut self, scope: Option<SpanScope>) {
        let mut target = self;
        loop {
            target.scope = scope.clone();
            match target.overflow.as_deref_mut() {
                Some(next) => target = next,
                None => return,
            }
        }
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
        message: Option<TextInput<'_>>,
        line: u32,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> usize {
        let (target, row) = self.append_header(pack_dynamic(entry_type), anchor, clock);
        if let Some(message) = message {
            // Intern into the buffer the row landed in, which after an overflow
            // is NOT `self`. A cell and the bytes it names always live in the
            // same object; that is what makes `dynamic_message_at` correct
            // without carrying an arena reference across the chain.
            let cell = target.intern_text(message);
            target.messages.set(row, target.capacity, cell);
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
        let (target, row) = self.append_header(header, anchor, clock);
        target.line_numbers[row] = line;
        Ok(row)
    }

    fn append_header(
        &mut self,
        header: u32,
        anchor: &TraceAnchor,
        clock: &dyn Clock,
    ) -> (&mut SpanBuffer, usize) {
        let target = self.append_target();
        if target.write_index == target.capacity {
            target.overflow = Some(Box::new(SpanBuffer {
                identity: target.identity.clone(),
                capacity: target.capacity,
                write_index: 0,
                timestamps: vec![0i64; target.capacity],
                headers: vec![0u32; target.capacity],
                line_numbers: vec![0u32; target.capacity],
                messages: StrColumn::new(),
                arena: StringArena::new(StringArena::OFFSET_SPACE),
                source: None,
                overflow: None,
                children: Vec::new(),
                // Same span, so the same scope — see `set_scope_handle`.
                scope: target.scope.clone(),
            }));
            let target = target
                .overflow
                .as_deref_mut()
                .expect("overflow inserted immediately above");
            let row = target.write_row(header, anchor, clock);
            return (target, row);
        }
        let row = target.write_row(header, anchor, clock);
        (target, row)
    }

    /// Dynamic message for this physical buffer row. Static rows return `None`.
    pub fn dynamic_message_at(&self, row: usize) -> Option<&str> {
        self.messages.get(row, &self.arena)
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
