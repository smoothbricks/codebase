//! One columnar row store per pinned thread.
//!
//! Unlike [`crate::buffer::SpanBuffer`], this buffer does not own a span tree.
//! Every span on the thread appends into the same fixed-capacity column blocks;
//! parentage is carried as `(parent_thread_id, parent_span_id)` values, so a
//! child remains linkable after its parent has completed or after a flush.
//!
//! Schema attributes live in [`AttributeCells`]: one stable allocation per
//! block that a foreign writer may view and store into directly. The row
//! lifecycle — opening, appending, completing, stamping, identity — stays on
//! this type's methods, so a foreign writer can name a value but never a row's
//! identity or entry type.
//!
//! Blocks are recycled, never freed while the store lives: [`Self::reset`] and
//! [`Self::retain_open`] keep every block's allocations, so the addresses a
//! foreign writer viewed stay valid and a flushed store writes again without
//! allocating.

use crate::arena::{ArenaFull, StringArena, TextInput};
use crate::attribute_cells::AttributeCells;
use crate::buffer::SourceMetadata;
use crate::columns::{FieldMeta, FieldStrategy, SharedStr};
use crate::entry_type::EntryType;
use crate::identity::{SpanIdentity, TraceId};
use crate::packed_header::{VocabularyId, pack_dynamic, pack_static};
use crate::scope::{ScopeEntry, ScopeValue, SpanScope};
use crate::tuning::{ARENA_RECLAIM_BYTES, MAX_CAPACITY, MAX_STRING_ARENA_BYTES, MIN_CAPACITY};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use crate::thread_kinds::{
    ATTRIBUTE_KIND_BOOLEAN, ATTRIBUTE_KIND_ENUM, ATTRIBUTE_KIND_NUMBER, ATTRIBUTE_KIND_TEXT,
    ATTRIBUTE_KIND_UINT64,
};
use crate::thread_schema::SYSTEM_COLUMN_COUNT;

/// A row-targeted schema attribute value.
///
/// `Copy` and allocation-free: `Text` carries the intern ordinal
/// [`ThreadSpanBuffer::intern`] issued, not a string, so handing a value to the
/// row store neither allocates nor touches a refcount. Callers holding bytes
/// intern once and then write integers, which is the shape the ABI already
/// takes — `intern`, then a cell store.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnValue {
    Number(f64),
    Uint64(u64),
    Boolean(bool),
    Text(u32),
    Enum(u16),
}

/// Alias used by callers that describe writes as attributes.
pub type AttributeValue = ColumnValue;

/// ABI kind tags are generated from the TypeScript schema table so native and
/// Wasm writers cannot drift.
pub use crate::thread_kinds::AttributeKind as ColumnValueKind;

impl ColumnValue {
    #[inline]
    pub const fn kind(&self) -> ColumnValueKind {
        match self {
            Self::Number(_) => ColumnValueKind::Number,
            Self::Uint64(_) => ColumnValueKind::Uint64,
            Self::Boolean(_) => ColumnValueKind::Boolean,
            Self::Text(_) => ColumnValueKind::Text,
            Self::Enum(_) => ColumnValueKind::Enum,
        }
    }
}

/// Borrowed view used by Arrow conversion without cloning string cells.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnValueRef<'a> {
    Number(f64),
    Uint64(u64),
    Boolean(bool),
    Text(&'a str),
    Enum(u16),
}

/// Encode a caller value as the cell `strategy` stores, or refuse a value of
/// the wrong kind. Text must be an ordinal this store's arena issued.
fn encode_cell(
    strategy: FieldStrategy,
    value: ColumnValue,
    ordinal: u16,
    arena: &StringArena,
) -> Result<u64, ThreadBufferError> {
    match (strategy, value) {
        (FieldStrategy::Number, ColumnValue::Number(value)) => Ok(value.to_bits()),
        (FieldStrategy::Uint64, ColumnValue::Uint64(value)) => Ok(value),
        (FieldStrategy::Boolean, ColumnValue::Boolean(value)) => Ok(u64::from(value)),
        (FieldStrategy::Category | FieldStrategy::Text, ColumnValue::Text(id)) => arena
            .get(id)
            .map(|_| u64::from(id))
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal)),
        (FieldStrategy::Enum(variants), ColumnValue::Enum(index)) => {
            if usize::from(index) < variants.len() {
                Ok(u64::from(index))
            } else {
                Err(ThreadBufferError::EnumOutOfRange {
                    ordinal,
                    index,
                    variants: variants.len(),
                })
            }
        }
        (strategy, value) => Err(ThreadBufferError::AttributeTypeMismatch {
            ordinal,
            expected: strategy.kind(),
            actual: value.kind(),
        }),
    }
}

/// Decode a stored cell. Total on purpose: a foreign writer may have stored any
/// bit pattern, so a text ordinal the arena never issued, or an enum index
/// outside the variants, reads as absent rather than as a value — and never
/// as a panic.
fn decode_cell(
    strategy: FieldStrategy,
    cell: u64,
    arena: &StringArena,
) -> Option<ColumnValueRef<'_>> {
    match strategy {
        FieldStrategy::Number => Some(ColumnValueRef::Number(f64::from_bits(cell))),
        FieldStrategy::Uint64 => Some(ColumnValueRef::Uint64(cell)),
        FieldStrategy::Boolean => Some(ColumnValueRef::Boolean(cell != 0)),
        FieldStrategy::Category | FieldStrategy::Text => u32::try_from(cell)
            .ok()
            .and_then(|id| arena.get(id))
            .map(ColumnValueRef::Text),
        FieldStrategy::Enum(variants) => u16::try_from(cell)
            .ok()
            .filter(|index| usize::from(*index) < variants.len())
            .map(ColumnValueRef::Enum),
    }
}

/// The cell a scope value fills, interning text into this store's arena once
/// per fill. `Ok(None)` is a scope value whose kind does not match the column.
fn scope_cell(
    strategy: FieldStrategy,
    value: &ScopeValue,
    arena: &mut StringArena,
) -> Result<Option<u64>, ThreadBufferError> {
    Ok(match (strategy, value) {
        (FieldStrategy::Number, ScopeValue::Number(value)) => Some(value.to_bits()),
        (FieldStrategy::Uint64, ScopeValue::Uint64(value)) => Some(*value),
        (FieldStrategy::Boolean, ScopeValue::Boolean(value)) => Some(u64::from(*value)),
        (FieldStrategy::Enum(variants), ScopeValue::EnumIndex(index))
            if usize::from(*index) < variants.len() =>
        {
            Some(u64::from(*index))
        }
        (FieldStrategy::Category | FieldStrategy::Text, ScopeValue::Text(text)) => Some(u64::from(
            arena
                .intern(text.as_ref())
                .map_err(ThreadBufferError::StringArenaFull)?,
        )),
        _ => None,
    })
}
struct RowInput {
    timestamp: i64,
    trace_id: TraceId,
    header: u32,
    span_id: u32,
    parent_thread_id: u64,
    parent_span_id: u32,
    message: Option<SharedStr>,
    line: u32,
}

struct OpenInput {
    span_id: u32,
    trace_id: TraceId,
    parent_thread_id: u64,
    parent_span_id: u32,
    start_header: u32,
    name: Option<SharedStr>,
    timestamp: i64,
    line: u32,
}

#[derive(Debug)]
struct ThreadSpanBlock {
    capacity: usize,
    rows: usize,
    timestamps: Vec<i64>,
    trace_ids: Vec<Option<TraceId>>,
    headers: Vec<u32>,
    span_ids: Vec<u32>,
    parent_thread_ids: Vec<u64>,
    parent_span_ids: Vec<u32>,
    lines: Vec<u32>,
    messages: Vec<Option<SharedStr>>,
    attributes: AttributeCells,
}

impl ThreadSpanBlock {
    fn new(capacity: usize, field_count: usize) -> Self {
        Self {
            capacity,
            rows: 0,
            timestamps: vec![0; capacity],
            trace_ids: vec![None; capacity],
            headers: vec![0; capacity],
            span_ids: vec![0; capacity],
            parent_thread_ids: vec![0; capacity],
            parent_span_ids: vec![0; capacity],
            lines: vec![0; capacity],
            messages: vec![None; capacity],
            attributes: AttributeCells::new(field_count, capacity),
        }
    }
    #[inline]
    fn remaining(&self) -> usize {
        self.capacity - self.rows
    }
    fn write_row(&mut self, input: RowInput) -> usize {
        let row = self.rows;
        self.timestamps[row] = input.timestamp;
        self.trace_ids[row] = Some(input.trace_id);
        self.headers[row] = input.header;
        self.span_ids[row] = input.span_id;
        self.parent_thread_ids[row] = input.parent_thread_id;
        self.parent_span_ids[row] = input.parent_span_id;
        self.lines[row] = input.line;
        // Always stored, `None` included: a recycled block still holds the
        // previous window's cells, and a row without a message must not read
        // one of theirs.
        self.messages[row] = input.message;
        self.rows += 1;
        row
    }
    /// The system cells of `row`, for moving the row elsewhere.
    fn row_input(&self, row: usize) -> RowInput {
        RowInput {
            timestamp: self.timestamps[row],
            trace_id: self.trace_ids[row]
                .clone()
                .expect("a written row carries its trace id"),
            header: self.headers[row],
            span_id: self.span_ids[row],
            parent_thread_id: self.parent_thread_ids[row],
            parent_span_id: self.parent_span_ids[row],
            message: self.messages[row],
            line: self.lines[row],
        }
    }
    /// Forget rows `start..` while keeping every allocation.
    fn truncate(&mut self, start: usize) {
        self.rows = self.rows.min(start);
        self.trace_ids[start..].fill(None);
        self.messages[start..].fill(None);
        self.attributes.clear_from(start);
    }
}

#[derive(Debug, Clone, Copy)]
struct SpanRecord {
    start_row: u32,
    completion_row: u32,
    ended: bool,
    /// Where the span's code lives, stamped once per span and emitted on every
    /// one of its rows. `None` when the writer never attributed it.
    source: Option<SourceMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushWindow {
    pub start_row: usize,
    pub row_count: usize,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadBufferError {
    InvalidCapacity(usize),
    InvalidColumnOrdinal(u16),
    UnknownSpan(u32),
    InvalidRow(usize),
    EnumOutOfRange {
        ordinal: u16,
        index: u16,
        variants: usize,
    },
    AttributeTypeMismatch {
        ordinal: u16,
        expected: ColumnValueKind,
        actual: ColumnValueKind,
    },
    InvalidUtf8,
    /// The string arena is at its byte budget. Distinct-string cardinality, not
    /// row count, is what reaches this; the caller's answer is to flush and
    /// reset rather than to keep writing.
    StringArenaFull(ArenaFull),
}

impl std::fmt::Display for ThreadBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCapacity(value) => write!(f, "invalid thread buffer capacity {value}"),
            Self::InvalidColumnOrdinal(value) => write!(f, "invalid schema column ordinal {value}"),
            Self::UnknownSpan(value) => write!(f, "unknown span id {value}"),
            Self::InvalidRow(value) => write!(f, "invalid thread buffer row {value}"),
            Self::EnumOutOfRange {
                ordinal,
                index,
                variants,
            } => write!(
                f,
                "enum column {ordinal} index {index} is outside 0..{variants}"
            ),
            Self::AttributeTypeMismatch {
                ordinal,
                expected,
                actual,
            } => write!(
                f,
                "attribute column {ordinal} expects {expected:?}, got {actual:?}"
            ),
            Self::InvalidUtf8 => f.write_str("dynamic thread-buffer string is not valid UTF-8"),
            Self::StringArenaFull(full) => write!(f, "thread {full}"),
        }
    }
}
impl std::error::Error for ThreadBufferError {}

/// Hashes a span id for the span and scope tables.
///
/// Every row write looks its span up, so the hash is on the row path: the
/// default SipHash spends more on a `u32` key than the rest of the write. Span
/// ids are the store's own dense counter, not attacker-chosen keys, so a
/// multiplicative mix (FxHash, the arena's hasher too) spreads them with one
/// multiply, and its output is the same on every run.
#[derive(Default)]
struct SpanIdHasher(u64);

impl Hasher for SpanIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.write_u8(byte);
        }
    }

    #[inline]
    fn write_u8(&mut self, byte: u8) {
        self.write_u64(u64::from(byte));
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.write_u64(u64::from(value));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = (self.0.rotate_left(5) ^ value).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

type SpanIdMap<V> = HashMap<u32, V, BuildHasherDefault<SpanIdHasher>>;

/// The one row store owned by a pinned thread.
#[derive(Debug)]
pub struct ThreadSpanBuffer {
    thread_id: u64,
    capacity: usize,
    fields: &'static [FieldMeta],
    /// Every block this store has allocated. Blocks `..active_blocks` hold
    /// rows; the rest are recycled and empty, kept so their attribute cells
    /// keep the addresses a foreign writer viewed.
    blocks: Vec<ThreadSpanBlock>,
    active_blocks: usize,
    row_count: usize,
    next_span_id: u32,
    spans: SpanIdMap<SpanRecord>,
    scopes: SpanIdMap<Option<SpanScope>>,
    /// `retain_open`'s working list, kept so a flush allocates nothing.
    open_scratch: Vec<(u32, u32)>,
    /// Every dynamic string this thread has written, stored once, contiguous.
    /// Replaces a `Vec<Arc<str>>` plus a `HashMap<Arc<str>, u32>` — two
    /// structures that between them allocated an `Arc` per distinct value and
    /// needed an owned key to answer a lookup.
    arena: StringArena,
    /// Bumped each time [`Self::reclaim_text`] renumbers the arena. An ordinal
    /// is valid within the epoch that issued it.
    text_epoch: u32,
}

impl ThreadSpanBuffer {
    pub fn new(thread_id: u64, capacity: usize, fields: &'static [FieldMeta]) -> Self {
        assert!(
            capacity.is_power_of_two() && (MIN_CAPACITY..=MAX_CAPACITY).contains(&capacity),
            "thread span buffer capacity must be a power of two in {MIN_CAPACITY}..={MAX_CAPACITY}"
        );
        let mut buffer = Self {
            thread_id,
            capacity,
            fields,
            blocks: Vec::new(),
            active_blocks: 1,
            row_count: 0,
            next_span_id: 1,
            spans: SpanIdMap::with_capacity_and_hasher(capacity / 2, BuildHasherDefault::default()),
            scopes: SpanIdMap::with_capacity_and_hasher(
                capacity / 2,
                BuildHasherDefault::default(),
            ),
            open_scratch: Vec::new(),
            arena: StringArena::new(MAX_STRING_ARENA_BYTES),
            text_epoch: 0,
        };
        buffer
            .blocks
            .push(ThreadSpanBlock::new(capacity, fields.len()));
        buffer
    }
    /// Release every row and span, keeping every block's allocation and, until
    /// the arena passes [`ARENA_RECLAIM_BYTES`], the interned vocabulary.
    ///
    /// The buffer is per-thread and long-lived: without this, a process that
    /// traces forever grows the row store forever. Vocabulary ids survive while
    /// they can — callers cache them, so re-interning after every window would
    /// cost a lookup per distinct string per window — and are reclaimed once
    /// the arena is large ([`Self::reclaim_text`]). Blocks survive so the next
    /// window writes without allocating and so a foreign writer's views of
    /// their attribute cells stay valid.
    pub fn reset(&mut self) {
        for block in &mut self.blocks[..self.active_blocks] {
            block.truncate(0);
        }
        self.active_blocks = 1;
        self.row_count = 0;
        self.next_span_id = 1;
        self.spans.clear();
        self.scopes.clear();
        self.reclaim_text();
    }

    /// The arena's current epoch. An ordinal from [`Self::intern`] names the
    /// same bytes until this changes; a binding that caches ordinals compares
    /// it after every [`Self::reset`] and [`Self::retain_open`] and drops its
    /// cache when it moved.
    #[inline]
    #[must_use]
    pub const fn text_epoch(&self) -> u32 {
        self.text_epoch
    }

    /// Rebuild the arena from the text the remaining rows still name, once it
    /// has passed [`ARENA_RECLAIM_BYTES`].
    ///
    /// A long-lived store writes unbounded distinct text — completion
    /// messages, causes, paths — and an append-only arena would fill and refuse
    /// every later dynamic write. The rows a window keeps (none after a reset,
    /// the open spans after a retain) are re-interned into a fresh arena; their
    /// message cells and text attribute cells are renumbered in place, and the
    /// epoch moves so ordinal caches know to forget. Below the threshold this
    /// is one comparison, and ordinals stay put.
    fn reclaim_text(&mut self) {
        if self.arena.len() < ARENA_RECLAIM_BYTES {
            return;
        }
        let mut fresh = StringArena::new(MAX_STRING_ARENA_BYTES);
        let old = &self.arena;
        let capacity = self.capacity;
        for row in 0..self.row_count {
            let block = &mut self.blocks[row / capacity];
            let local = row % capacity;
            if let Some(SharedStr::Arena(handle)) = block.messages[local] {
                block.messages[local] = Some(SharedStr::Arena(
                    fresh
                        .intern_str(old.resolve(handle))
                        .expect("the kept text is a subset of an arena within the same budget"),
                ));
            }
            for (field, meta) in self.fields.iter().enumerate() {
                if !matches!(meta.strategy, FieldStrategy::Category | FieldStrategy::Text) {
                    continue;
                }
                let Some(cell) = block.attributes.get(field, local) else {
                    continue;
                };
                // A foreign writer may have stored an ordinal the arena never
                // issued; it decodes as absent, and stays absent.
                if let Some(text) = u32::try_from(cell)
                    .ok()
                    .and_then(|ordinal| old.get(ordinal))
                {
                    let ordinal = fresh
                        .intern(text)
                        .expect("the kept text is a subset of an arena within the same budget");
                    block.attributes.set(field, local, u64::from(ordinal));
                }
            }
        }
        self.arena = fresh;
        self.text_epoch = self.text_epoch.wrapping_add(1);
    }
    #[inline]
    pub const fn thread_id(&self) -> u64 {
        self.thread_id
    }
    #[inline]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
    #[inline]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
    #[inline]
    pub const fn schema_fields(&self) -> &'static [FieldMeta] {
        self.fields
    }

    /// Intern a dynamic string once. The returned ordinal is stable within the
    /// current [`Self::text_epoch`] and survives overflow blocks: the arena is
    /// append-only between reclaims, so a consumer may cache on it until the
    /// epoch moves. Only [`Self::reset`] and [`Self::retain_open`] reclaim.
    ///
    /// A repeat costs one hash of the incoming bytes plus one slice comparison
    /// and allocates nothing. This used to be a linear scan bounded by a
    /// ceiling, because a `HashMap<Arc<str>, _>` needs an owned key to look one
    /// up — a consequence of storing `Arc<str>`, and gone with it.
    pub fn intern(&mut self, value: &str) -> Result<u32, ThreadBufferError> {
        self.arena
            .intern(value)
            .map_err(ThreadBufferError::StringArenaFull)
    }
    #[inline]
    pub fn interned(&self, ordinal: u32) -> Option<&str> {
        self.arena.get(ordinal)
    }
    /// Backing bytes for this buffer's dynamic strings. The flush pass reads
    /// cells through this rather than through per-row string copies.
    #[inline]
    pub fn arena(&self) -> &StringArena {
        &self.arena
    }
    /// The cell a previously issued ordinal names.
    #[inline]
    fn interned_cell(&self, ordinal: u32) -> Option<SharedStr> {
        self.arena.handle(ordinal).map(SharedStr::Arena)
    }
    /// Resolve caller text into a cell, interning a dynamic value.
    #[inline]
    fn cell(&mut self, text: TextInput<'_>) -> Result<SharedStr, ThreadBufferError> {
        match text {
            TextInput::Static(value) => Ok(SharedStr::Static(value)),
            TextInput::Dynamic(value) => {
                let ordinal = self.intern(value)?;
                Ok(self
                    .interned_cell(ordinal)
                    .expect("intern just issued this ordinal"))
            }
        }
    }
    /// Decode the scalar representation used by both native and WASM ABI adapters.
    ///
    /// Text values are intern ordinals, numbers preserve all f64 bits, and enum
    /// values occupy the low u16 bits. Invalid kind/value pairs are rejected
    /// before they reach a schema column.
    pub fn decode_abi_value(&self, kind: u8, value: u64) -> Option<ColumnValue> {
        match kind {
            ATTRIBUTE_KIND_NUMBER => Some(ColumnValue::Number(f64::from_bits(value))),
            ATTRIBUTE_KIND_UINT64 => Some(ColumnValue::Uint64(value)),
            ATTRIBUTE_KIND_BOOLEAN => (value <= 1).then_some(ColumnValue::Boolean(value != 0)),
            // Text stays an ordinal end to end: the ABI passed one in, the row
            // store records one, and the arena resolves it at flush. Decoding
            // only proves the ordinal names a live cell.
            ATTRIBUTE_KIND_TEXT => u32::try_from(value)
                .ok()
                .filter(|id| self.interned(*id).is_some())
                .map(ColumnValue::Text),
            ATTRIBUTE_KIND_ENUM => u16::try_from(value).ok().map(ColumnValue::Enum),
            _ => None,
        }
    }
    fn ensure_rows(&mut self, count: usize) {
        if self.blocks[self.active_blocks - 1].remaining() < count {
            if self.active_blocks == self.blocks.len() {
                self.blocks
                    .push(ThreadSpanBlock::new(self.capacity, self.fields.len()));
            }
            self.active_blocks += 1;
        }
    }
    fn allocate_span_id(&mut self) -> u32 {
        loop {
            let id = self.next_span_id;
            self.next_span_id = self.next_span_id.wrapping_add(1);
            if self.next_span_id == 0 {
                self.next_span_id = 1;
            }
            if id != 0 && !self.spans.contains_key(&id) {
                return id;
            }
        }
    }
    fn append_row(&mut self, input: RowInput) -> u32 {
        self.ensure_rows(1);
        let block_index = self.active_blocks - 1;
        let row = self.blocks[block_index].write_row(input);
        self.row_count += 1;
        u32::try_from(block_index * self.capacity + row).expect("thread buffer rows fit u32")
    }

    /// Open a dynamic-name span. The completion row is reserved immediately, matching the legacy row-0/row-1 shape; end methods overwrite it later.
    pub fn open_span(
        &mut self,
        trace_id: TraceId,
        parent_thread_id: u64,
        parent_span_id: u32,
        name: TextInput<'_>,
        timestamp: i64,
        line: u32,
    ) -> Result<u32, ThreadBufferError> {
        let name = self.cell(name)?;
        let span_id = self.allocate_span_id();
        self.open_rows(OpenInput {
            span_id,
            trace_id,
            parent_thread_id,
            parent_span_id,
            start_header: pack_dynamic(EntryType::SpanStart),
            name: Some(name),
            timestamp,
            line,
        })
    }
    /// Open a span with a manifest-global static vocabulary ID.
    pub fn open_span_static(
        &mut self,
        trace_id: TraceId,
        parent_thread_id: u64,
        parent_span_id: u32,
        name: VocabularyId,
        timestamp: i64,
        line: u32,
    ) -> Result<u32, ThreadBufferError> {
        let span_id = self.allocate_span_id();
        let header = pack_static(EntryType::SpanStart, name)
            .map_err(|_| ThreadBufferError::InvalidColumnOrdinal(u16::MAX))?;
        self.open_rows(OpenInput {
            span_id,
            trace_id,
            parent_thread_id,
            parent_span_id,
            start_header: header,
            name: None,
            timestamp,
            line,
        })
    }
    /// Open a span using an ordinal returned by [`Self::intern`].
    pub fn open_span_interned(
        &mut self,
        trace_id: TraceId,
        parent_thread_id: u64,
        parent_span_id: u32,
        name: u32,
        timestamp: i64,
        line: u32,
    ) -> Result<u32, ThreadBufferError> {
        let name = self
            .interned_cell(name)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(
                u16::try_from(name).unwrap_or(u16::MAX),
            ))?;
        let span_id = self.allocate_span_id();
        self.open_rows(OpenInput {
            span_id,
            trace_id,
            parent_thread_id,
            parent_span_id,
            start_header: pack_dynamic(EntryType::SpanStart),
            name: Some(name),
            timestamp,
            line,
        })
    }
    fn open_rows(&mut self, input: OpenInput) -> Result<u32, ThreadBufferError> {
        // Append each row independently so a pair crossing an overflow boundary
        // remains dense instead of skipping the last slot in the old block.
        let parent_thread_id = if input.parent_span_id == 0 {
            0
        } else {
            input.parent_thread_id
        };
        let inherited_scope = if input.parent_span_id != 0 && parent_thread_id == self.thread_id {
            self.scopes.get(&input.parent_span_id).cloned().flatten()
        } else {
            None
        };
        let start_row = self.append_row(RowInput {
            timestamp: input.timestamp,
            trace_id: input.trace_id.clone(),
            header: input.start_header,
            span_id: input.span_id,
            parent_thread_id,
            parent_span_id: input.parent_span_id,
            message: input.name,
            line: input.line,
        });
        let completion_row = self.append_row(RowInput {
            timestamp: input.timestamp,
            trace_id: input.trace_id,
            header: pack_dynamic(EntryType::SpanException),
            span_id: input.span_id,
            parent_thread_id,
            parent_span_id: input.parent_span_id,
            message: None,
            line: 0,
        });
        self.spans.insert(
            input.span_id,
            SpanRecord {
                start_row,
                completion_row,
                ended: false,
                source: None,
            },
        );
        self.scopes.insert(input.span_id, inherited_scope);
        Ok(input.span_id)
    }
    #[inline]
    fn record(&self, span_id: u32) -> Result<SpanRecord, ThreadBufferError> {
        self.spans
            .get(&span_id)
            .copied()
            .ok_or(ThreadBufferError::UnknownSpan(span_id))
    }
    fn block_row(row: usize, capacity: usize) -> (usize, usize) {
        (row / capacity, row % capacity)
    }
    fn block_at(&self, row: usize) -> Result<(&ThreadSpanBlock, usize), ThreadBufferError> {
        if row >= self.row_count {
            return Err(ThreadBufferError::InvalidRow(row));
        }
        let (block, local) = Self::block_row(row, self.capacity);
        Ok((&self.blocks[block], local))
    }
    fn block_at_mut(
        &mut self,
        row: usize,
    ) -> Result<(&mut ThreadSpanBlock, usize), ThreadBufferError> {
        if row >= self.row_count {
            return Err(ThreadBufferError::InvalidRow(row));
        }
        let (block, local) = Self::block_row(row, self.capacity);
        Ok((&mut self.blocks[block], local))
    }
    fn complete(
        &mut self,
        span_id: u32,
        entry_type: EntryType,
        timestamp: i64,
    ) -> Result<(), ThreadBufferError> {
        let record = self.record(span_id)?;
        let (block, row) = self.block_at_mut(record.completion_row as usize)?;
        block.timestamps[row] = timestamp;
        block.headers[row] = pack_dynamic(entry_type);
        self.spans
            .get_mut(&span_id)
            .expect("record checked above")
            .ended = true;
        Ok(())
    }
    /// Complete a span with the tracer's own entry type.
    ///
    /// `end_err` cannot express `SpanException`: collapsing a throw onto
    /// `SpanErr` erases the distinction between a handled failure and a bug,
    /// which is the whole point of the completion taxonomy.
    pub fn end(
        &mut self,
        span_id: u32,
        entry_type: EntryType,
        timestamp: i64,
    ) -> Result<(), ThreadBufferError> {
        self.complete(span_id, entry_type, timestamp)
    }
    pub fn end_ok(&mut self, span_id: u32, timestamp: i64) -> Result<(), ThreadBufferError> {
        self.complete(span_id, EntryType::SpanOk, timestamp)
    }
    pub fn end_err(&mut self, span_id: u32, timestamp: i64) -> Result<(), ThreadBufferError> {
        self.complete(span_id, EntryType::SpanErr, timestamp)
    }
    /// Store a span's terminal message on its reserved completion row.
    ///
    /// The js-heap lane writes result/error text into row 1 rather than
    /// appending a row; this is the same contract for the shared row store, so
    /// the two lanes produce the same row count for the same trace.
    pub fn set_completion_message(
        &mut self,
        span_id: u32,
        message: TextInput<'_>,
    ) -> Result<(), ThreadBufferError> {
        let message = self.cell(message)?;
        let record = self.record(span_id)?;
        let (block, row) = self.block_at_mut(record.completion_row as usize)?;
        block.messages[row] = Some(message);
        Ok(())
    }
    pub fn append_log(
        &mut self,
        span_id: u32,
        entry_type: EntryType,
        message: Option<TextInput<'_>>,
        line: u32,
        timestamp: i64,
    ) -> Result<u32, ThreadBufferError> {
        let message = message.map(|text| self.cell(text)).transpose()?;
        let record = self.record(span_id)?;
        let start_row = record.start_row as usize;
        let (block, local) = self.block_at(start_row)?;
        let trace_id = block.trace_ids[local]
            .as_ref()
            .expect("span start always carries trace id")
            .clone();
        let parent_thread_id = block.parent_thread_ids[local];
        let parent_span_id = block.parent_span_ids[local];
        Ok(self.append_row(RowInput {
            timestamp,
            trace_id,
            header: pack_dynamic(entry_type),
            span_id,
            parent_thread_id,
            parent_span_id,
            message,
            line,
        }))
    }
    pub fn append_log_interned(
        &mut self,
        span_id: u32,
        entry_type: EntryType,
        message: u32,
        line: u32,
        timestamp: i64,
    ) -> Result<u32, ThreadBufferError> {
        let message =
            self.interned_cell(message)
                .ok_or(ThreadBufferError::InvalidColumnOrdinal(
                    u16::try_from(message).unwrap_or(u16::MAX),
                ))?;
        self.append_log_cell(span_id, entry_type, Some(message), line, timestamp)
    }
    /// Shared tail of the message-carrying appends: everything after the caller's
    /// text has become a cell in this buffer's arena.
    fn append_log_cell(
        &mut self,
        span_id: u32,
        entry_type: EntryType,
        message: Option<SharedStr>,
        line: u32,
        timestamp: i64,
    ) -> Result<u32, ThreadBufferError> {
        let record = self.record(span_id)?;
        let start_row = record.start_row as usize;
        let (block, local) = self.block_at(start_row)?;
        let trace_id = block.trace_ids[local]
            .as_ref()
            .expect("span start always carries trace id")
            .clone();
        let parent_thread_id = block.parent_thread_ids[local];
        let parent_span_id = block.parent_span_ids[local];
        Ok(self.append_row(RowInput {
            timestamp,
            trace_id,
            header: pack_dynamic(entry_type),
            span_id,
            parent_thread_id,
            parent_span_id,
            message,
            line,
        }))
    }
    pub fn append_log_static(
        &mut self,
        span_id: u32,
        entry_type: EntryType,
        message: VocabularyId,
        line: u32,
        timestamp: i64,
    ) -> Result<u32, ThreadBufferError> {
        let record = self.record(span_id)?;
        let (block, local) = self.block_at(record.start_row as usize)?;
        let trace_id = block.trace_ids[local]
            .as_ref()
            .expect("span start always carries trace id")
            .clone();
        let parent_thread_id = block.parent_thread_ids[local];
        let parent_span_id = block.parent_span_ids[local];
        Ok(self.append_row(RowInput {
            timestamp,
            trace_id,
            header: pack_static(entry_type, message)
                .map_err(|_| ThreadBufferError::InvalidColumnOrdinal(u16::MAX))?,
            span_id,
            parent_thread_id,
            parent_span_id,
            message: None,
            line,
        }))
    }
    /// Write one schema attribute to an arbitrary row. Ordinal 0..12 is the fixed system prefix and is refused here.
    pub fn write_attr(
        &mut self,
        row: u32,
        ordinal: u16,
        value: ColumnValue,
    ) -> Result<(), ThreadBufferError> {
        let index = usize::from(ordinal)
            .checked_sub(SYSTEM_COLUMN_COUNT)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?;
        let strategy = self
            .fields
            .get(index)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?
            .strategy;
        let cell = encode_cell(strategy, value, ordinal, &self.arena)?;
        let (block, local) = self.block_at(row as usize)?;
        block.attributes.set(index, local, cell);
        Ok(())
    }
    /// Row-0 convenience matching `tag`: the row is looked up by span ID.
    pub fn write_tag(
        &mut self,
        span_id: u32,
        ordinal: u16,
        value: ColumnValue,
    ) -> Result<(), ThreadBufferError> {
        let row = self.record(span_id)?.start_row;
        self.write_attr(row, ordinal, value)
    }
    /// Merge a scope update into a span's side-table snapshot. Scope never occupies row storage; conversion materializes it into attribute lanes.
    pub fn set_scope(
        &mut self,
        span_id: u32,
        update: &[ScopeEntry],
    ) -> Result<(), ThreadBufferError> {
        self.record(span_id)?;
        let current = self.scopes.get(&span_id).cloned().flatten();
        let merged = SpanScope::merge(current.as_ref(), update);
        self.scopes.insert(span_id, merged);
        Ok(())
    }
    pub fn scope(&self, span_id: u32) -> Result<Option<&SpanScope>, ThreadBufferError> {
        self.record(span_id)?;
        Ok(self.scopes.get(&span_id).and_then(Option::as_ref))
    }
    /// Attribute a span to the code that opened it. Every row of the span
    /// carries this provenance when converted.
    pub fn set_source(
        &mut self,
        span_id: u32,
        source: SourceMetadata,
    ) -> Result<(), ThreadBufferError> {
        self.spans
            .get_mut(&span_id)
            .ok_or(ThreadBufferError::UnknownSpan(span_id))?
            .source = Some(source);
        Ok(())
    }
    /// The provenance [`Self::set_source`] recorded for a span still held.
    #[inline]
    pub fn source_of(&self, span_id: u32) -> Option<SourceMetadata> {
        self.spans.get(&span_id).and_then(|record| record.source)
    }
    /// [`Self::set_scope`] from the scalar ABI form every binding speaks: an
    /// attribute kind and its encoded value, or kind `0` to clear the field
    /// (01i `setScope({ field: null })`). Text arrives as an intern ordinal and
    /// is copied out once, because a scope is shared by refcount between spans
    /// and cannot carry this store's arena handle.
    pub fn set_scope_encoded(
        &mut self,
        span_id: u32,
        ordinal: u16,
        kind: u8,
        value: u64,
    ) -> Result<(), ThreadBufferError> {
        let field = usize::from(ordinal)
            .checked_sub(SYSTEM_COLUMN_COUNT)
            .and_then(|index| self.fields.get(index))
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?;
        let name = field.name;
        if kind == 0 {
            return self.set_scope(span_id, &[(name, None)]);
        }
        let value = self
            .decode_abi_value(kind, value)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?;
        let value = match value {
            ColumnValue::Number(value) => ScopeValue::Number(value),
            ColumnValue::Uint64(value) => ScopeValue::Uint64(value),
            ColumnValue::Boolean(value) => ScopeValue::Boolean(value),
            ColumnValue::Enum(value) => ScopeValue::EnumIndex(value),
            ColumnValue::Text(id) => ScopeValue::Text(
                self.interned(id)
                    .ok_or(ThreadBufferError::InvalidColumnOrdinal(ordinal))?
                    .to_owned()
                    .into(),
            ),
        };
        self.set_scope(span_id, &[(name, Some(value))])
    }
    /// Materialize scope values for one row window. This is intentionally a flush-time operation and uses validity-aware range fills; direct row writes remain authoritative.
    pub fn materialize_scope_window(
        &mut self,
        start_row: usize,
        row_count: usize,
    ) -> Result<usize, ThreadBufferError> {
        let end_row = start_row
            .checked_add(row_count)
            .ok_or(ThreadBufferError::InvalidRow(start_row))?;
        if end_row > self.row_count {
            return Err(ThreadBufferError::InvalidRow(end_row));
        }
        let mut filled = 0usize;
        let mut row = start_row;
        while row < end_row {
            let (block_index, local) = Self::block_row(row, self.capacity);
            let block_end = ((block_index + 1) * self.capacity).min(end_row);
            let block = &self.blocks[block_index];
            let span_id = block.span_ids[local];
            let mut run_end = row + 1;
            while run_end < block_end && block.span_ids[run_end % self.capacity] == span_id {
                run_end += 1;
            }
            let scope = self.scopes.get(&span_id).cloned().flatten();
            if let Some(scope) = scope {
                let fields = self.fields;
                let capacity = self.capacity;
                let local_end = if run_end == block_end && run_end.is_multiple_of(capacity) {
                    capacity
                } else {
                    run_end % capacity
                };
                for (name, value) in scope.iter() {
                    let Some(index) = fields.iter().position(|field| field.name == name) else {
                        continue;
                    };
                    // Interned ONCE per fill, not once per row: a scope value
                    // covers a run of rows and they all share the cell.
                    match scope_cell(fields[index].strategy, value, &mut self.arena)? {
                        Some(cell) => {
                            filled += self.blocks[block_index]
                                .attributes
                                .fill_unset(index, local, local_end, cell);
                        }
                        None => crate::scope::report_scope_mismatch(
                            name,
                            "matching schema column type",
                            value,
                        ),
                    }
                }
            }
            row = run_end;
        }
        Ok(filled)
    }
    /// Prepare a conversion window. Open spans stay open in live storage; Arrow synthesizes an exception completion at this timestamp.
    pub fn flush_window(
        &mut self,
        start_row: usize,
        row_count: usize,
        timestamp: i64,
    ) -> Result<FlushWindow, ThreadBufferError> {
        self.materialize_scope_window(start_row, row_count)?;
        Ok(FlushWindow {
            start_row,
            row_count,
            timestamp,
        })
    }
    /// Select the rows one streaming flush emits: every written row except the
    /// reserved completion row of a span that is still open. An open span's
    /// start row IS emitted — it is how a reader sees a span that has not ended
    /// — and is emitted again by every later flush while the span stays open,
    /// the last copy carrying the final attribute values. Scope is materialized
    /// over the emitted rows first.
    pub fn flush_rows(&mut self, rows: &mut Vec<usize>) -> Result<(), ThreadBufferError> {
        self.materialize_scope_window(0, self.row_count)?;
        rows.clear();
        rows.extend((0..self.row_count).filter(|&row| {
            let span_id = self.blocks[row / self.capacity].span_ids[row % self.capacity];
            self.spans
                .get(&span_id)
                .is_none_or(|record| record.ended || record.completion_row as usize != row)
        }));
        Ok(())
    }
    /// Drop every flushed row while keeping the spans still open, so a
    /// long-lived store can flush without losing a span that outlives the
    /// window. Each open span's start row and reserved completion row move to
    /// the front, in open order; everything else — completed spans, log rows,
    /// their scopes — is released. Blocks keep their allocations.
    ///
    /// Two rows move per open span. Nothing else is copied, and nothing is
    /// allocated once the working list has grown to the open-span count —
    /// unless the arena has passed its reclaim threshold, when the kept rows'
    /// text moves to a fresh arena ([`Self::reclaim_text`]).
    pub fn retain_open(&mut self) {
        let mut open = std::mem::take(&mut self.open_scratch);
        open.clear();
        open.extend(
            self.spans
                .iter()
                .filter(|(_, record)| !record.ended)
                .map(|(&id, record)| (record.start_row, id)),
        );
        open.sort_unstable();
        self.spans.retain(|_, record| !record.ended);
        let spans = &self.spans;
        self.scopes.retain(|id, _| spans.contains_key(id));

        let capacity = self.capacity;
        let mut next = 0usize;
        for &(start_row, span_id) in &open {
            // A span's rows are consecutive and open spans are visited in row
            // order, so the destination never passes the source: moving forward
            // in place overwrites only rows already moved or released.
            for source in [start_row as usize, start_row as usize + 1] {
                if source != next {
                    let input = self.blocks[source / capacity].row_input(source % capacity);
                    let (to_block, to) = (next / capacity, next % capacity);
                    let block = &mut self.blocks[to_block];
                    block.timestamps[to] = input.timestamp;
                    block.trace_ids[to] = Some(input.trace_id);
                    block.headers[to] = input.header;
                    block.span_ids[to] = input.span_id;
                    block.parent_thread_ids[to] = input.parent_thread_id;
                    block.parent_span_ids[to] = input.parent_span_id;
                    block.lines[to] = input.line;
                    block.messages[to] = input.message;
                    let from = &self.blocks[source / capacity].attributes;
                    self.blocks[to_block]
                        .attributes
                        .copy_row(to, from, source % capacity);
                }
                next += 1;
            }
            let record = self
                .spans
                .get_mut(&span_id)
                .expect("open span retained above");
            record.start_row = u32::try_from(next - 2).expect("thread buffer rows fit u32");
            record.completion_row = u32::try_from(next - 1).expect("thread buffer rows fit u32");
        }
        let kept_blocks = next.div_ceil(capacity).max(1);
        for (index, block) in self.blocks[..self.active_blocks].iter_mut().enumerate() {
            let keep = next.saturating_sub(index * capacity).min(capacity);
            block.truncate(keep);
        }
        self.active_blocks = kept_blocks;
        self.row_count = next;
        self.open_scratch = open;
        self.reclaim_text();
    }
    /// Blocks currently holding rows. Row `r` lives in block `r / capacity`.
    #[inline]
    pub fn block_count(&self) -> usize {
        self.active_blocks
    }
    /// The attribute cells of `block`, for a foreign writer's view. A block
    /// that exists keeps this allocation for the store's whole life, recycled
    /// or not; `None` names a block not yet allocated.
    #[inline]
    pub fn attribute_cells(&self, block: usize) -> Option<&AttributeCells> {
        self.blocks.get(block).map(|block| &block.attributes)
    }
    #[inline]
    pub fn timestamp_at(&self, row: usize) -> Option<i64> {
        self.block_at(row)
            .ok()
            .map(|(block, local)| block.timestamps[local])
    }
    /// The trace a span belongs to, as the store holds it: a child opened
    /// later — after the context that opened its parent is gone — joins the
    /// same trace by cloning this, not by re-parsing a string.
    #[inline]
    pub fn span_trace_id(&self, span_id: u32) -> Option<&TraceId> {
        let row = self.spans.get(&span_id)?.start_row as usize;
        let (block, local) = self.block_at(row).ok()?;
        block.trace_ids[local].as_ref()
    }
    #[inline]
    pub fn trace_id_at(&self, row: usize) -> Option<&str> {
        self.block_at(row)
            .ok()
            .and_then(|(block, local)| block.trace_ids[local].as_ref())
            .map(TraceId::as_str)
    }
    #[inline]
    pub fn packed_header_at(&self, row: usize) -> Option<u32> {
        self.block_at(row)
            .ok()
            .map(|(block, local)| block.headers[local])
    }
    #[inline]
    pub fn span_id_at(&self, row: usize) -> Option<u32> {
        self.block_at(row)
            .ok()
            .map(|(block, local)| block.span_ids[local])
    }
    #[inline]
    pub fn parent_thread_id_at(&self, row: usize) -> Option<u64> {
        self.block_at(row)
            .ok()
            .map(|(block, local)| block.parent_thread_ids[local])
    }
    #[inline]
    pub fn parent_span_id_at(&self, row: usize) -> Option<u32> {
        self.block_at(row)
            .ok()
            .map(|(block, local)| block.parent_span_ids[local])
    }
    #[inline]
    pub fn line_at(&self, row: usize) -> Option<u32> {
        self.block_at(row)
            .ok()
            .map(|(block, local)| block.lines[local])
    }
    #[inline]
    pub fn dynamic_message_at(&self, row: usize) -> Option<&str> {
        let (block, local) = self.block_at(row).ok()?;
        Some(block.messages[local].as_ref()?.resolve(&self.arena))
    }
    #[inline]
    pub fn attribute_at(&self, row: usize, ordinal: u16) -> Option<ColumnValueRef<'_>> {
        let index = usize::from(ordinal).checked_sub(SYSTEM_COLUMN_COUNT)?;
        let strategy = self.fields.get(index)?.strategy;
        let (block, local) = self.block_at(row).ok()?;
        decode_cell(strategy, block.attributes.get(index, local)?, &self.arena)
    }
    #[inline]
    pub fn is_span_open(&self, span_id: u32) -> bool {
        self.spans.get(&span_id).is_some_and(|record| !record.ended)
    }
    #[inline]
    pub fn completion_row(&self, span_id: u32) -> Option<usize> {
        self.spans
            .get(&span_id)
            .map(|record| record.completion_row as usize)
    }
    #[inline]
    pub fn start_row(&self, span_id: u32) -> Option<usize> {
        self.spans
            .get(&span_id)
            .map(|record| record.start_row as usize)
    }
    /// Iterate span IDs in deterministic open order for Arrow completion synthesis.
    pub fn span_ids(&self) -> impl Iterator<Item = u32> + '_ {
        let mut ids: Vec<(u32, u32)> = self
            .spans
            .iter()
            .map(|(&id, record)| (record.start_row, id))
            .collect();
        ids.sort_unstable();
        ids.into_iter().map(|(_, id)| id)
    }
    pub fn intern_utf8(&mut self, bytes: &[u8]) -> Result<u32, ThreadBufferError> {
        let value = std::str::from_utf8(bytes).map_err(|_| ThreadBufferError::InvalidUtf8)?;
        self.intern(value)
    }
    /// Validate caller bytes as UTF-8 without interning them. Used by the ABI
    /// where a refusal must be reported before any store state is touched.
    pub fn utf8(bytes: &[u8]) -> Result<&str, ThreadBufferError> {
        std::str::from_utf8(bytes).map_err(|_| ThreadBufferError::InvalidUtf8)
    }
    pub fn identity_at(&self, row: usize) -> Option<SpanIdentity> {
        let trace_id = TraceId::new(self.trace_id_at(row)?.to_owned()).ok()?;
        Some(SpanIdentity {
            thread_id: self.thread_id,
            span_id: self.span_id_at(row)?,
            trace_id,
            parent: None,
        })
    }
}

impl FieldStrategy {
    const fn kind(self) -> ColumnValueKind {
        match self {
            Self::Number => ColumnValueKind::Number,
            Self::Uint64 => ColumnValueKind::Uint64,
            Self::Boolean => ColumnValueKind::Boolean,
            Self::Category | Self::Text => ColumnValueKind::Text,
            Self::Enum(_) => ColumnValueKind::Enum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    static FIELDS: &[FieldMeta] = &[
        FieldMeta::new("answer", FieldStrategy::Number),
        FieldMeta::new("label", FieldStrategy::Category),
    ];
    fn trace() -> TraceId {
        TraceId::new("trace").unwrap()
    }
    #[test]
    fn opens_all_spans_on_one_buffer_and_preserves_parent_value_after_close() {
        let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
        let parent = buffer
            .open_span(trace(), 0, 0, "parent".into(), 10, 1)
            .unwrap();
        buffer.end_ok(parent, 11).unwrap();
        let child = buffer
            .open_span(trace(), 7, parent, "child".into(), 12, 2)
            .unwrap();
        let log = buffer
            .append_log(child, EntryType::Info, Some("hello".into()), 3, 13)
            .unwrap();
        assert_ne!(parent, 0);
        assert_eq!(
            buffer.parent_span_id_at(buffer.start_row(child).unwrap()),
            Some(parent)
        );
        assert_eq!(buffer.span_id_at(log as usize), Some(child));
    }
    #[test]
    fn a_flush_keeps_open_spans_and_emits_each_finished_row_once() {
        let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
        let pending = buffer
            .open_span(trace(), 0, 0, "pending".into(), 1, 1)
            .unwrap();
        let done = buffer
            .open_span(trace(), 7, pending, "done".into(), 2, 2)
            .unwrap();
        buffer
            .append_log(pending, EntryType::Info, Some("early".into()), 3, 3)
            .unwrap();
        buffer.end_ok(done, 4).unwrap();

        let mut rows = Vec::new();
        buffer.flush_rows(&mut rows).unwrap();
        // pending's reserved completion row (1) stays out; everything else goes.
        assert_eq!(rows, vec![0, 2, 3, 4]);
        buffer.retain_open();
        assert_eq!(buffer.row_count(), 2);
        assert_eq!(buffer.start_row(pending), Some(0));
        assert!(buffer.start_row(done).is_none());
        // A child opened after the flush joins the retained span's trace.
        assert_eq!(
            buffer.span_trace_id(pending).map(TraceId::as_str),
            Some("trace")
        );
        assert!(buffer.span_trace_id(done).is_none());

        // The retained span still takes rows and completes, and its tag lands
        // on the start row the next flush re-emits.
        buffer
            .write_tag(pending, 12, ColumnValue::Number(5.0))
            .unwrap();
        buffer
            .append_log(pending, EntryType::Info, Some("late".into()), 5, 5)
            .unwrap();
        buffer.end_ok(pending, 6).unwrap();
        buffer.flush_rows(&mut rows).unwrap();
        assert_eq!(rows, vec![0, 1, 2]);
        assert_eq!(buffer.timestamp_at(1), Some(6));
        assert_eq!(buffer.dynamic_message_at(0), Some("pending"));
        assert_eq!(buffer.dynamic_message_at(2), Some("late"));
        assert_eq!(
            buffer.attribute_at(0, 12),
            Some(ColumnValueRef::Number(5.0))
        );
        buffer.retain_open();
        assert_eq!(buffer.row_count(), 0);
    }
    #[test]
    fn recycled_blocks_keep_their_addresses_and_forget_their_rows() {
        let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
        let span = buffer.open_span(trace(), 0, 0, "a".into(), 1, 1).unwrap();
        for _ in 0..10 {
            buffer
                .append_log(span, EntryType::Info, Some("row".into()), 2, 2)
                .unwrap();
        }
        let first = buffer.attribute_cells(0).unwrap().as_ptr();
        let second = buffer.attribute_cells(1).unwrap().as_ptr();
        buffer.write_attr(9, 12, ColumnValue::Number(1.0)).unwrap();
        buffer.reset();
        let span = buffer.open_span(trace(), 0, 0, "b".into(), 3, 3).unwrap();
        for _ in 0..10 {
            buffer
                .append_log_static(span, EntryType::Info, VocabularyId::new(1).unwrap(), 4, 4)
                .unwrap();
        }
        assert_eq!(buffer.attribute_cells(0).unwrap().as_ptr(), first);
        assert_eq!(buffer.attribute_cells(1).unwrap().as_ptr(), second);
        assert_eq!(buffer.attribute_at(9, 12), None);
        assert_eq!(buffer.dynamic_message_at(9), None);
    }
    #[test]
    fn a_foreign_store_into_the_cells_reads_back_through_the_schema() {
        let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
        let span = buffer.open_span(trace(), 0, 0, "s".into(), 1, 1).unwrap();
        let label = buffer.intern("sku-1").unwrap();
        let row = buffer.start_row(span).unwrap();
        let cells = buffer.attribute_cells(0).unwrap();
        let words = cells.as_ptr();
        let stride = crate::attribute_cells::stride(8);
        // What a TypedArray writer does: a Float64 store into field 0, a
        // Uint32 store into field 1's low half, and a validity bit per field.
        // SAFETY: every offset is inside the block's allocation.
        unsafe {
            *words.cast::<f64>().add(row) = 2.5;
            *words.cast::<u8>().add(8 * 8 + (row >> 3)) |= 1 << (row & 7);
            *words.cast::<u32>().add((stride + row) * 2) = label;
            *words.cast::<u8>().add((stride + 8) * 8 + (row >> 3)) |= 1 << (row & 7);
        }
        assert_eq!(
            buffer.attribute_at(row, 12),
            Some(ColumnValueRef::Number(2.5))
        );
        assert_eq!(
            buffer.attribute_at(row, 13),
            Some(ColumnValueRef::Text("sku-1"))
        );
        // A stored ordinal the arena never issued reads as absent, not a panic.
        unsafe { *words.cast::<u32>().add((stride + row) * 2) = 999 };
        assert_eq!(buffer.attribute_at(row, 13), None);
    }
    #[test]
    fn direct_attribute_writes_survive_scope_materialization() {
        let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
        let span = buffer
            .open_span(trace(), 0, 0, "span".into(), 10, 1)
            .unwrap();
        buffer
            .write_tag(span, 12, ColumnValue::Number(2.0))
            .unwrap();
        buffer
            .set_scope(span, &[("answer", Some(ScopeValue::Number(9.0)))])
            .unwrap();
        buffer
            .materialize_scope_window(0, buffer.row_count())
            .unwrap();
        assert_eq!(
            buffer.attribute_at(buffer.start_row(span).unwrap(), 12),
            Some(ColumnValueRef::Number(2.0))
        );
        assert_eq!(
            buffer.attribute_at(buffer.completion_row(span).unwrap(), 12),
            Some(ColumnValueRef::Number(9.0))
        );
    }
    #[test]
    fn interned_names_are_stable_through_overflow() {
        let mut buffer = ThreadSpanBuffer::new(7, 8, FIELDS);
        let id = buffer.intern("dynamic-name").unwrap();
        let span = buffer.open_span_interned(trace(), 0, 0, id, 1, 0).unwrap();
        for _ in 0..8 {
            buffer
                .append_log_interned(span, EntryType::Info, id, 0, 1)
                .unwrap();
        }
        assert_eq!(buffer.intern("dynamic-name"), Ok(id));
        assert!(buffer.row_count() > 8);
    }

    #[test]
    fn abi_decoder_rejects_invalid_kind_and_value_pairs() {
        let mut buffer = ThreadSpanBuffer::new(7, 8, &[]);
        assert!(buffer.decode_abi_value(0, 0).is_none());
        assert!(buffer.decode_abi_value(ATTRIBUTE_KIND_BOOLEAN, 2).is_none());
        assert!(
            buffer
                .decode_abi_value(ATTRIBUTE_KIND_ENUM, u64::from(u16::MAX) + 1)
                .is_none()
        );

        let text_id = buffer.intern("dynamic").unwrap();
        assert!(matches!(
            buffer.decode_abi_value(ATTRIBUTE_KIND_TEXT, u64::from(text_id)),
            Some(ColumnValue::Text(_))
        ));
        assert!(
            buffer
                .decode_abi_value(ATTRIBUTE_KIND_TEXT, u64::from(text_id) + 1)
                .is_none()
        );
    }
}
