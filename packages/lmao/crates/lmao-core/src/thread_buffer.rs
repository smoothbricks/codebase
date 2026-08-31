//! One columnar row store per pinned thread.
//!
//! Unlike [`crate::buffer::SpanBuffer`], this buffer does not own a span tree.
//! Every span on the thread appends into the same fixed-capacity column blocks;
//! parentage is carried as `(parent_thread_id, parent_span_id)` values, so a
//! child remains linkable after its parent has completed or after a flush.

use crate::columns::{
    BoolColumn, EnumColumn, F64Column, FieldMeta, FieldStrategy, NumColumn, SharedStr, StrColumn,
    U64Column,
};
use crate::entry_type::EntryType;
use crate::identity::{SpanIdentity, TraceId};
use crate::packed_header::{MAX_VOCABULARY_ID, VocabularyId, pack_dynamic, pack_static};
use crate::scope::{ScopeEntry, ScopeValue, SpanScope};
use crate::tuning::{MAX_CAPACITY, MIN_CAPACITY};
use std::collections::HashMap;
use std::sync::Arc;

use crate::thread_schema::SYSTEM_COLUMN_COUNT;

/// A row-targeted schema attribute value.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Number(f64),
    Uint64(u64),
    Boolean(bool),
    Text(SharedStr),
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

#[derive(Debug)]
enum AttributeColumn {
    Number(F64Column),
    Uint64(U64Column),
    Boolean(BoolColumn),
    Text(StrColumn),
    Enum(EnumColumn),
}

impl AttributeColumn {
    fn from_strategy(strategy: FieldStrategy) -> Self {
        match strategy {
            FieldStrategy::Number => Self::Number(NumColumn::new()),
            FieldStrategy::Uint64 => Self::Uint64(NumColumn::new()),
            FieldStrategy::Boolean => Self::Boolean(NumColumn::new()),
            FieldStrategy::Category | FieldStrategy::Text => Self::Text(StrColumn::new()),
            FieldStrategy::Enum(_) => Self::Enum(NumColumn::new()),
        }
    }

    fn set(
        &mut self,
        row: usize,
        capacity: usize,
        value: ColumnValue,
        ordinal: u16,
        strategy: FieldStrategy,
    ) -> Result<(), ThreadBufferError> {
        match (self, value) {
            (Self::Number(column), ColumnValue::Number(value)) => column.set(row, capacity, value),
            (Self::Uint64(column), ColumnValue::Uint64(value)) => column.set(row, capacity, value),
            (Self::Boolean(column), ColumnValue::Boolean(value)) => {
                column.set(row, capacity, value)
            }
            (Self::Text(column), ColumnValue::Text(value)) => column.set(row, capacity, value),
            (Self::Enum(column), ColumnValue::Enum(value)) => {
                if let FieldStrategy::Enum(values) = strategy
                    && usize::from(value) >= values.len()
                {
                    return Err(ThreadBufferError::EnumOutOfRange {
                        ordinal,
                        index: value,
                        variants: values.len(),
                    });
                }
                column.set(row, capacity, value)
            }
            (_, value) => {
                return Err(ThreadBufferError::AttributeTypeMismatch {
                    ordinal,
                    expected: strategy.kind(),
                    actual: value.kind(),
                });
            }
        }
        Ok(())
    }

    fn get(&self, row: usize) -> Option<ColumnValueRef<'_>> {
        match self {
            Self::Number(column) => column.get(row).map(ColumnValueRef::Number),
            Self::Uint64(column) => column.get(row).map(ColumnValueRef::Uint64),
            Self::Boolean(column) => column.get(row).map(ColumnValueRef::Boolean),
            Self::Text(column) => column.get(row).map(ColumnValueRef::Text),
            Self::Enum(column) => column.get(row).map(ColumnValueRef::Enum),
        }
    }

    fn fill_range(
        &mut self,
        start: usize,
        end: usize,
        capacity: usize,
        value: &ScopeValue,
    ) -> Result<usize, ()> {
        match (self, value) {
            (Self::Number(column), ScopeValue::Number(value)) => {
                Ok(column.fill_unset_range(start, end, capacity, *value))
            }
            (Self::Uint64(column), ScopeValue::Uint64(value)) => {
                Ok(column.fill_unset_range(start, end, capacity, *value))
            }
            (Self::Boolean(column), ScopeValue::Boolean(value)) => {
                Ok(column.fill_unset_range(start, end, capacity, *value))
            }
            (Self::Text(column), ScopeValue::Text(value)) => {
                Ok(column.fill_unset_range(start, end, capacity, value))
            }
            (Self::Enum(column), ScopeValue::EnumIndex(value)) => {
                Ok(column.fill_unset_range(start, end, capacity, *value))
            }
            _ => Err(()),
        }
    }
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
    messages: StrColumn,
    attributes: Vec<AttributeColumn>,
}

impl ThreadSpanBlock {
    fn new(capacity: usize, fields: &'static [FieldMeta]) -> Self {
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
            messages: StrColumn::new(),
            attributes: fields
                .iter()
                .map(|field| AttributeColumn::from_strategy(field.strategy))
                .collect(),
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
        if let Some(message) = input.message {
            self.messages.set(row, self.capacity, message);
        }
        self.rows += 1;
        row
    }
}

#[derive(Debug, Clone, Copy)]
struct SpanRecord {
    start_row: u32,
    completion_row: u32,
    ended: bool,
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
    VocabularyOverflow,
    InvalidUtf8,
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
            Self::VocabularyOverflow => {
                f.write_str("thread vocabulary exceeds the packed u24 range")
            }
            Self::InvalidUtf8 => f.write_str("dynamic thread-buffer string is not valid UTF-8"),
        }
    }
}
impl std::error::Error for ThreadBufferError {}

/// The one row store owned by a pinned thread.
#[derive(Debug)]
pub struct ThreadSpanBuffer {
    thread_id: u64,
    capacity: usize,
    fields: &'static [FieldMeta],
    blocks: Vec<ThreadSpanBlock>,
    row_count: usize,
    next_span_id: u32,
    spans: HashMap<u32, SpanRecord>,
    scopes: HashMap<u32, Option<SpanScope>>,
    interned: Vec<Arc<str>>,
    intern_lookup: HashMap<Arc<str>, u32>,
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
            row_count: 0,
            next_span_id: 1,
            spans: HashMap::with_capacity(capacity / 2),
            scopes: HashMap::with_capacity(capacity / 2),
            interned: Vec::new(),
            intern_lookup: HashMap::with_capacity(capacity),
        };
        buffer.blocks.push(ThreadSpanBlock::new(capacity, fields));
        buffer
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

    /// Intern a dynamic string once. The returned ordinal is stable for this buffer's entire life and survives overflow blocks.
    pub fn intern(&mut self, value: &str) -> Result<u32, ThreadBufferError> {
        if let Some(&id) = self.intern_lookup.get(value) {
            return Ok(id);
        }
        let next = self
            .interned
            .len()
            .checked_add(1)
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value <= MAX_VOCABULARY_ID)
            .ok_or(ThreadBufferError::VocabularyOverflow)?;
        let value: Arc<str> = Arc::from(value);
        self.interned.push(Arc::clone(&value));
        self.intern_lookup.insert(value, next);
        Ok(next)
    }
    #[inline]
    pub fn interned(&self, ordinal: u32) -> Option<&str> {
        ordinal
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.interned.get(index))
            .map(AsRef::as_ref)
    }
    fn interned_shared(&self, ordinal: u32) -> Option<SharedStr> {
        ordinal
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.interned.get(index))
            .map(|value| SharedStr::from(Arc::clone(value)))
    }
    /// Borrow the intern table's Arc-backed value for a warmed ABI attribute
    /// write; cloning the handle does not allocate.
    pub fn interned_shared_value(&self, ordinal: u32) -> Option<SharedStr> {
        self.interned_shared(ordinal)
    }
    fn ensure_rows(&mut self, count: usize) {
        if self
            .blocks
            .last()
            .is_none_or(|block| block.remaining() < count)
        {
            self.blocks
                .push(ThreadSpanBlock::new(self.capacity, self.fields));
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
        let block = self
            .blocks
            .last_mut()
            .expect("thread buffer always has a block");
        let row = block.write_row(input);
        self.row_count += 1;
        u32::try_from((self.blocks.len() - 1) * self.capacity + row)
            .expect("thread buffer rows fit u32")
    }

    /// Open a dynamic-name span. The completion row is reserved immediately, matching the legacy row-0/row-1 shape; end methods overwrite it later.
    pub fn open_span(
        &mut self,
        trace_id: TraceId,
        parent_thread_id: u64,
        parent_span_id: u32,
        name: SharedStr,
        timestamp: i64,
        line: u32,
    ) -> Result<u32, ThreadBufferError> {
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
            .interned_shared(name)
            .ok_or(ThreadBufferError::InvalidColumnOrdinal(
                u16::try_from(name).unwrap_or(u16::MAX),
            ))?;
        self.open_span(
            trace_id,
            parent_thread_id,
            parent_span_id,
            name,
            timestamp,
            line,
        )
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
    pub fn end_ok(&mut self, span_id: u32, timestamp: i64) -> Result<(), ThreadBufferError> {
        self.complete(span_id, EntryType::SpanOk, timestamp)
    }
    pub fn end_err(&mut self, span_id: u32, timestamp: i64) -> Result<(), ThreadBufferError> {
        self.complete(span_id, EntryType::SpanErr, timestamp)
    }
    pub fn append_log(
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
    pub fn append_log_interned(
        &mut self,
        span_id: u32,
        entry_type: EntryType,
        message: u32,
        line: u32,
        timestamp: i64,
    ) -> Result<u32, ThreadBufferError> {
        let message =
            self.interned_shared(message)
                .ok_or(ThreadBufferError::InvalidColumnOrdinal(
                    u16::try_from(message).unwrap_or(u16::MAX),
                ))?;
        self.append_log(span_id, entry_type, Some(message), line, timestamp)
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
        let capacity = self.capacity;
        let (block, local) = self.block_at_mut(row as usize)?;
        block.attributes[index].set(local, capacity, value, ordinal, strategy)
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
                let block = &mut self.blocks[block_index];
                let local_end = if run_end == block_end && run_end.is_multiple_of(self.capacity) {
                    self.capacity
                } else {
                    run_end % self.capacity
                };
                for (name, value) in scope.iter() {
                    let Some(index) = fields.iter().position(|field| field.name == name) else {
                        continue;
                    };
                    match block.attributes[index].fill_range(local, local_end, self.capacity, value)
                    {
                        Ok(count) => filled += count,
                        Err(()) => crate::scope::report_scope_mismatch(
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
    #[inline]
    pub fn timestamp_at(&self, row: usize) -> Option<i64> {
        self.block_at(row)
            .ok()
            .map(|(block, local)| block.timestamps[local])
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
        self.block_at(row)
            .ok()
            .and_then(|(block, local)| block.messages.get(local))
    }
    #[inline]
    pub fn attribute_at(&self, row: usize, ordinal: u16) -> Option<ColumnValueRef<'_>> {
        let index = usize::from(ordinal).checked_sub(SYSTEM_COLUMN_COUNT)?;
        self.block_at(row)
            .ok()
            .and_then(|(block, local)| block.attributes.get(index)?.get(local))
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
    pub fn shared_utf8(bytes: &[u8]) -> Result<SharedStr, ThreadBufferError> {
        let value = std::str::from_utf8(bytes).map_err(|_| ThreadBufferError::InvalidUtf8)?;
        Ok(SharedStr::from(Arc::<str>::from(value)))
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
}
