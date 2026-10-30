//! Continuation-sufficient logical export and import of VM slots.
//!
//! A logical slot is everything a fold needs to continue after the slot is
//! rebuilt under a changed program: live rows, an aggregate's value and
//! count, a scalar's comparison timestamp, per-row comparison timestamps,
//! bitmap members, and TTL eviction entries (which a truncated log cannot
//! re-derive). Interned cells travel as text through caller-supplied
//! resolvers; this crate owns no interner, so the encoding of each u32 lane
//! (raw, boolean, or interned id) arrives in a SlotEncoding the descriptor
//! owner supplies. Struct fields carry their encoding in their field type:
//! String and ArrayString fields are interned, Bool fields are boolean.
//!
//! Export is checked: the image's geometry is validated through bind_slot
//! before any lane is read, and a malformed image is a typed refusal, never
//! a panic or a partial slot. Keyed rows export in a deterministic order
//! (ascending logical key), list and array rows in stored order, so equal
//! folds export equal logical slots byte for byte at this level.
//!
//! Import writes into a freshly initialized image owned exclusively by the
//! caller, with the same unjournaled raw writers init_state and grow_state
//! use. It must never run on an image inside a speculation window: there
//! is no undo log to roll these writes back, by design — the imported
//! image becomes a new snapshot, it does not mutate a live one.
//! When rows outgrow a target slot, the image grows through grow_state, so
//! unchanged regions are copied exactly the way growth copies them.
//!
//! rewrite_slot_set rewrites a set of slots eagerly: it exports every
//! source slot first, initializes one new image from the target program,
//! and per slot either region-copies (geometry identical and no
//! transform: the bytes and the intern ids move untouched, so the
//! interner is never re-based) or re-imports through the logical form,
//! applying the slot's field transform plan first when one is declared.
//! The expression language itself stays the embedder's, behind
//! ExprEvaluator, the same boundary row_exprs draws for RowExpression;
//! this module owns row iteration, field assembly, coercion to the
//! target field types, and the allocation bound (one new logical slot,
//! sized by the old slot's rows times the plan's fields).
//!
//! Text is a Cow tied to the caller's source: synchronous import adapters
//! borrow certified frame/interner text, and cloning a borrowed cell copies
//! only its view. Export is the cold ownership boundary: its frozen logical
//! snapshot owns text so the source interner may be mutated during rewrite.
//! The row-view lifetime is separate from its text-source lifetime; Copy and
//! expression transforms preserve borrowed text and computed text may own.

use crate::aggregates;
use crate::bitmap_ops;
use crate::bytes;
use crate::hash_table::{ENTRY_NONE, ENTRY_U32, FlatTable, max_load_for_cap};
use crate::key_index::{HashIndex, InternBytes, KeyCx, KeyIndex, KeyLanes};
use crate::meta::SlotMetaView;
use crate::nested::{self, NestedPrefix};
use crate::state_init;
use crate::struct_map::{ArenaRegion, StructMap2Slot, StructMapSlot};
use bitmosaic::{Bitmosaic, BitmosaicView, IMAGE_ID_LEN};
use columine_types::types::{
    AggType, CONDITION_TREE_STATE_BYTES, DERIVED_FACT_EMPTY_IDENTITY, EMPTY_KEY,
    EVICTED_BUFFER_CAP, ErrorCode, SlotMetaOffset, SlotType, StructFieldType, TOMBSTONE, align8,
    arena_elem_size, is_arena_field_type, next_power_of_2, struct_field_size,
};
use core::cmp::Ordering;
use std::borrow::Cow;

// =============================================================================
// Logical values
// =============================================================================

/// One logical cell. F64 equality is by IEEE bits, so a NaN the fold stored
/// round-trips as itself instead of comparing unequal to its own import.
#[derive(Clone, Debug)]
pub enum LogicalValue<'text> {
    Bool(bool),
    U32(u32),
    U64(u64),
    I64(i64),
    F64(f64),
    /// An interned value, or a bitmap ordinal under an ordinal codec,
    /// carried as the text it names.
    Text(Cow<'text, str>),
    /// Opaque value-object bytes; schema projection is the caller's codec.
    Bytes(Vec<u8>),
    Array(Vec<LogicalValue<'text>>),
}

impl PartialEq for LogicalValue<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::U32(a), Self::U32(b)) => a == b,
            (Self::U64(a), Self::U64(b)) => a == b,
            (Self::I64(a), Self::I64(b)) => a == b,
            (Self::F64(a), Self::F64(b)) => a.to_bits() == b.to_bits(),
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for LogicalValue<'_> {}

impl LogicalValue<'_> {
    /// The variant's name, for typed mismatch causes.
    pub(crate) fn kind_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::U32(_) => "u32",
            Self::U64(_) => "u64",
            Self::I64(_) => "i64",
            Self::F64(_) => "f64",
            Self::Text(_) => "text",
            Self::Bytes(_) => "bytes",
            Self::Array(_) => "array",
        }
    }
}

/// Total deterministic order over logical values: variant rank, then the
/// variant's own order (floats by the standard total order on bits).
/// Keyed exports sort by it, so export order never depends on probe order.
fn cmp_logical(a: &LogicalValue, b: &LogicalValue) -> Ordering {
    fn rank(v: &LogicalValue) -> u8 {
        match v {
            LogicalValue::Bool(_) => 0,
            LogicalValue::U32(_) => 1,
            LogicalValue::U64(_) => 2,
            LogicalValue::I64(_) => 3,
            LogicalValue::F64(_) => 4,
            LogicalValue::Text(_) => 5,
            LogicalValue::Bytes(_) => 6,
            LogicalValue::Array(_) => 7,
        }
    }
    fn float_key(v: f64) -> u64 {
        let bits = v.to_bits();
        if bits >> 63 == 0 {
            bits | (1u64 << 63)
        } else {
            !bits
        }
    }
    match (a, b) {
        (LogicalValue::Bool(x), LogicalValue::Bool(y)) => x.cmp(y),
        (LogicalValue::U32(x), LogicalValue::U32(y)) => x.cmp(y),
        (LogicalValue::U64(x), LogicalValue::U64(y)) => x.cmp(y),
        (LogicalValue::I64(x), LogicalValue::I64(y)) => x.cmp(y),
        (LogicalValue::F64(x), LogicalValue::F64(y)) => float_key(*x).cmp(&float_key(*y)),
        (LogicalValue::Text(x), LogicalValue::Text(y)) => x.cmp(y),
        (LogicalValue::Bytes(x), LogicalValue::Bytes(y)) => x.cmp(y),
        (LogicalValue::Array(x), LogicalValue::Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                let ord = cmp_logical(xi, yi);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => rank(a).cmp(&rank(b)),
    }
}

/// One live row. For a hash map, fields holds exactly the value. For sets
/// and bitmaps fields is empty. For an array slot the key is the stored
/// index and fields holds the indexed value. cmp_ts is the comparison
/// timestamp lane the fold keeps for latest-wins kinds, when the slot has
/// one; without it a rebuilt slot would let a stale row win.
#[derive(Clone, Debug)]
pub struct LogicalRow<'text> {
    pub key: LogicalValue<'text>,
    /// The second key lane of a StructMap2 row.
    pub key2: Option<LogicalValue<'text>>,
    /// Present fields in descriptor order; None is a field the row never
    /// set (its presence bit is clear), which a reader decodes by its own
    /// wrapper, exactly as in the image.
    pub fields: Vec<Option<LogicalValue<'text>>>,
    pub cmp_ts: Option<f64>,
}

impl PartialEq for LogicalRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        let ts_eq = match (self.cmp_ts, other.cmp_ts) {
            (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
            (None, None) => true,
            _ => false,
        };
        self.key == other.key && self.key2 == other.key2 && self.fields == other.fields && ts_eq
    }
}

impl Eq for LogicalRow<'_> {}

/// An aggregate's continuation: its accumulated value and its count. An
/// average stores its sum as the value, so value and count together are
/// what the next batch folds onto.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalAggregate<'text> {
    pub value: LogicalValue<'text>,
    pub count: u64,
}

/// A scalar's continuation: its value and the comparison timestamp that
/// won it. Importing the value without the timestamp would let an older
/// row overwrite it after the slot is rebuilt.
#[derive(Clone, Debug)]
pub struct LogicalScalar<'text> {
    pub value: LogicalValue<'text>,
    pub cmp_ts: f64,
}

impl PartialEq for LogicalScalar<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.cmp_ts.to_bits() == other.cmp_ts.to_bits()
    }
}

impl Eq for LogicalScalar<'_> {}

/// One TTL eviction entry, decoded through the slot's encodings. For hash
/// maps the value is the mapped value at eviction tracking time; for sets
/// and bitmaps it is the key itself, as the TTL index stores it.
#[derive(Clone, Debug)]
pub struct LogicalEvictionEntry<'text> {
    pub timestamp: f64,
    pub key: LogicalValue<'text>,
    pub value: LogicalValue<'text>,
}

impl PartialEq for LogicalEvictionEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp.to_bits() == other.timestamp.to_bits()
            && self.key == other.key
            && self.value == other.value
    }
}

impl Eq for LogicalEvictionEntry<'_> {}

/// A condition-tree slot's continuation: its lifecycle state and every
/// occupied derived-facts cell (live and tombstone identities alike, so
/// probe chains survive). Cells carry their stored position; importing
/// into a table of another capacity writes positions as exported and the
/// owner of the derived-facts table rehashes, as it does after grow_state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalCondition {
    pub lifecycle_generation: u32,
    pub last_removed_key: u32,
    pub cells: Vec<LogicalConditionCell>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalConditionCell {
    pub pos: u32,
    pub identity: u64,
    /// The cell's value: its `values_lo` lane as the low 32 bits and its
    /// `values_hi` lane as the high 32 bits.
    pub value: u64,
}

/// A condition-tree slot's derived-facts table as init_state lays it out
/// after the slot's lifecycle state and the rule engine writes it: a u64
/// identity lane, then each value split across `values_lo: u32[capacity]`
/// and `values_hi: u32[capacity]`, 16 bytes per cell. Export and import
/// address cells only through it, so both read and write the lanes the rules do.
#[derive(Clone, Copy)]
struct FactLanes {
    identities: u32,
    capacity: u32,
}

impl FactLanes {
    fn of(slot_offset: u32, capacity: u32) -> Self {
        Self {
            identities: slot_offset + CONDITION_TREE_STATE_BYTES,
            capacity,
        }
    }

    fn identity_at(self, pos: u32) -> u32 {
        self.identities + pos * 8
    }

    fn lo_at(self, pos: u32) -> u32 {
        self.identities + self.capacity * 8 + pos * 4
    }

    fn hi_at(self, pos: u32) -> u32 {
        self.identities + self.capacity * 12 + pos * 4
    }

    fn identity(self, state: &[u8], pos: u32) -> u64 {
        bytes::read_u64(state, self.identity_at(pos))
    }

    fn value(self, state: &[u8], pos: u32) -> u64 {
        (u64::from(bytes::read_u32(state, self.hi_at(pos))) << 32)
            | u64::from(bytes::read_u32(state, self.lo_at(pos)))
    }

    fn write(self, image: &mut [u8], cell: &LogicalConditionCell) {
        let [l0, l1, l2, l3, h0, h1, h2, h3] = cell.value.to_le_bytes();
        bytes::write_u64(image, self.identity_at(cell.pos), cell.identity);
        bytes::write_u32(
            image,
            self.lo_at(cell.pos),
            u32::from_le_bytes([l0, l1, l2, l3]),
        );
        bytes::write_u32(
            image,
            self.hi_at(cell.pos),
            u32::from_le_bytes([h0, h1, h2, h3]),
        );
    }
}

/// One outer entry of a nested slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalNestedEntry<'text> {
    pub outer_key: LogicalValue<'text>,
    pub inner: LogicalInner<'text>,
}

/// An inner container of a nested slot: a map's or set's rows, or an
/// aggregate's value and count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalInner<'text> {
    pub kind: SlotType,
    /// Raw AggType byte for an aggregate inner container.
    pub agg_type: u8,
    pub rows: Vec<LogicalRow<'text>>,
    pub aggregate: Option<LogicalAggregate<'text>>,
}

/// The logical form of one slot: rows plus every continuation field its
/// kind keeps outside the rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalSlot<'text> {
    pub slot: u32,
    pub kind: SlotType,
    /// Raw AggType byte (aggregate and scalar subtype); for a nested slot,
    /// the inner aggregate's byte. Zero where the kind has no subtype.
    pub agg_type: u8,
    /// Struct field types in descriptor order (struct maps and struct
    /// ordered lists). For a scalar ordered list, the single element type.
    pub field_types: Vec<StructFieldType>,
    pub rows: Vec<LogicalRow<'text>>,
    pub aggregate: Option<LogicalAggregate<'text>>,
    pub scalar: Option<LogicalScalar<'text>>,
    pub nested: Vec<LogicalNestedEntry<'text>>,
    /// Live TTL eviction-index entries, in stored order.
    pub eviction: Vec<LogicalEvictionEntry<'text>>,
    /// Entries in the evicted ring (evict-trigger slots), in stored order.
    pub evicted: Vec<LogicalEvictionEntry<'text>>,
    pub condition: Option<LogicalCondition>,
}

impl LogicalSlot<'_> {
    fn empty(slot: u32, kind: SlotType, agg_type: u8) -> Self {
        Self {
            slot,
            kind,
            agg_type,
            field_types: Vec::new(),
            rows: Vec::new(),
            aggregate: None,
            scalar: None,
            nested: Vec::new(),
            eviction: Vec::new(),
            evicted: Vec::new(),
            condition: None,
        }
    }
}

// =============================================================================
// Encodings and interner boundary
// =============================================================================

/// How one u32 lane decodes to a logical value. Boolean is its own lane
/// kind: a descriptor that says a value is boolean (rather than an
/// interned id) is part of the schema identity, and the two must never
/// be guessed from the stored id.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CellEncoding {
    Raw,
    Boolean,
    Interned,
}

/// The lane encodings of one slot, as its canonical descriptor declares.
/// Struct row fields ignore this: their field types carry the encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotEncoding {
    pub key: CellEncoding,
    pub key2: CellEncoding,
    /// The value lane: hash-map values, set members, bitmap members (so
    /// an ordinal codec is a resolver), array values, ordered-list
    /// elements, and scalar values. Nested outer keys use key.
    pub value: CellEncoding,
    pub inner_key: CellEncoding,
    pub inner_value: CellEncoding,
}

impl SlotEncoding {
    pub const RAW: Self = Self {
        key: CellEncoding::Raw,
        key2: CellEncoding::Raw,
        value: CellEncoding::Raw,
        inner_key: CellEncoding::Raw,
        inner_value: CellEncoding::Raw,
    };
}

impl Default for SlotEncoding {
    fn default() -> Self {
        Self::RAW
    }
}

/// Read side of the caller's interner (or of a bitmap ordinal codec):
/// the text an id names.
pub trait InternLookup {
    fn text(&self, id: u32) -> Option<&str>;
}

/// The caller's interner: resolves ids to text on export and interns text
/// back to ids on import. Passing the source image's own interner keeps
/// every id stable — the interner moves without re-basing.
pub trait Interner: InternLookup {
    fn intern(&mut self, text: &str) -> Result<u32, ErrorCode>;
}

// =============================================================================
// Errors
// =============================================================================

/// Why an export, import, rewrite, or transform application refused. The
/// VM's own refusals ride as Vm with their ErrorCode intact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalExportCause {
    Vm(ErrorCode),
    /// An interned id with no text in the caller's resolver.
    UnknownInternId(u32),
    KindMismatch {
        expected: SlotType,
        found: SlotType,
    },
    /// Import target slot already holds rows; import owns fresh images.
    NonEmptyTarget,
    DuplicateKey,
    /// A key cell holding a table sentinel (EMPTY_KEY or TOMBSTONE).
    SentinelKey(u32),
    FieldShape {
        expected: usize,
        found: usize,
    },
    FieldTypeMismatch {
        field: u32,
        expected: &'static str,
        found: &'static str,
    },
    Capacity {
        needed: u64,
        capacity: u64,
    },
    InvalidRow(&'static str),
}

impl From<ErrorCode> for LogicalExportCause {
    fn from(code: ErrorCode) -> Self {
        Self::Vm(code)
    }
}

/// One refused logical operation, naming the operation, the slot it acted
/// on, and the cause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalExportError {
    pub op: &'static str,
    pub slot: u32,
    pub cause: LogicalExportCause,
}

type CauseResult<T> = Result<T, LogicalExportCause>;

fn wrap<T>(op: &'static str, slot: u32, result: CauseResult<T>) -> Result<T, LogicalExportError> {
    result.map_err(|cause| LogicalExportError { op, slot, cause })
}

// =============================================================================
// Cell codecs
// =============================================================================

fn decode_u32(
    raw: u32,
    enc: CellEncoding,
    lookup: &dyn InternLookup,
) -> CauseResult<LogicalValue<'static>> {
    match enc {
        CellEncoding::Raw => Ok(LogicalValue::U32(raw)),
        CellEncoding::Boolean => Ok(LogicalValue::Bool(raw != 0)),
        CellEncoding::Interned => lookup
            .text(raw)
            .map(|text| LogicalValue::Text(text.to_owned().into()))
            .ok_or(LogicalExportCause::UnknownInternId(raw)),
    }
}

fn encode_u32(
    value: &LogicalValue,
    enc: CellEncoding,
    interner: &mut dyn Interner,
) -> CauseResult<u32> {
    match (enc, value) {
        (CellEncoding::Raw, LogicalValue::U32(v)) => Ok(*v),
        (CellEncoding::Boolean, LogicalValue::Bool(v)) => Ok(u32::from(*v)),
        (CellEncoding::Interned, LogicalValue::Text(text)) => {
            interner.intern(text).map_err(LogicalExportCause::Vm)
        }
        _ => Err(LogicalExportCause::FieldTypeMismatch {
            field: 0,
            expected: match enc {
                CellEncoding::Raw => "u32",
                CellEncoding::Boolean => "bool",
                CellEncoding::Interned => "text",
            },
            found: value.kind_name(),
        }),
    }
}

fn key_u32(
    value: &LogicalValue,
    enc: CellEncoding,
    interner: &mut dyn Interner,
) -> CauseResult<u32> {
    let key = encode_u32(value, enc, interner)?;
    if key == EMPTY_KEY || key == TOMBSTONE {
        return Err(LogicalExportCause::SentinelKey(key));
    }
    Ok(key)
}

// =============================================================================
// Export
// =============================================================================

fn meta_byte(state: &[u8], meta: &SlotMetaView, off: u32) -> u8 {
    state[(meta.meta_base + off) as usize]
}

/// The struct-map kind byte: nonzero when the slot carries a per-row
/// f64 comparison-timestamp lane after its rows.
fn struct_kind_byte(state: &[u8], meta: &SlotMetaView) -> u8 {
    meta_byte(state, meta, SlotMetaOffset::TTL_SECONDS + 2)
}

fn sort_rows(rows: &mut [LogicalRow]) {
    rows.sort_by(|a, b| {
        cmp_logical(&a.key, &b.key).then_with(|| match (&a.key2, &b.key2) {
            (Some(x), Some(y)) => cmp_logical(x, y),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        })
    });
}

fn export_eviction(
    state: &[u8],
    meta: &SlotMetaView,
    key_enc: CellEncoding,
    value_enc: CellEncoding,
    lookup: &dyn InternLookup,
    out: &mut LogicalSlot<'static>,
) -> CauseResult<()> {
    if !meta.has_ttl() {
        return Ok(());
    }
    let read_entries = |base: u32, count: u32| -> CauseResult<Vec<LogicalEvictionEntry<'static>>> {
        let mut entries = Vec::with_capacity(count as usize);
        for i in 0..count {
            let at = base + i * state_init::EVICTION_ENTRY_SIZE;
            entries.push(LogicalEvictionEntry {
                timestamp: bytes::read_f64(state, at),
                key: decode_u32(bytes::read_u32(state, at + 8), key_enc, lookup)?,
                value: decode_u32(bytes::read_u32(state, at + 12), value_enc, lookup)?,
            });
        }
        Ok(entries)
    };
    out.eviction = read_entries(
        meta.eviction_index_offset(state),
        meta.eviction_index_size(state),
    )?;
    if meta.has_evict_trigger() {
        out.evicted = read_entries(meta.evicted_buffer_offset(state), meta.evicted_count(state))?;
    }
    Ok(())
}

fn aggregate_value(
    state: &[u8],
    offset: u32,
    agg_byte: u8,
) -> CauseResult<LogicalAggregate<'static>> {
    let kind = if agg_byte == 0 {
        AggType::Sum
    } else {
        AggType::from_u8(agg_byte).ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?
    };
    let count = aggregates::agg_count(state, offset, agg_byte);
    let value = match kind {
        AggType::Sum | AggType::Min | AggType::Max | AggType::Avg => {
            LogicalValue::F64(aggregates::agg_value_f64(state, offset))
        }
        AggType::Count => LogicalValue::U64(count),
        AggType::SumI64 | AggType::MinI64 | AggType::MaxI64 => {
            LogicalValue::I64(aggregates::agg_value_i64(state, offset))
        }
        AggType::ScalarU32 | AggType::ScalarF64 | AggType::ScalarI64 => {
            return Err(LogicalExportCause::Vm(ErrorCode::InvalidState));
        }
    };
    Ok(LogicalAggregate { value, count })
}

fn decode_arena_payload(
    payload: &[u8],
    field_type: StructFieldType,
    lookup: &dyn InternLookup,
) -> CauseResult<LogicalValue<'static>> {
    let text = |id: u32| -> CauseResult<LogicalValue<'static>> {
        lookup
            .text(id)
            .map(|t| LogicalValue::Text(t.to_owned().into()))
            .ok_or(LogicalExportCause::UnknownInternId(id))
    };
    let value = match field_type {
        StructFieldType::ArrayU32 => LogicalValue::Array(
            payload
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| LogicalValue::U32(bytes::read_u32(c, 0)))
                .collect(),
        ),
        StructFieldType::ArrayString => LogicalValue::Array(
            payload
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| text(bytes::read_u32(c, 0)))
                .collect::<CauseResult<Vec<_>>>()?,
        ),
        StructFieldType::ArrayI64 => LogicalValue::Array(
            payload
                .as_chunks::<8>()
                .0
                .iter()
                .map(|c| LogicalValue::I64(bytes::read_i64(c, 0)))
                .collect(),
        ),
        StructFieldType::ArrayF64 => LogicalValue::Array(
            payload
                .as_chunks::<8>()
                .0
                .iter()
                .map(|c| LogicalValue::F64(bytes::read_f64(c, 0)))
                .collect(),
        ),
        StructFieldType::ArrayBool => LogicalValue::Array(
            payload
                .iter()
                .map(|&b| LogicalValue::Bool(b != 0))
                .collect(),
        ),
        StructFieldType::Vo => LogicalValue::Bytes(payload.to_vec()),
        _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
    };
    Ok(value)
}

/// Decode one stored struct row's fields. arena_payloads resolves an arena
/// field's raw bytes by field index; scalar fields read inline. A list
/// row has no arena: its resolver refuses, which a set arena-field bit in
/// a list surfaces as a typed refusal instead of invented bytes.
fn decode_row_fields(
    state: &[u8],
    row_off: u32,
    bitset_bytes: u32,
    descriptor: &[StructFieldType],
    lookup: &dyn InternLookup,
    mut arena_payload: impl FnMut(u8) -> CauseResult<Option<Vec<u8>>>,
) -> CauseResult<Vec<Option<LogicalValue<'static>>>> {
    let mut fields = Vec::with_capacity(descriptor.len());
    let mut offset = bitset_bytes;
    for (index, &field_type) in descriptor.iter().enumerate() {
        let field = index as u8;
        let size = struct_field_size(field_type);
        if !StructMapSlot::is_field_set(state, row_off, field) {
            fields.push(None);
            offset += size;
            continue;
        }
        let at = row_off + offset;
        let value = match field_type {
            StructFieldType::UInt32 => LogicalValue::U32(bytes::read_u32(state, at)),
            StructFieldType::String => {
                let id = bytes::read_u32(state, at);
                lookup
                    .text(id)
                    .map(|t| LogicalValue::Text(t.to_owned().into()))
                    .ok_or(LogicalExportCause::UnknownInternId(id))?
            }
            StructFieldType::Int64 => LogicalValue::I64(bytes::read_i64(state, at)),
            StructFieldType::Float64 => LogicalValue::F64(bytes::read_f64(state, at)),
            StructFieldType::Bool => LogicalValue::Bool(state[at as usize] != 0),
            arena if is_arena_field_type(arena) => {
                let payload =
                    arena_payload(field)?.ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?;
                decode_arena_payload(&payload, arena, lookup)?
            }
            _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
        };
        fields.push(Some(value));
        offset += size;
    }
    Ok(fields)
}

fn descriptor_of(state: &[u8], offset: u32, num_fields: u8) -> CauseResult<Vec<StructFieldType>> {
    let mut out = Vec::with_capacity(usize::from(num_fields));
    for i in 0..u32::from(num_fields) {
        let byte = state[(offset + i) as usize];
        out.push(
            StructFieldType::from_u8(byte)
                .ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?,
        );
    }
    Ok(out)
}

fn export_slot_inner<I: KeyIndex>(
    state: &[u8],
    slot: u32,
    enc: &SlotEncoding,
    lookup: &dyn InternLookup,
) -> CauseResult<LogicalSlot<'static>> {
    let meta = state_init::bind_slot_in::<I>(state, slot).map_err(LogicalExportCause::Vm)?;
    let kind = meta.slot_type();
    let offset = meta.offset;
    let capacity = meta.capacity;
    // Byte 13 is the aggregate/scalar subtype only for those kinds; on
    // struct kinds it counts fields, and a nested slot keeps its inner
    // aggregate byte in its prefix (set in the nested arm below).
    let agg_type = match kind {
        SlotType::Aggregate | SlotType::Scalar => meta.agg_type_byte(state),
        _ => 0,
    };
    let mut out = LogicalSlot::empty(slot, kind, agg_type);
    match kind {
        SlotType::HashMap => {
            let values = offset + capacity * 4;
            let timestamps = offset + capacity * 8;
            let has_ts = meta.has_hashmap_timestamp_storage();
            for pos in 0..capacity {
                let key = bytes::read_u32(state, offset + pos * 4);
                if key == EMPTY_KEY || key == TOMBSTONE {
                    continue;
                }
                out.rows.push(LogicalRow {
                    key: decode_u32(key, enc.key, lookup)?,
                    key2: None,
                    fields: vec![Some(decode_u32(
                        bytes::read_u32(state, values + pos * 4),
                        enc.value,
                        lookup,
                    )?)],
                    cmp_ts: has_ts.then(|| bytes::read_f64(state, timestamps + pos * 8)),
                });
            }
            sort_rows(&mut out.rows);
            export_eviction(state, &meta, enc.key, enc.value, lookup, &mut out)?;
        }
        SlotType::HashSet => {
            for pos in 0..capacity {
                let key = bytes::read_u32(state, offset + pos * 4);
                if key == EMPTY_KEY || key == TOMBSTONE {
                    continue;
                }
                out.rows.push(LogicalRow {
                    key: decode_u32(key, enc.key, lookup)?,
                    key2: None,
                    fields: Vec::new(),
                    cmp_ts: None,
                });
            }
            sort_rows(&mut out.rows);
            export_eviction(state, &meta, enc.key, enc.key, lookup, &mut out)?;
        }
        SlotType::Aggregate => {
            out.aggregate = Some(aggregate_value(state, offset, meta.agg_type_byte(state))?);
        }
        SlotType::Scalar => {
            let agg_byte = meta.agg_type_byte(state);
            let value = match AggType::from_u8(agg_byte) {
                Some(AggType::ScalarU32) => LogicalValue::U32(bytes::read_u32(state, offset)),
                Some(AggType::ScalarF64) => LogicalValue::F64(bytes::read_f64(state, offset)),
                Some(AggType::ScalarI64) => LogicalValue::I64(bytes::read_i64(state, offset)),
                _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
            };
            // A scalar u32 lane is an interned id when the descriptor
            // says so, like any other value lane.
            let value = match value {
                LogicalValue::U32(raw) => decode_u32(raw, enc.value, lookup)?,
                other => other,
            };
            out.scalar = Some(LogicalScalar {
                value,
                cmp_ts: bytes::read_f64(state, offset + 8),
            });
        }
        SlotType::Array => {
            let timestamps = offset + capacity * 4;
            for pos in 0..capacity {
                let value = bytes::read_u32(state, offset + pos * 4);
                if value == EMPTY_KEY || value == TOMBSTONE {
                    continue;
                }
                out.rows.push(LogicalRow {
                    key: LogicalValue::U32(pos),
                    key2: None,
                    fields: vec![Some(decode_u32(value, enc.value, lookup)?)],
                    cmp_ts: Some(bytes::read_f64(state, timestamps + pos * 8)),
                });
            }
        }
        SlotType::Bitmap => {
            let storage = bitmap_ops::get_bitmap_storage(&meta);
            let data = storage
                .serialized_data(state)
                .map_err(LogicalExportCause::Vm)?;
            if data.len() > IMAGE_ID_LEN {
                let view = BitmosaicView::open_verified(data)
                    .ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?;
                for member in view.range() {
                    out.rows.push(LogicalRow {
                        key: decode_u32(member, enc.value, lookup)?,
                        key2: None,
                        fields: Vec::new(),
                        cmp_ts: None,
                    });
                }
            }
            sort_rows(&mut out.rows);
            export_eviction(state, &meta, enc.value, enc.value, lookup, &mut out)?;
        }
        SlotType::ConditionTree => {
            let lanes = FactLanes::of(offset, capacity);
            let mut cells = Vec::new();
            for pos in 0..capacity {
                let identity = lanes.identity(state, pos);
                if identity == DERIVED_FACT_EMPTY_IDENTITY {
                    continue;
                }
                cells.push(LogicalConditionCell {
                    pos,
                    identity,
                    value: lanes.value(state, pos),
                });
            }
            out.condition = Some(LogicalCondition {
                lifecycle_generation: bytes::read_u32(state, offset),
                last_removed_key: bytes::read_u32(state, offset + 4),
                cells,
            });
        }
        SlotType::StructMap => {
            let map = StructMapSlot::<I>::bind_in(state, slot);
            let descriptor = descriptor_of(state, offset, map.num_fields)?;
            out.field_types = descriptor.clone();
            let has_ts = struct_kind_byte(state, &meta) != 0;
            let timestamps = map.rows_base + capacity * map.row_size;
            for pos in 0..capacity {
                let Some(key) = map.key_at(state, pos) else {
                    continue;
                };
                let fields = decode_row_fields(
                    state,
                    map.row_off(pos),
                    map.bitset_bytes,
                    &descriptor,
                    lookup,
                    |field| {
                        map.arena_field(state, pos, field)
                            .map(|payload| payload.map(<[u8]>::to_vec))
                            .map_err(LogicalExportCause::Vm)
                    },
                )?;
                out.rows.push(LogicalRow {
                    key: decode_u32(key, enc.key, lookup)?,
                    key2: None,
                    fields,
                    cmp_ts: has_ts.then(|| bytes::read_f64(state, timestamps + pos * 8)),
                });
            }
            sort_rows(&mut out.rows);
        }
        SlotType::StructMap2 => {
            let map = StructMap2Slot::<I>::bind_in(state, slot);
            let descriptor = descriptor_of(state, offset, map.num_fields)?;
            out.field_types = descriptor.clone();
            for pos in 0..capacity {
                let Some((key1, key2)) = map.key_at(state, pos) else {
                    continue;
                };
                let fields = decode_row_fields(
                    state,
                    map.row_off(pos),
                    map.bitset_bytes,
                    &descriptor,
                    lookup,
                    |field| {
                        map.rows()
                            .arena_field(state, pos, field)
                            .map(|payload| payload.map(<[u8]>::to_vec))
                            .map_err(LogicalExportCause::Vm)
                    },
                )?;
                out.rows.push(LogicalRow {
                    key: decode_u32(key1, enc.key, lookup)?,
                    key2: Some(decode_u32(key2, enc.key2, lookup)?),
                    fields,
                    cmp_ts: None,
                });
            }
            sort_rows(&mut out.rows);
        }
        SlotType::OrderedList => {
            let num_fields = meta_byte(state, &meta, SlotMetaOffset::AGG_TYPE);
            let bitset_bytes =
                u32::from(meta_byte(state, &meta, SlotMetaOffset::STRUCT_BITSET_BYTES));
            let row_size = u32::from(u16::from_le_bytes([
                state[(meta.meta_base + SlotMetaOffset::TTL_SECONDS) as usize],
                state[(meta.meta_base + SlotMetaOffset::TTL_SECONDS + 1) as usize],
            ]));
            let kind_byte = struct_kind_byte(state, &meta);
            let count = meta.size(state);
            if kind_byte == 0xff {
                let descriptor = descriptor_of(state, offset, num_fields)?;
                out.field_types = descriptor.clone();
                let rows_base = offset + align8(u32::from(num_fields));
                for index in 0..count {
                    let fields = decode_row_fields(
                        state,
                        rows_base + index * row_size,
                        bitset_bytes,
                        &descriptor,
                        lookup,
                        |_field| {
                            Err(LogicalExportCause::InvalidRow(
                                "arena field set in an ordered-list row, which has no arena",
                            ))
                        },
                    )?;
                    out.rows.push(LogicalRow {
                        key: LogicalValue::U32(index),
                        key2: None,
                        fields,
                        cmp_ts: None,
                    });
                }
            } else {
                let elem = StructFieldType::from_u8(kind_byte)
                    .ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?;
                if is_arena_field_type(elem) {
                    return Err(LogicalExportCause::InvalidRow(
                        "ordered-list element type has no inline logical form",
                    ));
                }
                out.field_types = vec![elem];
                for index in 0..count {
                    let at = offset + index * row_size;
                    let value = decode_scalar_cell(state, at, elem, lookup)?;
                    out.rows.push(LogicalRow {
                        key: LogicalValue::U32(index),
                        key2: None,
                        fields: vec![Some(value)],
                        cmp_ts: None,
                    });
                }
            }
        }
        SlotType::Nested => {
            let prefix = nested::read_nested_prefix(state, offset);
            out.agg_type = prefix.inner_agg_type_byte;
            let keys = nested::outer_keys_offset(offset);
            let pointers = nested::outer_ptrs_offset(offset, capacity);
            for pos in 0..capacity {
                let outer = bytes::read_u32(state, keys + pos * 4);
                if outer == EMPTY_KEY || outer == TOMBSTONE {
                    continue;
                }
                let pointer = bytes::read_u32(state, pointers + pos * 4);
                let inner = export_inner(state, pointer, &prefix, enc, lookup)?;
                out.nested.push(LogicalNestedEntry {
                    outer_key: decode_u32(outer, enc.key, lookup)?,
                    inner,
                });
            }
            out.nested
                .sort_by(|a, b| cmp_logical(&a.outer_key, &b.outer_key));
        }
    }
    Ok(out)
}

fn decode_scalar_cell(
    state: &[u8],
    at: u32,
    elem: StructFieldType,
    lookup: &dyn InternLookup,
) -> CauseResult<LogicalValue<'static>> {
    Ok(match elem {
        StructFieldType::UInt32 => LogicalValue::U32(bytes::read_u32(state, at)),
        StructFieldType::String => {
            let id = bytes::read_u32(state, at);
            lookup
                .text(id)
                .map(|t| LogicalValue::Text(t.to_owned().into()))
                .ok_or(LogicalExportCause::UnknownInternId(id))?
        }
        StructFieldType::Int64 => LogicalValue::I64(bytes::read_i64(state, at)),
        StructFieldType::Float64 => LogicalValue::F64(bytes::read_f64(state, at)),
        StructFieldType::Bool => LogicalValue::Bool(state[at as usize] != 0),
        _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
    })
}

fn export_inner(
    state: &[u8],
    pointer: u32,
    prefix: &NestedPrefix,
    enc: &SlotEncoding,
    lookup: &dyn InternLookup,
) -> CauseResult<LogicalInner<'static>> {
    let mut inner = LogicalInner {
        kind: prefix.inner_type,
        agg_type: prefix.inner_agg_type_byte,
        rows: Vec::new(),
        aggregate: None,
    };
    match prefix.inner_type {
        SlotType::HashMap => {
            let table = FlatTable::bind(state, pointer, ENTRY_U32);
            for pos in 0..table.cap {
                let key = table.key_at(state, pos);
                if key == EMPTY_KEY || key == TOMBSTONE {
                    continue;
                }
                inner.rows.push(LogicalRow {
                    key: decode_u32(key, enc.inner_key, lookup)?,
                    key2: None,
                    fields: vec![Some(decode_u32(
                        table.entry_u32_at(state, pos),
                        enc.inner_value,
                        lookup,
                    )?)],
                    cmp_ts: None,
                });
            }
            sort_rows(&mut inner.rows);
        }
        SlotType::HashSet => {
            let table = FlatTable::bind(state, pointer, ENTRY_NONE);
            for pos in 0..table.cap {
                let key = table.key_at(state, pos);
                if key == EMPTY_KEY || key == TOMBSTONE {
                    continue;
                }
                inner.rows.push(LogicalRow {
                    key: decode_u32(key, enc.inner_key, lookup)?,
                    key2: None,
                    fields: Vec::new(),
                    cmp_ts: None,
                });
            }
            sort_rows(&mut inner.rows);
        }
        SlotType::Aggregate => {
            inner.aggregate = Some(aggregate_value(state, pointer, prefix.inner_agg_type_byte)?);
        }
        _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
    }
    Ok(inner)
}

/// Export one slot to its continuation-sufficient logical form. The
/// image is geometry-validated first; a malformed image, an out-of-range
/// slot, or an interned id the resolver does not know is a typed refusal
/// and no partial slot escapes.
pub fn export_slot(
    state: &[u8],
    slot: u32,
    enc: &SlotEncoding,
    lookup: &dyn InternLookup,
) -> Result<LogicalSlot<'static>, LogicalExportError> {
    export_slot_in::<HashIndex>(state, slot, enc, lookup)
}

/// [`export_slot`] from an image of key-index family `I`. Keyed rows export
/// in logical-key order whatever the family's own order.
pub fn export_slot_in<I: KeyIndex>(
    state: &[u8],
    slot: u32,
    enc: &SlotEncoding,
    lookup: &dyn InternLookup,
) -> Result<LogicalSlot<'static>, LogicalExportError> {
    wrap(
        "export_slot",
        slot,
        export_slot_inner::<I>(state, slot, enc, lookup),
    )
}

// =============================================================================
// Import
// =============================================================================

fn reserve_image(size: u32) -> CauseResult<Vec<u8>> {
    let mut image = Vec::new();
    image
        .try_reserve_exact(size as usize)
        .map_err(|_| LogicalExportCause::Vm(ErrorCode::OutOfMemory))?;
    image.resize(size as usize, 0);
    Ok(image)
}

fn grow_image<I: KeyIndex>(image: &mut Vec<u8>, slot: u32) -> CauseResult<()> {
    let new_size = state_init::calculate_grown_state_size_in::<I>(image, slot)
        .map_err(LogicalExportCause::Vm)?;
    let mut next = reserve_image(new_size)?;
    state_init::grow_state_in::<I>(image, &mut next, slot).map_err(LogicalExportCause::Vm)?;
    *image = next;
    Ok(())
}

/// Grow slot until fits accepts the image. Growth doubles, so the loop
/// is bounded by the capacity word's range; 64 iterations is beyond it.
fn grow_until<I: KeyIndex>(
    image: &mut Vec<u8>,
    slot: u32,
    mut fits: impl FnMut(&[u8]) -> CauseResult<bool>,
) -> CauseResult<()> {
    for _ in 0..64 {
        if fits(image)? {
            return Ok(());
        }
        grow_image::<I>(image, slot)?;
    }
    Err(LogicalExportCause::Capacity {
        needed: u64::MAX,
        capacity: 0,
    })
}

fn bind_target<I: KeyIndex>(image: &[u8], logical: &LogicalSlot) -> CauseResult<SlotMetaView> {
    let meta =
        state_init::bind_slot_in::<I>(image, logical.slot).map_err(LogicalExportCause::Vm)?;
    if meta.slot_type() != logical.kind {
        return Err(LogicalExportCause::KindMismatch {
            expected: logical.kind,
            found: meta.slot_type(),
        });
    }
    Ok(meta)
}

fn require_empty(image: &[u8], meta: &SlotMetaView) -> CauseResult<()> {
    let non_empty = match meta.slot_type() {
        // Values, not containers: import overwrites them whole.
        SlotType::Aggregate | SlotType::Scalar => false,
        SlotType::Bitmap => meta.size(image) != 0,
        _ => meta.size(image) != 0 || (meta.has_ttl() && meta.eviction_index_size(image) != 0),
    };
    if non_empty {
        return Err(LogicalExportCause::NonEmptyTarget);
    }
    Ok(())
}

/// The comparison timestamp a target with a timestamp lane must store
/// for this row. A missing one refuses: inventing a value would make
/// the row lose every latest-wins comparison after import.
fn required_cmp_ts(row: &LogicalRow) -> CauseResult<f64> {
    row.cmp_ts.ok_or(LogicalExportCause::InvalidRow(
        "row is missing the comparison timestamp the target slot stores",
    ))
}

/// Refuse a comparison timestamp a target with no lane cannot store.
/// Dropping one is the caller's explicit choice on the logical slot,
/// never a silent loss inside import.
fn no_cmp_ts(row: &LogicalRow) -> CauseResult<()> {
    if row.cmp_ts.is_some() {
        return Err(LogicalExportCause::InvalidRow(
            "row carries a comparison timestamp the target slot cannot store",
        ));
    }
    Ok(())
}

fn single_field<'row, 'text>(
    row: &'row LogicalRow<'text>,
) -> CauseResult<&'row LogicalValue<'text>> {
    if row.fields.len() != 1 {
        return Err(LogicalExportCause::FieldShape {
            expected: 1,
            found: row.fields.len(),
        });
    }
    row.fields[0].as_ref().ok_or(LogicalExportCause::InvalidRow(
        "a value row with an absent value",
    ))
}

fn no_fields(row: &LogicalRow) -> CauseResult<()> {
    if !row.fields.is_empty() {
        return Err(LogicalExportCause::FieldShape {
            expected: 0,
            found: row.fields.len(),
        });
    }
    Ok(())
}

struct ArenaWriter {
    data: u32,
    header: u32,
    capacity: u32,
    used: u32,
}

impl ArenaWriter {
    fn bind(state: &[u8], header: u32) -> CauseResult<Self> {
        let arena = ArenaRegion::bind(state, header).map_err(LogicalExportCause::Vm)?;
        Ok(Self {
            data: arena.data,
            header: arena.header,
            capacity: arena.capacity,
            used: arena.used,
        })
    }

    fn append(&mut self, state: &mut [u8], payload: &[u8]) -> CauseResult<u32> {
        let len = u32::try_from(payload.len())
            .map_err(|_| LogicalExportCause::Vm(ErrorCode::InvalidState))?;
        let next = self
            .used
            .checked_add(len)
            .ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?;
        if next > self.capacity {
            return Err(LogicalExportCause::Capacity {
                needed: u64::from(next),
                capacity: u64::from(self.capacity),
            });
        }
        bytes::copy(state, self.data + self.used, payload, 0, len);
        let offset = self.used;
        self.used = next;
        bytes::write_u32(state, self.header + 4, self.used);
        Ok(offset)
    }
}

fn encode_arena_payload(
    value: &LogicalValue,
    field_type: StructFieldType,
    field: u32,
    interner: &mut dyn Interner,
) -> CauseResult<Vec<u8>> {
    let mismatch = |found: &'static str| LogicalExportCause::FieldTypeMismatch {
        field,
        expected: "array",
        found,
    };
    if field_type == StructFieldType::Vo {
        return match value {
            LogicalValue::Bytes(payload) => Ok(payload.clone()),
            other => Err(LogicalExportCause::FieldTypeMismatch {
                field,
                expected: "bytes",
                found: other.kind_name(),
            }),
        };
    }
    let LogicalValue::Array(items) = value else {
        return Err(mismatch(value.kind_name()));
    };
    let mut payload = Vec::with_capacity(items.len() * arena_elem_size(field_type) as usize);
    for item in items {
        match (field_type, item) {
            (StructFieldType::ArrayU32, LogicalValue::U32(v)) => {
                payload.extend_from_slice(&v.to_le_bytes())
            }
            (StructFieldType::ArrayString, LogicalValue::Text(t)) => payload.extend_from_slice(
                &interner
                    .intern(t)
                    .map_err(LogicalExportCause::Vm)?
                    .to_le_bytes(),
            ),
            (StructFieldType::ArrayI64, LogicalValue::I64(v)) => {
                payload.extend_from_slice(&v.to_le_bytes())
            }
            (StructFieldType::ArrayF64, LogicalValue::F64(v)) => {
                payload.extend_from_slice(&v.to_le_bytes())
            }
            (StructFieldType::ArrayBool, LogicalValue::Bool(v)) => payload.push(u8::from(*v)),
            _ => {
                return Err(LogicalExportCause::FieldTypeMismatch {
                    field,
                    expected: "array element",
                    found: item.kind_name(),
                });
            }
        }
    }
    Ok(payload)
}

fn arena_payload_len(value: &LogicalValue, field_type: StructFieldType) -> u64 {
    match value {
        LogicalValue::Bytes(payload) if field_type == StructFieldType::Vo => payload.len() as u64,
        LogicalValue::Array(items) => items.len() as u64 * u64::from(arena_elem_size(field_type)),
        _ => 0,
    }
}

/// Write one struct row's fields into a zeroed row.
fn write_row_fields(
    state: &mut [u8],
    row_off: u32,
    bitset_bytes: u32,
    descriptor: &[StructFieldType],
    fields: &[Option<LogicalValue>],
    arena: Option<&mut ArenaWriter>,
    interner: &mut dyn Interner,
) -> CauseResult<()> {
    if fields.len() != descriptor.len() {
        return Err(LogicalExportCause::FieldShape {
            expected: descriptor.len(),
            found: fields.len(),
        });
    }
    let mut arena = arena;
    let mut offset = bitset_bytes;
    for (index, (&field_type, value)) in descriptor.iter().zip(fields.iter()).enumerate() {
        let size = struct_field_size(field_type);
        let Some(value) = value else {
            offset += size;
            continue;
        };
        let at = row_off + offset;
        let mismatch = |expected: &'static str| LogicalExportCause::FieldTypeMismatch {
            field: index as u32,
            expected,
            found: value.kind_name(),
        };
        match field_type {
            StructFieldType::UInt32 => match value {
                LogicalValue::U32(v) => bytes::write_u32(state, at, *v),
                _ => return Err(mismatch("u32")),
            },
            StructFieldType::String => match value {
                LogicalValue::Text(t) => {
                    let id = interner.intern(t).map_err(LogicalExportCause::Vm)?;
                    bytes::write_u32(state, at, id);
                }
                _ => return Err(mismatch("text")),
            },
            StructFieldType::Int64 => match value {
                LogicalValue::I64(v) => bytes::write_i64(state, at, *v),
                _ => return Err(mismatch("i64")),
            },
            StructFieldType::Float64 => match value {
                LogicalValue::F64(v) => bytes::write_f64(state, at, *v),
                _ => return Err(mismatch("f64")),
            },
            StructFieldType::Bool => match value {
                LogicalValue::Bool(v) => state[at as usize] = u8::from(*v),
                _ => return Err(mismatch("bool")),
            },
            arena_type if is_arena_field_type(arena_type) => {
                let writer = arena.as_deref_mut().ok_or(LogicalExportCause::InvalidRow(
                    "arena field in a row whose slot has no arena",
                ))?;
                let payload = encode_arena_payload(value, arena_type, index as u32, interner)?;
                let count = if arena_type == StructFieldType::Vo {
                    payload.len() as u32
                } else {
                    match value {
                        LogicalValue::Array(items) => items.len() as u32,
                        _ => return Err(mismatch("array")),
                    }
                };
                let arena_off = writer.append(state, &payload)?;
                bytes::write_u32(state, at, arena_off);
                bytes::write_u32(state, at + 4, count);
            }
            _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
        }
        StructMapSlot::set_field_bit(state, row_off, index as u8);
        offset += size;
    }
    Ok(())
}

fn target_descriptor(
    image: &[u8],
    meta: &SlotMetaView,
) -> CauseResult<(Vec<StructFieldType>, u8, u32, u32)> {
    let num_fields = meta_byte(image, meta, SlotMetaOffset::AGG_TYPE);
    let bitset_bytes = u32::from(meta_byte(image, meta, SlotMetaOffset::STRUCT_BITSET_BYTES));
    let row_size = u32::from(u16::from_le_bytes([
        image[(meta.meta_base + SlotMetaOffset::TTL_SECONDS) as usize],
        image[(meta.meta_base + SlotMetaOffset::TTL_SECONDS + 1) as usize],
    ]));
    let descriptor = descriptor_of(image, meta.offset, num_fields)?;
    Ok((descriptor, num_fields, bitset_bytes, row_size))
}

fn require_descriptor(logical: &LogicalSlot, descriptor: &[StructFieldType]) -> CauseResult<()> {
    if logical.field_types != descriptor {
        return Err(LogicalExportCause::FieldShape {
            expected: descriptor.len(),
            found: logical.field_types.len(),
        });
    }
    Ok(())
}

fn write_eviction(
    image: &mut [u8],
    meta: &SlotMetaView,
    logical: &LogicalSlot,
    key_enc: CellEncoding,
    value_enc: CellEncoding,
    interner: &mut dyn Interner,
) -> CauseResult<()> {
    if !meta.has_ttl() {
        if !logical.eviction.is_empty() || !logical.evicted.is_empty() {
            return Err(LogicalExportCause::InvalidRow(
                "eviction entries for a slot without TTL would be lost",
            ));
        }
        return Ok(());
    }
    let write_entries = |state: &mut [u8],
                         base: u32,
                         entries: &[LogicalEvictionEntry],
                         interner: &mut dyn Interner|
     -> CauseResult<()> {
        for (i, entry) in entries.iter().enumerate() {
            let at = base + i as u32 * state_init::EVICTION_ENTRY_SIZE;
            bytes::write_f64(state, at, entry.timestamp);
            bytes::write_u32(state, at + 8, encode_u32(&entry.key, key_enc, interner)?);
            bytes::write_u32(
                state,
                at + 12,
                encode_u32(&entry.value, value_enc, interner)?,
            );
        }
        Ok(())
    };
    if logical.eviction.len() as u64 > u64::from(meta.eviction_index_capacity(image)) {
        return Err(LogicalExportCause::Capacity {
            needed: logical.eviction.len() as u64,
            capacity: u64::from(meta.eviction_index_capacity(image)),
        });
    }
    let base = meta.eviction_index_offset(image);
    write_entries(image, base, &logical.eviction, interner)?;
    meta.set_eviction_index_size(image, logical.eviction.len() as u32);
    if meta.has_evict_trigger() {
        if logical.evicted.len() as u64 > u64::from(EVICTED_BUFFER_CAP) {
            return Err(LogicalExportCause::Capacity {
                needed: logical.evicted.len() as u64,
                capacity: u64::from(EVICTED_BUFFER_CAP),
            });
        }
        let base = meta.evicted_buffer_offset(image);
        write_entries(image, base, &logical.evicted, interner)?;
        meta.set_evicted_count(image, logical.evicted.len() as u32);
    } else if !logical.evicted.is_empty() {
        return Err(LogicalExportCause::InvalidRow(
            "evicted entries for a slot without an evict trigger would be lost",
        ));
    }
    Ok(())
}

fn import_slot_inner<I: KeyIndex>(
    image: &mut Vec<u8>,
    logical: &LogicalSlot,
    enc: &SlotEncoding,
    interner: &mut dyn Interner,
) -> CauseResult<()> {
    let slot = logical.slot;
    let meta = bind_target::<I>(image, logical)?;
    require_empty(image, &meta)?;
    match logical.kind {
        SlotType::HashMap => {
            let rows = logical.rows.len() as u32;
            let eviction = logical.eviction.len() as u32;
            grow_until::<I>(image, slot, |img| {
                let m = SlotMetaView::read(img, slot);
                Ok(rows <= max_load_for_cap(m.capacity) && eviction <= m.capacity)
            })?;
            let meta = bind_target::<I>(image, logical)?;
            let table = FlatTable::bind_external(
                meta.offset,
                meta.capacity,
                meta.meta_base + SlotMetaOffset::SIZE,
                ENTRY_U32,
            );
            let has_ts = meta.has_hashmap_timestamp_storage();
            let timestamps = meta.offset + meta.capacity * 8;
            for row in &logical.rows {
                if row.key2.is_some() {
                    return Err(LogicalExportCause::InvalidRow(
                        "second key on a hash-map row",
                    ));
                }
                let key = key_u32(&row.key, enc.key, interner)?;
                let value = encode_u32(single_field(row)?, enc.value, interner)?;
                let probe = table
                    .find_insert(image, key)
                    .ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?;
                if probe.found {
                    return Err(LogicalExportCause::DuplicateKey);
                }
                if table.size(image) >= table.max_load() {
                    return Err(LogicalExportCause::Capacity {
                        needed: u64::from(table.size(image)) + 1,
                        capacity: u64::from(table.max_load()),
                    });
                }
                table.set_key_at(image, probe.pos, key);
                table.set_entry_u32_at(image, probe.pos, value);
                let size = table.size(image);
                table.set_size(image, size + 1);
                if has_ts {
                    bytes::write_f64(image, timestamps + probe.pos * 8, required_cmp_ts(row)?);
                } else {
                    no_cmp_ts(row)?;
                }
            }
            let meta = bind_target::<I>(image, logical)?;
            write_eviction(image, &meta, logical, enc.key, enc.value, interner)?;
        }
        SlotType::HashSet => {
            let rows = logical.rows.len() as u32;
            let eviction = logical.eviction.len() as u32;
            grow_until::<I>(image, slot, |img| {
                let m = SlotMetaView::read(img, slot);
                Ok(rows <= max_load_for_cap(m.capacity) && eviction <= m.capacity)
            })?;
            let meta = bind_target::<I>(image, logical)?;
            let table = FlatTable::bind_external(
                meta.offset,
                meta.capacity,
                meta.meta_base + SlotMetaOffset::SIZE,
                ENTRY_NONE,
            );
            for row in &logical.rows {
                no_cmp_ts(row)?;
                no_fields(row)?;
                if row.key2.is_some() {
                    return Err(LogicalExportCause::InvalidRow(
                        "second key on a hash-set row",
                    ));
                }
                let key = key_u32(&row.key, enc.key, interner)?;
                match table.insert_key(image, key) {
                    Some(true) => {}
                    Some(false) => return Err(LogicalExportCause::DuplicateKey),
                    None => {
                        return Err(LogicalExportCause::Capacity {
                            needed: u64::from(table.size(image)) + 1,
                            capacity: u64::from(table.max_load()),
                        });
                    }
                }
            }
            let meta = bind_target::<I>(image, logical)?;
            write_eviction(image, &meta, logical, enc.key, enc.key, interner)?;
        }
        SlotType::Aggregate => {
            let aggregate = logical
                .aggregate
                .clone()
                .ok_or(LogicalExportCause::InvalidRow(
                    "aggregate slot without a value",
                ))?;
            let meta = bind_target::<I>(image, logical)?;
            let agg_byte = meta.agg_type_byte(image);
            if agg_byte != logical.agg_type && logical.agg_type != 0 {
                return Err(LogicalExportCause::InvalidRow("aggregate subtype mismatch"));
            }
            write_aggregate(image, meta.offset, agg_byte, &aggregate)?;
        }
        SlotType::Scalar => {
            let scalar = logical
                .scalar
                .clone()
                .ok_or(LogicalExportCause::InvalidRow(
                    "scalar slot without a value",
                ))?;
            let meta = bind_target::<I>(image, logical)?;
            match AggType::from_u8(meta.agg_type_byte(image)) {
                Some(AggType::ScalarU32) => {
                    let raw = encode_u32(&scalar.value, enc.value, interner)?;
                    bytes::write_u32(image, meta.offset, raw);
                }
                Some(AggType::ScalarF64) => match scalar.value {
                    LogicalValue::F64(v) => bytes::write_f64(image, meta.offset, v),
                    ref other => {
                        return Err(LogicalExportCause::FieldTypeMismatch {
                            field: 0,
                            expected: "f64",
                            found: other.kind_name(),
                        });
                    }
                },
                Some(AggType::ScalarI64) => match scalar.value {
                    LogicalValue::I64(v) => bytes::write_i64(image, meta.offset, v),
                    ref other => {
                        return Err(LogicalExportCause::FieldTypeMismatch {
                            field: 0,
                            expected: "i64",
                            found: other.kind_name(),
                        });
                    }
                },
                _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
            }
            bytes::write_f64(image, meta.offset + 8, scalar.cmp_ts);
        }
        SlotType::Array => {
            let mut max_pos: Option<u32> = None;
            for row in &logical.rows {
                let LogicalValue::U32(pos) = row.key else {
                    return Err(LogicalExportCause::InvalidRow(
                        "array row key is not a stored index",
                    ));
                };
                max_pos = Some(max_pos.map_or(pos, |m: u32| m.max(pos)));
            }
            if let Some(max_pos) = max_pos {
                grow_until::<I>(image, slot, |img| {
                    Ok(SlotMetaView::read(img, slot).capacity > max_pos)
                })?;
            }
            let meta = bind_target::<I>(image, logical)?;
            let timestamps = meta.offset + meta.capacity * 4;
            let mut live = 0u32;
            for row in &logical.rows {
                let LogicalValue::U32(pos) = row.key else {
                    unreachable!("checked while sizing");
                };
                let value = encode_u32(single_field(row)?, enc.value, interner)?;
                if value == EMPTY_KEY || value == TOMBSTONE {
                    return Err(LogicalExportCause::SentinelKey(value));
                }
                if bytes::read_u32(image, meta.offset + pos * 4) != EMPTY_KEY {
                    return Err(LogicalExportCause::DuplicateKey);
                }
                bytes::write_u32(image, meta.offset + pos * 4, value);
                bytes::write_f64(image, timestamps + pos * 8, required_cmp_ts(row)?);
                live += 1;
            }
            meta.set_size(image, live);
        }
        SlotType::Bitmap => {
            let mut members: Vec<u32> = Vec::with_capacity(logical.rows.len());
            for row in &logical.rows {
                no_cmp_ts(row)?;
                no_fields(row)?;
                members.push(encode_u32(&row.key, enc.value, interner)?);
            }
            members.sort_unstable();
            if members.windows(2).any(|w| w[0] == w[1]) {
                return Err(LogicalExportCause::DuplicateKey);
            }
            let image_bytes = Bitmosaic::from_sorted(members.iter().copied()).to_forest_bytes();
            let needed = image_bytes.len() as u32;
            let eviction = logical.eviction.len() as u32;
            grow_until::<I>(image, slot, |img| {
                let m = SlotMetaView::read(img, slot);
                Ok(bitmap_ops::bitmap_payload_capacity(m.capacity) >= needed
                    && eviction <= m.capacity)
            })?;
            let meta = bind_target::<I>(image, logical)?;
            let storage = bitmap_ops::get_bitmap_storage(&meta);
            bytes::zero(image, storage.data_offset, storage.payload_capacity);
            bytes::copy(image, storage.data_offset, &image_bytes, 0, needed);
            meta.set_size(image, members.len() as u32);
            write_eviction(image, &meta, logical, enc.value, enc.value, interner)?;
        }
        SlotType::ConditionTree => {
            let condition = logical
                .condition
                .as_ref()
                .ok_or(LogicalExportCause::InvalidRow(
                    "condition slot without state",
                ))?;
            let meta = bind_target::<I>(image, logical)?;
            let lanes = FactLanes::of(meta.offset, meta.capacity);
            bytes::write_u32(image, meta.offset, condition.lifecycle_generation);
            bytes::write_u32(image, meta.offset + 4, condition.last_removed_key);
            for cell in &condition.cells {
                if cell.pos >= meta.capacity {
                    return Err(LogicalExportCause::Capacity {
                        needed: u64::from(cell.pos) + 1,
                        capacity: u64::from(meta.capacity),
                    });
                }
                if lanes.identity(image, cell.pos) != DERIVED_FACT_EMPTY_IDENTITY {
                    return Err(LogicalExportCause::DuplicateKey);
                }
                lanes.write(image, cell);
            }
            meta.set_size(image, condition.cells.len() as u32);
        }
        SlotType::StructMap => {
            let rows = logical.rows.len() as u32;
            grow_until::<I>(image, slot, |img| {
                let m = SlotMetaView::read(img, slot);
                if rows > I::max_rows(m.capacity) {
                    return Ok(false);
                }
                let map = StructMapSlot::<I>::bind_in(img, slot);
                if map.arena_header_off == 0 {
                    return Ok(true);
                }
                let arena =
                    ArenaRegion::bind(img, map.arena_header_off).map_err(LogicalExportCause::Vm)?;
                Ok(u64::from(arena.capacity) >= arena_demand(logical))
            })?;
            let meta = bind_target::<I>(image, logical)?;
            let (descriptor, _, _, _) = target_descriptor(image, &meta)?;
            require_descriptor(logical, &descriptor)?;
            let map = StructMapSlot::<I>::bind_in(image, slot);
            let mut writer = if map.arena_header_off != 0 {
                Some(ArenaWriter::bind(image, map.arena_header_off)?)
            } else {
                None
            };
            let has_ts = struct_kind_byte(image, &meta) != 0;
            let timestamps = map.rows_base + map.capacity * map.row_size;
            let mut scratch = I::Scratch::default();
            for row in &logical.rows {
                let key = key_u32(&row.key, enc.key, interner)?;
                let strings = InternerBytes(&*interner);
                let mut cx = KeyCx::<I, _>::new(&strings, &mut scratch);
                let plan = map
                    .plan_upsert(image, key, &mut cx)
                    .map_err(|code| import_refusal(code, rows, map.capacity))?;
                if !plan.is_new {
                    return Err(LogicalExportCause::DuplicateKey);
                }
                map.apply_upsert(image, key, &plan, &mut cx)
                    .map_err(LogicalExportCause::Vm)?;
                let row_off = map.row_off(plan.ordinal);
                bytes::zero(image, row_off, map.row_size);
                write_row_fields(
                    image,
                    row_off,
                    map.bitset_bytes,
                    &descriptor,
                    &row.fields,
                    writer.as_mut(),
                    interner,
                )?;
                if has_ts {
                    bytes::write_f64(image, timestamps + plan.ordinal * 8, required_cmp_ts(row)?);
                } else {
                    no_cmp_ts(row)?;
                }
            }
        }
        SlotType::StructMap2 => {
            let rows = logical.rows.len() as u32;
            grow_until::<I>(image, slot, |img| {
                let m = SlotMetaView::read(img, slot);
                if rows > I::max_rows(m.capacity) {
                    return Ok(false);
                }
                let header = bytes::read_u32(img, m.meta_base + SlotMetaOffset::GRACE_SECONDS);
                if header == 0 {
                    return Ok(true);
                }
                let arena = ArenaRegion::bind(img, header).map_err(LogicalExportCause::Vm)?;
                Ok(u64::from(arena.capacity) >= arena_demand(logical))
            })?;
            let meta = bind_target::<I>(image, logical)?;
            let (descriptor, _, _, _) = target_descriptor(image, &meta)?;
            require_descriptor(logical, &descriptor)?;
            let map = StructMap2Slot::<I>::bind_in(image, slot);
            let mut writer = if map.arena_header_off != 0 {
                Some(ArenaWriter::bind(image, map.arena_header_off)?)
            } else {
                None
            };
            let mut scratch = I::Scratch::default();
            for row in &logical.rows {
                no_cmp_ts(row)?;
                let key1 = key_u32(&row.key, enc.key, interner)?;
                let key2 = match &row.key2 {
                    Some(value) => encode_u32(value, enc.key2, interner)?,
                    None => {
                        return Err(LogicalExportCause::InvalidRow(
                            "struct-map2 row without its second key",
                        ));
                    }
                };
                let strings = InternerBytes(&*interner);
                let mut cx = KeyCx::<I, _>::new(&strings, &mut scratch);
                let plan = map
                    .plan_upsert(image, key1, key2, &mut cx)
                    .map_err(|code| import_refusal(code, rows, map.capacity))?;
                if !plan.is_new {
                    return Err(LogicalExportCause::DuplicateKey);
                }
                map.apply_upsert(image, key1, key2, &plan, &mut cx)
                    .map_err(LogicalExportCause::Vm)?;
                let row_off = map.row_off(plan.ordinal);
                bytes::zero(image, row_off, map.row_size);
                write_row_fields(
                    image,
                    row_off,
                    map.bitset_bytes,
                    &descriptor,
                    &row.fields,
                    writer.as_mut(),
                    interner,
                )?;
            }
        }
        SlotType::OrderedList => {
            let rows = logical.rows.len() as u32;
            grow_until::<I>(image, slot, |img| {
                Ok(SlotMetaView::read(img, slot).capacity >= rows)
            })?;
            let meta = bind_target::<I>(image, logical)?;
            let kind_byte = struct_kind_byte(image, &meta);
            if kind_byte == 0xff {
                let (descriptor, _, bitset_bytes, row_size) = target_descriptor(image, &meta)?;
                require_descriptor(logical, &descriptor)?;
                let rows_base = meta.offset + align8(descriptor.len() as u32);
                for (index, row) in logical.rows.iter().enumerate() {
                    no_cmp_ts(row)?;
                    let row_off = rows_base + index as u32 * row_size;
                    bytes::zero(image, row_off, row_size);
                    write_row_fields(
                        image,
                        row_off,
                        bitset_bytes,
                        &descriptor,
                        &row.fields,
                        None,
                        interner,
                    )?;
                }
            } else {
                let elem = StructFieldType::from_u8(kind_byte)
                    .ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?;
                if logical.field_types != [elem] {
                    return Err(LogicalExportCause::FieldShape {
                        expected: 1,
                        found: logical.field_types.len(),
                    });
                }
                let row_size = struct_field_size(elem);
                for (index, row) in logical.rows.iter().enumerate() {
                    no_cmp_ts(row)?;
                    let at = meta.offset + index as u32 * row_size;
                    write_scalar_cell(image, at, elem, single_field(row)?, interner)?;
                }
            }
            meta.set_size(image, rows);
        }
        SlotType::Nested => {
            import_nested::<I>(image, logical, enc, interner)?;
        }
    }
    Ok(())
}

fn arena_demand(logical: &LogicalSlot) -> u64 {
    let mut demand = 0u64;
    for row in &logical.rows {
        for (value, &field_type) in row.fields.iter().zip(logical.field_types.iter()) {
            if let Some(value) = value {
                demand += arena_payload_len(value, field_type);
            }
        }
    }
    demand
}

fn write_aggregate(
    state: &mut [u8],
    offset: u32,
    agg_byte: u8,
    aggregate: &LogicalAggregate,
) -> CauseResult<()> {
    let kind = if agg_byte == 0 {
        AggType::Sum
    } else {
        AggType::from_u8(agg_byte).ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?
    };
    match (kind, &aggregate.value) {
        (AggType::Sum | AggType::Min | AggType::Max | AggType::Avg, LogicalValue::F64(v)) => {
            aggregates::agg_set_value_f64(state, offset, *v);
        }
        (AggType::Count, LogicalValue::U64(_)) => {}
        (AggType::SumI64 | AggType::MinI64 | AggType::MaxI64, LogicalValue::I64(v)) => {
            aggregates::agg_set_value_i64(state, offset, *v);
        }
        (_, other) => {
            return Err(LogicalExportCause::FieldTypeMismatch {
                field: 0,
                expected: "aggregate value",
                found: other.kind_name(),
            });
        }
    }
    let count = match &aggregate.value {
        LogicalValue::U64(v) if kind == AggType::Count => *v,
        _ => aggregate.count,
    };
    aggregates::agg_set_count(state, offset, agg_byte, count);
    Ok(())
}

fn write_scalar_cell(
    state: &mut [u8],
    at: u32,
    elem: StructFieldType,
    value: &LogicalValue,
    interner: &mut dyn Interner,
) -> CauseResult<()> {
    let mismatch = |expected: &'static str| LogicalExportCause::FieldTypeMismatch {
        field: 0,
        expected,
        found: value.kind_name(),
    };
    match elem {
        StructFieldType::UInt32 => match value {
            LogicalValue::U32(v) => bytes::write_u32(state, at, *v),
            _ => return Err(mismatch("u32")),
        },
        StructFieldType::String => match value {
            LogicalValue::Text(t) => {
                let id = interner.intern(t).map_err(LogicalExportCause::Vm)?;
                bytes::write_u32(state, at, id);
            }
            _ => return Err(mismatch("text")),
        },
        StructFieldType::Int64 => match value {
            LogicalValue::I64(v) => bytes::write_i64(state, at, *v),
            _ => return Err(mismatch("i64")),
        },
        StructFieldType::Float64 => match value {
            LogicalValue::F64(v) => bytes::write_f64(state, at, *v),
            _ => return Err(mismatch("f64")),
        },
        StructFieldType::Bool => match value {
            LogicalValue::Bool(v) => state[at as usize] = u8::from(*v),
            _ => return Err(mismatch("bool")),
        },
        _ => {
            return Err(LogicalExportCause::InvalidRow(
                "ordered-list element type has no inline logical form",
            ));
        }
    }
    Ok(())
}

fn chosen_inner_capacity(prefix: &NestedPrefix, rows: usize) -> u32 {
    let mut cap = next_power_of_2(u32::from(prefix.inner_initial_cap).max(16));
    while (max_load_for_cap(cap) as usize) < rows.max(1) {
        cap *= 2;
    }
    cap
}

fn import_nested<I: KeyIndex>(
    image: &mut Vec<u8>,
    logical: &LogicalSlot,
    enc: &SlotEncoding,
    interner: &mut dyn Interner,
) -> CauseResult<()> {
    let slot = logical.slot;
    let meta0 = SlotMetaView::read(image, slot);
    let prefix = nested::read_nested_prefix(image, meta0.offset);
    for entry in &logical.nested {
        if entry.inner.kind != prefix.inner_type {
            return Err(LogicalExportCause::KindMismatch {
                expected: prefix.inner_type,
                found: entry.inner.kind,
            });
        }
        // Like the top-level aggregate guard: a stated inner subtype
        // must match the target's, or a Sum would import as a Min
        // (both store an f64 value and count).
        if entry.inner.kind == SlotType::Aggregate
            && entry.inner.agg_type != 0
            && entry.inner.agg_type != prefix.inner_agg_type_byte
        {
            return Err(LogicalExportCause::InvalidRow(
                "nested aggregate subtype mismatch",
            ));
        }
    }
    let arena_needed: u64 = logical
        .nested
        .iter()
        .map(|entry| {
            let rows = entry.inner.rows.len();
            let cap = chosen_inner_capacity(&prefix, rows);
            u64::from(align8(nested::inner_container_size(
                prefix.inner_type,
                cap,
                prefix.inner_agg_type_byte,
            )))
        })
        .sum();
    let outer_rows = logical.nested.len() as u32;
    grow_until::<I>(image, slot, |img| {
        let m = SlotMetaView::read(img, slot);
        if outer_rows > max_load_for_cap(m.capacity) {
            return Ok(false);
        }
        let arena = nested::Arena::bind(nested::arena_header_offset(m.offset, m.capacity));
        Ok(u64::from(arena.capacity(img)) >= arena_needed)
    })?;
    let meta = bind_target::<I>(image, logical)?;
    let prefix = nested::read_nested_prefix(image, meta.offset);
    let outer = FlatTable::bind_external(
        nested::outer_keys_offset(meta.offset),
        meta.capacity,
        meta.meta_base + SlotMetaOffset::SIZE,
        ENTRY_U32,
    );
    let arena = nested::Arena::bind(nested::arena_header_offset(meta.offset, meta.capacity));
    for entry in &logical.nested {
        let outer_key = key_u32(&entry.outer_key, enc.key, interner)?;
        let cap = chosen_inner_capacity(&prefix, entry.inner.rows.len());
        let size = nested::inner_container_size(prefix.inner_type, cap, prefix.inner_agg_type_byte);
        let pointer = arena
            .alloc(image, size)
            .ok_or(LogicalExportCause::Capacity {
                needed: arena_needed,
                capacity: u64::from(arena.capacity(image)),
            })?;
        match prefix.inner_type {
            SlotType::HashMap => {
                let table = FlatTable::init(image, pointer, cap, ENTRY_U32);
                for row in &entry.inner.rows {
                    no_cmp_ts(row)?;
                    let key = key_u32(&row.key, enc.inner_key, interner)?;
                    let value = encode_u32(single_field(row)?, enc.inner_value, interner)?;
                    match table.upsert_u32(image, key, value) {
                        Some(true) => {}
                        Some(false) => return Err(LogicalExportCause::DuplicateKey),
                        None => {
                            return Err(LogicalExportCause::Capacity {
                                needed: entry.inner.rows.len() as u64,
                                capacity: u64::from(table.max_load()),
                            });
                        }
                    }
                }
            }
            SlotType::HashSet => {
                let table = FlatTable::init(image, pointer, cap, ENTRY_NONE);
                for row in &entry.inner.rows {
                    no_cmp_ts(row)?;
                    no_fields(row)?;
                    let key = key_u32(&row.key, enc.inner_key, interner)?;
                    match table.insert_key(image, key) {
                        Some(true) => {}
                        Some(false) => return Err(LogicalExportCause::DuplicateKey),
                        None => {
                            return Err(LogicalExportCause::Capacity {
                                needed: entry.inner.rows.len() as u64,
                                capacity: u64::from(table.max_load()),
                            });
                        }
                    }
                }
            }
            SlotType::Aggregate => {
                let aggregate =
                    entry
                        .inner
                        .aggregate
                        .clone()
                        .ok_or(LogicalExportCause::InvalidRow(
                            "nested aggregate without a value",
                        ))?;
                bytes::zero(image, pointer, size);
                write_aggregate(image, pointer, prefix.inner_agg_type_byte, &aggregate)?;
            }
            _ => return Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
        }
        match outer.upsert_u32(image, outer_key, pointer) {
            Some(true) => {}
            Some(false) => return Err(LogicalExportCause::DuplicateKey),
            None => {
                return Err(LogicalExportCause::Capacity {
                    needed: u64::from(outer_rows),
                    capacity: u64::from(outer.max_load()),
                });
            }
        }
    }
    Ok(())
}

/// Import one logical slot into a freshly initialized image. The image
/// grows through grow_state when the rows, the bitmap image, the struct
/// arena, or the eviction index outgrow the target slot. The target slot
/// must be empty (aggregates and scalars are overwritten whole); change
/// flags stay clear, as on a freshly initialized image.
pub fn import_slot(
    image: &mut Vec<u8>,
    logical: &LogicalSlot,
    enc: &SlotEncoding,
    interner: &mut dyn Interner,
) -> Result<(), LogicalExportError> {
    import_slot_in::<HashIndex>(image, logical, enc, interner)
}

/// [`import_slot`] into an image of key-index family `I`. The interner
/// resolves the ids it hands out as the index orders them, so a family that
/// reads interned lanes reads the text this import interned.
pub fn import_slot_in<I: KeyIndex>(
    image: &mut Vec<u8>,
    logical: &LogicalSlot,
    enc: &SlotEncoding,
    interner: &mut dyn Interner,
) -> Result<(), LogicalExportError> {
    wrap(
        "import_slot",
        logical.slot,
        import_slot_inner::<I>(image, logical, enc, interner),
    )
}

/// An interner's text as a key index's resolver: the bytes of the text it
/// already holds, so an id this import interned resolves for the index.
struct InternerBytes<'a>(&'a dyn Interner);

impl InternBytes for InternerBytes<'_> {
    fn bytes(&self, id: u32) -> Option<&[u8]> {
        self.0.text(id).map(str::as_bytes)
    }
}

/// A struct-map insert the family refused: capacity past its row bound, or
/// the family's own refusal.
fn import_refusal(code: ErrorCode, rows: u32, capacity: u32) -> LogicalExportCause {
    if code == ErrorCode::CapacityExceeded || code == ErrorCode::InvalidKey {
        LogicalExportCause::Capacity {
            needed: u64::from(rows),
            capacity: u64::from(capacity),
        }
    } else {
        LogicalExportCause::Vm(code)
    }
}

// =============================================================================
// Field transforms
// =============================================================================

/// How one target field is produced from the old row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldTransform {
    /// Copy old field by index (coerced to the target field type).
    Copy(u8),
    /// An opaque expression over the old row, evaluated by the
    /// embedder's ExprEvaluator: bytecode and its constants, in the
    /// embedder's own expression encoding.
    Expr {
        bytecode: Vec<u8>,
        constants: Vec<u8>,
    },
    Absent,
    /// Project an old value-object field's writer bytes through the
    /// reader codec named by its root commitment: an evolved read of the
    /// stored frame. Evaluation goes through ExprEvaluator::project:
    /// this crate owns no value-object codec, exactly as it owns no
    /// expression evaluator.
    Project {
        from: u8,
        reader: [u8; 32],
    },
}

/// One slot's per-field transform plan, in target field order. A scalar or
/// aggregate slot takes exactly one field, over its old value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransformPlan {
    pub fields: Vec<FieldTransform>,
}

/// The frozen old row a transform expression reads: its key, its fields
/// (the old value as field 0 for scalar and aggregate slots), its
/// comparison timestamp, and the whole exported slot set as the frozen
/// basis for reads of other slots in the set.
pub struct TransformRowView<'a, 'text> {
    pub key: Option<&'a LogicalValue<'text>>,
    pub key2: Option<&'a LogicalValue<'text>>,
    pub fields: &'a [Option<LogicalValue<'text>>],
    pub cmp_ts: Option<f64>,
    pub basis: &'a [LogicalSlot<'text>],
}

/// The embedder's expression evaluator for field transforms. Evaluation
/// is a pure function of the bytecode, constants, and the frozen row
/// view: no clock, no entropy, no live state (the old image is already
/// frozen in the exported slot set). None is an absent field.
pub trait ExprEvaluator {
    fn eval<'text>(
        &mut self,
        bytecode: &[u8],
        constants: &[u8],
        row: &TransformRowView<'_, 'text>,
    ) -> Result<Option<LogicalValue<'text>>, ErrorCode>;

    /// Project one value-object field's writer bytes to the reader codec
    /// named by its root commitment. The default refuses: an evaluator
    /// with no value-object codecs cannot silently pass writer bytes
    /// through as reader bytes.
    fn project<'text>(
        &mut self,
        _bytes: &[u8],
        _reader: &[u8; 32],
        _row: &TransformRowView<'_, 'text>,
    ) -> Result<Option<LogicalValue<'text>>, ErrorCode> {
        Err(ErrorCode::InvalidProgram)
    }
}

fn coerce<'text>(
    value: LogicalValue<'text>,
    target: StructFieldType,
    field: u32,
) -> CauseResult<Option<LogicalValue<'text>>> {
    let found = value.kind_name();
    let mismatch = |expected: &'static str| LogicalExportCause::FieldTypeMismatch {
        field,
        expected,
        found,
    };
    let integral_f64 = |v: f64| -> Option<i128> {
        if v.is_finite() && v == v.trunc() && (-1.7e38..=1.7e38).contains(&v) {
            // Exactness is checked by the caller's range test on the
            // narrowed value; this only rejects fractions and non-finite.
            Some(v as i128)
        } else {
            None
        }
    };
    let out = match target {
        StructFieldType::UInt32 => match value {
            LogicalValue::U32(v) => LogicalValue::U32(v),
            LogicalValue::U64(v) if v <= u64::from(u32::MAX) => LogicalValue::U32(v as u32),
            LogicalValue::I64(v) if v >= 0 && v <= i64::from(u32::MAX) => {
                LogicalValue::U32(v as u32)
            }
            LogicalValue::F64(v) => match integral_f64(v) {
                Some(n) if n >= 0 && n <= i128::from(u32::MAX) && (n as u32) as f64 == v => {
                    LogicalValue::U32(n as u32)
                }
                _ => return Err(mismatch("u32")),
            },
            _ => return Err(mismatch("u32")),
        },
        StructFieldType::Int64 => match value {
            LogicalValue::I64(v) => LogicalValue::I64(v),
            LogicalValue::U32(v) => LogicalValue::I64(i64::from(v)),
            LogicalValue::U64(v) if v <= i64::MAX as u64 => LogicalValue::I64(v as i64),
            LogicalValue::F64(v) => match integral_f64(v) {
                Some(n)
                    if n >= i128::from(i64::MIN)
                        && n <= i128::from(i64::MAX)
                        && (n as i64) as f64 == v =>
                {
                    LogicalValue::I64(n as i64)
                }
                _ => return Err(mismatch("i64")),
            },
            _ => return Err(mismatch("i64")),
        },
        StructFieldType::Float64 => match value {
            LogicalValue::F64(v) => LogicalValue::F64(v),
            LogicalValue::U32(v) => LogicalValue::F64(f64::from(v)),
            LogicalValue::U64(v) => LogicalValue::F64(v as f64),
            LogicalValue::I64(v) => LogicalValue::F64(v as f64),
            _ => return Err(mismatch("f64")),
        },
        StructFieldType::Bool => match value {
            LogicalValue::Bool(v) => LogicalValue::Bool(v),
            _ => return Err(mismatch("bool")),
        },
        StructFieldType::String => match value {
            LogicalValue::Text(t) => LogicalValue::Text(t),
            _ => return Err(mismatch("text")),
        },
        StructFieldType::Vo => match value {
            LogicalValue::Bytes(b) => LogicalValue::Bytes(b),
            _ => return Err(mismatch("bytes")),
        },
        arena if is_arena_field_type(arena) => match value {
            LogicalValue::Array(items) => {
                let elem = match arena {
                    StructFieldType::ArrayU32 => StructFieldType::UInt32,
                    StructFieldType::ArrayString => StructFieldType::String,
                    StructFieldType::ArrayI64 => StructFieldType::Int64,
                    StructFieldType::ArrayF64 => StructFieldType::Float64,
                    StructFieldType::ArrayBool => StructFieldType::Bool,
                    _ => unreachable!("arena kinds enumerated"),
                };
                let mut coerced = Vec::with_capacity(items.len());
                for item in items {
                    // A scalar target coerces to a present value or
                    // refuses; absence has no element form.
                    let item = coerce(item, elem, field)?.ok_or_else(|| mismatch("array"))?;
                    coerced.push(item);
                }
                LogicalValue::Array(coerced)
            }
            _ => return Err(mismatch("array")),
        },
        _ => return Err(mismatch("value")),
    };
    Ok(Some(out))
}

fn eval_field_transform<'text>(
    up: &FieldTransform,
    view: &TransformRowView<'_, 'text>,
    target: StructFieldType,
    field: u32,
    eval: &mut dyn ExprEvaluator,
) -> CauseResult<Option<LogicalValue<'text>>> {
    match up {
        FieldTransform::Absent => Ok(None),
        FieldTransform::Copy(old) => match view.fields.get(usize::from(*old)) {
            Some(Some(value)) => coerce(value.clone(), target, field),
            Some(None) => Ok(None),
            None => Err(LogicalExportCause::FieldShape {
                expected: usize::from(*old) + 1,
                found: view.fields.len(),
            }),
        },
        FieldTransform::Project { from, reader } => match view.fields.get(usize::from(*from)) {
            Some(Some(LogicalValue::Bytes(payload))) => {
                match eval
                    .project(payload, reader, view)
                    .map_err(LogicalExportCause::Vm)?
                {
                    Some(value) => coerce(value, target, field),
                    None => Ok(None),
                }
            }
            Some(Some(other)) => Err(LogicalExportCause::FieldTypeMismatch {
                field,
                expected: "bytes",
                found: other.kind_name(),
            }),
            Some(None) => Ok(None),
            None => Err(LogicalExportCause::FieldShape {
                expected: usize::from(*from) + 1,
                found: view.fields.len(),
            }),
        },
        FieldTransform::Expr {
            bytecode,
            constants,
        } => match eval
            .eval(bytecode, constants, view)
            .map_err(LogicalExportCause::Vm)?
        {
            Some(value) => coerce(value, target, field),
            None => Ok(None),
        },
    }
}

fn scalar_target_type(old: &LogicalSlot) -> CauseResult<StructFieldType> {
    match AggType::from_u8(old.agg_type) {
        Some(AggType::ScalarU32) => Ok(StructFieldType::UInt32),
        Some(AggType::ScalarF64)
        | Some(AggType::Sum)
        | Some(AggType::Min)
        | Some(AggType::Max)
        | Some(AggType::Avg) => Ok(StructFieldType::Float64),
        Some(AggType::ScalarI64)
        | Some(AggType::SumI64)
        | Some(AggType::MinI64)
        | Some(AggType::MaxI64) => Ok(StructFieldType::Int64),
        Some(AggType::Count) => Ok(StructFieldType::UInt32),
        _ if old.agg_type == 0 => Ok(StructFieldType::Float64),
        _ => Err(LogicalExportCause::Vm(ErrorCode::InvalidState)),
    }
}

/// Apply one slot's field transform plan over its exported rows, producing the
/// target-shape logical slot. Rows evaluate in export order and fields
/// in plan order, so the result is a deterministic function of the old
/// slot, the plan, and the evaluator's answers. Keys, comparison
/// timestamps, aggregate counts, and eviction entries carry unchanged:
/// the plan rewrites values, never row identity or fold continuation.
pub fn apply_transform<'text>(
    old: &LogicalSlot<'text>,
    target_field_types: &[StructFieldType],
    plan: &TransformPlan,
    basis: &[LogicalSlot<'text>],
    eval: &mut dyn ExprEvaluator,
) -> Result<LogicalSlot<'text>, LogicalExportError> {
    wrap(
        "apply_transform",
        old.slot,
        apply_transform_inner(old, target_field_types, plan, basis, eval),
    )
}

fn apply_transform_inner<'text>(
    old: &LogicalSlot<'text>,
    target_field_types: &[StructFieldType],
    plan: &TransformPlan,
    basis: &[LogicalSlot<'text>],
    eval: &mut dyn ExprEvaluator,
) -> CauseResult<LogicalSlot<'text>> {
    let mut out = old.clone();
    if let Some(aggregate) = &old.aggregate {
        if plan.fields.len() != 1 {
            return Err(LogicalExportCause::FieldShape {
                expected: 1,
                found: plan.fields.len(),
            });
        }
        let target = scalar_target_type(old)?;
        let fields = [Some(aggregate.value.clone())];
        let view = TransformRowView {
            key: None,
            key2: None,
            fields: &fields,
            cmp_ts: None,
            basis,
        };
        let value = eval_field_transform(&plan.fields[0], &view, target, 0, eval)?.ok_or(
            LogicalExportCause::InvalidRow("aggregate transform produced no value"),
        )?;
        let value = if AggType::from_u8(old.agg_type) == Some(AggType::Count) {
            match value {
                LogicalValue::U32(v) => LogicalValue::U64(u64::from(v)),
                other => other,
            }
        } else {
            value
        };
        out.aggregate = Some(LogicalAggregate {
            value,
            count: aggregate.count,
        });
        return Ok(out);
    }
    if let Some(scalar) = &old.scalar {
        if plan.fields.len() != 1 {
            return Err(LogicalExportCause::FieldShape {
                expected: 1,
                found: plan.fields.len(),
            });
        }
        let target = if target_field_types.len() == 1 {
            target_field_types[0]
        } else {
            scalar_target_type(old)?
        };
        let fields = [Some(scalar.value.clone())];
        let view = TransformRowView {
            key: None,
            key2: None,
            fields: &fields,
            cmp_ts: Some(scalar.cmp_ts),
            basis,
        };
        let value = eval_field_transform(&plan.fields[0], &view, target, 0, eval)?.ok_or(
            LogicalExportCause::InvalidRow("scalar transform produced no value"),
        )?;
        out.scalar = Some(LogicalScalar {
            value,
            cmp_ts: scalar.cmp_ts,
        });
        return Ok(out);
    }
    if plan.fields.len() != target_field_types.len() {
        return Err(LogicalExportCause::FieldShape {
            expected: target_field_types.len(),
            found: plan.fields.len(),
        });
    }
    let mut rows = Vec::with_capacity(old.rows.len());
    for row in &old.rows {
        let view = TransformRowView {
            key: Some(&row.key),
            key2: row.key2.as_ref(),
            fields: &row.fields,
            cmp_ts: row.cmp_ts,
            basis,
        };
        let mut fields = Vec::with_capacity(plan.fields.len());
        for (index, up) in plan.fields.iter().enumerate() {
            fields.push(eval_field_transform(
                up,
                &view,
                target_field_types[index],
                index as u32,
                eval,
            )?);
        }
        rows.push(LogicalRow {
            key: row.key.clone(),
            key2: row.key2.clone(),
            fields,
            cmp_ts: row.cmp_ts,
        });
    }
    out.rows = rows;
    out.field_types = target_field_types.to_vec();
    Ok(out)
}

// =============================================================================
// Eager slot-set rewrite
// =============================================================================

/// One slot's place in an eager rewrite: where it comes from in the old
/// image, where it lands in the target program, its lane encodings, and
/// its field transform plan when one is declared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotTransfer {
    pub source: u32,
    pub target: u32,
    pub encoding: SlotEncoding,
    pub transform: Option<TransformPlan>,
}

/// Primary data size of a slot whose geometry bind_slot validated, as
/// state_init's growth walk computes it.
fn primary_size<I: KeyIndex>(state: &[u8], meta: &SlotMetaView) -> CauseResult<u32> {
    let capacity = meta.capacity;
    let size = match meta.slot_type() {
        SlotType::StructMap | SlotType::StructMap2 => {
            let num_fields = meta_byte(state, meta, SlotMetaOffset::AGG_TYPE);
            let row_size = u32::from(u16::from_le_bytes([
                state[(meta.meta_base + SlotMetaOffset::TTL_SECONDS) as usize],
                state[(meta.meta_base + SlotMetaOffset::TTL_SECONDS + 1) as usize],
            ]));
            let lanes = KeyLanes::stored(
                meta.slot_type() == SlotType::StructMap2,
                meta_byte(state, meta, SlotMetaOffset::STRUCT_KEY_LANES),
            )
            .ok_or(LogicalExportCause::Vm(ErrorCode::InvalidState))?;
            let timestamps =
                meta.slot_type() == SlotType::StructMap && struct_kind_byte(state, meta) != 0;
            u32::try_from(state_init::struct_map_primary_size::<I>(
                align8(u32::from(num_fields)),
                capacity,
                row_size,
                lanes,
                timestamps,
            ))
            .map_err(|_| LogicalExportCause::Vm(ErrorCode::InvalidState))?
        }
        SlotType::OrderedList => {
            let num_fields = meta_byte(state, meta, SlotMetaOffset::AGG_TYPE);
            let row_size = u32::from(u16::from_le_bytes([
                state[(meta.meta_base + SlotMetaOffset::TTL_SECONDS) as usize],
                state[(meta.meta_base + SlotMetaOffset::TTL_SECONDS + 1) as usize],
            ]));
            if struct_kind_byte(state, meta) == 0xff {
                align8(u32::from(num_fields)) + capacity * row_size
            } else {
                capacity * row_size
            }
        }
        SlotType::Nested => {
            let prefix = nested::read_nested_prefix(state, meta.offset);
            nested::nested_slot_data_size(
                capacity,
                next_power_of_2(u32::from(prefix.inner_initial_cap)),
                prefix.inner_type,
                prefix.inner_agg_type_byte,
            )
        }
        kind => crate::slot_growth::slot_data_size(
            kind,
            capacity,
            meta.has_hashmap_timestamp_storage(),
            meta.agg_type_byte(state),
        ),
    };
    Ok(size)
}

fn arena_header_of(state: &[u8], meta: &SlotMetaView) -> u32 {
    if matches!(meta.slot_type(), SlotType::StructMap | SlotType::StructMap2) {
        bytes::read_u32(state, meta.meta_base + SlotMetaOffset::GRACE_SECONDS)
    } else {
        0
    }
}

/// Whether the target slot can take the source slot's bytes whole: same
/// kind, capacity, subtype, TTL shape, descriptor bytes, and arena
/// capacity. Nested slots never qualify — their outer pointers are
/// absolute and would need relocation; they take the logical path.
fn geometry_identical<I: KeyIndex>(
    old: &[u8],
    old_meta: &SlotMetaView,
    new: &[u8],
    new_meta: &SlotMetaView,
) -> CauseResult<bool> {
    if old_meta.slot_type() != new_meta.slot_type()
        || old_meta.capacity != new_meta.capacity
        || old_meta.type_flags.to_byte() != new_meta.type_flags.to_byte()
        || old_meta.agg_type_byte(old) != new_meta.agg_type_byte(new)
        || old_meta.slot_type() == SlotType::Nested
    {
        return Ok(false);
    }
    if primary_size::<I>(old, old_meta)? != primary_size::<I>(new, new_meta)? {
        return Ok(false);
    }
    if matches!(
        old_meta.slot_type(),
        SlotType::StructMap | SlotType::StructMap2 | SlotType::OrderedList
    ) {
        // Overlay metadata: field count, bitset width, row size, kind byte,
        // and a struct map's key lanes, which decide its index bytes.
        for off in [
            SlotMetaOffset::STRUCT_BITSET_BYTES,
            SlotMetaOffset::TTL_SECONDS,
            SlotMetaOffset::TTL_SECONDS + 1,
            SlotMetaOffset::TTL_SECONDS + 2,
            SlotMetaOffset::STRUCT_KEY_LANES,
        ] {
            if meta_byte(old, old_meta, off) != meta_byte(new, new_meta, off) {
                return Ok(false);
            }
        }
        let num_fields = meta_byte(old, old_meta, SlotMetaOffset::AGG_TYPE);
        let desc = u32::from(num_fields) as usize;
        let has_descriptor = old_meta.slot_type() != SlotType::OrderedList
            || struct_kind_byte(old, old_meta) == 0xff;
        if has_descriptor
            && old[old_meta.offset as usize..old_meta.offset as usize + desc]
                != new[new_meta.offset as usize..new_meta.offset as usize + desc]
        {
            return Ok(false);
        }
    }
    let (old_hdr, new_hdr) = (
        arena_header_of(old, old_meta),
        arena_header_of(new, new_meta),
    );
    match (old_hdr != 0, new_hdr != 0) {
        (true, true) => {
            let old_arena = ArenaRegion::bind(old, old_hdr).map_err(LogicalExportCause::Vm)?;
            let new_arena = ArenaRegion::bind(new, new_hdr).map_err(LogicalExportCause::Vm)?;
            if old_arena.capacity != new_arena.capacity {
                return Ok(false);
            }
        }
        (false, false) => {}
        _ => return Ok(false),
    }
    Ok(true)
}

/// Region-copy one geometry-identical slot, the way grow_state copies a
/// slot it is not growing: primary bytes and arena whole, TTL regions
/// whole, sizes carried, change flags cleared.
fn region_copy_slot<I: KeyIndex>(
    old: &[u8],
    old_meta: &SlotMetaView,
    new: &mut [u8],
    new_meta: &SlotMetaView,
) -> CauseResult<()> {
    let primary = primary_size::<I>(old, old_meta)?;
    bytes::copy(new, new_meta.offset, old, old_meta.offset, primary);
    let (old_hdr, new_hdr) = (
        arena_header_of(old, old_meta),
        arena_header_of(new, new_meta),
    );
    if old_hdr != 0 {
        let arena = ArenaRegion::bind(old, old_hdr).map_err(LogicalExportCause::Vm)?;
        bytes::copy(
            new,
            new_hdr,
            old,
            old_hdr,
            state_init::ARENA_HEADER_SIZE + arena.capacity,
        );
    }
    new_meta.set_size(new, old_meta.size(old));
    if old_meta.has_ttl() {
        let eviction_bytes = old_meta.capacity * state_init::EVICTION_ENTRY_SIZE;
        bytes::copy(
            new,
            new_meta.eviction_index_offset(new),
            old,
            old_meta.eviction_index_offset(old),
            eviction_bytes,
        );
        new_meta.set_eviction_index_size(new, old_meta.eviction_index_size(old));
        if old_meta.has_evict_trigger() {
            bytes::copy(
                new,
                new_meta.evicted_buffer_offset(new),
                old,
                old_meta.evicted_buffer_offset(old),
                EVICTED_BUFFER_CAP * state_init::EVICTION_ENTRY_SIZE,
            );
            new_meta.set_evicted_count(new, old_meta.evicted_count(old));
        }
    }
    Ok(())
}

/// Rewrite a set of slots from the old image into one new image of the
/// target program, eagerly. Every source is exported first, so
/// transform expressions read a frozen slot set; then each slot either
/// region-copies (geometry identical and no transform) or imports
/// through its logical form with its transform plan applied. Target
/// slots the transfers do not name stay at their initial values. The
/// result is geometry-validated before it is returned.
pub fn rewrite_slot_set(
    old: &[u8],
    target_program: &[u8],
    accepted_program_magics: &[u32],
    plan: &[SlotTransfer],
    interner: &mut dyn Interner,
    evaluator: Option<&mut dyn ExprEvaluator>,
) -> Result<Vec<u8>, LogicalExportError> {
    rewrite_slot_set_in::<HashIndex>(
        old,
        target_program,
        accepted_program_magics,
        plan,
        interner,
        evaluator,
    )
}

/// [`rewrite_slot_set`] between images of key-index family `I`.
pub fn rewrite_slot_set_in<I: KeyIndex>(
    old: &[u8],
    target_program: &[u8],
    accepted_program_magics: &[u32],
    plan: &[SlotTransfer],
    interner: &mut dyn Interner,
    evaluator: Option<&mut dyn ExprEvaluator>,
) -> Result<Vec<u8>, LogicalExportError> {
    let mut exported: Vec<LogicalSlot> = Vec::with_capacity(plan.len());
    for rewrite in plan {
        exported.push(export_slot_in::<I>(
            old,
            rewrite.source,
            &rewrite.encoding,
            interner,
        )?);
    }
    let size = state_init::calculate_state_size_in::<I>(target_program, accepted_program_magics)
        .map_err(|refusal| LogicalExportError {
            op: "rewrite_slot_set",
            slot: 0,
            cause: LogicalExportCause::Vm(refusal.code()),
        })?;
    let mut image = wrap("rewrite_slot_set", 0, reserve_image(size))?;
    state_init::init_state_in::<I>(&mut image, target_program, accepted_program_magics).map_err(
        |code| LogicalExportError {
            op: "rewrite_slot_set",
            slot: 0,
            cause: LogicalExportCause::Vm(code),
        },
    )?;
    let mut evaluator = evaluator;
    for (rewrite, logical) in plan.iter().zip(exported.iter()) {
        let old_meta = state_init::bind_slot_in::<I>(old, rewrite.source).map_err(|code| {
            LogicalExportError {
                op: "rewrite_slot_set",
                slot: rewrite.source,
                cause: LogicalExportCause::Vm(code),
            }
        })?;
        let new_meta = state_init::bind_slot_in::<I>(&image, rewrite.target).map_err(|code| {
            LogicalExportError {
                op: "rewrite_slot_set",
                slot: rewrite.target,
                cause: LogicalExportCause::Vm(code),
            }
        })?;
        let identical = rewrite.transform.is_none()
            && wrap(
                "rewrite_slot_set",
                rewrite.target,
                geometry_identical::<I>(old, &old_meta, &image, &new_meta),
            )?;
        if identical {
            wrap(
                "rewrite_slot_set",
                rewrite.target,
                region_copy_slot::<I>(old, &old_meta, &mut image, &new_meta),
            )?;
            continue;
        }
        let mut logical = logical.clone();
        logical.slot = rewrite.target;
        if let Some(transform) = &rewrite.transform {
            let eval = evaluator.as_deref_mut().ok_or(LogicalExportError {
                op: "rewrite_slot_set",
                slot: rewrite.target,
                cause: LogicalExportCause::InvalidRow(
                    "a transform plan with no evaluator to run it",
                ),
            })?;
            let has_descriptor = match new_meta.slot_type() {
                SlotType::StructMap | SlotType::StructMap2 => true,
                SlotType::OrderedList => struct_kind_byte(&image, &new_meta) == 0xff,
                _ => false,
            };
            let target_types: Vec<StructFieldType> = if has_descriptor {
                let (descriptor, _, _, _) = wrap(
                    "rewrite_slot_set",
                    rewrite.target,
                    target_descriptor(&image, &new_meta),
                )?;
                descriptor
            } else {
                Vec::new()
            };
            logical = apply_transform(&logical, &target_types, transform, &exported, eval)?;
            logical.slot = rewrite.target;
        }
        import_slot_in::<I>(&mut image, &logical, &rewrite.encoding, interner)?;
    }
    state_init::validate_state_in::<I>(&image).map_err(|code| LogicalExportError {
        op: "rewrite_slot_set",
        slot: 0,
        cause: LogicalExportCause::Vm(code),
    })?;
    Ok(image)
}
