//! Replaces `packages/columine/src/vm/types.zig`.
//!
//! This module owns the shared VM type tables and raw-state layout constants.

use core::mem::size_of;

pub const STATE_MAGIC: u32 = 0x5354_4154;
pub const PROGRAM_MAGIC: u32 = 0x3145_5841;
pub const RETE_MAGIC: u32 = 0x4554_4552;
pub const STATE_HEADER_SIZE: u32 = 32;
pub const PROGRAM_HASH_PREFIX: u32 = 32;
pub const PROGRAM_HEADER_SIZE: u32 = 46;
pub const RETE_HEADER_SIZE: u32 = 16;
pub const STATE_FORMAT_VERSION: u8 = 2;
pub const EMPTY_KEY: u32 = u32::MAX;
pub const TOMBSTONE: u32 = u32::MAX - 1;
/// Empty and tombstone markers for the collision-free derived-fact identity
/// lane. Valid identities only use the low 48 bits (`fact_idx:u16`, `key:u32`),
/// so both sentinels are outside the valid domain and cannot alias a fact.
pub const DERIVED_FACT_EMPTY_IDENTITY: u64 = u64::MAX;
pub const DERIVED_FACT_TOMBSTONE_IDENTITY: u64 = u64::MAX - 1;
pub const BITMAP_SERIALIZED_LEN_BYTES: u32 = 4;
pub const BITMAP_BYTES_PER_CAPACITY: u32 = 8;
pub const BITMAP_BASE_BYTES: u32 = 256;

pub struct StateHeaderOffset;

impl StateHeaderOffset {
    pub const MAGIC: u32 = 0;
    pub const FORMAT_VERSION: u32 = 4;
    pub const PROGRAM_VERSION: u32 = 5;
    pub const RULESET_VERSION: u32 = 7;
    pub const NUM_SLOTS: u32 = 9;
    pub const NUM_VARS: u32 = 10;
    pub const NUM_BITVECS: u32 = 11;
    pub const FLAGS: u32 = 12;
    pub const DERIVED_FACTS_OFFSET: u32 = 13;
    pub const DERIVED_FACTS_CAPACITY: u32 = 17;
    pub const NUM_DERIVED_FACT_SCHEMAS: u32 = 19;
    pub const DERIVED_FACTS_CHANGE_FLAG: u32 = 20;
}

pub struct StateFlags;

impl StateFlags {
    pub const HAS_RETE: u8 = 0x01;
}

pub const SLOT_META_SIZE: u32 = 48;

pub struct SlotMetaOffset;

impl SlotMetaOffset {
    pub const OFFSET: u32 = 0;
    pub const CAPACITY: u32 = 4;
    pub const SIZE: u32 = 8;
    pub const TYPE_FLAGS: u32 = 12;
    pub const AGG_TYPE: u32 = 13;
    pub const CHANGE_FLAGS: u32 = 14;
    pub const TIMESTAMP_FIELD_IDX: u32 = 15;
    pub const TTL_SECONDS: u32 = 16;
    pub const GRACE_SECONDS: u32 = 20;
    pub const EVICTION_INDEX_OFFSET: u32 = 24;
    pub const EVICTION_INDEX_CAPACITY: u32 = 28;
    pub const EVICTION_INDEX_SIZE: u32 = 32;
    pub const EVICTED_BUFFER_OFFSET: u32 = 36;
    pub const EVICTED_COUNT: u32 = 40;
    pub const START_OF: u32 = 44;
}

/// Bytecode encoding for duration units from the Ax expression language
/// (specs/axe/10d-ax-expression-language.md). The Zig variants use the short
/// forms the JS compiler normalizes to: s, m, h, d, w, M (month), Q (quarter),
/// y — Rust spells them out; the discriminants are the contract.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurationUnit {
    None = 0,
    Second = 1,
    Minute = 2,
    Hour = 3,
    Day = 4,
    Week = 5,
    Month = 6,
    Quarter = 7,
    Year = 8,
}

impl DurationUnit {
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::Second),
            2 => Some(Self::Minute),
            3 => Some(Self::Hour),
            4 => Some(Self::Day),
            5 => Some(Self::Week),
            6 => Some(Self::Month),
            7 => Some(Self::Quarter),
            8 => Some(Self::Year),
            _ => None,
        }
    }
}

pub struct ChangeFlag;

impl ChangeFlag {
    pub const INSERTED: u8 = 0x01;
    pub const UPDATED: u8 = 0x02;
    pub const REMOVED: u8 = 0x04;
    pub const SIZE_CHANGED: u8 = 0x08;
    pub const EVICTED: u8 = 0x10;
}

/// VM slot category. This deliberately has a four-bit encoding in
/// `SlotTypeFlags`; the Rust enum uses `u8` because Rust has no `u4` repr.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotType {
    HashMap = 0,
    HashSet = 1,
    Aggregate = 2,
    Array = 3,
    ConditionTree = 4,
    Scalar = 5,
    StructMap = 6,
    OrderedList = 7,
    Bitmap = 8,
    Nested = 9,
    /// Two-u32-key struct map. Separate kind preserves StructMap's compact
    /// one-key physical layout and probe hot path.
    StructMap2 = 10,
}

impl SlotType {
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::HashMap),
            1 => Some(Self::HashSet),
            2 => Some(Self::Aggregate),
            3 => Some(Self::Array),
            4 => Some(Self::ConditionTree),
            5 => Some(Self::Scalar),
            6 => Some(Self::StructMap),
            7 => Some(Self::OrderedList),
            8 => Some(Self::Bitmap),
            9 => Some(Self::Nested),
            10 => Some(Self::StructMap2),
            _ => None,
        }
    }
}

/// Deliberate equivalent of Zig's `packed struct(u8) SlotTypeFlags`.
///
/// Logical fields are extracted from the backing byte with masks and shifts;
/// no bitfield crate is used so the bytecode ABI is explicit.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotTypeFlags {
    bits: u8,
}

impl SlotTypeFlags {
    const SLOT_TYPE_MASK: u8 = 0x0f;
    const HAS_TTL_MASK: u8 = 0x10;
    const HAS_EVICT_TRIGGER_MASK: u8 = 0x20;
    const NO_HASHMAP_TIMESTAMPS_MASK: u8 = 0x40;
    const RESERVED_MASK: u8 = 0x80;

    pub const fn new(
        slot_type: SlotType,
        has_ttl: bool,
        has_evict_trigger: bool,
        no_hashmap_timestamps: bool,
        reserved: bool,
    ) -> Self {
        Self {
            bits: (slot_type as u8)
                | ((has_ttl as u8) << 4)
                | ((has_evict_trigger as u8) << 5)
                | ((no_hashmap_timestamps as u8) << 6)
                | ((reserved as u8) << 7),
        }
    }

    pub const fn from_byte(bits: u8) -> Self {
        Self { bits }
    }

    pub const fn to_byte(self) -> u8 {
        self.bits
    }

    pub const fn slot_type(self) -> Option<SlotType> {
        SlotType::from_u8(self.bits & Self::SLOT_TYPE_MASK)
    }

    pub const fn has_ttl(self) -> bool {
        self.bits & Self::HAS_TTL_MASK != 0
    }

    pub const fn has_evict_trigger(self) -> bool {
        self.bits & Self::HAS_EVICT_TRIGGER_MASK != 0
    }

    pub const fn no_hashmap_timestamps(self) -> bool {
        self.bits & Self::NO_HASHMAP_TIMESTAMPS_MASK != 0
    }

    pub const fn reserved(self) -> bool {
        self.bits & Self::RESERVED_MASK != 0
    }
}

/// Subtype for aggregate and scalar slots, stored in slot metadata byte 13.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggType {
    Sum = 1,
    Count = 2,
    Min = 3,
    Max = 4,
    Avg = 5,
    ScalarU32 = 8,
    ScalarF64 = 9,
    ScalarI64 = 10,
    SumI64 = 11,
    MinI64 = 12,
    MaxI64 = 13,
}

impl AggType {
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Sum),
            2 => Some(Self::Count),
            3 => Some(Self::Min),
            4 => Some(Self::Max),
            5 => Some(Self::Avg),
            8 => Some(Self::ScalarU32),
            9 => Some(Self::ScalarF64),
            10 => Some(Self::ScalarI64),
            11 => Some(Self::SumI64),
            12 => Some(Self::MinI64),
            13 => Some(Self::MaxI64),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructFieldType {
    UInt32 = 0,
    Int64 = 1,
    Float64 = 2,
    Bool = 3,
    String = 4,
    ArrayU32 = 5,
    ArrayI64 = 6,
    ArrayF64 = 7,
    ArrayString = 8,
    ArrayBool = 9,
}

impl StructFieldType {
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::UInt32),
            1 => Some(Self::Int64),
            2 => Some(Self::Float64),
            3 => Some(Self::Bool),
            4 => Some(Self::String),
            5 => Some(Self::ArrayU32),
            6 => Some(Self::ArrayI64),
            7 => Some(Self::ArrayF64),
            8 => Some(Self::ArrayString),
            9 => Some(Self::ArrayBool),
            _ => None,
        }
    }
}

/// Zig's `packed struct` EvictionEntry is backed by u128: size 16, align 16 on
/// BOTH native and wasm32-freestanding (verified with a zig 0.16.0 probe:
/// `@sizeOf`/`@alignOf` native run + wasm32 comptime assert). `repr(C, align(16))`
/// matches size, alignment, and the little-endian byte image (timestamp@0,
/// key_or_idx@8, value@12).
///
/// Latent Zig fragility to confront in the VM-core stage: state_init.zig places
/// TTL buffers with `align8`, while `[*]EvictionEntry` casts in vm.zig demand
/// align 16 — offsets are 16-aligned in practice today, but Rust accessors
/// should either guarantee that or use unaligned reads.
#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EvictionEntry {
    pub timestamp: f64,
    pub key_or_idx: u32,
    pub value: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConditionTreeState {
    pub lifecycle_generation: u32,
    pub last_removed_key: u32,
}

pub const CONDITION_TREE_MATCHER_PLAN_VERSION: u16 = 1;
pub const CT_NODE_EQ: u8 = 1;
pub const CT_NODE_NEQ: u8 = 2;
pub const CT_NODE_GT: u8 = 3;
pub const CT_NODE_GTE: u8 = 4;
pub const CT_NODE_LT: u8 = 5;
pub const CT_NODE_LTE: u8 = 6;
pub const CT_NODE_IN: u8 = 7;
pub const CT_NODE_RANGE: u8 = 8;
pub const CT_NODE_BOOLEAN: u8 = 9;
pub const CT_NODE_NOT: u8 = 10;
pub const CT_NODE_DESTINATION: u8 = 11;
pub const CONDITION_TREE_STATE_BYTES: u32 = size_of::<ConditionTreeState>() as u32;

/// Bytecode opcodes as documented in types.zig (the module vm.zig imports).
///
/// Drift RESOLVED against vm.zig's dispatch (the ground truth): the decode
/// arms read a trailing `cmp_type:u8` (0=u32, 1=f64, 2=i64) on the LATEST and
/// MAX/MIN map upserts — 0x20/0x24 at operand 5 (vm.zig:1429/decode), 0x26/0x27
/// after cmp_col (vm.zig:1480/1492), and the `_IF` forms 0x28/0x2C/0x2D before
/// pred_col (vm.zig:2220-2591 body arms). opcodes.zig's registry was correct;
/// types.zig's comments omitted the operand. The docs below carry the
/// vm.zig-confirmed encodings.
#[repr(u8)]
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Opcode {
    Halt = 0x00,
    /// slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 \[, ttl:f32, grace:f32, ts_field:u8, start_of:u8\]
    /// For AGGREGATE: cap_lo=aggType, cap_hi=0.
    SlotDef = 0x10,
    /// For `.within()` without keyBy — stores array of events.
    SlotArray = 0x14,
    /// slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8
    BatchMapUpsertLatest = 0x20,
    /// slot:u8, key_col:u8, val_col:u8
    BatchMapUpsertFirst = 0x21,
    /// slot:u8, key_col:u8, val_col:u8
    BatchMapUpsertLast = 0x22,
    /// slot:u8, key_col:u8
    BatchMapRemove = 0x23,
    /// slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8 (tracks insertion in eviction index)
    BatchMapUpsertLatestTtl = 0x24,
    /// slot:u8, key_col:u8, val_col:u8, ts_col:u8 (tracks insertion in eviction index)
    BatchMapUpsertLastTtl = 0x25,
    /// slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8 (keep row with highest cmp value)
    BatchMapUpsertMax = 0x26,
    /// slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8 (keep row with lowest cmp value)
    BatchMapUpsertMin = 0x27,
    /// slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8, pred_col:u8
    BatchMapUpsertLatestIf = 0x28,
    /// slot:u8, key_col:u8, val_col:u8, pred_col:u8
    BatchMapUpsertFirstIf = 0x29,
    /// slot:u8, key_col:u8, val_col:u8, pred_col:u8
    BatchMapUpsertLastIf = 0x2a,
    /// slot:u8, key_col:u8, pred_col:u8
    BatchMapRemoveIf = 0x2b,
    /// slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8, pred_col:u8
    BatchMapUpsertMaxIf = 0x2c,
    /// slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8, pred_col:u8
    BatchMapUpsertMinIf = 0x2d,
    /// Keyed struct-map probe (body opcode, per FLAT_MAP element).
    /// probe_slot:u8, key_col:u8, miss_mode:u8(0=skip,1=null), out_slot:u8, num_fields:u8,
    /// \[probe_field_idx:u8, out_field_idx:u8\] × num_fields, out_key_col:u8
    BatchStructMapProbe = 0x2e,
    /// Fused probe+dispatch (body opcode, per FLAT_MAP element).
    /// probe_slot:u8, key_col:u8, miss_mode:u8(0=skip,1=null), route_col:u8, op_col:u8,
    /// num_routes:u8, \[kind:u8, dest_slot:u8, dest_field_idx:u8, out_key_col:u8, v_src_field_idx:u8\] × num_routes
    BatchStructMapProbeScatter = 0x2f,
    /// slot:u8, elem_col:u8
    BatchSetInsert = 0x30,
    /// slot:u8, elem_col:u8
    BatchSetRemove = 0x31,
    /// slot:u8, elem_col:u8, ts_col:u8
    BatchSetInsertTtl = 0x32,
    /// slot:u8, elem_col:u8, pred_col:u8
    BatchSetInsertIf = 0x33,
    /// slot:u8, elem_col:u8
    BatchBitmapAdd = 0x34,
    /// slot:u8, elem_col:u8
    BatchBitmapRemove = 0x35,
    /// target_slot:u8, source_slot:u8 (in-place slot × slot algebra)
    BatchBitmapAnd = 0x36,
    /// target_slot:u8, source_slot:u8
    BatchBitmapOr = 0x37,
    /// target_slot:u8, source_slot:u8
    BatchBitmapAndNot = 0x38,
    /// target_slot:u8, source_slot:u8
    BatchBitmapXor = 0x39,
    /// target_slot:u8 (slot × scratch result)
    BatchBitmapAndScratch = 0x3a,
    /// target_slot:u8
    BatchBitmapOrScratch = 0x3b,
    /// target_slot:u8
    BatchBitmapAndNotScratch = 0x3c,
    /// target_slot:u8
    BatchBitmapXorScratch = 0x3d,
    /// slot:u8, val_col:u8 (SIMD accelerated)
    BatchAggSum = 0x40,
    /// slot:u8
    BatchAggCount = 0x41,
    /// slot:u8, val_col:u8
    BatchAggMin = 0x42,
    /// slot:u8, val_col:u8
    BatchAggMax = 0x43,
    /// slot:u8, val_col:u8, pred_col:u8
    BatchAggSumIf = 0x44,
    /// slot:u8, pred_col:u8
    BatchAggCountIf = 0x45,
    /// slot:u8, val_col:u8, pred_col:u8
    BatchAggMinIf = 0x46,
    /// slot:u8, val_col:u8, pred_col:u8
    BatchAggMaxIf = 0x47,
    /// slot:u8, val_col:u8, cmp_col:u8 (AggType subtype lives in slot metadata)
    BatchScalarLatest = 0x48,
    /// slot:u8, val_col:u8 (lossless i64 accumulation)
    BatchAggSumI64 = 0x49,
    /// slot:u8, val_col:u8
    BatchAggMinI64 = 0x4a,
    /// slot:u8, val_col:u8
    BatchAggMaxI64 = 0x4b,
    /// slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8, num_fields:u8, \[field_type:u8 × num_fields\]
    SlotStructMap = 0x18,
    /// slot:u8, key_col:u8, num_vals:u8, \[val_col:u8, field_idx:u8\] × num_vals
    BatchStructMapUpsertLast = 0x80,
    /// Same encoding as 0x80; first-wins — writes only when the key is absent
    /// (opcodes.zig:236, dispatch vm.zig:1774/2775).
    BatchStructMapUpsertFirst = 0x81,
    /// Same row operands as 0x80 followed by comparison_field_idx:u8; replaces
    /// only when the incoming scalar comparison is strictly greater.
    BatchStructMapUpsertMax = 0x82,
    /// slot:u8, key1_col:u8, key2_col:u8, num_vals:u8,
    /// [val_col:u8, field_idx:u8] × num_vals
    BatchStructMap2UpsertLast = 0x83,
    /// slot:u8, key1_col:u8, key2_col:u8
    BatchStructMap2Remove = 0x86,
    /// slot:u8, val_col:u8 (body opcode inside FOR_EACH/FLAT_MAP blocks)
    ListAppend = 0x84,
    /// slot:u8, num_vals:u8, \[(val_col:u8, field_idx:u8) × N\]
    ListAppendStruct = 0x85,
    /// slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8 \[, num_fields:u8, field_type:u8 × num_fields\]
    SlotOrderedList = 0x19,
    /// slot:u8, outer_type_flags:u8, outer_cap_lo:u8, outer_cap_hi:u8, inner_type:u8,
    /// inner_cap_lo:u8, inner_cap_hi:u8, inner_agg_type:u8
    SlotNested = 0x1a,
    /// slot:u8, type_flags:u8, cap_lo:u8, cap_hi:u8, num_fields:u8,
    /// [field_type:u8 × num_fields]
    SlotStructMap2 = 0x1b,
    /// slot:u8, outer_key_col:u8, elem_col:u8 (body opcode inside FOR_EACH blocks)
    NestedSetInsert = 0x90,
    /// slot:u8, outer_key_col:u8, inner_key_col:u8, val_col:u8 (body opcode)
    NestedMapUpsertLast = 0x92,
    /// slot:u8, outer_key_col:u8, val_col:u8 (body opcode)
    NestedAggUpdate = 0x95,
    /// col:u8, match_count:u8, match_ids:u32le\[match_count\], body_len:u16le
    ForEach = 0xe0,
    /// offsets_col:u8, parent_ts_col:u8, inner_body_len_lo:u8, inner_body_len_hi:u8
    FlatMap = 0xe1,
}

impl Opcode {
    /// Decode one opcode byte. `None` is an UNKNOWN byte — vm.zig's dispatch
    /// is an open `enum(u8)` whose top-level `else` arm returns
    /// INVALID_PROGRAM for anything it does not handle, so the Rust dispatch
    /// maps `None` (and known-but-non-executable registry entries) to the
    /// same INVALID_PROGRAM result rather than panicking on wild bytes.
    pub const fn from_u8(byte: u8) -> Option<Self> {
        Some(match byte {
            0x00 => Self::Halt,
            0x10 => Self::SlotDef,
            0x14 => Self::SlotArray,
            0x18 => Self::SlotStructMap,
            0x19 => Self::SlotOrderedList,
            0x1a => Self::SlotNested,
            0x1b => Self::SlotStructMap2,
            0x20 => Self::BatchMapUpsertLatest,
            0x21 => Self::BatchMapUpsertFirst,
            0x22 => Self::BatchMapUpsertLast,
            0x23 => Self::BatchMapRemove,
            0x24 => Self::BatchMapUpsertLatestTtl,
            0x25 => Self::BatchMapUpsertLastTtl,
            0x26 => Self::BatchMapUpsertMax,
            0x27 => Self::BatchMapUpsertMin,
            0x28 => Self::BatchMapUpsertLatestIf,
            0x29 => Self::BatchMapUpsertFirstIf,
            0x2a => Self::BatchMapUpsertLastIf,
            0x2b => Self::BatchMapRemoveIf,
            0x2c => Self::BatchMapUpsertMaxIf,
            0x2d => Self::BatchMapUpsertMinIf,
            0x2e => Self::BatchStructMapProbe,
            0x2f => Self::BatchStructMapProbeScatter,
            0x30 => Self::BatchSetInsert,
            0x31 => Self::BatchSetRemove,
            0x32 => Self::BatchSetInsertTtl,
            0x33 => Self::BatchSetInsertIf,
            0x34 => Self::BatchBitmapAdd,
            0x35 => Self::BatchBitmapRemove,
            0x36 => Self::BatchBitmapAnd,
            0x37 => Self::BatchBitmapOr,
            0x38 => Self::BatchBitmapAndNot,
            0x39 => Self::BatchBitmapXor,
            0x3a => Self::BatchBitmapAndScratch,
            0x3b => Self::BatchBitmapOrScratch,
            0x3c => Self::BatchBitmapAndNotScratch,
            0x3d => Self::BatchBitmapXorScratch,
            0x40 => Self::BatchAggSum,
            0x41 => Self::BatchAggCount,
            0x42 => Self::BatchAggMin,
            0x43 => Self::BatchAggMax,
            0x44 => Self::BatchAggSumIf,
            0x45 => Self::BatchAggCountIf,
            0x46 => Self::BatchAggMinIf,
            0x47 => Self::BatchAggMaxIf,
            0x48 => Self::BatchScalarLatest,
            0x49 => Self::BatchAggSumI64,
            0x4a => Self::BatchAggMinI64,
            0x4b => Self::BatchAggMaxI64,
            0x80 => Self::BatchStructMapUpsertLast,
            0x81 => Self::BatchStructMapUpsertFirst,
            0x82 => Self::BatchStructMapUpsertMax,
            0x83 => Self::BatchStructMap2UpsertLast,
            0x84 => Self::ListAppend,
            0x85 => Self::ListAppendStruct,
            0x86 => Self::BatchStructMap2Remove,
            0x90 => Self::NestedSetInsert,
            0x92 => Self::NestedMapUpsertLast,
            0x95 => Self::NestedAggUpdate,
            0xe0 => Self::ForEach,
            0xe1 => Self::FlatMap,
            _ => return None,
        })
    }
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    Ok = 0,
    CapacityExceeded = 1,
    InvalidProgram = 2,
    InvalidSlot = 3,
    InvalidState = 4,
    NeedsGrowth = 5,
    ArenaOverflow = 6,
    /// Reserved StructMap2 lane-1 sentinel supplied as reducer data.
    InvalidKey = 7,
}

pub const fn hash_key(key: u32, cap: u32) -> u32 {
    let mut h = key as u64;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    (h as u32) & (cap - 1)
}

/// Stable probe-placement hash for an exact pair of u32 key lanes.
///
/// The hash never establishes identity: callers must compare both lanes.
/// Packing is allocation-free and preserves every input bit.
pub const fn hash_key_pair(first: u32, second: u32, cap: u32) -> u32 {
    let mut h = (first as u64) | ((second as u64) << 32);
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^= h >> 31;
    (h as u32) & (cap - 1)
}

/// Portable layout equivalent of Zig's `@Vector(4, f64)`.
#[repr(C, align(32))]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct V4f64 {
    pub lanes: [f64; 4],
}

/// Portable layout equivalent of Zig's `@Vector(4, u32)`.
#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct V4u32 {
    pub lanes: [u32; 4],
}

/// Portable layout equivalent of Zig's `@Vector(2, i64)`.
#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct V2i64 {
    pub lanes: [i64; 2],
}

/// Transient view over a slot's raw metadata bytes (types.zig:437-455).
///
/// The Zig original is a plain (non-`extern`) struct, so its in-memory layout
/// is UNSPECIFIED and never crosses the ABI — `getSlotMeta` constructs it from
/// state bytes and it lives only inside a call. This Rust struct therefore has
/// deliberately NO layout contract and no layout test; `SlotMetaOffset` is the
/// byte contract for the underlying 48-byte metadata record.
#[derive(Clone, Copy, Debug)]
pub struct SlotMeta {
    pub size_ptr: *mut u32,
    pub change_flags_ptr: *mut u8,
    pub eviction_index_size_ptr: *mut u32,
    pub evicted_count_ptr: *mut u32,
    pub offset: u32,
    pub capacity: u32,
    pub ttl_seconds: f32,
    pub grace_seconds: f32,
    pub eviction_index_offset: u32,
    pub eviction_index_capacity: u32,
    pub evicted_buffer_offset: u32,
    pub type_flags: SlotTypeFlags,
    pub agg_type: AggType,
    pub timestamp_field_idx: u8,
    pub start_of: DurationUnit,
}

impl SlotMeta {
    /// Helper to get `slot_type` from `type_flags`.
    pub const fn slot_type(self) -> SlotType {
        match self.type_flags.slot_type() {
            Some(slot_type) => slot_type,
            // `die!` is not const-callable; a bare `panic!()` keeps this fn
            // const while shipping no message string in the wasm artifact.
            #[cfg(target_arch = "wasm32")]
            None => panic!(),
            #[cfg(not(target_arch = "wasm32"))]
            None => panic!("invariant: slot metadata contains an invalid slot type"),
        }
    }

    /// Helper to check whether TTL is enabled.
    pub const fn has_ttl(self) -> bool {
        self.type_flags.has_ttl()
    }

    /// Helper to check whether eviction triggers RETE rules.
    pub const fn has_evict_trigger(self) -> bool {
        self.type_flags.has_evict_trigger()
    }

    /// HASHMAP timestamp/comparison side-array availability.
    pub fn has_hash_map_timestamp_storage(self) -> bool {
        self.slot_type() == SlotType::HashMap && !self.type_flags.no_hashmap_timestamps()
    }

    /// Calculate the cutoff time for eviction after JS has applied startOf/timezone.
    /// `cutoff = now - ttl_seconds - grace_seconds`.
    pub fn cutoff(self, now: f64) -> f64 {
        now - f64::from(self.ttl_seconds) - f64::from(self.grace_seconds)
    }
}

/// # Safety
///
/// `state_base` must point to a writable, correctly initialized VM state
/// buffer whose slot metadata is available at `slot`.
pub unsafe fn get_slot_meta(state_base: *mut u8, slot: u8) -> SlotMeta {
    let meta_offset = STATE_HEADER_SIZE as usize + usize::from(slot) * SLOT_META_SIZE as usize;
    let meta_bytes = unsafe { state_base.add(meta_offset) };
    let type_flags =
        SlotTypeFlags::from_byte(unsafe { *meta_bytes.add(SlotMetaOffset::TYPE_FLAGS as usize) });
    let slot_type = type_flags
        .slot_type()
        .unwrap_or_else(|| crate::die!("invariant: slot metadata contains an invalid slot type"));
    let agg_byte = unsafe { *meta_bytes.add(SlotMetaOffset::AGG_TYPE as usize) };
    let agg_type = if matches!(slot_type, SlotType::Aggregate | SlotType::Scalar) {
        AggType::from_u8(agg_byte).unwrap_or(AggType::Sum)
    } else {
        AggType::Sum
    };
    let start_of =
        DurationUnit::from_u8(unsafe { *meta_bytes.add(SlotMetaOffset::START_OF as usize) })
            .unwrap_or_else(|| {
                crate::die!("invariant: slot metadata contains an invalid duration unit")
            });

    SlotMeta {
        offset: unsafe { meta_bytes.cast::<u32>().read_unaligned() },
        capacity: unsafe { meta_bytes.add(4).cast::<u32>().read_unaligned() },
        size_ptr: unsafe { meta_bytes.add(SlotMetaOffset::SIZE as usize).cast() },
        type_flags,
        agg_type,
        change_flags_ptr: unsafe { meta_bytes.add(SlotMetaOffset::CHANGE_FLAGS as usize) },
        timestamp_field_idx: unsafe {
            *meta_bytes.add(SlotMetaOffset::TIMESTAMP_FIELD_IDX as usize)
        },
        ttl_seconds: unsafe {
            meta_bytes
                .add(SlotMetaOffset::TTL_SECONDS as usize)
                .cast::<f32>()
                .read_unaligned()
        },
        grace_seconds: unsafe {
            meta_bytes
                .add(SlotMetaOffset::GRACE_SECONDS as usize)
                .cast::<f32>()
                .read_unaligned()
        },
        start_of,
        eviction_index_offset: unsafe { meta_bytes.add(24).cast::<u32>().read_unaligned() },
        eviction_index_capacity: unsafe { meta_bytes.add(28).cast::<u32>().read_unaligned() },
        eviction_index_size_ptr: unsafe {
            meta_bytes
                .add(SlotMetaOffset::EVICTION_INDEX_SIZE as usize)
                .cast()
        },
        evicted_buffer_offset: unsafe { meta_bytes.add(36).cast::<u32>().read_unaligned() },
        evicted_count_ptr: unsafe {
            meta_bytes
                .add(SlotMetaOffset::EVICTED_COUNT as usize)
                .cast()
        },
    }
}

/// # Safety
///
/// `meta.change_flags_ptr` must be valid for one writable byte.
pub unsafe fn set_change_flag(meta: SlotMeta, flag: u8) {
    unsafe { *meta.change_flags_ptr |= flag };
}

/// # Safety
///
/// `state_base` must point to writable VM state metadata for `num_slots`.
pub unsafe fn clear_all_change_flags(state_base: *mut u8, num_slots: u8) {
    for slot in 0..num_slots {
        let offset = STATE_HEADER_SIZE as usize
            + usize::from(slot) * SLOT_META_SIZE as usize
            + SlotMetaOffset::CHANGE_FLAGS as usize;
        unsafe { *state_base.add(offset) = 0 };
    }
}

/// # Safety
///
/// `state_base` must point to initialized VM state metadata for `num_slots`.
pub unsafe fn has_relevant_changes(state_base: *const u8, num_slots: u8) -> bool {
    for slot in 0..num_slots {
        let offset = STATE_HEADER_SIZE as usize
            + usize::from(slot) * SLOT_META_SIZE as usize
            + SlotMetaOffset::CHANGE_FLAGS as usize;
        if unsafe { *state_base.add(offset) } != 0 {
            return true;
        }
    }
    false
}

pub const fn align8(n: u32) -> u32 {
    (n + 7) & !7
}

pub const fn next_power_of_2(n: u32) -> u32 {
    if n <= 16 {
        return 16;
    }
    let mut value = n - 1;
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value + 1
}

pub const fn struct_field_size(field_type: StructFieldType) -> u32 {
    match field_type {
        StructFieldType::UInt32 | StructFieldType::String => 4,
        StructFieldType::Int64 | StructFieldType::Float64 => 8,
        StructFieldType::Bool => 1,
        StructFieldType::ArrayU32
        | StructFieldType::ArrayI64
        | StructFieldType::ArrayF64
        | StructFieldType::ArrayString
        | StructFieldType::ArrayBool => 8,
    }
}

pub const fn arena_elem_size(field_type: StructFieldType) -> u32 {
    match field_type {
        StructFieldType::ArrayU32 | StructFieldType::ArrayString => 4,
        StructFieldType::ArrayI64 | StructFieldType::ArrayF64 => 8,
        StructFieldType::ArrayBool => 1,
        _ => 0,
    }
}

pub const fn is_array_field_type(field_type: StructFieldType) -> bool {
    matches!(
        field_type,
        StructFieldType::ArrayU32
            | StructFieldType::ArrayI64
            | StructFieldType::ArrayF64
            | StructFieldType::ArrayString
            | StructFieldType::ArrayBool
    )
}

pub fn has_array_fields(num_fields: u8, field_types: &[u8]) -> bool {
    field_types
        .iter()
        .take(usize::from(num_fields))
        .any(|&field_type| {
            is_array_field_type(StructFieldType::from_u8(field_type).unwrap_or_else(|| {
                crate::die!("invariant: struct-map descriptor contains an invalid field type")
            }))
        })
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StructRowLayout {
    pub row_size: u32,
    pub bitset_bytes: u32,
    pub descriptor_size: u32,
}

pub fn struct_field_offset(num_fields: u8, field_types: &[u8], target_field: u8) -> u32 {
    let mut offset = u32::from(num_fields).div_ceil(8);
    for &field_type in field_types.iter().take(usize::from(target_field)) {
        offset += struct_field_size(StructFieldType::from_u8(field_type).unwrap_or_else(|| {
            crate::die!("invariant: struct-map descriptor contains an invalid field type")
        }));
    }
    offset
}

/// # Safety
///
/// `ptrs` must point to an array containing `idx` and that entry must be a
/// correctly aligned `u32` column.
pub unsafe fn get_col_u32(ptrs: *const *const u8, idx: u8) -> *const u32 {
    unsafe { (*ptrs.add(usize::from(idx))).cast() }
}

/// # Safety
///
/// `ptrs` must point to an array containing `idx` and that entry must be a
/// correctly aligned `f64` column.
pub unsafe fn get_col_f64(ptrs: *const *const u8, idx: u8) -> *const f64 {
    unsafe { (*ptrs.add(usize::from(idx))).cast() }
}

/// # Safety
///
/// `ptrs` must point to an array containing `idx` and that entry must be a
/// correctly aligned `i64` column.
pub unsafe fn get_col_i64(ptrs: *const *const u8, idx: u8) -> *const i64 {
    unsafe { (*ptrs.add(usize::from(idx))).cast() }
}

/// # Safety
///
/// `ptrs` must point to an array containing `idx` and that entry must be a
/// correctly aligned column of `T` values.
pub unsafe fn get_col_as<T>(ptrs: *const *const u8, idx: u8) -> *const T {
    unsafe { (*ptrs.add(usize::from(idx))).cast() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    macro_rules! assert_discriminants {
        ($enum:ident, $type:ty; $($variant:ident = $value:expr),+ $(,)?) => {
            $(assert_eq!($enum::$variant as $type, $value);)+
        };
    }

    #[test]
    fn duration_unit_discriminants_match_zig() {
        assert_discriminants!(DurationUnit, u8;
            None = 0, Second = 1, Minute = 2, Hour = 3, Day = 4, Week = 5, Month = 6,
            Quarter = 7, Year = 8
        );
    }

    #[test]
    fn slot_type_discriminants_match_zig() {
        assert_discriminants!(SlotType, u8;
            HashMap = 0, HashSet = 1, Aggregate = 2, Array = 3, ConditionTree = 4,
            Scalar = 5, StructMap = 6, OrderedList = 7, Bitmap = 8, Nested = 9
        );
    }

    #[test]
    fn agg_type_discriminants_match_zig() {
        assert_discriminants!(AggType, u8;
            Sum = 1, Count = 2, Min = 3, Max = 4, Avg = 5, ScalarU32 = 8,
            ScalarF64 = 9, ScalarI64 = 10, SumI64 = 11, MinI64 = 12, MaxI64 = 13
        );
    }

    #[test]
    fn struct_field_type_discriminants_match_zig() {
        assert_discriminants!(StructFieldType, u8;
            UInt32 = 0, Int64 = 1, Float64 = 2, Bool = 3, String = 4, ArrayU32 = 5,
            ArrayI64 = 6, ArrayF64 = 7, ArrayString = 8, ArrayBool = 9
        );
    }

    #[test]
    fn opcode_discriminants_match_zig() {
        assert_discriminants!(Opcode, u8;
            Halt = 0x00, SlotDef = 0x10, SlotArray = 0x14, SlotStructMap = 0x18,
            SlotOrderedList = 0x19, SlotNested = 0x1a, BatchMapUpsertLatest = 0x20,
            BatchMapUpsertFirst = 0x21, BatchMapUpsertLast = 0x22, BatchMapRemove = 0x23,
            BatchMapUpsertLatestTtl = 0x24, BatchMapUpsertLastTtl = 0x25,
            BatchMapUpsertMax = 0x26, BatchMapUpsertMin = 0x27, BatchMapUpsertLatestIf = 0x28,
            BatchMapUpsertFirstIf = 0x29, BatchMapUpsertLastIf = 0x2a, BatchMapRemoveIf = 0x2b,
            BatchMapUpsertMaxIf = 0x2c, BatchMapUpsertMinIf = 0x2d,
            BatchStructMapProbe = 0x2e, BatchStructMapProbeScatter = 0x2f,
            BatchSetInsert = 0x30, BatchSetRemove = 0x31, BatchSetInsertTtl = 0x32,
            BatchSetInsertIf = 0x33, BatchBitmapAdd = 0x34, BatchBitmapRemove = 0x35,
            BatchBitmapAnd = 0x36, BatchBitmapOr = 0x37, BatchBitmapAndNot = 0x38,
            BatchBitmapXor = 0x39, BatchBitmapAndScratch = 0x3a, BatchBitmapOrScratch = 0x3b,
            BatchBitmapAndNotScratch = 0x3c, BatchBitmapXorScratch = 0x3d, BatchAggSum = 0x40,
            BatchAggCount = 0x41, BatchAggMin = 0x42, BatchAggMax = 0x43, BatchAggSumIf = 0x44,
            BatchAggCountIf = 0x45, BatchAggMinIf = 0x46, BatchAggMaxIf = 0x47,
            BatchScalarLatest = 0x48, BatchAggSumI64 = 0x49, BatchAggMinI64 = 0x4a,
            BatchAggMaxI64 = 0x4b, BatchStructMapUpsertLast = 0x80,
            BatchStructMapUpsertFirst = 0x81, ListAppend = 0x84,
            ListAppendStruct = 0x85, NestedSetInsert = 0x90, NestedMapUpsertLast = 0x92,
            NestedAggUpdate = 0x95, ForEach = 0xe0, FlatMap = 0xe1
        );
    }

    #[test]
    fn error_code_discriminants_match_zig() {
        assert_discriminants!(ErrorCode, u32;
            Ok = 0, CapacityExceeded = 1, InvalidProgram = 2, InvalidSlot = 3,
            InvalidState = 4, NeedsGrowth = 5, ArenaOverflow = 6
        );
    }

    #[test]
    fn slot_type_flags_layout_and_bitfield_round_trip_match_zig() {
        // Source: types.zig:176-190 (`packed struct(u8)`).
        assert_eq!(size_of::<SlotTypeFlags>(), 1);
        assert_eq!(align_of::<SlotTypeFlags>(), 1);
        assert_eq!(offset_of!(SlotTypeFlags, bits), 0);

        let flags = SlotTypeFlags::new(SlotType::Nested, true, true, true, true);
        assert_eq!(flags.to_byte(), 0xf9);
        assert_eq!(flags.slot_type(), Some(SlotType::Nested));
        assert!(flags.has_ttl());
        assert!(flags.has_evict_trigger());
        assert!(flags.no_hashmap_timestamps());
        assert!(flags.reserved());
        assert_eq!(SlotTypeFlags::from_byte(flags.to_byte()), flags);
    }

    #[test]
    fn constant_namespace_types_are_zero_sized() {
        // These model Zig compile-time-only `struct { pub const ... }` namespaces.
        assert_eq!(size_of::<StateHeaderOffset>(), 0);
        assert_eq!(align_of::<StateHeaderOffset>(), 1);
        assert_eq!(size_of::<StateFlags>(), 0);
        assert_eq!(align_of::<StateFlags>(), 1);
        assert_eq!(size_of::<SlotMetaOffset>(), 0);
        assert_eq!(align_of::<SlotMetaOffset>(), 1);
        assert_eq!(size_of::<ChangeFlag>(), 0);
        assert_eq!(align_of::<ChangeFlag>(), 1);
    }

    #[test]
    fn eviction_entry_layout_matches_zig() {
        // Source: types.zig:235-240; Zig compile-time layout probe: size=16, align=16.
        assert_eq!(size_of::<EvictionEntry>(), 16);
        assert_eq!(align_of::<EvictionEntry>(), 16);
        assert_eq!(offset_of!(EvictionEntry, timestamp), 0);
        assert_eq!(offset_of!(EvictionEntry, key_or_idx), 8);
        assert_eq!(offset_of!(EvictionEntry, value), 12);
    }

    #[test]
    fn condition_tree_state_layout_matches_zig() {
        // Source: types.zig:242-245; `extern struct` means C layout.
        assert_eq!(size_of::<ConditionTreeState>(), 8);
        assert_eq!(align_of::<ConditionTreeState>(), 4);
        assert_eq!(offset_of!(ConditionTreeState, lifecycle_generation), 0);
        assert_eq!(offset_of!(ConditionTreeState, last_removed_key), 4);
    }

    #[test]
    fn vector_layouts_match_zig() {
        // Source: types.zig:429-431; Zig layout probe records these vector ABI values.
        assert_eq!(size_of::<V4f64>(), 32);
        assert_eq!(align_of::<V4f64>(), 32);
        assert_eq!(offset_of!(V4f64, lanes), 0);
        assert_eq!(size_of::<V4u32>(), 16);
        assert_eq!(align_of::<V4u32>(), 16);
        assert_eq!(offset_of!(V4u32, lanes), 0);
        assert_eq!(size_of::<V2i64>(), 16);
        assert_eq!(align_of::<V2i64>(), 16);
        assert_eq!(offset_of!(V2i64, lanes), 0);
    }

    // SlotMeta deliberately has NO layout test: the Zig original is a plain
    // struct with unspecified layout that never crosses the ABI (and pointer
    // fields make any size assertion break on wasm32). See the type's doc.

    #[test]
    fn eviction_entry_byte_image_matches_zig_packed_le_layout() {
        // Independently computed: struct.pack('<dII', 1234.5, 42, 7).
        let entry = EvictionEntry {
            timestamp: 1234.5,
            key_or_idx: 42,
            value: 7,
        };
        let mut bytes = [0u8; 16];
        // Safety: EvictionEntry is repr(C), size 16, and Copy.
        unsafe {
            core::ptr::copy_nonoverlapping((&raw const entry).cast::<u8>(), bytes.as_mut_ptr(), 16);
        }
        assert_eq!(bytes, [0, 0, 0, 0, 0, 74, 147, 64, 42, 0, 0, 0, 7, 0, 0, 0]);
    }

    #[test]
    fn slot_type_flags_round_trips_every_byte() {
        for bits in 0..=u8::MAX {
            let flags = SlotTypeFlags::from_byte(bits);
            assert_eq!(flags.to_byte(), bits);
            // Bits 0-3 above 9 are unassigned slot types and must decode to None.
            let low = bits & 0x0f;
            assert_eq!(flags.slot_type().is_some(), low <= 9);
            assert_eq!(flags.has_ttl(), bits & 0x10 != 0);
            assert_eq!(flags.has_evict_trigger(), bits & 0x20 != 0);
            assert_eq!(flags.no_hashmap_timestamps(), bits & 0x40 != 0);
            assert_eq!(flags.reserved(), bits & 0x80 != 0);
        }
    }

    #[test]
    fn hash_key_matches_zig_reference_vectors() {
        // Vectors computed independently with the same avalanche steps
        // (Python, 64-bit wrapping): see types.zig:415-423.
        assert_eq!(hash_key(0, 16), 0);
        assert_eq!(hash_key(42, 16), 4);
        assert_eq!(hash_key(1, 1024), 183);
        assert_eq!(hash_key(0xffff_ffff, 65536), 12385);
        assert_eq!(hash_key(123_456_789, 256), 10);
    }

    #[test]
    fn struct_row_layout_covers_bool_and_array_fields() {
        // BOOL is 1 byte in-row; every ARRAY_* is 8 bytes in-row
        // (offset:u32 + length:u32); arena element sizes differ.
        let fields = [
            StructFieldType::Bool as u8,
            StructFieldType::ArrayU32 as u8,
            StructFieldType::ArrayBool as u8,
            StructFieldType::Int64 as u8,
        ];
        // (Row layout itself is computed by the one authoritative padded
        // helper in columine-vm's state_init; only field-level helpers live
        // here.)
        assert_eq!(struct_field_offset(4, &fields, 3), 1 + 1 + 8 + 8);
        assert!(has_array_fields(4, &fields));
        assert_eq!(arena_elem_size(StructFieldType::ArrayU32), 4);
        assert_eq!(arena_elem_size(StructFieldType::ArrayBool), 1);
        assert_eq!(arena_elem_size(StructFieldType::Int64), 0);
        assert_eq!(CONDITION_TREE_STATE_BYTES, 8);
    }

    #[test]
    fn align8_and_next_power_of_2_edges_match_zig() {
        assert_eq!(align8(0), 0);
        assert_eq!(align8(1), 8);
        assert_eq!(align8(8), 8);
        assert_eq!(align8(9), 16);
        assert_eq!(next_power_of_2(1), 16);
        assert_eq!(next_power_of_2(16), 16);
        assert_eq!(next_power_of_2(17), 32);
        assert_eq!(next_power_of_2(1 << 20), 1 << 20);
        assert_eq!(next_power_of_2((1 << 20) + 1), 1 << 21);
    }

    #[test]
    fn struct_row_layout_layout_matches_its_named_zig_return_value() {
        // Source: types.zig:615-624 anonymous return struct; Rust names it so it
        // can appear in the public function signature.
        assert_eq!(size_of::<StructRowLayout>(), 12);
        assert_eq!(align_of::<StructRowLayout>(), 4);
        assert_eq!(offset_of!(StructRowLayout, row_size), 0);
        assert_eq!(offset_of!(StructRowLayout, bitset_bytes), 4);
        assert_eq!(offset_of!(StructRowLayout, descriptor_size), 8);
    }

    #[test]
    fn utility_functions_match_types_zig() {
        assert_eq!(hash_key(42, 16), 4);
        assert_eq!(align8(9), 16);
        assert_eq!(next_power_of_2(0), 16);
        assert_eq!(next_power_of_2(17), 32);

        let fields = [
            StructFieldType::UInt32 as u8,
            StructFieldType::Float64 as u8,
        ];
        assert_eq!(struct_field_offset(2, &fields, 1), 5);
        assert!(!has_array_fields(2, &fields));
    }
}
