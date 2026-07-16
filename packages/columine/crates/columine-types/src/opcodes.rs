//! Replaces `packages/columine/src/vm/opcodes.zig`.
//!
//! This remains a separate canonical opcode registry, even where its tables
//! overlap `types`, because the Zig port inventory keeps the source modules
//! distinct. Like the Zig original, this is a registry/specification file:
//! vm.zig's dispatch (inline hex values) is the executable truth.
//!
//! Drift RESOLVED against vm.zig's dispatch: this registry's trailing
//! `cmp_type:u8` operand (0=u32, 1=f64, 2=i64) on the LATEST/MAX/MIN map
//! upserts (0x20, 0x24, 0x26-0x28, 0x2C, 0x2D) matches the decode arms
//! (vm.zig:1424-1508 top-level, :2213-2613 body ops) — this file was correct;
//! types.zig's operand comments omitted the operand and are now fixed on the
//! Rust side (types.rs carries the vm.zig-confirmed encodings).
//!
//! Ranges the Zig registry reserves for PLANNED opcodes (kept for tooling):
//! nested-container slot defs 0x15-0x17; time filters 0x50-0x53 (0x50+ range
//! also reserved for RETE in the axe-runtime superset binary); expressions
//! 0x60-0x69; JS callbacks 0x70-0x71; nested map ops 0x90-0x96 (partially
//! implemented per types.zig: 0x90, 0x92, 0x95).

#[repr(u8)]
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Opcode {
    /// Stop execution. Terminates both init and reduce sections.
    Halt = 0x00,
    /// Unified slot definition — covers HASHMAP, HASHSET, AGGREGATE, ARRAY,
    /// CONDITION_TREE, SCALAR, BITMAP slot types.
    SlotDef = 0x10,
    /// Array slot for `.within()` without `keyBy` — stores array of events.
    SlotArray = 0x14,
    /// Struct map slot — multi-field hash map with per-row field presence bitset.
    SlotStructMap = 0x18,
    /// Ordered list slot — append-only sequential storage (scalar or struct rows).
    SlotOrderedList = 0x19,
    /// Exact two-u32-key struct map; physical slot kind 10.
    SlotStructMap2 = 0x1b,
    /// `keyBy(field).keepValue(latest('timestamp'))` —
    /// slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8 (0=u32, 1=f64, 2=i64)
    BatchMapUpsertLatest = 0x20,
    /// `keyBy(field).keepValue(first)` — slot:u8, key_col:u8, val_col:u8
    BatchMapUpsertFirst = 0x21,
    /// `keyBy(field).keepValue(last)` — slot:u8, key_col:u8, val_col:u8
    BatchMapUpsertLast = 0x22,
    /// `.removeKeys(stream)` — slot:u8, key_col:u8
    BatchMapRemove = 0x23,
    /// TTL-aware latest upsert — slot:u8, key_col:u8, val_col:u8, ts_col:u8, cmp_type:u8
    BatchMapUpsertLatestTtl = 0x24,
    /// TTL-aware last upsert — slot:u8, key_col:u8, val_col:u8, ts_col:u8
    BatchMapUpsertLastTtl = 0x25,
    /// `keyBy(field).keepValue(max('field'))` —
    /// slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8
    BatchMapUpsertMax = 0x26,
    /// `keyBy(field).keepValue(min('field'))` —
    /// slot:u8, key_col:u8, val_col:u8, cmp_col:u8, cmp_type:u8
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
    /// `.lookup(other, { key, miss })` keyed struct-map probe.
    BatchStructMapProbe = 0x2e,
    /// Fused keyed struct-map probe and destination scatter.
    BatchStructMapProbeScatter = 0x2f,
    BatchSetInsert = 0x30,
    BatchSetRemove = 0x31,
    BatchSetInsertTtl = 0x32,
    BatchSetInsertIf = 0x33,
    BatchBitmapAdd = 0x34,
    BatchBitmapRemove = 0x35,
    BatchBitmapAnd = 0x36,
    BatchBitmapOr = 0x37,
    BatchBitmapAndNot = 0x38,
    BatchBitmapXor = 0x39,
    BatchBitmapAndScratch = 0x3a,
    BatchBitmapOrScratch = 0x3b,
    BatchBitmapAndNotScratch = 0x3c,
    BatchBitmapXorScratch = 0x3d,
    BatchAggSum = 0x40,
    BatchAggCount = 0x41,
    BatchAggMin = 0x42,
    BatchAggMax = 0x43,
    BatchAggSumIf = 0x44,
    BatchAggCountIf = 0x45,
    BatchAggMinIf = 0x46,
    BatchAggMaxIf = 0x47,
    /// Store the value from the event with the highest comparison timestamp.
    BatchScalarLatest = 0x48,
    /// Lossless i64 sum.
    BatchAggSumI64 = 0x49,
    /// Lossless i64 minimum.
    BatchAggMinI64 = 0x4a,
    /// Lossless i64 maximum.
    BatchAggMaxI64 = 0x4b,
    /// Upsert into struct map — last-write-wins per key.
    BatchStructMapUpsertLast = 0x80,
    /// Insert into struct map only when the key is absent from persisted/current
    /// state. Encoding is identical to BATCH_STRUCT_MAP_UPSERT_LAST (opcodes.zig:236).
    BatchStructMapUpsertFirst = 0x81,
    /// Upsert the whole row only when its mapped scalar comparison field is
    /// strictly greater. Encoding is 0x80 plus comparison_field_idx:u8.
    BatchStructMapUpsertMax = 0x82,
    /// Exact two-key row replacement: slot,key1_col,key2_col,count,pairs.
    BatchStructMap2UpsertLast = 0x83,
    /// Append scalar value to ordered list.
    ListAppend = 0x84,
    /// Append struct row to ordered list.
    ListAppendStruct = 0x85,
    /// Exact two-key row removal: slot,key1_col,key2_col.
    BatchStructMap2Remove = 0x86,
    /// Exact two-key conditional row replacement by signed-i64 pair maximum.
    BatchStructMap2UpsertMaxI64x2 = 0x87,
    /// Type-discriminated event loop with multi-match support.
    ForEach = 0xe0,
    /// Flat-map expansion over nested-array offsets.
    FlatMap = 0xe1,
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

/// Slot category encoded in the low four bits of a slot-definition flag byte.
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
    StructMap2 = 10,
}

/// Field storage type for STRUCT_MAP and ORDERED_LIST struct rows.
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

/// Duration unit encoding for TTL `startOf` truncation. Zig spells the
/// variants as the compiler-normalized short forms (s, m, h, d, w, M, Q, y);
/// the discriminants are the contract.
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

/// "AXE1" in little-endian.
pub const PROGRAM_MAGIC: u32 = 0x3145_5841;
/// Reserved bytes at program start for SHA-256 hash.
pub const PROGRAM_HASH_PREFIX: u32 = 32;
/// Total header size: hash prefix (32) + content header (14).
pub const PROGRAM_HEADER_SIZE: u32 = 46;

/// The byte-content header immediately after the 32-byte hash prefix.
///
/// Zig's `packed struct` (backed by u112) rounds this record to a 16-byte,
/// 16-aligned value — verified with a zig 0.16.0 layout probe (`@sizeOf`,
/// `@alignOf`, `@offsetOf`) — while only the first 14 bytes
/// ([`Self::WIRE_SIZE`]) are program wire content.
#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramHeader {
    pub magic: u32,
    pub version: u16,
    pub num_slots: u8,
    pub num_inputs: u8,
    pub num_callbacks: u8,
    pub flags: u8,
    pub init_code_len: u16,
    pub reduce_code_len: u16,
}

impl ProgramHeader {
    pub const WIRE_SIZE: usize = 14;

    /// Decodes the packed 14-byte little-endian program content header.
    pub const fn from_wire_bytes(bytes: [u8; Self::WIRE_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            version: u16::from_le_bytes([bytes[4], bytes[5]]),
            num_slots: bytes[6],
            num_inputs: bytes[7],
            num_callbacks: bytes[8],
            flags: bytes[9],
            init_code_len: u16::from_le_bytes([bytes[10], bytes[11]]),
            reduce_code_len: u16::from_le_bytes([bytes[12], bytes[13]]),
        }
    }

    /// Encodes the packed 14-byte little-endian program content header.
    pub const fn to_wire_bytes(self) -> [u8; Self::WIRE_SIZE] {
        let magic = self.magic.to_le_bytes();
        let version = self.version.to_le_bytes();
        let init_code_len = self.init_code_len.to_le_bytes();
        let reduce_code_len = self.reduce_code_len.to_le_bytes();
        [
            magic[0],
            magic[1],
            magic[2],
            magic[3],
            version[0],
            version[1],
            self.num_slots,
            self.num_inputs,
            self.num_callbacks,
            self.flags,
            init_code_len[0],
            init_code_len[1],
            reduce_code_len[0],
            reduce_code_len[1],
        ]
    }
}

/// "STAT" in little-endian.
pub const STATE_MAGIC: u32 = 0x5354_4154;
pub const STATE_HEADER_SIZE: u32 = 32;
pub const STATE_FORMAT_VERSION: u8 = 2;
pub const SLOT_META_SIZE: u32 = 48;

pub struct ChangeFlag;

impl ChangeFlag {
    pub const INSERTED: u8 = 0x01;
    pub const UPDATED: u8 = 0x02;
    pub const REMOVED: u8 = 0x04;
    pub const SIZE_CHANGED: u8 = 0x08;
    pub const EVICTED: u8 = 0x10;
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
    InvalidKey = 7,
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
    fn opcode_discriminants_match_zig() {
        assert_discriminants!(Opcode, u8;
            Halt = 0x00, SlotDef = 0x10, SlotArray = 0x14, SlotStructMap = 0x18,
            SlotOrderedList = 0x19, BatchMapUpsertLatest = 0x20, BatchMapUpsertFirst = 0x21,
            BatchMapUpsertLast = 0x22, BatchMapRemove = 0x23, BatchMapUpsertLatestTtl = 0x24,
            BatchMapUpsertLastTtl = 0x25, BatchMapUpsertMax = 0x26, BatchMapUpsertMin = 0x27,
            BatchMapUpsertLatestIf = 0x28, BatchMapUpsertFirstIf = 0x29,
            BatchMapUpsertLastIf = 0x2a, BatchMapRemoveIf = 0x2b, BatchMapUpsertMaxIf = 0x2c,
            BatchMapUpsertMinIf = 0x2d, BatchStructMapProbe = 0x2e,
            BatchStructMapProbeScatter = 0x2f, BatchSetInsert = 0x30, BatchSetRemove = 0x31,
            BatchSetInsertTtl = 0x32, BatchSetInsertIf = 0x33, BatchBitmapAdd = 0x34,
            BatchBitmapRemove = 0x35, BatchBitmapAnd = 0x36, BatchBitmapOr = 0x37,
            BatchBitmapAndNot = 0x38, BatchBitmapXor = 0x39, BatchBitmapAndScratch = 0x3a,
            BatchBitmapOrScratch = 0x3b, BatchBitmapAndNotScratch = 0x3c,
            BatchBitmapXorScratch = 0x3d, BatchAggSum = 0x40, BatchAggCount = 0x41,
            BatchAggMin = 0x42, BatchAggMax = 0x43, BatchAggSumIf = 0x44, BatchAggCountIf = 0x45,
            BatchAggMinIf = 0x46, BatchAggMaxIf = 0x47, BatchScalarLatest = 0x48,
            BatchAggSumI64 = 0x49, BatchAggMinI64 = 0x4a, BatchAggMaxI64 = 0x4b,
            BatchStructMapUpsertLast = 0x80, BatchStructMapUpsertFirst = 0x81,
            ListAppend = 0x84, ListAppendStruct = 0x85,
            ForEach = 0xe0, FlatMap = 0xe1
        );
    }

    #[test]
    fn aggregate_slot_field_duration_and_error_discriminants_match_zig() {
        assert_discriminants!(AggType, u8;
            Sum = 1, Count = 2, Min = 3, Max = 4, Avg = 5, ScalarU32 = 8,
            ScalarF64 = 9, ScalarI64 = 10, SumI64 = 11, MinI64 = 12, MaxI64 = 13
        );
        assert_discriminants!(SlotType, u8;
            HashMap = 0, HashSet = 1, Aggregate = 2, Array = 3, ConditionTree = 4,
            Scalar = 5, StructMap = 6, OrderedList = 7, Bitmap = 8
        );
        assert_discriminants!(StructFieldType, u8;
            UInt32 = 0, Int64 = 1, Float64 = 2, Bool = 3, String = 4, ArrayU32 = 5,
            ArrayI64 = 6, ArrayF64 = 7, ArrayString = 8, ArrayBool = 9
        );
        assert_discriminants!(DurationUnit, u8;
            None = 0, Second = 1, Minute = 2, Hour = 3, Day = 4, Week = 5, Month = 6,
            Quarter = 7, Year = 8
        );
        assert_discriminants!(ErrorCode, u32;
            Ok = 0, CapacityExceeded = 1, InvalidProgram = 2, InvalidSlot = 3,
            InvalidState = 4, NeedsGrowth = 5, ArenaOverflow = 6
        );
    }

    #[test]
    fn program_header_layout_matches_zig() {
        // Source: opcodes.zig:434-446. Zig compile-time layout probe: size=16, align=16.
        assert_eq!(size_of::<ProgramHeader>(), 16);
        assert_eq!(align_of::<ProgramHeader>(), 16);
        assert_eq!(offset_of!(ProgramHeader, magic), 0);
        assert_eq!(offset_of!(ProgramHeader, version), 4);
        assert_eq!(offset_of!(ProgramHeader, num_slots), 6);
        assert_eq!(offset_of!(ProgramHeader, num_inputs), 7);
        assert_eq!(offset_of!(ProgramHeader, num_callbacks), 8);
        assert_eq!(offset_of!(ProgramHeader, flags), 9);
        assert_eq!(offset_of!(ProgramHeader, init_code_len), 10);
        assert_eq!(offset_of!(ProgramHeader, reduce_code_len), 12);
    }

    #[test]
    fn program_header_wire_round_trip_matches_zig_packed_content() {
        let header = ProgramHeader {
            magic: PROGRAM_MAGIC,
            version: 7,
            num_slots: 3,
            num_inputs: 4,
            num_callbacks: 5,
            flags: 0x80,
            init_code_len: 0x1234,
            reduce_code_len: 0xabcd,
        };
        let bytes = header.to_wire_bytes();
        assert_eq!(bytes.len(), ProgramHeader::WIRE_SIZE);
        assert_eq!(ProgramHeader::from_wire_bytes(bytes), header);
    }

    #[test]
    fn change_flag_namespace_is_zero_sized() {
        // Zig's `struct { pub const ... }` exists only as a constant namespace.
        assert_eq!(size_of::<ChangeFlag>(), 0);
        assert_eq!(align_of::<ChangeFlag>(), 1);
    }
}
