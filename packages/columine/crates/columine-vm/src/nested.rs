//! Rust port of `packages/columine/src/vm/nested.zig` — nested container
//! slots: arena-allocated inner maps, sets, and aggregates.
//!
//! `Map<K, Set<V>>` / `Map<K, Map<K2, V>>` / `Map<K, Agg>`: the outer hash
//! table maps u32 keys → u32 arena offsets; inner containers are
//! bump-allocated in a per-slot arena. Inner growth allocates 2× in the
//! arena and abandons the old space; `vm_grow_state` (state_init/slot_growth,
//! slice 1) walks the tree and reclaims dead arena space at grow time.
//!
//! The two `e2e —` test blocks in nested.zig drive `vm.zig`'s
//! `vm_calculate_state_size` / `vm_init_state` / `vm_execute_batch` and are
//! deferred to the dispatch slice (see README).

use crate::meta::SlotMetaView;
use crate::{aggregates, bytes, hash_table};
use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, SlotType, TOMBSTONE, hash_key, next_power_of_2,
};

/// nested.zig:87 — prefix stored at the start of a NESTED slot's data:
/// `[inner_type:u8][inner_initial_cap:u16 le][inner_agg_type:u8][depth:u8][reserved:3]`.
pub const NESTED_PREFIX_SIZE: u32 = 8;

/// nested.zig Arena.HDR_SIZE — `[capacity:u32][used:u32]`.
pub const ARENA_HDR_SIZE: u32 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NestedPrefix {
    pub inner_type: SlotType,
    pub inner_initial_cap: u16,
    /// Raw AggType byte (see `aggregates` on why raw bytes are the contract).
    pub inner_agg_type_byte: u8,
    pub depth: u8,
}

/// nested.zig:107-115 `writeNestedPrefix`.
pub fn write_nested_prefix(state: &mut [u8], slot_offset: u32, prefix: NestedPrefix) {
    let base = slot_offset as usize;
    state[base] = prefix.inner_type as u8;
    state[base + 1] = (prefix.inner_initial_cap & 0xff) as u8;
    state[base + 2] = (prefix.inner_initial_cap >> 8) as u8;
    state[base + 3] = prefix.inner_agg_type_byte;
    state[base + 4] = prefix.depth;
    state[base + 5..base + 8].fill(0);
}

/// nested.zig:96-105 `readNestedPrefix`. The stored inner type is an
/// invariant this crate wrote; an invalid low nibble is a programmer bug.
pub fn read_nested_prefix(state: &[u8], slot_offset: u32) -> NestedPrefix {
    let base = slot_offset as usize;
    let inner_type = SlotType::from_u8(state[base] & 0x0f).unwrap_or_else(|| {
        columine_types::die!("invariant: nested prefix contains an invalid inner slot type")
    });
    let agg_byte = state[base + 3];
    NestedPrefix {
        inner_type,
        inner_initial_cap: u16::from(state[base + 1]) | (u16::from(state[base + 2]) << 8),
        // readNestedPrefix normalizes out-of-range bytes to SUM (=1).
        inner_agg_type_byte: if (1..=13).contains(&agg_byte) {
            agg_byte
        } else {
            1
        },
        depth: state[base + 4],
    }
}

/// nested.zig:121-123.
pub const fn outer_keys_offset(slot_offset: u32) -> u32 {
    slot_offset + NESTED_PREFIX_SIZE
}

/// nested.zig:125-127.
pub const fn outer_ptrs_offset(slot_offset: u32, capacity: u32) -> u32 {
    outer_keys_offset(slot_offset) + capacity * 4
}

/// nested.zig:129-133.
pub const fn arena_header_offset(slot_offset: u32, capacity: u32) -> u32 {
    outer_ptrs_offset(slot_offset, capacity) + capacity * 4
}

/// nested.zig:135-137.
pub const fn arena_data_offset(slot_offset: u32, capacity: u32) -> u32 {
    arena_header_offset(slot_offset, capacity) + ARENA_HDR_SIZE
}

/// nested.zig:143-152 `innerContainerSize` — inline-header container sizes.
pub fn inner_container_size(inner_type: SlotType, capacity: u32, inner_agg_type_byte: u8) -> u32 {
    match inner_type {
        SlotType::HashMap => hash_table::hashmap_byte_size(capacity),
        SlotType::HashSet => hash_table::hashset_byte_size(capacity),
        SlotType::Aggregate => aggregates::agg_slot_byte_size(inner_agg_type_byte),
        _ => 0,
    }
}

/// nested.zig:155-158 `nestedSlotDataSize`:
/// prefix + outer keys + outer ptrs + arena header + `outer_cap` pre-sized
/// inner containers (each align8-rounded).
pub fn nested_slot_data_size(
    outer_cap: u32,
    inner_initial_cap: u32,
    inner_type: SlotType,
    inner_agg_type_byte: u8,
) -> u32 {
    let per_inner = columine_types::types::align8(inner_container_size(
        inner_type,
        inner_initial_cap,
        inner_agg_type_byte,
    ));
    NESTED_PREFIX_SIZE + outer_cap * 4 + outer_cap * 4 + ARENA_HDR_SIZE + outer_cap * per_inner
}

/// Initialize the arena header (`[capacity][used=0]`) for a nested slot.
pub fn write_arena_header(state: &mut [u8], header_offset: u32, arena_capacity: u32) {
    bytes::write_u32(state, header_offset, arena_capacity);
    bytes::write_u32(state, header_offset + 4, 0);
}

// =============================================================================
// Arena — bump allocator within the slot data region (nested.zig:37-81)
// =============================================================================

/// nested.zig:42-44 — arena header field offsets.
const ARENA_HDR_USED: u32 = 4;

/// A bound arena view. Carries offsets, never pointers (see `bytes`).
#[derive(Clone, Copy, Debug)]
pub struct Arena {
    /// Absolute offset of the arena header (`[capacity:u32][used:u32]`).
    pub hdr_offset: u32,
    /// Absolute offset of the arena data start.
    pub data_offset: u32,
}

impl Arena {
    /// nested.zig:46 `bind`.
    pub const fn bind(hdr_offset: u32) -> Self {
        Self {
            hdr_offset,
            data_offset: hdr_offset + ARENA_HDR_SIZE,
        }
    }

    /// nested.zig:54 `initAt` — write `[cap][used=0]` and bind.
    pub fn init_at(state: &mut [u8], hdr_offset: u32, cap: u32) -> Self {
        write_arena_header(state, hdr_offset, cap);
        Self::bind(hdr_offset)
    }

    /// nested.zig:60 `capacity`.
    pub fn capacity(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.hdr_offset)
    }

    /// nested.zig:64 `used`.
    pub fn used(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.hdr_offset + ARENA_HDR_USED)
    }

    fn set_used(&self, state: &mut [u8], val: u32) {
        bytes::write_u32(state, self.hdr_offset + ARENA_HDR_USED, val);
    }

    /// nested.zig:73 `alloc` — bump-allocate `size` bytes (align8-rounded).
    /// Returns the ABSOLUTE offset, or None when the arena is full.
    pub fn alloc(&self, state: &mut [u8], size: u32) -> Option<u32> {
        let u = self.used(state);
        let aligned = columine_types::types::align8(size);
        if u + aligned > self.capacity(state) {
            return None;
        }
        let offset = self.data_offset + u;
        self.set_used(state, u + aligned);
        Some(offset)
    }
}

// =============================================================================
// Outer hash table — typed access to keys + arena pointers (nested.zig:164-252)
// =============================================================================

/// nested.zig:191 `resolve` result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Resolved {
    /// Absolute offset of the inner container.
    pub offset: u32,
    pub is_new: bool,
}

/// nested.zig:164 `OuterTable` — a bound view carrying offsets.
#[derive(Clone, Copy, Debug)]
pub struct OuterTable {
    pub cap: u32,
    /// Offset of the outer u32 size field (slot metadata `SIZE`).
    pub size_off: u32,
    pub keys_off: u32,
    /// Arena-offset array, one u32 per outer key slot.
    pub ptrs_off: u32,
    pub arena: Arena,
}

impl OuterTable {
    /// nested.zig:172 `bind`.
    pub fn bind(meta: &SlotMetaView) -> Self {
        let slot_off = meta.offset;
        let cap = meta.capacity;
        Self {
            cap,
            size_off: meta.meta_base + columine_types::types::SlotMetaOffset::SIZE,
            keys_off: outer_keys_offset(slot_off),
            ptrs_off: outer_ptrs_offset(slot_off, cap),
            arena: Arena::bind(arena_header_offset(slot_off, cap)),
        }
    }

    fn key_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys_off + pos * 4)
    }

    fn ptr_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.ptrs_off + pos * 4)
    }

    fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    /// nested.zig:191 `resolve` — outer key → inner container offset,
    /// allocating and initializing the inner container on first sight.
    /// None = sentinel key, outer load factor exceeded, arena full, or probe
    /// exhaustion (all CAPACITY_EXCEEDED at the op layer).
    ///
    /// The insertion probe uses the unified probe-then-reuse policy
    /// (post-parity: the deleted Zig claimed a tombstone immediately, so
    /// resolve could shadow a live outer key sitting past it while lookup
    /// probed on — an asymmetry that made resolve and lookup disagree).
    ///
    /// WHY a new AGGREGATE inner stays ZEROED (deliberate, not drift): the
    /// arena hands back zeroed memory for free, and every nested MIN/MAX
    /// reader guards with `count == 0` (nested.zig:217/329 convention)
    /// instead of `init_agg_slot`'s ±infinity sentinels — writing sentinels
    /// per inner would cost a write per allocation to change no observable
    /// behavior (pinned by the nested model proptests). The `count == 0`
    /// guard is also the algebra-correct shape (~/Dev/_wt/TREAT.md §1):
    /// count is group-invertible, so emptiness is exactly maintainable and
    /// is the right emptiness signal for min/max, whose values are the
    /// support-scan class and carry no meaningful sentinel under deltas.
    pub fn resolve(
        &self,
        state: &mut [u8],
        outer_key: u32,
        prefix: NestedPrefix,
    ) -> Option<Resolved> {
        if outer_key == EMPTY_KEY || outer_key == TOMBSTONE {
            return None;
        }
        debug_assert!(self.cap.is_power_of_two(), "probe mask requires pow2 cap");
        let mut pos = hash_key(outer_key, self.cap);
        let mut first_tombstone: Option<u32> = None;
        let mut insert_pos: Option<u32> = None;
        for _ in 0..self.cap {
            let k = self.key_at(state, pos);
            if k == outer_key {
                return Some(Resolved {
                    offset: self.ptr_at(state, pos),
                    is_new: false,
                });
            }
            if k == TOMBSTONE {
                if first_tombstone.is_none() {
                    first_tombstone = Some(pos);
                }
            } else if k == EMPTY_KEY {
                insert_pos = Some(first_tombstone.unwrap_or(pos));
            }
            if let Some(pos) = insert_pos {
                // Insert new outer key + allocate inner container.
                if self.size(state) >= self.cap * 7 / 10 {
                    return None;
                }

                let inner_cap = next_power_of_2(u32::from(prefix.inner_initial_cap));
                let inner_size =
                    inner_container_size(prefix.inner_type, inner_cap, prefix.inner_agg_type_byte);
                let inner_off = self.arena.alloc(state, inner_size)?;

                // Initialize the inner container (nested.zig:214-219).
                match prefix.inner_type {
                    SlotType::HashSet => {
                        hash_table::FlatTable::init(
                            state,
                            inner_off,
                            inner_cap,
                            hash_table::ENTRY_NONE,
                        );
                    }
                    SlotType::HashMap => {
                        hash_table::FlatTable::init(
                            state,
                            inner_off,
                            inner_cap,
                            hash_table::ENTRY_U32,
                        );
                    }
                    SlotType::Aggregate => bytes::zero(state, inner_off, inner_size),
                    _ => {}
                }

                bytes::write_u32(state, self.keys_off + pos * 4, outer_key);
                bytes::write_u32(state, self.ptrs_off + pos * 4, inner_off);
                let size = self.size(state);
                bytes::write_u32(state, self.size_off, size + 1);
                return Some(Resolved {
                    offset: inner_off,
                    is_new: true,
                });
            }
            pos = (pos + 1) & (self.cap - 1);
        }
        // No EMPTY cell found (table saturated with keys + tombstones):
        // the load-factor gate above refuses long before this in practice.
        None
    }

    /// nested.zig:232 `lookup` — inner container offset for `outer_key`, or
    /// 0 when absent. Only EMPTY_KEY terminates the probe; TOMBSTONE cells
    /// are probed past — symmetric with `resolve` post-parity.
    pub fn lookup(&self, state: &[u8], outer_key: u32) -> u32 {
        let mut pos = hash_key(outer_key, self.cap);
        for _ in 0..self.cap {
            let k = self.key_at(state, pos);
            if k == outer_key {
                return self.ptr_at(state, pos);
            }
            if k == EMPTY_KEY {
                return 0;
            }
            pos = (pos + 1) & (self.cap - 1);
        }
        0
    }

    /// nested.zig:245 `updatePtr` — repoint an EXISTING outer key at a grown
    /// inner container. The Zig loop is unbounded (absence is a programmer
    /// bug); here the probe is bounded by `cap` and panics past it.
    pub fn update_ptr(&self, state: &mut [u8], outer_key: u32, new_offset: u32) {
        let mut pos = hash_key(outer_key, self.cap);
        for _ in 0..self.cap {
            if self.key_at(state, pos) == outer_key {
                bytes::write_u32(state, self.ptrs_off + pos * 4, new_offset);
                return;
            }
            pos = (pos + 1) & (self.cap - 1);
        }
        columine_types::die!("update_ptr: outer key {outer_key} absent — resolve() must precede");
    }
}

// =============================================================================
// Public operations (nested.zig:258-347)
// =============================================================================

/// nested.zig:259 `nestedSetInsert` — `Map<outer_key, Set>.add(elem)`.
pub fn nested_set_insert(
    state: &mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    elem: u32,
) -> ErrorCode {
    let prefix = read_nested_prefix(state, meta.offset);
    let outer = OuterTable::bind(meta);
    let Some(resolved) = outer.resolve(state, outer_key, prefix) else {
        return ErrorCode::CapacityExceeded;
    };

    if resolved.is_new {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }

    let inner = hash_table::FlatTable::bind(state, resolved.offset, hash_table::ENTRY_NONE);
    if let Some(was_new) = inner.insert_key(state, elem) {
        if was_new {
            meta.set_change_flag(state, ChangeFlag::INSERTED);
        }
        return ErrorCode::Ok;
    }

    // Inner needs growth — allocate 2×, rehash, update outer pointer
    // (nested.zig:272-280).
    let new_size = hash_table::hashset_byte_size(inner.cap * 2);
    let Some(new_off) = outer.arena.alloc(state, new_size) else {
        return ErrorCode::CapacityExceeded;
    };
    let grown = inner.rehash_into(state, new_off, inner.cap * 2);
    outer.update_ptr(state, outer_key, new_off);

    let _ = grown.insert_key(state, elem);
    meta.set_change_flag(state, ChangeFlag::INSERTED);
    ErrorCode::Ok
}

/// nested.zig:284 `nestedMapUpsertLast` — `Map<outer_key, Map<inner_key, v>>`.
pub fn nested_map_upsert_last(
    state: &mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    inner_key: u32,
    value: u32,
) -> ErrorCode {
    let prefix = read_nested_prefix(state, meta.offset);
    let outer = OuterTable::bind(meta);
    let Some(resolved) = outer.resolve(state, outer_key, prefix) else {
        return ErrorCode::CapacityExceeded;
    };

    if resolved.is_new {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }

    let inner = hash_table::FlatTable::bind(state, resolved.offset, hash_table::ENTRY_U32);
    if let Some(was_new) = inner.upsert_u32(state, inner_key, value) {
        if was_new {
            meta.set_change_flag(state, ChangeFlag::INSERTED);
        } else {
            meta.set_change_flag(state, ChangeFlag::UPDATED);
        }
        return ErrorCode::Ok;
    }

    // Inner needs growth (nested.zig:297-305). The change flag reflects
    // what actually happened post-parity: INSERTED only on genuine inserts
    // (the deleted Zig set INSERTED unconditionally, even for overwrites).
    let new_size = hash_table::hashmap_byte_size(inner.cap * 2);
    let Some(new_off) = outer.arena.alloc(state, new_size) else {
        return ErrorCode::CapacityExceeded;
    };
    let grown = inner.rehash_into(state, new_off, inner.cap * 2);
    outer.update_ptr(state, outer_key, new_off);

    let was_new = grown.upsert_u32(state, inner_key, value).unwrap_or(false);
    meta.set_change_flag(
        state,
        if was_new {
            ChangeFlag::INSERTED
        } else {
            ChangeFlag::UPDATED
        },
    );
    ErrorCode::Ok
}

/// Raw AggType discriminants nested aggregates switch on (see `aggregates`
/// on why raw bytes, not the enum, are the contract here).
const AGG_SUM: u8 = 1;
const AGG_COUNT: u8 = 2;
const AGG_MIN: u8 = 3;
const AGG_MAX: u8 = 4;

/// nested.zig:309 `nestedAggUpdate` — `Map<outer_key, Agg>.update(value)`.
/// `value_bits` carries the f64 bit pattern exactly like the Zig u64 param.
pub fn nested_agg_update(
    state: &mut [u8],
    meta: &SlotMetaView,
    outer_key: u32,
    value_bits: u64,
) -> ErrorCode {
    let prefix = read_nested_prefix(state, meta.offset);
    let outer = OuterTable::bind(meta);
    let Some(resolved) = outer.resolve(state, outer_key, prefix) else {
        return ErrorCode::CapacityExceeded;
    };

    if resolved.is_new {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }

    // Aggregate is fixed-size, no hash table — just raw bytes. Zeroed at
    // allocation, so MIN/MAX use `count == 0` instead of infinity sentinels.
    let base = resolved.offset;
    match prefix.inner_agg_type_byte {
        AGG_COUNT => {
            let count = bytes::read_u64(state, base);
            bytes::write_u64(state, base, count + 1);
        }
        AGG_SUM => {
            let val = bytes::read_f64(state, base);
            let count = bytes::read_u64(state, base + 8);
            bytes::write_f64(state, base, val + f64::from_bits(value_bits));
            bytes::write_u64(state, base + 8, count + 1);
        }
        AGG_MIN => {
            let count = bytes::read_u64(state, base + 8);
            let new_val = f64::from_bits(value_bits);
            if count == 0 || new_val < bytes::read_f64(state, base) {
                bytes::write_f64(state, base, new_val);
            }
            bytes::write_u64(state, base + 8, count + 1);
        }
        AGG_MAX => {
            let count = bytes::read_u64(state, base + 8);
            let new_val = f64::from_bits(value_bits);
            if count == 0 || new_val > bytes::read_f64(state, base) {
                bytes::write_f64(state, base, new_val);
            }
            bytes::write_u64(state, base + 8, count + 1);
        }
        _ => {}
    }
    meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
    ErrorCode::Ok
}

// =============================================================================
// Read operations (nested.zig:353-384)
// =============================================================================

/// nested.zig:353 `getInnerOffset` — 0 when the outer key is absent.
pub fn get_inner_offset(state: &[u8], meta: &SlotMetaView, outer_key: u32) -> u32 {
    OuterTable::bind(meta).lookup(state, outer_key)
}

/// nested.zig:357 `getInnerSetSize`.
pub fn get_inner_set_size(state: &[u8], inner_offset: u32) -> u32 {
    if inner_offset == 0 {
        return 0;
    }
    hash_table::FlatTable::bind(state, inner_offset, hash_table::ENTRY_NONE).size(state)
}

/// nested.zig:362 `innerSetContains`.
pub fn inner_set_contains(state: &[u8], inner_offset: u32, elem: u32) -> bool {
    if inner_offset == 0 {
        return false;
    }
    hash_table::FlatTable::bind(state, inner_offset, hash_table::ENTRY_NONE).contains(state, elem)
}

/// nested.zig:367 `innerMapGet` — EMPTY_KEY when absent (either level).
pub fn inner_map_get(state: &[u8], inner_offset: u32, key: u32) -> u32 {
    if inner_offset == 0 {
        return EMPTY_KEY;
    }
    hash_table::FlatTable::bind(state, inner_offset, hash_table::ENTRY_U32)
        .get_u32(state, key)
        .unwrap_or(EMPTY_KEY)
}

/// nested.zig:373 `innerAggGetF64`.
pub fn inner_agg_get_f64(state: &[u8], inner_offset: u32) -> f64 {
    if inner_offset == 0 {
        return 0.0;
    }
    bytes::read_f64(state, inner_offset)
}

/// nested.zig:379 `innerAggGetCount` — count at offset 0 for COUNT, 8 otherwise.
pub fn inner_agg_get_count(state: &[u8], inner_offset: u32, agg_type_byte: u8) -> u64 {
    if inner_offset == 0 {
        return 0;
    }
    let count_off = if agg_type_byte == AGG_COUNT { 0 } else { 8 };
    bytes::read_u64(state, inner_offset + count_off)
}
