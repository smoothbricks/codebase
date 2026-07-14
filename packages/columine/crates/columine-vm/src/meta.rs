//! Borrowed slot-metadata view over the state buffer.
//!
//! The Zig side (`types.zig` `getSlotMeta`) materializes `SlotMeta`, a
//! pointer-carrying transient view. This crate never forms pointers into the
//! state buffer (see `bytes`), so the equivalent view carries the metadata
//! FIELD VALUES plus the offsets needed for the two writable fields the
//! container ops touch (`size`, `change_flags`). Reads happen once at
//! construction, exactly like the Zig view; writes go through `bytes`.

use crate::bytes;
use columine_types::types::{
    SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset, SlotType, SlotTypeFlags,
};

/// types.zig:485 `getSlotMeta` — metadata record base for `slot`.
pub const fn slot_meta_base(slot: u8) -> u32 {
    STATE_HEADER_SIZE + slot as u32 * SLOT_META_SIZE
}

/// Transient view of one slot's metadata (types.zig `SlotMeta`, minus the
/// pointers). Constructed per operation; never stored.
#[derive(Clone, Copy, Debug)]
pub struct SlotMetaView {
    /// Byte offset of this slot's 48-byte metadata record.
    pub meta_base: u32,
    /// Data-region offset (`SlotMetaOffset::OFFSET`).
    pub offset: u32,
    /// Element capacity (`SlotMetaOffset::CAPACITY`).
    pub capacity: u32,
    /// Decoded type/flags byte (`SlotMetaOffset::TYPE_FLAGS`).
    pub type_flags: SlotTypeFlags,
}

impl SlotMetaView {
    /// Bind to `slot`'s metadata. Panics on an invalid slot-type byte — that
    /// is a corrupted or hand-rolled state buffer, a programmer bug (the Zig
    /// side would be UB in ReleaseSmall).
    pub fn read(state: &[u8], slot: u8) -> Self {
        let meta_base = slot_meta_base(slot);
        let type_flags =
            SlotTypeFlags::from_byte(state[(meta_base + SlotMetaOffset::TYPE_FLAGS) as usize]);
        columine_types::check!(
            type_flags.slot_type().is_some(),
            "invalid slot type bits in metadata for slot {slot}"
        );
        Self {
            meta_base,
            offset: bytes::read_u32(state, meta_base + SlotMetaOffset::OFFSET),
            capacity: bytes::read_u32(state, meta_base + SlotMetaOffset::CAPACITY),
            type_flags,
        }
    }

    #[inline(always)]
    pub fn slot_type(&self) -> SlotType {
        // Validated in `read`; unreachable here keeps the accessor infallible
        // like Zig's `meta.slotType()`.
        self.type_flags
            .slot_type()
            .unwrap_or_else(|| columine_types::die!("validated in read"))
    }

    #[inline(always)]
    pub fn has_ttl(&self) -> bool {
        self.type_flags.has_ttl()
    }

    /// types.zig `hasHashMapTimestampStorage` — HASHMAP slots store an 8-byte
    /// comparison/timestamp lane unless `no_hashmap_timestamps` is set.
    #[inline(always)]
    pub fn has_hashmap_timestamp_storage(&self) -> bool {
        self.slot_type() == SlotType::HashMap && !self.type_flags.no_hashmap_timestamps()
    }

    /// Current element count (`size_ptr.*` reads on the Zig side).
    #[inline(always)]
    pub fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.meta_base + SlotMetaOffset::SIZE)
    }

    /// `size_ptr.* = value`.
    #[inline(always)]
    pub fn set_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.meta_base + SlotMetaOffset::SIZE, value);
    }

    /// types.zig `setChangeFlag` — OR one `ChangeFlag` bit into the slot's
    /// change-flags byte.
    #[inline(always)]
    pub fn set_change_flag(&self, state: &mut [u8], flag: u8) {
        let off = (self.meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize;
        state[off] |= flag;
    }

    /// Convenience for tests and TTL paths.
    pub fn change_flags(&self, state: &[u8]) -> u8 {
        state[(self.meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize]
    }

    /// Slot-meta byte 13 (`AGG_TYPE`) — aggregate/scalar subtype, raw.
    pub fn agg_type_byte(&self, state: &[u8]) -> u8 {
        state[(self.meta_base + SlotMetaOffset::AGG_TYPE) as usize]
    }

    /// Slot-meta byte 15 — the input column carrying this slot's timestamps.
    #[inline(always)]
    pub fn timestamp_field_idx(&self, state: &[u8]) -> u8 {
        state[(self.meta_base + SlotMetaOffset::TIMESTAMP_FIELD_IDX) as usize]
    }

    /// types.zig:479 `cutoff` — `now - ttl_seconds - grace_seconds` (both f32).
    #[inline(always)]
    pub fn cutoff(&self, state: &[u8], now: f64) -> f64 {
        let ttl = bytes::read_f32(state, self.meta_base + SlotMetaOffset::TTL_SECONDS);
        let grace = bytes::read_f32(state, self.meta_base + SlotMetaOffset::GRACE_SECONDS);
        now - f64::from(ttl) - f64::from(grace)
    }

    #[inline(always)]
    pub fn eviction_index_offset(&self, state: &[u8]) -> u32 {
        bytes::read_u32(
            state,
            self.meta_base + SlotMetaOffset::EVICTION_INDEX_OFFSET,
        )
    }

    #[inline(always)]
    pub fn eviction_index_capacity(&self, state: &[u8]) -> u32 {
        bytes::read_u32(
            state,
            self.meta_base + SlotMetaOffset::EVICTION_INDEX_CAPACITY,
        )
    }

    #[inline(always)]
    pub fn eviction_index_size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.meta_base + SlotMetaOffset::EVICTION_INDEX_SIZE)
    }

    #[inline(always)]
    pub fn set_eviction_index_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(
            state,
            self.meta_base + SlotMetaOffset::EVICTION_INDEX_SIZE,
            value,
        );
    }

    #[inline(always)]
    pub fn evicted_buffer_offset(&self, state: &[u8]) -> u32 {
        bytes::read_u32(
            state,
            self.meta_base + SlotMetaOffset::EVICTED_BUFFER_OFFSET,
        )
    }

    #[inline(always)]
    pub fn evicted_count(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.meta_base + SlotMetaOffset::EVICTED_COUNT)
    }

    #[inline(always)]
    pub fn set_evicted_count(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.meta_base + SlotMetaOffset::EVICTED_COUNT, value);
    }

    pub fn has_evict_trigger(&self) -> bool {
        self.type_flags.has_evict_trigger()
    }
}
