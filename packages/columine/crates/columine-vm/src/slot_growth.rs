//! Slot sizing and typed rehash/copy used by `vm_grow_state`.
//!
//! The stress tests exercise `FlatTable` rehashing alongside the container
//! family.

use crate::aggregates;
use crate::bytes;
use columine_types::types::{CONDITION_TREE_STATE_BYTES, EMPTY_KEY, SlotType, TOMBSTONE, hash_key};

/// Primary data size per slot type. STRUCT_MAP, ORDERED_LIST, and NESTED return
/// zero because their sizing is metadata-driven.
pub fn slot_data_size(
    slot_type: SlotType,
    capacity: u32,
    has_timestamps: bool,
    agg_type_byte: u8,
) -> u32 {
    match slot_type {
        SlotType::HashMap => {
            capacity * 4 + capacity * 4 + if has_timestamps { capacity * 8 } else { 0 }
        }
        SlotType::HashSet => capacity * 4,
        // COUNT=8, others=16
        SlotType::Aggregate => aggregates::agg_slot_byte_size(agg_type_byte),
        // Condition state + interleaved u64 identities + u32 low/high values.
        SlotType::ConditionTree => {
            CONDITION_TREE_STATE_BYTES + if capacity > 0 { capacity * 16 } else { 0 }
        }
        SlotType::Scalar => 16,
        // Canonical bitmap sizing is shared by allocation, grow copy, and
        // readers. Keeping `alloc == copy == reader capacity` prevents a
        // growth copy from overrunning its allocation.
        SlotType::Bitmap => {
            columine_types::types::BITMAP_SERIALIZED_LEN_BYTES
                + crate::bitmap_ops::bitmap_payload_capacity(capacity)
        }
        SlotType::StructMap | SlotType::StructMap2 | SlotType::OrderedList | SlotType::Nested => 0,
        SlotType::Array => capacity * 4 + capacity * 8,
    }
}

/// Probe live keys into a power-of-two table and invoke `on_place` at each new pos.
#[allow(clippy::too_many_arguments)]
fn rehash_occupied(
    old_state: &[u8],
    new_state: &mut [u8],
    old_keys: u32,
    new_keys: u32,
    old_cap: u32,
    new_cap: u32,
    hash: impl Fn(&[u8], u32, u32) -> u32,
    mut on_place: impl FnMut(&mut [u8], u32, u32),
) -> u32 {
    debug_assert!(new_cap.is_power_of_two() && new_cap > 0);
    bytes::fill_u32(new_state, new_keys, new_cap, EMPTY_KEY);
    let mask = new_cap - 1;
    let mut rehashed = 0u32;
    for i in 0..old_cap {
        let k = bytes::read_u32(old_state, old_keys + i * 4);
        if k == EMPTY_KEY || k == TOMBSTONE {
            continue;
        }
        let mut pos = hash(old_state, i, k);
        while bytes::read_u32(new_state, new_keys + pos * 4) != EMPTY_KEY {
            pos = (pos + 1) & mask;
        }
        bytes::write_u32(new_state, new_keys + pos * 4, k);
        on_place(new_state, i, pos);
        rehashed += 1;
    }
    rehashed
}

/// Rehash HASHMAP keys and values, carrying the timestamps side-array when
/// present. Return the number of entries rehashed.
pub fn grow_hash_map(
    old_state: &[u8],
    new_state: &mut [u8],
    old_offset: u32,
    new_offset: u32,
    old_cap: u32,
    new_cap: u32,
    has_timestamps: bool,
) -> u32 {
    let old_vals = old_offset + old_cap * 4;
    let new_vals = new_offset + new_cap * 4;
    let old_ts = old_offset + old_cap * 8;
    let new_ts = new_offset + new_cap * 8;
    rehash_occupied(
        old_state,
        new_state,
        old_offset,
        new_offset,
        old_cap,
        new_cap,
        |_, _, k| hash_key(k, new_cap),
        |new_state, i, pos| {
            let value = bytes::read_u32(old_state, old_vals + i * 4);
            bytes::write_u32(new_state, new_vals + pos * 4, value);
            if has_timestamps {
                let ts = bytes::read_f64(old_state, old_ts + i * 8);
                bytes::write_f64(new_state, new_ts + pos * 8, ts);
            }
        },
    )
}

/// Rehash HASHSET keys and return the number of entries rehashed.
pub fn grow_hash_set(
    old_state: &[u8],
    new_state: &mut [u8],
    old_offset: u32,
    new_offset: u32,
    old_cap: u32,
    new_cap: u32,
) -> u32 {
    rehash_occupied(
        old_state,
        new_state,
        old_offset,
        new_offset,
        old_cap,
        new_cap,
        |_, _, k| hash_key(k, new_cap),
        |_, _, _| {},
    )
}

/// Copy a STRUCT_MAP descriptor, rehash keys, and move live rows to their new
/// probe positions. Return the number of entries rehashed.
#[allow(clippy::too_many_arguments)]
pub fn grow_struct_map(
    old_state: &[u8],
    new_state: &mut [u8],
    old_offset: u32,
    new_offset: u32,
    old_cap: u32,
    new_cap: u32,
    num_fields: u32,
    row_size: u32,
) -> u32 {
    let desc_size = columine_types::types::align8(num_fields);
    bytes::copy(new_state, new_offset, old_state, old_offset, num_fields);
    let old_keys_offset = old_offset + desc_size;
    let new_keys_offset = new_offset + desc_size;
    let old_rows_base = old_keys_offset + old_cap * 4;
    let new_rows_base = new_keys_offset + new_cap * 4;
    rehash_occupied(
        old_state,
        new_state,
        old_keys_offset,
        new_keys_offset,
        old_cap,
        new_cap,
        |_, _, k| hash_key(k, new_cap),
        |new_state, i, pos| {
            bytes::copy(
                new_state,
                new_rows_base + pos * row_size,
                old_state,
                old_rows_base + i * row_size,
                row_size,
            );
        },
    )
}

/// Rehash an exact two-lane-key struct map while preserving both key cells
/// and each row byte-for-byte.
#[allow(clippy::too_many_arguments)]
pub fn grow_struct_map2(
    old_state: &[u8],
    new_state: &mut [u8],
    old_offset: u32,
    new_offset: u32,
    old_cap: u32,
    new_cap: u32,
    num_fields: u32,
    row_size: u32,
) -> u32 {
    let desc_size = columine_types::types::align8(num_fields);
    bytes::copy(new_state, new_offset, old_state, old_offset, num_fields);
    let old_keys1 = old_offset + desc_size;
    let old_keys2 = old_keys1 + old_cap * 4;
    let old_rows = old_keys2 + old_cap * 4;
    let new_keys1 = new_offset + desc_size;
    let new_keys2 = new_keys1 + new_cap * 4;
    let new_rows = new_keys2 + new_cap * 4;
    bytes::zero(new_state, new_keys2, new_cap * 4);
    rehash_occupied(
        old_state,
        new_state,
        old_keys1,
        new_keys1,
        old_cap,
        new_cap,
        |old_state, i, key1| {
            let key2 = bytes::read_u32(old_state, old_keys2 + i * 4);
            columine_types::types::hash_key_pair(key1, key2, new_cap)
        },
        |new_state, i, pos| {
            let key2 = bytes::read_u32(old_state, old_keys2 + i * 4);
            bytes::write_u32(new_state, new_keys2 + pos * 4, key2);
            bytes::copy(
                new_state,
                new_rows + pos * row_size,
                old_state,
                old_rows + i * row_size,
                row_size,
            );
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Rehashing into a larger HASHMAP preserves values.
    #[test]
    fn grow_hash_map_rehashes_into_larger_table_preserving_values() {
        let mut old_buf = vec![0u8; 4096];
        let mut new_buf = vec![0u8; 8192];
        let (old_cap, new_cap) = (16u32, 32u32);

        bytes::fill_u32(&mut old_buf, 0, old_cap, EMPTY_KEY);
        for (slot, key, value) in [(0u32, 42u32, 100u32), (3, 99, 200), (7, 7, 300)] {
            bytes::write_u32(&mut old_buf, slot * 4, key);
            bytes::write_u32(&mut old_buf, old_cap * 4 + slot * 4, value);
        }

        let rehashed = grow_hash_map(&old_buf, &mut new_buf, 0, 0, old_cap, new_cap, false);
        assert_eq!(rehashed, 3);

        let get = |key: u32| -> Option<u32> {
            let mut pos = hash_key(key, new_cap);
            loop {
                let k = bytes::read_u32(&new_buf, pos * 4);
                if k == EMPTY_KEY {
                    return None;
                }
                if k == key {
                    return Some(bytes::read_u32(&new_buf, new_cap * 4 + pos * 4));
                }
                pos = (pos + 1) & (new_cap - 1);
            }
        };
        assert_eq!(get(42), Some(100));
        assert_eq!(get(99), Some(200));
        assert_eq!(get(7), Some(300));
        assert_eq!(get(1), None);
    }

    // Rehashing a HASHSET preserves keys.
    #[test]
    fn grow_hash_set_rehashes_keys_only() {
        let mut old_buf = vec![0u8; 512];
        let mut new_buf = vec![0u8; 1024];
        let (old_cap, new_cap) = (16u32, 32u32);

        bytes::fill_u32(&mut old_buf, 0, old_cap, EMPTY_KEY);
        bytes::write_u32(&mut old_buf, 2 * 4, 10);
        bytes::write_u32(&mut old_buf, 5 * 4, 20);
        bytes::write_u32(&mut old_buf, 9 * 4, 30);

        let rehashed = grow_hash_set(&old_buf, &mut new_buf, 0, 0, old_cap, new_cap);
        assert_eq!(rehashed, 3);

        let found = (0..new_cap)
            .map(|i| bytes::read_u32(&new_buf, i * 4))
            .filter(|&k| k != EMPTY_KEY && k != TOMBSTONE)
            .count();
        assert_eq!(found, 3);
    }

    // Rehashing a HASHMAP preserves its timestamps side-array.
    #[test]
    fn grow_hash_map_preserves_timestamps_side_array() {
        let mut old_buf = vec![0u8; 4096];
        let mut new_buf = vec![0u8; 8192];
        let (old_cap, new_cap) = (16u32, 32u32);

        bytes::fill_u32(&mut old_buf, 0, old_cap, EMPTY_KEY);
        bytes::write_u32(&mut old_buf, 0, 42);
        bytes::write_u32(&mut old_buf, old_cap * 4, 100);
        bytes::write_f64(&mut old_buf, old_cap * 8, 999.5);

        let rehashed = grow_hash_map(&old_buf, &mut new_buf, 0, 0, old_cap, new_cap, true);
        assert_eq!(rehashed, 1);

        let found_pos = (0..new_cap)
            .find(|&i| bytes::read_u32(&new_buf, i * 4) == 42)
            .expect("key 42 must land somewhere");
        let ts = bytes::read_f64(&new_buf, new_cap * 8 + found_pos * 8);
        assert!((ts - 999.5).abs() < 0.001);
    }

    /// The grown-state bitmap allocation, grow-copy, and reader capacity all
    /// derive from one canonical formula:
    /// `bitmap_ops::bitmap_payload_capacity`.
    #[test]
    fn bitmap_alloc_copy_reader_capacity_are_one_formula() {
        use columine_types::types::BITMAP_SERIALIZED_LEN_BYTES;
        for cap in [0u32, 1, 16, 31, 32, 255, 256, 257, 512, 4096] {
            let reader_capacity = crate::bitmap_ops::bitmap_payload_capacity(cap);
            let alloc = slot_data_size(SlotType::Bitmap, cap, false, 0);
            // alloc == serialized_len u32 + full reader-visible payload
            assert_eq!(alloc, BITMAP_SERIALIZED_LEN_BYTES + reader_capacity);
            // canonical formula shape: cap*8 + 256
            assert_eq!(reader_capacity, cap * 8 + 256);
        }
    }
}
