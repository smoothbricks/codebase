//! Generic flat hash table (open addressing, linear probe) over a byte region.
//!
//! Memory layout at `offset`:
//! `[cap: u32] [size: u32] [keys: u32 × cap] [entries: Entry × cap]`.
//! Entries are omitted for sets, and the header is omitted for top-level VM
//! slots whose capacity and size live in slot metadata (`bind_external`).
//!
//! `entry_size` selects set (0), u32 map (4), or timestamped map (16)
//! semantics at runtime. No pointer/reference is formed into the state buffer:
//! `FlatTable` stores offsets and every access is an explicit LE byte copy.
//!
//! Probe sequence, sentinel handling, load factor, and rehash placement define
//! the observable ABI; the ascending key-cell scan order is exposed to
//! TypeScript backends.

use crate::bytes;
use columine_types::types::{EMPTY_KEY, TOMBSTONE, hash_key};

/// Inline table header field offsets.
const HDR_CAP: u32 = 0;
const HDR_SIZE: u32 = 4;
pub const HDR_BYTES: u32 = 8;

/// Entry sizes for the concrete table forms.
pub const ENTRY_NONE: u32 = 0; // HashSet
pub const ENTRY_U32: u32 = 4; // HashMap / PtrMap
pub const ENTRY_TIMESTAMPED: u32 = 16; // TimestampedMap

/// HashMap byte size: inline header, keys, and u32 entries.
pub const fn hashmap_byte_size(capacity: u32) -> u32 {
    byte_size(capacity, ENTRY_U32)
}

/// `HashSet.byteSize(capacity)` (`FlatHashTable(void)`) — header + keys.
pub const fn hashset_byte_size(capacity: u32) -> u32 {
    byte_size(capacity, ENTRY_NONE)
}

/// `TimestampedMap.byteSize(capacity)`.
pub const fn timestamped_map_byte_size(capacity: u32) -> u32 {
    byte_size(capacity, ENTRY_TIMESTAMPED)
}

/// Byte size of a table with the given capacity and entry size.
pub const fn byte_size(capacity: u32, entry_size: u32) -> u32 {
    HDR_BYTES + capacity * 4 + capacity * entry_size
}

/// Data size without the inline header.
pub const fn data_size_no_header(capacity: u32, entry_size: u32) -> u32 {
    capacity * 4 + capacity * entry_size
}

/// Initialize key cells in a metadata-managed table. Values are deliberately
/// left untouched; their initialization belongs to the caller.
#[inline(always)]
pub fn init_external_keys(state: &mut [u8], data_offset: u32, capacity: u32) {
    bytes::fill_u32(state, data_offset, capacity, EMPTY_KEY);
}

/// Result of a `find_insert` probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Probe {
    pub pos: u32,
    pub found: bool,
}

/// Bound table view. Carries offsets into the state buffer, never pointers.
#[derive(Clone, Copy, Debug)]
pub struct FlatTable {
    pub cap: u32,
    /// Offset of the u32 `size` field (inline header or slot metadata).
    pub size_off: u32,
    /// Offset of the keys array (`u32 × cap`).
    pub keys_off: u32,
    /// Offset of the entries array; meaningful only when `entry_size > 0`.
    pub entries_off: u32,
    /// Bytes per entry: 0 (set), 4 (u32 map), 16 (timestamped).
    pub entry_size: u32,
}

impl FlatTable {
    /// Bind to a table with an inline header.
    #[inline(always)]
    pub fn bind(state: &[u8], offset: u32, entry_size: u32) -> Self {
        let cap = bytes::read_u32(state, offset + HDR_CAP);
        Self {
            cap,
            size_off: offset + HDR_SIZE,
            keys_off: offset + HDR_BYTES,
            entries_off: offset + HDR_BYTES + cap * 4,
            entry_size,
        }
    }

    /// Bind to a headerless table; capacity and size live externally
    /// (`size_off` typically points into slot metadata).
    pub fn bind_external(data_off: u32, cap: u32, size_off: u32, entry_size: u32) -> Self {
        Self {
            cap,
            size_off,
            keys_off: data_off,
            entries_off: data_off + cap * 4,
            entry_size,
        }
    }

    /// Initialize an inline-header table and fill keys with `EMPTY_KEY`.
    pub fn init(state: &mut [u8], offset: u32, capacity: u32, entry_size: u32) -> Self {
        bytes::write_u32(state, offset + HDR_CAP, capacity);
        bytes::write_u32(state, offset + HDR_SIZE, 0);
        bytes::fill_u32(state, offset + HDR_BYTES, capacity, EMPTY_KEY);
        Self {
            cap: capacity,
            size_off: offset + HDR_SIZE,
            keys_off: offset + HDR_BYTES,
            entries_off: offset + HDR_BYTES + capacity * 4,
            entry_size,
        }
    }

    /// Initialize a headerless table.
    pub fn init_external(
        state: &mut [u8],
        data_off: u32,
        cap: u32,
        size_off: u32,
        entry_size: u32,
    ) -> Self {
        bytes::fill_u32(state, data_off, cap, EMPTY_KEY);
        bytes::write_u32(state, size_off, 0);
        Self::bind_external(data_off, cap, size_off, entry_size)
    }

    #[inline(always)]
    pub fn key_at(&self, state: &[u8], pos: u32) -> u32 {
        bytes::read_u32(state, self.keys_off + pos * 4)
    }

    #[inline(always)]
    pub fn set_key_at(&self, state: &mut [u8], pos: u32, key: u32) {
        bytes::write_u32(state, self.keys_off + pos * 4, key);
    }

    /// u32-entry accessors (HashMap/PtrMap). Caller must have entry_size 4.
    #[inline(always)]
    pub fn entry_u32_at(&self, state: &[u8], pos: u32) -> u32 {
        debug_assert_eq!(self.entry_size, ENTRY_U32);
        bytes::read_u32(state, self.entries_off + pos * 4)
    }

    #[inline(always)]
    pub fn set_entry_u32_at(&self, state: &mut [u8], pos: u32, value: u32) {
        debug_assert_eq!(self.entry_size, ENTRY_U32);
        bytes::write_u32(state, self.entries_off + pos * 4, value);
    }

    /// Timestamped entry accessors use `{ value: u32, _pad: u32, timestamp:
    /// f64 }`, 16 bytes.
    pub fn ts_entry_at(&self, state: &[u8], pos: u32) -> (u32, f64) {
        debug_assert_eq!(self.entry_size, ENTRY_TIMESTAMPED);
        let base = self.entries_off + pos * 16;
        (
            bytes::read_u32(state, base),
            bytes::read_f64(state, base + 8),
        )
    }

    pub fn set_ts_entry_at(&self, state: &mut [u8], pos: u32, value: u32, timestamp: f64) {
        debug_assert_eq!(self.entry_size, ENTRY_TIMESTAMPED);
        let base = self.entries_off + pos * 16;
        bytes::write_u32(state, base, value);
        bytes::write_u32(state, base + 4, 0); // _pad — keep the lane zeroed
        bytes::write_f64(state, base + 8, timestamp);
    }

    fn raw_entry_copy(&self, state: &mut [u8], src_off: u32, dst_pos: u32) {
        let dst = (self.entries_off + dst_pos * self.entry_size) as usize;
        let src = src_off as usize;
        let len = self.entry_size as usize;
        state.copy_within(src..src + len, dst);
    }

    /// Current table size.
    #[inline(always)]
    pub fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    #[inline(always)]
    pub fn set_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.size_off, value);
    }

    /// Maximum size before growth, using a 70% integer load factor.
    pub const fn max_load(&self) -> u32 {
        self.cap * 7 / 10
    }

    /// Find a key by linear probing. The sequence starts at `hash_key`, steps
    /// by one, and wraps with `& (cap - 1)`.
    pub fn find(&self, state: &[u8], key: u32) -> Option<u32> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        debug_assert!(self.cap.is_power_of_two(), "probe mask requires pow2 cap");
        let mut pos = hash_key(key, self.cap);
        for _ in 0..self.cap {
            let k = self.key_at(state, pos);
            if k == key {
                return Some(pos);
            }
            if k == EMPTY_KEY {
                return None;
            }
            pos = (pos + 1) & (self.cap - 1);
        }
        None
    }

    /// Insert-or-update probe: scan past tombstones to find a deeper matching
    /// key, then reuse the first tombstone when the key is absent. Returns
    /// `None` for sentinel keys or a full tombstone-free table.
    pub fn find_insert(&self, state: &[u8], key: u32) -> Option<Probe> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        debug_assert!(self.cap.is_power_of_two(), "probe mask requires pow2 cap");
        let mut pos = hash_key(key, self.cap);
        let mut first_tombstone: Option<u32> = None;
        for _ in 0..self.cap {
            let k = self.key_at(state, pos);
            if k == key {
                return Some(Probe { pos, found: true });
            }
            if k == EMPTY_KEY {
                return Some(Probe {
                    pos: first_tombstone.unwrap_or(pos),
                    found: false,
                });
            }
            if k == TOMBSTONE && first_tombstone.is_none() {
                first_tombstone = Some(pos);
            }
            pos = (pos + 1) & (self.cap - 1);
        }
        // A full table reuses the first tombstone when one exists.
        first_tombstone.map(|ft| Probe {
            pos: ft,
            found: false,
        })
    }

    /// Test whether a key is present.
    pub fn contains(&self, state: &[u8], key: u32) -> bool {
        self.find(state, key).is_some()
    }

    /// Read a value from a u32-entry table.
    pub fn get_u32(&self, state: &[u8], key: u32) -> Option<u32> {
        let pos = self.find(state, key)?;
        Some(self.entry_u32_at(state, pos))
    }

    /// Set semantics: `Some(true)` inserted, `Some(false)` already present,
    /// and `None` means sentinel, full table, or load factor exceeded.
    pub fn insert_key(&self, state: &mut [u8], key: u32) -> Option<bool> {
        let probe = self.find_insert(state, key)?;
        if probe.found {
            return Some(false);
        }
        if self.size(state) >= self.max_load() {
            return None;
        }
        self.set_key_at(state, probe.pos, key);
        let size = self.size(state);
        self.set_size(state, size + 1);
        Some(true)
    }

    /// Upsert in a u32-entry table: `Some(true)` inserted,
    /// `Some(false)` overwrote an existing value, and `None` means sentinel,
    /// full table, or load factor exceeded. The entry-size assertion enforces
    /// that this accessor is not used for set tables.
    pub fn upsert_u32(&self, state: &mut [u8], key: u32, value: u32) -> Option<bool> {
        let probe = self.find_insert(state, key)?;
        if probe.found {
            self.set_entry_u32_at(state, probe.pos, value);
            return Some(false);
        }
        if self.size(state) >= self.max_load() {
            return None;
        }
        self.set_key_at(state, probe.pos, key);
        self.set_entry_u32_at(state, probe.pos, value);
        let size = self.size(state);
        self.set_size(state, size + 1);
        Some(true)
    }

    /// Move live entries into a fresh inline-header table. The destination has
    /// no tombstones, so insertion uses plain probing; ascending source-slot
    /// iteration fixes destination placement.
    pub fn rehash_into(&self, state: &mut [u8], dst_offset: u32, new_cap: u32) -> Self {
        let dst = Self::init(state, dst_offset, new_cap, self.entry_size);
        let mut moved = 0u32;
        for i in 0..self.cap {
            let k = self.key_at(state, i);
            if k != EMPTY_KEY && k != TOMBSTONE {
                let mut pos = hash_key(k, new_cap);
                while dst.key_at(state, pos) != EMPTY_KEY {
                    pos = (pos + 1) & (new_cap - 1);
                }
                dst.set_key_at(state, pos, k);
                if self.entry_size > 0 {
                    let src = self.entries_off + i * self.entry_size;
                    dst.raw_entry_copy(state, src, pos);
                }
                moved += 1;
            }
        }
        dst.set_size(state, moved);
        dst
    }

    /// Ascending-slot-order scan of live keys — THE iteration order
    /// `vm_map_iter_*` exposes to TS backends (the vm slice wraps this).
    /// Yields `(slot_index, key)`.
    pub fn iter_live<'a>(&'a self, state: &'a [u8]) -> impl Iterator<Item = (u32, u32)> + 'a {
        (0..self.cap).filter_map(move |pos| {
            let k = self.key_at(state, pos);
            (k != EMPTY_KEY && k != TOMBSTONE).then_some((pos, k))
        })
    }
}
