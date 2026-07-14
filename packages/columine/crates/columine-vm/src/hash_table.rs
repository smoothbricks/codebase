//! Rust port of `packages/columine/src/vm/hash_table.zig` — generic flat
//! hash table (open addressing, linear probe) over a byte region.
//!
//! Memory layout at `offset` (hash_table.zig:15):
//!   `[cap: u32] [size: u32] [keys: u32 × cap] [entries: Entry × cap]`
//! with entries omitted for sets, and the header omitted for top-level VM
//! slots whose cap/size live in slot metadata (`bind_external`).
//!
//! Zig parameterizes by a comptime `Entry` type; here the table carries its
//! `entry_size` at runtime (0 = set, 4 = u32 map, 16 = timestamped map) and
//! entry access goes through typed helpers. Following this crate's
//! convention, no pointer/reference is ever formed into the state buffer —
//! `FlatTable` stores OFFSETS and every access is an explicit LE byte copy —
//! so the Zig `align(1) size_ptr` / `@alignCast` hazards do not exist here.
//!
//! Probe sequence, sentinel handling, load factor, and rehash placement are
//! byte-for-byte the Zig algorithm: this is observable ABI (the scan order of
//! the keys array is what `vm_map_iter_*` exposes to TS backends).

use crate::bytes;
use columine_types::types::{EMPTY_KEY, TOMBSTONE, hash_key};

/// hash_table.zig:27-29 — inline table header field offsets.
const HDR_CAP: u32 = 0;
const HDR_SIZE: u32 = 4;
pub const HDR_BYTES: u32 = 8;

/// Entry sizes for the concrete Zig instantiations (hash_table.zig:217-228).
pub const ENTRY_NONE: u32 = 0; // HashSet = FlatHashTable(void)
pub const ENTRY_U32: u32 = 4; // HashMap / PtrMap = FlatHashTable(u32)
pub const ENTRY_TIMESTAMPED: u32 = 16; // TimestampedMap = FlatHashTable(TimestampedEntry)

/// `FlatHashTable(u32).byteSize(capacity)` — header + keys + u32 entries.
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

/// hash_table.zig:48 `byteSize` for any entry size.
pub const fn byte_size(capacity: u32, entry_size: u32) -> u32 {
    HDR_BYTES + capacity * 4 + capacity * entry_size
}

/// hash_table.zig:53 `dataSizeNoHeader`.
pub const fn data_size_no_header(capacity: u32, entry_size: u32) -> u32 {
    capacity * 4 + capacity * entry_size
}

/// The key-initialization effect of `initExternal` on a headerless
/// (metadata-managed) table: every key cell becomes EMPTY_KEY. Values are
/// deliberately left untouched (Zig relies on the zeroed buffer).
#[inline(always)]
pub fn init_external_keys(state: &mut [u8], data_offset: u32, capacity: u32) {
    bytes::fill_u32(state, data_offset, capacity, EMPTY_KEY);
}

/// Result of `find_insert` (hash_table.zig:139 anonymous struct).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Probe {
    pub pos: u32,
    pub found: bool,
}

/// hash_table.zig:35 `FlatHashTable` — a bound table view. Carries offsets
/// into the state buffer, never pointers.
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
    /// hash_table.zig:58 `bind` — bind to a table with an inline header.
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

    /// hash_table.zig:71 `bindExternal` — headerless table; cap/size live
    /// externally (`size_off` typically points into slot metadata).
    pub fn bind_external(data_off: u32, cap: u32, size_off: u32, entry_size: u32) -> Self {
        Self {
            cap,
            size_off,
            keys_off: data_off,
            entries_off: data_off + cap * 4,
            entry_size,
        }
    }

    /// hash_table.zig:100 `init` — write header, fill keys with EMPTY_KEY.
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

    /// hash_table.zig:87 `initExternal` — headerless init.
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

    /// TimestampedEntry accessors (hash_table.zig:223 — extern struct
    /// `{ value: u32, _pad: u32, timestamp: f64 }`, 16 bytes).
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

    /// hash_table.zig:114 `size`.
    #[inline(always)]
    pub fn size(&self, state: &[u8]) -> u32 {
        bytes::read_u32(state, self.size_off)
    }

    #[inline(always)]
    pub fn set_size(&self, state: &mut [u8], value: u32) {
        bytes::write_u32(state, self.size_off, value);
    }

    /// hash_table.zig:118 `maxLoad` — 70% load factor, integer math.
    pub const fn max_load(&self) -> u32 {
        self.cap * 7 / 10
    }

    /// hash_table.zig:123 `find` — linear probe. The probe sequence
    /// (`hash_key` start, +1 steps, `& (cap - 1)` wrap) is observable ABI.
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

    /// hash_table.zig:139 `findInsert` — probes past tombstones to find the
    /// key deeper in the chain, reusing the FIRST tombstone for insertion
    /// when the key is truly absent. Returns None for sentinel keys or a
    /// full, tombstone-free table.
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
        // Table full — use first tombstone if available (hash_table.zig:151).
        first_tombstone.map(|ft| Probe {
            pos: ft,
            found: false,
        })
    }

    /// hash_table.zig:157 `contains`.
    pub fn contains(&self, state: &[u8], key: u32) -> bool {
        self.find(state, key).is_some()
    }

    /// hash_table.zig:162 `get` for u32-entry tables.
    pub fn get_u32(&self, state: &[u8], key: u32) -> Option<u32> {
        let pos = self.find(state, key)?;
        Some(self.entry_u32_at(state, pos))
    }

    /// hash_table.zig:170 `insertKey` — set semantics. Some(true) = newly
    /// inserted, Some(false) = already present, None = sentinel key, full
    /// table, or load factor exceeded (needs growth).
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

    /// hash_table.zig:181 `upsert` for u32-entry tables. Some(true) = newly
    /// inserted, Some(false) = overwrote existing, None = sentinel/full/load.
    /// (Zig makes set-typed `upsert` a compile error; here the entry-size
    /// debug_assert in the accessor plays that role.)
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

    /// hash_table.zig:197 `rehashInto` — move all live entries into a fresh
    /// inline-header table at `dst_offset`. Insertion placement is the plain
    /// probe WITHOUT tombstone logic (the destination has none); iteration
    /// over the source is ascending slot order, which fixes the destination
    /// layout byte-for-byte.
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
