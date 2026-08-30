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
use columine_types::types::{EMPTY_KEY, TOMBSTONE, hash_key, hash_key_pair};

/// Inline table header field offsets.
const HDR_CAP: u32 = 0;
const HDR_SIZE: u32 = 4;
pub const HDR_BYTES: u32 = 8;

/// Entry sizes for the concrete table forms.
pub const ENTRY_NONE: u32 = 0; // HashSet
pub const ENTRY_U32: u32 = 4; // HashMap / PtrMap

/// HashMap byte size: inline header, keys, and u32 entries.
pub const fn hashmap_byte_size(capacity: u32) -> u32 {
    byte_size(capacity, ENTRY_U32)
}

/// `HashSet.byteSize(capacity)` (`FlatHashTable(void)`) — header + keys.
pub const fn hashset_byte_size(capacity: u32) -> u32 {
    byte_size(capacity, ENTRY_NONE)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProbeCell {
    Match,
    Empty,
    Tombstone,
    Occupied,
}

/// One bounded linear-probe implementation for every VM table layout.
/// Layout-specific wrappers provide the home position and classify a cell;
/// tombstone reuse is data so lookup and insertion cannot drift.
#[inline]
pub(crate) fn probe_linear(
    capacity: u32,
    start: u32,
    reuse_tombstones: bool,
    mut classify: impl FnMut(u32) -> ProbeCell,
) -> Option<Probe> {
    debug_assert!(capacity.is_power_of_two(), "probe mask requires pow2 cap");
    let mut position = start;
    let mut first_tombstone = None;
    for _ in 0..capacity {
        match classify(position) {
            ProbeCell::Match => {
                return Some(Probe {
                    pos: position,
                    found: true,
                });
            }
            ProbeCell::Empty => {
                return Some(Probe {
                    pos: first_tombstone.unwrap_or(position),
                    found: false,
                });
            }
            ProbeCell::Tombstone if reuse_tombstones && first_tombstone.is_none() => {
                first_tombstone = Some(position);
            }
            ProbeCell::Tombstone | ProbeCell::Occupied => {}
        }
        position = (position + 1) & (capacity - 1);
    }
    first_tombstone.map(|pos| Probe { pos, found: false })
}

/// Point lookup over a headerless `u32` key array. This is the read half of
/// the probe contract, taking raw offsets so the exported ABI helpers — which
/// receive a slot offset and a capacity, never a bound view — resolve keys
/// through the same primitive as every bound table.
///
/// Both sentinels are refused as query keys: `EMPTY_KEY` marks a vacant cell
/// and `TOMBSTONE` a vacated one, so admitting either would match a dead cell
/// and report its stale payload as a live entry.
#[inline]
pub(crate) fn find_key(state: &[u8], keys_off: u32, cap: u32, key: u32) -> Option<u32> {
    if key == EMPTY_KEY || key == TOMBSTONE {
        return None;
    }
    probe_linear(cap, hash_key(key, cap), false, |pos| match bytes::read_u32(
        state,
        keys_off + pos * 4,
    ) {
        current if current == key => ProbeCell::Match,
        EMPTY_KEY => ProbeCell::Empty,
        TOMBSTONE => ProbeCell::Tombstone,
        _ => ProbeCell::Occupied,
    })
    .filter(|probe| probe.found)
    .map(|probe| probe.pos)
}

/// Point lookup over two parallel `u32` key lanes. Lane one owns the sentinel
/// state and lane two preserves every u32 value, so identity compares both
/// lanes while the hash only chooses the home position.
#[inline]
pub(crate) fn find_key_pair(
    state: &[u8],
    keys1_off: u32,
    keys2_off: u32,
    cap: u32,
    key1: u32,
    key2: u32,
) -> Option<u32> {
    if key1 == EMPTY_KEY || key1 == TOMBSTONE {
        return None;
    }
    probe_linear(cap, hash_key_pair(key1, key2, cap), false, |pos| {
        let first = bytes::read_u32(state, keys1_off + pos * 4);
        if first == key1 && bytes::read_u32(state, keys2_off + pos * 4) == key2 {
            ProbeCell::Match
        } else {
            match first {
                EMPTY_KEY => ProbeCell::Empty,
                TOMBSTONE => ProbeCell::Tombstone,
                _ => ProbeCell::Occupied,
            }
        }
    })
    .filter(|probe| probe.found)
    .map(|probe| probe.pos)
}

/// Index of the first live key cell at or after `from`, or `cap` when the scan
/// is exhausted. This ascending-cell order is THE iteration ABI exported to
/// TypeScript backends; both sentinels mark dead cells and are skipped.
#[inline]
pub(crate) fn next_live_key(state: &[u8], keys_off: u32, cap: u32, from: u32) -> u32 {
    (from..cap)
        .find(|&pos| {
            let key = bytes::read_u32(state, keys_off + pos * 4);
            key != EMPTY_KEY && key != TOMBSTONE
        })
        .unwrap_or(cap)
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
        find_key(state, self.keys_off, self.cap, key)
    }

    /// Insert-or-update probe: scan past tombstones to find a deeper matching
    /// key, then reuse the first tombstone when the key is absent.
    pub fn find_insert(&self, state: &[u8], key: u32) -> Option<Probe> {
        if key == EMPTY_KEY || key == TOMBSTONE {
            return None;
        }
        probe_linear(self.cap, hash_key(key, self.cap), true, |pos| {
            match self.key_at(state, pos) {
                current if current == key => ProbeCell::Match,
                EMPTY_KEY => ProbeCell::Empty,
                TOMBSTONE => ProbeCell::Tombstone,
                _ => ProbeCell::Occupied,
            }
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

    /// Ascending-cell scan of live keys, stepping with [`next_live_key`] so the
    /// bound view and the `vm_map_iter_*` exports walk one implementation.
    /// Yields `(cell_index, key)`.
    pub fn iter_live<'a>(&'a self, state: &'a [u8]) -> impl Iterator<Item = (u32, u32)> + 'a {
        let mut pos = next_live_key(state, self.keys_off, self.cap, 0);
        std::iter::from_fn(move || {
            (pos < self.cap).then(|| {
                let cell = (pos, self.key_at(state, pos));
                pos = next_live_key(state, self.keys_off, self.cap, pos + 1);
                cell
            })
        })
    }
}
