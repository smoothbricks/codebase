//! Rust port of `packages/columine/src/vm/intern.zig` — string interning to
//! u32 indices (fast u32 key comparisons, Arrow dictionary-encoding
//! compatibility, cross-batch dedup).
//!
//! Layout mirrors the Zig exactly (intern.zig:10): concatenated UTF-8 `data`
//! with `offsets[i]..offsets[i+1]` string boundaries (`offsets[count]` always
//! equals `data_len`), plus an open-addressing hash table (FNV-1a u32 hash,
//! linear probing, no tombstones — interning never removes).
//!
// Initial sizing: `data` = cap×32 ("assume ~32 bytes per string"),
// `offsets` = cap+1, hash = nextPowerOf2(cap×2) — and every buffer GROWS on
// demand (the deleted Zig bump-heap model had no overflow checks and
// overran/probed forever past capacity; interned u32 handles stay stable
// across growth because growth only appends).
//!
//! The `intern_*` exports (intern.zig:182-220) are the wasm ABI over a
//! global singleton + 4 MB bump heap — that surface belongs to the bindings
//! stage (stage 5); this module ports the `StringIntern` mechanism.

use columine_types::types::next_power_of_2;

/// intern.zig:36 — empty hash-table sentinel.
const EMPTY: u32 = 0xFFFF_FFFF;

/// intern.zig:81 `hashBytes` — FNV-1a, 32-bit (offset basis 2166136261,
/// prime 16777619, wrapping multiply).
pub fn hash_bytes(bytes: &[u8]) -> u32 {
    let mut h: u32 = 2_166_136_261;
    for &b in bytes {
        h ^= u32::from(b);
        h = h.wrapping_mul(16_777_619);
    }
    h
}

/// intern.zig:19 `StringIntern`. Buffers start at the Zig sizing model and
/// grow on demand; u32 handles are stable (growth only appends).
#[derive(Debug)]
pub struct StringIntern {
    data: Vec<u8>,
    data_len: u32,
    offsets: Vec<u32>,
    count: u32,
    hash_keys: Vec<u32>,
    hash_indices: Vec<u32>,
    hash_cap: u32,
}

impl StringIntern {
    /// intern.zig:38 `init` — capacities derived from `initial_cap`:
    /// data = cap×32 ("assume ~32 bytes per string"), offsets = cap+1,
    /// hash = nextPowerOf2(cap×2) (50% load factor). intern.zig's private
    /// `nextPowerOf2` (intern.zig:69) is identical to types.zig's ported
    /// `next_power_of_2` incl. the clamp-to-16 floor.
    pub fn new(initial_cap: u32) -> Self {
        let data_cap = initial_cap * 32;
        let offsets_cap = initial_cap + 1;
        let hash_cap = next_power_of_2(initial_cap * 2);
        let mut offsets = vec![0u32; offsets_cap as usize];
        offsets[0] = 0; // intern.zig:54 — first offset is 0
        Self {
            data: vec![0u8; data_cap as usize],
            data_len: 0,
            offsets,
            count: 0,
            hash_keys: vec![EMPTY; hash_cap as usize],
            hash_indices: vec![0u32; hash_cap as usize],
            hash_cap,
        }
    }

    /// intern.zig:94 `intern` — return the index of `s`, inserting if new.
    /// Probe: `h & (cap-1)`, +1 linear, wrapping; a matching hash verifies
    /// content before returning (FNV-1a collisions are real, e.g.
    /// "costarring"/"liquid").
    pub fn intern(&mut self, s: &[u8]) -> u32 {
        // Keep load factor <= 1/2 so probes terminate; rehash doubles the
        // table and re-seats every live hash (handles unchanged).
        if (self.count + 1) * 2 > self.hash_cap {
            self.grow_hash();
        }
        let h = hash_bytes(s);
        let mut slot = h & (self.hash_cap - 1);
        loop {
            let key = self.hash_keys[slot as usize];
            if key == EMPTY {
                return self.insert_new(s, h, slot);
            }
            if key == h {
                let idx = self.hash_indices[slot as usize];
                let start = self.offsets[idx as usize] as usize;
                let end = self.offsets[idx as usize + 1] as usize;
                if &self.data[start..end] == s {
                    return idx;
                }
            }
            slot = (slot + 1) & (self.hash_cap - 1);
        }
    }

    /// Double the hash table and re-seat all live entries. Handles (indices
    /// into `offsets`) are untouched.
    fn grow_hash(&mut self) {
        let new_cap = self.hash_cap * 2;
        let mut keys = vec![EMPTY; new_cap as usize];
        let mut indices = vec![0u32; new_cap as usize];
        for i in 0..self.hash_cap as usize {
            let key = self.hash_keys[i];
            if key == EMPTY {
                continue;
            }
            let mut slot = key & (new_cap - 1);
            while keys[slot as usize] != EMPTY {
                slot = (slot + 1) & (new_cap - 1);
            }
            keys[slot as usize] = key;
            indices[slot as usize] = self.hash_indices[i];
        }
        self.hash_keys = keys;
        self.hash_indices = indices;
        self.hash_cap = new_cap;
    }

    /// intern.zig:120 `insertNew` — buffers grow on demand.
    fn insert_new(&mut self, s: &[u8], h: u32, slot: u32) -> u32 {
        let idx = self.count;
        if (idx as usize) >= self.offsets.len() - 1 {
            let new_len = (self.offsets.len() - 1) * 2 + 1;
            self.offsets.resize(new_len, 0);
        }
        let start = self.data_len as usize;
        let end = start + s.len();
        if end > self.data.len() {
            self.data.resize(end.max(self.data.len() * 2), 0);
        }
        assert!(
            end <= self.data.len(),
            "StringIntern data capacity exceeded ({} + {} > {})",
            start,
            s.len(),
            self.data.len()
        );
        self.data[start..end].copy_from_slice(s);
        self.data_len += s.len() as u32;
        self.count += 1;
        self.offsets[self.count as usize] = self.data_len;
        self.hash_keys[slot as usize] = h;
        self.hash_indices[slot as usize] = idx;
        idx
    }

    /// intern.zig:141 `get` — bytes of string `idx`. Out-of-range `idx` is a
    /// programmer bug (Zig reads garbage offsets).
    pub fn get(&self, idx: u32) -> &[u8] {
        assert!(
            idx < self.count,
            "StringIntern.get({idx}) beyond count {}",
            self.count
        );
        let start = self.offsets[idx as usize] as usize;
        let end = self.offsets[idx as usize + 1] as usize;
        &self.data[start..end]
    }

    /// intern.zig:148 `getDataPtr` / :158 `getDataLen` — the concatenated
    /// UTF-8 buffer for Arrow export (live prefix only).
    pub fn data_bytes(&self) -> &[u8] {
        &self.data[..self.data_len as usize]
    }

    /// intern.zig:153 `getOffsetsPtr` — the offsets array for Arrow export;
    /// `count()+1` live entries, `offsets[count] == data_len`.
    pub fn offsets(&self) -> &[u32] {
        &self.offsets[..=self.count as usize]
    }

    /// intern.zig:163 `getCount`.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// intern.zig:158 `getDataLen`.
    pub fn data_len(&self) -> u32 {
        self.data_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FNV-1a 32 reference vectors: offset basis for "", and the classic
    /// published collision pair "costarring"/"liquid" (verified equal here,
    /// which is exactly what forces the content-verify branch).
    #[test]
    fn fnv1a_reference_vectors() {
        assert_eq!(hash_bytes(b""), 2_166_136_261);
        // "a": 2166136261 ^ 0x61 = 2166136192*… computed: 0xe40c292c
        assert_eq!(hash_bytes(b"a"), 0xe40c_292c);
        assert_eq!(hash_bytes(b"costarring"), hash_bytes(b"liquid"));
        assert_ne!(b"costarring".as_slice(), b"liquid".as_slice());
    }

    #[test]
    fn intern_dedups_and_assigns_sequential_indices() {
        let mut si = StringIntern::new(16);
        assert_eq!(si.intern(b"hello"), 0);
        assert_eq!(si.intern(b"world"), 1);
        assert_eq!(si.intern(b"hello"), 0); // dedup
        assert_eq!(si.intern(b""), 2); // empty string is a distinct entry
        assert_eq!(si.intern(b""), 2);
        assert_eq!(si.count(), 3);
        assert_eq!(si.get(0), b"hello");
        assert_eq!(si.get(1), b"world");
        assert_eq!(si.get(2), b"");
    }

    /// The Arrow-export layout: `data` is the concatenation in first-seen
    /// order; `offsets` has count+1 entries with offsets[count] == data_len
    /// (intern.zig:10-15).
    #[test]
    fn arrow_export_layout_is_pinned() {
        let mut si = StringIntern::new(16);
        si.intern(b"ab");
        si.intern(b"cde");
        si.intern(b"ab");
        si.intern(b"f");
        assert_eq!(si.data_bytes(), b"abcdef");
        assert_eq!(si.offsets(), &[0, 2, 5, 6]);
        assert_eq!(si.data_len(), 6);
    }

    /// Colliding hashes still intern to distinct indices via the
    /// content-verify branch (intern.zig:106-115).
    #[test]
    fn fnv_collision_pair_gets_distinct_indices() {
        let mut si = StringIntern::new(16);
        let a = si.intern(b"costarring");
        let b = si.intern(b"liquid");
        assert_ne!(a, b);
        assert_eq!(si.intern(b"costarring"), a);
        assert_eq!(si.intern(b"liquid"), b);
        assert_eq!(si.get(a), b"costarring");
        assert_eq!(si.get(b), b"liquid");
    }

    /// Post-parity fix pin: capacity is a starting size, not a ceiling —
    /// buffers and the hash table grow on demand (the deleted Zig overran
    /// its bump heap / probed forever past capacity). Handles stay stable
    /// and dedup keeps working across every growth boundary.
    #[test]
    fn growth_past_initial_capacity_keeps_handles_and_dedup() {
        let mut si = StringIntern::new(16);
        let mut handles = Vec::new();
        for i in 0..1000u32 {
            handles.push(
                si.intern(format!("string-number-{i}-padded-to-force-data-growth").as_bytes()),
            );
        }
        for (i, &h) in handles.iter().enumerate() {
            assert_eq!(
                si.get(h),
                format!("string-number-{i}-padded-to-force-data-growth").as_bytes()
            );
            assert_eq!(
                si.intern(format!("string-number-{i}-padded-to-force-data-growth").as_bytes()),
                h
            );
        }
    }
}
