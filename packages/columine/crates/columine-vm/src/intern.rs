//! Rust port of `packages/columine/src/vm/intern.zig` — string interning to
//! u32 indices (fast u32 key comparisons, Arrow dictionary-encoding
//! compatibility, cross-batch dedup).
//!
//! Layout mirrors the Zig exactly (intern.zig:10): concatenated UTF-8 `data`
//! with `offsets[i]..offsets[i+1]` string boundaries (`offsets[count]` always
//! equals `data_len`), plus an open-addressing hash table (FNV-1a u32 hash,
//! linear probing, no tombstones — interning never removes).
//!
// ZIG-PARITY: intern buffers are sized once at init and never grow (Zig bump-heap model, no overflow checks); intended fix: grow or return Err instead of the checked panic.
// Sizing: `data` = cap×32 ("assume ~32 bytes per string"), `offsets` = cap+1,
// hash = nextPowerOf2(cap×2). The Zig has NO overflow checks (overruns the
// heap / probes forever — ReleaseSmall UB); here exceeding capacity is a
// checked panic.
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

/// intern.zig:19 `StringIntern`. Buffers are `Vec`s sized once at
/// construction and never reallocated, matching the Zig bump-heap model.
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
        let h = hash_bytes(s);
        let mut slot = h & (self.hash_cap - 1);
        let mut probed: u32 = 0;
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
            probed += 1;
            // Zig probes forever on a full table (ReleaseSmall UB/hang);
            // a full intern table is a programmer bug here.
            assert!(
                probed <= self.hash_cap,
                "StringIntern hash table full (cap {}) — size the intern capacity for the schema",
                self.hash_cap
            );
        }
    }

    /// intern.zig:120 `insertNew`.
    fn insert_new(&mut self, s: &[u8], h: u32, slot: u32) -> u32 {
        let idx = self.count;
        // Zig overruns the bump heap past capacity (no check) — programmer
        // bug here, checked.
        assert!(
            (idx as usize) < self.offsets.len() - 1,
            "StringIntern offsets capacity exceeded ({} strings)",
            idx
        );
        let start = self.data_len as usize;
        let end = start + s.len();
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

    /// Documented divergence: the Zig overruns its bump heap / probes
    /// forever past capacity (ReleaseSmall UB); here it's a checked
    /// programmer-bug panic.
    #[test]
    #[should_panic(expected = "offsets capacity exceeded")]
    fn exceeding_string_capacity_panics_instead_of_zig_ub() {
        // initial_cap 16 → offsets_cap 17 → at most 16 strings.
        let mut si = StringIntern::new(16);
        for i in 0..17u32 {
            si.intern(format!("s{i}").as_bytes());
        }
    }
}
