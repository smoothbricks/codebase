//! Contiguous UTF-8 byte arena backing every dynamic string a row store holds.
//!
//! A row cell is [`ArenaStr`] — `(u32 offset, u32 len)` — not a refcounted
//! pointer. That buys three things the `Arc<str>` representation could not:
//!
//! - **No per-unique-string allocation.** Appending to the arena is amortized
//!   O(1); `Arc::from(text)` was one `malloc` per distinct value.
//! - **No refcount traffic.** A cell is two integers: nothing to clone on write,
//!   nothing to drop on flush. Reference counting is garbage collection, and a
//!   row store that buffers ten thousand rows should not run a collector.
//! - **Hashing costs nothing to attempt.** Dedup hashes the incoming `&str`'s
//!   bytes and compares candidates against arena slices, so a lookup allocates
//!   nothing. The linear scan this replaces existed *only* because a
//!   `HashMap<Arc<str>, _>` needs an owned key to look one up — a consequence of
//!   the storage choice, which is now gone.
//!
//! # Offsets are indices, not pointers
//!
//! [`StringArena::bytes`] is a single contiguous `String` and **it reallocates
//! on growth**. That is safe by construction: an [`ArenaStr`] is an
//! arena-relative index pair, so it names the same bytes before and after a
//! move. No slice pointer is ever cached — [`StringArena::resolve`] borrows the
//! arena at the call site, and the borrow checker forbids holding the resulting
//! `&str` across an `&mut` append. The stability argument is a type argument,
//! not a discipline one.
//!
//! A chunked arena (`Vec<Box<[u8]>>` with a virtual offset) would avoid the
//! realloc copy and loses anyway: non-contiguous bytes are not an Arrow Utf8
//! values buffer, so flush would have to concatenate the chunks — reintroducing
//! exactly the copy this type exists to delete — and every resolve would pay an
//! extra indirection. It trades an amortized cost paid O(log n) times per buffer
//! lifetime for a cost paid on every flush.
//!
//! # Ordinals are stable
//!
//! [`StringArena::intern`] returns a 1-based ordinal that is monotonic and never
//! reused. The arena is append-only and is never compacted, so an ordinal handed
//! out at any point still names the same bytes for the store's whole life.
//! Consumers cache on that (`lmao-wasm`'s JS-side intern memo does), and
//! reclaiming arena bytes is therefore a whole-store reset, never a renumbering.

use std::borrow::Cow;

/// A run of UTF-8 owned by one [`StringArena`]. `Copy`, eight bytes, no `Drop`.
///
/// Because the arena deduplicates, `offset` alone identifies the value: two
/// cells hold equal bytes exactly when they hold equal offsets. Flush uses that
/// to key its dictionary on an integer instead of on a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArenaStr {
    offset: u32,
    len: u32,
    /// Which arena issued this handle. Debug builds only, so a release cell
    /// stays eight bytes and the check costs nothing in production.
    #[cfg(debug_assertions)]
    arena: u32,
}

impl ArenaStr {
    #[inline]
    pub const fn offset(self) -> u32 {
        self.offset
    }
    #[inline]
    pub const fn len(self) -> u32 {
        self.len
    }
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// Source of debug-build arena identities. Not compiled into release, so the
/// per-arena construction cost — one relaxed increment — never lands on the
/// span-buffer creation path.
#[cfg(debug_assertions)]
static NEXT_ARENA_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

/// The arena had no room for a value. An operational failure, reported as a
/// value: the caller decides between refusing the write and flushing early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaFull {
    /// Bytes already held.
    pub held: usize,
    /// Bytes the rejected value needed.
    pub requested: usize,
    /// Ceiling this arena was built with.
    pub budget: usize,
}

impl std::fmt::Display for ArenaFull {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "string arena holds {} of {} bytes and cannot take {} more",
            self.held, self.budget, self.requested
        )
    }
}

impl std::error::Error for ArenaFull {}

/// Slots in the open-addressed index; `0` means empty, so ordinals are 1-based.
const EMPTY: u32 = 0;
/// First index allocation. Sized so a span that logs a handful of distinct
/// strings never rehashes, and so the untouched case owns no heap at all.
const INITIAL_INDEX_SLOTS: usize = 16;

/// FxHash's multiplier. The hasher is inlined rather than pulled in because
/// `lmao-core` ships with zero dependencies, and the whole function is nine
/// lines; `dict.rs` already established FxHash beats SipHash on this workload.
const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

#[inline]
const fn fx_mix(hash: u64, word: u64) -> u64 {
    (hash.rotate_left(5) ^ word).wrapping_mul(FX_SEED)
}

#[inline]
fn fx_hash(bytes: &[u8]) -> u64 {
    let mut hash = fx_mix(0, bytes.len() as u64);
    let (chunks, tail) = bytes.as_chunks::<8>();
    for chunk in chunks {
        hash = fx_mix(hash, u64::from_ne_bytes(*chunk));
    }
    if !tail.is_empty() {
        let mut word = [0u8; 8];
        word[..tail.len()].copy_from_slice(tail);
        hash = fx_mix(hash, u64::from_ne_bytes(word));
    }
    hash
}

/// Contiguous storage plus the dedup index for one row store's dynamic strings.
///
/// Lazily allocated: an arena that never interns owns no heap, so a span whose
/// names and messages are all static vocabulary costs nothing for having one.
#[derive(Debug)]
pub struct StringArena {
    /// Every distinct value, concatenated in first-insertion order. This is
    /// Arrow's Utf8 values buffer, byte for byte. `String` rather than
    /// `Vec<u8>` so `resolve` is two `is_char_boundary` checks instead of a
    /// linear UTF-8 validation.
    bytes: String,
    /// Ordinal `n` names `entries[n - 1]`. Append-only and never renumbered.
    /// Together with `bytes` this is Arrow's offsets buffer, already computed.
    entries: Vec<ArenaStr>,
    /// Open-addressed `hash -> ordinal`, power-of-two, linear probe. Empty
    /// until the first intern.
    index: Vec<u32>,
    /// Live slots in `index`, for the 7/8 load-factor test.
    occupied: usize,
    /// Ceiling on `bytes.len()`.
    budget: usize,
    /// This arena's debug-build identity, checked by [`Self::resolve`].
    #[cfg(debug_assertions)]
    id: u32,
}

impl StringArena {
    /// An arena bounded only by the `u32` offset space its handles can address.
    /// Use this where exhaustion is an invariant break rather than an
    /// operational one — a single span cannot reach four gigabytes of distinct
    /// strings without the allocator failing first.
    pub const OFFSET_SPACE: usize = u32::MAX as usize;

    #[must_use]
    pub fn new(budget: usize) -> Self {
        Self {
            bytes: String::new(),
            entries: Vec::new(),
            index: Vec::new(),
            occupied: 0,
            budget,
            #[cfg(debug_assertions)]
            id: NEXT_ARENA_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Bytes held. This is the arena's contribution to the store's footprint.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Distinct values held, which is also the highest ordinal issued.
    #[inline]
    #[must_use]
    pub fn distinct(&self) -> usize {
        self.entries.len()
    }

    /// Heap owned by the arena, for the buffer's memory accounting.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        self.bytes.capacity()
            + self.entries.capacity() * size_of::<ArenaStr>()
            + self.index.len() * size_of::<u32>()
    }

    /// The whole values buffer. Contiguous UTF-8 in ordinal order, which is what
    /// an Arrow `StringArray` wants for its values, with `entries` supplying the
    /// offsets that would otherwise be recomputed.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.bytes
    }

    /// Every handle in ordinal order. `entries()[n - 1]` is ordinal `n`.
    #[inline]
    #[must_use]
    pub fn entries(&self) -> &[ArenaStr] {
        &self.entries
    }

    /// The bytes `handle` names.
    ///
    /// # Panics
    /// Debug builds panic if `handle` came from a different arena; release
    /// builds do not check, so the handle is resolved against whatever lives at
    /// that offset. That asymmetry is deliberate and it is not the real
    /// defence: the real defence is that [`TextInput`] — the only way to put
    /// text into a row store — has no arena arm, so a caller cannot hand a
    /// store a handle another store issued. The debug check exists to catch the
    /// remaining way in, which is pulling a cell out of one store with
    /// [`crate::columns::StrColumn::cell`] and resolving it against another.
    ///
    /// Also panics if the range is not in bounds or not on a character
    /// boundary, which is how an out-of-range foreign handle already failed.
    #[inline]
    #[must_use]
    pub fn resolve(&self, handle: ArenaStr) -> &str {
        #[cfg(debug_assertions)]
        assert_eq!(
            handle.arena, self.id,
            "arena handle resolved against a different arena"
        );
        let start = handle.offset as usize;
        &self.bytes[start..start + handle.len as usize]
    }

    /// The handle for a previously issued ordinal, or `None` if never issued.
    #[inline]
    #[must_use]
    pub fn handle(&self, ordinal: u32) -> Option<ArenaStr> {
        let index = usize::try_from(ordinal.checked_sub(1)?).ok()?;
        self.entries.get(index).copied()
    }

    /// The bytes a previously issued ordinal names.
    #[inline]
    #[must_use]
    pub fn get(&self, ordinal: u32) -> Option<&str> {
        Some(self.resolve(self.handle(ordinal)?))
    }

    /// Intern `value`, returning its stable 1-based ordinal.
    ///
    /// A repeat is a hash of the incoming bytes plus one slice comparison and
    /// allocates nothing. A novel value appends `value.len()` bytes.
    pub fn intern(&mut self, value: &str) -> Result<u32, ArenaFull> {
        let hash = fx_hash(value.as_bytes());
        if self.index.is_empty() {
            self.grow_index(INITIAL_INDEX_SLOTS);
        }
        match self.probe(hash, value) {
            Ok(ordinal) => Ok(ordinal),
            Err(slot) => self.insert(slot, value),
        }
    }

    /// Intern `value`, returning the handle a row cell stores.
    #[inline]
    pub fn intern_str(&mut self, value: &str) -> Result<ArenaStr, ArenaFull> {
        let ordinal = self.intern(value)?;
        Ok(self.entries[ordinal as usize - 1])
    }

    /// `Ok(ordinal)` for a hit, `Err(slot)` naming the free slot a miss belongs
    /// in. The index is never full when this is called — [`Self::insert`] keeps
    /// the load factor under 7/8 — so the probe always terminates.
    fn probe(&self, hash: u64, value: &str) -> Result<u32, usize> {
        let mask = self.index.len() - 1;
        let mut slot = (hash as usize) & mask;
        loop {
            let ordinal = self.index[slot];
            if ordinal == EMPTY {
                return Err(slot);
            }
            if self.resolve(self.entries[ordinal as usize - 1]) == value {
                return Ok(ordinal);
            }
            slot = (slot + 1) & mask;
        }
    }

    fn insert(&mut self, slot: usize, value: &str) -> Result<u32, ArenaFull> {
        let held = self.bytes.len();
        let requested = value.len();
        // Both the configured budget and the offset width are ceilings; the
        // tighter one refuses. `held + requested` cannot overflow because both
        // are already bounded by an in-memory allocation.
        let ceiling = self.budget.min(Self::OFFSET_SPACE);
        if held + requested > ceiling {
            return Err(ArenaFull {
                held,
                requested,
                budget: ceiling,
            });
        }
        let handle = ArenaStr {
            offset: held as u32,
            len: requested as u32,
            #[cfg(debug_assertions)]
            arena: self.id,
        };
        self.bytes.push_str(value);
        self.entries.push(handle);
        let ordinal = self.entries.len() as u32;
        self.index[slot] = ordinal;
        self.occupied += 1;
        // 7/8 load factor: linear probing degrades sharply past it, and the
        // table holds u32 slots so doubling is cheap relative to the bytes.
        if self.occupied * 8 >= self.index.len() * 7 {
            self.grow_index(self.index.len() * 2);
        }
        Ok(ordinal)
    }

    fn grow_index(&mut self, slots: usize) {
        debug_assert!(slots.is_power_of_two());
        let mut index = vec![EMPTY; slots];
        let mask = slots - 1;
        for (position, handle) in self.entries.iter().enumerate() {
            let ordinal = position as u32 + 1;
            let start = handle.offset as usize;
            let hash = fx_hash(&self.bytes.as_bytes()[start..start + handle.len as usize]);
            let mut slot = (hash as usize) & mask;
            while index[slot] != EMPTY {
                slot = (slot + 1) & mask;
            }
            index[slot] = ordinal;
        }
        self.index = index;
    }
}

/// A scope attribute's text value, which is shared between spans by refcount and
/// therefore cannot hold an arena handle: the child resolves against a different
/// arena than the parent interned into. `Borrowed` keeps `'static` vocabulary
/// free; `Owned` pays one allocation on the cold scope-merge path, which already
/// allocates. Rows intern this into their own arena at materialization.
pub type ScopeText = Cow<'static, str>;

/// Text on its way INTO a row store, before the store decides how to hold it.
///
/// This is deliberately a different type from the cell it becomes
/// ([`crate::columns::SharedStr`]), and the difference is load-bearing: an input
/// has no arena arm, so a caller **cannot** hand one store a handle another
/// store issued. The wrong-arena read is not guarded against at runtime; it is
/// unrepresentable.
///
/// It also names the cost at the call site. `Static` is a compile-time value —
/// a log template, a vocabulary name — and never touches the arena, so it costs
/// nothing at all. `Dynamic` is deduplicated: a repeat is a hash and a slice
/// compare, and only a value the store has never seen appends bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextInput<'a> {
    Static(&'static str),
    Dynamic(&'a str),
}

impl TextInput<'_> {
    #[inline]
    #[must_use]
    pub const fn as_str(&self) -> &str {
        match self {
            Self::Static(value) => value,
            Self::Dynamic(value) => value,
        }
    }
}

impl From<&'static str> for TextInput<'static> {
    #[inline]
    fn from(value: &'static str) -> Self {
        Self::Static(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeat_interns_are_the_same_ordinal_and_append_no_bytes() {
        let mut arena = StringArena::new(StringArena::OFFSET_SPACE);
        let first = arena.intern("check-availability").unwrap();
        let held = arena.len();
        for _ in 0..1000 {
            assert_eq!(arena.intern("check-availability"), Ok(first));
        }
        assert_eq!(arena.len(), held, "a repeat must not append");
        assert_eq!(arena.distinct(), 1);
    }

    #[test]
    fn handles_survive_reallocation_of_the_byte_buffer() {
        let mut arena = StringArena::new(StringArena::OFFSET_SPACE);
        let first = arena.intern_str("first").unwrap();
        // Force many reallocations of `bytes`; `first` must still name "first".
        for i in 0..4096 {
            arena.intern(&format!("filler-{i:08}")).unwrap();
        }
        assert!(arena.len() > 4096, "the arena must actually have grown");
        assert_eq!(arena.resolve(first), "first");
        assert_eq!(arena.get(1), Some("first"));
    }

    #[test]
    fn ordinals_stay_stable_across_index_growth() {
        let mut arena = StringArena::new(StringArena::OFFSET_SPACE);
        let ordinals: Vec<u32> = (0..512)
            .map(|i| arena.intern(&format!("value-{i}")).unwrap())
            .collect();
        for (position, ordinal) in ordinals.iter().enumerate() {
            assert_eq!(*ordinal, position as u32 + 1);
            assert_eq!(
                arena.get(*ordinal),
                Some(format!("value-{position}").as_str())
            );
        }
        for (position, _) in ordinals.iter().enumerate() {
            assert_eq!(
                arena.intern(&format!("value-{position}")),
                Ok(position as u32 + 1),
                "re-interning after growth must find the original ordinal"
            );
        }
    }

    #[test]
    fn budget_exhaustion_is_a_value_not_a_panic() {
        let mut arena = StringArena::new(8);
        assert_eq!(arena.intern("12345678"), Ok(1));
        assert_eq!(
            arena.intern("9"),
            Err(ArenaFull {
                held: 8,
                requested: 1,
                budget: 8
            })
        );
        // A refused value must leave the arena exactly as it was, so the
        // already-interned ordinals a caller holds keep resolving.
        assert_eq!(arena.get(1), Some("12345678"));
        assert_eq!(arena.distinct(), 1);
        assert_eq!(
            arena.intern("12345678"),
            Ok(1),
            "dedup still works at budget"
        );
    }

    #[test]
    fn empty_and_multibyte_values_round_trip() {
        let mut arena = StringArena::new(StringArena::OFFSET_SPACE);
        let empty = arena.intern_str("").unwrap();
        let snowman = arena.intern_str("☃ вечер 🌒").unwrap();
        let also_empty = arena.intern_str("").unwrap();
        assert_eq!(empty, also_empty, "the empty string dedupes like any other");
        assert_eq!(arena.resolve(empty), "");
        assert_eq!(arena.resolve(snowman), "☃ вечер 🌒");
        assert_eq!(arena.distinct(), 2);
    }

    #[test]
    fn untouched_arena_owns_no_heap() {
        let arena = StringArena::new(StringArena::OFFSET_SPACE);
        assert_eq!(arena.allocated_bytes(), 0);
        assert_eq!(arena.entries(), &[]);
        assert_eq!(arena.as_str(), "");
    }

    #[test]
    fn entries_are_the_arrow_offsets_buffer() {
        let mut arena = StringArena::new(StringArena::OFFSET_SPACE);
        for value in ["alpha", "beta", "gamma"] {
            arena.intern(value).unwrap();
        }
        // Contiguous, monotonic, gap-free: exactly Arrow's Utf8 layout.
        assert_eq!(arena.as_str(), "alphabetagamma");
        let offsets: Vec<u32> = arena.entries().iter().map(|e| e.offset()).collect();
        assert_eq!(offsets, vec![0, 5, 9]);
        let ends: Vec<u32> = arena
            .entries()
            .iter()
            .map(|e| e.offset() + e.len())
            .collect();
        assert_eq!(ends, vec![5, 9, 14]);
    }
}
