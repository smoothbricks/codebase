//! Two-profile maplet: mutable O(1) cache-line-bucketed [`Maplet`] and sealed
//! zero-copy Format 2 [`MapletView`].
//!
//! - **Live mutable profile ([`Maplet`])**: SplinterDB-style 64-byte
//!   cache-line-aligned buckets holding 16 packed `u32` slots
//!   `((fp16 as u32) << 16) | (val as u32 + 1)`. On `aarch64` little-endian,
//!   16-slot matching runs in NEON (`vuzp2q_u16` / `vceqq_u16`); other targets
//!   use a portable scalar matcher. Per-bucket overflow bits, a 1-step linear
//!   probe, and a spill log guarantee zero false negatives for every `(u32, u16)`
//!   pair including `u16::MAX` and arbitrary hash collisions. Lookups allocate
//!   nothing and visit matches newest-first.
//! - **Sealed compact profile ([`MapletView`])**: Canonical zero-copy wire image
//!   built directly on bitmosaic Format 2 ([`V2View`]: `Bitmap`, `Single`, `List`,
//!   `Ef2View`, or `Directory`) paired with a tight packed `u8`/`u16` value plane
//!   indexed by [`V2View::rank_if_present`]. Supports both unique-key and
//!   multi-value maps, full admission via [`MapletView::open`], O(1) trusted
//!   attachment via [`MapletView::open_admitted`], and lossless hydration back to
//!   [`Maplet`] via [`MapletView::promote`].

use core::ops::ControlFlow;

use crate::admission::{AdmissionClass, AdmissionError};
use crate::codec::{
    read_u16_at, read_uint_le, read_uleb_checked, uleb_len, width_of, write_uint_le, write_uleb,
};
use crate::v2::{V2View, image_len_v2, write_image_into_v2};

// ── Sealed wire format constants ─────────────────────────────────────────

const MAPLET_MAGIC: u8 = 0x4D; // 'M'
const MAPLET_VERSION_MASK: u8 = 0xF0;
const MAPLET_VERSION_1: u8 = 0x10;
const FLAG_VAL_U16: u8 = 0x01;
const FLAG_MULTI_VALUE: u8 = 0x02;
const FLAG_EMPTY: u8 = 0x04;
const FLAG_RESERVED: u8 = 0x08;

/// Canonical 2-byte sealed image for an empty maplet.
pub const EMPTY_MAPLET_IMAGE: [u8; 2] = [MAPLET_MAGIC, MAPLET_VERSION_1 | FLAG_EMPTY];

// ── Live Maplet bucket parameters ────────────────────────────────────────

/// Number of packed `u32` slots per 64-byte cache-line bucket.
const BUCKET_SLOTS: usize = 16;

/// Target maximum entries per 16-slot bucket before doubling (9 / 16 = 56.25%).
const MAX_LOAD_PER_BUCKET: usize = 9;

/// Initial power-of-two bucket count once the maplet becomes non-empty
/// (`16 * 64 B = 1 KiB`, holding up to 144 entries before its first resize).
const MIN_BUCKETS: usize = 16;

/// Largest value representable in a packed 16-bit `val + 1` slot (`0` = empty).
/// `u16::MAX` routes through `spill` with zero false negatives.
const MAX_INDEXED_VAL: u16 = u16::MAX - 1;

/// Bijective 32-bit key mixer spreading sequential, clustered, and hashed keys
/// uniformly across both power-of-two bucket indices and 15-bit fingerprints.
#[inline(always)]
const fn mix32(mut x: u32) -> u32 {
    x = x.wrapping_add(0x9e37_79b9);
    x ^= x >> 16;
    x = x.wrapping_mul(0x7feb_352d);
    x ^= x >> 15;
    x = x.wrapping_mul(0x846c_a68b);
    x ^= x >> 16;
    x
}

/// Non-zero 15-bit-entropy fingerprint from the upper half of `mixed`.
#[inline(always)]
const fn maplet_fp(mixed: u32) -> u16 {
    ((mixed >> 16) as u16) | 1
}

/// Pack `(fp16, val)` into one non-zero `u32` slot (`(fp16 << 16) | (val + 1)`).
#[inline(always)]
const fn pack_slot(fp16: u16, val: u16) -> u32 {
    ((fp16 as u32) << 16) | (val as u32 + 1)
}

/// Unpack `val` from a non-zero slot.
#[inline(always)]
const fn slot_val(slot: u32) -> u16 {
    ((slot & 0xFFFF) - 1) as u16
}

/// One 64-byte cache-line-aligned bucket holding 16 `(fp16, val + 1)` slots.
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
struct Bucket {
    slots: [u32; BUCKET_SLOTS],
}

impl Bucket {
    const EMPTY: Self = Self {
        slots: [0u32; BUCKET_SLOTS],
    };
}

/// Exact backing entry for O(N) resize and canonical sealing. Never touched on
/// the common lookup hot path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MapletEntry {
    key: u32,
    val: u16,
}

/// Portable 16-slot fingerprint matcher returning a 16-bit mask where bit `i`
/// is set iff `(slots[i] >> 16) == fp16` (for non-zero `fp16`).
#[cfg(any(test, not(all(target_arch = "aarch64", target_endian = "little"))))]
#[inline(always)]
#[must_use]
fn match_bucket_scalar(slots: &[u32; BUCKET_SLOTS], fp16: u16) -> u16 {
    let target = u32::from(fp16);
    let mut mask = 0u16;
    for (i, &slot) in slots.iter().enumerate() {
        if (slot >> 16) == target {
            mask |= 1u16 << i;
        }
    }
    mask
}

/// `aarch64` NEON 16-slot fingerprint matcher: loads the 64-byte bucket as four
/// 128-bit vectors, deinterleaves the 16 high `u16` fingerprint lanes with two
/// `UZP2`, compares all 16 against `fp16` with two `CMEQ`, and reduces to a
/// 16-bit bitmask via weighted `AND`/`ORR`/`ADDV`.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[inline(always)]
#[must_use]
fn match_bucket_neon(slots: &[u32; BUCKET_SLOTS], fp16: u16) -> u16 {
    use core::arch::aarch64::{
        uint16x8_t, vaddvq_u16, vandq_u16, vceqq_u16, vdupq_n_u16, vld1q_u16, vorrq_u16, vuzp2q_u16,
    };

    const WEIGHTS_LO: [u16; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
    const WEIGHTS_HI: [u16; 8] = [256, 512, 1024, 2048, 4096, 8192, 16384, 32768];

    // SAFETY: `slots` is `[u32; 16]` (64 bytes = 32 `u16` lanes), 64-byte aligned
    // inside `Bucket` (and >= 4-byte aligned in general). On little-endian `aarch64`,
    // the odd `u16` lanes of `[u32; 16]` are the upper 16 bits (`fp16`) of each slot.
    unsafe {
        let base = slots.as_ptr().cast::<u16>();
        let v0: uint16x8_t = vld1q_u16(base);
        let v1: uint16x8_t = vld1q_u16(base.add(8));
        let v2: uint16x8_t = vld1q_u16(base.add(16));
        let v3: uint16x8_t = vld1q_u16(base.add(24));

        let fp_lo = vuzp2q_u16(v0, v1);
        let fp_hi = vuzp2q_u16(v2, v3);

        let target = vdupq_n_u16(fp16);
        let eq_lo = vceqq_u16(fp_lo, target);
        let eq_hi = vceqq_u16(fp_hi, target);

        let w_lo = vld1q_u16(WEIGHTS_LO.as_ptr());
        let w_hi = vld1q_u16(WEIGHTS_HI.as_ptr());
        let bits = vorrq_u16(vandq_u16(eq_lo, w_lo), vandq_u16(eq_hi, w_hi));
        vaddvq_u16(bits)
    }
}

/// Match all 16 slots of a bucket against non-zero `fp16`.
#[inline(always)]
#[must_use]
fn match_bucket(slots: &[u32; BUCKET_SLOTS], fp16: u16) -> u16 {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        match_bucket_neon(slots, fp16)
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    {
        match_bucket_scalar(slots, fp16)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotPlace {
    Duplicate,
    Inserted,
    Full,
}

#[inline(always)]
fn try_place_in_bucket(bucket: &mut Bucket, slot: u32) -> SlotPlace {
    for cell in &mut bucket.slots {
        if *cell == slot {
            return SlotPlace::Duplicate;
        }
        if *cell == 0 {
            *cell = slot;
            return SlotPlace::Inserted;
        }
    }
    SlotPlace::Full
}

/// Outcome of [`Maplet::insert_bounded`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapletInsert {
    /// Entry was placed into bucket `b0` or its linear-probe neighbour `b1`.
    Inserted,
    /// Exact `(fp16, value)` slot already existed in `b0` or `b1`.
    Duplicate,
    /// Home bucket `b0` is saturated (both `b0` and `b1` full, or
    /// `value > MAX_INDEXED_VAL`); no spill entry was recorded and bounded
    /// lookups for keys hashing to `b0` return [`MapletMatch::Saturated`].
    Saturated,
}

/// Outcome of [`Maplet::for_each_match_bounded`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapletMatch<R = ()> {
    /// Every candidate value for `key` was visited without early break.
    Complete,
    /// Visitor returned `ControlFlow::Break(value)`.
    Break(R),
    /// The key's home bucket is saturated; caller must fall back to a
    /// conservative full scan (zero false negatives).
    Saturated,
}

// ── Mutable Live Profile: Maplet ─────────────────────────────────────────

/// Live mutable O(1) cache-line-bucketed multi-value routing maplet.
///
/// Maps `u32` keys (fingerprints or IDs) to one or more `u16` values. On the
/// common path, [`Self::for_each_match`] and [`Self::get`] touch a single
/// 64-byte cache line with zero heap allocation and visit values newest-first.
#[derive(Clone, Debug, Default)]
pub struct Maplet {
    /// Power-of-two 64-byte cache-line buckets (empty while `entries` is empty).
    buckets: Vec<Bucket>,
    /// `buckets.len() - 1` when `buckets` is non-empty, else `0`.
    mask: usize,
    /// 1-bit-per-bucket overflow bitmap: bit `b` is set iff a slot belonging
    /// to bucket `b` spilled into `(b + 1) & mask`.
    overflow: Vec<u64>,
    /// 1-bit-per-bucket spill bitmap: bit `b` is set iff an entry belonging to
    /// home bucket `b` was placed into `spill` by [`Self::insert`]. Replaces
    /// the global `spill.is_empty()` lookup gate so a spill in one bucket never
    /// forces unrelated buckets off the single-cache-line hot path.
    spilled: Vec<u64>,
    /// 1-bit-per-bucket saturation bitmap: bit `b` is set iff a bounded insert
    /// for home bucket `b` found both `b` and `(b + 1) & mask` full (or saw an
    /// unindexable value). Bounded inserts for `b` short-circuit in O(1) with
    /// zero spill and zero growth, and bounded lookups for `b` return
    /// [`MapletMatch::Saturated`] without disturbing unsaturated buckets.
    saturated: Vec<u64>,
    /// 1-bit-per-bucket bitmap recording buckets where [`Self::insert_bounded`]
    /// observed a non-tail `(fp16, val)` slot duplicate without scanning
    /// `entries` (eliminating the O(N) `entries.iter().rev().any` scan). Before
    /// a resize the matching slot is already in `b0`/`b1` (zero false negatives,
    /// zero saturation); if `grow()` later splits the bucket, any set bit here
    /// promotes to `saturated` across the split strides so a colliding key is
    /// never dropped.
    dup_on_grow: Vec<u64>,
    /// Exact insertion-ordered log of unique `(key, val)` entries for O(N)
    /// dynamic resize and canonical sealing via [`Self::seal_into`].
    entries: Vec<MapletEntry>,
    /// Overflow spillover for `val == u16::MAX` or double-bucket collisions,
    /// guaranteeing zero false negatives under arbitrary workloads.
    spill: Vec<MapletEntry>,
}

impl Maplet {
    /// Construct an empty maplet with no initial allocation.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an empty maplet pre-sized for `expected_entries` without
    /// exceeding the 56.25% bucket load threshold.
    #[must_use]
    pub fn with_capacity(expected_entries: usize) -> Self {
        if expected_entries == 0 {
            return Self::new();
        }
        let target_buckets = expected_entries
            .div_ceil(MAX_LOAD_PER_BUCKET)
            .max(MIN_BUCKETS)
            .next_power_of_two();
        let bitmap_words = target_buckets.div_ceil(64);
        Self {
            buckets: vec![Bucket::EMPTY; target_buckets],
            mask: target_buckets - 1,
            overflow: vec![0u64; bitmap_words],
            spilled: vec![0u64; bitmap_words],
            saturated: vec![0u64; bitmap_words],
            dup_on_grow: vec![0u64; bitmap_words],
            entries: Vec::with_capacity(expected_entries),
            spill: Vec::new(),
        }
    }

    /// Number of unique `(key, value)` associations stored in the maplet.
    #[inline(always)]
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the maplet holds zero entries.
    #[inline(always)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `key`'s home bucket is currently marked saturated.
    #[inline(always)]
    #[must_use]
    pub fn is_key_saturated(&self, key: u32) -> bool {
        if self.buckets.is_empty() {
            return false;
        }
        let b0 = (mix32(key) as usize) & self.mask;
        self.bucket_saturated(b0)
    }

    /// Exact heap capacity retained by every backing vector of this maplet.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.buckets.capacity() * core::mem::size_of::<Bucket>()
            + self.overflow.capacity() * core::mem::size_of::<u64>()
            + self.spilled.capacity() * core::mem::size_of::<u64>()
            + self.saturated.capacity() * core::mem::size_of::<u64>()
            + self.dup_on_grow.capacity() * core::mem::size_of::<u64>()
            + self.entries.capacity() * core::mem::size_of::<MapletEntry>()
            + self.spill.capacity() * core::mem::size_of::<MapletEntry>()
    }

    /// Capacity-retaining reset: subsequent inserts reuse bucket allocations.
    pub fn clear(&mut self) {
        self.buckets.fill(Bucket::EMPTY);
        self.overflow.fill(0);
        self.spilled.fill(0);
        self.saturated.fill(0);
        self.dup_on_grow.fill(0);
        self.entries.clear();
        self.spill.clear();
    }

    /// Insert `(key, value)` into the maplet.
    ///
    /// Duplicate exact `(key, value)` pairs are deduplicated; distinct values
    /// for the same `key` are all preserved and returned newest-first.
    #[inline]
    pub fn insert(&mut self, key: u32, value: u16) {
        let entry = MapletEntry { key, val: value };
        if value > MAX_INDEXED_VAL {
            if self.spill.contains(&entry) {
                return;
            }
            if !self.buckets.is_empty() {
                let b0 = (mix32(key) as usize) & self.mask;
                self.set_bucket_spilled(b0);
            }
            self.entries.push(entry);
            self.spill.push(entry);
            return;
        }

        self.ensure_capacity_for_one(false);
        let mixed = mix32(key);
        let fp16 = maplet_fp(mixed);
        let slot = pack_slot(fp16, value);
        let b0 = (mixed as usize) & self.mask;

        if let Some(bucket0) = self.buckets.get_mut(b0) {
            match try_place_in_bucket(bucket0, slot) {
                SlotPlace::Inserted => {
                    self.entries.push(entry);
                    return;
                }
                SlotPlace::Duplicate => {
                    if self.entries.last() != Some(&entry)
                        && !self.entries.iter().rev().any(|e| *e == entry)
                    {
                        self.entries.push(entry);
                    }
                    return;
                }
                SlotPlace::Full => {}
            }
        }

        let b1 = (b0 + 1) & self.mask;
        if let Some(bucket1) = self.buckets.get_mut(b1) {
            match try_place_in_bucket(bucket1, slot) {
                SlotPlace::Inserted => {
                    self.set_bucket_overflow(b0);
                    self.entries.push(entry);
                    return;
                }
                SlotPlace::Duplicate => {
                    self.set_bucket_overflow(b0);
                    if self.entries.last() != Some(&entry)
                        && !self.entries.iter().rev().any(|e| *e == entry)
                    {
                        self.entries.push(entry);
                    }
                    return;
                }
                SlotPlace::Full => {}
            }
        }

        if !self.spill.contains(&entry) {
            self.set_bucket_spilled(b0);
            self.entries.push(entry);
            self.spill.push(entry);
        }
    }

    /// Bounded-work insert with per-bucket saturation and zero spill growth.
    ///
    /// Attempts to place `(key, value)` into home bucket `b0` or its neighbour
    /// `b1` (at most `2 * BUCKET_SLOTS = 32` slot checks). When `b0` is
    /// already saturated, both `b0` and `b1` are full, or `value == u16::MAX`,
    /// sets `saturated[b0]` and stores nothing in `spill` or `entries`.
    /// Subsequent [`Self::for_each_match_bounded`] probes for keys hashing to
    /// `b0` return [`MapletMatch::Saturated`] in O(1), while all unsaturated
    /// buckets stay on the single-cache-line hot path with zero spill scans.
    #[inline]
    pub fn insert_bounded(&mut self, key: u32, value: u16) -> MapletInsert {
        let mixed = mix32(key);
        if !self.buckets.is_empty() {
            let b0 = (mixed as usize) & self.mask;
            if self.bucket_saturated(b0) {
                return MapletInsert::Saturated;
            }
        }

        self.ensure_capacity_for_one(true);
        let b0 = (mixed as usize) & self.mask;
        if self.bucket_saturated(b0) {
            return MapletInsert::Saturated;
        }
        if value > MAX_INDEXED_VAL {
            self.set_bucket_saturated(b0);
            return MapletInsert::Saturated;
        }

        let entry = MapletEntry { key, val: value };
        let fp16 = maplet_fp(mixed);
        let slot = pack_slot(fp16, value);

        if let Some(bucket0) = self.buckets.get_mut(b0) {
            match try_place_in_bucket(bucket0, slot) {
                SlotPlace::Inserted => {
                    self.entries.push(entry);
                    return MapletInsert::Inserted;
                }
                SlotPlace::Duplicate => {
                    if self.entries.last() != Some(&entry) {
                        self.set_bucket_dup_on_grow(b0);
                    }
                    return MapletInsert::Duplicate;
                }
                SlotPlace::Full => {}
            }
        }

        let b1 = (b0 + 1) & self.mask;
        if let Some(bucket1) = self.buckets.get_mut(b1) {
            match try_place_in_bucket(bucket1, slot) {
                SlotPlace::Inserted => {
                    self.set_bucket_overflow(b0);
                    self.entries.push(entry);
                    return MapletInsert::Inserted;
                }
                SlotPlace::Duplicate => {
                    self.set_bucket_overflow(b0);
                    if self.entries.last() != Some(&entry) {
                        self.set_bucket_dup_on_grow(b0);
                    }
                    return MapletInsert::Duplicate;
                }
                SlotPlace::Full => {}
            }
        }

        self.set_bucket_saturated(b0);
        MapletInsert::Saturated
    }

    /// Return the newest candidate value matching `key`, or `None` if absent.
    ///
    /// Zero heap allocation; touches one 64-byte cache line on the common path.
    #[inline(always)]
    #[must_use]
    pub fn get(&self, key: u32) -> Option<u16> {
        match self.for_each_match(key, ControlFlow::Break) {
            ControlFlow::Break(v) => Some(v),
            ControlFlow::Continue(()) => None,
        }
    }

    /// Visit every candidate value matching `key` in newest-first order,
    /// returning [`MapletMatch::Saturated`] in O(1) when `key`'s home bucket
    /// was saturated by [`Self::insert_bounded`].
    ///
    /// Zero heap allocation; unsaturated non-overflowed buckets stay on the
    /// single 64-byte cache-line SIMD hot path regardless of how many other
    /// buckets are saturated.
    #[inline(always)]
    pub fn for_each_match_bounded<R>(
        &self,
        key: u32,
        mut visit: impl FnMut(u16) -> ControlFlow<R>,
    ) -> MapletMatch<R> {
        if self.buckets.is_empty() {
            return match self.visit_spill_only(key, &mut visit) {
                ControlFlow::Continue(()) => MapletMatch::Complete,
                ControlFlow::Break(done) => MapletMatch::Break(done),
            };
        }
        let mixed = mix32(key);
        let b0 = (mixed as usize) & self.mask;
        if self.bucket_saturated(b0) {
            return MapletMatch::Saturated;
        }
        let Some(bucket0) = self.buckets.get(b0) else {
            return MapletMatch::Complete;
        };
        let fp16 = maplet_fp(mixed);
        let mask0 = match_bucket(&bucket0.slots, fp16);

        if !self.bucket_overflowed(b0) && !self.bucket_spilled(b0) {
            let mut m = mask0;
            while m != 0 {
                let slot_idx = 15 - (m.leading_zeros() as usize);
                m &= !(1u16 << slot_idx);
                if let Some(&slot) = bucket0.slots.get(slot_idx)
                    && let ControlFlow::Break(done) = visit(slot_val(slot))
                {
                    return MapletMatch::Break(done);
                }
            }
            return MapletMatch::Complete;
        }

        match self.for_each_match_cold(key, b0, bucket0, mask0, fp16, visit) {
            ControlFlow::Continue(()) => MapletMatch::Complete,
            ControlFlow::Break(done) => MapletMatch::Break(done),
        }
    }

    /// Visit every candidate value matching `key` in newest-first order with
    /// zero heap allocation.
    ///
    /// Guaranteed zero false negatives: every `value` previously inserted for
    /// `key` is visited (unless `visit` returns `ControlFlow::Break`).
    #[inline(always)]
    pub fn for_each_match<R>(
        &self,
        key: u32,
        mut visit: impl FnMut(u16) -> ControlFlow<R>,
    ) -> ControlFlow<R> {
        if self.buckets.is_empty() {
            return self.visit_spill_only(key, &mut visit);
        }
        let mixed = mix32(key);
        let b0 = (mixed as usize) & self.mask;
        let Some(bucket0) = self.buckets.get(b0) else {
            return self.visit_spill_only(key, &mut visit);
        };
        let fp16 = maplet_fp(mixed);
        let mask0 = match_bucket(&bucket0.slots, fp16);

        if !self.bucket_overflowed(b0) && !self.bucket_spilled(b0) {
            let mut m = mask0;
            while m != 0 {
                let slot_idx = 15 - (m.leading_zeros() as usize);
                m &= !(1u16 << slot_idx);
                if let Some(&slot) = bucket0.slots.get(slot_idx)
                    && let ControlFlow::Break(done) = visit(slot_val(slot))
                {
                    return ControlFlow::Break(done);
                }
            }
            return ControlFlow::Continue(());
        }

        self.for_each_match_cold(key, b0, bucket0, mask0, fp16, visit)
    }

    #[cold]
    #[inline(never)]
    fn visit_spill_only<R>(
        &self,
        key: u32,
        visit: &mut impl FnMut(u16) -> ControlFlow<R>,
    ) -> ControlFlow<R> {
        for entry in self.spill.iter().rev() {
            if entry.key == key
                && let ControlFlow::Break(done) = visit(entry.val)
            {
                return ControlFlow::Break(done);
            }
        }
        ControlFlow::Continue(())
    }

    #[cold]
    #[inline(never)]
    fn for_each_match_cold<R>(
        &self,
        key: u32,
        b0: usize,
        bucket0: &Bucket,
        mut mask0: u16,
        fp16: u16,
        mut visit: impl FnMut(u16) -> ControlFlow<R>,
    ) -> ControlFlow<R> {
        if self.bucket_spilled(b0) {
            for entry in self.spill.iter().rev() {
                if entry.key == key
                    && let ControlFlow::Break(done) = visit(entry.val)
                {
                    return ControlFlow::Break(done);
                }
            }
        }
        if self.bucket_overflowed(b0) {
            let b1 = (b0 + 1) & self.mask;
            if let Some(bucket1) = self.buckets.get(b1) {
                let mut mask1 = match_bucket(&bucket1.slots, fp16);
                while mask1 != 0 {
                    let slot_idx = 15 - (mask1.leading_zeros() as usize);
                    mask1 &= !(1u16 << slot_idx);
                    if let Some(&slot) = bucket1.slots.get(slot_idx)
                        && let ControlFlow::Break(done) = visit(slot_val(slot))
                    {
                        return ControlFlow::Break(done);
                    }
                }
            }
        }
        while mask0 != 0 {
            let slot_idx = 15 - (mask0.leading_zeros() as usize);
            mask0 &= !(1u16 << slot_idx);
            if let Some(&slot) = bucket0.slots.get(slot_idx)
                && let ControlFlow::Break(done) = visit(slot_val(slot))
            {
                return ControlFlow::Break(done);
            }
        }
        ControlFlow::Continue(())
    }

    /// Seal this live maplet into canonical [`MapletView`] bytes appended to
    /// `out`, returning the number of bytes written.
    ///
    /// Automatically selects `u8` vs `u16` value width from the maximum stored
    /// value, unique vs multi-value layout from key multiplicity, and the
    /// minimal Format 2 key image (`Bitmap`, `Single`, `List`, `Ef2View`, or
    /// `Directory`) via [`write_image_into_v2`].
    pub fn seal_into(&self, out: &mut Vec<u8>) -> usize {
        if self.is_empty() {
            out.extend_from_slice(&EMPTY_MAPLET_IMAGE);
            return EMPTY_MAPLET_IMAGE.len();
        }

        // Tag each entry with its insertion sequence so equal keys sort
        // newest-first (descending `seq`) without using std::collections.
        let mut tagged: Vec<(u32, usize, u16)> = self
            .entries
            .iter()
            .enumerate()
            .map(|(seq, e)| (e.key, seq, e.val))
            .collect();
        // Sort ascending by key, descending by seq (newest-first within each key).
        tagged.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));

        let mut keys: Vec<u32> = Vec::with_capacity(tagged.len());
        let mut group_ends: Vec<u32> = Vec::new();
        let mut max_val = 0u16;
        let mut i = 0usize;
        while i < tagged.len() {
            let k = tagged[i].0;
            keys.push(k);
            let mut j = i;
            while j < tagged.len() && tagged[j].0 == k {
                if tagged[j].2 > max_val {
                    max_val = tagged[j].2;
                }
                j += 1;
            }
            if j < tagged.len() {
                group_ends.push(j as u32);
            }
            i = j;
        }

        let total_values = tagged.len();
        let key_count = keys.len();
        let multi_value = total_values > key_count;
        let val_u16 = max_val > u8::MAX as u16;

        let v2_len = image_len_v2(&keys);
        let val_width = if val_u16 { 2usize } else { 1usize };
        let off_width = if multi_value && key_count > 1 {
            width_of((total_values - 1) as u32)
        } else {
            0usize
        };
        let offsets_bytes = if multi_value {
            group_ends.len() * off_width
        } else {
            0usize
        };
        let values_bytes = total_values * val_width;

        let header_len = 2
            + uleb_len(v2_len as u32)
            + if multi_value {
                uleb_len(total_values as u32)
            } else {
                0
            };
        let total_bytes = header_len + v2_len + offsets_bytes + values_bytes;

        let start = out.len();
        out.resize(start + total_bytes, 0);
        let dst = &mut out[start..start + total_bytes];

        let mut flags = MAPLET_VERSION_1;
        if val_u16 {
            flags |= FLAG_VAL_U16;
        }
        if multi_value {
            flags |= FLAG_MULTI_VALUE;
        }
        dst[0] = MAPLET_MAGIC;
        dst[1] = flags;
        let mut at = 2usize;
        write_uleb(dst, &mut at, v2_len as u32);
        if multi_value {
            write_uleb(dst, &mut at, total_values as u32);
        }
        debug_assert_eq!(at, header_len);

        let written_v2 =
            write_image_into_v2(&keys, &mut dst[at..at + v2_len]).expect("exact Format 2 capacity");
        debug_assert_eq!(written_v2, v2_len);
        at += v2_len;

        if multi_value && off_width > 0 {
            for &end in &group_ends {
                write_uint_le(dst, at, end, off_width);
                at += off_width;
            }
        }

        if val_u16 {
            for &(_, _, v) in &tagged {
                dst[at..at + 2].copy_from_slice(&v.to_le_bytes());
                at += 2;
            }
        } else {
            for &(_, _, v) in &tagged {
                dst[at] = v as u8;
                at += 1;
            }
        }
        debug_assert_eq!(at, total_bytes);
        total_bytes
    }

    /// Seal this live maplet into a fresh canonical byte vector.
    #[must_use]
    pub fn seal(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.seal_into(&mut out);
        out
    }

    #[inline(always)]
    fn bucket_overflowed(&self, bucket_idx: usize) -> bool {
        self.overflow
            .get(bucket_idx >> 6)
            .is_some_and(|&word| (word & (1u64 << (bucket_idx & 63))) != 0)
    }

    #[inline(always)]
    fn set_bucket_overflow(&mut self, bucket_idx: usize) {
        if let Some(word) = self.overflow.get_mut(bucket_idx >> 6) {
            *word |= 1u64 << (bucket_idx & 63);
        }
    }

    #[inline(always)]
    fn bucket_spilled(&self, bucket_idx: usize) -> bool {
        self.spilled
            .get(bucket_idx >> 6)
            .is_some_and(|&word| (word & (1u64 << (bucket_idx & 63))) != 0)
    }

    #[inline(always)]
    fn set_bucket_spilled(&mut self, bucket_idx: usize) {
        if let Some(word) = self.spilled.get_mut(bucket_idx >> 6) {
            *word |= 1u64 << (bucket_idx & 63);
        }
    }

    #[inline(always)]
    fn bucket_saturated(&self, bucket_idx: usize) -> bool {
        self.saturated
            .get(bucket_idx >> 6)
            .is_some_and(|&word| (word & (1u64 << (bucket_idx & 63))) != 0)
    }

    #[inline(always)]
    fn set_bucket_saturated(&mut self, bucket_idx: usize) {
        if let Some(word) = self.saturated.get_mut(bucket_idx >> 6) {
            *word |= 1u64 << (bucket_idx & 63);
        }
    }

    #[inline(always)]
    fn bucket_dup_on_grow(&self, bucket_idx: usize) -> bool {
        self.dup_on_grow
            .get(bucket_idx >> 6)
            .is_some_and(|&word| (word & (1u64 << (bucket_idx & 63))) != 0)
    }

    #[inline(always)]
    fn set_bucket_dup_on_grow(&mut self, bucket_idx: usize) {
        if let Some(word) = self.dup_on_grow.get_mut(bucket_idx >> 6) {
            *word |= 1u64 << (bucket_idx & 63);
        }
    }

    fn ensure_capacity_for_one(&mut self, bounded: bool) {
        let needed = self.entries.len().saturating_add(1);
        let current_max = self.buckets.len().saturating_mul(MAX_LOAD_PER_BUCKET);
        if !self.buckets.is_empty() && needed <= current_max {
            return;
        }
        let target_buckets = needed
            .div_ceil(MAX_LOAD_PER_BUCKET)
            .max(MIN_BUCKETS)
            .next_power_of_two();
        self.grow(target_buckets, bounded);
    }

    fn grow(&mut self, target_buckets: usize, bounded: bool) {
        debug_assert!(target_buckets.is_power_of_two() && target_buckets >= MIN_BUCKETS);
        let old_buckets = self.buckets.len();
        self.buckets.clear();
        self.buckets.resize(target_buckets, Bucket::EMPTY);
        self.mask = target_buckets - 1;
        let bitmap_words = target_buckets.div_ceil(64);
        self.overflow.clear();
        self.overflow.resize(bitmap_words, 0);
        self.spilled.clear();
        self.spilled.resize(bitmap_words, 0);

        // Preserve per-bucket saturation across power-of-two bucket doubling:
        // every key whose old home bucket `old_b = mix32(k) & (old_buckets - 1)`
        // was saturated (or held an unlogged `(fp16, val)` duplicate) now hashes
        // to `old_b + j * old_buckets` for some `j`. Replicating each set bit
        // in-place guarantees zero false negatives after resize with zero
        // temporary allocation.
        if old_buckets == 0 {
            self.saturated.clear();
            self.saturated.resize(bitmap_words, 0);
        } else {
            self.saturated.resize(bitmap_words, 0);
            for old_b in 0..old_buckets {
                if self.bucket_saturated(old_b) || self.bucket_dup_on_grow(old_b) {
                    let mut dst = old_b;
                    while dst < target_buckets {
                        self.set_bucket_saturated(dst);
                        dst += old_buckets;
                    }
                }
            }
        }
        self.dup_on_grow.clear();
        self.dup_on_grow.resize(bitmap_words, 0);
        self.spill.clear();

        for i in 0..self.entries.len() {
            let entry = self.entries[i];
            let mixed = mix32(entry.key);
            let b0 = (mixed as usize) & self.mask;
            if self.bucket_saturated(b0) {
                continue;
            }
            if entry.val > MAX_INDEXED_VAL {
                if bounded {
                    self.set_bucket_saturated(b0);
                } else {
                    self.set_bucket_spilled(b0);
                    self.spill.push(entry);
                }
                continue;
            }
            let fp16 = maplet_fp(mixed);
            let slot = pack_slot(fp16, entry.val);
            let placed0 = self
                .buckets
                .get_mut(b0)
                .is_some_and(|b| !matches!(try_place_in_bucket(b, slot), SlotPlace::Full));
            if placed0 {
                continue;
            }
            let b1 = (b0 + 1) & self.mask;
            let placed1 = self
                .buckets
                .get_mut(b1)
                .is_some_and(|b| !matches!(try_place_in_bucket(b, slot), SlotPlace::Full));
            if placed1 {
                self.set_bucket_overflow(b0);
                continue;
            }
            if bounded {
                self.set_bucket_saturated(b0);
            } else {
                self.set_bucket_spilled(b0);
                self.spill.push(entry);
            }
        }
    }
}

// ── Sealed Compact Profile: MapletView<'a> ───────────────────────────────

/// Zero-copy borrowed view over a sealed canonical maplet image.
///
/// Backed by a Format 2 [`V2View`] key set and a packed `u8`/`u16` value plane
/// (plus compact fixed-width group end offsets in multi-value mode). Every
/// admission and lookup path allocates nothing and never reads past [`Self::extent`].
#[derive(Clone, Copy, Debug)]
pub struct MapletView<'a> {
    bytes: &'a [u8],
    keys: Option<V2View<'a>>,
    key_count: usize,
    total_values: usize,
    offsets_at: usize,
    values_at: usize,
    off_width: u8,
    val_u16: bool,
    multi_value: bool,
}

impl<'a> MapletView<'a> {
    /// Admit and fully verify a sealed maplet image at the start of `bytes`
    /// with zero heap allocation.
    ///
    /// Refuses truncated buffers, bad magic/version/flags, non-canonical Format 2
    /// key images, mismatched extents, out-of-order multi-value offsets, and
    /// non-minimal value widths (`u16` plane when all values fit in `u8`).
    pub fn open(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        if bytes.len() < 2 {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                bytes.len(),
                "a maplet image requires at least a 2-byte header",
            ));
        }
        if bytes[0] != MAPLET_MAGIC {
            return Err(AdmissionError::new(
                AdmissionClass::Magic,
                0,
                "maplet magic byte does not match",
            ));
        }
        let flags = bytes[1];
        if (flags & MAPLET_VERSION_MASK) != MAPLET_VERSION_1 {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                1,
                "unsupported maplet format version",
            ));
        }
        if (flags & FLAG_RESERVED) != 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                1,
                "reserved maplet header flag bit is set",
            ));
        }
        let val_u16 = (flags & FLAG_VAL_U16) != 0;
        let multi_value = (flags & FLAG_MULTI_VALUE) != 0;
        let is_empty = (flags & FLAG_EMPTY) != 0;

        if is_empty {
            if val_u16 || multi_value {
                return Err(AdmissionError::new(
                    AdmissionClass::Canonical,
                    1,
                    "empty maplet must clear value-width and multi-value flags",
                ));
            }
            return Ok(Self {
                bytes: &bytes[..2],
                keys: None,
                key_count: 0,
                total_values: 0,
                offsets_at: 2,
                values_at: 2,
                off_width: 0,
                val_u16: false,
                multi_value: false,
            });
        }

        let mut at = 2usize;
        let v2_len_at = at;
        let v2_len = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Extent)? as usize;
        if v2_len == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                v2_len_at,
                "non-empty maplet has zero-length key image",
            ));
        }
        let total_values_opt = if multi_value {
            let tv_at = at;
            let tv = read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Count)? as usize;
            Some((tv_at, tv))
        } else {
            None
        };

        let v2_start = at;
        let v2_end = v2_start.checked_add(v2_len).ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Extent,
                v2_len_at,
                "maplet key image extent overflows address space",
            )
        })?;
        let v2_slice = bytes.get(v2_start..v2_end).ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Extent,
                v2_len_at,
                "maplet key image is truncated",
            )
        })?;
        let keys = V2View::open_canonical(v2_slice).map_err(|e| e.shifted(v2_start))?;
        if keys.serialized_len() != v2_len {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                v2_len_at,
                "declared key image length disagrees with Format 2 extent",
            ));
        }
        let key_count = usize::try_from(keys.len()).map_err(|_| {
            AdmissionError::new(
                AdmissionClass::Count,
                v2_start,
                "maplet key count exceeds address space",
            )
        })?;
        if key_count == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Canonical,
                v2_start,
                "non-empty maplet encodes an empty Format 2 key set",
            ));
        }

        let total_values = match total_values_opt {
            Some((tv_at, tv)) => {
                if tv <= key_count {
                    return Err(AdmissionError::new(
                        AdmissionClass::Canonical,
                        tv_at,
                        "multi-value maplet must hold strictly more values than keys",
                    ));
                }
                tv
            }
            None => key_count,
        };

        let offsets_at = v2_end;
        let (off_width, offsets_bytes) = if multi_value && key_count > 1 {
            let max_off = u32::try_from(total_values - 1).map_err(|_| {
                AdmissionError::new(
                    AdmissionClass::Count,
                    offsets_at,
                    "multi-value total exceeds u32 offset domain",
                )
            })?;
            let w = width_of(max_off);
            let n_bytes = (key_count - 1).checked_mul(w).ok_or_else(|| {
                AdmissionError::new(
                    AdmissionClass::Extent,
                    offsets_at,
                    "maplet offset plane overflows address space",
                )
            })?;
            (w as u8, n_bytes)
        } else {
            (0u8, 0usize)
        };

        let values_at = offsets_at.checked_add(offsets_bytes).ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Extent,
                offsets_at,
                "maplet value plane offset overflows address space",
            )
        })?;
        let val_width = if val_u16 { 2usize } else { 1usize };
        let values_bytes = total_values.checked_mul(val_width).ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Extent,
                values_at,
                "maplet value plane size overflows address space",
            )
        })?;
        let extent = values_at.checked_add(values_bytes).ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Extent,
                values_at,
                "maplet total extent overflows address space",
            )
        })?;
        let slice = bytes.get(..extent).ok_or_else(|| {
            AdmissionError::new(
                AdmissionClass::Extent,
                bytes.len(),
                "maplet image is truncated before end of value plane",
            )
        })?;

        // Verify strictly ascending, in-range group end offsets in multi-value mode.
        if off_width > 0 {
            let w = usize::from(off_width);
            let mut prev = 0usize;
            for idx in 0..(key_count - 1) {
                let pos = offsets_at + idx * w;
                let off = read_uint_le(slice, pos, w) as usize;
                if off <= prev {
                    return Err(AdmissionError::new(
                        AdmissionClass::MapOrder,
                        pos,
                        "multi-value group end offsets are not strictly ascending",
                    ));
                }
                if off >= total_values {
                    return Err(AdmissionError::new(
                        AdmissionClass::PlaneRange,
                        pos,
                        "multi-value group end offset reaches or exceeds total_values",
                    ));
                }
                prev = off;
            }
        }

        // Verify canonical u16 value width: at least one value must exceed u8::MAX.
        if val_u16 {
            let mut any_wide = false;
            for idx in 0..total_values {
                if read_u16_at(slice, values_at + idx * 2) > u16::from(u8::MAX) {
                    any_wide = true;
                    break;
                }
            }
            if !any_wide {
                return Err(AdmissionError::new(
                    AdmissionClass::Canonical,
                    values_at,
                    "u16 value plane holds no value above 255",
                ));
            }
        }

        Ok(Self {
            bytes: slice,
            keys: Some(keys),
            key_count,
            total_values,
            offsets_at,
            values_at,
            off_width,
            val_u16,
            multi_value,
        })
    }

    /// O(1) zero-allocation attach for an already-admitted sealed maplet image.
    #[inline]
    #[must_use]
    pub fn open_admitted(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < 2 || bytes[0] != MAPLET_MAGIC {
            return None;
        }
        let flags = bytes[1];
        if (flags & MAPLET_VERSION_MASK) != MAPLET_VERSION_1 || (flags & FLAG_RESERVED) != 0 {
            return None;
        }
        let val_u16 = (flags & FLAG_VAL_U16) != 0;
        let multi_value = (flags & FLAG_MULTI_VALUE) != 0;
        if (flags & FLAG_EMPTY) != 0 {
            return Some(Self {
                bytes: &bytes[..2],
                keys: None,
                key_count: 0,
                total_values: 0,
                offsets_at: 2,
                values_at: 2,
                off_width: 0,
                val_u16: false,
                multi_value: false,
            });
        }
        let mut at = 2usize;
        let v2_len =
            read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Extent).ok()? as usize;
        let total_values_opt = if multi_value {
            Some(read_uleb_checked(bytes, &mut at, u32::MAX, AdmissionClass::Count).ok()? as usize)
        } else {
            None
        };
        let v2_end = at.checked_add(v2_len)?;
        let keys = V2View::open_admitted(bytes.get(at..v2_end)?)?;
        let key_count = usize::try_from(keys.len()).ok()?;
        let total_values = total_values_opt.unwrap_or(key_count);
        let offsets_at = v2_end;
        let off_width = if multi_value && key_count > 1 {
            width_of((total_values - 1) as u32) as u8
        } else {
            0u8
        };
        let offsets_bytes = if off_width > 0 {
            (key_count - 1).checked_mul(usize::from(off_width))?
        } else {
            0usize
        };
        let values_at = offsets_at.checked_add(offsets_bytes)?;
        let val_width = if val_u16 { 2usize } else { 1usize };
        let extent = values_at.checked_add(total_values.checked_mul(val_width)?)?;
        let slice = bytes.get(..extent)?;
        Some(Self {
            bytes: slice,
            keys: Some(keys),
            key_count,
            total_values,
            offsets_at,
            values_at,
            off_width,
            val_u16,
            multi_value,
        })
    }

    /// Total byte extent of the sealed image.
    #[inline(always)]
    #[must_use]
    pub fn extent(&self) -> usize {
        self.bytes.len()
    }

    /// Total number of `(key, value)` entries stored in the sealed maplet.
    #[inline(always)]
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_values
    }

    /// Number of distinct keys stored in the underlying Format 2 image.
    #[inline(always)]
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// Whether the sealed maplet holds zero entries.
    #[inline(always)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether this sealed image uses multi-value grouping (`len() > key_count()`).
    #[inline(always)]
    #[must_use]
    pub fn is_multi_value(&self) -> bool {
        self.multi_value
    }

    /// Look up `key` in O(1) via [`V2View::rank_if_present`] with zero heap
    /// allocation.
    ///
    /// Returns the single associated value in unique-key mode, or the newest
    /// value in multi-value mode.
    #[inline(always)]
    #[must_use]
    pub fn get(&self, key: u32) -> Option<u16> {
        let keys = self.keys?;
        let rank = keys.rank_if_present(key)? as usize;
        let idx = if !self.multi_value || rank == 0 {
            rank
        } else {
            self.read_offset(rank - 1)
        };
        Some(self.read_value(idx))
    }

    /// Visit every value associated with `key` in newest-first order with zero
    /// heap allocation.
    #[inline]
    pub fn for_each_match<R>(
        &self,
        key: u32,
        mut visit: impl FnMut(u16) -> ControlFlow<R>,
    ) -> ControlFlow<R> {
        let Some(keys) = self.keys else {
            return ControlFlow::Continue(());
        };
        let Some(rank) = keys.rank_if_present(key) else {
            return ControlFlow::Continue(());
        };
        let rank = rank as usize;
        if !self.multi_value {
            return visit(self.read_value(rank));
        }
        let (start, end) = self.value_range(rank);
        for idx in start..end {
            if let ControlFlow::Break(done) = visit(self.read_value(idx)) {
                return ControlFlow::Break(done);
            }
        }
        ControlFlow::Continue(())
    }

    /// Hydrate this sealed maplet back into a live mutable [`Maplet`],
    /// preserving every `(key, value)` entry and exact newest-first ordering.
    #[must_use]
    pub fn promote(&self) -> Maplet {
        let Some(keys) = self.keys else {
            return Maplet::new();
        };
        let mut maplet = Maplet::with_capacity(self.len());
        let mut rank = 0usize;
        keys.for_each(|key| {
            if !self.multi_value {
                maplet.insert(key, self.read_value(rank));
            } else {
                let (start, end) = self.value_range(rank);
                // Sealed values are stored newest-first; insert oldest-first so
                // `Maplet::for_each_match` visits them in identical newest-first order.
                for idx in (start..end).rev() {
                    maplet.insert(key, self.read_value(idx));
                }
            }
            rank += 1;
        });
        maplet
    }

    #[inline(always)]
    fn read_offset(&self, idx: usize) -> usize {
        let w = usize::from(self.off_width);
        read_uint_le(self.bytes, self.offsets_at + idx * w, w) as usize
    }

    #[inline(always)]
    fn value_range(&self, rank: usize) -> (usize, usize) {
        let start = if rank == 0 {
            0
        } else {
            self.read_offset(rank - 1)
        };
        let end = if rank + 1 == self.key_count {
            self.total_values
        } else {
            self.read_offset(rank)
        };
        (start, end)
    }

    #[inline(always)]
    fn read_value(&self, idx: usize) -> u16 {
        if self.val_u16 {
            read_u16_at(self.bytes, self.values_at + idx * 2)
        } else {
            u16::from(self.bytes[self.values_at + idx])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_layout_is_64_byte_cache_line_aligned() {
        assert_eq!(core::mem::size_of::<Bucket>(), 64);
        assert_eq!(core::mem::align_of::<Bucket>(), 64);
    }

    #[test]
    fn simd_and_scalar_matchers_agree_on_every_lane_and_pattern() {
        let target_fp = 0xA55Bu16;
        for lane in 0..BUCKET_SLOTS {
            let mut bucket = Bucket::EMPTY;
            bucket.slots[lane] = pack_slot(target_fp, (lane * 17) as u16);
            let expected = 1u16 << lane;
            assert_eq!(match_bucket_scalar(&bucket.slots, target_fp), expected);
            assert_eq!(match_bucket(&bucket.slots, target_fp), expected);
        }

        let mut seed = 0x1234_5678_9ABC_DEF0u64;
        for _ in 0..512 {
            let mut bucket = Bucket::EMPTY;
            for (i, cell) in bucket.slots.iter_mut().enumerate() {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let fp = if (seed & 3) == 0 {
                    target_fp
                } else {
                    maplet_fp((seed >> 32) as u32)
                };
                *cell = if (seed & 7) == 7 {
                    0
                } else {
                    pack_slot(fp, (i as u16).wrapping_add(((seed >> 16) as u16) & 0x3FF))
                };
            }
            assert_eq!(
                match_bucket(&bucket.slots, target_fp),
                match_bucket_scalar(&bucket.slots, target_fp)
            );
        }
    }

    #[test]
    fn bucket_overflow_and_double_overflow_zero_false_negatives() {
        // Force 40 keys whose mixed hashes land in the exact same initial bucket 5.
        let mut maplet = Maplet::with_capacity(64);
        let mask = maplet.mask;
        let mut keys = Vec::new();
        let mut candidate = 0u32;
        while keys.len() < 40 {
            if (mix32(candidate) as usize & mask) == 5 {
                keys.push(candidate);
            }
            candidate += 1;
        }
        for (i, &k) in keys.iter().enumerate() {
            maplet.insert(k, i as u16);
        }
        assert!(maplet.bucket_overflowed(5));
        assert!(!maplet.spill.is_empty());

        for (i, &k) in keys.iter().enumerate() {
            let mut found = Vec::new();
            let _ = maplet.for_each_match::<()>(k, |v| {
                found.push(v);
                ControlFlow::Continue(())
            });
            assert!(
                found.contains(&(i as u16)),
                "false negative for key={k} val={i}: found={found:?}"
            );
        }

        // Round-trip through seal -> open -> promote preserves all 40 exact entries.
        let sealed = maplet.seal();
        let view = MapletView::open(&sealed).expect("valid sealed image");
        assert_eq!(view.len(), 40);
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(view.get(k), Some(i as u16));
        }
        let promoted = view.promote();
        for (i, &k) in keys.iter().enumerate() {
            let mut found = Vec::new();
            let _ = promoted.for_each_match::<()>(k, |v| {
                found.push(v);
                ControlFlow::Continue(())
            });
            assert!(found.contains(&(i as u16)));
        }
    }

    #[test]
    fn bounded_insert_whitebox_zero_spill_local_gate_and_resize_soundness() {
        // 1. 240-frame x 100-key storm: 0 spill entries, bounded entry count,
        //    per-bucket saturation bitset active, unsaturated sibling unaffected.
        let mut maplet = Maplet::with_capacity(4096);
        let storm_keys: Vec<u32> = (1u32..=100u32)
            .map(|i| i.wrapping_mul(0x9E37_79B1))
            .collect();
        for frame in 0u16..240 {
            for &k in &storm_keys {
                let _ = maplet.insert_bounded(k, frame);
            }
        }
        assert!(
            maplet.spill.is_empty(),
            "bounded storm must accumulate 0 spill entries"
        );
        assert!(maplet.len() <= storm_keys.len() * 32);
        let sat_count: usize = maplet
            .saturated
            .iter()
            .map(|&w| w.count_ones() as usize)
            .sum();
        assert!(sat_count > 0);
        for &k in &storm_keys {
            assert!(maplet.is_key_saturated(k));
        }

        // 2. Per-bucket spill gate on legacy insert: spilling bucket 5 leaves
        //    unrelated buckets' `bucket_spilled` bit clear.
        let mut legacy = Maplet::with_capacity(64);
        let mask = legacy.mask;
        let mut b5_keys = Vec::new();
        let mut cand = 0u32;
        while b5_keys.len() < 40 {
            if (mix32(cand) as usize & mask) == 5 {
                b5_keys.push(cand);
            }
            cand += 1;
        }
        for (i, &k) in b5_keys.iter().enumerate() {
            legacy.insert(k, i as u16);
        }
        assert!(legacy.bucket_spilled(5));
        let other_b = (0..legacy.buckets.len())
            .find(|&b| b != 5 && !legacy.bucket_spilled(b))
            .expect("other bucket must remain unspilled");
        assert!(!legacy.bucket_spilled(other_b));

        // 3. Saturation survives power-of-two grow() with 0 spill.
        let mut grow_map = Maplet::new();
        let hot = 0xCAFE_BABEu32;
        for frame in 0u16..40 {
            let _ = grow_map.insert_bounded(hot, frame);
        }
        let buckets_before = grow_map.buckets.len();
        assert!(grow_map.spill.is_empty());
        for i in 1u32..600 {
            let k = i.wrapping_mul(0x045D_9F3B);
            if k != hot {
                let _ = grow_map.insert_bounded(k, (i & 0xFF) as u16);
            }
        }
        assert!(grow_map.buckets.len() > buckets_before);
        assert!(grow_map.spill.is_empty());
        assert_eq!(
            grow_map.for_each_match_bounded::<()>(hot, |_| ControlFlow::Continue(())),
            MapletMatch::Saturated
        );

        // 4. u16::MAX under insert_bounded sets saturation with 0 spill.
        let mut max_map = Maplet::with_capacity(64);
        assert_eq!(
            max_map.insert_bounded(42, u16::MAX),
            MapletInsert::Saturated
        );
        assert!(max_map.spill.is_empty());
    }
}
