//! Elias-Fano plane over a whole `u32` set: the ladder's sparse arm.
//!
//! The forest splits the universe at a FIXED point — bit 16 — and pays a
//! directory record per occupied chunk. That is Elias-Fano with `low_bits`
//! pinned to 16, and on sparse data the pin is wrong by an order of
//! magnitude: `u13m/n100` occupies 81 chunks for 100 values, so 810 of its
//! 1,064 wire bytes are directory. This arm lets the split float to
//! `floor(log2(span / n))`, which is the same structure with the constant
//! removed — the high plane IS the chunk directory, at one bit per bucket
//! instead of ten bytes.
//!
//! Probes are position-computed, never searched. `select` reads one sample
//! and finishes inside a word; `contains`/`rank` read one zero-sample and
//! walk a bucket whose expected length is `n / 2^ceil(log2(n))` — under 2 by
//! construction, because `low_bits` is chosen so the high plane holds about
//! as many zeros as ones. No binary search appears on any read path.

use crate::wire::{read_u32_at, read_u64_at};

/// One sample every 64 ones and every 64 zeros. The samples are absolute
/// BIT POSITIONS, not word indices: a word index would leave the running
/// rank unknown at the landing point and force a second scan to recover it.
pub(crate) const SAMPLE_STRIDE: u32 = 64;

/// Geometry of one Elias-Fano plane, derived from `(n, span)` alone.
///
/// Every field is a closed form of the two inputs, so the encoder can price
/// the arm exactly — to the byte, before writing anything — and the decoder
/// can rebuild the same geometry from cardinality and span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EfLayout {
    pub(crate) low_bits: u8,
    pub(crate) high_words: usize,
    /// Bit positions in the high plane: `(span >> low_bits) + n + 1`.
    pub(crate) high_bits: u64,
    /// Zeros in the high plane = bucket count = `(span >> low_bits) + 1`.
    pub(crate) zeros: u64,
    pub(crate) low_bytes: usize,
    pub(crate) ones_samples: usize,
    pub(crate) zero_samples: usize,
}

impl EfLayout {
    /// `None` when the geometry would not fit `usize` — the caller then
    /// keeps the forest, which is the arm this one has to beat anyway.
    pub(crate) fn new(n: u64, span: u64) -> Option<EfLayout> {
        if n == 0 || span > u64::from(u32::MAX) || n > span + 1 {
            return None;
        }
        // floor(log2(span / n)) by bit-length difference, corrected by one
        // shifted compare - the same derivation ptmcart's monotone plane
        // uses, and for the same reason: a divide here would be the only
        // divide in the encoder.
        let mut low_bits = if span <= n {
            0u8
        } else {
            ((63 - span.leading_zeros()) - (63 - n.leading_zeros())) as u8
        };
        if low_bits != 0 && (span >> low_bits) < n {
            low_bits -= 1;
        }
        let buckets = (span >> low_bits) + 1;
        let high_bits = buckets.checked_add(n)?;
        if high_bits - 1 > u64::from(u32::MAX) {
            return None;
        }
        let high_words = usize::try_from(high_bits.div_ceil(64)).ok()?;
        let low_bytes = usize::try_from((n * u64::from(low_bits)).div_ceil(8)).ok()?;
        Some(EfLayout {
            low_bits,
            high_words,
            high_bits,
            zeros: buckets,
            low_bytes,
            ones_samples: usize::try_from((n - 1) / u64::from(SAMPLE_STRIDE)).ok()?,
            zero_samples: usize::try_from(buckets.div_ceil(u64::from(SAMPLE_STRIDE))).ok()?,
        })
    }

    /// Exact payload extent; the following sample plane bounds low-field reads.
    pub(crate) fn payload_bytes(&self) -> usize {
        self.high_words * 8 + self.low_bytes + (self.ones_samples + self.zero_samples) * 4
    }

    pub(crate) fn low_at(&self) -> usize {
        self.high_words * 8
    }

    pub(crate) fn ones_at(&self) -> usize {
        self.low_at() + self.low_bytes
    }

    pub(crate) fn zeros_at(&self) -> usize {
        self.ones_at() + self.ones_samples * 4
    }
}

/// Write the payload for an ascending, distinct sequence with `values[0] ==
/// base` over `out`, which is exactly [`EfLayout::payload_bytes`] long and
/// need not be zeroed.
///
/// Takes an iterator, not a slice: the encoder is forward-only in both
/// planes and in both sample tables, so serialising a frozen set never
/// materialises it.
pub(crate) fn encode_into(
    values: impl Iterator<Item = u32>,
    base: u32,
    layout: EfLayout,
    out: &mut [u8],
) {
    debug_assert_eq!(out.len(), layout.payload_bytes());
    out.fill(0);
    let buf = out;
    let (high, rest) = buf.split_at_mut(layout.high_words * 8);
    let (low, samples) = rest.split_at_mut(layout.low_bytes);
    let (ones_s, zero_s) = samples.split_at_mut(layout.ones_samples * 4);

    let mask = (1u64 << layout.low_bits) - 1;
    let stride = u64::from(SAMPLE_STRIDE);
    let mut next_zero = 0u64;
    let mut count = 0u64;
    for (k, v) in values.enumerate() {
        let k = k as u64;
        let d = u64::from(v - base);
        let h = d >> layout.low_bits;
        let p = h + k;

        // High plane: bit `h + k` is set. The set bits ARE the values; the
        // runs of clear bits between them are the bucket boundaries, which
        // is why no separate directory exists.
        let at = (p / 64) as usize * 8;
        let word = read_u64_at(high, at) | 1u64 << (p % 64);
        high[at..at + 8].copy_from_slice(&word.to_le_bytes());

        // Sampled zeros below h sit at bit `z + k`; next_zero advances
        // only to the next sample, without visiting unsampled buckets.
        while next_zero < h {
            let at = (next_zero / stride) as usize * 4;
            zero_s[at..at + 4].copy_from_slice(&((next_zero + k) as u32).to_le_bytes());
            next_zero += stride;
        }

        if k != 0 && k.is_multiple_of(stride) {
            let at = (k / stride - 1) as usize * 4;
            ones_s[at..at + 4].copy_from_slice(&(p as u32).to_le_bytes());
        }

        if layout.low_bits != 0 {
            let bit = k * u64::from(layout.low_bits);
            let byte = (bit / 8) as usize;
            let window = ((d & mask) << (bit % 8)).to_le_bytes();
            let width = ((bit % 8 + u64::from(layout.low_bits)).div_ceil(8)) as usize;
            for i in 0..width {
                low[byte + i] |= window[i];
            }
        }
        count = k + 1;
    }
    // Buckets past the last value: one final run of zeros, all at `z + n`.
    while next_zero < layout.zeros {
        let at = (next_zero / stride) as usize * 4;
        zero_s[at..at + 4].copy_from_slice(&((next_zero + count) as u32).to_le_bytes());
        next_zero += stride;
    }
}

// ── view ─────────────────────────────────────────────────────────────────

/// Borrowed Elias-Fano plane. Every field is a number; the bytes stay in the
/// mapped page.
#[derive(Clone, Copy, Debug)]
pub struct EfView<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) layout: EfLayout,
    pub(crate) base: u32,
    pub(crate) span: u32,
    pub(crate) len: u64,
}

impl<'a> EfView<'a> {
    /// `bytes` is the payload alone. `None` when the geometry and the byte
    /// length disagree — the one check that makes every later index safe.
    pub(crate) fn new(bytes: &'a [u8], base: u32, span: u32, len: u64) -> Option<EfView<'a>> {
        base.checked_add(span)?;
        let layout = EfLayout::new(len, u64::from(span))?;
        if bytes.len() != layout.payload_bytes() {
            return None;
        }
        let high_used = layout.high_bits % 64;
        if high_used != 0 && read_u64_at(bytes, (layout.high_words - 1) * 8) >> high_used != 0 {
            return None;
        }
        let low_used = (len * u64::from(layout.low_bits)) % 8;
        if low_used != 0 && bytes[layout.low_at() + layout.low_bytes - 1] >> low_used != 0 {
            return None;
        }
        Some(EfView {
            bytes,
            layout,
            base,
            span,
            len,
        })
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// A word of the high plane. Unbounded on purpose — this is the load on
    /// every probe — so each scan that could outrun an UNVERIFIED plane
    /// carries its own end test (`select_one`, `select_zero`, `for_each`,
    /// the cursor's refill) and `bit` tests the position.
    #[inline(always)]
    pub(crate) fn high_word(&self, w: usize) -> u64 {
        read_u64_at(self.bytes, w * 8)
    }

    #[inline(always)]
    pub(crate) fn low(&self, k: u64) -> u64 {
        if self.layout.low_bits == 0 {
            return 0;
        }
        let bit = k * u64::from(self.layout.low_bits);
        let at = self.layout.low_at() + (bit / 8) as usize;
        let window = u64::from(read_u32_at(self.bytes, at)) | (u64::from(self.bytes[at + 4]) << 32);
        (window >> (bit % 8)) & ((1u64 << self.layout.low_bits) - 1)
    }

    /// Bit position of the `k`-th one (0-indexed). One sample load, then a
    /// bounded forward scan of at most 64 ones' worth of words.
    ///
    /// `high_bits` — a position past the plane — when the scan runs out of
    /// words. That happens only on an UNVERIFIED plane whose samples or
    /// popcount disagree with its length; the end test exists so such a
    /// plane ends every walk at its own bounds instead of past them.
    #[inline]
    pub(crate) fn select_one(&self, k: u64) -> u64 {
        let s = k / u64::from(SAMPLE_STRIDE);
        let start = if s == 0 {
            0
        } else {
            u64::from(read_u32_at(
                self.bytes,
                self.layout.ones_at() + (s as usize - 1) * 4,
            ))
        };
        let mut remaining = k - s * u64::from(SAMPLE_STRIDE);
        let mut w = (start / 64) as usize;
        if w >= self.layout.high_words {
            return self.layout.high_bits;
        }
        // The sample lands ON a one, so the bits below it in that word are
        // already accounted for.
        let mut word = self.high_word(w) & !((1u64 << (start % 64)) - 1);
        loop {
            let ones = u64::from(word.count_ones());
            if remaining < ones {
                let mut bits = word;
                for _ in 0..remaining {
                    bits &= bits - 1;
                }
                return (w as u64) * 64 + u64::from(bits.trailing_zeros());
            }
            remaining -= ones;
            w += 1;
            if w >= self.layout.high_words {
                return self.layout.high_bits;
            }
            word = self.high_word(w);
        }
    }

    /// Bit position of the `z`-th zero (0-indexed), or `high_bits` when the
    /// plane has no such zero (or, unverified, runs out of words first).
    #[inline]
    pub(crate) fn select_zero(&self, z: u64) -> u64 {
        if z >= self.layout.zeros {
            return self.layout.high_bits;
        }
        let s = z / u64::from(SAMPLE_STRIDE);
        let start = u64::from(read_u32_at(
            self.bytes,
            self.layout.zeros_at() + s as usize * 4,
        ));
        let mut remaining = z - s * u64::from(SAMPLE_STRIDE);
        let mut w = (start / 64) as usize;
        if w >= self.layout.high_words {
            return self.layout.high_bits;
        }
        let mut word = !self.high_word(w) & !((1u64 << (start % 64)) - 1);
        loop {
            let zeros = u64::from(word.count_ones());
            if remaining < zeros {
                let mut bits = word;
                for _ in 0..remaining {
                    bits &= bits - 1;
                }
                return (w as u64) * 64 + u64::from(bits.trailing_zeros());
            }
            remaining -= zeros;
            w += 1;
            if w >= self.layout.high_words {
                return self.layout.high_bits;
            }
            word = !self.high_word(w);
        }
    }

    /// Index of the first value whose high part is >= `h`, and the bit
    /// position at which that bucket's run of ones begins.
    #[inline(always)]
    pub(crate) fn bucket_start(&self, h: u64) -> (u64, u64) {
        if h == 0 {
            (0, 0)
        } else {
            // The `z`-th zero sits at or past position `z`; saturating for
            // an UNVERIFIED sample that says otherwise — a rank past `len`
            // ends the walks above without a read.
            let p = self.select_zero(h - 1);
            (p.saturating_sub(h - 1), p + 1)
        }
    }

    #[inline]
    pub fn contains(&self, value: u32) -> bool {
        if value < self.base || value > self.base.saturating_add(self.span) {
            return false;
        }
        let d = u64::from(value - self.base);
        let h = d >> self.layout.low_bits;
        let want = d & ((1u64 << self.layout.low_bits) - 1);
        let (mut k, mut p) = self.bucket_start(h);
        while k < self.len && self.bit(p) {
            let low = self.low(k);
            if low == want {
                return true;
            }
            if low > want {
                return false;
            }
            k += 1;
            p += 1;
        }
        false
    }

    /// Members strictly below `value`.
    #[inline]
    pub fn rank(&self, value: u32) -> u64 {
        if value <= self.base {
            return 0;
        }
        if value > self.base.saturating_add(self.span) {
            return self.len;
        }
        let d = u64::from(value - self.base);
        let h = d >> self.layout.low_bits;
        let want = d & ((1u64 << self.layout.low_bits) - 1);
        let (mut k, mut p) = self.bucket_start(h);
        while k < self.len && self.bit(p) && self.low(k) < want {
            k += 1;
            p += 1;
        }
        k
    }

    /// `k`-th member ascending; `None` past the end.
    #[inline]
    pub fn select(&self, k: u64) -> Option<u32> {
        if k >= self.len {
            return None;
        }
        let p = self.select_one(k);
        let high = p.wrapping_sub(k);
        Some(
            self.base
                .wrapping_add(((high << self.layout.low_bits) | self.low(k)) as u32),
        )
    }

    #[inline(always)]
    pub(crate) fn bit(&self, p: u64) -> bool {
        p < self.layout.high_bits && self.high_word((p / 64) as usize) >> (p % 64) & 1 == 1
    }

    /// Content verification run once by `BitmosaicView::open_verified`: the high
    /// plane holds exactly `len` ones and `zeros` zeros inside `high_bits`
    /// (nothing set past the end), every sample names the position of the
    /// one or zero it stands for, and the decoded sequence is strictly
    /// ascending from `base` to exactly `base + span`. With these, every
    /// sample-seeded scan in `select_one`/`select_zero` terminates inside
    /// the plane and every decoded member is the writer's. One pass over
    /// the high plane; no allocation.
    pub(crate) fn verify(&self) -> bool {
        let Some(last) = self.base.checked_add(self.span) else {
            return false;
        };
        let stride = u64::from(SAMPLE_STRIDE);
        let (mut ones, mut zeros) = (0u64, 0u64);
        let mut prev: Option<u32> = None;
        for w in 0..self.layout.high_words {
            let word = self.high_word(w);
            let base_pos = (w as u64) * 64;
            // Bits at or past `high_bits` must be clear.
            let live = self.layout.high_bits.saturating_sub(base_pos).min(64);
            if live < 64 && word >> live != 0 {
                return false;
            }
            for b in 0..live {
                let p = base_pos + b;
                if word >> b & 1 == 1 {
                    if ones >= self.len {
                        return false;
                    }
                    if (ones == 0 && p != 0)
                        || (ones != 0
                            && ones.is_multiple_of(stride)
                            && u64::from(read_u32_at(
                                self.bytes,
                                self.layout.ones_at() + (ones / stride - 1) as usize * 4,
                            )) != p)
                    {
                        return false;
                    }
                    let high = p - ones;
                    let value = (high << self.layout.low_bits) | self.low(ones);
                    if value > u64::from(self.span) {
                        return false;
                    }
                    let value = self.base + value as u32;
                    if prev.is_some_and(|q| q >= value) {
                        return false;
                    }
                    prev = Some(value);
                    ones += 1;
                } else {
                    if zeros >= self.layout.zeros {
                        return false;
                    }
                    if zeros.is_multiple_of(stride)
                        && u64::from(read_u32_at(
                            self.bytes,
                            self.layout.zeros_at() + (zeros / stride) as usize * 4,
                        )) != p
                    {
                        return false;
                    }
                    zeros += 1;
                }
            }
        }
        ones == self.len && zeros == self.layout.zeros && prev == Some(last)
    }

    /// Ascending walk, allocation-free: one pass over the high plane rather
    /// than `len` independent selects. The bucket index is carried, so no
    /// position is recomputed.
    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        let mut k = 0u64;
        let mut w = 0usize;
        while k < self.len && w < self.layout.high_words {
            let word = self.high_word(w);
            let mut bits = word;
            let base_pos = (w as u64) * 64;
            // Rank-bounded inside the word too: an UNVERIFIED plane holding
            // more ones than its length would otherwise index the low plane
            // past its exact end.
            while bits != 0 && k < self.len {
                let b = bits.trailing_zeros() as u64;
                bits &= bits - 1;
                let high = base_pos + b - k;
                f(self
                    .base
                    .wrapping_add(((high << self.layout.low_bits) | self.low(k)) as u32));
                k += 1;
            }
            w += 1;
        }
    }

    /// An ascending cursor over this plane, positioned at rank 0.
    #[inline]
    pub(crate) fn range(&self) -> EfRange<'a> {
        EfRange::at_rank(*self, 0)
    }
}

/// Ascending cursor over a borrowed Elias-Fano plane.
///
/// [`EfView::for_each`] already walks the high plane in one pass; this is
/// the same walk with its state made addressable, so a merge can advance
/// ONE side. Driving the walk through [`EfView::select`] instead would
/// re-enter the sample table and rescan from a sample boundary for every
/// element — the same `select`-in-a-loop the crate rejects everywhere else.
///
/// The `bits`/`k` pair is normalised on construction and after every step,
/// so `front` is a `trailing_zeros` plus one low-plane read over state the
/// cursor already holds. That is the idempotent-`front` invariant
/// [`crate::Range`] requires.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EfRange<'a> {
    view: EfView<'a>,
    /// Index of the next high word to load; `bits` belongs to `word - 1`.
    word: usize,
    /// Unconsumed set bits of the word at `word - 1`. Non-zero whenever
    /// `k < view.len`.
    bits: u64,
    /// Rank of the head — also the low-plane index, which is why the walk
    /// needs no separate counter.
    k: u64,
}

impl<'a> EfRange<'a> {
    /// Position at rank `k`. `k >= len` yields an exhausted cursor.
    ///
    /// This is the ONE place a `select_one` is paid, and a leapfrog seek
    /// pays it once per jump rather than once per element.
    pub(crate) fn at_rank(view: EfView<'a>, k: u64) -> EfRange<'a> {
        let p = if k >= view.len {
            view.layout.high_bits
        } else {
            view.select_one(k)
        };
        // Past the plane — `k >= len`, or an UNVERIFIED plane whose ones ran
        // out before rank `k` — is an exhausted cursor, never a word read.
        if p >= view.layout.high_bits {
            return EfRange {
                view,
                word: 0,
                bits: 0,
                k: view.len,
            };
        }
        let w = (p / 64) as usize;
        EfRange {
            view,
            word: w + 1,
            bits: view.high_word(w) & !((1u64 << (p % 64)) - 1),
            k,
        }
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> u64 {
        self.view.len - self.k
    }

    #[inline(always)]
    pub(crate) fn empty(&self) -> bool {
        self.k >= self.view.len
    }

    #[inline(always)]
    pub(crate) fn front(&self) -> u32 {
        debug_assert!(!self.empty(), "front on an empty Elias-Fano range");
        // `bits` belongs to word - 1, and the high part of the k-th member
        // is its one-bit position minus its rank. Wrapping, not checked: on
        // a verified plane neither can wrap, and on an unverified one the
        // answer is already the plane's own — it must not become a panic.
        let p = (self.word as u64).wrapping_sub(1) * 64 + u64::from(self.bits.trailing_zeros());
        let high = p.wrapping_sub(self.k);
        self.view
            .base
            .wrapping_add(((high << self.view.layout.low_bits) | self.view.low(self.k)) as u32)
    }

    #[inline(always)]
    pub(crate) fn pop_front(&mut self) {
        debug_assert!(!self.empty(), "pop_front on an empty Elias-Fano range");
        // A dry word here means an UNVERIFIED plane ran out of ones before
        // its length; the cursor is exhausted, not one bit short of a panic.
        if self.bits == 0 {
            self.k = self.view.len;
            return;
        }
        self.bits &= self.bits - 1;
        self.k += 1;
        // Refill only when the current word runs dry, and stop on rank
        // rather than on the plane's length: the tail past the last one is
        // all zeros and walking it would cost the span, not the count.
        while self.bits == 0 && self.k < self.view.len {
            if self.word >= self.view.layout.high_words {
                self.k = self.view.len;
                return;
            }
            self.bits = self.view.high_word(self.word);
            self.word += 1;
        }
    }

    /// Advance to the first member at or after `value`. Never moves
    /// backwards.
    #[inline]
    pub(crate) fn seek(&mut self, value: u32) {
        let k = self.view.rank(value);
        if k > self.k {
            *self = EfRange::at_rank(self.view, k);
        }
    }
}
