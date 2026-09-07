//! Compressed ordered sets with owned builders, native byte views, and witnessed
//! fixed-capacity updates. The u32 domain splits into 65,536-value chunks; the
//! u64 tier adds a directory of u32 forests.
//!
//! Container selection compares owned payload prices, with singleton Array and
//! recognized Stride taking priority. Cone is eligible through 1,024 members,
//! Array through 4,096; Words/Cone/Array retain their tie order and Runs wins
//! only strictly. Native forest-versus-Elias–Fano selection separately compares
//! complete serialized sizes.
//!
//! - **Stride** stores an arithmetic progression and counts intersections arithmetically.
//! - **Cone** stores a fixed-point model and signed-byte residuals.
//! - **Array** stores sorted u16 members.
//! - **Words** stores a dense plane, block summary, and cumulative rank directory.
//! - **Runs** stores maximal inclusive intervals and exclusive member prefixes.
//!
//! Borrowed readers never allocate or expand a bitmap. Run count/algebra paths
//! use intervals rather than enumerating their members. [`patch`] and
//! [`patch_witnessed`] retain scratch and finalize touched chunks through the
//! same selection policy as sealing; refusal never changes slot bytes.

// UNSAFE DOCTRINE: every `unsafe` block below carries a SAFETY proof from a
// STRUCTURAL invariant (u16 domain bounds, freeze-time array shapes, hoisted
// slice lengths) - never from caller goodwill. All such paths are covered by
// the BTreeSet differential oracle in tests and the roaring-rs oracle in
// benches/ops.rs.

mod ef;
mod patch;
mod range;
mod wire;

pub use ef::EfView;
pub use patch::{
    BatchOutcome, PatchError, PatchReport, PatchScratch, Witness, patch, patch_witnessed,
};
pub(crate) use range::ContainerRange;
pub use range::{
    AndNotRange, AndRange, Forest64Range, ForestRange, OrRange, Range, RangeIter, XorRange,
};
pub use wire::{
    Bitmosaic64View, Bitmosaic64ViewRange, BitmosaicView, BitmosaicViewRange, EMPTY_U32_IMAGE,
    EMPTY_U64_IMAGE, IMAGE_ID_LEN, ImageHeader, KeyWidth, image_header,
};

const CHUNK_WORDS: usize = 1024;
const BLOCK_WORDS: usize = 8;
const SUMMARY_WORDS: usize = 2; // 128 blocks
const DIR_STRIDE: usize = 4; // directory entry per 4 words
const ARRAY_MAX: usize = 4096;
const CONE_MAX_EPS: i64 = 63;
/// Above this per-chunk cardinality, word-AND (~190 ns flat) beats any
/// O(n) sequence merge - the ladder prefers Words for op throughput even
/// when a cone would be smaller (measured: jittered-20k cones cost 61 us
/// vs 0.9 us words). Bytes-first profiles can raise this.
const CONE_MAX_LEN: usize = 1024;

// ─────────────────────────────────────────────────────────────────────────
// Containers
// ─────────────────────────────────────────────────────────────────────────

/// Maximal inclusive interval, with its exclusive member prefix.
#[derive(Clone, Copy, Debug)]
struct Run {
    start: u16,
    end: u16,
    before: u16,
}

fn runs_from_members(values: &[u16], count: usize) -> Box<[Run]> {
    let mut runs = Vec::with_capacity(count);
    let mut begin = 0;
    for i in 1..=values.len() {
        if i == values.len() || u32::from(values[i]) != u32::from(values[i - 1]) + 1 {
            runs.push(Run {
                start: values[begin],
                end: values[i - 1],
                before: begin as u16,
            });
            begin = i;
        }
    }
    runs.into_boxed_slice()
}

pub(crate) fn dense_run_count(words: &[u64; CHUNK_WORDS]) -> usize {
    let mut previous = 0u64;
    let mut count = 0;
    for &word in words {
        count += (word & !((word << 1) | previous)).count_ones() as usize;
        previous = word >> 63;
    }
    count
}

/// Maximal spans without stepping over their individual members.
fn for_each_word_run(words: &[u64; CHUNK_WORDS], mut emit: impl FnMut(u32, u32)) {
    let mut position = 0usize;
    while position < 65_536 {
        let mut w = position / 64;
        let mut bits = words[w] & (u64::MAX << (position % 64));
        while bits == 0 {
            w += 1;
            if w == CHUNK_WORDS {
                return;
            }
            bits = words[w];
        }
        let start = w * 64 + bits.trailing_zeros() as usize;
        position = start;
        loop {
            let shift = position % 64;
            let held = (words[position / 64] >> shift).trailing_ones() as usize;
            position += held;
            if held < 64 - shift || position == 65_536 {
                break;
            }
        }
        emit(start as u32, position as u32);
    }
}

fn runs_from_words(words: &[u64; CHUNK_WORDS], count: usize) -> Box<[Run]> {
    let mut runs = Vec::with_capacity(count);
    let mut before = 0u32;
    for_each_word_run(words, |start, end| {
        runs.push(Run {
            start: start as u16,
            end: (end - 1) as u16,
            before: before as u16,
        });
        before += end - start;
    });
    runs.into_boxed_slice()
}

/// Reused across matching chunks; output intervals spill once to the dense arm.
struct AlgebraScratch {
    runs: Vec<Run>,
    plane: [u64; CHUNK_WORDS],
    members: Vec<u16>,
}

fn fill_word_span(plane: &mut [u64; CHUNK_WORDS], start: u32, end: u32) {
    let mut at = start;
    while at < end {
        let word = at as usize / 64;
        let next = end.min((word as u32 + 1) * 64);
        let low = at % 64;
        let high = next % 64;
        let mask = (if high == 0 {
            u64::MAX
        } else {
            (1u64 << high) - 1
        }) & (u64::MAX << low);
        plane[word] |= mask;
        at = next;
    }
}

fn append_interval(
    runs: &mut Vec<Run>,
    plane: &mut [u64; CHUNK_WORDS],
    dense: &mut bool,
    len: &mut u32,
    start: u32,
    end: u32,
) {
    if start >= end {
        return;
    }
    let before = *len;
    *len += end - start;
    if *dense {
        fill_word_span(plane, start, end);
        return;
    }
    if let Some(last) = runs.last_mut()
        && u32::from(last.end) + 1 == start
    {
        last.end = (end - 1) as u16;
        return;
    }
    if runs.len() == (WORDS_HEAP_BYTES - 1) / 6 {
        plane.fill(0);
        for run in runs.iter() {
            fill_word_span(plane, u32::from(run.start), u32::from(run.end) + 1);
        }
        runs.clear();
        *dense = true;
        fill_word_span(plane, start, end);
    } else {
        runs.push(Run {
            start: start as u16,
            end: (end - 1) as u16,
            before: before as u16,
        });
    }
}

fn freeze_intervals(scratch: &mut AlgebraScratch, dense: bool, len: u32) -> Option<Container> {
    if len == 0 {
        return None;
    }
    if dense {
        return freeze_chunk(&scratch.plane, &mut scratch.members);
    }
    if len as usize <= ARRAY_MAX {
        scratch.members.clear();
        for run in &scratch.runs {
            scratch.members.extend(run.start..=run.end);
        }
        return freeze_members(&scratch.members, None);
    }
    if scratch.runs.len() == 1 {
        return Some(Container::Stride {
            first: scratch.runs[0].start,
            stride: 1,
            len: (len - 1) as u16,
            magic: 1u64 << 32,
        });
    }
    debug_assert!(matches!(
        choose_candidates(
            len as usize,
            Some((WORDS_HEAP_BYTES, Arm::Words)),
            None,
            None,
            scratch.runs.len()
        ),
        Arm::Runs { .. }
    ));
    Some(Container::Runs {
        runs: scratch.runs.clone().into_boxed_slice(),
        len,
    })
}

/// Sweep run intervals against compact partner spans, never the run's members.
fn combine_runs(
    runs: &[Run],
    other: &Container,
    keep: [bool; 3],
    scratch: &mut AlgebraScratch,
) -> Option<Container> {
    scratch.runs.clear();
    scratch.runs.reserve((WORDS_HEAP_BYTES - 1) / 6);
    let (mut dense, mut len) = (false, 0);
    let (mut i, mut left) = (0, u32::from(runs[0].start));
    let mut emit = |start, end| {
        append_interval(
            &mut scratch.runs,
            &mut scratch.plane,
            &mut dense,
            &mut len,
            start,
            end,
        )
    };
    other.for_each_span(|mut right, end| {
        while i < runs.len() && u32::from(runs[i].end) < right {
            if keep[0] {
                emit(left, u32::from(runs[i].end) + 1);
            }
            i += 1;
            if i < runs.len() {
                left = u32::from(runs[i].start);
            }
        }
        while i < runs.len() && left < end {
            if left < right && keep[0] {
                emit(left, right);
            }
            if right < left && keep[1] {
                emit(right, left);
            }
            let overlap_end = end.min(u32::from(runs[i].end) + 1);
            if keep[2] {
                emit(left.max(right), overlap_end);
            }
            right = overlap_end;
            if overlap_end == u32::from(runs[i].end) + 1 {
                i += 1;
                if i < runs.len() {
                    left = u32::from(runs[i].start);
                }
            } else {
                left = overlap_end;
                break;
            }
        }
        if right < end && keep[1] {
            emit(right, end);
        }
    });
    while i < runs.len() {
        if keep[0] {
            emit(left, u32::from(runs[i].end) + 1);
        }
        i += 1;
        if i < runs.len() {
            left = u32::from(runs[i].start);
        }
    }
    freeze_intervals(scratch, dense, len)
}
#[derive(Clone, Debug)]
enum Container {
    /// Word bitmap + block summary + cumulative popcount directory.
    /// Discriminant 0: the most probe-heavy kind takes the first arm of
    /// every match tree.
    Words {
        words: Box<[u64; CHUNK_WORDS]>,
        summary: [u64; SUMMARY_WORDS],
        /// dir[i] = popcount of words[..i*DIR_STRIDE].
        dir: Box<[u16]>,
        len: u32,
    },
    /// Pure arithmetic progression: first + i*stride, i in 0..=len.
    /// `magic` = ceil(2^32 / stride): freeze-computed reciprocal so probes
    /// never divide (derivable at load; costs no serialized bytes and no
    /// enum size - the Words variant dominates the union).
    Stride {
        first: u16,
        stride: u16,
        len: u16,
        magic: u64,
    },
    /// first + (i*scale >> 32) + residual[i]; |residual| <= eps.
    Cone {
        first: u16,
        scale: u64,
        eps: u8,
        residuals: Box<[i8]>,
    },
    /// Sorted unique values.
    Array(Box<[u16]>),
    Runs {
        runs: Box<[Run]>,
        len: u32,
    },
}

/// Directory group holding the `k`-th member of a `Words` chunk: the largest
/// `g` with `at(g) <= k`, over a monotone cumulative-popcount directory of
/// `groups` entries covering `len` members.
///
/// Seeded by interpolation rather than bisected. `Words` is selected only
/// above `ARRAY_MAX` per-chunk cardinality, so the chunk is dense and its
/// members are close to uniform over the window — the interpolated group is
/// right or nearly right, and the correction gallops to a bracket before
/// bisecting it, which is `O(log error)` instead of `O(log groups)`.
///
/// The distinction is the whole cost of `select` on a dense chunk: a plain
/// bisection over 256 groups is EIGHT SERIAL DEPENDENT LOADS before a single
/// member byte is touched, while the word scan that follows is one or two.
/// Every other probe in this crate predicts and verifies; this one now does
/// too.
#[inline(always)]
fn select_group(groups: usize, len: u32, k: u32, at: impl Fn(usize) -> u32) -> usize {
    debug_assert!(groups > 0 && k < len);
    let seed = ((k as u64 * groups as u64) / (len as u64).max(1)) as usize;
    let seed = seed.min(groups - 1);
    // `at(0)` is 0, so a group satisfying the predicate always exists.
    let (mut lo, mut hi) = if at(seed) <= k {
        if seed + 1 == groups || at(seed + 1) > k {
            return seed;
        }
        let (mut lo, mut step) = (seed + 1, 1usize);
        while lo + step < groups && at(lo + step) <= k {
            lo += step;
            step *= 2;
        }
        (lo, (lo + step).min(groups))
    } else {
        let mut hi = seed;
        let mut step = 1usize;
        while step <= hi && at(hi - step) > k {
            hi -= step;
            step *= 2;
        }
        (hi.saturating_sub(step), hi)
    };
    // Invariant: at(lo) <= k, and hi == groups or at(hi) > k.
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if at(mid) <= k {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    lo
}

impl Container {
    fn for_each_span(&self, mut emit: impl FnMut(u32, u32)) {
        match self {
            Self::Runs { runs, .. } => {
                for run in runs {
                    emit(u32::from(run.start), u32::from(run.end) + 1);
                }
            }
            Self::Words { words, .. } => for_each_word_run(words, emit),
            Self::Stride {
                first,
                stride: 1,
                len,
                ..
            } => {
                emit(u32::from(*first), u32::from(*first) + u32::from(*len) + 1);
            }
            _ => self.for_each(|value| emit(u32::from(value), u32::from(value) + 1)),
        }
    }

    fn cardinality(&self) -> u32 {
        match self {
            Container::Stride { len, .. } => *len as u32 + 1,
            Container::Cone { residuals, .. } => residuals.len() as u32,
            Container::Array(values) => values.len() as u32,
            Container::Words { len, .. } => *len,
            Container::Runs { len, .. } => *len,
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Container::Stride { .. } => 6,
            Container::Cone { residuals, .. } => 16 + residuals.len(),
            Container::Array(values) => 2 * values.len(),
            Container::Words { dir, .. } => 8 * CHUNK_WORDS + 16 + 2 * dir.len(),
            Container::Runs { runs, .. } => 6 * runs.len(),
        }
    }

    /// `floor(delta / stride)` from the frozen reciprocal, no division.
    ///
    /// 32-bit because the per-chunk domain is u16: with
    /// `magic = ceil(2^32 / stride)` the identity holds for every
    /// `delta < 2^16`, since the accumulated reciprocal error
    /// `delta * ((-2^32) mod stride)` is at most `65535 * 65534 < 2^32`.
    /// A 64-bit reciprocal cannot express `stride == 1` - `ceil(2^64/1)` is
    /// unrepresentable - and would force a `u128` multiply on the probe path.
    #[inline(always)]
    fn quotient(delta: u32, magic: u64) -> u32 {
        ((delta as u64 * magic) >> 32) as u32
    }

    #[inline]
    fn cone_value(first: u16, scale: u64, residuals: &[i8], i: usize) -> u16 {
        (first as i64 + ((i as u64 * scale) >> 32) as i64 + residuals[i] as i64) as u16
    }

    #[inline(always)]
    fn contains(&self, v: u16) -> bool {
        match self {
            Container::Runs { runs, .. } => {
                let i = runs.partition_point(|run| run.end < v);
                runs.get(i).is_some_and(|run| run.start <= v)
            }
            Container::Stride {
                first,
                stride,
                len,
                magic,
            } => {
                // Predict-verify with the frozen reciprocal: one mulhi, one
                // mul, no division anywhere on the probe path.
                let (first, stride) = (*first as u32, *stride as u32);
                let v = v as u32;
                if v < first {
                    return false;
                }
                let q = Container::quotient(v - first, *magic);
                q <= *len as u32 && first + q * stride == v
            }
            Container::Cone {
                first,
                scale,
                eps,
                residuals,
            } => {
                let delta = ((v as u64).saturating_sub(*first as u64)) << 32;
                // `checked_div` answers the zero-scale arm itself: None maps
                // to a zero prediction, and a nonzero divisor truncates exactly
                // like the raw division it replaces.
                let guess = delta.checked_div(*scale).unwrap_or(0) as i64;
                let margin = 2 + *eps as i64;
                let lo = (guess - margin).max(0) as usize;
                let hi = ((guess + margin + 1) as usize).min(residuals.len());
                (lo..hi).any(|i| Self::cone_value(*first, *scale, residuals, i) == v)
            }
            Container::Array(values) => values.binary_search(&v).is_ok(),
            // SAFETY: v: u16 so v >> 6 <= 1023 < CHUNK_WORDS = 1024.
            Container::Words { words, .. } => unsafe {
                *words.get_unchecked((v >> 6) as usize) >> (v & 63) & 1 == 1
            },
        }
    }

    /// Count of members strictly below `v`.
    #[inline(always)]
    fn rank_below(&self, v: u16) -> u32 {
        match self {
            Container::Runs { runs, len } => {
                let i = runs.partition_point(|run| run.end < v);
                runs.get(i).map_or(*len, |run| {
                    u32::from(run.before) + u32::from(v.saturating_sub(run.start))
                })
            }
            Container::Stride {
                first, len, magic, ..
            } => {
                let first = *first as u32;
                let v = v as u32;
                if v <= first {
                    0
                } else {
                    (Container::quotient(v - 1 - first, *magic) + 1).min(*len as u32 + 1)
                }
            }
            Container::Cone {
                first,
                scale,
                eps,
                residuals,
            } => {
                // The eps bound guarantees every ordinal before the window's
                // start holds a value < v; walk only the window.
                let delta = ((v as u64).saturating_sub(*first as u64)) << 32;
                // `checked_div` answers the zero-scale arm itself: None maps
                // to a zero prediction, and a nonzero divisor truncates exactly
                // like the raw division it replaces.
                let guess = delta.checked_div(*scale).unwrap_or(0) as i64;
                let margin = 2 + *eps as i64;
                // Clamped at BOTH ends: the model's prediction for a probe
                // above the chunk's last member runs off the residual plane,
                // and an unclamped start would report that prediction as the
                // rank. Every member is then below `v`, so `n` is the answer.
                let start = (guess - margin).max(0).min(residuals.len() as i64) as usize;
                let hi = ((guess + margin + 1) as usize).min(residuals.len());
                let mut at = start;
                while at < hi && Self::cone_value(*first, *scale, residuals, at) < v {
                    at += 1;
                }
                at as u32
            }
            Container::Array(values) => values.partition_point(|x| *x < v) as u32,
            Container::Words { words, dir, .. } => {
                let word = (v >> 6) as usize;
                // SAFETY: word <= 1023, so word / DIR_STRIDE <= 255 <
                // dir.len() = CHUNK_WORDS / DIR_STRIDE = 256 (freeze shape).
                let mut count = unsafe { *dir.get_unchecked(word / DIR_STRIDE) } as u32;
                for w in (word / DIR_STRIDE) * DIR_STRIDE..word {
                    count += words[w].count_ones();
                }
                count + (words[word] & ((1u64 << (v & 63)) - 1)).count_ones()
            }
        }
    }

    /// k-th member (caller guarantees k < cardinality).
    #[inline(always)]
    fn select(&self, k: u32) -> u16 {
        match self {
            Container::Runs { runs, len } => {
                let run = runs[select_group(runs.len(), *len, k, |i| u32::from(runs[i].before))];
                (u32::from(run.start) + k - u32::from(run.before)) as u16
            }
            Container::Stride { first, stride, .. } => (*first as u32 + k * *stride as u32) as u16,
            Container::Cone {
                first,
                scale,
                residuals,
                ..
            } => Self::cone_value(*first, *scale, residuals, k as usize),
            // SAFETY: k < cardinality = values.len() (caller contract,
            // upheld by Bitmosaic::select's cumulative-plane arithmetic).
            Container::Array(values) => unsafe { *values.get_unchecked(k as usize) },
            Container::Words {
                words, dir, len, ..
            } => {
                // SAFETY: select_group returns an index < dir.len().
                let g = select_group(dir.len(), *len, k, |i| unsafe {
                    *dir.get_unchecked(i) as u32
                });
                // SAFETY: g < dir.len() as above.
                let mut remaining = k - unsafe { *dir.get_unchecked(g) } as u32;
                let mut w = g * DIR_STRIDE;
                loop {
                    // SAFETY: k < cardinality (caller contract), so the
                    // cumulative popcount reaches k at some w < CHUNK_WORDS.
                    let word_bits = unsafe { *words.get_unchecked(w) };
                    let ones = word_bits.count_ones();
                    if remaining < ones {
                        let mut word = word_bits;
                        for _ in 0..remaining {
                            word &= word - 1;
                        }
                        return ((w as u32) * 64 + word.trailing_zeros()) as u16;
                    }
                    remaining -= ones;
                    w += 1;
                }
            }
        }
    }

    /// Ascending cursor over this container's members.
    ///
    /// Every arm advances on state it already holds. In particular the
    /// `Words` arm carries the word index and the unconsumed set bits of the
    /// current word, so a full walk is one word load per 64 positions and one
    /// `trailing_zeros` per member, where [`Container::select`] would run
    /// `select_group` plus a word scan for every single one.
    #[inline]
    fn range(&self) -> ContainerRange<'_> {
        match self {
            Container::Runs { runs, len } => ContainerRange::Runs {
                runs,
                at: 0,
                next: u32::from(runs[0].start),
                remaining: *len,
            },
            Container::Words { words, len, .. } => ContainerRange::words(words, *len),
            Container::Stride {
                first, stride, len, ..
            } => ContainerRange::Stride {
                next: *first as u32,
                stride: *stride as u32,
                remaining: *len as u32 + 1,
            },
            Container::Cone {
                first,
                scale,
                residuals,
                ..
            } => ContainerRange::Cone {
                first: *first,
                scale: *scale,
                residuals,
                at: 0,
            },
            Container::Array(values) => ContainerRange::Array(values),
        }
    }

    /// Internal-iteration walk, for the merge kernels that consume a whole
    /// container in one call.
    ///
    /// Kept as its own loop rather than `self.iter().for_each(f)`. The two
    /// walks are the same shape and collapsing them is tempting, but this one
    /// already streamed — the select-per-element defect was `iter`'s alone —
    /// and it sits under `and`/`or`/`xor`/`andnot`. Routing a hot,
    /// already-correct path through a new enum to save a duplicate loop is a
    /// trade with an upside of zero. No wall-clock instrument here can price
    /// the downside either: two builds of the same source disagreed 2.2x on
    /// exactly these ops (`examples/g216_iter_stream.rs`), and whether that
    /// was code layout or machine load was never resolved. Zero upside is
    /// reason enough; the unpriceable downside just closes it.
    fn for_each(&self, mut f: impl FnMut(u16)) {
        match self {
            Container::Runs { runs, .. } => {
                for run in runs {
                    for v in u32::from(run.start)..=u32::from(run.end) {
                        f(v as u16);
                    }
                }
            }
            Container::Stride {
                first, stride, len, ..
            } => {
                let mut v = *first as u32;
                for _ in 0..=*len as u32 {
                    f(v as u16);
                    v += *stride as u32;
                }
            }
            Container::Cone {
                first,
                scale,
                residuals,
                ..
            } => {
                for i in 0..residuals.len() {
                    f(Self::cone_value(*first, *scale, residuals, i));
                }
            }
            Container::Array(values) => values.iter().copied().for_each(f),
            Container::Words { words, .. } => {
                for (w, &word) in words.iter().enumerate() {
                    let mut bits = word;
                    while bits != 0 {
                        f((w as u32 * 64 + bits.trailing_zeros()) as u16);
                        bits &= bits - 1;
                    }
                }
            }
        }
    }
}

// ── Fused AND cardinality (the hot op) ───────────────────────────────────

/// Word-AND popcount over two equal-length word planes. On aarch64+dotprod
/// this hand-rolls the ldp/and/cnt/udot kernel (64 B per iteration, four
/// independent accumulator chains - the exact shape roaring's vectorizer
/// reaches); elsewhere the zip loop lets LLVM choose.
///
/// The length is dynamic because a frozen chunk is span-tight: the wire form
/// stores only the occupied word window, and an intersection sweeps only the
/// overlap of two windows. A whole chunk is 1024 words, so the trip count is
/// bounded and the u16 lane accumulators still cannot overflow.
/// Word-AND popcount over `N` equal-length byte windows at any alignment.
///
/// aarch64+neon runs an `ld1/and/cnt/vpadal` kernel: 64 B per operand per
/// iteration, four independent accumulator chains, no per-word bounds check.
/// `N` is a const so the AND fan-in unrolls — two- and three-term
/// intersections share one kernel instead of the three-way falling back to a
/// scalar zip.
///
/// Windows are BYTES, so a caller reading a frozen segment hands over mapped
/// bytes with no decode. The `u16` lanes take at most 16 per iteration, so
/// the accumulators drain to a `usize` every `NEON_DRAIN` iterations and the
/// window length is unbounded.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
pub fn words_and_count<const N: usize>(windows: [&[u8]; N]) -> usize {
    use core::arch::aarch64::*;
    /// 2048 * 16 = 32,768 <= u16::MAX: one chain's lanes cannot wrap between
    /// drains. Each chain drains through `vaddlvq_u16` (a WIDENING horizontal
    /// add) before any cross-chain arithmetic: a lanewise u16 sum across
    /// chains wraps from 1,024 all-ones blocks (4 x 16,384 = 65,536 > u16),
    /// which undercounted 64 KiB+ windows until the N6 differential caught it.
    const NEON_DRAIN: usize = 2048;

    let blocks = windows.iter().map(|window| window.len()).min().unwrap_or(0) / 64;
    let mut total = 0usize;
    // SAFETY: neon is statically enabled (cfg gate). `blocks` is the floor
    // of the SHORTEST window over 64, and every pointer advances exactly
    // 64 B per iteration for `blocks` iterations, so all four 16 B loads per
    // operand stay inside that operand's slice.
    unsafe {
        let mut heads = windows.map(|window| window.as_ptr());
        let mut done = 0usize;
        while done < blocks {
            let run = (blocks - done).min(NEON_DRAIN);
            let mut chains = [vdupq_n_u16(0); 4];
            for _ in 0..run {
                for (lane, chain) in chains.iter_mut().enumerate() {
                    let at = lane * 16;
                    let mut bits = vld1q_u8(heads[0].add(at));
                    for head in &heads[1..] {
                        bits = vandq_u8(bits, vld1q_u8(head.add(at)));
                    }
                    *chain = vpadalq_u8(*chain, vcntq_u8(bits));
                }
                for head in heads.iter_mut() {
                    *head = head.add(64);
                }
            }
            total = total.saturating_add(
                chains
                    .iter()
                    .map(|&chain| vaddlvq_u16(chain) as usize)
                    .sum(),
            );
            done += run;
        }
    }
    total.saturating_add(words_and_count_scalar(
        windows.map(|window| &window[blocks * 64..]),
    ))
}

/// The wasm32 SIMD128 arm: v128.and fan-in + the i8x16.popcnt composition
/// (`i8x16.popcnt` -> `i16x8.extadd_pairwise` accumulate), the NEON kernel's
/// shape expressed in the wasm vector ISA. Enabled PER FUNCTION with
/// `#[target_feature(enable = "simd128")]` — a `cfg(target_feature)` gate
/// would be dead code in every default build (the G152 lesson: no shipped
/// artifact carries the blanket `-C target-feature=+simd128`). wasm target
/// features are safe to call from safe code: the engine validates the whole
/// module up front, so there is no undetected-feature UB. The opt-out for
/// pre-2021 engines is the build-time `wasm-scalar-portable` feature, same
/// contract as ptmcart's.
#[cfg(all(target_arch = "wasm32", not(feature = "wasm-scalar-portable")))]
mod wasm_simd128 {
    use core::arch::wasm32::{
        u8x16_popcnt, u16x8_add, u16x8_extadd_pairwise_u8x16, u16x8_splat, u32x4_add,
        u32x4_extadd_pairwise_u16x8, u32x4_extract_lane, v128_and, v128_load,
    };

    /// Word-AND popcount over `N` payload windows, 64 B per operand per
    /// iteration, four independent u16x8 accumulator chains. Each lane takes
    /// at most 16 per iteration, so 2048 * 16 = 32,768 <= u16::MAX and one
    /// chain cannot wrap between drains; chains widen to u32 lanes BEFORE
    /// the cross-chain sum (the NEON arm's wrap lesson, applied from birth).
    #[target_feature(enable = "simd128")]
    pub(super) fn words_and_count<const N: usize>(windows: [&[u8]; N]) -> usize {
        const SIMD128_DRAIN: usize = 2048;

        let blocks = windows.iter().map(|window| window.len()).min().unwrap_or(0) / 64;
        let mut total = 0usize;
        // SAFETY: `blocks` is the floor of the SHORTEST window over 64, and
        // every pointer advances exactly 64 B per iteration for `blocks`
        // iterations, so all four 16 B loads per operand stay inside that
        // operand's slice.
        unsafe {
            let mut heads = windows.map(|window| window.as_ptr());
            let mut done = 0usize;
            while done < blocks {
                let run = (blocks - done).min(SIMD128_DRAIN);
                let mut chains = [u16x8_splat(0); 4];
                for _ in 0..run {
                    for (lane, chain) in chains.iter_mut().enumerate() {
                        let at = lane * 16;
                        let mut bits = v128_load(heads[0].add(at).cast());
                        for head in &heads[1..] {
                            bits = v128_and(bits, v128_load(head.add(at).cast()));
                        }
                        *chain = u16x8_add(*chain, u16x8_extadd_pairwise_u8x16(u8x16_popcnt(bits)));
                    }
                    for head in heads.iter_mut() {
                        *head = head.add(64);
                    }
                }
                // `u32x4_extadd_pairwise_u16x8` is a `#[target_feature]` fn
                // item, so it cannot be passed to `array::map`; widen inline.
                let sum = u32x4_add(
                    u32x4_add(
                        u32x4_extadd_pairwise_u16x8(chains[0]),
                        u32x4_extadd_pairwise_u16x8(chains[1]),
                    ),
                    u32x4_add(
                        u32x4_extadd_pairwise_u16x8(chains[2]),
                        u32x4_extadd_pairwise_u16x8(chains[3]),
                    ),
                );
                let drained = u32x4_extract_lane::<0>(sum)
                    + u32x4_extract_lane::<1>(sum)
                    + u32x4_extract_lane::<2>(sum)
                    + u32x4_extract_lane::<3>(sum);
                total = total.saturating_add(drained as usize);
                done += run;
            }
        }
        total.saturating_add(super::words_and_count_scalar(
            windows.map(|window| &window[blocks * 64..]),
        ))
    }
}

#[cfg(all(target_arch = "wasm32", not(feature = "wasm-scalar-portable")))]
#[inline]
pub fn words_and_count<const N: usize>(windows: [&[u8]; N]) -> usize {
    wasm_simd128::words_and_count(windows)
}

#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    all(target_arch = "wasm32", not(feature = "wasm-scalar-portable")),
)))]
#[inline]
pub fn words_and_count<const N: usize>(windows: [&[u8]; N]) -> usize {
    words_and_count_scalar(windows)
}

/// Scalar word-AND popcount. Every window is a whole number of 8-byte words
/// by construction, so `chunks_exact` never leaves a remainder and the
/// bounds work is one cursor advance per operand per word.
#[inline]
pub fn words_and_count_scalar<const N: usize>(windows: [&[u8]; N]) -> usize {
    let mut cursors = windows.map(|window| window.as_chunks::<8>().0.iter());
    let mut total = 0u64;
    'words: loop {
        let mut bits = u64::MAX;
        for cursor in cursors.iter_mut() {
            let Some(word) = cursor.next() else {
                break 'words;
            };
            bits &= u64::from_le_bytes(*word);
        }
        total += u64::from(bits.count_ones());
    }
    usize::try_from(total).unwrap_or(usize::MAX)
}

/// Byte view over a word plane, so the owned ladder feeds the same kernel a
/// frozen segment does.
#[inline]
fn words_as_bytes(words: &[u64]) -> &[u8] {
    // SAFETY: `u64` has no padding and no invalid bit patterns, its alignment
    // (8) exceeds `u8`'s, and the length is exact - `words.len() * 8` bytes are
    // initialised and owned by `words` for the whole borrow.
    unsafe { core::slice::from_raw_parts(words.as_ptr().cast::<u8>(), words.len() * 8) }
}

// ── Skew-adaptive drive: the |A| << |B| arm ──────────────────────────────
//
// Every sequence kernel below is a MERGE: it consumes |A| + |B| positions in
// ascending order, which is optimal at balance and quadratically wasteful
// under skew — a one-member side still pays the whole span of its partner.
// A DRIVER walks the smaller side and gallops into the larger, costing
// |A| * (2 + log2(|B|/|A|)) probes. The merge stays the balanced arm; the
// driver takes over past a measured cardinality ratio.
//
// The choice is made at the CALL SITE from the container tag and the two
// cardinalities, both already in register there. Nothing about it is frozen
// into the bytes or read from configuration: the same pair of containers
// takes whichever arm its own sizes name, on every call.

/// Skew-arm instrument: the balanced-arm force switch and the position
/// counters the A/B harness reads.
///
/// Compiled only under `bench-internals` (or `cfg(test)`). Counters publish
/// ONCE per kernel call from a local tally, never per position — an
/// instrument stepped inside a probe loop prices itself into the arm it is
/// comparing.
///
/// The counters are PER-THREAD, so a census belongs to the thread that
/// produced it and concurrent work elsewhere cannot land in it; they also
/// share no cache line with anything. The arm switch is process-global, on
/// the crate's other kernel-forcing seams: set it once at harness startup.
///
/// SUBJECT of every count: the positions the chooser controls, namely the
/// Array/Cone/Stride sequence pairs and the forest's chunk-key walk. The
/// `Words` arms are excluded because no cardinality ratio moves them — a
/// bitmap probe is O(1) from either side and the word-AND is flat.
#[cfg(any(test, feature = "bench-internals"))]
pub mod skew {
    use core::cell::Cell;
    use core::sync::atomic::{AtomicU8, Ordering};
    use std::thread::LocalKey;

    pub(crate) const AUTO: u8 = 0;
    pub(crate) const BALANCED: u8 = 1;
    pub(crate) const DRIVE: u8 = 2;

    pub(crate) static ARM: AtomicU8 = AtomicU8::new(AUTO);

    thread_local! {
        pub(crate) static LINEAR_POSITIONS: Cell<u64> = const { Cell::new(0) };
        pub(crate) static GALLOP_PROBES: Cell<u64> = const { Cell::new(0) };
        pub(crate) static KEY_POSITIONS: Cell<u64> = const { Cell::new(0) };
        pub(crate) static KEY_PROBES: Cell<u64> = const { Cell::new(0) };
    }

    /// Add to one counter, tolerating a thread whose locals are already
    /// torn down — a destructor is not a reason to abort a kernel.
    #[inline(always)]
    pub(crate) fn add(counter: &'static LocalKey<Cell<u64>>, n: u64) {
        let _ = counter.try_with(|cell| cell.set(cell.get() + n));
    }

    fn read(counter: &'static LocalKey<Cell<u64>>) -> u64 {
        counter.try_with(|cell| cell.get()).unwrap_or(0)
    }

    /// Positions and probes charged on THIS thread since the last [`reset`].
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct SkewCensus {
        /// Container-level positions consumed by a merge or scan arm.
        pub linear_positions: u64,
        /// Container-level probes issued by a drive arm.
        pub gallop_probes: u64,
        /// Chunk-key positions consumed by the linear forest walk.
        pub key_positions: u64,
        /// Chunk-key probes issued by the driven forest walk.
        pub key_probes: u64,
    }

    impl SkewCensus {
        /// Every position and probe the chooser controls, as one number.
        pub fn total(&self) -> u64 {
            self.linear_positions + self.gallop_probes + self.key_positions + self.key_probes
        }
    }

    /// Which arm the choosers take, ignoring cardinality.
    ///
    /// Three states, not two, because a crossover has two sides: forcing
    /// the DRIVE arm below the ratio is the only way to show that the merge
    /// deserves the range it keeps. Two states can only show that the
    /// chooser declined there, which is a statement about the chooser.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum Arm {
        /// The shipped chooser: cardinality ratio decides.
        #[default]
        Auto,
        /// Always merge.
        Balanced,
        /// Always drive from the smaller side.
        Drive,
    }

    /// Pin every chooser to one arm. Both arms then live in ONE binary and
    /// one process run, so an A/B over this switch cancels code layout,
    /// link order and frequency drift.
    ///
    /// Process-global: a harness sets it around the arm it is timing, and
    /// concurrent callers see the same arm.
    pub fn force(arm: Arm) {
        ARM.store(
            match arm {
                Arm::Auto => AUTO,
                Arm::Balanced => BALANCED,
                Arm::Drive => DRIVE,
            },
            Ordering::Relaxed,
        );
    }

    /// Zero this thread's counters.
    pub fn reset() {
        for counter in [
            &LINEAR_POSITIONS,
            &GALLOP_PROBES,
            &KEY_POSITIONS,
            &KEY_PROBES,
        ] {
            let _ = counter.try_with(|cell| cell.set(0));
        }
    }

    /// Read this thread's counters.
    pub fn census() -> SkewCensus {
        SkewCensus {
            linear_positions: read(&LINEAR_POSITIONS),
            gallop_probes: read(&GALLOP_PROBES),
            key_positions: read(&KEY_POSITIONS),
            key_probes: read(&KEY_PROBES),
        }
    }
}

/// Whether a `small`-sized side should drive into a `large`-sized one.
///
/// Without the instrument this is exactly the cardinality test: no switch
/// load, no branch, and the merge kernels stay reachable as the balanced
/// arm rather than a disabled fallback.
#[cfg(not(any(test, feature = "bench-internals")))]
#[inline(always)]
fn ratio_says_drive(small: usize, large: usize, ratio: usize) -> bool {
    large >= small * ratio
}

/// Under the instrument the forced arms override the ratio. Forced DRIVE
/// still respects DIRECTION — it drives from the genuinely smaller side —
/// so a bracket row below the crossover measures the arm the chooser would
/// have taken there, not an arbitrary orientation.
#[cfg(any(test, feature = "bench-internals"))]
#[inline(always)]
fn ratio_says_drive(small: usize, large: usize, ratio: usize) -> bool {
    match skew::ARM.load(core::sync::atomic::Ordering::Relaxed) {
        skew::BALANCED => false,
        skew::DRIVE => small <= large,
        _ => large >= small * ratio,
    }
}

/// Probe tally for one drive-arm call.
///
/// Zero-sized without the instrument, so the shipped kernel carries no
/// counter arithmetic at all. One `add` per probe on a probe-bound loop is
/// a real cost, and an instrument that charges the path it witnesses cannot
/// price that path.
#[cfg(any(test, feature = "bench-internals"))]
#[derive(Default)]
struct Probes(usize);

#[cfg(not(any(test, feature = "bench-internals")))]
struct Probes;

impl Probes {
    // The instrument's zero value. A named constructor keeps the call sites
    // identical across both cfgs without `Default` on a unit struct.
    #[inline(always)]
    fn zero() -> Self {
        #[cfg(any(test, feature = "bench-internals"))]
        {
            Probes(0)
        }
        #[cfg(not(any(test, feature = "bench-internals")))]
        {
            Probes
        }
    }
}

impl Probes {
    #[inline(always)]
    fn bump(&mut self) {
        #[cfg(any(test, feature = "bench-internals"))]
        {
            self.0 += 1;
        }
    }
}

/// Publish a container-level probe tally.
///
/// Guarded on non-zero: charging a counter from a kernel that probed
/// nothing bills a path the instrument did not measure.
#[inline(always)]
fn charge_gallop(_tally: Probes) {
    #[cfg(any(test, feature = "bench-internals"))]
    {
        if _tally.0 != 0 {
            skew::add(&skew::GALLOP_PROBES, _tally.0 as u64);
        }
    }
}

/// Publish a chunk-key probe tally.
#[inline(always)]
fn charge_key_probes(_tally: Probes) {
    #[cfg(any(test, feature = "bench-internals"))]
    {
        if _tally.0 != 0 {
            skew::add(&skew::KEY_PROBES, _tally.0 as u64);
        }
    }
}

/// Publish the positions a container-level merge or scan arm consumed.
#[inline(always)]
fn charge_linear(_positions: usize) {
    #[cfg(any(test, feature = "bench-internals"))]
    {
        if _positions != 0 {
            skew::add(&skew::LINEAR_POSITIONS, _positions as u64);
        }
    }
}

/// Publish the chunk-key positions the linear forest walk consumed.
#[inline(always)]
fn charge_key_positions(_positions: usize) {
    #[cfg(any(test, feature = "bench-internals"))]
    {
        if _positions != 0 {
            skew::add(&skew::KEY_POSITIONS, _positions as u64);
        }
    }
}

// Two crossovers, because there are two merges with different per-position
// costs, and one constant cannot describe both.
//
// A merge consumes `|A| + |B|` positions; a driver issues about
// `|A| * (2 + log2(|B|/|A|))` probes. Setting those equal gives the
// break-even only if a position and a probe cost the same, and they do not:
// a probe is a dependent load that breaks the chain, while a position's
// cost depends entirely on which merge is consuming it. So the ratio is
// bracketed per level in `examples/g261_skew_census.rs` rather than derived
// once and reused.
//
// Both are container-shape constants of the same family as [`ARRAY_MAX`]
// and [`CONE_MAX_LEN`]: nothing reads them from configuration, they never
// reach published bytes, and the operands they compare are cardinalities
// already in register at the call site.

/// Member-level crossover, for the sequence kernels inside a chunk.
///
/// Their merges retire 8 members per SIMD block, so a position is worth a
/// fraction of a probe and the crossover lands high: `(1 + r) / 8 = 2 +
/// log2 r` predicts `r ≈ 64`, and the bracket agrees.
const DRIVE_RATIO_MEMBERS: usize = 32;

/// Key-level crossover, for the forest's chunk-key walk.
///
/// That merge is scalar — one key compare and one unpredictable branch per
/// position, no blocking to amortize — so a position costs MORE than a
/// probe, not less, and the crossover collapses to the smallest ratio that
/// is not parity. The bracket finds no forest size at which the merge wins
/// above ratio 1: at ratio 2 the driver takes 1.72x (null 2.2%) over 32
/// chunks, and at ratio 1 the two arms read 0.93x-0.99x, where galloping is
/// strictly extra work for a walk that matches every key.
///
/// The algebraic estimate for this level (`1 + r = 2 + log2 r`, `r ≈ 4`)
/// is the one the measurement contradicted, in the direction the estimate
/// could not model: a mispredicted branch per merge position.
const DRIVE_RATIO_KEYS: usize = 2;

/// Whether a `small`-sized side should drive into a `large`-sized one at
/// the given crossover.
///
/// A power-of-two ratio keeps the test a shift and a compare. An empty side
/// stays on the merge arms, which return zero without touching either
/// operand; the driver would have to special-case it.
#[inline(always)]
fn drive_from_small(small: usize, large: usize, ratio: usize) -> bool {
    small != 0 && ratio_says_drive(small, large, ratio)
}

/// First index at or after `from` whose value is `>= target`, or `len`.
///
/// Exponential probe from the cursor, then bisect the bracket: `O(log d)`
/// in the distance travelled rather than `O(log n)` in the sequence. A
/// `binary_search` per member would pay full depth on every step and give
/// back most of what driving from the smaller side buys.
///
/// `at` is an index accessor rather than a slice so a `Cone` and a `Stride`
/// drive the same kernel an `Array` does: both are `O(1)`-indexable and
/// ascending by freeze construction, random-access sorted sequences that
/// compute their members instead of storing them.
#[inline(always)]
fn gallop(
    len: usize,
    from: usize,
    target: u16,
    at: impl Fn(usize) -> u16,
    tally: &mut Probes,
) -> usize {
    if from >= len {
        return len;
    }
    tally.bump();
    if at(from) >= target {
        return from;
    }
    // `at(from) < target`: bracket a position that is not below it. `lo`
    // and every `probe` stay below `len`, and `step` at most doubles past
    // `len` once before the guard fires, so the sum cannot overflow.
    let (mut lo, mut step) = (from, 1usize);
    let mut hi = loop {
        let probe = lo + step;
        if probe >= len {
            break len;
        }
        tally.bump();
        if at(probe) >= target {
            break probe;
        }
        lo = probe;
        step <<= 1;
    };
    // Invariant: at(lo) < target, and hi == len or at(hi) >= target.
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        tally.bump();
        if at(mid) < target {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    hi
}

/// Members `small` shares with `large`, driven from the small side: one
/// galloping probe per small member, resumed from the previous landing.
///
/// Threading the cursor rather than restarting is what makes the total
/// `O(|small| * log(|large|/|small|))` — the gaps actually crossed — instead
/// of `|small| * log|large|`.
///
/// Generic over both accessors so every container pair monomorphizes to a
/// direct loop; an enum dispatch per element measures 10x worse on exactly
/// these shapes.
#[inline(always)]
fn gallop_drive(
    small_len: usize,
    small_at: impl Fn(usize) -> u16,
    large_len: usize,
    large_at: impl Fn(usize) -> u16,
) -> u32 {
    let mut tally = Probes::zero();
    let (mut count, mut cursor) = (0u32, 0usize);
    for i in 0..small_len {
        let target = small_at(i);
        cursor = gallop(large_len, cursor, target, &large_at, &mut tally);
        if cursor == large_len {
            break;
        }
        // `cursor` is the first position not below `target`, so equality is
        // the only possible hit; a hit consumes the position, because every
        // later target is strictly greater.
        let hit = large_at(cursor) == target;
        count += hit as u32;
        cursor += hit as usize;
    }
    charge_gallop(tally);
    count
}

/// Walk the smaller sorted key sequence against the larger by galloping,
/// calling `on_match` with BOTH indices at every shared key.
///
/// [`gallop_drive`] cannot serve a forest: a matched key has to reach the
/// two containers behind it, so the caller needs the index pair rather than
/// a tally. Owned and mapped forests share this one walk — the key planes
/// differ only in how a key is read, which is the accessor's job.
#[inline(always)]
fn gallop_walk(
    small_len: usize,
    small_at: impl Fn(usize) -> u16,
    large_len: usize,
    large_at: impl Fn(usize) -> u16,
    mut on_match: impl FnMut(usize, usize),
) {
    let mut tally = Probes::zero();
    let mut cursor = 0usize;
    for i in 0..small_len {
        let key = small_at(i);
        cursor = gallop(large_len, cursor, key, &large_at, &mut tally);
        if cursor == large_len {
            break;
        }
        if large_at(cursor) == key {
            on_match(i, cursor);
            cursor += 1;
        }
    }
    charge_key_probes(tally);
}

/// A `Stride`'s member sequence as an index accessor.
///
/// `stride >= 1` by construction: `freeze_members` is the only `Stride`
/// constructor, it hands a single-member chunk to `Array` and takes the
/// stride arm only when the common difference is non-zero, so no clamp is
/// needed here or at any other stride consumer. `first + i * stride <=
/// u16::MAX` holds for every `i <= len` by the same construction, so the
/// `u32` arithmetic never wraps on the way back down.
#[inline(always)]
fn stride_at(first: u16, stride: u16, i: usize) -> u16 {
    debug_assert!(stride >= 1, "freeze_members never emits a zero stride");
    (first as u32 + i as u32 * stride as u32) as u16
}

/// Array x Array: branchless merge at balance, small-side drive under skew.
///
/// SAFETY (every drive arm below, here and in the two functions that
/// follow): `gallop_drive` indexes the driving side over `0..small_len`,
/// and the driven side only at a cursor `gallop` has already compared
/// against `large_len`.
#[inline(always)]
fn and_arrays(x: &[u16], y: &[u16]) -> u32 {
    if drive_from_small(x.len(), y.len(), DRIVE_RATIO_MEMBERS) {
        return gallop_drive(
            x.len(),
            |i| unsafe { *x.get_unchecked(i) },
            y.len(),
            |i| unsafe { *y.get_unchecked(i) },
        );
    }
    if drive_from_small(y.len(), x.len(), DRIVE_RATIO_MEMBERS) {
        return gallop_drive(
            y.len(),
            |i| unsafe { *y.get_unchecked(i) },
            x.len(),
            |i| unsafe { *x.get_unchecked(i) },
        );
    }
    merge_and_arrays(x, y)
}

/// Cone x Array. The cone reconstructs on the fly on every arm; under skew
/// whichever side is smaller drives.
#[inline(always)]
fn and_cone_array(fa: u16, sa: u64, ra: &[i8], b: &[u16]) -> u32 {
    if drive_from_small(ra.len(), b.len(), DRIVE_RATIO_MEMBERS) {
        return gallop_drive(
            ra.len(),
            |i| Container::cone_value(fa, sa, ra, i),
            b.len(),
            |i| unsafe { *b.get_unchecked(i) },
        );
    }
    if drive_from_small(b.len(), ra.len(), DRIVE_RATIO_MEMBERS) {
        return gallop_drive(
            b.len(),
            |i| unsafe { *b.get_unchecked(i) },
            ra.len(),
            |i| Container::cone_value(fa, sa, ra, i),
        );
    }
    merge_cone_array(fa, sa, ra, b)
}

/// Cone x Cone.
#[inline(always)]
fn and_cones(fa: u16, sa: u64, ra: &[i8], fb: u16, sb: u64, rb: &[i8]) -> u32 {
    if drive_from_small(ra.len(), rb.len(), DRIVE_RATIO_MEMBERS) {
        return gallop_drive(
            ra.len(),
            |i| Container::cone_value(fa, sa, ra, i),
            rb.len(),
            |i| Container::cone_value(fb, sb, rb, i),
        );
    }
    if drive_from_small(rb.len(), ra.len(), DRIVE_RATIO_MEMBERS) {
        return gallop_drive(
            rb.len(),
            |i| Container::cone_value(fb, sb, rb, i),
            ra.len(),
            |i| Container::cone_value(fa, sa, ra, i),
        );
    }
    merge_and_cones(fa, sa, ra, fb, sb, rb)
}

fn and_count(a: &Container, b: &Container) -> u32 {
    use Container::*;
    match (a, b) {
        (Runs { runs, .. }, other) | (other, Runs { runs, .. }) => and_runs(runs, other),
        // Closed form: zero data touched (measured 6,644x vs merge).
        (
            Stride {
                first: a0,
                stride: s1,
                len: n1,
                ..
            },
            Stride {
                first: b0,
                stride: s2,
                len: n2,
                ..
            },
        ) => stride_and_closed(
            *a0 as u32,
            *s1 as u32,
            *n1 as u32 + 1,
            *b0 as u32,
            *s2 as u32,
            *n2 as u32 + 1,
        ),
        // Summary-steered word AND, self-gated by the summary popcount.
        (
            Words {
                words: wa,
                summary: sa,
                len: la,
                ..
            },
            Words {
                words: wb,
                summary: sb,
                len: lb,
                ..
            },
        ) => {
            let mut count = 0u32;
            // Dense pair: both cardinalities in-register from the variant -
            // skip the summary loads and their dependency entirely.
            let dense = *la as usize + *lb as usize >= CHUNK_WORDS * 32;
            // Measured break-even: flat NEON sweep ~168 ns/chunk vs steered
            // ~3.5 ns/live-block -> steer only below ~32 live blocks.
            let live = if dense {
                u32::MAX
            } else {
                (sa[0] & sb[0]).count_ones() + (sa[1] & sb[1]).count_ones()
            };
            if live >= 32 {
                count = words_and_count([words_as_bytes(&wa[..]), words_as_bytes(&wb[..])])
                    .try_into()
                    .unwrap_or(u32::MAX);
            } else {
                for half in 0..SUMMARY_WORDS {
                    let mut mask = sa[half] & sb[half];
                    while mask != 0 {
                        let block = half * 64 + mask.trailing_zeros() as usize;
                        mask &= mask - 1;
                        let base = block * BLOCK_WORDS;
                        // SAFETY: block <= 127, so base + BLOCK_WORDS <=
                        // 128 * 8 = CHUNK_WORDS (summary shape at freeze).
                        let (xa, xb) = unsafe {
                            (
                                wa.get_unchecked(base..base + BLOCK_WORDS),
                                wb.get_unchecked(base..base + BLOCK_WORDS),
                            )
                        };
                        for (x, y) in xa.iter().zip(xb.iter()) {
                            count += (x & y).count_ones();
                        }
                    }
                }
            }
            count
        }
        // Enumerating side probes the bitmap side (measured 12x vs gallop).
        // Monomorphic loops: roaring's probe kernel is 8 instrs with zero
        // value-branches; a for_each closure was a `bl` per element.
        (Array(x), Words { words, .. }) | (Words { words, .. }, Array(x)) => {
            let mut count = 0u32;
            for &v in x.iter() {
                // SAFETY: v: u16 so v >> 6 <= 1023 < CHUNK_WORDS.
                count +=
                    (unsafe { *words.get_unchecked((v >> 6) as usize) } >> (v & 63) & 1) as u32;
            }
            count
        }
        (
            Stride {
                first, stride, len, ..
            },
            Words { words, .. },
        )
        | (
            Words { words, .. },
            Stride {
                first, stride, len, ..
            },
        ) => {
            let mut count = 0u32;
            let mut v = *first as u32;
            for _ in 0..=*len as u32 {
                // SAFETY: v stays in u16 range by freeze construction
                // (first + len*stride <= u16::MAX), so v >> 6 <= 1023.
                count +=
                    (unsafe { *words.get_unchecked((v >> 6) as usize) } >> (v & 63) & 1) as u32;
                v += *stride as u32;
            }
            count
        }
        (
            Cone {
                first,
                scale,
                residuals,
                ..
            },
            Words { words, .. },
        )
        | (
            Words { words, .. },
            Cone {
                first,
                scale,
                residuals,
                ..
            },
        ) => {
            let mut count = 0u32;
            for i in 0..residuals.len() {
                let v = Container::cone_value(*first, *scale, residuals, i);
                // SAFETY: v: u16 so v >> 6 <= 1023 < CHUNK_WORDS.
                count +=
                    (unsafe { *words.get_unchecked((v >> 6) as usize) } >> (v & 63) & 1) as u32;
            }
            count
        }
        // Irregular x irregular: branchless merge (measured) until the
        // cardinality ratio names the drive arm.
        (Array(x), Array(y)) => and_arrays(x, y),
        // Stride x enumerable. The stride side is O(1)-probeable, so the
        // enumerable side drives at balance; under skew that direction
        // inverts and the far shorter progression gallops instead.
        (
            s @ Stride {
                first, stride, len, ..
            },
            Array(y),
        )
        | (
            Array(y),
            s @ Stride {
                first, stride, len, ..
            },
        ) => {
            let ns = *len as usize + 1;
            if drive_from_small(ns, y.len(), DRIVE_RATIO_MEMBERS) {
                let (f, st) = (*first, *stride);
                gallop_drive(
                    ns,
                    |i| stride_at(f, st, i),
                    y.len(),
                    // SAFETY: the cursor is compared against `y.len()`
                    // inside `gallop` before any access.
                    |i| unsafe { *y.get_unchecked(i) },
                )
            } else {
                // Probe the stride arithmetically per element (mulhi
                // predict-verify, ~2 ns/elem, no merge loop, no dispatch).
                let mut count = 0u32;
                for &v in y.iter() {
                    count += s.contains(v) as u32;
                }
                charge_linear(y.len());
                count
            }
        }
        (
            s @ Stride {
                first, stride, len, ..
            },
            c @ Cone {
                first: fc,
                scale: sc,
                residuals: rc,
                ..
            },
        )
        | (
            c @ Cone {
                first: fc,
                scale: sc,
                residuals: rc,
                ..
            },
            s @ Stride {
                first, stride, len, ..
            },
        ) => {
            let ns = *len as usize + 1;
            if drive_from_small(ns, rc.len(), DRIVE_RATIO_MEMBERS) {
                let (f, st, fc, sc) = (*first, *stride, *fc, *sc);
                gallop_drive(
                    ns,
                    |i| stride_at(f, st, i),
                    rc.len(),
                    |i| Container::cone_value(fc, sc, rc, i),
                )
            } else {
                let mut count = 0u32;
                c.for_each(|v| count += s.contains(v) as u32);
                charge_linear(rc.len());
                count
            }
        }
        // Regular x regular: monomorphic branchy merges (enum dispatch per
        // element measured 10x worse than direct reconstruction loops).
        (
            Cone {
                first: fa,
                scale: sa,
                residuals: ra,
                ..
            },
            Cone {
                first: fb,
                scale: sb,
                residuals: rb,
                ..
            },
        ) => and_cones(*fa, *sa, ra, *fb, *sb, rb),
        (
            Cone {
                first: fa,
                scale: sa,
                residuals: ra,
                ..
            },
            Array(y),
        ) => and_cone_array(*fa, *sa, ra, y),
        (
            Array(x),
            Cone {
                first: fb,
                scale: sb,
                residuals: rb,
                ..
            },
        ) => and_cone_array(*fb, *sb, rb, x),
    }
}

fn merge_and_cones(fa: u16, sa: u64, ra: &[i8], fb: u16, sb: u64, rb: &[i8]) -> u32 {
    let (na, nb) = (ra.len(), rb.len());
    let (mut i, mut j, mut count) = (0usize, 0usize, 0u32);
    let (mut va, mut vb) = (
        Container::cone_value(fa, sa, ra, 0),
        Container::cone_value(fb, sb, rb, 0),
    );
    loop {
        if va == vb {
            count += 1;
            i += 1;
            j += 1;
            if i >= na || j >= nb {
                break;
            }
            va = Container::cone_value(fa, sa, ra, i);
            vb = Container::cone_value(fb, sb, rb, j);
        } else if va < vb {
            i += 1;
            if i >= na {
                break;
            }
            va = Container::cone_value(fa, sa, ra, i);
        } else {
            j += 1;
            if j >= nb {
                break;
            }
            vb = Container::cone_value(fb, sb, rb, j);
        }
    }
    charge_linear(i + j);
    count
}

fn merge_cone_array(fa: u16, sa: u64, ra: &[i8], b: &[u16]) -> u32 {
    let (na, nb) = (ra.len(), b.len());
    let (mut i, mut j, mut count) = (0usize, 0usize, 0u32);
    let mut va = Container::cone_value(fa, sa, ra, 0);
    loop {
        if j >= nb {
            break;
        }
        let vb = b[j];
        if va == vb {
            count += 1;
            i += 1;
            j += 1;
            if i >= na {
                break;
            }
            va = Container::cone_value(fa, sa, ra, i);
        } else if va < vb {
            i += 1;
            if i >= na {
                break;
            }
            va = Container::cone_value(fa, sa, ra, i);
        } else {
            j += 1;
        }
    }
    charge_linear(i + j);
    count
}

#[cfg(all(
    target_arch = "aarch64",
    target_feature = "neon",
    target_endian = "little"
))]
fn merge_and_arrays(a: &[u16], b: &[u16]) -> u32 {
    // SAFETY: u16 slices contain initialized bytes for exactly twice their element counts.
    let (a, b) = unsafe {
        (
            core::slice::from_raw_parts(a.as_ptr().cast::<u8>(), a.len() * 2),
            core::slice::from_raw_parts(b.as_ptr().cast::<u8>(), b.len() * 2),
        )
    };
    merge_and_array_bytes(a, b)
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub(crate) fn merge_and_array_bytes(a: &[u8], b: &[u8]) -> u32 {
    use core::arch::aarch64::*;
    let (na, nb) = (a.len() / 2, b.len() / 2);
    let (mut i, mut j) = (0usize, 0usize);
    let mut count = 0u32;
    // Block phase: 8-lane all-pairs equality. Each a-lane can match at most
    // one B value ever (sorted unique), and a block is re-examined only
    // while its partner side advances past disjoint values, so no lane is
    // double-counted (classic SIMD shuffling intersection).
    // SAFETY: each byte load reads 16 bytes under the eight-element guard;
    // vld1q_u8 imposes no alignment requirement and creates no Rust u16 reference.
    unsafe {
        while i + 8 <= na && j + 8 <= nb {
            let va = vreinterpretq_u16_u8(vld1q_u8(a.as_ptr().add(2 * i)));
            let vb = vreinterpretq_u16_u8(vld1q_u8(b.as_ptr().add(2 * j)));
            let mut eq = vceqq_u16(va, vb);
            let mut rot = vb;
            rot = vextq_u16(rot, rot, 1);
            eq = vorrq_u16(eq, vceqq_u16(va, rot));
            rot = vextq_u16(rot, rot, 1);
            eq = vorrq_u16(eq, vceqq_u16(va, rot));
            rot = vextq_u16(rot, rot, 1);
            eq = vorrq_u16(eq, vceqq_u16(va, rot));
            rot = vextq_u16(rot, rot, 1);
            eq = vorrq_u16(eq, vceqq_u16(va, rot));
            rot = vextq_u16(rot, rot, 1);
            eq = vorrq_u16(eq, vceqq_u16(va, rot));
            rot = vextq_u16(rot, rot, 1);
            eq = vorrq_u16(eq, vceqq_u16(va, rot));
            rot = vextq_u16(rot, rot, 1);
            eq = vorrq_u16(eq, vceqq_u16(va, rot));
            count += vaddvq_u16(vandq_u16(eq, vdupq_n_u16(1))) as u32;
            let amax = wire::read_u16_at(a, 2 * (i + 7));
            let bmax = wire::read_u16_at(b, 2 * (j + 7));
            i += 8 * (amax <= bmax) as usize;
            j += 8 * (bmax <= amax) as usize;
        }
    }
    charge_linear(i + j);
    count
        + merge_and_scalar_by(
            na - i,
            nb - j,
            |at| wire::read_u16_at(a, 2 * (i + at)),
            |at| wire::read_u16_at(b, 2 * (j + at)),
        )
}

#[cfg(not(all(
    target_arch = "aarch64",
    target_feature = "neon",
    target_endian = "little"
)))]
#[inline]
fn merge_and_arrays(a: &[u16], b: &[u16]) -> u32 {
    merge_and_scalar(a, b)
}

#[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
pub(crate) fn merge_and_array_bytes(a: &[u8], b: &[u8]) -> u32 {
    merge_and_scalar_by(
        a.len() / 2,
        b.len() / 2,
        |i| wire::read_u16_at(a, 2 * i),
        |i| wire::read_u16_at(b, 2 * i),
    )
}

#[cfg(not(all(
    target_arch = "aarch64",
    target_feature = "neon",
    target_endian = "little"
)))]
fn merge_and_scalar(a: &[u16], b: &[u16]) -> u32 {
    merge_and_scalar_by(a.len(), b.len(), |i| a[i], |i| b[i])
}

fn merge_and_scalar_by(
    na: usize,
    nb: usize,
    a: impl Fn(usize) -> u16,
    b: impl Fn(usize) -> u16,
) -> u32 {
    // Branchy three-way merge, hoisted lens (bounds checks provably elided).
    // Speculation beats a csel dependency chain here: the branch breaks the
    // loop-carried compare->advance latency (measured 1.7 ns/iter branchless
    // vs 0.7 ns/iter for this shape - the same lesson as the g98 audit).
    if na == 0 || nb == 0 {
        return 0;
    }
    let (mut i, mut j, mut count) = (0usize, 0usize, 0u32);
    loop {
        let (x, y) = (a(i), b(j));
        if x < y {
            i += 1;
            if i == na {
                break;
            }
        } else if x > y {
            j += 1;
            if j == nb {
                break;
            }
        } else {
            count += 1;
            i += 1;
            j += 1;
            if i == na || j == nb {
                break;
            }
        }
    }
    charge_linear(i + j);
    count
}

fn stride_and_closed(a0: u32, s1: u32, n1: u32, b0: u32, s2: u32, n2: u32) -> u32 {
    fn ext_gcd(a: i64, b: i64) -> (i64, i64, i64) {
        if b == 0 {
            (a, 1, 0)
        } else {
            let (g, x, y) = ext_gcd(b, a % b);
            (g, y, x - (a / b) * y)
        }
    }
    let (g, x, _) = ext_gcd(s1 as i64, s2 as i64);
    let diff = b0 as i64 - a0 as i64;
    if diff % g != 0 {
        return 0;
    }
    let lcm = (s1 as i64 / g) * s2 as i64;
    let mut first = a0 as i64 + ((diff / g) * x).rem_euclid(s2 as i64 / g) * s1 as i64;
    let start = (a0 as i64).max(b0 as i64);
    if first < start {
        first += ((start - first + lcm - 1) / lcm) * lcm;
    }
    let end =
        (a0 as i64 + (n1 as i64 - 1) * s1 as i64).min(b0 as i64 + (n2 as i64 - 1) * s2 as i64);
    if first > end {
        0
    } else {
        ((end - first) / lcm + 1) as u32
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The ladder: freeze staged words into the best container by exact bytes.
// ─────────────────────────────────────────────────────────────────────────

fn freeze_chunk(words: &[u64; CHUNK_WORDS], scratch: &mut Vec<u16>) -> Option<Container> {
    // Closed-form capacity: the popcount IS the size (G89) - the scratch is
    // the freeze arena, reused across chunks and freezes, never grown after
    // its high-water mark.
    let n: u32 = words.iter().map(|w| w.count_ones()).sum();
    if n as usize > ARRAY_MAX {
        let first_word = words
            .iter()
            .position(|word| *word != 0)
            .expect("nonempty dense plane");
        let first = first_word * 64 + words[first_word].trailing_zeros() as usize;
        return Some(match choose_dense_arm(words, first, n as usize) {
            Arm::Stride { first, stride } => Container::Stride {
                first,
                stride,
                len: (n - 1) as u16,
                magic: (1u64 << 32).div_ceil(u64::from(stride)),
            },
            Arm::Runs { count } => Container::Runs {
                runs: runs_from_words(words, count),
                len: n,
            },
            Arm::Words => freeze_words(words, n),
            _ => unreachable!("dense chooser offers only stride, runs and words"),
        });
    }
    scratch.clear();
    scratch.reserve(n as usize);
    for (w, &word) in words.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            scratch.push((w as u32 * 64 + bits.trailing_zeros()) as u16);
            bits &= bits - 1;
        }
    }
    freeze_members(scratch, Some(words))
}

/// The ladder proper, over one chunk's ascending unique members.
///
/// `plane` is the staged dense bitmap when the caller has one. ONLY the Words
/// arm reads it, and below `ARRAY_MAX` that arm cannot win: Words costs
/// `8 * CHUNK_WORDS + 16 + 2 * (CHUNK_WORDS / DIR_STRIDE)` = 8720 bytes FLAT,
/// where an `Array` of n members costs `2n` and is offered up to
/// `n == ARRAY_MAX`, i.e. 8192. Array therefore dominates Words across the
/// whole range where a sparse caller operates, so passing `None` forfeits no
/// arm the selection could have taken.
///
/// That is what lets a chunk holding a handful of members be frozen without
/// an 8 KB plane staged to hold them — the case a pending-fact set hands us,
/// where the plane was two orders of magnitude larger than the data.
fn freeze_members(values: &[u16], plane: Option<&[u64; CHUNK_WORDS]>) -> Option<Container> {
    debug_assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
    debug_assert!(
        plane.is_some() || values.len() <= ARRAY_MAX,
        "a chunk above ARRAY_MAX needs its dense plane to offer the Words arm"
    );
    if values.is_empty() {
        return None;
    }
    let n = values.len();
    Some(match choose_arm(values, plane.is_some()) {
        Arm::Runs { count } => Container::Runs {
            runs: runs_from_members(values, count),
            len: n as u32,
        },
        Arm::Array => Container::Array(values[..].into()),
        Arm::Stride { first, stride } => Container::Stride {
            first,
            stride,
            len: (n - 1) as u16,
            magic: (1u64 << 32).div_ceil(u64::from(stride)),
        },
        Arm::Cone { first, scale, eps } => Container::Cone {
            first,
            scale,
            eps,
            residuals: values
                .iter()
                .enumerate()
                .map(|(i, &v)| cone_residual(first, scale, i, v))
                .collect(),
        },
        Arm::Words => freeze_words(plane.expect("Words requires its plane"), n as u32),
    })
}

fn freeze_words(words: &[u64; CHUNK_WORDS], len: u32) -> Container {
    let mut summary = [0u64; SUMMARY_WORDS];
    let mut dir = Vec::with_capacity(CHUNK_WORDS / DIR_STRIDE);
    let mut running = 0u32;
    for (w, word) in words.iter().enumerate() {
        if w.is_multiple_of(DIR_STRIDE) {
            dir.push(running as u16);
        }
        if *word != 0 {
            let block = w / BLOCK_WORDS;
            summary[block / 64] |= 1u64 << (block % 64);
        }
        running += word.count_ones();
    }
    Container::Words {
        words: Box::new(*words),
        summary,
        dir: dir.into_boxed_slice(),
        len,
    }
}

fn choose_dense_arm(words: &[u64; CHUNK_WORDS], first: usize, n: usize) -> Arm {
    let runs = dense_run_count(words);
    let last = if runs == 1 {
        first + n - 1
    } else {
        let last_word = words
            .iter()
            .rposition(|word| *word != 0)
            .expect("nonempty dense plane");
        last_word * 64 + 63 - words[last_word].leading_zeros() as usize
    };
    choose_dense_facts(first, last, n, runs, |value| {
        words[value / 64] >> (value % 64) & 1 != 0
    })
}

fn choose_dense_facts(
    first: usize,
    last: usize,
    n: usize,
    runs: usize,
    has: impl Fn(usize) -> bool,
) -> Arm {
    if runs == 1 {
        return Arm::Stride {
            first: first as u16,
            stride: 1,
        };
    }
    if (last - first).is_multiple_of(n - 1) {
        let stride = (last - first) / (n - 1);
        if stride != 0 && (1..n).all(|i| has(first + i * stride)) {
            return Arm::Stride {
                first: first as u16,
                stride: stride as u16,
            };
        }
    }
    choose_candidates(n, Some((WORDS_HEAP_BYTES, Arm::Words)), None, None, runs)
}

/// The ladder's verdict for one chunk: which arm, with the parameters that
/// arm needs to be materialised. Both the seal-time freeze and the in-place
/// patch decide through [`choose_arm`] and materialise from this, so there
/// is exactly one ladder — a frozen set and a patched image agree byte for
/// byte because they cannot disagree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Arm {
    Stride { first: u16, stride: u16 },
    Cone { first: u16, scale: u64, eps: u8 },
    Array,
    Words,
    Runs { count: usize },
}

/// In-memory bytes of the `Words` container, the ladder's flat arm.
const WORDS_HEAP_BYTES: usize = 8 * CHUNK_WORDS + 16 + 2 * (CHUNK_WORDS / DIR_STRIDE);

/// The ladder, over one chunk's ascending unique members (`n >= 1`), by
/// exact in-memory bytes among the applicable arms. `words_offered` is
/// whether the caller holds a dense plane; without one the Words arm is
/// not on the ladder, which forfeits nothing below `ARRAY_MAX` (see
/// [`freeze_members`]).
pub(crate) fn choose_arm(values: &[u16], words_offered: bool) -> Arm {
    let n = values.len();
    debug_assert!(n >= 1);
    if n == 1 {
        return Arm::Array;
    }

    // Arm 1: pure stride.
    let stride = u32::from(values[1] - values[0]);
    if stride > 0 && n <= 65_536 && values.windows(2).all(|w| u32::from(w[1] - w[0]) == stride) {
        return Arm::Stride {
            first: values[0],
            stride: stride as u16,
        };
    }

    // Arm 2: cone fit (op-aware cap: see CONE_MAX_LEN).
    let cone = cone_fit(values).map(|(scale, eps)| {
        (
            16 + n,
            Arm::Cone {
                first: values[0],
                scale,
                eps,
            },
        )
    });

    // Arm 3: array (sparse irregular).
    let array = (n <= ARRAY_MAX).then_some((2 * n, Arm::Array));

    // Arm 4: words + summary + directory. Only reachable with a dense plane.
    let words = words_offered.then_some((WORDS_HEAP_BYTES, Arm::Words));

    choose_candidates(
        n,
        words,
        cone,
        array,
        values
            .windows(2)
            .filter(|pair| u32::from(pair[1]) != u32::from(pair[0]) + 1)
            .count()
            + 1,
    )
}

fn choose_candidates(
    n: usize,
    words: Option<(usize, Arm)>,
    cone: Option<(usize, Arm)>,
    array: Option<(usize, Arm)>,
    run_count: usize,
) -> Arm {
    debug_assert!(n != 0);
    let mut best = words
        .or(cone)
        .or(array)
        .expect("one non-run candidate applies");
    for candidate in [
        cone,
        array,
        Some((6 * run_count, Arm::Runs { count: run_count })),
    ]
    .into_iter()
    .flatten()
    {
        if candidate.0 < best.0 {
            best = candidate;
        }
    }
    best.1
}

fn and_runs(runs: &[Run], other: &Container) -> u32 {
    match other {
        Container::Runs { runs: right, .. } => run_overlap_count(
            runs.len(),
            |i| (u32::from(runs[i].start), u32::from(runs[i].end) + 1),
            right.len(),
            |i| (u32::from(right[i].start), u32::from(right[i].end) + 1),
        ),
        Container::Stride {
            first, stride, len, ..
        } => runs
            .iter()
            .map(|run| {
                progression_in_span(
                    u32::from(run.start),
                    u32::from(run.end) + 1,
                    u32::from(*first),
                    u32::from(*stride),
                    u32::from(*len) + 1,
                )
            })
            .sum(),
        Container::Words { len, .. } => runs
            .iter()
            .map(|run| {
                let after = if run.end == u16::MAX {
                    *len
                } else {
                    other.rank_below(run.end + 1)
                };
                after - other.rank_below(run.start)
            })
            .sum(),
        Container::Array(values) => runs_sequence_count(
            runs.len(),
            |i| (runs[i].start, runs[i].end),
            values.len(),
            |i| values[i],
        ),
        Container::Cone {
            first,
            scale,
            residuals,
            ..
        } => runs_sequence_count(
            runs.len(),
            |i| (runs[i].start, runs[i].end),
            residuals.len(),
            |i| Container::cone_value(*first, *scale, residuals, i),
        ),
    }
}

fn progression_in_span(start: u32, end: u32, first: u32, stride: u32, len: u32) -> u32 {
    let lo = start.saturating_sub(first).div_ceil(stride).min(len);
    let hi = end.saturating_sub(first).div_ceil(stride).min(len);
    hi - lo
}

fn run_overlap_count(
    na: usize,
    a: impl Fn(usize) -> (u32, u32),
    nb: usize,
    b: impl Fn(usize) -> (u32, u32),
) -> u32 {
    let (mut i, mut j, mut count) = (0, 0, 0);
    while i < na && j < nb {
        let (alo, ahi) = a(i);
        let (blo, bhi) = b(j);
        count += ahi.min(bhi).saturating_sub(alo.max(blo));
        if ahi <= bhi {
            i += 1;
        }
        if bhi <= ahi {
            j += 1;
        }
    }
    count
}

fn runs_sequence_count(
    nr: usize,
    run: impl Fn(usize) -> (u16, u16),
    nv: usize,
    value: impl Fn(usize) -> u16,
) -> u32 {
    let (mut r, mut v, mut count) = (0, 0, 0u32);
    let mut probes = Probes::zero();
    while r < nr && v < nv {
        let (start, end) = run(r);
        v = gallop(nv, v, start, &value, &mut probes);
        if v == nv {
            break;
        }
        if value(v) > end {
            r = gallop(nr, r, value(v), |i| run(i).1, &mut probes);
        } else {
            let after = if end == u16::MAX {
                nv
            } else {
                gallop(nv, v, end + 1, &value, &mut probes)
            };
            count += (after - v) as u32;
            v = after;
            r += 1;
        }
    }
    charge_gallop(probes);
    count
}

/// The cone model over `values` (`n >= 2`): `(scale, eps)` when every
/// residual is within `CONE_MAX_EPS`, else `None`. Refutes on the first
/// residual that does not fit, so irregular data costs a few steps.
pub(crate) fn cone_fit(values: &[u16]) -> Option<(u64, u8)> {
    let n = values.len();
    if n > CONE_MAX_LEN {
        return None;
    }
    let first = values[0];
    let span = u64::from(values[n - 1] - first);
    let scale = (span << 32) / (n as u64 - 1);
    let mut eps = 0i64;
    for (i, &v) in values.iter().enumerate() {
        let r = i64::from(v) - cone_predicted(first, scale, i);
        if r.abs() > CONE_MAX_EPS {
            return None;
        }
        eps = eps.max(r.abs());
    }
    Some((scale, eps as u8))
}

#[inline(always)]
fn cone_predicted(first: u16, scale: u64, i: usize) -> i64 {
    i64::from(first) + ((i as u64 * scale) >> 32) as i64
}

/// Residual of member `i` = `v` under the cone model; the fit above proved
/// it lies within `CONE_MAX_EPS`, so the narrowing is exact.
#[inline(always)]
pub(crate) fn cone_residual(first: u16, scale: u64, i: usize, v: u16) -> i8 {
    (i64::from(v) - cone_predicted(first, scale, i)) as i8
}

// ─────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────

/// Members a chunk stages inline before it takes the 8 KB dense plane.
///
/// The plane costs 8,192 bytes zeroed the instant it is taken, whatever the
/// chunk turns out to hold. Inline members cost `2n` bytes of an existing
/// allocation and a sorted insert is a bisect plus a memmove of at most
/// `2n`, so the plane only pays once accumulated shifting would exceed
/// zeroing it — around thirty members on the measured host. Thirty-two also
/// caps an idle chunk at 64 bytes, which is what makes a scattered sparse
/// set free: it stages in the chunk vector's own allocation and touches the
/// allocator not at all.
const STAGE_INLINE_MAX: usize = 32;

/// One chunk's staged members.
///
/// This is the staging half of the container ladder `freeze_members` applies
/// at seal time. Without it a chunk holding three members costs the same
/// 8 KB as one holding sixty thousand, because a `Words` plane is a flat
/// allocation whatever its occupancy. `Forest64Sink` bounds the same cost
/// from the other side by never holding more than one chunk open, which is
/// available only to an ASCENDING streaming caller; a membership set takes
/// values in any order and needs the ladder instead.
enum StagedChunk {
    /// Ascending members, inline, no allocation.
    Inline {
        len: u8,
        values: [u16; STAGE_INLINE_MAX],
    },
    /// The dense plane, reached only by a chunk that earned it.
    Words(Box<[u64; CHUNK_WORDS]>),
}

impl Default for StagedChunk {
    fn default() -> Self {
        Self::Inline {
            len: 0,
            values: [0; STAGE_INLINE_MAX],
        }
    }
}

impl StagedChunk {
    /// Stage `low`, returning whether it was NEW.
    #[inline]
    fn insert(&mut self, low: u16) -> bool {
        match self {
            Self::Inline { len, values } => {
                let n = usize::from(*len);
                let Err(at) = values[..n].binary_search(&low) else {
                    return false;
                };
                if n < STAGE_INLINE_MAX {
                    values.copy_within(at..n, at + 1);
                    values[at] = low;
                    *len = (n + 1) as u8;
                    return true;
                }
                // Earned the plane. Replay the inline members into it once;
                // they are the only members this chunk has.
                let mut plane = Box::new([0u64; CHUNK_WORDS]);
                for &value in values.iter() {
                    plane[(value >> 6) as usize] |= 1u64 << (value & 63);
                }
                plane[(low >> 6) as usize] |= 1u64 << (low & 63);
                *self = Self::Words(plane);
                true
            }
            Self::Words(plane) => {
                let word = &mut plane[(low >> 6) as usize];
                let mask = 1u64 << (low & 63);
                let fresh = *word & mask == 0;
                *word |= mask;
                fresh
            }
        }
    }

    /// Unstage `low`, returning whether it WAS a member.
    ///
    /// The mirror of [`StagedChunk::insert`]: a bisect plus a memmove on the
    /// inline arm, one masked store on the plane. A plane that empties keeps
    /// its storage — the chunk is the arena, exactly as
    /// [`StagedChunk::clear_retaining`] treats it — and `freeze` publishes
    /// nothing for it, so the emptied chunk costs no wire bytes.
    #[inline]
    fn remove(&mut self, low: u16) -> bool {
        match self {
            Self::Inline { len, values } => {
                let n = usize::from(*len);
                let Ok(at) = values[..n].binary_search(&low) else {
                    return false;
                };
                values.copy_within(at + 1..n, at);
                *len = (n - 1) as u8;
                true
            }
            Self::Words(plane) => {
                let word = &mut plane[(low >> 6) as usize];
                let mask = 1u64 << (low & 63);
                let held = *word & mask != 0;
                *word &= !mask;
                held
            }
        }
    }

    #[inline]
    fn contains(&self, low: u16) -> bool {
        match self {
            Self::Inline { len, values } => values[..usize::from(*len)].binary_search(&low).is_ok(),
            Self::Words(plane) => plane[(low >> 6) as usize] >> (low & 63) & 1 == 1,
        }
    }

    /// Empty for the next epoch, keeping whatever storage was earned.
    fn clear_retaining(&mut self) {
        match self {
            Self::Inline { len, .. } => *len = 0,
            Self::Words(plane) => plane.fill(0),
        }
    }

    fn freeze(&self, scratch: &mut Vec<u16>) -> Option<Container> {
        match self {
            // Below `ARRAY_MAX` the Words arm cannot win, so handing
            // `freeze_members` no plane forfeits no arm the ladder could
            // have selected — the container chosen is the one the dense
            // staging would have produced.
            Self::Inline { len, values } => freeze_members(&values[..usize::from(*len)], None),
            Self::Words(plane) => freeze_chunk(plane, scratch),
        }
    }

    /// OR this chunk's members into `dest`, a flat word plane whose word 0
    /// holds values `0..64` of the chunk.
    fn blit_into(&self, dest: &mut [u64]) {
        match self {
            Self::Inline { len, values } => {
                for &value in &values[..usize::from(*len)] {
                    if let Some(word) = dest.get_mut(usize::from(value >> 6)) {
                        *word |= 1u64 << (value & 63);
                    }
                }
            }
            Self::Words(plane) => {
                let span = dest.len().min(CHUNK_WORDS);
                let (Some(head), Some(src)) = (dest.get_mut(..span), plane.get(..span)) else {
                    return;
                };
                for (slot, word) in head.iter_mut().zip(src) {
                    *slot |= *word;
                }
            }
        }
    }
}

/// Mutable staging: a ladder-staged chunk per touched 65,536-value region;
/// [`BitmosaicBuilder::freeze`] runs the seal-time ladder once — seal-once
/// semantics.
#[derive(Default)]
pub struct BitmosaicBuilder {
    chunks: Vec<(u16, StagedChunk)>,
}

impl BitmosaicBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage `value`, returning whether it was NEW.
    ///
    /// "Was it already there" is what this write is about to change — the
    /// answer is a byproduct of the insert, not a second probe. Together
    /// with [`BitmosaicBuilder::contains`] that makes the builder usable as a
    /// mutable membership set (a BFS visited set, a dedup set) with freezing
    /// into a queryable [`Bitmosaic`] afterwards costing nothing extra.
    pub fn insert(&mut self, value: u32) -> bool {
        let key = (value >> 16) as u16;
        let low = (value & 0xFFFF) as u16;
        let at = match self.chunks.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(at) => at,
            Err(at) => {
                self.chunks.insert(at, (key, StagedChunk::default()));
                at
            }
        };
        self.chunks[at].1.insert(low)
    }

    /// Whether `value` is currently staged.
    ///
    /// The gap `README.md` named: before this a caller could not ask the
    /// question without freezing, which discards the mutable form. One key
    /// probe plus one member probe, and it allocates nothing.
    pub fn contains(&self, value: u32) -> bool {
        let key = (value >> 16) as u16;
        let low = (value & 0xFFFF) as u16;
        self.chunks
            .binary_search_by_key(&key, |(k, _)| *k)
            .is_ok_and(|at| self.chunks[at].1.contains(low))
    }

    /// Unstage `value`, returning whether it WAS a member.
    ///
    /// The write answers the membership question as its byproduct, exactly
    /// as [`BitmosaicBuilder::insert`] does. A chunk this empties keeps its
    /// entry and whatever plane it earned — the builder is an arena across
    /// epochs — and [`BitmosaicBuilder::freeze`] publishes no container for it.
    pub fn remove(&mut self, value: u32) -> bool {
        let key = (value >> 16) as u16;
        let low = (value & 0xFFFF) as u16;
        match self.chunks.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(at) => self.chunks[at].1.remove(low),
            Err(_) => false,
        }
    }

    pub fn extend(&mut self, values: impl IntoIterator<Item = u32>) {
        for v in values {
            self.insert(v);
        }
    }

    pub fn freeze(self) -> Bitmosaic {
        let n = self.chunks.len();
        let (mut keys, mut cumulative, mut containers) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let mut running = 0u64;
        let mut scratch: Vec<u16> = Vec::new();
        for (key, chunk) in self.chunks {
            if let Some(container) = chunk.freeze(&mut scratch) {
                running += container.cardinality() as u64;
                keys.push(key);
                cumulative.push(running);
                containers.push(container);
            }
        }
        Bitmosaic {
            keys: keys.into_boxed_slice(),
            cumulative: cumulative.into_boxed_slice(),
            containers: containers.into_boxed_slice(),
        }
    }

    /// Reset for the next epoch WITHOUT releasing chunk storage: the
    /// builder is the arena (zero steady-state allocation across epochs
    /// once the chunk set has reached its high-water mark).
    pub fn clear_retaining(&mut self) {
        for (_, chunk) in &mut self.chunks {
            chunk.clear_retaining();
        }
    }

    /// OR every staged value into `dest`, a flat word plane based at value
    /// zero — one bit per value, `dest[0]` holding `0..64`. Values beyond
    /// `dest` are dropped.
    ///
    /// For callers whose promotion target is ALREADY a flat full-domain
    /// plane. [`BitmosaicBuilder::freeze`] exists to produce a compressed QUERY
    /// form and runs the ladder; a caller widening into its own dense plane
    /// wants neither the ladder nor an element replay through a public
    /// iterator. A chunk that earned its plane blits word-for-word; a chunk
    /// still inline sets its handful of bits.
    pub fn blit_into(&self, dest: &mut [u64]) {
        for (key, chunk) in &self.chunks {
            let base = usize::from(*key) * CHUNK_WORDS;
            let Some(tail) = dest.get_mut(base..) else {
                return;
            };
            chunk.blit_into(tail);
        }
    }
}

/// One-pass overlap statistics (see [`Bitmosaic::overlap_stats`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverlapStats {
    pub and: u64,
    pub or: u64,
    pub xor: u64,
    pub only_self: u64,
    pub only_other: u64,
}

#[inline(always)]
fn jaccard_from_stats(stats: OverlapStats) -> f64 {
    if stats.or == 0 {
        0.0
    } else {
        stats.and as f64 / stats.or as f64
    }
}

#[inline(always)]
fn count_range_from_rank<T: Copy + Ord>(lo: T, hi: T, rank: impl Fn(T) -> u64) -> u64 {
    if hi <= lo {
        return 0;
    }
    rank(hi) - rank(lo)
}

#[inline(always)]
fn predecessor_from_rank<T: Copy>(
    value: T,
    contains: impl Fn(T) -> bool,
    rank: impl Fn(T) -> u64,
    select: impl Fn(u64) -> Option<T>,
) -> Option<T> {
    if contains(value) {
        return Some(value);
    }
    let ordinal = rank(value);
    if ordinal == 0 {
        None
    } else {
        select(ordinal - 1)
    }
}

#[inline(always)]
fn quantile_from_select<T>(len: u64, q: f64, select: impl Fn(u64) -> Option<T>) -> Option<T> {
    if len == 0 || !(0.0..=1.0).contains(&q) {
        return None;
    }
    select((q * (len - 1) as f64).round() as u64)
}

#[inline(always)]
fn sample_from_select<T>(len: u64, seed: u64, select: impl Fn(u64) -> Option<T>) -> Option<T> {
    if len == 0 {
        return None;
    }
    let mut state = seed | 1;
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    select(state % len)
}

#[inline(always)]
fn bucket_counts_from_rank<T, const DOMAIN_BITS: u32>(
    bucket_bits: u32,
    len: u64,
    mut rank: impl FnMut(u128) -> u64,
    base: impl Fn(u128) -> T,
    mut emit: impl FnMut(T, u64),
) {
    assert!((1..=DOMAIN_BITS).contains(&bucket_bits));
    let domain_end = 1u128 << DOMAIN_BITS;
    let bucket_width = 1u128 << bucket_bits;
    let mut lo = 0u128;
    let mut lo_rank = 0u64;
    while lo < domain_end {
        let hi = lo + bucket_width;
        let hi_rank = if hi >= domain_end { len } else { rank(hi) };
        if hi_rank > lo_rank {
            emit(base(lo), hi_rank - lo_rank);
        }
        lo = hi;
        lo_rank = hi_rank;
        if lo_rank >= len {
            break;
        }
    }
}

/// Frozen ladder-selected bitmap. Struct-of-arrays: probes and walks touch
/// the compact `keys`/`cumulative` planes (32 keys or 8 counts per cache
/// line); the fat container payloads are a separate plane reached exactly
/// once per resolved probe (the descent-kit layout, applied to ourselves).
pub struct Bitmosaic {
    keys: Box<[u16]>,
    /// Cardinality up to and INCLUDING chunk i (successor-as-boundary over
    /// count space: per-chunk counts are subtractions, never stored).
    cumulative: Box<[u64]>,
    containers: Box<[Container]>,
}

impl Bitmosaic {
    pub fn from_sorted(values: impl IntoIterator<Item = u32>) -> Bitmosaic {
        let mut builder = BitmosaicBuilder::new();
        builder.extend(values);
        builder.freeze()
    }

    pub fn len(&self) -> u64 {
        self.cumulative.last().copied().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Heap bytes of container payloads (the compression scoreboard).
    pub fn heap_bytes(&self) -> usize {
        self.containers.iter().map(|c| c.heap_bytes() + 12).sum()
    }

    /// Occupied chunk count — the wire directory's row count.
    pub(crate) fn chunk_count(&self) -> u32 {
        self.keys.len() as u32
    }

    pub(crate) fn keys(&self) -> &[u16] {
        &self.keys
    }

    pub(crate) fn containers(&self) -> &[Container] {
        &self.containers
    }

    /// Chunk index holding `key`, if present. Small forests (the common
    /// case) use a branchless linear walk - no binary-search mispredicts.
    #[inline(always)]
    fn locate(&self, key: u16) -> Option<usize> {
        // 64 keys = two cache lines: branchless walk beats binary search.
        if self.keys.len() <= 64 {
            let mut at = 0usize;
            for &k in self.keys.iter() {
                at += (k < key) as usize;
            }
            (at < self.keys.len() && self.keys[at] == key).then_some(at)
        } else {
            self.keys.binary_search(&key).ok()
        }
    }

    #[inline(always)]
    pub fn contains(&self, value: u32) -> bool {
        let key = (value >> 16) as u16;
        let n = self.keys.len();
        // Single-chunk forests (dense sets): nothing but verify + probe.
        if n == 1 {
            // SAFETY: n == 1; keys/containers share length (freeze shape).
            // Bitwise & : the probe is cheap and unconditional, removing a
            // short-circuit branch from the per-probe chain.
            return unsafe {
                (*self.keys.get_unchecked(0) == key)
                    & self
                        .containers
                        .get_unchecked(0)
                        .contains((value & 0xFFFF) as u16)
            };
        }
        if n == 0 {
            return false;
        }
        // Predict-verify: for contiguous chunk keys (any set spanning an
        // unbroken key range - the common shape) the guess is exact and the
        // probe costs one load + compare for ANY forest size.
        // SAFETY: guess is clamped below n = keys.len() = containers.len().
        let first = unsafe { *self.keys.get_unchecked(0) };
        let guess = (key.wrapping_sub(first) as usize).min(n - 1);
        // SAFETY: same clamp - guess = min(.., n-1) < n = keys.len().
        let hit = unsafe { *self.keys.get_unchecked(guess) };
        if hit == key {
            // SAFETY: guess < n as above.
            return unsafe {
                self.containers
                    .get_unchecked(guess)
                    .contains((value & 0xFFFF) as u16)
            };
        }
        if key < first {
            return false;
        }
        // Non-contiguous forest: verified walk.
        let at = if n <= 64 {
            let mut at = 0usize;
            for &k in self.keys.iter() {
                at += (k < key) as usize;
            }
            at
        } else {
            self.keys.partition_point(|&k| k < key)
        };
        if at >= n {
            return false;
        }
        // SAFETY: at < n = keys.len() = containers.len() (freeze shape).
        unsafe {
            *self.keys.get_unchecked(at) == key
                && self
                    .containers
                    .get_unchecked(at)
                    .contains((value & 0xFFFF) as u16)
        }
    }

    /// Count of members strictly below `value`.
    #[inline]
    pub fn rank(&self, value: u32) -> u64 {
        let key = (value >> 16) as u16;
        match self.locate(key) {
            Some(at) => {
                let before = if at == 0 { 0 } else { self.cumulative[at - 1] };
                before + self.containers[at].rank_below((value & 0xFFFF) as u16) as u64
            }
            None => {
                let at = self.keys.partition_point(|&k| k < key);
                if at == 0 { 0 } else { self.cumulative[at - 1] }
            }
        }
    }

    /// k-th member in ascending order.
    #[inline]
    pub fn select(&self, k: u64) -> Option<u32> {
        if k >= self.len() {
            return None;
        }
        let (at, before) = if self.cumulative.len() <= 16 {
            // k < len (checked above), so some entry exceeds k: the walk
            // terminates strictly inside the slice, carrying the running
            // prefix (no re-load, no at == 0 branch).
            let (mut at, mut before) = (0usize, 0u64);
            loop {
                // SAFETY: cumulative.last() = len > k bounds the walk.
                let c = unsafe { *self.cumulative.get_unchecked(at) };
                if c > k {
                    break (at, before);
                }
                before = c;
                at += 1;
            }
        } else {
            let at = self.cumulative.partition_point(|&c| c <= k);
            (at, if at == 0 { 0 } else { self.cumulative[at - 1] })
        };
        // SAFETY: k < len guarantees at < chunk count (cumulative plane).
        let container = unsafe { self.containers.get_unchecked(at) };
        // Priority test for Array (discriminant 3, otherwise last in the
        // match tree): the container kind selects paths on, dominated by
        // sparse forests.
        let low = if let Container::Array(values) = container {
            // SAFETY: k - before < cardinality by cumulative arithmetic.
            unsafe { *values.get_unchecked((k - before) as usize) }
        } else {
            container.select((k - before) as u32)
        };
        // SAFETY: at < chunk count as above.
        Some(((unsafe { *self.keys.get_unchecked(at) } as u32) << 16) | low as u32)
    }

    /// Fused AND cardinality: allocation-free, per-chunk kernel dispatch.
    ///
    /// The chunk-key planes are a sorted `u16` sequence exactly like an
    /// `Array` container, and a skewed pair skews HERE first: a set living
    /// in a handful of chunks otherwise pays the full span of a wide
    /// partner before a single container kernel runs. Its own crossover,
    /// because its merge is scalar where the container merges block.
    pub fn and_len(&self, other: &Bitmosaic) -> u64 {
        let (na, nb) = (self.keys.len(), other.keys.len());
        if drive_from_small(na, nb, DRIVE_RATIO_KEYS) {
            return self.and_len_driven(other);
        }
        if drive_from_small(nb, na, DRIVE_RATIO_KEYS) {
            return other.and_len_driven(self);
        }
        let mut count = 0u64;
        let (mut i, mut j) = (0usize, 0usize);
        while i < na && j < nb {
            // SAFETY: i < na, j < nb by the loop guard; containers share
            // keys' length by freeze construction.
            let (ka, kb) = unsafe { (*self.keys.get_unchecked(i), *other.keys.get_unchecked(j)) };
            if ka == kb {
                count += and_count(
                    // SAFETY: same bounds as above.
                    unsafe { self.containers.get_unchecked(i) },
                    // SAFETY: j < nb (loop guard); freeze-shape lengths.
                    unsafe { other.containers.get_unchecked(j) },
                ) as u64;
                i += 1;
                j += 1;
            } else if ka < kb {
                i += 1;
            } else {
                j += 1;
            }
        }
        charge_key_positions(i + j);
        count
    }

    /// Chunk-pair AND driven from the sparser forest.
    ///
    /// [`and_count`] is symmetric in its operands — every arm counts
    /// `|A ∩ B|` — so the total does not depend on which side drives, and
    /// the caller is free to orient the walk by cardinality alone.
    fn and_len_driven(&self, other: &Bitmosaic) -> u64 {
        let mut count = 0u64;
        gallop_walk(
            self.keys.len(),
            // SAFETY: the driving index runs over `0..self.keys.len()`.
            |i| unsafe { *self.keys.get_unchecked(i) },
            other.keys.len(),
            // SAFETY: `gallop` compares every index against the length it
            // was given before handing it here.
            |j| unsafe { *other.keys.get_unchecked(j) },
            |i, j| {
                // SAFETY: both indices are in range for their key planes,
                // and containers share keys' length by freeze construction.
                count += and_count(unsafe { self.containers.get_unchecked(i) }, unsafe {
                    other.containers.get_unchecked(j)
                }) as u64;
            },
        );
        count
    }

    /// Fused overlap statistics in ONE and_len pass: (and, or, xor,
    /// andnot_self, andnot_other) by inclusion-exclusion over the counts
    /// the containers already carry. Roaring computes these as separate
    /// whole-bitmap passes; here every derived count is arithmetic.
    pub fn overlap_stats(&self, other: &Bitmosaic) -> OverlapStats {
        let and = self.and_len(other);
        let (a, b) = (self.len(), other.len());
        OverlapStats {
            and,
            or: a + b - and,
            xor: a + b - 2 * and,
            only_self: a - and,
            only_other: b - and,
        }
    }

    /// Jaccard similarity from one fused pass (0.0 for two empty sets).
    pub fn jaccard(&self, other: &Bitmosaic) -> f64 {
        jaccard_from_stats(self.overlap_stats(other))
    }

    /// Members in `lo..hi` by rank subtraction: O(chunk walk + container
    /// depth), bounded by the structure rather than the result cardinality.
    pub fn count_range(&self, lo: u32, hi: u32) -> u64 {
        count_range_from_rank(lo, hi, |value| self.rank(value))
    }

    /// Smallest member >= `value`: rank + select, O(depth). roaring-rs
    /// answers this only by constructing a range iterator.
    pub fn successor(&self, value: u32) -> Option<u32> {
        // rank(v) counts members strictly below v, so the member at index
        // rank(v) is the least member >= v - whether or not v is present.
        // One rank + one select; no membership pre-pass.
        self.select(self.rank(value))
    }

    /// Largest member <= `value`, O(depth).
    pub fn predecessor(&self, value: u32) -> Option<u32> {
        predecessor_from_rank(
            value,
            |candidate| self.contains(candidate),
            |candidate| self.rank(candidate),
            |ordinal| self.select(ordinal),
        )
    }

    /// q-quantile member (q in [0,1]), O(depth): select(q * (len-1)).
    pub fn quantile(&self, q: f64) -> Option<u32> {
        quantile_from_select(self.len(), q, |ordinal| self.select(ordinal))
    }

    /// Uniform random member without scanning (select over a seeded draw).
    pub fn sample(&self, seed: u64) -> Option<u32> {
        sample_from_select(self.len(), seed, |ordinal| self.select(ordinal))
    }

    /// Per-bucket counts over 2^bucket_bits-aligned windows via rank
    /// arithmetic: O(buckets x depth) instead of roaring's O(n) scan.
    /// Calls `f(bucket_base, count)` for every non-empty bucket.
    pub fn bucket_counts(&self, bucket_bits: u32, f: impl FnMut(u32, u64)) {
        bucket_counts_from_rank::<u32, 32>(
            bucket_bits,
            self.len(),
            |bound| self.rank(bound as u32),
            |base| base as u32,
            f,
        )
    }

    /// Whole-set arithmetic-progression introspection: Some((first, stride,
    /// len)) when the entire set is one progression. Emergent from the
    /// representation - every chunk must be a compatible Stride container;
    /// roaring would have to scan all members to answer.
    pub fn as_arithmetic(&self) -> Option<(u32, u32, u64)> {
        let mut expect: Option<(u32, u32, u64, u32)> = None;
        for (i, container) in self.containers.iter().enumerate() {
            let Container::Stride {
                first, stride, len, ..
            } = *container
            else {
                return None;
            };
            let base = (self.keys[i] as u32) << 16;
            let (cf, cs, cn) = (base + first as u32, stride as u32, len as u64 + 1);
            match &mut expect {
                None => expect = Some((cf, cs, cn, cf + (cn as u32 - 1) * cs)),
                Some((_, s, n, last)) => {
                    if cs != *s || cf != *last + *s {
                        return None;
                    }
                    *n += cn;
                    *last = cf + (cn as u32 - 1) * cs;
                }
            }
        }
        expect.map(|(f, s, n, _)| (f, s, n))
    }

    /// Materializing intersection, finalizing each matching chunk once.
    pub fn and(&self, other: &Bitmosaic) -> Bitmosaic {
        self.combine(other, [false, false, true])
    }

    pub fn or(&self, other: &Bitmosaic) -> Bitmosaic {
        self.combine(other, [true, true, true])
    }

    pub fn xor(&self, other: &Bitmosaic) -> Bitmosaic {
        self.combine(other, [true, true, false])
    }

    pub fn andnot(&self, other: &Bitmosaic) -> Bitmosaic {
        self.combine(other, [true, false, false])
    }

    fn combine(&self, other: &Self, keep: [bool; 3]) -> Self {
        let (mut keys, mut containers, mut cumulative) = (Vec::new(), Vec::new(), Vec::new());
        let (mut i, mut j, mut total) = (0, 0, 0u64);
        let mut scratch = None;
        while i < self.keys.len() || j < other.keys.len() {
            let left = self.keys.get(i).copied();
            let right = other.keys.get(j).copied();
            let (key, container) = match (left, right) {
                (Some(a), Some(b)) if a == b => {
                    let scratch = scratch.get_or_insert_with(|| AlgebraScratch {
                        runs: Vec::new(),
                        plane: [0; CHUNK_WORDS],
                        members: Vec::new(),
                    });
                    let a = &self.containers[i];
                    let b = &other.containers[j];
                    let result = match (a, b) {
                        (Container::Runs { runs, .. }, _) => combine_runs(runs, b, keep, scratch),
                        (_, Container::Runs { runs, .. }) => {
                            combine_runs(runs, a, [keep[1], keep[0], keep[2]], scratch)
                        }
                        _ => {
                            scratch.plane.fill(0);
                            a.for_each(|v| {
                                if if b.contains(v) { keep[2] } else { keep[0] } {
                                    scratch.plane[usize::from(v) / 64] |= 1u64 << (v % 64);
                                }
                            });
                            if keep[1] {
                                b.for_each(|v| {
                                    if !a.contains(v) {
                                        scratch.plane[usize::from(v) / 64] |= 1u64 << (v % 64);
                                    }
                                });
                            }
                            freeze_chunk(&scratch.plane, &mut scratch.members)
                        }
                    };
                    let key = self.keys[i];
                    i += 1;
                    j += 1;
                    (key, result)
                }
                (Some(a), Some(b)) if a < b => {
                    let result = keep[0].then(|| self.containers[i].clone());
                    i += 1;
                    (a, result)
                }
                (Some(a), None) => {
                    let result = keep[0].then(|| self.containers[i].clone());
                    i += 1;
                    (a, result)
                }
                (_, Some(b)) => {
                    let result = keep[1].then(|| other.containers[j].clone());
                    j += 1;
                    (b, result)
                }
                (None, None) => break,
            };
            if let Some(container) = container {
                total += u64::from(container.cardinality());
                keys.push(key);
                containers.push(container);
                cumulative.push(total);
            }
        }
        Self {
            keys: keys.into_boxed_slice(),
            cumulative: cumulative.into_boxed_slice(),
            containers: containers.into_boxed_slice(),
        }
    }

    /// Ascending iterator over every member. Allocation-free, and streaming
    /// on every arm: a `Words` chunk walks its bit-words rather than
    /// re-entering the select directory per element.
    ///
    /// The SAME cursor [`Bitmosaic::range`] returns — a [`ForestRange`] is both
    /// a [`Range`] and an `ExactSizeIterator`. Two names because the two
    /// call sites read differently: `iter()` where a caller wants the
    /// ecosystem's `Iterator`, `range()` where it wants the merge
    /// primitives or a `save`-able cursor.
    #[inline]
    pub fn iter(&self) -> ForestRange<'_> {
        self.range()
    }

    /// Ascending [`Range`] over every member: the cursor `iter()` drives,
    /// and the source the lazy set adaptors compose over.
    ///
    /// At every non-empty position, `self.len() as usize - range.len()` is
    /// the zero-based ordinal of `range.front()`. The cursor already holds
    /// the remaining count, so this needs no [`Bitmosaic::rank`] call or
    /// directory descent; read it before advancing the range.
    #[inline]
    pub fn range(&self) -> ForestRange<'_> {
        ForestRange::new(self)
    }

    /// Lazy intersection — allocates nothing and materialises nothing.
    ///
    /// [`Bitmosaic::and`] restages and re-ladders a whole new forest;
    /// [`Bitmosaic::and_len`] counts but cannot hand you the values. This is
    /// the third shape, and it is the one that composes: `count()` it,
    /// `collect()` it, feed it to another adaptor, or pull one element.
    ///
    /// Not a replacement for `and_len` on the counting path — `and_len`
    /// dispatches a per-chunk kernel (closed form for stride∩stride, word
    /// popcount for dense) and never looks at a member, where this advances
    /// a cursor per element. See `examples/g217_ranges.rs`.
    ///
    /// While non-empty, [`AndRange::sources`] exposes both source cursors
    /// positioned on the current match. Subtract each source's exact
    /// remaining length from its set length to retain both input ordinals
    /// without re-ranking the value.
    #[inline]
    pub fn and_range<'a>(
        &'a self,
        other: &'a Bitmosaic,
    ) -> AndRange<ForestRange<'a>, ForestRange<'a>> {
        AndRange::leapfrog(self.range(), other.range())
    }

    /// Lazy union — the alloc-free form [`Bitmosaic::or`] never had.
    #[inline]
    pub fn or_range<'a>(
        &'a self,
        other: &'a Bitmosaic,
    ) -> OrRange<ForestRange<'a>, ForestRange<'a>> {
        OrRange::new(self.range(), other.range())
    }

    /// Lazy symmetric difference — the alloc-free form [`Bitmosaic::xor`]
    /// never had.
    #[inline]
    pub fn xor_range<'a>(
        &'a self,
        other: &'a Bitmosaic,
    ) -> XorRange<ForestRange<'a>, ForestRange<'a>> {
        XorRange::new(self.range(), other.range())
    }

    /// Lazy difference (`self` minus `other`) — the alloc-free form
    /// [`Bitmosaic::andnot`] never had.
    #[inline]
    pub fn andnot_range<'a>(
        &'a self,
        other: &'a Bitmosaic,
    ) -> AndNotRange<ForestRange<'a>, ForestRange<'a>> {
        AndNotRange::new(self.range(), other.range())
    }

    /// Container census (stride, cone, array, words, runs) - the ladder's verdict
    /// per dataset, for gates and observability.
    pub fn container_census(&self) -> (usize, usize, usize, usize, usize) {
        let mut census = (0, 0, 0, 0, 0);
        for container in &self.containers {
            match container {
                Container::Stride { .. } => census.0 += 1,
                Container::Cone { .. } => census.1 += 1,
                Container::Array(_) => census.2 += 1,
                Container::Words { .. } => census.3 += 1,
                Container::Runs { .. } => census.4 += 1,
            }
        }
        census
    }
}

// ── u64 tier ─────────────────────────────────────────────────────────────

/// Ladder-selected bitmap over the u64 domain: a sorted plane of high-32
/// prefixes, each owning an [`Bitmosaic`] forest over its low 32 bits. Ids
/// derived from dense ordinals (the engine's EntityId shape) all share
/// high32 = 0, so the common case is exactly one forest - zero overhead
/// against the u32 path. The mirror of roaring's RoaringTreemap, with the
/// same emergent surface (rank/select/successor/bucket arithmetic).
pub struct Bitmosaic64 {
    highs: Box<[u32]>,
    /// Cardinality up to and INCLUDING forest i.
    cumulative: Box<[u64]>,
    forests: Box<[Bitmosaic]>,
}

/// Per-chunk staging: ascending members until the dense plane is cheaper.
///
/// A `Vec<u16>` of n members costs 2n bytes and the plane costs 8192 flat, so
/// they cross at n = `ARRAY_MAX` = 4096 — which is exactly where `Array` stops
/// being offered and `Words` becomes reachable. ONE threshold governs both the
/// staging cost and the arm selection, so there is nothing here to tune.
#[derive(Default)]
struct ChunkStage {
    members: Vec<u16>,
    plane: Option<Box<[u64; CHUNK_WORDS]>>,
    dense: bool,
}

impl ChunkStage {
    /// Stage the next ASCENDING member of the open chunk.
    #[inline]
    fn push(&mut self, low: u16) {
        if let Some(plane) = self.plane.as_mut().filter(|_| self.dense) {
            plane[(low >> 6) as usize] |= 1u64 << (low & 63);
            return;
        }
        self.members.push(low);
        if self.members.len() > ARRAY_MAX {
            let plane = self
                .plane
                .get_or_insert_with(|| Box::new([0u64; CHUNK_WORDS]));
            plane.fill(0);
            for &value in &self.members {
                plane[(value >> 6) as usize] |= 1u64 << (value & 63);
            }
            self.members.clear();
            self.dense = true;
        }
    }

    /// Freeze the open chunk and reset for the next, RETAINING both buffers:
    /// the plane is allocated at most once per build, and only by a build that
    /// actually reaches a dense chunk.
    fn take(&mut self, scratch: &mut Vec<u16>) -> Option<Container> {
        let container = match self.plane.as_ref().filter(|_| self.dense) {
            Some(plane) => freeze_chunk(plane, scratch),
            None => freeze_members(&self.members, None),
        };
        self.members.clear();
        self.dense = false;
        container
    }
}

/// Streaming assembly of an [`Bitmosaic64`] from ASCENDING values.
///
/// A chunk is frozen the moment the walk leaves it and a forest the moment the
/// walk leaves its high-32 group, so peak staging is ONE chunk. That bound is
/// stronger than the builder's staging ladder rather than redundant with it:
/// it is set by the WALK rather than by the data, so a wide high-32 group
/// never stages its whole footprint at once. It is available here only
/// because the input is ascending — a membership set takes values in any
/// order, which is why [`StagedChunk`] carries the per-chunk ladder.
#[derive(Default)]
struct Forest64Sink {
    highs: Vec<u32>,
    cumulative: Vec<u64>,
    forests: Vec<Bitmosaic>,
    running: u64,
    keys: Vec<u16>,
    forest_cumulative: Vec<u64>,
    containers: Vec<Container>,
    forest_running: u64,
    stage: ChunkStage,
    scratch: Vec<u16>,
    /// High-32 prefix and chunk key of the chunk currently staged.
    open: Option<(u32, u16)>,
    last: Option<u64>,
}

impl Forest64Sink {
    /// Absorb the next value. It may not go backwards; equal values collapse,
    /// which is why no caller needs a `dedup` pass.
    fn push(&mut self, value: u64) {
        debug_assert!(
            self.last.is_none_or(|previous| previous <= value),
            "Forest64Sink requires ascending input"
        );
        if self.last == Some(value) {
            return;
        }
        self.last = Some(value);
        let high = (value >> 32) as u32;
        let key = ((value as u32) >> 16) as u16;
        if let Some((open_high, open_key)) = self.open
            && (open_high != high || open_key != key)
        {
            self.close_chunk(open_key);
            if open_high != high {
                self.close_forest(open_high);
            }
        }
        self.open = Some((high, key));
        self.stage.push(value as u16);
    }

    fn close_chunk(&mut self, key: u16) {
        if let Some(container) = self.stage.take(&mut self.scratch) {
            self.forest_running += u64::from(container.cardinality());
            self.keys.push(key);
            self.forest_cumulative.push(self.forest_running);
            self.containers.push(container);
        }
    }

    fn close_forest(&mut self, high: u32) {
        if self.containers.is_empty() {
            return;
        }
        self.running += self.forest_running;
        self.highs.push(high);
        self.cumulative.push(self.running);
        self.forests.push(Bitmosaic {
            keys: std::mem::take(&mut self.keys).into_boxed_slice(),
            cumulative: std::mem::take(&mut self.forest_cumulative).into_boxed_slice(),
            containers: std::mem::take(&mut self.containers).into_boxed_slice(),
        });
        self.forest_running = 0;
    }

    fn finish(mut self) -> Bitmosaic64 {
        if let Some((high, key)) = self.open {
            self.close_chunk(key);
            self.close_forest(high);
        }
        Bitmosaic64 {
            highs: self.highs.into_boxed_slice(),
            cumulative: self.cumulative.into_boxed_slice(),
            forests: self.forests.into_boxed_slice(),
        }
    }
}

impl Bitmosaic64 {
    /// Build from ASCENDING values. Duplicates are tolerated and collapse.
    ///
    /// Streams: each chunk is frozen as the walk leaves it, so peak staging is
    /// one chunk rather than one whole high-32 group, and a chunk holding a
    /// handful of members never stages a dense plane at all.
    pub fn from_sorted(values: impl IntoIterator<Item = u64>) -> Bitmosaic64 {
        let mut sink = Forest64Sink::default();
        for value in values {
            sink.push(value);
        }
        sink.finish()
    }

    /// Build from values in ANY order, duplicates included.
    ///
    /// The u64 tier had only [`Bitmosaic64::from_sorted`], so an unordered caller
    /// ran `sort_unstable` and then `dedup` over its OWN `Vec` and handed the
    /// result across — two passes and two containers to reach one. Here the
    /// ordering happens once over the raw ids, and the deduplication costs
    /// nothing at all: the ascending walk collapses equal neighbours as it
    /// goes, so there is no `dedup` pass to run.
    ///
    /// Empty input never reaches the allocator. An empty `Vec` owns no
    /// storage, nothing is staged, and the three planes are empty boxed slices
    /// over dangling pointers — which is every query against a database with
    /// no uncommitted writes.
    pub fn from_unsorted(values: impl IntoIterator<Item = u64>) -> Bitmosaic64 {
        let mut values: Vec<u64> = values.into_iter().collect();
        values.sort_unstable();
        Self::from_sorted(values)
    }

    pub fn len(&self) -> u64 {
        self.cumulative.last().copied().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.highs.is_empty()
    }

    pub fn heap_bytes(&self) -> usize {
        self.forests.iter().map(|f| f.heap_bytes() + 16).sum()
    }

    pub(crate) fn highs(&self) -> &[u32] {
        &self.highs
    }

    pub(crate) fn forests(&self) -> &[Bitmosaic] {
        &self.forests
    }

    #[inline(always)]
    fn locate(&self, high: u32) -> Option<usize> {
        let n = self.highs.len();
        if n == 0 {
            return None;
        }
        // Predict-verify: dense-ordinal populations have ONE forest (high 0).
        // SAFETY: index clamped below n = highs.len() (build shape).
        let first = unsafe { *self.highs.get_unchecked(0) };
        let guess = (high.wrapping_sub(first) as usize).min(n - 1);
        // SAFETY: guess = min(.., n-1) < n = highs.len().
        if unsafe { *self.highs.get_unchecked(guess) } == high {
            return Some(guess);
        }
        if high < first {
            return None;
        }
        self.highs.binary_search(&high).ok()
    }

    #[inline(always)]
    pub fn contains(&self, value: u64) -> bool {
        self.locate((value >> 32) as u32)
            // SAFETY: locate returns indices < forests.len() (build shape).
            .is_some_and(|at| unsafe { self.forests.get_unchecked(at) }.contains(value as u32))
    }

    /// Count of members strictly below `value`.
    pub fn rank(&self, value: u64) -> u64 {
        let high = (value >> 32) as u32;
        match self.locate(high) {
            Some(at) => {
                let before = if at == 0 { 0 } else { self.cumulative[at - 1] };
                before + self.forests[at].rank(value as u32)
            }
            None => {
                let at = self.highs.partition_point(|&h| h < high);
                if at == 0 { 0 } else { self.cumulative[at - 1] }
            }
        }
    }

    /// k-th member in ascending order.
    pub fn select(&self, k: u64) -> Option<u64> {
        if k >= self.len() {
            return None;
        }
        let at = if self.cumulative.len() <= 16 {
            let mut at = 0usize;
            // SAFETY: k < len bounds the walk inside the slice.
            while unsafe { *self.cumulative.get_unchecked(at) } <= k {
                at += 1;
            }
            at
        } else {
            self.cumulative.partition_point(|&c| c <= k)
        };
        let before = if at == 0 { 0 } else { self.cumulative[at - 1] };
        let low = self.forests[at].select(k - before)?;
        Some(((self.highs[at] as u64) << 32) | low as u64)
    }

    /// Fused AND cardinality across forests (allocation-free).
    pub fn and_len(&self, other: &Bitmosaic64) -> u64 {
        let mut count = 0u64;
        let (mut i, mut j) = (0usize, 0usize);
        let (na, nb) = (self.highs.len(), other.highs.len());
        while i < na && j < nb {
            let (ha, hb) = (self.highs[i], other.highs[j]);
            if ha == hb {
                count += self.forests[i].and_len(&other.forests[j]);
                i += 1;
                j += 1;
            } else if ha < hb {
                i += 1;
            } else {
                j += 1;
            }
        }
        count
    }

    /// Smallest member >= `value` (rank indexes the answer directly).
    pub fn successor(&self, value: u64) -> Option<u64> {
        self.select(self.rank(value))
    }

    /// Ascending iterator over every member, high-32 prefixes restored.
    ///
    /// The u64 tier had no walk of any kind before this — not `iter`, not
    /// `for_each` — which blocked every u64 consumer at the point where it
    /// wanted the values rather than a count. It is a [`ForestRange`] inside
    /// a walk of the high plane, so `Words` chunks stream their bit-words
    /// and nothing here calls `select`.
    #[inline]
    pub fn iter(&self) -> Forest64Range<'_> {
        self.range()
    }

    /// Ascending [`Range`] over every member.
    ///
    /// At every non-empty position, `self.len() as usize - range.len()` is
    /// the zero-based ordinal of `range.front()`, without an
    /// [`Bitmosaic64::rank`] call or directory descent. Read it before advancing
    /// the range.
    #[inline]
    pub fn range(&self) -> Forest64Range<'_> {
        Forest64Range::new(self)
    }

    /// Lazy intersection. The u32 tier got `and`/`or`/`xor`/`andnot` written
    /// out by hand and this tier got none of them; one generic adaptor over
    /// a range gives it all four, which is the whole argument for the
    /// protocol.
    ///
    /// While non-empty, [`AndRange::sources`] exposes both source cursors
    /// positioned on the current match, so both input ordinals are available
    /// from their exact remaining lengths without re-ranking.
    #[inline]
    pub fn and_range<'a>(
        &'a self,
        other: &'a Bitmosaic64,
    ) -> AndRange<Forest64Range<'a>, Forest64Range<'a>> {
        AndRange::leapfrog(self.range(), other.range())
    }

    /// Lazy union.
    #[inline]
    pub fn or_range<'a>(
        &'a self,
        other: &'a Bitmosaic64,
    ) -> OrRange<Forest64Range<'a>, Forest64Range<'a>> {
        OrRange::new(self.range(), other.range())
    }

    /// Lazy symmetric difference.
    #[inline]
    pub fn xor_range<'a>(
        &'a self,
        other: &'a Bitmosaic64,
    ) -> XorRange<Forest64Range<'a>, Forest64Range<'a>> {
        XorRange::new(self.range(), other.range())
    }

    /// Lazy difference (`self` minus `other`).
    #[inline]
    pub fn andnot_range<'a>(
        &'a self,
        other: &'a Bitmosaic64,
    ) -> AndNotRange<Forest64Range<'a>, Forest64Range<'a>> {
        AndNotRange::new(self.range(), other.range())
    }

    /// One-pass overlap statistics by inclusion-exclusion.
    pub fn overlap_stats(&self, other: &Bitmosaic64) -> OverlapStats {
        let and = self.and_len(other);
        let (a, b) = (self.len(), other.len());
        OverlapStats {
            and,
            or: a + b - and,
            xor: a + b - 2 * and,
            only_self: a - and,
            only_other: b - and,
        }
    }

    /// Jaccard similarity from one fused pass (0.0 for two empty sets).
    pub fn jaccard(&self, other: &Bitmosaic64) -> f64 {
        jaccard_from_stats(self.overlap_stats(other))
    }

    /// Members in `lo..hi` by two rank calls. No member walk is performed.
    pub fn count_range(&self, lo: u64, hi: u64) -> u64 {
        count_range_from_rank(lo, hi, |value| self.rank(value))
    }

    /// Largest member <= `value`, O(depth).
    pub fn predecessor(&self, value: u64) -> Option<u64> {
        predecessor_from_rank(
            value,
            |candidate| self.contains(candidate),
            |candidate| self.rank(candidate),
            |ordinal| self.select(ordinal),
        )
    }

    /// q-quantile member (q in [0,1]), O(depth): select(q * (len-1)).
    pub fn quantile(&self, q: f64) -> Option<u64> {
        quantile_from_select(self.len(), q, |ordinal| self.select(ordinal))
    }

    /// Uniform random member without scanning (select over a seeded draw).
    pub fn sample(&self, seed: u64) -> Option<u64> {
        sample_from_select(self.len(), seed, |ordinal| self.select(ordinal))
    }

    /// Per-bucket counts over 2^bucket_bits-aligned windows via rank
    /// arithmetic. The u128 bounds represent the exclusive end of the u64
    /// domain, including the final bucket containing [`u64::MAX`].
    /// Calls `f(bucket_base, count)` for every non-empty bucket.
    pub fn bucket_counts(&self, bucket_bits: u32, f: impl FnMut(u64, u64)) {
        bucket_counts_from_rank::<u64, 64>(
            bucket_bits,
            self.len(),
            |bound| self.rank(bound as u64),
            |base| base as u64,
            f,
        )
    }

    /// Intersect two sorted u64 id slices (the engine's semijoin shape)
    /// without building anything: chunk-free merged walk reusing the SIMD
    /// array kernel whenever both runs sit inside one 2^16 block.
    pub fn intersect_sorted(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
        out.clear();
        let (mut i, mut j) = (0usize, 0usize);
        while i < a.len() && j < b.len() {
            let (x, y) = (a[i], b[j]);
            if x < y {
                i += 1;
            } else if x > y {
                j += 1;
            } else {
                out.push(x);
                i += 1;
                j += 1;
            }
        }
    }
}

/// Words min/max refuter instrument: the force switch that reproduces the
/// pre-rider seek, and the counters that price it without a clock.
///
/// Compiled only under `bench-internals` (or `cfg(test)`), for the reason the
/// [`skew`] instrument gives: a load and a branch in front of two compares is
/// a cost on the very path being witnessed, and in a shipped build the
/// refuter is unconditional.
///
/// The acceptance this serves is COUNTS-FIRST and box-free. `rank_words` is
/// the exact number of word- and directory-plane loads `rank_below` charged
/// to `Words` chunks; running the same seek workload with the arm forced
/// [`Arm::Off`] and then [`Arm::Auto`] makes the difference between the two
/// totals the deleted work, in loads, with no timing assumption in it.
/// Counters publish ONCE per call from a local tally, never per position.
#[cfg(any(test, feature = "bench-internals"))]
pub mod refuter {
    use core::cell::Cell;
    use core::sync::atomic::{AtomicU8, Ordering};

    pub(crate) const AUTO: u8 = 0;
    pub(crate) const OFF: u8 = 1;

    pub(crate) static ARM: AtomicU8 = AtomicU8::new(AUTO);

    thread_local! {
        pub(crate) static SKIPPED: Cell<u64> = const { Cell::new(0) };
        pub(crate) static HEADED: Cell<u64> = const { Cell::new(0) };
        pub(crate) static WALKED: Cell<u64> = const { Cell::new(0) };
        pub(crate) static RANK_WORDS: Cell<u64> = const { Cell::new(0) };
    }

    /// Whether a seek consults the min/max plane. Process-global like the
    /// skew switch: set it once per arm at harness level, never mid-cell.
    #[inline(always)]
    pub(crate) fn armed() -> bool {
        ARM.load(Ordering::Relaxed) == AUTO
    }

    #[inline(always)]
    pub(crate) fn bump(counter: &'static std::thread::LocalKey<Cell<u64>>, n: u64) {
        let _ = counter.try_with(|cell| cell.set(cell.get() + n));
    }

    fn read(counter: &'static std::thread::LocalKey<Cell<u64>>) -> u64 {
        counter.try_with(|cell| cell.get()).unwrap_or(0)
    }

    /// Which seek path runs. `Off` is not "no refuter written" — the bytes
    /// are in the payload either way, because the wire format has no arms.
    /// It is the READ routed as it was before the rider existed, which is
    /// the only control that isolates the lever from the encoding.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum Arm {
        /// Consult min/max; fall through to `rank_below` only when the probe
        /// lands strictly inside the chunk.
        #[default]
        Auto,
        /// Always `rank_below`, whatever the head line says.
        Off,
    }

    /// Chunk verdicts and plane loads charged on THIS thread since [`reset`].
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct RefuterCensus {
        /// Chunks the refuter rejected outright: probe above the last member.
        pub skipped: u64,
        /// Chunks answered at rank 0: probe at or below the first member.
        pub headed: u64,
        /// Chunks the probe landed inside, which must pay `rank_below`.
        pub walked: u64,
        /// Word- and directory-plane loads `rank_below` charged to `Words`.
        pub rank_words: u64,
    }

    pub fn set_arm(arm: Arm) {
        ARM.store(
            if matches!(arm, Arm::Off) { OFF } else { AUTO },
            Ordering::Relaxed,
        );
    }

    pub fn reset() {
        for c in [&SKIPPED, &HEADED, &WALKED, &RANK_WORDS] {
            let _ = c.try_with(|cell| cell.set(0));
        }
    }

    pub fn census() -> RefuterCensus {
        RefuterCensus {
            skipped: read(&SKIPPED),
            headed: read(&HEADED),
            walked: read(&WALKED),
            rank_words: read(&RANK_WORDS),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn shapes() -> Vec<(&'static str, Vec<u32>)> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        vec![
            (
                "uniform-sparse",
                (0..3000)
                    .map(|_| xorshift(&mut state) as u32 % 500_000)
                    .collect(),
            ),
            ("strided", (0..20_000u32).map(|i| i * 13).collect()),
            (
                "jittered-stride",
                (0..20_000u32)
                    .map(|i| i * 11 + (xorshift(&mut state) % 3) as u32)
                    .collect(),
            ),
            (
                "clustered",
                (0..40u32)
                    .flat_map(|c| {
                        let base = c * 100_000;
                        (0..800u32).map(move |i| base + i * 2)
                    })
                    .collect(),
            ),
            (
                "dense-chunk",
                (0..60_000u32)
                    .map(|i| i.wrapping_mul(2_654_435_761) % 65_536)
                    .collect(),
            ),
            ("empty", Vec::new()),
            ("single", vec![42]),
        ]
    }

    #[test]
    fn membership_rank_select_match_reference() {
        for (name, values) in shapes() {
            let reference: BTreeSet<u32> = values.iter().copied().collect();
            let frozen = Bitmosaic::from_sorted(values.iter().copied());
            assert_eq!(frozen.len(), reference.len() as u64, "{name}: len");
            let mut state = 0xDEAD_BEEFu64;
            for &v in reference.iter().take(500) {
                assert!(frozen.contains(v), "{name}: present {v}");
            }
            for _ in 0..500 {
                let probe = xorshift(&mut state) as u32 % 600_000;
                assert_eq!(
                    frozen.contains(probe),
                    reference.contains(&probe),
                    "{name}: probe {probe}"
                );
            }
            for (k, &v) in reference.iter().enumerate() {
                if k % 97 == 0 {
                    assert_eq!(frozen.select(k as u64), Some(v), "{name}: select {k}");
                    assert_eq!(frozen.rank(v), k as u64, "{name}: rank {v}");
                }
            }
            assert_eq!(frozen.select(frozen.len()), None, "{name}: select end");
        }
    }

    #[test]
    fn binary_ops_match_reference() {
        let all = shapes();
        for (na, va) in &all {
            for (nb, vb) in &all {
                let ra: BTreeSet<u32> = va.iter().copied().collect();
                let rb: BTreeSet<u32> = vb.iter().copied().collect();
                let fa = Bitmosaic::from_sorted(va.iter().copied());
                let fb = Bitmosaic::from_sorted(vb.iter().copied());
                let and_ref: Vec<u32> = ra.intersection(&rb).copied().collect();
                assert_eq!(fa.and_len(&fb), and_ref.len() as u64, "{na} AND {nb} len");
                assert_eq!(
                    fa.and(&fb).iter().collect::<Vec<_>>(),
                    and_ref,
                    "{na} AND {nb}"
                );
                let or_ref: Vec<u32> = ra.union(&rb).copied().collect();
                assert_eq!(
                    fa.or(&fb).iter().collect::<Vec<_>>(),
                    or_ref,
                    "{na} OR {nb}"
                );
                let xor_ref: Vec<u32> = ra.symmetric_difference(&rb).copied().collect();
                assert_eq!(
                    fa.xor(&fb).iter().collect::<Vec<_>>(),
                    xor_ref,
                    "{na} XOR {nb}"
                );
                let andnot_ref: Vec<u32> = ra.difference(&rb).copied().collect();
                assert_eq!(
                    fa.andnot(&fb).iter().collect::<Vec<_>>(),
                    andnot_ref,
                    "{na} ANDNOT {nb}"
                );
            }
        }
    }

    /// Per-arm differential for the `words_and_count` kernel family (N6 ISA
    /// arms): the dispatched arm (NEON on aarch64, SIMD128 on wasm32, scalar
    /// elsewhere) must agree with the scalar reference on every fixture,
    /// including the drain boundary. The all-ones shapes at and past
    /// `DRAIN = 2048` blocks are the accumulator-wrap refuters: at 2048
    /// blocks every u16 lane of a chain holds exactly 32,768, so any
    /// lanewise u16 arithmetic ACROSS chains wraps to zero and undercounts
    /// by 65,536 per wrapped lane — a differential miss, not a panic.
    #[test]
    fn words_and_count_matches_scalar_on_adversarial_windows() {
        let mut state = 0xA5A5_5A5A_DEAD_BEEFu64;
        // (label, whole 64 B blocks, tail bytes). Tails exercise the scalar rim.
        let shapes: &[(&str, usize, usize)] = &[
            ("empty", 0, 0),
            ("one-word-tail", 0, 8),
            ("half-block", 0, 32),
            ("one-block", 1, 0),
            ("chunk", 128, 0),
            ("chunk-tail", 128, 24),
            ("pre-drain", 2047, 8),
            ("drain-exact", 2048, 0),
            ("post-drain", 2049, 16),
            ("two-drains", 4096, 8),
        ];
        for &(label, blocks, tail) in shapes {
            let len = blocks * 64 + tail;
            let ones = vec![0xFFu8; len];
            let zeros = vec![0u8; len];
            let random: Vec<u8> = (0..len).map(|_| xorshift(&mut state) as u8).collect();
            let alternating: Vec<u8> = (0..len)
                .map(|i| if i % 2 == 0 { 0xAA } else { 0x55 })
                .collect();
            let operands: &[(&str, &[u8])] = &[
                ("ones", &ones),
                ("zeros", &zeros),
                ("random", &random),
                ("alternating", &alternating),
            ];
            for (la, a) in operands {
                for (lb, b) in operands {
                    let expect = words_and_count_scalar([*a, *b]);
                    assert_eq!(words_and_count([*a, *b]), expect, "{label}: {la} AND {lb}");
                    for (lc, c) in operands {
                        let expect3 = words_and_count_scalar([*a, *b, *c]);
                        assert_eq!(
                            words_and_count([*a, *b, *c]),
                            expect3,
                            "{label}: {la} AND {lb} AND {lc}"
                        );
                    }
                }
            }
            // Negative half: the oracle can fail. Flip one mid-window bit of
            // an all-ones AND — the dispatched count MUST move by exactly 1.
            if len >= 8 {
                let mut flipped = ones.clone();
                flipped[len / 2] ^= 0x10;
                assert_eq!(
                    words_and_count([flipped.as_slice(), ones.as_slice()]) + 1,
                    words_and_count([ones.as_slice(), ones.as_slice()]),
                    "{label}: single-bit flip must move the count by one"
                );
            }
        }
    }

    #[test]
    fn ladder_picks_expected_containers() {
        let strided = Bitmosaic::from_sorted((0..20_000u32).map(|i| i * 13));
        let (stride, _, _, _, _) = strided.container_census();
        assert!(stride >= 3, "pure strides freeze to Stride containers");

        // Op-aware cap: DENSE jittered strides go to Words (word-AND wins
        // ops); SPARSE jittered strides (n <= CONE_MAX_LEN/chunk) go cones.
        let mut state = 7u64;
        let dense_jitter = Bitmosaic::from_sorted(
            (0..20_000u32).map(|i| i * 11 + (xorshift(&mut state) % 3) as u32),
        );
        let (_, _, _, words, _) = dense_jitter.container_census();
        assert!(
            words >= 3,
            "dense jitter freezes to Words: {:?}",
            dense_jitter.container_census()
        );
        let sparse_jitter = Bitmosaic::from_sorted(
            (0..5_000u32).map(|i| i * 97 + (xorshift(&mut state) % 5) as u32),
        );
        let (_, cones, _, _, _) = sparse_jitter.container_census();
        assert!(
            cones >= 3,
            "sparse jitter freezes to Cones: {:?}",
            sparse_jitter.container_census()
        );
    }

    /// The streaming iterator must agree with random access element for
    /// element, on every arm of the ladder.
    ///
    /// `select` is the reference: it is what `Bitmosaic::select` exposes, what
    /// the `BTreeSet` differential above checks, and what `iter()` itself
    /// used to call per element. `BitmosaicView::for_each` rides along as a
    /// third witness, because the owned and wire walks are separate code and
    /// the README claims they produce the same sequence.
    ///
    /// The census assertion at the end is the liveness proof: a sweep that
    /// never freezes a `Words` chunk cannot certify the arm this test is
    /// about, and every arm here reaches a different cursor.
    #[test]
    fn streaming_iteration_agrees_with_select_on_every_arm() {
        let cases: Vec<(&str, Vec<u32>)> = vec![
            ("empty", Vec::new()),
            ("single", vec![0]),
            ("single-high", vec![7]),
            // Duplicate inserts must collapse to one member on both walks.
            ("repeated", vec![5, 5, 5, 9, 9, 400]),
            // Wide span, few values: one Array container per occupied chunk.
            ("wide-sparse", (0..300u32).map(|i| i * 7919).collect()),
            // Contiguous: the stride-1 progression, the ladder's flagship.
            ("dense-narrow", (0..300u32).collect()),
            // Near-arithmetic under CONE_MAX_LEN: the Cone cursor.
            ("cone", (0..1_000u32).map(|i| i * 61 + i % 5).collect()),
            // One chunk, irregular, far above ARRAY_MAX: a Words container
            // spanning every one of its 256 directory groups, so the walk
            // runs well past the point where `select` would re-interpolate.
            (
                "words-full-chunk",
                (0..65_536u32).filter(|i| i % 37 != 0).collect(),
            ),
            // Multi-chunk Words, so the outer key walk is exercised too.
            (
                "words-multi-chunk",
                (0..200_000u32).filter(|i| i % 5 != 0).collect(),
            ),
            ("u32-boundary", vec![0, u32::MAX - 1, u32::MAX]),
            ("u32-top-chunk", (u32::MAX - 5_000..=u32::MAX).collect()),
        ];

        let mut census = (0usize, 0usize, 0usize, 0usize);
        for (name, values) in cases {
            let unique: Vec<u32> = values
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let frozen = Bitmosaic::from_sorted(values.iter().copied());
            let c = frozen.container_census();
            census = (
                census.0 + c.0,
                census.1 + c.1,
                census.2 + c.2,
                census.3 + c.3,
            );

            let streamed: Vec<u32> = frozen.iter().collect();
            let selected: Vec<u32> = (0..frozen.len())
                .map(|k| frozen.select(k).expect("k < len"))
                .collect();
            assert_eq!(streamed, selected, "{name}: stream vs select, census {c:?}");
            assert_eq!(streamed, unique, "{name}: stream vs input");

            // The wire walk is the third witness (README: view and owned
            // agree element for element).
            let bytes = frozen.to_bytes();
            let view = BitmosaicView::open(&bytes).expect("open");
            let mut walked = Vec::with_capacity(streamed.len());
            view.for_each(|v| walked.push(v));
            assert_eq!(walked, streamed, "{name}: view walk vs owned stream");

            // Length is exact before the walk, and exhaustion is terminal.
            let mut iter = frozen.iter();
            assert_eq!(iter.size_hint().0, unique.len(), "{name}: size_hint");
            for _ in 0..unique.len() {
                assert!(iter.next().is_some(), "{name}: short walk");
            }
            assert_eq!(iter.next(), None, "{name}: overrun");
            assert_eq!(iter.next(), None, "{name}: exhausted iterator restarted");
        }
        // Worthless if it never reached the arm it is about.
        assert!(census.0 > 0, "no case froze a Stride container: {census:?}");
        assert!(census.1 > 0, "no case froze a Cone container: {census:?}");
        assert!(census.2 > 0, "no case froze an Array container: {census:?}");
        assert!(census.3 > 0, "no case froze a Words container: {census:?}");
    }

    /// The magnitude of the streaming fix, as a COUNT rather than a timing.
    ///
    /// A wall-clock ratio expires the moment either arm is rewritten, and it
    /// is only as quiet as the machine that produced it. The work itself is
    /// deterministic and can be counted exactly, so it is counted here:
    ///
    /// - Streaming a `Words` chunk loads one word per 64 positions, from the
    ///   first occupied word to the last. That is a function of the chunk's
    ///   SPAN.
    /// - `select(k)` seeks a directory group and then scans forward word by
    ///   word to the word holding member `k`. Summed over every rank, that is
    ///   a function of the chunk's CARDINALITY, multiplied by how far a
    ///   member sits from its group boundary.
    ///
    /// Both sides are equalities against something independently known — the
    /// streamed count against the index of the last member's word, the
    /// select-driven count against `select`'s own group arithmetic — so
    /// neither can pass by never running.
    #[test]
    fn a_words_walk_costs_its_span_where_select_costs_its_cardinality() {
        // `u20k/n8000`: one Words chunk, the cell the published ratio is on.
        let mut state = 0xA1u64;
        let values: Vec<u32> = {
            let mut seen = BTreeSet::new();
            while seen.len() < 8_000 {
                seen.insert((xorshift(&mut state) % 20_000) as u32);
            }
            seen.into_iter().collect()
        };
        let frozen = Bitmosaic::from_sorted(values.iter().copied());
        assert_eq!(
            frozen.container_census(),
            (0, 0, 0, 1, 0),
            "corpus must freeze to one Words"
        );
        let container = &frozen.containers[0];
        let Container::Words { dir, len, .. } = container else {
            unreachable!("census asserted Words")
        };

        // Streaming: drive the cursor to exhaustion and read its word index,
        // which IS the number of words it loaded.
        let mut cursor = container.range();
        let mut yielded = 0u32;
        while !cursor.empty() {
            cursor.pop_front();
            yielded += 1;
        }
        let ContainerRange::Words {
            word: streamed_loads,
            ..
        } = cursor
        else {
            unreachable!("Words container yields the Words cursor")
        };
        assert_eq!(yielded, *len, "cursor must yield every member");

        // It stopped at the last member's word, not at the end of the chunk.
        let last_word = (container.select(*len - 1) >> 6) as usize + 1;
        assert_eq!(streamed_loads, last_word, "streaming loads the span, once");
        assert!(
            streamed_loads < CHUNK_WORDS,
            "and does not scan the empty tail"
        );

        // Select-driven: `select` lands on directory group `g` and then walks
        // words from `g * DIR_STRIDE` up to the member's own word.
        let select_loads: usize = (0..*len)
            .map(|k| {
                let g = select_group(dir.len(), *len, k, |i| dir[i] as u32);
                let w = (container.select(k) >> 6) as usize;
                w + 1 - g * DIR_STRIDE
            })
            .sum();

        // Two large counts against each other: neither is a zero that could
        // mean "never asked".
        assert!(
            select_loads > usize::try_from(*len).unwrap(),
            "at least one word per rank"
        );
        let ratio = select_loads / streamed_loads;
        assert!(
            ratio >= 50,
            "streaming {streamed_loads} word loads vs select-driven {select_loads} \
             over {len} members = {ratio}x; the whole point of the cursor is that \
             this is a large number"
        );
    }

    /// `count_range` and `bucket_counts` answer questions about members
    /// without a member walk, but rank is not literally payload-free on every
    /// arm: Array bisects values and Cone reconstructs an epsilon window.
    /// Pin the honest invariant instead — work is bounded by structure, not
    /// by the answer's cardinality.
    #[test]
    fn metadata_counts_are_bounded_by_structure_not_answer_cardinality() {
        let mut state = 0xB0_0C_E7u64;
        let values: Vec<u32> = {
            let mut seen = BTreeSet::new();
            while seen.len() < 8_000 {
                seen.insert((xorshift(&mut state) % 20_000) as u32);
            }
            seen.into_iter().collect()
        };
        let frozen = Bitmosaic::from_sorted(values.iter().copied());
        assert_eq!(
            frozen.container_census(),
            (0, 0, 0, 1, 0),
            "rank corpus must be Words"
        );
        let container = &frozen.containers[0];
        assert!(matches!(container, Container::Words { .. }));

        let rank_word_loads = |value: u16| {
            let word = (value >> 6) as usize;
            word - (word / DIR_STRIDE) * DIR_STRIDE + 1
        };
        for probe in 0..=u16::MAX {
            assert_eq!(
                container.rank_below(probe) as usize,
                values.partition_point(|value| *value < u32::from(probe)),
                "rank oracle at {probe}"
            );
            assert!(
                rank_word_loads(probe) <= DIR_STRIDE,
                "rank at {probe} exceeded the {DIR_STRIDE}-word directory window"
            );
        }

        let narrow = frozen.count_range(values[100], values[101]);
        let wide = frozen.count_range(0, 20_000);
        assert_eq!(narrow, 1, "adjacent members define a one-member range");
        assert_eq!(
            wide,
            values.len() as u64,
            "wide range contains the whole corpus"
        );
        assert!(
            wide > 1_000 * narrow,
            "the answers must differ by three orders"
        );
        assert!(
            rank_word_loads(values[100] as u16) + rank_word_loads(values[101] as u16)
                <= 2 * DIR_STRIDE
        );
        assert!(
            rank_word_loads(0) + rank_word_loads(20_000) <= 2 * DIR_STRIDE,
            "both ranges cost two bounded rank probes regardless of their answers"
        );

        fn bucket_rank_calls(set: &Bitmosaic64) -> (usize, usize) {
            let mut rank_calls = 0usize;
            let mut emitted = 0usize;
            bucket_counts_from_rank::<u64, 64>(
                32,
                set.len(),
                |bound| {
                    rank_calls += 1;
                    set.rank(bound as u64)
                },
                |base| base as u64,
                |_, _| emitted += 1,
            );
            (rank_calls, emitted)
        }

        let sparse = Bitmosaic64::from_sorted([0, (7u64 << 32) | 1]);
        let dense = Bitmosaic64::from_sorted(
            (0..8u64).flat_map(|high| (0..2_000u64).map(move |low| (high << 32) | (low * 3))),
        );
        assert!(
            dense.len() > sparse.len() * 1_000,
            "cardinalities must materially differ"
        );
        let sparse_work = bucket_rank_calls(&sparse);
        let dense_work = bucket_rank_calls(&dense);
        assert_eq!(sparse_work.0, 8, "positive control: eight rank calls ran");
        assert_eq!(
            dense_work.0, sparse_work.0,
            "rank calls depend on buckets, not members"
        );
        assert_eq!(sparse_work.1, 2, "two sparse buckets emitted");
        assert_eq!(dense_work.1, 8, "all dense buckets emitted");
    }

    #[test]
    fn bitmosaic64_matches_btreeset_oracle() {
        use std::collections::BTreeSet;
        let mut state = 9u64;
        // Ordinal-shaped (high32 = 0), multi-high, and boundary-straddling.
        let shapes: Vec<Vec<u64>> = vec![
            (0..40_000u64).map(|i| i * 7).collect(),
            (0..10_000u64)
                .map(|_| xorshift(&mut state) % (3u64 << 32))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            (0..2_000u64).map(|i| (i << 32) | (i * 977)).collect(),
        ];
        for values in &shapes {
            let ax = Bitmosaic64::from_sorted(values.iter().copied());
            let oracle: BTreeSet<u64> = values.iter().copied().collect();
            assert_eq!(ax.len(), oracle.len() as u64);
            for probe_seed in 0..300u64 {
                let v = xorshift(&mut (probe_seed | 1)) % values.last().map_or(1, |l| l + 3);
                assert_eq!(ax.contains(v), oracle.contains(&v), "contains({v})");
                assert_eq!(ax.rank(v), oracle.range(..v).count() as u64, "rank({v})");
                assert_eq!(
                    ax.successor(v),
                    oracle.range(v..).next().copied(),
                    "successor({v})"
                );
            }
            for k in (0..oracle.len() as u64).step_by(37) {
                assert_eq!(
                    ax.select(k),
                    oracle.iter().nth(k as usize).copied(),
                    "select({k})"
                );
            }
            let shifted = Bitmosaic64::from_sorted(values.iter().map(|v| v + 1));
            let sh: BTreeSet<u64> = values.iter().map(|v| v + 1).collect();
            assert_eq!(
                ax.and_len(&shifted),
                oracle.intersection(&sh).count() as u64
            );
            let stats = ax.overlap_stats(&shifted);
            assert_eq!(stats.or, oracle.union(&sh).count() as u64);
        }
        // Sorted-slice semijoin shape.
        let a: Vec<u64> = (0..5_000u64).map(|i| i * 3).collect();
        let b: Vec<u64> = (0..5_000u64).map(|i| i * 5).collect();
        let mut out = Vec::new();
        Bitmosaic64::intersect_sorted(&a, &b, &mut out);
        let want: Vec<u64> = a.iter().copied().filter(|v| v % 5 == 0).collect();
        assert_eq!(out, want);
    }

    #[test]
    fn emergent_apis_match_btreeset_oracle() {
        use std::collections::BTreeSet;
        let mut state = 42u64;
        let mut shapes: Vec<Vec<u32>> = vec![
            (0..5_000u32).map(|i| i * 97).collect(),
            (0..20_000u32)
                .map(|i| i * 13 + (xorshift(&mut state) % 3) as u32)
                .collect(),
            (0..3_000)
                .map(|_| xorshift(&mut state) as u32 % 500_000)
                .collect(),
            (0..60_000)
                .map(|_| xorshift(&mut state) as u32 % 65_536)
                .collect(),
        ];
        for values in &mut shapes {
            values.sort_unstable();
            values.dedup();
        }
        let probe_points: Vec<u32> = (0..200)
            .map(|_| xorshift(&mut state) as u32 % 600_000)
            .collect();
        for values in &shapes {
            let ax = Bitmosaic::from_sorted(values.iter().copied());
            let oracle: BTreeSet<u32> = values.iter().copied().collect();
            for &v in &probe_points {
                assert_eq!(
                    ax.successor(v),
                    oracle.range(v..).next().copied(),
                    "successor({v})"
                );
                assert_eq!(
                    ax.predecessor(v),
                    oracle.range(..=v).next_back().copied(),
                    "predecessor({v})"
                );
                let hi = v.saturating_add(77_777);
                assert_eq!(
                    ax.count_range(v, hi),
                    oracle.range(v..hi).count() as u64,
                    "count_range({v},{hi})"
                );
            }
            // Quantiles hit exact selects.
            for q in [0.0, 0.25, 0.5, 0.75, 1.0] {
                let got = ax.quantile(q).unwrap();
                assert!(oracle.contains(&got), "quantile({q}) member");
            }
            // Samples are members.
            for seed in 1..64u64 {
                assert!(oracle.contains(&ax.sample(seed).unwrap()), "sample member");
            }
            // Bucket counts partition the cardinality and match the oracle.
            let mut total = 0u64;
            ax.bucket_counts(16, |base, count| {
                let hi = base as u64 + (1 << 16);
                let want = oracle
                    .range(base..(hi.min(u32::MAX as u64 + 1)) as u32)
                    .count() as u64;
                assert_eq!(count, want, "bucket_counts({base})");
                total += count;
            });
            assert_eq!(total, ax.len(), "bucket counts partition len");
            // Overlap stats vs set algebra against a shifted sibling.
            let shifted = Bitmosaic::from_sorted(values.iter().map(|v| v + 1));
            let sh_oracle: BTreeSet<u32> = values.iter().map(|v| v + 1).collect();
            let s = ax.overlap_stats(&shifted);
            assert_eq!(s.and, oracle.intersection(&sh_oracle).count() as u64);
            assert_eq!(s.or, oracle.union(&sh_oracle).count() as u64);
            assert_eq!(
                s.xor,
                oracle.symmetric_difference(&sh_oracle).count() as u64
            );
            assert_eq!(s.only_self, oracle.difference(&sh_oracle).count() as u64);
        }
        // Arithmetic introspection: pure progression detected, jitter refused.
        let prog = Bitmosaic::from_sorted((0..200_000u32).map(|i| i * 7));
        assert_eq!(prog.as_arithmetic(), Some((0, 7, 200_000)));
        let broken = Bitmosaic::from_sorted((0..200_000u32).map(|i| i * 7 + (i == 77) as u32));
        assert_eq!(broken.as_arithmetic(), None);
    }

    #[test]
    fn compression_beats_arrays_on_regular_data() {
        let mut state = 11u64;
        let jittered: Vec<u32> = (0..5_000u32)
            .map(|i| i * 97 + (xorshift(&mut state) % 5) as u32)
            .collect();
        let frozen = Bitmosaic::from_sorted(jittered.iter().copied());
        let array_bytes = 2 * frozen.len() as usize;
        assert!(
            frozen.heap_bytes() < array_bytes,
            "cone forest {} B must beat array form {} B",
            frozen.heap_bytes(),
            array_bytes
        );
    }

    /// Stride 1 is the densest progression and the ladder's flagship arm, but
    /// a 64-bit reciprocal cannot express it: `ceil(2^64/1)` is unrepresentable.
    /// The oracle sweep above only ever uses stride 7, so a contiguous run had
    /// never been frozen. Covers freeze, contains, rank and select together
    /// because the reciprocal is shared by all four.
    #[test]
    fn contiguous_runs_freeze_and_probe() {
        for (base, len) in [(0u32, 1_000u32), (0, 65_536), (7, 300), (100_000, 70_000)] {
            let values: Vec<u32> = (base..base + len).collect();
            let frozen = Bitmosaic::from_sorted(values.iter().copied());
            let oracle: BTreeSet<u32> = values.iter().copied().collect();

            assert_eq!(
                frozen.len(),
                len as u64,
                "cardinality at base {base} len {len}"
            );
            assert_eq!(
                frozen.container_census().0,
                frozen.container_census().0
                    + frozen.container_census().1
                    + frozen.container_census().2
                    + frozen.container_census().3
                    + frozen.container_census().4,
                "every chunk of a contiguous run must be Stride (base {base} len {len})"
            );

            for probe in [
                base.saturating_sub(1),
                base,
                base + len / 2,
                base + len - 1,
                base + len,
            ] {
                assert_eq!(
                    frozen.contains(probe),
                    oracle.contains(&probe),
                    "contains({probe}) at base {base} len {len}"
                );
                assert_eq!(
                    frozen.rank(probe),
                    oracle.range(..probe).count() as u64,
                    "rank({probe}) at base {base} len {len}"
                );
            }
            for k in [0u64, 1, (len / 2) as u64, (len - 1) as u64] {
                assert_eq!(frozen.select(k), Some(base + k as u32), "select({k})");
            }
        }

        // Whole-set introspection must recognise the stride-1 progression.
        let contiguous = Bitmosaic::from_sorted(0..200_000u32);
        assert_eq!(contiguous.as_arithmetic(), Some((0, 1, 200_000)));
    }

    /// A cone's probe window is the model's prediction plus an eps margin.
    /// For a probe ABOVE the chunk's last member the prediction runs off the
    /// residual plane, and an unclamped window start is returned verbatim as
    /// the rank — an answer that can exceed the set's own cardinality.
    ///
    /// The sweep above never reached it because every shape's probe range
    /// topped out below that shape's own chunk keys, so `locate` missed and
    /// the chunk-boundary path answered instead. Reaching it needs a probe
    /// inside an OCCUPIED chunk but above its last member.
    #[test]
    fn rank_above_a_cone_chunks_last_member() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let values: Vec<u32> = (0..5_000u32)
            .map(|i| i * 97 + (xorshift(&mut state) % 5) as u32)
            .collect();
        let oracle: BTreeSet<u32> = values.iter().copied().collect();
        let frozen = Bitmosaic::from_sorted(values.iter().copied());
        assert!(
            frozen.container_census().1 > 0,
            "the shape must freeze to Cones"
        );

        let last = *oracle.iter().next_back().unwrap();
        let key = last >> 16;
        for probe in [last + 1, last + 1_000, (key << 16) | 0xFFFF] {
            assert_eq!(
                frozen.rank(probe) as usize,
                oracle.range(..probe).count(),
                "rank({probe}) inside chunk {key}, above its last member {last}"
            );
            assert!(
                frozen.rank(probe) <= frozen.len(),
                "rank({probe}) exceeds cardinality"
            );
        }
    }

    /// `BitmosaicBuilder::remove` against the `BTreeSet` oracle over a random
    /// insert/remove history that crosses the inline-to-plane staging
    /// boundary in both directions and empties whole chunks. Every answer
    /// the write returns is the oracle's, and the frozen form is the frozen
    /// form of the surviving set — byte for byte, since freeze is a pure
    /// function of the set.
    #[test]
    fn builder_remove_matches_reference_and_freezes_the_surviving_set() {
        let mut state = 0x5DEE_CE66_D1CE_5EEDu64;
        let mut builder = BitmosaicBuilder::new();
        let mut oracle = BTreeSet::new();
        // Three chunks, one of them driven far past STAGE_INLINE_MAX so the
        // plane arm is exercised, one kept inline, one emptied outright.
        for step in 0..20_000u32 {
            let roll = xorshift(&mut state);
            let value = match roll % 3 {
                0 => (roll >> 8) as u32 % 4_000,
                1 => 65_536 + (roll >> 8) as u32 % 24,
                _ => 131_072 + (roll >> 8) as u32 % 100,
            };
            if roll >> 40 & 1 == 0 || step % 7 == 0 {
                assert_eq!(
                    builder.insert(value),
                    oracle.insert(value),
                    "insert {value}"
                );
            } else {
                assert_eq!(
                    builder.remove(value),
                    oracle.remove(&value),
                    "remove {value}"
                );
            }
            assert_eq!(builder.contains(value), oracle.contains(&value));
        }
        for value in oracle.clone().range(131_072..) {
            assert!(builder.remove(*value), "emptying chunk 2: {value}");
            oracle.remove(value);
        }
        assert!(
            !builder.remove(131_072 + 5),
            "removing from an emptied chunk"
        );
        assert!(
            !builder.remove(1 << 20),
            "removing from a chunk never staged"
        );

        let frozen = builder.freeze();
        let expected = Bitmosaic::from_sorted(oracle.iter().copied());
        assert_eq!(frozen.len(), oracle.len() as u64);
        assert_eq!(
            frozen.chunk_count(),
            expected.chunk_count(),
            "emptied chunk dropped"
        );
        assert_eq!(
            frozen.to_bytes(),
            expected.to_bytes(),
            "byte-identical to a fresh freeze"
        );
        let mut walked = Vec::new();
        frozen.iter().for_each(|v| walked.push(v));
        assert_eq!(walked, oracle.iter().copied().collect::<Vec<_>>());
    }
}
