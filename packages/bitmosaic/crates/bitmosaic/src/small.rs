//! Small sets: the image forms that need no directory, and the readers that
//! answer from their bytes where they lie.
//!
//! A set of a few dozen ordinals is the common slot, and on it every byte of
//! framing is a tenth of the image. A small image is one tag byte and the
//! members, all below 65,536, in the shortest of four forms, each chosen
//! for the read as much as for the size:
//!
//! - **List8.** Up to 32 members below 256 as ascending bytes. `select` is
//!   one load; `contains` and `rank` bisect.
//! - **Window.** A bitmap of up to 32 whole bytes: bit `j` of byte `i` is
//!   the member `8 * (start + i) + j`. `contains` is one byte load, a shift
//!   and a mask; `rank` and the intersection of two windows are popcounts
//!   over at most four unaligned word loads. The window is byte-aligned in
//!   the value domain, not anchored at its first member, so two windows of
//!   any two images share bit positions and intersect with no shift.
//! - **List16.** Up to 32 members as ascending `u16`.
//! - **Spans.** Up to 32 inclusive intervals as `(start, end)` pairs of
//!   `u16`. One interval answers in closed form.
//!
//! Opening a small image reads its tag: the view is a pointer and a length,
//! plus the window's start. Nothing is derived from the members, and no
//! accessor copies them.
//!
//! Nothing here is shared with the directory reader ([`crate::wire`]): not
//! a container view, not a kernel. The directory code reaches a small image
//! only where it finds a forest of no chunks, through the `image_*`
//! functions below, which are never inlined there. So the code compiled for
//! a directory or an Elias-Fano root is the code compiled without this
//! module (the compiler specializes an internal function to the set of its
//! call sites, so one more caller of a directory function changes what
//! every directory probe runs).

use crate::wire::{read_u16_at, read_u32_at, read_u64_at};
use crate::{AdmissionClass, AdmissionError};

/// Members below 256 as ascending bytes. Payload: the members.
pub(crate) const K_LIST8: u32 = 5;
/// A bitmap from value zero. Payload: the bitmap.
pub(crate) const K_WINDOW: u32 = 6;
/// A bitmap from value `8 * start`. Payload: `start`, then the bitmap.
pub(crate) const K_WINDOW_AT: u32 = 7;
/// Members as ascending `u16`. Payload: the members.
pub(crate) const K_LIST16: u32 = 8;
/// Inclusive intervals. Payload: `(start: u16, end: u16)` pairs.
pub(crate) const K_SPANS: u32 = 9;

/// Lowest tag of a small image, `kk lllll` with `kk` from one: the kind as
/// `kk + 4` and the count as `lllll + 1`. The framed controls lie below.
pub(crate) const SMALL_MIN: u8 = 0x20;
/// First tag past the small forms.
const SMALL_END: u8 = 0xC0;
/// Most members of a list, bytes of a window's bitmap, intervals of Spans.
const COUNT_MAX: usize = 32;
/// Most payload bytes of a small image: 32 intervals.
pub(crate) const SMALL_PAYLOAD_MAX: usize = 4 * COUNT_MAX;

pub(crate) fn is_small(kind: u32) -> bool {
    kind >= K_LIST8
}

/// A small tag's kind and count, or `None` for a byte that is not one.
#[inline(always)]
pub(crate) fn read_small_tag(tag: u8) -> Option<(u32, u32)> {
    (SMALL_MIN..SMALL_END)
        .contains(&tag)
        .then(|| (u32::from(tag >> 5) + 4, u32::from(tag & 31) + 1))
}

pub(crate) fn small_tag(kind: u32, count: u32) -> u8 {
    ((kind - 4) as u8) << 5 | (count - 1) as u8
}

/// Payload bytes of a small image whose tag holds `count`.
#[inline(always)]
pub(crate) fn small_payload_len(kind: u32, count: u32) -> usize {
    let count = count as usize;
    match kind {
        K_WINDOW_AT => 1 + count,
        K_LIST16 => 2 * count,
        K_SPANS => 4 * count,
        _ => count,
    }
}

/// The count a small tag holds, from its payload length: the inverse of
/// [`small_payload_len`].
pub(crate) fn small_count(kind: u32, payload: usize) -> u32 {
    (match kind {
        K_WINDOW_AT => payload - 1,
        K_LIST16 => payload / 2,
        K_SPANS => payload / 4,
        _ => payload,
    }) as u32
}

/// The small form of a set that is one chunk at key zero: `card` members
/// from `first` to `last` in `runs` maximal runs, whose directory image
/// would be `directory` bytes. Its kind and payload length, or `None` when
/// the directory stays.
///
/// One rule, so one image per set: the shortest image wins. On a tie a
/// small form beats the directory, and among small forms the order is
/// Window from zero, Window, Spans, List8, List16: roughly the order of
/// their point lookups.
pub(crate) fn small_form(
    directory: usize,
    first: u16,
    last: u16,
    card: u32,
    runs: usize,
) -> Option<(u32, usize)> {
    let (start, end) = (usize::from(first) / 8, usize::from(last) / 8);
    let card = card as usize;
    let candidates = [
        (end < COUNT_MAX).then_some((K_WINDOW, end + 1)),
        (start >= 1 && start <= 255 && end - start < COUNT_MAX)
            .then_some((K_WINDOW_AT, 2 + end - start)),
        (runs <= COUNT_MAX).then(|| (K_SPANS, 4 * runs)),
        (last <= 255 && card <= COUNT_MAX).then_some((K_LIST8, card)),
        (card <= COUNT_MAX).then_some((K_LIST16, 2 * card)),
    ];
    let mut best: Option<(u32, usize)> = None;
    for candidate in candidates.into_iter().flatten() {
        if best.is_none_or(|(_, len)| candidate.1 < len) {
            best = Some(candidate);
        }
    }
    // Every small header is the tag alone.
    best.filter(|&(_, len)| 1 + len <= directory)
}

/// Writes a small payload from a set's ascending members. `out` is exactly
/// the length [`small_form`] priced.
pub(crate) struct SmallSink<'a> {
    out: &'a mut [u8],
    kind: u32,
    /// The value of the bitmap's bit zero.
    base: u16,
    /// Next list byte or pair, where the bitmap starts, or the open
    /// interval's pair.
    at: usize,
    /// Members written.
    pushed: usize,
}

impl<'a> SmallSink<'a> {
    pub(crate) fn new(out: &'a mut [u8], kind: u32, first: u16) -> Self {
        out.fill(0);
        let (base, at) = if kind == K_WINDOW_AT {
            out[0] = (first / 8) as u8;
            (first & !7, 1)
        } else {
            (0, 0)
        };
        Self {
            out,
            kind,
            base,
            at,
            pushed: 0,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, member: u16) {
        match self.kind {
            K_LIST8 => {
                self.out[self.at] = member as u8;
                self.at += 1;
            }
            K_LIST16 => {
                self.out[self.at..self.at + 2].copy_from_slice(&member.to_le_bytes());
                self.at += 2;
            }
            K_SPANS => {
                let end = self.at + 2..self.at + 4;
                let open = self.pushed != 0
                    && u32::from(read_u16_at(self.out, self.at + 2)) + 1 == u32::from(member);
                if !open {
                    if self.pushed != 0 {
                        self.at += 4;
                    }
                    self.out[self.at..self.at + 2].copy_from_slice(&member.to_le_bytes());
                }
                let end = if open { end } else { self.at + 2..self.at + 4 };
                self.out[end].copy_from_slice(&member.to_le_bytes());
            }
            _ => {
                let bit = usize::from(member - self.base);
                self.out[self.at + bit / 8] |= 1 << (bit % 8);
            }
        }
        self.pushed += 1;
    }
}

// ── reading a bitmap where it lies ───────────────────────────────────────

/// Bits `64 * i .. 64 * i + 64` of a bitmap, zero past its end. A whole word
/// is one unaligned load; the last, partial word is two overlapping loads of
/// the widest power of two it holds, so no read leaves the image.
#[inline(always)]
fn word_at(bits: &[u8], i: usize) -> u64 {
    let at = 8 * i;
    if at + 8 <= bits.len() {
        return read_u64_at(bits, at);
    }
    let rest = bits.get(at..).unwrap_or(&[]);
    let n = rest.len();
    if n >= 4 {
        u64::from(read_u32_at(rest, 0)) | u64::from(read_u32_at(rest, n - 4)) << (8 * (n - 4))
    } else if n >= 2 {
        u64::from(read_u16_at(rest, 0)) | u64::from(read_u16_at(rest, n - 2)) << (8 * (n - 2))
    } else if n == 1 {
        u64::from(rest[0])
    } else {
        0
    }
}

/// The set bits of a word, ascending.
struct Bits(u64);

impl Iterator for Bits {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        if self.0 == 0 {
            return None;
        }
        let bit = self.0.trailing_zeros();
        self.0 &= self.0 - 1;
        Some(bit)
    }
}

/// Position of the `k`-th set bit of `word`, `k` below its popcount: six
/// halvings by popcount, no branch and no bit-deposit instruction, the same
/// in every port.
#[inline(always)]
fn select_in_word(mut word: u64, mut k: u32) -> u32 {
    let mut position = 0;
    for width in [32u32, 16, 8, 4, 2, 1] {
        let low = (word & ((1u64 << width) - 1)).count_ones();
        let upper = u32::from(k >= low);
        k -= low * upper;
        position += width * upper;
        word >>= width * upper;
    }
    position
}

/// Set bits of `bits` below bit `below`.
#[inline(always)]
fn count_below(bits: &[u8], below: usize) -> u32 {
    let below = below.min(8 * bits.len());
    let mut count = 0;
    for i in 0..below / 64 {
        count += word_at(bits, i).count_ones();
    }
    if below % 64 != 0 {
        count += (word_at(bits, below / 64) & ((1u64 << (below % 64)) - 1)).count_ones();
    }
    count
}

/// Members two windows share below the value `below`: the bytes both cover,
/// ANDed a word at a time.
fn window_and(a: &[u8], a_base: u32, b: &[u8], b_base: u32, below: u32) -> u32 {
    let (a_start, b_start) = ((a_base / 8) as usize, (b_base / 8) as usize);
    let start = a_start.max(b_start);
    let end = (a_start + a.len()).min(b_start + b.len());
    if end <= start {
        return 0;
    }
    let (a, b) = (
        &a[start - a_start..end - a_start],
        &b[start - b_start..end - b_start],
    );
    let bits = (below as usize).saturating_sub(8 * start).min(8 * a.len());
    let mut count = 0;
    for i in 0..bits / 64 {
        count += (word_at(a, i) & word_at(b, i)).count_ones();
    }
    if bits % 64 != 0 {
        let both = word_at(a, bits / 64) & word_at(b, bits / 64);
        count += (both & ((1u64 << (bits % 64)) - 1)).count_ones();
    }
    count
}

// ── reading lists and intervals where they lie ───────────────────────────

/// A list's `i`-th member: a byte, or a `u16` when `wide`.
#[inline(always)]
fn member(list: &[u8], wide: bool, i: usize) -> u32 {
    if wide {
        u32::from(read_u16_at(list, 2 * i))
    } else {
        u32::from(list[i])
    }
}

/// Members of an ascending list strictly below `value`: a bisection that
/// moves by a conditional add, never a branch on a member.
#[inline(always)]
fn list_rank(list: &[u8], wide: bool, value: u32) -> usize {
    let n = list.len() >> u32::from(wide);
    if n == 0 {
        return 0;
    }
    if value > member(list, wide, n - 1) {
        return n;
    }
    let (mut low, mut len) = (0, n);
    for _ in 0..n.ilog2() + 1 {
        let half = len / 2;
        let probe = member(list, wide, low + half.saturating_sub(1));
        low += if half != 0 && probe < value { half } else { 0 };
        len -= half;
    }
    low + usize::from(member(list, wide, low) < value)
}

/// Interval `i` of a Spans payload, as `(start, end)` inclusive.
#[inline(always)]
fn span(spans: &[u8], i: usize) -> (u32, u32) {
    (
        u32::from(read_u16_at(spans, 4 * i)),
        u32::from(read_u16_at(spans, 4 * i + 2)),
    )
}

/// Members of the intervals strictly below `value`, and whether `value` is
/// one.
#[inline(always)]
fn spans_rank(spans: &[u8], value: u32) -> (u32, bool) {
    let mut below = 0;
    for i in 0..spans.len() / 4 {
        let (start, end) = span(spans, i);
        if value <= end {
            return (below + value.saturating_sub(start), value >= start);
        }
        below += end - start + 1;
    }
    (below, false)
}

// ── the small set ────────────────────────────────────────────────────────

/// A small image's members, read where they lie.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SmallSet<'a> {
    /// The members, ascending.
    List8(&'a [u8]),
    /// Bit `j` of byte `i` is the member `base + 8 * i + j`.
    Window { bits: &'a [u8], base: u32 },
    /// The members as ascending `u16`.
    List16(&'a [u8]),
    /// Inclusive `(start: u16, end: u16)` intervals, ascending and apart.
    Spans(&'a [u8]),
}

impl<'a> SmallSet<'a> {
    /// The set a small image holds, from its tag; `None` for any other
    /// image. Attachment proved the payload holds what the tag says.
    #[inline(always)]
    pub(crate) fn of(image: &'a [u8]) -> Option<Self> {
        let (kind, count) = read_small_tag(*image.first()?)?;
        Some(Self::from_validated(image.get(1..)?, kind, count))
    }

    /// Admit a small image: its tag, its exact extent and the structure no
    /// reader survives without. Member order is [`Self::verify`]'s.
    pub(crate) fn open(image: &'a [u8]) -> Result<Self, AdmissionError> {
        let Some((kind, count)) = image.first().and_then(|&tag| read_small_tag(tag)) else {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "unassigned small tag",
            ));
        };
        let need = 1 + small_payload_len(kind, count);
        if image.len() != need {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                image.len().min(need),
                "small payload does not cover its extent exactly",
            ));
        }
        let set = Self::from_validated(&image[1..], kind, count);
        if let SmallSet::Window { bits, base } = set {
            if kind == K_WINDOW_AT && base == 0 {
                return Err(AdmissionError::new(
                    AdmissionClass::Kind,
                    1,
                    "a Window from value zero is written without its start",
                ));
            }
            if bits.last() == Some(&0) {
                return Err(AdmissionError::new(
                    AdmissionClass::Extent,
                    image.len() - 1,
                    "Window bitmap ends in a zero byte",
                ));
            }
            if kind == K_WINDOW_AT && bits[0] == 0 {
                return Err(AdmissionError::new(
                    AdmissionClass::Extent,
                    2,
                    "Window bitmap starts with a zero byte",
                ));
            }
        }
        Ok(set)
    }

    #[inline(always)]
    pub(crate) fn from_validated(payload: &'a [u8], kind: u32, count: u32) -> Self {
        let payload = payload
            .get(..small_payload_len(kind, count))
            .unwrap_or(payload);
        match kind {
            K_LIST8 => SmallSet::List8(payload),
            K_WINDOW => SmallSet::Window {
                bits: payload,
                base: 0,
            },
            K_WINDOW_AT => SmallSet::Window {
                bits: payload.get(1..).unwrap_or(&[]),
                base: 8 * u32::from(payload.first().copied().unwrap_or(0)),
            },
            K_LIST16 => SmallSet::List16(payload),
            _ => SmallSet::Spans(payload),
        }
    }

    /// What structural admission leaves to the verifying one: members in
    /// strictly ascending order, intervals ascending and apart. Offsets are
    /// image-relative.
    pub(crate) fn verify(self) -> Result<(), AdmissionError> {
        let disorder = |at: usize, what: &'static str| {
            Err(AdmissionError::new(AdmissionClass::MapOrder, 1 + at, what))
        };
        match self {
            SmallSet::Window { .. } => {}
            SmallSet::List8(list) => {
                if let Some(i) = list.windows(2).position(|pair| pair[0] >= pair[1]) {
                    return disorder(i + 1, "List8 members are not strictly ascending");
                }
            }
            SmallSet::List16(list) => {
                for i in 1..list.len() / 2 {
                    if member(list, true, i - 1) >= member(list, true, i) {
                        return disorder(2 * i, "List16 members are not strictly ascending");
                    }
                }
            }
            SmallSet::Spans(spans) => {
                for i in 0..spans.len() / 4 {
                    let (start, end) = span(spans, i);
                    if end < start {
                        return disorder(4 * i + 2, "an interval ends before it starts");
                    }
                    if i != 0 && start <= span(spans, i - 1).1 + 1 {
                        return disorder(4 * i, "intervals overlap or touch");
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn kind(self) -> u32 {
        match self {
            SmallSet::List8(_) => K_LIST8,
            SmallSet::Window { base: 0, .. } => K_WINDOW,
            SmallSet::Window { .. } => K_WINDOW_AT,
            SmallSet::List16(_) => K_LIST16,
            SmallSet::Spans(_) => K_SPANS,
        }
    }

    pub(crate) fn payload_len(self) -> usize {
        match self {
            SmallSet::List8(bytes) | SmallSet::List16(bytes) | SmallSet::Spans(bytes) => {
                bytes.len()
            }
            SmallSet::Window { bits, base } => bits.len() + usize::from(base != 0),
        }
    }

    #[inline(always)]
    pub(crate) fn cardinality(self) -> u32 {
        match self {
            SmallSet::List8(list) => list.len() as u32,
            SmallSet::List16(list) => (list.len() / 2) as u32,
            SmallSet::Window { bits, .. } => count_below(bits, usize::MAX),
            SmallSet::Spans(spans) => spans_rank(spans, u32::MAX).0,
        }
    }

    #[inline(always)]
    pub(crate) fn contains(self, value: u32) -> bool {
        match self {
            SmallSet::Window { bits, base } => {
                let bit = value.wrapping_sub(base);
                bits.get((bit >> 3) as usize)
                    .is_some_and(|byte| byte >> (bit & 7) & 1 != 0)
            }
            SmallSet::List8(list) => {
                let at = list_rank(list, false, value);
                at < list.len() && member(list, false, at) == value
            }
            SmallSet::List16(list) => {
                let at = list_rank(list, true, value);
                at < list.len() / 2 && member(list, true, at) == value
            }
            SmallSet::Spans(spans) => spans_rank(spans, value).1,
        }
    }

    /// Members strictly below `value`.
    #[inline(always)]
    pub(crate) fn rank_below(self, value: u32) -> u32 {
        match self {
            SmallSet::Window { bits, base } => {
                count_below(bits, value.saturating_sub(base) as usize)
            }
            SmallSet::List8(list) => list_rank(list, false, value) as u32,
            SmallSet::List16(list) => list_rank(list, true, value) as u32,
            SmallSet::Spans(spans) => spans_rank(spans, value).0,
        }
    }

    /// The `k`-th member ascending, or `None` at or past the cardinality.
    #[inline(always)]
    pub(crate) fn select(self, k: u32) -> Option<u32> {
        let at = k as usize;
        match self {
            SmallSet::List8(list) => (at < list.len()).then(|| member(list, false, at)),
            SmallSet::List16(list) => (at < list.len() / 2).then(|| member(list, true, at)),
            SmallSet::Window { bits, base } => {
                let mut k = k;
                for i in 0..bits.len().div_ceil(8) {
                    let word = word_at(bits, i);
                    let count = word.count_ones();
                    if k < count {
                        return Some(base + 64 * i as u32 + select_in_word(word, k));
                    }
                    k -= count;
                }
                None
            }
            SmallSet::Spans(spans) => {
                let mut k = k;
                for i in 0..spans.len() / 4 {
                    let (start, end) = span(spans, i);
                    if k <= end - start {
                        return Some(start + k);
                    }
                    k -= end - start + 1;
                }
                None
            }
        }
    }

    #[inline(always)]
    pub(crate) fn for_each(self, mut f: impl FnMut(u32)) {
        match self {
            SmallSet::List8(list) => {
                for &member in list {
                    f(u32::from(member));
                }
            }
            SmallSet::List16(list) => {
                for i in 0..list.len() / 2 {
                    f(member(list, true, i));
                }
            }
            SmallSet::Window { bits, base } => {
                for i in 0..bits.len().div_ceil(8) {
                    let low = base + 64 * i as u32;
                    for bit in Bits(word_at(bits, i)) {
                        f(low + bit);
                    }
                }
            }
            SmallSet::Spans(spans) => {
                for i in 0..spans.len() / 4 {
                    let (start, end) = span(spans, i);
                    for value in start..=end {
                        f(value);
                    }
                }
            }
        }
    }

    /// `(first, step, members)` when two or more members are one arithmetic
    /// progression.
    pub(crate) fn progression(self) -> Option<(u32, u32, u32)> {
        if let SmallSet::Spans(spans) = self
            && spans.len() == 4
        {
            let (start, end) = span(spans, 0);
            return (end > start).then_some((start, 1, end - start + 1));
        }
        let (mut first, mut previous, mut step, mut members, mut even) = (0, 0, 0, 0u32, true);
        self.for_each(|value| {
            match members {
                0 => first = value,
                1 => step = value - previous,
                _ => even &= value - previous == step,
            }
            previous = value;
            members += 1;
        });
        (members >= 2 && even).then_some((first, step, members))
    }

    /// Members both sets hold below `below`. Two windows meet word by word;
    /// otherwise the side that is not an interval list is walked against
    /// the other's probe.
    pub(crate) fn and_below(self, other: SmallSet<'_>, below: u32) -> u32 {
        if let (
            SmallSet::Window {
                bits: a,
                base: a_base,
            },
            SmallSet::Window {
                bits: b,
                base: b_base,
            },
        ) = (self, other)
        {
            return window_and(a, a_base, b, b_base, below);
        }
        let mut count = 0;
        match (self, other) {
            (SmallSet::Spans(a), SmallSet::Spans(b)) => {
                let (mut i, mut j) = (0, 0);
                for _ in 0..(a.len() + b.len()) / 4 {
                    if i >= a.len() / 4 || j >= b.len() / 4 {
                        break;
                    }
                    let ((a_start, a_end), (b_start, b_end)) = (span(a, i), span(b, j));
                    let (low, high) = (a_start.max(b_start), a_end.min(b_end).min(below - 1));
                    if below != 0 && low <= high {
                        count += high - low + 1;
                    }
                    i += usize::from(a_end <= b_end);
                    j += usize::from(b_end <= a_end);
                }
            }
            (SmallSet::Spans(_), walked) => {
                walked.for_each(|v| count += u32::from(v < below && self.contains(v)));
            }
            (walked, probed) => {
                walked.for_each(|v| count += u32::from(v < below && probed.contains(v)));
            }
        }
        count
    }
}

// ── the ascending cursor ─────────────────────────────────────────────────

/// Ascending cursor over a small set: a list index, a Window's next word
/// and the unconsumed bits of the word before it, or an interval and the
/// next value in it. No member is decoded twice.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SmallRange<'a> {
    List {
        list: &'a [u8],
        wide: bool,
        at: usize,
    },
    Window {
        bits: &'a [u8],
        /// The value of bit zero of the word `word` holds.
        low: u32,
        /// Index of the next word to load.
        next: u32,
        /// Unconsumed bits of word `next - 1`: nonzero while members remain.
        word: u64,
        remaining: u32,
    },
    Spans {
        spans: &'a [u8],
        /// The interval `value` lies in.
        at: usize,
        value: u32,
        remaining: u32,
    },
}

impl<'a> SmallRange<'a> {
    pub(crate) fn open(set: SmallSet<'a>) -> Self {
        match set {
            SmallSet::List8(list) => SmallRange::List {
                list,
                wide: false,
                at: 0,
            },
            SmallSet::List16(list) => SmallRange::List {
                list,
                wide: true,
                at: 0,
            },
            SmallSet::Window { bits, base } => {
                let mut range = SmallRange::Window {
                    bits,
                    low: base.wrapping_sub(64),
                    next: 0,
                    word: 0,
                    remaining: set.cardinality(),
                };
                range.load();
                range
            }
            SmallSet::Spans(spans) => SmallRange::Spans {
                spans,
                at: 0,
                value: if spans.len() >= 4 {
                    span(spans, 0).0
                } else {
                    0
                },
                remaining: set.cardinality(),
            },
        }
    }

    /// Establish "`word` is nonzero unless the bitmap is spent".
    #[inline(always)]
    fn load(&mut self) {
        let SmallRange::Window {
            bits,
            low,
            next,
            word,
            ..
        } = self
        else {
            return;
        };
        for i in *next as usize..bits.len().div_ceil(8) {
            if *word != 0 {
                return;
            }
            *word = word_at(bits, i);
            *low = low.wrapping_add(64);
            *next = i as u32 + 1;
        }
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> u32 {
        match self {
            SmallRange::List { list, wide, at } => {
                ((list.len() >> u32::from(*wide)).saturating_sub(*at)) as u32
            }
            SmallRange::Window { remaining, .. } | SmallRange::Spans { remaining, .. } => {
                *remaining
            }
        }
    }

    /// The head; meaningful while members remain.
    #[inline(always)]
    pub(crate) fn front(&self) -> u32 {
        match self {
            SmallRange::List { list, wide, at } => {
                if *at < list.len() >> u32::from(*wide) {
                    member(list, *wide, *at)
                } else {
                    0
                }
            }
            SmallRange::Window { low, word, .. } => low.wrapping_add(word.trailing_zeros()),
            SmallRange::Spans { value, .. } => *value,
        }
    }

    #[inline(always)]
    pub(crate) fn pop_front(&mut self) {
        match self {
            SmallRange::List { at, .. } => *at += 1,
            SmallRange::Window {
                word, remaining, ..
            } => {
                *word &= word.wrapping_sub(1);
                *remaining = remaining.saturating_sub(1);
                self.load();
            }
            SmallRange::Spans {
                spans,
                at,
                value,
                remaining,
            } => {
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    return;
                }
                if *value < span(spans, *at).1 {
                    *value += 1;
                } else {
                    *at += 1;
                    *value = span(spans, *at).0;
                }
            }
        }
    }

    /// Advance to the first member at or after `target`; never moves back.
    pub(crate) fn seek(&mut self, target: u32) {
        match self {
            SmallRange::List { list, wide, at } => {
                *at = (*at).max(list_rank(list, *wide, target));
            }
            SmallRange::Window {
                bits,
                low,
                next,
                word,
                remaining,
            } => {
                let first = low.wrapping_sub(64 * next.wrapping_sub(1));
                let bit = target.saturating_sub(first) as usize;
                let i = bit / 64;
                if (i as u32) < next.wrapping_sub(1) {
                    return;
                }
                if i as u32 >= *next {
                    *word = word_at(bits, i);
                    *low = first + 64 * i as u32;
                    *next = i as u32 + 1;
                }
                *word &= !((1u64 << (bit % 64)) - 1);
                let mut left = word.count_ones();
                for later in *next as usize..bits.len().div_ceil(8) {
                    left += word_at(bits, later).count_ones();
                }
                *remaining = left;
                self.load();
            }
            SmallRange::Spans {
                spans,
                at,
                value,
                remaining,
            } => {
                if *remaining == 0 || target <= *value {
                    return;
                }
                let set = SmallSet::Spans(spans);
                let left = set.cardinality() - set.rank_below(target);
                *remaining = left;
                if left == 0 {
                    return;
                }
                for i in *at..spans.len() / 4 {
                    let (start, end) = span(spans, i);
                    if target <= end {
                        *at = i;
                        *value = target.max(start);
                        return;
                    }
                }
            }
        }
    }
}

// ── a small image's answers to the directory reader ──────────────────────
//
// A view holds a small image as a forest of no chunks over its bytes. Where
// the directory code finds no chunks it asks here, by a call that is never
// inlined: beside that branch the directory and Elias-Fano code is then the
// code it is without this module. The empty image is no small image and
// answers as the empty set.

#[cold]
#[inline(never)]
pub(crate) fn image_len(image: &[u8]) -> u64 {
    SmallSet::of(image).map_or(0, |set| u64::from(set.cardinality()))
}

#[cold]
#[inline(never)]
pub(crate) fn image_contains(image: &[u8], value: u32) -> bool {
    SmallSet::of(image).is_some_and(|set| set.contains(value))
}

#[cold]
#[inline(never)]
pub(crate) fn image_rank(image: &[u8], value: u32) -> u64 {
    SmallSet::of(image).map_or(0, |set| u64::from(set.rank_below(value)))
}

#[cold]
#[inline(never)]
pub(crate) fn image_select(image: &[u8], k: u64) -> Option<u32> {
    SmallSet::of(image)?.select(u32::try_from(k).ok()?)
}
