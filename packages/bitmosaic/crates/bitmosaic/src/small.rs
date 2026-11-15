//! Small sets: the containers for a chunk whose members lie within 256
//! positions of its first, and the header of an image that is one chunk at
//! key zero.
//!
//! A set of a few dozen ordinals is the common slot, and on it every byte of
//! framing is a tenth of the image. Three things make it small, each decided
//! by the byte count and nothing else:
//!
//! - **Bytes.** The first member as a varint, then one byte per further
//!   member: its distance from the first. A member costs one byte, not two.
//! - **Window.** The first member, then a bitmap of the positions after it,
//!   as long as the last member needs. A dense handful costs a bit a
//!   position. Its two kinds differ only in the bitmap's length, so that the
//!   length always fits the lone control byte.
//! - **The lone image.** One chunk at key zero needs no chunk count, key or
//!   length: its control byte carries the container kind and the one count
//!   the payload does not say, and the payload follows.
//!
//! The ladder still picks the in-memory arm ([`crate::choose_arm`]); a small
//! form replaces that arm's payload on the wire when it costs fewer bytes
//! ([`small_form`]), so the owned containers and their kernels are unchanged.

use crate::wire::{read_u16_at, read_uleb_checked, runs_payload_len, uleb_len, write_uleb};
use crate::{AdmissionClass, AdmissionError};

pub(crate) const K_BYTES: u32 = 5;
/// A Window whose bitmap is 1..=16 bytes.
pub(crate) const K_WINDOW: u32 = 6;
/// A Window whose bitmap is 17..=32 bytes.
pub(crate) const K_WINDOW_WIDE: u32 = 7;

/// Largest distance from a small container's first member to its last.
const SPAN_MAX: usize = 255;
/// Bitmap bytes the narrow Window kind reaches; the wide kind holds the
/// same number past them.
const WINDOW_NARROW_MAX: usize = 16;

pub(crate) fn is_window(kind: u32) -> bool {
    kind == K_WINDOW || kind == K_WINDOW_WIDE
}

/// The small form for a chunk of `card` members from `first` to `last` whose
/// ladder arm costs `arm_len` payload bytes, or `None` when the arm stays.
///
/// One rule, so one image per set: the shortest payload wins; the arm wins a
/// tie with a small form, and Bytes wins a tie with Window.
pub(crate) fn small_form(arm_len: usize, first: u16, last: u16, card: u32) -> Option<(u32, usize)> {
    let span = usize::from(last - first);
    if span > SPAN_MAX {
        return None;
    }
    let head = uleb_len(u32::from(first));
    let bitmap = span.div_ceil(8);
    let (bytes, window) = (head + card as usize - 1, head + bitmap);
    let (kind, len) = if window >= bytes {
        (K_BYTES, bytes)
    } else if bitmap <= WINDOW_NARROW_MAX {
        (K_WINDOW, window)
    } else {
        (K_WINDOW_WIDE, window)
    };
    (len < arm_len).then_some((kind, len))
}

/// A small payload's first member and the bytes it took. Attachment proved
/// the varint; a damaged one decodes to what its first three bytes hold.
#[inline(always)]
fn first_of(payload: &[u8]) -> (u16, usize) {
    let mut value = 0u32;
    for (i, &byte) in payload.iter().take(3).enumerate() {
        value |= u32::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return (value as u16, i + 1);
        }
    }
    (value as u16, payload.len().min(3))
}

/// The first member of a payload under admission: a shortest-form varint
/// within the chunk.
fn read_first(rest: &[u8]) -> Result<(u16, usize), AdmissionError> {
    let mut at = 0;
    let first = read_uleb_checked(
        rest,
        &mut at,
        u32::from(u16::MAX),
        AdmissionClass::PlaneRange,
    )?;
    Ok((first as u16, at))
}

/// Writes a small payload from a chunk's ascending members, the first
/// included. `out` is exactly the length [`small_form`] priced.
pub(crate) struct SmallSink<'a> {
    out: &'a mut [u8],
    kind: u32,
    first: u16,
    head: usize,
    at: usize,
}

impl<'a> SmallSink<'a> {
    pub(crate) fn new(out: &'a mut [u8], kind: u32, first: u16) -> Self {
        let mut head = 0;
        write_uleb(out, &mut head, u32::from(first));
        out[head..].fill(0);
        Self {
            out,
            kind,
            first,
            head,
            at: head,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, member: u16) {
        let delta = usize::from(member - self.first);
        if delta == 0 {
            return;
        }
        if self.kind == K_BYTES {
            self.out[self.at] = delta as u8;
            self.at += 1;
        } else {
            self.out[self.head + (delta - 1) / 8] |= 1 << ((delta - 1) % 8);
        }
    }
}

// ── Bytes ────────────────────────────────────────────────────────────────

/// `first`, then each further member's distance from it, ascending.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BytesView<'a> {
    first: u16,
    offsets: &'a [u8],
}

impl<'a> BytesView<'a> {
    pub(crate) fn open(rest: &'a [u8], card: u32) -> Result<Self, AdmissionError> {
        if !(1..=SPAN_MAX as u32 + 1).contains(&card) {
            return Err(AdmissionError::new(
                AdmissionClass::Count,
                0,
                "Bytes holds 1..=256 members",
            ));
        }
        let (first, head) = read_first(rest)?;
        let offsets = rest.get(head..head + card as usize - 1).ok_or_else(|| {
            AdmissionError::new(AdmissionClass::Extent, 0, "Bytes offset plane is truncated")
        })?;
        Ok(Self { first, offsets })
    }

    pub(crate) fn from_validated(payload: &'a [u8]) -> Self {
        let (first, head) = first_of(payload);
        Self {
            first,
            offsets: &payload[head..],
        }
    }

    pub(crate) fn verify(self) -> Result<(), AdmissionError> {
        let head = uleb_len(u32::from(self.first));
        let mut prev = 0u8;
        for (i, &offset) in self.offsets.iter().enumerate() {
            if offset <= prev {
                return Err(AdmissionError::new(
                    AdmissionClass::MapOrder,
                    head + i,
                    "Bytes offsets are not strictly ascending from the first member",
                ));
            }
            if u32::from(self.first) + u32::from(offset) > u32::from(u16::MAX) {
                return Err(AdmissionError::new(
                    AdmissionClass::PlaneRange,
                    head + i,
                    "Bytes member exceeds its u16 chunk",
                ));
            }
            prev = offset;
        }
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn cardinality(self) -> u32 {
        self.offsets.len() as u32 + 1
    }

    #[inline(always)]
    pub(crate) fn payload_len(self) -> usize {
        uleb_len(u32::from(self.first)) + self.offsets.len()
    }

    #[inline(always)]
    pub(crate) fn contains(self, v: u16) -> bool {
        match v.checked_sub(self.first) {
            None => false,
            Some(0) => true,
            Some(delta) => {
                u8::try_from(delta).is_ok_and(|delta| self.offsets.binary_search(&delta).is_ok())
            }
        }
    }

    #[inline(always)]
    pub(crate) fn rank_below(self, v: u16) -> u32 {
        match v.checked_sub(self.first) {
            None | Some(0) => 0,
            Some(delta) => {
                1 + u8::try_from(delta).map_or(self.offsets.len(), |delta| {
                    self.offsets.partition_point(|offset| *offset < delta)
                }) as u32
            }
        }
    }

    /// `k` is below the cardinality.
    #[inline(always)]
    pub(crate) fn select(self, k: u32) -> u16 {
        match k.checked_sub(1) {
            None => self.first,
            Some(i) => self.first.wrapping_add(u16::from(self.offsets[i as usize])),
        }
    }

    #[inline]
    pub(crate) fn for_each(self, mut f: impl FnMut(u16)) {
        f(self.first);
        for &offset in self.offsets {
            f(self.first.wrapping_add(u16::from(offset)));
        }
    }

    /// `(stride, members)` when the members are one arithmetic progression.
    pub(crate) fn progression(self) -> Option<(u16, u32)> {
        let stride = usize::from(*self.offsets.first()?);
        self.offsets
            .iter()
            .enumerate()
            .all(|(i, &offset)| usize::from(offset) == (i + 1) * stride)
            .then_some((stride as u16, self.cardinality()))
    }
}

/// Ascending cursor over a [`BytesView`]: `at` is the rank of the head.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BytesRange<'a> {
    view: BytesView<'a>,
    at: usize,
}

impl<'a> BytesRange<'a> {
    pub(crate) fn at_rank(view: BytesView<'a>, k: u32) -> Self {
        Self {
            view,
            at: k as usize,
        }
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> u32 {
        (self.view.offsets.len() + 1).saturating_sub(self.at) as u32
    }

    #[inline(always)]
    pub(crate) fn front(&self) -> u16 {
        self.view.select(self.at as u32)
    }

    #[inline(always)]
    pub(crate) fn pop_front(&mut self) {
        self.at += 1;
    }

    /// The head is below `target`.
    #[inline]
    pub(crate) fn seek(&mut self, target: u16) {
        let offsets = self.view.offsets;
        let from = self.at.saturating_sub(1).min(offsets.len());
        self.at = 1 + match u8::try_from(target.wrapping_sub(self.view.first)) {
            Ok(delta) => from + offsets[from..].partition_point(|offset| *offset < delta),
            Err(_) => offsets.len(),
        };
    }
}

// ── Window ───────────────────────────────────────────────────────────────

/// `first`, then a bitmap of the positions after it: bit `p` is the member
/// `first + 1 + p`. The last byte holds the last member, so it is never
/// zero, and the cardinality is one more than the popcount.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WindowView<'a> {
    first: u16,
    bits: &'a [u8],
    len: u32,
}

/// The bitmap bytes of a Window payload.
fn window_bits(payload: &[u8]) -> &[u8] {
    payload.get(first_of(payload).1..).unwrap_or(&[])
}

/// Members a lone image holds: its count, or for a Window, whose count is
/// its bitmap's length, one more than the bitmap's popcount.
pub(crate) fn lone_cardinality(kind: u32, count: u32, payload: &[u8]) -> u32 {
    if is_window(kind) {
        1 + window_bits(payload)
            .iter()
            .map(|byte| byte.count_ones())
            .sum::<u32>()
    } else {
        count
    }
}

#[inline(always)]
fn nth_set_bit(mut word: u64, n: u32) -> u32 {
    for _ in 0..n {
        word &= word.wrapping_sub(1);
    }
    word.trailing_zeros()
}

impl<'a> WindowView<'a> {
    pub(crate) fn open(rest: &'a [u8], kind: u32, card: u32) -> Result<Self, AdmissionError> {
        let (first, head) = read_first(rest)?;
        let bits = &rest[head..];
        let lengths = if kind == K_WINDOW {
            1..=WINDOW_NARROW_MAX
        } else {
            WINDOW_NARROW_MAX + 1..=2 * WINDOW_NARROW_MAX
        };
        if !lengths.contains(&bits.len()) {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                head,
                "Window bitmap length is outside its kind's range",
            ));
        }
        let last = bits[bits.len() - 1];
        if last == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                head + bits.len() - 1,
                "Window ends past its last member",
            ));
        }
        if lone_cardinality(kind, 0, rest) != card {
            return Err(AdmissionError::new(
                AdmissionClass::Count,
                head,
                "Window cardinality differs from its parent prefix",
            ));
        }
        let span = 8 * bits.len() - last.leading_zeros() as usize;
        if usize::from(first) + span > usize::from(u16::MAX) {
            return Err(AdmissionError::new(
                AdmissionClass::PlaneRange,
                0,
                "Window final member exceeds u16",
            ));
        }
        Ok(Self {
            first,
            bits,
            len: card,
        })
    }

    pub(crate) fn from_validated(payload: &'a [u8], card: u32) -> Self {
        let (first, head) = first_of(payload);
        Self {
            first,
            bits: &payload[head..],
            len: card,
        }
    }

    #[inline(always)]
    pub(crate) fn cardinality(self) -> u32 {
        self.len
    }

    #[inline(always)]
    pub(crate) fn payload_len(self) -> usize {
        uleb_len(u32::from(self.first)) + self.bits.len()
    }

    pub(crate) fn bitmap_len(self) -> usize {
        self.bits.len()
    }

    /// The kind this bitmap's length belongs to.
    pub(crate) fn kind(self) -> u32 {
        if self.bits.len() <= WINDOW_NARROW_MAX {
            K_WINDOW
        } else {
            K_WINDOW_WIDE
        }
    }

    /// The bitmap as four little-endian words, zero past its bytes.
    #[inline(always)]
    fn words(self) -> [u64; 4] {
        let mut padded = [0u8; 32];
        let n = self.bits.len().min(32);
        padded[..n].copy_from_slice(&self.bits[..n]);
        let mut words = [0u64; 4];
        for (word, bytes) in words.iter_mut().zip(padded.as_chunks::<8>().0) {
            *word = u64::from_le_bytes(*bytes);
        }
        words
    }

    #[inline(always)]
    pub(crate) fn contains(self, v: u16) -> bool {
        match v.checked_sub(self.first) {
            None => false,
            Some(0) => true,
            Some(delta) => {
                let position = usize::from(delta - 1);
                self.bits
                    .get(position / 8)
                    .is_some_and(|byte| byte >> (position % 8) & 1 == 1)
            }
        }
    }

    #[inline(always)]
    pub(crate) fn rank_below(self, v: u16) -> u32 {
        let Some(delta) = v.checked_sub(self.first).filter(|delta| *delta != 0) else {
            return 0;
        };
        let below = usize::from(delta - 1).min(256);
        let words = self.words();
        let mut rank = 1;
        for word in &words[..below / 64] {
            rank += word.count_ones();
        }
        if below % 64 != 0 {
            rank += (words[below / 64] & ((1u64 << (below % 64)) - 1)).count_ones();
        }
        rank
    }

    /// `k` is below the cardinality.
    #[inline(always)]
    pub(crate) fn select(self, k: u32) -> u16 {
        let Some(mut rank) = k.checked_sub(1) else {
            return self.first;
        };
        for (w, word) in self.words().into_iter().enumerate() {
            let ones = word.count_ones();
            if rank < ones {
                return self
                    .first
                    .wrapping_add((1 + w as u32 * 64 + nth_set_bit(word, rank)) as u16);
            }
            rank -= ones;
        }
        self.first
    }

    #[inline]
    pub(crate) fn for_each(self, mut f: impl FnMut(u16)) {
        f(self.first);
        for (w, mut word) in self.words().into_iter().enumerate() {
            while word != 0 {
                f(self
                    .first
                    .wrapping_add((1 + w as u32 * 64 + word.trailing_zeros()) as u16));
                word &= word - 1;
            }
        }
    }

    /// `(stride, members)` when the members are one arithmetic progression.
    pub(crate) fn progression(self) -> Option<(u16, u32)> {
        let stride = u32::from(self.select(1).wrapping_sub(self.first));
        let mut next = u32::from(self.first);
        let mut regular = true;
        self.for_each(|member| {
            regular &= u32::from(member) == next;
            next += stride;
        });
        regular.then_some((stride as u16, self.len))
    }
}

/// Ascending cursor over a [`WindowView`]: the head is held decoded and
/// `words` keeps the members after it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WindowRange {
    words: [u64; 4],
    /// The member bit zero stands for.
    base: u32,
    front: u16,
    remaining: u32,
}

impl WindowRange {
    pub(crate) fn at_rank(view: WindowView<'_>, k: u32) -> Self {
        let mut range = Self {
            words: view.words(),
            base: u32::from(view.first) + 1,
            front: view.first,
            remaining: view.len.saturating_sub(k),
        };
        if let Some(skipped) = k.checked_sub(1) {
            range.drop_lowest(skipped);
            range.take();
        }
        range
    }

    fn drop_lowest(&mut self, mut n: u32) {
        for word in &mut self.words {
            let ones = word.count_ones();
            if n < ones {
                for _ in 0..n {
                    *word &= *word - 1;
                }
                return;
            }
            *word = 0;
            n -= ones;
        }
    }

    /// Move the lowest remaining member into the head.
    #[inline(always)]
    fn take(&mut self) {
        for (w, word) in self.words.iter_mut().enumerate() {
            if *word != 0 {
                self.front = (self.base + w as u32 * 64 + word.trailing_zeros()) as u16;
                *word &= *word - 1;
                return;
            }
        }
        self.remaining = 0;
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> u32 {
        self.remaining
    }

    #[inline(always)]
    pub(crate) fn front(&self) -> u16 {
        self.front
    }

    #[inline(always)]
    pub(crate) fn pop_front(&mut self) {
        self.remaining -= 1;
        if self.remaining != 0 {
            self.take();
        }
    }

    /// The head is below `target`.
    #[inline]
    pub(crate) fn seek(&mut self, target: u16) {
        let below = (u32::from(target).saturating_sub(self.base) as usize).min(256);
        let mut skipped = 1;
        for (w, word) in self.words.iter_mut().enumerate() {
            let mask = match below.saturating_sub(w * 64) {
                0 => break,
                bits if bits >= 64 => u64::MAX,
                bits => (1u64 << bits) - 1,
            };
            skipped += (*word & mask).count_ones();
            *word &= !mask;
        }
        self.remaining = self.remaining.saturating_sub(skipped);
        if self.remaining != 0 {
            self.take();
        }
    }
}

// ── the small lone chunk ─────────────────────────────────────────────────

/// The chunk of a lone image whose kind is Bytes or Window. The directory
/// reader's chunk view has no arm for these: a small container exists only
/// as the whole of a lone image, and is read through this type.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SmallView<'a> {
    Bytes(BytesView<'a>),
    Window(WindowView<'a>),
}

impl<'a> SmallView<'a> {
    /// Admit `payload`, the whole extent after a lone header of this `kind`
    /// and `count`.
    pub(crate) fn open(payload: &'a [u8], kind: u32, count: u32) -> Result<Self, AdmissionError> {
        let view = if kind == K_BYTES {
            Self::Bytes(BytesView::open(payload, count)?)
        } else {
            Self::Window(WindowView::open(
                payload,
                kind,
                lone_cardinality(kind, count, payload),
            )?)
        };
        if view.payload_len() != payload.len() {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "container does not exactly cover its payload extent",
            ));
        }
        Ok(view)
    }

    pub(crate) fn from_validated(payload: &'a [u8], kind: u32, count: u32) -> Self {
        if kind == K_BYTES {
            Self::Bytes(BytesView::from_validated(payload))
        } else {
            Self::Window(WindowView::from_validated(
                payload,
                lone_cardinality(kind, count, payload),
            ))
        }
    }

    /// The view of an admitted payload whose cardinality is known.
    #[inline(always)]
    pub(crate) fn with_cardinality(payload: &'a [u8], kind: u32, card: u32) -> Self {
        if kind == K_BYTES {
            Self::Bytes(BytesView::from_validated(payload))
        } else {
            Self::Window(WindowView::from_validated(payload, card))
        }
    }
    pub(crate) fn verify(self) -> Result<(), AdmissionError> {
        match self {
            Self::Bytes(bytes) => bytes.verify(),
            // A Window's structure is its whole content: admission proved it.
            Self::Window(_) => Ok(()),
        }
    }

    pub(crate) fn kind(self) -> u32 {
        match self {
            Self::Bytes(_) => K_BYTES,
            Self::Window(window) => window.kind(),
        }
    }

    /// The count this chunk's lone header folds.
    pub(crate) fn lone_count(self) -> u32 {
        match self {
            Self::Bytes(bytes) => bytes.cardinality(),
            Self::Window(window) => window_count(window.kind(), window.bitmap_len()),
        }
    }

    #[inline(always)]
    pub(crate) fn cardinality(self) -> u32 {
        match self {
            Self::Bytes(bytes) => bytes.cardinality(),
            Self::Window(window) => window.cardinality(),
        }
    }

    pub(crate) fn payload_len(self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.payload_len(),
            Self::Window(window) => window.payload_len(),
        }
    }

    #[inline(always)]
    pub(crate) fn contains(self, v: u16) -> bool {
        match self {
            Self::Bytes(bytes) => bytes.contains(v),
            Self::Window(window) => window.contains(v),
        }
    }

    #[inline(always)]
    pub(crate) fn rank_below(self, v: u16) -> u32 {
        match self {
            Self::Bytes(bytes) => bytes.rank_below(v),
            Self::Window(window) => window.rank_below(v),
        }
    }

    /// `k` is below the cardinality.
    #[inline(always)]
    pub(crate) fn select(self, k: u32) -> u16 {
        match self {
            Self::Bytes(bytes) => bytes.select(k),
            Self::Window(window) => window.select(k),
        }
    }

    #[inline]
    pub(crate) fn for_each(self, f: impl FnMut(u16)) {
        match self {
            Self::Bytes(bytes) => bytes.for_each(f),
            Self::Window(window) => window.for_each(f),
        }
    }

    /// `(first, stride, members)` when the members are one arithmetic
    /// progression of at least two.
    pub(crate) fn progression(self) -> Option<(u16, u16, u32)> {
        let (stride, members) = match self {
            Self::Bytes(bytes) => bytes.progression()?,
            Self::Window(window) => window.progression()?,
        };
        Some((self.select(0), stride, members))
    }
}

/// Ascending cursor over a [`SmallView`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum SmallRange<'a> {
    Bytes(BytesRange<'a>),
    Window(WindowRange),
}

impl<'a> SmallRange<'a> {
    pub(crate) fn open(view: SmallView<'a>) -> Self {
        match view {
            SmallView::Bytes(bytes) => Self::Bytes(BytesRange::at_rank(bytes, 0)),
            SmallView::Window(window) => Self::Window(WindowRange::at_rank(window, 0)),
        }
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> u32 {
        match self {
            Self::Bytes(range) => range.remaining(),
            Self::Window(range) => range.remaining(),
        }
    }

    #[inline(always)]
    pub(crate) fn front(&self) -> u16 {
        match self {
            Self::Bytes(range) => range.front(),
            Self::Window(range) => range.front(),
        }
    }

    #[inline(never)]
    pub(crate) fn pop_front(&mut self) {
        match self {
            Self::Bytes(range) => range.pop_front(),
            Self::Window(range) => range.pop_front(),
        }
    }

    /// The head is below `target`.
    #[inline(never)]
    pub(crate) fn seek(&mut self, target: u16) {
        match self {
            Self::Bytes(range) => range.seek(target),
            Self::Window(range) => range.seek(target),
        }
    }
}
// ── the lone image ───────────────────────────────────────────────────────

/// Control bit of an image that is one chunk at key zero:
/// `control = 0x80 | folded << 3 | kind`.
pub(crate) const CONTROL_LONE: u8 = 0x80;
/// Counts through this fold into the control byte as `count - 1`.
const FOLDED_MAX: u32 = 14;
/// `folded` value saying one byte follows: `count - 15`.
const FOLDED_BYTE: u32 = 14;
/// `folded` value saying a little-endian `u16` follows: `count - 1`.
const FOLDED_WORD: u32 = 15;
/// Largest count the one-byte extension reaches.
const BYTE_MAX: u32 = FOLDED_MAX + 1 + 255;

/// A Window's lone count, `1..=16`: its bitmap bytes within its kind's
/// range. Zero for any other kind, whose count is its cardinality.
pub(crate) fn window_count(kind: u32, bitmap: usize) -> u32 {
    match kind {
        K_WINDOW => bitmap as u32,
        K_WINDOW_WIDE => (bitmap - WINDOW_NARROW_MAX) as u32,
        _ => 0,
    }
}

/// The one count a lone payload does not say for itself: a Window's
/// [`window_count`] (its cardinality is its popcount), any other kind's
/// cardinality.
pub(crate) fn lone_count(kind: u32, card: u32, window: u32) -> u32 {
    if is_window(kind) { window } else { card }
}

/// A Window's count always folds whole; any other count past 14 continues
/// in one byte, past 270 in two.
pub(crate) fn lone_header_len(kind: u32, count: u32) -> usize {
    if is_window(kind) || count <= FOLDED_MAX {
        1
    } else if count <= BYTE_MAX {
        2
    } else {
        3
    }
}

pub(crate) fn write_lone_header(out: &mut [u8], kind: u32, count: u32) -> usize {
    let (folded, at) = if is_window(kind) || count <= FOLDED_MAX {
        (count - 1, 1)
    } else if count <= BYTE_MAX {
        out[1] = (count - FOLDED_MAX - 1) as u8;
        (FOLDED_BYTE, 2)
    } else {
        out[1..3].copy_from_slice(&((count - 1) as u16).to_le_bytes());
        (FOLDED_WORD, 3)
    };
    out[0] = CONTROL_LONE | (folded as u8) << 3 | kind as u8;
    at
}

/// A lone image's kind, count and payload offset, from its first bytes.
pub(crate) fn read_lone_header(bytes: &[u8]) -> Result<(u32, u32, usize), AdmissionError> {
    let control = u32::from(bytes[0]);
    let (kind, folded) = (control & 7, control >> 3 & 15);
    let truncated = || AdmissionError::new(AdmissionClass::Extent, 1, "lone count is truncated");
    Ok(match folded {
        _ if is_window(kind) => (kind, folded + 1, 1),
        FOLDED_BYTE => (
            kind,
            FOLDED_MAX + 1 + u32::from(*bytes.get(1).ok_or_else(truncated)?),
            2,
        ),
        FOLDED_WORD => {
            let count = 1 + u32::from(read_u16_at(bytes.get(1..3).ok_or_else(truncated)?, 0));
            if count <= BYTE_MAX {
                return Err(AdmissionError::new(
                    AdmissionClass::Count,
                    1,
                    "lone count is not in its shortest form",
                ));
            }
            (kind, count, 3)
        }
        _ => (kind, folded + 1, 1),
    })
}

/// Payload bytes of a lone image, from the header's count and at most the
/// payload's first 20 bytes. Offsets in a refusal are relative to `rest`.
pub(crate) fn lone_payload_len(
    kind: u32,
    count: u32,
    rest: &[u8],
) -> Result<usize, AdmissionError> {
    let count = count as usize;
    Ok(match kind {
        crate::wire::K_STRIDE => 4,
        crate::wire::K_CONE => 11 + count,
        crate::wire::K_ARRAY => 2 * count,
        K_BYTES => first_of(rest).1 + count - 1,
        K_WINDOW => first_of(rest).1 + count,
        K_WINDOW_WIDE => first_of(rest).1 + WINDOW_NARROW_MAX + count,
        crate::wire::K_RUNS => {
            let mut at = 0;
            runs_payload_len(
                read_uleb_checked(rest, &mut at, 32_767, AdmissionClass::Count)? as usize + 1,
            )
        }
        _ => {
            let window = rest.get(18..20).map(|_| usize::from(read_u16_at(rest, 18)));
            match window {
                Some(words) if words != 0 && words.is_multiple_of(crate::BLOCK_WORDS) => {
                    crate::wire::words_payload_len(words)
                }
                Some(_) => {
                    return Err(AdmissionError::new(
                        AdmissionClass::Count,
                        16,
                        "Words window is not whole blocks within its u16 chunk",
                    ));
                }
                None => {
                    return Err(AdmissionError::new(
                        AdmissionClass::Extent,
                        0,
                        "Words metadata is truncated",
                    ));
                }
            }
        }
    })
}
